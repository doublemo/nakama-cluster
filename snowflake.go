package nakamacluster

/**
 * doc https://github.com/baidu/uid-generator/blob/master/README.zh_cn.md
 * Represents an implementation of {@link UidGenerator}
 *
 * The unique id has 64bits (long), default allocated as blow:<br>
 * <li>sign: The highest bit is 0
 * <li>delta seconds: The next 28 bits, represents delta seconds since a customer epoch(2016-05-20 00:00:00.000).
 *                    Supports about 8.7 years until to 2024-11-20 21:24:16
 * <li>worker id: The next 22 bits, represents the worker's id which assigns based on database, max id is about 420W
 * <li>sequence: The next 13 bits, represents a sequence within the same second, max for 8192/s<br><br>
 *
 * The {@link DefaultUidGenerator#parseUID(long)} is a tool method to parse the bits
 *
 * <pre>{@code
 * +------+----------------------+----------------+-----------+
 * | sign |     delta seconds    | worker node id | sequence  |
 * +------+----------------------+----------------+-----------+
 *   1bit          28bits              22bits         13bits
 * }</pre>
 *
 * You can also specified the bits by Spring property setting.
 * <li>timeBits: default as 28
 * <li>workerBits: default as 22
 * <li>seqBits: default as 13
 * <li>epochStr: Epoch date string format 'yyyy-MM-dd'. Default as '2016-05-20'<p>
 *
 * <b>Note that:</b> The total bits must be 64 -1
 **/

import (
	"fmt"
	"hash/crc32"
	"sync"
	"time"
)

const (
	defaultTimeBits   = 28
	defaultWorkerBits = 22
	defaultSeqBits    = 13
)

type snowflake struct {
	timeBits        int
	workerBits      int
	seqBits         int
	epochStr        string
	epochSeconds    uint64
	workerId        uint64
	sequence        uint64
	lastSecond      uint64
	maxDeltaSeconds uint64
	maxWorkerId     uint64
	maxSequence     uint64
	timestampShift  int
	workerIdShift   int
	sync.RWMutex
}

func (s *snowflake) NextId() (uint64, error) {
	currentSecond, err := s.getCurrentSecond()
	if err != nil {
		return 0, err
	}

	s.RLock()
	lastSecond := s.lastSecond
	sequence := s.sequence
	maxSequence := s.maxSequence
	epochSeconds := s.epochSeconds
	timestampShift := s.timestampShift
	workerId := s.workerId
	workerIdShift := s.workerIdShift
	s.RUnlock()

	if currentSecond < lastSecond {
		return 0, fmt.Errorf("timestamp bits is exhausted. Refusing UID generate. Now: %d", lastSecond-currentSecond)
	}

	if currentSecond == lastSecond {
		sequence = (sequence + 1) & maxSequence
		if sequence == 0 {
			currentSecond, err = s.getNextSecond(lastSecond)
			if err != nil {
				return 0, err
			}
		}
	} else {
		sequence = 0
	}

	s.Lock()
	s.lastSecond = currentSecond
	s.sequence = sequence
	s.Unlock()

	deltaSeconds := currentSecond - epochSeconds
	return (deltaSeconds << uint(timestampShift)) | (workerId << uint(workerIdShift)) | sequence, nil
}

func (s *snowflake) getCurrentSecond() (uint64, error) {
	s.RLock()
	epochSeconds := s.epochSeconds
	maxDeltaSeconds := s.maxDeltaSeconds
	s.RUnlock()

	currentSecond := uint64(time.Now().Unix())
	if currentSecond-epochSeconds > maxDeltaSeconds {
		return 0, fmt.Errorf("timestamp bits is exhausted. Refusing UID generate. Now: %d %d", currentSecond, maxDeltaSeconds)
	}

	return currentSecond, nil
}

func (s *snowflake) getNextSecond(lastTimestamp uint64) (uint64, error) {
	timestamp, err := s.getCurrentSecond()
	if err != nil {
		return 0, err
	}

	for timestamp <= lastTimestamp {
		time.Sleep(time.Duration(lastTimestamp-timestamp) * time.Millisecond)
		timestamp, err = s.getCurrentSecond()
		if err != nil {
			return 0, err
		}
	}

	return timestamp, nil
}

func (s *snowflake) Parse(id uint64) string {
	s.RLock()
	timestampBits := s.timeBits
	workerIdBits := s.workerBits
	sequenceBits := s.seqBits
	epochSeconds := s.epochSeconds
	s.RUnlock()

	totalBits := 1 + timestampBits + workerIdBits + sequenceBits
	sequence := (id << uint(totalBits-sequenceBits)) >> uint(totalBits-sequenceBits)
	workerId := (id << uint(timestampBits+1)) >> uint(totalBits-workerIdBits)
	deltaSeconds := id >> uint(workerIdBits+sequenceBits)
	thatTime := time.Unix(int64(epochSeconds+deltaSeconds), 0)
	return fmt.Sprintf("{\"UID\":\"%d\",\"timestamp\":\"%s\",\"workerId\":\"%d\",\"sequence\":\"%d\"}", id, thatTime.Format("2006-01-02 00:00:00"), workerId, sequence)
}

func newSnowflake(nodeId string) *snowflake {
	workerId := crc32.ChecksumIEEE([]byte(nodeId))
	maxDeltaSeconds := ^(-1 << defaultTimeBits)
	maxWorkerId := ^(-1 << defaultWorkerBits)
	maxSequence := ^(-1 << defaultSeqBits)
	uidg := snowflake{
		timeBits:        defaultTimeBits,
		workerBits:      defaultWorkerBits,
		seqBits:         defaultSeqBits,
		epochStr:        "2022-09-27",
		workerId:        uint64(workerId),
		maxDeltaSeconds: uint64(maxDeltaSeconds),
		maxWorkerId:     uint64(maxWorkerId),
		maxSequence:     uint64(maxSequence),
		timestampShift:  defaultWorkerBits + defaultSeqBits,
		workerIdShift:   defaultSeqBits,
	}

	es, _ := time.Parse("2006-01-02", uidg.epochStr)
	uidg.epochSeconds = uint64(es.Unix())
	return &uidg
}
