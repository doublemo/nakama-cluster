package nakamacluster

import (
	"context"
	"testing"
	"time"

	"github.com/doublemo/nakama-cluster/sd"
	"github.com/gofrs/uuid"
	"go.uber.org/zap"
)

func TestSessionWatch(t *testing.T) {
	//land_mint
	tk := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiJhY2VkODE4NC05ZDVkLTQzNDEtOTI4Yy1jZmM2NWViMDU0OWEiLCJ1c24iOiJyYW5keS5tc25AcXEuY29tIiwiZXhwIjoxNjY4NzU4OTY5fQ.c2ZRonHdhcrVKzK-AXkVv_F6-Way0p_Gtx0CcxX3mOA"
	rk := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiJhY2VkODE4NC05ZDVkLTQzNDEtOTI4Yy1jZmM2NWViMDU0OWEiLCJ1c24iOiJyYW5keS5tc25AcXEuY29tIiwiZXhwIjoxNjY4NzYxOTY5fQ.WFTgHYvxf76rJ4uxFYYsKJ_QwegWFsfhNyRixwwTYQk"
	sess := NewSessionCache(context.Background(), zap.NewNop(), "/test-kkk/sessions/", "v1", []string{"192.168.0.71:12379", "192.168.0.71:22379", "192.168.0.71:32379"}, sd.EtcdClientOptions{})
	uid := uuid.Must(uuid.NewV4())
	go func() {
		ts := time.NewTicker(time.Second)
		defer ts.Stop()

		for {
			<-ts.C
			sess.Add(uid, time.Now().Add(time.Second*15).Unix(), tk, time.Now().Add(time.Second*20).Unix(), rk)
			break
		}
	}()

	go func() {
		ts := time.NewTicker(time.Second * 5)
		defer ts.Stop()

		for {
			<-ts.C
			sess2 := NewSessionCache(context.Background(), zap.NewNop(), "/test-kkk/sessions/", "v1", []string{"192.168.0.71:12379", "192.168.0.71:22379", "192.168.0.71:32379"}, sd.EtcdClientOptions{})
			sess2.RemoveAll(uid)
			break
		}
	}()

	time.Sleep(time.Second * 30)
}
