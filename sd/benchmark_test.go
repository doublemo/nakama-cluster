package sd

import (
	"io"
	"testing"

	"github.com/doublemo/nakama-cluster/endpoint"
	"go.uber.org/zap"
)

func BenchmarkEndpoints(b *testing.B) {
	var (
		ca      = make(closer)
		cb      = make(closer)
		cmap    = map[string]io.Closer{"a": ca, "b": cb}
		factory = func(instance string) (endpoint.Endpoint, io.Closer, error) { return endpoint.Nop, cmap[instance], nil }
		c       = newEndpointCache(factory, zap.NewNop(), endpointerOptions{})
	)

	b.ReportAllocs()

	c.Update(Event{Instances: []string{"a", "b"}})

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Endpoints()
		}
	})
}
