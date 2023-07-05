package endpoint_test

import (
	"context"
	"fmt"

	"github.com/doublemo/nakama-cluster/endpoint"
)

func ExampleChain() {
	myEndpoint := endpoint.New("dsdd", "", "", nil, func(ctx context.Context, request interface{}) (response interface{}, err error) {
		fmt.Println("my endpoint!")
		return nil, nil
	})

	e := endpoint.Chain(
		annotate("first"),
		annotate("second"),
		annotate("third"),
	)(myEndpoint)

	if _, err := e.Process(ctx, req); err != nil {
		panic(err)
	}

	// Output:
	// first pre
	// second pre
	// third pre
	// my endpoint!
	// third post
	// second post
	// first post
}

var (
	ctx = context.Background()
	req = struct{}{}
)

func annotate(s string) endpoint.Middleware {
	return func(next endpoint.EndpointInterceptor) endpoint.EndpointInterceptor {
		return endpoint.EndpointFunc(func(ctx context.Context, request interface{}) (interface{}, error) {
			fmt.Println(s, "pre")
			defer fmt.Println(s, "post")
			return next.Process(ctx, request)
		})
	}
}
