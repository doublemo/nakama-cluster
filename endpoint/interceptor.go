package endpoint

import "context"

type EndpointInterceptor interface {
	Process(ctx context.Context, request interface{}) (response interface{}, err error)
}

type EndpointFunc func(ctx context.Context, request interface{}) (response interface{}, err error)

func (endpoint EndpointFunc) Process(ctx context.Context, request interface{}) (response interface{}, err error) {
	return endpoint(ctx, request)
}

type Middleware func(EndpointInterceptor) EndpointInterceptor

func Chain(outer Middleware, others ...Middleware) Middleware {
	return func(next EndpointInterceptor) EndpointInterceptor {
		for i := len(others) - 1; i >= 0; i-- { // reverse
			next = others[i](next)
		}
		return outer(next)
	}
}
