package endpoint

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"
)

type Endpoint interface {
	ID() string
	Address() string
	Name() string
	String() string
	Var(k string, defaultValue ...string) string
	SetVar(k string, v string)
	Process(ctx context.Context, request interface{}) (response interface{}, err error)
	SetProcessFunc(process EndpointFunc)
	FromString(s string) error
	Domain() string
	RemoteAddress() (string, error)
}

type LocalEndpoint struct {
	values  url.Values
	process atomic.Value
	sync.RWMutex
}

func New(id, name, addr string, vars map[string]string, process EndpointFunc) Endpoint {
	values := make(url.Values)
	values.Set("id", id)
	values.Set("name", name)
	values.Set("addr", addr)

	for k, v := range vars {
		values.Set(k, v)
	}

	endpoint := &LocalEndpoint{values: values}
	endpoint.process.Store(process)
	return endpoint
}

func (endpoint *LocalEndpoint) ID() string {
	endpoint.RLock()
	defer endpoint.RUnlock()
	return endpoint.values.Get("id")
}

func (endpoint *LocalEndpoint) Name() string {
	endpoint.RLock()
	defer endpoint.RUnlock()
	return endpoint.values.Get("name")
}

func (endpoint *LocalEndpoint) Address() string {
	endpoint.RLock()
	defer endpoint.RUnlock()
	return endpoint.values.Get("addr")
}

func (endpoint *LocalEndpoint) Domain() string {
	endpoint.RLock()
	defer endpoint.RUnlock()
	return endpoint.values.Get("domain")
}

func (endpoint *LocalEndpoint) Var(k string, defaultValues ...string) string {
	endpoint.RLock()
	defer endpoint.RUnlock()

	defaultValue := ""
	if len(defaultValues) > 0 {
		defaultValue = defaultValues[0]
	}

	v := endpoint.values.Get(k)
	if len(v) < 1 {
		return defaultValue
	}
	return v
}

func (endpoint *LocalEndpoint) SetVar(k string, v string) {
	endpoint.Lock()
	endpoint.values.Set(k, v)
	endpoint.Unlock()
}

func (endpoint *LocalEndpoint) String() string {
	endpoint.Lock()
	defer endpoint.Unlock()
	return endpoint.values.Encode()
}

func (endpoint *LocalEndpoint) Process(ctx context.Context, request interface{}) (response interface{}, err error) {
	if handle, ok := endpoint.process.Load().(EndpointFunc); ok && handle != nil {
		return handle(ctx, request)
	}
	return nil, nil
}

func (endpoint *LocalEndpoint) SetProcessFunc(process EndpointFunc) {
	endpoint.process.Store(process)
}

func (endpoint *LocalEndpoint) FromString(s string) error {
	values, err := url.ParseQuery(s)
	if err != nil {
		return err
	}

	endpoint.Lock()
	endpoint.values = values
	endpoint.Unlock()
	return nil
}

func (endpoint *LocalEndpoint) RemoteAddress() (string, error) {
	domain := endpoint.Domain()
	if len(domain) < 1 {
		domain = endpoint.Address()
	} else {
		uri, err := url.Parse(domain)
		if err != nil {
			return "", err
		}
		domain = uri.Host + ":" + uri.Port()
	}
	return domain, nil
}

var Nop = New("Nop", "Nop", "", make(map[string]string), nil)
