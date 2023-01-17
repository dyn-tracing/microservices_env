// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package loadbalancerhttp

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
    "go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.opentelemetry.io/collector/service/servicetest"
    "go.opentelemetry.io/collector/pdata/pcommon"
    "go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

func TestNewTracesExporter(t *testing.T) {
	for _, tt := range []struct {
		desc   string
		config *Config
		err    error
	}{
		{
			"simple",
			simpleConfig(),
			nil,
		},
		{
			"empty",
			&Config{},
			errNoResolver,
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			// test
			_, err := newTracesExporter(componenttest.NewNopExporterCreateSettings(), tt.config, zap.NewNop())

			// verify
			require.Equal(t, tt.err, err)
		})
	}
}

func TestTracesExporterStart(t *testing.T) {
	for _, tt := range []struct {
		desc string
		te   *traceExporterImp
		err  error
	}{
		{
			"ok",
			func() *traceExporterImp {
				p, _ := newTracesExporter(componenttest.NewNopExporterCreateSettings(), simpleConfig(), zap.NewNop())
				return p
			}(),
			nil,
		},
		{
			"error",
			func() *traceExporterImp {
				lb, _ := newLoadBalancer(componenttest.NewNopExporterCreateSettings(), simpleConfig(), nil)
				p, _ := newTracesExporter(componenttest.NewNopExporterCreateSettings(), simpleConfig(), zap.NewNop())

				lb.res = &mockResolver{
					onStart: func(context.Context) error {
						return errors.New("some expected err")
					},
				}
				p.loadBalancer = lb

				return p
			}(),
			errors.New("some expected err"),
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			p := tt.te

			// test
			res := p.Start(context.Background(), componenttest.NewNopHost())
			defer p.Shutdown(context.Background())

			// verify
			require.Equal(t, tt.err, res)
		})
	}
}

func TestTracesExporterShutdown(t *testing.T) {
	p, err := newTracesExporter(componenttest.NewNopExporterCreateSettings(), simpleConfig(), zap.NewNop())
	require.NotNil(t, p)
	require.NoError(t, err)

	// test
	res := p.Shutdown(context.Background())

	// verify
	assert.Nil(t, res)
}

func TestConsumeTraces(t *testing.T) {
	componentFactory := func(ctx context.Context, endpoint string) (component.Component, error) {
		return newNopMockTracesExporter(), nil
	}
	lb, err := newLoadBalancer(componenttest.NewNopExporterCreateSettings(), simpleConfig(), componentFactory)
	require.NotNil(t, lb)
	require.NoError(t, err)

	p, err := newTracesExporter(componenttest.NewNopExporterCreateSettings(), simpleConfig(), zap.NewNop())
	require.NotNil(t, p)
	require.NoError(t, err)

	// pre-load an exporter here, so that we don't use the actual OTLP exporter
	lb.exporters["endpoint-1"] = newNopMockTracesExporter()
	lb.res = &mockResolver{
		triggerCallbacks: true,
		onResolve: func(ctx context.Context) ([]string, error) {
			return []string{"endpoint-1"}, nil
		},
	}
	p.loadBalancer = lb

	err = p.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)
	defer p.Shutdown(context.Background())

	// test
	res := p.ConsumeTraces(context.Background(), simpleTraces())

	// verify
	assert.Nil(t, res)
}

func TestConsumeTracesExporterNotFound(t *testing.T) {
	componentFactory := func(ctx context.Context, endpoint string) (component.Component, error) {
		return newNopMockTracesExporter(), nil
	}
	lb, err := newLoadBalancer(componenttest.NewNopExporterCreateSettings(), simpleConfig(), componentFactory)
	require.NotNil(t, lb)
	require.NoError(t, err)

	p, err := newTracesExporter(componenttest.NewNopExporterCreateSettings(), simpleConfig(), zap.NewNop())
	require.NotNil(t, p)
	require.NoError(t, err)

	lb.res = &mockResolver{
		triggerCallbacks: true,
		onResolve: func(ctx context.Context) ([]string, error) {
			return []string{"endpoint-1"}, nil
		},
	}
	p.loadBalancer = lb

	err = p.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)
	defer p.Shutdown(context.Background())

	// test
	res := p.ConsumeTraces(context.Background(), simpleTraces())

	// verify
	assert.Error(t, res)
	assert.EqualError(t, res, fmt.Sprintf("couldn't find the exporter for the endpoint %q", "endpoint-1"))
}

func TestConsumeTracesExporterNoEndpoint(t *testing.T) {
	componentFactory := func(ctx context.Context, endpoint string) (component.Component, error) {
		return newNopMockTracesExporter(), nil
	}
	lb, err := newLoadBalancer(componenttest.NewNopExporterCreateSettings(), simpleConfig(), componentFactory)
	require.NotNil(t, lb)
	require.NoError(t, err)

	p, err := newTracesExporter(componenttest.NewNopExporterCreateSettings(), simpleConfig(), zap.NewNop())
	require.NotNil(t, p)
	require.NoError(t, err)

	lb.res = &mockResolver{
		triggerCallbacks: true,
		onResolve: func(ctx context.Context) ([]string, error) {
			return nil, nil
		},
	}
	p.loadBalancer = lb

	err = p.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)
	defer p.Shutdown(context.Background())

	// test
	res := p.ConsumeTraces(context.Background(), simpleTraces())

	// verify
	assert.Error(t, res)
	assert.EqualError(t, res, fmt.Sprintf("couldn't find the exporter for the endpoint %q", ""))
}

func TestConsumeTracesUnexpectedExporterType(t *testing.T) {
	componentFactory := func(ctx context.Context, endpoint string) (component.Component, error) {
		return newNopMockExporter(), nil
	}
	lb, err := newLoadBalancer(componenttest.NewNopExporterCreateSettings(), simpleConfig(), componentFactory)
	require.NotNil(t, lb)
	require.NoError(t, err)

	p, err := newTracesExporter(componenttest.NewNopExporterCreateSettings(), simpleConfig(), zap.NewNop())
	require.NotNil(t, p)
	require.NoError(t, err)

	// pre-load an exporter here, so that we don't use the actual OTLP exporter
	lb.exporters["endpoint-1"] = newNopMockExporter()
	lb.res = &mockResolver{
		triggerCallbacks: true,
		onResolve: func(ctx context.Context) ([]string, error) {
			return []string{"endpoint-1"}, nil
		},
	}
	p.loadBalancer = lb

	err = p.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)
	defer p.Shutdown(context.Background())

	// test
	res := p.ConsumeTraces(context.Background(), simpleTraces())

	// verify
	assert.Error(t, res)
	assert.EqualError(t, res, fmt.Sprintf("expected *component.TracesExporter but got %T", newNopMockExporter()))
}

func TestBuildExporterConfig(t *testing.T) {
	// prepare
	factories, err := componenttest.NopFactories()
	require.NoError(t, err)

	factories.Exporters[typeStr] = NewFactory()

	cfg, err := servicetest.LoadConfigAndValidate(filepath.Join("testdata", "test-build-exporter-config.yaml"), factories)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	c := cfg.Exporters[component.NewID(typeStr)]
	require.NotNil(t, c)

	// test
	defaultCfg := otlpexporter.NewFactory().CreateDefaultConfig().(*otlpexporter.Config)
	exporterCfg := buildExporterConfig(c.(*Config), "the-endpoint")

	// verify
	grpcSettings := defaultCfg.GRPCClientSettings
	grpcSettings.Endpoint = "the-endpoint"
	assert.Equal(t, grpcSettings, exporterCfg.GRPCClientSettings)

	assert.Equal(t, defaultCfg.TimeoutSettings, exporterCfg.TimeoutSettings)
	assert.Equal(t, defaultCfg.QueueSettings, exporterCfg.QueueSettings)
	assert.Equal(t, defaultCfg.RetrySettings, exporterCfg.RetrySettings)
}

func appendSimpleTraceWithID(dest ptrace.ResourceSpans, id pcommon.TraceID) {
	dest.ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetTraceID(id)
}

func TestBatchWithTwoTraces(t *testing.T) {
    sink := new(consumertest.TracesSink)
	componentFactory := func(ctx context.Context, endpoint string) (component.Component, error) {
		return newNopMockTracesExporter(), nil
	}
	lb, err := newLoadBalancer(componenttest.NewNopExporterCreateSettings(), simpleConfig(), componentFactory)
	require.NotNil(t, lb)
	require.NoError(t, err)

	p, err := newTracesExporter(componenttest.NewNopExporterCreateSettings(), simpleConfig(), zap.NewNop())
	require.NotNil(t, p)
	require.NoError(t, err)

	p.loadBalancer = lb
	err = p.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

    lb.addMissingExporters(context.Background(), []string{"endpoint-1"})

    td := simpleTraces()
	appendSimpleTraceWithID(td.ResourceSpans().AppendEmpty(), [16]byte{2, 3, 4, 5})

	// test
	err = p.ConsumeTraces(context.Background(), td)

	// verify
	assert.NoError(t, err)
	assert.Len(t, sink.AllTraces(), 2)
}

func TestNoTracesInBatch(t *testing.T) {
	for _, tt := range []struct {
		desc  string
		batch ptrace.Traces
        routingKey routingKey
		err        error
	}{
		{
			"no resource spans",
			ptrace.NewTraces(),
            traceIDRouting,
			errors.New("empty resource spans"),
		},
		{
			"no instrumentation library spans",
			func() ptrace.Traces {
				batch := ptrace.NewTraces()
				batch.ResourceSpans().AppendEmpty()
				return batch
			}(),
            traceIDRouting,
			errors.New("empty scope spans"),
		},
		{
			"no spans",
			func() ptrace.Traces {
				batch := ptrace.NewTraces()
				batch.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
				return batch
			}(),
            svcRouting,
			errors.New("empty spans"),
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
            res, err := routingIdentifiersFromTraces(tt.batch, tt.routingKey)
			assert.Equal(t, err, tt.err)
			assert.Equal(t, res, map[string]bool(nil))
		})
	}
}

func TestRollingUpdatesWhenConsumeTraces(t *testing.T) {
	// this test is based on the discussion in the following issue for this exporter:
	// https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/1690
	// prepare

	// simulate rolling updates, the dns resolver should resolve in the following order
	// ["127.0.0.1"] -> ["127.0.0.1", "127.0.0.2"] -> ["127.0.0.2"]
	res, err := newDNSResolver(zap.NewNop(), "service-1", "")
	require.NoError(t, err)

	var lastResolved []string
	res.onChange(func(s []string) {
		lastResolved = s
	})

	resolverCh := make(chan struct{}, 1)
	counter := 0
	resolve := [][]net.IPAddr{
		{
			{IP: net.IPv4(127, 0, 0, 1)},
		}, {
			{IP: net.IPv4(127, 0, 0, 1)},
			{IP: net.IPv4(127, 0, 0, 2)},
		}, {
			{IP: net.IPv4(127, 0, 0, 2)},
		},
	}
	res.resolver = &mockDNSResolver{
		onLookupIPAddr: func(context.Context, string) ([]net.IPAddr, error) {
			defer func() {
				counter++
			}()

			if counter <= 2 {
				return resolve[counter], nil
			}

			if counter == 3 {
				// stop as soon as rolling updates end
				resolverCh <- struct{}{}
			}

			return resolve[2], nil
		},
	}
	res.resInterval = 10 * time.Millisecond

	cfg := &Config{
		Resolver: ResolverSettings{
			DNS: &DNSResolver{Hostname: "service-1", Port: ""},
		},
	}
	componentFactory := func(ctx context.Context, endpoint string) (component.Component, error) {
		return newNopMockTracesExporter(), nil
	}
	lb, err := newLoadBalancer(componenttest.NewNopExporterCreateSettings(), cfg, componentFactory)
	require.NotNil(t, lb)
	require.NoError(t, err)

	p, err := newTracesExporter(componenttest.NewNopExporterCreateSettings(), cfg, zap.NewNop())
	require.NotNil(t, p)
	require.NoError(t, err)

	lb.res = res
	p.loadBalancer = lb

	var counter1, counter2 int64
	defaultExporters := map[string]component.Component{
		"127.0.0.1": newMockTracesExporter(func(ctx context.Context, td ptrace.Traces) error {
			atomic.AddInt64(&counter1, 1)
			// simulate an unreachable backend
			time.Sleep(10 * time.Second)
			return nil
		},
		),
		"127.0.0.2": newMockTracesExporter(func(ctx context.Context, td ptrace.Traces) error {
			atomic.AddInt64(&counter2, 1)
			return nil
		},
		),
	}

	// test
	err = p.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)
	defer p.Shutdown(context.Background())
	// ensure using default exporters
	lb.updateLock.Lock()
	lb.exporters = defaultExporters
	lb.updateLock.Unlock()
	lb.res.onChange(func(endpoints []string) {
		lb.updateLock.Lock()
		lb.exporters = defaultExporters
		lb.updateLock.Unlock()
	})

	ctx, cancel := context.WithCancel(context.Background())
	// keep consuming traces every 2ms
	consumeCh := make(chan struct{})
	go func(ctx context.Context) {
		ticker := time.NewTicker(2 * time.Millisecond)
		for {
			select {
			case <-ctx.Done():
				consumeCh <- struct{}{}
				return
			case <-ticker.C:
				go p.ConsumeTraces(ctx, randomTraces())
			}
		}
	}(ctx)

	// give limited but enough time to rolling updates. otherwise this test
	// will still pass due to the 10 secs of sleep that is used to simulate
	// unreachable backends.
	go func() {
		time.Sleep(1 * time.Second)
		resolverCh <- struct{}{}
	}()

	<-resolverCh
	cancel()
	<-consumeCh

	// verify
	require.Equal(t, []string{"127.0.0.2"}, lastResolved)
	require.Greater(t, atomic.LoadInt64(&counter1), int64(0))
	require.Greater(t, atomic.LoadInt64(&counter2), int64(0))
}

func randomTraces() ptrace.Traces {
	v1 := uint8(rand.Intn(256))
	v2 := uint8(rand.Intn(256))
	v3 := uint8(rand.Intn(256))
	v4 := uint8(rand.Intn(256))
    traces := ptrace.NewTraces()
	appendSimpleTraceWithID(traces.ResourceSpans().AppendEmpty(), [16]byte{v1, v2, v3, v4})
    return traces
}

func simpleTraces() ptrace.Traces {
    traces := ptrace.NewTraces()
	appendSimpleTraceWithID(traces.ResourceSpans().AppendEmpty(), [16]byte{1, 2, 3, 4})
	return traces
}

func simpleConfig() *Config {
	return &Config{
		Resolver: ResolverSettings{
			Static: &StaticResolver{Hostnames: []string{"endpoint-1"}},
		},
	}
}

type mockTracesExporter struct {
	component.Component
	ConsumeTracesFn func(ctx context.Context, td ptrace.Traces) error
}

func newMockTracesExporter(consumeTracesFn func(ctx context.Context, td ptrace.Traces) error) exporter.Traces {
	return &mockTracesExporter{
		Component:       mockComponent{},
		ConsumeTracesFn: consumeTracesFn,
	}
}

func newNopMockTracesExporter() component.TracesExporter {
	return &mockTracesExporter{
		Component: mockComponent{},
		ConsumeTracesFn: func(ctx context.Context, td ptrace.Traces) error {
			return nil
		},
	}
}

func (e *mockTracesExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *mockTracesExporter) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	if e.ConsumeTracesFn == nil {
		return nil
	}
	return e.ConsumeTracesFn(ctx, td)
}
