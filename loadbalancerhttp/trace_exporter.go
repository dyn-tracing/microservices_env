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

package loadbalancerhttp // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/otlphttpexporter"
    "go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/multierr"
    "go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/batchpersignal"
)

var _ exporter.Traces = (*traceExporterImp)(nil)

var (
	errNoTracesInBatch = errors.New("no traces were found in the batch")
)

type traceExporterImp struct {
    loadBalancer loadBalancer
	routingKey   routingKey

	stopped    bool
	shutdownWg sync.WaitGroup

    logger *zap.Logger
}

// Create new traces exporter
func newTracesExporter(params exporter.CreateSettings, cfg component.Config, logger *zap.Logger) (*traceExporterImp, error) {
	exporterFactory := otlphttpexporter.NewFactory()

	lb, err := newLoadBalancer(params, cfg, func(ctx context.Context, endpoint string) (component.Component, error) {
		oCfg := buildExporterConfig(cfg.(*Config), endpoint)
		return exporterFactory.CreateTracesExporter(ctx, params, &oCfg)
	})
	if err != nil {
		return nil, err
	}

	return &traceExporterImp{
		loadBalancer: lb,
        logger: logger,
	}, nil
}

func buildExporterConfig(cfg *Config, endpoint string) otlphttpexporter.Config {
	oCfg := cfg.Protocol.OTLP
	oCfg.Endpoint = endpoint
	return oCfg
}

func (e *traceExporterImp) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *traceExporterImp) Start(ctx context.Context, host component.Host) error {
	return e.loadBalancer.Start(ctx, host)
}

func (e *traceExporterImp) Shutdown(context.Context) error {
	e.stopped = true
	e.shutdownWg.Wait()
	return nil
}

func (e *traceExporterImp) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	var errs error
    // SplitTraces splits into batches of individual traces.
    // Now we need to consolidate traces which have the same endpoint into
    // the same message to be sent.

    /*
    batches := batchpersignal.SplitTraces(td)
    for _, batch := range batches {
        errs = multierr.Append(errs, e.consumeTrace(ctx, batch))

    }
    */

	traces := batchpersignal.SplitTraces(td)
    endpointToTraceData := make(map[string]ptrace.Traces)

	for trace := range traces {
	    routingIDs, err := routingIdentifiersFromTraces(traces[trace], e.routingKey)
        errs = multierr.Append(errs, err)
        for rid := range routingIDs {
		    endpoint := e.loadBalancer.Endpoint([]byte(rid))
            if _, ok := endpointToTraceData[endpoint]; ok {
                // append
                for i := 0; i < traces[trace].ResourceSpans().Len(); i++ {
                    traces[trace].ResourceSpans().At(i).CopyTo(endpointToTraceData[endpoint].ResourceSpans().AppendEmpty())
                }
            } else {
                newTrace := ptrace.NewTraces()
                for i := 0; i < traces[trace].ResourceSpans().Len(); i++ {
                    traces[trace].ResourceSpans().At(i).CopyTo(newTrace.ResourceSpans().AppendEmpty())
                }
                endpointToTraceData[endpoint] = newTrace
            }
        }
	}

    for endpoint, traces := range(endpointToTraceData) {
		exp, err := e.loadBalancer.Exporter(endpoint)
		if err != nil {
			return err
		}

		te, ok := exp.(exporter.Traces)
		if !ok {
            return fmt.Errorf("unable to export traces, unexpected exporter type: expected exporter.Traces but got %T", exp)
		}

		start := time.Now()
		err = te.ConsumeTraces(ctx, traces)
		duration := time.Since(start)

        if err == nil {
			_ = stats.RecordWithTags(
				ctx,
				[]tag.Mutator{tag.Upsert(endpointTagKey, endpoint), successTrueMutator},
				mBackendLatency.M(duration.Milliseconds()))
		} else {
			_ = stats.RecordWithTags(
				ctx,
				[]tag.Mutator{tag.Upsert(endpointTagKey, endpoint), successFalseMutator},
				mBackendLatency.M(duration.Milliseconds()))
		}
    }

	return errs
}

func (e *traceExporterImp) consumeTrace(ctx context.Context, td ptrace.Traces) error {
    var exp component.Component
	routingIds, err := routingIdentifiersFromTraces(td, e.routingKey)
	if err != nil {
		return err
	}
	for rid := range routingIds {
		endpoint := e.loadBalancer.Endpoint([]byte(rid))
		exp, err = e.loadBalancer.Exporter(endpoint)
		if err != nil {
			return err
		}

		te, ok := exp.(exporter.Traces)
		if !ok {
			expectType := (*exporter.Traces)(nil)
			return fmt.Errorf("expected %T but got %T", expectType, exp)
		}

		start := time.Now()
		err = te.ConsumeTraces(ctx, td)
		duration := time.Since(start)

		if err == nil {
			_ = stats.RecordWithTags(
				ctx,
				[]tag.Mutator{tag.Upsert(endpointTagKey, endpoint), successTrueMutator},
				mBackendLatency.M(duration.Milliseconds()))
		} else {
			_ = stats.RecordWithTags(
				ctx,
				[]tag.Mutator{tag.Upsert(endpointTagKey, endpoint), successFalseMutator},
				mBackendLatency.M(duration.Milliseconds()))
		}
	}
	return err
}

func routingIdentifiersFromTraces(td ptrace.Traces, key routingKey) (map[string]bool, error) {
	ids := make(map[string]bool)
	rs := td.ResourceSpans()
	if rs.Len() == 0 {
		return nil, errors.New("empty resource spans")
	}

	ils := rs.At(0).ScopeSpans()
	if ils.Len() == 0 {
		return nil, errors.New("empty scope spans")
	}

	spans := ils.At(0).Spans()
	if spans.Len() == 0 {
		return nil, errors.New("empty spans")
	}

	if key == svcRouting {
		for i := 0; i < rs.Len(); i++ {
			svc, ok := rs.At(i).Resource().Attributes().Get("service.name")
			if !ok {
				return nil, errors.New("unable to get service name")
			}
			ids[svc.Str()] = true
		}
		return ids, nil
	}
	tid := spans.At(0).TraceID()
	ids[string(tid[:])] = true
	return ids, nil
}
