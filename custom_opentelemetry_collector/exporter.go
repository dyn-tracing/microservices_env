// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package googlecloudstorageexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/googlecloudstorageexporter"

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
    "strings"

    storage "cloud.google.com/go/storage"
    conventions "go.opentelemetry.io/collector/model/semconv/v1.5.0"
    //"go.uber.org/multierr"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/model/otlp"
	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"
    //"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/batchpersignal"
)

const name = "googlecloudstorage"
const trace_bucket = "dyntraces"

type storageExporter struct {
	instanceName         string
	logger               *zap.Logger
	client               *storage.Client
	cancel               context.CancelFunc
	userAgent            string
	ceSource             string
	ceCompression        Compression
	config               *Config
	tracesMarshaler      pdata.TracesMarshaler
}

func (*storageExporter) Name() string {
	return name
}

type Encoding int

const (
	OtlpProtoTrace  Encoding = iota
	OtlpProtoMetric          = iota
	OtlpProtoLog             = iota
)

type Compression int

const (
	Uncompressed Compression = iota
	GZip                     = iota
)

func (ex *storageExporter) start(ctx context.Context, _ component.Host) error {
	ctx, ex.cancel = context.WithCancel(ctx)

	if ex.client == nil {
		client, err := storage.NewClient(ctx)
		if err != nil {
			return fmt.Errorf("failed creating the gRPC client to Storage: %w", err)
		}

		ex.client = client
	}
	ex.tracesMarshaler = otlp.NewProtobufTracesMarshaler()
    ex.spanBucketExists(ctx, trace_bucket)
	return nil
}

func (ex *storageExporter) shutdown(context.Context) error {
	if ex.client != nil {
		ex.client.Close()
		ex.client = nil
	}
	return nil
}

func (ex *storageExporter) serviceNameToBucketName(ctx context.Context, serviceName string) string {
    // TODO: is this the best way to get it into a format for bucket names?
    // There is probably a more robust way
    bucketID := strings.ReplaceAll(serviceName, ".", "")
    bucketID = strings.ReplaceAll(bucketID, "/", "")
    bucketID = strings.ReplaceAll(bucketID, "google", "")
    bucketID = strings.ReplaceAll(bucketID, "_", "")
    bucketID = strings.ToLower(bucketID)
    return bucketID
}

func (ex *storageExporter) spanBucketExists(ctx context.Context, serviceName string) error {
    bkt := ex.client.Bucket(ex.serviceNameToBucketName(ctx, serviceName))
    _, err := bkt.Attrs(ctx)
    if err == storage.ErrBucketNotExist {
        if err := bkt.Create(ctx, ex.config.ProjectID, nil); err != nil {
            return fmt.Errorf("failed creating bucket: %w", err)
        }
    }
    if err != nil {
        return fmt.Errorf("failed getting bucket attributes: %w", err)
    }
    return err
}

func (ex *storageExporter) publishSpan(ctx context.Context, data dataBuffer, serviceName string, spanID string) error {
    var err error

    // TODO:  figure out compression
    /*
	switch ex.ceCompression {
	case GZip:
    	data, err = ex.compress(data)
		if err != nil {
			return err
		}
	}
    */

    // bucket will be service name
    bucketID := ex.serviceNameToBucketName(ctx, serviceName)
    bkt := ex.client.Bucket(bucketID)
    ex.spanBucketExists(ctx, bucketID)

    // object will be span ID
    obj := bkt.Object(spanID)
    w := obj.NewWriter(ctx)
    if _, err := w.Write(data.buf.Bytes()); err != nil {
        return fmt.Errorf("failed creating the object: %w", err)
    }
    if err := w.Close(); err != nil {
        return fmt.Errorf("failed closing the span object in bucket %s: %w", bucketID, err)
    }

	return err
}

func (ex *storageExporter) publishTrace(ctx context.Context, data dataBuffer, traceID string) error {
    // now make sure to add it to the trace bucket
    // we know that trace bucket for sure already exists bc of the start function
    //trace_bkt := ex.client.Bucket(trace_bucket)
    trace_bkt := ex.client.Bucket(trace_bucket)
    ex.spanBucketExists(ctx, trace_bucket)

    trace_obj := trace_bkt.Object(traceID)
    w_trace := trace_obj.NewWriter(ctx)
    if _, err := w_trace.Write([]byte(data.buf.Bytes())); err != nil {
        return fmt.Errorf("failed creating the object: %w", err)
    }
    if err := w_trace.Close(); err != nil {
        return fmt.Errorf("failed closing the trace object %s: %w", traceID, err)
    }
    return nil
}

func (ex *storageExporter) compress(payload []byte) ([]byte, error) {
	switch ex.ceCompression {
	case GZip:
		var buf bytes.Buffer
		writer := gzip.NewWriter(&buf)
		_, err := writer.Write(payload)
		if err != nil {
			return nil, err
		}
		err = writer.Close()
		if err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}
	return payload, nil
}

/*
func (ex *storageExporter) consumeTraces(ctx context.Context, traces pdata.Traces) error {
	var errs error
    for _, singleTrace := range batchpersignal.SplitTraces(traces) {
		errs = multierr.Append(errs, ex.consumeTrace(ctx, singleTrace))
	}
    return errs
}
*/


func (ex *storageExporter) consumeTraces(ctx context.Context, traces pdata.Traces) error {
    // citation:  stole the structure of this code from https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/honeycombexporter/honeycomb.go and from https://github.com/open-telemetry/opentelemetry-collector/blob/0afea3faaac826d9b122046c68dbaae1e2a64ff5/internal/otlptext/traces.go#L29
    var traceID string
    traceBuf := dataBuffer{}

	rss := traces.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
        r := rs.Resource()

        // TODO:  somehow incorporate resource attrs

		if serviceName, ok := r.Attributes().Get(conventions.AttributeServiceName); ok {
            ex.spanBucketExists(ctx, serviceName.StringVal())
            ex.spanBucketExists(ctx, trace_bucket) // TODO:  why isn't this executing?
            ilss := rs.InstrumentationLibrarySpans()
            for j := 0; j < ilss.Len(); j++ {
                ils := ilss.At(j)
                // Buffer is created here because at this point, all the spans are from the same instrumentation library.
                // So this is essentially the same trace point
                spans := ils.Spans()
                for k := 0; k < spans.Len(); k++ {
                    buf := dataBuffer{}
                    buf.logEntry("Span #%d", k)
                    span := spans.At(k)
                    buf.logAttr("Trace ID", span.TraceID().HexString())
                    traceID = span.TraceID().HexString()
                    buf.logAttr("Parent ID", span.ParentSpanID().HexString())
                    buf.logAttr("ID", span.SpanID().HexString())
                    traceBuf.logEntry("%s:%s:%s", span.ParentSpanID().HexString(), span.SpanID().HexString(), serviceName.StringVal())
                    buf.logAttr("Name", span.Name())
                    buf.logAttr("Kind", span.Kind().String())
                    buf.logAttr("Start time", span.StartTimestamp().String())
                    buf.logAttr("End time", span.EndTimestamp().String())

                    buf.logAttr("Status code", span.Status().Code().String())
                    buf.logAttr("Status message", span.Status().Message())

                    buf.logAttributeMap("Attributes", span.Attributes())
                    buf.logEvents("Events", span.Events())
                    buf.logLinks("Links", span.Links())
                    ex.publishSpan(ctx, buf, serviceName.StringVal(), span.SpanID().HexString())
                }
            }
		}
	}
    return ex.publishTrace(ctx, traceBuf, traceID)
}
