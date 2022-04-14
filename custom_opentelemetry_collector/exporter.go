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
    "strconv"
    "time"

    storage "cloud.google.com/go/storage"
    conventions "go.opentelemetry.io/collector/model/semconv/v1.5.0"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/model/otlp"
	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"
    "hash/fnv"
)

const name = "googlecloudstorage"
const trace_bucket = "dyntraces"
const primeNumber = 97

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

type spanStr struct {
    parent string
    id string
    service string
}

type spanBuf struct {
    buf dataBuffer
    id string
    service string
}

// https://stackoverflow.com/questions/13582519/how-to-generate-hash-number-of-a-string-in-go
func hash(s string) uint32 {
        h := fnv.New32a()
        h.Write([]byte(s))
        return h.Sum32()
}

func serviceNameToBucketName(serviceName string) string {
    // TODO: is this the best way to get it into a format for bucket names?
    // There is probably a more robust way
    bucketID := strings.ReplaceAll(serviceName, ".", "")
    bucketID = strings.ReplaceAll(bucketID, "/", "")
    bucketID = strings.ReplaceAll(bucketID, "google", "")
    bucketID = strings.ReplaceAll(bucketID, "_", "")
    bucketID = strings.ToLower(bucketID)
    return bucketID + "-snicket"
}


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

func (ex *storageExporter) hashTraceFuture(ctx context.Context, spans []spanStr, traceID string) (chan error) {
    future := make (chan error, 1);
    go func () {future <- ex.hashTrace(ctx, spans, traceID); close(future) } ();
    return future;
}

func (ex *storageExporter) hashTrace(ctx context.Context, spans []spanStr, traceID string) error {
    var traceHash uint32
    traceHash = 0
    var root int
    for i := 0; i< len(spans); i++ {
        if spans[i].parent == "" {
            root = i
        }
    }
    spanIDToLevel := make(map[string]uint32)
    spanIDToLevel[spans[root].id] = 0
    if len(spans) == 1 {
        // only root span
        // you multiply by 0 so second term is 0
        traceHash = hash(spans[root].service)
    } else {
        foundSpan := true
        var spanCpy []spanStr
        copy(spanCpy, spans)
        for len(spanCpy) > 0 && foundSpan {
            foundSpan = false
            i := 0
            for i < len(spanCpy) {
                if val, ok := spanIDToLevel[spanCpy[i].parent]; ok {
                    spanIDToLevel[spanCpy[i].id] = val + 1
                    spanCpy[i] = spanCpy[len(spanCpy)-1]
                    spanCpy = spanCpy[:len(spanCpy)-1]
                    foundSpan = true
                }
                i += 1
            }
        }
        for i := 0; i<len(spans); i++ {
            // if trees are unconnected, just do hash of the tree you have
            if val, ok := spanIDToLevel[spans[i].id]; ok {
                traceHash += hash(spans[i].service) + val*uint32(primeNumber)
            }
        }
    }
    // computed trace hash;  now need to put that in storage
    //ex.spanBucketExists(ctx, "tracehashes")
    bkt := ex.client.Bucket(serviceNameToBucketName("tracehashes"))
    obj := bkt.Object(strconv.FormatUint(uint64(traceHash), 10)+"/"+traceID) // should this be 64 from the beginning?
    w := obj.NewWriter(ctx)
    if _, err := w.Write([]byte(traceID)); err != nil {
        return fmt.Errorf("failed creating the trace hash object: %w", err)
    }
    if err := w.Close(); err != nil {
        return fmt.Errorf("failed closing the hash object in bucket %s: %w", strconv.FormatUint(uint64(traceHash), 10)+"/"+traceID, err)
    }
    return nil
}


func (ex *storageExporter) spanBucketExists(ctx context.Context, serviceName string) error {
    storageClassAndLocation := &storage.BucketAttrs{
		StorageClass: "STANDARD",
		Location:     "US",
        LocationType: "region",
	}
    bkt := ex.client.Bucket(serviceNameToBucketName(serviceName))
    _, err := bkt.Attrs(ctx)
    if err == storage.ErrBucketNotExist {
        if err := bkt.Create(ctx, ex.config.ProjectID, storageClassAndLocation); err != nil {
            return fmt.Errorf("failed creating bucket: %w", err)
        }
    }
    if err != nil {
        return fmt.Errorf("failed getting bucket attributes: %s %w", serviceName, err)
    }
    return err
}

func (ex *storageExporter) publishSpanFuture(ctx context.Context, data dataBuffer, serviceName string, spanID string) (chan error) {
    future := make (chan error, 1)
    go func () { future <- ex.publishSpan(ctx, data, serviceName, spanID); close (future) }();
    return future;

}

func (ex *storageExporter) publishSpan(ctx context.Context, data dataBuffer, serviceName string, spanID string) error {
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
    //ex.spanBucketExists(ctx, serviceName)
    bucketID := serviceNameToBucketName(serviceName)
    bkt := ex.client.Bucket(bucketID)

    // object will be span ID
    obj := bkt.Object(spanID)
    w := obj.NewWriter(ctx)
    if _, err := w.Write(data.buf.Bytes()); err != nil {
        return fmt.Errorf("failed creating the span object: %w", err)
    }
    if err := w.Close(); err != nil {
        return fmt.Errorf("failed closing the span object in bucket %s: %w", bucketID, err)
    }
	return nil
}

func (ex *storageExporter) publishTraceFuture(ctx context.Context, spans []spanStr, traceID string) (chan error) {
    future := make (chan error, 1);
    go func () { future <- ex.publishTrace(ctx, spans, traceID); close(future) } ();
    return future;
}

func (ex *storageExporter) publishTrace(ctx context.Context, spans []spanStr, traceID string) error {
    traceBuf := dataBuffer{}
    for i := 0; i < len(spans); i++ {
        traceBuf.logEntry("%s:%s:%s", spans[i].parent, spans[i].id, spans[i].service)
    }
    // now make sure to add it to the trace bucket
    // we know that trace bucket for sure already exists bc of the start function
    trace_bkt := ex.client.Bucket(serviceNameToBucketName(trace_bucket))
    ex.spanBucketExists(ctx, trace_bucket)

    trace_obj := trace_bkt.Object(traceID)
    w_trace := trace_obj.NewWriter(ctx)
    if _, err := w_trace.Write([]byte(traceBuf.buf.Bytes())); err != nil {
        return fmt.Errorf("failed creating the trace object: %w", err)
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

func (ex *storageExporter) sendDummyData(ctx context.Context, traceID string) error {
    traceBuf := dataBuffer{}
    for i:=0; i<100; i++ {
        traceBuf.logEntry("Number #%d", i)
    }

    trace_bkt := ex.client.Bucket(serviceNameToBucketName(trace_bucket))
    /*
    ret := ex.spanBucketExists(ctx, trace_bucket)
    if ret != nil {
        return ret
    }
    */

    trace_obj := trace_bkt.Object(traceID)
    w_trace := trace_obj.NewWriter(ctx)
    if _, err := w_trace.Write([]byte(traceBuf.buf.Bytes())); err != nil {
        return fmt.Errorf("failed creating the trace object: %s %w", traceID, err)
    }
    if err := w_trace.Close(); err != nil {
        return fmt.Errorf("failed closing the trace object: %s %w", traceID, err)
    }
    return nil
}


// toook this from tail sampling processor here:
// https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/6cb401f1f25d2affcf5a10e737ad1c7e10912206/processor/tailsamplingprocessor/processor.go#L300
func (ex *storageExporter) groupSpansByTraceKey(resourceSpans pdata.ResourceSpans) map[pdata.TraceID][]*pdata.Span {
	idToSpans := make(map[pdata.TraceID][]*pdata.Span)
	ilss := resourceSpans.ScopeSpans()
	for j := 0; j < ilss.Len(); j++ {
		spans := ilss.At(j).Spans()
		spansLen := spans.Len()
		for k := 0; k < spansLen; k++ {
			span := spans.At(k)
			key := span.TraceID()
			idToSpans[key] = append(idToSpans[key], &span)
		}
	}
	return idToSpans
}

// Stores 2 things in GCS:
// 1. Trace ID to hash and struct
// 2. Hash to trace ID
func (ex *storageExporter) storeHashAndStruct(traceIDToSpans map[pdata.TraceID][]*pdata.Span) error {
    // TODO
    return nil
}

// A helper function that stores spans according to their resource.
func (ex *storageExporter) storeSpans(traces pdata.Traces) error {
    num_requests := 0
    rss := traces.ResourceSpans()
    for i := 0; i< rss.Len(); i++ {
        // Here we marshal by resourcespan;  because the groupbyattr processor is always used, we can be confident that
        // they have been grouped by resource already.
        // 1. Marshal the spans from the same resource into a buffer
		if sn, ok := rss.At(i).Resource().Attributes().Get(conventions.AttributeServiceName); ok {
            oneResourceSpans := pdata.NewTraces()
            rss.At(i).CopyTo(oneResourceSpans.ResourceSpans().AppendEmpty())
            buffer, err := ex.tracesMarshaler.MarshalTraces(oneResourceSpans)
            oneRs := oneResourceSpans.ResourceSpans()
            ex.logger.Info("len one resource spans", zap.Int("#len", oneResourceSpans.SpanCount()))
            ex.logger.Info("len scope spans", zap.Int("#len", oneRs.At(0).InstrumentationLibrarySpans().Len()))
            if err != nil {
                ex.logger.Info("could not marshal traces ", zap.Error(err))
                return err
            }

            // 2. Determine the bucket of the new object, and make sure it's a bucket that exists
            bucketName := serviceNameToBucketName(sn.StringVal()) 
            ex.logger.Info("bucket name is ", zap.String("bucketname: ", bucketName))
            bkt := ex.client.Bucket(bucketName)
            ctx := context.Background() // TODO:  Is this necessary every iteration?  Can I take it out?
            ret := ex.spanBucketExists(ctx, bucketName)
            if ret != nil {
                ex.logger.Info("span bucket exists error ", zap.Error(ret))
                return ret
            }
            // 3. Determine the name of the new object;  for now, this is a hash of the current time
            //    TODO: I'll make it more meaningful once I see whether this works
            now := strconv.FormatInt(time.Now().Unix(), 10)
            objectName := strconv.FormatUint(uint64(hash(now)), 10)
            // 4. Send the data under that bucket/object name to storage
            obj := bkt.Object(objectName)
            writer := obj.NewWriter(ctx)
            if _, err := writer.Write(buffer); err != nil {
                return fmt.Errorf("failed creating the span object: %w", err)
            }
            if err := writer.Close(); err != nil {
                return fmt.Errorf("failed closing the span object: %w", err)
            }
            num_requests = num_requests + 1
        } else {
            ex.logger.Info("didn't get service name")
        }
    }
    ex.logger.Info("num requests", zap.Int("#requests", num_requests))
    return nil
}


// This is the main function of the exporter.  It is called by consumers
// to process trace data and send it to GCS.
func (ex *storageExporter) consumeTraces(ctx context.Context, traces pdata.Traces) error {
    // once you have a batch, there are two things you must do with it:
    // 1. push spans to storage
    ret := ex.storeSpans(traces)
    if ret != nil {
        ex.logger.Error("error storing spans %s", zap.NamedError("error", ret))
        return ret
    }
    return nil
    /*
    now := strconv.FormatInt(time.Now().Unix(), 10)
    objectName := strconv.FormatUint(uint64(hash(now)), 10)
    return ex.sendDummyData(context.Background(), objectName)
    */



    /*
    // 2. push trace structure as well as the hash of the structure to storage
    // 2a. Create a mapping from trace ID to each of the spans in the trace
    traceIDToSpans := groupSpansByTraceKey(traces.ResourceSpans)
    ret :=  ex.storeHashAndStruct(traceIDToSpans)
    if ret != nil {
        ex.logger.Err("error storing trace structure and hash %s", zap.NamedError("error", ret))
    }
    */
}
