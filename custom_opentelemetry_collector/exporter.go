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

type spanWithResource struct {
    span *pdata.Span
    resource string
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
    return bucketID + "-snicket3"
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

func (ex *storageExporter) hashTrace(ctx context.Context, spans []spanStr, traceID string) string {
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
    return strconv.FormatUint(uint64(traceHash), 10)
}


func (ex *storageExporter) spanBucketExists(ctx context.Context, serviceName string) error {
    storageClassAndLocation := &storage.BucketAttrs{
		StorageClass: "STANDARD",
		Location:     "us-central1",
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

// This function is used for debugging only
func (ex *storageExporter) sendDummyData(ctx context.Context, traceID string) error {
    traceBuf := dataBuffer{}
    for i:=0; i<100; i++ {
        traceBuf.logEntry("Number #%d", i)
    }

    trace_bkt := ex.client.Bucket(serviceNameToBucketName(trace_bucket))
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


// took parts of this code from tail sampling processor here:
// https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/6cb401f1f25d2affcf5a10e737ad1c7e10912206/processor/tailsamplingprocessor/processor.go#L300
func (ex *storageExporter) groupSpansByTraceKey(traces pdata.Traces) map[pdata.TraceID][]spanWithResource {
	idToSpans := make(map[pdata.TraceID][]spanWithResource)
    for i := 0; i<traces.ResourceSpans().Len(); i++ {
        rs := traces.ResourceSpans().At(i)
        if serviceName, ok := rs.Resource().Attributes().Get(conventions.AttributeServiceName); ok {
            ilss := rs.InstrumentationLibrarySpans()
            for j := 0; j < ilss.Len(); j++ {
                spans := ilss.At(j).Spans()
                spansLen := spans.Len()
                for k := 0; k < spansLen; k++ {
                    span := spans.At(k)
                    key := span.TraceID()
                    idToSpans[key] = append(idToSpans[key], spanWithResource {span: &span, resource: serviceName.StringVal()})
                }
            }
        }
    }
	return idToSpans
}

// Stores 2 things in GCS:
// 1. Trace ID to hash and struct
// 2. Hash to trace ID
func (ex *storageExporter) storeHashAndStruct(traceIDToSpans map[pdata.TraceID][]spanWithResource, minTime string, maxTime string) error {
    // 1. Collect the trace structures in traceStructBuf, and a map of hashes to traceIDs
    ctx := context.Background()
    traceStructBuf := dataBuffer{}
	hashToTraceID := make(map[string][]string)
    for traceID, spans := range traceIDToSpans {
        var sp []spanStr
        traceStructBuf.logEntry("Trace ID: %s:", traceID.HexString())
        for i := 0; i< len(spans); i++ {
            parent := spans[i].span.ParentSpanID().HexString()
            spanID := spans[i].span.SpanID().HexString()
            resource := spans[i].resource
            traceStructBuf.logEntry("%s:%s:%s", parent, spanID, resource)
            sp = append(sp, spanStr{
               parent: parent,
               id: spanID,
               service: resource})
        }
        hash := ex.hashTrace(ctx, sp, traceID.HexString())
        hashToTraceID[hash] = append(hashToTraceID[hash], traceID.HexString())
    }
    // 2. Put the trace structure buffer in storage
    trace_bkt := ex.client.Bucket(serviceNameToBucketName(trace_bucket))
    ex.spanBucketExists(ctx, trace_bucket)

    objectName := strconv.FormatUint(uint64(hash(minTime)), 10)[0:2] + "-" + minTime + "-" + maxTime
    trace_obj := trace_bkt.Object(objectName)
    w_trace := trace_obj.NewWriter(ctx)
    if _, err := w_trace.Write([]byte(traceStructBuf.buf.Bytes())); err != nil {
        return fmt.Errorf("failed creating the trace object: %w", err)
    }
    if err := w_trace.Close(); err != nil {
        return fmt.Errorf("failed closing the trace object %w", err)
    }

    // 3. Put the hash to trace ID mapping in storage
    ex.spanBucketExists(ctx, "tracehashes")
    bkt := ex.client.Bucket(serviceNameToBucketName("tracehashes"))
    for hash, traces := range hashToTraceID {
        traceIDs := dataBuffer{}
        for i :=0; i<len(traces); i++ {
            traceIDs.logEntry("%s", traces[i])
        }
        obj := bkt.Object(hash+"/"+objectName)
        w := obj.NewWriter(ctx)
        if _, err := w.Write(traceIDs.buf.Bytes()); err != nil {
            return fmt.Errorf("failed creating the object: %w", err)
        }
        if err := w.Close(); err != nil {
            return fmt.Errorf("failed closing the hash object in bucket %s: %w", hash+"/"+objectName+"/"+minTime, err)
        }
    }
    return nil
}

// A helper function that stores spans according to their resource.
func (ex *storageExporter) storeSpans(traces pdata.Traces, minTime string, maxTime string) error {
    ctx := context.Background()
    rss := traces.ResourceSpans()
    for i := 0; i< rss.Len(); i++ {
        // Here we marshal by resourcespan;  because the groupbyattr processor is always used, we can be confident that
        // they have been grouped by resource already.
        // 1. Marshal the spans from the same resource into a buffer
		if sn, ok := rss.At(i).Resource().Attributes().Get(conventions.AttributeServiceName); ok {
            oneResourceSpans := pdata.NewTraces()
            rss.At(i).CopyTo(oneResourceSpans.ResourceSpans().AppendEmpty())
            buffer, err := ex.tracesMarshaler.MarshalTraces(oneResourceSpans)
            if err != nil {
                ex.logger.Info("could not marshal traces ", zap.Error(err))
                return err
            }

            // 2. Determine the bucket of the new object, and make sure it's a bucket that exists
            bucketName := serviceNameToBucketName(sn.StringVal()) 
            bkt := ex.client.Bucket(bucketName)
            ret := ex.spanBucketExists(ctx, sn.StringVal())
            if ret != nil {
                ex.logger.Info("span bucket exists error ", zap.Error(ret))
                return ret
            }
            // 3. Determine the name of the new object
            minTime := time.Date(2020, 2, 11, 20, 26, 12, 321, time.UTC) // dummy value, will be overwritten
            maxTime := time.Date(2020, 2, 11, 20, 26, 12, 321, time.UTC) // dummy value, will be overwritten
            for j := 0; j<oneResourceSpans.ResourceSpans().Len(); j++ {
                rsSpan := oneResourceSpans.ResourceSpans().At(j)
                ils := rsSpan.InstrumentationLibrarySpans()
                for k := 0; k < ils.Len(); k++ {
                    scopeSpans := ils.At(k).Spans()
                    for l:=0; l < scopeSpans.Len(); l++ {
                        if j == 0 && k == 0 && l == 0 {
                            minTime = scopeSpans.At(l).StartTimestamp().AsTime()
                            maxTime = scopeSpans.At(l).StartTimestamp().AsTime()
                        }
                        if scopeSpans.At(l).StartTimestamp().AsTime().Before(minTime) {
                            minTime = scopeSpans.At(l).StartTimestamp().AsTime()
                        }
                        if scopeSpans.At(l).StartTimestamp().AsTime().After(maxTime) {
                            maxTime = scopeSpans.At(l).StartTimestamp().AsTime()
                        }
                    }
                }
            }
            minTimeStr := strconv.FormatUint(uint64(minTime.Unix()), 10)
            maxTimeStr := strconv.FormatUint(uint64(minTime.Unix()), 10)
            objectName := strconv.FormatUint(uint64(hash(minTimeStr)), 10)[0:2] + "-" + minTimeStr + "-" + maxTimeStr

            // 4. Send the data under that bucket/object name to storage
            obj := bkt.Object(objectName)
            writer := obj.NewWriter(ctx)
            if _, err := writer.Write(buffer); err != nil {
                return fmt.Errorf("failed creating the span object: %w", err)
            }
            if err := writer.Close(); err != nil {
                return fmt.Errorf("failed closing the span object: %w", err)
            }
        } else {
            ex.logger.Info("didn't get service name")
        }
    }
    return nil
}


// This is the main function of the exporter.  It is called by consumers
// to process trace data and send it to GCS.
func (ex *storageExporter) consumeTraces(ctx context.Context, traces pdata.Traces) error {
    // once you have a batch, there are two things you must do with it:

    traceIDToSpans := ex.groupSpansByTraceKey(traces)

    // 1. Find time span
    minTime := time.Date(2020, 2, 11, 20, 26, 12, 321, time.UTC) // dummy value, will be overwritten
    maxTime := time.Date(2020, 2, 11, 20, 26, 12, 321, time.UTC) // dummy value, will be overwritten
    first_iteration := true
    for _, spans := range traceIDToSpans {
        for i := 0; i< len(spans); i++ {
            if first_iteration || spans[i].span.StartTimestamp().AsTime().Before(minTime) {
                if first_iteration {
                    ex.logger.Info("min: first iteration change")
                } else {
                    ex.logger.Info("min: non-first iteration change")
                }
                minTime = spans[i].span.StartTimestamp().AsTime()
            }
            if first_iteration || spans[i].span.EndTimestamp().AsTime().After(maxTime) {
                if first_iteration {
                    ex.logger.Info("max: first iteration change")
                } else {
                    ex.logger.Info("max: non-first iteration change")
                }
                maxTime = spans[i].span.StartTimestamp().AsTime()
            }
            first_iteration = false
        }
    }
    minTimeStr := strconv.FormatUint(uint64(minTime.Unix()), 10)
    maxTimeStr := strconv.FormatUint(uint64(maxTime.Unix()), 10)
    // 1. push spans to storage
    ret := ex.storeSpans(traces, minTimeStr, maxTimeStr)
    if ret != nil {
        ex.logger.Error("error storing spans %s", zap.NamedError("error", ret))
        return ret
    }
    // 2. push trace structure as well as the hash of the structure to storage
    // 2a. Create a mapping from trace ID to each of the spans in the trace
    ret =  ex.storeHashAndStruct(traceIDToSpans, minTimeStr, maxTimeStr)
    if ret != nil {
        ex.logger.Error("error storing trace structure and hash %s", zap.NamedError("error", ret))
    }
    return nil
}
