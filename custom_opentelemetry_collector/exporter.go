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

    storage "cloud.google.com/go/storage"
	"github.com/google/uuid"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/model/otlp"
	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"
)

const name = "googlecloudstorage"

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
	metricsMarshaler     pdata.MetricsMarshaler
	logsMarshaler        pdata.LogsMarshaler
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
	ex.metricsMarshaler = otlp.NewProtobufMetricsMarshaler()
	ex.logsMarshaler = otlp.NewProtobufLogsMarshaler()
	return nil
}

func (ex *storageExporter) shutdown(context.Context) error {
	if ex.client != nil {
		ex.client.Close()
		ex.client = nil
	}
	return nil
}

func (ex *storageExporter) publishMessage(ctx context.Context, encoding Encoding, data []byte) error {
	id, err := uuid.NewRandom()
	if err != nil {
		return err
	}

	attributes := map[string]string{
		"ce-specversion": "1.0",
		"ce-id":          id.String(),
		"ce-source":      ex.ceSource,
	}
	switch encoding {
	case OtlpProtoTrace:
		attributes["ce-type"] = "org.opentelemetry.otlp.traces.v1"
		attributes["content-type"] = "application/protobuf"
	case OtlpProtoMetric:
		attributes["ce-type"] = "org.opentelemetry.otlp.metrics.v1"
		attributes["content-type"] = "application/protobuf"
	case OtlpProtoLog:
		attributes["ce-type"] = "org.opentelemetry.otlp.logs.v1"
		attributes["content-type"] = "application/protobuf"
	}
	switch ex.ceCompression {
	case GZip:
		attributes["content-encoding"] = "gzip"
		data, err = ex.compress(data)
		if err != nil {
			return err
		}
	}
    bkt := ex.client.Bucket("dyn-tracing-example")
    obj := bkt.Object("obj-name"+id.String())
    w := obj.NewWriter(ctx)
    if _, err := fmt.Fprintf(w, "here's the data"); err != nil {
		return fmt.Errorf("failed creating the object: %w", err)
    }
    if err := w.Close(); err != nil {
		return fmt.Errorf("failed closing the object: %w", err)
    }
    /*
	_, err = ex.client.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: ex.topicName,
		Messages: []*pubsubpb.PubsubMessage{
			{
				Attributes: attributes,
				Data:       data,
			},
		},
	})
    */
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

func (ex *storageExporter) consumeTraces(ctx context.Context, traces pdata.Traces) error {
	buffer, err := ex.tracesMarshaler.MarshalTraces(traces)
	if err != nil {
		return err
	}
	return ex.publishMessage(ctx, OtlpProtoTrace, buffer)
}

func (ex *storageExporter) consumeMetrics(ctx context.Context, metrics pdata.Metrics) error {
	buffer, err := ex.metricsMarshaler.MarshalMetrics(metrics)
	if err != nil {
		return err
	}
	return ex.publishMessage(ctx, OtlpProtoMetric, buffer)
}

func (ex *storageExporter) consumeLogs(ctx context.Context, logs pdata.Logs) error {
	buffer, err := ex.logsMarshaler.MarshalLogs(logs)
	if err != nil {
		return err
	}
	return ex.publishMessage(ctx, OtlpProtoLog, buffer)
}
