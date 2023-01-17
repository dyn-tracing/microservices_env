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
	"context"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/collector/component"
    "go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/consumer"
    "go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const (
	// The value of "type" key in configuration.
	typeStr        = "googlecloudstorage"
	defaultTimeout = 12 * time.Second
    stability = component.StabilityLevelBeta
)

// NewFactory creates a factory for Google Cloud Storage exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		typeStr,
		createDefaultConfig,
        exporter.WithTraces(createTracesExporter, stability))
}

var exporters = map[*Config]*storageExporter{}

func ensureExporter(params exporter.CreateSettings, pCfg *Config) *storageExporter {
	receiver := exporters[pCfg]
	if receiver != nil {
		return receiver
	}
	receiver = &storageExporter{
		logger:           params.Logger,
		userAgent:        strings.ReplaceAll(pCfg.UserAgent, "{{version}}", params.BuildInfo.Version),
		ceSource:         fmt.Sprintf("/opentelemetry/collector/%s/%s", name, params.BuildInfo.Version),
		config:           pCfg,
		tracesMarshaler:  &ptrace.ProtoMarshaler{},
	}
	receiver.ceCompression, _ = pCfg.parseCompression()
	exporters[pCfg] = receiver
	return receiver
}

// createDefaultConfig creates the default configuration for exporter.
func createDefaultConfig() component.Config {
	return &Config{
		UserAgent:        "opentelemetry-collector-contrib {{version}}",
		TimeoutSettings:  exporterhelper.TimeoutSettings{Timeout: defaultTimeout},
        RetrySettings:    exporterhelper.NewDefaultRetrySettings(),
        QueueSettings:    exporterhelper.NewDefaultQueueSettings(),
	}
}

func createTracesExporter(
	ctx context.Context,
	params exporter.CreateSettings,
	cfg component.Config) (exporter.Traces, error) {

	pCfg := cfg.(*Config)
	storageExporter := ensureExporter(params, pCfg)

	return exporterhelper.NewTracesExporter(
        ctx,
		params,
        cfg,
		storageExporter.consumeTraces,
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		exporterhelper.WithTimeout(pCfg.TimeoutSettings),
		exporterhelper.WithRetry(pCfg.RetrySettings),
		exporterhelper.WithQueue(pCfg.QueueSettings),
		exporterhelper.WithStart(storageExporter.start),
		exporterhelper.WithShutdown(storageExporter.shutdown),
	)
}

