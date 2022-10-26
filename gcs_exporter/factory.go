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
	"go.opentelemetry.io/collector/config"
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
func NewFactory() component.ExporterFactory {
	return component.NewExporterFactory(
		typeStr,
		createDefaultConfig,
        component.WithTracesExporter(createTracesExporter, stability))
}

var exporters = map[*Config]*storageExporter{}

func ensureExporter(params component.ExporterCreateSettings, pCfg *Config) *storageExporter {
	receiver := exporters[pCfg]
	if receiver != nil {
		return receiver
	}
	receiver = &storageExporter{
		instanceName:     pCfg.ID().Name(),
		logger:           params.Logger,
		userAgent:        strings.ReplaceAll(pCfg.UserAgent, "{{version}}", params.BuildInfo.Version),
		ceSource:         fmt.Sprintf("/opentelemetry/collector/%s/%s", name, params.BuildInfo.Version),
		config:           pCfg,
		tracesMarshaler:  ptrace.NewProtoMarshaler(),
	}
	receiver.ceCompression, _ = pCfg.parseCompression()
	exporters[pCfg] = receiver
	return receiver
}

// createDefaultConfig creates the default configuration for exporter.
func createDefaultConfig() config.Exporter {
	return &Config{
		ExporterSettings: config.NewExporterSettings(config.NewComponentID(typeStr)),
		UserAgent:        "opentelemetry-collector-contrib {{version}}",
		TimeoutSettings:  exporterhelper.TimeoutSettings{Timeout: defaultTimeout},
        RetrySettings:    exporterhelper.NewDefaultRetrySettings(),
        QueueSettings:    exporterhelper.NewDefaultQueueSettings(),
	}
}

func createTracesExporter(
	ctx context.Context,
	set component.ExporterCreateSettings,
	cfg config.Exporter) (component.TracesExporter, error) {

	pCfg := cfg.(*Config)
	storageExporter := ensureExporter(set, pCfg)

	return exporterhelper.NewTracesExporter(
        ctx,
		set,
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

