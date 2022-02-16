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

package googlecloudstorageexporter

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap/zaptest"
)

func TestName(t *testing.T) {
	exporter := &storageExporter{}
	assert.Equal(t, "googlecloudstorage", exporter.Name())
}

func TestExporterDefaultSettings(t *testing.T) {
	ctx := context.Background()
	// Start a fake server running locally.

	exporter := &storageExporter{
		instanceName: "dummy",
		logger:       zaptest.NewLogger(t),
		userAgent:    "test-user-agent",

		config: &Config{
			Endpoint:  "127.0.0.1",
			Insecure:  true,
			ProjectID: "my-project",
			TimeoutSettings: exporterhelper.TimeoutSettings{
				Timeout: 12 * time.Second,
			},
		},
	}
	assert.NoError(t, exporter.start(ctx, nil))
	assert.NoError(t, exporter.consumeTraces(ctx, pdata.NewTraces()))
	assert.NoError(t, exporter.shutdown(ctx))
}

func TestExporterCompression(t *testing.T) {
	ctx := context.Background()
	// Start a fake server running locally.

	exporter := &storageExporter{
		instanceName: "dummy",
		logger:       zaptest.NewLogger(t),
		userAgent:    "test-user-agent",

		config: &Config{
			Endpoint:  "127.0.0.1",
			Insecure:  true,
			ProjectID: "my-project",
			TimeoutSettings: exporterhelper.TimeoutSettings{
				Timeout: 12 * time.Second,
			},
		},
		ceCompression: GZip,
	}
	assert.NoError(t, exporter.start(ctx, nil))
	assert.NoError(t, exporter.consumeTraces(ctx, pdata.NewTraces()))
	assert.NoError(t, exporter.shutdown(ctx))
}
