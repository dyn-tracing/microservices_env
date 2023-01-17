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
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
    "go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/confmap/confmaptest"
)

/*
func TestLoadConfig(t *testing.T) {
    cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

    factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

    sub, err := cm.Sub(component.NewIDWithName(typeStr, "").String())
	require.NoError(t, err)
	require.NoError(t, component.UnmarshalConfig(sub, cfg))

	customConfig := factory.CreateDefaultConfig().(*Config)

	customConfig.ProjectID = "my-project"
	customConfig.UserAgent = "opentelemetry-collector-contrib {{version}}"
	customConfig.Endpoint = "test-endpoint"
    customConfig.BucketSuffix = "snicket4"
    customConfig.NumOfDigitsForRandomHash = "2"
	customConfig.Insecure = true
	customConfig.TimeoutSettings = exporterhelper.TimeoutSettings{
		Timeout: 12 * time.Second,
	}
	customConfig.Compression = "gzip"
    assert.Equal(t, cfg, customConfig)
}
*/

func TestLoadConfig2(t *testing.T) {
	t.Parallel()

	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	// URL doesn't have a default value so set it directly.
	defaultCfg := createDefaultConfig().(*Config)

	tests := []struct {
		id       component.ID
		expected component.Config
	}{
		{
			id:       component.NewIDWithName(typeStr, "customname"),
			expected: defaultCfg,
		},
		{
			id: component.NewIDWithName(typeStr, ""),
			expected: &Config{
				RetrySettings: exporterhelper.RetrySettings{
					Enabled:         true,
					InitialInterval: 10 * time.Second,
					MaxInterval:     1 * time.Minute,
					MaxElapsedTime:  10 * time.Minute,
				},
				QueueSettings: exporterhelper.QueueSettings{
					Enabled:      true,
					NumConsumers: 2,
					QueueSize:    10,
				},
                ProjectID: "my-project",
                UserAgent: "opentelemetry-collector-contrib {{version}}",
                Endpoint: "test-endpoint",
                BucketSuffix: "snicket4",
                NumOfDigitsForRandomHash: "2",
                Insecure: true,
                TimeoutSettings: exporterhelper.TimeoutSettings{
                    Timeout: 12 * time.Second,
                },
	            Compression: "gzip",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.id.String(), func(t *testing.T) {
			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()

			sub, err := cm.Sub(tt.id.String())
			require.NoError(t, err)
			require.NoError(t, component.UnmarshalConfig(sub, cfg))

			assert.NoError(t, component.ValidateConfig(cfg))
			assert.Equal(t, tt.expected, cfg)
		})
	}
}

func TestCompressionConfigValidation(t *testing.T) {
	factory := NewFactory()
	c := factory.CreateDefaultConfig().(*Config)
	c.Compression = "xxx"
	assert.Error(t, c.validate())
	c.Compression = "gzip"
	assert.NoError(t, c.validate())
	c.Compression = "none"
	assert.Error(t, c.validate())
	c.Compression = ""
	assert.NoError(t, c.validate())
}

func sanitize(cfg *Config) *Config {
	cfg.Config.MetricConfig.MapMonitoredResource = nil
	cfg.Config.MetricConfig.GetMetricName = nil
	return cfg
}
