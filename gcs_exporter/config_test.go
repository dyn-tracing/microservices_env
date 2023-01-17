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

func TestLoadConfig(t *testing.T) {
    cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

    sub, err := cm.Sub(component.NewIDWithName(typeStr, "").String())
	require.NoError(t, err)
	require.NoError(t, component.UnmarshalConfig(sub, cfg))

	assert.Equal(t, len(cfg.Exporters), 2)

	defaultConfig := factory.CreateDefaultConfig().(*Config)
	assert.Equal(t, cfg.Exporters[config.NewComponentID(typeStr)], defaultConfig)

	customConfig := factory.CreateDefaultConfig().(*Config)
	customConfig.SetIDName("customname")

	customConfig.ProjectID = "my-project"
	customConfig.UserAgent = "opentelemetry-collector-contrib {{version}}"
	customConfig.Endpoint = "test-endpoint"
    customConfig.BucketSuffix = "snicket4"
    customConfig.NumOfDigitsForRandomHash = "2"
	customConfig.Insecure = true
	customConfig.TimeoutSettings = exporterhelper.TimeoutSettings{
		Timeout: 20 * time.Second,
	}
	customConfig.Compression = "gzip"
	assert.Equal(t, cfg.Exporters[config.NewComponentIDWithName(typeStr, "customname")], customConfig)
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
