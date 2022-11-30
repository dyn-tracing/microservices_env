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

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config"
)

func TestCreateDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	assert.NotNil(t, cfg, "failed to create default config")
	assert.NoError(t, componenttest.CheckConfigStruct(cfg))
}

func TestType(t *testing.T) {
	factory := NewFactory()
	assert.Equal(t, config.Type(typeStr), factory.Type())
}

func TestCreateTracesExporter(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	eCfg := cfg.(*Config)
	eCfg.Endpoint = "http://testing.invalid"

	te, err := factory.CreateTracesExporter(
		context.Background(),
		componenttest.NewNopExporterCreateSettings(),
		eCfg,
	)
	assert.NoError(t, err)
	assert.NotNil(t, te, "failed to create trace exporter")
}

func TestEnsureExporter(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	eCfg := cfg.(*Config)
	eCfg.Endpoint = "http://testing.invalid"

	exporter1 := ensureExporter(componenttest.NewNopExporterCreateSettings(), eCfg)
	exporter2 := ensureExporter(componenttest.NewNopExporterCreateSettings(), eCfg)
	assert.Equal(t, exporter1, exporter2)
}
