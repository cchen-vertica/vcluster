/*
 (c) Copyright [2023] Open Text.
 Licensed under the Apache License, Version 2.0 (the "License");
 You may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package commands

import (
	"os"
	"testing"

	"github.com/vertica/vcluster/vclusterops/vlog"

	"github.com/stretchr/testify/assert"
)

const (
	kubePort    = "5433"
	dbName      = "test_db"
	catalogPath = "/catalog/path"
)

func TestScrutinCmd(t *testing.T) {
	// Positive case
	os.Setenv(kubernetesPort, kubePort)
	os.Setenv(databaseName, dbName)
	os.Setenv(catalogPathPref, catalogPath)
	c := makeCmdScrutinize()
	*c.sOptions.HonorUserInput = true
	err := c.Analyze(vlog.Printer{})
	assert.Nil(t, err)
	assert.Equal(t, dbName, *c.sOptions.DBName)
	assert.Equal(t, catalogPath, *c.sOptions.CatalogPrefix)

	// Catalog Path not provided
	os.Setenv(catalogPathPref, "")
	c = makeCmdScrutinize()
	*c.sOptions.HonorUserInput = true
	err = c.Analyze(vlog.Printer{})
	assert.ErrorContains(t, err, "unable to get catalog path from environment variable")

	// Database Name not provided
	os.Setenv(databaseName, "")
	os.Setenv(catalogPathPref, catalogPath)
	c = makeCmdScrutinize()
	*c.sOptions.HonorUserInput = true
	err = c.Analyze(vlog.Printer{})
	assert.ErrorContains(t, err, "unable to get database name from environment variable")
}
