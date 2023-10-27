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
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

const (
	kubernetesPort  = "KUBERNETES_PORT"
	databaseName    = "DATABASE_NAME"
	catalogPathPref = "CATALOG_PATH"
)

/* CmdScrutinize
 *
 * Implements ClusterCommand interface
 *
 * Parses CLI arguments for scrutinize operation.
 * Prepares the inputs for the library.
 *
 */
type CmdScrutinize struct {
	CmdBase
	sOptions vclusterops.VScrutinizeOptions
}

func makeCmdScrutinize() *CmdScrutinize {
	newCmd := &CmdScrutinize{}
	newCmd.parser = flag.NewFlagSet("scrutinize", flag.ExitOnError)

	// Parse one variable and store it in a arbitrary location
	newCmd.sOptions.Password = newCmd.parser.String("password",
		"",
		util.GetOptionalFlagMsg("Database password. Consider using in single quotes to avoid shell substitution."))

	newCmd.sOptions.UserName = newCmd.parser.String("db-user",
		"",
		util.GetOptionalFlagMsg("Database username. Consider using single quotes to avoid shell substitution."))

	newCmd.sOptions.HonorUserInput = newCmd.parser.Bool("honor-user-input",
		false,
		util.GetOptionalFlagMsg("Forcefully use the user's input instead of reading the options from "+vclusterops.ConfigFileName))

	newCmd.hostListStr = newCmd.parser.String("hosts", "", util.GetOptionalFlagMsg("Comma-separated host list"))

	return newCmd
}

func (c *CmdScrutinize) CommandType() string {
	return "scrutinize"
}

func (c *CmdScrutinize) Parse(inputArgv []string, log vlog.Printer) error {
	c.argv = inputArgv
	// from now on we use the internal copy of argv
	return c.parseInternal(log)
}

func (c *CmdScrutinize) parseInternal(log vlog.Printer) error {
	if c.parser == nil {
		return fmt.Errorf("unexpected nil for CmdScrutinize.parser")
	}
	log.PrintInfo("Parsing scrutinize command input")
	parseError := c.ParseArgv()
	if parseError != nil {
		return parseError
	}

	log.Info("Parsing host list")
	var hostParseError error
	c.sOptions.RawHosts, hostParseError = util.SplitHosts(*c.hostListStr)
	if hostParseError != nil {
		return hostParseError
	}
	log.Info("Host list size and values", "size", len(c.sOptions.RawHosts), "values", c.sOptions.RawHosts)
	return nil
}

func (c *CmdScrutinize) Analyze(log vlog.Printer) error {
	log.Info("Called method Analyze()")

	var resolveError error
	c.sOptions.Hosts, resolveError = util.ResolveRawHostsToAddresses(c.sOptions.RawHosts, false /*ipv6?*/)
	if resolveError != nil {
		return resolveError
	}
	log.Info("Resolved host list to IPs", "hosts", c.sOptions.Hosts)

	var allErrs error
	port, found := os.LookupEnv(kubernetesPort)
	if found && port != "" && *c.sOptions.HonorUserInput {
		log.Info(kubernetesPort, " is set, k8s environment detected", found)
		dbName, found := os.LookupEnv(databaseName)
		if !found || dbName == "" {
			allErrs = errors.Join(allErrs, fmt.Errorf("unable to get database name from environment variable. "))
		} else {
			c.sOptions.DBName = &dbName
			log.Info("Setting database name from env as", "DBName", *c.sOptions.DBName)
		}

		catPrefix, found := os.LookupEnv(catalogPathPref)
		if !found || catPrefix == "" {
			allErrs = errors.Join(allErrs, fmt.Errorf("unable to get catalog path from environment variable. "))
		} else {
			c.sOptions.CatalogPrefix = &catPrefix
			log.Info("Setting catalog path from env as", "CatalogPrefix", *c.sOptions.CatalogPrefix)
		}
		if allErrs != nil {
			return allErrs
		}
	}
	return nil
}

func (c *CmdScrutinize) Run(vcc vclusterops.VClusterCommands) error {
	vcc.Log.PrintInfo("Running scrutinize")
	vcc.Log.V(0).Info("Calling method Run() for command " + c.CommandType())
	err := vcc.VScrutinize(&c.sOptions)
	vcc.Log.PrintInfo("Completed method Run() for command " + c.CommandType())
	return err
}
