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
	"flag"
	"fmt"
	"strconv"

	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdStopDB
 *
 * Parses arguments to stopDB and calls
 * the high-level function for stopDB.
 *
 * Implements ClusterCommand interface
 */

type CmdStopDB struct {
	stopDBOptions *vclusterops.VStopDatabaseOptions

	CmdBase
}

func MakeCmdStopDB() CmdStopDB {
	// CmdStopDB
	newCmd := CmdStopDB{}

	// parser, used to parse command-line flags
	newCmd.parser = flag.NewFlagSet("stop_db", flag.ExitOnError)
	stopDBOptions := vclusterops.VStopDatabaseOptionsFactory()

	// optional flags
	stopDBOptions.Name = newCmd.parser.String("name", "", util.GetOptionalFlagMsg("The name of the database to be stopped."+
		" Use it when you do not trust "+vclusterops.ConfigFileName))
	stopDBOptions.Password = newCmd.parser.String("password", "", util.GetOptionalFlagMsg("Database password in single quotes"))
	newCmd.hostListStr = newCmd.parser.String("hosts", "", util.GetOptionalFlagMsg("Comma-separated list of hosts to participate in database."+
		" Use it when you do not trust "+vclusterops.ConfigFileName))
	// new flags comparing to adminTools stop_db
	stopDBOptions.Ipv6 = newCmd.parser.Bool("ipv6", false, util.GetOptionalFlagMsg("Stop database with IPv6 hosts"))
	stopDBOptions.HonorUserInput = newCmd.parser.Bool("honor-user-input", false,
		util.GetOptionalFlagMsg("Forcefully use the user's input instead of reading the options from "+vclusterops.ConfigFileName))
	stopDBOptions.ConfigDirectory = newCmd.parser.String("config-directory", "",
		util.GetOptionalFlagMsg("Directory where "+vclusterops.ConfigFileName+" is located"))

	// Eon flags
	stopDBOptions.IsEon = newCmd.parser.Bool("eon-mode", false, util.GetEonFlagMsg("indicate if the database is an Eon db."+
		" Use it when you do not trust "+vclusterops.ConfigFileName))
	stopDBOptions.DrainSeconds = newCmd.parser.Int("drain-seconds", util.DefaultDrainSeconds,
		util.GetEonFlagMsg("seconds to wait for user connections to close."+
			" Default value is "+strconv.Itoa(util.DefaultDrainSeconds)+" seconds."+
			" When the time expires, connections will be forcibly closed and the db will shut down"))

	// hidden options
	// TODO use these hidden options in stop_db, CheckUserConn can be move to optional flags above when we support it
	stopDBOptions.CheckUserConn = newCmd.parser.Bool("if-no-users", false, util.SuppressHelp)
	stopDBOptions.ForceKill = newCmd.parser.Bool("force-kill", false, util.SuppressHelp)

	newCmd.stopDBOptions = &stopDBOptions

	newCmd.parser.Usage = func() {
		util.SetParserUsage(newCmd.parser, "stop_db")
	}

	return newCmd
}

func (c *CmdStopDB) CommandType() string {
	return "stop_db"
}

func (c *CmdStopDB) Parse(inputArgv []string) error {
	vlog.LogArgParse(&inputArgv)

	if c.parser == nil {
		return fmt.Errorf("unexpected nil - the parser was nil")
	}

	c.argv = inputArgv
	err := c.ValidateParseArgv(c.CommandType())
	if err != nil {
		return err
	}

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	if !util.IsOptionSet(c.parser, "password") {
		c.stopDBOptions.Password = nil
	}
	if !util.IsOptionSet(c.parser, "eon-mode") {
		c.stopDBOptions.IsEon = nil
	}
	if !util.IsOptionSet(c.parser, "drain-seconds") {
		c.stopDBOptions.DrainSeconds = nil
	}
	if !util.IsOptionSet(c.parser, "config-directory") {
		c.stopDBOptions.ConfigDirectory = nil
	}

	return c.validateParse()
}

// all validations of the arguments should go in here
func (c *CmdStopDB) validateParse() error {
	vlog.LogInfoln("Called validateParse()")

	// parse raw host str input into a []string of stopDBOptions
	if *c.stopDBOptions.HonorUserInput {
		err := c.ParseHostList(&c.stopDBOptions.DatabaseOptions)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *CmdStopDB) Analyze() error {
	vlog.LogInfoln("Called method Analyze()")
	return nil
}

func (c *CmdStopDB) Run() error {
	vlog.LogInfoln("Called method Run()")
	vcc := vclusterops.VClusterCommands{}
	dbName, stopError := vcc.VStopDatabase(c.stopDBOptions)
	if stopError != nil {
		return stopError
	}
	vlog.LogPrintInfo("Stopped a database with name [%s]", dbName)
	return nil
}
