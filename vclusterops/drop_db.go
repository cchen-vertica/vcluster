package vclusterops

import (
	"fmt"
	"os"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

// A good rule of thumb is to use normal strings unless you need nil.
// Normal strings are easier and safer to use in Go.
type VDropDatabaseOptions struct {
	VCreateDatabaseOptions
	ForceDelete *bool
}

func VDropDatabaseOptionsFactory() VDropDatabaseOptions {
	opt := VDropDatabaseOptions{}
	// set default values to the params
	opt.SetDefaultValues()

	return opt
}

func (options *VDropDatabaseOptions) AnalyzeOptions() error {
	hostAddresses, err := util.ResolveRawHostsToAddresses(options.RawHosts, options.Ipv6.ToBool())
	if err != nil {
		return err
	}

	options.Hosts = hostAddresses
	return nil
}

func VDropDatabase(options *VDropDatabaseOptions) error {
	/*
	 *   - Produce Instructions
	 *   - Create a VClusterOpEngine
	 *   - Give the instructions to the VClusterOpEngine to run
	 */

	// Analyze to produce vdb info for drop db use
	vdb := MakeVCoordinationDatabase()

	// TODO: load from options if HonorUserInput is true

	// load vdb info from the YAML config file
	var configDir string

	if options.ConfigDirectory != nil {
		configDir = *options.ConfigDirectory
	} else {
		currentDir, err := os.Getwd()
		if err != nil {
			return fmt.Errorf("fail to get current directory")
		}
		configDir = currentDir
	}

	clusterConfig, err := ReadConfig(configDir)
	if err != nil {
		return err
	}
	vdb.SetFromClusterConfig(&clusterConfig)

	// produce drop_db instructions
	instructions, err := produceDropDBInstructions(&vdb, options)
	if err != nil {
		vlog.LogPrintError("fail to produce instructions, %w", err)
		return err
	}

	// create a VClusterOpEngine, and add certs to the engine
	certs := HTTPSCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := MakeClusterOpEngine(instructions, &certs)

	// Give the instructions to the VClusterOpEngine to run
	runError := clusterOpEngine.Run()
	if runError != nil {
		vlog.LogPrintError("fail to drop database: %w", runError)
		return runError
	}

	// if the database is successfully dropped, the config file will be removed
	// if failed to remove it, we will ask users to manually do it
	err = RemoveConfigFile(configDir)
	if err != nil {
		vlog.LogPrintWarning("Fail to remove the config file(s), please manually clean up under directory %s", configDir)
	}

	return nil
}

/*
We expect that we will ultimately produce the following instructions:
    1. Check NMA connectivity
    2. Check Vertica versions
	3. Check to see if any dbs running
	4. Delete directories
*/

func produceDropDBInstructions(vdb *VCoordinationDatabase, options *VDropDatabaseOptions) ([]ClusterOp, error) {
	var instructions []ClusterOp

	hosts := vdb.HostList
	usePassword := false
	if options.Password != nil {
		usePassword = true
		err := options.ValidateUserName()
		if err != nil {
			return instructions, err
		}
	}

	nmaHealthOp := MakeNMAHealthOp("NMAHealthOp", hosts)

	// require to have the same vertica version
	nmaVerticaVersionOp := MakeNMAVerticaVersionOp("NMAVerticaVersionOp", hosts, true)

	// when checking the running database,
	// drop_db has the same checking items with create_db
	checkDBRunningOp := MakeHTTPCheckRunningDBOp("HTTPCheckDBRunningOp", hosts,
		usePassword, *options.UserName, options.Password, CreateDB)

	nmaDeleteDirectoriesOp, err := MakeNMADeleteDirectoriesOp("NMADeleteDirectoriesOp", vdb, *options.ForceDelete)
	if err != nil {
		return instructions, err
	}

	instructions = append(instructions,
		&nmaHealthOp,
		&nmaVerticaVersionOp,
		&checkDBRunningOp,
		&nmaDeleteDirectoriesOp,
	)

	return instructions, nil
}
