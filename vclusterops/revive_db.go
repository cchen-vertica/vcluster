package vclusterops

import (
	"fmt"
	"path/filepath"
	"sort"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

const (
	sourceFileName           = "cluster_config.json"
	sourceFileMetadataFolder = "metadata"
	destinationFilePath      = "/tmp/desc.json"
	// catalogPath is not used for now
	catalogPath = ""
)

type VReviveDBOptions struct {
	// part 1: basic db info
	DatabaseOptions
	// part 2: revive db info
	CommunalStorageLocation   *string
	CommunalStorageParameters map[string]string
	LoadCatalogTimeout        *uint
	ForceRemoval              *bool
}

func VReviveDBOptionsFactory() VReviveDBOptions {
	opt := VReviveDBOptions{}

	// set default values to the params
	opt.setDefaultValues()

	return opt
}

func (options *VReviveDBOptions) setDefaultValues() {
	options.DatabaseOptions.SetDefaultValues()

	// set default values for revive db options
	options.CommunalStorageLocation = new(string)
	options.CommunalStorageParameters = make(map[string]string)
	options.LoadCatalogTimeout = new(uint)
	*options.LoadCatalogTimeout = util.DefaultLoadCatalogTimeoutSeconds
	options.ForceRemoval = new(bool)
}

func (options *VReviveDBOptions) validateRequiredOptions() error {
	// database name
	if *options.Name == "" {
		return fmt.Errorf("must specify a database name")
	}
	err := util.ValidateName(*options.Name, "database")
	if err != nil {
		return err
	}

	// new hosts
	if len(options.RawHosts) == 0 {
		return fmt.Errorf("must specify a host or host list")
	}

	// communal storage
	if *options.CommunalStorageLocation == "" {
		return fmt.Errorf("must specify a communal storage location")
	}

	return nil
}

func (options *VReviveDBOptions) validateParseOptions() error {
	return options.validateRequiredOptions()
}

// analyzeOptions will modify some options based on what is chosen
func (options *VReviveDBOptions) analyzeOptions() (err error) {
	// resolve RawHosts to be IP addresses
	options.Hosts, err = util.ResolveRawHostsToAddresses(options.RawHosts, options.Ipv6.ToBool())
	if err != nil {
		return err
	}

	return nil
}

func (options *VReviveDBOptions) ValidateAnalyzeOptions() error {
	if err := options.validateParseOptions(); err != nil {
		return err
	}
	err := options.analyzeOptions()
	return err
}

// VReviveDB can revive a database which has been terminated but its communal storage data still exists
func (vcc *VClusterCommands) VReviveDB(options *VReviveDBOptions) error {
	/*
	 *   - Validate options
	 *   - Run VClusterOpEngine to get terminated database info
	 *   - Run VClusterOpEngine again to revive the database
	 */

	// validate and analyze options
	err := options.ValidateAnalyzeOptions()
	if err != nil {
		return err
	}

	vdb := MakeVCoordinationDatabase()

	// part 1: produce instructions for getting terminated database info, and save the info to vdb
	instructions1, err := produceReviveDBInstructions1(options, &vdb)
	if err != nil {
		vlog.LogPrintError("fail to production part 1 instructions in revive_db %v", err)
		return err
	}

	// generate clusterOpEngine certs
	certs := HTTPSCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	// feed the part1 instructions to the VClusterOpEngine
	clusterOpEngine := MakeClusterOpEngine(instructions1, &certs)
	err = clusterOpEngine.Run()
	if err != nil {
		vlog.LogPrintError("fail to collect the information of database in revive_db %v", err)
		return err
	}

	// part 2: produce instructions for reviving database using terminated database info
	instructions2, err := produceReviveDBInstructions2(options, &vdb)
	if err != nil {
		vlog.LogPrintError("fail to production part 2 instructions in revive_db %v", err)
		return err
	}

	// feed the part2 instructions to the VClusterOpEngine
	clusterOpEngine = MakeClusterOpEngine(instructions2, &certs)
	err = clusterOpEngine.Run()
	if err != nil {
		vlog.LogPrintError("fail to revive database %v", err)
		return err
	}
	return nil
}

// revive db instructions are split into two parts:
// 1. get terminated database info
// 2. revive database using the info we got from step 1
// The reason of using two set of instructions is: the second set of instructions needs the database info
// to initialize, but that info can only be retrieved after we ran first set of instructions in clusterOpEngine
//
// produceReviveDBInstructions1 will build the first half of revive_db instructions
// The generated instructions will later perform the following operations
//   - Check NMA connectivity
//   - Check NMA version
//   - Check any DB running on the hosts
//   - Download and read the description file from communal storage on the initiator
func produceReviveDBInstructions1(options *VReviveDBOptions, vdb *VCoordinationDatabase) ([]ClusterOp, error) {
	var instructions []ClusterOp

	nmaHealthOp := makeNMAHealthOp(options.Hosts)
	nmaVerticaVersionOp := makeNMAVerticaVersionOp(options.Hosts, true)

	checkDBRunningOp, err := makeHTTPCheckRunningDBOp(options.Hosts, false, /*use password auth*/
		"" /*username for https call*/, nil /*password for https call*/, ReviveDB)
	if err != nil {
		return instructions, err
	}

	initiator := getInitiator(options.Hosts)
	bootstrapHost := []string{initiator}

	// description file directory will be in the location like: s3://tfminio/test_db/metadata/test_db/cluster_config.json
	sourceFilePath := filepath.Join(*options.CommunalStorageLocation, sourceFileMetadataFolder, *options.Name, sourceFileName)
	nmaDownLoadFileOp, err := makeNMADownloadFileOp(bootstrapHost, options.Hosts, sourceFilePath, destinationFilePath, catalogPath,
		options.CommunalStorageParameters, vdb)
	if err != nil {
		return instructions, err
	}

	instructions = append(instructions,
		&nmaHealthOp,
		&nmaVerticaVersionOp,
		&checkDBRunningOp,
		&nmaDownLoadFileOp,
	)

	return instructions, nil
}

// produceReviveDBInstructions2 will build the second half of revive_db instructions
// The generated instructions will later perform the following operations
//   - Prepare database directories for all the hosts
//   - Get network profiles for all the hosts
//   - Load remote catalog from communal storage on all the hosts
func produceReviveDBInstructions2(options *VReviveDBOptions, vdb *VCoordinationDatabase) ([]ClusterOp, error) {
	var instructions []ClusterOp

	newVDB, oldHosts := generateReviveVDB(vdb, options.Hosts)

	// create a new HostNodeMap to prepare directories
	hostNodeMap := make(map[string]VCoordinationNode)
	// update storage locations of each node to exclude user storage locations
	// because user storage location will have different process way than db storage location
	for host := range newVDB.HostNodeMap {
		vNode := newVDB.HostNodeMap[host]
		userLocationSet := make(map[string]struct{})
		for _, userLocation := range vNode.UserStorageLocations {
			userLocationSet[userLocation] = struct{}{}
		}
		var newLocations []string
		for _, location := range vNode.StorageLocations {
			if _, exist := userLocationSet[location]; !exist {
				newLocations = append(newLocations, location)
			}
		}
		vNode.StorageLocations = newLocations
		hostNodeMap[host] = vNode
	}
	// prepare all directories
	nmaPrepareDirectoriesOp, err := makeNMAPrepareDirectoriesOp(hostNodeMap, *options.ForceRemoval, true /*for db revive*/)
	if err != nil {
		return instructions, err
	}

	nmaNetworkProfileOp := makeNMANetworkProfileOp(options.Hosts)

	nmaLoadRemoteCatalogOp := makeNMALoadRemoteCatalogOp(options.Hosts, oldHosts, *options.Name, *options.CommunalStorageLocation,
		options.CommunalStorageParameters, &newVDB, *options.LoadCatalogTimeout)

	instructions = append(instructions,
		&nmaPrepareDirectoriesOp,
		&nmaNetworkProfileOp,
		&nmaLoadRemoteCatalogOp,
	)

	return instructions, nil
}

// generateReviveVDB can create new vdb, and sort the old hosts with new hosts' order using the old cluster info
func generateReviveVDB(vdb *VCoordinationDatabase, hosts []string) (newVDB VCoordinationDatabase, oldHosts []string) {
	newVDB = MakeVCoordinationDatabase()
	// use new cluster hosts
	newVDB.HostList = hosts

	/* for example, in old vdb, we could have the unsorted HostNodeMap
	{
	"192.168.1.103": {Name: v_test_db_node0003, Address: "192.168.1.103", ...},
	"192.168.1.101": {Name: v_test_db_node0001, Address: "192.168.1.101", ...},
	"192.168.1.102": {Name: v_test_db_node0002, Address: "192.168.1.102", ...}
	}
	in new vdb, we want to update the HostNodeMap with the values in --hosts, e.g. 10.1.10.2,10.1.10.1,10.1.10.3;
	and we respect the user input order. We will have the new HostNodeMap like:
	{
	"10.1.10.2": {Name: v_test_db_node0001, Address: "10.1.10.2", ...},
	"10.1.10.1": {Name: v_test_db_node0002, Address: "10.1.10.1", ...},
	"10.1.10.3": {Name: v_test_db_node0003, Address: "10.1.10.3", ...}
	}
	*/
	// sort nodes by their names, and then assign new hosts to them
	var vNodes []VCoordinationNode
	for host := range vdb.HostNodeMap {
		vNodes = append(vNodes, vdb.HostNodeMap[host])
	}
	sort.Slice(vNodes, func(i, j int) bool {
		return vNodes[i].Name < vNodes[j].Name
	})

	newVDB.HostNodeMap = make(map[string]VCoordinationNode)
	for index, newHost := range newVDB.HostList {
		// recreate the old host list with new hosts' order
		oldHosts = append(oldHosts, vNodes[index].Address)
		vNodes[index].Address = newHost
		newVDB.HostNodeMap[newHost] = vNodes[index]
	}

	return newVDB, oldHosts
}
