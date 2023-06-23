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

package vclusterops

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

const (
	ksafetyThreshold = 3
	ksafeValue       = 1
)

// A good rule of thumb is to use normal strings unless you need nil.
// Normal strings are easier and safer to use in Go.
type VCreateDatabaseOptions struct {
	// part 1: basic db info
	DatabaseOptions
	Policy            *string
	SQLFile           *string
	LicensePathOnNode *string // required to be a fully qualified path
	// part 2: eon db info
	ShardCount                *int
	CommunalStorageLocation   *string
	CommunalStorageParamsPath *string
	DepotSize                 *string // like 10G
	GetAwsCredentialsFromEnv  *bool
	// part 3: optional info
	ConfigurationParameters   map[string]string
	ForceCleanupOnFailure     *bool
	ForceRemovalAtCreation    *bool
	SkipPackageInstall        *bool
	TimeoutNodeStartupSeconds *int
	// part 4: new params originally in installer generated admintools.conf, now in create db op
	Broadcast          *bool
	P2p                *bool
	LargeCluster       *int
	ClientPort         *int // for internal QA test only, do not abuse
	SpreadLogging      *bool
	SpreadLoggingLevel *int
	// part 5: other params
	SkipStartupPolling *bool
	ConfigDirectory    *string

	// hidden options (which cache information only)
	cachedUsername string
	bootstrapHost  []string
}

func VCreateDatabaseOptionsFactory() VCreateDatabaseOptions {
	opt := VCreateDatabaseOptions{}
	// without initialization it will panic
	opt.ConfigurationParameters = make(map[string]string)
	// set default values to the params
	opt.SetDefaultValues()

	return opt
}

func (opt *VCreateDatabaseOptions) SetDefaultValues() {
	opt.DatabaseOptions.SetDefaultValues()

	// basic db info
	defaultPolicy := util.DefaultRestartPolicy
	opt.Policy = &defaultPolicy
	opt.SQLFile = new(string)
	opt.LicensePathOnNode = new(string)

	// eon db Info
	opt.ShardCount = new(int)
	opt.CommunalStorageLocation = new(string)
	opt.CommunalStorageParamsPath = new(string)
	opt.DepotSize = new(string)
	opt.GetAwsCredentialsFromEnv = new(bool)

	// optional info
	opt.ForceCleanupOnFailure = new(bool)
	opt.ForceRemovalAtCreation = new(bool)
	opt.SkipPackageInstall = new(bool)

	defaultTimeoutNodeStartupSeconds := util.DefaultTimeoutSeconds
	opt.TimeoutNodeStartupSeconds = &defaultTimeoutNodeStartupSeconds

	// new params originally in installer generated admintools.conf, now in create db op
	opt.Broadcast = new(bool)

	defaultP2p := true
	opt.P2p = &defaultP2p

	defaultLargeCluster := -1
	opt.LargeCluster = &defaultLargeCluster

	defaultClientPort := util.DefaultClientPort
	opt.ClientPort = &defaultClientPort
	opt.SpreadLogging = new(bool)

	defaultSpreadLoggingLevel := -1
	opt.SpreadLoggingLevel = &defaultSpreadLoggingLevel

	// other params
	opt.SkipStartupPolling = new(bool)
}

func (opt *VCreateDatabaseOptions) CheckNilPointerParams() error {
	if err := opt.DatabaseOptions.CheckNilPointerParams(); err != nil {
		return err
	}

	// basic params
	if opt.Policy == nil {
		return util.ParamNotSetErrorMsg("policy")
	}
	if opt.SQLFile == nil {
		return util.ParamNotSetErrorMsg("sql")
	}
	if opt.LicensePathOnNode == nil {
		return util.ParamNotSetErrorMsg("license")
	}

	// eon params
	if opt.ShardCount == nil {
		return util.ParamNotSetErrorMsg("shard-count")
	}
	if opt.CommunalStorageLocation == nil {
		return util.ParamNotSetErrorMsg("communal-storage-location")
	}
	if opt.CommunalStorageParamsPath == nil {
		return util.ParamNotSetErrorMsg("communal_storage-params")
	}
	if opt.DepotSize == nil {
		return util.ParamNotSetErrorMsg("depot-size")
	}
	if opt.GetAwsCredentialsFromEnv == nil {
		return util.ParamNotSetErrorMsg("get-aws-credentials-from-env-vars")
	}

	return opt.CheckExtraNilPointerParams()
}

func (opt *VCreateDatabaseOptions) CheckExtraNilPointerParams() error {
	// optional params
	if opt.ForceCleanupOnFailure == nil {
		return util.ParamNotSetErrorMsg("force-cleanup-on-failure")
	}
	if opt.ForceRemovalAtCreation == nil {
		return util.ParamNotSetErrorMsg("force-removal-at-creation")
	}
	if opt.SkipPackageInstall == nil {
		return util.ParamNotSetErrorMsg("skip-package-install")
	}
	if opt.TimeoutNodeStartupSeconds == nil {
		return util.ParamNotSetErrorMsg("startup-timeout")
	}

	// other params
	if opt.Broadcast == nil {
		return util.ParamNotSetErrorMsg("broadcast")
	}
	if opt.P2p == nil {
		return util.ParamNotSetErrorMsg("point-to-point")
	}
	if opt.LargeCluster == nil {
		return util.ParamNotSetErrorMsg("large-cluster")
	}
	if opt.ClientPort == nil {
		return util.ParamNotSetErrorMsg("client-port")
	}
	if opt.SpreadLogging == nil {
		return util.ParamNotSetErrorMsg("spread-logging")
	}
	if opt.SpreadLoggingLevel == nil {
		return util.ParamNotSetErrorMsg("spread-logging-level")
	}
	if opt.SkipStartupPolling == nil {
		return util.ParamNotSetErrorMsg("skip-startup-polling")
	}

	return nil
}

func (opt *VCreateDatabaseOptions) ValidateRequiredOptions() error {
	// validate required parameters with default values
	if opt.Password == nil {
		opt.Password = new(string)
		*opt.Password = ""
		vlog.LogPrintInfoln("no password specified, using none")
	}

	if !util.StringInArray(*opt.Policy, util.RestartPolicyList) {
		return fmt.Errorf("policy must be one of %v", util.RestartPolicyList)
	}

	// MUST provide a fully qualified path,
	// because vcluster could be executed outside of Vertica cluster hosts
	// so no point to resolve relative paths to absolute paths by checking
	// localhost, where vcluster is run
	//
	// empty string ("") will be converted to the default license path (/opt/vertica/share/license.key)
	// in the /bootstrap-catalog endpoint
	if *opt.LicensePathOnNode != "" && !util.IsAbsPath(*opt.LicensePathOnNode) {
		return fmt.Errorf("must provide a fully qualified path for license file")
	}

	return nil
}

func validateDepotSizePercent(size string) (bool, error) {
	if !strings.Contains(size, "%") {
		return true, nil
	}
	cleanSize := strings.TrimSpace(size)
	// example percent depot size: '40%'
	r := regexp.MustCompile(`^([-+]?\d+)(%)$`)

	// example of matches: [[40%, 40, %]]
	matches := r.FindAllStringSubmatch(cleanSize, -1)

	if len(matches) != 1 {
		return false, fmt.Errorf("%s is not a well-formatted whole-number percentage of the format <int>%%", size)
	}

	valueStr := matches[0][1]
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return false, fmt.Errorf("%s is not a well-formatted whole-number percent of the format <int>%%", size)
	}

	if value > util.MaxDepotSize {
		return false, fmt.Errorf("depot-size %s is invalid, because it is greater than 100%%", size)
	} else if value < util.MinDepotSize {
		return false, fmt.Errorf("depot-size %s is invalid, because it is less than 0%%", size)
	}

	return true, nil
}

func validateDepotSizeBytes(size string) (bool, error) {
	// no need to validate for bytes if string contains '%'
	if strings.Contains(size, "%") {
		return true, nil
	}
	cleanSize := strings.TrimSpace(size)

	// example depot size: 1024K, 1024M, 2048G, 400T
	r := regexp.MustCompile(`^([-+]?\d+)([KMGT])$`)
	matches := r.FindAllStringSubmatch(cleanSize, -1)
	if len(matches) != 1 {
		return false, fmt.Errorf("%s is not a well-formatted whole-number size in bytes of the format <int>[KMGT]", size)
	}

	valueStr := matches[0][1]
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return false, fmt.Errorf("depot size %s is not a well-formatted whole-number size in bytes of the format <int>[KMGT]", size)
	}
	if value <= 0 {
		return false, fmt.Errorf("depot size %s is not a valid size because it is <= 0", size)
	}
	return true, nil
}

// may need to go back to consolt print for vcluster commands
// so return error information
func validateDepotSize(size string) (bool, error) {
	validDepotPercent, err := validateDepotSizePercent(size)
	if !validDepotPercent {
		return validDepotPercent, err
	}
	validDepotBytes, err := validateDepotSizeBytes(size)
	if !validDepotBytes {
		return validDepotBytes, err
	}
	return true, nil
}

func (opt *VCreateDatabaseOptions) ValidateEonOptions() error {
	if *opt.CommunalStorageLocation != "" {
		if *opt.DepotPrefix == "" {
			return fmt.Errorf("must specify a depot path with commual storage location")
		}
		if *opt.ShardCount == 0 {
			return fmt.Errorf("must specify a shard count greater than 0 with communal storage location")
		}
	}
	if *opt.DepotPrefix != "" && *opt.CommunalStorageLocation == "" {
		return fmt.Errorf("when depot path is given, communal storage location cannot be empty")
	}
	if *opt.GetAwsCredentialsFromEnv && *opt.CommunalStorageLocation == "" {
		return fmt.Errorf("AWS credentials are only used in Eon mode")
	}
	if *opt.DepotSize != "" {
		if *opt.DepotPrefix == "" {
			return fmt.Errorf("when depot size is given, depot path cannot be empty")
		}
		validDepotSize, err := validateDepotSize(*opt.DepotSize)
		if !validDepotSize {
			return err
		}
	}
	return nil
}

func (opt *VCreateDatabaseOptions) ValidateExtraOptions() error {
	if *opt.Broadcast && *opt.P2p {
		return fmt.Errorf("cannot use both Broadcast and Point-to-point networking mode")
	}
	// -1 is the default large cluster value, meaning 120 control nodes
	if *opt.LargeCluster != util.DefaultLargeCluster && (*opt.LargeCluster < 1 || *opt.LargeCluster > util.MaxLargeCluster) {
		return fmt.Errorf("must specify a valid large cluster value in range [1, 120]")
	}
	return nil
}

func (opt *VCreateDatabaseOptions) ValidateParseOptions() error {
	// check nil pointers in the required options
	err := opt.CheckNilPointerParams()
	if err != nil {
		return err
	}

	// validate base options
	err = opt.ValidateBaseOptions("create_db")
	if err != nil {
		return err
	}

	// batch 1: validate required parameters without default values
	err = opt.ValidateRequiredOptions()
	if err != nil {
		return err
	}
	// batch 2: validate eon params
	err = opt.ValidateEonOptions()
	if err != nil {
		return err
	}
	// batch 3: validate all other params
	err = opt.ValidateExtraOptions()
	if err != nil {
		return err
	}
	return nil
}

// Do advanced analysis on the options inputs, like resolve hostnames to be IPs
func (opt *VCreateDatabaseOptions) AnalyzeOptions() error {
	// resolve RawHosts to be IP addresses
	hostAddresses, err := util.ResolveRawHostsToAddresses(opt.RawHosts, *opt.Ipv6)
	if err != nil {
		return err
	}
	opt.Hosts = hostAddresses

	// process correct catalog path, data path and depot path prefixes
	*opt.CatalogPrefix = util.GetCleanPath(*opt.CatalogPrefix)
	*opt.DataPrefix = util.GetCleanPath(*opt.DataPrefix)
	*opt.DepotPrefix = util.GetCleanPath(*opt.DepotPrefix)
	if opt.ClientPort == nil {
		*opt.ClientPort = util.DefaultClientPort
	}
	if opt.LargeCluster == nil {
		*opt.LargeCluster = util.DefaultLargeCluster
	}
	return nil
}

func (opt *VCreateDatabaseOptions) ValidateAnalyzeOptions() error {
	if err := opt.ValidateParseOptions(); err != nil {
		return err
	}
	if err := opt.AnalyzeOptions(); err != nil {
		return err
	}
	return nil
}

func (opt *VCreateDatabaseOptions) VCreateDatabase() (VCoordinationDatabase, error) {
	/*
	 *   - Produce Instructions
	 *   - Create a VClusterOpEngine
	 *   - Give the instructions to the VClusterOpEngine to run
	 */
	// Analyze to produce vdb info, for later create db use and for cache db info
	vdb := MakeVCoordinationDatabase()
	err := vdb.SetFromCreateDBOptions(opt)
	if err != nil {
		return vdb, err
	}
	// produce instructions
	instructions, err := produceBasicCreateDBInstructions(&vdb, opt)
	if err != nil {
		vlog.LogPrintError("fail to produce instructions, %w", err)
		return vdb, err
	}

	additionalInstructions, err := produceAdditionalCreateDBInstructions(&vdb, opt)
	if err != nil {
		vlog.LogPrintError("fail to produce instructions, %w", err)
		return vdb, err
	}
	instructions = append(instructions, additionalInstructions...)

	// create a VClusterOpEngine, and add certs to the engine
	certs := HTTPSCerts{key: opt.Key, cert: opt.Cert, caCert: opt.CaCert}
	clusterOpEngine := MakeClusterOpEngine(instructions, &certs)

	// Give the instructions to the VClusterOpEngine to run
	runError := clusterOpEngine.Run()
	if runError != nil {
		vlog.LogPrintError("fail to create database, %w", runError)
		return vdb, runError
	}

	// write cluster information to the YAML config file
	err = writeClusterConfig(&vdb, opt.ConfigDirectory)
	if err != nil {
		vlog.LogPrintWarning("fail to write config file, details: %w", err)
	}

	return vdb, nil
}

/*
We expect that we will ultimately produce the following instructions:
    1. Check NMA connectivity
	2. Check to see if any dbs running
	3. Check Vertica versions
	4. Prepare directories
	5. Get network profiles
	6. Bootstrap the database
	7. Run the catalog editor
	8. Start node
	9. Create node
	10. Reload spread
	11. Transfer config files
	12. Start all nodes of the database
	13. Poll node startup
	14. Create depot
	15. Mark design ksafe
	16. Install packages
	17. sync catalog
*/

func produceBasicCreateDBInstructions(vdb *VCoordinationDatabase, options *VCreateDatabaseOptions) ([]ClusterOp, error) {
	var instructions []ClusterOp

	hosts := vdb.HostList
	initiator, err := getInitiator(hosts)
	if err != nil {
		return instructions, err
	}

	nmaHealthOp := MakeNMAHealthOp("NMAHealthOp", hosts)

	// require to have the same vertica version
	nmaVerticaVersionOp := MakeNMAVerticaVersionOp("NMAVerticaVersionOp", hosts, true)

	// need username for https operations
	username := *options.UserName
	if username == "" {
		var errGetUser error
		username, errGetUser = util.GetCurrentUsername()
		if errGetUser != nil {
			return instructions, errGetUser
		}
	}
	options.cachedUsername = username
	vlog.LogInfo("Current username is %s", username)

	checkDBRunningOp := MakeHTTPCheckRunningDBOp("HTTPCheckDBRunningOp", hosts,
		true /* use password auth */, username, options.Password, CreateDB)

	nmaPrepareDirectoriesOp, err := MakeNMAPrepareDirectoriesOp("NMAPrepareDirectoriesOp", vdb.HostNodeMap)
	if err != nil {
		return instructions, err
	}

	nmaNetworkProfileOp := MakeNMANetworkProfileOp("NMANetworkProfileOp", hosts)

	// should be only one bootstrap host
	// making it an array to follow the convention of passing a list of hosts to each operation
	bootstrapHost := []string{initiator}
	options.bootstrapHost = bootstrapHost
	nmaBootstrapCatalogOp, err := MakeNMABootstrapCatalogOp("NMABootstrapCatalogOp", vdb, options, bootstrapHost)
	if err != nil {
		return instructions, err
	}

	nmaFetchVdbFromCatEdOp, err := MakeNMAFetchVdbFromCatalogEditorOp("NMAFetchVdbFromCatalogEditorOp", vdb.HostNodeMap, bootstrapHost)
	if err != nil {
		return instructions, err
	}

	nmaStartNodeOp := MakeNMAStartNodeOp("NMAStartNodeOp", bootstrapHost)

	httpsPollBootstrapNodeStateOp := MakeHTTPSPollNodeStateOp("HTTPSPollNodeStateOp", bootstrapHost, true, username, options.Password)

	instructions = append(instructions,
		&nmaHealthOp,
		&nmaVerticaVersionOp,
		&checkDBRunningOp,
		&nmaPrepareDirectoriesOp,
		&nmaNetworkProfileOp,
		&nmaBootstrapCatalogOp,
		&nmaFetchVdbFromCatEdOp,
		&nmaStartNodeOp,
		&httpsPollBootstrapNodeStateOp,
	)

	if len(hosts) > 1 {
		httpCreateNodeOp := MakeHTTPCreateNodeOp("HTTPCreateNodeOp", bootstrapHost,
			true /* use password auth */, username, options.Password, vdb)
		instructions = append(instructions, &httpCreateNodeOp)
	}

	httpsReloadSpreadOp := MakeHTTPSReloadSpreadOp("HTTPSReloadSpreadOp", bootstrapHost, true, username, options.Password)
	instructions = append(instructions, &httpsReloadSpreadOp)

	if len(hosts) > 1 {
		produceTransferConfigOps(&instructions, bootstrapHost, vdb)
		newNodeHosts := util.SliceDiff(vdb.HostList, bootstrapHost)
		nmaStartNewNodesOp := MakeNMAStartNodeOp("NMAStartNodeOp", newNodeHosts)
		instructions = append(instructions,
			&nmaFetchVdbFromCatEdOp,
			&nmaStartNewNodesOp,
		)
	}

	return instructions, nil
}

func produceAdditionalCreateDBInstructions(vdb *VCoordinationDatabase, options *VCreateDatabaseOptions) ([]ClusterOp, error) {
	var instructions []ClusterOp

	hosts := vdb.HostList
	bootstrapHost := options.bootstrapHost
	username := options.cachedUsername

	if !*options.SkipStartupPolling {
		httpsPollNodeStateOp := MakeHTTPSPollNodeStateOp("HTTPSPollNodeStateOp", hosts, true, username, options.Password)
		instructions = append(instructions, &httpsPollNodeStateOp)
	}

	if vdb.UseDepot {
		httpsCreateDepotOp := MakeHTTPSCreateDepotOp("HTTPSCreateDepotOp", vdb, bootstrapHost, true, username, options.Password)
		instructions = append(instructions, &httpsCreateDepotOp)
	}

	if len(hosts) >= ksafetyThreshold {
		httpsMarkDesignKSafeOp := MakeHTTPSMarkDesignKSafeOp("HTTPSMarkDesignKsafeOp", bootstrapHost, true, username,
			options.Password, ksafeValue)
		instructions = append(instructions, &httpsMarkDesignKSafeOp)
	}

	if !*options.SkipPackageInstall {
		httpsInstallPackagesOp := MakeHTTPSInstallPackagesOp("HTTPSInstallPackagesOp", bootstrapHost, true, username, options.Password)
		instructions = append(instructions, &httpsInstallPackagesOp)
	}

	if vdb.IsEon {
		httpsSyncCatalogOp := MakeHTTPSSyncCatalogOp("HTTPSyncCatalogOp", bootstrapHost, true, username, options.Password)
		instructions = append(instructions, &httpsSyncCatalogOp)
	}
	return instructions, nil
}

func getInitiator(hosts []string) (string, error) {
	errMsg := "fail to find initiator node from the host list"

	for _, host := range hosts {
		isLocalHost, err := util.IsLocalHost(host)
		if err != nil {
			return "", fmt.Errorf("%s, %w", errMsg, err)
		}

		if isLocalHost {
			return host, nil
		}
	}

	// If none of the hosts is localhost, we assign the first host as initiator.
	// Our assumptions is that vcluster and vclusterops are not always running on a host,
	// that is a part of vertica cluster.
	// Therefore, we can of course prioritize localhost,
	//   if localhost is a part of the --hosts;
	// but if none of the given hosts is localhost,
	//   we should just nominate hosts[0] as the initiator to bootstrap catalog.
	return hosts[0], nil
}

func produceTransferConfigOps(instructions *[]ClusterOp, bootstrapHost []string, vdb *VCoordinationDatabase) {
	var verticaConfContent string
	nmaDownloadVerticaConfigOp := MakeNMADownloadConfigOp(
		"NMADownloadVerticaConfigOp", vdb, bootstrapHost, "config/vertica", &verticaConfContent)
	nmaUploadVerticaConfigOp := MakeNMAUploadConfigOp(
		"NMAUploadVerticaConfigOp", vdb, bootstrapHost, "config/vertica", &verticaConfContent)
	var spreadConfContent string
	nmaDownloadSpreadConfigOp := MakeNMADownloadConfigOp(
		"NMADownloadSpreadConfigOp", vdb, bootstrapHost, "config/spread", &spreadConfContent)
	nmaUploadSpreadConfigOp := MakeNMAUploadConfigOp(
		"NMAUploadSpreadConfigOp", vdb, bootstrapHost, "config/spread", &spreadConfContent)
	*instructions = append(*instructions,
		&nmaDownloadVerticaConfigOp,
		&nmaUploadVerticaConfigOp,
		&nmaDownloadSpreadConfigOp,
		&nmaUploadSpreadConfigOp,
	)
}

func writeClusterConfig(vdb *VCoordinationDatabase, configDir *string) error {
	/* build config information
	 */
	clusterConfig := MakeClusterConfig()
	clusterConfig.DBName = vdb.Name
	clusterConfig.Hosts = vdb.HostList
	clusterConfig.CatalogPath = vdb.CatalogPrefix
	clusterConfig.DataPath = vdb.DataPrefix
	clusterConfig.DepotPath = vdb.DepotPrefix
	for _, host := range vdb.HostList {
		nodeConfig := NodeConfig{}
		node, ok := vdb.HostNodeMap[host]
		if !ok {
			errMsg := fmt.Sprintf("cannot find node info from host %s", host)
			panic(vlog.ErrorLog + errMsg)
		}
		nodeConfig.Address = host
		nodeConfig.Name = node.Name
		clusterConfig.Nodes = append(clusterConfig.Nodes, nodeConfig)
	}
	clusterConfig.IsEon = vdb.IsEon
	clusterConfig.Ipv6 = vdb.Ipv6

	/* write config to a YAML file
	 */
	configFilePath, err := GetConfigFilePath(vdb.Name, configDir)
	if err != nil {
		return err
	}

	// if the config file exists already
	// create its backup before overwriting it
	err = BackupConfigFile(configFilePath)
	if err != nil {
		return err
	}

	err = clusterConfig.WriteConfig(configFilePath)
	if err != nil {
		return err
	}

	return nil
}
