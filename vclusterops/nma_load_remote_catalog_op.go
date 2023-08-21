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
	"encoding/json"
	"errors"
	"fmt"

	"github.com/vertica/vcluster/vclusterops/vlog"
)

const successfulCode = 0

type NMALoadRemoteCatalogOp struct {
	OpBase
	hostRequestBodyMap        map[string]string
	dbName                    string
	communalLocation          string
	communalStorageParameters map[string]string
	oldHosts                  []string
	vdb                       *VCoordinationDatabase
	timeout                   uint
	primaryNodeCount          uint
}

type loadRemoteCatalogRequestData struct {
	DBName             string              `json:"db_name"`
	StorageLocations   []string            `json:"storage_locations"`
	CommunalLocation   string              `json:"communal_location"`
	CatalogPath        string              `json:"catalog_path"`
	Host               string              `json:"host"`
	NodeName           string              `json:"node_name"`
	AWSAccessKeyID     string              `json:"aws_access_key_id,omitempty"`
	AWSSecretAccessKey string              `json:"aws_secret_access_key,omitempty"`
	NodeAddresses      map[string][]string `json:"node_addresses"`
	Parameters         map[string]string   `json:"parameters,omitempty"`
}

func makeNMALoadRemoteCatalogOp(newHosts, oldHosts []string, dbName, communalLocation string, communalStorageParameters map[string]string,
	vdb *VCoordinationDatabase, timeout uint) NMALoadRemoteCatalogOp {
	op := NMALoadRemoteCatalogOp{}
	op.name = "NMALoadRemoteCatalogOp"
	op.hosts = newHosts
	op.oldHosts = oldHosts
	op.dbName = dbName
	op.communalLocation = communalLocation
	op.communalStorageParameters = communalStorageParameters
	op.vdb = vdb
	op.timeout = timeout

	op.primaryNodeCount = 0
	for host := range vdb.HostNodeMap {
		if vdb.HostNodeMap[host].IsPrimary {
			op.primaryNodeCount++
		}
	}

	return op
}

// make https json data
func (op *NMALoadRemoteCatalogOp) setupRequestBody(execContext *OpEngineExecContext) error {
	if len(execContext.networkProfiles) != len(op.hosts) {
		return fmt.Errorf("[%s] the number of hosts in networkProfiles does not match"+
			" the number of hosts that will load remote catalogs", op.name)
	}

	// NodeAddresses format {node_name : [new_ip, new_ip_control_ip, new_ip_broadcast_ip]}
	nodeAddresses := make(map[string][]string)
	for host, profile := range execContext.networkProfiles {
		var addresses []string
		addresses = append(addresses, host, profile.Address, profile.Broadcast)
		nodeName := op.vdb.HostNodeMap[host].Name
		nodeAddresses[nodeName] = addresses
	}

	op.hostRequestBodyMap = make(map[string]string)
	for index, host := range op.hosts {
		requestData := loadRemoteCatalogRequestData{}
		requestData.DBName = op.dbName
		requestData.CommunalLocation = op.communalLocation
		requestData.Host = op.oldHosts[index]
		vNode := op.vdb.HostNodeMap[host]
		requestData.NodeName = vNode.Name
		requestData.CatalogPath = vNode.CatalogPath
		requestData.StorageLocations = vNode.StorageLocations
		// if aws auth is specified in communal storage params, we extract it.
		// create aws_access_key_id and aws_secret_access_key using aws auth for calling NMA /catalog/revive.
		// we might need to do this for GCP and Azure too in the future
		awsKeyID, awsKeySecret, found, err := extractAWSAuthFromParameters(op.communalStorageParameters)
		if err != nil {
			return fmt.Errorf("[%s] %w", op.name, err)
		}
		if found {
			requestData.AWSAccessKeyID = awsKeyID
			requestData.AWSSecretAccessKey = awsKeySecret
		}
		requestData.NodeAddresses = nodeAddresses
		requestData.Parameters = op.communalStorageParameters

		dataBytes, err := json.Marshal(requestData)
		if err != nil {
			return fmt.Errorf("[%s] fail to marshal request data to JSON string, detail %w", op.name, err)
		}

		op.hostRequestBodyMap[host] = string(dataBytes)
	}

	return nil
}

func (op *NMALoadRemoteCatalogOp) setupClusterHTTPRequest(hosts []string) error {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.BuildNMAEndpoint("catalog/revive")
		httpRequest.RequestData = op.hostRequestBodyMap[host]
		httpRequest.Timeout = int(op.timeout)

		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *NMALoadRemoteCatalogOp) prepare(execContext *OpEngineExecContext) error {
	err := op.setupRequestBody(execContext)
	if err != nil {
		return err
	}

	execContext.dispatcher.Setup(op.hosts)
	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *NMALoadRemoteCatalogOp) execute(execContext *OpEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *NMALoadRemoteCatalogOp) finalize(_ *OpEngineExecContext) error {
	return nil
}

type loadCatalogResponse struct {
	StatusCode int `json:"status"`
}

func (op *NMALoadRemoteCatalogOp) processResult(_ *OpEngineExecContext) error {
	var allErrs error
	var successPrimaryNodeCount uint

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.isPassing() {
			response := loadCatalogResponse{}
			err := op.parseAndCheckResponse(host, result.content, &response)
			if err != nil {
				allErrs = errors.Join(allErrs, err)
				continue
			}

			if response.StatusCode != successfulCode {
				err = fmt.Errorf(`[%s] fail to load remote catalog on host %s`, op.name, host)
				vlog.LogError(err.Error())
				allErrs = errors.Join(allErrs, err)
				continue
			}

			if op.vdb.HostNodeMap[host].IsPrimary {
				successPrimaryNodeCount++
			}
			continue
		}

		httpsErr := errors.Join(fmt.Errorf("[%s] HTTPS call failed on host %s", op.name, host), result.err)
		allErrs = errors.Join(allErrs, httpsErr)
	}

	// quorum check
	if !op.hasQuorum(successPrimaryNodeCount, op.primaryNodeCount) {
		err := fmt.Errorf("[%s] fail to load catalog on enough primary nodes. Success count: %d", op.name, successPrimaryNodeCount)
		vlog.LogError(err.Error())
		allErrs = errors.Join(allErrs, err)
		return appendHTTPSFailureError(allErrs)
	}

	return nil
}
