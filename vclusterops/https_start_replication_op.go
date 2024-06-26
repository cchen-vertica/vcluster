/*
 (c) Copyright [2023-2024] Open Text.
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

	"github.com/vertica/vcluster/vclusterops/util"
)

type httpsStartReplicationOp struct {
	opBase
	opHTTPSBase
	hostRequestBodyMap map[string]string
	sourceDB           string
	targetHosts        string
	targetDB           string
	sandbox            string
	targetUserName     string
	targetPassword     *string
	tlsConfig          string
}

func makeHTTPSStartReplicationOp(dbName string, sourceHosts []string,
	sourceUseHTTPPassword bool, sourceUserName string,
	sourceHTTPPassword *string, targetUseHTTPPassword bool, targetDB, targetUserName, targetHosts string,
	targetHTTPSPassword *string, tlsConfig, sandbox string) (httpsStartReplicationOp, error) {
	op := httpsStartReplicationOp{}
	op.name = "HTTPSStartReplicationOp"
	op.description = "Start database replication"
	op.sourceDB = dbName
	op.hosts = sourceHosts
	op.useHTTPPassword = sourceUseHTTPPassword
	op.targetDB = targetDB
	op.targetHosts = targetHosts
	op.tlsConfig = tlsConfig
	op.sandbox = sandbox

	if sourceUseHTTPPassword {
		err := util.ValidateUsernameAndPassword(op.name, sourceUseHTTPPassword, sourceUserName)
		if err != nil {
			return op, err
		}
		op.userName = sourceUserName
		op.httpsPassword = sourceHTTPPassword
	}
	if targetUseHTTPPassword {
		err := util.ValidateUsernameAndPassword(op.name, targetUseHTTPPassword, targetUserName)
		if err != nil {
			return op, err
		}
		op.targetUserName = targetUserName
		op.targetPassword = targetHTTPSPassword
	}

	return op, nil
}

type replicateRequestData struct {
	TargetHost     string  `json:"host"`
	TargetDB       string  `json:"dbname"`
	TargetUserName string  `json:"user,omitempty"`
	TargetPassword *string `json:"password,omitempty"`
	TLSConfig      string  `json:"tls_config,omitempty"`
}

func (op *httpsStartReplicationOp) setupRequestBody(hosts []string) error {
	op.hostRequestBodyMap = make(map[string]string)

	for _, host := range hosts {
		replicateData := replicateRequestData{}
		replicateData.TargetHost = op.targetHosts
		replicateData.TargetDB = op.targetDB
		replicateData.TargetUserName = op.targetUserName
		replicateData.TargetPassword = op.targetPassword
		replicateData.TLSConfig = op.tlsConfig

		dataBytes, err := json.Marshal(replicateData)
		if err != nil {
			return fmt.Errorf("[%s] fail to marshal request data to JSON string, detail %w", op.name, err)
		}

		op.hostRequestBodyMap[host] = string(dataBytes)
	}

	return nil
}

func (op *httpsStartReplicationOp) setupClusterHTTPRequest(hosts []string) error {
	for _, host := range hosts {
		httpRequest := hostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.buildHTTPSEndpoint("replicate/start")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}
		httpRequest.RequestData = op.hostRequestBodyMap[host]
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
	return nil
}

func (op *httpsStartReplicationOp) prepare(execContext *opEngineExecContext) error {
	if len(execContext.nodesInfo) == 0 {
		return fmt.Errorf(`[%s] cannot find any hosts in OpEngineExecContext`, op.name)
	}
	// source hosts will be :
	// 1. up hosts from the main subcluster if the sandbox is empty
	// 2. up hosts from the sandbox if the sandbox is specified
	var sourceHosts []string
	for _, node := range execContext.nodesInfo {
		if node.State != util.NodeDownState && node.Sandbox == op.sandbox {
			sourceHosts = append(sourceHosts, node.Address)
		}
	}
	sourceHosts = util.SliceCommon(op.hosts, sourceHosts)
	if len(sourceHosts) == 0 {
		if op.sandbox == "" {
			return fmt.Errorf("[%s] cannot find any up hosts from source database %s", op.name, op.sourceDB)
		}
		return fmt.Errorf("[%s] cannot find any up hosts in the sandbox %s", op.name, op.sandbox)
	}

	op.hosts = []string{sourceHosts[0]}

	err := op.setupRequestBody(op.hosts)
	if err != nil {
		return err
	}
	execContext.dispatcher.setup(op.hosts)
	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *httpsStartReplicationOp) execute(execContext *opEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *httpsStartReplicationOp) processResult(_ *opEngineExecContext) error {
	var allErrs error

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.isUnauthorizedRequest() {
			// skip checking response from other nodes because we will get the same error there
			return result.err
		}
		if !result.isPassing() {
			allErrs = errors.Join(allErrs, result.err)
			continue
		}

		// decode the json-format response
		// The successful response object will be a dictionary as below:
		// {"detail": "REPLICATE"}
		startRepRsp, err := op.parseAndCheckMapResponse(host, result.content)
		if err != nil {
			err = fmt.Errorf("[%s] fail to parse result on host %s, details: %w", op.name, host, err)
			allErrs = errors.Join(allErrs, err)
			continue
		}

		// verify if the response's content is correct
		const startReplicationOpSuccMsg = "REPLICATE"
		if startRepRsp["detail"] != startReplicationOpSuccMsg {
			err = fmt.Errorf(`[%s] response detail should be '%s' but got '%s'`, op.name, startReplicationOpSuccMsg, startRepRsp["detail"])
			allErrs = errors.Join(allErrs, err)
		}
	}

	return allErrs
}

func (op *httpsStartReplicationOp) finalize(_ *opEngineExecContext) error {
	return nil
}
