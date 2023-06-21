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

type VClusterDatabaseOptions struct {
	// part 1: basic db info
	Name            *string
	RawHosts        []string // expected to be IP addresses or hostnames
	Hosts           []string // expected to be IP addresses resolved from RawHosts
	Ipv6            *bool
	CatalogPrefix   *string
	DataPrefix      *string
	ConfigDirectory *string

	// part 2: eon db info
	DepotPrefix *string
	IsEon       *bool

	// part 3: auth info
	UserName string
	Password *string
	Key      string
	Cert     string
	CaCert   string

	// part 4: other info
	HonorUserInput *bool
}

// TODO validate basic options
