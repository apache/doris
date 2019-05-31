// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

namespace cpp doris
namespace java org.apache.doris.thrift

include "Status.thrift"
include "Types.thrift"

struct TMasterInfo {
    1: required Types.TNetworkAddress network_address
    2: required Types.TClusterId cluster_id
    3: required Types.TEpoch epoch
    4: optional string token 
    5: optional string backend_ip
    6: optional Types.TPort http_port
}

struct TBackendInfo {
    1: required Types.TPort be_port
    2: required Types.TPort http_port
    3: optional Types.TPort be_rpc_port
    4: optional Types.TPort brpc_port
}

struct THeartbeatResult {
    1: required Status.TStatus status 
    2: required TBackendInfo backend_info
}

service HeartbeatService {
    THeartbeatResult heartbeat(1:TMasterInfo master_info);
}
