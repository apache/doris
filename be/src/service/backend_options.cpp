// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "service/backend_options.h"

#include "common/logging.h"
#include "common/status.h"
#include "util/network_util.h"

namespace palo {

std::string BackendOptions::_localhost;

void BackendOptions::init() {
    std::vector<InetAddress> hosts;
    Status status = get_hosts_v4(&hosts);

    if (!status.ok()) {
        LOG(FATAL) << status.get_error_msg();
    }

    if (hosts.empty()) {
        LOG(FATAL) << "failed to get host";
    }

    std::string loopback;
    std::vector<InetAddress>::iterator addr_it = hosts.begin();
    for (; addr_it != hosts.end(); ++addr_it) {
        if ((*addr_it).is_address_v4()) {
            if ((*addr_it).is_loopback_v4()) {
                loopback = (*addr_it).get_host_address_v4();
            } else {
                _localhost = (*addr_it).get_host_address_v4();
                break;
            }
        }
    }

    if (_localhost.empty()) {
        _localhost = loopback;
    }
}

std::string BackendOptions::get_localhost() {
    return _localhost;
}

}
