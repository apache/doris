// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
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

#ifndef DORIS_BE_SRC_COMMON_UTIL_NETWORK_UTIL_H
#define DORIS_BE_SRC_COMMON_UTIL_NETWORK_UTIL_H

#include <vector>

#include "common/status.h"
#include "gen_cpp/Types_types.h"

namespace doris {

//TODO: ipv6
class InetAddress {
public:
    InetAddress(struct sockaddr* addr);
    bool is_address_v4() const;
    bool is_loopback_v4();
    std::string get_host_address_v4();

private:
    struct sockaddr_in addr;
};

// Looks up all IP addresses associated with a given hostname. Returns
// an error status if any system call failed, otherwise OK. Even if OK
// is returned, addresses may still be of zero length.
Status hostname_to_ip_addrs(const std::string& name, std::vector<std::string>* addresses);

// Finds the first non-localhost IP address in the given list. Returns
// true if such an address was found, false otherwise.
bool find_first_non_localhost(const std::vector<std::string>& addresses, std::string* addr);

// Sets the output argument to the system defined hostname.
// Returns OK if a hostname can be found, false otherwise.
Status get_hostname(std::string* hostname);

Status get_hosts_v4(std::vector<InetAddress>* hosts);

// Utility method because Thrift does not supply useful constructors
TNetworkAddress make_network_address(const std::string& hostname, int port);

Status get_inet_interfaces(std::vector<std::string>* interfaces, bool include_ipv6 = false);

} // namespace doris

#endif
