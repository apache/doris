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

#pragma once

#include <string>

namespace doris::cloud {

/**
 * Gets local IP with give CIDR mask.
 * * If the priority_networks is empty, the IP binded to hostname is returned
 * * With a non-empty priority_networks, a matching non-loopback address is preferred. A matching
 *   loopback is used only when no non-loopback address matches; if no CIDR matches, a loopback
 *   fallback is returned when available.
 * Both IPv4 and IPv6 CIDRs are supported. When both loopback families are present, the IPv4
 * loopback is preferred as the fallback.
 *
 * @param priority_networks CIDR mask, can be empty
 * @return the IP string
 */
std::string get_local_ip(const std::string& priority_networks);

/**
 * Formats "host:port", bracketing the host when it is an IPv6 literal ("[fd00::1]:5000"), as
 * required by brpc endpoint parsing.
 */
std::string get_host_port(const std::string& host, int port);

/**
 * Wildcard bind address for the brpc server: "[::0]" if get_local_ip() selects an IPv6 address
 * for these networks, "0.0.0.0" otherwise.
 */
std::string get_service_bind_address(const std::string& priority_networks);

} // namespace doris::cloud
