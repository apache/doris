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

#include <mutex>
#include <string>

namespace doris::cloud {

/**
 * Process-lifetime local network identity.
 *
 * The selected address must be stable for the lifetime of the process: the BRPC listener is
 * created once, while the meta service registry is renewed periodically. Selecting the address
 * independently in those paths can publish an endpoint whose address family is not served by the
 * listener.
 *
 * init() should be called once before starting network services. Accessors lazily initialize from
 * config::priority_networks so tests and legacy callers that do not call init() keep working.
 */
class NetworkOptions {
public:
    NetworkOptions() = default;
    NetworkOptions(const NetworkOptions&) = delete;
    NetworkOptions& operator=(const NetworkOptions&) = delete;

    void init(const std::string& priority_networks);

    const std::string& local_ip() const;
    bool is_ipv6() const;
    const std::string& bind_address() const;
    std::string host_port(int port) const;

private:
    void ensure_initialized() const;
    void initialize(const std::string& priority_networks) const;

    mutable std::once_flag init_once_;
    mutable std::string local_ip_;
    mutable bool is_ipv6_ = false;
    mutable std::string bind_address_;
};

NetworkOptions& network_options();

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
 * Low-level wildcard bind address helper for callers that need to derive a bind address from an
 * explicit priority_networks value. Production services should use network_options().bind_address()
 * so the listener and registry share the same process-lifetime address selection.
 */
std::string get_service_bind_address(const std::string& priority_networks);

} // namespace doris::cloud
