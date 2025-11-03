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

#include <sys/un.h>

#include <array>
#include <cstdint>
#include <string>

namespace doris {

// Classless Inter-Domain Routing
class CIDR {
public:
    void reset();
    bool reset(const std::string& cidr_str);
    bool contains(const CIDR& ip) const;

private:
    bool contains(uint32_t ip_int);
    sa_family_t _family;
    std::array<std::uint8_t, 16> _address;
    std::uint8_t _netmask_len;
};

} // end namespace doris
