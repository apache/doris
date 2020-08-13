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

#ifndef DORIS_BE_SRC_COMMON_UTIL_CIDR_H
#define DORIS_BE_SRC_COMMON_UTIL_CIDR_H

#include <string>

namespace doris {

// Classless Inter-Domain Routing
class CIDR {
public:
    CIDR();
    void reset();
    bool reset(const std::string& cidr_str);
    bool contains(const std::string& ip);

private:
    bool ip_to_int(const std::string& ip_str, uint32_t* value);
    bool contains(uint32_t ip_int);

    uint32_t _address;
    uint32_t _netmask;
};

} // end namespace doris

#endif // DORIS_BE_SRC_COMMON_UTIL_CIDR_H
