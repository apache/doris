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

#include "util/uid_util.h"

namespace doris {

std::ostream& operator<<(std::ostream& os, const UniqueId& uid) {
    os << uid.to_string();
    return os;
}

std::string print_id(const TUniqueId& id) {
    std::stringstream out;
    out << std::hex << id.hi << "-" << id.lo;
    return out.str();
}

std::string print_id(const PUniqueId& id) {
    std::stringstream out;
    out << std::hex << id.hi() << "-" << id.lo();
    return out.str();
}

bool parse_id(const std::string& s, TUniqueId* id) {
    DCHECK(id != nullptr);

    const char* hi_part = s.c_str();
    char* colon = const_cast<char*>(strchr(hi_part, '-'));

    if (colon == nullptr) {
        return false;
    }

    const char* lo_part = colon + 1;
    *colon = '\0';

    char* error_hi = nullptr;
    char* error_lo = nullptr;
    id->hi = strtoul(hi_part, &error_hi, 16);
    id->lo = strtoul(lo_part, &error_lo, 16);

    bool valid = *error_hi == '\0' && *error_lo == '\0';
    *colon = ':';
    return valid;
}

} // namespace doris
