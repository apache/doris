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

#include <fmt/compile.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>

#include <cstdlib>

#include "util/hash_util.hpp"

namespace doris {

size_t UniqueId::hash(size_t seed) const {
    return doris::HashUtil::hash(this, sizeof(*this), seed);
}

std::size_t hash_value(const doris::TUniqueId& id) {
    std::size_t seed = 0;
    HashUtil::hash_combine(seed, id.lo);
    HashUtil::hash_combine(seed, id.hi);
    return seed;
}

std::ostream& operator<<(std::ostream& os, const UniqueId& uid) {
    os << uid.to_string();
    return os;
}

std::string print_id(const TUniqueId& id) {
    return fmt::format(FMT_COMPILE("{:x}-{:x}"), static_cast<uint64_t>(id.hi),
                       static_cast<uint64_t>(id.lo));
}

std::string print_id(const PUniqueId& id) {
    return fmt::format(FMT_COMPILE("{:x}-{:x}"), static_cast<uint64_t>(id.hi()),
                       static_cast<uint64_t>(id.lo()));
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
