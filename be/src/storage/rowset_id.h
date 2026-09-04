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

#include <cstdint>
#include <functional>
#include <iosfwd>
#include <string>
#include <string_view>

// Deliberately kept dependency-free (no config/exception/logging/hex utils):
// this header is included by core/column/column.h and therefore by nearly
// every TU. The method bodies that need those facilities live in
// storage/rowset_id.cpp.

namespace doris {

// 8 bit rowset id version
// 56 bit, inc number from 1
// 128 bit backend uid, it is a uuid bit, id version
struct RowsetId {
    int8_t version = 0;
    int64_t hi = 0;
    int64_t mi = 0;
    int64_t lo = 0;

    void init(std::string_view rowset_id_str);

    // to compatible with old version
    void init(int64_t rowset_id);

    void init(int64_t id_version, int64_t high, int64_t middle, int64_t low);

    std::string to_string() const;

    // std::unordered_map need this api
    bool operator==(const RowsetId& rhs) const {
        return hi == rhs.hi && mi == rhs.mi && lo == rhs.lo;
    }

    bool operator!=(const RowsetId& rhs) const {
        return hi != rhs.hi || mi != rhs.mi || lo != rhs.lo;
    }

    bool operator<(const RowsetId& rhs) const {
        if (hi != rhs.hi) {
            return hi < rhs.hi;
        } else if (mi != rhs.mi) {
            return mi < rhs.mi;
        } else {
            return lo < rhs.lo;
        }
    }
};

std::ostream& operator<<(std::ostream& out, const RowsetId& rowset_id);

} // namespace doris

// This intended to be a "good" hash function.  It may change from time to time.
template <>
struct std::hash<doris::RowsetId> {
    size_t operator()(const doris::RowsetId& rowset_id) const;
};
