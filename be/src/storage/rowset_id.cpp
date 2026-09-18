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

#include "storage/rowset_id.h"

#include <charconv>
#include <ostream>
#include <system_error>

#include "common/cast_set.h"
#include "common/compiler_util.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "util/hash_util.hpp"
#include "util/uid_util.h"

namespace doris {

namespace {
constexpr int64_t MAX_ROWSET_ID = 1L << 56;
constexpr int64_t LOW_56_BITS = 0x00ffffffffffffff;
} // namespace

void RowsetId::init(std::string_view rowset_id_str) {
    // for new rowsetid its a 48 hex string
    // if the len < 48, then it is an old format rowset id
    if (rowset_id_str.length() < 48) [[unlikely]] {
        int64_t high;
        auto [_, ec] = std::from_chars(rowset_id_str.data(),
                                       rowset_id_str.data() + rowset_id_str.length(), high);
        if (ec != std::errc {}) [[unlikely]] {
            if (config::force_regenerate_rowsetid_on_start_error) {
                LOG(WARNING) << "failed to init rowset id: " << rowset_id_str;
                high = MAX_ROWSET_ID - 1;
            } else {
                throw Exception(Status::FatalError("failed to init rowset id: {}", rowset_id_str));
            }
        }
        init(1, high, 0, 0);
    } else {
        int64_t high = 0;
        int64_t middle = 0;
        int64_t low = 0;
        from_hex(&high, rowset_id_str.substr(0, 16));
        from_hex(&middle, rowset_id_str.substr(16, 16));
        from_hex(&low, rowset_id_str.substr(32, 16));
        init(high >> 56, high & LOW_56_BITS, middle, low);
    }
}

void RowsetId::init(int64_t rowset_id) {
    init(1, rowset_id, 0, 0);
}

void RowsetId::init(int64_t id_version, int64_t high, int64_t middle, int64_t low) {
    version = cast_set<int8_t>(id_version);
    if (UNLIKELY(high >= MAX_ROWSET_ID)) {
        throw Exception(Status::FatalError("inc rowsetid is too large:{}", high));
    }
    hi = (id_version << 56) + (high & LOW_56_BITS);
    mi = middle;
    lo = low;
}

std::string RowsetId::to_string() const {
    if (version < 2) {
        return std::to_string(hi & LOW_56_BITS);
    } else {
        char buf[48];
        to_hex(hi, buf);
        to_hex(mi, buf + 16);
        to_hex(lo, buf + 32);
        return {buf, 48};
    }
}

std::ostream& operator<<(std::ostream& out, const RowsetId& rowset_id) {
    out << rowset_id.to_string();
    return out;
}

} // namespace doris

size_t std::hash<doris::RowsetId>::operator()(const doris::RowsetId& rowset_id) const {
    size_t seed = 0;
    seed = doris::HashUtil::xxHash64WithSeed((const char*)&rowset_id.hi, sizeof(rowset_id.hi),
                                             seed);
    seed = doris::HashUtil::xxHash64WithSeed((const char*)&rowset_id.mi, sizeof(rowset_id.mi),
                                             seed);
    seed = doris::HashUtil::xxHash64WithSeed((const char*)&rowset_id.lo, sizeof(rowset_id.lo),
                                             seed);
    return seed;
}
