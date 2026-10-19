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

#include "doris_txn.h"

#include <array>
#include <bit>
#include <cstring>

#include "meta-store/versionstamp.h"

namespace doris::cloud {

int get_txn_id_from_fdb_ts(std::string_view fdb_vts, int64_t* txn_id) {
    if (fdb_vts.size() != 10) return 1; // Malformed version timestamp

    static_assert(std::endian::native == std::endian::little);
    // Copy the possibly unaligned input before decoding its big-endian fields.
    std::array<uint8_t, 10> bytes;
    std::memcpy(bytes.data(), fdb_vts.data(), bytes.size());
    const Versionstamp versionstamp(bytes);
    uint64_t ver = versionstamp.version();
    uint16_t seq = versionstamp.order();

    // CAUTION: DO NOT EVER TOUCH IT!!! UNLESS YOU ARE PREPARED FOR THE DOOM!!!
    // CAUTION: DO NOT EVER TOUCH IT!!! UNLESS YOU ARE PREPARED FOR THE DOOM!!!
    // CAUTION: DO NOT EVER TOUCH IT!!! UNLESS YOU ARE PREPARED FOR THE DOOM!!!
    static constexpr int SEQ_RETAIN_BITS = 10;

    if (seq >= (1L << SEQ_RETAIN_BITS)) {
        // seq exceeds the max value
        return 2;
    }

    // Squeeze seq into ver
    seq &= ((1L << SEQ_RETAIN_BITS) - 1L); // Strip off uninterested part
    ver <<= SEQ_RETAIN_BITS;
    ver |= seq;

    *txn_id = static_cast<int64_t>(ver);
    return 0;
}

} // namespace doris::cloud
