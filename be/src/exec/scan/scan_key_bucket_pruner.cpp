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

#include "exec/scan/scan_key_bucket_pruner.h"

#include "storage/olap_utils.h"
#include "util/hash_util.hpp"

namespace doris {

bool ScanKeyBucketPruner::init(const std::vector<std::unique_ptr<OlapScanRange>>& ranges,
                               int32_t bucket_num) {
    DORIS_CHECK_GT(bucket_num, 0);
    _bucket_ranges.clear();
    for (const auto& range : ranges) {
        if (!range->has_lower_bound || !range->has_upper_bound || !range->begin_include ||
            !range->end_include || range->begin_scan_range.size() != 1 ||
            range->end_scan_range.size() != 1) {
            return false;
        }
        const auto& begin = range->begin_scan_range.get_field(0);
        const auto& end = range->end_scan_range.get_field(0);
        if (begin.get_type() != TYPE_VARCHAR || end.get_type() != TYPE_VARCHAR || begin != end) {
            return false;
        }
    }
    _bucket_ranges.resize(bucket_num);
    for (const auto& range : ranges) {
        const auto& value = range->begin_scan_range.get_field(0).get<TYPE_VARCHAR>();
        // Identical to ColumnString::update_crcs_with_value for a single non-null
        // VARCHAR distribution column: raw bytes, seed zero, and the full bucket count.
        const uint32_t hash =
                HashUtil::zlib_crc_hash(value.data(), static_cast<uint32_t>(value.size()), 0);
        _bucket_ranges[hash % bucket_num].push_back(range.get());
    }
    return true;
}

const std::vector<OlapScanRange*>& ScanKeyBucketPruner::ranges_for_bucket(
        int32_t bucket_seq) const {
    DCHECK_GE(bucket_seq, 0);
    DCHECK_LT(bucket_seq, _bucket_ranges.size());
    return _bucket_ranges[bucket_seq];
}

} // namespace doris
