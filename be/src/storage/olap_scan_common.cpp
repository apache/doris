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

#include "storage/olap_scan_common.h"

#include <utility>
#include <vector>

#include "storage/olap_utils.h"

namespace doris {
template <>
const typename ColumnValueRange<TYPE_FLOAT>::CppType ColumnValueRange<TYPE_FLOAT>::TYPE_MIN =
        -std::numeric_limits<float>::infinity();
template <>
const typename ColumnValueRange<TYPE_FLOAT>::CppType ColumnValueRange<TYPE_FLOAT>::TYPE_MAX =
        std::numeric_limits<float>::quiet_NaN();
template <>
const typename ColumnValueRange<TYPE_DOUBLE>::CppType ColumnValueRange<TYPE_DOUBLE>::TYPE_MIN =
        -std::numeric_limits<double>::infinity();
template <>
const typename ColumnValueRange<TYPE_DOUBLE>::CppType ColumnValueRange<TYPE_DOUBLE>::TYPE_MAX =
        std::numeric_limits<double>::quiet_NaN();

/// Convert the internal scan key pairs (_begin_scan_keys / _end_scan_keys)
/// into a vector of OlapScanRange objects that the scanner / tablet reader can consume.
///
/// Each pair (_begin_scan_keys[i], _end_scan_keys[i]) becomes one OlapScanRange with
/// has_lower_bound = true and has_upper_bound = true, because these tuples always
/// carry real typed boundary values produced by extend_scan_key().
///
/// If _begin_scan_keys is empty (no key predicates were pushed down), the caller
/// (_init_scanners) will create a single default-constructed OlapScanRange with
/// has_lower_bound = false / has_upper_bound = false to represent a full table scan.
///
/// Example – two scan key pairs from "k1 IN (1, 2) AND k2 = 10":
///   _begin_scan_keys = [ (1, 10),  (2, 10)  ]
///   _end_scan_keys   = [ (1, 10),  (2, 10)  ]
///   => two OlapScanRange objects, both with has_lower_bound=true, has_upper_bound=true.
Status OlapScanKeys::get_key_range(std::vector<std::unique_ptr<OlapScanRange>>* key_range) {
    key_range->clear();

    for (int i = 0; i < _begin_scan_keys.size(); ++i) {
        std::unique_ptr<OlapScanRange> range(new OlapScanRange());
        range->begin_scan_range = _begin_scan_keys[i];
        range->end_scan_range = _end_scan_keys[i];
        range->has_lower_bound = true;
        range->has_upper_bound = true;
        range->begin_include = _begin_include;
        range->end_include = _end_include;
        key_range->emplace_back(std::move(range));
    }

    return Status::OK();
}

} // namespace doris

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
