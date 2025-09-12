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

#include "exec/olap_common.h"

#include <utility>
#include <vector>

#include "exec/olap_utils.h"

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

Status OlapScanKeys::get_key_range(std::vector<std::unique_ptr<OlapScanRange>>* key_range) {
    key_range->clear();

    for (int i = 0; i < _begin_scan_keys.size(); ++i) {
        std::unique_ptr<OlapScanRange> range(new OlapScanRange());
        range->begin_scan_range = _begin_scan_keys[i];
        range->end_scan_range = _end_scan_keys[i];
        range->begin_include = _begin_include;
        range->end_include = _end_include;
        key_range->emplace_back(std::move(range));
    }

    return Status::OK();
}

} // namespace doris

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
