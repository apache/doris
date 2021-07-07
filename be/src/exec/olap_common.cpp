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

#include <boost/lexical_cast.hpp>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "exec/olap_utils.h"

namespace doris {

template <>
std::string cast_to_string(__int128 value) {
    std::stringstream ss;
    ss << value;
    return ss.str();
}

template <>
std::string cast_to_string(int8_t value) {
    return std::to_string(static_cast<int>(value));
}

template <>
void ColumnValueRange<StringValue>::convert_to_fixed_value() {
    return;
}

template <>
void ColumnValueRange<DecimalV2Value>::convert_to_fixed_value() {
    return;
}

template <>
void ColumnValueRange<__int128>::convert_to_fixed_value() {
    return;
}

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
