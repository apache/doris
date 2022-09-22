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
#include "runtime/large_int_value.h"

namespace doris {

template <>
void ColumnValueRange<PrimitiveType::TYPE_STRING>::convert_to_fixed_value() {
    return;
}

template <>
void ColumnValueRange<PrimitiveType::TYPE_CHAR>::convert_to_fixed_value() {
    return;
}

template <>
void ColumnValueRange<PrimitiveType::TYPE_VARCHAR>::convert_to_fixed_value() {
    return;
}

template <>
void ColumnValueRange<PrimitiveType::TYPE_HLL>::convert_to_fixed_value() {
    return;
}

template <>
void ColumnValueRange<PrimitiveType::TYPE_DECIMALV2>::convert_to_fixed_value() {
    return;
}

template <>
void ColumnValueRange<PrimitiveType::TYPE_LARGEINT>::convert_to_fixed_value() {
    return;
}

template <>
std::vector<ColumnValueRange<PrimitiveType::TYPE_STRING>>
ColumnValueRange<PrimitiveType::TYPE_STRING>::split(size_t count) {
    __builtin_unreachable();
}

template <>
std::vector<ColumnValueRange<PrimitiveType::TYPE_CHAR>>
ColumnValueRange<PrimitiveType::TYPE_CHAR>::split(size_t count) {
    __builtin_unreachable();
}

template <>
std::vector<ColumnValueRange<PrimitiveType::TYPE_VARCHAR>>
ColumnValueRange<PrimitiveType::TYPE_VARCHAR>::split(size_t count) {
    __builtin_unreachable();
}

template <>
std::vector<ColumnValueRange<PrimitiveType::TYPE_HLL>>
ColumnValueRange<PrimitiveType::TYPE_HLL>::split(size_t count) {
    __builtin_unreachable();
}

template <>
std::vector<ColumnValueRange<PrimitiveType::TYPE_DECIMALV2>>
ColumnValueRange<PrimitiveType::TYPE_DECIMALV2>::split(size_t count) {
    __builtin_unreachable();
}

template <>
std::vector<ColumnValueRange<PrimitiveType::TYPE_LARGEINT>>
ColumnValueRange<PrimitiveType::TYPE_LARGEINT>::split(size_t count) {
    __builtin_unreachable();
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

Status OlapScanKeys::extend_scan_splitted_keys(std::vector<ColumnValueRangeType>& ranges) {
    using namespace std;
    DCHECK(!_has_range_value);

    std::vector<OlapTuple> new_begin_keys;
    std::vector<OlapTuple> new_end_keys;
    for (size_t i = 0; i != ranges.size(); ++i) {
        std::visit(
                [&](auto&& range) {
                    using RangeType = std::decay_t<decltype(range)>;
                    using CppType = typename RangeType::CppType;
                    auto begin_keys = _begin_scan_keys;
                    auto end_keys = _end_scan_keys;
                    if (begin_keys.empty()) {
                        begin_keys.emplace_back();
                        begin_keys.back().add_value(
                                cast_to_string<RangeType::Type, CppType>(
                                        range.get_range_min_value(), range.scale()),
                                range.contain_null());
                        end_keys.emplace_back();
                        end_keys.back().add_value(cast_to_string<RangeType::Type, CppType>(
                                range.get_range_max_value(), range.scale()));
                    } else {
                        for (int i = 0; i < begin_keys.size(); ++i) {
                            begin_keys[i].add_value(
                                    cast_to_string<RangeType::Type, CppType>(
                                            range.get_range_min_value(), range.scale()),
                                    range.contain_null());
                        }

                        for (int i = 0; i < end_keys.size(); ++i) {
                            end_keys[i].add_value(cast_to_string<RangeType::Type, CppType>(
                                    range.get_range_max_value(), range.scale()));
                        }
                    }
                    new_begin_keys.insert(new_begin_keys.end(), begin_keys.begin(),
                                          begin_keys.end());
                    new_end_keys.insert(new_end_keys.end(), end_keys.begin(), end_keys.end());
                },
                ranges[i]);
    }
    _begin_scan_keys = new_begin_keys;
    _end_scan_keys = new_end_keys;
    return Status::OK();
}

OlapScanKeys OlapScanKeys::merge(size_t to_ranges_count) {
    OlapScanKeys merged;
    merged.set_is_convertible(_is_convertible);
    merged.set_max_scan_key_num(_max_scan_key_num);
    bool exact_value = false;
    for (size_t i = 0; i != _column_ranges.size(); ++i) {
        std::visit(
                [&](auto&& range) {
                    if (i == _index_of_max_size_range) {
                        return;
                    }
                    merged.extend_scan_key(range, &exact_value);
                },
                _column_ranges[i]);
    }

    size_t size_of_ranges = std::max(size_t(1), merged.size());
    size_t split_to_count = (to_ranges_count + size_of_ranges - 1) / size_of_ranges;
    std::vector<ColumnValueRangeType> splitted = std::visit(
            [&](auto&& range) {
                auto splitted = range.split(split_to_count);
                std::vector<ColumnValueRangeType> splitted_variants;
                splitted_variants.assign(splitted.cbegin(), splitted.cend());
                return splitted_variants;
            },
            _column_ranges[_index_of_max_size_range]);

    merged.extend_scan_splitted_keys(splitted);
    return merged;
}

} // namespace doris

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
