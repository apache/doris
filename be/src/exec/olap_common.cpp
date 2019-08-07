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
#include <string>
#include <utility>
#include <vector>
#include <sstream>

#include "exec/olap_utils.h"

namespace doris {

template<>
std::string cast_to_string(__int128 value) {
    std::stringstream ss;
    ss << value;
    return ss.str();
}

template<>
void ColumnValueRange<StringValue>::convert_to_fixed_value() {
    return;
}

template<>
void ColumnValueRange<DecimalValue>::convert_to_fixed_value() {
    return;
}

template<>
void ColumnValueRange<DecimalV2Value>::convert_to_fixed_value() {
    return;
}

template<>
void ColumnValueRange<__int128>::convert_to_fixed_value() {
    return;
}

Status OlapScanKeys::get_key_range(std::vector<OlapScanRange>* key_range) {
    key_range->clear();

    for (int i = 0; i < _begin_scan_keys.size(); ++i) {
        OlapScanRange range;
        range.begin_scan_range = _begin_scan_keys[i];
        range.end_scan_range = _end_scan_keys[i];
        range.begin_include = _begin_include;
        range.end_include = _end_include;
        key_range->push_back(range);
    }

    return Status::OK();
}

Status DorisScanRange::init() {
    if (!_scan_range.__isset.partition_column_ranges) {
        return Status::OK();
    }

    const std::vector<TKeyRange>& partition_column_ranges
        = _scan_range.partition_column_ranges;

    // TODO(hujie01): Only support first column's partition range
    for (int i = 0; i < partition_column_ranges.size() && i < 1; ++i) {
        switch (thrift_to_type(partition_column_ranges[i].column_type)) {
        case TYPE_TINYINT: {
            ColumnValueRange<int32_t> range(partition_column_ranges[i].column_name,
                                            thrift_to_type(partition_column_ranges[i].column_type),
                                            std::numeric_limits<int8_t>::min(),
                                            std::numeric_limits<int8_t>::max());
            int32_t begin_key = static_cast<int32_t>(partition_column_ranges[i].begin_key);
            int32_t end_key = static_cast<int32_t>(partition_column_ranges[i].end_key);
            range.add_range(FILTER_LARGER_OR_EQUAL, begin_key);
            range.add_range(FILTER_LESS_OR_EQUAL, end_key);

            if (!range.is_empty_value_range()) {
                _partition_column_range[partition_column_ranges[i].column_name] = range;
            }

            break;
        }

        case TYPE_SMALLINT: {
            ColumnValueRange<int16_t> range(partition_column_ranges[i].column_name,
                                            thrift_to_type(partition_column_ranges[i].column_type),
                                            std::numeric_limits<int16_t>::min(),
                                            std::numeric_limits<int16_t>::max());
            int32_t begin_key = static_cast<int32_t>(partition_column_ranges[i].begin_key);
            int32_t end_key = static_cast<int32_t>(partition_column_ranges[i].end_key);
            range.add_range(FILTER_LARGER_OR_EQUAL, begin_key);
            range.add_range(FILTER_LESS_OR_EQUAL, end_key);

            if (!range.is_empty_value_range()) {
                _partition_column_range[partition_column_ranges[i].column_name] = range;
            }

            break;
        }

        case TYPE_INT: {
            ColumnValueRange<int32_t> range(partition_column_ranges[i].column_name,
                                            thrift_to_type(partition_column_ranges[i].column_type),
                                            std::numeric_limits<int32_t>::min(),
                                            std::numeric_limits<int32_t>::max());
            int32_t begin_key = static_cast<int32_t>(partition_column_ranges[i].begin_key);
            int32_t end_key = static_cast<int32_t>(partition_column_ranges[i].end_key);
            range.add_range(FILTER_LARGER_OR_EQUAL, begin_key);
            range.add_range(FILTER_LESS_OR_EQUAL, end_key);

            if (!range.is_empty_value_range()) {
                _partition_column_range[partition_column_ranges[i].column_name] = range;
            }

            break;
        }

        case TYPE_BIGINT: {
            ColumnValueRange<int64_t> range(partition_column_ranges[i].column_name,
                                            thrift_to_type(partition_column_ranges[i].column_type),
                                            std::numeric_limits<int64_t>::min(),
                                            std::numeric_limits<int64_t>::max());
            int64_t begin_key = static_cast<int64_t>(partition_column_ranges[i].begin_key);
            int64_t end_key = static_cast<int64_t>(partition_column_ranges[i].end_key);
            range.add_range(FILTER_LARGER_OR_EQUAL, begin_key);
            range.add_range(FILTER_LESS_OR_EQUAL, end_key);

            if (!range.is_empty_value_range()) {
                _partition_column_range[partition_column_ranges[i].column_name] = range;
            }

            break;
        }

        case TYPE_VARCHAR:
        case TYPE_CHAR:
        case TYPE_DECIMAL:
        case TYPE_DECIMALV2:
        case TYPE_DATE:
        case TYPE_DATETIME:
            break;

        default:
            DCHECK(false) << "Unsupport Range Type!";
        }

        // Note: Only use first partition column to select ScanRange
        //       since only first partition column is ordered
        break;
    }

    return Status::OK();
}

int DorisScanRange::has_intersection(const std::string column_name,
                                   ColumnValueRangeType& value_range) {
    IsEmptyValueRangeVisitor empty_visitor;

    if (boost::apply_visitor(empty_visitor, value_range)) {
        return 0;
    }

    if (_partition_column_range.find(column_name) == _partition_column_range.end()) {
        return -1;
    }

    HasIntersectionVisitor visitor;

    if (boost::apply_visitor(visitor, _partition_column_range[column_name], value_range)) {
        return 1;
    } else {
        return 0;
    }
}

}  // namespace doris

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
