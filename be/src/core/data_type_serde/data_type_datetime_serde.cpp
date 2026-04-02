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

#include "core/data_type_serde/data_type_datetime_serde.h"

#include "common/status.h"
#include "core/column/column_const.h"
#include "core/value/vdatetime_value.h"
#include "util/io_helper.h"

namespace doris {
#include "common/compile_check_begin.h"

Status DataTypeDateTimeSerDe::serialize_column_to_json(const IColumn& column, int64_t start_idx,
                                                       int64_t end_idx, BufferWritable& bw,
                                                       FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

Status DataTypeDateTimeSerDe::serialize_one_cell_to_json(const IColumn& column, int64_t row_num,
                                                         BufferWritable& bw,
                                                         FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    auto value = assert_cast<const ColumnDateTime&>(*ptr).get_element(row_num);
    if (_nesting_level > 1) {
        bw.write('"');
    }

    char buf[64];
    char* pos = value.to_string(buf);
    bw.write(buf, pos - buf - 1);
    if (_nesting_level > 1) {
        bw.write('"');
    }
    return Status::OK();
}

Status DataTypeDateTimeSerDe::deserialize_column_from_json_vector(
        IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
        const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR()
    return Status::OK();
}

Status DataTypeDateTimeSerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                             const FormatOptions& options) const {
    auto& column_data = assert_cast<ColumnDateTime&>(column);
    if (_nesting_level > 1) {
        slice.trim_quote();
    }
    VecDateTimeValue val;
    if (StringRef str(slice.data, slice.size); !read_datetime_text_impl(val, str)) {
        return Status::InvalidArgument("parse datetime fail, string: '{}'", str.to_string());
    }
    column_data.insert_value(val);
    return Status::OK();
}

Status DataTypeDateTimeSerDe::read_column_from_arrow(IColumn& column,
                                                     const arrow::Array* arrow_array, int64_t start,
                                                     int64_t end,
                                                     const cctz::time_zone& ctz) const {
    return _read_column_from_arrow<false>(column, arrow_array, start, end, ctz);
}

} // namespace doris
