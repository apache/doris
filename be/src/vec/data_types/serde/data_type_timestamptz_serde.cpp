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

#include "data_type_timestamptz_serde.h"

#include <arrow/builder.h>

#include "runtime/primitive_type.h"
#include "vec/functions/cast/cast_parameters.h"
#include "vec/functions/cast/cast_to_string.h"
#include "vec/functions/cast/cast_to_timestamptz.h"
#include "vec/runtime/timestamptz_value.h"
namespace doris::vectorized {

// The implementation of these functions mainly refers to data_type_datetimev2_serde.cpp

Status DataTypeTimeStampTzSerDe::from_string(StringRef& str, IColumn& column,
                                             const FormatOptions& options) const {
    auto& col_data = assert_cast<ColumnTimeStampTz&>(column);

    CastParameters params {.status = Status::OK(), .is_strict = false};

    TimestampTzValue res;

    if (!CastToTimstampTz::from_string(str, res, params, options.timezone, _scale)) [[unlikely]] {
        return Status::InvalidArgument("parse timestamptz fail, string: '{}'", str.to_string());
    }
    col_data.insert_value(res);
    return Status::OK();
}

Status DataTypeTimeStampTzSerDe::from_olap_string(const std::string& str, Field& field,
                                                  const FormatOptions& options) const {
    CastParameters params {.status = Status::OK(), .is_strict = false};

    TimestampTzValue res;

    if (!CastToTimstampTz::from_string(StringRef(str), res, params, options.timezone, _scale))
            [[unlikely]] {
        return Status::InvalidArgument("parse timestamptz fail, string: '{}'", str);
    }
    field = Field::create_field<TYPE_TIMESTAMPTZ>(std::move(res));
    return Status::OK();
}

Status DataTypeTimeStampTzSerDe::from_string_batch(const ColumnString& col_str,
                                                   ColumnNullable& col_res,
                                                   const FormatOptions& options) const {
    auto& col_data = assert_cast<ColumnTimeStampTz&>(col_res.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(col_res.get_null_map_column());
    size_t row = col_str.size();
    col_res.resize(row);

    CastParameters params {.status = Status::OK(), .is_strict = false};
    for (size_t i = 0; i < row; ++i) {
        auto str = col_str.get_data_at(i);
        TimestampTzValue res;
        if (!CastToTimstampTz::from_string(str, res, params, options.timezone, _scale))
                [[unlikely]] {
            col_nullmap.get_data()[i] = true;
            col_data.get_data()[i] = TimestampTzValue(TimestampTzValue::default_column_value());
        } else {
            col_nullmap.get_data()[i] = false;
            col_data.get_data()[i] = res;
        }
    }
    return Status::OK();
}

Status DataTypeTimeStampTzSerDe::from_string_strict_mode(StringRef& str, IColumn& column,
                                                         const FormatOptions& options) const {
    auto& col_data = assert_cast<ColumnTimeStampTz&>(column);

    CastParameters params {.status = Status::OK(), .is_strict = true};

    TimestampTzValue res;
    CastToTimstampTz::from_string(str, res, params, options.timezone, _scale);

    if (!params.status.ok()) [[unlikely]] {
        params.status.prepend(
                fmt::format("parse {} to timestamptz failed: ", str.to_string_view()));
        return params.status;
    }
    col_data.insert_value(res);
    return Status::OK();
}

Status DataTypeTimeStampTzSerDe::from_string_strict_mode_batch(
        const ColumnString& col_str, IColumn& col_res, const FormatOptions& options,
        const NullMap::value_type* null_map) const {
    size_t row = col_str.size();
    col_res.resize(row);
    auto& col_data = assert_cast<ColumnTimeStampTz&>(col_res);

    CastParameters params {.status = Status::OK(), .is_strict = true};
    for (size_t i = 0; i < row; ++i) {
        if (null_map && null_map[i]) {
            continue;
        }
        auto str = col_str.get_data_at(i);
        TimestampTzValue res;
        CastToTimstampTz::from_string(str, res, params, options.timezone, _scale);
        // only after we called something with `IS_STRICT = true`, params.status will be set
        if (!params.status.ok()) [[unlikely]] {
            params.status.prepend(
                    fmt::format("parse {} to timestamptz failed: ", str.to_string_view()));
            return params.status;
        }

        col_data.get_data()[i] = res;
    }
    return Status::OK();
}

Status DataTypeTimeStampTzSerDe::serialize_column_to_json(const IColumn& column, int64_t start_idx,
                                                          int64_t end_idx, BufferWritable& bw,
                                                          FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

Status DataTypeTimeStampTzSerDe::serialize_one_cell_to_json(const IColumn& column, int64_t row_num,
                                                            BufferWritable& bw,
                                                            FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;
    if (_nesting_level > 1) {
        bw.write('"');
    }
    auto val = assert_cast<const ColumnTimeStampTz&, TypeCheckOnRelease::DISABLE>(*ptr).get_element(
            row_num);

    auto str = val.to_string(*options.timezone, _scale);
    bw.write(str.data(), str.size());

    if (_nesting_level > 1) {
        bw.write('"');
    }
    return Status::OK();
}

Status DataTypeTimeStampTzSerDe::deserialize_column_from_json_vector(
        IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
        const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR();
    return Status::OK();
}
Status DataTypeTimeStampTzSerDe::deserialize_one_cell_from_json(
        IColumn& column, Slice& slice, const FormatOptions& options) const {
    auto& column_data = assert_cast<ColumnTimeStampTz&, TypeCheckOnRelease::DISABLE>(column);
    if (_nesting_level > 1) {
        slice.trim_quote();
    }
    CastParameters params {.status = Status::OK(), .is_strict = false};
    TimestampTzValue res;

    if (StringRef str(slice.data, slice.size);
        !CastToTimstampTz::from_string(str, res, params, options.timezone, _scale)) {
        return Status::InvalidArgument("parse timestamptz fail, string: '{}'", str.to_string());
    }
    column_data.insert_value(res);
    return Status::OK();
}

Status DataTypeTimeStampTzSerDe::write_column_to_mysql_binary(const IColumn& column,
                                                              MysqlRowBinaryBuffer& result,
                                                              int64_t row_idx, bool col_const,
                                                              const FormatOptions& options) const {
    const auto& data = assert_cast<const ColumnTimeStampTz&>(column).get_data();
    const auto col_index = index_check_const(row_idx, col_const);
    auto val = data[col_index];
    if (UNLIKELY(0 != result.push_timestamptz(val, *options.timezone, _scale))) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    return Status::OK();
}

Status DataTypeTimeStampTzSerDe::write_column_to_arrow(const IColumn& column,
                                                       const NullMap* null_map,
                                                       arrow::ArrayBuilder* array_builder,
                                                       int64_t start, int64_t end,
                                                       const cctz::time_zone& ctz) const {
    const auto& col_data = assert_cast<const ColumnTimeStampTz&>(column).get_data();
    auto& timestamp_builder = assert_cast<arrow::TimestampBuilder&>(*array_builder);
    std::shared_ptr<arrow::TimestampType> timestamp_type =
            std::static_pointer_cast<arrow::TimestampType>(array_builder->type());
    static const auto& UTC = cctz::utc_time_zone();
    for (size_t i = start; i < end; ++i) {
        if (null_map && (*null_map)[i]) {
            RETURN_IF_ERROR(checkArrowStatus(timestamp_builder.AppendNull(), column.get_name(),
                                             array_builder->type()->name()));
        } else {
            int64_t timestamp = 0;
            const auto& tz = col_data[i];
            tz.unix_timestamp(&timestamp, UTC);

            if (_scale > 3) {
                uint32_t microsecond = tz.microsecond();
                timestamp = (timestamp * 1000000) + microsecond;
            } else if (_scale > 0) {
                uint32_t millisecond = tz.microsecond() / 1000;
                timestamp = (timestamp * 1000) + millisecond;
            }
            RETURN_IF_ERROR(checkArrowStatus(timestamp_builder.Append(timestamp), column.get_name(),
                                             array_builder->type()->name()));
        }
    }
    return Status::OK();
}

Status DataTypeTimeStampTzSerDe::write_column_to_orc(const std::string& timezone,
                                                     const IColumn& column, const NullMap* null_map,
                                                     orc::ColumnVectorBatch* orc_col_batch,
                                                     int64_t start, int64_t end,
                                                     vectorized::Arena& arena,
                                                     const FormatOptions& options) const {
    const auto& col_data = assert_cast<const ColumnTimeStampTz&>(column).get_data();
    auto* cur_batch = dynamic_cast<orc::TimestampVectorBatch*>(orc_col_batch);
    static const int64_t micro_to_nano_second = 1000;
    static const auto& UTC = cctz::utc_time_zone();
    for (size_t row_id = start; row_id < end; row_id++) {
        if (cur_batch->notNull[row_id] == 0) {
            continue;
        }

        int64_t timestamp = 0;
        col_data[row_id].unix_timestamp(&timestamp, UTC);

        cur_batch->data[row_id] = timestamp;
        cur_batch->nanoseconds[row_id] = col_data[row_id].microsecond() * micro_to_nano_second;
    }
    cur_batch->numElements = end - start;
    return Status::OK();
}

std::string DataTypeTimeStampTzSerDe::to_olap_string(const vectorized::Field& field) const {
    return CastToString::from_timestamptz(field.get<TYPE_TIMESTAMPTZ>(), 6);
}

} // namespace doris::vectorized
