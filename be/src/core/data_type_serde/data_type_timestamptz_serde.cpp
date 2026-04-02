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

#include "core/data_type_serde/data_type_timestamptz_serde.h"

#include <arrow/array.h>
#include <arrow/builder.h>

#include "core/data_type/primitive_type.h"
#include "core/value/timestamptz_value.h"
#include "exprs/function/cast/cast_parameters.h"
#include "exprs/function/cast/cast_to_string.h"
#include "exprs/function/cast/cast_to_timestamptz.h"
#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"
namespace doris {

// The implementation of these functions mainly refers to data_type_datetimev2_serde.cpp

Status DataTypeTimeStampTzSerDe::from_string(StringRef& str, IColumn& column,
                                             const FormatOptions& options) const {
    auto& col_data = assert_cast<ColumnTimeStampTz&>(column);

    CastParameters params {.status = Status::OK(), .is_strict = false};

    TimestampTzValue res;

    if (!CastToTimestampTz::from_string(str, res, params, options.timezone, _scale)) [[unlikely]] {
        return Status::InvalidArgument("parse timestamptz fail, string: '{}'", str.to_string());
    }
    col_data.insert_value(res);
    return Status::OK();
}

Status DataTypeTimeStampTzSerDe::from_olap_string(const std::string& str, Field& field,
                                                  const FormatOptions& options) const {
    CastParameters params {.status = Status::OK(), .is_strict = false};

    TimestampTzValue res;

    if (!CastToTimestampTz::from_string(StringRef(str), res, params, options.timezone, _scale))
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
        if (!CastToTimestampTz::from_string(str, res, params, options.timezone, _scale))
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
    CastToTimestampTz::from_string(str, res, params, options.timezone, _scale);

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
        CastToTimestampTz::from_string(str, res, params, options.timezone, _scale);
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
        !CastToTimestampTz::from_string(str, res, params, options.timezone, _scale)) {
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
                                                     int64_t start, int64_t end, Arena& arena,
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

std::string DataTypeTimeStampTzSerDe::to_olap_string(const Field& field) const {
    return CastToString::from_timestamptz(field.get<TYPE_TIMESTAMPTZ>(), 6);
}

Status DataTypeTimeStampTzSerDe::write_column_to_pb(const IColumn& column, PValues& result,
                                                    int64_t start, int64_t end) const {
    auto row_count = cast_set<int>(end - start);
    auto* ptype = result.mutable_type();
    const auto* col = check_and_get_column<ColumnType>(column);
    auto& data = col->get_data();
    ptype->set_id(PGenericType::UINT64);
    auto* values = result.mutable_uint64_value();
    values->Reserve(row_count);
    values->Add((uint64_t*)data.begin() + start, (uint64_t*)data.begin() + end);
    return Status::OK();
}

Status DataTypeTimeStampTzSerDe::read_column_from_pb(IColumn& column, const PValues& arg) const {
    auto old_column_size = column.size();
    column.resize(old_column_size + arg.uint64_value_size());
    auto& data = reinterpret_cast<ColumnType&>(column).get_data();
    for (int i = 0; i < arg.uint64_value_size(); ++i) {
        data[old_column_size + i] = binary_cast<UInt64, TimestampTzValue>(arg.uint64_value(i));
    }
    return Status::OK();
}

void DataTypeTimeStampTzSerDe::write_one_cell_to_jsonb(const IColumn& column,
                                                       JsonbWriterT<JsonbOutStream>& result,
                                                       Arena& mem_pool, int32_t col_id,
                                                       int64_t row_num,
                                                       const FormatOptions& options) const {
    result.writeKey(cast_set<JsonbKeyValue::keyid_type>(col_id));
    StringRef data_ref = column.get_data_at(row_num);
    int64_t val = *reinterpret_cast<const int64_t*>(data_ref.data);
    result.writeInt64(val);
}

void DataTypeTimeStampTzSerDe::read_one_cell_from_jsonb(IColumn& column,
                                                        const JsonbValue* arg) const {
    auto& col = reinterpret_cast<ColumnType&>(column);
    col.insert_value(
            binary_cast<UInt64, TimestampTzValue>((UInt64)arg->unpack<JsonbInt64Val>()->val()));
}

Status DataTypeTimeStampTzSerDe::read_column_from_arrow(IColumn& column,
                                                        const arrow::Array* arrow_array,
                                                        int64_t start, int64_t end,
                                                        const cctz::time_zone& ctz) const {
    auto row_count = end - start;
    auto& col_data = static_cast<ColumnType&>(column).get_data();

    if (arrow_array->type_id() == arrow::Type::STRING) {
        const auto* concrete_array = dynamic_cast<const arrow::StringArray*>(arrow_array);
        std::shared_ptr<arrow::Buffer> buffer = concrete_array->value_data();
        const auto* offsets_data = concrete_array->value_offsets()->data();
        const size_t offset_size = sizeof(int32_t);
        for (size_t offset_i = start; offset_i < end; ++offset_i) {
            if (!concrete_array->IsNull(offset_i)) {
                int32_t start_offset = 0;
                int32_t end_offset = 0;
                memcpy(&start_offset, offsets_data + offset_i * offset_size, offset_size);
                memcpy(&end_offset, offsets_data + (offset_i + 1) * offset_size, offset_size);
                const auto* raw_data = buffer->data() + start_offset;
                const auto raw_data_len = end_offset - start_offset;
                if (raw_data_len == 0) {
                    col_data.emplace_back(TimestampTzValue());
                } else {
                    StringRef str_ref(raw_data, raw_data_len);
                    UInt64 val = 0;
                    if (!try_read_int_text(val, str_ref)) {
                        return Status::Error(ErrorCode::INVALID_ARGUMENT,
                                             "parse number fail, string: '{}'",
                                             std::string(str_ref.data, str_ref.size).c_str());
                    }
                    col_data.emplace_back(binary_cast<UInt64, TimestampTzValue>(val));
                }
            } else {
                col_data.emplace_back(TimestampTzValue());
            }
        }
        return Status::OK();
    }

    // Buffer path
    if (row_count == 0 || arrow_array->data()->buffers[1] == nullptr) {
        return Status::OK();
    }
    std::shared_ptr<arrow::Buffer> buffer = arrow_array->data()->buffers[1];
    const auto* raw_data =
            reinterpret_cast<const typename PrimitiveTypeTraits<TYPE_TIMESTAMPTZ>::CppType*>(
                    buffer->data()) +
            start;
    col_data.insert(raw_data, raw_data + row_count);
    return Status::OK();
}

} // namespace doris
