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

#include <arrow/builder.h>
#include <cctz/time_zone.h>

#include "core/data_type/primitive_type.h"
#include "core/data_type_serde/decoded_column_view.h"
#include "core/data_type_serde/parquet_decode_source.h"
#include "core/data_type_serde/parquet_timestamp.h"
#include "core/value/timestamptz_value.h"
#include "exprs/function/cast/cast_parameters.h"
#include "exprs/function/cast/cast_to_string.h"
#include "exprs/function/cast/cast_to_timestamptz.h"
#include "util/unaligned.h"
namespace doris {

namespace {

Status append_timestamptz_from_utc_epoch_micros(ColumnTimeStampTz::Container& data,
                                                int64_t timestamp_micros) {
    static constexpr int64_t MICROS_PER_SECOND = 1000000;
    static const auto UTC = cctz::utc_time_zone();

    int64_t epoch_seconds = timestamp_micros / MICROS_PER_SECOND;
    int64_t micros_of_second = timestamp_micros % MICROS_PER_SECOND;
    if (micros_of_second < 0) {
        micros_of_second += MICROS_PER_SECOND;
        --epoch_seconds;
    }

    TimestampTzValue timestamp_tz;
    timestamp_tz.from_unixtime(epoch_seconds, UTC);
    timestamp_tz.set_microsecond(static_cast<uint32_t>(micros_of_second));
    if (!timestamp_tz.is_valid_date()) {
        return Status::DataQualityError(
                "Decoded TIMESTAMPTZ is outside the Doris 0001-9999 range: micros={}",
                timestamp_micros);
    }
    data.push_back(timestamp_tz);
    return Status::OK();
}

ParquetTimeUnit decoded_parquet_time_unit(const DecodedColumnView& view) {
    if (view.time_unit == DecodedTimeUnit::MILLIS) {
        return ParquetTimeUnit::MILLIS;
    }
    if (view.time_unit == DecodedTimeUnit::NANOS) {
        return ParquetTimeUnit::NANOS;
    }
    return ParquetTimeUnit::MICROS;
}

class TimestampTzParquetConsumer final : public ParquetFixedValueConsumer {
public:
    TimestampTzParquetConsumer(IColumn& column, const ParquetDecodeContext& context,
                               ParquetMaterializationState* state = nullptr)
            : _data(assert_cast<ColumnTimeStampTz&>(column).get_data()),
              _context(context),
              _state(state) {}

    Status consume(const uint8_t* values, size_t num_values, size_t value_width) override {
        const size_t old_size = _data.size();
        for (size_t row = 0; row < num_values; ++row) {
            int64_t timestamp_micros;
            Status status;
            if (_context.physical_type == ParquetPhysicalType::INT96) {
                DORIS_CHECK_EQ(value_width, sizeof(ParquetInt96Timestamp));
                status = parquet_int96_timestamp_micros(
                        unaligned_load<ParquetInt96Timestamp>(values +
                                                              row * sizeof(ParquetInt96Timestamp)),
                        &timestamp_micros);
            } else {
                DORIS_CHECK(_context.physical_type == ParquetPhysicalType::INT64);
                DORIS_CHECK_EQ(value_width, sizeof(int64_t));
                status = parquet_timestamp_micros(
                        _context.time_unit, unaligned_load<int64_t>(values + row * sizeof(int64_t)),
                        &timestamp_micros);
            }
            if (status.ok()) {
                status = append_timestamptz_from_utc_epoch_micros(_data, timestamp_micros);
            }
            if (!status.ok()) {
                if (_state != nullptr && _state->mark_conversion_failure(_data.size())) {
                    _data.emplace_back();
                    continue;
                }
                _data.resize(old_size);
                return status;
            }
        }
        return Status::OK();
    }

private:
    ColumnTimeStampTz::Container& _data;
    const ParquetDecodeContext& _context;
    ParquetMaterializationState* _state;
};

class RejectTimestampTzBinaryConsumer final : public ParquetBinaryValueConsumer {
public:
    Status consume(const StringRef* values, size_t num_values) override {
        return Status::NotSupported("Binary Parquet values cannot be materialized as TIMESTAMPTZ");
    }
};

} // namespace

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
    auto& col_nullmap = col_res.get_null_map_column();
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
            RETURN_IF_ERROR(
                    checkArrowStatus(timestamp_builder.AppendNull(), column, *array_builder));
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
            RETURN_IF_ERROR(
                    checkArrowStatus(timestamp_builder.Append(timestamp), column, *array_builder));
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

Status DataTypeTimeStampTzSerDe::read_column_from_decoded_values(
        IColumn& column, const DecodedColumnView& view) const {
    if (view.value_kind != DecodedValueKind::INT64 && view.value_kind != DecodedValueKind::INT96) {
        return decoded_column_view_handle_conversion_failure(
                column, view,
                Status::NotSupported("TIMESTAMPTZ decoded reader expects INT64 or INT96 source"));
    }
    if (view.values == nullptr && decoded_column_view_has_non_null_value(view)) {
        return Status::Corruption("Decoded value buffer is null for {}", column.get_name());
    }

    auto& data = assert_cast<ColumnTimeStampTz&>(column).get_data();
    const auto old_size = data.size();
    if (view.value_kind == DecodedValueKind::INT96) {
        const auto* values = reinterpret_cast<const ParquetInt96Timestamp*>(view.values);
        for (int64_t row = 0; row < view.row_count; ++row) {
            if (decoded_column_view_row_is_null(view, row)) {
                data.push_back(TimestampTzValue());
                continue;
            }
            int64_t timestamp_micros;
            auto status = parquet_int96_timestamp_micros(values[row], &timestamp_micros);
            if (status.ok()) {
                status = append_timestamptz_from_utc_epoch_micros(data, timestamp_micros);
            }
            if (!status.ok()) {
                if (decoded_column_view_can_null_on_conversion_failure(view)) {
                    decoded_column_view_insert_null_on_conversion_failure(column, view, row);
                    continue;
                }
                data.resize(old_size);
                return status;
            }
        }
        return Status::OK();
    }

    const auto* values = reinterpret_cast<const int64_t*>(view.values);
    for (int64_t row = 0; row < view.row_count; ++row) {
        if (decoded_column_view_row_is_null(view, row)) {
            data.push_back(TimestampTzValue());
            continue;
        }
        int64_t timestamp_micros;
        auto status = parquet_timestamp_micros(decoded_parquet_time_unit(view), values[row],
                                               &timestamp_micros);
        if (status.ok()) {
            status = append_timestamptz_from_utc_epoch_micros(data, timestamp_micros);
        }
        if (!status.ok()) {
            if (decoded_column_view_can_null_on_conversion_failure(view)) {
                decoded_column_view_insert_null_on_conversion_failure(column, view, row);
                continue;
            }
            data.resize(old_size);
            return status;
        }
    }
    return Status::OK();
}

Status DataTypeTimeStampTzSerDe::read_parquet_dictionary(
        IColumn& column, ParquetDecodeSource& source, const ParquetDecodeContext& context) const {
    TimestampTzParquetConsumer consumer(column, context);
    RejectTimestampTzBinaryConsumer binary_consumer;
    return source.decode_dictionary(consumer, binary_consumer);
}

Status DataTypeTimeStampTzSerDe::read_column_from_parquet(
        IColumn& column, ParquetDecodeSource& source, const ParquetDecodeContext& context,
        size_t num_values, ParquetMaterializationState& state) const {
    if (context.physical_type != ParquetPhysicalType::INT64 &&
        context.physical_type != ParquetPhysicalType::INT96) {
        return Status::NotSupported("TIMESTAMPTZ expects Parquet INT64 or INT96");
    }
    TimestampTzParquetConsumer consumer(column, context, &state);
    if (context.encoding != ParquetValueEncoding::DICTIONARY) {
        return source.decode_fixed_values(num_values, consumer);
    }
    if (state.dictionary_generation != source.dictionary_generation()) {
        state.typed_dictionary = column.clone_empty();
        auto* output_null_map = state.begin_dictionary_conversion(source.dictionary_size());
        TimestampTzParquetConsumer dictionary_consumer(*state.typed_dictionary, context, &state);
        RejectTimestampTzBinaryConsumer binary_consumer;
        const Status dictionary_status =
                source.decode_dictionary(dictionary_consumer, binary_consumer);
        state.end_dictionary_conversion(output_null_map);
        RETURN_IF_ERROR(dictionary_status);
        DORIS_CHECK_EQ(state.typed_dictionary->size(), source.dictionary_size());
        state.dictionary_generation = source.dictionary_generation();
    }
    return state.materialize_dictionary(column, source, num_values);
}

std::string DataTypeTimeStampTzSerDe::to_olap_string(const Field& field) const {
    return CastToString::from_timestamptz(field.get<TYPE_TIMESTAMPTZ>(), 6);
}

void DataTypeTimeStampTzSerDe::write_one_cell_to_binary(const IColumn& src_column,
                                                        ColumnString::Chars& chars,
                                                        int64_t row_num) const {
    const auto type = static_cast<uint8_t>(FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ);
    const auto& data_ref = assert_cast<const ColumnTimeStampTz&>(src_column).get_data_at(row_num);
    const auto sc = static_cast<uint8_t>(_scale);

    const size_t old_size = chars.size();
    const size_t new_size = old_size + sizeof(uint8_t) + sizeof(uint8_t) + data_ref.size;
    chars.resize(new_size);
    memcpy(chars.data() + old_size, reinterpret_cast<const char*>(&type), sizeof(uint8_t));
    memcpy(chars.data() + old_size + sizeof(uint8_t), reinterpret_cast<const char*>(&sc),
           sizeof(uint8_t));
    memcpy(chars.data() + old_size + sizeof(uint8_t) + sizeof(uint8_t), data_ref.data,
           data_ref.size);
}

} // namespace doris
