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
#include "core/data_type_serde/data_type_serde.h"

#include <cctz/time_zone.h>

#include <array>
#include <cstring>
#include <memory>
#include <orc/OrcFile.hh>
#include <orc/Vector.hh>
#include <vector>

#include "common/cast_set.h"
#include "common/check.h"
#include "common/consts.h"
#include "common/exception.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/storage_field_type.h"
#include "core/data_type_serde/data_type_array_serde.h"
#include "core/data_type_serde/data_type_datetimev2_serde.h"
#include "core/data_type_serde/data_type_datev2_serde.h"
#include "core/data_type_serde/data_type_decimal_serde.h"
#include "core/data_type_serde/data_type_jsonb_serde.h"
#include "core/data_type_serde/data_type_nullable_serde.h"
#include "core/data_type_serde/data_type_number_serde.h"
#include "core/data_type_serde/data_type_string_serde.h"
#include "core/data_type_serde/data_type_timestamptz_serde.h"
#include "core/field.h"
#include "core/types.h"
#include "core/value/timestamptz_value.h"
#include "core/value/vdatetime_value.h"
#include "exprs/function/cast/cast_base.h"
#include "runtime/descriptors.h"
#include "storage/olap_common.h"
#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"
namespace doris {
namespace {

constexpr int DECIMAL_PRECISION_FOR_HIVE11 = BeConsts::MAX_DECIMAL128_PRECISION;
constexpr int32_t DORIS_DATE_EPOCH_DAYNR = 719528;

bool orc_row_is_null(const ::orc::ColumnVectorBatch& batch, size_t row) {
    return batch.hasNulls && !batch.notNull[row];
}

size_t orc_decode_row_count(size_t rows, const std::vector<size_t>* selected_rows) {
    if (selected_rows == nullptr) {
        return rows;
    }
    return selected_rows->size();
}

size_t orc_source_row_at(size_t row, const std::vector<size_t>* selected_rows) {
    if (selected_rows == nullptr) {
        return row;
    }
    return (*selected_rows)[row];
}

DecodedColumnView make_orc_decoded_view(const OrcDecodedColumnView& orc_view,
                                        DecodedValueKind value_kind) {
    DecodedColumnView view;
    view.value_kind = value_kind;
    view.row_count = cast_set<int64_t>(orc_decode_row_count(orc_view.rows, orc_view.selected_rows));
    view.timezone = orc_view.timezone;
    return view;
}

void fill_orc_decoded_null_map(const ::orc::ColumnVectorBatch& batch, size_t rows,
                               const std::vector<size_t>* selected_rows, NullMap* null_map) {
    DORIS_CHECK(null_map != nullptr);
    if (!batch.hasNulls) {
        return;
    }
    const auto output_rows = orc_decode_row_count(rows, selected_rows);
    null_map->resize(output_rows);
    for (size_t row = 0; row < output_rows; ++row) {
        (*null_map)[row] = !batch.notNull[orc_source_row_at(row, selected_rows)];
    }
}

void append_orc_null_map(const ::orc::ColumnVectorBatch& batch, size_t rows,
                         const std::vector<size_t>* selected_rows, NullMap* null_map) {
    DORIS_CHECK(null_map != nullptr);
    const auto output_rows = orc_decode_row_count(rows, selected_rows);
    const auto old_size = null_map->size();
    null_map->resize(old_size + output_rows);
    if (batch.hasNulls) {
        for (size_t row = 0; row < output_rows; ++row) {
            (*null_map)[old_size + row] = !batch.notNull[orc_source_row_at(row, selected_rows)];
        }
        return;
    }
    std::memset(null_map->data() + old_size, 0, output_rows);
}

size_t trim_right_spaces(const char* value, size_t length) {
    while (length > 0 && value[length - 1] == ' ') {
        --length;
    }
    return length;
}

Status append_orc_string_ref(const ::orc::Type& file_type, const char* data, int64_t length,
                             std::vector<StringRef>& binary_values) {
    if (length < 0) {
        return Status::Corruption("Invalid negative ORC string length {}", length);
    }
    auto value_length = static_cast<size_t>(length);
    if (file_type.getKind() == ::orc::TypeKind::CHAR) {
        value_length = trim_right_spaces(data, value_length);
    }
    binary_values.emplace_back(value_length == 0 ? "" : data, value_length);
    return Status::OK();
}

Int128 to_int128(::orc::Int128 value) {
    const auto high_bits = static_cast<__uint128_t>(static_cast<uint64_t>(value.getHighBits()));
    const auto low_bits = static_cast<__uint128_t>(value.getLowBits());
    return static_cast<Int128>((high_bits << 64) | low_bits);
}

::orc::Int128 to_orc_int128(Int128 value) {
    const auto unsigned_value = static_cast<__uint128_t>(value);
    return ::orc::Int128(static_cast<int64_t>(static_cast<uint64_t>(unsigned_value >> 64)),
                         static_cast<uint64_t>(unsigned_value));
}

Status scale_decimal_value(Int128 value, int32_t source_scale, int32_t target_scale,
                           Int128* scaled_value) {
    DORIS_CHECK(scaled_value != nullptr);
    if (source_scale == target_scale) {
        *scaled_value = value;
        return Status::OK();
    }
    if (source_scale < target_scale) {
        bool overflow = false;
        const auto scaled = ::orc::scaleUpInt128ByPowerOfTen(to_orc_int128(value),
                                                             target_scale - source_scale, overflow);
        if (overflow) {
            return Status::DataQualityError(
                    "ORC decimal value overflows when scaling from {} to {}", source_scale,
                    target_scale);
        }
        *scaled_value = to_int128(scaled);
        return Status::OK();
    }
    *scaled_value = to_int128(
            ::orc::scaleDownInt128ByPowerOfTen(to_orc_int128(value), source_scale - target_scale));
    return Status::OK();
}

void fill_decimal_big_endian_value(Int128 value, std::array<uint8_t, sizeof(Int128)>* bytes) {
    DORIS_CHECK(bytes != nullptr);
    const auto unsigned_value = static_cast<__uint128_t>(value);
    for (size_t byte_idx = 0; byte_idx < bytes->size(); ++byte_idx) {
        const auto shift = (bytes->size() - byte_idx - 1) * 8;
        (*bytes)[byte_idx] = static_cast<uint8_t>(unsigned_value >> shift);
    }
}

Status read_decoded_values(const DataTypeSerDe& serde, IColumn& column, DecodedColumnView* view) {
    DORIS_CHECK(view != nullptr);
    RETURN_IF_ERROR(serde.read_column_from_decoded_values(column, *view));
    return Status::OK();
}

template <typename SourceType>
void fill_selected_values(const SourceType* source_values, size_t rows,
                          const std::vector<size_t>* selected_rows,
                          std::vector<SourceType>* selected_values) {
    DORIS_CHECK(source_values != nullptr);
    DORIS_CHECK(selected_values != nullptr);
    const auto output_rows = orc_decode_row_count(rows, selected_rows);
    selected_values->resize(output_rows);
    for (size_t row = 0; row < output_rows; ++row) {
        (*selected_values)[row] = source_values[orc_source_row_at(row, selected_rows)];
    }
}

template <typename OrcBatchType, typename SourceType>
Status decode_fixed_orc_values(const DataTypeSerDe& serde, IColumn& column,
                               const OrcDecodedColumnView& orc_view, DecodedValueKind value_kind) {
    const auto* orc_batch = dynamic_cast<const OrcBatchType*>(orc_view.batch);
    if (orc_batch == nullptr) {
        return Status::InternalError("Unexpected ORC scalar batch type {}",
                                     orc_view.batch->toString());
    }
    auto view = make_orc_decoded_view(orc_view, value_kind);
    NullMap null_map;
    fill_orc_decoded_null_map(*orc_view.batch, orc_view.rows, orc_view.selected_rows, &null_map);
    view.null_map = null_map.empty() ? nullptr : null_map.data();
    std::vector<SourceType> selected_values;
    if (orc_view.selected_rows == nullptr) {
        view.values = reinterpret_cast<const uint8_t*>(orc_batch->data.data());
    } else {
        fill_selected_values(orc_batch->data.data(), orc_view.rows, orc_view.selected_rows,
                             &selected_values);
        view.values = reinterpret_cast<const uint8_t*>(selected_values.data());
    }
    RETURN_IF_ERROR(read_decoded_values(serde, column, &view));
    return Status::OK();
}

Status decode_float_orc_values(const DataTypeSerDe& serde, IColumn& column,
                               const OrcDecodedColumnView& orc_view) {
    const auto* orc_batch = dynamic_cast<const ::orc::DoubleVectorBatch*>(orc_view.batch);
    if (orc_batch == nullptr) {
        return Status::InternalError("Unexpected ORC float batch type {}",
                                     orc_view.batch->toString());
    }
    auto view = make_orc_decoded_view(orc_view, DecodedValueKind::FLOAT);
    NullMap null_map;
    fill_orc_decoded_null_map(*orc_view.batch, orc_view.rows, orc_view.selected_rows, &null_map);
    view.null_map = null_map.empty() ? nullptr : null_map.data();
    const auto output_rows = orc_decode_row_count(orc_view.rows, orc_view.selected_rows);
    std::vector<float> float_values;
    float_values.resize(output_rows);
    for (size_t row = 0; row < output_rows; ++row) {
        float_values[row] =
                static_cast<float>(orc_batch->data[orc_source_row_at(row, orc_view.selected_rows)]);
    }
    view.values = reinterpret_cast<const uint8_t*>(float_values.data());
    RETURN_IF_ERROR(read_decoded_values(serde, column, &view));
    return Status::OK();
}

Status decode_boolean_orc_values(const DataTypeSerDe& serde, IColumn& column,
                                 const OrcDecodedColumnView& orc_view) {
    const auto* orc_batch = dynamic_cast<const ::orc::LongVectorBatch*>(orc_view.batch);
    if (orc_batch == nullptr) {
        return Status::InternalError("Unexpected ORC boolean batch type {}",
                                     orc_view.batch->toString());
    }
    auto view = make_orc_decoded_view(orc_view, DecodedValueKind::BOOL);
    NullMap null_map;
    fill_orc_decoded_null_map(*orc_view.batch, orc_view.rows, orc_view.selected_rows, &null_map);
    view.null_map = null_map.empty() ? nullptr : null_map.data();
    const auto output_rows = orc_decode_row_count(orc_view.rows, orc_view.selected_rows);
    std::unique_ptr<bool[]> bool_values = std::make_unique<bool[]>(output_rows);
    for (size_t row = 0; row < output_rows; ++row) {
        bool_values[row] = orc_batch->data[orc_source_row_at(row, orc_view.selected_rows)] != 0;
    }
    view.values = reinterpret_cast<const uint8_t*>(bool_values.get());
    RETURN_IF_ERROR(read_decoded_values(serde, column, &view));
    return Status::OK();
}

Status decode_string_orc_values(const DataTypeSerDe& serde, IColumn& column,
                                const OrcDecodedColumnView& orc_view) {
    DORIS_CHECK(orc_view.file_type != nullptr);
    if (const auto* encoded_batch =
                dynamic_cast<const ::orc::EncodedStringVectorBatch*>(orc_view.batch);
        encoded_batch != nullptr && encoded_batch->isEncoded) {
        if (encoded_batch->dictionary == nullptr) {
            return Status::InternalError("Encoded ORC string batch has no dictionary");
        }
        auto view = make_orc_decoded_view(orc_view, DecodedValueKind::BINARY);
        NullMap null_map;
        fill_orc_decoded_null_map(*orc_view.batch, orc_view.rows, orc_view.selected_rows,
                                  &null_map);
        view.null_map = null_map.empty() ? nullptr : null_map.data();
        const auto output_rows = orc_decode_row_count(orc_view.rows, orc_view.selected_rows);
        std::vector<StringRef> binary_values;
        binary_values.reserve(output_rows);
        for (size_t row = 0; row < output_rows; ++row) {
            const auto source_row = orc_source_row_at(row, orc_view.selected_rows);
            if (orc_row_is_null(*orc_view.batch, source_row)) {
                binary_values.emplace_back("", 0);
                continue;
            }
            char* data = nullptr;
            int64_t length = 0;
            encoded_batch->dictionary->getValueByIndex(encoded_batch->index[source_row], data,
                                                       length);
            RETURN_IF_ERROR(
                    append_orc_string_ref(*orc_view.file_type, data, length, binary_values));
        }
        view.binary_values = &binary_values;
        RETURN_IF_ERROR(read_decoded_values(serde, column, &view));
        return Status::OK();
    }

    const auto* orc_batch = dynamic_cast<const ::orc::StringVectorBatch*>(orc_view.batch);
    if (orc_batch == nullptr) {
        return Status::InternalError("Unexpected ORC string batch type {}",
                                     orc_view.batch->toString());
    }
    auto view = make_orc_decoded_view(orc_view, DecodedValueKind::BINARY);
    NullMap null_map;
    fill_orc_decoded_null_map(*orc_view.batch, orc_view.rows, orc_view.selected_rows, &null_map);
    view.null_map = null_map.empty() ? nullptr : null_map.data();
    const auto output_rows = orc_decode_row_count(orc_view.rows, orc_view.selected_rows);
    std::vector<StringRef> binary_values;
    binary_values.reserve(output_rows);
    for (size_t row = 0; row < output_rows; ++row) {
        const auto source_row = orc_source_row_at(row, orc_view.selected_rows);
        if (orc_row_is_null(*orc_view.batch, source_row)) {
            binary_values.emplace_back("", 0);
            continue;
        }
        RETURN_IF_ERROR(append_orc_string_ref(*orc_view.file_type, orc_batch->data[source_row],
                                              orc_batch->length[source_row], binary_values));
    }
    view.binary_values = &binary_values;
    RETURN_IF_ERROR(read_decoded_values(serde, column, &view));
    return Status::OK();
}

Status decode_date_orc_values(const DataTypeSerDe& serde, IColumn& column,
                              const OrcDecodedColumnView& orc_view) {
    const auto* orc_batch = dynamic_cast<const ::orc::LongVectorBatch*>(orc_view.batch);
    if (orc_batch == nullptr) {
        return Status::InternalError("Unexpected ORC date batch type {}",
                                     orc_view.batch->toString());
    }
    auto view = make_orc_decoded_view(orc_view, DecodedValueKind::INT32);
    NullMap null_map;
    fill_orc_decoded_null_map(*orc_view.batch, orc_view.rows, orc_view.selected_rows, &null_map);
    view.null_map = null_map.empty() ? nullptr : null_map.data();
    const auto output_rows = orc_decode_row_count(orc_view.rows, orc_view.selected_rows);
    std::vector<int32_t> date_values;
    date_values.resize(output_rows);
    auto& date_dict = date_day_offset_dict::get();
    for (size_t row = 0; row < output_rows; ++row) {
        const auto source_row = orc_source_row_at(row, orc_view.selected_rows);
        const auto date = date_dict[cast_set<int>(orc_batch->data[source_row])];
        date_values[row] = cast_set<int32_t>(date.daynr() - DORIS_DATE_EPOCH_DAYNR);
    }
    view.values = reinterpret_cast<const uint8_t*>(date_values.data());
    RETURN_IF_ERROR(read_decoded_values(serde, column, &view));
    return Status::OK();
}

Status decode_decimal_orc_values(const DataTypeSerDe& serde, IColumn& column,
                                 const OrcDecodedColumnView& orc_view, int32_t target_scale) {
    DORIS_CHECK(orc_view.file_type != nullptr);
    auto view = make_orc_decoded_view(orc_view, DecodedValueKind::FIXED_BINARY);
    view.decimal_precision = orc_view.file_type->getPrecision() == 0
                                     ? DECIMAL_PRECISION_FOR_HIVE11
                                     : cast_set<int>(orc_view.file_type->getPrecision());
    NullMap null_map;
    fill_orc_decoded_null_map(*orc_view.batch, orc_view.rows, orc_view.selected_rows, &null_map);
    view.null_map = null_map.empty() ? nullptr : null_map.data();
    view.fixed_length = sizeof(Int128);

    std::vector<StringRef> binary_values;
    std::vector<std::array<uint8_t, sizeof(Int128)>> decimal_values;
    const auto output_rows = orc_decode_row_count(orc_view.rows, orc_view.selected_rows);
    decimal_values.resize(output_rows);
    binary_values.reserve(output_rows);
    if (const auto* decimal64_batch =
                dynamic_cast<const ::orc::Decimal64VectorBatch*>(orc_view.batch);
        decimal64_batch != nullptr) {
        view.decimal_scale = decimal64_batch->scale;
        for (size_t row = 0; row < output_rows; ++row) {
            Int128 value = 0;
            const auto source_row = orc_source_row_at(row, orc_view.selected_rows);
            if (!orc_row_is_null(*orc_view.batch, source_row)) {
                RETURN_IF_ERROR(scale_decimal_value(decimal64_batch->values[source_row],
                                                    decimal64_batch->scale, target_scale, &value));
            }
            fill_decimal_big_endian_value(value, &decimal_values[row]);
            binary_values.emplace_back(reinterpret_cast<const char*>(decimal_values[row].data()),
                                       decimal_values[row].size());
        }
        view.binary_values = &binary_values;
        RETURN_IF_ERROR(read_decoded_values(serde, column, &view));
        return Status::OK();
    }

    const auto* decimal128_batch =
            dynamic_cast<const ::orc::Decimal128VectorBatch*>(orc_view.batch);
    if (decimal128_batch == nullptr) {
        return Status::InternalError("Unexpected ORC decimal batch type {}",
                                     orc_view.batch->toString());
    }
    view.decimal_scale = decimal128_batch->scale;
    for (size_t row = 0; row < output_rows; ++row) {
        Int128 value = 0;
        const auto source_row = orc_source_row_at(row, orc_view.selected_rows);
        if (!orc_row_is_null(*orc_view.batch, source_row)) {
            RETURN_IF_ERROR(scale_decimal_value(to_int128(decimal128_batch->values[source_row]),
                                                decimal128_batch->scale, target_scale, &value));
        }
        fill_decimal_big_endian_value(value, &decimal_values[row]);
        binary_values.emplace_back(reinterpret_cast<const char*>(decimal_values[row].data()),
                                   decimal_values[row].size());
    }
    view.binary_values = &binary_values;
    RETURN_IF_ERROR(read_decoded_values(serde, column, &view));
    return Status::OK();
}

Status append_orc_offsets(ColumnArray::Offsets64& doris_offsets,
                          const ::orc::DataBuffer<int64_t>& orc_offsets, size_t rows,
                          size_t* element_size, const std::vector<size_t>* selected_rows,
                          std::vector<size_t>* element_selection) {
    DORIS_CHECK(element_size != nullptr);
    if (selected_rows != nullptr) {
        DORIS_CHECK(element_selection != nullptr);
        const auto prev_offset = doris_offsets.empty() ? 0 : doris_offsets.back();
        ColumnArray::Offset64 current_offset = prev_offset;
        element_selection->clear();
        for (size_t row = 0; row < selected_rows->size(); ++row) {
            const auto source_row = (*selected_rows)[row];
            DORIS_CHECK(source_row < rows);
            const auto begin_offset = orc_offsets[source_row];
            const auto end_offset = orc_offsets[source_row + 1];
            if (end_offset < begin_offset) {
                return Status::Corruption("Invalid ORC offsets");
            }
            const auto delta = static_cast<size_t>(end_offset - begin_offset);
            for (size_t element_idx = 0; element_idx < delta; ++element_idx) {
                element_selection->push_back(static_cast<size_t>(begin_offset) + element_idx);
            }
            current_offset += static_cast<ColumnArray::Offset64>(delta);
            doris_offsets.push_back(current_offset);
        }
        *element_size = element_selection->size();
        return Status::OK();
    }

    const auto prev_offset = doris_offsets.empty() ? 0 : doris_offsets.back();
    const auto base_offset = orc_offsets[0];
    for (size_t idx = 1; idx <= rows; ++idx) {
        const auto delta = orc_offsets[idx] - base_offset;
        if (delta < 0) {
            return Status::Corruption("Invalid ORC offsets");
        }
        doris_offsets.push_back(prev_offset + static_cast<ColumnArray::Offset64>(delta));
    }
    const auto total_delta = orc_offsets[rows] - base_offset;
    if (total_delta < 0) {
        return Status::Corruption("Invalid ORC offsets");
    }
    *element_size = static_cast<size_t>(total_delta);
    return Status::OK();
}

int64_t find_struct_child_index(const ::orc::Type& type, const std::string& field_name) {
    DORIS_CHECK(type.getKind() == ::orc::TypeKind::STRUCT);
    for (uint64_t child_idx = 0; child_idx < type.getSubtypeCount(); ++child_idx) {
        if (type.getFieldName(child_idx) == field_name) {
            return static_cast<int64_t>(child_idx);
        }
    }
    return -1;
}

Status decode_timestamp_orc_values(IColumn& nested_column, const OrcDecodedColumnView& orc_view,
                                   const cctz::time_zone& timezone) {
    const auto* orc_batch = dynamic_cast<const ::orc::TimestampVectorBatch*>(orc_view.batch);
    if (orc_batch == nullptr) {
        return Status::InternalError("Unexpected ORC timestamp batch type {}",
                                     orc_view.batch->toString());
    }
    auto& data = assert_cast<ColumnDateTimeV2&>(nested_column).get_data();
    const size_t old_data_size = data.size();
    const auto output_rows = orc_decode_row_count(orc_view.rows, orc_view.selected_rows);
    data.resize(old_data_size + output_rows);
    for (size_t row = 0; row < output_rows; ++row) {
        const auto source_row = orc_source_row_at(row, orc_view.selected_rows);
        if (orc_row_is_null(*orc_view.batch, source_row)) {
            data[old_data_size + row] = DateV2Value<DateTimeV2ValueType> {};
            continue;
        }
        auto& value =
                reinterpret_cast<DateV2Value<DateTimeV2ValueType>&>(data[old_data_size + row]);
        value.from_unixtime(orc_batch->data[source_row], timezone);
        value.set_microsecond(cast_set<uint64_t>(orc_batch->nanoseconds[source_row] / 1000));
    }
    return Status::OK();
}

Status decode_timestamp_tz_orc_values(IColumn& nested_column,
                                      const OrcDecodedColumnView& orc_view) {
    const auto* orc_batch = dynamic_cast<const ::orc::TimestampVectorBatch*>(orc_view.batch);
    if (orc_batch == nullptr) {
        return Status::InternalError("Unexpected ORC timestamp batch type {}",
                                     orc_view.batch->toString());
    }
    auto& data = assert_cast<ColumnTimeStampTz&>(nested_column).get_data();
    const size_t old_data_size = data.size();
    const auto output_rows = orc_decode_row_count(orc_view.rows, orc_view.selected_rows);
    data.resize(old_data_size + output_rows);
    static const auto utc_time_zone = cctz::utc_time_zone();
    for (size_t row = 0; row < output_rows; ++row) {
        const auto source_row = orc_source_row_at(row, orc_view.selected_rows);
        if (orc_row_is_null(*orc_view.batch, source_row)) {
            data[old_data_size + row] = TimestampTzValue {};
            continue;
        }
        auto& value = data[old_data_size + row];
        value.from_unixtime(orc_batch->data[source_row], utc_time_zone);
        value.set_microsecond(cast_set<uint64_t>(orc_batch->nanoseconds[source_row] / 1000));
    }
    return Status::OK();
}

OrcDecodedColumnView make_child_orc_view(const OrcDecodedColumnView& parent_view,
                                         const ::orc::Type* file_type,
                                         const ::orc::Type* selected_type,
                                         const ::orc::ColumnVectorBatch* batch, size_t rows,
                                         const std::vector<size_t>* selected_rows) {
    OrcDecodedColumnView child_view = parent_view;
    child_view.file_type = file_type;
    child_view.selected_type = selected_type;
    child_view.batch = batch;
    child_view.rows = rows;
    child_view.selected_rows = selected_rows;
    return child_view;
}

Status read_orc_child_column(const DataTypeSerDeSPtr& child_serde, MutableColumnPtr& child_column,
                             const OrcDecodedColumnView& child_view) {
    DORIS_CHECK(child_serde != nullptr);
    RETURN_IF_ERROR(child_serde->read_column_from_orc(*child_column, child_view));
    return Status::OK();
}

Status decode_list_orc_values(const DataTypeSerDeSPtr& nested_serde, IColumn& nested_column,
                              const OrcDecodedColumnView& orc_view) {
    const auto* orc_list = dynamic_cast<const ::orc::ListVectorBatch*>(orc_view.batch);
    if (orc_list == nullptr) {
        return Status::InternalError("Unexpected ORC list batch type {}",
                                     orc_view.batch->toString());
    }
    DORIS_CHECK(orc_view.file_type != nullptr);
    DORIS_CHECK(orc_view.selected_type != nullptr);
    DORIS_CHECK(orc_view.file_type->getSubtypeCount() == 1);
    DORIS_CHECK(orc_view.selected_type->getSubtypeCount() == 1);
    DORIS_CHECK(orc_list->elements != nullptr);
    const auto* file_element_type = orc_view.file_type->getSubtype(0);
    const auto* selected_element_type = orc_view.selected_type->getSubtype(0);
    DORIS_CHECK(file_element_type != nullptr);
    DORIS_CHECK(selected_element_type != nullptr);

    auto& array_column = assert_cast<ColumnArray&>(nested_column);
    size_t element_size = 0;
    std::vector<size_t> element_selection;
    RETURN_IF_ERROR(append_orc_offsets(array_column.get_offsets(), orc_list->offsets, orc_view.rows,
                                       &element_size, orc_view.selected_rows, &element_selection));
    auto element_column = array_column.get_data_ptr()->assert_mutable();
    const auto child_rows = orc_view.selected_rows == nullptr
                                    ? element_size
                                    : static_cast<size_t>(orc_list->elements->numElements);
    const auto* child_selection = orc_view.selected_rows == nullptr ? nullptr : &element_selection;
    auto child_view = make_child_orc_view(orc_view, file_element_type, selected_element_type,
                                          orc_list->elements.get(), child_rows, child_selection);
    RETURN_IF_ERROR(read_orc_child_column(nested_serde, element_column, child_view));
    array_column.get_data_ptr() = std::move(element_column);
    return Status::OK();
}

Status decode_map_orc_values(const DataTypeSerDeSPtr& key_serde,
                             const DataTypeSerDeSPtr& value_serde, IColumn& nested_column,
                             const OrcDecodedColumnView& orc_view) {
    const auto* orc_map = dynamic_cast<const ::orc::MapVectorBatch*>(orc_view.batch);
    if (orc_map == nullptr) {
        return Status::InternalError("Unexpected ORC map batch type {}",
                                     orc_view.batch->toString());
    }
    DORIS_CHECK(orc_view.file_type != nullptr);
    DORIS_CHECK(orc_view.selected_type != nullptr);
    DORIS_CHECK(orc_view.file_type->getSubtypeCount() == 2);
    DORIS_CHECK(orc_view.selected_type->getSubtypeCount() == 2);
    DORIS_CHECK(orc_map->keys != nullptr);
    DORIS_CHECK(orc_map->elements != nullptr);
    auto& map_column = assert_cast<ColumnMap&>(nested_column);
    size_t element_size = 0;
    std::vector<size_t> element_selection;
    RETURN_IF_ERROR(append_orc_offsets(map_column.get_offsets(), orc_map->offsets, orc_view.rows,
                                       &element_size, orc_view.selected_rows, &element_selection));

    const auto* file_key_type = orc_view.file_type->getSubtype(0);
    const auto* selected_key_type = orc_view.selected_type->getSubtype(0);
    DORIS_CHECK(file_key_type != nullptr);
    DORIS_CHECK(selected_key_type != nullptr);
    const auto child_rows = orc_view.selected_rows == nullptr
                                    ? element_size
                                    : static_cast<size_t>(orc_map->keys->numElements);
    const auto* child_selection = orc_view.selected_rows == nullptr ? nullptr : &element_selection;
    auto key_column = map_column.get_keys_ptr()->assert_mutable();
    auto key_view = make_child_orc_view(orc_view, file_key_type, selected_key_type,
                                        orc_map->keys.get(), child_rows, child_selection);
    RETURN_IF_ERROR(read_orc_child_column(key_serde, key_column, key_view));
    map_column.get_keys_ptr() = std::move(key_column);

    const auto* file_value_type = orc_view.file_type->getSubtype(1);
    const auto* selected_value_type = orc_view.selected_type->getSubtype(1);
    DORIS_CHECK(file_value_type != nullptr);
    DORIS_CHECK(selected_value_type != nullptr);
    auto value_column = map_column.get_values_ptr()->assert_mutable();
    auto value_view = make_child_orc_view(
            orc_view, file_value_type, selected_value_type, orc_map->elements.get(),
            orc_view.selected_rows == nullptr ? element_size
                                              : static_cast<size_t>(orc_map->elements->numElements),
            child_selection);
    RETURN_IF_ERROR(read_orc_child_column(value_serde, value_column, value_view));
    map_column.get_values_ptr() = std::move(value_column);
    return Status::OK();
}

Status decode_struct_orc_values(const DataTypeSerDeSPtrs& elem_serdes_ptrs, IColumn& nested_column,
                                const OrcDecodedColumnView& orc_view) {
    const auto* orc_struct = dynamic_cast<const ::orc::StructVectorBatch*>(orc_view.batch);
    if (orc_struct == nullptr) {
        return Status::InternalError("Unexpected ORC struct batch type {}",
                                     orc_view.batch->toString());
    }
    DORIS_CHECK(orc_view.file_type != nullptr);
    DORIS_CHECK(orc_view.selected_type != nullptr);
    DORIS_CHECK(orc_view.selected_type->getSubtypeCount() == orc_struct->fields.size());
    auto& struct_column = assert_cast<ColumnStruct&>(nested_column);
    DORIS_CHECK(struct_column.tuple_size() == orc_view.selected_type->getSubtypeCount());
    DORIS_CHECK(elem_serdes_ptrs.size() == orc_view.selected_type->getSubtypeCount());

    for (uint64_t selected_idx = 0; selected_idx < orc_view.selected_type->getSubtypeCount();
         ++selected_idx) {
        const auto field_name = orc_view.selected_type->getFieldName(selected_idx);
        const auto file_child_idx = find_struct_child_index(*orc_view.file_type, field_name);
        if (file_child_idx < 0) {
            return Status::InternalError("Selected ORC field {} is not in file struct", field_name);
        }
        const auto* file_child_type =
                orc_view.file_type->getSubtype(static_cast<uint64_t>(file_child_idx));
        const auto* selected_child_type = orc_view.selected_type->getSubtype(selected_idx);
        DORIS_CHECK(file_child_type != nullptr);
        DORIS_CHECK(selected_child_type != nullptr);
        DORIS_CHECK(selected_idx < orc_struct->fields.size());
        auto child_column =
                struct_column.get_column_ptr(static_cast<size_t>(selected_idx))->assert_mutable();
        auto child_view = make_child_orc_view(orc_view, file_child_type, selected_child_type,
                                              orc_struct->fields[selected_idx], orc_view.rows,
                                              orc_view.selected_rows);
        RETURN_IF_ERROR(
                read_orc_child_column(elem_serdes_ptrs[selected_idx], child_column, child_view));
        struct_column.get_column_ptr(static_cast<size_t>(selected_idx)) = std::move(child_column);
    }
    return Status::OK();
}

} // namespace

DataTypeSerDe::~DataTypeSerDe() = default;

bool decoded_column_view_can_null_on_conversion_failure(const DecodedColumnView& view) {
    return !view.enable_strict_mode && view.conversion_failure_null_map != nullptr;
}

void decoded_column_view_insert_null_on_conversion_failure(IColumn& column,
                                                           const DecodedColumnView& view,
                                                           int64_t row) {
    DORIS_CHECK(decoded_column_view_can_null_on_conversion_failure(view));
    DORIS_CHECK(row >= 0);
    DORIS_CHECK(row < view.row_count);
    DORIS_CHECK(view.conversion_failure_null_map_offset >= 0);
    const auto null_map_row = view.conversion_failure_null_map_offset + row;
    DORIS_CHECK(null_map_row >= 0);
    DORIS_CHECK(static_cast<size_t>(null_map_row) < view.conversion_failure_null_map->size());
    column.insert_default();
    (*view.conversion_failure_null_map)[null_map_row] = 1;
}

Status decoded_column_view_handle_conversion_failure(IColumn& column, const DecodedColumnView& view,
                                                     const Status& status) {
    if (!decoded_column_view_can_null_on_conversion_failure(view)) {
        return status;
    }
    for (int64_t row = 0; row < view.row_count; ++row) {
        decoded_column_view_insert_null_on_conversion_failure(column, view, row);
    }
    return Status::OK();
}

Status DataTypeSerDe::read_column_from_decoded_values(IColumn& column,
                                                      const DecodedColumnView& view) const {
    return decoded_column_view_handle_conversion_failure(
            column, view,
            Status::NotSupported("read_column_from_decoded_values is not supported for {}",
                                 get_name()));
}

Status DataTypeSerDe::read_column_from_orc(IColumn& column,
                                           const OrcDecodedColumnView& view) const {
    return Status::NotSupported("read_column_from_orc is not supported for {}", get_name());
}

Status DataTypeNullableSerDe::read_column_from_orc(IColumn& column,
                                                   const OrcDecodedColumnView& view) const {
    DORIS_CHECK(view.file_type != nullptr);
    DORIS_CHECK(view.selected_type != nullptr);
    DORIS_CHECK(view.batch != nullptr);
    DORIS_CHECK(view.file_type->getKind() == view.selected_type->getKind());
    auto& nullable_column = assert_cast<ColumnNullable&>(column);
    const auto output_rows = orc_decode_row_count(view.rows, view.selected_rows);
    if (output_rows == 0) {
        return Status::OK();
    }

    auto& null_map = nullable_column.get_null_map_data();
    const auto old_null_map_size = null_map.size();
    auto& nested_column = nullable_column.get_nested_column();
    const auto old_nested_size = nested_column.size();
    append_orc_null_map(*view.batch, view.rows, view.selected_rows, &null_map);
    auto st = nested_serde->read_column_from_orc(nested_column, view);
    if (!st.ok()) {
        null_map.resize(old_null_map_size);
        nested_column.resize(old_nested_size);
    }
    return st;
}

template <PrimitiveType T>
Status DataTypeNumberSerDe<T>::read_column_from_orc(IColumn& column,
                                                    const OrcDecodedColumnView& view) const {
    DORIS_CHECK(view.file_type != nullptr);
    DORIS_CHECK(view.batch != nullptr);
    if (orc_decode_row_count(view.rows, view.selected_rows) == 0) {
        return Status::OK();
    }

    if constexpr (T == TYPE_BOOLEAN) {
        DORIS_CHECK(view.file_type->getKind() == ::orc::TypeKind::BOOLEAN);
        return decode_boolean_orc_values(*this, column, view);
    } else if constexpr (T == TYPE_TINYINT || T == TYPE_SMALLINT || T == TYPE_INT ||
                         T == TYPE_BIGINT) {
        if constexpr (T == TYPE_TINYINT) {
            DORIS_CHECK(view.file_type->getKind() == ::orc::TypeKind::BYTE);
        } else if constexpr (T == TYPE_SMALLINT) {
            DORIS_CHECK(view.file_type->getKind() == ::orc::TypeKind::SHORT);
        } else if constexpr (T == TYPE_INT) {
            DORIS_CHECK(view.file_type->getKind() == ::orc::TypeKind::INT);
        } else {
            DORIS_CHECK(view.file_type->getKind() == ::orc::TypeKind::LONG);
        }
        return decode_fixed_orc_values<::orc::LongVectorBatch, int64_t>(*this, column, view,
                                                                        DecodedValueKind::INT64);
    } else if constexpr (T == TYPE_FLOAT) {
        DORIS_CHECK(view.file_type->getKind() == ::orc::TypeKind::FLOAT);
        return decode_float_orc_values(*this, column, view);
    } else if constexpr (T == TYPE_DOUBLE) {
        DORIS_CHECK(view.file_type->getKind() == ::orc::TypeKind::DOUBLE);
        return decode_fixed_orc_values<::orc::DoubleVectorBatch, double>(*this, column, view,
                                                                         DecodedValueKind::DOUBLE);
    }
    return DataTypeSerDe::read_column_from_orc(column, view);
}

template <typename ColumnType>
Status DataTypeStringSerDeBase<ColumnType>::read_column_from_orc(
        IColumn& column, const OrcDecodedColumnView& view) const {
    DORIS_CHECK(view.file_type != nullptr);
    DORIS_CHECK(view.batch != nullptr);
    const auto kind = view.file_type->getKind();
    DORIS_CHECK(kind == ::orc::TypeKind::STRING || kind == ::orc::TypeKind::BINARY ||
                kind == ::orc::TypeKind::VARCHAR || kind == ::orc::TypeKind::CHAR);
    if (orc_decode_row_count(view.rows, view.selected_rows) == 0) {
        return Status::OK();
    }
    return decode_string_orc_values(*this, column, view);
}

template <PrimitiveType T>
Status DataTypeDecimalSerDe<T>::read_column_from_orc(IColumn& column,
                                                     const OrcDecodedColumnView& view) const {
    DORIS_CHECK(view.file_type != nullptr);
    DORIS_CHECK(view.batch != nullptr);
    DORIS_CHECK(view.file_type->getKind() == ::orc::TypeKind::DECIMAL);
    if (orc_decode_row_count(view.rows, view.selected_rows) == 0) {
        return Status::OK();
    }
    return decode_decimal_orc_values(*this, column, view, cast_set<int32_t>(scale));
}

Status DataTypeDateV2SerDe::read_column_from_orc(IColumn& column,
                                                 const OrcDecodedColumnView& view) const {
    DORIS_CHECK(view.file_type != nullptr);
    DORIS_CHECK(view.batch != nullptr);
    DORIS_CHECK(view.file_type->getKind() == ::orc::TypeKind::DATE);
    if (orc_decode_row_count(view.rows, view.selected_rows) == 0) {
        return Status::OK();
    }
    return decode_date_orc_values(*this, column, view);
}

Status DataTypeDateTimeV2SerDe::read_column_from_orc(IColumn& column,
                                                     const OrcDecodedColumnView& view) const {
    DORIS_CHECK(view.file_type != nullptr);
    DORIS_CHECK(view.batch != nullptr);
    const auto kind = view.file_type->getKind();
    DORIS_CHECK(kind == ::orc::TypeKind::TIMESTAMP || kind == ::orc::TypeKind::TIMESTAMP_INSTANT);
    DORIS_CHECK(view.timezone != nullptr);
    if (orc_decode_row_count(view.rows, view.selected_rows) == 0) {
        return Status::OK();
    }
    return decode_timestamp_orc_values(column, view, *view.timezone);
}

Status DataTypeTimeStampTzSerDe::read_column_from_orc(IColumn& column,
                                                      const OrcDecodedColumnView& view) const {
    DORIS_CHECK(view.file_type != nullptr);
    DORIS_CHECK(view.batch != nullptr);
    DORIS_CHECK(view.file_type->getKind() == ::orc::TypeKind::TIMESTAMP_INSTANT);
    DORIS_CHECK(view.enable_mapping_timestamp_tz);
    if (orc_decode_row_count(view.rows, view.selected_rows) == 0) {
        return Status::OK();
    }
    return decode_timestamp_tz_orc_values(column, view);
}

Status DataTypeArraySerDe::read_column_from_orc(IColumn& column,
                                                const OrcDecodedColumnView& view) const {
    DORIS_CHECK(view.file_type != nullptr);
    DORIS_CHECK(view.batch != nullptr);
    DORIS_CHECK(view.file_type->getKind() == ::orc::TypeKind::LIST);
    if (orc_decode_row_count(view.rows, view.selected_rows) == 0) {
        return Status::OK();
    }
    return decode_list_orc_values(nested_serde, column, view);
}

Status DataTypeMapSerDe::read_column_from_orc(IColumn& column,
                                              const OrcDecodedColumnView& view) const {
    DORIS_CHECK(view.file_type != nullptr);
    DORIS_CHECK(view.batch != nullptr);
    DORIS_CHECK(view.file_type->getKind() == ::orc::TypeKind::MAP);
    if (orc_decode_row_count(view.rows, view.selected_rows) == 0) {
        return Status::OK();
    }
    return decode_map_orc_values(key_serde, value_serde, column, view);
}

Status DataTypeStructSerDe::read_column_from_orc(IColumn& column,
                                                 const OrcDecodedColumnView& view) const {
    DORIS_CHECK(view.file_type != nullptr);
    DORIS_CHECK(view.batch != nullptr);
    DORIS_CHECK(view.file_type->getKind() == ::orc::TypeKind::STRUCT);
    if (orc_decode_row_count(view.rows, view.selected_rows) == 0) {
        return Status::OK();
    }
    return decode_struct_orc_values(elem_serdes_ptrs, column, view);
}

template Status DataTypeNumberSerDe<TYPE_BOOLEAN>::read_column_from_orc(
        IColumn& column, const OrcDecodedColumnView& view) const;
template Status DataTypeNumberSerDe<TYPE_TINYINT>::read_column_from_orc(
        IColumn& column, const OrcDecodedColumnView& view) const;
template Status DataTypeNumberSerDe<TYPE_SMALLINT>::read_column_from_orc(
        IColumn& column, const OrcDecodedColumnView& view) const;
template Status DataTypeNumberSerDe<TYPE_INT>::read_column_from_orc(
        IColumn& column, const OrcDecodedColumnView& view) const;
template Status DataTypeNumberSerDe<TYPE_BIGINT>::read_column_from_orc(
        IColumn& column, const OrcDecodedColumnView& view) const;
template Status DataTypeNumberSerDe<TYPE_LARGEINT>::read_column_from_orc(
        IColumn& column, const OrcDecodedColumnView& view) const;
template Status DataTypeNumberSerDe<TYPE_FLOAT>::read_column_from_orc(
        IColumn& column, const OrcDecodedColumnView& view) const;
template Status DataTypeNumberSerDe<TYPE_DOUBLE>::read_column_from_orc(
        IColumn& column, const OrcDecodedColumnView& view) const;
template Status DataTypeNumberSerDe<TYPE_DATE>::read_column_from_orc(
        IColumn& column, const OrcDecodedColumnView& view) const;
template Status DataTypeNumberSerDe<TYPE_DATEV2>::read_column_from_orc(
        IColumn& column, const OrcDecodedColumnView& view) const;
template Status DataTypeNumberSerDe<TYPE_DATETIME>::read_column_from_orc(
        IColumn& column, const OrcDecodedColumnView& view) const;
template Status DataTypeNumberSerDe<TYPE_DATETIMEV2>::read_column_from_orc(
        IColumn& column, const OrcDecodedColumnView& view) const;
template Status DataTypeNumberSerDe<TYPE_IPV4>::read_column_from_orc(
        IColumn& column, const OrcDecodedColumnView& view) const;
template Status DataTypeNumberSerDe<TYPE_IPV6>::read_column_from_orc(
        IColumn& column, const OrcDecodedColumnView& view) const;
template Status DataTypeNumberSerDe<TYPE_TIME>::read_column_from_orc(
        IColumn& column, const OrcDecodedColumnView& view) const;
template Status DataTypeNumberSerDe<TYPE_TIMEV2>::read_column_from_orc(
        IColumn& column, const OrcDecodedColumnView& view) const;
template Status DataTypeNumberSerDe<TYPE_TIMESTAMPTZ>::read_column_from_orc(
        IColumn& column, const OrcDecodedColumnView& view) const;

template Status DataTypeStringSerDeBase<ColumnString>::read_column_from_orc(
        IColumn& column, const OrcDecodedColumnView& view) const;
template Status DataTypeStringSerDeBase<ColumnString64>::read_column_from_orc(
        IColumn& column, const OrcDecodedColumnView& view) const;
template Status DataTypeStringSerDeBase<ColumnFixedLengthObject>::read_column_from_orc(
        IColumn& column, const OrcDecodedColumnView& view) const;

template Status DataTypeDecimalSerDe<TYPE_DECIMAL32>::read_column_from_orc(
        IColumn& column, const OrcDecodedColumnView& view) const;
template Status DataTypeDecimalSerDe<TYPE_DECIMAL64>::read_column_from_orc(
        IColumn& column, const OrcDecodedColumnView& view) const;
template Status DataTypeDecimalSerDe<TYPE_DECIMAL128I>::read_column_from_orc(
        IColumn& column, const OrcDecodedColumnView& view) const;
template Status DataTypeDecimalSerDe<TYPE_DECIMALV2>::read_column_from_orc(
        IColumn& column, const OrcDecodedColumnView& view) const;
template Status DataTypeDecimalSerDe<TYPE_DECIMAL256>::read_column_from_orc(
        IColumn& column, const OrcDecodedColumnView& view) const;

Status DataTypeSerDe::read_field_from_decoded_value(const IDataType& data_type, Field* field,
                                                    const DecodedColumnView& view) const {
    DORIS_CHECK(field != nullptr);
    DORIS_CHECK(view.row_count == 1);
    auto column = data_type.create_column();
    RETURN_IF_ERROR(read_column_from_decoded_values(*column, view));
    DORIS_CHECK(column->size() == 1);
    column->get(0, *field);
    return Status::OK();
}

DataTypeSerDeSPtrs create_data_type_serdes(const DataTypes& types) {
    DataTypeSerDeSPtrs serdes;
    serdes.reserve(types.size());
    for (const DataTypePtr& type : types) {
        serdes.push_back(type->get_serde());
    }
    return serdes;
}

DataTypeSerDeSPtrs create_data_type_serdes(const std::vector<SlotDescriptor*>& slots) {
    DataTypeSerDeSPtrs serdes;
    serdes.reserve(slots.size());
    for (const SlotDescriptor* slot : slots) {
        serdes.push_back(slot->get_data_type_ptr()->get_serde());
    }
    return serdes;
}

Status DataTypeSerDe::default_from_string(StringRef& str, IColumn& column) const {
    auto slice = str.to_slice();
    DataTypeSerDe::FormatOptions options;
    options.converted_from_string = true;
    ///TODO: Think again, when do we need to consider escape characters?
    // options.escape_char = '\\';
    // Deserialize the string into the column
    return deserialize_one_cell_from_json(column, slice, options);
}

Status DataTypeSerDe::serialize_column_to_jsonb_vector(const IColumn& from_column,
                                                       ColumnString& to_column) const {
    const auto size = from_column.size();
    JsonbWriter writer;
    for (int i = 0; i < size; i++) {
        writer.reset();
        RETURN_IF_ERROR(serialize_column_to_jsonb(from_column, i, writer));
        to_column.insert_data(writer.getOutput()->getBuffer(), writer.getOutput()->getSize());
    }
    return Status::OK();
}

Status DataTypeSerDe::parse_column_from_jsonb_string(IColumn& column, const JsonbValue* jsonb_value,
                                                     CastParameters& castParms) const {
    DCHECK(jsonb_value->isString());
    const auto* blob = jsonb_value->unpack<JsonbBinaryVal>();

    Slice slice(blob->getBlob(), blob->getBlobLen());

    DataTypeSerDe::FormatOptions format_options;
    format_options.converted_from_string = true;
    format_options.escape_char = '\\';

    return deserialize_one_cell_from_json(column, slice, format_options);
}

Status DataTypeSerDe::deserialize_column_from_jsonb_vector(ColumnNullable& column_to,
                                                           const ColumnString& col_from_json,
                                                           CastParameters& castParms) const {
    const size_t size = col_from_json.size();
    const bool is_strict = castParms.is_strict;
    for (size_t i = 0; i < size; ++i) {
        const auto& val = col_from_json.get_data_at(i);
        const auto* value = handle_jsonb_value(val);
        if (!value) {
            column_to.insert_default();
            continue;
        }
        Status from_st =
                deserialize_column_from_jsonb(column_to.get_nested_column(), value, castParms);

        if (from_st.ok()) {
            // fill not null if success
            column_to.get_null_map_data().push_back(0);
        } else {
            if (is_strict) {
                return from_st;
            } else {
                // fill null if fail
                column_to.insert_default();
            }
        }
    }
    return Status::OK();
}

void DataTypeSerDe::to_string_batch(const IColumn& column, ColumnString& column_to,
                                    const FormatOptions& options) const {
    const auto size = column.size();
    column_to.reserve(size);
    VectorBufferWriter write_buffer(column_to);
    for (size_t i = 0; i < size; ++i) {
        to_string(column, i, write_buffer, options);
        write_buffer.commit();
    }
}

void DataTypeSerDe::to_string(const IColumn& column, size_t row_num, BufferWritable& bw,
                              const FormatOptions& options) const {
    throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                           "Data type {} to_string_batch not implement.", get_name());
}

std::string DataTypeSerDe::to_olap_string(const Field& value) const {
    throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                           "Data type {} to_olap_string not implement.", get_name());
    return "";
}

bool DataTypeSerDe::write_column_to_mysql_text(const IColumn& column, BufferWritable& bw,
                                               int64_t row_idx,
                                               const FormatOptions& options) const {
    to_string(column, row_idx, bw, options);
    return true;
}

bool DataTypeSerDe::write_column_to_presto_text(const IColumn& column, BufferWritable& bw,
                                                int64_t row_idx,
                                                const FormatOptions& options) const {
    to_string(column, row_idx, bw, options);
    return true;
}

bool DataTypeSerDe::write_column_to_hive_text(const IColumn& column, BufferWritable& bw,
                                              int64_t row_idx, const FormatOptions& options) const {
    to_string(column, row_idx, bw, options);
    return true;
}

const std::string DataTypeSerDe::NULL_IN_COMPLEX_TYPE = "null";
const std::string DataTypeSerDe::NULL_IN_CSV_FOR_ORDINARY_TYPE = "\\N";

const uint8_t* DataTypeSerDe::deserialize_binary_to_column(const uint8_t* data, IColumn& column) {
    auto& nullable_column = assert_cast<ColumnNullable&, TypeCheckOnRelease::DISABLE>(column);
    const FieldType type = static_cast<FieldType>(*data++);
    const uint8_t* end = data;
    switch (type) {
#define HANDLE_SIMPLE_SERDE(FT, SERDE)                                                        \
    case FieldType::FT: {                                                                     \
        end = SERDE::deserialize_binary_to_column(data, nullable_column.get_nested_column()); \
        nullable_column.push_false_to_nullmap(1);                                             \
        break;                                                                                \
    }

#define HANDLE_T_NUM_SERDE(FT, TYPEID)                                   \
    case FieldType::FT: {                                                \
        end = DataTypeNumberSerDe<TYPEID>::deserialize_binary_to_column( \
                data, nullable_column.get_nested_column());              \
        nullable_column.push_false_to_nullmap(1);                        \
        break;                                                           \
    }

#define HANDLE_T_DEC_SERDE(FT, TYPEID)                                    \
    case FieldType::FT: {                                                 \
        end = DataTypeDecimalSerDe<TYPEID>::deserialize_binary_to_column( \
                data, nullable_column.get_nested_column());               \
        nullable_column.push_false_to_nullmap(1);                         \
        break;                                                            \
    }

        HANDLE_SIMPLE_SERDE(OLAP_FIELD_TYPE_STRING, DataTypeStringSerDe)
        HANDLE_T_NUM_SERDE(OLAP_FIELD_TYPE_TINYINT, TYPE_TINYINT)
        HANDLE_T_NUM_SERDE(OLAP_FIELD_TYPE_SMALLINT, TYPE_SMALLINT)
        HANDLE_T_NUM_SERDE(OLAP_FIELD_TYPE_INT, TYPE_INT)
        HANDLE_T_NUM_SERDE(OLAP_FIELD_TYPE_BIGINT, TYPE_BIGINT)
        HANDLE_T_NUM_SERDE(OLAP_FIELD_TYPE_LARGEINT, TYPE_LARGEINT)
        HANDLE_T_NUM_SERDE(OLAP_FIELD_TYPE_FLOAT, TYPE_FLOAT)
        HANDLE_T_NUM_SERDE(OLAP_FIELD_TYPE_DOUBLE, TYPE_DOUBLE)
        HANDLE_SIMPLE_SERDE(OLAP_FIELD_TYPE_JSONB, DataTypeJsonbSerDe)
        HANDLE_SIMPLE_SERDE(OLAP_FIELD_TYPE_ARRAY, DataTypeArraySerDe)
        HANDLE_T_NUM_SERDE(OLAP_FIELD_TYPE_IPV4, TYPE_IPV4)
        HANDLE_T_NUM_SERDE(OLAP_FIELD_TYPE_IPV6, TYPE_IPV6)
        HANDLE_T_NUM_SERDE(OLAP_FIELD_TYPE_DATEV2, TYPE_DATEV2)
        HANDLE_T_NUM_SERDE(OLAP_FIELD_TYPE_DATETIMEV2, TYPE_DATETIMEV2)
        HANDLE_T_DEC_SERDE(OLAP_FIELD_TYPE_DECIMAL32, TYPE_DECIMAL32)
        HANDLE_T_DEC_SERDE(OLAP_FIELD_TYPE_DECIMAL64, TYPE_DECIMAL64)
        HANDLE_T_DEC_SERDE(OLAP_FIELD_TYPE_DECIMAL128I, TYPE_DECIMAL128I)
        HANDLE_T_DEC_SERDE(OLAP_FIELD_TYPE_DECIMAL256, TYPE_DECIMAL256)
        HANDLE_T_NUM_SERDE(OLAP_FIELD_TYPE_BOOL, TYPE_BOOLEAN)
        HANDLE_T_NUM_SERDE(OLAP_FIELD_TYPE_TIMESTAMPTZ, TYPE_TIMESTAMPTZ)

    case FieldType::OLAP_FIELD_TYPE_NONE: {
        end = data;
        nullable_column.insert_default();
        break;
    }
    default:
        throw doris::Exception(ErrorCode::OUT_OF_BOUND,
                               "Type ({}) for deserialize_binary_to_column is invalid", type);
    }

#undef HANDLE_T_DEC_SERDE
#undef HANDLE_T_NUM_SERDE
#undef HANDLE_SIMPLE_SERDE

    return end;
}

const uint8_t* DataTypeSerDe::deserialize_binary_to_field(const uint8_t* data, Field& field,
                                                          FieldInfo& info) {
    const FieldType type = static_cast<FieldType>(*data++);
    info.scalar_type_id = storage_field_type_to_primitive_type(type);
    const uint8_t* end = data;
    switch (type) {
#define HANDLE_SIMPLE_SERDE(FT, SERDE)                               \
    case FieldType::FT: {                                            \
        end = SERDE::deserialize_binary_to_field(data, field, info); \
        break;                                                       \
    }

#define HANDLE_T_NUM_SERDE(FT, TYPEID)                                                     \
    case FieldType::FT: {                                                                  \
        end = DataTypeNumberSerDe<TYPEID>::deserialize_binary_to_field(data, field, info); \
        break;                                                                             \
    }

#define HANDLE_T_DEC_SERDE(FT, TYPEID)                                                      \
    case FieldType::FT: {                                                                   \
        end = DataTypeDecimalSerDe<TYPEID>::deserialize_binary_to_field(data, field, info); \
        break;                                                                              \
    }

        HANDLE_SIMPLE_SERDE(OLAP_FIELD_TYPE_STRING, DataTypeStringSerDe)
        HANDLE_T_NUM_SERDE(OLAP_FIELD_TYPE_TINYINT, TYPE_TINYINT)
        HANDLE_T_NUM_SERDE(OLAP_FIELD_TYPE_SMALLINT, TYPE_SMALLINT)
        HANDLE_T_NUM_SERDE(OLAP_FIELD_TYPE_INT, TYPE_INT)
        HANDLE_T_NUM_SERDE(OLAP_FIELD_TYPE_BIGINT, TYPE_BIGINT)
        HANDLE_T_NUM_SERDE(OLAP_FIELD_TYPE_LARGEINT, TYPE_LARGEINT)
        HANDLE_T_NUM_SERDE(OLAP_FIELD_TYPE_FLOAT, TYPE_FLOAT)
        HANDLE_T_NUM_SERDE(OLAP_FIELD_TYPE_DOUBLE, TYPE_DOUBLE)
        HANDLE_SIMPLE_SERDE(OLAP_FIELD_TYPE_JSONB, DataTypeJsonbSerDe)
        HANDLE_SIMPLE_SERDE(OLAP_FIELD_TYPE_ARRAY, DataTypeArraySerDe)
        HANDLE_T_NUM_SERDE(OLAP_FIELD_TYPE_IPV4, TYPE_IPV4)
        HANDLE_T_NUM_SERDE(OLAP_FIELD_TYPE_IPV6, TYPE_IPV6)
        HANDLE_T_NUM_SERDE(OLAP_FIELD_TYPE_DATEV2, TYPE_DATEV2)
        HANDLE_T_NUM_SERDE(OLAP_FIELD_TYPE_DATETIMEV2, TYPE_DATETIMEV2)
        HANDLE_T_DEC_SERDE(OLAP_FIELD_TYPE_DECIMAL32, TYPE_DECIMAL32)
        HANDLE_T_DEC_SERDE(OLAP_FIELD_TYPE_DECIMAL64, TYPE_DECIMAL64)
        HANDLE_T_DEC_SERDE(OLAP_FIELD_TYPE_DECIMAL128I, TYPE_DECIMAL128I)
        HANDLE_T_DEC_SERDE(OLAP_FIELD_TYPE_DECIMAL256, TYPE_DECIMAL256)
        HANDLE_T_NUM_SERDE(OLAP_FIELD_TYPE_BOOL, TYPE_BOOLEAN)
        HANDLE_T_NUM_SERDE(OLAP_FIELD_TYPE_TIMESTAMPTZ, TYPE_TIMESTAMPTZ)

    case FieldType::OLAP_FIELD_TYPE_NONE: {
        end = data;
        break;
    }
    default:
        throw doris::Exception(ErrorCode::OUT_OF_BOUND,
                               "Type ({}) for deserialize_binary_to_field is invalid", type);
    }

#undef HANDLE_T_DEC_SERDE
#undef HANDLE_T_NUM_SERDE
#undef HANDLE_SIMPLE_SERDE
    return end;
}

} // namespace doris
