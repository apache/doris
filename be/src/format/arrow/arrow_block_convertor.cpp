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

#include "format/arrow/arrow_block_convertor.h"

#include <arrow/array/builder_base.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_decimal.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/util/decimal.h>
#include <arrow/util/key_value_metadata.h>
#include <arrow/visit_type_inline.h>
#include <arrow/visitor.h>
#include <cctz/time_zone.h>
#include <glog/logging.h>

#include <array>
#include <cstring>
#include <ctime>
#include <memory>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/value/vdatetime_value.h"
#include "format/arrow/arrow_row_batch.h"
#include "format/arrow/arrow_utils.h"

namespace arrow {
class Array;
} // namespace arrow

namespace doris {

namespace {

constexpr const char* ICEBERG_ORIGINAL_TYPE_KEY = "originalType";
constexpr const char* ICEBERG_UUID_TYPE_VALUE = "uuid";

bool is_iceberg_uuid_field(const std::shared_ptr<arrow::Field>& field) {
    if (field == nullptr || !field->HasMetadata()) {
        return false;
    }
    const auto result = field->metadata()->Get(ICEBERG_ORIGINAL_TYPE_KEY);
    return result.ok() && result.ValueUnsafe() == ICEBERG_UUID_TYPE_VALUE;
}

int hex_value(char c) {
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
    }
    return -1;
}

Status parse_uuid_to_bytes(StringRef uuid, std::array<uint8_t, 16>* bytes) {
    if (uuid.size == 16) {
        std::memcpy(bytes->data(), uuid.data, bytes->size());
        return Status::OK();
    }
    if (uuid.size != 32 && uuid.size != 36) {
        return Status::InvalidArgument("Invalid UUID string length: {}", uuid.size);
    }

    int hex_count = 0;
    int high_nibble = -1;
    int byte_index = 0;
    for (size_t i = 0; i < uuid.size; ++i) {
        char c = uuid.data[i];
        if (uuid.size == 36 && (i == 8 || i == 13 || i == 18 || i == 23)) {
            if (c != '-') {
                return Status::InvalidArgument("Invalid UUID string format");
            }
            continue;
        }
        if (c == '-') {
            return Status::InvalidArgument("Invalid UUID string format");
        }

        int value = hex_value(c);
        if (value < 0) {
            return Status::InvalidArgument("Invalid UUID string format");
        }
        if (hex_count % 2 == 0) {
            high_nibble = value;
        } else {
            (*bytes)[byte_index++] = static_cast<uint8_t>((high_nibble << 4) | value);
        }
        ++hex_count;
    }

    if (hex_count != 32 || byte_index != 16) {
        return Status::InvalidArgument("Invalid UUID string format");
    }
    return Status::OK();
}

Status write_iceberg_uuid_string_column_to_arrow(const IColumn& column, const DataTypePtr& type,
                                                 arrow::ArrayBuilder* array_builder, int64_t start,
                                                 int64_t end) {
    if (array_builder->type()->id() != arrow::Type::FIXED_SIZE_BINARY) {
        return Status::InvalidArgument("Iceberg UUID must be written to fixed size binary");
    }
    const int byte_width =
            static_cast<const arrow::FixedSizeBinaryType&>(*array_builder->type()).byte_width();
    if (byte_width != 16) {
        return Status::InvalidArgument("Iceberg UUID expects 16 bytes, got {}", byte_width);
    }

    auto& builder = assert_cast<arrow::FixedSizeBinaryBuilder&>(*array_builder);
    const IColumn* data_column = &column;
    const NullMap* null_map = nullptr;
    if (type->is_nullable()) {
        const auto& nullable_column = assert_cast<const ColumnNullable&>(column);
        data_column = &nullable_column.get_nested_column();
        null_map = &nullable_column.get_null_map_data();
    }
    if (!data_column->is_column_string()) {
        return Status::InvalidArgument(
                "Iceberg UUID string conversion expects string column, got {}",
                data_column->get_name());
    }

    const auto& string_column = assert_cast<const ColumnString&>(*data_column);
    for (size_t row = start; row < end; ++row) {
        if (null_map != nullptr && (*null_map)[row]) {
            RETURN_IF_ERROR(checkArrowStatus(builder.AppendNull(), column, builder));
            continue;
        }
        std::array<uint8_t, 16> bytes;
        RETURN_IF_ERROR(parse_uuid_to_bytes(string_column.get_data_at(row), &bytes));
        RETURN_IF_ERROR(checkArrowStatus(builder.Append(bytes.data()), column, builder));
    }
    return Status::OK();
}

} // namespace

Status FromBlockToRecordBatchConverter::convert(std::shared_ptr<arrow::RecordBatch>* out) {
    int num_fields = _schema->num_fields();
    if (_block.columns() != num_fields) {
        return Status::InvalidArgument("number fields not match");
    }

    // Calculate actual row range to convert
    size_t actual_start = _row_range_start;
    size_t actual_rows = _row_range_end > 0 ? (_row_range_end - _row_range_start)
                                            : (_block.rows() - _row_range_start);

    // Validate range
    if (actual_start + actual_rows > _block.rows()) {
        return Status::InvalidArgument(
                "Row range out of bounds: start={}, num_rows={}, block_rows={}", actual_start,
                actual_rows, _block.rows());
    }

    _arrays.resize(num_fields);

    for (int idx = 0; idx < num_fields; ++idx) {
        _cur_field_idx = idx;
        _cur_start = actual_start;
        _cur_rows = actual_rows;
        _cur_col = _block.get_by_position(idx).column;
        _cur_type = _block.get_by_position(idx).type;
        auto column = _cur_col->convert_to_full_column_if_const();
        auto arrow_type = _schema->field(idx)->type();
        if (arrow_type->id() == arrow::Type::STRING && column->byte_size() >= MAX_ARROW_UTF8) {
            arrow_type = arrow::large_utf8();
        } else if (arrow_type->id() == arrow::Type::BINARY &&
                   column->byte_size() >= MAX_ARROW_UTF8) {
            arrow_type = arrow::large_binary();
        }
        std::unique_ptr<arrow::ArrayBuilder> builder;
        auto arrow_st = arrow::MakeBuilder(_pool, arrow_type, &builder);
        if (!arrow_st.ok()) {
            return to_doris_status(arrow_st);
        }
        _cur_builder = builder.get();
        try {
            if (is_iceberg_uuid_field(_schema->field(idx)) &&
                is_string_type(remove_nullable(_cur_type)->get_primitive_type())) {
                RETURN_IF_ERROR(write_iceberg_uuid_string_column_to_arrow(
                        *column, _cur_type, _cur_builder, _cur_start, _cur_start + _cur_rows));
            } else {
                RETURN_IF_ERROR(_cur_type->get_serde()->write_column_to_arrow(
                        *column, nullptr, _cur_builder, _cur_start, _cur_start + _cur_rows,
                        _timezone_obj));
            }
        } catch (std::exception& e) {
            return Status::InternalError(
                    "Fail to convert block data to arrow data, type: {}, name: {}, error: {}",
                    _cur_type->get_name(), _block.get_by_position(idx).name, e.what());
        }
        arrow_st = _cur_builder->Finish(&_arrays[_cur_field_idx]);
        if (!arrow_st.ok()) {
            return to_doris_status(arrow_st);
        }
    }
    *out = arrow::RecordBatch::Make(_schema, actual_rows, std::move(_arrays));
    return Status::OK();
}

Status FromRecordBatchToBlockConverter::convert(Block* block) {
    DCHECK(block);
    int num_fields = _batch->num_columns();
    if ((size_t)num_fields != _types.size()) {
        return Status::InvalidArgument("number fields not match");
    }

    int64_t num_rows = _batch->num_rows();
    _columns.reserve(num_fields);

    for (int idx = 0; idx < num_fields; ++idx) {
        auto doris_type = _types[idx];
        auto doris_column = doris_type->create_column();
        auto arrow_column = _batch->column(idx);
        DCHECK_EQ(arrow_column->length(), num_rows);
        RETURN_IF_ERROR(doris_type->get_serde()->read_column_from_arrow(
                *doris_column, &*arrow_column, 0, num_rows, _timezone_obj));
        _columns.emplace_back(std::move(doris_column), std::move(doris_type), std::to_string(idx));
    }

    block->swap(_columns);
    return Status::OK();
}

Status convert_to_arrow_batch(const Block& block, const std::shared_ptr<arrow::Schema>& schema,
                              arrow::MemoryPool* pool, std::shared_ptr<arrow::RecordBatch>* result,
                              const cctz::time_zone& timezone_obj) {
    FromBlockToRecordBatchConverter converter(block, schema, pool, timezone_obj);
    return converter.convert(result);
}

Status convert_to_arrow_batch(const Block& block, const std::shared_ptr<arrow::Schema>& schema,
                              arrow::MemoryPool* pool, std::shared_ptr<arrow::RecordBatch>* result,
                              const cctz::time_zone& timezone_obj, size_t start_row,
                              size_t end_row) {
    FromBlockToRecordBatchConverter converter(block, schema, pool, timezone_obj, start_row,
                                              end_row);
    return converter.convert(result);
}

Status make_zero_column_arrow_batch(const std::shared_ptr<arrow::Schema>& schema, int64_t rows,
                                    std::shared_ptr<arrow::RecordBatch>* result) {
    if (schema->num_fields() != 0) {
        return Status::InvalidArgument("schema should have no fields for zero column batch");
    }
    *result = arrow::RecordBatch::Make(schema, rows, std::vector<std::shared_ptr<arrow::Array>> {});
    return Status::OK();
}

Status convert_from_arrow_batch(const std::shared_ptr<arrow::RecordBatch>& batch,
                                const DataTypes& types, Block* block,
                                const cctz::time_zone& timezone_obj) {
    FromRecordBatchToBlockConverter converter(batch, types, timezone_obj);
    return converter.convert(block);
}

} // namespace doris
