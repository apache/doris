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
#include <arrow/array/util.h>
#include <arrow/extension_type.h>
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
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "core/value/vdatetime_value.h"
#include "format/arrow/arrow_row_batch.h"
#include "format/arrow/arrow_utils.h"
#include "util/timezone_utils.h"

namespace arrow {
class Array;
} // namespace arrow

namespace doris {

namespace {

std::shared_ptr<arrow::DataType> arrow_storage_type(const std::shared_ptr<arrow::DataType>& type) {
    switch (type->id()) {
    case arrow::Type::EXTENSION:
        return arrow_storage_type(static_cast<const arrow::ExtensionType&>(*type).storage_type());
    case arrow::Type::LIST: {
        const auto& list = assert_cast<const arrow::ListType&>(*type);
        return arrow::list(list.value_field()->WithType(arrow_storage_type(list.value_type())));
    }
    case arrow::Type::MAP: {
        const auto& map = assert_cast<const arrow::MapType&>(*type);
        return std::make_shared<arrow::MapType>(
                map.key_field()->WithType(arrow_storage_type(map.key_type())),
                map.item_field()->WithType(arrow_storage_type(map.item_type())), map.keys_sorted());
    }
    case arrow::Type::STRUCT: {
        arrow::FieldVector fields;
        fields.reserve(type->num_fields());
        for (const auto& field : type->fields()) {
            fields.push_back(field->WithType(arrow_storage_type(field->type())));
        }
        return arrow::struct_(fields);
    }
    default:
        return type;
    }
}

std::shared_ptr<arrow::ArrayData> restore_arrow_logical_type(
        const std::shared_ptr<arrow::ArrayData>& storage,
        const std::shared_ptr<arrow::DataType>& logical_type) {
    if (storage->type->Equals(logical_type)) {
        return storage;
    }
    auto result = storage->Copy();
    result->type = logical_type;
    DORIS_CHECK_EQ(result->child_data.size(), logical_type->num_fields());
    for (int index = 0; index < logical_type->num_fields(); ++index) {
        result->child_data[index] = restore_arrow_logical_type(result->child_data[index],
                                                               logical_type->field(index)->type());
    }
    return result;
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

bool is_declared_timestamp_binding(PrimitiveType primitive,
                                   const arrow::TimestampType& plain_timestamp,
                                   const arrow::TimestampType& target_timestamp) {
    if (plain_timestamp.unit() != target_timestamp.unit()) {
        return false;
    }
    // A timezone-free Arrow timestamp is a wall-clock value; TIMESTAMPTZ must retain its zone.
    if (target_timestamp.timezone().empty()) {
        return primitive == TYPE_DATETIMEV2;
    }
    cctz::time_zone target_timezone;
    return TimezoneUtils::find_cctz_time_zone(target_timestamp.timezone(), target_timezone) &&
           target_timezone.name() == plain_timestamp.timezone();
}

// `type` is the Doris logical type; `plain_arrow_type` is its ordinary SerDe mapping;
// `target_type` is the Arrow representation requested by the consumer. For example,
// DATETIMEV2(6) may bind timestamp(us) without a timezone, but TIMESTAMPTZ(6) must
// retain a timezone to preserve instant semantics. Nested bindings obey the same rule.
bool is_declared_plain_arrow_binding(const DataTypePtr& type,
                                     const std::shared_ptr<arrow::DataType>& plain_arrow_type,
                                     const std::shared_ptr<arrow::DataType>& target_type) {
    if (plain_arrow_type->Equals(target_type)) {
        return true;
    }
    if (plain_arrow_type->id() == arrow::Type::TIMESTAMP &&
        target_type->id() == arrow::Type::TIMESTAMP) {
        const auto& plain_timestamp = assert_cast<const arrow::TimestampType&>(*plain_arrow_type);
        const auto& target_timestamp = assert_cast<const arrow::TimestampType&>(*target_type);
        return is_declared_timestamp_binding(remove_nullable(type)->get_primitive_type(),
                                             plain_timestamp, target_timestamp);
    }
    const PrimitiveType primitive = remove_nullable(type)->get_primitive_type();
    if (primitive == TYPE_ARRAY && plain_arrow_type->id() == arrow::Type::LIST &&
        target_type->id() == arrow::Type::LIST) {
        const auto& array = assert_cast<const DataTypeArray&>(*remove_nullable(type));
        const auto& plain_list = assert_cast<const arrow::ListType&>(*plain_arrow_type);
        const auto& target_list = assert_cast<const arrow::ListType&>(*target_type);
        return plain_list.value_field()
                       ->WithType(target_list.value_type())
                       ->Equals(target_list.value_field()) &&
               is_declared_plain_arrow_binding(array.get_nested_type(), plain_list.value_type(),
                                               target_list.value_type());
    }
    if (primitive == TYPE_MAP && plain_arrow_type->id() == arrow::Type::MAP &&
        target_type->id() == arrow::Type::MAP) {
        const auto& map = assert_cast<const DataTypeMap&>(*remove_nullable(type));
        const auto& plain_map = assert_cast<const arrow::MapType&>(*plain_arrow_type);
        const auto& target_map = assert_cast<const arrow::MapType&>(*target_type);
        return plain_map.keys_sorted() == target_map.keys_sorted() &&
               plain_map.key_field()
                       ->WithType(target_map.key_type())
                       ->Equals(target_map.key_field()) &&
               plain_map.item_field()
                       ->WithType(target_map.item_type())
                       ->Equals(target_map.item_field()) &&
               is_declared_plain_arrow_binding(map.get_key_type(), plain_map.key_type(),
                                               target_map.key_type()) &&
               is_declared_plain_arrow_binding(map.get_value_type(), plain_map.item_type(),
                                               target_map.item_type());
    }
    if (primitive == TYPE_STRUCT && plain_arrow_type->id() == arrow::Type::STRUCT &&
        target_type->id() == arrow::Type::STRUCT) {
        const auto& structure = assert_cast<const DataTypeStruct&>(*remove_nullable(type));
        if (plain_arrow_type->num_fields() != target_type->num_fields() ||
            structure.get_elements().size() != static_cast<size_t>(target_type->num_fields())) {
            return false;
        }
        for (int i = 0; i < target_type->num_fields(); ++i) {
            const auto& plain_field = plain_arrow_type->field(i);
            const auto& target_field = target_type->field(i);
            if (!plain_field->WithType(target_field->type())->Equals(target_field) ||
                !is_declared_plain_arrow_binding(structure.get_element(i), plain_field->type(),
                                                 target_field->type())) {
                return false;
            }
        }
        return true;
    }
    if (is_string_type(primitive)) {
        return target_type->id() == arrow::Type::STRING ||
               target_type->id() == arrow::Type::LARGE_STRING ||
               target_type->id() == arrow::Type::BINARY ||
               target_type->id() == arrow::Type::LARGE_BINARY;
    }
    if (primitive == TYPE_VARBINARY) {
        return target_type->id() == arrow::Type::STRING ||
               target_type->id() == arrow::Type::BINARY ||
               target_type->id() == arrow::Type::LARGE_BINARY;
    }
    if (primitive == TYPE_VARIANT) {
        return target_type->id() == arrow::Type::STRING ||
               target_type->id() == arrow::Type::LARGE_STRING;
    }
    return false;
}

} // namespace

Status parse_iceberg_uuid_to_bytes(StringRef uuid, std::array<uint8_t, 16>* bytes) {
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

Status ArrowBlockConvertor::write_plain_arrow_column(const std::shared_ptr<const IDataType>& type,
                                                     const DataTypeSerDe& serde,
                                                     const IColumn& column, const NullMap* null_map,
                                                     const std::shared_ptr<arrow::Field>& field,
                                                     arrow::ArrayBuilder* array_builder,
                                                     int64_t start, int64_t end,
                                                     const cctz::time_zone& ctz) const {
    std::shared_ptr<arrow::DataType> plain_arrow_type;
    RETURN_IF_ERROR(convert_to_arrow_type(type, &plain_arrow_type, ctz.name()));
    // This is an exact binding check selected by the target converter, not a recovery path. A
    // mismatch returns without invoking SerDe, and a SerDe error is never retried elsewhere.
    if (!is_declared_plain_arrow_binding(type, plain_arrow_type, field->type())) {
        return Status::InvalidArgument(
                "Plain Arrow writer is not bound for Doris type {} and Arrow field {}",
                type->get_name(), field->ToString());
    }
    return serde.write_column_to_arrow(column, null_map, array_builder, start, end, ctz);
}

Status DorisArrowBlockConvertor::write_column(const std::shared_ptr<const IDataType>& type,
                                              const DataTypeSerDe& serde, const IColumn& column,
                                              const NullMap* null_map,
                                              const std::shared_ptr<arrow::Field>& field,
                                              arrow::ArrayBuilder* array_builder, int64_t start,
                                              int64_t end, const cctz::time_zone& ctz) const {
    return write_plain_arrow_column(type, serde, column, null_map, field, array_builder, start, end,
                                    ctz);
}

Status ArrowBlockConvertor::init() {
    if (_arrow_schema == nullptr) {
        return Status::InvalidArgument("Arrow converter schema is not initialized");
    }
    return Status::OK();
}

Status DorisArrowBlockConvertor::init() {
    if (_arrow_schema == nullptr) {
        // cctz names fixed offsets as "Fixed/UTC+HH:MM:SS", which is not the Arrow
        // protocol label. Keep the declared name so Python metadata and batches agree.
        RETURN_IF_ERROR(get_arrow_schema_from_block(_header, &_arrow_schema, _timezone_name,
                                                    _datetime_naive));
    }
    return ArrowBlockConvertor::init();
}

Status ArrowBlockConvertor::convert_from_arrow(const std::shared_ptr<arrow::RecordBatch>& batch,
                                               const DataTypes& types, Block* block) const {
    // Iceberg and Paimon physical layouts are not the generic Arrow SerDe read contract.
    return Status::NotSupported("This Arrow converter does not support reading");
}

Status ArrowBlockConvertor::convert_to_arrow(const Block& block, arrow::MemoryPool* pool,
                                             std::shared_ptr<arrow::RecordBatch>* out,
                                             size_t start_row, size_t end_row) const {
    if (_arrow_schema == nullptr) {
        return Status::InvalidArgument("Arrow converter schema is not initialized");
    }
    const auto& schema = _arrow_schema;
    int num_fields = schema->num_fields();
    if (block.columns() != num_fields) {
        return Status::InvalidArgument("number fields not match");
    }
    const size_t actual_end = end_row == 0 ? block.rows() : end_row;
    // Validate endpoints before subtraction: an inverted unsigned range otherwise wraps.
    if (start_row > actual_end || actual_end > block.rows()) {
        return Status::InvalidArgument("Row range out of bounds: start={}, end={}, block_rows={}",
                                       start_row, actual_end, block.rows());
    }
    const size_t actual_rows = actual_end - start_row;
    std::vector<std::shared_ptr<arrow::Array>> arrays(num_fields);
    for (int idx = 0; idx < num_fields; ++idx) {
        const auto& entry = block.get_by_position(idx);
        auto column = entry.column->convert_to_full_column_if_const();
        auto builder_arrow_type = schema->field(idx)->type();
        if (builder_arrow_type->id() == arrow::Type::STRING &&
            column->byte_size() >= MAX_ARROW_UTF8) {
            builder_arrow_type = arrow::large_utf8();
        } else if (builder_arrow_type->id() == arrow::Type::BINARY &&
                   column->byte_size() >= MAX_ARROW_UTF8) {
            builder_arrow_type = arrow::large_binary();
        }
        std::unique_ptr<arrow::ArrayBuilder> builder;
        auto arrow_st = arrow::MakeBuilder(pool, arrow_storage_type(builder_arrow_type), &builder);
        if (!arrow_st.ok()) {
            return to_doris_status(arrow_st);
        }
        try {
            const auto serde = entry.type->get_serde();
            RETURN_IF_ERROR(write_column(entry.type, *serde, *column, nullptr, schema->field(idx),
                                         builder.get(), start_row, actual_end, _timezone));
        } catch (std::exception& e) {
            return Status::InternalError(
                    "Fail to convert block data to arrow data, type: {}, name: {}, error: {}",
                    entry.type->get_name(), entry.name, e.what());
        }
        std::shared_ptr<arrow::Array> storage_array;
        arrow_st = builder->Finish(&storage_array);
        if (!arrow_st.ok()) {
            return to_doris_status(arrow_st);
        }
        arrays[idx] = arrow::MakeArray(
                restore_arrow_logical_type(storage_array->data(), builder_arrow_type));
    }
    *out = arrow::RecordBatch::Make(schema, actual_rows, std::move(arrays));
    return Status::OK();
}

Status DorisArrowBlockConvertor::convert_from_arrow(
        const std::shared_ptr<arrow::RecordBatch>& batch, const DataTypes& types,
        Block* block) const {
    DCHECK(block);
    int num_fields = batch->num_columns();
    if ((size_t)num_fields != types.size()) {
        return Status::InvalidArgument("number fields not match");
    }
    int64_t num_rows = batch->num_rows();
    ColumnsWithTypeAndName columns;
    columns.reserve(num_fields);
    for (int idx = 0; idx < num_fields; ++idx) {
        auto doris_type = types[idx];
        auto doris_column = doris_type->create_column();
        auto arrow_column = batch->column(idx);
        DCHECK_EQ(arrow_column->length(), num_rows);
        RETURN_IF_ERROR(doris_type->get_serde()->read_column_from_arrow(
                *doris_column, &*arrow_column, 0, num_rows, _timezone));
        columns.emplace_back(std::move(doris_column), std::move(doris_type), std::to_string(idx));
    }
    block->swap(columns);
    return Status::OK();
}

Status make_zero_column_arrow_batch(const std::shared_ptr<arrow::Schema>& schema, int64_t rows,
                                    std::shared_ptr<arrow::RecordBatch>* result) {
    if (schema->num_fields() != 0) {
        return Status::InvalidArgument("schema should have no fields for zero column batch");
    }
    *result = arrow::RecordBatch::Make(schema, rows, std::vector<std::shared_ptr<arrow::Array>> {});
    return Status::OK();
}

} // namespace doris
