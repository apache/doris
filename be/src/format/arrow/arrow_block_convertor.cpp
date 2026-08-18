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

#include <ctime>
#include <memory>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
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
#include "common/compile_check_begin.h"

namespace {

bool contains_extension_type(const std::shared_ptr<arrow::DataType>& type) {
    if (type->id() == arrow::Type::EXTENSION) {
        return true;
    }
    for (const auto& field : type->fields()) {
        if (contains_extension_type(field->type())) {
            return true;
        }
    }
    return false;
}

std::shared_ptr<arrow::DataType> extension_storage_type(
        const std::shared_ptr<arrow::DataType>& type) {
    switch (type->id()) {
    case arrow::Type::EXTENSION: {
        const auto& extension = static_cast<const arrow::ExtensionType&>(*type);
        return extension_storage_type(extension.storage_type());
    }
    case arrow::Type::LIST: {
        const auto& list = assert_cast<const arrow::ListType&>(*type);
        return std::make_shared<arrow::ListType>(
                list.value_field()->WithType(extension_storage_type(list.value_type())));
    }
    case arrow::Type::MAP: {
        const auto& map = assert_cast<const arrow::MapType&>(*type);
        return std::make_shared<arrow::MapType>(
                map.key_field()->WithType(extension_storage_type(map.key_type())),
                map.item_field()->WithType(extension_storage_type(map.item_type())),
                map.keys_sorted());
    }
    case arrow::Type::STRUCT: {
        std::vector<std::shared_ptr<arrow::Field>> fields;
        fields.reserve(type->num_fields());
        for (const auto& field : type->fields()) {
            fields.push_back(field->WithType(extension_storage_type(field->type())));
        }
        return arrow::struct_(std::move(fields));
    }
    default:
        return type;
    }
}

bool is_declared_canonical_binding(const DataTypePtr& type,
                                   const std::shared_ptr<arrow::DataType>& canonical_type,
                                   const std::shared_ptr<arrow::DataType>& target_type) {
    if (canonical_type->Equals(target_type)) {
        return true;
    }
    const PrimitiveType primitive = remove_nullable(type)->get_primitive_type();
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

Status wrap_extension_arrays(const std::shared_ptr<arrow::DataType>& target_type,
                             const std::shared_ptr<arrow::Array>& storage_array,
                             std::shared_ptr<arrow::Array>* result) {
    if (target_type->id() == arrow::Type::EXTENSION) {
        const auto& extension = static_cast<const arrow::ExtensionType&>(*target_type);
        std::shared_ptr<arrow::Array> normalized_storage;
        RETURN_IF_ERROR(wrap_extension_arrays(extension.storage_type(), storage_array,
                                              &normalized_storage));
        if (!extension.storage_type()->Equals(normalized_storage->type())) {
            return Status::InvalidArgument(
                    "Arrow extension storage type mismatch: expected {}, got {}",
                    extension.storage_type()->ToString(), normalized_storage->type()->ToString());
        }
        *result = arrow::ExtensionType::WrapArray(target_type, normalized_storage);
        return Status::OK();
    }

    if (target_type->num_fields() == 0) {
        if (!target_type->Equals(storage_array->type())) {
            return Status::InvalidArgument("Arrow storage type mismatch: expected {}, got {}",
                                           target_type->ToString(),
                                           storage_array->type()->ToString());
        }
        *result = storage_array;
        return Status::OK();
    }

    const auto& storage_data = storage_array->data();
    if (target_type->num_fields() != static_cast<int>(storage_data->child_data.size())) {
        return Status::InvalidArgument(
                "Arrow nested storage child count mismatch for {}: expected {}, got {}",
                target_type->ToString(), target_type->num_fields(),
                storage_data->child_data.size());
    }

    auto target_data = storage_data->Copy();
    target_data->type = target_type;
    for (int i = 0; i < target_type->num_fields(); ++i) {
        std::shared_ptr<arrow::Array> child;
        RETURN_IF_ERROR(wrap_extension_arrays(target_type->field(i)->type(),
                                              arrow::MakeArray(storage_data->child_data[i]),
                                              &child));
        target_data->child_data[i] = child->data();
    }
    *result = arrow::MakeArray(std::move(target_data));
    return Status::OK();
}

} // namespace

Status ArrowWriteConverter::write_type_serde_column(const std::shared_ptr<const IDataType>& type,
                                                    const DataTypeSerDe& serde,
                                                    const IColumn& column, const NullMap* null_map,
                                                    const std::shared_ptr<arrow::Field>& field,
                                                    arrow::ArrayBuilder* array_builder,
                                                    int64_t start, int64_t end,
                                                    const cctz::time_zone& ctz) const {
    const auto storage_field = field->WithType(extension_storage_type(field->type()));
    return serde.write_column_to_arrow(type, column, null_map, storage_field, array_builder, start,
                                       end, ctz, *this);
}

Status ArrowWriteConverter::write_canonical_column(const std::shared_ptr<const IDataType>& type,
                                                   const DataTypeSerDe& serde,
                                                   const IColumn& column, const NullMap* null_map,
                                                   const std::shared_ptr<arrow::Field>& field,
                                                   arrow::ArrayBuilder* array_builder,
                                                   int64_t start, int64_t end,
                                                   const cctz::time_zone& ctz) const {
    std::shared_ptr<arrow::DataType> canonical_type;
    RETURN_IF_ERROR(convert_to_arrow_type(type, &canonical_type, ctz.name()));
    const auto storage_type = extension_storage_type(field->type());
    // This is an exact binding check selected by the target converter, not a recovery path. A
    // mismatch returns without invoking SerDe, and a SerDe error is never retried elsewhere.
    if (!is_declared_canonical_binding(type, canonical_type, storage_type)) {
        return Status::InvalidArgument(
                "Canonical Arrow writer is not bound for Doris type {} and Arrow field {}",
                type->get_name(), field->ToString());
    }
    return write_type_serde_column(type, serde, column, null_map, field, array_builder, start, end,
                                   ctz);
}

namespace {

class CanonicalArrowWriteConverter final : public ArrowWriteConverter {
public:
    Status write_column(const std::shared_ptr<const IDataType>& type, const DataTypeSerDe& serde,
                        const IColumn& column, const NullMap* null_map,
                        const std::shared_ptr<arrow::Field>& field,
                        arrow::ArrayBuilder* array_builder, int64_t start, int64_t end,
                        const cctz::time_zone& ctz) const override {
        return write_canonical_column(type, serde, column, null_map, field, array_builder, start,
                                      end, ctz);
    }
};

} // namespace

const ArrowWriteConverter& canonical_arrow_write_converter() {
    static const CanonicalArrowWriteConverter converter;
    return converter;
}

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
        auto target_arrow_type = _schema->field(idx)->type();
        const bool has_extension = contains_extension_type(target_arrow_type);
        auto builder_arrow_type =
                has_extension ? extension_storage_type(target_arrow_type) : target_arrow_type;
        if (builder_arrow_type->id() == arrow::Type::STRING &&
            column->byte_size() >= MAX_ARROW_UTF8) {
            builder_arrow_type = arrow::large_utf8();
        } else if (builder_arrow_type->id() == arrow::Type::BINARY &&
                   column->byte_size() >= MAX_ARROW_UTF8) {
            builder_arrow_type = arrow::large_binary();
        }
        std::unique_ptr<arrow::ArrayBuilder> builder;
        auto arrow_st = arrow::MakeBuilder(_pool, builder_arrow_type, &builder);
        if (!arrow_st.ok()) {
            return to_doris_status(arrow_st);
        }
        _cur_builder = builder.get();
        try {
            const auto serde = _cur_type->get_serde();
            RETURN_IF_ERROR(_write_converter.write_column(
                    _cur_type, *serde, *column, nullptr, _schema->field(idx), _cur_builder,
                    _cur_start, _cur_start + _cur_rows, _timezone_obj));
        } catch (std::exception& e) {
            return Status::InternalError(
                    "Fail to convert block data to arrow data, type: {}, name: {}, error: {}",
                    _cur_type->get_name(), _block.get_by_position(idx).name, e.what());
        }
        std::shared_ptr<arrow::Array> storage_array;
        arrow_st = _cur_builder->Finish(&storage_array);
        if (!arrow_st.ok()) {
            return to_doris_status(arrow_st);
        }
        if (has_extension) {
            RETURN_IF_ERROR(wrap_extension_arrays(target_arrow_type, storage_array,
                                                  &_arrays[_cur_field_idx]));
        } else {
            _arrays[_cur_field_idx] = std::move(storage_array);
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
                              const cctz::time_zone& timezone_obj, size_t start_row, size_t end_row,
                              const ArrowWriteConverter& write_converter) {
    FromBlockToRecordBatchConverter converter(block, schema, pool, timezone_obj, start_row, end_row,
                                              write_converter);
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

#include "common/compile_check_end.h"
} // namespace doris
