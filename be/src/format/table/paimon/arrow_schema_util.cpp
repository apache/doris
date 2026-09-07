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

#include "format/table/paimon/arrow_schema_util.h"

#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <limits>
#include <vector>

#include "core/block/block.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "format/arrow/arrow_row_batch.h"

namespace doris::paimon {
#include "common/compile_check_begin.h"

namespace {

constexpr const char* PARQUET_FIELD_ID = "PARQUET:field_id";
constexpr int32_t STRUCTURED_TYPE_FIELD_ID_BASE = std::numeric_limits<int32_t>::max() / 4;
constexpr int32_t STRUCTURED_TYPE_FIELD_DEPTH_LIMIT = 1 << 10;

const schema::external::TField* get_field(const schema::external::TFieldPtr& field_ptr) {
    if (!field_ptr.__isset.field_ptr || field_ptr.field_ptr == nullptr) {
        return nullptr;
    }
    return field_ptr.field_ptr.get();
}

std::shared_ptr<arrow::KeyValueMetadata> field_id_metadata(int32_t field_id) {
    return arrow::KeyValueMetadata::Make({PARQUET_FIELD_ID}, {std::to_string(field_id)});
}

int32_t array_element_id(int32_t anchor_id, int32_t depth) {
    return STRUCTURED_TYPE_FIELD_ID_BASE + anchor_id * STRUCTURED_TYPE_FIELD_DEPTH_LIMIT + depth;
}

int32_t map_key_id(int32_t anchor_id, int32_t depth) {
    return STRUCTURED_TYPE_FIELD_ID_BASE - anchor_id * STRUCTURED_TYPE_FIELD_DEPTH_LIMIT - depth;
}

int32_t map_value_id(int32_t anchor_id, int32_t depth) {
    return STRUCTURED_TYPE_FIELD_ID_BASE + anchor_id * STRUCTURED_TYPE_FIELD_DEPTH_LIMIT + depth;
}

Status convert_field(const DataTypePtr& doris_type, const schema::external::TField& field,
                     const std::string& name, int32_t field_id, int32_t collection_anchor_id,
                     int32_t collection_depth, const std::string& timezone, bool force_required,
                     std::shared_ptr<arrow::Field>* result) {
    if (!field.__isset.type) {
        return Status::InvalidArgument("Paimon field '{}' is missing its type", name);
    }

    const auto nested_type = remove_nullable(doris_type);
    std::shared_ptr<arrow::DataType> arrow_type;
    switch (field.type.type) {
    case TPrimitiveType::ARRAY: {
        if (nested_type->get_primitive_type() != TYPE_ARRAY || !field.__isset.nestedField ||
            !field.nestedField.__isset.array_field ||
            !field.nestedField.array_field.__isset.item_field) {
            return Status::InvalidArgument("Invalid Paimon ARRAY schema for field '{}'", name);
        }
        const auto* item = get_field(field.nestedField.array_field.item_field);
        if (item == nullptr) {
            return Status::InvalidArgument("Paimon ARRAY field '{}' has no element", name);
        }
        const auto* array_type = assert_cast<const DataTypeArray*>(nested_type.get());
        const int32_t child_depth = collection_depth + 1;
        std::shared_ptr<arrow::Field> item_field;
        RETURN_IF_ERROR(convert_field(array_type->get_nested_type(), *item, "element",
                                      array_element_id(collection_anchor_id, child_depth),
                                      collection_anchor_id, child_depth, timezone, false,
                                      &item_field));
        arrow_type = arrow::list(std::move(item_field));
        break;
    }
    case TPrimitiveType::MAP: {
        if (nested_type->get_primitive_type() != TYPE_MAP || !field.__isset.nestedField ||
            !field.nestedField.__isset.map_field ||
            !field.nestedField.map_field.__isset.key_field ||
            !field.nestedField.map_field.__isset.value_field) {
            return Status::InvalidArgument("Invalid Paimon MAP schema for field '{}'", name);
        }
        const auto* key = get_field(field.nestedField.map_field.key_field);
        const auto* value = get_field(field.nestedField.map_field.value_field);
        if (key == nullptr || value == nullptr) {
            return Status::InvalidArgument("Paimon MAP field '{}' has incomplete children", name);
        }
        const auto* map_type = assert_cast<const DataTypeMap*>(nested_type.get());
        const int32_t child_depth = collection_depth + 1;
        std::shared_ptr<arrow::Field> key_field;
        std::shared_ptr<arrow::Field> value_field;
        RETURN_IF_ERROR(convert_field(map_type->get_key_type(), *key, "key",
                                      map_key_id(collection_anchor_id, child_depth),
                                      collection_anchor_id, child_depth, timezone, true,
                                      &key_field));
        RETURN_IF_ERROR(convert_field(map_type->get_value_type(), *value, "value",
                                      map_value_id(collection_anchor_id, child_depth),
                                      collection_anchor_id, child_depth, timezone, false,
                                      &value_field));
        arrow_type = std::make_shared<arrow::MapType>(std::move(key_field), std::move(value_field));
        break;
    }
    case TPrimitiveType::STRUCT: {
        if (nested_type->get_primitive_type() != TYPE_STRUCT || !field.__isset.nestedField ||
            !field.nestedField.__isset.struct_field ||
            !field.nestedField.struct_field.__isset.fields) {
            return Status::InvalidArgument("Invalid Paimon ROW schema for field '{}'", name);
        }
        const auto* struct_type = assert_cast<const DataTypeStruct*>(nested_type.get());
        const auto& children = field.nestedField.struct_field.fields;
        if (children.size() != struct_type->get_elements().size()) {
            return Status::InvalidArgument(
                    "Paimon ROW field '{}' child count {} does not match Doris type {}", name,
                    children.size(), struct_type->get_elements().size());
        }
        std::vector<std::shared_ptr<arrow::Field>> arrow_children;
        arrow_children.reserve(children.size());
        for (size_t i = 0; i < children.size(); ++i) {
            const auto* child = get_field(children[i]);
            if (child == nullptr || !child->__isset.id || !child->__isset.name) {
                return Status::InvalidArgument("Paimon ROW field '{}' has an invalid child", name);
            }
            std::shared_ptr<arrow::Field> arrow_child;
            RETURN_IF_ERROR(convert_field(struct_type->get_element(i), *child, child->name,
                                          child->id, child->id, 0, timezone, false, &arrow_child));
            arrow_children.emplace_back(std::move(arrow_child));
        }
        arrow_type = arrow::struct_(std::move(arrow_children));
        break;
    }
    case TPrimitiveType::VARIANT:
        return Status::NotSupported("Paimon VARIANT native write is a phase-two feature");
    case TPrimitiveType::VARBINARY:
        // Doris represents both STRING and VARBINARY columns with a string column at runtime.
        // Preserve Paimon's logical BINARY type explicitly instead of inferring UTF8 from the
        // runtime block type.
        arrow_type = arrow::binary();
        break;
    case TPrimitiveType::TIMESTAMPTZ:
    case TPrimitiveType::DATETIMEV2: {
        const int scale = field.type.__isset.scale ? field.type.scale : 0;
        const auto unit = scale <= 3 ? arrow::TimeUnit::MILLI : arrow::TimeUnit::MICRO;
        const std::string arrow_timezone =
                field.type.type == TPrimitiveType::TIMESTAMPTZ ? timezone : "";
        arrow_type = arrow::timestamp(unit, arrow_timezone);
        break;
    }
    default:
        RETURN_IF_ERROR(convert_to_arrow_type(doris_type, &arrow_type, timezone));
        break;
    }

    const bool nullable = !force_required && (!field.__isset.is_optional || field.is_optional);
    *result = arrow::field(name, std::move(arrow_type), nullable, field_id_metadata(field_id));
    return Status::OK();
}

} // namespace

Status ArrowSchemaUtil::convert(const schema::external::TSchema& schema, const Block& block,
                                const std::string& timezone,
                                std::shared_ptr<arrow::Schema>* arrow_schema) {
    if (!schema.__isset.root_field || !schema.root_field.__isset.fields) {
        return Status::InvalidArgument("Paimon native writer schema has no root fields");
    }
    if (schema.root_field.fields.size() != block.columns()) {
        return Status::InvalidArgument(
                "Paimon native writer schema columns {} do not match block columns {}",
                schema.root_field.fields.size(), block.columns());
    }

    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(block.columns());
    for (size_t i = 0; i < block.columns(); ++i) {
        const auto* field = get_field(schema.root_field.fields[i]);
        if (field == nullptr || !field->__isset.id || !field->__isset.name) {
            return Status::InvalidArgument("Paimon native writer has an invalid root field at {}",
                                           i);
        }
        std::shared_ptr<arrow::Field> arrow_field;
        RETURN_IF_ERROR(convert_field(block.get_by_position(i).type, *field, field->name, field->id,
                                      field->id, 0, timezone, false, &arrow_field));
        fields.emplace_back(std::move(arrow_field));
    }
    *arrow_schema = arrow::schema(std::move(fields));
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris::paimon
