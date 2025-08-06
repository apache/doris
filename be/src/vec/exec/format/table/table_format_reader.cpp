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

#include "table_format_reader.h"

#include <algorithm>
#include <string>

#include "common/status.h"
#include "gen_cpp/ExternalTableSchema_types.h"
#include "util/string_util.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/exec/format/generic_reader.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
const Status TableSchemaChangeHelper::BuildTableInfoUtil::SCHEMA_ERROR = Status::NotSupported(
        "In the parquet/orc reader, it is not possible to read scenarios where the complex column "
        "types"
        "of the table and the file are inconsistent.");

Status TableSchemaChangeHelper::BuildTableInfoUtil::by_parquet_name(
        const TupleDescriptor* table_tuple_descriptor, const FieldDescriptor& parquet_field_desc,
        std::shared_ptr<TableSchemaChangeHelper::Node>& node,
        const std::set<TSlotId>* is_file_slot) {
    auto struct_node = std::make_shared<TableSchemaChangeHelper::StructNode>();
    auto parquet_fields_schema = parquet_field_desc.get_fields_schema();
    std::map<std::string, size_t> file_column_name_idx_map;
    for (size_t idx = 0; idx < parquet_fields_schema.size(); idx++) {
        file_column_name_idx_map.emplace(to_lower(parquet_fields_schema[idx].name), idx);
    }

    for (const auto& slot : table_tuple_descriptor->slots()) {
        const auto& table_column_name = slot->col_name();
        // https://github.com/apache/doris/pull/23369/files
        if ((is_file_slot == nullptr || is_file_slot->contains(slot->id())) &&
            file_column_name_idx_map.contains(table_column_name)) {
            auto file_column_idx = file_column_name_idx_map[table_column_name];
            std::shared_ptr<TableSchemaChangeHelper::Node> field_node = nullptr;
            RETURN_IF_ERROR(by_parquet_name(slot->type(), parquet_fields_schema[file_column_idx],
                                            field_node));

            struct_node->add_children(table_column_name,
                                      parquet_fields_schema[file_column_idx].name, field_node);
        } else {
            struct_node->add_not_exist_children(table_column_name);
        }
    }

    node = struct_node;
    return Status::OK();
};

Status TableSchemaChangeHelper::BuildTableInfoUtil::by_parquet_name(
        const DataTypePtr& table_data_type, const FieldSchema& file_field,
        std::shared_ptr<TableSchemaChangeHelper::Node>& node) {
    switch (table_data_type->get_primitive_type()) {
    case TYPE_MAP: {
        if (file_field.data_type->get_primitive_type() != TYPE_MAP) [[unlikely]] {
            return SCHEMA_ERROR;
        }
        MOCK_REMOVE(DCHECK(file_field.children.size() == 1));
        MOCK_REMOVE(DCHECK(file_field.children[0].children.size() == 2));
        std::shared_ptr<TableSchemaChangeHelper::Node> key_node = nullptr;

        {
            const auto& key_type = assert_cast<const DataTypePtr&>(
                    assert_cast<const DataTypeMap*>(remove_nullable(table_data_type).get())
                            ->get_key_type());

            RETURN_IF_ERROR(
                    by_parquet_name(key_type, file_field.children[0].children[0], key_node));
        }

        std::shared_ptr<TableSchemaChangeHelper::Node> value_node = nullptr;
        {
            const auto& value_type = assert_cast<const DataTypePtr&>(
                    assert_cast<const DataTypeMap*>(remove_nullable(table_data_type).get())
                            ->get_value_type());

            RETURN_IF_ERROR(
                    by_parquet_name(value_type, file_field.children[0].children[1], value_node));
        }
        node = std::make_shared<TableSchemaChangeHelper::MapNode>(key_node, value_node);
        break;
    }
    case TYPE_ARRAY: {
        if (file_field.data_type->get_primitive_type() != TYPE_ARRAY) [[unlikely]] {
            return SCHEMA_ERROR;
        }
        MOCK_REMOVE(DCHECK(file_field.children.size() == 1));

        std::shared_ptr<TableSchemaChangeHelper::Node> element_node = nullptr;
        const auto& element_type = assert_cast<const DataTypePtr&>(
                assert_cast<const DataTypeArray*>(remove_nullable(table_data_type).get())
                        ->get_nested_type());

        RETURN_IF_ERROR(by_parquet_name(element_type, file_field.children[0], element_node));

        node = std::make_shared<TableSchemaChangeHelper::ArrayNode>(element_node);
        break;
    }
    case TYPE_STRUCT: {
        if (file_field.data_type->get_primitive_type() != TYPE_STRUCT) [[unlikely]] {
            return SCHEMA_ERROR;
        }

        auto struct_node = std::make_shared<TableSchemaChangeHelper::StructNode>();

        const auto struct_data_type =
                assert_cast<const DataTypeStruct*>(remove_nullable(table_data_type).get());

        std::map<std::string, size_t> parquet_field_names;
        for (size_t idx = 0; idx < file_field.children.size(); idx++) {
            parquet_field_names.emplace(to_lower(file_field.children[idx].name), idx);
        }
        for (size_t idx = 0; idx < struct_data_type->get_elements().size(); idx++) {
            const auto& doris_field_name = struct_data_type->get_element_name(idx);

            if (parquet_field_names.contains(doris_field_name)) {
                auto parquet_field_idx = parquet_field_names[doris_field_name];
                std::shared_ptr<TableSchemaChangeHelper::Node> field_node = nullptr;

                RETURN_IF_ERROR(by_parquet_name(struct_data_type->get_element(idx),
                                                file_field.children[parquet_field_idx],
                                                field_node));
                struct_node->add_children(doris_field_name,
                                          file_field.children[parquet_field_idx].name, field_node);
            } else {
                struct_node->add_not_exist_children(doris_field_name);
            }
        }
        node = struct_node;
        break;
    }
    default: {
        node = std::make_shared<TableSchemaChangeHelper::ScalarNode>();
        break;
    }
    }

    return Status::OK();
}

Status TableSchemaChangeHelper::BuildTableInfoUtil::by_orc_name(
        const TupleDescriptor* table_tuple_descriptor, const orc::Type* orc_type_ptr,
        std::shared_ptr<TableSchemaChangeHelper::Node>& node,
        const std::set<TSlotId>* is_file_slot) {
    auto struct_node = std::make_shared<TableSchemaChangeHelper::StructNode>();

    std::map<std::string, uint64_t> file_column_name_idx_map;
    for (uint64_t idx = 0; idx < orc_type_ptr->getSubtypeCount(); idx++) {
        // to_lower for match table column name.
        file_column_name_idx_map.emplace(to_lower(orc_type_ptr->getFieldName(idx)), idx);
    }

    for (const auto& slot : table_tuple_descriptor->slots()) {
        const auto& table_column_name = slot->col_name();
        if ((is_file_slot == nullptr || is_file_slot->contains(slot->id())) &&
            file_column_name_idx_map.contains(table_column_name)) {
            auto file_column_idx = file_column_name_idx_map[table_column_name];
            std::shared_ptr<TableSchemaChangeHelper::Node> field_node = nullptr;
            RETURN_IF_ERROR(by_orc_name(slot->type(), orc_type_ptr->getSubtype(file_column_idx),
                                        field_node));
            struct_node->add_children(table_column_name,
                                      orc_type_ptr->getFieldName(file_column_idx), field_node);
        } else {
            struct_node->add_not_exist_children(table_column_name);
        }
    }
    node = struct_node;
    return Status::OK();
}

Status TableSchemaChangeHelper::BuildTableInfoUtil::by_orc_name(
        const DataTypePtr& table_data_type, const orc::Type* orc_root,
        std::shared_ptr<TableSchemaChangeHelper::Node>& node) {
    switch (table_data_type->get_primitive_type()) {
    case TYPE_MAP: {
        if (orc_root->getKind() != orc::TypeKind::MAP) [[unlikely]] {
            return SCHEMA_ERROR;
        }
        MOCK_REMOVE(DCHECK(orc_root->getSubtypeCount() == 2));

        std::shared_ptr<TableSchemaChangeHelper::Node> key_node = nullptr;
        const auto& key_type = assert_cast<const DataTypePtr&>(
                assert_cast<const DataTypeMap*>(remove_nullable(table_data_type).get())
                        ->get_key_type());
        RETURN_IF_ERROR(by_orc_name(key_type, orc_root->getSubtype(0), key_node));

        std::shared_ptr<TableSchemaChangeHelper::Node> value_node = nullptr;
        const auto& value_type = assert_cast<const DataTypePtr&>(
                assert_cast<const DataTypeMap*>(remove_nullable(table_data_type).get())
                        ->get_value_type());
        RETURN_IF_ERROR(by_orc_name(value_type, orc_root->getSubtype(1), value_node));
        node = std::make_shared<TableSchemaChangeHelper::MapNode>(key_node, value_node);

        break;
    }
    case TYPE_ARRAY: {
        if (orc_root->getKind() != orc::TypeKind::LIST) [[unlikely]] {
            return SCHEMA_ERROR;
        }
        MOCK_REMOVE(DCHECK(orc_root->getSubtypeCount() == 1));

        std::shared_ptr<TableSchemaChangeHelper::Node> element_node = nullptr;
        const auto& element_type = assert_cast<const DataTypePtr&>(
                assert_cast<const DataTypeArray*>(remove_nullable(table_data_type).get())
                        ->get_nested_type());

        RETURN_IF_ERROR(by_orc_name(element_type, orc_root->getSubtype(0), element_node));
        node = std::make_shared<TableSchemaChangeHelper::ArrayNode>(element_node);
        break;
    }
    case TYPE_STRUCT: {
        if (orc_root->getKind() != orc::TypeKind::STRUCT) [[unlikely]] {
            return SCHEMA_ERROR;
        }
        auto struct_node = std::make_shared<TableSchemaChangeHelper::StructNode>();

        const auto struct_data_type =
                assert_cast<const DataTypeStruct*>(remove_nullable(table_data_type).get());
        std::map<std::string, uint64_t> orc_field_names;
        for (uint64_t idx = 0; idx < orc_root->getSubtypeCount(); idx++) {
            orc_field_names.emplace(to_lower(orc_root->getFieldName(idx)), idx);
        }

        for (size_t idx = 0; idx < struct_data_type->get_elements().size(); idx++) {
            const auto& doris_field_name = struct_data_type->get_element_name(idx);

            if (orc_field_names.contains(doris_field_name)) {
                std::shared_ptr<TableSchemaChangeHelper::Node> field_node = nullptr;

                auto orc_field_idx = orc_field_names[doris_field_name];
                RETURN_IF_ERROR(by_orc_name(struct_data_type->get_element(idx),
                                            orc_root->getSubtype(orc_field_idx), field_node));
                struct_node->add_children(doris_field_name, orc_root->getFieldName(orc_field_idx),
                                          field_node);
            } else {
                struct_node->add_not_exist_children(doris_field_name);
            }
        }
        node = struct_node;
        break;
    }
    default: {
        node = std::make_shared<TableSchemaChangeHelper::ScalarNode>();
        break;
    }
    }
    return Status::OK();
}

Status TableSchemaChangeHelper::BuildTableInfoUtil::by_table_field_id(
        const schema::external::TField table_schema, const schema::external::TField file_schema,
        std::shared_ptr<TableSchemaChangeHelper::Node>& node) {
    switch (table_schema.type.type) {
    case TPrimitiveType::MAP: {
        if (file_schema.type.type != TPrimitiveType::MAP) [[unlikely]] {
            return SCHEMA_ERROR;
        }
        MOCK_REMOVE(DCHECK(table_schema.__isset.nestedField));
        MOCK_REMOVE(DCHECK(table_schema.nestedField.__isset.map_field));
        MOCK_REMOVE(DCHECK(table_schema.nestedField.map_field.__isset.key_field));
        MOCK_REMOVE(DCHECK(table_schema.nestedField.map_field.__isset.value_field));
        MOCK_REMOVE(DCHECK(table_schema.nestedField.map_field.key_field.field_ptr != nullptr));
        MOCK_REMOVE(DCHECK(table_schema.nestedField.map_field.value_field.field_ptr != nullptr));

        MOCK_REMOVE(DCHECK(file_schema.__isset.nestedField));
        MOCK_REMOVE(DCHECK(file_schema.nestedField.__isset.map_field));
        MOCK_REMOVE(DCHECK(file_schema.nestedField.map_field.__isset.key_field));
        MOCK_REMOVE(DCHECK(file_schema.nestedField.map_field.__isset.value_field));
        MOCK_REMOVE(DCHECK(file_schema.nestedField.map_field.key_field.field_ptr != nullptr));
        MOCK_REMOVE(DCHECK(file_schema.nestedField.map_field.value_field.field_ptr != nullptr));

        std::shared_ptr<TableSchemaChangeHelper::Node> key_node = nullptr;
        RETURN_IF_ERROR(by_table_field_id(*table_schema.nestedField.map_field.key_field.field_ptr,
                                          *file_schema.nestedField.map_field.key_field.field_ptr,
                                          key_node));

        std::shared_ptr<TableSchemaChangeHelper::Node> value_node = nullptr;
        RETURN_IF_ERROR(by_table_field_id(*table_schema.nestedField.map_field.value_field.field_ptr,
                                          *file_schema.nestedField.map_field.value_field.field_ptr,
                                          value_node));

        node = std::make_shared<TableSchemaChangeHelper::MapNode>(key_node, value_node);
        break;
    }
    case TPrimitiveType::ARRAY: {
        if (file_schema.type.type != TPrimitiveType::ARRAY) [[unlikely]] {
            return SCHEMA_ERROR;
        }

        MOCK_REMOVE(DCHECK(table_schema.__isset.nestedField));
        MOCK_REMOVE(DCHECK(table_schema.nestedField.__isset.array_field));
        MOCK_REMOVE(DCHECK(table_schema.nestedField.array_field.__isset.item_field));
        MOCK_REMOVE(DCHECK(table_schema.nestedField.array_field.item_field.field_ptr != nullptr));

        MOCK_REMOVE(DCHECK(file_schema.__isset.nestedField));
        MOCK_REMOVE(DCHECK(file_schema.nestedField.__isset.array_field));
        MOCK_REMOVE(DCHECK(file_schema.nestedField.array_field.__isset.item_field));
        MOCK_REMOVE(DCHECK(file_schema.nestedField.array_field.item_field.field_ptr != nullptr));

        std::shared_ptr<TableSchemaChangeHelper::Node> item_node = nullptr;
        RETURN_IF_ERROR(by_table_field_id(
                *table_schema.nestedField.array_field.item_field.field_ptr,
                *file_schema.nestedField.array_field.item_field.field_ptr, item_node));

        node = std::make_shared<TableSchemaChangeHelper::ArrayNode>(item_node);
        break;
    }
    case TPrimitiveType::STRUCT: {
        if (file_schema.type.type != TPrimitiveType::STRUCT) [[unlikely]] {
            return SCHEMA_ERROR;
        }
        MOCK_REMOVE(DCHECK(table_schema.__isset.nestedField));
        MOCK_REMOVE(DCHECK(table_schema.nestedField.__isset.struct_field));

        MOCK_REMOVE(DCHECK(file_schema.__isset.nestedField));
        MOCK_REMOVE(DCHECK(file_schema.nestedField.__isset.struct_field));

        RETURN_IF_ERROR(by_table_field_id(table_schema.nestedField.struct_field,
                                          file_schema.nestedField.struct_field, node));
        break;
    }
    default: {
        node = std::make_shared<TableSchemaChangeHelper::ScalarNode>();
        break;
    }
    }

    return Status::OK();
}

Status TableSchemaChangeHelper::BuildTableInfoUtil::by_table_field_id(
        const schema::external::TStructField& table_schema,
        const schema::external::TStructField& file_schema,
        std::shared_ptr<TableSchemaChangeHelper::Node>& node) {
    std::map<int32_t, size_t> file_field_id_to_idx;
    for (size_t idx = 0; idx < file_schema.fields.size(); ++idx) {
        file_field_id_to_idx.emplace(file_schema.fields[idx].field_ptr->id, idx);
    }
    auto struct_node = std::make_shared<TableSchemaChangeHelper::StructNode>();

    for (const auto& table_field : table_schema.fields) {
        const auto& table_column_name = table_field.field_ptr->name;

        if (file_field_id_to_idx.contains(table_field.field_ptr->id)) {
            const auto& file_field =
                    file_schema.fields.at(file_field_id_to_idx[table_field.field_ptr->id]);

            std::shared_ptr<TableSchemaChangeHelper::Node> field_node = nullptr;
            RETURN_IF_ERROR(
                    by_table_field_id(*table_field.field_ptr, *file_field.field_ptr, field_node));

            struct_node->add_children(table_column_name, file_field.field_ptr->name, field_node);
        } else {
            struct_node->add_not_exist_children(table_column_name);
        }
    }
    node = std::move(struct_node);
    return Status::OK();
}

Status TableSchemaChangeHelper::BuildTableInfoUtil::by_parquet_field_id(
        const schema::external::TStructField& table_schema,
        const FieldDescriptor& parquet_field_desc,
        std::shared_ptr<TableSchemaChangeHelper::Node>& node, bool& exist_field_id) {
    auto struct_node = std::make_shared<TableSchemaChangeHelper::StructNode>();
    auto parquet_fields_schema = parquet_field_desc.get_fields_schema();
    std::map<int32_t, size_t> file_column_id_idx_map;
    for (size_t idx = 0; idx < parquet_fields_schema.size(); idx++) {
        if (parquet_fields_schema[idx].field_id == -1) {
            exist_field_id = false;
            return Status::OK();
        } else {
            file_column_id_idx_map.emplace(parquet_fields_schema[idx].field_id, idx);
        }
    }

    for (const auto& table_field : table_schema.fields) {
        const auto& table_column_name = table_field.field_ptr->name;

        if (file_column_id_idx_map.contains(table_field.field_ptr->id)) {
            auto file_column_idx = file_column_id_idx_map[table_field.field_ptr->id];
            std::shared_ptr<TableSchemaChangeHelper::Node> field_node = nullptr;
            RETURN_IF_ERROR(by_parquet_field_id(*table_field.field_ptr,
                                                parquet_fields_schema[file_column_idx], field_node,
                                                exist_field_id));
            struct_node->add_children(table_column_name,
                                      parquet_fields_schema[file_column_idx].name, field_node);
        } else {
            struct_node->add_not_exist_children(table_column_name);
        }
    }

    node = struct_node;
    return Status::OK();
}

Status TableSchemaChangeHelper::BuildTableInfoUtil::by_parquet_field_id(
        const schema::external::TField& table_schema, const FieldSchema& parquet_field,
        std::shared_ptr<TableSchemaChangeHelper::Node>& node, bool& exist_field_id) {
    switch (table_schema.type.type) {
    case TPrimitiveType::MAP: {
        if (parquet_field.data_type->get_primitive_type() != TYPE_MAP) [[unlikely]] {
            return SCHEMA_ERROR;
        }
        MOCK_REMOVE(DCHECK(table_schema.__isset.nestedField));
        MOCK_REMOVE(DCHECK(table_schema.nestedField.__isset.map_field));
        MOCK_REMOVE(DCHECK(table_schema.nestedField.map_field.__isset.key_field));
        MOCK_REMOVE(DCHECK(table_schema.nestedField.map_field.__isset.value_field));
        MOCK_REMOVE(DCHECK(table_schema.nestedField.map_field.key_field.field_ptr != nullptr));
        MOCK_REMOVE(DCHECK(table_schema.nestedField.map_field.value_field.field_ptr != nullptr));

        MOCK_REMOVE(DCHECK(parquet_field.children.size() == 1));
        MOCK_REMOVE(DCHECK(parquet_field.children[0].children.size() == 2));

        std::shared_ptr<TableSchemaChangeHelper::Node> key_node = nullptr;
        std::shared_ptr<TableSchemaChangeHelper::Node> value_node = nullptr;

        RETURN_IF_ERROR(by_parquet_field_id(*table_schema.nestedField.map_field.key_field.field_ptr,
                                            parquet_field.children[0].children[0], key_node,
                                            exist_field_id));

        RETURN_IF_ERROR(by_parquet_field_id(
                *table_schema.nestedField.map_field.value_field.field_ptr,
                parquet_field.children[0].children[1], value_node, exist_field_id));

        node = std::make_shared<TableSchemaChangeHelper::MapNode>(key_node, value_node);
        break;
    }
    case TPrimitiveType::ARRAY: {
        if (parquet_field.data_type->get_primitive_type() != TYPE_ARRAY) [[unlikely]] {
            return SCHEMA_ERROR;
        }
        MOCK_REMOVE(DCHECK(table_schema.__isset.nestedField));
        MOCK_REMOVE(DCHECK(table_schema.nestedField.__isset.array_field));
        MOCK_REMOVE(DCHECK(table_schema.nestedField.array_field.__isset.item_field));
        MOCK_REMOVE(DCHECK(table_schema.nestedField.array_field.item_field.field_ptr != nullptr));

        MOCK_REMOVE(DCHECK(parquet_field.children.size() == 1));

        std::shared_ptr<TableSchemaChangeHelper::Node> element_node = nullptr;
        RETURN_IF_ERROR(
                by_parquet_field_id(*table_schema.nestedField.array_field.item_field.field_ptr,
                                    parquet_field.children[0], element_node, exist_field_id));

        node = std::make_shared<TableSchemaChangeHelper::ArrayNode>(element_node);
        break;
    }
    case TPrimitiveType::STRUCT: {
        if (parquet_field.data_type->get_primitive_type() != TYPE_STRUCT) [[unlikely]] {
            return SCHEMA_ERROR;
        }
        MOCK_REMOVE(DCHECK(table_schema.__isset.nestedField));
        MOCK_REMOVE(DCHECK(table_schema.nestedField.__isset.struct_field));

        auto struct_node = std::make_shared<TableSchemaChangeHelper::StructNode>();

        std::map<int32_t, size_t> file_column_id_idx_map;
        for (size_t idx = 0; idx < parquet_field.children.size(); idx++) {
            if (parquet_field.children[idx].field_id == -1) {
                exist_field_id = false;
                return Status::OK();
            } else {
                file_column_id_idx_map.emplace(parquet_field.children[idx].field_id, idx);
            }
        }

        for (const auto& table_field : table_schema.nestedField.struct_field.fields) {
            const auto& table_column_name = table_field.field_ptr->name;
            if (file_column_id_idx_map.contains(table_field.field_ptr->id)) {
                const auto& file_field = parquet_field.children.at(
                        file_column_id_idx_map[table_field.field_ptr->id]);
                std::shared_ptr<TableSchemaChangeHelper::Node> field_node = nullptr;
                RETURN_IF_ERROR(by_parquet_field_id(*table_field.field_ptr, file_field, field_node,
                                                    exist_field_id));
                struct_node->add_children(table_column_name, file_field.name, field_node);
            } else {
                struct_node->add_not_exist_children(table_column_name);
            }
        }
        node = struct_node;
        break;
    }
    default: {
        node = std::make_shared<ScalarNode>();
        break;
    }
    }
    return Status::OK();
}

Status TableSchemaChangeHelper::BuildTableInfoUtil::by_orc_field_id(
        const schema::external::TStructField& table_schema, const orc::Type* orc_root,
        const std::string& field_id_attribute_key,
        std::shared_ptr<TableSchemaChangeHelper::Node>& node, bool& exist_field_id) {
    auto struct_node = std::make_shared<TableSchemaChangeHelper::StructNode>();

    std::map<int32_t, size_t> file_column_id_idx_map;
    for (size_t idx = 0; idx < orc_root->getSubtypeCount(); idx++) {
        if (orc_root->getSubtype(idx)->hasAttributeKey(field_id_attribute_key)) {
            auto field_id =
                    std::stoi(orc_root->getSubtype(idx)->getAttributeValue(field_id_attribute_key));
            file_column_id_idx_map.emplace(field_id, idx);
        } else {
            exist_field_id = false;
            return Status::OK();
        }
    }

    for (const auto& table_field : table_schema.fields) {
        const auto& table_column_name = table_field.field_ptr->name;
        if (file_column_id_idx_map.contains(table_field.field_ptr->id)) {
            auto file_field_idx = file_column_id_idx_map[table_field.field_ptr->id];
            const auto& file_field = orc_root->getSubtype(file_field_idx);
            std::shared_ptr<TableSchemaChangeHelper::Node> field_node = nullptr;
            RETURN_IF_ERROR(by_orc_field_id(*table_field.field_ptr, file_field,
                                            field_id_attribute_key, field_node, exist_field_id));
            struct_node->add_children(table_column_name, orc_root->getFieldName(file_field_idx),
                                      field_node);
        } else {
            struct_node->add_not_exist_children(table_column_name);
        }
    }
    node = struct_node;
    return Status::OK();
}

Status TableSchemaChangeHelper::BuildTableInfoUtil::by_orc_field_id(
        const schema::external::TField& table_schema, const orc::Type* orc_root,
        const std::string& field_id_attribute_key,
        std::shared_ptr<TableSchemaChangeHelper::Node>& node, bool& exist_field_id) {
    switch (table_schema.type.type) {
    case TPrimitiveType::MAP: {
        if (orc_root->getKind() != orc::TypeKind::MAP) [[unlikely]] {
            return SCHEMA_ERROR;
        }
        MOCK_REMOVE(DCHECK(table_schema.__isset.nestedField));
        MOCK_REMOVE(DCHECK(table_schema.nestedField.__isset.map_field));
        MOCK_REMOVE(DCHECK(table_schema.nestedField.map_field.__isset.key_field));
        MOCK_REMOVE(DCHECK(table_schema.nestedField.map_field.__isset.value_field));
        MOCK_REMOVE(DCHECK(table_schema.nestedField.map_field.key_field.field_ptr != nullptr));
        MOCK_REMOVE(DCHECK(table_schema.nestedField.map_field.value_field.field_ptr != nullptr));

        MOCK_REMOVE(DCHECK(orc_root->getSubtypeCount() == 2));

        std::shared_ptr<TableSchemaChangeHelper::Node> key_node = nullptr;
        std::shared_ptr<TableSchemaChangeHelper::Node> value_node = nullptr;

        RETURN_IF_ERROR(by_orc_field_id(*table_schema.nestedField.map_field.key_field.field_ptr,
                                        orc_root->getSubtype(0), field_id_attribute_key, key_node,
                                        exist_field_id));

        RETURN_IF_ERROR(by_orc_field_id(*table_schema.nestedField.map_field.value_field.field_ptr,
                                        orc_root->getSubtype(1), field_id_attribute_key, value_node,
                                        exist_field_id));

        node = std::make_shared<TableSchemaChangeHelper::MapNode>(key_node, value_node);
        break;
    }
    case TPrimitiveType::ARRAY: {
        if (orc_root->getKind() != orc::TypeKind::LIST) [[unlikely]] {
            return SCHEMA_ERROR;
        }
        MOCK_REMOVE(DCHECK(table_schema.__isset.nestedField));
        MOCK_REMOVE(DCHECK(table_schema.nestedField.__isset.array_field));
        MOCK_REMOVE(DCHECK(table_schema.nestedField.array_field.__isset.item_field));
        MOCK_REMOVE(DCHECK(table_schema.nestedField.array_field.item_field.field_ptr != nullptr));

        MOCK_REMOVE(DCHECK(orc_root->getSubtypeCount() == 1));

        std::shared_ptr<TableSchemaChangeHelper::Node> element_node = nullptr;
        RETURN_IF_ERROR(by_orc_field_id(*table_schema.nestedField.array_field.item_field.field_ptr,
                                        orc_root->getSubtype(0), field_id_attribute_key,
                                        element_node, exist_field_id));

        node = std::make_shared<TableSchemaChangeHelper::ArrayNode>(element_node);
        break;
    }
    case TPrimitiveType::STRUCT: {
        if (orc_root->getKind() != orc::TypeKind::STRUCT) [[unlikely]] {
            return SCHEMA_ERROR;
        }
        MOCK_REMOVE(DCHECK(table_schema.__isset.nestedField));
        MOCK_REMOVE(DCHECK(table_schema.nestedField.__isset.struct_field));
        RETURN_IF_ERROR(by_orc_field_id(table_schema.nestedField.struct_field, orc_root,
                                        field_id_attribute_key, node, exist_field_id));

        break;
    }
    default: {
        node = std::make_shared<ScalarNode>();
        break;
    }
    }

    return Status::OK();
}

std::string TableSchemaChangeHelper::debug(const std::shared_ptr<Node>& root, size_t level) {
    std::string ans;

    auto indent = [](size_t level) { return std::string(level * 2, ' '); };

    std::string prefix = indent(level);

    if (std::dynamic_pointer_cast<ScalarNode>(root)) {
        ans += prefix + "ScalarNode\n";
    } else if (auto struct_node = std::dynamic_pointer_cast<StructNode>(root)) {
        ans += prefix + "StructNode\n";
        for (const auto& [table_col_name, value] : struct_node->get_childrens()) {
            const auto& [child_node, file_col_name, exist] = value;
            ans += indent(level + 1) + table_col_name;
            if (exist) {
                ans += " (file: " + file_col_name + ")";
            } else {
                ans += " (not exists)";
            }
            ans += "\n";
            if (child_node) {
                ans += debug(child_node, level + 2);
            }
        }
    } else if (auto array_node = std::dynamic_pointer_cast<ArrayNode>(root)) {
        ans += prefix + "ArrayNode\n";
        ans += indent(level + 1) + "Element:\n";
        ans += debug(array_node->get_element_node(), level + 2);
    } else if (auto map_node = std::dynamic_pointer_cast<MapNode>(root)) {
        ans += prefix + "MapNode\n";
        ans += indent(level + 1) + "Key:\n";
        ans += debug(map_node->get_key_node(), level + 2);
        ans += indent(level + 1) + "Value:\n";
        ans += debug(map_node->get_value_node(), level + 2);
    } else if (std::dynamic_pointer_cast<ConstNode>(root)) {
        ans += prefix + "ConstNode\n";
    } else {
        ans += prefix + "UnknownNodeType\n";
    }

    return ans;
}
#include "common/compile_check_end.h"
} // namespace doris::vectorized