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

#include "format_v2/parquet/native_schema_node.h"

#include <algorithm>
#include <utility>

#include "core/assert_cast.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "util/string_util.h"

namespace doris::format::parquet {

void NativeStructSchemaNode::add_child(std::string table_name, std::string file_name,
                                       std::shared_ptr<NativeSchemaNode> node) {
    _children.emplace(std::move(table_name), Child {std::move(file_name), std::move(node)});
}

void NativeStructSchemaNode::add_missing_child(std::string table_name) {
    _children.emplace(std::move(table_name), Child {});
}

std::shared_ptr<NativeSchemaNode> NativeStructSchemaNode::child(
        const std::string& table_name) const {
    const auto it = _children.find(table_name);
    return it == _children.end() ? nullptr : it->second.node;
}

std::string NativeStructSchemaNode::file_child_name(const std::string& table_name) const {
    const auto it = _children.find(table_name);
    return it == _children.end() ? std::string {} : it->second.file_name;
}

bool NativeStructSchemaNode::has_child(const std::string& table_name) const {
    const auto it = _children.find(table_name);
    return it != _children.end() && it->second.node != nullptr;
}

Status build_native_schema_node(const DataTypePtr& projected_type,
                                const ParquetColumnSchema& file_schema,
                                std::shared_ptr<NativeSchemaNode>* result) {
    if (projected_type == nullptr || result == nullptr) {
        return Status::InvalidArgument("Native Parquet schema mapping input is null");
    }
    const auto type = remove_nullable(projected_type);
    switch (type->get_primitive_type()) {
    case TYPE_STRUCT: {
        if (file_schema.kind != ParquetColumnSchemaKind::STRUCT) {
            return Status::Corruption("Parquet column {} is not a STRUCT", file_schema.name);
        }
        const auto* struct_type = assert_cast<const DataTypeStruct*>(type.get());
        std::map<std::string, const ParquetColumnSchema*> file_children;
        for (const auto& child : file_schema.children) {
            const auto [_, inserted] = file_children.emplace(to_lower(child->name), child.get());
            if (UNLIKELY(!inserted)) {
                // Case-insensitive projection has no field-id input at this layer, so choosing one
                // of two case-distinct siblings would silently bind the other physical column.
                return Status::Corruption(
                        "Parquet STRUCT {} has ambiguous case-insensitive child name {}",
                        file_schema.name, child->name);
            }
        }
        auto node = std::make_shared<NativeStructSchemaNode>();
        for (size_t i = 0; i < struct_type->get_elements().size(); ++i) {
            const auto& table_name = struct_type->get_element_name(i);
            // Native metadata keeps writer casing. Match normalized names while preserving the
            // original file name used to address the physical child reader.
            const auto child_it = file_children.find(to_lower(table_name));
            if (child_it == file_children.end()) {
                node->add_missing_child(table_name);
                continue;
            }
            std::shared_ptr<NativeSchemaNode> child_node;
            RETURN_IF_ERROR(build_native_schema_node(struct_type->get_element(i), *child_it->second,
                                                     &child_node));
            node->add_child(table_name, child_it->second->name, std::move(child_node));
        }
        *result = std::move(node);
        return Status::OK();
    }
    case TYPE_ARRAY: {
        if (file_schema.kind != ParquetColumnSchemaKind::LIST || file_schema.children.size() != 1) {
            return Status::Corruption("Parquet column {} is not an ARRAY", file_schema.name);
        }
        const auto* array_type = assert_cast<const DataTypeArray*>(type.get());
        std::shared_ptr<NativeSchemaNode> element;
        RETURN_IF_ERROR(build_native_schema_node(array_type->get_nested_type(),
                                                 *file_schema.children[0], &element));
        *result = std::make_shared<NativeArraySchemaNode>(std::move(element));
        return Status::OK();
    }
    case TYPE_MAP: {
        if (file_schema.kind != ParquetColumnSchemaKind::MAP || file_schema.children.size() != 2) {
            return Status::Corruption("Parquet column {} is not a MAP", file_schema.name);
        }
        const auto* map_type = assert_cast<const DataTypeMap*>(type.get());
        std::shared_ptr<NativeSchemaNode> key;
        std::shared_ptr<NativeSchemaNode> value;
        RETURN_IF_ERROR(
                build_native_schema_node(map_type->get_key_type(), *file_schema.children[0], &key));
        RETURN_IF_ERROR(build_native_schema_node(map_type->get_value_type(),
                                                 *file_schema.children[1], &value));
        *result = std::make_shared<NativeMapSchemaNode>(std::move(key), std::move(value));
        return Status::OK();
    }
    default:
        if (file_schema.kind != ParquetColumnSchemaKind::PRIMITIVE) {
            return Status::Corruption("Parquet column {} is not a scalar", file_schema.name);
        }
        *result = std::make_shared<NativeScalarSchemaNode>();
        return Status::OK();
    }
}

} // namespace doris::format::parquet
