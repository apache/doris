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

#include "format/table/iceberg_reader.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/parquet_types.h>
#include <glog/logging.h>
#include <parallel_hashmap/phmap.h>
#include <rapidjson/document.h>

#include <algorithm>
#include <cstring>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <set>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_struct.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "format/format_common.h"
#include "format/generic_reader.h"
#include "format/orc/vorc_reader.h"
#include "format/parquet/schema_desc.h"
#include "format/parquet/vparquet_column_chunk_reader.h"
#include "format/table/deletion_vector_reader.h"
#include "format/table/iceberg/iceberg_orc_nested_column_utils.h"
#include "format/table/iceberg/iceberg_parquet_nested_column_utils.h"
#include "format/table/iceberg_default_value.h"
#include "format/table/iceberg_delete_file_reader_helper.h"
#include "format/table/iceberg_scan_semantics.h"
#include "format/table/nested_column_access_helper.h"
#include "format/table/table_format_reader.h"
#include "format_v2/expr/cast.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/coding.h"

namespace cctz {
#include "common/compile_check_begin.h"
class time_zone;
} // namespace cctz
namespace doris {
class RowDescriptor;
class SlotDescriptor;
class TupleDescriptor;

namespace io {
struct IOContext;
} // namespace io
class VExprContext;
} // namespace doris

namespace doris {
namespace {

const schema::external::TField* find_iceberg_struct_child(const schema::external::TField& field,
                                                          const std::string& name) {
    DORIS_CHECK(field.__isset.nestedField);
    DORIS_CHECK(field.nestedField.__isset.struct_field);
    DORIS_CHECK(field.nestedField.struct_field.__isset.fields);
    for (const auto& child_ptr : field.nestedField.struct_field.fields) {
        if (child_ptr.__isset.field_ptr && child_ptr.field_ptr != nullptr &&
            child_ptr.field_ptr->__isset.name && iequal(child_ptr.field_ptr->name, name)) {
            return child_ptr.field_ptr.get();
        }
    }
    return nullptr;
}

struct ProjectedIcebergStructChild {
    size_t index;
    const schema::external::TField* field;
};

std::optional<ProjectedIcebergStructChild> find_projected_iceberg_struct_child(
        const schema::external::TField& parent, const schema::external::TField& target,
        const DataTypeStruct& projected_type) {
    DORIS_CHECK(parent.__isset.nestedField);
    DORIS_CHECK(parent.nestedField.__isset.struct_field);
    DORIS_CHECK(parent.nestedField.struct_field.__isset.fields);
    DORIS_CHECK(target.__isset.name);
    DORIS_CHECK(target.__isset.id);

    // FE keeps current children first and appends historical equality fields to the schema carrier
    // by field ID. A dropped and re-added field may therefore appear twice with the same name,
    // while the query's DataTypeStruct contains only the current occurrence.
    size_t schema_name_ordinal = 0;
    const schema::external::TField* schema_child = nullptr;
    for (const auto& child_ptr : parent.nestedField.struct_field.fields) {
        DORIS_CHECK(child_ptr.__isset.field_ptr && child_ptr.field_ptr != nullptr);
        const auto& child = child_ptr.field_ptr;
        DORIS_CHECK(child->__isset.name);
        DORIS_CHECK(child->__isset.id);
        if (child->name != target.name) {
            continue;
        }
        if (child->id == target.id) {
            schema_child = child.get();
            break;
        }
        ++schema_name_ordinal;
    }
    if (schema_child == nullptr) {
        return std::nullopt;
    }

    size_t projected_name_ordinal = 0;
    for (size_t index = 0; index < projected_type.get_elements().size(); ++index) {
        if (projected_type.get_element_name(index) != target.name) {
            continue;
        }
        if (projected_name_ordinal == schema_name_ordinal) {
            return ProjectedIcebergStructChild {.index = index, .field = schema_child};
        }
        ++projected_name_ordinal;
    }
    return std::nullopt;
}

// This recursive type dispatcher mirrors Iceberg's nested types; DORIS_CHECK expansion inflates
// the measured complexity.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
bool projected_iceberg_field_requires_required_validation(const schema::external::TField& field,
                                                          const DataTypePtr& data_type) {
    DORIS_CHECK(data_type != nullptr);
    if (field.__isset.is_optional && !field.is_optional) {
        return true;
    }
    const auto value_type = remove_nullable(data_type);
    switch (value_type->get_primitive_type()) {
    case TYPE_STRUCT: {
        const auto& struct_type = assert_cast<const DataTypeStruct&>(*value_type);
        for (size_t child = 0; child < struct_type.get_elements().size(); ++child) {
            const auto* child_field =
                    find_iceberg_struct_child(field, struct_type.get_element_name(child));
            DORIS_CHECK(child_field != nullptr);
            if (projected_iceberg_field_requires_required_validation(
                        *child_field, struct_type.get_element(child))) {
                return true;
            }
        }
        return false;
    }
    case TYPE_ARRAY: {
        DORIS_CHECK(field.__isset.nestedField);
        DORIS_CHECK(field.nestedField.__isset.array_field);
        DORIS_CHECK(field.nestedField.array_field.__isset.item_field);
        const auto& child_ptr = field.nestedField.array_field.item_field;
        DORIS_CHECK(child_ptr.__isset.field_ptr && child_ptr.field_ptr != nullptr);
        return projected_iceberg_field_requires_required_validation(
                *child_ptr.field_ptr,
                assert_cast<const DataTypeArray&>(*value_type).get_nested_type());
    }
    case TYPE_MAP: {
        DORIS_CHECK(field.__isset.nestedField);
        DORIS_CHECK(field.nestedField.__isset.map_field);
        const auto& map_field = field.nestedField.map_field;
        DORIS_CHECK(map_field.__isset.key_field && map_field.__isset.value_field);
        DORIS_CHECK(map_field.key_field.__isset.field_ptr &&
                    map_field.key_field.field_ptr != nullptr);
        DORIS_CHECK(map_field.value_field.__isset.field_ptr &&
                    map_field.value_field.field_ptr != nullptr);
        const auto& map_type = assert_cast<const DataTypeMap&>(*value_type);
        return projected_iceberg_field_requires_required_validation(*map_field.key_field.field_ptr,
                                                                    map_type.get_key_type()) ||
               projected_iceberg_field_requires_required_validation(
                       *map_field.value_field.field_ptr, map_type.get_value_type());
    }
    default:
        return false;
    }
}

bool expression_references_required_validation_slot(
        const VExprSPtr& expr, const std::unordered_set<int>& required_validation_slot_ids) {
    DORIS_CHECK(expr != nullptr);
    const auto target = expr->is_rf_wrapper() ? expr->get_impl() : expr;
    DORIS_CHECK(target != nullptr);
    if (target->is_slot_ref()) {
        return required_validation_slot_ids.contains(
                assert_cast<const VSlotRef&>(*target).slot_id());
    }
    return std::ranges::any_of(target->children(), [&](const VExprSPtr& child) {
        return expression_references_required_validation_slot(child, required_validation_slot_ids);
    });
}

template <typename Offsets>
const NullMap* project_iceberg_parent_null_map(const NullMap* own_null_map,
                                               const NullMap* ancestor_null_map, size_t rows,
                                               const Offsets& offsets, size_t child_rows,
                                               NullMap* const projected_null_map) {
    if (own_null_map == nullptr && ancestor_null_map == nullptr) {
        return nullptr;
    }
    DORIS_CHECK(own_null_map == nullptr || own_null_map->size() == rows);
    DORIS_CHECK(ancestor_null_map == nullptr || ancestor_null_map->size() == rows);
    DORIS_CHECK(offsets.size() == rows);
    projected_null_map->resize_fill(child_rows, 0);
    size_t begin = 0;
    for (size_t row = 0; row < rows; ++row) {
        const size_t end = offsets[row];
        DORIS_CHECK(begin <= end && end <= child_rows);
        if ((own_null_map != nullptr && (*own_null_map)[row] != 0) ||
            (ancestor_null_map != nullptr && (*ancestor_null_map)[row] != 0)) {
            std::fill(projected_null_map->begin() + begin, projected_null_map->begin() + end, 1);
        }
        begin = end;
    }
    DORIS_CHECK(begin == child_rows);
    return projected_null_map;
}

// This recursive type dispatcher mirrors Iceberg's nested types; DORIS_CHECK expansion inflates
// the measured complexity.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
Status validate_iceberg_required_field(const schema::external::TField& field,
                                       const DataTypePtr& data_type, const ColumnPtr& column,
                                       const NullMap* ancestor_null_map = nullptr) {
    DORIS_CHECK(data_type != nullptr);
    DORIS_CHECK(column.get() != nullptr);
    const auto full_column = column->convert_to_full_column_if_const();
    const IColumn* nested_column = full_column.get();
    const NullMap* own_null_map = nullptr;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(*nested_column)) {
        own_null_map = &nullable->get_null_map_data();
        nested_column = &nullable->get_nested_column();
        if (field.__isset.is_optional && !field.is_optional && nullable->has_null()) {
            DORIS_CHECK(ancestor_null_map == nullptr ||
                        ancestor_null_map->size() == own_null_map->size());
            for (size_t row = 0; row < own_null_map->size(); ++row) {
                if ((*own_null_map)[row] != 0 &&
                    (ancestor_null_map == nullptr || (*ancestor_null_map)[row] == 0)) {
                    return Status::InvalidArgument("Required Iceberg field '{}' contains NULL",
                                                   field.name);
                }
            }
        }
    }

    NullMap combined_parent_null_map;
    const NullMap* descendant_parent_null_map = ancestor_null_map;
    if (own_null_map != nullptr) {
        descendant_parent_null_map = own_null_map;
        if (ancestor_null_map != nullptr) {
            DORIS_CHECK(ancestor_null_map->size() == own_null_map->size());
            combined_parent_null_map.resize(own_null_map->size());
            for (size_t row = 0; row < own_null_map->size(); ++row) {
                combined_parent_null_map[row] = (*own_null_map)[row] || (*ancestor_null_map)[row];
            }
            descendant_parent_null_map = &combined_parent_null_map;
        }
    }

    const auto value_type = remove_nullable(data_type);
    switch (value_type->get_primitive_type()) {
    case TYPE_STRUCT: {
        const auto& struct_type = assert_cast<const DataTypeStruct&>(*value_type);
        const auto& struct_column = assert_cast<const ColumnStruct&>(*nested_column);
        DORIS_CHECK(struct_type.get_elements().size() == struct_column.tuple_size());
        for (size_t child = 0; child < struct_type.get_elements().size(); ++child) {
            const auto* child_field =
                    find_iceberg_struct_child(field, struct_type.get_element_name(child));
            DORIS_CHECK(child_field != nullptr);
            RETURN_IF_ERROR(validate_iceberg_required_field(
                    *child_field, struct_type.get_element(child),
                    struct_column.get_column_ptr(child), descendant_parent_null_map));
        }
        return Status::OK();
    }
    case TYPE_ARRAY: {
        DORIS_CHECK(field.__isset.nestedField);
        DORIS_CHECK(field.nestedField.__isset.array_field);
        DORIS_CHECK(field.nestedField.array_field.__isset.item_field);
        const auto& child_ptr = field.nestedField.array_field.item_field;
        DORIS_CHECK(child_ptr.__isset.field_ptr && child_ptr.field_ptr != nullptr);
        const auto& array_type = assert_cast<const DataTypeArray&>(*value_type);
        const auto& array_column = assert_cast<const ColumnArray&>(*nested_column);
        NullMap element_parent_null_map;
        const NullMap* element_parent = project_iceberg_parent_null_map(
                own_null_map, ancestor_null_map, full_column->size(), array_column.get_offsets(),
                array_column.get_data().size(), &element_parent_null_map);
        return validate_iceberg_required_field(*child_ptr.field_ptr, array_type.get_nested_type(),
                                               array_column.get_data_ptr(), element_parent);
    }
    case TYPE_MAP: {
        DORIS_CHECK(field.__isset.nestedField);
        DORIS_CHECK(field.nestedField.__isset.map_field);
        const auto& map_field = field.nestedField.map_field;
        DORIS_CHECK(map_field.__isset.key_field);
        DORIS_CHECK(map_field.__isset.value_field);
        DORIS_CHECK(map_field.key_field.__isset.field_ptr &&
                    map_field.key_field.field_ptr != nullptr);
        DORIS_CHECK(map_field.value_field.__isset.field_ptr &&
                    map_field.value_field.field_ptr != nullptr);
        const auto& map_type = assert_cast<const DataTypeMap&>(*value_type);
        const auto& map_column = assert_cast<const ColumnMap&>(*nested_column);
        NullMap entry_parent_null_map;
        const NullMap* entry_parent = project_iceberg_parent_null_map(
                own_null_map, ancestor_null_map, full_column->size(), map_column.get_offsets(),
                map_column.get_keys().size(), &entry_parent_null_map);
        RETURN_IF_ERROR(validate_iceberg_required_field(*map_field.key_field.field_ptr,
                                                        map_type.get_key_type(),
                                                        map_column.get_keys_ptr(), entry_parent));
        return validate_iceberg_required_field(*map_field.value_field.field_ptr,
                                               map_type.get_value_type(),
                                               map_column.get_values_ptr(), entry_parent);
    }
    default:
        return Status::OK();
    }
}

Status validate_projected_missing_iceberg_field(const schema::external::TField& field,
                                                const DataTypePtr& data_type,
                                                const cctz::time_zone* timezone) {
    DORIS_CHECK(field.__isset.is_optional);
    // A missing optional field without an initial default materializes as NULL. Its required
    // descendants are not logically visible, so validation stops at this missing ancestor.
    if (field.is_optional && !field.__isset.initial_default_value) {
        return Status::OK();
    }
    ColumnPtr default_value;
    return iceberg::create_initial_default_column(field, data_type, &default_value, timezone);
}

// This recursive type dispatcher mirrors Iceberg's nested types; DORIS_CHECK expansion inflates
// the measured complexity.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
Status validate_projected_missing_required_iceberg_fields(
        const schema::external::TField& field, const DataTypePtr& data_type,
        const std::shared_ptr<TableSchemaChangeHelper::Node>& mapping,
        const cctz::time_zone* timezone) {
    DORIS_CHECK(data_type != nullptr);
    DORIS_CHECK(mapping != nullptr);
    if (std::dynamic_pointer_cast<TableSchemaChangeHelper::ConstNode>(mapping) != nullptr) {
        return Status::OK();
    }

    const auto value_type = remove_nullable(data_type);
    switch (value_type->get_primitive_type()) {
    case TYPE_STRUCT: {
        const auto struct_mapping =
                std::dynamic_pointer_cast<TableSchemaChangeHelper::StructNode>(mapping);
        DORIS_CHECK(struct_mapping != nullptr);
        const auto& struct_type = assert_cast<const DataTypeStruct&>(*value_type);
        for (size_t child = 0; child < struct_type.get_elements().size(); ++child) {
            const auto& child_name = struct_type.get_element_name(child);
            const auto* child_field = find_iceberg_struct_child(field, child_name);
            DORIS_CHECK(child_field != nullptr);
            if (!struct_mapping->children_column_exists(child_name)) {
                const auto* missing_field = struct_mapping->get_missing_column_field(child_name);
                DORIS_CHECK(missing_field != nullptr);
                RETURN_IF_ERROR(validate_projected_missing_iceberg_field(
                        *missing_field, struct_type.get_element(child), timezone));
                continue;
            }
            RETURN_IF_ERROR(validate_projected_missing_required_iceberg_fields(
                    *child_field, struct_type.get_element(child),
                    struct_mapping->get_children_node(child_name), timezone));
        }
        return Status::OK();
    }
    case TYPE_ARRAY: {
        DORIS_CHECK(field.__isset.nestedField);
        DORIS_CHECK(field.nestedField.__isset.array_field);
        DORIS_CHECK(field.nestedField.array_field.__isset.item_field);
        const auto& child_ptr = field.nestedField.array_field.item_field;
        DORIS_CHECK(child_ptr.__isset.field_ptr && child_ptr.field_ptr != nullptr);
        const auto array_mapping =
                std::dynamic_pointer_cast<TableSchemaChangeHelper::ArrayNode>(mapping);
        DORIS_CHECK(array_mapping != nullptr);
        const auto& array_type = assert_cast<const DataTypeArray&>(*value_type);
        return validate_projected_missing_required_iceberg_fields(
                *child_ptr.field_ptr, array_type.get_nested_type(),
                array_mapping->get_element_node(), timezone);
    }
    case TYPE_MAP: {
        DORIS_CHECK(field.__isset.nestedField);
        DORIS_CHECK(field.nestedField.__isset.map_field);
        const auto& map_field = field.nestedField.map_field;
        DORIS_CHECK(map_field.__isset.key_field);
        DORIS_CHECK(map_field.__isset.value_field);
        DORIS_CHECK(map_field.key_field.__isset.field_ptr &&
                    map_field.key_field.field_ptr != nullptr);
        DORIS_CHECK(map_field.value_field.__isset.field_ptr &&
                    map_field.value_field.field_ptr != nullptr);
        const auto map_mapping =
                std::dynamic_pointer_cast<TableSchemaChangeHelper::MapNode>(mapping);
        DORIS_CHECK(map_mapping != nullptr);
        const auto& map_type = assert_cast<const DataTypeMap&>(*value_type);
        RETURN_IF_ERROR(validate_projected_missing_required_iceberg_fields(
                *map_field.key_field.field_ptr, map_type.get_key_type(),
                map_mapping->get_key_node(), timezone));
        return validate_projected_missing_required_iceberg_fields(
                *map_field.value_field.field_ptr, map_type.get_value_type(),
                map_mapping->get_value_node(), timezone);
    }
    default:
        return Status::OK();
    }
}

class GroupedDeleteRowsVisitor final : public IcebergPositionDeleteVisitor {
public:
    using DeleteRows = std::vector<int64_t>;
    using DeleteFile = phmap::parallel_flat_hash_map<
            std::string, std::unique_ptr<DeleteRows>, std::hash<std::string>, std::equal_to<>,
            std::allocator<std::pair<const std::string, std::unique_ptr<DeleteRows>>>, 8,
            std::mutex>;

    explicit GroupedDeleteRowsVisitor(DeleteFile* position_delete)
            : _position_delete(position_delete) {}

    Status visit(const std::string& file_path, int64_t pos) override {
        if (_position_delete == nullptr) {
            return Status::InvalidArgument("position delete map is null");
        }

        auto iter = _position_delete->find(file_path);
        DeleteRows* delete_rows = nullptr;
        if (iter == _position_delete->end()) {
            delete_rows = new DeleteRows;
            (*_position_delete)[file_path] = std::unique_ptr<DeleteRows>(delete_rows);
        } else {
            delete_rows = iter->second.get();
        }
        delete_rows->push_back(pos);
        return Status::OK();
    }

private:
    DeleteFile* _position_delete;
};

constexpr auto kIcebergOrcAttribute = "iceberg.id";

bool orc_subtree_has_iceberg_id(const orc::Type* type, const std::string& attribute) {
    if (type->hasAttributeKey(attribute)) {
        return true;
    }
    for (uint64_t idx = 0; idx < type->getSubtypeCount(); ++idx) {
        if (orc_subtree_has_iceberg_id(type->getSubtype(idx), attribute)) {
            return true;
        }
    }
    return false;
}

bool parquet_subtree_has_iceberg_id(const FieldSchema& field) {
    if (field.field_id >= 0) {
        return true;
    }
    return std::ranges::any_of(field.children, parquet_subtree_has_iceberg_id);
}

struct ParquetEqualityFieldPath {
    std::vector<const FieldSchema*> fields;
    std::vector<size_t> child_indexes;
};

bool find_parquet_equality_field_path_by_id(const FieldDescriptor* descriptor, int32_t field_id,
                                            ParquetEqualityFieldPath* result) {
    DORIS_CHECK(descriptor != nullptr);
    DORIS_CHECK(result != nullptr);
    const auto find = [field_id](const auto& self, const FieldSchema* field,
                                 ParquetEqualityFieldPath* path) -> bool {
        DORIS_CHECK(field != nullptr);
        path->fields.push_back(field);
        if (field->field_id == field_id) {
            return true;
        }
        for (size_t index = 0; index < field->children.size(); ++index) {
            path->child_indexes.push_back(index);
            if (self(self, &field->children[index], path)) {
                return true;
            }
            path->child_indexes.pop_back();
        }
        path->fields.pop_back();
        return false;
    };
    for (int index = 0; index < descriptor->size(); ++index) {
        if (find(find, descriptor->get_column(index), result)) {
            return true;
        }
    }
    return false;
}

bool find_parquet_equality_field_prefix_by_id_path(
        const FieldDescriptor* descriptor,
        const std::vector<const schema::external::TField*>& table_path,
        ParquetEqualityFieldPath* result) {
    DORIS_CHECK(descriptor != nullptr);
    DORIS_CHECK(result != nullptr);
    DORIS_CHECK(!table_path.empty());
    const std::vector<FieldSchema>* candidates = nullptr;
    for (size_t path_index = 0; path_index < table_path.size(); ++path_index) {
        const auto* table_field = table_path[path_index];
        DORIS_CHECK(table_field != nullptr);
        DORIS_CHECK(table_field->__isset.id);
        const FieldSchema* match = nullptr;
        size_t match_index = 0;
        const size_t candidate_count =
                candidates == nullptr ? cast_set<size_t>(descriptor->size()) : candidates->size();
        for (size_t candidate_index = 0; candidate_index < candidate_count; ++candidate_index) {
            const auto* candidate = candidates == nullptr
                                            ? descriptor->get_column(cast_set<int>(candidate_index))
                                            : &(*candidates)[candidate_index];
            if (candidate != nullptr && candidate->field_id == table_field->id) {
                match = candidate;
                match_index = candidate_index;
                break;
            }
        }
        if (match == nullptr) {
            const auto wrapper =
                    candidates == nullptr
                            ? TableSchemaChangeHelper::BuildTableInfoUtil::
                                      find_unique_idless_parquet_wrapper_index(
                                              *table_field, descriptor->get_fields_schema())
                            : TableSchemaChangeHelper::BuildTableInfoUtil::
                                      find_unique_idless_parquet_wrapper_index(*table_field,
                                                                               *candidates);
            if (wrapper.has_value()) {
                match_index = *wrapper;
                match = candidates == nullptr ? descriptor->get_column(cast_set<int>(match_index))
                                              : &(*candidates)[match_index];
            }
        }
        if (match == nullptr) {
            return false;
        }
        if (!result->fields.empty()) {
            result->child_indexes.push_back(match_index);
        }
        result->fields.push_back(match);
        candidates = &match->children;
    }
    return true;
}

std::vector<std::string> equality_field_name_candidates(const schema::external::TField& table_field,
                                                        const std::string* leaf_fallback) {
    std::vector<std::string> candidates;
    if (table_field.__isset.name_mapping) {
        candidates.insert(candidates.end(), table_field.name_mapping.begin(),
                          table_field.name_mapping.end());
        if (table_field.__isset.name_mapping_is_authoritative &&
            table_field.name_mapping_is_authoritative) {
            return candidates;
        }
    }
    if (table_field.__isset.name) {
        candidates.push_back(table_field.name);
    }
    if (leaf_fallback != nullptr) {
        candidates.push_back(*leaf_fallback);
    }
    return candidates;
}

bool find_parquet_equality_field_prefix_by_name_path(
        const FieldDescriptor* descriptor,
        const std::vector<const schema::external::TField*>& table_path,
        const std::string& leaf_fallback, ParquetEqualityFieldPath* result) {
    DORIS_CHECK(descriptor != nullptr);
    DORIS_CHECK(result != nullptr);
    DORIS_CHECK(!table_path.empty());
    const std::vector<FieldSchema>* children = nullptr;
    for (size_t path_index = 0; path_index < table_path.size(); ++path_index) {
        const auto* table_field = table_path[path_index];
        DORIS_CHECK(table_field != nullptr);
        const auto names = equality_field_name_candidates(
                *table_field, path_index + 1 == table_path.size() ? &leaf_fallback : nullptr);
        const FieldSchema* match = nullptr;
        size_t match_index = 0;
        const size_t child_count =
                children == nullptr ? cast_set<size_t>(descriptor->size()) : children->size();
        for (const auto& name : names) {
            for (size_t child_index = 0; child_index < child_count; ++child_index) {
                const auto* child = children == nullptr
                                            ? descriptor->get_column(cast_set<int>(child_index))
                                            : &(*children)[child_index];
                if (child != nullptr && iequal(child->name, name)) {
                    match = child;
                    match_index = child_index;
                    break;
                }
            }
            if (match != nullptr) {
                break;
            }
        }
        if (match == nullptr) {
            return false;
        }
        if (!result->fields.empty()) {
            result->child_indexes.push_back(match_index);
        }
        result->fields.push_back(match);
        children = &match->children;
    }
    return true;
}

struct OrcEqualityFieldPath {
    std::vector<const orc::Type*> fields;
    std::vector<std::string> names;
    std::vector<size_t> child_indexes;
};

bool find_orc_equality_field_path_by_id(const orc::Type* root, int32_t field_id,
                                        OrcEqualityFieldPath* result) {
    DORIS_CHECK(root != nullptr);
    DORIS_CHECK(result != nullptr);
    const auto find = [field_id](const auto& self, const orc::Type* field,
                                 const std::string& field_name,
                                 OrcEqualityFieldPath* path) -> bool {
        DORIS_CHECK(field != nullptr);
        path->fields.push_back(field);
        path->names.push_back(field_name);
        if (field->hasAttributeKey(kIcebergOrcAttribute) &&
            std::stoi(field->getAttributeValue(kIcebergOrcAttribute)) == field_id) {
            return true;
        }
        for (size_t index = 0; index < field->getSubtypeCount(); ++index) {
            path->child_indexes.push_back(index);
            if (self(self, field->getSubtype(index), field->getFieldName(index), path)) {
                return true;
            }
            path->child_indexes.pop_back();
        }
        path->fields.pop_back();
        path->names.pop_back();
        return false;
    };
    for (size_t index = 0; index < root->getSubtypeCount(); ++index) {
        if (find(find, root->getSubtype(index), root->getFieldName(index), result)) {
            return true;
        }
    }
    return false;
}

bool find_orc_equality_field_prefix_by_id_path(
        const orc::Type* root, const std::vector<const schema::external::TField*>& table_path,
        OrcEqualityFieldPath* result) {
    DORIS_CHECK(root != nullptr);
    DORIS_CHECK(result != nullptr);
    DORIS_CHECK(!table_path.empty());
    const orc::Type* parent = root;
    for (const auto* table_field : table_path) {
        DORIS_CHECK(table_field != nullptr);
        DORIS_CHECK(table_field->__isset.id);
        const orc::Type* match = nullptr;
        size_t match_index = 0;
        for (size_t candidate_index = 0; candidate_index < parent->getSubtypeCount();
             ++candidate_index) {
            const auto* candidate = parent->getSubtype(candidate_index);
            if (candidate->hasAttributeKey(kIcebergOrcAttribute) &&
                std::stoi(candidate->getAttributeValue(kIcebergOrcAttribute)) == table_field->id) {
                match = candidate;
                match_index = candidate_index;
                break;
            }
        }
        if (match == nullptr) {
            const auto wrapper = TableSchemaChangeHelper::BuildTableInfoUtil::
                    find_unique_idless_orc_wrapper_index(*table_field, parent,
                                                         kIcebergOrcAttribute);
            if (wrapper.has_value()) {
                match_index = *wrapper;
                match = parent->getSubtype(match_index);
            }
        }
        if (match == nullptr) {
            return false;
        }
        if (!result->fields.empty()) {
            result->child_indexes.push_back(match_index);
        }
        result->fields.push_back(match);
        result->names.push_back(parent->getFieldName(match_index));
        parent = match;
    }
    return true;
}

bool find_orc_equality_field_prefix_by_name_path(
        const orc::Type* root, const std::vector<const schema::external::TField*>& table_path,
        const std::string& leaf_fallback, OrcEqualityFieldPath* result) {
    DORIS_CHECK(root != nullptr);
    DORIS_CHECK(result != nullptr);
    DORIS_CHECK(!table_path.empty());
    const orc::Type* parent = root;
    for (size_t path_index = 0; path_index < table_path.size(); ++path_index) {
        const auto* table_field = table_path[path_index];
        DORIS_CHECK(table_field != nullptr);
        const auto names = equality_field_name_candidates(
                *table_field, path_index + 1 == table_path.size() ? &leaf_fallback : nullptr);
        const orc::Type* match = nullptr;
        size_t match_index = 0;
        for (const auto& name : names) {
            for (size_t child_index = 0; child_index < parent->getSubtypeCount(); ++child_index) {
                if (iequal(parent->getFieldName(child_index), name)) {
                    match = parent->getSubtype(child_index);
                    match_index = child_index;
                    break;
                }
            }
            if (match != nullptr) {
                break;
            }
        }
        if (match == nullptr) {
            return false;
        }
        if (!result->fields.empty()) {
            result->child_indexes.push_back(match_index);
        }
        result->fields.push_back(match);
        result->names.push_back(parent->getFieldName(match_index));
        parent = match;
    }
    return true;
}

} // namespace

const std::string IcebergOrcReader::ICEBERG_ORC_ATTRIBUTE = kIcebergOrcAttribute;

bool IcebergTableReader::_is_fully_dictionary_encoded(
        const tparquet::ColumnMetaData& column_metadata) {
    const auto is_dictionary_encoding = [](tparquet::Encoding::type encoding) {
        return encoding == tparquet::Encoding::PLAIN_DICTIONARY ||
               encoding == tparquet::Encoding::RLE_DICTIONARY;
    };
    const auto is_data_page = [](tparquet::PageType::type page_type) {
        return page_type == tparquet::PageType::DATA_PAGE ||
               page_type == tparquet::PageType::DATA_PAGE_V2;
    };
    const auto is_level_encoding = [](tparquet::Encoding::type encoding) {
        return encoding == tparquet::Encoding::RLE || encoding == tparquet::Encoding::BIT_PACKED;
    };

    // A column chunk may have a dictionary page but still contain plain-encoded data pages.
    // Only treat it as dictionary-coded when all data pages are dictionary encoded.
    if (column_metadata.__isset.encoding_stats) {
        bool has_data_page_stats = false;
        for (const tparquet::PageEncodingStats& enc_stat : column_metadata.encoding_stats) {
            if (is_data_page(enc_stat.page_type) && enc_stat.count > 0) {
                has_data_page_stats = true;
                if (!is_dictionary_encoding(enc_stat.encoding)) {
                    return false;
                }
            }
        }
        if (has_data_page_stats) {
            return true;
        }
    }

    bool has_dict_encoding = false;
    bool has_nondict_encoding = false;
    for (const tparquet::Encoding::type& encoding : column_metadata.encodings) {
        if (is_dictionary_encoding(encoding)) {
            has_dict_encoding = true;
        }

        if (!is_dictionary_encoding(encoding) && !is_level_encoding(encoding)) {
            has_nondict_encoding = true;
            break;
        }
    }
    if (!has_dict_encoding || has_nondict_encoding) {
        return false;
    }

    return true;
}

IcebergTableReader::IcebergTableReader(std::unique_ptr<GenericReader> file_format_reader,
                                       RuntimeProfile* profile, RuntimeState* state,
                                       const TFileScanRangeParams& params,
                                       const TFileRangeDesc& range, ShardedKVCache* kv_cache,
                                       io::IOContext* io_ctx, FileMetaCache* meta_cache)
        : TableFormatReader(std::move(file_format_reader), state, profile, params, range, io_ctx,
                            meta_cache),
          _kv_cache(kv_cache) {
    static const char* iceberg_profile = "IcebergProfile";
    ADD_TIMER(_profile, iceberg_profile);
    _iceberg_profile.num_delete_files =
            ADD_CHILD_COUNTER(_profile, "NumDeleteFiles", TUnit::UNIT, iceberg_profile);
    _iceberg_profile.num_delete_rows =
            ADD_CHILD_COUNTER(_profile, "NumDeleteRows", TUnit::UNIT, iceberg_profile);
    _iceberg_profile.delete_files_read_time =
            ADD_CHILD_TIMER(_profile, "DeleteFileReadTime", iceberg_profile);
    _iceberg_profile.delete_rows_sort_time =
            ADD_CHILD_TIMER(_profile, "DeleteRowsSortTime", iceberg_profile);
    _iceberg_profile.parse_delete_file_time =
            ADD_CHILD_TIMER(_profile, "ParseDeleteFileTime", iceberg_profile);
}

Status IcebergTableReader::get_next_block_inner(Block* block, size_t* read_rows, bool* eof) {
    RETURN_IF_ERROR(_expand_block_if_need(block));

    RETURN_IF_ERROR(_file_format_reader->get_next_block(block, read_rows, eof));
    RETURN_IF_ERROR(_materialize_missing_table_columns(block));
    RETURN_IF_ERROR(_materialize_missing_equality_delete_columns(block));
    RETURN_IF_ERROR(_materialize_nested_equality_delete_columns(block));
    RETURN_IF_ERROR(_apply_iceberg_row_filters(block));

    *read_rows = block->rows();
    return _shrink_block_if_need(block);
}

Status IcebergTableReader::set_fill_columns(
        const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&
                partition_columns,
        const std::unordered_map<std::string, VExprContextSPtr>& missing_columns,
        const std::unordered_map<std::string, bool>& partition_value_is_null) {
    auto iceberg_missing_columns = missing_columns;
    if (supports_iceberg_scan_semantics_v1(&_params)) {
        const auto struct_node =
                std::dynamic_pointer_cast<TableSchemaChangeHelper::StructNode>(table_info_node_ptr);
        DORIS_CHECK(struct_node != nullptr);
        const bool use_v2_semantics = supports_iceberg_scan_semantics_v2(&_params);
        for (auto& [column_name, default_expr] : iceberg_missing_columns) {
            if (struct_node->children_column_exists(column_name)) {
                continue;
            }
            const auto* field = struct_node->get_missing_column_field(column_name);
            if (field == nullptr || (!use_v2_semantics && !field->__isset.initial_default_value)) {
                continue;
            }
            const auto type = _required_column_types.find(column_name);
            DORIS_CHECK(type != _required_column_types.end());
            ColumnPtr default_column;
            RETURN_IF_ERROR(iceberg::create_initial_default_column(
                    *field, type->second, &default_column, &_state->timezone_obj()));
            default_expr = VExprContext::create_shared(
                    VLiteral::create_shared(type->second, (*default_column)[0]));
        }
        for (const auto& column_name : _physical_missing_equality_delete_columns) {
            iceberg_missing_columns.emplace(column_name, nullptr);
        }
    }
    return _file_format_reader->set_fill_columns(partition_columns, iceberg_missing_columns,
                                                 partition_value_is_null);
}

const schema::external::TStructField* IcebergTableReader::_current_schema_root() const {
    if (!_params.__isset.history_schema_info || _params.history_schema_info.empty()) {
        return nullptr;
    }
    const schema::external::TSchema* current_schema = &_params.history_schema_info.front();
    if (_params.__isset.current_schema_id) {
        for (const auto& schema : _params.history_schema_info) {
            if (schema.__isset.schema_id && schema.schema_id == _params.current_schema_id) {
                current_schema = &schema;
                break;
            }
        }
    }
    return current_schema->__isset.root_field ? &current_schema->root_field : nullptr;
}

const schema::external::TField* IcebergTableReader::_find_current_schema_field(
        const std::string& name) const {
    const auto* root = _current_schema_root();
    if (root == nullptr || !root->__isset.fields) {
        return nullptr;
    }
    for (const auto& field_ptr : root->fields) {
        if (field_ptr.__isset.field_ptr && field_ptr.field_ptr != nullptr &&
            field_ptr.field_ptr->__isset.name && iequal(field_ptr.field_ptr->name, name)) {
            return field_ptr.field_ptr.get();
        }
    }
    return nullptr;
}

bool IcebergTableReader::_find_schema_field_path_in_field(
        const schema::external::TField* field, int32_t field_id,
        std::vector<const schema::external::TField*>* path) {
    DORIS_CHECK(path != nullptr);
    if (field == nullptr) {
        return false;
    }
    path->push_back(field);
    if (field->__isset.id && field->id == field_id) {
        return true;
    }
    if (field->__isset.nestedField) {
        if (field->nestedField.__isset.struct_field &&
            field->nestedField.struct_field.__isset.fields) {
            for (const auto& child_ptr : field->nestedField.struct_field.fields) {
                if (child_ptr.__isset.field_ptr && child_ptr.field_ptr != nullptr &&
                    _find_schema_field_path_in_field(child_ptr.field_ptr.get(), field_id, path)) {
                    return true;
                }
            }
        } else if (field->nestedField.__isset.array_field &&
                   field->nestedField.array_field.__isset.item_field) {
            const auto& child_ptr = field->nestedField.array_field.item_field;
            if (child_ptr.__isset.field_ptr && child_ptr.field_ptr != nullptr &&
                _find_schema_field_path_in_field(child_ptr.field_ptr.get(), field_id, path)) {
                return true;
            }
        } else if (field->nestedField.__isset.map_field) {
            const auto& map = field->nestedField.map_field;
            if (map.__isset.key_field && map.key_field.__isset.field_ptr &&
                map.key_field.field_ptr != nullptr &&
                _find_schema_field_path_in_field(map.key_field.field_ptr.get(), field_id, path)) {
                return true;
            }
            if (map.__isset.value_field && map.value_field.__isset.field_ptr &&
                map.value_field.field_ptr != nullptr &&
                _find_schema_field_path_in_field(map.value_field.field_ptr.get(), field_id, path)) {
                return true;
            }
        }
    }
    path->pop_back();
    return false;
}

bool IcebergTableReader::_find_schema_field_path_in_root(
        const schema::external::TStructField* root, int32_t field_id,
        std::vector<const schema::external::TField*>* path) {
    DORIS_CHECK(path != nullptr);
    if (root == nullptr || !root->__isset.fields) {
        return false;
    }
    for (const auto& field_ptr : root->fields) {
        if (field_ptr.__isset.field_ptr && field_ptr.field_ptr != nullptr &&
            _find_schema_field_path_in_field(field_ptr.field_ptr.get(), field_id, path)) {
            return true;
        }
    }
    return false;
}

std::vector<const schema::external::TField*> IcebergTableReader::_find_schema_field_path(
        int32_t field_id) const {
    std::vector<const schema::external::TField*> path;
    if (_find_schema_field_path_in_root(_current_schema_root(), field_id, &path)) {
        return path;
    }
    const auto& iceberg_params = _range.table_format_params.iceberg_params;
    if (iceberg_params.__isset.equality_delete_schema &&
        iceberg_params.equality_delete_schema.__isset.root_field) {
        path.clear();
        if (_find_schema_field_path_in_root(&iceberg_params.equality_delete_schema.root_field,
                                            field_id, &path)) {
            return path;
        }
    }
    if (!_params.__isset.history_schema_info) {
        return {};
    }
    for (const auto& schema : _params.history_schema_info) {
        if (!schema.__isset.root_field) {
            continue;
        }
        path.clear();
        if (_find_schema_field_path_in_root(&schema.root_field, field_id, &path)) {
            return path;
        }
    }
    return {};
}

Status IcebergTableReader::_materialize_missing_table_columns(Block* block) {
    if (!supports_iceberg_scan_semantics_v1(&_params)) {
        return Status::OK();
    }
    const auto struct_node =
            std::dynamic_pointer_cast<TableSchemaChangeHelper::StructNode>(table_info_node_ptr);
    if (struct_node == nullptr) {
        return Status::OK();
    }
    const bool use_v2_semantics = supports_iceberg_scan_semantics_v2(&_params);
    for (const auto& col_name : _all_required_col_names) {
        if (_row_lineage_columns != nullptr &&
            (col_name == ROW_LINEAGE_ROW_ID || col_name == ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER)) {
            continue;
        }
        // Equality-delete carriers are hidden reader columns, not table fields. They are populated
        // from another projected column after ordinary missing table defaults are materialized.
        if (_physical_missing_equality_delete_columns.contains(col_name)) {
            continue;
        }
        if (struct_node->children_column_exists(col_name)) {
            continue;
        }
        const auto* field = struct_node->get_missing_column_field(col_name);
        if (field == nullptr || (!use_v2_semantics && !field->__isset.initial_default_value)) {
            continue;
        }
        DORIS_CHECK(_col_name_to_block_idx != nullptr);
        const auto position = _col_name_to_block_idx->find(col_name);
        if (position == _col_name_to_block_idx->end()) {
            return Status::InternalError("Missing column: {} not found in block {}", col_name,
                                         block->dump_structure());
        }
        DORIS_CHECK(position->second < block->columns());
        auto default_value = _missing_initial_default_values.find(col_name);
        if (default_value == _missing_initial_default_values.end()) {
            ColumnPtr value;
            RETURN_IF_ERROR(iceberg::create_initial_default_column(
                    *field, block->get_by_position(position->second).type, &value,
                    &_state->timezone_obj()));
            default_value =
                    _missing_initial_default_values.emplace(col_name, std::move(value)).first;
        }
        // Parquet and ORC have already filled every missing column with placeholders. Replace the
        // whole accumulated column using the filtered Block size: the physical read count may be
        // larger when predicates remove rows, while row-id fetch paths may retain earlier batches.
        const size_t materialized_rows = block->rows();
        block->get_by_position(position->second).column =
                iceberg::repeat_initial_default_column(default_value->second, materialized_rows);
    }
    return Status::OK();
}

Status IcebergTableReader::_validate_projected_missing_required_fields() const {
    if (!supports_iceberg_scan_semantics_v2(&_params)) {
        return Status::OK();
    }
    const auto struct_mapping =
            std::dynamic_pointer_cast<TableSchemaChangeHelper::StructNode>(table_info_node_ptr);
    DORIS_CHECK(struct_mapping != nullptr);
    for (const auto& [field_id, column_name] : _id_to_block_column_name) {
        if (std::ranges::find(_all_required_col_names, column_name) ==
            _all_required_col_names.end()) {
            continue;
        }
        if (_row_lineage_columns != nullptr &&
            (column_name == ROW_LINEAGE_ROW_ID ||
             column_name == ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER)) {
            continue;
        }
        std::vector<const schema::external::TField*> path;
        if (!_find_schema_field_path_in_root(_current_schema_root(), field_id, &path)) {
            continue;
        }
        DORIS_CHECK(path.size() == 1);
        const auto data_type = _required_column_types.find(column_name);
        DORIS_CHECK(data_type != _required_column_types.end());
        DORIS_CHECK(struct_mapping->get_children().contains(column_name));
        if (!struct_mapping->children_column_exists(column_name)) {
            const auto* missing_field = struct_mapping->get_missing_column_field(column_name);
            DORIS_CHECK(missing_field != nullptr);
            RETURN_IF_ERROR(validate_projected_missing_iceberg_field(
                    *missing_field, data_type->second, &_state->timezone_obj()));
            continue;
        }
        // FE replaces the file-scan SlotDescriptor type with NestedColumnPruning's pruned type.
        // Recursing through this DataType therefore validates only projected nested children even
        // though BuildTableInfo retains the complete table-schema mapping.
        RETURN_IF_ERROR(validate_projected_missing_required_iceberg_fields(
                *path.front(), data_type->second, struct_mapping->get_children_node(column_name),
                &_state->timezone_obj()));
    }
    return Status::OK();
}

Status IcebergTableReader::_validate_required_table_columns(Block* block) const {
    if (!supports_iceberg_scan_semantics_v2(&_params)) {
        return Status::OK();
    }
    DORIS_CHECK(block != nullptr);
    DORIS_CHECK(_col_name_to_block_idx != nullptr);
    for (const auto& [field_id, column_name] : _id_to_block_column_name) {
        std::vector<const schema::external::TField*> path;
        if (!_find_schema_field_path_in_root(_current_schema_root(), field_id, &path)) {
            continue;
        }
        DORIS_CHECK(path.size() == 1);
        const auto position = _col_name_to_block_idx->find(column_name);
        DORIS_CHECK(position != _col_name_to_block_idx->end());
        DORIS_CHECK(position->second < block->columns());
        const auto data_type = _required_column_types.find(column_name);
        DORIS_CHECK(data_type != _required_column_types.end());
        RETURN_IF_ERROR(validate_iceberg_required_field(
                *path.front(), data_type->second, block->get_by_position(position->second).column));
    }
    return Status::OK();
}

Status IcebergTableReader::_apply_iceberg_row_filters(Block* block) {
    DORIS_CHECK(block != nullptr);
    if (!_equality_delete_impls.empty()) {
        IColumn::Filter filter(block->rows(), 1);
        DORIS_CHECK(_equality_delete_impls.size() == _equality_delete_filter_column_names.size());
        for (size_t filter_index = 0; filter_index < _equality_delete_impls.size();
             ++filter_index) {
            RETURN_IF_ERROR(_equality_delete_impls[filter_index]->filter_data_block(
                    block, _col_name_to_block_idx,
                    _equality_delete_filter_column_names[filter_index], filter));
        }
        Block::filter_block_internal(block, filter, block->columns());
    }
    RETURN_IF_ERROR(_validate_required_table_columns(block));
    return _filter_deferred_required_column_predicates(block);
}

Status IcebergTableReader::_filter_deferred_required_column_predicates(Block* block) const {
    DORIS_CHECK(block != nullptr);
    if (_deferred_required_column_predicates == nullptr || block->rows() == 0) {
        return Status::OK();
    }
    DORIS_CHECK(block->rows() <= std::numeric_limits<uint16_t>::max());
    std::vector<uint16_t> selector(block->rows());
    std::iota(selector.begin(), selector.end(), 0);
    uint16_t selected_rows = 0;
    {
        auto columns_guard = block->mutate_columns_scoped();
        selected_rows = _deferred_required_column_predicates->evaluate(
                columns_guard.mutable_columns(), selector.data(),
                cast_set<uint16_t>(block->rows()));
    }
    IColumn::Filter filter(block->rows(), 0);
    for (uint16_t row = 0; row < selected_rows; ++row) {
        filter[selector[row]] = 1;
    }
    Block::filter_block_internal(block, filter, block->columns());
    return Status::OK();
}

void IcebergTableReader::_prepare_physical_reader_predicates(
        const TupleDescriptor* tuple_descriptor, const VExprContextSPtrs& conjuncts,
        const VExprContextSPtrs* not_single_slot_filter_conjuncts,
        const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts) {
    DORIS_CHECK(tuple_descriptor != nullptr);
    _required_validation_slot_ids.clear();
    const auto* current_root =
            supports_iceberg_scan_semantics_v2(&_params) ? _current_schema_root() : nullptr;
    if (current_root != nullptr) {
        for (const auto* slot : tuple_descriptor->slots()) {
            DORIS_CHECK(slot != nullptr);
            std::vector<const schema::external::TField*> path;
            if (!_find_schema_field_path_in_root(current_root, slot->col_unique_id(), &path)) {
                continue;
            }
            DORIS_CHECK(path.size() == 1);
            if (projected_iceberg_field_requires_required_validation(*path.front(), slot->type())) {
                _required_validation_slot_ids.insert(slot->id());
            }
        }
    }

    const auto keep_for_physical_reader = [&](const VExprContextSPtr& conjunct) {
        DORIS_CHECK(conjunct != nullptr);
        return !expression_references_required_validation_slot(conjunct->root(),
                                                               _required_validation_slot_ids);
    };
    _physical_reader_conjuncts.clear();
    std::ranges::copy_if(conjuncts, std::back_inserter(_physical_reader_conjuncts),
                         keep_for_physical_reader);

    _physical_reader_not_single_slot_filter_conjuncts.clear();
    if (not_single_slot_filter_conjuncts != nullptr) {
        std::ranges::copy_if(*not_single_slot_filter_conjuncts,
                             std::back_inserter(_physical_reader_not_single_slot_filter_conjuncts),
                             keep_for_physical_reader);
    }

    _physical_reader_slot_id_to_filter_conjuncts.clear();
    if (slot_id_to_filter_conjuncts != nullptr) {
        for (const auto& [slot_id, slot_conjuncts] : *slot_id_to_filter_conjuncts) {
            if (!_required_validation_slot_ids.contains(slot_id)) {
                _physical_reader_slot_id_to_filter_conjuncts.emplace(slot_id, slot_conjuncts);
            }
        }
    }
    if (_push_down_agg_type == TPushAggOp::type::COUNT && !_required_validation_slot_ids.empty()) {
        // A physical COUNT block contains only row-count placeholders. Decode the selected
        // required field so requiredness validation observes real values instead of synthetic
        // NULLs, including files that have no applicable delete file of their own.
        _file_format_reader->set_push_down_agg_type(TPushAggOp::type::NONE);
    }
}

// This helper keeps V1/V2 equality-delete fallback semantics together; DORIS_CHECK expansion
// pushes the measured complexity just above the threshold.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
Status IcebergTableReader::_create_missing_equality_delete_value(int32_t field_id,
                                                                 const DataTypePtr& delete_key_type,
                                                                 size_t physical_path_size,
                                                                 ColumnPtr* value) const {
    DORIS_CHECK(delete_key_type != nullptr);
    DORIS_CHECK(value != nullptr);
    const auto table_path = _find_schema_field_path(field_id);
    if (table_path.empty()) {
        return Status::InternalError(
                "Missing Iceberg schema metadata for equality-delete field id {}", field_id);
    }
    const size_t missing_index =
            physical_path_size < table_path.size() ? physical_path_size : table_path.size() - 1;
    const auto* missing_field = table_path[missing_index];
    DORIS_CHECK(missing_field != nullptr);

    if (!supports_iceberg_scan_semantics_v2(&_params) &&
        !missing_field->__isset.initial_default_value) {
        *value = delete_key_type->create_column_const(1, Field());
        return Status::OK();
    }

    DataTypePtr missing_type = delete_key_type;
    for (size_t index = table_path.size(); index > missing_index + 1; --index) {
        const auto* parent = table_path[index - 2];
        const auto* child = table_path[index - 1];
        DORIS_CHECK(parent != nullptr);
        DORIS_CHECK(child != nullptr);
        DORIS_CHECK(child->__isset.name);
        if (!parent->__isset.nestedField || !parent->nestedField.__isset.struct_field) {
            return Status::NotSupported(
                    "Iceberg equality delete field id {} has a non-struct missing ancestor",
                    field_id);
        }
        missing_type = std::make_shared<DataTypeStruct>(DataTypes {std::move(missing_type)},
                                                        Strings {child->name});
        if (parent->__isset.is_optional && parent->is_optional) {
            missing_type = make_nullable(missing_type);
        }
    }

    ColumnPtr missing_root_value;
    RETURN_IF_ERROR(iceberg::create_initial_default_column(
            *missing_field, missing_type, &missing_root_value, &_state->timezone_obj()));
    if (missing_index + 1 == table_path.size()) {
        *value = std::move(missing_root_value);
        return Status::OK();
    }
    const IColumn* current = missing_root_value.get();
    bool is_null = false;
    for (size_t index = missing_index + 1; index < table_path.size(); ++index) {
        if (const auto* nullable = check_and_get_column<ColumnNullable>(*current);
            nullable != nullptr) {
            DORIS_CHECK(nullable->size() == 1);
            is_null = is_null || nullable->is_null_at(0);
            current = &nullable->get_nested_column();
        }
        const auto* struct_column = check_and_get_column<ColumnStruct>(*current);
        DORIS_CHECK(struct_column != nullptr);
        DORIS_CHECK(struct_column->tuple_size() == 1);
        current = &struct_column->get_column(0);
    }
    if (const auto* nullable = check_and_get_column<ColumnNullable>(*current);
        nullable != nullptr) {
        DORIS_CHECK(nullable->size() == 1);
        is_null = is_null || nullable->is_null_at(0);
        current = &nullable->get_nested_column();
    }
    auto result = ColumnNullable::create(remove_nullable(delete_key_type)->create_column(),
                                         ColumnUInt8::create());
    if (is_null) {
        result->insert_default();
    } else {
        result->get_nested_column().insert_from(*current, 0);
        result->get_null_map_data().push_back(0);
    }
    *value = std::move(result);
    return Status::OK();
}

Status IcebergTableReader::_register_missing_equality_delete_column(
        int32_t field_id, const std::string& name, const DataTypePtr& delete_key_type) {
    DORIS_CHECK(delete_key_type != nullptr);
    ColumnPtr default_column;
    RETURN_IF_ERROR(
            _create_missing_equality_delete_value(field_id, delete_key_type, 0, &default_column));
    const bool inserted =
            _missing_equality_delete_values.emplace(name, std::move(default_column)).second;
    DORIS_CHECK(inserted);
    return Status::OK();
}

std::string IcebergTableReader::_get_or_register_equality_delete_carrier(
        int32_t field_id, const std::string& source_name, const DataTypePtr& delete_key_type) {
    DORIS_CHECK(delete_key_type != nullptr);
    const auto key = std::make_pair(field_id, delete_key_type->get_name());
    const auto existing = _equality_delete_carriers.find(key);
    if (existing != _equality_delete_carriers.end()) {
        return existing->second;
    }

    const std::string carrier_name = "__equality_delete_column__" + std::to_string(field_id) + "_" +
                                     std::to_string(_equality_delete_carriers.size());
    _expand_col_names.push_back(source_name);
    _expand_col_field_ids.push_back(field_id);
    _expand_columns.emplace_back(delete_key_type->create_column(), delete_key_type, carrier_name);
    _equality_delete_carriers.emplace(key, carrier_name);
    return carrier_name;
}

Status IcebergTableReader::_materialize_missing_equality_delete_columns(Block* block) {
    DORIS_CHECK(block != nullptr);
    const size_t rows = block->rows();
    for (const auto& [name, value] : _missing_equality_delete_values) {
        const auto position = _col_name_to_block_idx->find(name);
        const ColumnPtr repeated = iceberg::repeat_initial_default_column(value, rows);
        if (position == _col_name_to_block_idx->end()) {
            const auto expand_col = std::find_if(
                    _expand_columns.begin(), _expand_columns.end(),
                    [&](const ColumnWithTypeAndName& col) { return col.name == name; });
            DORIS_CHECK(expand_col != _expand_columns.end());
            (*_col_name_to_block_idx)[name] = block->columns();
            block->insert({repeated, expand_col->type, name});
            continue;
        }
        DORIS_CHECK(position->second < block->columns());
        block->get_by_position(position->second).column = repeated;
    }
    return Status::OK();
}

Status IcebergTableReader::_prepare_nested_equality_delete_column(
        NestedEqualityDeleteColumn* nested_field) const {
    DORIS_CHECK(nested_field != nullptr);
    DORIS_CHECK(nested_field->source_leaf_type != nullptr);
    DORIS_CHECK(nested_field->leaf_type != nullptr);
    const DataTypePtr source_type = make_nullable(remove_nullable(nested_field->source_leaf_type));
    const DataTypePtr target_type = make_nullable(remove_nullable(nested_field->leaf_type));
    if (source_type->equals(*target_type)) {
        return Status::OK();
    }

    auto slot_ref = VSlotRef::create_shared(0, 0, -1, source_type, nested_field->block_name);
    auto cast_expr = format::Cast::create_shared(target_type);
    cast_expr->add_child(std::move(slot_ref));
    nested_field->cast_context = VExprContext::create_shared(std::move(cast_expr));
    RowDescriptor row_desc;
    RETURN_IF_ERROR(nested_field->cast_context->prepare(_state, row_desc));
    return nested_field->cast_context->open(_state);
}

Status IcebergTableReader::_extract_nested_equality_delete_column(
        const ColumnPtr& root_column, const NestedEqualityDeleteColumn& nested_field,
        ColumnPtr* leaf_column) const {
    DORIS_CHECK(static_cast<bool>(root_column));
    DORIS_CHECK(nested_field.leaf_type != nullptr);
    DORIS_CHECK(leaf_column != nullptr);
    const IColumn* current = root_column.get();
    std::vector<const NullMap*> ancestor_null_maps;
    for (size_t child_index : nested_field.child_indexes) {
        if (const auto* nullable = check_and_get_column<ColumnNullable>(*current);
            nullable != nullptr) {
            ancestor_null_maps.push_back(&nullable->get_null_map_data());
            current = &nullable->get_nested_column();
        }
        const auto* struct_column = check_and_get_column<ColumnStruct>(*current);
        if (struct_column == nullptr || child_index >= struct_column->tuple_size()) {
            return Status::InternalError(
                    "Iceberg equality delete path for field id {} is absent from column {}",
                    nested_field.field_id, root_column->get_name());
        }
        current = &struct_column->get_column(child_index);
    }
    if (const auto* nullable = check_and_get_column<ColumnNullable>(*current);
        nullable != nullptr) {
        ancestor_null_maps.push_back(&nullable->get_null_map_data());
        current = &nullable->get_nested_column();
    }
    ColumnPtr repeated_missing_value;
    if (static_cast<bool>(nested_field.missing_value)) {
        repeated_missing_value = iceberg::repeat_initial_default_column(nested_field.missing_value,
                                                                        root_column->size());
        current = repeated_missing_value.get();
        if (const auto* nullable = check_and_get_column<ColumnNullable>(*current);
            nullable != nullptr) {
            ancestor_null_maps.push_back(&nullable->get_null_map_data());
            current = &nullable->get_nested_column();
        }
    }

    DORIS_CHECK(nested_field.source_leaf_type != nullptr);
    auto result = ColumnNullable::create(
            remove_nullable(nested_field.source_leaf_type)->create_column(), ColumnUInt8::create());
    auto& result_data = result->get_nested_column();
    auto& result_null_map = result->get_null_map_data();
    result_data.reserve(root_column->size());
    result_null_map.reserve(root_column->size());
    for (size_t row = 0; row < root_column->size(); ++row) {
        bool is_null = false;
        for (const auto* null_map : ancestor_null_maps) {
            if ((*null_map)[row] != 0) {
                is_null = true;
                break;
            }
        }
        if (is_null) {
            result_data.insert_default();
            result_null_map.push_back(1);
        } else {
            result_data.insert_from(*current, row);
            result_null_map.push_back(0);
        }
    }
    const DataTypePtr source_type = make_nullable(remove_nullable(nested_field.source_leaf_type));
    const DataTypePtr target_type = make_nullable(remove_nullable(nested_field.leaf_type));
    if (source_type->equals(*target_type)) {
        *leaf_column = std::move(result);
        return Status::OK();
    }

    DORIS_CHECK(nested_field.cast_context != nullptr);
    Block cast_block;
    cast_block.insert({std::move(result), source_type, nested_field.block_name});
    return nested_field.cast_context->execute(&cast_block, *leaf_column);
}

Status IcebergTableReader::_materialize_nested_equality_delete_columns(Block* block) {
    DORIS_CHECK(block != nullptr);
    struct MaterializedColumn {
        uint32_t position;
        ColumnPtr column;
        DataTypePtr type;
    };
    std::vector<MaterializedColumn> materialized_columns;
    materialized_columns.reserve(_nested_equality_delete_columns.size());
    for (const auto& nested_field : _nested_equality_delete_columns) {
        const std::string& source_name = nested_field.source_block_name.empty()
                                                 ? nested_field.block_name
                                                 : nested_field.source_block_name;
        const auto source_position = _col_name_to_block_idx->find(source_name);
        DORIS_CHECK(source_position != _col_name_to_block_idx->end());
        DORIS_CHECK(source_position->second < block->columns());
        const auto target_position = _col_name_to_block_idx->find(nested_field.block_name);
        DORIS_CHECK(target_position != _col_name_to_block_idx->end());
        DORIS_CHECK(target_position->second < block->columns());
        ColumnPtr leaf;
        RETURN_IF_ERROR(_extract_nested_equality_delete_column(
                block->get_by_position(source_position->second).column, nested_field, &leaf));
        materialized_columns.push_back(
                {target_position->second, std::move(leaf), make_nullable(nested_field.leaf_type)});
    }
    for (auto& materialized : materialized_columns) {
        auto& column = block->get_by_position(materialized.position);
        column.column = std::move(materialized.column);
        column.type = std::move(materialized.type);
    }
    return Status::OK();
}

Status IcebergTableReader::_get_projected_schema_equality_delete_path(
        int32_t field_id, std::vector<size_t>* child_indexes, DataTypePtr* leaf_type,
        bool* path_is_projected) const {
    DORIS_CHECK(child_indexes != nullptr);
    DORIS_CHECK(leaf_type != nullptr);
    DORIS_CHECK(path_is_projected != nullptr);
    child_indexes->clear();
    *path_is_projected = false;
    const auto path = _find_schema_field_path(field_id);
    if (path.empty()) {
        return Status::InternalError("Missing Iceberg schema path for equality-delete field id {}",
                                     field_id);
    }
    DORIS_CHECK(path.front()->__isset.id);
    const auto root_name = _id_to_block_column_name.find(path.front()->id);
    DORIS_CHECK(root_name != _id_to_block_column_name.end());
    const auto root_type = _required_column_types.find(root_name->second);
    DORIS_CHECK(root_type != _required_column_types.end());
    std::vector<const schema::external::TField*> projected_root_path;
    if (!_find_schema_field_path_in_root(_current_schema_root(), path.front()->id,
                                         &projected_root_path)) {
        return Status::OK();
    }
    DORIS_CHECK(projected_root_path.size() == 1);
    const auto* projected_parent = projected_root_path.front();
    DataTypePtr current_type = root_type->second;
    for (size_t path_index = 1; path_index < path.size(); ++path_index) {
        const auto* child = path[path_index];
        DORIS_CHECK(projected_parent != nullptr);
        DORIS_CHECK(child != nullptr);
        if (!projected_parent->__isset.nestedField ||
            !projected_parent->nestedField.__isset.struct_field ||
            !projected_parent->nestedField.struct_field.__isset.fields) {
            return Status::NotSupported(
                    "Iceberg equality-delete field id {} has a non-struct projected-schema parent",
                    field_id);
        }
        DORIS_CHECK(child->__isset.name);
        DORIS_CHECK(child->__isset.id);
        const auto* struct_type =
                typeid_cast<const DataTypeStruct*>(remove_nullable(current_type).get());
        if (struct_type == nullptr) {
            return Status::OK();
        }
        const auto projected_child =
                find_projected_iceberg_struct_child(*projected_parent, *child, *struct_type);
        if (!projected_child.has_value()) {
            return Status::OK();
        }
        child_indexes->push_back(projected_child->index);
        current_type = struct_type->get_element(projected_child->index);
        projected_parent = projected_child->field;
    }
    *leaf_type = make_nullable(remove_nullable(current_type));
    *path_is_projected = true;
    return Status::OK();
}

Status IcebergTableReader::init_row_filters() {
    // We get the count value by doris's be, so we don't need to read the delete file.
    // A table-level row count of 0 (e.g. an all-deleted table read with ignore_iceberg_dangling_delete,
    // where total-records == total-position-deletes) is still a valid pushed-down count, so accept >= 0.
    // FE sends -1 when there is no table-level count; using > 0 here would drop a genuine 0 into the
    // delete-applying path below and never produce the intended CountReader(0).
    if (_push_down_agg_type == TPushAggOp::type::COUNT && _table_level_row_count >= 0) {
        return Status::OK();
    }

    const auto& table_desc = _range.table_format_params.iceberg_params;

    auto* parquet_reader = dynamic_cast<ParquetReader*>(_file_format_reader.get());
    auto* orc_reader = dynamic_cast<OrcReader*>(_file_format_reader.get());

    // Initialize file information for $row_id generation
    // Extract from table_desc which contains current file's metadata.
    // NOTE: row-id generation only needs the data file path / partition info / row positions,
    // which are independent of delete-file support, so it MUST be set up before the
    // format-version gate below. The FE adds the hidden __DORIS_ICEBERG_ROWID_COL__ column
    // whenever show_hidden_columns is on, regardless of format version (see
    // IcebergExternalTable.getFullSchema). If a v1 table selects this column we still have to
    // fill it; otherwise it stays empty while the other columns are filtered down, tripping the
    // `block->rows() == col.column->size()` check in RowGroupReader::_do_lazy_read.
    if (_need_row_id_column) {
        std::string file_path = table_desc.original_file_path;
        int32_t partition_spec_id = 0;
        std::string partition_data_json;
        if (table_desc.__isset.partition_spec_id) {
            partition_spec_id = table_desc.partition_spec_id;
        }
        if (table_desc.__isset.partition_data_json) {
            partition_data_json = table_desc.partition_data_json;
        }

        if (parquet_reader != nullptr) {
            parquet_reader->set_iceberg_rowid_params(file_path, partition_spec_id,
                                                     partition_data_json, _row_id_column_position);
        } else if (orc_reader != nullptr) {
            orc_reader->set_iceberg_rowid_params(file_path, partition_spec_id, partition_data_json,
                                                 _row_id_column_position);
        }
        LOG(INFO) << "Initialized $row_id generation for file: " << file_path
                  << ", partition_spec_id: " << partition_spec_id;
    }

    const auto& version = table_desc.format_version;
    if (version < MIN_SUPPORT_DELETE_FILES_VERSION) {
        return Status::OK();
    }

    std::vector<TIcebergDeleteFileDesc> position_delete_files;
    std::vector<TIcebergDeleteFileDesc> equality_delete_files;
    std::vector<TIcebergDeleteFileDesc> deletion_vector_files;
    for (const TIcebergDeleteFileDesc& desc : table_desc.delete_files) {
        if (desc.content == POSITION_DELETE) {
            position_delete_files.emplace_back(desc);
        } else if (desc.content == EQUALITY_DELETE) {
            equality_delete_files.emplace_back(desc);
        } else if (desc.content == DELETION_VECTOR) {
            deletion_vector_files.emplace_back(desc);
        }
    }

    if (!equality_delete_files.empty()) {
        RETURN_IF_ERROR(_process_equality_delete(equality_delete_files));
        _file_format_reader->set_push_down_agg_type(TPushAggOp::NONE);
    }

    if (!deletion_vector_files.empty()) {
        if (deletion_vector_files.size() != 1) [[unlikely]] {
            /*
             * Deletion vectors are a binary representation of deletes for a single data file that is more efficient
             * at execution time than position delete files. Unlike equality or position delete files, there can be
             * at most one deletion vector for a given data file in a snapshot.
             */
            return Status::DataQualityError("This iceberg data file has multiple DVs.");
        }
        RETURN_IF_ERROR(
                read_deletion_vector(table_desc.original_file_path, deletion_vector_files[0]));

        _file_format_reader->set_push_down_agg_type(TPushAggOp::NONE);
        // Readers can safely ignore position delete files if there is a DV for a data file.
    } else if (!position_delete_files.empty()) {
        RETURN_IF_ERROR(
                _position_delete_base(table_desc.original_file_path, position_delete_files));
        _file_format_reader->set_push_down_agg_type(TPushAggOp::NONE);
    }

    COUNTER_UPDATE(_iceberg_profile.num_delete_files, table_desc.delete_files.size());
    return Status::OK();
}

void IcebergTableReader::_generate_equality_delete_block(
        Block* block, const std::vector<std::string>& equality_delete_col_names,
        const std::vector<DataTypePtr>& equality_delete_col_types) {
    for (int i = 0; i < equality_delete_col_names.size(); ++i) {
        DataTypePtr data_type = make_nullable(equality_delete_col_types[i]);
        MutableColumnPtr data_column = data_type->create_column();
        block->insert(ColumnWithTypeAndName(std::move(data_column), data_type,
                                            equality_delete_col_names[i]));
    }
}

Status IcebergTableReader::_expand_block_if_need(Block* block) {
    std::set<std::string> names;
    auto block_names = block->get_names();
    names.insert(block_names.begin(), block_names.end());
    for (auto& col : _expand_columns) {
        if (_missing_equality_delete_values.contains(col.name)) {
            continue;
        }
        auto mutable_column = IColumn::mutate(std::move(col.column));
        mutable_column->clear();
        col.column = std::move(mutable_column);
        if (names.contains(col.name)) {
            return Status::InternalError("Wrong expand column '{}'", col.name);
        }
        names.insert(col.name);
        (*_col_name_to_block_idx)[col.name] = static_cast<uint32_t>(block->columns());
        block->insert(col);
    }
    return Status::OK();
}

Status IcebergTableReader::_shrink_block_if_need(Block* block) {
    std::set<size_t> positions_to_erase;
    for (const std::string& expand_col : _expand_col_names) {
        if (!_col_name_to_block_idx->contains(expand_col)) {
            return Status::InternalError("Wrong erase column '{}', block: {}", expand_col,
                                         block->dump_names());
        }
        positions_to_erase.emplace((*_col_name_to_block_idx)[expand_col]);
    }
    block->erase(positions_to_erase);
    for (const std::string& expand_col : _expand_col_names) {
        _col_name_to_block_idx->erase(expand_col);
    }
    return Status::OK();
}

Status IcebergTableReader::_position_delete_base(
        const std::string data_file_path, const std::vector<TIcebergDeleteFileDesc>& delete_files) {
    std::vector<DeleteRows*> delete_rows_array;
    int64_t num_delete_rows = 0;
    for (const auto& delete_file : delete_files) {
        SCOPED_TIMER(_iceberg_profile.delete_files_read_time);
        Status create_status = Status::OK();
        auto* delete_file_cache = _kv_cache->get<DeleteFile>(
                _delet_file_cache_key(delete_file.path), [&]() -> DeleteFile* {
                    auto* position_delete = new DeleteFile;
                    create_status = _read_position_delete_file(delete_file, position_delete);

                    if (!create_status) {
                        return nullptr;
                    }

                    return position_delete;
                });
        if (create_status.is<ErrorCode::END_OF_FILE>()) {
            continue;
        } else if (!create_status.ok()) {
            return create_status;
        }

        DeleteFile& delete_file_map = *((DeleteFile*)delete_file_cache);
        auto get_value = [&](const auto& v) {
            DeleteRows* row_ids = v.second.get();
            if (!row_ids->empty()) {
                delete_rows_array.emplace_back(row_ids);
                num_delete_rows += row_ids->size();
            }
        };
        delete_file_map.if_contains(data_file_path, get_value);
    }
    // Use a KV cache to store the delete rows corresponding to a data file path.
    // The Parquet/ORC reader holds a reference (pointer) to this cached entry.
    // This allows delete rows to be reused when a single data file is split into
    // multiple splits, avoiding excessive memory usage when delete rows are large.
    if (num_delete_rows > 0) {
        SCOPED_TIMER(_iceberg_profile.delete_rows_sort_time);
        _iceberg_delete_rows =
                _kv_cache->get<DeleteRows>(data_file_path,
                                           [&]() -> DeleteRows* {
                                               auto* data_file_position_delete = new DeleteRows;
                                               _sort_delete_rows(delete_rows_array, num_delete_rows,
                                                                 *data_file_position_delete);

                                               return data_file_position_delete;
                                           }

                );
        set_delete_rows();
        COUNTER_UPDATE(_iceberg_profile.num_delete_rows, num_delete_rows);
    }
    return Status::OK();
}

Status IcebergTableReader::_read_position_delete_file(const TIcebergDeleteFileDesc& delete_file,
                                                      DeleteFile* position_delete) {
    GroupedDeleteRowsVisitor visitor(position_delete);
    IcebergDeleteFileReaderOptions options;
    options.state = _state;
    options.profile = _profile;
    options.scan_params = &_params;
    options.io_ctx = _io_ctx;
    options.meta_cache = _meta_cache;
    options.fs_name = &_range.fs_name;
    options.batch_size = READ_DELETE_FILE_BATCH_SIZE;
    return read_iceberg_position_delete_file(delete_file, options, &visitor);
}

/**
 * https://iceberg.apache.org/spec/#position-delete-files
 * The rows in the delete file must be sorted by file_path then position to optimize filtering rows while scanning.
 * Sorting by file_path allows filter pushdown by file in columnar storage formats.
 * Sorting by position allows filtering rows while scanning, to avoid keeping deletes in memory.
 */
void IcebergTableReader::_sort_delete_rows(
        const std::vector<std::vector<int64_t>*>& delete_rows_array, int64_t num_delete_rows,
        std::vector<int64_t>& result) {
    if (delete_rows_array.empty()) {
        return;
    }
    if (delete_rows_array.size() == 1) {
        result.resize(num_delete_rows);
        memcpy(result.data(), delete_rows_array.front()->data(), sizeof(int64_t) * num_delete_rows);
        return;
    }
    if (delete_rows_array.size() == 2) {
        result.resize(num_delete_rows);
        std::merge(delete_rows_array.front()->begin(), delete_rows_array.front()->end(),
                   delete_rows_array.back()->begin(), delete_rows_array.back()->end(),
                   result.begin());
        return;
    }

    using vec_pair = std::pair<std::vector<int64_t>::iterator, std::vector<int64_t>::iterator>;
    result.resize(num_delete_rows);
    auto row_id_iter = result.begin();
    auto iter_end = result.end();
    std::vector<vec_pair> rows_array;
    for (auto* rows : delete_rows_array) {
        if (!rows->empty()) {
            rows_array.emplace_back(rows->begin(), rows->end());
        }
    }
    size_t array_size = rows_array.size();
    while (row_id_iter != iter_end) {
        int64_t min_index = 0;
        int64_t min = *rows_array[0].first;
        for (size_t i = 0; i < array_size; ++i) {
            if (*rows_array[i].first < min) {
                min_index = i;
                min = *rows_array[i].first;
            }
        }
        *row_id_iter++ = min;
        rows_array[min_index].first++;
        if (UNLIKELY(rows_array[min_index].first == rows_array[min_index].second)) {
            rows_array.erase(rows_array.begin() + min_index);
            array_size--;
        }
    }
}

Status IcebergParquetReader::init_reader(
        const std::vector<std::string>& file_col_names,
        std::unordered_map<std::string, uint32_t>* col_name_to_block_idx,
        const VExprContextSPtrs& conjuncts,
        phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>>&
                slot_id_to_predicates,
        const TupleDescriptor* tuple_descriptor, const RowDescriptor* row_descriptor,
        const std::unordered_map<std::string, int>* colname_to_slot_id,
        const VExprContextSPtrs* not_single_slot_filter_conjuncts,
        const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts) {
    _file_format = Fileformat::PARQUET;
    _col_name_to_block_idx = col_name_to_block_idx;
    _physical_equality_delete_root_columns.clear();
    auto* parquet_reader = static_cast<ParquetReader*>(_file_format_reader.get());
    RETURN_IF_ERROR(parquet_reader->get_file_metadata_schema(&_data_file_field_desc));
    DCHECK(_data_file_field_desc != nullptr);
    if (_row_lineage_columns != nullptr) {
        const auto& table_desc = _range.table_format_params.iceberg_params;
        _row_lineage_columns->first_row_id =
                table_desc.__isset.first_row_id ? table_desc.first_row_id : -1;
        _row_lineage_columns->last_updated_sequence_number =
                table_desc.__isset.last_updated_sequence_number
                        ? table_desc.last_updated_sequence_number
                        : -1;
        parquet_reader->set_row_lineage_columns(_row_lineage_columns);
    }

    _all_required_col_names = file_col_names;
    for (const auto* slot : tuple_descriptor->slots()) {
        _id_to_block_column_name.emplace(slot->col_unique_id(), slot->col_name());
        _required_column_types.emplace(slot->col_name(), slot->type());
    }
    RETURN_IF_ERROR(init_row_filters());

    if (!_params.__isset.history_schema_info || _params.history_schema_info.empty()) [[unlikely]] {
        RETURN_IF_ERROR(BuildTableInfoUtil::by_parquet_name(
                tuple_descriptor, *_data_file_field_desc, table_info_node_ptr));
    } else {
        RETURN_IF_ERROR(BuildTableInfoUtil::by_parquet_field_id_with_name_mapping(
                _params.history_schema_info.front().root_field, *_data_file_field_desc,
                table_info_node_ptr, supports_iceberg_scan_semantics_v2(&_params)));
    }
    RETURN_IF_ERROR(_validate_projected_missing_required_fields());
    _prepare_physical_reader_predicates(tuple_descriptor, conjuncts,
                                        not_single_slot_filter_conjuncts,
                                        slot_id_to_filter_conjuncts);

    auto column_id_result =
            _create_column_ids(_data_file_field_desc, tuple_descriptor, table_info_node_ptr);
    auto& column_ids = column_id_result.column_ids;
    const auto& filter_column_ids = column_id_result.filter_column_ids;

    bool all_file_columns_have_field_ids = true;
    bool any_file_column_has_field_id = false;
    for (int index = 0; index < _data_file_field_desc->size(); ++index) {
        const auto* field = _data_file_field_desc->get_column(index);
        if (field == nullptr) {
            continue;
        }
        if (field->field_id < 0) {
            all_file_columns_have_field_ids = false;
        }
        if (parquet_subtree_has_iceberg_id(*field)) {
            any_file_column_has_field_id = true;
        }
    }
    const bool use_field_ids = supports_iceberg_scan_semantics_v2(&_params)
                                       ? any_file_column_has_field_id
                                       : all_file_columns_have_field_ids;
    std::unordered_map<std::string, std::string> physical_root_sources;
    std::vector<std::string> new_expand_col_names;
    DORIS_CHECK(_expand_col_names.size() == _expand_col_field_ids.size());
    DORIS_CHECK(_expand_col_names.size() == _expand_columns.size());
    for (size_t index = 0; index < _expand_col_names.size(); ++index) {
        const std::string old_name = _expand_col_names[index];
        const int32_t field_id = _expand_col_field_ids[index];
        const FieldSchema* file_column = nullptr;
        ParquetEqualityFieldPath file_path;
        bool complete_file_path = false;
        if (use_field_ids) {
            complete_file_path = find_parquet_equality_field_path_by_id(_data_file_field_desc,
                                                                        field_id, &file_path);
            if (!complete_file_path && supports_iceberg_scan_semantics_v2(&_params)) {
                const auto table_path = _find_schema_field_path(field_id);
                if (!table_path.empty()) {
                    complete_file_path = find_parquet_equality_field_prefix_by_id_path(
                            _data_file_field_desc, table_path, &file_path);
                }
            }
            if (!file_path.fields.empty()) {
                file_column = file_path.fields.front();
            }
        } else {
            const auto table_path = _find_schema_field_path(field_id);
            if (!table_path.empty()) {
                complete_file_path = find_parquet_equality_field_prefix_by_name_path(
                        _data_file_field_desc, table_path, old_name, &file_path);
                if (!file_path.fields.empty()) {
                    file_column = file_path.fields.front();
                }
            }
        }

        const std::string block_name = _expand_columns[index].name;
        const DataTypePtr target_leaf_type = _expand_columns[index].type;
        new_expand_col_names.push_back(block_name);
        if (file_column == nullptr) {
            RETURN_IF_ERROR(_register_missing_equality_delete_column(field_id, block_name,
                                                                     target_leaf_type));
            continue;
        }

        std::string source_block_name;
        std::vector<size_t> source_child_indexes;
        DataTypePtr source_leaf_type;
        ColumnPtr missing_value;
        bool reads_physical_root = false;
        const auto table_path = _find_schema_field_path(field_id);
        bool uses_projected_root = false;
        if (!table_path.empty() && table_path.front()->__isset.id &&
            _id_to_block_column_name.contains(table_path.front()->id)) {
            RETURN_IF_ERROR(_get_projected_schema_equality_delete_path(
                    field_id, &source_child_indexes, &source_leaf_type, &uses_projected_root));
        }
        if (uses_projected_root) {
            source_block_name = _id_to_block_column_name.at(table_path.front()->id);
        } else {
            const std::string root_name = to_lower(file_column->name);
            const auto root_source = physical_root_sources.find(root_name);
            if (root_source == physical_root_sources.end()) {
                source_block_name = block_name;
                physical_root_sources.emplace(root_name, source_block_name);
                reads_physical_root = true;
                _physical_equality_delete_root_columns.insert(block_name);
                _expand_columns[index].type = make_nullable(file_column->data_type);
                _expand_columns[index].column = _expand_columns[index].type->create_column();
                table_info_node_ptr->add_children(
                        block_name, file_column->name,
                        TableSchemaChangeHelper::ConstNode::get_instance());
            } else {
                source_block_name = root_source->second;
            }
            source_child_indexes = file_path.child_indexes;
            if (complete_file_path) {
                source_leaf_type = make_nullable(file_path.fields.back()->data_type);
            } else {
                source_leaf_type = target_leaf_type;
                RETURN_IF_ERROR(_create_missing_equality_delete_value(
                        field_id, target_leaf_type, file_path.fields.size(), &missing_value));
            }
        }
        if (!reads_physical_root) {
            _physical_missing_equality_delete_columns.insert(block_name);
        }
        _nested_equality_delete_columns.push_back({
                .field_id = field_id,
                .block_name = block_name,
                .source_block_name = source_block_name,
                .source_leaf_type = source_leaf_type,
                .leaf_type = target_leaf_type,
                .child_indexes = std::move(source_child_indexes),
                .missing_value = std::move(missing_value),
                .cast_context = nullptr,
        });
        RETURN_IF_ERROR(
                _prepare_nested_equality_delete_column(&_nested_equality_delete_columns.back()));
        for (uint64_t column_id = file_column->get_column_id();
             column_id <= file_column->get_max_column_id(); ++column_id) {
            column_ids.insert(column_id);
        }
        _all_required_col_names.push_back(block_name);
    }
    _expand_col_names = std::move(new_expand_col_names);
    parquet_reader->set_duplicate_file_column_aliases(_physical_equality_delete_root_columns);

    auto physical_slot_id_to_predicates = slot_id_to_predicates;
    auto deferred_required_column_predicates = AndBlockColumnPredicate::create_unique();
    for (int slot_id : _required_validation_slot_ids) {
        const auto predicates = physical_slot_id_to_predicates.find(slot_id);
        if (predicates != physical_slot_id_to_predicates.end()) {
            for (const auto& predicate : predicates->second) {
                deferred_required_column_predicates->add_column_predicate(
                        SingleColumnBlockPredicate::create_unique(
                                predicate->clone(predicate->column_id())));
            }
        }
        physical_slot_id_to_predicates.erase(slot_id);
    }
    _deferred_required_column_predicates.reset();
    if (deferred_required_column_predicates->num_of_column_predicate() != 0) {
        _deferred_required_column_predicates = std::move(deferred_required_column_predicates);
    }
    return parquet_reader->init_reader(_all_required_col_names, _col_name_to_block_idx,
                                       _physical_reader_conjuncts, physical_slot_id_to_predicates,
                                       tuple_descriptor, row_descriptor, colname_to_slot_id,
                                       &_physical_reader_not_single_slot_filter_conjuncts,
                                       &_physical_reader_slot_id_to_filter_conjuncts,
                                       table_info_node_ptr, true, column_ids, filter_column_ids);
}

ColumnIdResult IcebergParquetReader::_create_column_ids(
        const FieldDescriptor* field_desc, const TupleDescriptor* tuple_descriptor,
        const std::shared_ptr<TableSchemaChangeHelper::Node>& table_info_node) {
    // First, assign column IDs to the field descriptor
    auto* mutable_field_desc = const_cast<FieldDescriptor*>(field_desc);
    mutable_field_desc->assign_ids();

    // map top-level table column iceberg_id -> FieldSchema*
    std::unordered_map<int, const FieldSchema*> iceberg_id_to_field_schema_map;

    for (int i = 0; i < field_desc->size(); ++i) {
        auto field_schema = field_desc->get_column(i);
        if (!field_schema) continue;

        int iceberg_id = field_schema->field_id;
        iceberg_id_to_field_schema_map[iceberg_id] = field_schema;
    }

    std::set<uint64_t> column_ids;
    std::set<uint64_t> filter_column_ids;

    // helper to process access paths for a given top-level parquet field
    auto process_access_paths = [](const FieldSchema* parquet_field,
                                   const std::vector<TColumnAccessPath>& access_paths,
                                   std::set<uint64_t>& out_ids) {
        process_nested_access_paths(
                parquet_field, access_paths, out_ids,
                [](const FieldSchema* field) { return field->get_column_id(); },
                [](const FieldSchema* field) { return field->get_max_column_id(); },
                IcebergParquetNestedColumnUtils::extract_nested_column_ids);
    };

    const auto* struct_node =
            dynamic_cast<const TableSchemaChangeHelper::StructNode*>(table_info_node.get());

    for (const auto* slot : tuple_descriptor->slots()) {
        const FieldSchema* field_schema = nullptr;
        if (struct_node != nullptr) {
            if (struct_node->get_children().contains(slot->col_name()) &&
                struct_node->children_column_exists(slot->col_name())) {
                const auto& file_column_name =
                        struct_node->children_file_column_name(slot->col_name());
                for (int index = 0; index < field_desc->size(); ++index) {
                    const auto* candidate = field_desc->get_column(index);
                    if (candidate != nullptr && candidate->name == file_column_name) {
                        field_schema = candidate;
                        break;
                    }
                }
                DORIS_CHECK(field_schema != nullptr);
            }
        } else {
            auto it = iceberg_id_to_field_schema_map.find(slot->col_unique_id());
            if (it != iceberg_id_to_field_schema_map.end()) {
                field_schema = it->second;
            }
        }
        if (field_schema == nullptr) {
            continue;
        }

        // primitive (non-nested) types: direct mapping by name
        if ((slot->col_type() != TYPE_STRUCT && slot->col_type() != TYPE_ARRAY &&
             slot->col_type() != TYPE_MAP)) {
            column_ids.insert(field_schema->column_id);

            if (slot->is_predicate()) {
                filter_column_ids.insert(field_schema->column_id);
            }
            continue;
        }

        // complex types:
        const auto& all_access_paths = slot->all_access_paths();
        process_access_paths(field_schema, all_access_paths, column_ids);

        const auto& predicate_access_paths = slot->predicate_access_paths();
        if (!predicate_access_paths.empty()) {
            process_access_paths(field_schema, predicate_access_paths, filter_column_ids);
        }
    }
    return {std::move(column_ids), std::move(filter_column_ids)};
}

Status IcebergOrcReader::init_reader(
        const std::vector<std::string>& file_col_names,
        std::unordered_map<std::string, uint32_t>* col_name_to_block_idx,
        const VExprContextSPtrs& conjuncts, const TupleDescriptor* tuple_descriptor,
        const RowDescriptor* row_descriptor,
        const std::unordered_map<std::string, int>* colname_to_slot_id,
        const VExprContextSPtrs* not_single_slot_filter_conjuncts,
        const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts) {
    _file_format = Fileformat::ORC;
    _col_name_to_block_idx = col_name_to_block_idx;
    auto* orc_reader = static_cast<OrcReader*>(_file_format_reader.get());
    RETURN_IF_ERROR(orc_reader->get_file_type(&_data_file_type_desc));
    if (_row_lineage_columns != nullptr) {
        const auto& table_desc = _range.table_format_params.iceberg_params;
        _row_lineage_columns->first_row_id =
                table_desc.__isset.first_row_id ? table_desc.first_row_id : -1;
        _row_lineage_columns->last_updated_sequence_number =
                table_desc.__isset.last_updated_sequence_number
                        ? table_desc.last_updated_sequence_number
                        : -1;
        orc_reader->set_row_lineage_columns(_row_lineage_columns);
    }

    _all_required_col_names = file_col_names;
    for (const auto* slot : tuple_descriptor->slots()) {
        _id_to_block_column_name.emplace(slot->col_unique_id(), slot->col_name());
        _required_column_types.emplace(slot->col_name(), slot->type());
    }
    RETURN_IF_ERROR(init_row_filters());
    if (!_params.__isset.history_schema_info || _params.history_schema_info.empty()) [[unlikely]] {
        RETURN_IF_ERROR(BuildTableInfoUtil::by_orc_name(tuple_descriptor, _data_file_type_desc,
                                                        table_info_node_ptr));
    } else {
        RETURN_IF_ERROR(BuildTableInfoUtil::by_orc_field_id_with_name_mapping(
                _params.history_schema_info.front().root_field, _data_file_type_desc,
                ICEBERG_ORC_ATTRIBUTE, table_info_node_ptr,
                supports_iceberg_scan_semantics_v2(&_params)));
    }
    RETURN_IF_ERROR(_validate_projected_missing_required_fields());
    _prepare_physical_reader_predicates(tuple_descriptor, conjuncts,
                                        not_single_slot_filter_conjuncts,
                                        slot_id_to_filter_conjuncts);

    auto column_id_result =
            _create_column_ids(_data_file_type_desc, tuple_descriptor, table_info_node_ptr);
    auto& column_ids = column_id_result.column_ids;
    const auto& filter_column_ids = column_id_result.filter_column_ids;

    bool all_file_columns_have_field_ids = true;
    for (size_t index = 0; index < _data_file_type_desc->getSubtypeCount(); ++index) {
        if (!_data_file_type_desc->getSubtype(index)->hasAttributeKey(ICEBERG_ORC_ATTRIBUTE)) {
            all_file_columns_have_field_ids = false;
        }
    }
    const bool use_field_ids =
            supports_iceberg_scan_semantics_v2(&_params)
                    ? orc_subtree_has_iceberg_id(_data_file_type_desc, ICEBERG_ORC_ATTRIBUTE)
                    : all_file_columns_have_field_ids;
    std::unordered_map<std::string, std::string> physical_root_sources;
    std::vector<std::string> new_expand_col_names;
    DORIS_CHECK(_expand_col_names.size() == _expand_col_field_ids.size());
    DORIS_CHECK(_expand_col_names.size() == _expand_columns.size());
    for (size_t index = 0; index < _expand_col_names.size(); ++index) {
        const std::string old_name = _expand_col_names[index];
        const int32_t field_id = _expand_col_field_ids[index];
        const orc::Type* file_column = nullptr;
        OrcEqualityFieldPath file_path;
        bool complete_file_path = false;
        if (use_field_ids) {
            complete_file_path =
                    find_orc_equality_field_path_by_id(_data_file_type_desc, field_id, &file_path);
            if (!complete_file_path && supports_iceberg_scan_semantics_v2(&_params)) {
                const auto table_path = _find_schema_field_path(field_id);
                if (!table_path.empty()) {
                    complete_file_path = find_orc_equality_field_prefix_by_id_path(
                            _data_file_type_desc, table_path, &file_path);
                }
            }
            if (!file_path.fields.empty()) {
                file_column = file_path.fields.front();
            }
        } else {
            const auto table_path = _find_schema_field_path(field_id);
            if (!table_path.empty()) {
                complete_file_path = find_orc_equality_field_prefix_by_name_path(
                        _data_file_type_desc, table_path, old_name, &file_path);
                if (!file_path.fields.empty()) {
                    file_column = file_path.fields.front();
                }
            }
        }

        const std::string block_name = _expand_columns[index].name;
        const DataTypePtr target_leaf_type = _expand_columns[index].type;
        new_expand_col_names.push_back(block_name);
        if (file_column == nullptr) {
            RETURN_IF_ERROR(_register_missing_equality_delete_column(field_id, block_name,
                                                                     target_leaf_type));
            continue;
        }

        std::string source_block_name;
        std::vector<size_t> source_child_indexes;
        DataTypePtr source_leaf_type;
        ColumnPtr missing_value;
        bool reads_physical_root = false;
        const auto table_path = _find_schema_field_path(field_id);
        bool uses_projected_root = false;
        if (!table_path.empty() && table_path.front()->__isset.id &&
            _id_to_block_column_name.contains(table_path.front()->id)) {
            RETURN_IF_ERROR(_get_projected_schema_equality_delete_path(
                    field_id, &source_child_indexes, &source_leaf_type, &uses_projected_root));
        }
        if (uses_projected_root) {
            source_block_name = _id_to_block_column_name.at(table_path.front()->id);
        } else {
            DORIS_CHECK(!file_path.names.empty());
            const std::string root_name = to_lower(file_path.names.front());
            const auto root_source = physical_root_sources.find(root_name);
            if (root_source == physical_root_sources.end()) {
                source_block_name = block_name;
                physical_root_sources.emplace(root_name, source_block_name);
                reads_physical_root = true;
                _expand_columns[index].type =
                        make_nullable(orc_reader->convert_to_doris_type(file_column));
                _expand_columns[index].column = _expand_columns[index].type->create_column();
                table_info_node_ptr->add_children(
                        block_name, file_path.names.front(),
                        TableSchemaChangeHelper::ConstNode::get_instance());
            } else {
                source_block_name = root_source->second;
            }
            source_child_indexes = file_path.child_indexes;
            if (complete_file_path) {
                source_leaf_type =
                        make_nullable(orc_reader->convert_to_doris_type(file_path.fields.back()));
            } else {
                source_leaf_type = target_leaf_type;
                RETURN_IF_ERROR(_create_missing_equality_delete_value(
                        field_id, target_leaf_type, file_path.fields.size(), &missing_value));
            }
        }
        if (!reads_physical_root) {
            _physical_missing_equality_delete_columns.insert(block_name);
        }
        _nested_equality_delete_columns.push_back({
                .field_id = field_id,
                .block_name = block_name,
                .source_block_name = source_block_name,
                .source_leaf_type = source_leaf_type,
                .leaf_type = target_leaf_type,
                .child_indexes = std::move(source_child_indexes),
                .missing_value = std::move(missing_value),
                .cast_context = nullptr,
        });
        RETURN_IF_ERROR(
                _prepare_nested_equality_delete_column(&_nested_equality_delete_columns.back()));
        for (uint64_t column_id = file_column->getColumnId();
             column_id <= file_column->getMaximumColumnId(); ++column_id) {
            column_ids.insert(column_id);
        }
        _all_required_col_names.push_back(block_name);
    }
    _expand_col_names = std::move(new_expand_col_names);

    return orc_reader->init_reader(
            &_all_required_col_names, _col_name_to_block_idx, _physical_reader_conjuncts, false,
            tuple_descriptor, row_descriptor, &_physical_reader_not_single_slot_filter_conjuncts,
            &_physical_reader_slot_id_to_filter_conjuncts, table_info_node_ptr, column_ids,
            filter_column_ids);
}

ColumnIdResult IcebergOrcReader::_create_column_ids(
        const orc::Type* orc_type, const TupleDescriptor* tuple_descriptor,
        const std::shared_ptr<TableSchemaChangeHelper::Node>& table_info_node) {
    // map top-level table column iceberg_id -> orc::Type*
    std::unordered_map<int, const orc::Type*> iceberg_id_to_orc_type_map;
    for (uint64_t i = 0; i < orc_type->getSubtypeCount(); ++i) {
        auto orc_sub_type = orc_type->getSubtype(i);
        if (!orc_sub_type) continue;

        if (!orc_sub_type->hasAttributeKey(ICEBERG_ORC_ATTRIBUTE)) {
            continue;
        }
        int iceberg_id = std::stoi(orc_sub_type->getAttributeValue(ICEBERG_ORC_ATTRIBUTE));
        iceberg_id_to_orc_type_map[iceberg_id] = orc_sub_type;
    }

    std::set<uint64_t> column_ids;
    std::set<uint64_t> filter_column_ids;

    // helper to process access paths for a given top-level orc field
    auto process_access_paths = [](const orc::Type* orc_field,
                                   const std::vector<TColumnAccessPath>& access_paths,
                                   std::set<uint64_t>& out_ids) {
        process_nested_access_paths(
                orc_field, access_paths, out_ids,
                [](const orc::Type* type) { return type->getColumnId(); },
                [](const orc::Type* type) { return type->getMaximumColumnId(); },
                IcebergOrcNestedColumnUtils::extract_nested_column_ids);
    };

    const auto* struct_node =
            dynamic_cast<const TableSchemaChangeHelper::StructNode*>(table_info_node.get());

    for (const auto* slot : tuple_descriptor->slots()) {
        const orc::Type* orc_field = nullptr;
        if (struct_node != nullptr) {
            if (struct_node->get_children().contains(slot->col_name()) &&
                struct_node->children_column_exists(slot->col_name())) {
                const auto& file_column_name =
                        struct_node->children_file_column_name(slot->col_name());
                for (uint64_t index = 0; index < orc_type->getSubtypeCount(); ++index) {
                    if (orc_type->getFieldName(index) == file_column_name) {
                        orc_field = orc_type->getSubtype(index);
                        break;
                    }
                }
                DORIS_CHECK(orc_field != nullptr);
            }
        } else {
            auto it = iceberg_id_to_orc_type_map.find(slot->col_unique_id());
            if (it != iceberg_id_to_orc_type_map.end()) {
                orc_field = it->second;
            }
        }
        if (orc_field == nullptr) {
            continue;
        }

        // primitive (non-nested) types
        if ((slot->col_type() != TYPE_STRUCT && slot->col_type() != TYPE_ARRAY &&
             slot->col_type() != TYPE_MAP)) {
            column_ids.insert(orc_field->getColumnId());
            if (slot->is_predicate()) {
                filter_column_ids.insert(orc_field->getColumnId());
            }
            continue;
        }

        // complex types
        const auto& all_access_paths = slot->all_access_paths();
        process_access_paths(orc_field, all_access_paths, column_ids);

        const auto& predicate_access_paths = slot->predicate_access_paths();
        if (!predicate_access_paths.empty()) {
            process_access_paths(orc_field, predicate_access_paths, filter_column_ids);
        }
    }

    return {std::move(column_ids), std::move(filter_column_ids)};
}

// Directly read the deletion vector using the `content_offset` and
// `content_size_in_bytes` provided by FE in `delete_file_desc`.
// These two fields indicate the location of a blob in storage.
// Since the current format is `deletion-vector-v1`, which does not
// compress any blobs, we can temporarily skip parsing the Puffin footer.
Status IcebergTableReader::read_deletion_vector(const std::string& data_file_path,
                                                const TIcebergDeleteFileDesc& delete_file_desc) {
    Status create_status = Status::OK();
    SCOPED_TIMER(_iceberg_profile.delete_files_read_time);
    _iceberg_delete_rows = _kv_cache->get<DeleteRows>(data_file_path, [&]() -> DeleteRows* {
        auto* delete_rows = new DeleteRows;

        TFileRangeDesc delete_range;
        // must use __set() method to make sure __isset is true
        delete_range.__set_fs_name(_range.fs_name);
        delete_range.path = delete_file_desc.path;
        delete_range.start_offset = delete_file_desc.content_offset;
        delete_range.size = delete_file_desc.content_size_in_bytes;
        delete_range.file_size = -1;

        // We may consider caching the DeletionVectorReader when reading Puffin files,
        // where the underlying reader is an `InMemoryFileReader` and a single data file is
        // split into multiple splits. However, we need to ensure that the underlying
        // reader supports multi-threaded access.
        DeletionVectorReader dv_reader(_state, _profile, _params, delete_range, _io_ctx);
        create_status = dv_reader.open();
        if (!create_status.ok()) [[unlikely]] {
            return nullptr;
        }

        size_t buffer_size = delete_range.size;
        std::vector<char> buf(buffer_size);
        if (buffer_size < 12) [[unlikely]] {
            // Minimum size: 4 bytes length + 4 bytes magic + 4 bytes CRC32
            create_status = Status::DataQualityError("Deletion vector file size too small: {}",
                                                     buffer_size);
            return nullptr;
        }

        create_status = dv_reader.read_at(delete_range.start_offset, {buf.data(), buffer_size});
        if (!create_status) [[unlikely]] {
            return nullptr;
        }
        // The serialized blob contains:
        //
        // Combined length of the vector and magic bytes stored as 4 bytes, big-endian
        // A 4-byte magic sequence, D1 D3 39 64
        // The vector, serialized as described below
        // A CRC-32 checksum of the magic bytes and serialized vector as 4 bytes, big-endian

        auto total_length = BigEndian::Load32(buf.data());
        if (total_length + 8 != buffer_size) [[unlikely]] {
            create_status = Status::DataQualityError(
                    "Deletion vector length mismatch, expected: {}, actual: {}", total_length + 8,
                    buffer_size);
            return nullptr;
        }

        constexpr static char MAGIC_NUMBER[] = {'\xD1', '\xD3', '\x39', '\x64'};
        if (memcmp(buf.data() + sizeof(total_length), MAGIC_NUMBER, 4)) [[unlikely]] {
            create_status = Status::DataQualityError("Deletion vector magic number mismatch");
            return nullptr;
        }

        roaring::Roaring64Map bitmap;
        SCOPED_TIMER(_iceberg_profile.parse_delete_file_time);
        try {
            bitmap = roaring::Roaring64Map::readSafe(buf.data() + 8, buffer_size - 12);
        } catch (const std::runtime_error& e) {
            create_status = Status::DataQualityError("Decode roaring bitmap failed, {}", e.what());
            return nullptr;
        }
        // skip CRC-32 checksum

        delete_rows->reserve(bitmap.cardinality());
        for (auto it = bitmap.begin(); it != bitmap.end(); it++) {
            delete_rows->push_back(*it);
        }
        COUNTER_UPDATE(_iceberg_profile.num_delete_rows, delete_rows->size());
        return delete_rows;
    });

    RETURN_IF_ERROR(create_status);
    if (!_iceberg_delete_rows->empty()) [[likely]] {
        set_delete_rows();
    }
    return Status::OK();
}

// Similar to the code structure of IcebergOrcReader::_process_equality_delete,
// but considering the significant differences in how parquet/orc obtains
// attributes/column IDs, it is not easy to combine them.
Status IcebergParquetReader::_process_equality_delete(
        const std::vector<TIcebergDeleteFileDesc>& delete_files) {
    struct ReadSpec {
        NestedEqualityDeleteColumn nested_field;
        std::string root_name;
        DataTypePtr root_type;
    };
    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    std::unordered_map<std::string, VExprContextSPtr> missing_columns;

    for (const auto& delete_file : delete_files) {
        if (!delete_file.__isset.field_ids) [[unlikely]] {
            return Status::InternalError(
                    "missing delete field ids when reading equality delete file");
        }
        TFileRangeDesc delete_desc;
        delete_desc.__set_fs_name(_range.fs_name);
        delete_desc.path = delete_file.path;
        delete_desc.start_offset = 0;
        delete_desc.size = -1;
        delete_desc.file_size = -1;

        auto delete_reader = ParquetReader::create_unique(
                _profile, _params, delete_desc, READ_DELETE_FILE_BATCH_SIZE,
                const_cast<cctz::time_zone*>(&_state->timezone_obj()), _io_ctx, _state,
                _meta_cache);
        RETURN_IF_ERROR(delete_reader->init_schema_reader());
        const FieldDescriptor* delete_field_desc = nullptr;
        RETURN_IF_ERROR(delete_reader->get_file_metadata_schema(&delete_field_desc));
        DORIS_CHECK(delete_field_desc != nullptr);

        std::vector<ReadSpec> read_specs;
        std::vector<std::string> delete_col_names;
        std::vector<DataTypePtr> delete_col_types;
        std::vector<int> delete_col_ids;
        std::unordered_map<int, std::string> filter_column_names;
        std::vector<std::string> read_root_names;
        std::vector<DataTypePtr> read_root_types;
        std::unordered_map<std::string, uint32_t> read_root_positions;
        auto eq_file_node = std::make_shared<TableSchemaChangeHelper::StructNode>();
        for (int32_t field_id : delete_file.field_ids) {
            ParquetEqualityFieldPath path;
            if (!find_parquet_equality_field_path_by_id(delete_field_desc, field_id, &path)) {
                return Status::DataQualityError(
                        "missing field id {} when reading equality delete file {}", field_id,
                        delete_file.path);
            }
            DORIS_CHECK(!path.fields.empty());
            const auto* root = path.fields.front();
            const auto* leaf = path.fields.back();
            if (!leaf->children.empty()) {
                return Status::NotSupported(
                        "Iceberg equality delete does not support complex column {}", leaf->name);
            }
            const std::string leaf_name = to_lower(leaf->name);
            const std::string root_name = to_lower(root->name);
            const auto leaf_type = make_nullable(leaf->data_type);
            read_specs.push_back({
                    {
                            .field_id = field_id,
                            .block_name = leaf_name,
                            .source_block_name = {},
                            .source_leaf_type = leaf_type,
                            .leaf_type = leaf_type,
                            .child_indexes = path.child_indexes,
                            .missing_value = nullptr,
                            .cast_context = nullptr,
                    },
                    root_name,
                    make_nullable(root->data_type),
            });
            delete_col_ids.push_back(field_id);
            delete_col_names.push_back(leaf_name);
            delete_col_types.push_back(leaf_type);
            _equality_delete_col_ids.insert(field_id);
            filter_column_names.emplace(field_id, _get_or_register_equality_delete_carrier(
                                                          field_id, leaf_name, leaf_type));
            if (!read_root_positions.contains(root_name)) {
                read_root_positions.emplace(root_name, read_root_names.size());
                read_root_names.push_back(root_name);
                read_root_types.push_back(make_nullable(root->data_type));
                eq_file_node->add_children(root_name, root->name,
                                           TableSchemaChangeHelper::ConstNode::get_instance());
            }
        }

        phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>> predicates;
        RETURN_IF_ERROR(delete_reader->init_reader(read_root_names, &read_root_positions, {},
                                                   predicates, nullptr, nullptr, nullptr, nullptr,
                                                   nullptr, eq_file_node, false));
        RETURN_IF_ERROR(delete_reader->set_fill_columns(partition_columns, missing_columns));

        EqualityDeleteSchemaKey schema_key;
        schema_key.reserve(delete_col_ids.size());
        for (size_t index = 0; index < delete_col_ids.size(); ++index) {
            schema_key.emplace_back(delete_col_ids[index], delete_col_types[index]->get_name());
        }
        if (!_equality_delete_block_map.contains(schema_key)) {
            _equality_delete_block_map.emplace(schema_key, _equality_delete_blocks.size());
            Block block;
            _generate_equality_delete_block(&block, delete_col_names, delete_col_types);
            _equality_delete_blocks.emplace_back(std::move(block));
            _equality_delete_filter_field_ids.push_back(delete_col_ids);
            _equality_delete_filter_column_names.push_back(std::move(filter_column_names));
        }
        Block& equality_block = _equality_delete_blocks[_equality_delete_block_map[schema_key]];
        bool eof = false;
        while (!eof) {
            Block raw_block;
            for (size_t index = 0; index < read_root_names.size(); ++index) {
                raw_block.insert({read_root_types[index]->create_column(), read_root_types[index],
                                  read_root_names[index]});
            }
            size_t read_rows = 0;
            RETURN_IF_ERROR(delete_reader->get_next_block(&raw_block, &read_rows, &eof));
            if (read_rows == 0) {
                continue;
            }
            Block key_block;
            for (size_t index = 0; index < read_specs.size(); ++index) {
                ColumnPtr leaf;
                RETURN_IF_ERROR(_extract_nested_equality_delete_column(
                        raw_block
                                .get_by_position(
                                        read_root_positions.at(read_specs[index].root_name))
                                .column,
                        read_specs[index].nested_field, &leaf));
                key_block.insert(
                        {std::move(leaf), delete_col_types[index], delete_col_names[index]});
            }
            ScopedMutableBlock mutable_block(&equality_block);
            RETURN_IF_ERROR(mutable_block.mutable_block().merge(key_block));
        }
    }

    DORIS_CHECK(_equality_delete_blocks.size() == _equality_delete_filter_field_ids.size());
    for (size_t block_idx = 0; block_idx < _equality_delete_blocks.size(); ++block_idx) {
        auto& equality_block = _equality_delete_blocks[block_idx];
        auto equality_delete_impl = EqualityDeleteBase::get_delete_impl(
                &equality_block, _equality_delete_filter_field_ids[block_idx]);
        RETURN_IF_ERROR(equality_delete_impl->init(_profile));
        _equality_delete_impls.emplace_back(std::move(equality_delete_impl));
    }
    return Status::OK();
}

Status IcebergOrcReader::_process_equality_delete(
        const std::vector<TIcebergDeleteFileDesc>& delete_files) {
    struct ReadSpec {
        NestedEqualityDeleteColumn nested_field;
        std::string root_name;
        DataTypePtr root_type;
    };
    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    std::unordered_map<std::string, VExprContextSPtr> missing_columns;

    for (const auto& delete_file : delete_files) {
        if (!delete_file.__isset.field_ids) [[unlikely]] {
            return Status::InternalError(
                    "missing delete field ids when reading equality delete file");
        }
        TFileRangeDesc delete_desc;
        delete_desc.__set_fs_name(_range.fs_name);
        delete_desc.path = delete_file.path;
        delete_desc.start_offset = 0;
        delete_desc.size = -1;
        delete_desc.file_size = -1;

        auto delete_reader = OrcReader::create_unique(_profile, _state, _params, delete_desc,
                                                      READ_DELETE_FILE_BATCH_SIZE,
                                                      _state->timezone(), _io_ctx, _meta_cache);
        RETURN_IF_ERROR(delete_reader->init_schema_reader());
        const orc::Type* delete_root = nullptr;
        RETURN_IF_ERROR(delete_reader->get_file_type(&delete_root));
        DORIS_CHECK(delete_root != nullptr);

        std::vector<ReadSpec> read_specs;
        std::vector<std::string> delete_col_names;
        std::vector<DataTypePtr> delete_col_types;
        std::vector<int> delete_col_ids;
        std::unordered_map<int, std::string> filter_column_names;
        std::vector<std::string> read_root_names;
        std::vector<DataTypePtr> read_root_types;
        std::unordered_map<std::string, uint32_t> read_root_positions;
        auto eq_file_node = std::make_shared<TableSchemaChangeHelper::StructNode>();
        for (int32_t field_id : delete_file.field_ids) {
            OrcEqualityFieldPath path;
            if (!find_orc_equality_field_path_by_id(delete_root, field_id, &path)) {
                return Status::DataQualityError(
                        "missing field id {} when reading equality delete file {}", field_id,
                        delete_file.path);
            }
            DORIS_CHECK(!path.fields.empty());
            DORIS_CHECK(!path.names.empty());
            const auto* root = path.fields.front();
            const auto* leaf = path.fields.back();
            if (leaf->getSubtypeCount() > 0) {
                return Status::NotSupported(
                        "Iceberg equality delete does not support complex column {}",
                        path.names.back());
            }
            const std::string leaf_name = to_lower(path.names.back());
            const std::string root_name = to_lower(path.names.front());
            const auto leaf_type = make_nullable(delete_reader->convert_to_doris_type(leaf));
            const auto root_type = make_nullable(delete_reader->convert_to_doris_type(root));
            read_specs.push_back({
                    {
                            .field_id = field_id,
                            .block_name = leaf_name,
                            .source_block_name = {},
                            .source_leaf_type = leaf_type,
                            .leaf_type = leaf_type,
                            .child_indexes = path.child_indexes,
                            .missing_value = nullptr,
                            .cast_context = nullptr,
                    },
                    root_name,
                    root_type,
            });
            delete_col_ids.push_back(field_id);
            delete_col_names.push_back(leaf_name);
            delete_col_types.push_back(leaf_type);
            _equality_delete_col_ids.insert(field_id);
            filter_column_names.emplace(field_id, _get_or_register_equality_delete_carrier(
                                                          field_id, leaf_name, leaf_type));
            if (!read_root_positions.contains(root_name)) {
                read_root_positions.emplace(root_name, read_root_names.size());
                read_root_names.push_back(root_name);
                read_root_types.push_back(root_type);
                eq_file_node->add_children(root_name, path.names.front(),
                                           TableSchemaChangeHelper::ConstNode::get_instance());
            }
        }

        RETURN_IF_ERROR(delete_reader->init_reader(&read_root_names, &read_root_positions, {},
                                                   false, nullptr, nullptr, nullptr, nullptr,
                                                   eq_file_node));
        RETURN_IF_ERROR(delete_reader->set_fill_columns(partition_columns, missing_columns));

        EqualityDeleteSchemaKey schema_key;
        schema_key.reserve(delete_col_ids.size());
        for (size_t index = 0; index < delete_col_ids.size(); ++index) {
            schema_key.emplace_back(delete_col_ids[index], delete_col_types[index]->get_name());
        }
        if (!_equality_delete_block_map.contains(schema_key)) {
            _equality_delete_block_map.emplace(schema_key, _equality_delete_blocks.size());
            Block block;
            _generate_equality_delete_block(&block, delete_col_names, delete_col_types);
            _equality_delete_blocks.emplace_back(std::move(block));
            _equality_delete_filter_field_ids.push_back(delete_col_ids);
            _equality_delete_filter_column_names.push_back(std::move(filter_column_names));
        }
        Block& equality_block = _equality_delete_blocks[_equality_delete_block_map[schema_key]];
        bool eof = false;
        while (!eof) {
            Block raw_block;
            for (size_t index = 0; index < read_root_names.size(); ++index) {
                raw_block.insert({read_root_types[index]->create_column(), read_root_types[index],
                                  read_root_names[index]});
            }
            size_t read_rows = 0;
            RETURN_IF_ERROR(delete_reader->get_next_block(&raw_block, &read_rows, &eof));
            if (read_rows == 0) {
                continue;
            }
            Block key_block;
            for (size_t index = 0; index < read_specs.size(); ++index) {
                ColumnPtr leaf;
                RETURN_IF_ERROR(_extract_nested_equality_delete_column(
                        raw_block
                                .get_by_position(
                                        read_root_positions.at(read_specs[index].root_name))
                                .column,
                        read_specs[index].nested_field, &leaf));
                key_block.insert(
                        {std::move(leaf), delete_col_types[index], delete_col_names[index]});
            }
            ScopedMutableBlock mutable_block(&equality_block);
            RETURN_IF_ERROR(mutable_block.mutable_block().merge(key_block));
        }
    }

    DORIS_CHECK(_equality_delete_blocks.size() == _equality_delete_filter_field_ids.size());
    for (size_t block_idx = 0; block_idx < _equality_delete_blocks.size(); ++block_idx) {
        auto& equality_block = _equality_delete_blocks[block_idx];
        auto equality_delete_impl = EqualityDeleteBase::get_delete_impl(
                &equality_block, _equality_delete_filter_field_ids[block_idx]);
        RETURN_IF_ERROR(equality_delete_impl->init(_profile));
        _equality_delete_impls.emplace_back(std::move(equality_delete_impl));
    }
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris
