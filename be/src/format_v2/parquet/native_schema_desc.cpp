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

#include "format_v2/parquet/native_schema_desc.h"

#include <ctype.h>

#include <algorithm>
#include <ostream>
#include <utility>

#include "common/cast_set.h"
#include "common/exception.h"
#include "common/logging.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/define_primitive_type.h"
#include "util/slice.h"
#include "util/string_util.h"

namespace doris::format::parquet {

static bool is_group_node(const tparquet::SchemaElement& schema) {
    return schema.num_children > 0;
}

static bool is_list_node(const tparquet::SchemaElement& schema) {
    return schema.__isset.converted_type && schema.converted_type == tparquet::ConvertedType::LIST;
}

static bool is_map_node(const tparquet::SchemaElement& schema) {
    return schema.__isset.converted_type &&
           (schema.converted_type == tparquet::ConvertedType::MAP ||
            schema.converted_type == tparquet::ConvertedType::MAP_KEY_VALUE);
}

static bool is_repeated_node(const tparquet::SchemaElement& schema) {
    return schema.__isset.repetition_type &&
           schema.repetition_type == tparquet::FieldRepetitionType::REPEATED;
}

static bool is_required_node(const tparquet::SchemaElement& schema) {
    return schema.__isset.repetition_type &&
           schema.repetition_type == tparquet::FieldRepetitionType::REQUIRED;
}

static bool is_optional_node(const tparquet::SchemaElement& schema) {
    return schema.__isset.repetition_type &&
           schema.repetition_type == tparquet::FieldRepetitionType::OPTIONAL;
}

static int num_children_node(const tparquet::SchemaElement& schema) {
    return schema.__isset.num_children ? schema.num_children : 0;
}

static Status validate_native_schema_structure(
        const std::vector<tparquet::SchemaElement>& schemas) {
    if (schemas.empty()) {
        return Status::InvalidArgument("Wrong parquet root schema element");
    }

    const auto& root = schemas[0];
    if (root.__isset.type || !root.__isset.num_children || root.num_children <= 0 ||
        (root.__isset.repetition_type &&
         root.repetition_type != tparquet::FieldRepetitionType::REQUIRED)) {
        // Writers may encode the root's implicit REQUIRED repetition explicitly, but an optional
        // or repeated root would make every descendant's definition/repetition levels ambiguous.
        return Status::InvalidArgument("Wrong parquet root schema element");
    }

    struct PendingGroup {
        size_t remaining_children;
        size_t depth;
    };
    const auto root_children = num_children_node(schemas[0]);
    if (root_children < 0 || static_cast<size_t>(root_children) > schemas.size() - 1) {
        return Status::InvalidArgument("Invalid parquet root child count {}", root_children);
    }
    std::vector<PendingGroup> pending {{static_cast<size_t>(root_children), 0}};
    for (size_t pos = 1; pos < schemas.size(); ++pos) {
        while (!pending.empty() && pending.back().remaining_children == 0) {
            pending.pop_back();
        }
        if (pending.empty()) {
            return Status::InvalidArgument("Schema element {} is not reachable from the root", pos);
        }

        const size_t depth = pending.back().depth + 1;
        --pending.back().remaining_children;
        const auto& schema = schemas[pos];
        if (!schema.__isset.repetition_type) {
            return Status::InvalidArgument("Schema element {} has no repetition type", pos);
        }
        const bool has_children = schema.__isset.num_children && schema.num_children > 0;
        if (schema.__isset.type == has_children) {
            // Some legacy parquet-cpp files explicitly encode num_children=0 on primitive nodes.
            // Only a positive count denotes a group, preserving strict rejection of real dual-kind
            // nodes without dropping those otherwise valid files.
            return Status::InvalidArgument("Schema element {} has ambiguous primitive/group kind",
                                           pos);
        }
        if (schema.__isset.num_children && schema.num_children < 0) {
            return Status::InvalidArgument("Schema element {} has a negative child count", pos);
        }
        const int children = num_children_node(schemas[pos]);
        if (children < 0 || static_cast<size_t>(children) > schemas.size() - pos - 1) {
            return Status::InvalidArgument("Invalid child count {} at schema element {}", children,
                                           pos);
        }
        if (children > 0) {
            // Bound the tree before any resize or recursion so an untrusted footer cannot
            // turn a tiny schema into an unbounded allocation or parser stack.
            if (depth > MAX_NATIVE_SCHEMA_DEPTH) {
                return Status::InvalidArgument("Parquet schema depth {} exceeds limit {}", depth,
                                               MAX_NATIVE_SCHEMA_DEPTH);
            }
            pending.push_back({static_cast<size_t>(children), depth});
        }
    }
    while (!pending.empty() && pending.back().remaining_children == 0) {
        pending.pop_back();
    }
    if (!pending.empty()) {
        return Status::InvalidArgument("Parquet schema ended before all children were parsed");
    }
    return Status::OK();
}

/**
 * `repeated_parent_def_level` is the definition level of the first ancestor node whose repetition_type equals REPEATED.
 * Empty array/map values are not stored in doris columns, so have to use `repeated_parent_def_level` to skip the
 * empty or null values in ancestor node.
 *
 * For instance, considering an array of strings with 3 rows like the following:
 * null, [], [a, b, c]
 * We can store four elements in data column: null, a, b, c
 * and the offsets column is: 1, 1, 4
 * and the null map is: 1, 0, 0
 * For the i-th row in array column: range from `offsets[i - 1]` until `offsets[i]` represents the elements in this row,
 * so we can't store empty array/map values in doris data column.
 * As a comparison, spark does not require `repeated_parent_def_level`,
 * because the spark column stores empty array/map values , and use anther length column to indicate empty values.
 * Please reference: https://github.com/apache/spark/blob/master/sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/ParquetColumnVector.java
 *
 * Furthermore, we can also avoid store null array/map values in doris data column.
 * The same three rows as above, We can only store three elements in data column: a, b, c
 * and the offsets column is: 0, 0, 3
 * and the null map is: 1, 0, 0
 *
 * Inherit the repetition and definition level from parent node, if the parent node is repeated,
 * we should set repeated_parent_def_level = definition_level, otherwise as repeated_parent_def_level.
 * @param parent parent node
 * @param repeated_parent_def_level the first ancestor node whose repetition_type equals REPEATED
 */
static void set_child_node_level(NativeFieldSchema* parent, int16_t repeated_parent_def_level) {
    for (auto& child : parent->children) {
        child.repetition_level = parent->repetition_level;
        child.definition_level = parent->definition_level;
        child.repeated_parent_def_level = repeated_parent_def_level;
    }
}

static bool is_struct_list_node(const tparquet::SchemaElement& schema,
                                const std::string& enclosing_list_name) {
    // The legacy Parquet exception is exact: accepting every "*_tuple" wrapper changes a standard
    // one-child LIST wrapper from ARRAY<T> to ARRAY<STRUCT<T>>.
    return schema.name == "array" || schema.name == enclosing_list_name + "_tuple";
}

static bool has_logical_annotation(const tparquet::SchemaElement& schema) {
    return schema.__isset.logicalType || schema.__isset.converted_type;
}

std::string NativeFieldSchema::debug_string() const {
    std::stringstream ss;
    ss << "NativeFieldSchema(name=" << name << ", R=" << repetition_level
       << ", D=" << definition_level;
    if (children.size() > 0) {
        ss << ", type=" << data_type->get_name() << ", children=[";
        for (int i = 0; i < children.size(); ++i) {
            if (i != 0) {
                ss << ", ";
            }
            ss << children[i].debug_string();
        }
        ss << "]";
    } else {
        ss << ", physical_type=" << physical_type;
        ss << " , doris_type=" << data_type->get_name();
    }
    ss << ")";
    return ss.str();
}

Status NativeFieldDescriptor::parse_from_thrift(
        const std::vector<tparquet::SchemaElement>& t_schemas) {
    _fields.clear();
    _physical_fields.clear();
    _name_to_field.clear();
    RETURN_IF_ERROR(validate_native_schema_structure(t_schemas));
    const auto& root_schema = t_schemas[0];
    _fields.resize(root_schema.num_children);
    _next_schema_pos = 1;

    for (int i = 0; i < root_schema.num_children; ++i) {
        RETURN_IF_ERROR(parse_node_field(t_schemas, _next_schema_pos, &_fields[i]));
        if (_name_to_field.find(_fields[i].name) != _name_to_field.end()) {
            return Status::InvalidArgument("Duplicated field name: {}", _fields[i].name);
        }
        _name_to_field.emplace(_fields[i].name, &_fields[i]);
    }

    if (_next_schema_pos != t_schemas.size()) {
        return Status::InvalidArgument("Remaining {} unparsed schema elements",
                                       t_schemas.size() - _next_schema_pos);
    }

    return Status::OK();
}

Status NativeFieldDescriptor::parse_node_field(
        const std::vector<tparquet::SchemaElement>& t_schemas, size_t curr_pos,
        NativeFieldSchema* node_field) {
    if (curr_pos >= t_schemas.size()) {
        return Status::InvalidArgument("Out-of-bounds index of schema elements");
    }
    auto& t_schema = t_schemas[curr_pos];
    if (is_group_node(t_schema)) {
        // nested structure or nullable list
        return parse_group_field(t_schemas, curr_pos, node_field);
    }
    if (is_repeated_node(t_schema)) {
        // repeated <primitive-type> <name> (LIST)
        // produce required list<element>
        node_field->repetition_level++;
        node_field->definition_level++;
        node_field->children.resize(1);
        set_child_node_level(node_field, node_field->definition_level);
        auto child = &node_field->children[0];
        parse_physical_field(t_schema, false, child);

        node_field->name = t_schema.name;
        node_field->lower_case_name = to_lower(t_schema.name);
        node_field->data_type = std::make_shared<DataTypeArray>(make_nullable(child->data_type));
        _next_schema_pos = curr_pos + 1;
        node_field->field_id = t_schema.__isset.field_id ? t_schema.field_id : -1;
    } else {
        bool is_optional = is_optional_node(t_schema);
        if (is_optional) {
            node_field->definition_level++;
        }
        parse_physical_field(t_schema, is_optional, node_field);
        _next_schema_pos = curr_pos + 1;
    }
    return Status::OK();
}

void NativeFieldDescriptor::parse_physical_field(const tparquet::SchemaElement& physical_schema,
                                                 bool is_nullable,
                                                 NativeFieldSchema* physical_field) {
    physical_field->name = physical_schema.name;
    physical_field->lower_case_name = to_lower(physical_field->name);
    physical_field->parquet_schema = physical_schema;
    physical_field->physical_type = physical_schema.type;
    physical_field->column_id = NATIVE_UNASSIGNED_COLUMN_ID; // Initialize column_id
    _physical_fields.push_back(physical_field);
    physical_field->physical_column_index = cast_set<int>(_physical_fields.size() - 1);
    auto type = get_doris_type(physical_schema, is_nullable);
    physical_field->data_type = type.first;
    physical_field->is_type_compatibility = type.second;
    physical_field->field_id = physical_schema.__isset.field_id ? physical_schema.field_id : -1;
}

std::pair<DataTypePtr, bool> NativeFieldDescriptor::get_doris_type(
        const tparquet::SchemaElement& physical_schema, bool nullable) {
    std::pair<DataTypePtr, bool> ans = {std::make_shared<DataTypeNothing>(), false};
    try {
        if (physical_schema.__isset.logicalType) {
            ans = convert_to_doris_type(physical_schema.logicalType, nullable);
        } else if (physical_schema.__isset.converted_type) {
            ans = convert_to_doris_type(physical_schema, nullable);
        }
    } catch (...) {
        // now the Not supported exception are ignored
        // so those byte_array maybe be treated as varbinary(now) : string(before)
    }
    if (ans.first->get_primitive_type() == PrimitiveType::INVALID_TYPE) {
        switch (physical_schema.type) {
        case tparquet::Type::BOOLEAN:
            ans.first = DataTypeFactory::instance().create_data_type(TYPE_BOOLEAN, nullable);
            break;
        case tparquet::Type::INT32:
            ans.first = DataTypeFactory::instance().create_data_type(TYPE_INT, nullable);
            break;
        case tparquet::Type::INT64:
            ans.first = DataTypeFactory::instance().create_data_type(TYPE_BIGINT, nullable);
            break;
        case tparquet::Type::INT96:
            if (_enable_mapping_timestamp_tz) {
                // treat INT96 as TIMESTAMPTZ
                ans.first = DataTypeFactory::instance().create_data_type(TYPE_TIMESTAMPTZ, nullable,
                                                                         0, 6);
            } else {
                // in most cases, it's a nano timestamp
                ans.first = DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, nullable,
                                                                         0, 6);
            }
            break;
        case tparquet::Type::FLOAT:
            ans.first = DataTypeFactory::instance().create_data_type(TYPE_FLOAT, nullable);
            break;
        case tparquet::Type::DOUBLE:
            ans.first = DataTypeFactory::instance().create_data_type(TYPE_DOUBLE, nullable);
            break;
        case tparquet::Type::BYTE_ARRAY:
            if (_enable_mapping_varbinary) {
                // if physical_schema not set logicalType and converted_type,
                // we treat BYTE_ARRAY as VARBINARY by default, so that we can read all data directly.
                ans.first = DataTypeFactory::instance().create_data_type(TYPE_VARBINARY, nullable);
            } else {
                ans.first = DataTypeFactory::instance().create_data_type(TYPE_STRING, nullable);
            }
            break;
        case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
            ans.first = DataTypeFactory::instance().create_data_type(TYPE_STRING, nullable);
            break;
        default:
            throw Exception(Status::InternalError("Not supported parquet logicalType{}",
                                                  physical_schema.type));
            break;
        }
    }
    return ans;
}

std::pair<DataTypePtr, bool> NativeFieldDescriptor::convert_to_doris_type(
        tparquet::LogicalType logicalType, bool nullable) {
    std::pair<DataTypePtr, bool> ans = {std::make_shared<DataTypeNothing>(), false};
    bool& is_type_compatibility = ans.second;
    if (logicalType.__isset.STRING || logicalType.__isset.ENUM || logicalType.__isset.JSON ||
        logicalType.__isset.BSON) {
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_STRING, nullable);
    } else if (logicalType.__isset.DECIMAL) {
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_DECIMAL128I, nullable,
                                                                 logicalType.DECIMAL.precision,
                                                                 logicalType.DECIMAL.scale);
    } else if (logicalType.__isset.DATE) {
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_DATEV2, nullable);
    } else if (logicalType.__isset.INTEGER) {
        if (logicalType.INTEGER.isSigned) {
            if (logicalType.INTEGER.bitWidth <= 8) {
                ans.first = DataTypeFactory::instance().create_data_type(TYPE_TINYINT, nullable);
            } else if (logicalType.INTEGER.bitWidth <= 16) {
                ans.first = DataTypeFactory::instance().create_data_type(TYPE_SMALLINT, nullable);
            } else if (logicalType.INTEGER.bitWidth <= 32) {
                ans.first = DataTypeFactory::instance().create_data_type(TYPE_INT, nullable);
            } else {
                ans.first = DataTypeFactory::instance().create_data_type(TYPE_BIGINT, nullable);
            }
        } else {
            is_type_compatibility = true;
            if (logicalType.INTEGER.bitWidth <= 8) {
                ans.first = DataTypeFactory::instance().create_data_type(TYPE_SMALLINT, nullable);
            } else if (logicalType.INTEGER.bitWidth <= 16) {
                ans.first = DataTypeFactory::instance().create_data_type(TYPE_INT, nullable);
            } else if (logicalType.INTEGER.bitWidth <= 32) {
                ans.first = DataTypeFactory::instance().create_data_type(TYPE_BIGINT, nullable);
            } else {
                ans.first = DataTypeFactory::instance().create_data_type(TYPE_LARGEINT, nullable);
            }
        }
    } else if (logicalType.__isset.TIME) {
        const int scale = logicalType.TIME.unit.__isset.MILLIS ? 3 : 6;
        // TIME stores an integer unit, so its Doris scale must preserve the footer unit or
        // sub-second values are silently truncated by the target SerDe.
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_TIMEV2, nullable, 0, scale);
    } else if (logicalType.__isset.TIMESTAMP) {
        if (_enable_mapping_timestamp_tz) {
            if (logicalType.TIMESTAMP.isAdjustedToUTC) {
                // treat TIMESTAMP with isAdjustedToUTC as TIMESTAMPTZ
                ans.first = DataTypeFactory::instance().create_data_type(
                        TYPE_TIMESTAMPTZ, nullable, 0,
                        logicalType.TIMESTAMP.unit.__isset.MILLIS ? 3 : 6);
                return ans;
            }
        }
        ans.first = DataTypeFactory::instance().create_data_type(
                TYPE_DATETIMEV2, nullable, 0, logicalType.TIMESTAMP.unit.__isset.MILLIS ? 3 : 6);
    } else if (logicalType.__isset.UUID) {
        if (_enable_mapping_varbinary) {
            ans.first = DataTypeFactory::instance().create_data_type(TYPE_VARBINARY, nullable, -1,
                                                                     -1, 16);
        } else {
            ans.first = DataTypeFactory::instance().create_data_type(TYPE_STRING, nullable);
        }
    } else if (logicalType.__isset.FLOAT16) {
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_FLOAT, nullable);
    } else {
        throw Exception(Status::InternalError("Not supported parquet logicalType"));
    }
    return ans;
}

std::pair<DataTypePtr, bool> NativeFieldDescriptor::convert_to_doris_type(
        const tparquet::SchemaElement& physical_schema, bool nullable) {
    std::pair<DataTypePtr, bool> ans = {std::make_shared<DataTypeNothing>(), false};
    bool& is_type_compatibility = ans.second;
    switch (physical_schema.converted_type) {
    case tparquet::ConvertedType::type::UTF8:
    case tparquet::ConvertedType::type::ENUM:
    case tparquet::ConvertedType::type::JSON:
    case tparquet::ConvertedType::type::BSON:
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_STRING, nullable);
        break;
    case tparquet::ConvertedType::type::DECIMAL:
        ans.first = DataTypeFactory::instance().create_data_type(
                TYPE_DECIMAL128I, nullable, physical_schema.precision, physical_schema.scale);
        break;
    case tparquet::ConvertedType::type::DATE:
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_DATEV2, nullable);
        break;
    case tparquet::ConvertedType::type::TIME_MILLIS:
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_TIMEV2, nullable, 0, 3);
        break;
    case tparquet::ConvertedType::type::TIME_MICROS:
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_TIMEV2, nullable, 0, 6);
        break;
    case tparquet::ConvertedType::type::TIMESTAMP_MILLIS:
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, nullable, 0, 3);
        break;
    case tparquet::ConvertedType::type::TIMESTAMP_MICROS:
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, nullable, 0, 6);
        break;
    case tparquet::ConvertedType::type::INT_8:
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_TINYINT, nullable);
        break;
    case tparquet::ConvertedType::type::UINT_8:
        is_type_compatibility = true;
        [[fallthrough]];
    case tparquet::ConvertedType::type::INT_16:
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_SMALLINT, nullable);
        break;
    case tparquet::ConvertedType::type::UINT_16:
        is_type_compatibility = true;
        [[fallthrough]];
    case tparquet::ConvertedType::type::INT_32:
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_INT, nullable);
        break;
    case tparquet::ConvertedType::type::UINT_32:
        is_type_compatibility = true;
        [[fallthrough]];
    case tparquet::ConvertedType::type::INT_64:
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_BIGINT, nullable);
        break;
    case tparquet::ConvertedType::type::UINT_64:
        is_type_compatibility = true;
        ans.first = DataTypeFactory::instance().create_data_type(TYPE_LARGEINT, nullable);
        break;
    default:
        throw Exception(Status::InternalError("Not supported parquet ConvertedType: {}",
                                              physical_schema.converted_type));
    }
    return ans;
}

Status NativeFieldDescriptor::parse_group_field(
        const std::vector<tparquet::SchemaElement>& t_schemas, size_t curr_pos,
        NativeFieldSchema* group_field) {
    auto& group_schema = t_schemas[curr_pos];
    if ((group_schema.__isset.logicalType && group_schema.logicalType.__isset.ENUM) ||
        (group_schema.__isset.converted_type &&
         group_schema.converted_type == tparquet::ConvertedType::ENUM)) {
        // ENUM describes primitive bytes only. Rejecting it before recursive group parsing keeps
        // the native metadata tree from silently accepting a schema the Parquet contract forbids.
        return Status::InvalidArgument("Logical type Enum cannot be applied to group node");
    }
    if (is_map_node(group_schema)) {
        // the map definition:
        // optional group <name> (MAP) {
        //   repeated group map (MAP_KEY_VALUE) {
        //     required <type> key;
        //     optional <type> value;
        //   }
        // }
        return parse_map_field(t_schemas, curr_pos, group_field);
    }
    if (is_list_node(group_schema)) {
        // the list definition:
        // optional group <name> (LIST) {
        //   repeated group [bag | list] { // hive or spark
        //     optional <type> [array_element | element]; // hive or spark
        //   }
        // }
        return parse_list_field(t_schemas, curr_pos, group_field);
    }

    if (is_repeated_node(group_schema)) {
        group_field->repetition_level++;
        group_field->definition_level++;
        group_field->children.resize(1);
        set_child_node_level(group_field, group_field->definition_level);
        auto struct_field = &group_field->children[0];
        // the list of struct:
        // repeated group <name> (LIST) {
        //   optional/required <type> <name>;
        //   ...
        // }
        // produce a non-null list<struct>
        RETURN_IF_ERROR(parse_struct_field(t_schemas, curr_pos, struct_field));

        group_field->name = group_schema.name;
        group_field->lower_case_name = to_lower(group_field->name);
        group_field->column_id = NATIVE_UNASSIGNED_COLUMN_ID; // Initialize column_id
        group_field->data_type =
                std::make_shared<DataTypeArray>(make_nullable(struct_field->data_type));
        group_field->field_id = group_schema.__isset.field_id ? group_schema.field_id : -1;
    } else {
        RETURN_IF_ERROR(parse_struct_field(t_schemas, curr_pos, group_field));
    }

    return Status::OK();
}

Status NativeFieldDescriptor::parse_list_field(
        const std::vector<tparquet::SchemaElement>& t_schemas, size_t curr_pos,
        NativeFieldSchema* list_field, bool repeated_node_is_enclosing_list_element) {
    // the list definition:
    // spark and hive have three level schemas but with different schema name
    // spark: <column-name> - "list" - "element"
    // hive: <column-name> - "bag" - "array_element"
    // parse three level schemas to two level primitive like: LIST<INT>,
    // or nested structure like: LIST<MAP<INT, INT>>
    auto& first_level = t_schemas[curr_pos];
    if (first_level.num_children != 1) {
        return Status::InvalidArgument("List element should have only one child");
    }

    if (curr_pos + 1 >= t_schemas.size()) {
        return Status::InvalidArgument("List element should have the second level schema");
    }

    if (first_level.repetition_type == tparquet::FieldRepetitionType::REPEATED &&
        !repeated_node_is_enclosing_list_element) {
        return Status::InvalidArgument("List element can't be a repeated schema");
    }

    // the repeated schema element
    auto& second_level = t_schemas[curr_pos + 1];
    if (second_level.repetition_type != tparquet::FieldRepetitionType::REPEATED) {
        return Status::InvalidArgument("The second level of list element should be repeated");
    }

    // This indicates if this list is nullable.
    bool is_optional = is_optional_node(first_level);
    if (is_optional) {
        list_field->definition_level++;
    }
    list_field->repetition_level++;
    list_field->definition_level++;
    list_field->children.resize(1);
    NativeFieldSchema* list_child = &list_field->children[0];

    size_t num_children = num_children_node(second_level);
    if (num_children > 0) {
        const bool structural_wrapper = is_struct_list_node(second_level, first_level.name);
        const auto& only_child = t_schemas[curr_pos + 2];
        if (num_children == 1 && !structural_wrapper &&
            has_logical_annotation(second_level)) {
            // The repeated node is already the outer LIST element. Preserve its own LIST/MAP
            // annotation, but do not interpret its REPEATED marker as another outer array.
            set_child_node_level(list_field, list_field->definition_level);
            if (is_list_node(second_level)) {
                RETURN_IF_ERROR(parse_list_field(t_schemas, curr_pos + 1, list_child, true));
            } else if (is_map_node(second_level)) {
                RETURN_IF_ERROR(parse_map_field(t_schemas, curr_pos + 1, list_child, true));
            } else {
                RETURN_IF_ERROR(parse_struct_field(t_schemas, curr_pos + 1, list_child));
            }
        } else if (num_children == 1 && !structural_wrapper &&
                   !is_repeated_node(only_child)) {
            // optional field, and the third level element is the nested structure in list
            // produce nested structure like: LIST<INT>, LIST<MAP>, LIST<LIST<...>>
            // skip bag/list, it's a repeated element.
            set_child_node_level(list_field, list_field->definition_level);
            RETURN_IF_ERROR(parse_node_field(t_schemas, curr_pos + 2, list_child));
        } else {
            // required field, produce the list of struct
            set_child_node_level(list_field, list_field->definition_level);
            RETURN_IF_ERROR(parse_struct_field(t_schemas, curr_pos + 1, list_child));
        }
    } else if (num_children == 0) {
        // required two level list, for compatibility reason.
        set_child_node_level(list_field, list_field->definition_level);
        parse_physical_field(second_level, false, list_child);
        _next_schema_pos = curr_pos + 2;
    }

    list_field->name = first_level.name;
    list_field->lower_case_name = to_lower(first_level.name);
    list_field->column_id = NATIVE_UNASSIGNED_COLUMN_ID; // Initialize column_id
    list_field->data_type =
            std::make_shared<DataTypeArray>(make_nullable(list_field->children[0].data_type));
    if (is_optional) {
        list_field->data_type = make_nullable(list_field->data_type);
    }
    list_field->field_id = first_level.__isset.field_id ? first_level.field_id : -1;

    return Status::OK();
}

Status NativeFieldDescriptor::parse_map_field(const std::vector<tparquet::SchemaElement>& t_schemas,
                                              size_t curr_pos, NativeFieldSchema* map_field,
                                              bool repeated_node_is_enclosing_list_element) {
    // the map definition in parquet:
    // optional group <name> (MAP) {
    //   repeated group map (MAP_KEY_VALUE) {
    //     required <type> key;
    //     optional <type> value;
    //   }
    // }
    // Map value can be optional, the map without values is a SET
    if (curr_pos + 2 >= t_schemas.size()) {
        return Status::InvalidArgument("Map element should have at least three levels");
    }
    auto& map_schema = t_schemas[curr_pos];
    if (map_schema.num_children != 1) {
        return Status::InvalidArgument(
                "Map element should have only one child(name='map', type='MAP_KEY_VALUE')");
    }
    if (is_repeated_node(map_schema) && !repeated_node_is_enclosing_list_element) {
        return Status::InvalidArgument("Map element can't be a repeated schema");
    }
    auto& map_key_value = t_schemas[curr_pos + 1];
    if (!is_group_node(map_key_value) || !is_repeated_node(map_key_value)) {
        return Status::InvalidArgument(
                "the second level in map must be a repeated group(key and value)");
    }
    auto& map_key = t_schemas[curr_pos + 2];
    if (!is_required_node(map_key)) {
        LOG(WARNING) << "Filed " << map_schema.name << " is map type, but with nullable key column";
    }

    if (map_key_value.num_children == 1) {
        // The map with three levels is a SET
        return parse_list_field(t_schemas, curr_pos, map_field,
                                repeated_node_is_enclosing_list_element);
    }
    if (map_key_value.num_children != 2) {
        // A standard map should have four levels
        return Status::InvalidArgument(
                "the second level in map(MAP_KEY_VALUE) should have two children");
    }
    // standard map
    bool is_optional = is_optional_node(map_schema);
    if (is_optional) {
        map_field->definition_level++;
    }
    map_field->repetition_level++;
    map_field->definition_level++;

    // Directly create key and value children instead of intermediate key_value node
    map_field->children.resize(2);
    // map is a repeated node, we should set the `repeated_parent_def_level` of its children as `definition_level`
    set_child_node_level(map_field, map_field->definition_level);

    auto key_field = &map_field->children[0];
    auto value_field = &map_field->children[1];

    // Parse key and value fields directly from the key_value group's children
    _next_schema_pos = curr_pos + 2; // Skip key_value group, go directly to key
    RETURN_IF_ERROR(parse_node_field(t_schemas, _next_schema_pos, key_field));
    RETURN_IF_ERROR(parse_node_field(t_schemas, _next_schema_pos, value_field));

    map_field->name = map_schema.name;
    map_field->lower_case_name = to_lower(map_field->name);
    map_field->column_id = NATIVE_UNASSIGNED_COLUMN_ID; // Initialize column_id
    map_field->data_type = std::make_shared<DataTypeMap>(make_nullable(key_field->data_type),
                                                         make_nullable(value_field->data_type));
    if (is_optional) {
        map_field->data_type = make_nullable(map_field->data_type);
    }
    map_field->field_id = map_schema.__isset.field_id ? map_schema.field_id : -1;

    return Status::OK();
}

Status NativeFieldDescriptor::parse_struct_field(
        const std::vector<tparquet::SchemaElement>& t_schemas, size_t curr_pos,
        NativeFieldSchema* struct_field) {
    // the nested column in parquet, parse group to struct.
    auto& struct_schema = t_schemas[curr_pos];
    bool is_optional = is_optional_node(struct_schema);
    if (is_optional) {
        struct_field->definition_level++;
    }
    auto num_children = struct_schema.num_children;
    struct_field->children.resize(num_children);
    set_child_node_level(struct_field, struct_field->repeated_parent_def_level);
    _next_schema_pos = curr_pos + 1;
    for (int i = 0; i < num_children; ++i) {
        RETURN_IF_ERROR(parse_node_field(t_schemas, _next_schema_pos, &struct_field->children[i]));
    }
    struct_field->name = struct_schema.name;
    struct_field->lower_case_name = to_lower(struct_field->name);
    struct_field->column_id = NATIVE_UNASSIGNED_COLUMN_ID; // Initialize column_id

    struct_field->field_id = struct_schema.__isset.field_id ? struct_schema.field_id : -1;
    DataTypes res_data_types;
    std::vector<String> names;
    for (int i = 0; i < num_children; ++i) {
        res_data_types.push_back(make_nullable(struct_field->children[i].data_type));
        names.push_back(struct_field->children[i].name);
    }
    struct_field->data_type = std::make_shared<DataTypeStruct>(res_data_types, names);
    if (is_optional) {
        struct_field->data_type = make_nullable(struct_field->data_type);
    }
    return Status::OK();
}

int NativeFieldDescriptor::get_column_index(const std::string& column) const {
    for (int32_t i = 0; i < _fields.size(); i++) {
        if (_fields[i].name == column) {
            return i;
        }
    }
    return -1;
}

NativeFieldSchema* NativeFieldDescriptor::get_column(const std::string& name) const {
    auto it = _name_to_field.find(name);
    if (it != _name_to_field.end()) {
        return it->second;
    }
    throw Exception(Status::InternalError("Name {} not found in NativeFieldDescriptor!", name));
    return nullptr;
}

void NativeFieldDescriptor::get_column_names(std::unordered_set<std::string>* names) const {
    names->clear();
    for (const NativeFieldSchema& f : _fields) {
        names->emplace(f.name);
    }
}

std::string NativeFieldDescriptor::debug_string() const {
    std::stringstream ss;
    ss << "fields=[";
    for (int i = 0; i < _fields.size(); ++i) {
        if (i != 0) {
            ss << ", ";
        }
        ss << _fields[i].debug_string();
    }
    ss << "]";
    return ss.str();
}

void NativeFieldDescriptor::assign_ids() {
    uint64_t next_id = 1;
    for (auto& field : _fields) {
        field.assign_ids(next_id);
    }
}

const NativeFieldSchema* NativeFieldDescriptor::find_column_by_id(uint64_t column_id) const {
    for (const auto& field : _fields) {
        if (auto result = field.find_column_by_id(column_id)) {
            return result;
        }
    }
    return nullptr;
}

void NativeFieldSchema::assign_ids(uint64_t& next_id) {
    column_id = next_id++;

    for (auto& child : children) {
        child.assign_ids(next_id);
    }

    max_column_id = next_id - 1;
}

const NativeFieldSchema* NativeFieldSchema::find_column_by_id(uint64_t target_id) const {
    if (column_id == target_id) {
        return this;
    }

    for (const auto& child : children) {
        if (auto result = child.find_column_by_id(target_id)) {
            return result;
        }
    }

    return nullptr;
}

uint64_t NativeFieldSchema::get_column_id() const {
    return column_id;
}

void NativeFieldSchema::set_column_id(uint64_t id) {
    column_id = id;
}

uint64_t NativeFieldSchema::get_max_column_id() const {
    return max_column_id;
}

} // namespace doris::format::parquet
