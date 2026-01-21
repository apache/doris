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

#pragma once

#include <gen_cpp/parquet_types.h>
#include <stddef.h>
#include <stdint.h>

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/cast_set.h"
#include "common/status.h"
#include "gen_cpp/Planner_types.h"
#include "runtime/types.h"
#include "util/slice.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nothing.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

// Constant for unassigned column IDs
constexpr uint64_t UNASSIGNED_COLUMN_ID = UINT64_MAX;

struct FieldSchema {
    std::string name;
    std::string lower_case_name; // for hms column name case insensitive match
    // the referenced parquet schema element
    tparquet::SchemaElement parquet_schema;

    // Used to identify whether this field is a nested field.
    DataTypePtr data_type;

    // Only valid when this field is a leaf node
    tparquet::Type::type physical_type;
    // The index order in FieldDescriptor._physical_fields
    int physical_column_index = -1;
    int16_t definition_level = 0;
    int16_t repetition_level = 0;
    int16_t repeated_parent_def_level = 0;
    std::vector<FieldSchema> children;

    //For UInt8 -> Int16,UInt16 -> Int32,UInt32 -> Int64,UInt64 -> Int128.
    bool is_type_compatibility = false;

    FieldSchema()
            : data_type(std::make_shared<DataTypeNothing>()), column_id(UNASSIGNED_COLUMN_ID) {}
    ~FieldSchema() = default;
    FieldSchema(const FieldSchema& fieldSchema) = default;
    std::string debug_string() const;

    int32_t field_id = -1;
    uint64_t column_id = UNASSIGNED_COLUMN_ID;
    uint64_t max_column_id = 0; // Maximum column ID for this field and its children

    // Column ID assignment and lookup methods
    void assign_ids(uint64_t& next_id);
    const FieldSchema* find_column_by_id(uint64_t target_id) const;
    uint64_t get_column_id() const;
    void set_column_id(uint64_t id);
    uint64_t get_max_column_id() const;
};

class FieldDescriptor {
private:
    // Only the schema elements at the first level
    std::vector<FieldSchema> _fields;
    // The leaf node of schema elements
    std::vector<FieldSchema*> _physical_fields;
    // Name to _fields, not all schema elements
    std::unordered_map<std::string, FieldSchema*> _name_to_field;
    // Used in from_thrift, marking the next schema position that should be parsed
    size_t _next_schema_pos;
    // useful for parse_node_field to decide whether to convert byte_array to VARBINARY type
    bool _enable_mapping_varbinary = false;
    bool _enable_mapping_timestamp_tz = false;

private:
    void parse_physical_field(const tparquet::SchemaElement& physical_schema, bool is_nullable,
                              FieldSchema* physical_field);

    Status parse_list_field(const std::vector<tparquet::SchemaElement>& t_schemas, size_t curr_pos,
                            FieldSchema* list_field);

    Status parse_map_field(const std::vector<tparquet::SchemaElement>& t_schemas, size_t curr_pos,
                           FieldSchema* map_field);

    Status parse_struct_field(const std::vector<tparquet::SchemaElement>& t_schemas,
                              size_t curr_pos, FieldSchema* struct_field);

    Status parse_group_field(const std::vector<tparquet::SchemaElement>& t_schemas, size_t curr_pos,
                             FieldSchema* group_field);

    Status parse_node_field(const std::vector<tparquet::SchemaElement>& t_schemas, size_t curr_pos,
                            FieldSchema* node_field);

    std::pair<DataTypePtr, bool> convert_to_doris_type(tparquet::LogicalType logicalType,
                                                       bool nullable);
    std::pair<DataTypePtr, bool> convert_to_doris_type(
            const tparquet::SchemaElement& physical_schema, bool nullable);
    std::pair<DataTypePtr, bool> get_doris_type(const tparquet::SchemaElement& physical_schema,
                                                bool nullable);

public:
    FieldDescriptor() = default;
    ~FieldDescriptor() = default;

    /**
     * Parse FieldDescriptor from parquet thrift FileMetaData.
     * @param t_schemas list of schema elements
     */
    Status parse_from_thrift(const std::vector<tparquet::SchemaElement>& t_schemas);

    int get_column_index(const std::string& column) const;

    /**
     * Get the column(the first level schema element, maybe nested field) by index.
     * @param index Column index in _fields
     */
    const FieldSchema* get_column(int index) const { return &_fields[index]; }

    /**
     * Get the column(the first level schema element, maybe nested field) by name.
     * @param name Column name
     * @return FieldSchema or nullptr if not exists
     */
    FieldSchema* get_column(const std::string& name) const;

    void get_column_names(std::unordered_set<std::string>* names) const;

    std::string debug_string() const;

    int32_t size() const { return cast_set<int32_t>(_fields.size()); }

    const std::vector<FieldSchema>& get_fields_schema() const { return _fields; }

    /**
     * Assign stable column IDs to schema fields.
     *
     * This uses an ORC-compatible encoding so that the results of
     * create_column_ids() are consistent across formats. IDs start from 1
     * and are assigned in a pre-order traversal (parent before children).
     * After calling this, each FieldSchema will have column_id and
     * max_column_id populated.
     */
    void assign_ids();

    const FieldSchema* find_column_by_id(uint64_t column_id) const;
    void set_enable_mapping_varbinary(bool enable) { _enable_mapping_varbinary = enable; }
    void set_enable_mapping_timestamp_tz(bool enable) { _enable_mapping_timestamp_tz = enable; }
};
#include "common/compile_check_end.h"

} // namespace doris::vectorized
