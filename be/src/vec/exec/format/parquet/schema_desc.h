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

#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "runtime/types.h"

namespace doris::vectorized {

struct FieldSchema {
    std::string name;
    // the referenced parquet schema element
    tparquet::SchemaElement parquet_schema;

    // Used to identify whether this field is a nested field.
    TypeDescriptor type;
    bool is_nullable;

    // Only valid when this field is a leaf node
    tparquet::Type::type physical_type;
    // The index order in FieldDescriptor._physical_fields
    int physical_column_index = -1;

    int16_t definition_level = 0;
    int16_t repetition_level = 0;
    std::vector<FieldSchema> children;

    std::string debug_string() const;
};

class FieldDescriptor {
private:
    // Only the schema elements at the first level
    std::vector<FieldSchema> _fields;
    // The leaf node of schema elements
    std::vector<FieldSchema*> _physical_fields;
    // Name to _fields, not all schema elements
    std::unordered_map<std::string, const FieldSchema*> _name_to_field;
    // Used in from_thrift, marking the next schema position that should be parsed
    size_t _next_schema_pos;

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

    TypeDescriptor convert_to_doris_type(tparquet::LogicalType logicalType);

    TypeDescriptor convert_to_doris_type(tparquet::ConvertedType::type convertedType);

    TypeDescriptor get_doris_type(const tparquet::SchemaElement& physical_schema);

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
    const FieldSchema* get_column(const std::string& name) const;

    void get_column_names(std::unordered_set<std::string>* names) const;

    std::string debug_string() const;

    int32_t size() const { return _fields.size(); };
};

} // namespace doris::vectorized
