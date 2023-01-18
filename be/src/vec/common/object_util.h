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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/ObjectUtils.cpp
// and modified by Doris

#pragma once

#include <parallel_hashmap/phmap.h>
#include <vec/columns/column_object.h>
#include <vec/common/field_visitors.h>
#include <vec/core/block.h>
#include <vec/core/column_with_type_and_name.h>
#include <vec/data_types/data_type_number.h>

#include "olap/tablet_schema.h"

namespace doris {
class LocalSchemaChangeRecorder;
}

namespace doris::vectorized::object_util {
/// Returns number of dimensions in Array type. 0 if type is not array.
size_t get_number_of_dimensions(const IDataType& type);

/// Returns number of dimensions in Array column. 0 if column is not array.
size_t get_number_of_dimensions(const IColumn& column);

/// Returns type of scalars of Array of arbitrary dimensions.
DataTypePtr get_base_type_of_array(const DataTypePtr& type);

/// Returns Array with requested number of dimensions and no scalars.
Array create_empty_array_field(size_t num_dimensions);

/// Converts Object types and columns to Tuples in @columns_list and @block
/// and checks that types are consistent with types in @extended_storage_columns.
Status convert_objects_to_tuples(Block& block);

/// Receives several Tuple types and deduces the least common type among them.
DataTypePtr get_least_common_type_for_object(const DataTypes& types,
                                             bool check_ambiguos_paths = false);

/// Flattens nested Tuple to plain Tuple. I.e extracts all paths and types from tuple.
/// E.g. Tuple(t Tuple(c1 UInt32, c2 String), c3 UInt64) -> Tuple(t.c1 UInt32, t.c2 String, c3 UInt32)
std::pair<PathsInData, DataTypes> flatten_tuple(const DataTypePtr& type);

/// Flattens nested Tuple column to plain columns.
Columns flatten_tuple(const ColumnPtr& column);

void flatten_tuple(Block& block);

/// The reverse operation to 'flattenTuple'.
/// Creates nested Tuple from all paths and types.
/// E.g. Tuple(t.c1 UInt32, t.c2 String, c3 UInt32) -> Tuple(t Tuple(c1 UInt32, c2 String), c3 UInt64)
DataTypePtr unflatten_tuple(const PathsInData& paths, const DataTypes& tuple_types);

std::pair<ColumnPtr, DataTypePtr> unflatten_tuple(const PathsInData& paths,
                                                  const DataTypes& tuple_types,
                                                  const Columns& tuple_columns);

// None nested type
FieldType get_field_type(const IDataType* data_type);

// NOTICE: the last column must be dynamic column
// 1. The dynamic column will be parsed to ColumnObject and the parsed column will
// be flattened to multiple subcolumns, thus the dynamic schema is infered from the
// dynamic column.
// 2. Schema change which is add columns will be performed if the infered schema is
// different from the original tablet schema, new columns added to schema change history
Status parse_and_expand_dynamic_column(Block& block, const TabletSchema& schema_hints,
                                       LocalSchemaChangeRecorder* history);

Status parse_object_column(Block& block, size_t position);

Status parse_object_column(ColumnObject& dest, const IColumn& src, bool need_finalize,
                           const int* row_begin, const int* row_end);

// Object column will be flattened and if replace_if_duplicated
// the original column in the block will be replaced with the subcolumn
// from object column.Also if column in block is empty, it will be filled
// with num_rows of default values
Status flatten_object(Block& block, bool replace_if_duplicated);

/// If both of types are signed/unsigned integers and size of left field type
/// is less than right type, we don't need to convert field,
/// because all integer fields are stored in Int64/UInt64.
bool is_conversion_required_between_integers(const IDataType& lhs, const IDataType& rhs);
bool is_conversion_required_between_integers(FieldType lhs, FieldType rhs);

// Cast column to type
Status cast_column(const ColumnWithTypeAndName& arg, const DataTypePtr& type, ColumnPtr* result);

// Align block schema with tablet schema
// eg.
// Block:   col1(int), col2(string)
// Schema:  col1(double), col3(date)
// 1. col1(int) in block which type missmatch with schema col1 will be converted to double
// 2. col2 in block which missing in current schema will launch a schema change rpc
// 3. col3 in schema which missing in block will be ignored
// After schema changed, schame change history will add new columns
Status align_block_with_schema(const TabletSchema& schema, int64_t table_id /*for schema change*/,
                               Block& block, LocalSchemaChangeRecorder* history);
// record base schema column infos
// maybe use col_unique_id as key in the future
// but for dynamic table, column name if ok
struct FullBaseSchemaView {
    phmap::flat_hash_map<std::string, TColumn> column_name_to_column;
    int32_t schema_version = -1;
    int32_t table_id = 0;
    std::string table_name;
    std::string db_name;

    bool empty() { return column_name_to_column.empty() && schema_version == -1; }
};

Status send_add_columns_rpc(ColumnsWithTypeAndName column_type_names,
                            FullBaseSchemaView* schema_view);

Status send_fetch_full_base_schema_view_rpc(FullBaseSchemaView* schema_view);

// block alignment
// TODO using column_unique_id instead of names
void align_block_by_name_and_type(MutableBlock* mblock, const Block* block, const int* row_begin,
                                  const int* row_end);
void align_block_by_name_and_type(MutableBlock* mblock, const Block* block, size_t row_begin,
                                  size_t length);

void align_append_block_by_selector(MutableBlock* mblock,
                const Block* block, const IColumn::Selector& selector);

// For tracking local schema change during load procedure
class LocalSchemaChangeRecorder {
public:
    void add_extended_columns(const TabletColumn& new_column, int32_t schema_version);
    bool has_extended_columns();
    std::map<std::string, TabletColumn> copy_extended_columns();
    const TabletColumn& column(const std::string& col_name);
    int32_t schema_version();

private:
    std::mutex _lock;
    int32_t _schema_version = -1;
    std::map<std::string, TabletColumn> _extended_columns;
};

} // namespace  doris::vectorized::object_util
