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

#include <gen_cpp/Descriptors_types.h>
#include <parallel_hashmap/phmap.h>
#include <stddef.h>
#include <stdint.h>

#include <map>
#include <mutex>
#include <string>

#include "common/status.h"
#include "olap/tablet_schema.h"
#include "udf/udf.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type.h"

namespace doris {
class LocalSchemaChangeRecorder;
enum class FieldType;

namespace vectorized {
class Block;
class IColumn;
struct ColumnWithTypeAndName;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized::schema_util {
/// Returns number of dimensions in Array type. 0 if type is not array.
size_t get_number_of_dimensions(const IDataType& type);

/// Returns number of dimensions in Array column. 0 if column is not array.
size_t get_number_of_dimensions(const IColumn& column);

/// Returns type of scalars of Array of arbitrary dimensions.
DataTypePtr get_base_type_of_array(const DataTypePtr& type);

/// Returns Array with requested number of dimensions and no scalars.
Array create_empty_array_field(size_t num_dimensions);

// Cast column to dst type
Status cast_column(const ColumnWithTypeAndName& arg, const DataTypePtr& type, ColumnPtr* result,
                   RuntimeState* = nullptr);

// Object column will be unfolded and if  cast_to_original_type
// the original column in the block will be replaced with the subcolumn
// from object column and casted to the new type from slot_descs.
// Also if column in block is empty, it will be filled
// with num_rows of default values
Status unfold_object(size_t dynamic_col_position, Block& block, bool cast_to_original_type,
                     RuntimeState* = nullptr);

/// If both of types are signed/unsigned integers and size of left field type
/// is less than right type, we don't need to convert field,
/// because all integer fields are stored in Int64/UInt64.
bool is_conversion_required_between_integers(const IDataType& lhs, const IDataType& rhs);
bool is_conversion_required_between_integers(FieldType lhs, FieldType rhs);

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
    ENABLE_FACTORY_CREATOR(FullBaseSchemaView);
    phmap::flat_hash_map<std::string, TColumn> column_name_to_column;
    int32_t schema_version = -1;
    int32_t table_id = 0;
    std::string table_name;
    std::string db_name;
};

Status send_add_columns_rpc(ColumnsWithTypeAndName column_type_names,
                            FullBaseSchemaView* schema_view);

Status send_fetch_full_base_schema_view_rpc(FullBaseSchemaView* schema_view);

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

} // namespace  doris::vectorized::schema_util
