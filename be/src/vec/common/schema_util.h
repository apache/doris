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

#include <parallel_hashmap/phmap.h>
#include <vec/columns/column_object.h>
#include <vec/common/field_visitors.h>
#include <vec/core/block.h>
#include <vec/core/column_with_type_and_name.h>
#include <vec/data_types/data_type_number.h>

#include "olap/tablet_schema.h"
#include "runtime/descriptors.h"
#include "vec/data_types/data_type.h"

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
Status cast_column(const ColumnWithTypeAndName& arg, const DataTypePtr& type, ColumnPtr* result);

// Object column will be unfolded and if  cast_to_original_type
// the original column in the block will be replaced with the subcolumn
// from object column and casted to the new type from slot_descs.
// Also if column in block is empty, it will be filled
// with num_rows of default values
void unfold_object(size_t dynamic_col_position, Block& block, bool cast_to_original_type);

/// If both of types are signed/unsigned integers and size of left field type
/// is less than right type, we don't need to convert field,
/// because all integer fields are stored in Int64/UInt64.
bool is_conversion_required_between_integers(const IDataType& lhs, const IDataType& rhs);
bool is_conversion_required_between_integers(FieldType lhs, FieldType rhs);

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

} // namespace  doris::vectorized::schema_util
