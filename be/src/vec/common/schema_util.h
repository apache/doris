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
#include "olap/tablet_fwd.h"
#include "olap/tablet_schema.h"
#include "udf/udf.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_variant.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"

namespace doris {
enum class FieldType;
namespace segment_v2 {
struct VariantStatisticsPB;
} // namespace segment_v2
namespace vectorized {
class Block;
class IColumn;
struct ColumnWithTypeAndName;
struct ParseConfig;
} // namespace vectorized
} // namespace doris

const std::string SPARSE_COLUMN_PATH = "__DORIS_VARIANT_SPARSE__";
namespace doris::vectorized::schema_util {
using PathToNoneNullValues = std::unordered_map<std::string, int64_t>;
using PathToDataTypes = std::unordered_map<PathInData, std::vector<DataTypePtr>, PathInData::Hash>;

struct VariantExtendedInfo {
    PathToNoneNullValues path_to_none_null_values; // key: path, value: number of none null values
    std::unordered_set<std::string> sparse_paths;  // sparse paths in this variant column
    std::unordered_set<std::string> typed_paths;   // typed paths in this variant column
    std::unordered_set<vectorized::PathInData, vectorized::PathInData::Hash>
            nested_paths;               // nested paths in this variant column
    PathToDataTypes path_to_data_types; // key: path, value: data types
};

/// Returns number of dimensions in Array type. 0 if type is not array.
size_t get_number_of_dimensions(const IDataType& type);

/// Returns number of dimensions in Array column. 0 if column is not array.
size_t get_number_of_dimensions(const IColumn& column);

/// Returns type of scalars of Array of arbitrary dimensions.
DataTypePtr get_base_type_of_array(const DataTypePtr& type);

// Cast column to dst type
Status cast_column(const ColumnWithTypeAndName& arg, const DataTypePtr& type, ColumnPtr* result);

/// If both of types are signed/unsigned integers and size of left field type
/// is less than right type, we don't need to convert field,
/// because all integer fields are stored in Int64/UInt64.
bool is_conversion_required_between_integers(const PrimitiveType& lhs, const PrimitiveType& rhs);

struct ExtraInfo {
    // -1 indicates it's not a Frontend generated column
    int32_t unique_id = -1;
    int32_t parent_unique_id = -1;
    vectorized::PathInData path_info;
};

TabletColumn get_column_by_type(const vectorized::DataTypePtr& data_type, const std::string& name,
                                const ExtraInfo& ext_info);

// three steps to parse and encode variant columns into flatterned columns
// 1. parse variant from raw json string
// 2. finalize variant column to each subcolumn least commn types, default ignore sparse sub columns
// 3. encode sparse sub columns
Status parse_variant_columns(Block& block, const std::vector<int>& variant_pos,
                             const ParseConfig& config);

// check if the tuple_paths has ambiguous paths
// situation:
// throw exception if there exists a prefix with matched names, but not matched structure (is Nested, number of dimensions).
Status check_variant_has_no_ambiguous_paths(const std::vector<PathInData>& paths);

// Pick the tablet schema with the highest schema version as the reference.
// Then update all variant columns to there least common types.
// Return the final merged schema as common schema.
// If base_schema == nullptr then, max schema version tablet schema will be picked as base schema
Status get_least_common_schema(const std::vector<TabletSchemaSPtr>& schemas,
                               const TabletSchemaSPtr& base_schema, TabletSchemaSPtr& result,
                               bool check_schema_size = false);

// Get least common types for extracted columns which has Path info,
// with a speicified variant column's unique id
Status update_least_common_schema(const std::vector<TabletSchemaSPtr>& schemas,
                                  TabletSchemaSPtr& common_schema, int32_t variant_col_unique_id,
                                  std::set<PathInData>* path_set);

// inherit attributes like index/agg info from it's parent column
void inherit_column_attributes(TabletSchemaSPtr& schema);

// source: variant column
// target: extracted column from variant column
void inherit_column_attributes(const TabletColumn& source, TabletColumn& target,
                               TabletSchemaSPtr* target_schema = nullptr);

// get sorted subcolumns of variant
vectorized::ColumnVariant::Subcolumns get_sorted_subcolumns(
        const vectorized::ColumnVariant::Subcolumns& subcolumns);

bool has_schema_index_diff(const TabletSchema* new_schema, const TabletSchema* old_schema,
                           int32_t new_col_idx, int32_t old_col_idx);

// create ColumnMap<String, String>
TabletColumn create_sparse_column(const TabletColumn& variant);

void get_field_info(const Field& field, FieldInfo* info);

// inherit index from parent column
bool inherit_index(const std::vector<const TabletIndex*>& parent_indexes,
                   TabletIndexes& sub_column_indexes, FieldType column_type,
                   const std::string& suffix_path, bool is_array_nested_type = false);

bool inherit_index(const std::vector<const TabletIndex*>& parent_indexes,
                   TabletIndexes& sub_column_indexes, const TabletColumn& column);

bool inherit_index(const std::vector<const TabletIndex*>& parent_indexes,
                   TabletIndexes& sub_column_indexes, const segment_v2::ColumnMetaPB& column_pb);

Status update_least_schema_internal(const std::map<PathInData, DataTypes>& subcolumns_types,
                                    TabletSchemaSPtr& common_schema, int32_t variant_col_unique_id,
                                    const std::map<std::string, TabletColumnPtr>& typed_columns,
                                    std::set<PathInData>* path_set = nullptr);

bool generate_sub_column_info(const TabletSchema& schema, int32_t col_unique_id,
                              const std::string& path,
                              TabletSchema::SubColumnInfo* sub_column_info);

class VariantCompactionUtil {
public:
    // get the subpaths and sparse paths for the variant column
    static void get_subpaths(int32_t max_subcolumns_count, const PathToNoneNullValues& path_stats,
                             TabletSchema::PathsSetInfo& paths_set_info);

    // collect extended info from the variant column
    static Status aggregate_variant_extended_info(
            const RowsetSharedPtr& rs,
            std::unordered_map<int32_t, VariantExtendedInfo>* uid_to_variant_extended_info);

    // collect path stats from the variant column
    static Status aggregate_path_to_stats(
            const RowsetSharedPtr& rs,
            std::unordered_map<int32_t, PathToNoneNullValues>* uid_to_path_stats);

    // Build the temporary schema for compaction, this will reduce the memory usage of compacting variant columns
    static Status get_extended_compaction_schema(const std::vector<RowsetSharedPtr>& rowsets,
                                                 TabletSchemaSPtr& target);

    // Used to collect all the subcolumns types of variant column from rowsets
    static TabletSchemaSPtr calculate_variant_extended_schema(
            const std::vector<RowsetSharedPtr>& rowsets, const TabletSchemaSPtr& base_schema);

    // Check if the path stats are consistent between inputs rowsets and output rowset.
    // Used to check the correctness of compaction.
    static Status check_path_stats(const std::vector<RowsetSharedPtr>& intputs,
                                   RowsetSharedPtr output, BaseTabletSPtr tablet);

    // Calculate statistics about variant data paths from the encoded sparse column
    static void calculate_variant_stats(const IColumn& encoded_sparse_column,
                                        segment_v2::VariantStatisticsPB* stats, size_t row_pos,
                                        size_t num_rows);

    static void get_compaction_subcolumns_from_subpaths(
            TabletSchema::PathsSetInfo& paths_set_info, const TabletColumnPtr parent_column,
            const TabletSchemaSPtr& target, const PathToDataTypes& path_to_data_types,
            const std::unordered_set<std::string>& sparse_paths, TabletSchemaSPtr& output_schema);

    static void get_compaction_subcolumns_from_data_types(
            TabletSchema::PathsSetInfo& paths_set_info, const TabletColumnPtr parent_column,
            const TabletSchemaSPtr& target, const PathToDataTypes& path_to_data_types,
            TabletSchemaSPtr& output_schema);

    static Status get_compaction_typed_columns(const TabletSchemaSPtr& target,
                                               const std::unordered_set<std::string>& typed_paths,
                                               const TabletColumnPtr parent_column,
                                               TabletSchemaSPtr& output_schema,
                                               TabletSchema::PathsSetInfo& paths_set_info);

    static Status get_compaction_nested_columns(
            const std::unordered_set<vectorized::PathInData, vectorized::PathInData::Hash>&
                    nested_paths,
            const PathToDataTypes& path_to_data_types, const TabletColumnPtr parent_column,
            TabletSchemaSPtr& output_schema, TabletSchema::PathsSetInfo& paths_set_info);
};

} // namespace  doris::vectorized::schema_util
