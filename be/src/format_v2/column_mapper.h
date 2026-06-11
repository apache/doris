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

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type.h"
#include "exprs/vexpr_fwd.h"
#include "format_v2/expr/literal.h"
#include "format_v2/file_reader.h"

namespace doris {
class ColumnPredicate;
class RuntimeState;
} // namespace doris

namespace doris::format {

struct ColumnDefinition;
struct TableFilter;

// Table-level simple predicates grouped by table/global output position. The key is not
// LocalColumnId: TableColumnMapper resolves it through ColumnMapping before creating file pruning
// hints.
using TableColumnPredicates = std::map<GlobalIndex, std::vector<std::shared_ptr<ColumnPredicate>>>;

enum class TableColumnMappingMode {
    // Match by ColumnDefinition::identifier TYPE_INT as field id.
    BY_FIELD_ID,
    // Match by ColumnDefinition::identifier TYPE_STRING, or logical name when identifier is null.
    BY_NAME,
    // Match top-level columns by file position. This mainly serves Hive1 ORC style files whose
    // column names are placeholder values such as `_col0` / `_col1`, where position is the only
    // reliable way to select the correct column.
    BY_INDEX,
};

enum TableVirtualColumnType {
    INVALID = 0, // not a virtual column
    ROW_ID = 1,
    LAST_UPDATED_SEQUENCE_NUMBER = 2,
    ICEBERG_ROWID = 3,
};

enum class FilterConversionType {
    COPY_DIRECTLY, // filter can be copied directly from file layer without any change, e.g. column type and table type are the same and no complex nested projection is involved.
    CAST_FILTER, // filter can be converted from file layer by adding a cast, e.g. column type is nullable but table type is not, or file column has a trivial nested projection but table column has a complex nested projection.
    READER_EXPRESSION,
    FINALIZE_ONLY, // filter cannot be converted to file layer and should be evaluated at table reader finalize phase, e.g. a child column of a nested column is null in file schema.
    CONSTANT,
};

// Nested global-to-local child mapping. The root index points either to a request-local slot or to
// a child id, depending on the owner. child_mapping keeps the recursive table-child to file-child
// relationship explicit instead of encoding it in ColumnMapping flags.
struct IndexMapping {
    int32_t index = -1;
    std::map<int32_t, std::shared_ptr<IndexMapping>> child_mapping;
};

// Recursive result produced after one table/global column is assigned to a file-local source.
struct ColumnMapResult {
    std::optional<LocalColumnId> local_column_id;
    std::optional<LocalColumnIndex> column_index;
    std::optional<IndexMapping> mapping;
};

// Final mapping entry from one global result column to one file-local source.
struct ColumnMapEntry {
    IndexMapping mapping;
    DataTypePtr local_type;
    DataTypePtr global_type;
    FilterConversionType filter_conversion = FilterConversionType::FINALIZE_ONLY;
};

// Collection of final result-column mappings produced for one file/split.
struct ResultColumnMapping {
    std::map<GlobalIndex, ColumnMapEntry> global_to_local;
};

// 单个 table column 到 file column 的映射结果。
// 这是 table 层和 file 层的核心边界对象。
struct ColumnMapping {
    // Position of the top-level projected column in the table/global output block. Table-level
    // filters and column predicates refer to this index after FileScannerV2 translates FE ids at
    // the scanner boundary.
    GlobalIndex global_index;
    std::string table_column_name;
    // File-reader local id for the mapped node.
    //
    // For a root mapping it is convertible to LocalColumnId. For a nested mapping it is the
    // LocalColumnIndex child id under the parent projection. This is deliberately separated from
    // ColumnDefinition::identifier, which is the table-to-file matching key such as Parquet/Iceberg
    // field_id or column name.
    //
    // Empty means the table column is constant, missing, partition-only, or virtual.
    std::optional<int32_t> file_local_id;
    std::string file_column_name;
    // Full file type/children before nested projection pruning. Used to rebuild projected types
    // and to localize nested filters that reference children not present in the output projection.
    DataTypePtr original_file_type;
    std::vector<ColumnDefinition> original_file_children;
    // File children after applying the scan projection. The order follows the physical file schema,
    // not table child order. TableReader uses this to map table-output children back to the
    // file-local block layout when projection, predicate-only children, and schema evolution mix.
    std::vector<ColumnDefinition> projected_file_children;
    // Split/file-local constant entry when this mapping is produced from partition/default/virtual
    // expression instead of physical file data.
    std::optional<ConstantIndex> constant_index;
    // Effective file type after applying casts/remaps/nested projection pruning.
    DataTypePtr file_type;
    // Target table/global type after final materialization.
    DataTypePtr table_type;

    // 最终输出表达式。用于把 file-local value 转成 table/global value，例如 cast、
    // default、partition、generated column 或复杂列 remap。
    VExprContextSPtr projection;

    // Mapping tree for nested table children. The order follows table output children, while file
    // children can be pruned/reordered through each child mapping's file-reader local id.
    std::vector<ColumnMapping> child_mappings;
    // True when file value can be used directly as table value without cast or child remap.
    bool is_trivial = false;
    // How filters referencing this table/global column can be converted below table-reader
    // finalize. This is metadata for localize_filters() and future constant-filter evaluation.
    FilterConversionType filter_conversion = FilterConversionType::FINALIZE_ONLY;
    TableVirtualColumnType virtual_column_type = TableVirtualColumnType::INVALID;
    VExprContextSPtr default_expr;

    std::string debug_string() const;
};

struct TableColumnMapperOptions {
    TableColumnMappingMode mode = TableColumnMappingMode::BY_FIELD_ID;
    bool allow_missing_columns = true;

    std::string debug_string() const;
};

Status clone_table_expr_tree(const VExprSPtr& expr, VExprSPtr* cloned_expr);

// 通用 table schema 到 file schema 映射层。
// Iceberg 会使用 BY_FIELD_ID；普通 by-name 场景可以复用该组件，但不应把它命名成
// Iceberg-only 组件。
class TableColumnMapper {
public:
    explicit TableColumnMapper(TableColumnMapperOptions options = {})
            : _options(std::move(options)) {}
    virtual ~TableColumnMapper() = default;

    // 建立 table schema 到 file schema 的列映射。
    // 输出的 ColumnMapping 描述 table column 如何从 file column、常量列或表达式得到；
    // 后续 projection、filter localization 和 table block finalize 都应复用这份映射。
    virtual Status create_mapping(const std::vector<ColumnDefinition>& projected_columns,
                                  const std::map<std::string, Field>& partition_values,
                                  const std::vector<ColumnDefinition>& file_schema);

    // 把 table-level scan 请求转换成 file-local scan 请求。table_filters 保留 row-level
    // 过滤语义并转换成 file-local conjuncts；table_column_predicates 只转换成 file-layer
    // pruning hints，不参与 batch row filtering。
    virtual Status create_scan_request(const std::vector<TableFilter>& table_filters,
                                       const TableColumnPredicates& table_column_predicates,
                                       const std::vector<ColumnDefinition>& projected_columns,
                                       FileScanRequest* file_request,
                                       RuntimeState* runtime_state = nullptr);

    // 将 table-level filter 定位到文件 schema。
    // trivial mapping 可以直接复制结构化谓词；类型变化时可以尝试安全 cast；无法安全
    // 下推的表达式应通过 reader_expression_map 或 table-level finalize/filter fallback 处理。
    virtual Status localize_filters(const std::vector<TableFilter>& table_filters,
                                    const TableColumnPredicates& table_column_predicates,
                                    FileScanRequest* file_request,
                                    RuntimeState* runtime_state = nullptr);
    void clear() {
        _mappings.clear();
        _constant_map.clear();
        _filter_entries.clear();
    }
    const std::vector<ColumnMapping>& mappings() const { return _mappings; }
    const std::map<GlobalIndex, FilterEntry>& filter_entries() const { return _filter_entries; }
    const ConstantMap& constant_map() const { return _constant_map; }
    std::string debug_string() const;

private:
    const ColumnDefinition* _find_file_field(
            const ColumnDefinition& table_column,
            const std::vector<ColumnDefinition>& file_schema) const;
    Status _create_direct_mapping(const ColumnDefinition& table_column,
                                  const ColumnDefinition& file_field, ColumnMapping* mapping) const;

    Status _create_by_index_mapping(const ColumnDefinition& table_column,
                                    const std::vector<ColumnDefinition>& file_schema,
                                    ColumnMapping* mapping);
    Status _build_filter_entries(const FileScanRequest& file_request);
    Status _build_result_column_mapping(const FileScanRequest& file_request);

    void _set_constant_mapping(ColumnMapping* mapping, VExprContextSPtr expr);

    ColumnMapping* _find_mapping(GlobalIndex global_index);

    TableColumnMapperOptions _options;
    // Column mapping for each projected column, in the same order as projected_columns. Each entry describes how to get one table/global column from file-local sources, and carries metadata for filter localization and result finalize.
    std::vector<ColumnMapping> _mappings;
    std::map<GlobalIndex, FilterEntry> _filter_entries;
    ConstantMap _constant_map;
};

} // namespace doris::format
