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
#include "format/reader/expr/literal.h"
#include "format/reader/file_reader.h"

namespace doris {
class ColumnPredicate;
} // namespace doris

namespace doris::reader {

struct ColumnDefinition;
struct TableFilter;

// Table-level simple predicates grouped by table/global output position. The key is not
// LocalColumnId: TableColumnMapper resolves it through ColumnMapping before creating file pruning
// hints.
using TableColumnPredicates = std::map<GlobalIndex, std::vector<std::shared_ptr<ColumnPredicate>>>;

enum class TableColumnMappingMode {
    // Match by ColumnDefinition::Identifier::FIELD_ID against ColumnDefinition::identifier.field_id.
    BY_FIELD_ID,
    // Match by ColumnDefinition::Identifier::NAME / logical name against ColumnDefinition::name.
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
};

enum class FilterConversionType {
    COPY_DIRECTLY,
    CAST_FILTER,
    READER_EXPRESSION,
    FINALIZE_ONLY,
    CONSTANT,
};

// Nested global-to-local child mapping. The root index points either to a request-local slot or to
// a child id, depending on the owner. child_mapping keeps the recursive table-child to file-child
// relationship explicit instead of encoding it in ColumnMapping flags.
struct IndexMapping {
    int32_t index = -1;
    std::map<int32_t, std::shared_ptr<IndexMapping>> child_mapping;
};

// Recursive result produced while matching one table/global column against one file-local schema.
//
// This mirrors DuckDB's split between temporary recursive mapping and final output mapping, but
// uses Doris column/index types. It should absorb construction-only state currently stored in
// ColumnMapping, such as original file children and default/projection expressions.
struct ColumnMapResult {
    std::optional<LocalColumnId> local_column_id;
    std::optional<LocalColumnIndex> column_index;
    std::optional<IndexMapping> mapping;
    VExprContextSPtr projection;
    VExprContextSPtr default_expr;
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
    std::string error;

    bool has_error() const { return !error.empty(); }
};

// 单个 table column 到 file column 的映射结果。
// 这是 table 层和 file 层的核心边界对象。
struct ColumnMapping {
    // Position of the top-level projected column in the table/global output block. Table-level
    // filters and column predicates refer to this index after FileScannerV2 translates FE ids at
    // the scanner boundary.
    GlobalIndex global_index;
    std::string table_column_name;
    // File-local field id for the mapped node. For a root mapping it is convertible to
    // LocalColumnId; for a nested child mapping it is the child id under its parent projection.
    // Empty means the table column is constant, missing, partition-only, or virtual.
    std::optional<int32_t> field_id;
    std::string file_column_name;
    // Full file type/children before nested projection pruning. Used to rebuild projected types
    // and to localize nested filters that reference children not present in the output projection.
    DataTypePtr original_file_type;
    std::vector<ColumnDefinition> original_file_children;
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
    // children can be pruned/reordered through each child mapping's field_id.
    std::vector<ColumnMapping> child_mappings;
    // True when file value can be used directly as table value without cast or child remap.
    bool is_trivial = false;
    // True when the nested value read from file has a pruned/remapped child layout and must be
    // reconstructed before returning to table/global schema.
    bool has_complex_projection = false;
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
    bool enable_reader_expression_fallback = true;

    std::string debug_string() const;
};

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
                                       FileScanRequest* file_request);

    // 将 table-level filter 定位到文件 schema。
    // trivial mapping 可以直接复制结构化谓词；类型变化时可以尝试安全 cast；无法安全
    // 下推的表达式应通过 reader_expression_map 或 table-level finalize/filter fallback 处理。
    virtual Status localize_filters(const std::vector<TableFilter>& table_filters,
                                    const TableColumnPredicates& table_column_predicates,
                                    FileScanRequest* file_request);
    void clear() {
        _mappings.clear();
        _constant_map.clear();
        _filter_entries.clear();
        _column_map_results.clear();
        _result_mapping = {};
    }
    const std::vector<ColumnMapping>& mappings() const { return _mappings; }
    const std::map<GlobalIndex, FilterEntry>& filter_entries() const { return _filter_entries; }
    const std::map<GlobalIndex, ColumnMapResult>& column_map_results() const {
        return _column_map_results;
    }
    const ResultColumnMapping& result_mapping() const { return _result_mapping; }
    const ConstantMap& constant_map() const { return _constant_map; }
    std::string debug_string() const;

    static std::string debug_string(const ColumnDefinition& column);
    static std::string debug_string(const LocalColumnIndex& projection);
    static std::string debug_string(const FileColumnPredicateFilter& filter);
    static std::string debug_string(const FileScanRequest& request);

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
    const ColumnMapping* _find_mapping(GlobalIndex global_index) const;

    bool _is_same_type(const DataTypePtr& table_type, const DataTypePtr& file_type) const {
        DORIS_CHECK(table_type != nullptr);
        DORIS_CHECK(file_type != nullptr);
        return table_type->equals(*file_type);
    }

    TableColumnMapperOptions _options;
    std::vector<ColumnMapping> _mappings;
    std::map<GlobalIndex, FilterEntry> _filter_entries;
    std::map<GlobalIndex, ColumnMapResult> _column_map_results;
    ResultColumnMapping _result_mapping;
    ConstantMap _constant_map;
};

} // namespace doris::reader
