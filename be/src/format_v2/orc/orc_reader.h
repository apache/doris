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

#include <memory>
#include <optional>
#include <orc/Reader.hh>
#include <set>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/column/column.h"
#include "format_v2/file_reader.h"
#include "runtime/runtime_profile.h"

namespace cctz {
class time_zone;
} // namespace cctz

namespace doris::format::orc {

struct OrcReaderScanState;

// =============================================================================
// format::orc::OrcReader —— 新框架 ORC FileReader
// =============================================================================
//
// 架构层次：
//   TableReader  (上层：表语义、schema evolution、partition、delete)
//      ↓
//   format::FileReader 接口
//      ↓
//   format::orc::OrcReader  (本类：把 ORC C++ 库包成 file-local reader)
//      ↓
//   ORC C++ 库 (Reader / RowReader / SearchArgument)
//
// 设计原则（详见 file_reader.h 注释）：
//   - 文件层只看 file-local concept，不假设表语义
//   - 输出列永远 nullable（让 table 层判断 NOT NULL 约束）
//   - 用 LocalColumnId 而非 SQL slot id；Block 位置通过 local_positions 映射
//
// 生命周期（6 个 override 方法）：
//   new OrcReader → init → get_schema → open(request)
//                → get_block 循环（或 get_aggregate_result，二选一）
//                → close
//
// lazy materialization：
//   ORC lazy : ORC 库内部 LEADERS/FOLLOWERS 模式 + filter callback
//              通过 filter()/filterTypes() 标出 predicate leaders，复杂投影也走
//              filterTypes()；只有当全部 row-level filter 都能在 callback 已解码列上运行时启用
//              收益：ORC 库内部 column reader decode 跳行；
//                    get_block 只对 ORC callback 选中的行 materialize non-predicate 列
//   普通路径 : 先 decode 完请求列，再由 Doris row-level filter 裁 block
//              当前不在 reader 层单独做 predicate-first lazy materialization
//
// 2 类 filter 串行 AND（_build_keep_filter）：
//   1. conjuncts        : file-local SQL VExpr 表达式
//   2. delete_conjuncts : Iceberg/Hudi delete 标记（不转 SARG）
//
// 三层 pruning（粒度从粗到细）：
//   1. Stripe-level   : SARG (ORC 库内)
//   2. Row group-level: ORC 库内部用 SARG + row group statistics（不可见）
//   3. Row-level      : 2 类 filter 行级精确（_build_keep_filter）
//
// 虚拟列（OrcReader 自己生成的列）：
//   __file_row_position : 当前行在文件里的物理行号（BIGINT）
//                         用于 Iceberg position delete
//   __DORIS_GLOBAL_ROWID_COL__ : 全局唯一行号（STRING）
class OrcReader final : public format::FileReader {
public:
    OrcReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
              std::unique_ptr<io::FileDescription>& file_description,
              std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
              std::optional<format::GlobalRowIdContext> global_rowid_context = std::nullopt);
    ~OrcReader() override;

    static format::ColumnDefinition row_position_column_definition();

    // 6 个生命周期方法（实现 format::FileReader 接口）
    Status init(RuntimeState* state) override; // 打开 ORC 文件，建 ORC Reader 对象
    Status get_schema(std::vector<format::ColumnDefinition>* const file_schema) const override;
    std::unique_ptr<format::TableColumnMapper> create_column_mapper(
            format::TableColumnMapperOptions options) const override;
    Status open(std::shared_ptr<format::FileScanRequest> request) override; // 配置投影/SARG/pruning
    Status get_block(Block* file_block, size_t* rows, bool* eof) override; // 读一批
    Status get_aggregate_result(const format::FileAggregateRequest& request,
                                format::FileAggregateResult* result) override;
    Status close() override;

private:
    // RuntimeProfile counters (跟 Parquet 命名对齐)
    // _init_profile 在 init() 时由基类调用注册；_collect_profile 在 close() 时统一推数据
    struct OrcProfile {
        RuntimeProfile::Counter* reader_call = nullptr;                 // ReaderCall
        RuntimeProfile::Counter* reader_inclusive_latency_us = nullptr; // ReaderInclusiveLatencyUs
        RuntimeProfile::Counter* decompression_call = nullptr;          // DecompressionCall
        RuntimeProfile::Counter* decompression_latency_us = nullptr;    // DecompressionLatencyUs
        RuntimeProfile::Counter* decoding_call = nullptr;               // DecodingCall
        RuntimeProfile::Counter* decoding_latency_us = nullptr;         // DecodingLatencyUs
        RuntimeProfile::Counter* byte_decoding_call = nullptr;          // ByteDecodingCall
        RuntimeProfile::Counter* byte_decoding_latency_us = nullptr;    // ByteDecodingLatencyUs
        RuntimeProfile::Counter* io_count = nullptr;                    // IOCount
        RuntimeProfile::Counter* io_blocking_latency_us = nullptr;      // IOBlockingLatencyUs
        RuntimeProfile::Counter* selected_row_group_count = nullptr;    // SelectedRowGroupCount
        RuntimeProfile::Counter* evaluated_row_group_count = nullptr;   // EvaluatedRowGroupCount
        RuntimeProfile::Counter* read_row_count = nullptr;              // ReadRowCount
        RuntimeProfile::Counter* filtered_row_groups = nullptr;         // RowGroupsFiltered
        RuntimeProfile::Counter* filtered_row_groups_by_min_max = nullptr;
        RuntimeProfile::Counter* filtered_group_rows = nullptr; // FilteredRowsByGroup
        RuntimeProfile::Counter* filtered_bytes = nullptr;
        RuntimeProfile::Counter* read_row_groups = nullptr; // RowGroupsReadNum
        RuntimeProfile::Counter* lazy_read_filtered_rows = nullptr;
        RuntimeProfile::Counter* orc_lazy_read_filtered_rows = nullptr;
        RuntimeProfile::Counter* open_file_num = nullptr;
    };

    // ORC 库 filter callback 的实现类（cpp 中定义）。
    // ORC 库要的是 ::orc::ORCFilter 接口，本类用 PIMPL 风格转发到 OrcReader::_filter_orc_batch。
    class OrcFilterImpl;

    void _init_profile() override;
    void _collect_profile() const;

    // ============ Schema 转换：ORC type → Doris ColumnDefinition ============
    // _convert_to_doris_type   单个 ORC type 转 Doris DataType（永远 nullable）
    // _fill_schema_field       递归填一个 ColumnDefinition（含 Iceberg.id 透传）
    // _fill_*_schema_children  STRUCT / LIST / MAP 的 children 递归
    DataTypePtr _convert_to_doris_type(const ::orc::Type& type) const;
    DataTypePtr _convert_list_to_doris_type(const ::orc::Type& type) const;
    DataTypePtr _convert_map_to_doris_type(const ::orc::Type& type) const;
    DataTypePtr _convert_struct_to_doris_type(const ::orc::Type& type) const;
    Status _fill_schema_field(const ::orc::Type& type, int32_t local_id,
                              const std::string& field_name,
                              format::ColumnDefinition* const field) const;
    Status _fill_struct_schema_children(const ::orc::Type& type,
                                        format::ColumnDefinition* const field) const;
    Status _fill_list_schema_children(const ::orc::Type& type,
                                      format::ColumnDefinition* const field) const;
    Status _fill_map_schema_children(const ::orc::Type& type,
                                     format::ColumnDefinition* const field) const;

    // ============ open() 阶段配置 ============
    // _configure_row_reader_projection  简单 include vs 复杂 includeTypes
    // _can_apply_orc_lazy_callback      判断本次请求能否在 ORC lazy callback 内完整过滤
    // _init_search_argument_from_local_filters  file-local conjuncts → ORC SARG
    // _select_stripe_ranges_by_statistics   SARG stripe pruning
    // _apply_current_stripe_range / _advance_to_next_stripe_range
    //                                  stripe pruning 后剩下的可能是不连续区间，要分段读
    // _create_row_reader               实际创建 ORC RowReader（含 lazy callback 注入）
    Status _configure_row_reader_projection();
    bool _can_apply_orc_lazy_callback() const;
    Status _init_search_argument_from_local_filters();
    Status _select_stripe_ranges_by_statistics();
    void _apply_current_stripe_range();
    Status _advance_to_next_stripe_range(bool* advanced);
    Status _create_row_reader();

    // ORC 库 LEADERS phase 完成后回调本函数。
    // 在这里 decode predicate 列、跑 row-level filter、写 sel[] 给 FOLLOWERS phase 用。
    Status _filter_orc_batch(::orc::ColumnVectorBatch& data, uint16_t* sel, uint16_t size,
                             void* arg);

    // ============ Decode 子系统 ============
    // _decode_column 主分发器，按 ORC TypeKind 走对应分支。
    // 标量列优先走 DataTypeSerde::read_column_from_decoded_values；
    // timestamp 仍保留 ORC 专有时区语义路径，复杂列递归调 _decode_column。
    // 所有 decode 路径都接受 selected_rows 做 selection-aware decode：
    //   selected_rows == nullptr → 全行 decode
    //   selected_rows != nullptr → ORC lazy callback 只要求 decode 命中的行
    Status _decode_column(const ::orc::Type& file_type, const ::orc::Type& selected_type,
                          const ::orc::ColumnVectorBatch& batch, MutableColumnPtr& column,
                          size_t rows, const std::vector<size_t>* selected_rows = nullptr) const;
    Status _decode_timestamp_column(const ::orc::ColumnVectorBatch& batch,
                                    const cctz::time_zone& timezone,
                                    MutableColumnPtr& nested_column, size_t rows,
                                    const std::vector<size_t>* selected_rows) const;
    Status _decode_list_column(const ::orc::Type& file_type, const ::orc::Type& selected_type,
                               const ::orc::ColumnVectorBatch& batch,
                               MutableColumnPtr& nested_column, size_t rows,
                               const std::vector<size_t>* selected_rows) const;
    Status _decode_map_column(const ::orc::Type& file_type, const ::orc::Type& selected_type,
                              const ::orc::ColumnVectorBatch& batch,
                              MutableColumnPtr& nested_column, size_t rows,
                              const std::vector<size_t>* selected_rows) const;
    Status _decode_struct_column(const ::orc::Type& file_type, const ::orc::Type& selected_type,
                                 const ::orc::ColumnVectorBatch& batch,
                                 MutableColumnPtr& nested_column, size_t rows,
                                 const std::vector<size_t>* selected_rows) const;
    Status _decode_column_into_block(const ::orc::StructVectorBatch& struct_batch,
                                     format::LocalColumnId file_column_id, size_t rows,
                                     Block* file_block,
                                     const std::vector<size_t>* selected_rows = nullptr) const;
    Status _decode_columns(const ::orc::StructVectorBatch& struct_batch,
                           const std::vector<format::LocalColumnIndex>& projections, size_t rows,
                           Block* file_block, std::set<format::LocalColumnId>* decoded_columns,
                           const std::vector<size_t>* selected_rows = nullptr) const;

    // 虚拟列填充：__file_row_position 是 file-local 物理行号，不存在文件里，
    // 由 reader 用 row_reader->getRowNumber() + batch 内偏移计算得到。
    // 主要给 Iceberg position delete 使用（DeletePredicate 引用这一列）。
    void _fill_row_position_column(Block* file_block, size_t rows,
                                   const std::vector<size_t>* selected_rows = nullptr) const;
    Status _fill_global_rowid_column(Block* file_block, size_t rows,
                                     const std::vector<size_t>* selected_rows = nullptr) const;

    // ============ Filter 子系统（2 类 filter 的执行 + ORC lazy 决策）============
    //
    // _can_filter_with_decoded_columns
    //     给定一组已 decode 的列，能不能跑所有 row-level filter？
    //     仅用于判断 ORC lazy callback 能否安全运行全部 filter。
    //     row-level filter 引用的所有列都已 decode → callback 可用
    // _filter_has_row_level_predicates
    //     有任何 row-level filter？没有的话直接跳过整套 filter 流程
    //
    // _build_keep_filter
    //     row-level filter 串行 AND 入口，按顺序：
    //       1. _execute_conjuncts         (file-local VExpr)
    //       2. _execute_delete_conjuncts  (Iceberg/Hudi delete)
    //
    // _filter_block / _filter_block_with_keep_filter
    //     旧路径：全列 decode 完后整 block 一次裁齐
    //
    // _filter_decoded_columns
    //     ORC lazy 路径：只对 callback 前已 decode 的列裁；
    //     non-predicate 列已经 selection-aware decode 过，不用再裁
    //
    // _filter_requested_columns
    //     给定 keep_filter，对所有 request 列裁
    bool _can_filter_with_decoded_columns(
            const std::set<format::LocalColumnId>& decoded_columns) const;
    bool _filter_has_row_level_predicates() const;
    Status _build_keep_filter(Block* file_block, size_t rows, IColumn::Filter* keep_filter) const;
    Status _filter_block(Block* file_block, size_t* rows) const;
    Status _execute_conjuncts(Block* file_block, size_t rows, IColumn::Filter* keep_filter) const;
    Status _execute_delete_conjuncts(Block* file_block, size_t rows,
                                     IColumn::Filter* keep_filter) const;
    void _filter_block_with_keep_filter(Block* file_block, const IColumn::Filter& keep_filter,
                                        size_t selected_rows, size_t* rows) const;
    void _filter_decoded_columns(Block* file_block, const IColumn::Filter& keep_filter,
                                 size_t selected_rows,
                                 const std::set<format::LocalColumnId>& decoded_columns) const;
    void _filter_requested_columns(Block* file_block, const IColumn::Filter& keep_filter,
                                   size_t selected_rows) const;

    std::unique_ptr<OrcFilterImpl> _orc_filter; // ORC 库 filter callback (PIMPL)
    std::unique_ptr<OrcReaderScanState> _state; // 所有运行时可变状态都装这里
    OrcProfile _orc_profile;                    // RuntimeProfile counters
    std::optional<format::GlobalRowIdContext> _global_rowid_context;
};

} // namespace doris::format::orc
