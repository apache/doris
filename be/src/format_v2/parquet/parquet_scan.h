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
#include <vector>

#include "common/status.h"
#include "core/column/column.h"
#include "format_v2/file_reader.h"
#include "format_v2/parquet/parquet_profile.h"
#include "format_v2/parquet/parquet_statistics.h"
#include "format_v2/parquet/reader/column_reader.h"
#include "format_v2/parquet/selection_vector.h"
#include "runtime/runtime_profile.h"
#include "storage/segment/condition_cache.h"

namespace parquet {
class FileMetaData;
class ParquetFileReader;
class RowGroupReader;
} // namespace parquet

namespace cctz {
class time_zone;
} // namespace cctz

namespace doris {
class Block;

namespace format {
struct FileScanRequest;
} // namespace format
} // namespace doris

namespace doris::format::parquet {

struct ParquetFileContext;
struct ParquetColumnSchema;

// ============================================================================
// 扫描范围 & 裁剪计划
// ============================================================================

// 文件扫描范围，来自 FileDescription 的 range_start_offset/range_size/file_size。
// 用于判断 RowGroup 是否落在当前 split 负责的 offset 区间内。
struct ParquetScanRange {
    int64_t start_offset = 0;
    int64_t size = -1;      // -1 表示读整个文件
    int64_t file_size = -1; // -1 表示未知
};

// 单个 RowGroup 的读取计划，由 plan_parquet_row_groups() 生成。
struct RowGroupReadPlan {
    int row_group_id = -1;                 // RG 编号
    int64_t first_file_row = 0;            // 该 RG 在文件中的起始行号（从 0 累加）
    int64_t row_group_rows = 0;            // 该 RG 的总行数
    std::vector<RowRange> selected_ranges; // page index 裁剪后需读取的行范围
    std::map<int, ParquetPageSkipPlan> page_skip_plans; // leaf_column_id → 可完全跳过的 data page
};

// 整个文件的扫描计划 — plan_parquet_row_groups() 的输出。
struct RowGroupScanPlan {
    std::vector<RowGroupReadPlan> row_groups; // 裁剪后需要扫描的 RowGroup 列表
    ParquetPruningStats pruning_stats;        // 裁剪统计
};

// ============================================================================
// RowGroup 裁剪 & 调度
// ============================================================================

// 最外层裁剪入口：从文件的所有 RowGroup 中选出需要扫描的 RG 及其内部行范围。
//
// 裁剪流水线（代价从低到高）：
//   1. 按 scan_range 过滤（O(1) offset 算术）→ 只在当前 split offset 范围内的 RG 进入下一步
//   2. select_row_groups_by_statistics() → statistics/dictionary/bloom filter 裁剪
//   3. select_row_group_ranges_by_page_index() → 逐 page 细粒度裁剪，生成 selected_ranges
//
// 输出 RowGroupScanPlan，供 ParquetScanScheduler 逐批调度读取。
Status plan_parquet_row_groups(const ::parquet::FileMetaData& metadata,
                               ::parquet::ParquetFileReader* file_reader,
                               const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
                               const format::FileScanRequest& request,
                               const ParquetScanRange& scan_range, bool enable_bloom_filter,
                               RowGroupScanPlan* plan, const cctz::time_zone* timezone = nullptr);

// 将 SelectionVector 转换为 IColumn::Filter（Doris 的 bitmap 格式）。
// 用于在 late materialization 中对已读取的 predicate 列做行过滤。
IColumn::Filter selection_to_filter(const SelectionVector& selection, uint16_t selected_rows,
                                    int64_t batch_rows);

// 执行批次级的 conjuncts + delete_conjuncts 过滤。
// 对 predicate 列的全量 batch 执行表达式，生成 SelectionVector 标记命中行。
Status execute_batch_filters(const format::FileScanRequest& request, int64_t batch_rows,
                             Block* file_block, SelectionVector* selection, uint16_t* selected_rows,
                             int64_t* conjunct_filtered_rows = nullptr);

// ============================================================================
// Parquet 扫描调度器
// ============================================================================
//
// 持有 RowGroupScanPlan，在 get_block() 被调用时逐批推进扫描。
//
// 核心循环（read_next_batch）：
//   while true:
//     ① open_next_row_group()     — 打开下一个 RG，创建 predicate/non-predicate ColumnReaders
//     ② skip range gap            — 跳过被 page index 裁剪掉的行范围
//     ③ read_current_row_group_batch(batch_rows)
//          ├─ read_filter_columns()  ：读 predicate 列 → 执行 conjuncts → SelectionVector
//          ├─ filter predicate columns：按 selection 过滤已读的 predicate 列
//          └─ read non-predicate      ：select() 只读命中的 non-predicate 列
//     ④ 返回 batch，或继续循环（当前 batch 全被 filter 掉）
//
// 设计要点：
// - Late materialization：predicate 列先读（全量 4096 行），生成 selection；
//   non-predicate 列按 selection 的 select() 只读命中行，大幅减少 I/O。
// - RowGroup 级别的 ColumnReader 工厂在 open_next_row_group 中创建，
//   每个 RG 创建一套新的 reader，reader 生命周期绑定到当前 RG。
// ============================================================================
class ParquetScanScheduler {
public:
    void set_plan(RowGroupScanPlan plan);
    void set_page_skip_profile(ParquetPageSkipProfile page_skip_profile) {
        _page_skip_profile = page_skip_profile;
    }
    void set_scan_profile(ParquetScanProfile scan_profile) { _scan_profile = scan_profile; }
    void set_global_rowid_context(std::optional<format::GlobalRowIdContext> context) {
        _global_rowid_context = context;
    }
    void set_condition_cache_context(std::shared_ptr<ConditionCacheContext> ctx);
    void set_timezone(const cctz::time_zone* timezone) { _timezone = timezone; }
    void set_enable_strict_mode(bool enable_strict_mode) {
        _enable_strict_mode = enable_strict_mode;
    }
    void reset();
    bool empty() const { return _row_group_plans.empty(); }
    int64_t condition_cache_filtered_rows() const { return _condition_cache_filtered_rows; }
    int64_t predicate_filtered_rows() const { return _predicate_filtered_rows; }

    Status read_next_batch(ParquetFileContext& file_context,
                           const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
                           const format::FileScanRequest& request, Block* file_block, size_t* rows,
                           bool* eof);

private:
    void reset_current_row_group();

    // 打开下一个 RG，创建 ParquetColumnReaderFactory 并创建 predicate/non-predicate readers。
    Status open_next_row_group(ParquetFileContext& file_context,
                               const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
                               const format::FileScanRequest& request, bool* has_row_group);

    // 跳过当前 RG 内 rows 行（range gap），对所有 reader 调用 skip()。
    Status skip_current_row_group_rows(int64_t rows);

    // 读取 predicate 列并执行 conjuncts + delete_conjuncts，生成 SelectionVector。
    Status read_filter_columns(int64_t batch_rows, const format::FileScanRequest& request,
                               Block* file_block, SelectionVector* selection,
                               uint16_t* selected_rows, int64_t* conjunct_filtered_rows);

    // 单 batch 的完整读取：predicate 列 → filter → non-predicate 列（select 模式）。
    Status read_current_row_group_batch(int64_t batch_rows, const format::FileScanRequest& request,
                                        int64_t batch_first_file_row, Block* file_block,
                                        size_t* rows);

    void mark_condition_cache_granules(const SelectionVector& selection, uint16_t selected_rows,
                                       int64_t batch_first_file_row);

    // ======== 计划状态 ========
    std::vector<RowGroupReadPlan> _row_group_plans; // 待扫描的 RG 队列
    size_t _next_row_group_plan_idx = 0;            // 下一个待处理的 RG 下标

    // ======== 当前 RG 状态 ========
    std::shared_ptr<::parquet::RowGroupReader> _current_row_group; // Arrow RowGroup 读取器
    std::map<ColumnId, std::unique_ptr<ParquetColumnReader>>
            _current_predicate_columns; // predicate ColumnReaders
    std::map<ColumnId, std::unique_ptr<ParquetColumnReader>>
            _current_non_predicate_columns;   // non-predicate ColumnReaders
    int64_t _current_row_group_rows = 0;      // 当前 RG 总行数
    int64_t _current_row_group_rows_read = 0; // 当前 RG 已读行数（游标）
    int64_t _current_row_group_first_row = 0; // 当前 RG 在文件中的起始行号
    std::vector<RowRange>
            _current_selected_ranges;     // 当前 RG 的 selected_ranges（page index 裁剪后）
    size_t _current_range_idx = 0;        // 当前处理到第几个 selected_range
    int64_t _current_range_rows_read = 0; // 当前 range 已读行数

    // ======== 配置 ========
    ParquetPageSkipProfile _page_skip_profile;
    ParquetScanProfile _scan_profile;
    std::optional<format::GlobalRowIdContext> _global_rowid_context;
    const cctz::time_zone* _timezone = nullptr;
    bool _enable_strict_mode = false;
    std::shared_ptr<ConditionCacheContext> _condition_cache_ctx;
    int64_t _condition_cache_filtered_rows = 0;
    int64_t _predicate_filtered_rows = 0;
};

} // namespace doris::format::parquet
