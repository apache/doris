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

#include <cstdint>
#include <map>
#include <memory>
#include <vector>

#include "common/status.h"
#include "core/field.h"
#include "format_v2/file_reader.h"
#include "format_v2/parquet/selection_vector.h"

namespace parquet {
class BloomFilter;
class FileMetaData;
class ParquetFileReader;
class Statistics;
} // namespace parquet

namespace cctz {
class time_zone;
} // namespace cctz

namespace doris {
class ColumnPredicate;
} // namespace doris

namespace doris::format::parquet {

struct ParquetColumnSchema;

// ============================================================================
// 裁剪统计信息
// ============================================================================

// RowGroup/Page 裁剪过程中的统计计数。
// 由 plan_parquet_row_groups() 填充，最终汇总到 ParquetProfile 的 RuntimeProfile。
struct ParquetPruningStats {
    int64_t total_row_groups = 0;                    // 文件中 RG 总数
    int64_t selected_row_groups = 0;                 // 裁剪后选中的 RG 数
    int64_t filtered_row_groups_by_statistics = 0;   // 被 min/max statistics 裁剪的 RG 数
    int64_t filtered_row_groups_by_dictionary = 0;   // 被 dictionary 裁剪的 RG 数
    int64_t filtered_row_groups_by_bloom_filter = 0; // 被 bloom filter 裁剪的 RG 数
    int64_t filtered_row_groups_by_page_index = 0;   // 被 page index 完全裁剪的 RG 数
    int64_t filtered_group_rows = 0;                 // 被裁剪的 RG 的总行数
    int64_t filtered_page_rows = 0;                  // 被 page index 裁剪的行数
    int64_t selected_row_ranges = 0;                 // 选中的行范围数
    int64_t page_index_read_calls = 0;               // Page Index 读取次数
    int64_t bloom_filter_read_time = 0;              // Bloom filter 读取耗时 (ns)
    int64_t row_group_filter_time = 0;               // RG 级裁剪耗时 (ns)
    int64_t page_index_filter_time = 0;              // Page index 裁剪耗时 (ns)
    int64_t read_page_index_time = 0;                // Page index 读取耗时 (ns)
};

// Parquet ColumnChunk statistics 转换后的 Doris 统计视图。
// 将 Arrow 的 min/max 物理值按 type_descriptor 转换为 Doris Field，
// 供 ColumnPredicate::evaluate_and() 判断是否可以裁剪。
struct ParquetColumnStatistics {
    Field min_value;             // 列最小值（已转换为 Doris 类型）
    Field max_value;             // 列最大值
    bool has_null = false;       // 是否包含 NULL
    bool has_not_null = false;   // 是否包含非 NULL 值
    bool has_null_count = false; // null_count 字段是否有效
    bool has_min_max = false;    // min/max 字段是否有效（转换成功）

    bool has_any_statistics() const { return has_null_count || has_min_max; }
};

// ============================================================================
// Parquet 文件级裁剪工具
// ============================================================================
//
// 裁剪逻辑分层：
//   ① select_row_groups_by_statistics()  — RG 级：min/max + dictionary + bloom filter
//   ② select_row_group_ranges_by_page_index() — Page 级：ColumnIndex 细粒度 range 裁剪 + skip plan
//
// 这些函数只消费已 localize 到 file schema 的 FileScanRequest，
// 不理解 table/global schema。所有裁剪发生在 plan_parquet_row_groups() 阶段。
//
// 内部实现（.cpp 中）：
//   row_group_prune_reason() 是统一的 RG 级裁剪入口，依次检查：
//     statistics(TransformColumnStatistics + check_statistics)
//     → dictionary(read_dictionary_words + predicate::evaluate_and)
//     → bloom filter(bloom_filter_prune_reason)
//   返回第一个命中裁剪的原因。
// ============================================================================
struct ParquetStatisticsUtils {
    // 将 Arrow Parquet Statistics 转换为 Doris 的 ParquetColumnStatistics。
    // 如果 min/max 是 Parquet 物理值（如 INT32 存的 decimal、INT96 的 timestamp），
    // 会按 column_schema.type_descriptor 转换为 Doris 逻辑值。
    static ParquetColumnStatistics TransformColumnStatistics(
            const ParquetColumnSchema& column_schema,
            const std::shared_ptr<::parquet::Statistics>& statistics,
            const cctz::time_zone* timezone = nullptr);

    // 检查 bloom filter 是否排除该列的所有 predicate 值。
    // 通过 ArrowParquetBloomFilterAdapter 将 Arrow BloomFilter 适配为 Doris BloomFilter 接口。
    static bool BloomFilterExcludes(const ParquetColumnSchema& column_schema,
                                    const format::FileColumnPredicateFilter& column_filter,
                                    const ::parquet::BloomFilter& bloom_filter);
};

// ① RG 级裁剪：对 candidate_row_groups 中的每个 RG，调用 row_group_prune_reason()
// 依次检查 statistics → dictionary → bloom filter。
// candidate_row_groups 为 nullptr 时遍历所有 RG。
Status select_row_groups_by_statistics(
        const ::parquet::FileMetaData& metadata, ::parquet::ParquetFileReader* file_reader,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const format::FileScanRequest& request, const std::vector<int>* candidate_row_groups,
        std::vector<int>* selected_row_groups, bool enable_bloom_filter,
        ParquetPruningStats* pruning_stats, const cctz::time_zone* timezone = nullptr);

// ② Page 级裁剪：对指定 RG，通过 ColumnIndex (page statistics) 和 OffsetIndex
// 对每个 column_predicate_filter 逐 page 裁剪，生成 selected_ranges（该 RG 内需读取的行范围）
// 和 page_skip_plans（完全跳过哪些 data page，供 Arrow RecordReader 在物理读取层跳过）。
//
// 所有 column_filter 的 range 取交集（intersect_ranges），
// 交集为空则该 RG 被完全裁剪。
Status select_row_group_ranges_by_page_index(
        ::parquet::ParquetFileReader* file_reader,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const format::FileScanRequest& request, int row_group_idx, int64_t row_group_rows,
        std::vector<RowRange>* selected_ranges, std::map<int, ParquetPageSkipPlan>* page_skip_plans,
        ParquetPruningStats* pruning_stats, const cctz::time_zone* timezone = nullptr);

} // namespace doris::format::parquet
