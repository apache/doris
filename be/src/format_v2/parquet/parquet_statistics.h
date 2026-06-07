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
#include "format_v2/parquet/selection_vector.h"
#include "format_v2/file_reader.h"

namespace parquet {
class BloomFilter;
class FileMetaData;
class ParquetFileReader;
class RowGroupMetaData;
class Statistics;
} // namespace parquet

namespace doris {
class ColumnPredicate;
} // namespace doris

namespace doris::parquet {

struct ParquetColumnSchema;

enum class ParquetRowGroupPruneReason {
    NONE,
    STATISTICS,
    DICTIONARY,
    BLOOM_FILTER,
};

struct ParquetPruningStats {
    int64_t total_row_groups = 0;
    int64_t selected_row_groups = 0;
    int64_t filtered_row_groups_by_statistics = 0;
    int64_t filtered_row_groups_by_dictionary = 0;
    int64_t filtered_row_groups_by_bloom_filter = 0;
    int64_t filtered_row_groups_by_page_index = 0;
    int64_t filtered_group_rows = 0;
    int64_t filtered_page_rows = 0;
    int64_t selected_row_ranges = 0;
    int64_t page_index_read_calls = 0;
    int64_t bloom_filter_read_time = 0;
};

// Parquet row group column statistics 转换后的 Doris 统计视图。
// DuckDB 会把 Parquet stats 转换成 BaseStatistics，然后让 TableFilter 自己判断；
// Doris 新 reader 先保存 file-local min/max/null 信息，再交给 ColumnPredicate 判断。
struct ParquetColumnStatistics {
    Field min_value;
    Field max_value;
    bool has_null = false;
    bool has_not_null = false;
    bool has_null_count = false;
    bool has_min_max = false;

    bool has_any_statistics() const { return has_null_count || has_min_max; }
};

// Parquet file-local statistics/page index/bloom filter 工具类。
// 结构参考 DuckDB ParquetStatisticsUtils：先把 Parquet metadata 转成统一统计对象，
// 再由 filter/predicate 判断是否可以裁剪。这里不理解 table/global schema。
struct ParquetStatisticsUtils {
    static const ParquetColumnSchema* ResolvePredicateLeafSchema(
            const std::vector<std::unique_ptr<ParquetColumnSchema>>& schema,
            const format::FileColumnPredicateFilter& column_filter);

    static ParquetColumnStatistics TransformColumnStatistics(
            const ParquetColumnSchema& column_schema,
            const std::shared_ptr<::parquet::Statistics>& statistics);

    // Return true if the statistics indicate that the row group can be safely skipped according to
    // the local single-column predicate filter.
    static bool CheckStatistics(const format::FileColumnPredicateFilter& column_filter,
                                const ParquetColumnStatistics& statistics);

    static ParquetRowGroupPruneReason RowGroupPruneReason(
            const ::parquet::RowGroupMetaData& row_group, ::parquet::ParquetFileReader* file_reader,
            int row_group_idx, const std::vector<std::unique_ptr<ParquetColumnSchema>>& schema,
            const format::FileColumnPredicateFilter& column_filter);

    static bool RowGroupExcludes(const ::parquet::RowGroupMetaData& row_group,
                                 ::parquet::ParquetFileReader* file_reader, int row_group_idx,
                                 const std::vector<std::unique_ptr<ParquetColumnSchema>>& schema,
                                 const format::FileColumnPredicateFilter& column_filter);

    static Status SelectRowGroups(
            const ::parquet::FileMetaData& metadata, ::parquet::ParquetFileReader* file_reader,
            const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
            const format::FileScanRequest& request, std::vector<int>* selected_row_groups,
            bool enable_bloom_filter, ParquetPruningStats* pruning_stats);

    static bool BloomFilterSupported(const ParquetColumnSchema& column_schema);

    static bool BloomFilterExcludes(const ParquetColumnSchema& column_schema,
                                    const format::FileColumnPredicateFilter& column_filter,
                                    const ::parquet::BloomFilter& bloom_filter);
};

// Parquet file-local statistics/page index/bloom filter 裁剪入口。
// 这里只消费已经 localize 到 file schema 的 FileScanRequest，不理解 table/global schema。
// 后续 page index、dictionary、bloom filter 等文件格式优化也应继续收敛在这一层，避免污染
// ParquetReader 的 scan 调度代码。
Status select_row_groups_by_statistics(
        const ::parquet::FileMetaData& metadata, ::parquet::ParquetFileReader* file_reader,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const format::FileScanRequest& request, std::vector<int>* selected_row_groups,
        bool enable_bloom_filter, ParquetPruningStats* pruning_stats);

Status select_row_group_ranges_by_page_index(
        ::parquet::ParquetFileReader* file_reader,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const format::FileScanRequest& request, int row_group_idx, int64_t row_group_rows,
        std::vector<RowRange>* selected_ranges, std::map<int, ParquetPageSkipPlan>* page_skip_plans,
        ParquetPruningStats* pruning_stats);

} // namespace doris::parquet
