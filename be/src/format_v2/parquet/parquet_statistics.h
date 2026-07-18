// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <gen_cpp/parquet_types.h>

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "core/field.h"
#include "core/string_ref.h"
#include "exprs/vexpr_fwd.h"
#include "format_v2/file_reader.h"
#include "format_v2/parquet/selection_vector.h"

namespace parquet {
class BloomFilter;
class ColumnIndex;
class FileMetaData;
class ParquetFileReader;
class Statistics;
} // namespace parquet

namespace cctz {
class time_zone;
} // namespace cctz

namespace doris {
class RuntimeState;
namespace segment_v2 {
struct ZoneMap;
} // namespace segment_v2
} // namespace doris

namespace doris::format::parquet {

struct ParquetColumnSchema;
struct ParquetFileContext;
struct ParquetTypeDescriptor;

namespace detail {
Status validate_native_bloom_filter_layout(int64_t offset, uint32_t header_size,
                                           int64_t payload_size, int64_t declared_length,
                                           size_t file_size);
bool can_use_native_footer_min_max(const ParquetTypeDescriptor& type_descriptor,
                                   const tparquet::Statistics& statistics);
} // namespace detail

// ============================================================================
// ============================================================================

struct ParquetDictionaryWords {
    std::vector<std::string> values;
    std::vector<StringRef> refs;

    void clear() {
        values.clear();
        refs.clear();
    }

    void build_refs() {
        refs.clear();
        refs.reserve(values.size());
        for (const auto& value : values) {
            refs.emplace_back(value.data(), value.size());
        }
    }
};

// Reads the PLAIN dictionary page for BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY columns and owns copied
// dictionary bytes in `values`. Both row-group pruning and row-level dictionary predicates use this
// helper so they agree on dictionary id -> Doris string value mapping.
bool read_dictionary_words(::parquet::ParquetFileReader* file_reader, int row_group_idx,
                           int leaf_column_id, const ParquetColumnSchema& column_schema,
                           ParquetDictionaryWords* dict_words);

std::vector<Field> dictionary_fields_from_words(const ParquetDictionaryWords& dict_words);

// ============================================================================
// ============================================================================

struct ParquetPruningStats {
    int64_t total_row_groups = 0;                    // total row groups in the file
    int64_t selected_row_groups = 0;                 // row groups selected after pruning
    int64_t filtered_row_groups_by_statistics = 0;   // row groups pruned by ZoneMap statistics
    int64_t filtered_row_groups_by_dictionary = 0;   // row groups pruned by dictionary
    int64_t filtered_row_groups_by_bloom_filter = 0; // row groups pruned by bloom filter
    int64_t filtered_row_groups_by_page_index = 0;   // row groups fully pruned by page index
    int64_t filtered_group_rows = 0;                 // rows in pruned row groups
    int64_t filtered_bytes = 0;                      // requested bytes in pruned row groups
    int64_t filtered_page_rows = 0;                  // rows pruned by page index
    int64_t selected_row_ranges = 0;                 // selected row range count
    int64_t page_index_read_calls = 0;               // Page Index read count
    int64_t bloom_filter_read_time = 0;              // Bloom filter read time (ns)
    int64_t row_group_filter_time = 0;               // row-group pruning time (ns)
    int64_t page_index_filter_time = 0;              // page-index pruning time (ns)
    int64_t read_page_index_time = 0;                // page-index read time (ns)
    int64_t parse_page_index_time = 0;               // lazy page-index materialization time (ns)
    int64_t expr_zonemap_unusable_evals = 0;         // VExpr ZoneMap unusable evaluations
    int64_t in_zonemap_point_check_count = 0;        // VExpr IN ZoneMap point checks
    int64_t in_zonemap_range_only_count = 0;         // VExpr IN ZoneMap range-only checks
};

struct ParquetColumnStatistics {
    Field min_value;             // column minimum value converted to Doris type
    Field max_value;             // column maximum value
    bool has_null = false;       // whether NULL exists
    bool has_not_null = false;   // whether non-NULL values exist
    bool has_null_count = false; // whether null_count is valid
    bool has_min_max = false;    // whether min/max is valid after conversion

    bool has_any_statistics() const { return has_null_count || has_min_max; }
};

struct NativeParquetPageIndex {
    tparquet::ColumnIndex column_index;
    tparquet::OffsetIndex offset_index;
};

bool can_use_parquet_page_index(const format::FileScanRequest& request,
                                const RuntimeState* runtime_state);

// ============================================================================
// ============================================================================
//     VExpr ZoneMap(TransformColumnStatistics + evaluate_zonemap_filter)
//     -> page-index ZoneMap(evaluate_zonemap_filter)
//     dictionary(read_dictionary_words + evaluate_dictionary_filter)
//     -> bloom filter(evaluate_bloom_filter)
// ============================================================================
struct ParquetStatisticsUtils {
    static std::shared_ptr<segment_v2::ZoneMap> MakeZoneMap(
            const ParquetColumnStatistics& statistics);

    static ParquetColumnStatistics TransformColumnStatistics(
            const ParquetColumnSchema& column_schema,
            const std::shared_ptr<::parquet::Statistics>& statistics,
            const cctz::time_zone* timezone = nullptr);

    static ParquetColumnStatistics TransformColumnStatistics(
            const ParquetColumnSchema& column_schema, const tparquet::Statistics* statistics,
            int64_t column_value_count, const cctz::time_zone* timezone = nullptr);

    static bool TransformColumnIndexStatistics(
            const std::shared_ptr<::parquet::ColumnIndex>& column_index,
            const ParquetColumnSchema& column_schema, size_t page_idx,
            ParquetColumnStatistics* page_statistics, const cctz::time_zone* timezone = nullptr);

    static bool BloomFilterExcludes(const ParquetColumnSchema& column_schema, int slot_index,
                                    const VExprContextSPtrs& conjuncts,
                                    const ::parquet::BloomFilter& bloom_filter);
};

Status select_row_groups_by_metadata(
        const ::parquet::FileMetaData& metadata, ::parquet::ParquetFileReader* file_reader,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const format::FileScanRequest& request, const std::vector<int>* candidate_row_groups,
        std::vector<int>* selected_row_groups, bool enable_bloom_filter,
        ParquetPruningStats* pruning_stats, const cctz::time_zone* timezone = nullptr,
        const RuntimeState* runtime_state = nullptr);

Status select_row_groups_by_metadata(
        const tparquet::FileMetaData& metadata,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const format::FileScanRequest& request, const std::vector<int>* candidate_row_groups,
        std::vector<int>* selected_row_groups, bool enable_bloom_filter,
        ParquetPruningStats* pruning_stats, const cctz::time_zone* timezone = nullptr,
        const RuntimeState* runtime_state = nullptr, ParquetFileContext* file_context = nullptr);

Status select_row_group_ranges_by_page_index(
        ::parquet::ParquetFileReader* file_reader,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const format::FileScanRequest& request, int row_group_idx, int64_t row_group_rows,
        std::vector<RowRange>* selected_ranges, std::map<int, ParquetPageSkipPlan>* page_skip_plans,
        ParquetPruningStats* pruning_stats, const cctz::time_zone* timezone = nullptr,
        const RuntimeState* runtime_state = nullptr);

Status select_row_group_ranges_by_native_page_index(
        const std::unordered_map<int, NativeParquetPageIndex>& page_indexes,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const format::FileScanRequest& request, int64_t row_group_rows,
        std::vector<RowRange>* selected_ranges, std::map<int, ParquetPageSkipPlan>* page_skip_plans,
        ParquetPruningStats* pruning_stats, const cctz::time_zone* timezone = nullptr,
        const RuntimeState* runtime_state = nullptr);

} // namespace doris::format::parquet
