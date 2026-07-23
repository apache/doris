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

#include "format_v2/parquet/parquet_profile.h"

#include "format_v2/parquet/parquet_statistics.h"
#include "runtime/file_scan_profile.h"

namespace doris::format::parquet {

void ParquetProfile::init(RuntimeProfile* profile) {
    if (profile == nullptr) {
        return;
    }

    file_scan_profile::ensure_hierarchy(profile);
    static const char* parquet_profile = "ParquetReader";
    total_time =
            ADD_CHILD_TIMER_WITH_LEVEL(profile, parquet_profile, file_scan_profile::FILE_READER, 1);

    // Row-group counters are part of the long-standing ParquetReader profile contract. Keep them
    // below the format node so profile parsers and operators can attribute pruning to Parquet.
    filtered_row_groups = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "RowGroupsFiltered", TUnit::UNIT,
                                                       parquet_profile, 1);
    filtered_row_groups_by_min_max = ADD_CHILD_COUNTER_WITH_LEVEL(
            profile, "RowGroupsFilteredByMinMax", TUnit::UNIT, parquet_profile, 1);
    filtered_row_groups_by_dictionary = ADD_CHILD_COUNTER_WITH_LEVEL(
            profile, "RowGroupsFilteredByDictionary", TUnit::UNIT, parquet_profile, 1);
    filtered_row_groups_by_bloom_filter = ADD_CHILD_COUNTER_WITH_LEVEL(
            profile, "RowGroupsFilteredByBloomFilter", TUnit::UNIT, parquet_profile, 1);
    filtered_row_groups_by_page_index = ADD_CHILD_COUNTER_WITH_LEVEL(
            profile, "RowGroupsFilteredByPageIndex", TUnit::UNIT, parquet_profile, 1);
    to_read_row_groups = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "RowGroupsReadNum", TUnit::UNIT,
                                                      parquet_profile, 1);
    total_row_groups = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "RowGroupsTotalNum", TUnit::UNIT,
                                                    parquet_profile, 1);
    selected_row_ranges = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "SelectedRowRanges", TUnit::UNIT,
                                                       parquet_profile, 1);
    filtered_group_rows = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "FilteredRowsByGroup", TUnit::UNIT,
                                                       parquet_profile, 1);
    filtered_page_rows = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "FilteredRowsByPage", TUnit::UNIT,
                                                      parquet_profile, 1);
    pages_skipped_by_data_page_filter = ADD_CHILD_COUNTER_WITH_LEVEL(
            profile, "PagesSkippedByDataPageFilter", TUnit::UNIT, parquet_profile, 1);
    data_page_filter_skip_bytes = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "DataPageFilterSkipBytes",
                                                               TUnit::BYTES, parquet_profile, 1);
    selected_rows =
            ADD_CHILD_COUNTER_WITH_LEVEL(profile, "SelectedRows", TUnit::UNIT, parquet_profile, 1);
    // Keep every Parquet scan metric below the format node: profile consumers extract that
    // subtree and otherwise silently lose this counter even though filtering happened.
    rows_filtered_by_conjunct = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "RowsFilteredByConjunct",
                                                             TUnit::UNIT, parquet_profile, 1);
    total_batches =
            ADD_CHILD_COUNTER_WITH_LEVEL(profile, "TotalBatches", TUnit::UNIT, parquet_profile, 1);
    dense_batches =
            ADD_CHILD_COUNTER_WITH_LEVEL(profile, "DenseBatches", TUnit::UNIT, parquet_profile, 1);
    selected_batches = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "SelectedBatches", TUnit::UNIT,
                                                    parquet_profile, 1);
    empty_selection_batches = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "EmptySelectionBatches",
                                                           TUnit::UNIT, parquet_profile, 1);
    range_gap_skipped_rows = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "RangeGapSkippedRows",
                                                          TUnit::UNIT, parquet_profile, 1);
    reader_read_rows = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "ReaderReadRows", TUnit::UNIT,
                                                    parquet_profile, 1);
    reader_skip_rows = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "ReaderSkipRows", TUnit::UNIT,
                                                    parquet_profile, 1);
    reader_select_rows = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "ReaderSelectRows", TUnit::UNIT,
                                                      parquet_profile, 1);
    level_only_read_time =
            ADD_CHILD_TIMER_WITH_LEVEL(profile, "LevelOnlyReadTime", parquet_profile, 1);
    level_only_skip_time =
            ADD_CHILD_TIMER_WITH_LEVEL(profile, "LevelOnlySkipTime", parquet_profile, 1);
    materialization_time =
            ADD_CHILD_TIMER_WITH_LEVEL(profile, "MaterializationTime", parquet_profile, 1);
    hybrid_selection_batches = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "HybridSelectionBatches",
                                                            TUnit::UNIT, parquet_profile, 1);
    hybrid_selection_ranges = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "HybridSelectionRanges",
                                                           TUnit::UNIT, parquet_profile, 1);
    hybrid_selection_null_fallback_batches = ADD_CHILD_COUNTER_WITH_LEVEL(
            profile, "HybridSelectionNullFallbackBatches", TUnit::UNIT, parquet_profile, 1);
    native_read_calls = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "NativeReadCalls", TUnit::UNIT,
                                                     parquet_profile, 1);
    native_page_fragments = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "NativePageFragments",
                                                         TUnit::UNIT, parquet_profile, 1);
    page_crossing_batches = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "PageCrossingBatches",
                                                         TUnit::UNIT, parquet_profile, 1);
    nested_batches =
            ADD_CHILD_COUNTER_WITH_LEVEL(profile, "NestedBatches", TUnit::UNIT, parquet_profile, 1);
    lazy_read_filtered_rows = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "FilteredRowsByLazyRead",
                                                           TUnit::UNIT, parquet_profile, 1);
    // Format-specific counters stay below ParquetReader so subtree consumers cannot silently
    // attribute them to a different file format initialized on the same RuntimeProfile.
    filtered_bytes = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "FilteredBytes", TUnit::BYTES,
                                                  parquet_profile, 1);
    raw_rows_read =
            ADD_CHILD_COUNTER_WITH_LEVEL(profile, "RawRowsRead", TUnit::UNIT, parquet_profile, 1);
    column_read_time = ADD_CHILD_TIMER_WITH_LEVEL(profile, "ColumnReadTime", parquet_profile, 1);
    parse_meta_time = ADD_CHILD_TIMER_WITH_LEVEL(profile, "ParseMetaTime", parquet_profile, 1);
    parse_footer_time = ADD_CHILD_TIMER_WITH_LEVEL(profile, "ParseFooterTime", parquet_profile, 1);
    file_reader_create_time =
            ADD_CHILD_TIMER_WITH_LEVEL(profile, "FileReaderCreateTime", parquet_profile, 1);
    open_file_num =
            ADD_CHILD_COUNTER_WITH_LEVEL(profile, "FileNum", TUnit::UNIT, parquet_profile, 1);
    page_index_read_calls = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "PageIndexReadCalls", TUnit::UNIT,
                                                         parquet_profile, 1);
    page_index_filter_time =
            ADD_CHILD_TIMER_WITH_LEVEL(profile, "PageIndexFilterTime", parquet_profile, 1);
    read_page_index_time =
            ADD_CHILD_TIMER_WITH_LEVEL(profile, "PageIndexReadTime", parquet_profile, 1);
    parse_page_index_time =
            ADD_CHILD_TIMER_WITH_LEVEL(profile, "PageIndexParseTime", parquet_profile, 1);
    expr_zonemap_unusable = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "ExprZoneMapUnusableEvals",
                                                         TUnit::UNIT, parquet_profile, 1);
    in_zonemap_point_check = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "InZoneMapPointCheckCount",
                                                          TUnit::UNIT, parquet_profile, 1);
    in_zonemap_range_only = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "InZoneMapRangeOnlyCount",
                                                         TUnit::UNIT, parquet_profile, 1);
    row_group_filter_time =
            ADD_CHILD_TIMER_WITH_LEVEL(profile, "RowGroupFilterTime", parquet_profile, 1);
    file_footer_read_calls = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "FileFooterReadCalls",
                                                          TUnit::UNIT, parquet_profile, 1);
    file_footer_hit_cache = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "FileFooterHitCache", TUnit::UNIT,
                                                         parquet_profile, 1);
    decompress_time = ADD_CHILD_TIMER_WITH_LEVEL(profile, "DecompressTime", parquet_profile, 1);
    decompress_cnt = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "DecompressCount", TUnit::UNIT,
                                                  parquet_profile, 1);
    page_read_counter =
            ADD_CHILD_COUNTER_WITH_LEVEL(profile, "PageReadCount", TUnit::UNIT, parquet_profile, 1);
    page_cache_write_counter = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "PageCacheWriteCount",
                                                            TUnit::UNIT, parquet_profile, 1);
    page_cache_compressed_write_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
            profile, "PageCacheCompressedWriteCount", TUnit::UNIT, parquet_profile, 1);
    page_cache_decompressed_write_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
            profile, "PageCacheDecompressedWriteCount", TUnit::UNIT, parquet_profile, 1);
    page_cache_hit_counter = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "PageCacheHitCount", TUnit::UNIT,
                                                          parquet_profile, 1);
    page_cache_missing_counter = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "PageCacheMissingCount",
                                                              TUnit::UNIT, parquet_profile, 1);
    page_cache_compressed_hit_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
            profile, "PageCacheCompressedHitCount", TUnit::UNIT, parquet_profile, 1);
    page_cache_decompressed_hit_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
            profile, "PageCacheDecompressedHitCount", TUnit::UNIT, parquet_profile, 1);
    decode_header_time =
            ADD_CHILD_TIMER_WITH_LEVEL(profile, "PageHeaderDecodeTime", parquet_profile, 1);
    read_page_header_time =
            ADD_CHILD_TIMER_WITH_LEVEL(profile, "PageHeaderReadTime", parquet_profile, 1);
    decode_value_time = ADD_CHILD_TIMER_WITH_LEVEL(profile, "DecodeValueTime", parquet_profile, 1);
    decode_dict_time = ADD_CHILD_TIMER_WITH_LEVEL(profile, "DecodeDictTime", parquet_profile, 1);
    decode_level_time = ADD_CHILD_TIMER_WITH_LEVEL(profile, "DecodeLevelTime", parquet_profile, 1);
    decode_null_map_time =
            ADD_CHILD_TIMER_WITH_LEVEL(profile, "DecodeNullMapTime", parquet_profile, 1);
    skip_page_header_num = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "SkipPageHeaderNum", TUnit::UNIT,
                                                        parquet_profile, 1);
    parse_page_header_num = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "ParsePageHeaderNum", TUnit::UNIT,
                                                         parquet_profile, 1);
    predicate_filter_time =
            ADD_CHILD_TIMER_WITH_LEVEL(profile, "PredicateFilterTime", parquet_profile, 1);
    predicate_compaction_time =
            ADD_CHILD_TIMER_WITH_LEVEL(profile, "PredicateCompactionTime", parquet_profile, 1);
    predicate_compaction_bytes = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "PredicateCompactionBytes",
                                                              TUnit::BYTES, parquet_profile, 1);
    predicate_compaction_count = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "PredicateCompactionCount",
                                                              TUnit::UNIT, parquet_profile, 1);
    fixed_width_predicate_direct_batches = ADD_CHILD_COUNTER_WITH_LEVEL(
            profile, "FixedWidthPredicateDirectBatches", TUnit::UNIT, parquet_profile, 1);
    fixed_width_predicate_direct_rows = ADD_CHILD_COUNTER_WITH_LEVEL(
            profile, "FixedWidthPredicateDirectRows", TUnit::UNIT, parquet_profile, 1);
    dict_filter_rewrite_time =
            ADD_CHILD_TIMER_WITH_LEVEL(profile, "DictFilterRewriteTime", parquet_profile, 1);
    dict_filter_expr_rewrite_time =
            ADD_CHILD_TIMER_WITH_LEVEL(profile, "DictFilterExprRewriteTime", parquet_profile, 1);
    dict_filter_read_dict_time =
            ADD_CHILD_TIMER_WITH_LEVEL(profile, "DictFilterReadDictTime", parquet_profile, 1);
    dict_filter_build_time =
            ADD_CHILD_TIMER_WITH_LEVEL(profile, "DictFilterBuildTime", parquet_profile, 1);
    dict_filter_candidate_columns = ADD_CHILD_COUNTER_WITH_LEVEL(
            profile, "DictFilterCandidateColumns", TUnit::UNIT, parquet_profile, 1);
    dict_filter_columns = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "DictFilterColumns", TUnit::UNIT,
                                                       parquet_profile, 1);
    dict_filter_unsupported_columns = ADD_CHILD_COUNTER_WITH_LEVEL(
            profile, "DictFilterUnsupportedColumns", TUnit::UNIT, parquet_profile, 1);
    dict_filter_read_failures = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "DictFilterReadFailures",
                                                             TUnit::UNIT, parquet_profile, 1);
    rows_filtered_by_dict_filter = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "RowsFilteredByDictFilter",
                                                                TUnit::UNIT, parquet_profile, 1);
    bloom_filter_read_time =
            ADD_CHILD_TIMER_WITH_LEVEL(profile, "BloomFilterReadTime", parquet_profile, 1);
}

void ParquetProfile::update_pruning_stats(const ParquetPruningStats& pruning_stats) const {
    COUNTER_UPDATE(filtered_row_groups,
                   pruning_stats.total_row_groups - pruning_stats.selected_row_groups);
    COUNTER_UPDATE(filtered_row_groups_by_min_max, pruning_stats.filtered_row_groups_by_statistics);
    COUNTER_UPDATE(filtered_row_groups_by_dictionary,
                   pruning_stats.filtered_row_groups_by_dictionary);
    COUNTER_UPDATE(filtered_row_groups_by_bloom_filter,
                   pruning_stats.filtered_row_groups_by_bloom_filter);
    COUNTER_UPDATE(filtered_row_groups_by_page_index,
                   pruning_stats.filtered_row_groups_by_page_index);
    COUNTER_UPDATE(to_read_row_groups, pruning_stats.selected_row_groups);
    COUNTER_UPDATE(total_row_groups, pruning_stats.total_row_groups);
    COUNTER_UPDATE(selected_row_ranges, pruning_stats.selected_row_ranges);
    COUNTER_UPDATE(filtered_group_rows, pruning_stats.filtered_group_rows);
    COUNTER_UPDATE(filtered_bytes, pruning_stats.filtered_bytes);
    COUNTER_UPDATE(filtered_page_rows, pruning_stats.filtered_page_rows);
    COUNTER_UPDATE(page_index_read_calls, pruning_stats.page_index_read_calls);
    COUNTER_UPDATE(bloom_filter_read_time, pruning_stats.bloom_filter_read_time);
    COUNTER_UPDATE(row_group_filter_time, pruning_stats.row_group_filter_time);
    COUNTER_UPDATE(page_index_filter_time, pruning_stats.page_index_filter_time);
    COUNTER_UPDATE(read_page_index_time, pruning_stats.read_page_index_time);
    COUNTER_UPDATE(parse_page_index_time, pruning_stats.parse_page_index_time);
    COUNTER_UPDATE(expr_zonemap_unusable, pruning_stats.expr_zonemap_unusable_evals);
    COUNTER_UPDATE(in_zonemap_point_check, pruning_stats.in_zonemap_point_check_count);
    COUNTER_UPDATE(in_zonemap_range_only, pruning_stats.in_zonemap_range_only_count);
}

void ParquetProfile::update_deferred_pruning_stats(const ParquetPruningStats& pruning_stats,
                                                   bool selected) const {
    const int64_t filtered = selected ? 0 : 1;
    COUNTER_UPDATE(filtered_row_groups, filtered);
    COUNTER_UPDATE(filtered_row_groups_by_dictionary,
                   pruning_stats.filtered_row_groups_by_dictionary);
    COUNTER_UPDATE(filtered_row_groups_by_bloom_filter,
                   pruning_stats.filtered_row_groups_by_bloom_filter);
    COUNTER_UPDATE(filtered_row_groups_by_page_index,
                   pruning_stats.filtered_row_groups_by_page_index);
    // Initial footer planning counts every surviving candidate as readable. Lazy probes correct
    // that estimate only when a later dictionary, Bloom, or page-index check removes the group.
    COUNTER_UPDATE(to_read_row_groups, -filtered);
    COUNTER_UPDATE(selected_row_ranges, pruning_stats.selected_row_ranges);
    COUNTER_UPDATE(filtered_group_rows, pruning_stats.filtered_group_rows);
    COUNTER_UPDATE(filtered_bytes, pruning_stats.filtered_bytes);
    COUNTER_UPDATE(filtered_page_rows, pruning_stats.filtered_page_rows);
    COUNTER_UPDATE(page_index_read_calls, pruning_stats.page_index_read_calls);
    COUNTER_UPDATE(bloom_filter_read_time, pruning_stats.bloom_filter_read_time);
    COUNTER_UPDATE(row_group_filter_time, pruning_stats.row_group_filter_time);
    COUNTER_UPDATE(page_index_filter_time, pruning_stats.page_index_filter_time);
    COUNTER_UPDATE(read_page_index_time, pruning_stats.read_page_index_time);
    COUNTER_UPDATE(parse_page_index_time, pruning_stats.parse_page_index_time);
    COUNTER_UPDATE(expr_zonemap_unusable, pruning_stats.expr_zonemap_unusable_evals);
    COUNTER_UPDATE(in_zonemap_point_check, pruning_stats.in_zonemap_point_check_count);
    COUNTER_UPDATE(in_zonemap_range_only, pruning_stats.in_zonemap_range_only_count);
}

ParquetPageSkipProfile ParquetProfile::page_skip_profile() const {
    return {
            .skipped_pages = pages_skipped_by_data_page_filter,
            .skipped_bytes = data_page_filter_skip_bytes,
    };
}

ParquetColumnReaderProfile ParquetProfile::column_reader_profile() const {
    return {
            .reader_read_rows = reader_read_rows,
            .reader_skip_rows = reader_skip_rows,
            .reader_select_rows = reader_select_rows,
            .level_only_read_time = level_only_read_time,
            .level_only_skip_time = level_only_skip_time,
            .materialization_time = materialization_time,
            .hybrid_selection_batches = hybrid_selection_batches,
            .hybrid_selection_ranges = hybrid_selection_ranges,
            .hybrid_selection_null_fallback_batches = hybrid_selection_null_fallback_batches,
            .decompress_time = decompress_time,
            .decompress_count = decompress_cnt,
            .decode_header_time = decode_header_time,
            .decode_value_time = decode_value_time,
            .decode_dictionary_time = decode_dict_time,
            .decode_level_time = decode_level_time,
            .decode_null_map_time = decode_null_map_time,
            .page_index_read_calls = page_index_read_calls,
            .skip_page_header_count = skip_page_header_num,
            .parse_page_header_count = parse_page_header_num,
            .read_page_header_time = read_page_header_time,
            .page_read_count = page_read_counter,
            .page_cache_write_count = page_cache_write_counter,
            .page_cache_compressed_write_count = page_cache_compressed_write_counter,
            .page_cache_decompressed_write_count = page_cache_decompressed_write_counter,
            .page_cache_hit_count = page_cache_hit_counter,
            .page_cache_miss_count = page_cache_missing_counter,
            .page_cache_compressed_hit_count = page_cache_compressed_hit_counter,
            .page_cache_decompressed_hit_count = page_cache_decompressed_hit_counter,
            .native_read_calls = native_read_calls,
            .native_page_fragments = native_page_fragments,
            .page_crossing_batches = page_crossing_batches,
            .nested_batches = nested_batches,
    };
}

ParquetScanProfile ParquetProfile::scan_profile() const {
    return {
            .raw_rows_read = raw_rows_read,
            .selected_rows = selected_rows,
            .rows_filtered_by_conjunct = rows_filtered_by_conjunct,
            .lazy_read_filtered_rows = lazy_read_filtered_rows,
            .total_batches = total_batches,
            .dense_batches = dense_batches,
            .selected_batches = selected_batches,
            .empty_selection_batches = empty_selection_batches,
            .range_gap_skipped_rows = range_gap_skipped_rows,
            .column_read_time = column_read_time,
            .predicate_filter_time = predicate_filter_time,
            .predicate_compaction_time = predicate_compaction_time,
            .predicate_compaction_bytes = predicate_compaction_bytes,
            .predicate_compaction_count = predicate_compaction_count,
            .fixed_width_predicate_direct_batches = fixed_width_predicate_direct_batches,
            .fixed_width_predicate_direct_rows = fixed_width_predicate_direct_rows,
            .dict_filter_rewrite_time = dict_filter_rewrite_time,
            .dict_filter_expr_rewrite_time = dict_filter_expr_rewrite_time,
            .dict_filter_read_dict_time = dict_filter_read_dict_time,
            .dict_filter_build_time = dict_filter_build_time,
            .dict_filter_candidate_columns = dict_filter_candidate_columns,
            .dict_filter_columns = dict_filter_columns,
            .dict_filter_unsupported_columns = dict_filter_unsupported_columns,
            .dict_filter_read_failures = dict_filter_read_failures,
            .rows_filtered_by_dict_filter = rows_filtered_by_dict_filter,
            .column_reader_profile = column_reader_profile(),
    };
}

} // namespace doris::format::parquet
