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

#include "runtime/runtime_profile.h"

namespace doris::format::parquet {

struct ParquetPruningStats;

// ============================================================================
// ============================================================================
struct ParquetPageSkipProfile {
    RuntimeProfile::Counter* skipped_pages = nullptr; // number of data pages skipped by page index
    RuntimeProfile::Counter* skipped_bytes = nullptr; // compressed bytes skipped
};

// ============================================================================
// ============================================================================
struct ParquetColumnReaderProfile {
    RuntimeProfile::Counter* reader_read_rows = nullptr;        // rows read by read()
    RuntimeProfile::Counter* reader_skip_rows = nullptr;        // rows skipped by skip()
    RuntimeProfile::Counter* reader_select_rows = nullptr;      // rows selected by select()
    RuntimeProfile::Counter* arrow_read_records_time = nullptr; // Arrow RecordReader time (ns)
    RuntimeProfile::Counter* arrow_skip_records_time = nullptr; // Arrow SkipRecords time (ns)
    RuntimeProfile::Counter* materialization_time = nullptr;    // value materialization time (ns)
    // Native page/encoding reader internals. These counters intentionally mirror v1 so a v1/v2
    // profile comparison attributes page IO, decompression, levels, value decode and conversion to
    // the same stages.
    RuntimeProfile::Counter* decompress_time = nullptr;
    RuntimeProfile::Counter* decompress_count = nullptr;
    RuntimeProfile::Counter* decode_header_time = nullptr;
    RuntimeProfile::Counter* decode_value_time = nullptr;
    RuntimeProfile::Counter* decode_dictionary_time = nullptr;
    RuntimeProfile::Counter* decode_level_time = nullptr;
    RuntimeProfile::Counter* decode_null_map_time = nullptr;
    RuntimeProfile::Counter* convert_time = nullptr;
    RuntimeProfile::Counter* page_index_read_calls = nullptr;
    RuntimeProfile::Counter* skip_page_header_count = nullptr;
    RuntimeProfile::Counter* parse_page_header_count = nullptr;
    RuntimeProfile::Counter* read_page_header_time = nullptr;
    RuntimeProfile::Counter* page_read_count = nullptr;
    RuntimeProfile::Counter* page_cache_write_count = nullptr;
    RuntimeProfile::Counter* page_cache_compressed_write_count = nullptr;
    RuntimeProfile::Counter* page_cache_decompressed_write_count = nullptr;
    RuntimeProfile::Counter* page_cache_hit_count = nullptr;
    RuntimeProfile::Counter* page_cache_miss_count = nullptr;
    RuntimeProfile::Counter* page_cache_compressed_hit_count = nullptr;
    RuntimeProfile::Counter* page_cache_decompressed_hit_count = nullptr;
    RuntimeProfile::Counter* native_read_calls = nullptr;     // v1-native reader calls
    RuntimeProfile::Counter* native_page_fragments = nullptr; // page-bounded read fragments
    RuntimeProfile::Counter* page_crossing_batches = nullptr; // batches spanning multiple pages
    RuntimeProfile::Counter* nested_batches = nullptr;        // complex-column read batches
};

// ============================================================================
// ============================================================================
struct ParquetScanProfile {
    RuntimeProfile::Counter* raw_rows_read = nullptr; // raw rows read from RecordReader
    RuntimeProfile::Counter* selected_rows = nullptr; // rows selected after conjunct filtering
    RuntimeProfile::Counter* rows_filtered_by_conjunct = nullptr; // rows filtered by conjuncts
    RuntimeProfile::Counter* lazy_read_filtered_rows =
            nullptr;                                  // rows avoided by late materialization
    RuntimeProfile::Counter* total_batches = nullptr; // total batch count
    RuntimeProfile::Counter* dense_batches = nullptr; // batches retaining every physical row
    RuntimeProfile::Counter* selected_batches =
            nullptr; // non-empty batches compacted by predicates
    RuntimeProfile::Counter* empty_selection_batches =
            nullptr;                                           // empty batches after full filtering
    RuntimeProfile::Counter* range_gap_skipped_rows = nullptr; // rows skipped by range gaps
    RuntimeProfile::Counter* column_read_time = nullptr;       // column read time (ns)
    RuntimeProfile::Counter* predicate_filter_time = nullptr;  // predicate filter time (ns)
    RuntimeProfile::Counter* dict_filter_rewrite_time = nullptr; // dictionary rewrite time (ns)
    RuntimeProfile::Counter* dict_filter_expr_rewrite_time =
            nullptr; // expression/residual rewrite time (ns)
    RuntimeProfile::Counter* dict_filter_read_dict_time = nullptr; // dictionary page read time (ns)
    RuntimeProfile::Counter* dict_filter_build_time =
            nullptr; // dictionary entry bitmap build time (ns)
    RuntimeProfile::Counter* dict_filter_candidate_columns = nullptr;   // candidate columns
    RuntimeProfile::Counter* dict_filter_columns = nullptr;             // optimized columns
    RuntimeProfile::Counter* dict_filter_unsupported_columns = nullptr; // unsupported columns
    RuntimeProfile::Counter* dict_filter_read_failures = nullptr;       // dictionary read failures
    RuntimeProfile::Counter* rows_filtered_by_dict_filter = nullptr;    // rows filtered by dict
    ParquetColumnReaderProfile column_reader_profile; // nested column read statistics
};

// ============================================================================
// ============================================================================
// ============================================================================
struct ParquetProfile {
    void init(RuntimeProfile* profile);
    void update_pruning_stats(const ParquetPruningStats& pruning_stats) const;

    ParquetPageSkipProfile page_skip_profile() const;
    ParquetColumnReaderProfile column_reader_profile() const;
    ParquetScanProfile scan_profile() const;

    RuntimeProfile::Counter* filtered_row_groups = nullptr;
    RuntimeProfile::Counter* filtered_row_groups_by_min_max = nullptr;
    RuntimeProfile::Counter* filtered_row_groups_by_dictionary = nullptr;
    RuntimeProfile::Counter* filtered_row_groups_by_bloom_filter = nullptr;
    RuntimeProfile::Counter* filtered_row_groups_by_page_index = nullptr;
    RuntimeProfile::Counter* to_read_row_groups = nullptr;
    RuntimeProfile::Counter* total_row_groups = nullptr;
    RuntimeProfile::Counter* selected_row_ranges = nullptr;
    RuntimeProfile::Counter* filtered_group_rows = nullptr;
    RuntimeProfile::Counter* filtered_page_rows = nullptr;

    // ======== Page Skip ========
    RuntimeProfile::Counter* pages_skipped_by_data_page_filter = nullptr;
    RuntimeProfile::Counter* data_page_filter_skip_bytes = nullptr;

    RuntimeProfile::Counter* selected_rows = nullptr;
    RuntimeProfile::Counter* rows_filtered_by_conjunct = nullptr;
    RuntimeProfile::Counter* total_batches = nullptr;
    RuntimeProfile::Counter* dense_batches = nullptr;
    RuntimeProfile::Counter* selected_batches = nullptr;
    RuntimeProfile::Counter* empty_selection_batches = nullptr;
    RuntimeProfile::Counter* range_gap_skipped_rows = nullptr;

    // ======== Column Reader ========
    RuntimeProfile::Counter* reader_read_rows = nullptr;
    RuntimeProfile::Counter* reader_skip_rows = nullptr;
    RuntimeProfile::Counter* reader_select_rows = nullptr;
    RuntimeProfile::Counter* arrow_read_records_time = nullptr;
    RuntimeProfile::Counter* arrow_skip_records_time = nullptr;
    RuntimeProfile::Counter* materialization_time = nullptr;
    RuntimeProfile::Counter* native_read_calls = nullptr;
    RuntimeProfile::Counter* native_page_fragments = nullptr;
    RuntimeProfile::Counter* page_crossing_batches = nullptr;
    RuntimeProfile::Counter* nested_batches = nullptr;

    RuntimeProfile::Counter* lazy_read_filtered_rows = nullptr;
    RuntimeProfile::Counter* filtered_bytes = nullptr;
    RuntimeProfile::Counter* raw_rows_read = nullptr;
    RuntimeProfile::Counter* column_read_time = nullptr;

    RuntimeProfile::Counter* parse_meta_time = nullptr;
    RuntimeProfile::Counter* parse_footer_time = nullptr;
    RuntimeProfile::Counter* file_reader_create_time = nullptr;
    RuntimeProfile::Counter* open_file_num = nullptr;
    RuntimeProfile::Counter* file_footer_read_calls = nullptr;
    RuntimeProfile::Counter* file_footer_hit_cache = nullptr;

    RuntimeProfile::Counter* row_group_filter_time = nullptr;
    RuntimeProfile::Counter* page_index_read_calls = nullptr;
    RuntimeProfile::Counter* page_index_filter_time = nullptr;
    RuntimeProfile::Counter* read_page_index_time = nullptr;
    RuntimeProfile::Counter* parse_page_index_time = nullptr;
    RuntimeProfile::Counter* expr_zonemap_unusable = nullptr;
    RuntimeProfile::Counter* in_zonemap_point_check = nullptr;
    RuntimeProfile::Counter* in_zonemap_range_only = nullptr;

    RuntimeProfile::Counter* decompress_time = nullptr;
    RuntimeProfile::Counter* decompress_cnt = nullptr;
    RuntimeProfile::Counter* page_read_counter = nullptr;
    RuntimeProfile::Counter* page_cache_write_counter = nullptr;
    RuntimeProfile::Counter* page_cache_compressed_write_counter = nullptr;
    RuntimeProfile::Counter* page_cache_decompressed_write_counter = nullptr;
    RuntimeProfile::Counter* page_cache_hit_counter = nullptr;
    RuntimeProfile::Counter* page_cache_missing_counter = nullptr;
    RuntimeProfile::Counter* page_cache_compressed_hit_counter = nullptr;
    RuntimeProfile::Counter* page_cache_decompressed_hit_counter = nullptr;

    RuntimeProfile::Counter* decode_header_time = nullptr;
    RuntimeProfile::Counter* read_page_header_time = nullptr;
    RuntimeProfile::Counter* decode_value_time = nullptr;
    RuntimeProfile::Counter* decode_dict_time = nullptr;
    RuntimeProfile::Counter* decode_level_time = nullptr;
    RuntimeProfile::Counter* decode_null_map_time = nullptr;
    RuntimeProfile::Counter* skip_page_header_num = nullptr;
    RuntimeProfile::Counter* parse_page_header_num = nullptr;

    RuntimeProfile::Counter* predicate_filter_time = nullptr;
    RuntimeProfile::Counter* dict_filter_rewrite_time = nullptr;
    RuntimeProfile::Counter* dict_filter_expr_rewrite_time = nullptr;
    RuntimeProfile::Counter* dict_filter_read_dict_time = nullptr;
    RuntimeProfile::Counter* dict_filter_build_time = nullptr;
    RuntimeProfile::Counter* dict_filter_candidate_columns = nullptr;
    RuntimeProfile::Counter* dict_filter_columns = nullptr;
    RuntimeProfile::Counter* dict_filter_unsupported_columns = nullptr;
    RuntimeProfile::Counter* dict_filter_read_failures = nullptr;
    RuntimeProfile::Counter* rows_filtered_by_dict_filter = nullptr;
    RuntimeProfile::Counter* convert_time = nullptr;
    RuntimeProfile::Counter* bloom_filter_read_time = nullptr;
};

} // namespace doris::format::parquet
