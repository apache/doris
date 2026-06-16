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

#include "runtime/runtime_profile.h"

namespace doris::format::parquet {

struct ParquetPruningStats;

struct ParquetPageSkipProfile {
    RuntimeProfile::Counter* skipped_pages = nullptr;
    RuntimeProfile::Counter* skipped_bytes = nullptr;
};

struct ParquetColumnReaderProfile {
    RuntimeProfile::Counter* reader_read_rows = nullptr;
    RuntimeProfile::Counter* reader_skip_rows = nullptr;
    RuntimeProfile::Counter* reader_select_rows = nullptr;
    RuntimeProfile::Counter* arrow_read_records_time = nullptr;
    RuntimeProfile::Counter* materialization_time = nullptr;
};

struct ParquetScanProfile {
    RuntimeProfile::Counter* raw_rows_read = nullptr;
    RuntimeProfile::Counter* selected_rows = nullptr;
    RuntimeProfile::Counter* rows_filtered_by_conjunct = nullptr;
    RuntimeProfile::Counter* lazy_read_filtered_rows = nullptr;
    RuntimeProfile::Counter* total_batches = nullptr;
    RuntimeProfile::Counter* empty_selection_batches = nullptr;
    RuntimeProfile::Counter* range_gap_skipped_rows = nullptr;
    RuntimeProfile::Counter* column_read_time = nullptr;
    RuntimeProfile::Counter* predicate_filter_time = nullptr;
    ParquetColumnReaderProfile column_reader_profile;
};

// Owns every RuntimeProfile counter exposed by the new Parquet reader and builds the
// narrower profile views used by scan scheduling, page skipping, and column readers.
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
    RuntimeProfile::Counter* to_read_row_groups = nullptr;
    RuntimeProfile::Counter* total_row_groups = nullptr;
    RuntimeProfile::Counter* selected_row_ranges = nullptr;
    RuntimeProfile::Counter* filtered_group_rows = nullptr;
    RuntimeProfile::Counter* filtered_page_rows = nullptr;
    RuntimeProfile::Counter* pages_skipped_by_data_page_filter = nullptr;
    RuntimeProfile::Counter* data_page_filter_skip_bytes = nullptr;
    RuntimeProfile::Counter* selected_rows = nullptr;
    RuntimeProfile::Counter* rows_filtered_by_conjunct = nullptr;
    RuntimeProfile::Counter* total_batches = nullptr;
    RuntimeProfile::Counter* empty_selection_batches = nullptr;
    RuntimeProfile::Counter* range_gap_skipped_rows = nullptr;
    RuntimeProfile::Counter* reader_read_rows = nullptr;
    RuntimeProfile::Counter* reader_skip_rows = nullptr;
    RuntimeProfile::Counter* reader_select_rows = nullptr;
    RuntimeProfile::Counter* arrow_read_records_time = nullptr;
    RuntimeProfile::Counter* materialization_time = nullptr;
    RuntimeProfile::Counter* lazy_read_filtered_rows = nullptr;
    RuntimeProfile::Counter* filtered_bytes = nullptr;
    RuntimeProfile::Counter* raw_rows_read = nullptr;
    RuntimeProfile::Counter* column_read_time = nullptr;
    RuntimeProfile::Counter* parse_meta_time = nullptr;
    RuntimeProfile::Counter* parse_footer_time = nullptr;
    RuntimeProfile::Counter* file_reader_create_time = nullptr;
    RuntimeProfile::Counter* open_file_num = nullptr;
    RuntimeProfile::Counter* row_group_filter_time = nullptr;
    RuntimeProfile::Counter* page_index_read_calls = nullptr;
    RuntimeProfile::Counter* page_index_filter_time = nullptr;
    RuntimeProfile::Counter* read_page_index_time = nullptr;
    RuntimeProfile::Counter* parse_page_index_time = nullptr;
    RuntimeProfile::Counter* file_footer_read_calls = nullptr;
    RuntimeProfile::Counter* file_footer_hit_cache = nullptr;
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
    RuntimeProfile::Counter* convert_time = nullptr;
    RuntimeProfile::Counter* bloom_filter_read_time = nullptr;
};

} // namespace doris::format::parquet
