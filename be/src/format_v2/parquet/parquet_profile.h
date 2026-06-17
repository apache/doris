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

// ============================================================================
// Page Skip Profile — data page 级跳过统计
// ============================================================================
struct ParquetPageSkipProfile {
    RuntimeProfile::Counter* skipped_pages = nullptr; // 被 page index 跳过的 data page 数
    RuntimeProfile::Counter* skipped_bytes = nullptr; // 被跳过的压缩字节数
};

// ============================================================================
// Column Reader Profile — 列读取统计
// ============================================================================
struct ParquetColumnReaderProfile {
    RuntimeProfile::Counter* reader_read_rows = nullptr;        // read() 读取的行数
    RuntimeProfile::Counter* reader_skip_rows = nullptr;        // skip() 跳过的行数
    RuntimeProfile::Counter* reader_select_rows = nullptr;      // select() 选中的行数
    RuntimeProfile::Counter* arrow_read_records_time = nullptr; // Arrow RecordReader 耗时 (ns)
    RuntimeProfile::Counter* materialization_time = nullptr;    // 值物化耗时 (ns)
};

// ============================================================================
// Scan Profile — 扫描调度统计（每个 batch 的粒度）
// ============================================================================
struct ParquetScanProfile {
    RuntimeProfile::Counter* raw_rows_read = nullptr; // 从 RecordReader 读取的原始行数
    RuntimeProfile::Counter* selected_rows = nullptr; // conjuncts 过滤后选中的行数
    RuntimeProfile::Counter* rows_filtered_by_conjunct = nullptr; // 被 conjuncts 过滤掉的行数
    RuntimeProfile::Counter* lazy_read_filtered_rows =
            nullptr; // 因 late materialization 减少读取的行数
    RuntimeProfile::Counter* total_batches = nullptr;           // 总批次数
    RuntimeProfile::Counter* empty_selection_batches = nullptr; // 全过滤的空批次数
    RuntimeProfile::Counter* range_gap_skipped_rows = nullptr;  // range gap 跳过的行数
    RuntimeProfile::Counter* column_read_time = nullptr;        // 列读取耗时 (ns)
    RuntimeProfile::Counter* predicate_filter_time = nullptr;   // predicate 过滤耗时 (ns)
    ParquetColumnReaderProfile column_reader_profile;           // 嵌套的列读取统计
};

// ============================================================================
// Parquet Profile — 统一的 RuntimeProfile Counter 集合
// ============================================================================
//
// 管理 new Parquet reader 暴露的所有 RuntimeProfile Counter。
// 通过 page_skip_profile() / column_reader_profile() / scan_profile() 方法
// 将整体的 Counter 集合拆分为不同模块需要的窄视图。
// ============================================================================
struct ParquetProfile {
    void init(RuntimeProfile* profile);
    void update_pruning_stats(const ParquetPruningStats& pruning_stats) const;

    // 构建各模块的窄视图（只是指针透传，不创建新 Counter）
    ParquetPageSkipProfile page_skip_profile() const;
    ParquetColumnReaderProfile column_reader_profile() const;
    ParquetScanProfile scan_profile() const;

    // ======== RowGroup 裁剪 ========
    RuntimeProfile::Counter* filtered_row_groups = nullptr;
    RuntimeProfile::Counter* filtered_row_groups_by_min_max = nullptr;
    RuntimeProfile::Counter* filtered_row_groups_by_dictionary = nullptr;
    RuntimeProfile::Counter* filtered_row_groups_by_bloom_filter = nullptr;
    RuntimeProfile::Counter* to_read_row_groups = nullptr;
    RuntimeProfile::Counter* total_row_groups = nullptr;
    RuntimeProfile::Counter* selected_row_ranges = nullptr;
    RuntimeProfile::Counter* filtered_group_rows = nullptr;
    RuntimeProfile::Counter* filtered_page_rows = nullptr;

    // ======== Page Skip ========
    RuntimeProfile::Counter* pages_skipped_by_data_page_filter = nullptr;
    RuntimeProfile::Counter* data_page_filter_skip_bytes = nullptr;

    // ======== Batch 读取 ========
    RuntimeProfile::Counter* selected_rows = nullptr;
    RuntimeProfile::Counter* rows_filtered_by_conjunct = nullptr;
    RuntimeProfile::Counter* total_batches = nullptr;
    RuntimeProfile::Counter* empty_selection_batches = nullptr;
    RuntimeProfile::Counter* range_gap_skipped_rows = nullptr;

    // ======== Column Reader ========
    RuntimeProfile::Counter* reader_read_rows = nullptr;
    RuntimeProfile::Counter* reader_skip_rows = nullptr;
    RuntimeProfile::Counter* reader_select_rows = nullptr;
    RuntimeProfile::Counter* arrow_read_records_time = nullptr;
    RuntimeProfile::Counter* materialization_time = nullptr;

    // ======== 延迟读取 ========
    RuntimeProfile::Counter* lazy_read_filtered_rows = nullptr;
    RuntimeProfile::Counter* filtered_bytes = nullptr;
    RuntimeProfile::Counter* raw_rows_read = nullptr;
    RuntimeProfile::Counter* column_read_time = nullptr;

    // ======== 文件操作 ========
    RuntimeProfile::Counter* parse_meta_time = nullptr;
    RuntimeProfile::Counter* parse_footer_time = nullptr;
    RuntimeProfile::Counter* file_reader_create_time = nullptr;
    RuntimeProfile::Counter* open_file_num = nullptr;
    RuntimeProfile::Counter* file_footer_read_calls = nullptr;
    RuntimeProfile::Counter* file_footer_hit_cache = nullptr;

    // ======== 裁剪耗时 ========
    RuntimeProfile::Counter* row_group_filter_time = nullptr;
    RuntimeProfile::Counter* page_index_read_calls = nullptr;
    RuntimeProfile::Counter* page_index_filter_time = nullptr;
    RuntimeProfile::Counter* read_page_index_time = nullptr;
    RuntimeProfile::Counter* parse_page_index_time = nullptr;

    // ======== 解压 & Page Cache ========
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

    // ======== 解码 ========
    RuntimeProfile::Counter* decode_header_time = nullptr;
    RuntimeProfile::Counter* read_page_header_time = nullptr;
    RuntimeProfile::Counter* decode_value_time = nullptr;
    RuntimeProfile::Counter* decode_dict_time = nullptr;
    RuntimeProfile::Counter* decode_level_time = nullptr;
    RuntimeProfile::Counter* decode_null_map_time = nullptr;
    RuntimeProfile::Counter* skip_page_header_num = nullptr;
    RuntimeProfile::Counter* parse_page_header_num = nullptr;

    // ======== 其他 ========
    RuntimeProfile::Counter* predicate_filter_time = nullptr;
    RuntimeProfile::Counter* dict_filter_rewrite_time = nullptr;
    RuntimeProfile::Counter* convert_time = nullptr;
    RuntimeProfile::Counter* bloom_filter_read_time = nullptr;
};

} // namespace doris::format::parquet
