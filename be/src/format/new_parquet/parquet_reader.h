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
#include <vector>

#include "common/status.h"
#include "format/reader/file_reader.h"
#include "parquet_column_schema.h"

namespace doris {
namespace io {
struct IOContext;
} // namespace io
} // namespace doris

namespace doris::parquet {

struct ParquetReaderScanState;

// Parquet 文件物理读取层。
// 该类只理解 Parquet file-local schema 和 FileScanRequest，不理解 Iceberg/global
// schema，不处理 table-level cast/default/generated/partition 语义。
class ParquetReader : public reader::FileReader {
public:
    ParquetReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
                  std::unique_ptr<io::FileDescription>& file_description,
                  std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile);
    ~ParquetReader() override;

    // 打开 Parquet 文件并解析 footer metadata。
    // init 成功后可以调用 get_schema() 获取 Parquet file-local schema。
    Status init(RuntimeState* state) override;

    // 返回 init() 阶段解析出的 Parquet 文件自身 schema。
    // 该方法只能在 init() 成功后调用，不要求 open() 已经执行。
    // 这里不做 Iceberg schema evolution，也不把字段转换成 table/global schema。
    Status get_schema(std::vector<reader::ColumnDefinition>* file_schema) const override;

    Status open(std::unique_ptr<reader::FileScanRequest>& request) override;
    // 读取下一批 Parquet file-local block。
    // 该方法只能在 init() 成功后调用。
    // 返回列必须保持 file-local 语义，不能在这里补 default/generated/partition 列。
    Status get_block(Block* file_block, size_t* rows, bool* eof) override;

    Status get_aggregate_result(const reader::FileAggregateRequest& request,
                                reader::FileAggregateResult* result) override;

    Status close() override;

protected:
    void _init_profile() override;

private:
    struct ParquetProfile {
        RuntimeProfile::Counter* filtered_row_groups = nullptr;
        RuntimeProfile::Counter* filtered_row_groups_by_min_max = nullptr;
        RuntimeProfile::Counter* filtered_row_groups_by_dictionary = nullptr;
        RuntimeProfile::Counter* filtered_row_groups_by_bloom_filter = nullptr;
        RuntimeProfile::Counter* to_read_row_groups = nullptr;
        RuntimeProfile::Counter* total_row_groups = nullptr;
        RuntimeProfile::Counter* selected_row_ranges = nullptr;
        RuntimeProfile::Counter* filtered_group_rows = nullptr;
        RuntimeProfile::Counter* filtered_page_rows = nullptr;
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
    void _fill_column_definition(const ParquetColumnSchema& column_schema,
                                 reader::ColumnDefinition* field) const;

    std::unique_ptr<ParquetReaderScanState> _state;
    ParquetProfile _parquet_profile;
};

} // namespace doris::parquet
