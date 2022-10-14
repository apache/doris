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

#include <stdint.h>

#include <queue>
#include <string>
#include <vector>

#include "common/status.h"
#include "exec/olap_common.h"
#include "gen_cpp/parquet_types.h"
#include "io/file_reader.h"
#include "vec/core/block.h"
#include "vec/exec/format/generic_reader.h"
#include "vparquet_column_reader.h"
#include "vparquet_file_metadata.h"
#include "vparquet_group_reader.h"
#include "vparquet_page_index.h"

namespace doris::vectorized {

class ParquetReader : public GenericReader {
public:
    struct Statistics {
        int32_t filtered_row_groups = 0;
        int32_t read_row_groups = 0;
        int64_t filtered_group_rows = 0;
        int64_t filtered_page_rows = 0;
        int64_t read_rows = 0;
        int64_t filtered_bytes = 0;
        int64_t read_bytes = 0;
        int64_t column_read_time = 0;
        int64_t parse_meta_time = 0;
    };

    ParquetReader(RuntimeProfile* profile, const TFileScanRangeParams& params,
                  const TFileRangeDesc& range, const std::vector<std::string>& column_names,
                  size_t batch_size, cctz::time_zone* ctz);

    virtual ~ParquetReader();
    // for test
    void set_file_reader(FileReader* file_reader) { _file_reader.reset(file_reader); }

    Status init_reader(
            std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range);

    Status get_next_block(Block* block, size_t* read_rows, bool* eof) override;

    void close();

    int64_t size() const { return _file_reader->size(); }

    std::unordered_map<std::string, TypeDescriptor> get_name_to_type() override;
    Status get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                       std::unordered_set<std::string>* missing_cols) override;

    Statistics& statistics() { return _statistics; }

private:
    struct ParquetProfile {
        RuntimeProfile::Counter* filtered_row_groups;
        RuntimeProfile::Counter* to_read_row_groups;
        RuntimeProfile::Counter* filtered_group_rows;
        RuntimeProfile::Counter* filtered_page_rows;
        RuntimeProfile::Counter* filtered_bytes;
        RuntimeProfile::Counter* to_read_bytes;
        RuntimeProfile::Counter* column_read_time;
        RuntimeProfile::Counter* parse_meta_time;

        RuntimeProfile::Counter* file_read_time;
        RuntimeProfile::Counter* file_read_calls;
        RuntimeProfile::Counter* file_read_bytes;
        RuntimeProfile::Counter* decompress_time;
        RuntimeProfile::Counter* decompress_cnt;
        RuntimeProfile::Counter* decode_header_time;
        RuntimeProfile::Counter* decode_value_time;
        RuntimeProfile::Counter* decode_dict_time;
        RuntimeProfile::Counter* decode_level_time;
        RuntimeProfile::Counter* decode_null_map_time;
    };

    void _init_profile();
    bool _next_row_group_reader();
    Status _init_read_columns();
    Status _init_row_group_readers();
    // Page Index Filter
    bool _has_page_index(const std::vector<tparquet::ColumnChunk>& columns, PageIndex& page_index);
    Status _process_page_index(const tparquet::RowGroup& row_group,
                               std::vector<RowRange>& candidate_row_ranges);

    // Row Group Filter
    bool _is_misaligned_range_group(const tparquet::RowGroup& row_group);
    Status _process_column_stat_filter(const std::vector<tparquet::ColumnChunk>& column_meta,
                                       bool* filter_group);
    Status _process_row_group_filter(const tparquet::RowGroup& row_group, bool* filter_group);
    void _init_chunk_dicts();
    Status _process_dict_filter(bool* filter_group);
    void _init_bloom_filter();
    Status _process_bloom_filter(bool* filter_group);
    Status _filter_row_groups();
    int64_t _get_column_start_offset(const tparquet::ColumnMetaData& column_init_column_readers);

private:
    RuntimeProfile* _profile;
    const TFileScanRangeParams& _scan_params;
    const TFileRangeDesc& _scan_range;
    std::unique_ptr<FileReader> _file_reader = nullptr;

    std::shared_ptr<FileMetaData> _file_metadata;
    const tparquet::FileMetaData* _t_metadata;
    std::list<std::shared_ptr<RowGroupReader>> _row_group_readers;
    std::shared_ptr<RowGroupReader> _current_group_reader;
    int32_t _total_groups;                  // num of groups(stripes) of a parquet(orc) file
    std::map<std::string, int> _map_column; // column-name <---> column-index
    std::unordered_map<std::string, ColumnValueRangeType>* _colname_to_value_range;
    std::vector<ParquetReadColumn> _read_columns;
    std::list<int32_t> _read_row_groups;
    // parquet file reader object
    size_t _batch_size;
    int64_t _range_start_offset;
    int64_t _range_size;
    cctz::time_zone* _ctz;

    std::unordered_map<int, tparquet::OffsetIndex> _col_offsets;
    const std::vector<std::string> _column_names;

    std::vector<std::string> _missing_cols;
    Statistics _statistics;
    ParquetColumnReader::Statistics _column_statistics;
    ParquetProfile _parquet_profile;
    bool _closed = false;
};
} // namespace doris::vectorized
