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

#include "storage/binlog.h"
#include "storage/segment/historical_row_retriever.h"
#include "storage/segment/segment_writer.h"
namespace doris {

namespace segment_v2 {
#define BINLOG_COLNUM 3

class RowBinlogSourceDataWriter {
public:
    explicit RowBinlogSourceDataWriter(const SegmentWriteBinlogOptions& opt);

    ~RowBinlogSourceDataWriter();

    Status init();

    Status prepare_by_source_block(const Block* block, size_t row_pos, size_t num_rows,
                                   std::vector<uint32_t>& partial_source_cids,
                                   Block* full_block = nullptr);

    Status prepare_seq_column(const ColumnWithTypeAndName& col, int32_t seq_col_id_in_schema,
                              size_t row_pos, size_t num_rows);

    Status fill_normal_columns(std::vector<std::unique_ptr<ColumnWriter>>& column_writers,
                               size_t start, size_t end,
                               std::vector<uint32_t>& partial_source_cids);

    void clear();

    IOlapColumnDataAccessor* get_converted_column(uint32_t cid) { return _converted_columns[cid]; }

    bool need_before() const { return _opt.write_before; }

    const std::vector<IOlapColumnDataAccessor*>& source_key_columns() const { return _key_columns; }
    const IOlapColumnDataAccessor* seq_column() const { return _seq_column; }

    std::unique_ptr<OlapBlockDataConvertor>& olap_data_convertor() { return _olap_data_convertor; }

    void filter_source_ids(std::vector<uint32_t>& full_cids, std::vector<uint32_t>& res_cids) {
        res_cids.reserve(full_cids.size());
        std::set_intersection(_normal_column_ids.begin(), _normal_column_ids.end(),
                              full_cids.begin(), full_cids.end(), std::back_inserter(res_cids));
    }

private:
    const SegmentWriteBinlogOptions& _opt;
    std::unique_ptr<OlapBlockDataConvertor> _olap_data_convertor;
    std::vector<uint32_t> _normal_column_ids;
    std::vector<IOlapColumnDataAccessor*> _converted_columns;
    size_t _num_rows = 0;

    std::vector<IOlapColumnDataAccessor*> _key_columns;
    IOlapColumnDataAccessor* _seq_column = nullptr;
};

class RowBinlogSegmentWriter : public SegmentWriter {
public:
    explicit RowBinlogSegmentWriter(io::FileWriter* file_writer, uint32_t segment_id,
                                    TabletSchemaSPtr tablet_schema, BaseTabletSPtr tablet,
                                    DataDir* data_dir, const SegmentWriterOptions& opts,
                                    const segment_v2::SegmentWriteBinlogOptions& row_binlog_opts);

    ~RowBinlogSegmentWriter() override = default;

    Status init() override;

    Status append_block(const Block* block, size_t row_pos, size_t num_rows) override;

    // append block directly for binlog compaction
    Status _append_direct_block(const Block* block, size_t row_pos, size_t num_rows);

    Status _fill_binlog_columns(size_t num_rows, const std::vector<int64_t>& op_types);

    Status _fill_before_columns(size_t num_rows);

private:
    bool _write_before = false;
    bool _fill_empty_before_value = false;

    uint32_t _normal_col_start_id = 0;
    uint32_t _before_col_start_id = 0;
    uint32_t _binlog_col_start_id = 0;

    std::unique_ptr<RowBinlogSourceDataWriter> _source_data_writer;
    std::unique_ptr<HistoricalRowRetriever> _historical_data_writer;

    const SegmentWriteBinlogOptions& _binlog_opts;

    std::vector<IOlapColumnDataAccessor*> _converted_key_columns;
    std::shared_ptr<const std::vector<int128_t>> _lsn_ids;
};

} // namespace segment_v2
} // namespace doris
