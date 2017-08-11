// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#include "olap/writer.h"

#include "olap/column_file/data_writer.h"
#include "olap/olap_data.h"
#include "olap/olap_index.h"
#include "olap/olap_table.h"
#include "olap/row_block.h"
#include "olap/row_cursor.h"

namespace palo {

IWriter* IWriter::create(SmartOLAPTable table, OLAPIndex *index, bool is_push_write) {
    IWriter* writer = NULL;

    switch (table->data_file_type()) {
    case OLAP_DATA_FILE:
        writer = new (std::nothrow) OLAPDataWriter(table, index, is_push_write);
        break;
    case COLUMN_ORIENTED_FILE:
        writer = new (std::nothrow) column_file::ColumnDataWriter(table, index, is_push_write);
        break;
    default:
        OLAP_LOG_WARNING("unknown data file type. [type=%s]",
                         DataFileType_Name(table->data_file_type()).c_str());
        break;
    }

    return writer;
}

OLAPDataWriter::OLAPDataWriter(SmartOLAPTable table, OLAPIndex* index, bool is_push_write) : 
        IWriter(is_push_write, table),
        _index(index),
        _data(NULL),
        _current_segment_size(0),
        _max_segment_size(OLAP_MAX_SEGMENT_FILE_SIZE),
        _row_block(NULL),
        _num_rows(0),
        _is_push_write(is_push_write) {}

OLAPDataWriter::~OLAPDataWriter() {
    SAFE_DELETE(_row_block);
    SAFE_DELETE(_data);
}

OLAPStatus OLAPDataWriter::init() {
    return init(_table->num_rows_per_row_block());
}

OLAPStatus OLAPDataWriter::init(uint32_t num_rows_per_row_block) {
    OLAPStatus res = OLAP_SUCCESS;

    res = IWriter::init();
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init res. [res=%d]", res);
        return res;
    }
    
    if (_table->segment_size() < _max_segment_size) {
        _max_segment_size = _table->segment_size();
    }

    _data = new (std::nothrow) OLAPData(_index);
    if (NULL == _data) {
        OLAP_LOG_WARNING("fail to new OLAPData. [table='%s']", _table->full_name().c_str());
        return OLAP_ERR_MALLOC_ERROR;
    }

    if (OLAP_SUCCESS != (res = _data->init())) {
        OLAP_LOG_WARNING("fail to initiate OLAPData. [table='%s' res=%d]",
                         _table->full_name().c_str(),
                         res);
        return res;
    }

    _row_block = new (std::nothrow) RowBlock(_table->tablet_schema());
    if (NULL == _row_block) {
        OLAP_LOG_WARNING("fail to new RowBlock. [table='%s']", _table->full_name().c_str());
        return OLAP_ERR_MALLOC_ERROR;
    }

    OLAP_LOG_DEBUG("init OLAPData writer. [table='%s' block_row_size=%lu]",
                   _table->full_name().c_str(),
                   _table->num_rows_per_row_block());
    
    RowBlockInfo block_info(0U, num_rows_per_row_block, 0);
    block_info.data_file_type = OLAP_DATA_FILE;
    block_info.null_supported = true;
    if (OLAP_SUCCESS != (res = _row_block->init(block_info))) {
        OLAP_LOG_WARNING("fail to initiate row block. [res=%d]", res);
        return res;
    }

    if (OLAP_SUCCESS != (res = _data->add_segment())) {
        OLAP_LOG_WARNING("fail to add data segment. [res=%d]", res);
        return res;
    }

    if (OLAP_SUCCESS != (res = _index->add_segment())) {
        OLAP_LOG_WARNING("fail to add index segment. [res=%d]", res);
        return res;
    }

    if (_is_push_write) {
        _write_mbytes_per_sec = config::push_write_mbytes_per_sec;
    } else {
        _write_mbytes_per_sec = config::base_expansion_write_mbytes_per_sec;
    }
    
    _speed_limit_watch.reset();

    return OLAP_SUCCESS;
}

OLAPStatus OLAPDataWriter::attached_by(RowCursor* row_cursor) {
    if (_row_index >= _table->num_rows_per_row_block()) {
        if (OLAP_SUCCESS != flush()) {
            OLAP_LOG_WARNING("failed to flush data while attaching row cursor.");
            return OLAP_ERR_OTHER_ERROR;
        }
    }

    // Row points to the memory that needs to write in _row_block.
    if (OLAP_SUCCESS != _row_block->get_row_to_write(_row_index, row_cursor)) {
        OLAP_LOG_WARNING("fail to get row in row_block. [row_num=%u]", _row_index);
        return OLAP_ERR_OTHER_ERROR;
    }

    return OLAP_SUCCESS;
}

OLAPStatus OLAPDataWriter::flush() {
    if (_row_index < 1) {
        return OLAP_SUCCESS;
    }

    if (OLAP_SUCCESS != _row_block->finalize(_row_index)) {
        OLAP_LOG_WARNING("fail to finalize row block. [num_rows=%u]", _row_index);
        return OLAP_ERR_WRITER_ROW_BLOCK_ERROR;
    }

    // Write a ready row block into OLAPData.
    // Add one index item into OLAPIndex.
    if (OLAP_SUCCESS != write_row_block(_row_block)) {
        OLAP_LOG_WARNING("fail to write row block. [row_num=%u]", _row_index);
        return OLAP_ERR_WRITER_DATA_WRITE_ERROR;
    }

    _row_index = 0U;
    return OLAP_SUCCESS;
}

void OLAPDataWriter::sync() {
    _data->sync();
    _index->sync();
}

OLAPStatus OLAPDataWriter::write_row_block(RowBlock* row_block) {
    if (NULL == row_block || row_block->row_block_info().row_num == 0) {
        return OLAP_SUCCESS;
    }

    // If _current_segment_size plus row_block size without compressing data
    // exceeds the max data segment size, finalize the current data/index
    // segment, and add new data/index segment.
    if (static_cast<int64_t>(_current_segment_size) + static_cast<int64_t>(row_block->buf_len())
            > static_cast<int64_t>(_max_segment_size)) {
        // Finalize data and index segment.
        uint32_t data_segment_size;
        if (OLAP_SUCCESS != _data->finalize_segment(&data_segment_size)) {
            OLAP_LOG_WARNING("fail to finish segment from olap_data.");
            return OLAP_ERR_WRITER_DATA_WRITE_ERROR;
        }

        if (OLAP_SUCCESS != _index->finalize_segment(data_segment_size, _num_rows)) {
            OLAP_LOG_WARNING("fail to finish segment from olap_index.");
            return OLAP_ERR_WRITER_INDEX_WRITE_ERROR;
        }

        if (OLAP_SUCCESS != _data->add_segment()
                || OLAP_SUCCESS != _index->add_segment()) {
            OLAP_LOG_WARNING("fail to add data or index segment.");
            return OLAP_ERR_OTHER_ERROR;
        }

        _num_rows = 0;
        _current_segment_size = 0U;
    }

    // Add row block into olap data.
    uint32_t start_offset;
    uint32_t end_offset;
    if (OLAP_SUCCESS != _data->add_row_block(*row_block,
                                             &start_offset,
                                             &end_offset)) {
        OLAP_LOG_WARNING("fail to write data.");
        return OLAP_ERR_WRITER_DATA_WRITE_ERROR;
    }

    // Add the corresponding index item into olap index.
    if (OLAP_SUCCESS != _index->add_row_block(*row_block, start_offset)) {
        OLAP_LOG_WARNING("fail to update index.");
        return OLAP_ERR_WRITER_INDEX_WRITE_ERROR;
    }

    _current_segment_size = end_offset;
    _num_rows += row_block->row_block_info().row_num;

    if (_write_mbytes_per_sec > 0) {
        uint64_t delta_time_us = _speed_limit_watch.get_elapse_time_us();
        int64_t sleep_time =
                _current_segment_size / _write_mbytes_per_sec - delta_time_us;
        if (sleep_time > 0) {
            OLAP_LOG_DEBUG("sleep to limit merge speed. [time=%lu bytes=%lu]",
                    sleep_time, _current_segment_size);
            usleep(sleep_time);
        }
    }
    
    // In order to reuse row_block, clear the row_block after finalize
    row_block->clear();

    return OLAP_SUCCESS;
}

// Finalize may be success in spite of write() failure.
OLAPStatus OLAPDataWriter::finalize() {
    // Write the last row block into OLAPData
    if (OLAP_SUCCESS != flush()) {
        OLAP_LOG_WARNING("fail to flush row block.");
        return OLAP_ERR_WRITER_DATA_WRITE_ERROR;
    }

    // Finalize data and index segment.
    uint32_t data_segment_size;
    if (OLAP_SUCCESS != _data->finalize_segment(&data_segment_size)) {
        OLAP_LOG_WARNING("fail to finish segment from olap_data.");
        return OLAP_ERR_WRITER_DATA_WRITE_ERROR;
    }

    if (OLAP_SUCCESS != _index->finalize_segment(data_segment_size, _num_rows)) {
        OLAP_LOG_WARNING("fail to finish segment from olap_index.");
        return OLAP_ERR_WRITER_INDEX_WRITE_ERROR;
    }

    OLAPStatus res = _index->set_column_statistics(_column_statistics);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("Fail to set delta pruning![res=%d]", res);
        return res;
    }
    
    _num_rows = 0;

    return OLAP_SUCCESS;
}

uint64_t OLAPDataWriter::written_bytes() {
    return _current_segment_size + _index->num_segments() * _max_segment_size;
}

}  // namespace palo
