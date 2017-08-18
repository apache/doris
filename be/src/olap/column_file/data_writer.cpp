// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "olap/column_file/data_writer.h"

#include <math.h>

#include "olap/column_file/segment_writer.h"
#include "olap/olap_index.h"
#include "olap/row_block.h"


namespace palo {
namespace column_file {

ColumnDataWriter::ColumnDataWriter(SmartOLAPTable table, OLAPIndex* index, bool is_push_write) : 
        IWriter(is_push_write, table),
        _index(index),
        _row_block(NULL),
        _segment_writer(NULL),
        _num_rows(0),
        _block_id(0),
        _max_segment_size(OLAP_MAX_SEGMENT_FILE_SIZE),
        _segment(0) {}

ColumnDataWriter::~ColumnDataWriter() {
    SAFE_DELETE(_row_block);
    SAFE_DELETE(_segment_writer);
}

OLAPStatus ColumnDataWriter::init() {
    OLAPStatus res = OLAP_SUCCESS;

    res = IWriter::init();
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init res. [res=%d]", res);
        return res;
    }

    double size = static_cast<double>(_table->segment_size());
    size *= OLAP_COLUMN_FILE_SEGMENT_SIZE_SCALE;
    _max_segment_size = (uint32_t)lround(size);

    _row_block = new(std::nothrow) RowBlock(_table->tablet_schema());

    if (NULL == _row_block) {
        OLAP_LOG_WARNING("fail to new RowBlock. [table='%s']",
                _table->full_name().c_str());
        return OLAP_ERR_MALLOC_ERROR;
    }

    res = _cursor.init(_table->tablet_schema());
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to initiate row cursor. [res=%d]", res);
        return res;
    }

    OLAP_LOG_DEBUG("init ColumnData writer. [table='%s' block_row_size=%lu]",
            _table->full_name().c_str(), _table->num_rows_per_row_block());
    RowBlockInfo block_info(0U, _table->num_rows_per_row_block(), 0);
    block_info.data_file_type = DataFileType::COLUMN_ORIENTED_FILE;
    block_info.null_supported = true;

    res = _row_block->init(block_info);
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to initiate row block. [res=%d]", res);
        return res;
    }

    res = _add_segment();
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to add segment. [res=%d]", res);
        return res;
    }

    res = _index->add_segment();
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to add index segment. [res=%d]", res);
        return res;
    }

    return res;
}

OLAPStatus ColumnDataWriter::attached_by(RowCursor* row_cursor) {
    if (_row_index >= _table->num_rows_per_row_block()) {
        if (OLAP_SUCCESS != _flush_row_block(false)) {
            OLAP_LOG_WARNING("failed to flush data while attaching row cursor.");
            return OLAP_ERR_OTHER_ERROR;
        }
    }

    if (OLAP_SUCCESS != _row_block->get_row_to_write(_row_index, row_cursor)) {
        OLAP_LOG_WARNING("fail to get row in row_block.");
        return OLAP_ERR_OTHER_ERROR;
    }

    return OLAP_SUCCESS;
}

OLAPStatus ColumnDataWriter::finalize() {
    OLAPStatus res;

    res =  _flush_row_block(true);
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("failed to flush data while attaching row cursor.[res=%d]", res);
        return res;
    }

    res =  _finalize_segment();
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to finalize segment.[res=%d]", res);
        return res;
    }

    res = _index->set_column_statistics(_column_statistics);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("Fail to set delta pruning![res=%d]", res);
        return res;
    }

    return OLAP_SUCCESS;
}

OLAPStatus ColumnDataWriter::_add_segment() {
    std::string file_name;

    if (NULL != _segment_writer) {
        OLAP_LOG_WARNING("previous segment is not finalized before add new segment.");
        return OLAP_ERR_WRITER_SEGMENT_NOT_FINALIZED;
    }

    file_name = _table->construct_data_file_path(_index->version(),
                _index->version_hash(),
                _segment);
    _segment_writer = new(std::nothrow) SegmentWriter(file_name, _table,
            OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE);

    if (NULL == _segment_writer) {
        OLAP_LOG_WARNING("fail to allocate SegmentWriter");
        return OLAP_ERR_MALLOC_ERROR;
    }

    OLAPStatus res = OLAP_SUCCESS;
    if (_is_push_write) {
        res = _segment_writer->init(
                config::push_write_mbytes_per_sec);
    } else {
        res = _segment_writer->init(
                config::base_expansion_write_mbytes_per_sec);
    }

    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to init segment writer");
        return res;
    }

    ++_segment;
    _block_id = 0;
    return OLAP_SUCCESS;
}

OLAPStatus ColumnDataWriter::_finalize_segment() {
    OLAPStatus res = OLAP_SUCCESS;
    uint32_t data_segment_size;

    if (OLAP_SUCCESS != _segment_writer->finalize(&data_segment_size)) {
        OLAP_LOG_WARNING("fail to finish segment from olap_data.");
        return OLAP_ERR_WRITER_DATA_WRITE_ERROR;
    }

    if (OLAP_SUCCESS != _index->finalize_segment(data_segment_size, _num_rows)) {
        OLAP_LOG_WARNING("fail to finish segment from olap_index.");
        return OLAP_ERR_WRITER_INDEX_WRITE_ERROR;
    }

    SAFE_DELETE(_segment_writer);
    return res;
}

OLAPStatus ColumnDataWriter::_flush_row_block(RowBlock* row_block, bool is_finalized) {
    OLAPStatus res;

    // 目标是将自己的block按条写入目标block中。
    for (uint32_t i = 0; i < row_block->row_block_info().row_num; i++) {
        res = row_block->get_row_to_read(i, &_cursor);
        if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("fail to get row from row block. [res=%d]", res);
            return OLAP_ERR_WRITER_DATA_WRITE_ERROR;
        }

        res = _segment_writer->write(&_cursor);
        if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("fail to write row to segment. [res=%d]", res);
            return OLAP_ERR_WRITER_DATA_WRITE_ERROR;
        }
    }

    /*
    if (OLAP_SUCCESS != (res = _segment_writer->create_row_index_entry())) {
        OLAP_LOG_WARNING("fail to record block position. [res=%d]", res);
        return OLAP_ERR_WRITER_INDEX_WRITE_ERROR;
    }
    */

    // 在OLAPIndex中记录的不是数据文件的偏移,而是block的编号
    if (OLAP_SUCCESS != _index->add_row_block(*row_block, _block_id++)) {
        OLAP_LOG_WARNING("fail to update index.");
        return OLAP_ERR_WRITER_INDEX_WRITE_ERROR;
    }

    if ((_segment_writer->estimate_segment_size() >= _max_segment_size) &&
            _segment_writer->is_row_block_full() && !is_finalized) {
        res =  _finalize_segment();
        if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("fail to finalize segment. [res=%d]", res);
            return OLAP_ERR_WRITER_DATA_WRITE_ERROR;
        }

        res = _add_segment();
        if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("fail to add segment. [res=%d]", res);
            return OLAP_ERR_WRITER_DATA_WRITE_ERROR;
        }

        res = _index->add_segment();
        if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("fail to add index segment. [res=%d]", res);
            return res;
        }

        _num_rows = 0;
    }

    row_block->reset_block();
    return OLAP_SUCCESS;
}

OLAPStatus ColumnDataWriter::_flush_row_block(bool is_finalized) {
    OLAPStatus res = OLAP_SUCCESS;

    if (_row_index < 1) {
        return OLAP_SUCCESS;
    }

    // 与OLAPDataWriter不同,这里不是真的写RowBlock,所以并不需要finalize RowBlock
    // 但考虑到兼容Row Block的使用方式,还是调用了finalize
    res = _row_block->finalize(_row_index);
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to finalize row block. [num_rows=%u res=%d]",
                _row_index, res);
        return OLAP_ERR_WRITER_ROW_BLOCK_ERROR;
    }

    res = _flush_row_block(_row_block, is_finalized);
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to flush row block. [res=%d]", res);
        return res;
    }

    // In order to reuse row_block, clear the row_block after finalize
    _row_block->clear();
    _num_rows += _row_index;
    _row_index = 0U;

    return OLAP_SUCCESS;
}

// 这个接口目前只使用在schema change的时候. 对于ColumnFile而言, 未来
// 的schema change应该是轻量级的Schema change, 这个接口就只会用来进行
// Roll up. 从OLAP Data创建Column File的roll up应该是非常少的.
OLAPStatus ColumnDataWriter::write_row_block(RowBlock* row_block) {
    OLAP_LOG_DEBUG("write block, block size = %d", row_block->row_block_info().row_num);

    if (NULL == row_block || 0 == row_block->row_block_info().row_num) {
        return OLAP_SUCCESS;
    }

    return _flush_row_block(row_block, false);
}

uint64_t ColumnDataWriter::written_bytes() {
    uint64_t size = _segment * _max_segment_size + _segment_writer->estimate_segment_size();
    return size;
}

}  // namespace column_file
}  // namespace palo

