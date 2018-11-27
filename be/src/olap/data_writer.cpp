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

#include "olap/data_writer.h"

#include <math.h>

#include "olap/segment_writer.h"
#include "olap/rowset.h"
#include "olap/row_block.h"


namespace doris {

ColumnDataWriter* ColumnDataWriter::create(OLAPTablePtr table, Rowset *index, bool is_push_write) {
    ColumnDataWriter* writer = NULL;
    switch (table->data_file_type()) {
    case COLUMN_ORIENTED_FILE:
        writer = new (std::nothrow) ColumnDataWriter(table, index, is_push_write);
        break;
    default:
        LOG(WARNING) << "unknown data file type. type=" << DataFileType_Name(table->data_file_type());
        break;
    }

    return writer;
}

ColumnDataWriter::ColumnDataWriter(OLAPTablePtr table, Rowset* index, bool is_push_write)
    : _is_push_write(is_push_write),
      _table(table),
      _column_statistics(_table->num_key_fields(),
                         std::pair<WrapperField*, WrapperField*>(NULL, NULL)),
      _row_index(0),
      _index(index),
      _row_block(NULL),
      _segment_writer(NULL),
      _num_rows(0),
      _block_id(0),
      _max_segment_size(OLAP_MAX_COLUMN_SEGMENT_FILE_SIZE),
      _segment(0),
      _all_num_rows(0),
      _new_segment_created(false)
{
    init();
}

ColumnDataWriter::~ColumnDataWriter() {
    for (size_t i = 0; i < _column_statistics.size(); ++i) {
        SAFE_DELETE(_column_statistics[i].first);
        SAFE_DELETE(_column_statistics[i].second);
    }
    SAFE_DELETE(_row_block);
    SAFE_DELETE(_segment_writer);
}

OLAPStatus ColumnDataWriter::init() {
    OLAPStatus res = OLAP_SUCCESS;

    for (size_t i = 0; i < _column_statistics.size(); ++i) {
        _column_statistics[i].first = WrapperField::create(_table->tablet_schema()[i]);
        DCHECK(_column_statistics[i].first != nullptr) << "fail to create column statistics field.";
        _column_statistics[i].first->set_to_max();

        _column_statistics[i].second = WrapperField::create(_table->tablet_schema()[i]);
        DCHECK(_column_statistics[i].second != nullptr) << "fail to create column statistics field.";
        _column_statistics[i].second->set_null();
        _column_statistics[i].second->set_to_min();
    }

    double size = static_cast<double>(_table->segment_size());
    size *= OLAP_COLUMN_FILE_SEGMENT_SIZE_SCALE;
    _max_segment_size = static_cast<uint32_t>(lround(size));

    _row_block = new(std::nothrow) RowBlock(_table->tablet_schema());

    if (NULL == _row_block) {
        LOG(WARNING) << "fail to new RowBlock. [table='" << _table->full_name() << "']";
        return OLAP_ERR_MALLOC_ERROR;
    }

    res = _cursor.init(_table->tablet_schema());
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to initiate row cursor. [res=%d]", res);
        return res;
    }

    VLOG(3) << "init ColumnData writer. [table='" << _table->full_name()
            << "' block_row_size=" << _table->num_rows_per_row_block() << "]";
    RowBlockInfo block_info(0U, _table->num_rows_per_row_block(), 0);
    block_info.data_file_type = DataFileType::COLUMN_ORIENTED_FILE;
    block_info.null_supported = true;

    res = _row_block->init(block_info);
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to initiate row block. [res=%d]", res);
        return res;
    }
    return OLAP_SUCCESS;
}

OLAPStatus ColumnDataWriter::_init_segment() {
    OLAPStatus res = _add_segment();
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to add segment. [res=%d]", res);
        return res;
    }

    res = _index->add_segment();
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to add index segment. [res=%d]", res);
        return res;
    }

    _new_segment_created = true;
    return res;
}

OLAPStatus ColumnDataWriter::attached_by(RowCursor* row_cursor) {
    if (_row_index >= _table->num_rows_per_row_block()) {
        if (OLAP_SUCCESS != _flush_row_block(false)) {
            OLAP_LOG_WARNING("failed to flush data while attaching row cursor.");
            return OLAP_ERR_OTHER_ERROR;
        }
        RETURN_NOT_OK(_flush_segment_with_verfication());
    }
    _row_block->get_row(_row_index, row_cursor);
    return OLAP_SUCCESS;
}

OLAPStatus ColumnDataWriter::write(const char* row) {
    if (_row_index >= _table->num_rows_per_row_block()) {
        if (OLAP_SUCCESS != _flush_row_block(false)) {
            OLAP_LOG_WARNING("failed to flush data while attaching row cursor.");
            return OLAP_ERR_OTHER_ERROR;
        }
        RETURN_NOT_OK(_flush_segment_with_verfication());
    }
    _row_block->set_row(_row_index, row);
    return OLAP_SUCCESS;
}


void ColumnDataWriter::next(const RowCursor& row_cursor) {
    for (size_t i = 0; i < _table->num_key_fields(); ++i) {
        char* right = row_cursor.get_field_by_index(i)->get_field_ptr(row_cursor.get_buf());
        if (_column_statistics[i].first->cmp(right) > 0) {
            _column_statistics[i].first->copy(right);
        }

        if (_column_statistics[i].second->cmp(right) < 0) {
            _column_statistics[i].second->copy(right);
        }
    }

    ++_row_index;
}

void ColumnDataWriter::next(const char* row, const Schema* schema) {
    for (size_t i = 0; i < _table->num_key_fields(); ++i) {
        char* right = const_cast<char*>(row + schema->get_col_offset(i));
        if (_column_statistics[i].first->cmp(right) > 0) {
            _column_statistics[i].first->copy(right);
        }

        if (_column_statistics[i].second->cmp(right) < 0) {
            _column_statistics[i].second->copy(right);
        }
    }

    ++_row_index;
}

OLAPStatus ColumnDataWriter::finalize() {
    if (_all_num_rows == 0 && _row_index == 0) {
        _index->set_empty(true);
        return OLAP_SUCCESS;
    }
    OLAPStatus res = _flush_row_block(true);
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("failed to flush data while attaching row cursor.[res=%d]", res);
        return res;
    }

    res = _finalize_segment();
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to finalize segment.[res=%d]", res);
        return res;
    }

    res = _index->add_column_statistics(_column_statistics);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("Fail to set delta pruning![res=%d]", res);
        return res;
    }

    return OLAP_SUCCESS;
}

OLAPStatus ColumnDataWriter::_flush_row_block(bool finalize) {
    if (!_new_segment_created) {
        RETURN_NOT_OK(_init_segment());
    }

    if (_row_index < 1) { return OLAP_SUCCESS; }
    // 与OLAPDataWriter不同,这里不是真的写RowBlock,所以并不需要finalize RowBlock
    // 但考虑到兼容Row Block的使用方式,还是调用了finalize
    OLAPStatus res = _row_block->finalize(_row_index);
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to finalize row block. [num_rows=%u res=%d]",
                _row_index, res);
        return OLAP_ERR_WRITER_ROW_BLOCK_ERROR;
    }

    // 目标是将自己的block按条写入目标block中。
    res = _segment_writer->write_batch(_row_block, &_cursor, finalize);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to write row to segment. [res=%d]", res);
        return OLAP_ERR_WRITER_DATA_WRITE_ERROR;
    }

    // 在Rowset中记录的不是数据文件的偏移,而是block的编号
    if (OLAP_SUCCESS != _index->add_row_block(*_row_block, _block_id++)) {
        OLAP_LOG_WARNING("fail to update index.");
        return OLAP_ERR_WRITER_INDEX_WRITE_ERROR;
    }

    // In order to reuse row_block, clear the row_block after finalize
    _row_block->clear();
    _num_rows += _row_index;
    _all_num_rows += _row_index;
    _row_index = 0;

    return OLAP_SUCCESS;
}

OLAPStatus ColumnDataWriter::_add_segment() {
    std::string file_name;

    if (NULL != _segment_writer) {
        OLAP_LOG_WARNING("previous segment is not finalized before add new segment.");
        return OLAP_ERR_WRITER_SEGMENT_NOT_FINALIZED;
    }

    file_name = _index->construct_data_file_path(_index->rowset_id(), _segment);
    _segment_writer = new(std::nothrow) SegmentWriter(file_name, _table,
            OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE);

    if (NULL == _segment_writer) {
        OLAP_LOG_WARNING("fail to allocate SegmentWriter");
        return OLAP_ERR_MALLOC_ERROR;
    }

    OLAPStatus res = OLAP_SUCCESS;
    if (_is_push_write) {
        res = _segment_writer->init(config::push_write_mbytes_per_sec);
    } else {
        res = _segment_writer->init(
                config::base_compaction_write_mbytes_per_sec);
    }

    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to init segment writer");
        return res;
    }

    ++_segment;
    _block_id = 0;
    return OLAP_SUCCESS;
}

OLAPStatus ColumnDataWriter::_flush_segment_with_verfication() {
    uint64_t segment_size = _segment_writer->estimate_segment_size();
    if (UNLIKELY(segment_size < _max_segment_size)) {
        return OLAP_SUCCESS;
    }

    OLAPStatus res = _finalize_segment();
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to finalize segment. [res=%d]", res);
        return OLAP_ERR_WRITER_DATA_WRITE_ERROR;
    }

    _new_segment_created = false;
    _num_rows = 0;
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

uint64_t ColumnDataWriter::written_bytes() {
    uint64_t size = _segment * _max_segment_size + _segment_writer->estimate_segment_size();
    return size;
}

MemPool* ColumnDataWriter::mem_pool() {
    return _row_block->mem_pool();
}

}  // namespace doris

