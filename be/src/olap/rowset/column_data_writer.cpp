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

#include "olap/rowset/column_data_writer.h"

#include <math.h>

#include "olap/row.h"
#include "olap/row_block.h"
#include "olap/rowset/segment_group.h"
#include "olap/rowset/segment_writer.h"

namespace doris {

ColumnDataWriter* ColumnDataWriter::create(SegmentGroup* segment_group, bool is_push_write,
                                           CompressKind compress_kind, double bloom_filter_fpp) {
    ColumnDataWriter* writer = new (std::nothrow)
            ColumnDataWriter(segment_group, is_push_write, compress_kind, bloom_filter_fpp);
    return writer;
}

ColumnDataWriter::ColumnDataWriter(SegmentGroup* segment_group, bool is_push_write,
                                   CompressKind compress_kind, double bloom_filter_fpp)
        : _segment_group(segment_group),
          _is_push_write(is_push_write),
          _compress_kind(compress_kind),
          _bloom_filter_fpp(bloom_filter_fpp),
          _zone_maps(segment_group->get_num_zone_map_columns(), KeyRange(nullptr, nullptr)),
          _row_index(0),
          _row_block(nullptr),
          _segment_writer(nullptr),
          _num_rows(0),
          _block_id(0),
          _max_segment_size(OLAP_MAX_COLUMN_SEGMENT_FILE_SIZE),
          _segment(0),
          _all_num_rows(0),
          _new_segment_created(false) {}

ColumnDataWriter::~ColumnDataWriter() {
    for (size_t i = 0; i < _zone_maps.size(); ++i) {
        SAFE_DELETE(_zone_maps[i].first);
        SAFE_DELETE(_zone_maps[i].second);
    }
    SAFE_DELETE(_row_block);
    SAFE_DELETE(_segment_writer);
}

OLAPStatus ColumnDataWriter::init() {
    OLAPStatus res = OLAP_SUCCESS;

    for (size_t i = 0; i < _zone_maps.size(); ++i) {
        _zone_maps[i].first = WrapperField::create(_segment_group->get_tablet_schema().column(i));
        DCHECK(_zone_maps[i].first != nullptr) << "fail to create column statistics field.";
        _zone_maps[i].first->set_to_max();

        _zone_maps[i].second = WrapperField::create(_segment_group->get_tablet_schema().column(i));
        DCHECK(_zone_maps[i].second != nullptr) << "fail to create column statistics field.";
        _zone_maps[i].second->set_null();
        _zone_maps[i].second->set_to_min();
    }

    double size = static_cast<double>(OLAP_MAX_COLUMN_SEGMENT_FILE_SIZE);
    size *= OLAP_COLUMN_FILE_SEGMENT_SIZE_SCALE;
    _max_segment_size = static_cast<uint32_t>(lround(size));

    _row_block = new (std::nothrow) RowBlock(&(_segment_group->get_tablet_schema()));

    if (nullptr == _row_block) {
        LOG(WARNING) << "fail to new RowBlock.";
        return OLAP_ERR_MALLOC_ERROR;
    }

    res = _cursor.init(_segment_group->get_tablet_schema());
    if (OLAP_SUCCESS != res) {
        LOG(WARNING) << "fail to initiate row cursor. [res=" << res << "]";
        return res;
    }

    VLOG_NOTICE << "init ColumnData writer. segment_group_id=" << _segment_group->segment_group_id()
                << ", block_row_number=" << _segment_group->get_num_rows_per_row_block();
    RowBlockInfo block_info(0U, _segment_group->get_num_rows_per_row_block());
    block_info.null_supported = true;

    _row_block->init(block_info);
    return OLAP_SUCCESS;
}

OLAPStatus ColumnDataWriter::_init_segment() {
    OLAPStatus res = _add_segment();
    if (OLAP_SUCCESS != res) {
        LOG(WARNING) << "fail to add segment. [res=" << res << "]";
        return res;
    }

    res = _segment_group->add_segment();
    if (OLAP_SUCCESS != res) {
        LOG(WARNING) << "fail to add index segment. [res=" << res << "]";
        return res;
    }

    _new_segment_created = true;
    return res;
}

template <typename RowType>
OLAPStatus ColumnDataWriter::write(const RowType& row) {
    // copy input row to row block
    _row_block->get_row(_row_index, &_cursor);
    copy_row(&_cursor, row, _row_block->mem_pool());
    next(row);
    if (_row_index >= _segment_group->get_num_rows_per_row_block()) {
        if (OLAP_SUCCESS != _flush_row_block(false)) {
            LOG(WARNING) << "failed to flush data while attaching row cursor.";
            return OLAP_ERR_OTHER_ERROR;
        }
        RETURN_NOT_OK(_flush_segment_with_verification());
    }
    return OLAP_SUCCESS;
}

template <typename RowType>
void ColumnDataWriter::next(const RowType& row) {
    for (size_t cid = 0; cid < _segment_group->get_num_zone_map_columns(); ++cid) {
        auto field = row.schema()->column(cid);
        auto cell = row.cell(cid);

        if (field->compare_cell(*_zone_maps[cid].first, cell) > 0) {
            field->direct_copy(_zone_maps[cid].first, cell);
        }

        if (field->compare_cell(*_zone_maps[cid].second, cell) < 0) {
            field->direct_copy(_zone_maps[cid].second, cell);
        }
    }

    ++_row_index;
}

OLAPStatus ColumnDataWriter::finalize() {
    if (_all_num_rows == 0 && _row_index == 0) {
        _segment_group->set_empty(true);
        return OLAP_SUCCESS;
    }

    // Segment which size reaches OLAP_MAX_COLUMN_SEGMENT_FILE_SIZE
    // will be flushed into disk. If the previous segment reach
    // the threshold just right, and been flushed into disk.
    // The following finalize() when closing ColumnDataWriter
    // will generate a non-sense segment.
    // In this scenario, undefined behavior will happens.
    if (_num_rows == 0 && _row_index == 0) {
        // If the two conditions are all satisfied,
        // it dedicates that there is no necessity
        // to generate segment object and file.
        // Return OLAP_SUCCESS is OK.
        return OLAP_SUCCESS;
    }

    OLAPStatus res = _flush_row_block(true);
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("failed to flush data while attaching row cursor.[res=%d]", res);
        return res;
    }

    res = _finalize_segment();
    if (OLAP_SUCCESS != res) {
        LOG(WARNING) << "fail to finalize segment. res=" << res << ", _row_index=" << _row_index
                     << ", _all_num_rows=" << _all_num_rows;
        return res;
    }

    res = _segment_group->add_zone_maps(_zone_maps);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "Fail to set zone_map! res=" << res;
        return res;
    }

    return OLAP_SUCCESS;
}

OLAPStatus ColumnDataWriter::_flush_row_block(bool finalize) {
    if (!_new_segment_created) {
        RETURN_NOT_OK(_init_segment());
    }

    if (_row_index < 1) {
        return OLAP_SUCCESS;
    }
    // 与OLAPDataWriter不同,这里不是真的写RowBlock,所以并不需要finalize RowBlock
    // 但考虑到兼容Row Block的使用方式,还是调用了finalize
    OLAPStatus res = _row_block->finalize(_row_index);
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to finalize row block. [num_rows=%u res=%d]", _row_index, res);
        return OLAP_ERR_WRITER_ROW_BLOCK_ERROR;
    }

    // 目标是将自己的block按条写入目标block中。
    res = _segment_writer->write_batch(_row_block, &_cursor, finalize);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to write row to segment. [res=%d]", res);
        return OLAP_ERR_WRITER_DATA_WRITE_ERROR;
    }

    // 在SegmentGroup中记录的不是数据文件的偏移,而是block的编号
    if (OLAP_SUCCESS != _segment_group->add_row_block(*_row_block, _block_id++)) {
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

    if (nullptr != _segment_writer) {
        OLAP_LOG_WARNING("previous segment is not finalized before add new segment.");
        return OLAP_ERR_WRITER_SEGMENT_NOT_FINALIZED;
    }

    file_name = _segment_group->construct_data_file_path(_segment);
    _segment_writer = new (std::nothrow)
            SegmentWriter(file_name, _segment_group, OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE,
                          _compress_kind, _bloom_filter_fpp);

    if (nullptr == _segment_writer) {
        OLAP_LOG_WARNING("fail to allocate SegmentWriter");
        return OLAP_ERR_MALLOC_ERROR;
    }

    OLAPStatus res = OLAP_SUCCESS;
    if (_is_push_write) {
        res = _segment_writer->init(config::push_write_mbytes_per_sec);
    } else {
        res = _segment_writer->init(config::base_compaction_write_mbytes_per_sec);
    }

    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to init segment writer");
        return res;
    }

    ++_segment;
    _block_id = 0;
    return OLAP_SUCCESS;
}

OLAPStatus ColumnDataWriter::_flush_segment_with_verification() {
    uint64_t segment_size = _segment_writer->estimate_segment_size();
    if (UNLIKELY(segment_size < _max_segment_size)) {
        return OLAP_SUCCESS;
    }

    OLAPStatus res = _finalize_segment();
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to finalize segment. [res=%d]", res);
        return res;
    }

    _new_segment_created = false;
    _num_rows = 0;
    return OLAP_SUCCESS;
}

OLAPStatus ColumnDataWriter::_finalize_segment() {
    OLAPStatus res = OLAP_SUCCESS;
    uint32_t data_segment_size;

    if ((res = _segment_writer->finalize(&data_segment_size)) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to finish segment from olap_data.");
        return res;
    }

    if ((res != _segment_group->finalize_segment(data_segment_size, _num_rows)) != OLAP_SUCCESS) {
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

CompressKind ColumnDataWriter::compress_kind() {
    return _compress_kind;
}

template OLAPStatus ColumnDataWriter::write<RowCursor>(const RowCursor& row);
template OLAPStatus ColumnDataWriter::write<ContiguousRow>(const ContiguousRow& row);

template void ColumnDataWriter::next<RowCursor>(const RowCursor& row);
template void ColumnDataWriter::next<ContiguousRow>(const ContiguousRow& row);

} // namespace doris
