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

#include "olap/rowset/beta_rowset_writer.h"

#include <cmath> // lround
#include <cstdio> // remove
#include <cstring> // strerror_r
#include <ctime> // time

#include "common/config.h"
#include "common/logging.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/segment_v2/segment_writer.h"
#include "olap/row.h" // ContiguousRow
#include "olap/row_cursor.h" // RowCursor

namespace doris {

BetaRowsetWriter::BetaRowsetWriter()
    : _rowset_meta(nullptr),
      _num_segment(0),
      _segment_writer(nullptr),
      _num_rows_written(0),
      _total_data_size(0) {
    auto size = static_cast<double>(OLAP_MAX_COLUMN_SEGMENT_FILE_SIZE);
    size *= OLAP_COLUMN_FILE_SEGMENT_SIZE_SCALE;
    _max_segment_size = static_cast<uint32_t>(lround(size));
}

BetaRowsetWriter::~BetaRowsetWriter() {
    if (!_rowset_build) { // abnormal exit, remove all files generated
        _segment_writer.reset(nullptr); // ensure all files are closed
        for (int i = 0; i < _num_segment; ++i) {
            auto path = BetaRowset::segment_file_path(_context.rowset_path_prefix, _context.rowset_id, i);
            if (::remove(path.c_str()) != 0) {
                char errmsg[64];
                LOG(WARNING) << "failed to delete file. err=" << strerror_r(errno, errmsg, 64)
                             << ", path=" << path;
            }
        }
    }
}

OLAPStatus BetaRowsetWriter::init(const RowsetWriterContext& rowset_writer_context) {
    _context = rowset_writer_context;
    _rowset_meta.reset(new RowsetMeta);
    _rowset_meta->set_rowset_id(_context.rowset_id);
    _rowset_meta->set_partition_id(_context.partition_id);
    _rowset_meta->set_tablet_id(_context.tablet_id);
    _rowset_meta->set_tablet_schema_hash(_context.tablet_schema_hash);
    _rowset_meta->set_rowset_type(_context.rowset_type);
    _rowset_meta->set_rowset_state(_context.rowset_state);
    if (_context.rowset_state == PREPARED || _context.rowset_state == COMMITTED) {
        _is_pending = true;
        _rowset_meta->set_txn_id(_context.txn_id);
        _rowset_meta->set_load_id(_context.load_id);
    } else {
        _rowset_meta->set_version(_context.version);
        _rowset_meta->set_version_hash(_context.version_hash);
    }
    _rowset_meta->set_tablet_uid(_context.tablet_uid);

    return OLAP_SUCCESS;
}

template<typename RowType>
OLAPStatus BetaRowsetWriter::_add_row(const RowType& row) {
    if (PREDICT_FALSE(_segment_writer == nullptr)) {
        RETURN_NOT_OK(_create_segment_writer());
    }
    // TODO update rowset's zonemap
    auto s = _segment_writer->append_row(row);
    if (PREDICT_FALSE(!s.ok())) {
        LOG(WARNING) << "failed to append row: " << s.to_string();
        return OLAP_ERR_WRITER_DATA_WRITE_ERROR;
    }
    if (PREDICT_FALSE(_segment_writer->estimate_segment_size() >= _max_segment_size ||
            _segment_writer->num_rows_written() >= _context.max_rows_per_segment)) {
        RETURN_NOT_OK(_flush_segment_writer());
    }
    _num_rows_written++;
    return OLAP_SUCCESS;
}

template OLAPStatus BetaRowsetWriter::_add_row(const RowCursor& row);
template OLAPStatus BetaRowsetWriter::_add_row(const ContiguousRow& row);

OLAPStatus BetaRowsetWriter::add_rowset(RowsetSharedPtr rowset) {
    assert(rowset->rowset_meta()->rowset_type() == BETA_ROWSET);
    RETURN_NOT_OK(rowset->link_files_to(_context.rowset_path_prefix, _context.rowset_id));
    _num_rows_written += rowset->num_rows();
    _total_data_size += rowset->rowset_meta()->data_disk_size();
    _num_segment += rowset->num_segments();
    // TODO update zonemap
    if (rowset->rowset_meta()->has_delete_predicate()) {
        _rowset_meta->set_delete_predicate(rowset->rowset_meta()->delete_predicate());
    }
    return OLAP_SUCCESS;
}

OLAPStatus BetaRowsetWriter::add_rowset_for_linked_schema_change(RowsetSharedPtr rowset,
                                                                 const SchemaMapping& schema_mapping) {
    // TODO use schema_mapping to transfer zonemap
    return add_rowset(rowset);
}

OLAPStatus BetaRowsetWriter::flush() {
    if (_segment_writer != nullptr) {
        RETURN_NOT_OK(_flush_segment_writer());
    }
    return OLAP_SUCCESS;
}

RowsetSharedPtr BetaRowsetWriter::build() {
    _rowset_meta->set_num_rows(_num_rows_written);
    _rowset_meta->set_total_disk_size(_total_data_size);
    _rowset_meta->set_data_disk_size(_total_data_size);
    _rowset_meta->set_index_disk_size(0); // TODO collect index size
    // TODO write zonemap to meta
    _rowset_meta->set_empty(_num_rows_written == 0);
    _rowset_meta->set_creation_time(time(nullptr));
    _rowset_meta->set_num_segments(_num_segment);
    if (_is_pending) {
        _rowset_meta->set_rowset_state(COMMITTED);
    } else {
        _rowset_meta->set_rowset_state(VISIBLE);
    }

    RowsetSharedPtr rowset;
    auto status = RowsetFactory::create_rowset(_context.tablet_schema,
                                               _context.rowset_path_prefix,
                                               _context.data_dir,
                                               _rowset_meta,
                                               &rowset);
    if (status != OLAP_SUCCESS) {
        LOG(WARNING) << "rowset init failed when build new rowset, res=" << status;
        return nullptr;
    }
    _rowset_build = true;
    return rowset;
}

OLAPStatus BetaRowsetWriter::_create_segment_writer() {
    auto path = BetaRowset::segment_file_path(_context.rowset_path_prefix, _context.rowset_id, _num_segment);
    segment_v2::SegmentWriterOptions writer_options;
    _segment_writer.reset(new segment_v2::SegmentWriter(path, _num_segment, _context.tablet_schema, writer_options));
    // TODO set write_mbytes_per_sec based on writer type (load/base compaction/cumulative compaction)
    auto s = _segment_writer->init(config::push_write_mbytes_per_sec);
    if (!s.ok()) {
        LOG(WARNING) << "failed to init segment writer: " << s.to_string();
        return OLAP_ERR_INIT_FAILED;
    }
    _num_segment++;
    return OLAP_SUCCESS;
}

OLAPStatus BetaRowsetWriter::_flush_segment_writer() {
    uint64_t segment_size;
    auto s = _segment_writer->finalize(&segment_size);
    if (!s.ok()) {
        LOG(WARNING) << "failed to finalize segment: " << s.to_string();
        return OLAP_ERR_WRITER_DATA_WRITE_ERROR;
    }
    // TODO calc index size also
    _total_data_size += segment_size;
    _segment_writer.reset(nullptr);
    return OLAP_SUCCESS;
}

} // namespace doris