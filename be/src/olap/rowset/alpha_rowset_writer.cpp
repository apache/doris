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

#include "olap/rowset/alpha_rowset.h"

#include "olap/rowset/alpha_rowset_writer.h"
#include "olap/rowset/alpha_rowset_meta.h"
#include "olap/rowset/rowset_meta_manager.h"

namespace doris {

AlphaRowsetWriter::AlphaRowsetWriter() :
    _segment_group_id(0),
    _cur_segment_group(nullptr),
    _column_data_writer(nullptr),
    _current_rowset_meta(nullptr),
    _is_pending_rowset(false),
    _num_rows_written(0),
    _is_inited(false),
    _rowset_build(false) { }

AlphaRowsetWriter::~AlphaRowsetWriter() {
    SAFE_DELETE(_column_data_writer);
    if (!_rowset_build) {
        garbage_collection();
    }
    _segment_groups.clear();
}

OLAPStatus AlphaRowsetWriter::init(const RowsetWriterContext& rowset_writer_context) {
    _rowset_writer_context = rowset_writer_context;
    _current_rowset_meta.reset(new AlphaRowsetMeta());
    _current_rowset_meta->set_rowset_id(_rowset_writer_context.rowset_id);
    _current_rowset_meta->set_tablet_id(_rowset_writer_context.tablet_id);
    _current_rowset_meta->set_tablet_schema_hash(_rowset_writer_context.tablet_schema_hash);
    _current_rowset_meta->set_rowset_type(_rowset_writer_context.rowset_type);
    _current_rowset_meta->set_rowset_state(rowset_writer_context.rowset_state);
    RowsetStatePB rowset_state = _rowset_writer_context.rowset_state;
    if (rowset_state == PREPARED
            || rowset_state == COMMITTED) {
        _is_pending_rowset = true;
    }
    if (_is_pending_rowset) {
        _current_rowset_meta->set_txn_id(_rowset_writer_context.txn_id);
        _current_rowset_meta->set_load_id(_rowset_writer_context.load_id);
    } else {
        _current_rowset_meta->set_version(_rowset_writer_context.version);
        _current_rowset_meta->set_version_hash(_rowset_writer_context.version_hash);
    }
    _init();
    _is_inited = true;
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowsetWriter::add_row(RowCursor* row) {
    if (!_is_inited) {
        _init();
        _is_inited = true;
    }
    OLAPStatus status = _column_data_writer->write(row);
    if (status != OLAP_SUCCESS) {
        std::string error_msg = "add row failed";
        LOG(WARNING) << error_msg;
        return status;
    }
    _column_data_writer->next(*row);
    _num_rows_written++;
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowsetWriter::add_row(const char* row, Schema* schema) {
    if (!_is_inited) {
        _init();
        _is_inited = true;
    }
    OLAPStatus status = _column_data_writer->write(row);
    if (status != OLAP_SUCCESS) {
        std::string error_msg = "add row failed";
        LOG(WARNING) << error_msg;
        return status;
    }
    _column_data_writer->next(row, schema);
    _num_rows_written++;
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowsetWriter::add_row_block(RowBlock* row_block) {
    if (!_is_inited) {
        _init();
        _is_inited = true;
    }
    size_t pos = 0;
    row_block->set_pos(pos);
    RowCursor row_cursor;
    row_cursor.init(*(_rowset_writer_context.tablet_schema));
    while (pos < row_block->limit()) {
        row_block->get_row(pos, &row_cursor);
        add_row(&row_cursor);
        row_block->pos_inc();
        pos = row_block->pos();
        _num_rows_written++;
    }
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowsetWriter::add_rowset(RowsetSharedPtr rowset) {
    // this api is for LinkedSchemaChange
    // use create hard link to copy rowset for performance
    // this is feasible because LinkedSchemaChange is done on the same disk
    AlphaRowsetSharedPtr alpha_rowset = std::dynamic_pointer_cast<AlphaRowset>(rowset);
    for (auto& segment_group : alpha_rowset->_segment_groups) {
        RETURN_NOT_OK(segment_group->copy_segments_to_path(_rowset_writer_context.rowset_path_prefix,
                                                           _rowset_writer_context.rowset_id));
        _cur_segment_group->set_empty(segment_group->empty());
        _cur_segment_group->set_num_segments(segment_group->num_segments());
        _cur_segment_group->add_zone_maps_for_linked_schema_change(segment_group->get_zone_maps());
        RETURN_NOT_OK(_cur_segment_group->load());
        _num_rows_written += alpha_rowset->num_rows();
    }
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowsetWriter::flush() {
    DCHECK(_is_inited);
    OLAPStatus status = _column_data_writer->finalize();
    SAFE_DELETE(_column_data_writer);
    _cur_segment_group->load();
    _is_inited = false;
    return status;
}

RowsetSharedPtr AlphaRowsetWriter::build() {
    for (auto& segment_group : _segment_groups) {
        _current_rowset_meta->set_data_disk_size(_current_rowset_meta->data_disk_size() + segment_group->data_size());
        _current_rowset_meta->set_index_disk_size(_current_rowset_meta->index_disk_size() + segment_group->index_size());
        _current_rowset_meta->set_total_disk_size(_current_rowset_meta->total_disk_size()
                + segment_group->index_size() + segment_group->data_size());
        SegmentGroupPB segment_group_pb;
        segment_group_pb.set_segment_group_id(segment_group->segment_group_id());
        segment_group_pb.set_num_segments(segment_group->num_segments());
        segment_group_pb.set_index_size(segment_group->index_size());
        segment_group_pb.set_data_size(segment_group->data_size());
        segment_group_pb.set_num_rows(segment_group->num_rows());
        const std::vector<KeyRange>& zone_maps = segment_group->get_zone_maps();
        if (!zone_maps.empty()) {
            for (size_t i = 0; i < zone_maps.size(); ++i) {
                ZoneMap* new_zone_map = segment_group_pb.add_zone_maps();
                new_zone_map->set_min(zone_maps.at(i).first->to_string());
                new_zone_map->set_max(zone_maps.at(i).second->to_string());
                new_zone_map->set_null_flag(zone_maps.at(i).first->is_null());
            }
        }
        if (_is_pending_rowset) {
            PUniqueId* unique_id = segment_group_pb.mutable_load_id();
            unique_id->set_hi(_rowset_writer_context.load_id.hi());
            unique_id->set_lo(_rowset_writer_context.load_id.lo());
        }
        segment_group_pb.set_empty(segment_group->empty());
        AlphaRowsetMetaSharedPtr alpha_rowset_meta
            = std::dynamic_pointer_cast<AlphaRowsetMeta>(_current_rowset_meta);
        alpha_rowset_meta->add_segment_group(segment_group_pb);
    }
    if (_is_pending_rowset) {
        _current_rowset_meta->set_rowset_state(COMMITTED);
    } else {
        _current_rowset_meta->set_rowset_state(VISIBLE);
    }

    _current_rowset_meta->set_empty(_num_rows_written == 0);
    _current_rowset_meta->set_num_rows(_num_rows_written);
    _current_rowset_meta->set_creation_time(time(NULL));

    RowsetSharedPtr rowset(new(std::nothrow) AlphaRowset(_rowset_writer_context.tablet_schema,
                                    _rowset_writer_context.rowset_path_prefix,
                                    _rowset_writer_context.data_dir, _current_rowset_meta));
    DCHECK(rowset != nullptr) << "new rowset failed when build new rowset";

    OLAPStatus status = rowset->init();
    if (status != OLAP_SUCCESS) {
        LOG(WARNING) << "rowset init failed when build new rowset";
        return nullptr;
    }
    _rowset_build = true;
    return rowset;
}

MemPool* AlphaRowsetWriter::mem_pool() {
    if (_column_data_writer != nullptr) {
        return _column_data_writer->mem_pool();
    } else {
        return nullptr;
    }
}

Version AlphaRowsetWriter::version() {
    return _rowset_writer_context.version;
}

int32_t AlphaRowsetWriter::num_rows() {
    return _num_rows_written;
}

OLAPStatus AlphaRowsetWriter::garbage_collection() {
    for (auto segment_group : _segment_groups) {
        bool ret = segment_group->delete_all_files();
        if (!ret) {
            LOG(WARNING) << "delete segment group files failed."
                         << " tablet id:" << segment_group->get_tablet_id()
                         << ", rowset path:" << segment_group->rowset_path_prefix();
            return OLAP_ERR_ROWSET_DELETE_SEGMENT_GROUP_FILE_FAILED;
        }
    }
    return OLAP_SUCCESS;
}

void AlphaRowsetWriter::_init() {
    if (_is_pending_rowset) {
        _cur_segment_group.reset(new SegmentGroup(
                _rowset_writer_context.tablet_id,
                _rowset_writer_context.rowset_id,
                _rowset_writer_context.tablet_schema,
                _rowset_writer_context.rowset_path_prefix,
                false, _segment_group_id, 0, true,
                _rowset_writer_context.partition_id, _rowset_writer_context.txn_id));
    } else {
        _cur_segment_group.reset(new SegmentGroup(
                _rowset_writer_context.tablet_id,
                _rowset_writer_context.rowset_id,
                _rowset_writer_context.tablet_schema,
                _rowset_writer_context.rowset_path_prefix,
                _rowset_writer_context.version,
                _rowset_writer_context.version_hash,
                false, _segment_group_id, 0));
    }
    DCHECK(_cur_segment_group != nullptr) << "failed to malloc SegmentGroup";
    _cur_segment_group->acquire();
    //_cur_segment_group->set_load_id(_rowset_writer_context.load_id);
    _segment_groups.push_back(_cur_segment_group);

    _column_data_writer = ColumnDataWriter::create(_cur_segment_group.get(), true,
                                                   _rowset_writer_context.tablet_schema->compress_kind(),
                                                   _rowset_writer_context.tablet_schema->bloom_filter_fpp());
    DCHECK(_column_data_writer != nullptr) << "memory error occurs when creating writer";
    _segment_group_id++;
}

} // namespace doris
