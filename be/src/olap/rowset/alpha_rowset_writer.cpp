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

#include "olap/rowset/alpha_rowset_writer.h"

#include "olap/row.h"
#include "olap/rowset/alpha_rowset.h"
#include "olap/rowset/alpha_rowset_meta.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta_manager.h"

namespace doris {

AlphaRowsetWriter::AlphaRowsetWriter()
        : _segment_group_id(0),
          _cur_segment_group(nullptr),
          _column_data_writer(nullptr),
          _current_rowset_meta(nullptr),
          _is_pending_rowset(false),
          _num_rows_written(0),
          _rowset_build(false),
          _writer_state(WRITER_CREATED),
          _need_column_data_writer(true) {}

AlphaRowsetWriter::~AlphaRowsetWriter() {
    SAFE_DELETE(_column_data_writer);
    if (!_rowset_build) {
        _garbage_collection();
    }
    for (auto& segment_group : _segment_groups) {
        segment_group->release();
        delete segment_group;
    }
    _segment_groups.clear();
}

OLAPStatus AlphaRowsetWriter::init(const RowsetWriterContext& rowset_writer_context) {
    _rowset_writer_context = rowset_writer_context;
    _current_rowset_meta.reset(new (std::nothrow) AlphaRowsetMeta());
    _current_rowset_meta->set_rowset_id(_rowset_writer_context.rowset_id);
    _current_rowset_meta->set_partition_id(_rowset_writer_context.partition_id);
    _current_rowset_meta->set_tablet_uid(_rowset_writer_context.tablet_uid);
    _current_rowset_meta->set_tablet_id(_rowset_writer_context.tablet_id);
    _current_rowset_meta->set_tablet_schema_hash(_rowset_writer_context.tablet_schema_hash);
    _current_rowset_meta->set_rowset_type(_rowset_writer_context.rowset_type);
    _current_rowset_meta->set_rowset_state(rowset_writer_context.rowset_state);
    _current_rowset_meta->set_segments_overlap(rowset_writer_context.segments_overlap);
    RowsetStatePB rowset_state = _rowset_writer_context.rowset_state;
    if (rowset_state == PREPARED || rowset_state == COMMITTED) {
        _is_pending_rowset = true;
    }
    if (_is_pending_rowset) {
        _current_rowset_meta->set_txn_id(_rowset_writer_context.txn_id);
        _current_rowset_meta->set_load_id(_rowset_writer_context.load_id);
    } else {
        _current_rowset_meta->set_version(_rowset_writer_context.version);
    }
    RETURN_NOT_OK(_init());
    return OLAP_SUCCESS;
}

template <typename RowType>
OLAPStatus AlphaRowsetWriter::_add_row(const RowType& row) {
    if (_writer_state != WRITER_INITED) {
        RETURN_NOT_OK(_init());
    }
    OLAPStatus status = _column_data_writer->write(row);
    if (status != OLAP_SUCCESS) {
        std::string error_msg = "add row failed";
        LOG(WARNING) << error_msg;
        return status;
    }
    ++_num_rows_written;
    return OLAP_SUCCESS;
}

template OLAPStatus AlphaRowsetWriter::_add_row(const RowCursor& row);
template OLAPStatus AlphaRowsetWriter::_add_row(const ContiguousRow& row);

OLAPStatus AlphaRowsetWriter::add_rowset(RowsetSharedPtr rowset) {
    _need_column_data_writer = false;
    // this api is for clone
    AlphaRowsetSharedPtr alpha_rowset = std::dynamic_pointer_cast<AlphaRowset>(rowset);
    for (auto& segment_group : alpha_rowset->_segment_groups) {
        RETURN_NOT_OK(_init());
        RETURN_NOT_OK(segment_group->link_segments_to_path(
                _rowset_writer_context.path_desc.filepath, _rowset_writer_context.rowset_id));
        _cur_segment_group->set_empty(segment_group->empty());
        _cur_segment_group->set_num_segments(segment_group->num_segments());
        _cur_segment_group->add_zone_maps(segment_group->get_zone_maps());
        RETURN_NOT_OK(flush());
        _num_rows_written += segment_group->num_rows();
    }
    // process delete predicate
    if (rowset->rowset_meta()->has_delete_predicate()) {
        _current_rowset_meta->set_delete_predicate(rowset->rowset_meta()->delete_predicate());
    }
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowsetWriter::add_rowset_for_linked_schema_change(
        RowsetSharedPtr rowset, const SchemaMapping& schema_mapping) {
    _need_column_data_writer = false;
    // this api is for LinkedSchemaChange
    // use create hard link to copy rowset for performance
    // this is feasible because LinkedSchemaChange is done on the same disk
    AlphaRowsetSharedPtr alpha_rowset = std::dynamic_pointer_cast<AlphaRowset>(rowset);
    for (auto& segment_group : alpha_rowset->_segment_groups) {
        RETURN_NOT_OK(_init());
        RETURN_NOT_OK(segment_group->link_segments_to_path(
                _rowset_writer_context.path_desc.filepath, _rowset_writer_context.rowset_id));
        _cur_segment_group->set_empty(segment_group->empty());
        _cur_segment_group->set_num_segments(segment_group->num_segments());
        _cur_segment_group->add_zone_maps_for_linked_schema_change(segment_group->get_zone_maps(),
                                                                   schema_mapping);
        RETURN_NOT_OK(flush());
        _num_rows_written += segment_group->num_rows();
    }
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowsetWriter::flush() {
    if (_writer_state == WRITER_FLUSHED) {
        return OLAP_SUCCESS;
    }
    DCHECK(_writer_state == WRITER_INITED);
    if (_need_column_data_writer) {
        // column_data_writer finalize will call segment_group->set_empty()
        RETURN_NOT_OK(_column_data_writer->finalize());
    }
    SAFE_DELETE(_column_data_writer);
    _writer_state = WRITER_FLUSHED;
    return OLAP_SUCCESS;
}

RowsetSharedPtr AlphaRowsetWriter::build() {
    if (_current_rowset_meta->rowset_id().version == 0) {
        LOG(WARNING) << "invalid rowset id, version == 0, rowset id="
                     << _current_rowset_meta->rowset_id().to_string();
        return nullptr;
    }
    if (_writer_state != WRITER_FLUSHED) {
        LOG(WARNING) << "invalid writer state before build, state:" << _writer_state;
        return nullptr;
    }
    int total_num_segments = 0;
    for (auto& segment_group : _segment_groups) {
        if (segment_group->load() != OLAP_SUCCESS) {
            return nullptr;
        }
        if (!segment_group->check()) {
            return nullptr;
        }
        _current_rowset_meta->set_data_disk_size(_current_rowset_meta->data_disk_size() +
                                                 segment_group->data_size());
        _current_rowset_meta->set_index_disk_size(_current_rowset_meta->index_disk_size() +
                                                  segment_group->index_size());
        _current_rowset_meta->set_total_disk_size(_current_rowset_meta->total_disk_size() +
                                                  segment_group->index_size() +
                                                  segment_group->data_size());
        SegmentGroupPB segment_group_pb;
        segment_group_pb.set_segment_group_id(segment_group->segment_group_id());
        segment_group_pb.set_num_segments(segment_group->num_segments());
        total_num_segments += segment_group->num_segments();
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
        AlphaRowsetMetaSharedPtr alpha_rowset_meta =
                std::dynamic_pointer_cast<AlphaRowsetMeta>(_current_rowset_meta);
        alpha_rowset_meta->add_segment_group(segment_group_pb);
    }
    _current_rowset_meta->set_num_segments(total_num_segments);
    if (total_num_segments <= 1) {
        _current_rowset_meta->set_segments_overlap(NONOVERLAPPING);
    }
    if (_is_pending_rowset) {
        _current_rowset_meta->set_rowset_state(COMMITTED);
    } else {
        _current_rowset_meta->set_rowset_state(VISIBLE);
    }

    _current_rowset_meta->set_empty(_num_rows_written == 0);
    _current_rowset_meta->set_num_rows(_num_rows_written);
    _current_rowset_meta->set_creation_time(time(nullptr));

    // validate rowset arguments before create rowset
    bool ret = _validate_rowset();
    if (!ret) {
        LOG(FATAL) << "validate rowset arguments failed";
        return nullptr;
    }

    RowsetSharedPtr rowset;
    auto status = RowsetFactory::create_rowset(_rowset_writer_context.tablet_schema,
                                               _rowset_writer_context.path_desc,
                                               _current_rowset_meta, &rowset);
    if (status != OLAP_SUCCESS) {
        LOG(WARNING) << "rowset init failed when build new rowset, res=" << status;
        return nullptr;
    }
    _rowset_build = true;
    return rowset;
}

OLAPStatus AlphaRowsetWriter::_garbage_collection() {
    for (auto& segment_group : _segment_groups) {
        bool ret = segment_group->delete_all_files();
        if (!ret) {
            LOG(WARNING) << "delete segment group files failed."
                         << " tablet id:" << segment_group->get_tablet_id()
                         << ", rowset path:" << segment_group->rowset_path_prefix();
            return OLAP_ERR_ROWSET_DELETE_FILE_FAILED;
        }
    }
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowsetWriter::_init() {
    if (_writer_state == WRITER_INITED) {
        return OLAP_SUCCESS;
    }
    if (_is_pending_rowset) {
        _cur_segment_group = new (std::nothrow) SegmentGroup(
                _rowset_writer_context.tablet_id, _rowset_writer_context.rowset_id,
                _rowset_writer_context.tablet_schema, _rowset_writer_context.path_desc.filepath,
                false, _segment_group_id, 0, true, _rowset_writer_context.partition_id,
                _rowset_writer_context.txn_id);
    } else {
        _cur_segment_group = new (std::nothrow) SegmentGroup(
                _rowset_writer_context.tablet_id, _rowset_writer_context.rowset_id,
                _rowset_writer_context.tablet_schema, _rowset_writer_context.path_desc.filepath,
                _rowset_writer_context.version, false,
                _segment_group_id, 0);
    }
    DCHECK(_cur_segment_group != nullptr) << "failed to malloc SegmentGroup";
    _cur_segment_group->acquire();
    //_cur_segment_group->set_load_id(_rowset_writer_context.load_id);
    _segment_groups.push_back(_cur_segment_group);

    _column_data_writer = ColumnDataWriter::create(
            _cur_segment_group, true, _rowset_writer_context.tablet_schema->compress_kind(),
            _rowset_writer_context.tablet_schema->bloom_filter_fpp());
    DCHECK(_column_data_writer != nullptr) << "memory error occurs when creating writer";
    OLAPStatus res = _column_data_writer->init();
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "column data writer init failed";
        return res;
    }

    _segment_group_id++;
    _writer_state = WRITER_INITED;
    return OLAP_SUCCESS;
}

bool AlphaRowsetWriter::_validate_rowset() {
    if (_is_pending_rowset) {
        int64_t partition_id = _current_rowset_meta->partition_id();
        if (partition_id <= 0) {
            LOG(WARNING) << "invalid partition id:" << partition_id << " for pending rowset."
                         << ", rowset_id:" << _current_rowset_meta->rowset_id()
                         << ", tablet_id:" << _current_rowset_meta->tablet_id()
                         << ", schema_hash:" << _current_rowset_meta->tablet_schema_hash();
            return false;
        }
    }
    int64_t num_rows = 0;
    for (auto& segment_group : _segment_groups) {
        num_rows += segment_group->num_rows();
    }
    if (num_rows != _current_rowset_meta->num_rows()) {
        LOG(WARNING) << "num_rows between rowset and segment_groups do not match. "
                     << "num_rows of segment_groups:" << num_rows
                     << ", num_rows of rowset:" << _current_rowset_meta->num_rows();

        return false;
    }
    return true;
}

} // namespace doris
