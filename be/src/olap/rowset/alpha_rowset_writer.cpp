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
#include "olap/rowset/alpha_rowset_meta.h"
#include "olap/rowset/alpha_rowset.h"

namespace doris {

AlphaRowsetWriter::AlphaRowsetWriter() :
    _segment_group_id(0),
    _cur_segment_group(nullptr),
    _column_data_writer(nullptr),
    _current_rowset_meta(nullptr),
    _is_pending_rowset(false),
    _num_rows_written(0) {
}

OLAPStatus AlphaRowsetWriter::init(const RowsetWriterContext& rowset_writer_context) {
    _rowset_writer_context = rowset_writer_context;
    _current_rowset_meta->set_rowset_id(_rowset_writer_context.rowset_id);
    _current_rowset_meta->set_tablet_id(_rowset_writer_context.tablet_id);
    _current_rowset_meta->set_tablet_schema_hash(_rowset_writer_context.tablet_schema_hash);
    _current_rowset_meta->set_rowset_type(_rowset_writer_context.rowset_type);
    _current_rowset_meta->set_rowset_state(rowset_writer_context.rowset_state);
    _current_rowset_meta->set_rowset_path(_rowset_writer_context.rowset_path_prefix);
    RowsetStatePB rowset_state = _rowset_writer_context.rowset_state;
    if (rowset_state == PREPARED
            || rowset_state == COMMITTED) {
        _is_pending_rowset = true;
    }
    if (is_pending_rowset) {
        _current_rowset_meta->set_txn_id(_rowset_writer_context.txn_id);
        _current_rowset_meta->set_load_id(_rowset_writer_context.load_id);
    } else {
        _current_rowset_meta->set_version(_rowset_writer_context.version);
        _current_rowset_meta->set_version_hash(_rowset_writer_context.version_hash);
    }
    
    _init();
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowsetWriter::add_row(RowCursor* row) {
    OLAPStatus status = _column_data_writer->write(row);
    if (status != OLAP_SUCCESS) {
        std::string error_msg = "add row failed";
        LOG(WARNING) << error_msg;
        return status;
    }
    _column_data_writer->next(*row);
    _is_pending_rowset++;
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowsetWriter::add_row(const char* row, Schema* schema) {
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

OLAPStatus AlphaRowsetBuilder::add_row_block(RowBlock* row_block) {
    size_t pos = 0;
    row_block->set_pos(pos);
    RowCursor row_cursor;
    row_cursor.init(_rowset_builder_context.tablet_schema);
    while (pos < row_block->limit()) {
        row_block->get_row(pos, &row_cursor);
        add_row(&row_cursor);
        row_block->pos_inc();
        pos = row_block->pos();
        _num_rows_written++;
    }
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowsetBuilder::add_rowset(RowsetSharedPtr rowset) {
    // this api is for LinkedSchemaChange
    // use create hard link to copy rowset for performance
    // this is feasible because LinkedSchemaChange is done on the same disk
    AlphaRowset* alpha_rowset = reinterpret_cast<AlphaRowset*>(rowset.get());
    for (auto& segment_group : alpha_rowset->_segment_groups) {
        _init();
        segment_group->copy_segments_to_path(_rowset_builder_context.rowset_path_prefix);
        _cur_segment_group->set_empty(segment_group->empty());
        _cur_segment_group->set_num_segments(segment_group->num_segments());
        _cur_segment_group->add_column_statistics_for_linked_schema_change(segment_group->get_column_statistics());
        _num_rows_written += alpha_rowset->num_rows();
    }
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowsetBuilder::flush() {
    OLAPStatus status = _column_data_writer->finalize();
    SAFE_DELETE(_column_data_writer);
    _cur_segment_group->load();
    _init();
    return status;
}

RowsetSharedPtr AlphaRowsetWriter::build() {
    for (auto& segment_group : _segment_groups) {
        if (_is_pending_rowset) {
            PendingSegmentGroupPB pending_segment_group_pb;
            pending_segment_group_pb.set_pending_segment_group_id(segment_group->segment_group_id());
            pending_segment_group_pb.set_num_segments(segment_group->num_segments());
            PUniqueId* unique_id = pending_segment_group_pb.mutable_load_id();
            unique_id->set_hi(_rowset_writer_context.load_id.hi());
            unique_id->set_lo(_rowset_writer_context.load_id.lo());
            pending_segment_group_pb.set_empty(segment_group->empty());
            const std::vector<KeyRange>* column_statistics = &(segment_group->get_column_statistics());
            if (column_statistics != nullptr) {
                for (size_t i = 0; i < column_statistics->size(); ++i) {
                    ColumnPruning* column_pruning = pending_segment_group_pb.add_column_pruning();
                    column_pruning->set_min(column_statistics->at(i).first->to_string());
                    column_pruning->set_max(column_statistics->at(i).second->to_string());
                    column_pruning->set_null_flag(column_statistics->at(i).first->is_null());
                }
            }
            AlphaRowsetMeta* alpha_rowset_meta = (AlphaRowsetMeta*)_current_rowset_meta.get();
            alpha_rowset_meta->add_pending_segment_group(pending_segment_group_pb);
        } else {
            SegmentGroupPB segment_group_pb;
            segment_group_pb.set_segment_group_id(segment_group->segment_group_id());
            segment_group_pb.set_num_segments(segment_group->num_segments());
            segment_group_pb.set_index_size(segment_group->index_size());
            segment_group_pb.set_data_size(segment_group->data_size());
            segment_group_pb.set_num_rows(segment_group->num_rows());
            const std::vector<KeyRange>* column_statistics = &(segment_group->get_column_statistics());
            if (column_statistics != nullptr) {
                for (size_t i = 0; i < column_statistics->size(); ++i) {
                    ColumnPruning* column_pruning = segment_group_pb.add_column_pruning();
                    column_pruning->set_min(column_statistics->at(i).first->to_string());
                    column_pruning->set_max(column_statistics->at(i).second->to_string());
                    column_pruning->set_null_flag(column_statistics->at(i).first->is_null());
                }
            }
            segment_group_pb.set_empty(segment_group->empty());
            AlphaRowsetMeta* alpha_rowset_meta = reinterpret_cast<AlphaRowsetMeta*>(_current_rowset_meta.get());
            alpha_rowset_meta->add_segment_group(segment_group_pb);
        }
    }
    Rowset* rowset = new AlphaRowset(_rowset_writer_context.tablet_schema,
                                     _rowset_writer_context.rowset_path_prefix,
                                     _rowset_writer_context.data_dir, _current_rowset_meta);
    rowset->init();
    return std::shared_ptr<Rowset>(rowset);
}

OLAPStatus AlphaRowsetBuilder::release() {
    OLAPStatus status = _column_data_writer->finalize();
    SAFE_DELETE(_column_data_writer);
    for (auto segment_group : _segment_groups) {
        segment_group->delete_all_files();
    }
    return status;
}

MemPool* AlphaRowsetBuilder::mem_pool() {
    if (_column_data_writer != nullptr) {
        return _column_data_writer->mem_pool();
    } else {
        return nullptr;
    }
}

Version AlphaRowsetBuilder::version() {
    return _rowset_builder_context.version;
}

int32_t AlphaRowsetBuilder::num_rows() {
    return _num_rows_written;
}

void AlphaRowsetBuilder::_init() {
    _segment_group_id++;
    if (_is_pending_rowset) {
        _cur_segment_group = new SegmentGroup(
                _rowset_writer_context.tablet_id,
                _rowset_writer_context.rowset_id,
                _rowset_writer_context.tablet_schema,
                _rowset_writer_context.rowset_path_prefix,
                false, _segment_group_id, 0, true,
                _rowset_writer_context.partition_id, _rowset_writer_context.txn_id);
    } else {
        _cur_segment_group = new SegmentGroup(
                _rowset_writer_context.tablet_id,
                _rowset_writer_context.rowset_id,
                _rowset_writer_context.tablet_schema,
                _rowset_writer_context.rowset_path_prefix,
                _rowset_writer_context.version,
                _rowset_writer_context.version_hash,
                false, _segment_group_id, 0);
    }
    DCHECK(_cur_segment_group != nullptr) << "failed to malloc SegmentGroup";
    _cur_segment_group->acquire();
    //_cur_segment_group->set_load_id(_rowset_writer_context.load_id);
    _segment_groups.push_back(_cur_segment_group);

    _column_data_writer= ColumnDataWriter::create(_cur_segment_group, true,
                                                  _rowset_writer_context.tablet_schema->compress_kind(),
                                                  _rowset_writer_context.tablet_schema->bloom_filter_fpp());
    DCHECK(_column_data_writer != nullptr) << "memory error occur when creating writer";
}

} // namespace doris