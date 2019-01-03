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

#include "olap/rowset/alpha_rowset_builder.h"
#include "olap/rowset/alpha_rowset_meta.h"
#include "olap/rowset/alpha_rowset.h"

namespace doris {

AlphaRowsetBuilder::AlphaRowsetBuilder() : _segment_group_id(0),
    _cur_segment_group(nullptr),
    _column_data_writer(nullptr),
    _current_rowset_meta(nullptr) {
}

OLAPStatus AlphaRowsetBuilder::init(const RowsetBuilderContext& rowset_builder_context) {
    _rowset_builder_context = rowset_builder_context;
    _init();
    _current_rowset_meta->set_rowset_id(_rowset_builder_context.rowset_id);
    _current_rowset_meta->set_tablet_id(_rowset_builder_context.tablet_id);
    _current_rowset_meta->set_txn_id(_rowset_builder_context.txn_id);
    _current_rowset_meta->set_tablet_schema_hash(_rowset_builder_context.tablet_schema_hash);
    _current_rowset_meta->set_rowset_type(_rowset_builder_context.rowset_type);
    _current_rowset_meta->set_rowset_state(PREPARING);
    _current_rowset_meta->set_rowset_path(_rowset_builder_context.rowset_path_prefix);
    _current_rowset_meta->set_version(_rowset_builder_context.version);
    _current_rowset_meta->set_version_hash(_rowset_builder_context.version_hash);
    _current_rowset_meta->set_load_id(_rowset_builder_context.load_id);
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowsetBuilder::add_row(RowCursor* row) {
    OLAPStatus status = _column_data_writer->attached_by(row);
    if (status != OLAP_SUCCESS) {
        std::string error_msg = "add row failed";
        LOG(WARNING) << error_msg;
        return status; 
    }
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowsetBuilder::flush() {
    _column_data_writer->finalize();
    SAFE_DELETE(_column_data_writer);
    _init();
    return OLAP_SUCCESS;
}

std::shared_ptr<Rowset> AlphaRowsetBuilder::build() {
    // TODO: set total_disk_size/data_disk_size/index_disk_size
    for (auto& segment_group : _segment_groups) {
        PendingSegmentGroupPB pending_segment_group_pb;
        pending_segment_group_pb.set_pending_segment_group_id(segment_group->segment_group_id());
        pending_segment_group_pb.set_num_segments(segment_group->num_segments());
        PUniqueId* unique_id = pending_segment_group_pb.mutable_load_id();
        unique_id->set_hi(_rowset_builder_context.load_id.hi());
        unique_id->set_lo(_rowset_builder_context.load_id.lo());
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
    }
    Rowset* rowset = new(std::nothrow) AlphaRowset(_rowset_builder_context.tablet_schema,
            _rowset_builder_context.num_key_fields, _rowset_builder_context.num_short_key_fields,
            _rowset_builder_context.num_rows_per_row_block, _rowset_builder_context.rowset_path_prefix,
            _current_rowset_meta);
    rowset->init();
    return std::shared_ptr<Rowset>(rowset);
}

void AlphaRowsetBuilder::_init() {
    _segment_group_id++;
    _cur_segment_group = new SegmentGroup(_rowset_builder_context.tablet_id,
            _rowset_builder_context.rowset_id,
            _rowset_builder_context.tablet_schema,
            _rowset_builder_context.num_key_fields,
            _rowset_builder_context.num_short_key_fields,
            _rowset_builder_context.num_rows_per_row_block,
            _rowset_builder_context.rowset_path_prefix,
            false, _segment_group_id, 0, true,
            _rowset_builder_context.partition_id, _rowset_builder_context.txn_id);
    DCHECK(_cur_segment_group != nullptr) << "failed to malloc SegmentGroup";
    _cur_segment_group->acquire();
    _cur_segment_group->set_load_id(_rowset_builder_context.load_id);
    _segment_groups.push_back(_cur_segment_group);

    _column_data_writer = ColumnDataWriter::create(_cur_segment_group, true,
            _rowset_builder_context.compress_kind, _rowset_builder_context.bloom_filter_fpp);
    DCHECK(_column_data_writer != nullptr) << "memory error occur when creating writer";
}

}  // namespace doris
