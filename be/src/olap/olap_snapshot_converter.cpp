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

#include "olap/olap_snapshot_converter.h"

#include "olap/rowset/alpha_rowset.h"
#include "olap/rowset/alpha_rowset_meta.h"
#include "olap/rowset/rowset_id_generator.h"
#include "olap/storage_engine.h"

namespace doris {

OLAPStatus OlapSnapshotConverter::convert_to_pdelta(const RowsetMetaPB& rowset_meta_pb,
                                                    PDelta* delta) {
    if (!rowset_meta_pb.has_start_version()) {
        LOG(FATAL) << "rowset does not have start_version."
                   << " rowset id = " << rowset_meta_pb.rowset_id();
    }
    delta->set_start_version(rowset_meta_pb.start_version());
    if (!rowset_meta_pb.has_end_version()) {
        LOG(FATAL) << "rowset does not have end_version."
                   << " rowset id = " << rowset_meta_pb.rowset_id();
    }
    delta->set_end_version(rowset_meta_pb.end_version());
    if (!rowset_meta_pb.has_version_hash()) {
        LOG(FATAL) << "rowset does not have version_hash."
                   << " rowset id = " << rowset_meta_pb.rowset_id();
    }
    delta->set_version_hash(rowset_meta_pb.version_hash());
    if (!rowset_meta_pb.has_creation_time()) {
        LOG(FATAL) << "rowset does not have creation_time."
                   << " rowset id = " << rowset_meta_pb.rowset_id();
    }
    delta->set_creation_time(rowset_meta_pb.creation_time());
    AlphaRowsetExtraMetaPB extra_meta_pb = rowset_meta_pb.alpha_rowset_extra_meta_pb();

    for (auto& segment_group : extra_meta_pb.segment_groups()) {
        SegmentGroupPB* new_segment_group = delta->add_segment_group();
        *new_segment_group = segment_group;
    }
    if (rowset_meta_pb.has_delete_predicate()) {
        DeletePredicatePB* delete_condition = delta->mutable_delete_condition();
        *delete_condition = rowset_meta_pb.delete_predicate();
    }
    return OLAP_SUCCESS;
}

OLAPStatus OlapSnapshotConverter::convert_to_rowset_meta(const PDelta& delta,
                                                         const RowsetId& rowset_id,
                                                         int64_t tablet_id, int32_t schema_hash,
                                                         RowsetMetaPB* rowset_meta_pb) {
    rowset_meta_pb->set_rowset_id(0);
    rowset_meta_pb->set_rowset_id_v2(rowset_id.to_string());
    rowset_meta_pb->set_tablet_id(tablet_id);
    rowset_meta_pb->set_tablet_schema_hash(schema_hash);
    rowset_meta_pb->set_rowset_type(RowsetTypePB::ALPHA_ROWSET);
    rowset_meta_pb->set_rowset_state(RowsetStatePB::VISIBLE);
    rowset_meta_pb->set_start_version(delta.start_version());
    rowset_meta_pb->set_end_version(delta.end_version());
    rowset_meta_pb->set_version_hash(delta.version_hash());

    bool empty = true;
    int64_t num_rows = 0;
    int64_t index_size = 0;
    int64_t data_size = 0;
    AlphaRowsetExtraMetaPB* extra_meta_pb = rowset_meta_pb->mutable_alpha_rowset_extra_meta_pb();
    for (auto& segment_group : delta.segment_group()) {
        SegmentGroupPB* new_segment_group = extra_meta_pb->add_segment_groups();
        *new_segment_group = segment_group;
        // if segment group does not has empty property, then it is not empty
        // if segment group's empty == false, then it is not empty
        if (!segment_group.has_empty() || !segment_group.empty()) {
            empty = false;
        }
        num_rows += segment_group.num_rows();
        index_size += segment_group.index_size();
        data_size += segment_group.data_size();
    }

    rowset_meta_pb->set_empty(empty);
    rowset_meta_pb->set_num_rows(num_rows);
    rowset_meta_pb->set_data_disk_size(data_size);
    rowset_meta_pb->set_index_disk_size(index_size);
    rowset_meta_pb->set_total_disk_size(data_size + index_size);
    if (delta.has_delete_condition()) {
        DeletePredicatePB* delete_condition = rowset_meta_pb->mutable_delete_predicate();
        *delete_condition = delta.delete_condition();
    }
    rowset_meta_pb->set_creation_time(delta.creation_time());
    LOG(INFO) << "convert visible delta start_version = " << delta.start_version()
              << " end_version = " << delta.end_version()
              << " version_hash = " << delta.version_hash() << " to rowset id = " << rowset_id
              << " tablet_id = " << tablet_id;
    return OLAP_SUCCESS;
}

OLAPStatus OlapSnapshotConverter::convert_to_rowset_meta(const PPendingDelta& pending_delta,
                                                         const RowsetId& rowset_id,
                                                         int64_t tablet_id, int32_t schema_hash,
                                                         RowsetMetaPB* rowset_meta_pb) {
    rowset_meta_pb->set_rowset_id(0);
    rowset_meta_pb->set_rowset_id_v2(rowset_id.to_string());
    rowset_meta_pb->set_tablet_id(tablet_id);
    rowset_meta_pb->set_tablet_schema_hash(schema_hash);
    rowset_meta_pb->set_rowset_type(RowsetTypePB::ALPHA_ROWSET);
    rowset_meta_pb->set_rowset_state(RowsetStatePB::COMMITTED);
    rowset_meta_pb->set_partition_id(pending_delta.partition_id());
    rowset_meta_pb->set_txn_id(pending_delta.transaction_id());
    rowset_meta_pb->set_creation_time(pending_delta.creation_time());

    bool empty = true;
    int64_t num_rows = 0;
    int64_t index_size = 0;
    int64_t data_size = 0;
    AlphaRowsetExtraMetaPB* extra_meta_pb = rowset_meta_pb->mutable_alpha_rowset_extra_meta_pb();
    for (auto& pending_segment_group : pending_delta.pending_segment_group()) {
        SegmentGroupPB* new_segment_group = extra_meta_pb->add_segment_groups();
        new_segment_group->set_segment_group_id(pending_segment_group.pending_segment_group_id());
        new_segment_group->set_num_segments(pending_segment_group.num_segments());
        new_segment_group->set_index_size(0);
        new_segment_group->set_data_size(0);
        new_segment_group->set_num_rows(0);
        for (auto& pending_zone_map : pending_segment_group.zone_maps()) {
            ZoneMap* zone_map = new_segment_group->add_zone_maps();
            *zone_map = pending_zone_map;
        }
        new_segment_group->set_empty(pending_segment_group.empty());
        PUniqueId* load_id = new_segment_group->mutable_load_id();
        *load_id = pending_segment_group.load_id();

        if (!pending_segment_group.empty()) {
            empty = false;
        }
    }

    rowset_meta_pb->set_empty(empty);
    rowset_meta_pb->set_num_rows(num_rows);
    rowset_meta_pb->set_data_disk_size(data_size);
    rowset_meta_pb->set_index_disk_size(index_size);
    rowset_meta_pb->set_total_disk_size(data_size + index_size);
    if (pending_delta.has_delete_condition()) {
        DeletePredicatePB* delete_condition = rowset_meta_pb->mutable_delete_predicate();
        *delete_condition = pending_delta.delete_condition();
    }
    rowset_meta_pb->set_creation_time(pending_delta.creation_time());
    LOG(INFO) << "convert pending delta txn id = " << pending_delta.transaction_id()
              << " tablet_id = " << tablet_id << " schema_hash = " << schema_hash
              << " to rowset id = " << rowset_id;
    return OLAP_SUCCESS;
}

OLAPStatus OlapSnapshotConverter::to_column_pb(const ColumnMessage& column_msg,
                                               ColumnPB* column_pb) {
    if (column_msg.has_unique_id()) {
        column_pb->set_unique_id(column_msg.unique_id());
    }
    column_pb->set_name(column_msg.name());
    column_pb->set_type(column_msg.type());
    column_pb->set_is_key(column_msg.is_key());
    column_pb->set_aggregation(column_msg.aggregation());
    if (column_msg.has_is_allow_null()) {
        column_pb->set_is_nullable(column_msg.is_allow_null());
    }
    if (column_msg.has_default_value()) {
        column_pb->set_default_value(column_msg.default_value());
    }
    if (column_msg.has_precision()) {
        column_pb->set_precision(column_msg.precision());
    }
    if (column_msg.has_frac()) {
        column_pb->set_frac(column_msg.frac());
    }
    column_pb->set_length(column_msg.length());
    if (column_msg.has_index_length()) {
        column_pb->set_index_length(column_msg.index_length());
    }
    if (column_msg.has_is_bf_column()) {
        column_pb->set_is_bf_column(column_msg.is_bf_column());
    }
    if (column_msg.has_has_bitmap_index()) {
        column_pb->set_has_bitmap_index(column_msg.has_bitmap_index());
    }
    // TODO(ygl) calculate column id from column list
    // column_pb->set_referenced_column_id(column_msg.());

    if (column_msg.has_referenced_column()) {
        column_pb->set_referenced_column(column_msg.referenced_column());
    }
    return OLAP_SUCCESS;
}

OLAPStatus OlapSnapshotConverter::to_column_msg(const ColumnPB& column_pb,
                                                ColumnMessage* column_msg) {
    if (!column_pb.has_name()) {
        LOG(FATAL) << "column pb does not have name"
                   << " column id " << column_pb.unique_id();
    }
    column_msg->set_name(column_pb.name());
    column_msg->set_type(column_pb.type());
    if (!column_pb.has_aggregation()) {
        LOG(FATAL) << "column pb does not have aggregation"
                   << " column id " << column_pb.unique_id();
    }
    column_msg->set_aggregation(column_pb.aggregation());
    if (!column_pb.has_length()) {
        LOG(FATAL) << "column pb does not have length"
                   << " column id " << column_pb.unique_id();
    }
    column_msg->set_length(column_pb.length());
    if (!column_pb.has_is_key()) {
        LOG(FATAL) << "column pb does not have is_key"
                   << " column id " << column_pb.unique_id();
    }
    column_msg->set_is_key(column_pb.is_key());
    if (column_pb.has_default_value()) {
        column_msg->set_default_value(column_pb.default_value());
    }
    if (column_pb.has_referenced_column()) {
        column_msg->set_referenced_column(column_pb.referenced_column());
    }
    if (column_pb.has_index_length()) {
        column_msg->set_index_length(column_pb.index_length());
    }
    if (column_pb.has_precision()) {
        column_msg->set_precision(column_pb.precision());
    }
    if (column_pb.has_frac()) {
        column_msg->set_frac(column_pb.frac());
    }
    if (column_pb.has_is_nullable()) {
        column_msg->set_is_allow_null(column_pb.is_nullable());
    }
    column_msg->set_unique_id(column_pb.unique_id());
    if (column_pb.has_is_bf_column()) {
        column_msg->set_is_bf_column(column_pb.is_bf_column());
    }
    if (column_pb.has_has_bitmap_index()) {
        column_msg->set_has_bitmap_index(column_pb.has_bitmap_index());
    }
    column_msg->set_is_root_column(true);
    return OLAP_SUCCESS;
}

OLAPStatus OlapSnapshotConverter::to_alter_tablet_pb(
        const SchemaChangeStatusMessage& schema_change_msg, AlterTabletPB* alter_tablet_pb) {
    alter_tablet_pb->set_related_tablet_id(schema_change_msg.related_tablet_id());
    alter_tablet_pb->set_related_schema_hash(schema_change_msg.related_schema_hash());
    alter_tablet_pb->set_alter_type(
            static_cast<AlterTabletType>(schema_change_msg.schema_change_type()));
    if (schema_change_msg.versions_to_changed().size() == 0) {
        alter_tablet_pb->set_alter_state(AlterTabletState::ALTER_FINISHED);
    } else {
        alter_tablet_pb->set_alter_state(AlterTabletState::ALTER_FAILED);
    }
    return OLAP_SUCCESS;
}

OLAPStatus OlapSnapshotConverter::save(const string& file_path,
                                       const OLAPHeaderMessage& olap_header) {
    DCHECK(!file_path.empty());

    FileHeader<OLAPHeaderMessage> file_header;
    FileHandler file_handler;

    if (file_handler.open_with_mode(file_path.c_str(), O_CREAT | O_WRONLY | O_TRUNC,
                                    S_IRUSR | S_IWUSR) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to open header file. file='" << file_path;
        return OLAP_ERR_IO_ERROR;
    }

    try {
        file_header.mutable_message()->CopyFrom(olap_header);
    } catch (...) {
        LOG(WARNING) << "fail to copy protocol buffer object. file='" << file_path;
        return OLAP_ERR_OTHER_ERROR;
    }

    if (file_header.prepare(&file_handler) != OLAP_SUCCESS ||
        file_header.serialize(&file_handler) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to serialize to file header. file='" << file_path;
        return OLAP_ERR_SERIALIZE_PROTOBUF_ERROR;
    }

    return OLAP_SUCCESS;
}

void OlapSnapshotConverter::_modify_old_segment_group_id(RowsetMetaPB& rowset_meta) {
    if (!rowset_meta.has_alpha_rowset_extra_meta_pb()) {
        return;
    }
    AlphaRowsetExtraMetaPB* alpha_rowset_extra_meta_pb =
            rowset_meta.mutable_alpha_rowset_extra_meta_pb();
    for (auto& segment_group_pb : alpha_rowset_extra_meta_pb->segment_groups()) {
        if (segment_group_pb.segment_group_id() == -1) {
            // check if segment groups size == 1
            if (alpha_rowset_extra_meta_pb->segment_groups().size() != 1) {
                LOG(FATAL) << "the rowset has a segment group's id == -1 but it contains more than "
                              "one segment group"
                           << " it should not happen";
            }
            (const_cast<SegmentGroupPB&>(segment_group_pb)).set_segment_group_id(0);
        }
    }
}

} // namespace doris
