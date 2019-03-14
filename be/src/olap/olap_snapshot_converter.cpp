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
#include "olap/rowset/rowset_id_generator.h"

namespace doris {

OLAPStatus OlapSnapshotConverter::to_olap_header(const TabletMetaPB& tablet_meta_pb, OLAPHeaderMessage* olap_header) {
    olap_header->set_num_rows_per_data_block(tablet_meta_pb.schema().num_rows_per_row_block());
    olap_header->set_cumulative_layer_point(tablet_meta_pb.cumulative_layer_point());
    olap_header->set_num_short_key_fields(tablet_meta_pb.schema().num_short_key_columns());

    for (auto& column : tablet_meta_pb.schema().column()) {
        ColumnMessage* column_msg = olap_header->add_column();
        to_column_msg(column, column_msg);
    }

    olap_header->set_creation_time(tablet_meta_pb.creation_time());
    olap_header->set_data_file_type(DataFileType::COLUMN_ORIENTED_FILE);
    olap_header->set_next_column_unique_id(tablet_meta_pb.schema().next_column_unique_id());
    olap_header->set_compress_kind(tablet_meta_pb.schema().compress_kind());
   
    olap_header->set_bf_fpp(tablet_meta_pb.schema().bf_fpp());
    olap_header->set_keys_type(tablet_meta_pb.schema().keys_type());

    for (auto& rs_meta : tablet_meta_pb.rs_metas()) {
        PDelta* pdelta = olap_header->add_delta();
        convert_to_pdelta(rs_meta, pdelta);
    }
    // not add pending delta, it is usedless in clone or backup restore
    for (auto& inc_rs_meta : tablet_meta_pb.inc_rs_metas()) {
        PDelta* pdelta = olap_header->add_incremental_delta();
        convert_to_pdelta(inc_rs_meta, pdelta);
    }

    olap_header->set_in_restore_mode(tablet_meta_pb.in_restore_mode());
    olap_header->set_tablet_id(tablet_meta_pb.tablet_id());
    olap_header->set_schema_hash(tablet_meta_pb.schema_hash());
    olap_header->set_shard(tablet_meta_pb.shard_id());
    return OLAP_SUCCESS;
}

OLAPStatus OlapSnapshotConverter::to_tablet_meta_pb(const OLAPHeaderMessage& olap_header, TabletMetaPB* tablet_meta_pb, 
                                                  vector<RowsetMetaPB>* pending_rowsets, DataDir* data_dir) {
    tablet_meta_pb->set_tablet_id(olap_header.tablet_id());
    tablet_meta_pb->set_schema_hash(olap_header.schema_hash());
    tablet_meta_pb->set_shard_id(olap_header.shard());
    tablet_meta_pb->set_creation_time(olap_header.creation_time());
    tablet_meta_pb->set_cumulative_layer_point(olap_header.cumulative_layer_point());

    TabletSchemaPB* schema = tablet_meta_pb->mutable_schema();
    for (auto& column_msg : olap_header.column()) {
        ColumnPB* column_pb = schema->add_column();
        to_column_pb(column_msg, column_pb);
    }
    schema->set_keys_type(olap_header.keys_type());
    schema->set_num_short_key_columns(olap_header.num_short_key_fields());
    schema->set_num_rows_per_row_block(olap_header.num_rows_per_data_block());
    schema->set_compress_kind(olap_header.compress_kind());
    schema->set_bf_fpp(olap_header.bf_fpp());
    schema->set_next_column_unique_id(olap_header.next_column_unique_id());

    std::unordered_map<Version, RowsetMetaPB*, HashOfVersion> _rs_version_map;
    for (auto& delta : olap_header.delta()) {
        RowsetMetaPB* rowset_meta = tablet_meta_pb->add_rs_metas();
        RowsetId next_id;
        RETURN_NOT_OK(data_dir->next_id(&next_id));
        convert_to_rowset_meta(delta, next_id, olap_header.tablet_id(), olap_header.schema_hash(), rowset_meta);
        Version rowset_version = { delta.start_version(), delta.end_version() };
        _rs_version_map[rowset_version] = rowset_meta;
    }

    for (auto& inc_delta : olap_header.incremental_delta()) {
        // check if inc delta already exist in delta
        Version rowset_version = { inc_delta.start_version(), inc_delta.end_version() };
        auto exist_rs = _rs_version_map.find(rowset_version); 
        if (exist_rs != _rs_version_map.end()) {
            RowsetMetaPB* rowset_meta = tablet_meta_pb->add_inc_rs_metas();
            *rowset_meta = *(exist_rs->second);
            continue;
        }
        RowsetMetaPB* rowset_meta = tablet_meta_pb->add_inc_rs_metas();
        RowsetId next_id;
        RETURN_NOT_OK(data_dir->next_id(&next_id));
        convert_to_rowset_meta(inc_delta, next_id, olap_header.tablet_id(), olap_header.schema_hash(), rowset_meta);
    }

    for (auto& pending_delta : olap_header.pending_delta()) {
        RowsetMetaPB rowset_meta;
        RowsetId next_id;
        RETURN_NOT_OK(data_dir->next_id(&next_id));
        convert_to_rowset_meta(pending_delta, next_id, olap_header.tablet_id(), olap_header.schema_hash(), &rowset_meta);
        pending_rowsets->emplace_back(std::move(rowset_meta));
    }
    if (olap_header.has_schema_change_status()) {
        AlterTabletPB* alter_tablet_pb = tablet_meta_pb->mutable_alter_tablet_task();
        to_alter_tablet_pb(olap_header.schema_change_status(), alter_tablet_pb);
    }
    tablet_meta_pb->set_in_restore_mode(olap_header.in_restore_mode());
    tablet_meta_pb->set_tablet_state(TabletStatePB::PB_RUNNING);
    VLOG(3) << "convert tablet meta tablet id = " << olap_header.tablet_id() 
            << " schema hash = " << olap_header.schema_hash() << " successfully.";
    return OLAP_SUCCESS;
}

OLAPStatus OlapSnapshotConverter::convert_to_pdelta(const RowsetMetaPB& rowset_meta_pb, PDelta* delta) {
    delta->set_start_version(rowset_meta_pb.start_version());
    delta->set_end_version(rowset_meta_pb.end_version());
    delta->set_version_hash(rowset_meta_pb.version_hash());
    delta->set_creation_time(rowset_meta_pb.creation_time());
    AlphaRowsetExtraMetaPB extra_meta_pb;
    extra_meta_pb.ParseFromString(rowset_meta_pb.extra_properties());
    
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
        int64_t rowset_id, int64_t tablet_id, int32_t schema_hash, RowsetMetaPB* rowset_meta_pb) {
    rowset_meta_pb->set_rowset_id(rowset_id);
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
    AlphaRowsetExtraMetaPB extra_meta_pb;
    for (auto& segment_group : delta.segment_group()) {
        SegmentGroupPB* new_segment_group = extra_meta_pb.add_segment_groups();
        *new_segment_group = segment_group;
        if (!segment_group.empty()) {
            empty = false;
        }
        num_rows += segment_group.num_rows();
        index_size += segment_group.index_size();
        data_size += segment_group.data_size();
    }
    std::string extra_properties;
    extra_meta_pb.SerializeToString(&extra_properties);
    rowset_meta_pb->set_extra_properties(extra_properties);

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
    VLOG(3) << "convert pending delta start_version = " << delta.start_version()
            << " end_version = " <<  delta.end_version()
            << " version_hash = " << delta.version_hash()
            << " to rowset id = " << rowset_id;
    return OLAP_SUCCESS;
}

OLAPStatus OlapSnapshotConverter::convert_to_rowset_meta(const PPendingDelta& pending_delta, 
        int64_t rowset_id, int64_t tablet_id, int32_t schema_hash, RowsetMetaPB* rowset_meta_pb) {
    rowset_meta_pb->set_rowset_id(rowset_id);
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
    AlphaRowsetExtraMetaPB extra_meta_pb;
    for (auto& pending_segment_group : pending_delta.pending_segment_group()) {
        SegmentGroupPB* new_segment_group = extra_meta_pb.add_segment_groups();
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
    std::string extra_properties;
    extra_meta_pb.SerializeToString(&extra_properties);
    rowset_meta_pb->set_extra_properties(extra_properties);

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
    VLOG(3) << "convert pending delta txn id = " << pending_delta.transaction_id()
            << " tablet_id = " <<  tablet_id
            << " schema_hash = " << schema_hash
            << " to rowset id = " << rowset_id;
    return OLAP_SUCCESS;
}

OLAPStatus OlapSnapshotConverter::to_column_pb(const ColumnMessage& column_msg, ColumnPB* column_pb) {
    column_pb->set_unique_id(column_msg.unique_id());
    column_pb->set_name(column_msg.name());
    column_pb->set_type(column_msg.type());
    column_pb->set_is_key(column_msg.is_key());
    column_pb->set_aggregation(column_msg.aggregation());
    column_pb->set_is_nullable(column_msg.is_allow_null());
    column_pb->set_default_value(column_msg.default_value());
    column_pb->set_precision(column_msg.precision());
    column_pb->set_frac(column_msg.frac());
    column_pb->set_length(column_msg.length());
    column_pb->set_index_length(column_msg.index_length());
    column_pb->set_is_bf_column(column_msg.is_bf_column());
    // TODO(ygl) calculate column id from column list
    // column_pb->set_referenced_column_id(column_msg.());
    column_pb->set_referenced_column(column_msg.referenced_column());
    return OLAP_SUCCESS;
}

OLAPStatus OlapSnapshotConverter::to_column_msg(const ColumnPB& column_pb, ColumnMessage* column_msg) {
    column_msg->set_name(column_pb.name());
    column_msg->set_type(column_pb.type());
    column_msg->set_aggregation(column_pb.aggregation());
    column_msg->set_length(column_pb.length());
    column_msg->set_is_key(column_pb.is_key());
    column_msg->set_default_value(column_pb.default_value());
    column_msg->set_referenced_column(column_pb.referenced_column());
    column_msg->set_index_length(column_pb.index_length());
    column_msg->set_precision(column_pb.precision());
    column_msg->set_frac(column_pb.frac());
    column_msg->set_is_allow_null(column_pb.is_nullable());
    column_msg->set_unique_id(column_pb.unique_id());
    column_msg->set_is_bf_column(column_pb.is_bf_column());
    column_msg->set_is_root_column(true);
    return OLAP_SUCCESS;
}

OLAPStatus OlapSnapshotConverter::to_alter_tablet_pb(const SchemaChangeStatusMessage& schema_change_msg, 
                                                   AlterTabletPB* alter_tablet_pb) {
    alter_tablet_pb->set_related_tablet_id(schema_change_msg.related_tablet_id());
    alter_tablet_pb->set_related_schema_hash(schema_change_msg.related_schema_hash());
    alter_tablet_pb->set_alter_type(static_cast<AlterTabletType>(schema_change_msg.schema_change_type()));
    if (schema_change_msg.versions_to_changed().size() == 0) {
        alter_tablet_pb->set_alter_state(AlterTabletState::ALTER_FINISHED);
    } else {
        alter_tablet_pb->set_alter_state(AlterTabletState::ALTER_FAILED);
    }
    return OLAP_SUCCESS;
}

// from olap header to tablet meta
OLAPStatus OlapSnapshotConverter::to_new_snapshot(const OLAPHeaderMessage& olap_header, const string& old_data_path_prefix, 
    const string& new_data_path_prefix, DataDir& data_dir, TabletMetaPB* tablet_meta_pb, vector<RowsetMetaPB>* pending_rowsets) {
    RETURN_NOT_OK(to_tablet_meta_pb(olap_header, tablet_meta_pb, pending_rowsets, &data_dir));

    TabletSchema tablet_schema;
    RETURN_NOT_OK(tablet_schema.init_from_pb(tablet_meta_pb->schema()));

    // convert visible pdelta file to rowsets
    for (auto& visible_rowset : tablet_meta_pb->rs_metas()) {
        RowsetMetaSharedPtr alpha_rowset_meta(new AlphaRowsetMeta());
        alpha_rowset_meta->init_from_pb(visible_rowset);
        AlphaRowset rowset(&tablet_schema, new_data_path_prefix, &data_dir, alpha_rowset_meta);
        RETURN_NOT_OK(rowset.init());
        std::vector<std::string> success_files;
        RETURN_NOT_OK(rowset.convert_from_old_files(old_data_path_prefix, &success_files));
    }

    // convert inc delta file to rowsets
    for (auto& inc_rowset : tablet_meta_pb->inc_rs_metas()) {
        RowsetMetaSharedPtr alpha_rowset_meta(new AlphaRowsetMeta());
        alpha_rowset_meta->init_from_pb(inc_rowset);
        AlphaRowset rowset(&tablet_schema, new_data_path_prefix, &data_dir, alpha_rowset_meta);
        RETURN_NOT_OK(rowset.init());
        std::vector<std::string> success_files;
        RETURN_NOT_OK(rowset.convert_from_old_files(old_data_path_prefix, &success_files));
    }

    for (auto it = pending_rowsets->begin(); it != pending_rowsets->end(); ++it) {
        RowsetMetaSharedPtr alpha_rowset_meta(new AlphaRowsetMeta());
        alpha_rowset_meta->init_from_pb(*it);
        AlphaRowset rowset(&tablet_schema, new_data_path_prefix, &data_dir, alpha_rowset_meta);
        RETURN_NOT_OK(rowset.init());
        std::vector<std::string> success_files;
        // std::string pending_delta_path = old_data_path_prefix + PENDING_DELTA_PREFIX;
        RETURN_NOT_OK(rowset.convert_from_old_files(old_data_path_prefix, &success_files));
        // pending delta does not have row num, index size, data size info
        // should load the pending delta, get these info and reset rowset meta's row num
        // data size, index size
        RETURN_NOT_OK(rowset.reset_sizeinfo());
        rowset.to_rowset_pb(&(*it));
    }
    return OLAP_SUCCESS;
}

// from tablet meta to olap header
OLAPStatus OlapSnapshotConverter::to_old_snapshot(const TabletMetaPB& tablet_meta_pb, string& new_data_path_prefix, 
    string& old_data_path_prefix, OLAPHeaderMessage* olap_header) {
    RETURN_NOT_OK(to_olap_header(tablet_meta_pb, olap_header));

    TabletSchema tablet_schema;
    RETURN_NOT_OK(tablet_schema.init_from_pb(tablet_meta_pb.schema()));

    // convert visible pdelta file to rowsets
    for (auto& visible_rowset : tablet_meta_pb.rs_metas()) {
        RowsetMetaSharedPtr alpha_rowset_meta(new AlphaRowsetMeta());
        alpha_rowset_meta->init_from_pb(visible_rowset);
        AlphaRowset rowset(&tablet_schema, new_data_path_prefix, nullptr, alpha_rowset_meta);
        RETURN_NOT_OK(rowset.init());
        RETURN_NOT_OK(rowset.load());
        std::vector<std::string> success_files;
        RETURN_NOT_OK(rowset.convert_to_old_files(old_data_path_prefix, &success_files));
    }

    // convert inc delta file to rowsets
    for (auto& inc_rowset : tablet_meta_pb.inc_rs_metas()) {
        RowsetMetaSharedPtr alpha_rowset_meta(new AlphaRowsetMeta());
        alpha_rowset_meta->init_from_pb(inc_rowset);
        AlphaRowset rowset(&tablet_schema, new_data_path_prefix, nullptr, alpha_rowset_meta);
        RETURN_NOT_OK(rowset.init());
        RETURN_NOT_OK(rowset.load());
        std::vector<std::string> success_files;
        RETURN_NOT_OK(rowset.convert_to_old_files(old_data_path_prefix, &success_files));
    }
    return OLAP_SUCCESS;
}

OLAPStatus OlapSnapshotConverter::save(const string& file_path, const OLAPHeaderMessage& olap_header) {
    DCHECK(!file_path.empty());

    FileHeader<OLAPHeaderMessage> file_header;
    FileHandler file_handler;

    if (file_handler.open_with_mode(file_path.c_str(),
            O_CREAT | O_WRONLY | O_TRUNC, S_IRUSR | S_IWUSR) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to open header file. file='" << file_path;
        return OLAP_ERR_IO_ERROR;
    }

    try {
        file_header.mutable_message()->CopyFrom(olap_header);
    } catch (...) {
        LOG(WARNING) << "fail to copy protocol buffer object. file='" << file_path;
        return OLAP_ERR_OTHER_ERROR;
    }

    if (file_header.prepare(&file_handler) != OLAP_SUCCESS
            || file_header.serialize(&file_handler) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to serialize to file header. file='" << file_path;
        return OLAP_ERR_SERIALIZE_PROTOBUF_ERROR;
    }

    return OLAP_SUCCESS;
}

}
