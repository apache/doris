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

OLAPStatus OlapSnapshotConverter::to_olap_header(const TabletMetaPB& tablet_meta_pb, OLAPHeaderMessage* olap_header) {
    if (!tablet_meta_pb.schema().has_num_rows_per_row_block()) {
        LOG(FATAL) << "tablet schema does not have num_rows_per_row_block."
                   << " tablet id = " << tablet_meta_pb.tablet_id();
    }
    olap_header->set_num_rows_per_data_block(tablet_meta_pb.schema().num_rows_per_row_block());
    if (!tablet_meta_pb.has_cumulative_layer_point()) {
        LOG(FATAL) << "tablet schema does not have cumulative_layer_point."
                   << " tablet id = " << tablet_meta_pb.tablet_id();
    }
    olap_header->set_cumulative_layer_point(-1);
    if (!tablet_meta_pb.schema().has_num_short_key_columns()) {
        LOG(FATAL) << "tablet schema does not have num_short_key_columns."
                   << " tablet id = " << tablet_meta_pb.tablet_id();
    }
    olap_header->set_num_short_key_fields(tablet_meta_pb.schema().num_short_key_columns());

    for (auto& column : tablet_meta_pb.schema().column()) {
        ColumnMessage* column_msg = olap_header->add_column();
        to_column_msg(column, column_msg);
    }

    if (!tablet_meta_pb.has_creation_time()) {
        LOG(FATAL) << "tablet schema does not have creation_time."
                   << " tablet id = " << tablet_meta_pb.tablet_id();
    }
    olap_header->set_creation_time(tablet_meta_pb.creation_time());
    olap_header->set_data_file_type(DataFileType::COLUMN_ORIENTED_FILE);
    if (tablet_meta_pb.schema().has_next_column_unique_id()) {
        olap_header->set_next_column_unique_id(tablet_meta_pb.schema().next_column_unique_id());
    }
    if (tablet_meta_pb.schema().has_compress_kind()) {
        olap_header->set_compress_kind(tablet_meta_pb.schema().compress_kind());
    }
    if (tablet_meta_pb.schema().has_bf_fpp()) {
        olap_header->set_bf_fpp(tablet_meta_pb.schema().bf_fpp());
    }
    if (tablet_meta_pb.schema().has_keys_type()) {
        olap_header->set_keys_type(tablet_meta_pb.schema().keys_type());
    }

    for (auto& rs_meta : tablet_meta_pb.rs_metas()) {
        // Add delete predicate OLAPHeaderMessage from PDelta.
        PDelta* pdelta = olap_header->add_delta();
        convert_to_pdelta(rs_meta, pdelta);
        if (pdelta->has_delete_condition()) {
            DeletePredicatePB* delete_condition = olap_header->add_delete_data_conditions();
            *delete_condition = pdelta->delete_condition();
        }
    }
    // not add pending delta, it is usedless in clone or backup restore
    for (auto& inc_rs_meta : tablet_meta_pb.inc_rs_metas()) {
        PDelta* pdelta = olap_header->add_incremental_delta();
        convert_to_pdelta(inc_rs_meta, pdelta);
    }
    if (tablet_meta_pb.has_in_restore_mode()) {
        olap_header->set_in_restore_mode(tablet_meta_pb.in_restore_mode());
    }
    if (tablet_meta_pb.has_tablet_id()) {
        olap_header->set_tablet_id(tablet_meta_pb.tablet_id());
    }
    if (tablet_meta_pb.has_schema_hash()) {
        olap_header->set_schema_hash(tablet_meta_pb.schema_hash());
    }
    if (tablet_meta_pb.has_shard_id()) {
        olap_header->set_shard_id(tablet_meta_pb.shard_id());
    }
    return OLAP_SUCCESS;
}

OLAPStatus OlapSnapshotConverter::to_tablet_meta_pb(const OLAPHeaderMessage& olap_header,
        TabletMetaPB* tablet_meta_pb, vector<RowsetMetaPB>* pending_rowsets) {
    if (olap_header.has_tablet_id()) {
        tablet_meta_pb->set_tablet_id(olap_header.tablet_id());
    }
    if (olap_header.has_schema_hash()) {
        tablet_meta_pb->set_schema_hash(olap_header.schema_hash());
    }
    if (olap_header.has_shard_id()) {
        tablet_meta_pb->set_shard_id(olap_header.shard_id());
    }
    tablet_meta_pb->set_creation_time(olap_header.creation_time());
    tablet_meta_pb->set_cumulative_layer_point(-1);

    TabletSchemaPB* schema = tablet_meta_pb->mutable_schema();
    for (auto& column_msg : olap_header.column()) {
        ColumnPB* column_pb = schema->add_column();
        to_column_pb(column_msg, column_pb);
    }
    if (olap_header.has_keys_type()) {
        schema->set_keys_type(olap_header.keys_type());
    } else {
        // Doris support AGG_KEYS/UNIQUE_KEYS/DUP_KEYS/ three storage model.
        // Among these three model, UNIQUE_KYES/DUP_KEYS is added after AGG_KEYS.
        // For historical tablet, the keys_type field to indicate storage model
        // may be missed for AGG_KEYS.
        // So upgrade from historical tablet, this situation should be taken into
        // consideration and set to be AGG_KEYS.
        schema->set_keys_type(KeysType::AGG_KEYS);
    }

    schema->set_num_short_key_columns(olap_header.num_short_key_fields());
    schema->set_num_rows_per_row_block(olap_header.num_rows_per_data_block());
    schema->set_compress_kind(olap_header.compress_kind());
    if (olap_header.has_bf_fpp()) {
        schema->set_bf_fpp(olap_header.bf_fpp());
    }
    if (olap_header.has_next_column_unique_id()) {
        schema->set_next_column_unique_id(olap_header.next_column_unique_id());
    }

    std::unordered_map<Version, RowsetMetaPB*, HashOfVersion> _rs_version_map;
    const DelPredicateArray& delete_conditions = olap_header.delete_data_conditions();
    for (auto& delta : olap_header.delta()) {
        RowsetId next_id = StorageEngine::instance()->next_rowset_id();
        RowsetMetaPB* rowset_meta = tablet_meta_pb->add_rs_metas();
        PDelta temp_delta = delta;
        // PDelta is not corresponding with RowsetMeta in DeletePredicate
        // Add delete predicate to PDelta from OLAPHeaderMessage.
        // Only after this, convert from PDelta to RowsetMeta is valid.
        if (temp_delta.start_version() == temp_delta.end_version()) {
            for (auto& del_pred : delete_conditions) {
                if (temp_delta.start_version() == del_pred.version()) {
                    DeletePredicatePB* delete_condition = temp_delta.mutable_delete_condition();
                    *delete_condition = del_pred;
                }
            }
        }
        convert_to_rowset_meta(temp_delta, next_id, olap_header.tablet_id(), olap_header.schema_hash(), rowset_meta);
        Version rowset_version = { temp_delta.start_version(), temp_delta.end_version() };
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
        RowsetId next_id = StorageEngine::instance()->next_rowset_id();
        RowsetMetaPB* rowset_meta = tablet_meta_pb->add_inc_rs_metas();
        PDelta temp_inc_delta = inc_delta;
        if (temp_inc_delta.start_version() == temp_inc_delta.end_version()) {
            for (auto& del_pred : delete_conditions) {
                if (temp_inc_delta.start_version() == del_pred.version()) {
                    DeletePredicatePB* delete_condition = temp_inc_delta.mutable_delete_condition();
                    *delete_condition = del_pred;
                }
            }
        }
        convert_to_rowset_meta(temp_inc_delta, next_id, olap_header.tablet_id(), olap_header.schema_hash(), rowset_meta);
    }

    for (auto& pending_delta : olap_header.pending_delta()) {
        RowsetId next_id = StorageEngine::instance()->next_rowset_id();
        RowsetMetaPB rowset_meta;
        convert_to_rowset_meta(pending_delta, next_id, olap_header.tablet_id(), olap_header.schema_hash(), &rowset_meta);
        pending_rowsets->emplace_back(std::move(rowset_meta));
    }
    if (olap_header.has_schema_change_status()) {
        AlterTabletPB* alter_tablet_pb = tablet_meta_pb->mutable_alter_task();
        to_alter_tablet_pb(olap_header.schema_change_status(), alter_tablet_pb);
    }
    if (olap_header.has_in_restore_mode()) {
        tablet_meta_pb->set_in_restore_mode(olap_header.in_restore_mode());
    }
    tablet_meta_pb->set_tablet_state(TabletStatePB::PB_RUNNING);
    *(tablet_meta_pb->mutable_tablet_uid()) = TabletUid::gen_uid().to_proto();
    VLOG(3) << "convert tablet meta tablet id = " << olap_header.tablet_id()
            << " schema hash = " << olap_header.schema_hash() << " successfully.";
    return OLAP_SUCCESS;
}

OLAPStatus OlapSnapshotConverter::convert_to_pdelta(const RowsetMetaPB& rowset_meta_pb, PDelta* delta) {
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
        const RowsetId& rowset_id, int64_t tablet_id, int32_t schema_hash, RowsetMetaPB* rowset_meta_pb) {
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
            << " end_version = " <<  delta.end_version()
            << " version_hash = " << delta.version_hash()
            << " to rowset id = " << rowset_id
            << " tablet_id = " <<  tablet_id;
    return OLAP_SUCCESS;
}

OLAPStatus OlapSnapshotConverter::convert_to_rowset_meta(const PPendingDelta& pending_delta,
        const RowsetId& rowset_id, int64_t tablet_id, int32_t schema_hash, RowsetMetaPB* rowset_meta_pb) {
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
            << " tablet_id = " <<  tablet_id
            << " schema_hash = " << schema_hash
            << " to rowset id = " << rowset_id;
    return OLAP_SUCCESS;
}

OLAPStatus OlapSnapshotConverter::to_column_pb(const ColumnMessage& column_msg, ColumnPB* column_pb) {
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

OLAPStatus OlapSnapshotConverter::to_column_msg(const ColumnPB& column_pb, ColumnMessage* column_msg) {
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
    const string& new_data_path_prefix, TabletMetaPB* tablet_meta_pb, 
    vector<RowsetMetaPB>* pending_rowsets, bool is_startup) {
    RETURN_NOT_OK(to_tablet_meta_pb(olap_header, tablet_meta_pb, pending_rowsets));

    TabletSchema tablet_schema;
    tablet_schema.init_from_pb(tablet_meta_pb->schema());

    // convert visible pdelta file to rowsets
    for (auto& visible_rowset : tablet_meta_pb->rs_metas()) {
        RowsetMetaSharedPtr alpha_rowset_meta(new AlphaRowsetMeta());
        alpha_rowset_meta->init_from_pb(visible_rowset);
        alpha_rowset_meta->set_tablet_uid(tablet_meta_pb->tablet_uid());
        AlphaRowset rowset(&tablet_schema, new_data_path_prefix, alpha_rowset_meta);
        RETURN_NOT_OK(rowset.init());
        std::vector<std::string> success_files;
        RETURN_NOT_OK(rowset.convert_from_old_files(old_data_path_prefix, &success_files));
        _modify_old_segment_group_id(const_cast<RowsetMetaPB&>(visible_rowset));
    }

    // convert inc delta file to rowsets
    for (auto& inc_rowset : tablet_meta_pb->inc_rs_metas()) {
        RowsetMetaSharedPtr alpha_rowset_meta(new AlphaRowsetMeta());
        alpha_rowset_meta->init_from_pb(inc_rowset);
        alpha_rowset_meta->set_tablet_uid(tablet_meta_pb->tablet_uid());
        AlphaRowset rowset(&tablet_schema, new_data_path_prefix, alpha_rowset_meta);
        RETURN_NOT_OK(rowset.init());
        std::vector<std::string> success_files;
        std::string inc_data_path = old_data_path_prefix;
        // in clone case: there is no incremental perfix
        // in start up case: there is incremental prefix
        if (is_startup) {
            inc_data_path = inc_data_path + "/" + INCREMENTAL_DELTA_PREFIX;
        }
        RETURN_NOT_OK(rowset.convert_from_old_files(inc_data_path, &success_files));
        _modify_old_segment_group_id(const_cast<RowsetMetaPB&>(inc_rowset));
    }

    for (auto it = pending_rowsets->begin(); it != pending_rowsets->end(); ++it) {
        RowsetMetaSharedPtr alpha_rowset_meta(new AlphaRowsetMeta());
        alpha_rowset_meta->init_from_pb(*it);
        alpha_rowset_meta->set_tablet_uid(tablet_meta_pb->tablet_uid());
        AlphaRowset rowset(&tablet_schema, new_data_path_prefix, alpha_rowset_meta);
        RETURN_NOT_OK(rowset.init());
        std::vector<std::string> success_files;
        // std::string pending_delta_path = old_data_path_prefix + PENDING_DELTA_PREFIX;
        // if this is a pending segment group, rowset will add pending_delta_prefix when
        // construct old file path
        RETURN_NOT_OK(rowset.convert_from_old_files(old_data_path_prefix, &success_files));
        // pending delta does not have row num, index size, data size info
        // should load the pending delta, get these info and reset rowset meta's row num
        // data size, index size
        RETURN_NOT_OK(rowset.reset_sizeinfo());
        // pending rowset not have segment group id == -1 problem, not need to modify sg id in meta
        rowset.to_rowset_pb(&(*it));
    }
    return OLAP_SUCCESS;
}

// from tablet meta to olap header
OLAPStatus OlapSnapshotConverter::to_old_snapshot(const TabletMetaPB& tablet_meta_pb, string& new_data_path_prefix,
    string& old_data_path_prefix, OLAPHeaderMessage* olap_header) {
    RETURN_NOT_OK(to_olap_header(tablet_meta_pb, olap_header));

    TabletSchema tablet_schema;
    tablet_schema.init_from_pb(tablet_meta_pb.schema());

    // convert visible pdelta file to rowsets
    for (auto& visible_rowset : tablet_meta_pb.rs_metas()) {
        RowsetMetaSharedPtr alpha_rowset_meta(new AlphaRowsetMeta());
        alpha_rowset_meta->init_from_pb(visible_rowset);
        AlphaRowset rowset(&tablet_schema, new_data_path_prefix, alpha_rowset_meta);
        RETURN_NOT_OK(rowset.init());
        RETURN_NOT_OK(rowset.load());
        std::vector<std::string> success_files;
        RETURN_NOT_OK(rowset.convert_to_old_files(old_data_path_prefix, &success_files));
    }

    // convert inc delta file to rowsets
    for (auto& inc_rowset : tablet_meta_pb.inc_rs_metas()) {
        RowsetMetaSharedPtr alpha_rowset_meta(new AlphaRowsetMeta());
        alpha_rowset_meta->init_from_pb(inc_rowset);
        AlphaRowset rowset(&tablet_schema, new_data_path_prefix, alpha_rowset_meta);
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

void OlapSnapshotConverter::_modify_old_segment_group_id(RowsetMetaPB& rowset_meta) {
    if (!rowset_meta.has_alpha_rowset_extra_meta_pb()) {
        return;
    }
    AlphaRowsetExtraMetaPB* alpha_rowset_extra_meta_pb = rowset_meta.mutable_alpha_rowset_extra_meta_pb();
    for (auto& segment_group_pb : alpha_rowset_extra_meta_pb->segment_groups()) {
        if (segment_group_pb.segment_group_id() == -1) {
            // check if segment groups size == 1
            if (alpha_rowset_extra_meta_pb->segment_groups().size() != 1) {
                LOG(FATAL) << "the rowset has a segment group's id == -1 but it contains more than one segment group"
                           << " it should not happen";
            }
            (const_cast<SegmentGroupPB&>(segment_group_pb)).set_segment_group_id(0);
        }
    }
}

}
