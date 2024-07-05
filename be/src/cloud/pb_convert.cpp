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

#include "cloud/pb_convert.h"

#include <gen_cpp/olap_file.pb.h>

#include <utility>

namespace doris::cloud {

RowsetMetaCloudPB doris_rowset_meta_to_cloud(const RowsetMetaPB& in) {
    RowsetMetaCloudPB out;
    doris_rowset_meta_to_cloud(&out, in);
    return out;
}

RowsetMetaCloudPB doris_rowset_meta_to_cloud(RowsetMetaPB&& in) {
    RowsetMetaCloudPB out;
    doris_rowset_meta_to_cloud(&out, std::move(in));
    return out;
}

void doris_rowset_meta_to_cloud(RowsetMetaCloudPB* out, const RowsetMetaPB& in) {
    // ATTN: please keep the set order aligned with the definition of proto `TabletSchemaCloudPB`.
    out->set_rowset_id(in.rowset_id());
    out->set_partition_id(in.partition_id());
    out->set_tablet_id(in.tablet_id());
    out->set_txn_id(in.txn_id());
    out->set_tablet_schema_hash(in.tablet_schema_hash());
    out->set_rowset_type(in.rowset_type());
    out->set_rowset_state(in.rowset_state());
    out->set_start_version(in.start_version());
    out->set_end_version(in.end_version());
    out->set_version_hash(in.version_hash());
    out->set_num_rows(in.num_rows());
    out->set_total_disk_size(in.total_disk_size());
    out->set_data_disk_size(in.data_disk_size());
    out->set_index_disk_size(in.index_disk_size());
    out->mutable_zone_maps()->CopyFrom(in.zone_maps());
    if (in.has_delete_predicate()) {
        out->mutable_delete_predicate()->CopyFrom(in.delete_predicate());
    }
    out->set_empty(in.empty());
    if (in.has_load_id()) {
        out->mutable_load_id()->CopyFrom(in.load_id());
    }
    out->set_delete_flag(in.delete_flag());
    out->set_creation_time(in.creation_time());
    if (in.has_tablet_uid()) {
        out->mutable_tablet_uid()->CopyFrom(in.tablet_uid());
    }
    out->set_num_segments(in.num_segments());
    out->set_rowset_id_v2(in.rowset_id_v2());
    out->set_resource_id(in.resource_id());
    out->set_newest_write_timestamp(in.newest_write_timestamp());
    out->mutable_segments_key_bounds()->CopyFrom(in.segments_key_bounds());
    if (in.has_tablet_schema()) {
        doris_tablet_schema_to_cloud(out->mutable_tablet_schema(), in.tablet_schema());
    }
    out->set_txn_expiration(in.txn_expiration());
    out->set_segments_overlap_pb(in.segments_overlap_pb());
    out->mutable_segments_file_size()->CopyFrom(in.segments_file_size());
    out->set_index_id(in.index_id());
    if (in.has_schema_version()) {
        // See cloud/src/meta-service/meta_service_schema.cpp for details.
        out->set_schema_version(in.schema_version());
    }
    out->set_enable_segments_file_size(in.enable_segments_file_size());
    out->set_has_variant_type_in_schema(in.has_has_variant_type_in_schema());
}

void doris_rowset_meta_to_cloud(RowsetMetaCloudPB* out, RowsetMetaPB&& in) {
    // ATTN: please keep the set order aligned with the definition of proto `TabletSchemaCloudPB`.
    out->set_rowset_id(in.rowset_id());
    out->set_partition_id(in.partition_id());
    out->set_tablet_id(in.tablet_id());
    out->set_txn_id(in.txn_id());
    out->set_tablet_schema_hash(in.tablet_schema_hash());
    out->set_rowset_type(in.rowset_type());
    out->set_rowset_state(in.rowset_state());
    out->set_start_version(in.start_version());
    out->set_end_version(in.end_version());
    out->set_version_hash(in.version_hash());
    out->set_num_rows(in.num_rows());
    out->set_total_disk_size(in.total_disk_size());
    out->set_data_disk_size(in.data_disk_size());
    out->set_index_disk_size(in.index_disk_size());
    out->mutable_zone_maps()->Swap(in.mutable_zone_maps());
    if (in.has_delete_predicate()) {
        out->mutable_delete_predicate()->Swap(in.mutable_delete_predicate());
    }
    out->set_empty(in.empty());
    if (in.has_load_id()) {
        out->mutable_load_id()->CopyFrom(in.load_id());
    }
    out->set_delete_flag(in.delete_flag());
    out->set_creation_time(in.creation_time());
    if (in.has_tablet_uid()) {
        out->mutable_tablet_uid()->CopyFrom(in.tablet_uid());
    }
    out->set_num_segments(in.num_segments());
    out->set_rowset_id_v2(in.rowset_id_v2());
    out->set_resource_id(in.resource_id());
    out->set_newest_write_timestamp(in.newest_write_timestamp());
    out->mutable_segments_key_bounds()->Swap(in.mutable_segments_key_bounds());
    if (in.has_tablet_schema()) {
        doris_tablet_schema_to_cloud(out->mutable_tablet_schema(),
                                     std::move(*in.mutable_tablet_schema()));
    }
    out->set_txn_expiration(in.txn_expiration());
    out->set_segments_overlap_pb(in.segments_overlap_pb());
    out->mutable_segments_file_size()->Swap(in.mutable_segments_file_size());
    out->set_index_id(in.index_id());
    if (in.has_schema_version()) {
        // See cloud/src/meta-service/meta_service_schema.cpp for details.
        out->set_schema_version(in.schema_version());
    }
    out->set_enable_segments_file_size(in.enable_segments_file_size());
    out->set_has_variant_type_in_schema(in.has_variant_type_in_schema());
}

RowsetMetaPB cloud_rowset_meta_to_doris(const RowsetMetaCloudPB& in) {
    RowsetMetaPB out;
    cloud_rowset_meta_to_doris(&out, in);
    return out;
}

RowsetMetaPB cloud_rowset_meta_to_doris(RowsetMetaCloudPB&& in) {
    RowsetMetaPB out;
    cloud_rowset_meta_to_doris(&out, std::move(in));
    return out;
}

void cloud_rowset_meta_to_doris(RowsetMetaPB* out, const RowsetMetaCloudPB& in) {
    // ATTN: please keep the set order aligned with the definition of proto `TabletSchemaCloudPB`.
    out->set_rowset_id(in.rowset_id());
    out->set_partition_id(in.partition_id());
    out->set_tablet_id(in.tablet_id());
    out->set_txn_id(in.txn_id());
    out->set_tablet_schema_hash(in.tablet_schema_hash());
    out->set_rowset_type(in.rowset_type());
    out->set_rowset_state(in.rowset_state());
    out->set_start_version(in.start_version());
    out->set_end_version(in.end_version());
    out->set_version_hash(in.version_hash());
    out->set_num_rows(in.num_rows());
    out->set_total_disk_size(in.total_disk_size());
    out->set_data_disk_size(in.data_disk_size());
    out->set_index_disk_size(in.index_disk_size());
    out->mutable_zone_maps()->CopyFrom(in.zone_maps());
    if (in.has_delete_predicate()) {
        out->mutable_delete_predicate()->CopyFrom(in.delete_predicate());
    }
    out->set_empty(in.empty());
    out->mutable_load_id()->CopyFrom(in.load_id());
    out->set_delete_flag(in.delete_flag());
    out->set_creation_time(in.creation_time());
    if (in.has_tablet_uid()) {
        out->mutable_tablet_uid()->CopyFrom(in.tablet_uid());
    }
    out->set_num_segments(in.num_segments());
    out->set_rowset_id_v2(in.rowset_id_v2());
    out->set_resource_id(in.resource_id());
    out->set_newest_write_timestamp(in.newest_write_timestamp());
    out->mutable_segments_key_bounds()->CopyFrom(in.segments_key_bounds());
    if (in.has_tablet_schema()) {
        cloud_tablet_schema_to_doris(out->mutable_tablet_schema(), in.tablet_schema());
    }
    out->set_txn_expiration(in.txn_expiration());
    out->set_segments_overlap_pb(in.segments_overlap_pb());
    out->mutable_segments_file_size()->CopyFrom(in.segments_file_size());
    out->set_index_id(in.index_id());
    if (in.has_schema_version()) {
        // See cloud/src/meta-service/meta_service_schema.cpp for details.
        out->set_schema_version(in.schema_version());
    }
    out->set_enable_segments_file_size(in.enable_segments_file_size());
}

void cloud_rowset_meta_to_doris(RowsetMetaPB* out, RowsetMetaCloudPB&& in) {
    // ATTN: please keep the set order aligned with the definition of proto `TabletSchemaCloudPB`.
    out->set_rowset_id(in.rowset_id());
    out->set_partition_id(in.partition_id());
    out->set_tablet_id(in.tablet_id());
    out->set_txn_id(in.txn_id());
    out->set_tablet_schema_hash(in.tablet_schema_hash());
    out->set_rowset_type(in.rowset_type());
    out->set_rowset_state(in.rowset_state());
    out->set_start_version(in.start_version());
    out->set_end_version(in.end_version());
    out->set_version_hash(in.version_hash());
    out->set_num_rows(in.num_rows());
    out->set_total_disk_size(in.total_disk_size());
    out->set_data_disk_size(in.data_disk_size());
    out->set_index_disk_size(in.index_disk_size());
    out->mutable_zone_maps()->Swap(in.mutable_zone_maps());
    if (in.has_delete_predicate()) {
        out->mutable_delete_predicate()->Swap(in.mutable_delete_predicate());
    }
    out->set_empty(in.empty());
    out->mutable_load_id()->CopyFrom(in.load_id());
    out->set_delete_flag(in.delete_flag());
    out->set_creation_time(in.creation_time());
    if (in.has_tablet_uid()) {
        out->mutable_tablet_uid()->CopyFrom(in.tablet_uid());
    }
    out->set_num_segments(in.num_segments());
    out->set_rowset_id_v2(in.rowset_id_v2());
    out->set_resource_id(in.resource_id());
    out->set_newest_write_timestamp(in.newest_write_timestamp());
    out->mutable_segments_key_bounds()->Swap(in.mutable_segments_key_bounds());
    if (in.has_tablet_schema()) {
        cloud_tablet_schema_to_doris(out->mutable_tablet_schema(),
                                     std::move(*in.mutable_tablet_schema()));
    }
    out->set_txn_expiration(in.txn_expiration());
    out->set_segments_overlap_pb(in.segments_overlap_pb());
    out->mutable_segments_file_size()->Swap(in.mutable_segments_file_size());
    out->set_index_id(in.index_id());
    if (in.has_schema_version()) {
        // See cloud/src/meta-service/meta_service_schema.cpp for details.
        out->set_schema_version(in.schema_version());
    }
    out->set_enable_segments_file_size(in.enable_segments_file_size());
}

TabletSchemaCloudPB doris_tablet_schema_to_cloud(const TabletSchemaPB& in) {
    TabletSchemaCloudPB out;
    doris_tablet_schema_to_cloud(&out, in);
    return out;
}

TabletSchemaCloudPB doris_tablet_schema_to_cloud(TabletSchemaPB&& in) {
    // ATTN: please keep the set order aligned with the definition of proto.
    TabletSchemaCloudPB out;
    doris_tablet_schema_to_cloud(&out, std::move(in));
    return out;
}

void doris_tablet_schema_to_cloud(TabletSchemaCloudPB* out, const TabletSchemaPB& in) {
    // ATTN: please keep the set order aligned with the definition of proto.
    out->set_keys_type(in.keys_type());
    out->mutable_column()->CopyFrom(in.column());
    out->set_num_short_key_columns(in.num_short_key_columns());
    out->set_num_rows_per_row_block(in.num_rows_per_row_block());
    out->set_compress_kind(in.compress_kind());
    out->set_bf_fpp(in.bf_fpp());
    out->set_next_column_unique_id(in.next_column_unique_id());
    out->set_delete_sign_idx(in.delete_sign_idx());
    out->set_sequence_col_idx(in.sequence_col_idx());
    out->set_sort_type(in.sort_type());
    out->set_sort_col_num(in.sort_col_num());
    out->set_compression_type(in.compression_type());
    out->set_schema_version(in.schema_version());
    out->set_disable_auto_compaction(in.disable_auto_compaction());
    out->mutable_index()->CopyFrom(in.index());
    out->set_version_col_idx(in.version_col_idx());
    out->set_store_row_column(in.store_row_column());
    out->set_enable_single_replica_compaction(in.enable_single_replica_compaction());
    out->set_skip_write_index_on_load(in.skip_write_index_on_load());
    out->mutable_cluster_key_idxes()->CopyFrom(in.cluster_key_idxes());
    out->set_is_dynamic_schema(in.is_dynamic_schema());
    out->mutable_row_store_column_unique_ids()->CopyFrom(in.row_store_column_unique_ids());
    out->set_inverted_index_storage_format(in.inverted_index_storage_format());
}

void doris_tablet_schema_to_cloud(TabletSchemaCloudPB* out, TabletSchemaPB&& in) {
    out->set_keys_type(in.keys_type());
    out->mutable_column()->Swap(in.mutable_column());
    out->set_num_short_key_columns(in.num_short_key_columns());
    out->set_num_rows_per_row_block(in.num_rows_per_row_block());
    out->set_compress_kind(in.compress_kind());
    out->set_bf_fpp(in.bf_fpp());
    out->set_next_column_unique_id(in.next_column_unique_id());
    out->set_delete_sign_idx(in.delete_sign_idx());
    out->set_sequence_col_idx(in.sequence_col_idx());
    out->set_sort_type(in.sort_type());
    out->set_sort_col_num(in.sort_col_num());
    out->set_compression_type(in.compression_type());
    out->set_schema_version(in.schema_version());
    out->set_disable_auto_compaction(in.disable_auto_compaction());
    out->mutable_index()->Swap(in.mutable_index());
    out->set_version_col_idx(in.version_col_idx());
    out->set_store_row_column(in.store_row_column());
    out->set_enable_single_replica_compaction(in.enable_single_replica_compaction());
    out->set_skip_write_index_on_load(in.skip_write_index_on_load());
    out->mutable_cluster_key_idxes()->Swap(in.mutable_cluster_key_idxes());
    out->set_is_dynamic_schema(in.is_dynamic_schema());
    out->mutable_row_store_column_unique_ids()->Swap(in.mutable_row_store_column_unique_ids());
    out->set_inverted_index_storage_format(in.inverted_index_storage_format());
}

TabletSchemaPB cloud_tablet_schema_to_doris(const TabletSchemaCloudPB& in) {
    TabletSchemaPB out;
    cloud_tablet_schema_to_doris(&out, in);
    return out;
}

TabletSchemaPB cloud_tablet_schema_to_doris(TabletSchemaCloudPB&& in) {
    TabletSchemaPB out;
    cloud_tablet_schema_to_doris(&out, std::move(in));
    return out;
}

void cloud_tablet_schema_to_doris(TabletSchemaPB* out, const TabletSchemaCloudPB& in) {
    // ATTN: please keep the set order aligned with the definition of proto.
    out->set_keys_type(in.keys_type());
    out->mutable_column()->CopyFrom(in.column());
    out->set_num_short_key_columns(in.num_short_key_columns());
    out->set_num_rows_per_row_block(in.num_rows_per_row_block());
    out->set_compress_kind(in.compress_kind());
    out->set_bf_fpp(in.bf_fpp());
    out->set_next_column_unique_id(in.next_column_unique_id());
    out->set_delete_sign_idx(in.delete_sign_idx());
    out->set_sequence_col_idx(in.sequence_col_idx());
    out->set_sort_type(in.sort_type());
    out->set_sort_col_num(in.sort_col_num());
    out->set_compression_type(in.compression_type());
    out->set_schema_version(in.schema_version());
    out->set_disable_auto_compaction(in.disable_auto_compaction());
    out->mutable_index()->CopyFrom(in.index());
    out->set_version_col_idx(in.version_col_idx());
    out->set_store_row_column(in.store_row_column());
    out->set_enable_single_replica_compaction(in.enable_single_replica_compaction());
    out->set_skip_write_index_on_load(in.skip_write_index_on_load());
    out->mutable_cluster_key_idxes()->CopyFrom(in.cluster_key_idxes());
    out->set_is_dynamic_schema(in.is_dynamic_schema());
    out->mutable_row_store_column_unique_ids()->CopyFrom(in.row_store_column_unique_ids());
    out->set_inverted_index_storage_format(in.inverted_index_storage_format());
}

void cloud_tablet_schema_to_doris(TabletSchemaPB* out, TabletSchemaCloudPB&& in) {
    // ATTN: please keep the set order aligned with the definition of proto.
    out->set_keys_type(in.keys_type());
    out->mutable_column()->Swap(in.mutable_column());
    out->set_num_short_key_columns(in.num_short_key_columns());
    out->set_num_rows_per_row_block(in.num_rows_per_row_block());
    out->set_compress_kind(in.compress_kind());
    out->set_bf_fpp(in.bf_fpp());
    out->set_next_column_unique_id(in.next_column_unique_id());
    out->set_delete_sign_idx(in.delete_sign_idx());
    out->set_sequence_col_idx(in.sequence_col_idx());
    out->set_sort_type(in.sort_type());
    out->set_sort_col_num(in.sort_col_num());
    out->set_compression_type(in.compression_type());
    out->set_schema_version(in.schema_version());
    out->set_disable_auto_compaction(in.disable_auto_compaction());
    out->mutable_index()->Swap(in.mutable_index());
    out->set_version_col_idx(in.version_col_idx());
    out->set_store_row_column(in.store_row_column());
    out->set_enable_single_replica_compaction(in.enable_single_replica_compaction());
    out->set_skip_write_index_on_load(in.skip_write_index_on_load());
    out->mutable_cluster_key_idxes()->Swap(in.mutable_cluster_key_idxes());
    out->set_is_dynamic_schema(in.is_dynamic_schema());
    out->mutable_row_store_column_unique_ids()->Swap(in.mutable_row_store_column_unique_ids());
    out->set_inverted_index_storage_format(in.inverted_index_storage_format());
}

TabletMetaCloudPB doris_tablet_meta_to_cloud(const TabletMetaPB& in) {
    TabletMetaCloudPB out;
    doris_tablet_meta_to_cloud(&out, in);
    return out;
}

TabletMetaCloudPB doris_tablet_meta_to_cloud(TabletMetaPB&& in) {
    TabletMetaCloudPB out;
    doris_tablet_meta_to_cloud(&out, std::move(in));
    return out;
}

void doris_tablet_meta_to_cloud(TabletMetaCloudPB* out, const TabletMetaPB& in) {
    // ATTN: please keep the set order aligned with the definition of proto.
    out->set_table_id(in.table_id());
    out->set_partition_id(in.partition_id());
    out->set_tablet_id(in.tablet_id());
    out->set_schema_hash(in.schema_hash());
    out->set_shard_id(in.shard_id());
    out->set_creation_time(in.creation_time());
    out->set_cumulative_layer_point(in.cumulative_layer_point());
    out->set_tablet_state(in.tablet_state());
    if (in.has_schema()) {
        doris_tablet_schema_to_cloud(out->mutable_schema(), in.schema());
    }
    if (in.rs_metas_size()) {
        out->mutable_rs_metas()->Reserve(in.rs_metas_size());
        for (const auto& rs_meta : in.rs_metas()) {
            doris_rowset_meta_to_cloud(out->add_rs_metas(), rs_meta);
        }
    }
    // ATTN: inc_rs_metas are deprecated, ignored here.
    if (in.has_alter_task()) {
        out->mutable_alter_task()->CopyFrom(in.alter_task());
    }
    out->set_in_restore_mode(in.in_restore_mode());
    out->mutable_tablet_uid()->CopyFrom(in.tablet_uid());
    out->set_end_rowset_id(in.end_rowset_id());
    out->set_preferred_rowset_type(in.preferred_rowset_type());
    out->set_tablet_type(in.tablet_type());
    if (in.stale_rs_metas_size()) {
        out->mutable_stale_rs_metas()->Reserve(in.stale_rs_metas_size());
        for (const auto& rs_meta : in.stale_rs_metas()) {
            doris_rowset_meta_to_cloud(out->add_stale_rs_metas(), rs_meta);
        }
    }
    out->set_replica_id(in.replica_id());
    if (in.has_delete_bitmap()) {
        out->mutable_delete_bitmap()->CopyFrom(in.delete_bitmap());
    }
    out->set_enable_unique_key_merge_on_write(in.enable_unique_key_merge_on_write());
    out->set_storage_policy_id(in.storage_policy_id());
    out->mutable_cooldown_meta_id()->CopyFrom(in.cooldown_meta_id());
    if (in.has_binlog_config()) {
        out->mutable_binlog_config()->CopyFrom(in.binlog_config());
    }
    out->set_compaction_policy(in.compaction_policy());
    out->set_time_series_compaction_goal_size_mbytes(in.time_series_compaction_goal_size_mbytes());
    out->set_time_series_compaction_file_count_threshold(
            in.time_series_compaction_file_count_threshold());
    out->set_time_series_compaction_time_threshold_seconds(
            in.time_series_compaction_time_threshold_seconds());
    out->set_time_series_compaction_empty_rowsets_threshold(
            in.time_series_compaction_empty_rowsets_threshold());
    out->set_time_series_compaction_level_threshold(in.time_series_compaction_level_threshold());
    out->set_index_id(in.index_id());
    out->set_is_in_memory(in.is_in_memory());
    out->set_is_persistent(in.is_persistent());
    out->set_table_name(in.table_name());
    out->set_ttl_seconds(in.ttl_seconds());
    if (in.has_schema_version()) {
        out->set_schema_version(in.schema_version());
    }
}

void doris_tablet_meta_to_cloud(TabletMetaCloudPB* out, TabletMetaPB&& in) {
    // ATTN: please keep the set order aligned with the definition of proto.
    out->set_table_id(in.table_id());
    out->set_partition_id(in.partition_id());
    out->set_tablet_id(in.tablet_id());
    out->set_schema_hash(in.schema_hash());
    out->set_shard_id(in.shard_id());
    out->set_creation_time(in.creation_time());
    out->set_cumulative_layer_point(in.cumulative_layer_point());
    out->set_tablet_state(in.tablet_state());
    if (in.has_schema()) {
        doris_tablet_schema_to_cloud(out->mutable_schema(), std::move(*in.mutable_schema()));
    }
    if (in.rs_metas_size()) {
        size_t rs_metas_size = in.rs_metas_size();
        out->mutable_rs_metas()->Reserve(rs_metas_size);
        for (size_t i = 0; i < rs_metas_size; ++i) {
            doris_rowset_meta_to_cloud(out->add_rs_metas(), std::move(*in.mutable_rs_metas(i)));
        }
    }
    // ATTN: inc_rs_metas are deprecated, ignored here.
    if (in.has_alter_task()) {
        out->mutable_alter_task()->Swap(in.mutable_alter_task());
    }
    out->set_in_restore_mode(in.in_restore_mode());
    out->mutable_tablet_uid()->CopyFrom(in.tablet_uid());
    out->set_end_rowset_id(in.end_rowset_id());
    out->set_preferred_rowset_type(in.preferred_rowset_type());
    out->set_tablet_type(in.tablet_type());
    if (in.stale_rs_metas_size()) {
        size_t rs_metas_size = in.stale_rs_metas_size();
        out->mutable_stale_rs_metas()->Reserve(rs_metas_size);
        for (size_t i = 0; i < rs_metas_size; i++) {
            doris_rowset_meta_to_cloud(out->add_stale_rs_metas(),
                                       std::move(*in.mutable_stale_rs_metas(i)));
        }
    }
    out->set_replica_id(in.replica_id());
    if (in.has_delete_bitmap()) {
        out->mutable_delete_bitmap()->Swap(in.mutable_delete_bitmap());
    }
    out->set_enable_unique_key_merge_on_write(in.enable_unique_key_merge_on_write());
    out->set_storage_policy_id(in.storage_policy_id());
    out->mutable_cooldown_meta_id()->CopyFrom(in.cooldown_meta_id());
    if (in.has_binlog_config()) {
        out->mutable_binlog_config()->Swap(in.mutable_binlog_config());
    }
    out->set_compaction_policy(in.compaction_policy());
    out->set_time_series_compaction_goal_size_mbytes(in.time_series_compaction_goal_size_mbytes());
    out->set_time_series_compaction_file_count_threshold(
            in.time_series_compaction_file_count_threshold());
    out->set_time_series_compaction_time_threshold_seconds(
            in.time_series_compaction_time_threshold_seconds());
    out->set_time_series_compaction_empty_rowsets_threshold(
            in.time_series_compaction_empty_rowsets_threshold());
    out->set_time_series_compaction_level_threshold(in.time_series_compaction_level_threshold());
    out->set_index_id(in.index_id());
    out->set_is_in_memory(in.is_in_memory());
    out->set_is_persistent(in.is_persistent());
    out->set_table_name(in.table_name());
    out->set_ttl_seconds(in.ttl_seconds());
    if (in.has_schema_version()) {
        out->set_schema_version(in.schema_version());
    }
}

TabletMetaPB cloud_tablet_meta_to_doris(const TabletMetaCloudPB& in) {
    TabletMetaPB out;
    cloud_tablet_meta_to_doris(&out, in);
    return out;
}

TabletMetaPB cloud_tablet_meta_to_doris(TabletMetaCloudPB&& in) {
    TabletMetaPB out;
    cloud_tablet_meta_to_doris(&out, std::move(in));
    return out;
}

void cloud_tablet_meta_to_doris(TabletMetaPB* out, const TabletMetaCloudPB& in) {
    // ATTN: please keep the set order aligned with the definition of proto.
    out->set_table_id(in.table_id());
    out->set_partition_id(in.partition_id());
    out->set_tablet_id(in.tablet_id());
    out->set_schema_hash(in.schema_hash());
    out->set_shard_id(in.shard_id());
    out->set_creation_time(in.creation_time());
    out->set_cumulative_layer_point(in.cumulative_layer_point());
    out->set_tablet_state(in.tablet_state());
    if (in.has_schema()) {
        cloud_tablet_schema_to_doris(out->mutable_schema(), in.schema());
    }
    if (in.rs_metas_size()) {
        out->mutable_rs_metas()->Reserve(in.rs_metas_size());
        for (const auto& rs_meta : in.rs_metas()) {
            cloud_rowset_meta_to_doris(out->add_rs_metas(), rs_meta);
        }
    }
    // ATTN: inc_rs_metas are deprecated, ignored here.
    if (in.has_alter_task()) {
        out->mutable_alter_task()->CopyFrom(in.alter_task());
    }
    out->set_in_restore_mode(in.in_restore_mode());
    out->mutable_tablet_uid()->CopyFrom(in.tablet_uid());
    out->set_end_rowset_id(in.end_rowset_id());
    out->set_preferred_rowset_type(in.preferred_rowset_type());
    out->set_tablet_type(in.tablet_type());
    if (in.stale_rs_metas_size()) {
        out->mutable_stale_rs_metas()->Reserve(in.stale_rs_metas_size());
        for (const auto& rs_meta : in.stale_rs_metas()) {
            cloud_rowset_meta_to_doris(out->add_stale_rs_metas(), rs_meta);
        }
    }
    out->set_replica_id(in.replica_id());
    if (in.has_delete_bitmap()) {
        out->mutable_delete_bitmap()->CopyFrom(in.delete_bitmap());
    }
    out->set_enable_unique_key_merge_on_write(in.enable_unique_key_merge_on_write());
    out->set_storage_policy_id(in.storage_policy_id());
    out->mutable_cooldown_meta_id()->CopyFrom(in.cooldown_meta_id());
    if (in.has_binlog_config()) {
        out->mutable_binlog_config()->CopyFrom(in.binlog_config());
    }
    out->set_compaction_policy(in.compaction_policy());
    out->set_time_series_compaction_goal_size_mbytes(in.time_series_compaction_goal_size_mbytes());
    out->set_time_series_compaction_file_count_threshold(
            in.time_series_compaction_file_count_threshold());
    out->set_time_series_compaction_time_threshold_seconds(
            in.time_series_compaction_time_threshold_seconds());
    out->set_time_series_compaction_empty_rowsets_threshold(
            in.time_series_compaction_empty_rowsets_threshold());
    out->set_time_series_compaction_level_threshold(in.time_series_compaction_level_threshold());
    out->set_index_id(in.index_id());
    out->set_is_in_memory(in.is_in_memory());
    out->set_is_persistent(in.is_persistent());
    out->set_table_name(in.table_name());
    out->set_ttl_seconds(in.ttl_seconds());
    if (in.has_schema_version()) {
        out->set_schema_version(in.schema_version());
    }
}

void cloud_tablet_meta_to_doris(TabletMetaPB* out, TabletMetaCloudPB&& in) {
    // ATTN: please keep the set order aligned with the definition of proto.
    out->set_table_id(in.table_id());
    out->set_partition_id(in.partition_id());
    out->set_tablet_id(in.tablet_id());
    out->set_schema_hash(in.schema_hash());
    out->set_shard_id(in.shard_id());
    out->set_creation_time(in.creation_time());
    out->set_cumulative_layer_point(in.cumulative_layer_point());
    out->set_tablet_state(in.tablet_state());
    if (in.has_schema()) {
        cloud_tablet_schema_to_doris(out->mutable_schema(), std::move(*in.mutable_schema()));
    }
    if (in.rs_metas_size()) {
        size_t rs_metas_size = in.rs_metas_size();
        out->mutable_rs_metas()->Reserve(rs_metas_size);
        for (size_t i = 0; i < rs_metas_size; ++i) {
            cloud_rowset_meta_to_doris(out->add_rs_metas(), std::move(*in.mutable_rs_metas(i)));
        }
    }
    // ATTN: inc_rs_metas are deprecated, ignored here.
    if (in.has_alter_task()) {
        out->mutable_alter_task()->Swap(in.mutable_alter_task());
    }
    out->set_in_restore_mode(in.in_restore_mode());
    out->mutable_tablet_uid()->CopyFrom(in.tablet_uid());
    out->set_end_rowset_id(in.end_rowset_id());
    out->set_preferred_rowset_type(in.preferred_rowset_type());
    out->set_tablet_type(in.tablet_type());
    if (in.stale_rs_metas_size()) {
        size_t rs_metas_size = in.stale_rs_metas_size();
        out->mutable_stale_rs_metas()->Reserve(rs_metas_size);
        for (size_t i = 0; i < rs_metas_size; i++) {
            cloud_rowset_meta_to_doris(out->add_stale_rs_metas(),
                                       std::move(*in.mutable_stale_rs_metas(i)));
        }
    }
    out->set_replica_id(in.replica_id());
    if (in.has_delete_bitmap()) {
        out->mutable_delete_bitmap()->Swap(in.mutable_delete_bitmap());
    }
    out->set_enable_unique_key_merge_on_write(in.enable_unique_key_merge_on_write());
    out->set_storage_policy_id(in.storage_policy_id());
    out->mutable_cooldown_meta_id()->CopyFrom(in.cooldown_meta_id());
    if (in.has_binlog_config()) {
        out->mutable_binlog_config()->Swap(in.mutable_binlog_config());
    }
    out->set_compaction_policy(in.compaction_policy());
    out->set_time_series_compaction_goal_size_mbytes(in.time_series_compaction_goal_size_mbytes());
    out->set_time_series_compaction_file_count_threshold(
            in.time_series_compaction_file_count_threshold());
    out->set_time_series_compaction_time_threshold_seconds(
            in.time_series_compaction_time_threshold_seconds());
    out->set_time_series_compaction_empty_rowsets_threshold(
            in.time_series_compaction_empty_rowsets_threshold());
    out->set_time_series_compaction_level_threshold(in.time_series_compaction_level_threshold());
    out->set_index_id(in.index_id());
    out->set_is_in_memory(in.is_in_memory());
    out->set_is_persistent(in.is_persistent());
    out->set_table_name(in.table_name());
    out->set_ttl_seconds(in.ttl_seconds());
    if (in.has_schema_version()) {
        out->set_schema_version(in.schema_version());
    }
}

} // namespace doris::cloud
