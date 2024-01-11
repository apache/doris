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

namespace doris::cloud {

TabletSchemaCloudPB doris_tablet_schema_to_cloud(const TabletSchemaPB& in) {
    // ATTN: please keep the set order aligned with the definition of proto `TabletSchemaCloudPB`.
    TabletSchemaCloudPB out;
    out.set_keys_type(in.keys_type());
    out.mutable_column()->CopyFrom(in.column());
    out.set_num_short_key_columns(in.num_short_key_columns());
    out.set_num_rows_per_row_block(in.num_rows_per_row_block());
    out.set_compress_kind(in.compress_kind());
    out.set_bf_fpp(in.bf_fpp());
    out.set_next_column_unique_id(in.next_column_unique_id());
    out.set_delete_sign_idx(in.delete_sign_idx());
    out.set_sequence_col_idx(in.sequence_col_idx());
    out.set_sort_type(in.sort_type());
    out.set_sort_col_num(in.sort_col_num());
    out.set_compression_type(in.compression_type());
    out.set_schema_version(in.schema_version());
    out.set_disable_auto_compaction(in.disable_auto_compaction());
    out.mutable_index()->CopyFrom(in.index());
    out.set_version_col_idx(in.version_col_idx());
    out.set_store_row_column(in.store_row_column());
    out.set_enable_single_replica_compaction(in.enable_single_replica_compaction());
    out.set_skip_write_index_on_load(in.skip_write_index_on_load());
    out.mutable_cluster_key_idxes()->CopyFrom(in.cluster_key_idxes());
    out.set_is_dynamic_schema(in.is_dynamic_schema());
    return out;
}

TabletSchemaCloudPB doris_tablet_schema_to_cloud(TabletSchemaPB&& in) {
    // ATTN: please keep the set order aligned with the definition of proto.
    TabletSchemaCloudPB out;
    out.set_keys_type(in.keys_type());
    out.mutable_column()->Swap(in.mutable_column());
    out.set_num_short_key_columns(in.num_short_key_columns());
    out.set_num_rows_per_row_block(in.num_rows_per_row_block());
    out.set_compress_kind(in.compress_kind());
    out.set_bf_fpp(in.bf_fpp());
    out.set_next_column_unique_id(in.next_column_unique_id());
    out.set_delete_sign_idx(in.delete_sign_idx());
    out.set_sequence_col_idx(in.sequence_col_idx());
    out.set_sort_type(in.sort_type());
    out.set_sort_col_num(in.sort_col_num());
    out.set_compression_type(in.compression_type());
    out.set_schema_version(in.schema_version());
    out.set_disable_auto_compaction(in.disable_auto_compaction());
    out.mutable_index()->Swap(in.mutable_index());
    out.set_version_col_idx(in.version_col_idx());
    out.set_store_row_column(in.store_row_column());
    out.set_enable_single_replica_compaction(in.enable_single_replica_compaction());
    out.set_skip_write_index_on_load(in.skip_write_index_on_load());
    out.mutable_cluster_key_idxes()->Swap(in.mutable_cluster_key_idxes());
    out.set_is_dynamic_schema(in.is_dynamic_schema());
    return out;
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
}

} // namespace doris::cloud
