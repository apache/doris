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

#include "olap/tablet_meta.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/tablet_meta_manager.h"

namespace doris {

OLAPStatus TabletMeta::create(int64_t table_id, int64_t partition_id,
                              int64_t tablet_id, int64_t schema_hash,
                              uint64_t shard_id, const TTabletSchema& tablet_schema,
                              uint32_t next_unique_id,
                              const std::unordered_map<uint32_t, uint32_t>& col_ordinal_to_unique_id,
                              TabletMeta** tablet_meta) {
    *tablet_meta = new TabletMeta(table_id, partition_id,
                                  tablet_id, schema_hash,
                                  shard_id, tablet_schema,
                                  next_unique_id, col_ordinal_to_unique_id);
    return OLAP_SUCCESS;
}

TabletMeta::TabletMeta(int64_t table_id, int64_t partition_id,
                       int64_t tablet_id, int64_t schema_hash,
                       uint64_t shard_id, const TTabletSchema& tablet_schema,
                       uint32_t next_unique_id,
                       const std::unordered_map<uint32_t, uint32_t>& col_ordinal_to_unique_id) {
    _tablet_meta_pb.set_table_id(table_id);
    _tablet_meta_pb.set_partition_id(partition_id);
    _tablet_meta_pb.set_tablet_id(tablet_id);
    _tablet_meta_pb.set_schema_hash(schema_hash);
    _tablet_meta_pb.set_shard_id(shard_id);
    TabletSchemaPB* schema = _tablet_meta_pb.mutable_schema();
    schema->set_num_short_key_columns(tablet_schema.short_key_column_count);
    schema->set_num_rows_per_row_block(config::default_num_rows_per_column_file_block);
    switch(tablet_schema.keys_type) {
        case TKeysType::DUP_KEYS:
            schema->set_keys_type(KeysType::DUP_KEYS);
            break;
        case TKeysType::UNIQUE_KEYS:
            schema->set_keys_type(KeysType::UNIQUE_KEYS);
            break;
        case TKeysType::AGG_KEYS:
            schema->set_keys_type(KeysType::AGG_KEYS);
            break;
        default:
            LOG(WARNING) << "unknown tablet keys type";
            break;
    }
    schema->set_compress_kind(COMPRESS_LZ4);

    // set column information
    uint32_t col_ordinal = 0;
    uint32_t key_count = 0;
    bool has_bf_columns = false;
    for (TColumn tcolumn : tablet_schema.columns) {
        ColumnPB* column = schema->add_column();
        uint32_t unique_id = col_ordinal_to_unique_id.at(col_ordinal);
        column->set_unique_id(unique_id);
        column->set_name(tcolumn.column_name);
        string data_type;
        EnumToString(TPrimitiveType, tcolumn.column_type.type, data_type);
        column->set_type(data_type);
        if (tcolumn.column_type.type == TPrimitiveType::DECIMAL) {
            column->set_precision(tcolumn.column_type.precision);
            column->set_frac(tcolumn.column_type.scale);
        }
        uint32_t length = FieldInfo::get_field_length_by_type(
                tcolumn.column_type.type, tcolumn.column_type.len);
                column->set_length(length);
        column->set_index_length(length);
        if (tcolumn.column_type.type == TPrimitiveType::VARCHAR || tcolumn.column_type.type == TPrimitiveType::HLL) {
            if (!tcolumn.column_type.__isset.index_len) {
                column->set_index_length(10);
            } else {
                column->set_index_length(tcolumn.column_type.index_len);
            }
        }
        if (!tcolumn.is_key) {
            column->set_is_key(false);
            string aggregation_type;
            EnumToString(TAggregationType, tcolumn.aggregation_type, aggregation_type);
            column->set_aggregation(aggregation_type);
        } else {
            ++key_count;
            column->set_is_key(true);
            column->set_aggregation("NONE");
        }
        column->set_is_nullable(tcolumn.is_allow_null);
        if (tcolumn.__isset.default_value) {
            column->set_default_value(tcolumn.default_value);
        }
        if (tcolumn.__isset.is_bloom_filter_column) {
            column->set_is_bf_column(tcolumn.is_bloom_filter_column);
            has_bf_columns = true;
        }
    }

    schema->set_next_column_unique_id(next_unique_id);
    if (has_bf_columns && tablet_schema.__isset.bloom_filter_fpp) {
        schema->set_bf_fpp(tablet_schema.bloom_filter_fpp);
    }

    _tablet_meta_pb.set_creation_time(time(NULL));
    _tablet_meta_pb.set_cumulative_layer_point(-1);
}

TabletMeta::TabletMeta(DataDir* data_dir) {
    _data_dir = data_dir;
}

OLAPStatus TabletMeta::serialize(string* meta_binary) const {
    std::lock_guard<std::mutex> lock(_mutex);
    return serialize_unlock(meta_binary);
};

OLAPStatus TabletMeta::serialize_unlock(string* meta_binary) const {
    _tablet_meta_pb.SerializeToString(meta_binary);
    return OLAP_SUCCESS;
};

OLAPStatus TabletMeta::deserialize(const string& meta_binary) {
    std::lock_guard<std::mutex> lock(_mutex);
    return deserialize_unlock(meta_binary);
}

OLAPStatus TabletMeta::deserialize_unlock(const string& meta_binary) {
    _tablet_meta_pb.ParseFromString(meta_binary);
    _table_id = _tablet_meta_pb.table_id();
    _partition_id = _tablet_meta_pb.partition_id();
    _tablet_id = _tablet_meta_pb.tablet_id();
    _schema_hash = _tablet_meta_pb.schema_hash();
    _shard_id = _tablet_meta_pb.shard_id();
    RETURN_NOT_OK(_schema.init_from_pb(_tablet_meta_pb.schema()));
    for (auto& it : _tablet_meta_pb.rs_metas()) {
        RowsetMetaSharedPtr rs_meta(new RowsetMeta());
        rs_meta->init_from_pb(it);
        _rs_metas.push_back(std::move(rs_meta));
    }
    for (auto& it : _tablet_meta_pb.inc_rs_metas()) {
        RowsetMetaSharedPtr rs_meta(new RowsetMeta());
        rs_meta->init_from_pb(it);
        _rs_metas.push_back(std::move(rs_meta));
    }

    // generate TabletState
    switch (_tablet_meta_pb.tablet_state()) {
        case PB_NOTREADY:
            _tablet_state = TabletState::TABLET_NOTREADY;
            break;
        case PB_RUNNING:
            _tablet_state = TabletState::TABLET_RUNNING;
            break;
        case PB_TOMBSTONED:
            _tablet_state = TabletState::TABLET_TOMBSTONED;
            break;
        case PB_STOPPED:
            _tablet_state = TabletState::TABLET_STOPPED;
            break;
        case PB_SHUTDOWN:
            _tablet_state = TabletState::TABLET_SHUTDOWN;
            break;
        default:
            LOG(WARNING) << "tablet has no state. tablet=" << _tablet_id
                          << ", schema_hash=" << _schema_hash;
    }

    // generate AlterTabletTask
    RETURN_NOT_OK(_alter_task.init_from_pb(_tablet_meta_pb.alter_tablet_task()));
    return OLAP_SUCCESS;
}

OLAPStatus TabletMeta::save_meta() {
    std::lock_guard<std::mutex> lock(_mutex);
    return save_meta_unlock();
}

OLAPStatus TabletMeta::save_meta_unlock() {
    string meta_binary;
    serialize_unlock(&meta_binary);
    OLAPStatus status = TabletMetaManager::save(_data_dir, _tablet_id, _schema_hash, meta_binary);
    if (status != OLAP_SUCCESS) {
       LOG(WARNING) << "fail to save tablet_meta. status=" << status
                    << ", tablet_id=" << _tablet_id
                    << ", schema_hash=" << _schema_hash;
    }
    return status;
}

OLAPStatus TabletMeta::to_tablet_pb(TabletMetaPB* tablet_meta_pb) {
    std::lock_guard<std::mutex> lock(_mutex);
    return to_tablet_pb_unlock(tablet_meta_pb);
}

OLAPStatus TabletMeta::to_tablet_pb_unlock(TabletMetaPB* tablet_meta_pb) {
    tablet_meta_pb->set_table_id(_table_id);
    tablet_meta_pb->set_partition_id(_partition_id);
    tablet_meta_pb->set_table_id(_tablet_id);
    tablet_meta_pb->set_schema_hash(_schema_hash);
    tablet_meta_pb->set_shard_id(_shard_id);

    for (auto& rs : _rs_metas) {
        rs->to_rowset_pb(tablet_meta_pb->add_rs_metas());
    }
    for (auto rs : _inc_rs_metas) {
        rs->to_rowset_pb(tablet_meta_pb->add_inc_rs_metas());
    }
    _schema.to_schema_pb(tablet_meta_pb->mutable_schema());

    return OLAP_SUCCESS;
}

OLAPStatus TabletMeta::add_inc_rs_meta(const RowsetMetaSharedPtr& rs_meta) {
    std::lock_guard<std::mutex> lock(_mutex);

    // check RowsetMeta is valid
    for (auto rs : _inc_rs_metas) {
        if (rs->rowset_id() == rs_meta->rowset_id()
            && rs->start_version() == rs_meta->start_version()
            && rs->end_version() == rs_meta->end_version()) {
            LOG(WARNING) << "rowset already exist. rowset_id=" << rs->rowset_id();
            return OLAP_SUCCESS;
        }
    }

    _inc_rs_metas.push_back(std::move(rs_meta));
    RowsetMetaPB* rs_meta_pb = _tablet_meta_pb.add_inc_rs_metas();
    RETURN_NOT_OK(rs_meta->to_rowset_pb(rs_meta_pb));
    RETURN_NOT_OK(save_meta_unlock());

    return OLAP_SUCCESS;
}

OLAPStatus TabletMeta::delete_inc_rs_meta_by_version(const Version& version) {
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _inc_rs_metas.begin();
    while (it != _inc_rs_metas.end()) {
        if ((*it)->version().first == version.first
              && (*it)->version().second == version.second) {
            _inc_rs_metas.erase(it);
        }
    }

    TabletMetaPB tablet_meta_pb;
    RETURN_NOT_OK(to_tablet_pb_unlock(&tablet_meta_pb));
    _tablet_meta_pb = std::move(tablet_meta_pb);
    RETURN_NOT_OK(save_meta_unlock());

    return OLAP_SUCCESS;
}

const RowsetMetaSharedPtr TabletMeta::get_inc_rowset(const Version& version) const {
    std::lock_guard<std::mutex> lock(_mutex);
    RowsetMetaSharedPtr rs_meta = nullptr;
    for (int i = 0; i < _inc_rs_metas.size(); ++i) {
        if (_inc_rs_metas[i]->version().first == version.first
              && _inc_rs_metas[i]->version().second == version.second) {
            rs_meta = _inc_rs_metas[i];
            break;
        }
    }
    return rs_meta;
}

OLAPStatus TabletMeta::modify_rowsets(const vector<RowsetMetaSharedPtr>& to_add,
                                     const vector<RowsetMetaSharedPtr>& to_delete) {
    std::lock_guard<std::mutex> lock(_mutex);
    for (auto del_rs : to_delete) {
        auto it = _rs_metas.begin();
        while (it != _rs_metas.end()) {
            if (del_rs->version().first == (*it)->version().first
                  && del_rs->version().second == (*it)->version().second) {
                _rs_metas.erase(it);
            }
        }
    }

    for (auto rs : to_add) {
        _rs_metas.push_back(std::move(rs));
    }

    TabletMetaPB tablet_meta_pb;
    RETURN_NOT_OK(to_tablet_pb_unlock(&tablet_meta_pb));
    _tablet_meta_pb = std::move(tablet_meta_pb);
    RETURN_NOT_OK(save_meta_unlock());

    return OLAP_SUCCESS;
}

OLAPStatus TabletMeta::add_alter_task(const AlterTabletTask& alter_task) {
    std::lock_guard<std::mutex> lock(_mutex);
    _alter_task = alter_task;
    RETURN_NOT_OK(_alter_task.to_alter_pb(_tablet_meta_pb.mutable_alter_tablet_task()));
    RETURN_NOT_OK(save_meta_unlock());
    return OLAP_SUCCESS;
}

OLAPStatus TabletMeta::delete_alter_task() {
    std::lock_guard<std::mutex> lock(_mutex);
    _tablet_meta_pb.clear_alter_tablet_task();
    RETURN_NOT_OK(save_meta_unlock());

    return OLAP_SUCCESS;
}

}  // namespace doris
