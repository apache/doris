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

namespace doris {

TabletMeta::TabletMeta(DataDir* data_dir) {
    _data_dir = data_dir;
}

NewStatus TabletMeta::serialize(string* meta_binary) {
    std::lock_guard<std::mutex> lock(_mutex);
    return serialize_unlock(meta_binary);
};

NewStatus TabletMeta::serialize_unlock(string* meta_binary) {
    _tablet_meta_pb.SerializeToString(meta_binary);
    return NewStatus::OK();
};

NewStatus TabletMeta::deserialize(string* meta_binary) {
    std::lock_guard<std::mutex> lock(_mutex);
    return deserialize_unlock(meta_binary);
}

NewStatus TabletMeta::deserialize_unlock(const string& meta_binary) {
    _tablet_meta_pb.ParseFromString(meta_binary);
    _table_id = _tablet_meta_pb.table_id();
    _partition_id = _tablet_meta_pb.partition_id();
    _tablet_id = _tablet_meta_pb.tablet_id();
    _schema_hash = _tablet_meta_pb.schema_hash();
    _shard_id = _tablet_meta_pb.shard_id();
    RETURN_NOT_OK(_schema.deserialize_from_pb(_tablet_meta_pb.schema()));
    for (auto& it : _tablet_meta_pb.rs_metas()) {
        RowsetMeta rs_meta;
        rs_meta.deserialize_from_pb(it);
        _rs_metas.push_back(std::move(rs_meta));
    }
    for (auto& it : _tablet_meta_pb.inc_rs_metas()) {
        RowsetMeta rs_meta;
        rs_meta.deserialize_from_pb(it);
        _rs_metas.push_back(std::move(rs_meta));
    }

    // generate TabletState
    switch (_tablet_meta_pb.tablet_state()) {
        case NOT_READY:
            _tablet_state = TabletState::kNotReady;
            break;
        case RUNNING:
            _tablet_state = TabletState::kRunning;
            break;
        case TOMBSTONED:
            _tablet_state = TabletState::kTombstoned;
            break;
        case STOPPED:
            _tablet_state = TabletState::kStopped;
            break;
        case SHUTDOWN:
            _tablet_state = TabletState::kShutdown;
            break;
        default:
            LOG(WARNNING) << "tablet has no state. tablet=" << _tablet_id
                          << ", schema_hash=" << _schema_hash;
    }

    // generate AlterTabletTask
    RETURN(_alter_task.deserialize_from_pb(_tablet_meta_pb.alter_tablet_task()));
    return NewStatus::OK();
}

NewStatus TabletMeta::save_meta() {
    std::lock_guard<std::mutex> lock(_mutex);
    return save_meta_unlock();
}

NewStatus TabletMeta::save_meta_unlock() {
    string meta_binary;
    serialize_unlock(meta_binary);
    NewStatus status = TabletMetaManager::save(_data_dir, _tablet_id, _schema_hash, meta_binary);
    if (!status.ok()) {
       LOG(WARNING) << "fail to save tablet_meta. status=" << status.to_string()
                    << ", tablet_id=" << _tablet_id
                    << ", schema_hash=" << _schema_hash;
    }
    return status;
}

NewStatus TabletMeta::to_meta_pb(TabletMetaPB* tablet_meta_pb) {
    std::lock_guard<std::mutex> lock(_mutex);
    return to_meta_pb_unlock(tablet_meta_pb);
}

NewStatus TabletMeta::to_meta_pb_unlock(TabletMetaPB* tablet_meta_pb) {
    tablet_meta_pb->set_table_id(_table_id);
    tablet_meta_pb->set_partition_id(_partition_id);
    tablet_meta_pb->set_table_id(_tablet_id);
    tablet_meta_pb->set_schema_hash(_schema_hash);
    tablet_meta_pb->set_shard_id(_shard_id);

    tablet_meta_pb->set_tablet_name(_tablet_name);
    for (auto rs : _rs_metas) {
        rs->to_meta_pb(pb.add_rs_meta());
    }
    for (auto rs : _inc_rs_metas) {
        rs->to_meta_pb(pb.add_inc_rc_meta());
    }
    _schema.to_schema_pb(pb.mutable_schema());

    return Status::OK();
}

NewStatus TabletMeta::add_inc_rs_meta(const RowsetMeta& rs_meta) {
    std::lock_guard<std::mutex> lock(_mutex);

    // check RowsetMeta is valid
    for (auts rs : _inc_rs_metas) {
        if (rs.rowset_id() == rs_meta.rowset_id()()) {
            LOG(WARNING) << "rowset already exist. rowset_id=" << rs.rowset_id();
            return NewStatus::AlreadyExist("rowset already exist.");
        }
    }

    _inc_rs_metas.push_back(std::move(rs_meta));
    RowsetMetaPB* rs_meta_pb = _tablet_meta_pb->add_inc_rs_meta();
    RETURN_NOT_OK(rs_meta->to_meta_pb(rs_meta_pb));
    RETURN_NOT_OK(save_meta_unlock());

    return Status::OK();
}

NewStatus TabletMeta::delete_inc_rs_meta_by_version(const Version& version)
    std::lock_guard<std::mutex> lock(_mutex);
    for (auto rs : _inc_rs_metas) {
        if (rs.version() == version) {
            _inc_rs_metas.erase(rs);
        }
    }

    TabletMetaPB tablet_meta_pb;
    RETURN_NOT_OK(to_meta_pb_unlock(&tablet_meta_pb));
    _tablet_meta_pb = std::move(tablet_meta_pb);
    RETURN_NOT_OK(save_meta_unlock());

    return NewStatus::OK();
}

const RowsetMeta* TabletMeta::get_inc_rowset(const Version& version) const;
    std::lock_guard<std::mutex> lock(_mutex);
    RowsetMeta* rs_meta = nullptr;
    for (int i = 0; i < _inc_rs_metas.size(); ++i) {
        if (_inc_rs_metas[i].version() == version) {
            rs_meta = &(_inc_rs_metas[i]);
            break;
        }
    }
    return rs_meta;
}

NewStatus TabletMeta::modify_rowsets(const vector<RowsetMeta>& to_add,
                                     const vector<RowsetMeta>& to_delete) {
    std::lock_guard<std::mutex> lock(_mutex);
    for (auto& to_delete_rs : to_delete) {
        if (ContainsKey(to_delete, _rs_metas)) {
            _rs_metas.erase(rs);
        }
    }

    for (auto rs : to_add) {
        _rs_metas.push_back(std::move(rs));
    }

    TabletMetaPB tablet_meta_pb;
    RETURN_NOT_OK(to_meta_pb_unlock(&tablet_meta_pb));
    _tablet_meta_pb = std::move(tablet_meta_pb);
    RETURN_NOT_OK(save_meta_unlock());

    return NewStatus::OK();
}

NewStatus TabletMeta::add_alter_task(const AlterTabletTask& alter_task) {
    std::lock_guard<std::mutex> lock(_mutex);
    _alter_task = alter_task;
    RETURN_NOT_OK(_alter_task.to_alter_pb(_tablet_meta_pb.mutable_alter_tablet_task));
    RETURN_NOT_OK(save_meta_unlock());
    return NewStatus::OK();
}

NewStatus TabletMeta::delete_alter_task() {
    std::lock_guard<std::mutex> lock(_mutex);
    alter_task.clear();
    _tablet_meta_pb.clear_alter_tablet_task();
    RETURN_NOT_OK(save_meta_unlock());

    return NewStatus::OK();
}

}  // namespace doris
