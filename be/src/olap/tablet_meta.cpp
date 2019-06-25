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

#include <sstream>

#include "olap/file_helper.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/rowset/alpha_rowset_meta.h"
#include "olap/tablet_meta_manager.h"
#include "util/uid_util.h"
#include "util/url_coding.h"

namespace doris {

OLAPStatus AlterTabletTask::init_from_pb(const AlterTabletPB& alter_task) {
    _alter_state = alter_task.alter_state();
    _related_tablet_id = alter_task.related_tablet_id();
    _related_schema_hash = alter_task.related_schema_hash();
    _alter_type = alter_task.alter_type();
    return OLAP_SUCCESS;
}

OLAPStatus AlterTabletTask::to_alter_pb(AlterTabletPB* alter_task) {
    alter_task->set_alter_state(_alter_state);
    alter_task->set_related_tablet_id(_related_tablet_id);
    alter_task->set_related_schema_hash(_related_schema_hash);
    alter_task->set_alter_type(_alter_type);
    return OLAP_SUCCESS;
}

OLAPStatus AlterTabletTask::set_alter_state(AlterTabletState alter_state) { 
    if (_alter_state == ALTER_FAILED && alter_state != ALTER_FAILED) {
        return OLAP_ERR_ALTER_STATUS_ERR;
    } else if (_alter_state == ALTER_FINISHED && alter_state != ALTER_FINISHED) {
        return OLAP_ERR_ALTER_STATUS_ERR;
    }
    _alter_state = alter_state; 
    return OLAP_SUCCESS;
}

OLAPStatus TabletMeta::create(int64_t table_id, int64_t partition_id,
                              int64_t tablet_id, int32_t schema_hash,
                              uint64_t shard_id, const TTabletSchema& tablet_schema,
                              uint32_t next_unique_id,
                              const std::unordered_map<uint32_t, uint32_t>& col_ordinal_to_unique_id,
                              TabletMetaSharedPtr* tablet_meta, TabletUid& tablet_uid) {
    tablet_meta->reset(new TabletMeta(table_id, partition_id,
                                tablet_id, schema_hash,
                                shard_id, tablet_schema,
                                next_unique_id, col_ordinal_to_unique_id, tablet_uid));
    return OLAP_SUCCESS;
}

TabletMeta::TabletMeta() {}

TabletMeta::TabletMeta(int64_t table_id, int64_t partition_id,
                       int64_t tablet_id, int32_t schema_hash,
                       uint64_t shard_id, const TTabletSchema& tablet_schema,
                       uint32_t next_unique_id,
                       const std::unordered_map<uint32_t, uint32_t>& col_ordinal_to_unique_id, 
                       TabletUid tablet_uid) {
    TabletMetaPB tablet_meta_pb;
    tablet_meta_pb.set_table_id(table_id);
    tablet_meta_pb.set_partition_id(partition_id);
    tablet_meta_pb.set_tablet_id(tablet_id);
    tablet_meta_pb.set_schema_hash(schema_hash);
    tablet_meta_pb.set_shard_id(shard_id);
    tablet_meta_pb.set_creation_time(time(NULL));
    tablet_meta_pb.set_cumulative_layer_point(-1);
    tablet_meta_pb.set_tablet_state(PB_RUNNING);
    *(tablet_meta_pb.mutable_tablet_uid()) = tablet_uid.to_proto();
    tablet_meta_pb.set_end_rowset_id(10000);
    TabletSchemaPB* schema = tablet_meta_pb.mutable_schema();
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
    tablet_meta_pb.set_in_restore_mode(false);

    // set column information
    uint32_t col_ordinal = 0;
    uint32_t key_count = 0;
    bool has_bf_columns = false;
    for (TColumn tcolumn : tablet_schema.columns) {
        ColumnPB* column = schema->add_column();
        uint32_t unique_id = col_ordinal_to_unique_id.at(col_ordinal++);
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

    init_from_pb(tablet_meta_pb);
}

OLAPStatus TabletMeta::create_from_file(const std::string& file_path) {
    FileHeader<TabletMetaPB> file_header;
    FileHandler file_handler;

    if (file_handler.open(file_path.c_str(), O_RDONLY) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to open ordinal file. file=" << file_path;
        return OLAP_ERR_IO_ERROR;
    }

    // In file_header.unserialize(), it validates file length, signature, checksum of protobuf.
    if (file_header.unserialize(&file_handler) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to unserialize tablet_meta. file='" << file_path;
        return OLAP_ERR_PARSE_PROTOBUF_ERROR;
    }

    TabletMetaPB tablet_meta_pb;
    try {
       tablet_meta_pb.CopyFrom(file_header.message());
    } catch (...) {
        LOG(WARNING) << "fail to copy protocol buffer object. file='" << file_path;
        return OLAP_ERR_PARSE_PROTOBUF_ERROR;
    }

    return init_from_pb(tablet_meta_pb);
}

OLAPStatus TabletMeta::reset_tablet_uid(const std::string& file_path) {
    OLAPStatus res = OLAP_SUCCESS;
    TabletMeta tmp_tablet_meta;
    if ((res = tmp_tablet_meta.create_from_file(file_path)) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to load tablet meta from file"
                     << ", meta_file=" << file_path;
        return res;
    }
    TabletMetaPB tmp_tablet_meta_pb;
    res = tmp_tablet_meta.to_meta_pb(&tmp_tablet_meta_pb);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to serialize tablet meta to pb object. " 
                     << " , meta_file=" << file_path;
        return res;
    }
    *(tmp_tablet_meta_pb.mutable_tablet_uid()) = TabletUid().to_proto();
    res = save(file_path, tmp_tablet_meta_pb);
    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "fail to save tablet meta pb to " 
                     << " meta_file=" << file_path;
        return res;
    }
    return res;
}

const TabletUid TabletMeta::tablet_uid() {
    return _tablet_uid;
}

std::string TabletMeta::construct_header_file_path(const std::string& schema_hash_path, const int64_t tablet_id) {
    std::stringstream header_name_stream;
    header_name_stream << schema_hash_path << "/" << tablet_id << ".hdr";
    return header_name_stream.str();
}

OLAPStatus TabletMeta::save(const string& file_path) {
    TabletMetaPB tablet_meta_pb;
    RETURN_NOT_OK(to_meta_pb(&tablet_meta_pb));
    return TabletMeta::save(file_path, tablet_meta_pb);
}

OLAPStatus TabletMeta::save(const string& file_path, TabletMetaPB& tablet_meta_pb) {
    DCHECK(!file_path.empty());

    FileHeader<TabletMetaPB> file_header;
    FileHandler file_handler;

    if (file_handler.open_with_mode(file_path.c_str(),
            O_CREAT | O_WRONLY | O_TRUNC, S_IRUSR | S_IWUSR) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to open header file. file='" << file_path;
        return OLAP_ERR_IO_ERROR;
    }

    try {
        file_header.mutable_message()->CopyFrom(tablet_meta_pb);
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

OLAPStatus TabletMeta::save_meta(DataDir* data_dir) {
    WriteLock wrlock(&_meta_lock);
    return _save_meta(data_dir);
}

OLAPStatus TabletMeta::_save_meta(DataDir* data_dir) {
    // check if rowset id all valid, should remove it later
    for (auto& rs_meta : _rs_metas) {
        if (rs_meta->rowset_id() >= _next_rowset_id) {
            LOG(FATAL) << "meta contains invalid rowsetid " 
                       << " tablet=" << full_name() 
                       << " rowset_id=" <<  rs_meta->rowset_id()
                       << " next_rowset_id=" << _next_rowset_id;
        }
    }
    for (auto& rs_meta : _inc_rs_metas) {
        if (rs_meta->rowset_id() >= _next_rowset_id) {
            LOG(FATAL) << "meta contains invalid rowsetid " 
                       << " tablet=" << full_name() 
                       << " rowset_id=" <<  rs_meta->rowset_id()
                       << " next_rowset_id=" << _next_rowset_id;
        }
    }
    // check if _end_rowset_id > 10000
    if (_end_rowset_id < 10000) {
        LOG(FATAL) << "end_rowset_id is invalid" 
                   << " tablet=" << full_name() 
                   << " end_rowset_id=" << _end_rowset_id; 
    }
    // check if tablet uid is valid
    if (_tablet_uid.hi == 0 && _tablet_uid.lo == 0) {
        LOG(FATAL) << "tablet_uid is invalid" 
                   << " tablet=" << full_name() 
                   << " _tablet_uid=" << _tablet_uid.to_string(); 
    }
    string meta_binary;
    RETURN_NOT_OK(serialize(&meta_binary));
    OLAPStatus status = TabletMetaManager::save(data_dir, tablet_id(), schema_hash(), meta_binary);
    if (status != OLAP_SUCCESS) {
       LOG(FATAL) << "fail to save tablet_meta. status=" << status
                  << ", tablet_id=" << tablet_id()
                  << ", schema_hash=" << schema_hash();
    }
    return status;
}

OLAPStatus TabletMeta::serialize(string* meta_binary) {
    TabletMetaPB tablet_meta_pb;
    RETURN_NOT_OK(to_meta_pb(&tablet_meta_pb));
    bool serialize_success = tablet_meta_pb.SerializeToString(meta_binary);
    if (!serialize_success) {
        LOG(FATAL) << "failed to serialize meta " << full_name();
    }
    // deserialize the meta to check the result is correct
    TabletMetaPB de_tablet_meta_pb;
    bool parsed = de_tablet_meta_pb.ParseFromString(*meta_binary);
    if (!parsed) {
        LOG(FATAL) << "deserialize from previous serialize result failed " << full_name();
    }
    return OLAP_SUCCESS;
};

OLAPStatus TabletMeta::deserialize(const string& meta_binary) {
    TabletMetaPB tablet_meta_pb;
    bool parsed = tablet_meta_pb.ParseFromString(meta_binary);
    if (!parsed) {
        LOG(WARNING) << "parse tablet meta failed";
        return OLAP_ERR_INIT_FAILED;
    }
    return init_from_pb(tablet_meta_pb);
}

OLAPStatus TabletMeta::init_from_pb(const TabletMetaPB& tablet_meta_pb) {
    _table_id = tablet_meta_pb.table_id();
    _partition_id = tablet_meta_pb.partition_id();
    _tablet_id = tablet_meta_pb.tablet_id();
    _schema_hash = tablet_meta_pb.schema_hash();
    _shard_id = tablet_meta_pb.shard_id();
    _creation_time = tablet_meta_pb.creation_time();
    _cumulative_layer_point = tablet_meta_pb.cumulative_layer_point();
    _tablet_uid = TabletUid(tablet_meta_pb.tablet_uid());
    _end_rowset_id = tablet_meta_pb.end_rowset_id();
    _initial_end_rowset_id = tablet_meta_pb.end_rowset_id();
    _next_rowset_id = _end_rowset_id + 1;

    // init _tablet_state
    switch (tablet_meta_pb.tablet_state()) {
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
            LOG(WARNING) << "tablet has no state. tablet=" << tablet_id()
                          << ", schema_hash=" << schema_hash();
    }

    // init _schema
    RETURN_NOT_OK(_schema.init_from_pb(tablet_meta_pb.schema()));

    // init _rs_metas
    for (auto& it : tablet_meta_pb.rs_metas()) {
        RowsetMetaSharedPtr rs_meta(new AlphaRowsetMeta());
        rs_meta->init_from_pb(it);
        if (rs_meta->has_delete_predicate()) {
            add_delete_predicate(rs_meta->delete_predicate(), rs_meta->version().first);	
        }
        _rs_metas.push_back(std::move(rs_meta));
    }
    for (auto& it : tablet_meta_pb.inc_rs_metas()) {
        RowsetMetaSharedPtr rs_meta(new AlphaRowsetMeta());
        rs_meta->init_from_pb(it);
        _inc_rs_metas.push_back(std::move(rs_meta));
    }

    // generate AlterTabletTask
    if (tablet_meta_pb.has_alter_task()) {
        AlterTabletTask* alter_tablet_task = new AlterTabletTask();
        RETURN_NOT_OK(alter_tablet_task->init_from_pb(tablet_meta_pb.alter_task()));
        _alter_task.reset(alter_tablet_task);
    }

    if (tablet_meta_pb.has_in_restore_mode()) {
        _in_restore_mode = tablet_meta_pb.in_restore_mode();
    }
    return OLAP_SUCCESS;
}

OLAPStatus TabletMeta::to_meta_pb(TabletMetaPB* tablet_meta_pb) {
    tablet_meta_pb->set_table_id(table_id());
    tablet_meta_pb->set_partition_id(partition_id());
    tablet_meta_pb->set_tablet_id(tablet_id());
    tablet_meta_pb->set_schema_hash(schema_hash());
    tablet_meta_pb->set_shard_id(shard_id());
    tablet_meta_pb->set_creation_time(creation_time());
    tablet_meta_pb->set_cumulative_layer_point(cumulative_layer_point());
    *(tablet_meta_pb->mutable_tablet_uid()) = tablet_uid().to_proto();
    tablet_meta_pb->set_end_rowset_id(_end_rowset_id);
    switch (tablet_state()) {
        case TABLET_NOTREADY:
            tablet_meta_pb->set_tablet_state(PB_NOTREADY);
            break;
        case TABLET_RUNNING:
            tablet_meta_pb->set_tablet_state(PB_RUNNING);
            break;
        case TABLET_TOMBSTONED:
            tablet_meta_pb->set_tablet_state(PB_TOMBSTONED);
            break;
        case TABLET_STOPPED:
            tablet_meta_pb->set_tablet_state(PB_STOPPED);
            break;
        case TABLET_SHUTDOWN:
            tablet_meta_pb->set_tablet_state(PB_SHUTDOWN);
            break;
    }

    for (auto& rs : _rs_metas) {
        rs->to_rowset_pb(tablet_meta_pb->add_rs_metas());
    }
    for (auto rs : _inc_rs_metas) {
        rs->to_rowset_pb(tablet_meta_pb->add_inc_rs_metas());
    }
    _schema.to_schema_pb(tablet_meta_pb->mutable_schema());
    if (_alter_task != nullptr) {
        _alter_task->to_alter_pb(tablet_meta_pb->mutable_alter_task());
    }

    tablet_meta_pb->set_in_restore_mode(in_restore_mode());
    return OLAP_SUCCESS;
}

OLAPStatus TabletMeta::to_json(std::string* json_string, json2pb::Pb2JsonOptions& options) {
    TabletMetaPB tablet_meta_pb;
    RETURN_NOT_OK(to_meta_pb(&tablet_meta_pb));
    json2pb::ProtoMessageToJson(tablet_meta_pb, json_string, options);
    return OLAP_SUCCESS;
}

Version TabletMeta::max_version() const {
    Version max_version = { -1, 0 };
    for (auto& rs_meta : _rs_metas) {
        if (rs_meta->end_version() > max_version.second)  {
            max_version = rs_meta->version();
        } else if (rs_meta->end_version() == max_version.second
                && rs_meta->start_version() == max_version.first) {
            max_version = rs_meta->version();
        }
    }
    return max_version;
}

OLAPStatus TabletMeta::add_rs_meta(const RowsetMetaSharedPtr& rs_meta) {
    // check RowsetMeta is valid
    for (auto& rs : _rs_metas) {
        if (rs->start_version() == rs_meta->start_version()
            && rs->end_version() == rs_meta->end_version()) {
            if (rs->rowset_id() != rs_meta->rowset_id()) {
                LOG(WARNING) << "version already exist. rowset_id=" << rs->rowset_id()
                            << " start_version=" << rs_meta->start_version()
                            << ", end_version=" << rs_meta->end_version()
                            << ", tablet=" << full_name();
                return OLAP_ERR_PUSH_VERSION_ALREADY_EXIST;
            } else {
                // rowsetid,version is equal, it is a duplicate req, skip it
                return OLAP_SUCCESS;
            }
        }
    }

    _rs_metas.push_back(std::move(rs_meta));
    if (rs_meta->has_delete_predicate()) {
        add_delete_predicate(rs_meta->delete_predicate(), rs_meta->version().first);
    }

    return OLAP_SUCCESS;
}

OLAPStatus TabletMeta::delete_rs_meta_by_version(const Version& version, vector<RowsetMetaSharedPtr>* deleted_rs_metas) {
    auto it = _rs_metas.begin();
    while (it != _rs_metas.end()) {
        if ((*it)->version().first == version.first
              && (*it)->version().second == version.second) {
            if (deleted_rs_metas != nullptr) {
                deleted_rs_metas->push_back(*it);
            }
            _rs_metas.erase(it);
        } else {
            ++it;
        }
    }

    return OLAP_SUCCESS;
}

OLAPStatus TabletMeta::modify_rs_metas(const vector<RowsetMetaSharedPtr>& to_add,
                                       const vector<RowsetMetaSharedPtr>& to_delete) {
    for (auto rs_to_del : to_delete) {
        auto it = _rs_metas.begin();
        while (it != _rs_metas.end()) {
            if (rs_to_del->version().first == (*it)->version().first
                  && rs_to_del->version().second == (*it)->version().second) {
                if ((*it)->has_delete_predicate()) {
                    remove_delete_predicate_by_version((*it)->version());
                }
                _rs_metas.erase(it);
            } else {
                it++;
            }
        }
    }

    for (auto rs : to_add) {
        _rs_metas.push_back(std::move(rs));
    }

    return OLAP_SUCCESS;
}

OLAPStatus TabletMeta::revise_rs_metas(const std::vector<RowsetMetaSharedPtr>& rs_metas) {
    WriteLock wrlock(&_meta_lock);
    // delete alter task
    _alter_task.reset();

    // remove all old rs_meta and add new rs_meta
    _rs_metas.clear();

    for (auto& rs_meta : rs_metas) {
        _rs_metas.push_back(rs_meta);
    }

    return OLAP_SUCCESS;
}

OLAPStatus TabletMeta::revise_inc_rs_metas(const std::vector<RowsetMetaSharedPtr>& rs_metas) {
    WriteLock wrlock(&_meta_lock);
    // delete alter task
    _alter_task.reset();

    // remove all old rs_meta and add new rs_meta
    _inc_rs_metas.clear();

    for (auto& rs_meta : rs_metas) {
        _inc_rs_metas.push_back(rs_meta);
    }

    return OLAP_SUCCESS;
}

OLAPStatus TabletMeta::add_inc_rs_meta(const RowsetMetaSharedPtr& rs_meta) {
    // check RowsetMeta is valid
    for (auto rs : _inc_rs_metas) {
        if (rs->start_version() == rs_meta->start_version()
            && rs->end_version() == rs_meta->end_version()) {
            LOG(WARNING) << "rowset already exist. rowset_id=" << rs->rowset_id();
            return OLAP_ERR_ROWSET_ALREADY_EXIST;
        }
    }

    _inc_rs_metas.push_back(std::move(rs_meta));
    return OLAP_SUCCESS;
}

RowsetMetaSharedPtr TabletMeta::acquire_rs_meta_by_version(const Version& version) const {
    RowsetMetaSharedPtr rs_meta = nullptr;
    for (int i = 0; i < _rs_metas.size(); ++i) {
        if (_rs_metas[i]->version().first == version.first
              && _rs_metas[i]->version().second == version.second) {
            rs_meta = _rs_metas[i];
            break;
        }
    }
    return rs_meta;
}

OLAPStatus TabletMeta::delete_inc_rs_meta_by_version(const Version& version) {
    auto it = _inc_rs_metas.begin();
    while (it != _inc_rs_metas.end()) {
        if ((*it)->version().first == version.first
              && (*it)->version().second == version.second) {
            _inc_rs_metas.erase(it);
            break;
        } else {
            it++;
        }
    }

    return OLAP_SUCCESS;
}

RowsetMetaSharedPtr TabletMeta::acquire_inc_rs_meta_by_version(const Version& version) const {
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

OLAPStatus TabletMeta::add_delete_predicate(
            const DeletePredicatePB& delete_predicate, int64_t version) {
    int ordinal = 0;
    for (auto& del_pred : _del_pred_array) {
        if (del_pred.version() == version) {
            break;
        }
        ordinal++;
    }

    if (ordinal < _del_pred_array.size()) {
        // clear existed predicate
        DeletePredicatePB* del_pred = &(_del_pred_array[ordinal]);
        del_pred->clear_sub_predicates();
        for (const string& predicate : delete_predicate.sub_predicates()) {
            del_pred->add_sub_predicates(predicate);
        }
    } else {
        DeletePredicatePB* del_pred = _del_pred_array.Add();
        del_pred->set_version(version);
        for (const string& predicate : delete_predicate.sub_predicates()) {
            del_pred->add_sub_predicates(predicate);
        }
    }
    return OLAP_SUCCESS;
}

OLAPStatus TabletMeta::remove_delete_predicate_by_version(const Version& version) {
    DCHECK(version.first == version.second)
        << "version=" << version.first << "-" << version.second;
    int ordinal = 0;
    for (; ordinal < _del_pred_array.size(); ++ordinal) {
        const DeletePredicatePB& temp = _del_pred_array.Get(ordinal);
        if (temp.version() == version.first) {
            // log delete condtion
            string del_cond_str;
            const google::protobuf::RepeatedPtrField<string>& sub_predicates = temp.sub_predicates();

            for (int i = 0; i != sub_predicates.size(); ++i) {
                del_cond_str += sub_predicates.Get(i) + ";";
            }

            LOG(INFO) << "remove one del_pred. version=" << temp.version()
                      << ", condition=" << del_cond_str;

            // remove delete condition from PB
            _del_pred_array.SwapElements(ordinal, _del_pred_array.size() - 1);
            _del_pred_array.RemoveLast();
        }
    }
    return OLAP_SUCCESS;
}

DelPredicateArray TabletMeta::delete_predicates() const {
    return _del_pred_array;
}

bool TabletMeta::version_for_delete_predicate(const Version& version) {
    if (version.first != version.second) {
        return false;
    }

    for (auto& del_pred : _del_pred_array) {
        if (del_pred.version() == version.first) {
            return true;
        }
    }

    return false;
}

OLAPStatus TabletMeta::get_next_rowset_id(RowsetId* gen_rowset_id, DataDir* data_dir) {
    WriteLock wrlock(&_meta_lock);
    if (_next_rowset_id >= _end_rowset_id) {
        ++_next_rowset_id;
        _end_rowset_id = _next_rowset_id + _batch_interval;
        RETURN_NOT_OK(_save_meta(data_dir));
    }
    *gen_rowset_id = _next_rowset_id;
    ++_next_rowset_id;
    return OLAP_SUCCESS;
}

OLAPStatus TabletMeta::set_next_rowset_id(RowsetId new_rowset_id, DataDir* data_dir) {
    WriteLock wrlock(&_meta_lock);
    // must be < not <=
    if (new_rowset_id < _next_rowset_id) {
        return OLAP_SUCCESS;
    }
    if (new_rowset_id >= _end_rowset_id) {
        _end_rowset_id = new_rowset_id + _batch_interval;
        RETURN_NOT_OK(_save_meta(data_dir));
    }
    _next_rowset_id = new_rowset_id + 1;
    return OLAP_SUCCESS;
}

RowsetId TabletMeta::get_cur_rowset_id() {
    return _next_rowset_id;
}


// return value not reference
// MVCC modification for alter task, upper application get a alter task mirror
AlterTabletTaskSharedPtr TabletMeta::TabletMeta::alter_task() {
    ReadLock rlock(&_meta_lock);
    return _alter_task;
}

OLAPStatus TabletMeta::add_alter_task(const AlterTabletTask& alter_task) {
    WriteLock wrlock(&_meta_lock);
    AlterTabletTask* new_alter_task = new AlterTabletTask();
    *new_alter_task = alter_task;
    _alter_task.reset(new_alter_task);
    return OLAP_SUCCESS;
}

OLAPStatus TabletMeta::delete_alter_task() {
    WriteLock wrlock(&_meta_lock);
    _alter_task.reset();
    return OLAP_SUCCESS;
}

// if alter task is nullptr, return error?
OLAPStatus TabletMeta::set_alter_state(AlterTabletState alter_state) {
    WriteLock wrlock(&_meta_lock);
    if (_alter_task == nullptr) {
        // alter state should be set to ALTER_PREPARED when starting to 
        // alter tablet. In this scenario, _alter_task is null pointer.
        LOG(WARNING) << "original alter task is null, could not set state";
        return OLAP_ERR_ALTER_STATUS_ERR;
    } else {
        AlterTabletTask* alter_tablet_task = new AlterTabletTask();
        *alter_tablet_task = *_alter_task;
        OLAPStatus reset_status = alter_tablet_task->set_alter_state(alter_state);
        if (reset_status != OLAP_SUCCESS) {
            return reset_status;
        }
        _alter_task.reset(alter_tablet_task);
        return OLAP_SUCCESS;
    }
}

std::string TabletMeta::full_name() const {
    std::stringstream ss;
    ss << _tablet_id 
       << "." << _schema_hash 
       << "." << _tablet_uid.to_string();
    return ss.str();
}

OLAPStatus TabletMeta::set_partition_id(int64_t partition_id) {
    if ((_partition_id > 0 && _partition_id != partition_id) || partition_id < 1) {
        LOG(FATAL) << "cur partition id=" << _partition_id
                   << " new partition id=" << partition_id
                   << " not equal";
    }
    _partition_id = partition_id;
    return OLAP_SUCCESS;
}

}  // namespace doris
