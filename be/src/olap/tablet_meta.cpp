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

#include <boost/algorithm/string.hpp>
#include <sstream>

#include "olap/file_helper.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/rowset/alpha_rowset_meta.h"
#include "olap/tablet_meta_manager.h"
#include "util/uid_util.h"
#include "util/url_coding.h"

using std::string;
using std::unordered_map;
using std::vector;

namespace doris {

Status TabletMeta::create(const TCreateTabletReq& request, const TabletUid& tablet_uid,
                          uint64_t shard_id, uint32_t next_unique_id,
                          const unordered_map<uint32_t, uint32_t>& col_ordinal_to_unique_id,
                          TabletMetaSharedPtr* tablet_meta) {
    tablet_meta->reset(new TabletMeta(
            request.table_id, request.partition_id, request.tablet_id,
            request.tablet_schema.schema_hash, shard_id, request.tablet_schema, next_unique_id,
            col_ordinal_to_unique_id, tablet_uid,
            request.__isset.tablet_type ? request.tablet_type : TTabletType::TABLET_TYPE_DISK,
            request.storage_medium, request.storage_param.storage_name));
    return Status::OK();
}

TabletMeta::TabletMeta() : _tablet_uid(0, 0), _schema(new TabletSchema) {}

TabletMeta::TabletMeta(int64_t table_id, int64_t partition_id, int64_t tablet_id,
                       int32_t schema_hash, uint64_t shard_id, const TTabletSchema& tablet_schema,
                       uint32_t next_unique_id,
                       const std::unordered_map<uint32_t, uint32_t>& col_ordinal_to_unique_id,
                       TabletUid tablet_uid, TTabletType::type tabletType,
                       TStorageMedium::type t_storage_medium, const std::string& storage_name)
        : _tablet_uid(0, 0), _schema(new TabletSchema) {
    TabletMetaPB tablet_meta_pb;
    tablet_meta_pb.set_table_id(table_id);
    tablet_meta_pb.set_partition_id(partition_id);
    tablet_meta_pb.set_tablet_id(tablet_id);
    tablet_meta_pb.set_schema_hash(schema_hash);
    tablet_meta_pb.set_shard_id(shard_id);
    tablet_meta_pb.set_creation_time(time(nullptr));
    tablet_meta_pb.set_cumulative_layer_point(-1);
    tablet_meta_pb.set_tablet_state(PB_RUNNING);
    *(tablet_meta_pb.mutable_tablet_uid()) = tablet_uid.to_proto();
    tablet_meta_pb.set_tablet_type(tabletType == TTabletType::TABLET_TYPE_DISK
                                           ? TabletTypePB::TABLET_TYPE_DISK
                                           : TabletTypePB::TABLET_TYPE_MEMORY);
    tablet_meta_pb.set_storage_medium(fs::fs_util::get_storage_medium_pb(t_storage_medium));
    tablet_meta_pb.set_remote_storage_name(storage_name);
    TabletSchemaPB* schema = tablet_meta_pb.mutable_schema();
    schema->set_num_short_key_columns(tablet_schema.short_key_column_count);
    schema->set_num_rows_per_row_block(config::default_num_rows_per_column_file_block);
    schema->set_sequence_col_idx(tablet_schema.sequence_col_idx);
    switch (tablet_schema.keys_type) {
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

    switch (tablet_schema.sort_type) {
    case TSortType::type::ZORDER:
        schema->set_sort_type(SortType::ZORDER);
        break;
    default:
        schema->set_sort_type(SortType::LEXICAL);
    }
    schema->set_sort_col_num(tablet_schema.sort_col_num);
    tablet_meta_pb.set_in_restore_mode(false);

    // set column information
    uint32_t col_ordinal = 0;
    uint32_t key_count = 0;
    bool has_bf_columns = false;
    for (TColumn tcolumn : tablet_schema.columns) {
        ColumnPB* column = schema->add_column();
        uint32_t unique_id = col_ordinal_to_unique_id.at(col_ordinal++);
        _init_column_from_tcolumn(unique_id, tcolumn, column);

        if (column->is_key()) {
            ++key_count;
        }

        if (column->is_bf_column()) {
            has_bf_columns = true;
        }

        if (tablet_schema.__isset.indexes) {
            for (auto& index : tablet_schema.indexes) {
                if (index.index_type == TIndexType::type::BITMAP) {
                    DCHECK_EQ(index.columns.size(), 1);
                    if (boost::iequals(tcolumn.column_name, index.columns[0])) {
                        column->set_has_bitmap_index(true);
                        break;
                    }
                }
            }
        }
    }

    schema->set_next_column_unique_id(next_unique_id);
    if (has_bf_columns && tablet_schema.__isset.bloom_filter_fpp) {
        schema->set_bf_fpp(tablet_schema.bloom_filter_fpp);
    }

    if (tablet_schema.__isset.is_in_memory) {
        schema->set_is_in_memory(tablet_schema.is_in_memory);
    }

    if (tablet_schema.__isset.delete_sign_idx) {
        schema->set_delete_sign_idx(tablet_schema.delete_sign_idx);
    }

    init_from_pb(tablet_meta_pb);
}

TabletMeta::TabletMeta(const TabletMeta& b)
        : _table_id(b._table_id),
          _partition_id(b._partition_id),
          _tablet_id(b._tablet_id),
          _schema_hash(b._schema_hash),
          _shard_id(b._shard_id),
          _creation_time(b._creation_time),
          _cumulative_layer_point(b._cumulative_layer_point),
          _tablet_uid(b._tablet_uid),
          _tablet_type(b._tablet_type),
          _tablet_state(b._tablet_state),
          _schema(b._schema),
          _rs_metas(b._rs_metas),
          _stale_rs_metas(b._stale_rs_metas),
          _del_pred_array(b._del_pred_array),
          _in_restore_mode(b._in_restore_mode),
          _preferred_rowset_type(b._preferred_rowset_type) {}

void TabletMeta::_init_column_from_tcolumn(uint32_t unique_id, const TColumn& tcolumn,
                                           ColumnPB* column) {
    column->set_unique_id(unique_id);
    column->set_name(tcolumn.column_name);
    column->set_has_bitmap_index(false);
    string data_type;
    EnumToString(TPrimitiveType, tcolumn.column_type.type, data_type);
    column->set_type(data_type);

    if (tcolumn.column_type.type == TPrimitiveType::DECIMALV2) {
        column->set_precision(tcolumn.column_type.precision);
        column->set_frac(tcolumn.column_type.scale);
    }
    uint32_t length = TabletColumn::get_field_length_by_type(tcolumn.column_type.type,
                                                             tcolumn.column_type.len);
    column->set_length(length);
    column->set_index_length(length);
    if (tcolumn.column_type.type == TPrimitiveType::VARCHAR ||
        tcolumn.column_type.type == TPrimitiveType::STRING) {
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
        column->set_is_key(true);
        column->set_aggregation("NONE");
    }
    column->set_is_nullable(tcolumn.is_allow_null);
    if (tcolumn.__isset.default_value) {
        column->set_default_value(tcolumn.default_value);
    }
    if (tcolumn.__isset.is_bloom_filter_column) {
        column->set_is_bf_column(tcolumn.is_bloom_filter_column);
    }
    if (tcolumn.column_type.type == TPrimitiveType::ARRAY) {
        ColumnPB* children_column = column->add_children_columns();
        _init_column_from_tcolumn(0, tcolumn.children_column[0], children_column);
    }
}

Status TabletMeta::create_from_file(const string& file_path) {
    FileHeader<TabletMetaPB> file_header;
    FileHandler file_handler;

    if (file_handler.open(file_path, O_RDONLY) != Status::OK()) {
        LOG(WARNING) << "fail to open ordinal file. file=" << file_path;
        return Status::OLAPInternalError(OLAP_ERR_IO_ERROR);
    }

    // In file_header.unserialize(), it validates file length, signature, checksum of protobuf.
    if (file_header.unserialize(&file_handler) != Status::OK()) {
        LOG(WARNING) << "fail to unserialize tablet_meta. file='" << file_path;
        return Status::OLAPInternalError(OLAP_ERR_PARSE_PROTOBUF_ERROR);
    }

    TabletMetaPB tablet_meta_pb;
    try {
        tablet_meta_pb.CopyFrom(file_header.message());
    } catch (...) {
        LOG(WARNING) << "fail to copy protocol buffer object. file='" << file_path;
        return Status::OLAPInternalError(OLAP_ERR_PARSE_PROTOBUF_ERROR);
    }

    init_from_pb(tablet_meta_pb);
    return Status::OK();
}

Status TabletMeta::reset_tablet_uid(const string& header_file) {
    Status res = Status::OK();
    TabletMeta tmp_tablet_meta;
    if ((res = tmp_tablet_meta.create_from_file(header_file)) != Status::OK()) {
        LOG(WARNING) << "fail to load tablet meta from file"
                     << ", meta_file=" << header_file;
        return res;
    }
    TabletMetaPB tmp_tablet_meta_pb;
    tmp_tablet_meta.to_meta_pb(&tmp_tablet_meta_pb);
    *(tmp_tablet_meta_pb.mutable_tablet_uid()) = TabletUid::gen_uid().to_proto();
    res = save(header_file, tmp_tablet_meta_pb);
    if (!res.ok()) {
        LOG(FATAL) << "fail to save tablet meta pb to "
                   << " meta_file=" << header_file;
        return res;
    }
    return res;
}

std::string TabletMeta::construct_header_file_path(const string& schema_hash_path,
                                                   int64_t tablet_id) {
    std::stringstream header_name_stream;
    header_name_stream << schema_hash_path << "/" << tablet_id << ".hdr";
    return header_name_stream.str();
}

Status TabletMeta::save(const string& file_path) {
    TabletMetaPB tablet_meta_pb;
    to_meta_pb(&tablet_meta_pb);
    return TabletMeta::save(file_path, tablet_meta_pb);
}

Status TabletMeta::save(const string& file_path, const TabletMetaPB& tablet_meta_pb) {
    DCHECK(!file_path.empty());

    FileHeader<TabletMetaPB> file_header;
    FileHandler file_handler;

    if (!file_handler.open_with_mode(file_path, O_CREAT | O_WRONLY | O_TRUNC, S_IRUSR | S_IWUSR)) {
        LOG(WARNING) << "fail to open header file. file='" << file_path;
        return Status::OLAPInternalError(OLAP_ERR_IO_ERROR);
    }

    try {
        file_header.mutable_message()->CopyFrom(tablet_meta_pb);
    } catch (...) {
        LOG(WARNING) << "fail to copy protocol buffer object. file='" << file_path;
        return Status::OLAPInternalError(OLAP_ERR_OTHER_ERROR);
    }

    if (file_header.prepare(&file_handler) != Status::OK() ||
        file_header.serialize(&file_handler) != Status::OK()) {
        LOG(WARNING) << "fail to serialize to file header. file='" << file_path;
        return Status::OLAPInternalError(OLAP_ERR_SERIALIZE_PROTOBUF_ERROR);
    }

    return Status::OK();
}

Status TabletMeta::save_meta(DataDir* data_dir) {
    std::lock_guard<std::shared_mutex> wrlock(_meta_lock);
    return _save_meta(data_dir);
}

Status TabletMeta::_save_meta(DataDir* data_dir) {
    // check if tablet uid is valid
    if (_tablet_uid.hi == 0 && _tablet_uid.lo == 0) {
        LOG(FATAL) << "tablet_uid is invalid"
                   << " tablet=" << full_name() << " _tablet_uid=" << _tablet_uid.to_string();
    }
    string meta_binary;
    RETURN_NOT_OK(serialize(&meta_binary));
    Status status = TabletMetaManager::save(data_dir, tablet_id(), schema_hash(), meta_binary);
    if (!status.ok()) {
        LOG(FATAL) << "fail to save tablet_meta. status=" << status << ", tablet_id=" << tablet_id()
                   << ", schema_hash=" << schema_hash();
    }
    return status;
}

Status TabletMeta::serialize(string* meta_binary) {
    TabletMetaPB tablet_meta_pb;
    to_meta_pb(&tablet_meta_pb);
    bool serialize_success = tablet_meta_pb.SerializeToString(meta_binary);
    if (!serialize_success) {
        LOG(FATAL) << "failed to serialize meta " << full_name();
    }
    return Status::OK();
}

Status TabletMeta::deserialize(const string& meta_binary) {
    TabletMetaPB tablet_meta_pb;
    bool parsed = tablet_meta_pb.ParseFromString(meta_binary);
    if (!parsed) {
        LOG(WARNING) << "parse tablet meta failed";
        return Status::OLAPInternalError(OLAP_ERR_INIT_FAILED);
    }
    init_from_pb(tablet_meta_pb);
    return Status::OK();
}

void TabletMeta::init_from_pb(const TabletMetaPB& tablet_meta_pb) {
    _table_id = tablet_meta_pb.table_id();
    _partition_id = tablet_meta_pb.partition_id();
    _tablet_id = tablet_meta_pb.tablet_id();
    _schema_hash = tablet_meta_pb.schema_hash();
    _shard_id = tablet_meta_pb.shard_id();
    _creation_time = tablet_meta_pb.creation_time();
    _cumulative_layer_point = tablet_meta_pb.cumulative_layer_point();
    _tablet_uid = TabletUid(tablet_meta_pb.tablet_uid());
    if (tablet_meta_pb.has_tablet_type()) {
        _tablet_type = tablet_meta_pb.tablet_type();
    } else {
        _tablet_type = TabletTypePB::TABLET_TYPE_DISK;
    }

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
    _schema->init_from_pb(tablet_meta_pb.schema());

    // init _rs_metas
    for (auto& it : tablet_meta_pb.rs_metas()) {
        RowsetMetaSharedPtr rs_meta(new AlphaRowsetMeta());
        rs_meta->init_from_pb(it);
        if (rs_meta->has_delete_predicate()) {
            add_delete_predicate(rs_meta->delete_predicate(), rs_meta->version().first);
        }
        _rs_metas.push_back(std::move(rs_meta));
    }

    for (auto& it : tablet_meta_pb.stale_rs_metas()) {
        RowsetMetaSharedPtr rs_meta(new AlphaRowsetMeta());
        rs_meta->init_from_pb(it);
        _stale_rs_metas.push_back(std::move(rs_meta));
    }

    if (tablet_meta_pb.has_in_restore_mode()) {
        _in_restore_mode = tablet_meta_pb.in_restore_mode();
    }

    if (tablet_meta_pb.has_preferred_rowset_type()) {
        _preferred_rowset_type = tablet_meta_pb.preferred_rowset_type();
    }

    _remote_storage_name = tablet_meta_pb.remote_storage_name();
    _storage_medium = tablet_meta_pb.storage_medium();
}

void TabletMeta::to_meta_pb(TabletMetaPB* tablet_meta_pb) {
    tablet_meta_pb->set_table_id(table_id());
    tablet_meta_pb->set_partition_id(partition_id());
    tablet_meta_pb->set_tablet_id(tablet_id());
    tablet_meta_pb->set_schema_hash(schema_hash());
    tablet_meta_pb->set_shard_id(shard_id());
    tablet_meta_pb->set_creation_time(creation_time());
    tablet_meta_pb->set_cumulative_layer_point(cumulative_layer_point());
    *(tablet_meta_pb->mutable_tablet_uid()) = tablet_uid().to_proto();
    tablet_meta_pb->set_tablet_type(_tablet_type);
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
    for (auto rs : _stale_rs_metas) {
        rs->to_rowset_pb(tablet_meta_pb->add_stale_rs_metas());
    }
    _schema->to_schema_pb(tablet_meta_pb->mutable_schema());

    tablet_meta_pb->set_in_restore_mode(in_restore_mode());

    // to avoid modify tablet meta to the greatest extend
    if (_preferred_rowset_type == BETA_ROWSET) {
        tablet_meta_pb->set_preferred_rowset_type(_preferred_rowset_type);
    }

    tablet_meta_pb->set_remote_storage_name(_remote_storage_name);
    tablet_meta_pb->set_storage_medium(_storage_medium);
}

uint32_t TabletMeta::mem_size() const {
    auto size = sizeof(TabletMeta);
    size += _schema->mem_size();
    return size;
}

void TabletMeta::to_json(string* json_string, json2pb::Pb2JsonOptions& options) {
    TabletMetaPB tablet_meta_pb;
    to_meta_pb(&tablet_meta_pb);
    json2pb::ProtoMessageToJson(tablet_meta_pb, json_string, options);
}

Version TabletMeta::max_version() const {
    Version max_version = {-1, 0};
    for (auto& rs_meta : _rs_metas) {
        if (rs_meta->end_version() > max_version.second) {
            max_version = rs_meta->version();
        }
    }
    return max_version;
}

Status TabletMeta::add_rs_meta(const RowsetMetaSharedPtr& rs_meta) {
    // check RowsetMeta is valid
    for (auto& rs : _rs_metas) {
        if (rs->version() == rs_meta->version()) {
            if (rs->rowset_id() != rs_meta->rowset_id()) {
                LOG(WARNING) << "version already exist. rowset_id=" << rs->rowset_id()
                             << " version=" << rs->version() << ", tablet=" << full_name();
                return Status::OLAPInternalError(OLAP_ERR_PUSH_VERSION_ALREADY_EXIST);
            } else {
                // rowsetid,version is equal, it is a duplicate req, skip it
                return Status::OK();
            }
        }
    }

    _rs_metas.push_back(rs_meta);
    if (rs_meta->has_delete_predicate()) {
        add_delete_predicate(rs_meta->delete_predicate(), rs_meta->version().first);
    }

    return Status::OK();
}

void TabletMeta::delete_rs_meta_by_version(const Version& version,
                                           std::vector<RowsetMetaSharedPtr>* deleted_rs_metas) {
    auto it = _rs_metas.begin();
    while (it != _rs_metas.end()) {
        if ((*it)->version() == version) {
            if (deleted_rs_metas != nullptr) {
                deleted_rs_metas->push_back(*it);
            }
            _rs_metas.erase(it);
            return;
        } else {
            ++it;
        }
    }
}

void TabletMeta::modify_rs_metas(const std::vector<RowsetMetaSharedPtr>& to_add,
                                 const std::vector<RowsetMetaSharedPtr>& to_delete,
                                 bool same_version) {
    // Remove to_delete rowsets from _rs_metas
    for (auto rs_to_del : to_delete) {
        auto it = _rs_metas.begin();
        while (it != _rs_metas.end()) {
            if (rs_to_del->version() == (*it)->version()) {
                if ((*it)->has_delete_predicate()) {
                    remove_delete_predicate_by_version((*it)->version());
                }
                _rs_metas.erase(it);
                // there should be only one rowset match the version
                break;
            } else {
                ++it;
            }
        }
    }
    if (!same_version) {
        // put to_delete rowsets in _stale_rs_metas.
        _stale_rs_metas.insert(_stale_rs_metas.end(), to_delete.begin(), to_delete.end());
    }
    // put to_add rowsets in _rs_metas.
    _rs_metas.insert(_rs_metas.end(), to_add.begin(), to_add.end());
}

// Use the passing "rs_metas" to replace the rs meta in this tablet meta
// Also clear the _stale_rs_metas because this tablet meta maybe copyied from
// an existing tablet before. Add after revise, only the passing "rs_metas"
// is needed.
void TabletMeta::revise_rs_metas(std::vector<RowsetMetaSharedPtr>&& rs_metas) {
    std::lock_guard<std::shared_mutex> wrlock(_meta_lock);
    _rs_metas = std::move(rs_metas);
    _stale_rs_metas.clear();
}

void TabletMeta::delete_stale_rs_meta_by_version(const Version& version) {
    auto it = _stale_rs_metas.begin();
    while (it != _stale_rs_metas.end()) {
        if ((*it)->version() == version) {
            it = _stale_rs_metas.erase(it);
        } else {
            it++;
        }
    }
}

RowsetMetaSharedPtr TabletMeta::acquire_rs_meta_by_version(const Version& version) const {
    for (auto it : _rs_metas) {
        if (it->version() == version) {
            return it;
        }
    }
    return nullptr;
}

RowsetMetaSharedPtr TabletMeta::acquire_stale_rs_meta_by_version(const Version& version) const {
    for (auto it : _stale_rs_metas) {
        if (it->version() == version) {
            return it;
        }
    }
    return nullptr;
}

void TabletMeta::add_delete_predicate(const DeletePredicatePB& delete_predicate, int64_t version) {
    for (auto& del_pred : _del_pred_array) {
        if (del_pred.version() == version) {
            *del_pred.mutable_sub_predicates() = delete_predicate.sub_predicates();
            return;
        }
    }
    DeletePredicatePB* del_pred = _del_pred_array.Add();
    del_pred->set_version(version);
    *del_pred->mutable_sub_predicates() = delete_predicate.sub_predicates();
    *del_pred->mutable_in_predicates() = delete_predicate.in_predicates();
}

void TabletMeta::remove_delete_predicate_by_version(const Version& version) {
    DCHECK(version.first == version.second) << "version=" << version;
    for (int ordinal = 0; ordinal < _del_pred_array.size(); ++ordinal) {
        const DeletePredicatePB& temp = _del_pred_array.Get(ordinal);
        if (temp.version() == version.first) {
            // log delete condition
            string del_cond_str;
            for (const auto& it : temp.sub_predicates()) {
                del_cond_str += it + ";";
            }
            VLOG_NOTICE << "remove one del_pred. version=" << temp.version()
                        << ", condition=" << del_cond_str;

            // remove delete condition from PB
            _del_pred_array.SwapElements(ordinal, _del_pred_array.size() - 1);
            _del_pred_array.RemoveLast();
        }
    }
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

std::string TabletMeta::full_name() const {
    std::stringstream ss;
    ss << _tablet_id << "." << _schema_hash << "." << _tablet_uid.to_string();
    return ss.str();
}

Status TabletMeta::set_partition_id(int64_t partition_id) {
    if ((_partition_id > 0 && _partition_id != partition_id) || partition_id < 1) {
        LOG(FATAL) << "cur partition id=" << _partition_id << " new partition id=" << partition_id
                   << " not equal";
    }
    _partition_id = partition_id;
    return Status::OK();
}

bool operator==(const TabletMeta& a, const TabletMeta& b) {
    if (a._table_id != b._table_id) return false;
    if (a._partition_id != b._partition_id) return false;
    if (a._tablet_id != b._tablet_id) return false;
    if (a._schema_hash != b._schema_hash) return false;
    if (a._shard_id != b._shard_id) return false;
    if (a._creation_time != b._creation_time) return false;
    if (a._cumulative_layer_point != b._cumulative_layer_point) return false;
    if (a._tablet_uid != b._tablet_uid) return false;
    if (a._tablet_type != b._tablet_type) return false;
    if (a._tablet_state != b._tablet_state) return false;
    if (*a._schema != *b._schema) return false;
    if (a._rs_metas.size() != b._rs_metas.size()) return false;
    for (int i = 0; i < a._rs_metas.size(); ++i) {
        if (a._rs_metas[i] != b._rs_metas[i]) return false;
    }
    if (a._in_restore_mode != b._in_restore_mode) return false;
    if (a._preferred_rowset_type != b._preferred_rowset_type) return false;
    if (a._storage_medium != b._storage_medium) return false;
    if (a._remote_storage_name != b._remote_storage_name) return false;
    return true;
}

bool operator!=(const TabletMeta& a, const TabletMeta& b) {
    return !(a == b);
}

} // namespace doris
