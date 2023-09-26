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

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/olap_common.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/segment_v2.pb.h>
#include <gen_cpp/types.pb.h>
#include <json2pb/pb_to_json.h>
#include <time.h>

#include <set>
#include <utility>

#include "common/config.h"
#include "io/fs/file_writer.h"
#include "olap/data_dir.h"
#include "olap/file_header.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/tablet_meta_manager.h"
#include "olap/utils.h"
#include "util/string_util.h"
#include "util/time.h"
#include "util/uid_util.h"

using std::string;
using std::unordered_map;
using std::vector;

namespace doris {
using namespace ErrorCode;

Status TabletMeta::create(const TCreateTabletReq& request, const TabletUid& tablet_uid,
                          uint64_t shard_id, uint32_t next_unique_id,
                          const unordered_map<uint32_t, uint32_t>& col_ordinal_to_unique_id,
                          TabletMetaSharedPtr* tablet_meta) {
    std::optional<TBinlogConfig> binlog_config;
    if (request.__isset.binlog_config) {
        binlog_config = request.binlog_config;
    }
    *tablet_meta = std::make_shared<TabletMeta>(
            request.table_id, request.partition_id, request.tablet_id, request.replica_id,
            request.tablet_schema.schema_hash, shard_id, request.tablet_schema, next_unique_id,
            col_ordinal_to_unique_id, tablet_uid,
            request.__isset.tablet_type ? request.tablet_type : TTabletType::TABLET_TYPE_DISK,
            request.compression_type, request.storage_policy_id,
            request.__isset.enable_unique_key_merge_on_write
                    ? request.enable_unique_key_merge_on_write
                    : false,
            std::move(binlog_config), request.compaction_policy,
            request.time_series_compaction_goal_size_mbytes,
            request.time_series_compaction_file_count_threshold,
            request.time_series_compaction_time_threshold_seconds);
    return Status::OK();
}

TabletMeta::TabletMeta()
        : _tablet_uid(0, 0),
          _schema(new TabletSchema),
          _delete_bitmap(new DeleteBitmap(_tablet_id)) {}

TabletMeta::TabletMeta(int64_t table_id, int64_t partition_id, int64_t tablet_id,
                       int64_t replica_id, int32_t schema_hash, uint64_t shard_id,
                       const TTabletSchema& tablet_schema, uint32_t next_unique_id,
                       const std::unordered_map<uint32_t, uint32_t>& col_ordinal_to_unique_id,
                       TabletUid tablet_uid, TTabletType::type tabletType,
                       TCompressionType::type compression_type, int64_t storage_policy_id,
                       bool enable_unique_key_merge_on_write,
                       std::optional<TBinlogConfig> binlog_config, std::string compaction_policy,
                       int64_t time_series_compaction_goal_size_mbytes,
                       int64_t time_series_compaction_file_count_threshold,
                       int64_t time_series_compaction_time_threshold_seconds)
        : _tablet_uid(0, 0),
          _schema(new TabletSchema),
          _delete_bitmap(new DeleteBitmap(tablet_id)) {
    TabletMetaPB tablet_meta_pb;
    tablet_meta_pb.set_table_id(table_id);
    tablet_meta_pb.set_partition_id(partition_id);
    tablet_meta_pb.set_tablet_id(tablet_id);
    tablet_meta_pb.set_replica_id(replica_id);
    tablet_meta_pb.set_schema_hash(schema_hash);
    tablet_meta_pb.set_shard_id(shard_id);
    // Persist the creation time, but it is not used
    tablet_meta_pb.set_creation_time(time(nullptr));
    tablet_meta_pb.set_cumulative_layer_point(-1);
    tablet_meta_pb.set_tablet_state(PB_RUNNING);
    *(tablet_meta_pb.mutable_tablet_uid()) = tablet_uid.to_proto();
    tablet_meta_pb.set_tablet_type(tabletType == TTabletType::TABLET_TYPE_DISK
                                           ? TabletTypePB::TABLET_TYPE_DISK
                                           : TabletTypePB::TABLET_TYPE_MEMORY);
    tablet_meta_pb.set_enable_unique_key_merge_on_write(enable_unique_key_merge_on_write);
    tablet_meta_pb.set_storage_policy_id(storage_policy_id);
    tablet_meta_pb.set_compaction_policy(compaction_policy);
    tablet_meta_pb.set_time_series_compaction_goal_size_mbytes(
            time_series_compaction_goal_size_mbytes);
    tablet_meta_pb.set_time_series_compaction_file_count_threshold(
            time_series_compaction_file_count_threshold);
    tablet_meta_pb.set_time_series_compaction_time_threshold_seconds(
            time_series_compaction_time_threshold_seconds);
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
    // compress_kind used to compress segment files
    schema->set_compress_kind(COMPRESS_LZ4);

    // compression_type used to compress segment page
    switch (compression_type) {
    case TCompressionType::NO_COMPRESSION:
        schema->set_compression_type(segment_v2::NO_COMPRESSION);
        break;
    case TCompressionType::SNAPPY:
        schema->set_compression_type(segment_v2::SNAPPY);
        break;
    case TCompressionType::LZ4:
        schema->set_compression_type(segment_v2::LZ4);
        break;
    case TCompressionType::LZ4F:
        schema->set_compression_type(segment_v2::LZ4F);
        break;
    case TCompressionType::ZLIB:
        schema->set_compression_type(segment_v2::ZLIB);
        break;
    case TCompressionType::ZSTD:
        schema->set_compression_type(segment_v2::ZSTD);
        break;
    default:
        schema->set_compression_type(segment_v2::LZ4F);
        break;
    }

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
    bool has_bf_columns = false;
    for (TColumn tcolumn : tablet_schema.columns) {
        ColumnPB* column = schema->add_column();
        uint32_t unique_id = -1;
        if (tcolumn.col_unique_id >= 0) {
            unique_id = tcolumn.col_unique_id;
        } else {
            unique_id = col_ordinal_to_unique_id.at(col_ordinal);
        }
        col_ordinal++;
        init_column_from_tcolumn(unique_id, tcolumn, column);

        if (column->is_bf_column()) {
            has_bf_columns = true;
        }

        if (tablet_schema.__isset.indexes) {
            for (auto& index : tablet_schema.indexes) {
                if (index.index_type == TIndexType::type::BITMAP) {
                    DCHECK_EQ(index.columns.size(), 1);
                    if (iequal(tcolumn.column_name, index.columns[0])) {
                        column->set_has_bitmap_index(true);
                        break;
                    }
                } else if (index.index_type == TIndexType::type::BLOOMFILTER ||
                           index.index_type == TIndexType::type::NGRAM_BF) {
                    DCHECK_EQ(index.columns.size(), 1);
                    if (iequal(tcolumn.column_name, index.columns[0])) {
                        column->set_is_bf_column(true);
                        break;
                    }
                }
            }
        }
    }

    // copy index meta
    if (tablet_schema.__isset.indexes) {
        for (auto& index : tablet_schema.indexes) {
            TabletIndexPB* index_pb = schema->add_index();
            index_pb->set_index_id(index.index_id);
            index_pb->set_index_name(index.index_name);
            // init col_unique_id in index at be side, since col_unique_id may be -1 at fe side
            // get column unique id by name
            for (auto column_name : index.columns) {
                for (auto column : schema->column()) {
                    if (iequal(column.name(), column_name)) {
                        index_pb->add_col_unique_id(column.unique_id());
                    }
                }
            }
            switch (index.index_type) {
            case TIndexType::BITMAP:
                index_pb->set_index_type(IndexType::BITMAP);
                break;
            case TIndexType::INVERTED:
                index_pb->set_index_type(IndexType::INVERTED);
                break;
            case TIndexType::BLOOMFILTER:
                index_pb->set_index_type(IndexType::BLOOMFILTER);
                break;
            case TIndexType::NGRAM_BF:
                index_pb->set_index_type(IndexType::NGRAM_BF);
                break;
            }

            if (index.__isset.properties) {
                auto properties = index_pb->mutable_properties();
                for (auto kv : index.properties) {
                    (*properties)[kv.first] = kv.second;
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

    if (tablet_schema.__isset.disable_auto_compaction) {
        schema->set_disable_auto_compaction(tablet_schema.disable_auto_compaction);
    }

    if (tablet_schema.__isset.enable_single_replica_compaction) {
        schema->set_enable_single_replica_compaction(
                tablet_schema.enable_single_replica_compaction);
    }

    if (tablet_schema.__isset.delete_sign_idx) {
        schema->set_delete_sign_idx(tablet_schema.delete_sign_idx);
    }
    if (tablet_schema.__isset.store_row_column) {
        schema->set_store_row_column(tablet_schema.store_row_column);
    }
    if (tablet_schema.__isset.skip_write_index_on_load) {
        schema->set_skip_write_index_on_load(tablet_schema.skip_write_index_on_load);
    }
    if (binlog_config.has_value()) {
        BinlogConfig tmp_binlog_config;
        tmp_binlog_config = binlog_config.value();
        tmp_binlog_config.to_pb(tablet_meta_pb.mutable_binlog_config());
    }

    init_from_pb(tablet_meta_pb);
    LOG(INFO) << "init tablet meta from pb: " << tablet_meta_pb.ShortDebugString();
}

TabletMeta::TabletMeta(const TabletMeta& b)
        : _table_id(b._table_id),
          _partition_id(b._partition_id),
          _tablet_id(b._tablet_id),
          _replica_id(b._replica_id),
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
          _in_restore_mode(b._in_restore_mode),
          _preferred_rowset_type(b._preferred_rowset_type),
          _storage_policy_id(b._storage_policy_id),
          _cooldown_meta_id(b._cooldown_meta_id),
          _enable_unique_key_merge_on_write(b._enable_unique_key_merge_on_write),
          _delete_bitmap(b._delete_bitmap),
          _binlog_config(b._binlog_config),
          _compaction_policy(b._compaction_policy),
          _time_series_compaction_goal_size_mbytes(b._time_series_compaction_goal_size_mbytes),
          _time_series_compaction_file_count_threshold(
                  b._time_series_compaction_file_count_threshold),
          _time_series_compaction_time_threshold_seconds(
                  b._time_series_compaction_time_threshold_seconds) {};

void TabletMeta::init_column_from_tcolumn(uint32_t unique_id, const TColumn& tcolumn,
                                          ColumnPB* column) {
    column->set_unique_id(unique_id);
    column->set_name(tcolumn.column_name);
    column->set_has_bitmap_index(tcolumn.has_bitmap_index);
    string data_type;
    EnumToString(TPrimitiveType, tcolumn.column_type.type, data_type);
    column->set_type(data_type);

    uint32_t length = TabletColumn::get_field_length_by_type(tcolumn.column_type.type,
                                                             tcolumn.column_type.len);
    column->set_length(length);
    column->set_index_length(length);
    column->set_precision(tcolumn.column_type.precision);
    column->set_frac(tcolumn.column_type.scale);

    if (tcolumn.__isset.result_is_nullable) {
        column->set_result_is_nullable(tcolumn.result_is_nullable);
    }

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
        if (tcolumn.__isset.aggregation) {
            column->set_aggregation(tcolumn.aggregation);
        } else {
            string aggregation_type;
            EnumToString(TAggregationType, tcolumn.aggregation_type, aggregation_type);
            column->set_aggregation(aggregation_type);
        }
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
    for (size_t i = 0; i < tcolumn.children_column.size(); i++) {
        ColumnPB* children_column = column->add_children_columns();
        init_column_from_tcolumn(i, tcolumn.children_column[i], children_column);
    }
}

Status TabletMeta::create_from_file(const string& file_path) {
    FileHeader<TabletMetaPB> file_header(file_path);
    // In file_header.deserialize(), it validates file length, signature, checksum of protobuf.
    RETURN_IF_ERROR(file_header.deserialize());
    TabletMetaPB tablet_meta_pb;
    try {
        tablet_meta_pb.CopyFrom(file_header.message());
    } catch (...) {
        return Status::Error<PARSE_PROTOBUF_ERROR>("fail to copy protocol buffer object. file={}",
                                                   file_path);
    }

    init_from_pb(tablet_meta_pb);
    return Status::OK();
}

std::string TabletMeta::construct_header_file_path(const string& schema_hash_path,
                                                   int64_t tablet_id) {
    std::stringstream header_name_stream;
    header_name_stream << schema_hash_path << "/" << tablet_id << ".hdr";
    return header_name_stream.str();
}

Status TabletMeta::save_as_json(const string& file_path, DataDir* dir) {
    std::string json_meta;
    json2pb::Pb2JsonOptions json_options;
    json_options.pretty_json = true;
    json_options.bytes_to_base64 = true;
    to_json(&json_meta, json_options);
    // save to file
    io::FileWriterPtr file_writer;
    RETURN_IF_ERROR(dir->fs()->create_file(file_path, &file_writer));
    RETURN_IF_ERROR(file_writer->append(json_meta));
    RETURN_IF_ERROR(file_writer->close());
    return Status::OK();
}

Status TabletMeta::save(const string& file_path) {
    TabletMetaPB tablet_meta_pb;
    to_meta_pb(&tablet_meta_pb);
    return TabletMeta::save(file_path, tablet_meta_pb);
}

Status TabletMeta::save(const string& file_path, const TabletMetaPB& tablet_meta_pb) {
    DCHECK(!file_path.empty());
    FileHeader<TabletMetaPB> file_header(file_path);
    try {
        file_header.mutable_message()->CopyFrom(tablet_meta_pb);
    } catch (...) {
        LOG(WARNING) << "fail to copy protocol buffer object. file='" << file_path;
        return Status::Error<ErrorCode::INTERNAL_ERROR>(
                "fail to copy protocol buffer object. file={}", file_path);
    }
    RETURN_IF_ERROR(file_header.prepare());
    RETURN_IF_ERROR(file_header.serialize());
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
                   << " tablet=" << tablet_id() << " _tablet_uid=" << _tablet_uid.to_string();
    }
    string meta_binary;

    auto t1 = MonotonicMicros();
    RETURN_IF_ERROR(serialize(&meta_binary));
    auto t2 = MonotonicMicros();
    Status status = TabletMetaManager::save(data_dir, tablet_id(), schema_hash(), meta_binary);
    if (!status.ok()) {
        LOG(FATAL) << "fail to save tablet_meta. status=" << status << ", tablet_id=" << tablet_id()
                   << ", schema_hash=" << schema_hash();
    }
    auto t3 = MonotonicMicros();
    auto cost = t3 - t1;
    if (cost > 1 * 1000 * 1000) {
        LOG(INFO) << "save tablet(" << full_name() << ") meta too slow. serialize cost " << t2 - t1
                  << "(us), serialized binary size: " << meta_binary.length()
                  << "(bytes), write rocksdb cost " << t3 - t2 << "(us)";
    }
    return status;
}

Status TabletMeta::serialize(string* meta_binary) {
    TabletMetaPB tablet_meta_pb;
    to_meta_pb(&tablet_meta_pb);
    bool serialize_success = tablet_meta_pb.SerializeToString(meta_binary);
    if (!serialize_success) {
        LOG(FATAL) << "failed to serialize meta " << tablet_id();
    }
    return Status::OK();
}

Status TabletMeta::deserialize(const string& meta_binary) {
    TabletMetaPB tablet_meta_pb;
    bool parsed = tablet_meta_pb.ParseFromString(meta_binary);
    if (!parsed) {
        return Status::Error<INIT_FAILED>("parse tablet meta failed");
    }
    init_from_pb(tablet_meta_pb);
    return Status::OK();
}

void TabletMeta::init_rs_metas_fs(const io::FileSystemSPtr& fs) {
    for (auto& rs_meta : _rs_metas) {
        if (rs_meta->is_local()) {
            rs_meta->set_fs(fs);
        }
    }
    for (auto& rs_meta : _stale_rs_metas) {
        if (rs_meta->is_local()) {
            rs_meta->set_fs(fs);
        }
    }
}

void TabletMeta::init_from_pb(const TabletMetaPB& tablet_meta_pb) {
    _table_id = tablet_meta_pb.table_id();
    _partition_id = tablet_meta_pb.partition_id();
    _tablet_id = tablet_meta_pb.tablet_id();
    _replica_id = tablet_meta_pb.replica_id();
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

    if (tablet_meta_pb.has_enable_unique_key_merge_on_write()) {
        _enable_unique_key_merge_on_write = tablet_meta_pb.enable_unique_key_merge_on_write();
    }

    // init _rs_metas
    for (auto& it : tablet_meta_pb.rs_metas()) {
        RowsetMetaSharedPtr rs_meta(new RowsetMeta());
        rs_meta->init_from_pb(it);
        _rs_metas.push_back(std::move(rs_meta));
    }

    // For mow table, delete bitmap of stale rowsets has not been persisted.
    // When be restart, query should not read the stale rowset, otherwise duplicate keys
    // will be read out. Therefore, we don't add them to _stale_rs_meta for mow table.
    if (!_enable_unique_key_merge_on_write) {
        for (auto& it : tablet_meta_pb.stale_rs_metas()) {
            RowsetMetaSharedPtr rs_meta(new RowsetMeta());
            rs_meta->init_from_pb(it);
            _stale_rs_metas.push_back(std::move(rs_meta));
        }
    }

    if (tablet_meta_pb.has_in_restore_mode()) {
        _in_restore_mode = tablet_meta_pb.in_restore_mode();
    }

    if (tablet_meta_pb.has_preferred_rowset_type()) {
        _preferred_rowset_type = tablet_meta_pb.preferred_rowset_type();
    }

    _storage_policy_id = tablet_meta_pb.storage_policy_id();
    if (tablet_meta_pb.has_cooldown_meta_id()) {
        _cooldown_meta_id = tablet_meta_pb.cooldown_meta_id();
    }

    if (tablet_meta_pb.has_delete_bitmap()) {
        int rst_ids_size = tablet_meta_pb.delete_bitmap().rowset_ids_size();
        int seg_ids_size = tablet_meta_pb.delete_bitmap().segment_ids_size();
        int versions_size = tablet_meta_pb.delete_bitmap().versions_size();
        int seg_maps_size = tablet_meta_pb.delete_bitmap().segment_delete_bitmaps_size();
        CHECK(rst_ids_size == seg_ids_size && seg_ids_size == seg_maps_size &&
              seg_maps_size == versions_size);
        for (size_t i = 0; i < rst_ids_size; ++i) {
            RowsetId rst_id;
            rst_id.init(tablet_meta_pb.delete_bitmap().rowset_ids(i));
            auto seg_id = tablet_meta_pb.delete_bitmap().segment_ids(i);
            uint32_t ver = tablet_meta_pb.delete_bitmap().versions(i);
            auto bitmap = tablet_meta_pb.delete_bitmap().segment_delete_bitmaps(i).data();
            delete_bitmap().delete_bitmap[{rst_id, seg_id, ver}] = roaring::Roaring::read(bitmap);
        }
    }

    if (tablet_meta_pb.has_binlog_config()) {
        _binlog_config = tablet_meta_pb.binlog_config();
    }
    _compaction_policy = tablet_meta_pb.compaction_policy();
    _time_series_compaction_goal_size_mbytes =
            tablet_meta_pb.time_series_compaction_goal_size_mbytes();
    _time_series_compaction_file_count_threshold =
            tablet_meta_pb.time_series_compaction_file_count_threshold();
    _time_series_compaction_time_threshold_seconds =
            tablet_meta_pb.time_series_compaction_time_threshold_seconds();
}

void TabletMeta::to_meta_pb(TabletMetaPB* tablet_meta_pb) {
    tablet_meta_pb->set_table_id(table_id());
    tablet_meta_pb->set_partition_id(partition_id());
    tablet_meta_pb->set_tablet_id(tablet_id());
    tablet_meta_pb->set_replica_id(replica_id());
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
    if (_storage_policy_id > 0) {
        tablet_meta_pb->set_storage_policy_id(_storage_policy_id);
    }
    if (_cooldown_meta_id.initialized()) {
        tablet_meta_pb->mutable_cooldown_meta_id()->CopyFrom(_cooldown_meta_id.to_proto());
    }

    tablet_meta_pb->set_enable_unique_key_merge_on_write(_enable_unique_key_merge_on_write);

    if (_enable_unique_key_merge_on_write) {
        std::set<RowsetId> stale_rs_ids;
        for (const auto& rowset : _stale_rs_metas) {
            stale_rs_ids.insert(rowset->rowset_id());
        }
        DeleteBitmapPB* delete_bitmap_pb = tablet_meta_pb->mutable_delete_bitmap();
        for (auto& [id, bitmap] : delete_bitmap().snapshot().delete_bitmap) {
            auto& [rowset_id, segment_id, ver] = id;
            if (stale_rs_ids.count(rowset_id) != 0) {
                continue;
            }
            delete_bitmap_pb->add_rowset_ids(rowset_id.to_string());
            delete_bitmap_pb->add_segment_ids(segment_id);
            delete_bitmap_pb->add_versions(ver);
            std::string bitmap_data(bitmap.getSizeInBytes(), '\0');
            bitmap.write(bitmap_data.data());
            *(delete_bitmap_pb->add_segment_delete_bitmaps()) = std::move(bitmap_data);
        }
    }
    _binlog_config.to_pb(tablet_meta_pb->mutable_binlog_config());
    tablet_meta_pb->set_compaction_policy(compaction_policy());
    tablet_meta_pb->set_time_series_compaction_goal_size_mbytes(
            time_series_compaction_goal_size_mbytes());
    tablet_meta_pb->set_time_series_compaction_file_count_threshold(
            time_series_compaction_file_count_threshold());
    tablet_meta_pb->set_time_series_compaction_time_threshold_seconds(
            time_series_compaction_time_threshold_seconds());
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
                return Status::Error<PUSH_VERSION_ALREADY_EXIST>(
                        "version already exist. rowset_id={}, version={}, tablet={}",
                        rs->rowset_id().to_string(), rs->version().to_string(), tablet_id());
            } else {
                // rowsetid,version is equal, it is a duplicate req, skip it
                return Status::OK();
            }
        }
    }
    _rs_metas.push_back(rs_meta);
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
                _rs_metas.erase(it);
                // there should be only one rowset match the version
                break;
            } else {
                ++it;
            }
        }
        // delete delete_bitmap of to_delete's rowsets if not added to _stale_rs_metas.
        if (same_version && _enable_unique_key_merge_on_write) {
            delete_bitmap().remove({rs_to_del->rowset_id(), 0, 0},
                                   {rs_to_del->rowset_id(), UINT32_MAX, 0});
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

// This method should call after revise_rs_metas, since new rs_metas might be a subset
// of original tablet, we should revise the delete_bitmap according to current rowset.
//
// Delete bitmap is protected by Tablet::_meta_lock, we don't need to acquire the
// TabletMeta's _meta_lock
void TabletMeta::revise_delete_bitmap_unlocked(const DeleteBitmap& delete_bitmap) {
    _delete_bitmap = std::make_unique<DeleteBitmap>(tablet_id());
    for (auto rs : _rs_metas) {
        DeleteBitmap rs_bm(tablet_id());
        delete_bitmap.subset({rs->rowset_id(), 0, 0}, {rs->rowset_id(), UINT32_MAX, INT64_MAX},
                             &rs_bm);
        _delete_bitmap->merge(rs_bm);
    }
    for (auto rs : _stale_rs_metas) {
        DeleteBitmap rs_bm(tablet_id());
        delete_bitmap.subset({rs->rowset_id(), 0, 0}, {rs->rowset_id(), UINT32_MAX, INT64_MAX},
                             &rs_bm);
        _delete_bitmap->merge(rs_bm);
    }
}

void TabletMeta::delete_stale_rs_meta_by_version(const Version& version) {
    auto it = _stale_rs_metas.begin();
    while (it != _stale_rs_metas.end()) {
        if ((*it)->version() == version) {
            if (_enable_unique_key_merge_on_write) {
                // remove rowset delete bitmap
                delete_bitmap().remove({(*it)->rowset_id(), 0, 0},
                                       {(*it)->rowset_id(), UINT32_MAX, 0});
            }
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
    if (a._replica_id != b._replica_id) return false;
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
    if (a._storage_policy_id != b._storage_policy_id) return false;
    if (a._compaction_policy != b._compaction_policy) return false;
    if (a._time_series_compaction_goal_size_mbytes != b._time_series_compaction_goal_size_mbytes)
        return false;
    if (a._time_series_compaction_file_count_threshold !=
        b._time_series_compaction_file_count_threshold)
        return false;
    if (a._time_series_compaction_time_threshold_seconds !=
        b._time_series_compaction_time_threshold_seconds)
        return false;
    return true;
}

bool operator!=(const TabletMeta& a, const TabletMeta& b) {
    return !(a == b);
}

DeleteBitmap::DeleteBitmap(int64_t tablet_id) : _tablet_id(tablet_id) {
    _agg_cache.reset(new AggCache(config::delete_bitmap_agg_cache_capacity));
}

DeleteBitmap::DeleteBitmap(const DeleteBitmap& o) {
    delete_bitmap = o.delete_bitmap; // just copy data
    _tablet_id = o._tablet_id;
}

DeleteBitmap& DeleteBitmap::operator=(const DeleteBitmap& o) {
    delete_bitmap = o.delete_bitmap; // just copy data
    _tablet_id = o._tablet_id;
    return *this;
}

DeleteBitmap::DeleteBitmap(DeleteBitmap&& o) {
    delete_bitmap = std::move(o.delete_bitmap);
    _tablet_id = o._tablet_id;
}

DeleteBitmap& DeleteBitmap::operator=(DeleteBitmap&& o) {
    delete_bitmap = std::move(o.delete_bitmap);
    _tablet_id = o._tablet_id;
    return *this;
}

DeleteBitmap DeleteBitmap::snapshot() const {
    std::shared_lock l(lock);
    return DeleteBitmap(*this);
}

DeleteBitmap DeleteBitmap::snapshot(Version version) const {
    // Take snapshot first, then remove keys greater than given version.
    DeleteBitmap snapshot = this->snapshot();
    auto it = snapshot.delete_bitmap.begin();
    while (it != snapshot.delete_bitmap.end()) {
        if (std::get<2>(it->first) > version) {
            it = snapshot.delete_bitmap.erase(it);
        } else {
            it++;
        }
    }
    return snapshot;
}

void DeleteBitmap::add(const BitmapKey& bmk, uint32_t row_id) {
    std::lock_guard l(lock);
    delete_bitmap[bmk].add(row_id);
}

int DeleteBitmap::remove(const BitmapKey& bmk, uint32_t row_id) {
    std::lock_guard l(lock);
    auto it = delete_bitmap.find(bmk);
    if (it == delete_bitmap.end()) return -1;
    it->second.remove(row_id);
    return 0;
}

void DeleteBitmap::remove(const BitmapKey& start, const BitmapKey& end) {
    std::lock_guard l(lock);
    for (auto it = delete_bitmap.lower_bound(start); it != delete_bitmap.end();) {
        auto& [k, _] = *it;
        if (k >= end) {
            break;
        }
        it = delete_bitmap.erase(it);
    }
}

bool DeleteBitmap::contains(const BitmapKey& bmk, uint32_t row_id) const {
    std::shared_lock l(lock);
    auto it = delete_bitmap.find(bmk);
    return it != delete_bitmap.end() && it->second.contains(row_id);
}

bool DeleteBitmap::contains_agg(const BitmapKey& bmk, uint32_t row_id) const {
    return get_agg(bmk)->contains(row_id);
}

bool DeleteBitmap::empty() const {
    std::shared_lock l(lock);
    return delete_bitmap.empty();
}

bool DeleteBitmap::contains_agg_without_cache(const BitmapKey& bmk, uint32_t row_id) const {
    std::shared_lock l(lock);
    DeleteBitmap::BitmapKey start {std::get<0>(bmk), std::get<1>(bmk), 0};
    for (auto it = delete_bitmap.lower_bound(start); it != delete_bitmap.end(); ++it) {
        auto& [k, bm] = *it;
        if (std::get<0>(k) != std::get<0>(bmk) || std::get<1>(k) != std::get<1>(bmk) ||
            std::get<2>(k) > std::get<2>(bmk)) {
            break;
        }
        if (bm.contains(row_id)) {
            return true;
        }
    }
    return false;
}

int DeleteBitmap::set(const BitmapKey& bmk, const roaring::Roaring& segment_delete_bitmap) {
    std::lock_guard l(lock);
    auto [_, inserted] = delete_bitmap.insert_or_assign(bmk, segment_delete_bitmap);
    return inserted;
}

int DeleteBitmap::get(const BitmapKey& bmk, roaring::Roaring* segment_delete_bitmap) const {
    std::shared_lock l(lock);
    auto it = delete_bitmap.find(bmk);
    if (it == delete_bitmap.end()) return -1;
    *segment_delete_bitmap = it->second; // copy
    return 0;
}

const roaring::Roaring* DeleteBitmap::get(const BitmapKey& bmk) const {
    std::shared_lock l(lock);
    auto it = delete_bitmap.find(bmk);
    if (it == delete_bitmap.end()) return nullptr;
    return &(it->second); // get address
}

void DeleteBitmap::subset(const BitmapKey& start, const BitmapKey& end,
                          DeleteBitmap* subset_rowset_map) const {
    roaring::Roaring roaring;
    DCHECK(start < end);
    std::shared_lock l(lock);
    for (auto it = delete_bitmap.lower_bound(start); it != delete_bitmap.end(); ++it) {
        auto& [k, bm] = *it;
        if (k >= end) {
            break;
        }
        subset_rowset_map->set(k, bm);
    }
}

void DeleteBitmap::merge(const BitmapKey& bmk, const roaring::Roaring& segment_delete_bitmap) {
    std::lock_guard l(lock);
    auto [iter, succ] = delete_bitmap.emplace(bmk, segment_delete_bitmap);
    if (!succ) {
        iter->second |= segment_delete_bitmap;
    }
}

void DeleteBitmap::merge(const DeleteBitmap& other) {
    std::lock_guard l(lock);
    for (auto& i : other.delete_bitmap) {
        auto [j, succ] = this->delete_bitmap.insert(i);
        if (!succ) j->second |= i.second;
    }
}

// We cannot just copy the underlying memory to construct a string
// due to equivalent objects may have different padding bytes.
// Reading padding bytes is undefined behavior, neither copy nor
// placement new will help simplify the code.
// Refer to C11 standards §6.2.6.1/6 and §6.7.9/21 for more info.
static std::string agg_cache_key(int64_t tablet_id, const DeleteBitmap::BitmapKey& bmk) {
    std::string ret(sizeof(tablet_id) + sizeof(bmk), '\0');
    *reinterpret_cast<int64_t*>(ret.data()) = tablet_id;
    auto t = reinterpret_cast<DeleteBitmap::BitmapKey*>(ret.data() + sizeof(tablet_id));
    std::get<RowsetId>(*t).version = std::get<RowsetId>(bmk).version;
    std::get<RowsetId>(*t).hi = std::get<RowsetId>(bmk).hi;
    std::get<RowsetId>(*t).mi = std::get<RowsetId>(bmk).mi;
    std::get<RowsetId>(*t).lo = std::get<RowsetId>(bmk).lo;
    std::get<1>(*t) = std::get<1>(bmk);
    std::get<2>(*t) = std::get<2>(bmk);
    return ret;
}

std::shared_ptr<roaring::Roaring> DeleteBitmap::get_agg(const BitmapKey& bmk) const {
    std::string key_str = agg_cache_key(_tablet_id, bmk); // Cache key container
    CacheKey key(key_str);
    Cache::Handle* handle = _agg_cache->repr()->lookup(key);

    AggCache::Value* val =
            handle == nullptr
                    ? nullptr
                    : reinterpret_cast<AggCache::Value*>(_agg_cache->repr()->value(handle));
    // FIXME: do we need a mutex here to get rid of duplicated initializations
    //        of cache entries in some cases?
    if (val == nullptr) { // Renew if needed, put a new Value to cache
        val = new AggCache::Value();
        {
            std::shared_lock l(lock);
            DeleteBitmap::BitmapKey start {std::get<0>(bmk), std::get<1>(bmk), 0};
            for (auto it = delete_bitmap.lower_bound(start); it != delete_bitmap.end(); ++it) {
                auto& [k, bm] = *it;
                if (std::get<0>(k) != std::get<0>(bmk) || std::get<1>(k) != std::get<1>(bmk) ||
                    std::get<2>(k) > std::get<2>(bmk)) {
                    break;
                }
                val->bitmap |= bm;
            }
        }
        static auto deleter = [](const CacheKey& key, void* value) {
            delete (AggCache::Value*)value; // Just delete to reclaim
        };
        size_t charge = val->bitmap.getSizeInBytes() + sizeof(AggCache::Value);
        handle = _agg_cache->repr()->insert(key, val, charge, deleter, CachePriority::NORMAL);
    }

    // It is natural for the cache to reclaim the underlying memory
    return std::shared_ptr<roaring::Roaring>(
            &val->bitmap, [this, handle](...) { _agg_cache->repr()->release(handle); });
}

std::atomic<ShardedLRUCache*> DeleteBitmap::AggCache::s_repr {nullptr};

} // namespace doris
