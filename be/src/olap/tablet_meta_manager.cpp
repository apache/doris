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

#include "olap/tablet_meta_manager.h"

#include <fmt/format.h>
#include <gen_cpp/olap_file.pb.h>
#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>

#include <boost/algorithm/string/trim.hpp>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "common/logging.h"
#include "common/status.h"
#include "gutil/endian.h"
#include "json2pb/json_to_pb.h"
#include "json2pb/pb_to_json.h"
#include "olap/data_dir.h"
#include "olap/olap_define.h"
#include "olap/olap_meta.h"
#include "olap/utils.h"

namespace rocksdb {
class Iterator;
class Slice;
class Status;
struct ColumnFamilyOptions;
struct DBOptions;
struct ReadOptions;
struct WriteOptions;
} // namespace rocksdb
namespace doris {
using namespace ErrorCode;

// should use tablet->generate_tablet_meta_copy() method to get a copy of current tablet meta
// there are some rowset meta in local meta store and in in-memory tablet meta
// but not in tablet meta in local meta store
Status TabletMetaManager::get_meta(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash,
                                   TabletMetaSharedPtr tablet_meta) {
    OlapMeta* meta = store->get_meta();
    std::stringstream key_stream;
    key_stream << HEADER_PREFIX << tablet_id << "_" << schema_hash;
    std::string key = key_stream.str();
    std::string value;
    Status s = meta->get(META_COLUMN_FAMILY_INDEX, key, &value);
    if (s.is<META_KEY_NOT_FOUND>()) {
        return Status::Error<META_KEY_NOT_FOUND>("tablet_id:{}, schema_hash:{}, not found.",
                                                 tablet_id, schema_hash);
    } else if (!s.ok()) {
        LOG(WARNING) << "load tablet_id:" << tablet_id << ", schema_hash:" << schema_hash
                     << " failed.";
        return s;
    }
    return tablet_meta->deserialize(value);
}

Status TabletMetaManager::get_json_meta(DataDir* store, TTabletId tablet_id,
                                        TSchemaHash schema_hash, std::string* json_meta) {
    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    Status s = get_meta(store, tablet_id, schema_hash, tablet_meta);
    if (!s.ok()) {
        return s;
    }
    json2pb::Pb2JsonOptions json_options;
    json_options.pretty_json = true;
    tablet_meta->to_json(json_meta, json_options);
    return Status::OK();
}

// TODO(ygl):
// 1. if term > 0 then save to remote meta store first using term
// 2. save to local meta store
Status TabletMetaManager::save(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash,
                               TabletMetaSharedPtr tablet_meta, std::string_view header_prefix) {
    std::string key = fmt::format("{}{}_{}", header_prefix, tablet_id, schema_hash);
    std::string value;
    tablet_meta->serialize(&value);
    if (tablet_meta->partition_id() <= 0) {
        LOG(WARNING) << "invalid partition id " << tablet_meta->partition_id() << " tablet "
                     << tablet_meta->tablet_id();
        // TODO(dx): after fix partition id eq 0 bug, fix it
        // return Status::InternalError("invaid partition id {} tablet {}",
        //  tablet_meta->partition_id(), tablet_meta->tablet_id());
    }
    OlapMeta* meta = store->get_meta();
    VLOG_NOTICE << "save tablet meta"
                << ", key:" << key << ", meta length:" << value.length();
    return meta->put(META_COLUMN_FAMILY_INDEX, key, value);
}

Status TabletMetaManager::save(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash,
                               const std::string& meta_binary, std::string_view header_prefix) {
    std::string key = fmt::format("{}{}_{}", header_prefix, tablet_id, schema_hash);
    OlapMeta* meta = store->get_meta();
    VLOG_NOTICE << "save tablet meta "
                << ", key:" << key << " meta_size=" << meta_binary.length();
    return meta->put(META_COLUMN_FAMILY_INDEX, key, meta_binary);
}

// TODO(ygl):
// 1. remove load data first
// 2. remove from load meta store using term if term > 0
Status TabletMetaManager::remove(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash,
                                 std::string_view header_prefix) {
    std::string key = fmt::format("{}{}_{}", header_prefix, tablet_id, schema_hash);
    OlapMeta* meta = store->get_meta();
    Status res = meta->remove(META_COLUMN_FAMILY_INDEX, key);
    VLOG_NOTICE << "remove tablet_meta, key:" << key << ", res:" << res;
    return res;
}

Status TabletMetaManager::traverse_headers(
        OlapMeta* meta, std::function<bool(long, long, std::string_view)> const& func,
        std::string_view header_prefix) {
    auto traverse_header_func = [&func](std::string_view key, std::string_view value) -> bool {
        std::vector<std::string> parts;
        // old format key format: "hdr_" + tablet_id + "_" + schema_hash  0.11
        // new format key format: "tabletmeta_" + tablet_id + "_" + schema_hash  0.10
        static_cast<void>(split_string(key, '_', &parts));
        if (parts.size() != 3) {
            LOG(WARNING) << "invalid tablet_meta key:" << key << ", split size:" << parts.size();
            return true;
        }
        TTabletId tablet_id = std::stol(parts[1], nullptr, 10);
        TSchemaHash schema_hash = std::stol(parts[2], nullptr, 10);
        return func(tablet_id, schema_hash, value);
    };
    Status status = meta->iterate(META_COLUMN_FAMILY_INDEX, header_prefix, traverse_header_func);
    return status;
}

Status TabletMetaManager::load_json_meta(DataDir* store, const std::string& meta_path) {
    std::ifstream infile(meta_path);
    char buffer[102400];
    std::string json_meta;
    while (!infile.eof()) {
        infile.getline(buffer, 102400);
        json_meta = json_meta + buffer;
    }
    boost::algorithm::trim(json_meta);
    TabletMetaPB tablet_meta_pb;
    std::string error;
    bool ret = json2pb::JsonToProtoMessage(json_meta, &tablet_meta_pb, &error);
    if (!ret) {
        return Status::Error<HEADER_LOAD_JSON_HEADER>("JSON to protobuf message failed: {}", error);
    }

    std::string meta_binary;
    tablet_meta_pb.SerializeToString(&meta_binary);
    TTabletId tablet_id = tablet_meta_pb.tablet_id();
    TSchemaHash schema_hash = tablet_meta_pb.schema_hash();
    return save(store, tablet_id, schema_hash, meta_binary);
}

Status TabletMetaManager::save_pending_publish_info(DataDir* store, TTabletId tablet_id,
                                                    int64_t publish_version,
                                                    const std::string& meta_binary) {
    std::string key = fmt::format("{}{}_{}", PENDING_PUBLISH_INFO, tablet_id, publish_version);
    OlapMeta* meta = store->get_meta();
    LOG(INFO) << "save pending publish rowset, key:" << key
              << " meta_size=" << meta_binary.length();
    return meta->put(META_COLUMN_FAMILY_INDEX, key, meta_binary);
}

Status TabletMetaManager::remove_pending_publish_info(DataDir* store, TTabletId tablet_id,
                                                      int64_t publish_version) {
    std::string key = fmt::format("{}{}_{}", PENDING_PUBLISH_INFO, tablet_id, publish_version);
    OlapMeta* meta = store->get_meta();
    Status res = meta->remove(META_COLUMN_FAMILY_INDEX, key);
    LOG(INFO) << "remove pending publish_info, key:" << key << ", res:" << res;
    return res;
}

Status TabletMetaManager::traverse_pending_publish(
        OlapMeta* meta, std::function<bool(int64_t, int64_t, std::string_view)> const& func) {
    auto traverse_header_func = [&func](std::string_view key, std::string_view value) -> bool {
        std::vector<std::string> parts;
        // key format: "ppi_" + tablet_id + "_" + publish_version
        static_cast<void>(split_string(key, '_', &parts));
        if (parts.size() != 3) {
            LOG(WARNING) << "invalid pending publish info key:" << key
                         << ", split size:" << parts.size();
            return true;
        }
        int64_t tablet_id = std::stol(parts[1], nullptr, 10);
        int64_t version = std::stol(parts[2], nullptr, 10);
        return func(tablet_id, version, value);
    };
    Status status =
            meta->iterate(META_COLUMN_FAMILY_INDEX, PENDING_PUBLISH_INFO, traverse_header_func);
    return status;
}

std::string TabletMetaManager::encode_delete_bitmap_key(TTabletId tablet_id, int64_t version) {
    std::string key;
    key.reserve(20);
    key.append(DELETE_BITMAP);
    put_fixed64_le(&key, BigEndian::FromHost64(tablet_id));
    put_fixed64_le(&key, BigEndian::FromHost64(version));
    return key;
}

std::string TabletMetaManager::encode_delete_bitmap_key(TTabletId tablet_id) {
    std::string key;
    key.reserve(12);
    key.append(DELETE_BITMAP);
    put_fixed64_le(&key, BigEndian::FromHost64(tablet_id));
    return key;
}

void TabletMetaManager::decode_delete_bitmap_key(std::string_view enc_key, TTabletId* tablet_id,
                                                 int64_t* version) {
    DCHECK_EQ(enc_key.size(), 20);
    *tablet_id = BigEndian::ToHost64(UNALIGNED_LOAD64(enc_key.data() + 4));
    *version = BigEndian::ToHost64(UNALIGNED_LOAD64(enc_key.data() + 12));
}

Status TabletMetaManager::save_delete_bitmap(DataDir* store, TTabletId tablet_id,
                                             DeleteBitmapPtr delete_bitmap, int64_t version) {
    VLOG_NOTICE << "save delete bitmap, tablet_id:" << tablet_id << ", version: " << version;
    if (delete_bitmap->delete_bitmap.empty()) {
        return Status::OK();
    }
    OlapMeta* meta = store->get_meta();
    DeleteBitmapPB delete_bitmap_pb;
    for (auto& [id, bitmap] : delete_bitmap->delete_bitmap) {
        auto& rowset_id = std::get<0>(id);
        int64_t segment_id = std::get<1>(id);
        delete_bitmap_pb.add_rowset_ids(rowset_id.to_string());
        delete_bitmap_pb.add_segment_ids(segment_id);
        std::string bitmap_data(bitmap.getSizeInBytes(), '\0');
        bitmap.write(bitmap_data.data());
        *(delete_bitmap_pb.add_segment_delete_bitmaps()) = std::move(bitmap_data);
    }
    std::string key = encode_delete_bitmap_key(tablet_id, version);
    std::string val;
    bool ok = delete_bitmap_pb.SerializeToString(&val);
    if (!ok) {
        auto msg = fmt::format("failed to serialize delete bitmap, tablet_id: {}, version: {}",
                               tablet_id, version);
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }
    return meta->put(META_COLUMN_FAMILY_INDEX, key, val);
}

Status TabletMetaManager::traverse_delete_bitmap(
        OlapMeta* meta, std::function<bool(int64_t, int64_t, std::string_view)> const& func) {
    auto traverse_header_func = [&func](std::string_view key, std::string_view value) -> bool {
        TTabletId tablet_id;
        int64_t version;
        decode_delete_bitmap_key(key, &tablet_id, &version);
        VLOG_NOTICE << "traverse delete bitmap, tablet_id: " << tablet_id
                    << ", version: " << version;
        return func(tablet_id, version, value);
    };
    return meta->iterate(META_COLUMN_FAMILY_INDEX, DELETE_BITMAP, traverse_header_func);
}

Status TabletMetaManager::remove_old_version_delete_bitmap(DataDir* store, TTabletId tablet_id,
                                                           int64_t version) {
    OlapMeta* meta = store->get_meta();
    std::string begin_key = encode_delete_bitmap_key(tablet_id);
    std::string end_key = encode_delete_bitmap_key(tablet_id, version);

    std::vector<std::string> remove_keys;
    auto get_remove_keys_func = [&](std::string_view key, std::string_view val) -> bool {
        // include end_key
        if (key > end_key) {
            return false;
        }
        remove_keys.emplace_back(key);
        return true;
    };
    LOG(INFO) << "remove old version delete bitmap, tablet_id: " << tablet_id
              << " version: " << version << " removed keys size: " << remove_keys.size();
    ;
    RETURN_IF_ERROR(meta->iterate(META_COLUMN_FAMILY_INDEX, begin_key, get_remove_keys_func));
    return meta->remove(META_COLUMN_FAMILY_INDEX, remove_keys);
}

} // namespace doris
