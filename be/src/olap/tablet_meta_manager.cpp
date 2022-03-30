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

#include <boost/algorithm/string/trim.hpp>
#include <fmt/format.h>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "common/logging.h"
#include "json2pb/json_to_pb.h"
#include "json2pb/pb_to_json.h"
#include "olap/olap_define.h"
#include "olap/olap_meta.h"
#include "olap/storage_engine.h"

using rocksdb::DB;
using rocksdb::DBOptions;
using rocksdb::ColumnFamilyDescriptor;
using rocksdb::ColumnFamilyHandle;
using rocksdb::ColumnFamilyOptions;
using rocksdb::ReadOptions;
using rocksdb::WriteOptions;
using rocksdb::Slice;
using rocksdb::Iterator;
using rocksdb::Status;
using rocksdb::kDefaultColumnFamilyName;

namespace doris {

// should use tablet->generate_tablet_meta_copy() method to get a copy of current tablet meta
// there are some rowset meta in local meta store and in in-memory tablet meta
// but not in tablet meta in local meta store
OLAPStatus TabletMetaManager::get_meta(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash,
                                       TabletMetaSharedPtr tablet_meta) {
    OlapMeta* meta = store->get_meta();
    std::stringstream key_stream;
    key_stream << HEADER_PREFIX << tablet_id << "_" << schema_hash;
    std::string key = key_stream.str();
    std::string value;
    OLAPStatus s = meta->get(META_COLUMN_FAMILY_INDEX, key, &value);
    if (s == OLAP_ERR_META_KEY_NOT_FOUND) {
        LOG(WARNING) << "tablet_id:" << tablet_id << ", schema_hash:" << schema_hash
                     << " not found.";
        return OLAP_ERR_META_KEY_NOT_FOUND;
    } else if (s != OLAP_SUCCESS) {
        LOG(WARNING) << "load tablet_id:" << tablet_id << ", schema_hash:" << schema_hash
                     << " failed.";
        return s;
    }
    return tablet_meta->deserialize(value);
}

OLAPStatus TabletMetaManager::get_json_meta(DataDir* store, TTabletId tablet_id,
                                            TSchemaHash schema_hash, std::string* json_meta) {
    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    OLAPStatus s = get_meta(store, tablet_id, schema_hash, tablet_meta);
    if (s != OLAP_SUCCESS) {
        return s;
    }
    json2pb::Pb2JsonOptions json_options;
    json_options.pretty_json = true;
    tablet_meta->to_json(json_meta, json_options);
    return OLAP_SUCCESS;
}

// TODO(ygl):
// 1. if term > 0 then save to remote meta store first using term
// 2. save to local meta store
OLAPStatus TabletMetaManager::save(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash,
                                   TabletMetaSharedPtr tablet_meta, const string& header_prefix) {
    std::string key = fmt::format("{}{}_{}", header_prefix, tablet_id, schema_hash);
    std::string value;
    tablet_meta->serialize(&value);
    OlapMeta* meta = store->get_meta();
    VLOG_NOTICE << "save tablet meta" << ", key:" << key << ", meta length:" << value.length();
    return meta->put(META_COLUMN_FAMILY_INDEX, key, value);
}

OLAPStatus TabletMetaManager::save(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash,
                                   const std::string& meta_binary, const string& header_prefix) {
    std::string key = fmt::format("{}{}_{}", header_prefix, tablet_id, schema_hash);
    OlapMeta* meta = store->get_meta();
    VLOG_NOTICE << "save tablet meta " << ", key:" << key << " meta_size=" << meta_binary.length();
    return meta->put(META_COLUMN_FAMILY_INDEX, key, meta_binary);
}

// TODO(ygl):
// 1. remove load data first
// 2. remove from load meta store using term if term > 0
OLAPStatus TabletMetaManager::remove(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash,
                                     const string& header_prefix) {
    std::string key = fmt::format("{}{}_{}", header_prefix, tablet_id, schema_hash);
    OlapMeta* meta = store->get_meta();
    OLAPStatus res = meta->remove(META_COLUMN_FAMILY_INDEX, key);
    VLOG_NOTICE << "remove tablet_meta, key:" << key << ", res:" << res;
    return res;
}

OLAPStatus TabletMetaManager::traverse_headers(
        OlapMeta* meta, std::function<bool(long, long, const std::string&)> const& func,
        const string& header_prefix) {
    auto traverse_header_func = [&func](const std::string& key, const std::string& value) -> bool {
        std::vector<std::string> parts;
        // old format key format: "hdr_" + tablet_id + "_" + schema_hash  0.11
        // new format key format: "tabletmeta_" + tablet_id + "_" + schema_hash  0.10
        split_string<char>(key, '_', &parts);
        if (parts.size() != 3) {
            LOG(WARNING) << "invalid tablet_meta key:" << key << ", split size:" << parts.size();
            return true;
        }
        TTabletId tablet_id = std::stol(parts[1].c_str(), nullptr, 10);
        TSchemaHash schema_hash = std::stol(parts[2].c_str(), nullptr, 10);
        return func(tablet_id, schema_hash, value);
    };
    OLAPStatus status =
            meta->iterate(META_COLUMN_FAMILY_INDEX, header_prefix, traverse_header_func);
    return status;
}

OLAPStatus TabletMetaManager::load_json_meta(DataDir* store, const std::string& meta_path) {
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
        LOG(ERROR) << "JSON to protobuf message failed: " << error;
        return OLAP_ERR_HEADER_LOAD_JSON_HEADER;
    }

    std::string meta_binary;
    tablet_meta_pb.SerializeToString(&meta_binary);
    TTabletId tablet_id = tablet_meta_pb.tablet_id();
    TSchemaHash schema_hash = tablet_meta_pb.schema_hash();
    return save(store, tablet_id, schema_hash, meta_binary);
}

} // namespace doris
