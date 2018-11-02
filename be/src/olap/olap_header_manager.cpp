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

#include "olap/olap_header_manager.h"

#include <vector>
#include <sstream>
#include <string>
#include <fstream>
#include <boost/algorithm/string/trim.hpp>

#include "olap/olap_define.h"
#include "olap/olap_engine.h"
#include "olap/olap_meta.h"
#include "common/logging.h"
#include "json2pb/json_to_pb.h"
#include "json2pb/pb_to_json.h"

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

const std::string HEADER_PREFIX = "hdr_";

OLAPStatus OlapHeaderManager::get_header(OlapStore* store,
        TTabletId tablet_id, TSchemaHash schema_hash, OLAPHeader* header) {
    OlapMeta* meta = store->get_meta();
    std::stringstream key_stream;
    key_stream << HEADER_PREFIX << tablet_id << "_" << schema_hash;
    std::string key = key_stream.str();
    std::string value;
    OLAPStatus s = meta->get(META_COLUMN_FAMILY_INDEX, key, value);
    if (s == OLAP_ERR_META_KEY_NOT_FOUND) {
        LOG(WARNING) << "tablet_id:" << tablet_id << ", schema_hash:" << schema_hash << " not found.";
        return OLAP_ERR_META_KEY_NOT_FOUND;
    } else if (s != OLAP_SUCCESS) {
        LOG(WARNING) << "load tablet_id:" << tablet_id << ", schema_hash:" << schema_hash << " failed.";
        return s;
    }
    header->ParseFromString(value);
    return header->init();
}

OLAPStatus OlapHeaderManager::get_json_header(OlapStore* store,
        TTabletId tablet_id, TSchemaHash schema_hash, std::string* json_header) {
    OLAPHeader header;
    OLAPStatus s = get_header(store, tablet_id, schema_hash, &header);
    if (s != OLAP_SUCCESS) {
        return s;
    }
    json2pb::Pb2JsonOptions json_options;
    json_options.pretty_json = true;
    json2pb::ProtoMessageToJson(header, json_header, json_options);
    return OLAP_SUCCESS;
}


OLAPStatus OlapHeaderManager::save(OlapStore* store,
        TTabletId tablet_id, TSchemaHash schema_hash, const OLAPHeader* header) {
    std::stringstream key_stream;
    key_stream << HEADER_PREFIX << tablet_id << "_" << schema_hash;
    std::string key = key_stream.str();
    std::string value;
    header->SerializeToString(&value);
    OlapMeta* meta = store->get_meta();
    OLAPStatus s = meta->put(META_COLUMN_FAMILY_INDEX, key, value);
    return s;
}

OLAPStatus OlapHeaderManager::remove(OlapStore* store, TTabletId tablet_id, TSchemaHash schema_hash) {
    std::stringstream key_stream;
    key_stream << HEADER_PREFIX << tablet_id << "_" << schema_hash;
    std::string key = key_stream.str();
    OlapMeta* meta = store->get_meta();
    LOG(INFO) << "start to remove header, key:" << key;
    OLAPStatus res = meta->remove(META_COLUMN_FAMILY_INDEX, key);
    LOG(INFO) << "remove header, key:" << key << ", res:" << res;
    return res;
}

OLAPStatus OlapHeaderManager::get_header_converted(OlapStore* store, bool& flag) {
    // get is_header_converted flag
    std::string value;
    std::string key = IS_HEADER_CONVERTED;
    OlapMeta* meta = store->get_meta();
    OLAPStatus s = meta->get(DEFAULT_COLUMN_FAMILY_INDEX, key, value);
    if (s == OLAP_ERR_META_KEY_NOT_FOUND || value == "false") {
        flag = false;
    } else if (value == "true") {
        flag = true;
    } else {
        LOG(WARNING) << "invalid _is_header_converted. _is_header_converted=" << value;
        return OLAP_ERR_HEADER_INVALID_FLAG;
    }
    return OLAP_SUCCESS;
}

OLAPStatus OlapHeaderManager::set_converted_flag(OlapStore* store) {
    OlapMeta* meta = store->get_meta();
    OLAPStatus s = meta->put(DEFAULT_COLUMN_FAMILY_INDEX, IS_HEADER_CONVERTED, CONVERTED_FLAG);
    return s;
}

OLAPStatus OlapHeaderManager::traverse_headers(OlapMeta* meta,
        std::function<bool(long, long, const std::string&)> const& func) {
    auto traverse_header_func = [&func](const std::string& key, const std::string& value) -> bool {
        std::vector<std::string> parts;
        // key format: "hdr_" + tablet_id + "_" + schema_hash
        split_string<char>(key, '_', &parts);
        if (parts.size() != 3) {
            LOG(WARNING) << "invalid header key:" << key << ", splitted size:" << parts.size();
            return true;
        }
        TTabletId tablet_id = std::stol(parts[1].c_str(), NULL, 10);
        TSchemaHash schema_hash = std::stol(parts[2].c_str(), NULL, 10);
        return func(tablet_id, schema_hash, value);
    };
    OLAPStatus status = meta->iterate(META_COLUMN_FAMILY_INDEX, HEADER_PREFIX, traverse_header_func);
    return status;
}

OLAPStatus OlapHeaderManager::load_json_header(OlapStore* store, const std::string& header_path) {
    std::ifstream infile(header_path);
    char buffer[1024];
    std::string json_header;
    while (!infile.eof()) {
        infile.getline(buffer, 1024);
        json_header = json_header + buffer;
    }
    boost::algorithm::trim(json_header);
    OLAPHeader header;
    bool ret = json2pb::JsonToProtoMessage(json_header, &header);
    if (!ret) {
        return OLAP_ERR_HEADER_LOAD_JSON_HEADER;
    }
    TTabletId tablet_id = header.tablet_id();
    TSchemaHash schema_hash = header.schema_hash();
    OLAPStatus s = save(store, tablet_id, schema_hash, &header);
    return s;
}

OLAPStatus OlapHeaderManager::dump_header(OlapStore* store, TTabletId tablet_id,
        TSchemaHash schema_hash, const std::string& dump_path) {
    OLAPHeader header;
    OLAPStatus res = OlapHeaderManager::get_header(store, tablet_id, schema_hash, &header);
    if (res != OLAP_SUCCESS) {
        return res;
    }
    res = header.save(dump_path);
    return res;
}

}
