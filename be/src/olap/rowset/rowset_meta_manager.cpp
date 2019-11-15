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

#include "olap/rowset/rowset_meta_manager.h"

#include <vector>
#include <sstream>
#include <string>
#include <fstream>
#include <boost/algorithm/string/trim.hpp>

#include "olap/olap_define.h"
#include "olap/storage_engine.h"
#include "olap/utils.h"
#include "common/logging.h"
#include "json2pb/json_to_pb.h"
#include "json2pb/pb_to_json.h"

namespace doris {

const std::string ROWSET_PREFIX = "rst_";

bool RowsetMetaManager::check_rowset_meta(OlapMeta* meta, TabletUid tablet_uid, const RowsetId& rowset_id) {
    std::string key = ROWSET_PREFIX + tablet_uid.to_string() + "_" + rowset_id.to_string();
    std::string value;
    OLAPStatus s = meta->get(META_COLUMN_FAMILY_INDEX, key, &value);
    if (s != OLAP_SUCCESS) {
        return false; 
    }
    return true;
}

OLAPStatus RowsetMetaManager::get_rowset_meta(OlapMeta* meta, TabletUid tablet_uid, const RowsetId& rowset_id, RowsetMetaSharedPtr rowset_meta) {
    std::string key = ROWSET_PREFIX + tablet_uid.to_string() + "_" + rowset_id.to_string();
    std::string value;
    OLAPStatus s = meta->get(META_COLUMN_FAMILY_INDEX, key, &value);
    if (s == OLAP_ERR_META_KEY_NOT_FOUND) {
        std::string error_msg = "rowset id:" + key + " not found.";
        LOG(WARNING) << error_msg;
        return OLAP_ERR_META_KEY_NOT_FOUND; 
    } else if (s != OLAP_SUCCESS) {
        std::string error_msg = "load rowset id:" + key + " failed.";
        LOG(WARNING) << error_msg;
        return OLAP_ERR_IO_ERROR; 
    }
    bool ret = rowset_meta->init(value);
    if (!ret) {
        std::string error_msg = "parse rowset meta failed. rowset id:" + key;
        return OLAP_ERR_SERIALIZE_PROTOBUF_ERROR; 
    }
    return OLAP_SUCCESS;
}

OLAPStatus RowsetMetaManager::get_json_rowset_meta(OlapMeta* meta, TabletUid tablet_uid, const RowsetId& rowset_id, std::string* json_rowset_meta) {
    RowsetMetaSharedPtr rowset_meta_ptr(new(std::nothrow) RowsetMeta());
    OLAPStatus status = get_rowset_meta(meta, tablet_uid, rowset_id, rowset_meta_ptr);
    if (status != OLAP_SUCCESS) {
        return status;
    }
    bool ret = rowset_meta_ptr->json_rowset_meta(json_rowset_meta);
    if (!ret) {
        std::string error_msg = "get json rowset meta failed. rowset id:" + rowset_id.to_string();
        return OLAP_ERR_SERIALIZE_PROTOBUF_ERROR; 
    }
    return OLAP_SUCCESS;
}

OLAPStatus RowsetMetaManager::save(OlapMeta* meta, TabletUid tablet_uid, const RowsetId& rowset_id, const RowsetMetaPB& rowset_meta_pb) {
    std::string key = ROWSET_PREFIX + tablet_uid.to_string() + "_" + rowset_id.to_string();
    std::string value;
    bool ret = rowset_meta_pb.SerializeToString(&value);;
    if (!ret) {
        std::string error_msg = "serialize rowset pb failed. rowset id:" + key;
        LOG(WARNING) << error_msg;
        return OLAP_ERR_SERIALIZE_PROTOBUF_ERROR;
    }
    OLAPStatus status = meta->put(META_COLUMN_FAMILY_INDEX, key, value);
    return status;
}

OLAPStatus RowsetMetaManager::remove(OlapMeta* meta, TabletUid tablet_uid, const RowsetId& rowset_id) {
    std::string key = ROWSET_PREFIX + tablet_uid.to_string() + "_" + rowset_id.to_string();
    VLOG(3) << "start to remove rowset, key:" << key;
    OLAPStatus status = meta->remove(META_COLUMN_FAMILY_INDEX, key);
    LOG(INFO) << "remove rowset key:" << key << " finished";
    return status;
}

OLAPStatus RowsetMetaManager::traverse_rowset_metas(OlapMeta* meta,
            std::function<bool(const TabletUid&, const RowsetId&, const std::string&)> const& func) {
    auto traverse_rowset_meta_func = [&func](const std::string& key, const std::string& value) -> bool {
        std::vector<std::string> parts;
        // key format: rst_uuid_rowset_id
        split_string<char>(key, '_', &parts);
        if (parts.size() != 3) {
            LOG(WARNING) << "invalid rowset key:" << key << ", splitted size:" << parts.size();
            return true;
        }
        RowsetId rowset_id;
        rowset_id.init(parts[2]);
        std::vector<std::string> uid_parts;
        split_string<char>(parts[1], '-', &uid_parts);
        TabletUid tablet_uid(uid_parts[0], uid_parts[1]);
        return func(tablet_uid, rowset_id, value);
    };
    OLAPStatus status = meta->iterate(META_COLUMN_FAMILY_INDEX, ROWSET_PREFIX, traverse_rowset_meta_func);
    return status;
}

OLAPStatus RowsetMetaManager::load_json_rowset_meta(OlapMeta* meta, const std::string& rowset_meta_path) {
    std::ifstream infile(rowset_meta_path);
    char buffer[1024];
    std::string json_rowset_meta;
    while (!infile.eof()) {
        infile.getline(buffer, 1024);
        json_rowset_meta = json_rowset_meta + buffer;
    }
    boost::algorithm::trim(json_rowset_meta);
    RowsetMeta rowset_meta;
    bool ret = rowset_meta.init_from_json(json_rowset_meta);
    if (!ret) {
        std::string error_msg = "parse json rowset meta failed.";
        LOG(WARNING) << error_msg;
        return OLAP_ERR_SERIALIZE_PROTOBUF_ERROR;
    }
    RowsetId rowset_id = rowset_meta.rowset_id();
    TabletUid tablet_uid = rowset_meta.tablet_uid();
    RowsetMetaPB rowset_meta_pb;
    rowset_meta.to_rowset_pb(&rowset_meta_pb);
    OLAPStatus status = save(meta, tablet_uid, rowset_id, rowset_meta_pb);
    return status;
}

} // namespace doris
