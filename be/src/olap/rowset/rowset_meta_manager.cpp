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
#include "olap/utils.h"
#include "common/logging.h"
#include "json2pb/json_to_pb.h"
#include "json2pb/pb_to_json.h"

namespace doris {

const std::string ROWSET_PREFIX = "rst_";

NewStatus convert_meta_status(OLAPStatus status) {
    if (status == OLAP_SUCCESS) {
        return NewStatus::OK();
    } else {
        std::string error_msg = "meta operation failed";
        LOG(WARNING) << error_msg;
        return NewStatus::IOError(error_msg);
    }
}

NewStatus RowsetMetaManager::get_rowset_meta(OlapMeta* meta, int64_t rowset_id, RowsetMeta* rowset_meta) {
    std::string key = ROWSET_PREFIX + std::to_string(rowset_id);
    std::string value;
    OLAPStatus s = meta->get(META_COLUMN_FAMILY_INDEX, key, value);
    if (s == OLAP_ERR_META_KEY_NOT_FOUND) {
        std::string error_msg = "rowset id:" + std::to_string(rowset_id) + " not found.";
        LOG(WARNING) << error_msg;
        return NewStatus::NotFound(error_msg);
    } else if (s != OLAP_SUCCESS) {
        std::string error_msg = "load rowset id:" + std::to_string(rowset_id) + " failed.";
        LOG(WARNING) << error_msg;
        return NewStatus::IOError(error_msg);
    }
    bool ret = rowset_meta->init(value);
    if (!ret) {
        std::string error_msg = "parse rowset meta failed. rowset id:" + std::to_string(rowset_id);
        return NewStatus::Corruption(error_msg);
    }
    return NewStatus::OK();
}

NewStatus RowsetMetaManager::get_json_rowset_meta(OlapMeta* meta, int64_t rowset_id, std::string* json_rowset_meta) {
    RowsetMeta rowset_meta;
    NewStatus s = get_rowset_meta(meta, rowset_id, &rowset_meta);
    if (!s.ok()) {
        return s;
    }
    bool ret = rowset_meta.get_json_rowset_meta(json_rowset_meta);
    if (!ret) {
        std::string error_msg = "get json rowset meta failed. rowset id:" + std::to_string(rowset_id);
        return NewStatus::Corruption(error_msg);
    }
    return NewStatus::OK();
}

NewStatus RowsetMetaManager::save(OlapMeta* meta, int64_t rowset_id, RowsetMeta* rowset_meta) {
    std::string key = ROWSET_PREFIX + std::to_string(rowset_id);
    std::string value;
    bool ret = rowset_meta->serialize(&value);
    if (!ret) {
        std::string error_msg = "serialize rowset pb failed. rowset id:" + std::to_string(rowset_id);
        LOG(WARNING) << error_msg;
        return NewStatus::Corruption(error_msg);
    }
    OLAPStatus status = meta->put(META_COLUMN_FAMILY_INDEX, key, value);
    return convert_meta_status(status);
}

NewStatus RowsetMetaManager::remove(OlapMeta* meta, int64_t rowset_id) {
    std::string key = ROWSET_PREFIX + std::to_string(rowset_id);
    LOG(INFO) << "start to remove rowset, key:" << key;
    OLAPStatus status = meta->remove(META_COLUMN_FAMILY_INDEX, key);
    LOG(INFO) << "remove rowset key:" << key << " finished";
    return convert_meta_status(status);
}

NewStatus RowsetMetaManager::traverse_rowset_metas(OlapMeta* meta,
            std::function<bool(uint64_t, const std::string&)> const& func) {
    auto traverse_rowset_meta_func = [&func](const std::string& key, const std::string& value) -> bool {
        std::vector<std::string> parts;
        // key format: "rst_" + rowset_id
        split_string<char>(key, '_', &parts);
        if (parts.size() != 2) {
            LOG(WARNING) << "invalid rowset key:" << key << ", splitted size:" << parts.size();
            return true;
        }
        uint64_t rowset_id = std::stol(parts[1].c_str(), NULL, 10);
        return func(rowset_id, value);
    };
    OLAPStatus status = meta->iterate(META_COLUMN_FAMILY_INDEX, ROWSET_PREFIX, traverse_rowset_meta_func);
    return convert_meta_status(status);
}

NewStatus RowsetMetaManager::load_json_rowset_meta(OlapMeta* meta, const std::string& rowset_meta_path) {
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
        return NewStatus::Corruption(error_msg);
    }
    uint64_t rowset_id = rowset_meta.get_rowset_id();
    NewStatus status = save(meta, rowset_id, &rowset_meta);
    return status;
}

} // namespace doris