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

#include <fmt/format.h>
#include <gen_cpp/olap_file.pb.h>

#include <boost/algorithm/string/trim.hpp>
#include <fstream>
#include <functional>
#include <memory>
#include <new>
#include <string>
#include <string_view>
#include <vector>

#include "common/logging.h"
#include "olap/binlog.h"
#include "olap/olap_define.h"
#include "olap/olap_meta.h"
#include "olap/utils.h"

namespace doris {
namespace {
const std::string ROWSET_PREFIX = "rst_";
} // namespace

using namespace ErrorCode;

bool RowsetMetaManager::check_rowset_meta(OlapMeta* meta, TabletUid tablet_uid,
                                          const RowsetId& rowset_id) {
    std::string key = ROWSET_PREFIX + tablet_uid.to_string() + "_" + rowset_id.to_string();
    std::string value;
    return meta->key_may_exist(META_COLUMN_FAMILY_INDEX, key, &value);
}

Status RowsetMetaManager::exists(OlapMeta* meta, TabletUid tablet_uid, const RowsetId& rowset_id) {
    std::string key = ROWSET_PREFIX + tablet_uid.to_string() + "_" + rowset_id.to_string();
    std::string value;
    return meta->get(META_COLUMN_FAMILY_INDEX, key, &value);
}

Status RowsetMetaManager::get_rowset_meta(OlapMeta* meta, TabletUid tablet_uid,
                                          const RowsetId& rowset_id,
                                          RowsetMetaSharedPtr rowset_meta) {
    std::string key = ROWSET_PREFIX + tablet_uid.to_string() + "_" + rowset_id.to_string();
    std::string value;
    Status s = meta->get(META_COLUMN_FAMILY_INDEX, key, &value);
    if (s.is<META_KEY_NOT_FOUND>()) {
        std::string error_msg = "rowset id:" + key + " not found.";
        LOG(WARNING) << error_msg;
        return Status::Error<META_KEY_NOT_FOUND>();
    } else if (!s.ok()) {
        std::string error_msg = "load rowset id:" + key + " failed.";
        LOG(WARNING) << error_msg;
        return Status::Error<IO_ERROR>();
    }
    bool ret = rowset_meta->init(value);
    if (!ret) {
        std::string error_msg = "parse rowset meta failed. rowset id:" + key;
        return Status::Error<SERIALIZE_PROTOBUF_ERROR>();
    }
    return Status::OK();
}

Status RowsetMetaManager::get_json_rowset_meta(OlapMeta* meta, TabletUid tablet_uid,
                                               const RowsetId& rowset_id,
                                               std::string* json_rowset_meta) {
    RowsetMetaSharedPtr rowset_meta_ptr(new (std::nothrow) RowsetMeta());
    Status status = get_rowset_meta(meta, tablet_uid, rowset_id, rowset_meta_ptr);
    if (!status.ok()) {
        return status;
    }
    bool ret = rowset_meta_ptr->json_rowset_meta(json_rowset_meta);
    if (!ret) {
        std::string error_msg = "get json rowset meta failed. rowset id:" + rowset_id.to_string();
        return Status::Error<SERIALIZE_PROTOBUF_ERROR>();
    }
    return Status::OK();
}
Status RowsetMetaManager::save(OlapMeta* meta, TabletUid tablet_uid, const RowsetId& rowset_id,
                               const RowsetMetaPB& rowset_meta_pb, bool enable_binlog) {
    if (enable_binlog) {
        return _save_with_binlog(meta, tablet_uid, rowset_id, rowset_meta_pb);
    } else {
        return save(meta, tablet_uid, rowset_id, rowset_meta_pb);
    }
}

Status RowsetMetaManager::save(OlapMeta* meta, TabletUid tablet_uid, const RowsetId& rowset_id,
                               const RowsetMetaPB& rowset_meta_pb) {
    std::string key =
            fmt::format("{}{}_{}", ROWSET_PREFIX, tablet_uid.to_string(), rowset_id.to_string());
    std::string value;
    if (!rowset_meta_pb.SerializeToString(&value)) {
        LOG(WARNING) << "serialize rowset pb failed. rowset id:" << key;
        return Status::Error<SERIALIZE_PROTOBUF_ERROR>();
    }

    return meta->put(META_COLUMN_FAMILY_INDEX, key, value);
}

Status RowsetMetaManager::_save_with_binlog(OlapMeta* meta, TabletUid tablet_uid,
                                            const RowsetId& rowset_id,
                                            const RowsetMetaPB& rowset_meta_pb) {
    // create rowset write data
    std::string rowset_key =
            fmt::format("{}{}_{}", ROWSET_PREFIX, tablet_uid.to_string(), rowset_id.to_string());
    std::string rowset_value;
    if (!rowset_meta_pb.SerializeToString(&rowset_value)) {
        LOG(WARNING) << "serialize rowset pb failed. rowset id:" << rowset_key;
        return Status::Error<SERIALIZE_PROTOBUF_ERROR>();
    }

    // create binlog write data
    // binlog_meta_key format: {kBinlogPrefix}meta_{tablet_uid}_{version}_{rowset_id}
    // binlog_data_key format: {kBinlogPrefix}data_{tablet_uid}_{version}_{rowset_id}
    // version is formatted to 20 bytes to avoid the problem of sorting, version is lower, timestamp is lower
    // binlog key is not supported for cumulative rowset
    if (rowset_meta_pb.start_version() != rowset_meta_pb.end_version()) {
        LOG(WARNING) << "binlog key is not supported for cumulative rowset. rowset id:"
                     << rowset_key;
        return Status::Error<ROWSET_BINLOG_NOT_ONLY_ONE_VERSION>();
    }
    auto version = rowset_meta_pb.start_version();
    std::string binlog_meta_key = make_binlog_meta_key(tablet_uid, version, rowset_id);
    std::string binlog_data_key = make_binlog_data_key(tablet_uid, version, rowset_id);
    BinlogMetaEntryPB binlog_meta_entry_pb;
    binlog_meta_entry_pb.set_version(version);
    binlog_meta_entry_pb.set_tablet_id(rowset_meta_pb.tablet_id());
    binlog_meta_entry_pb.set_rowset_id(rowset_meta_pb.rowset_id());
    binlog_meta_entry_pb.set_num_segments(rowset_meta_pb.num_segments());
    binlog_meta_entry_pb.set_creation_time(rowset_meta_pb.creation_time());
    std::string binlog_meta_value;
    if (!binlog_meta_entry_pb.SerializeToString(&binlog_meta_value)) {
        LOG(WARNING) << "serialize binlog pb failed. rowset id:" << binlog_meta_key;
        return Status::Error<SERIALIZE_PROTOBUF_ERROR>();
    }

    // create batch entries
    std::vector<OlapMeta::BatchEntry> entries = {
            {std::cref(rowset_key), std::cref(rowset_value)},
            {std::cref(binlog_meta_key), std::cref(binlog_meta_value)},
            {std::cref(binlog_data_key), std::cref(rowset_value)}};

    return meta->put(META_COLUMN_FAMILY_INDEX, entries);
}

std::vector<std::string> RowsetMetaManager::get_binlog_filenames(OlapMeta* meta,
                                                                 TabletUid tablet_uid,
                                                                 std::string_view binlog_version,
                                                                 int64_t segment_idx) {
    auto prefix_key = make_binlog_filename_key(tablet_uid, binlog_version);
    LOG(INFO) << fmt::format("prefix_key:{}", prefix_key);

    std::vector<std::string> binlog_files;
    std::string rowset_id;
    int64_t num_segments = -1;
    auto traverse_func = [&rowset_id, &num_segments](const std::string& key,
                                                     const std::string& value) -> bool {
        LOG(INFO) << fmt::format("key:{}, value:{}", key, value);
        // key is 'binglog_meta_6943f1585fe834b5-e542c2b83a21d0b7_00000000000000000069_020000000000000135449d7cd7eadfe672aa0f928fa99593', extract last part '020000000000000135449d7cd7eadfe672aa0f928fa99593'
        // check starts with "binlog_meta_"
        if (!starts_with_binlog_meta(key)) {
            LOG(WARNING) << fmt::format("invalid binlog meta key:{}", key);
            return false;
        }
        if (auto pos = key.rfind("_"); pos == std::string::npos) {
            LOG(WARNING) << fmt::format("invalid binlog meta key:{}", key);
            return false;
        } else {
            rowset_id = key.substr(pos + 1);
        }

        BinlogMetaEntryPB binlog_meta_entry_pb;
        if (!binlog_meta_entry_pb.ParseFromString(value)) {
            LOG(WARNING) << fmt::format("invalid binlog meta value:{}", value);
            return false;
        }
        num_segments = binlog_meta_entry_pb.num_segments();

        return false;
    };
    LOG(INFO) << "result:" << rowset_id;

    // get binlog meta by prefix
    Status status = meta->iterate(META_COLUMN_FAMILY_INDEX, prefix_key, traverse_func);
    if (!status.ok() || rowset_id.empty() || num_segments < 0) {
        LOG(WARNING) << fmt::format(
                "fail to get binlog filename. tablet uid:{}, binlog version:{}, status:{}, "
                "rowset_id:{}, num_segments:{}",
                tablet_uid.to_string(), binlog_version, status.to_string(), rowset_id,
                num_segments);
    }

    // construct binlog_files list
    if (segment_idx >= num_segments) {
        LOG(WARNING) << fmt::format("invalid segment idx:{}, num_segments:{}", segment_idx,
                                    num_segments);
        return binlog_files;
    }
    for (int64_t i = 0; i < num_segments; ++i) {
        // TODO(Drogon): Update to filesystem path
        auto segment_file = fmt::format("{}_{}.dat", rowset_id, i);
        binlog_files.emplace_back(std::move(segment_file));
    }
    return binlog_files;
}

std::pair<std::string, int64_t> RowsetMetaManager::get_binlog_info(
        OlapMeta* meta, TabletUid tablet_uid, std::string_view binlog_version) {
    LOG(INFO) << fmt::format("tablet_uid:{}, binlog_version:{}", tablet_uid.to_string(),
                             binlog_version);
    auto prefix_key = make_binlog_filename_key(tablet_uid, binlog_version);
    LOG(INFO) << fmt::format("prefix_key:{}", prefix_key);

    std::string rowset_id;
    int64_t num_segments = -1;
    auto traverse_func = [&rowset_id, &num_segments](const std::string& key,
                                                     const std::string& value) -> bool {
        LOG(INFO) << fmt::format("key:{}, value:{}", key, value);
        // key is 'binglog_meta_6943f1585fe834b5-e542c2b83a21d0b7_00000000000000000069_020000000000000135449d7cd7eadfe672aa0f928fa99593', extract last part '020000000000000135449d7cd7eadfe672aa0f928fa99593'
        auto pos = key.rfind("_");
        if (pos == std::string::npos) {
            LOG(WARNING) << fmt::format("invalid binlog meta key:{}", key);
            return false;
        }
        rowset_id = key.substr(pos + 1);

        BinlogMetaEntryPB binlog_meta_entry_pb;
        binlog_meta_entry_pb.ParseFromString(value);
        num_segments = binlog_meta_entry_pb.num_segments();

        return false;
    };
    LOG(INFO) << "result:" << rowset_id;

    // get binlog meta by prefix
    Status status = meta->iterate(META_COLUMN_FAMILY_INDEX, prefix_key, traverse_func);
    if (!status.ok() || rowset_id.empty() || num_segments < 0) {
        LOG(WARNING) << fmt::format(
                "fail to get binlog filename. tablet uid:{}, binlog version:{}, status:{}, "
                "rowset_id:{}, num_segments:{}",
                tablet_uid.to_string(), binlog_version, status.to_string(), rowset_id,
                num_segments);
    }

    return std::make_pair(rowset_id, num_segments);
}

std::string RowsetMetaManager::get_binlog_rowset_meta(OlapMeta* meta, TabletUid tablet_uid,
                                                      std::string_view binlog_version,
                                                      std::string_view rowset_id) {
    auto binlog_data_key = make_binlog_data_key(tablet_uid.to_string(), binlog_version, rowset_id);
    LOG(INFO) << fmt::format("get binlog_meta_key:{}", binlog_data_key);

    std::string binlog_meta_value;
    Status status = meta->get(META_COLUMN_FAMILY_INDEX, binlog_data_key, &binlog_meta_value);
    if (!status.ok()) {
        LOG(WARNING) << fmt::format(
                "fail to get binlog meta. tablet uid:{}, binlog version:{}, "
                "rowset_id:{}, status:{}",
                tablet_uid.to_string(), binlog_version, rowset_id, status.to_string());
        return "";
    }
    return binlog_meta_value;
}

Status RowsetMetaManager::remove(OlapMeta* meta, TabletUid tablet_uid, const RowsetId& rowset_id) {
    std::string key = ROWSET_PREFIX + tablet_uid.to_string() + "_" + rowset_id.to_string();
    VLOG_NOTICE << "start to remove rowset, key:" << key;
    Status status = meta->remove(META_COLUMN_FAMILY_INDEX, key);
    VLOG_NOTICE << "remove rowset key:" << key << " finished";
    return status;
}

Status RowsetMetaManager::traverse_rowset_metas(
        OlapMeta* meta,
        std::function<bool(const TabletUid&, const RowsetId&, const std::string&)> const& func) {
    auto traverse_rowset_meta_func = [&func](const std::string& key,
                                             const std::string& value) -> bool {
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
    Status status =
            meta->iterate(META_COLUMN_FAMILY_INDEX, ROWSET_PREFIX, traverse_rowset_meta_func);
    return status;
}

Status RowsetMetaManager::load_json_rowset_meta(OlapMeta* meta,
                                                const std::string& rowset_meta_path) {
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
        return Status::Error<SERIALIZE_PROTOBUF_ERROR>();
    }
    RowsetId rowset_id = rowset_meta.rowset_id();
    TabletUid tablet_uid = rowset_meta.tablet_uid();
    Status status = save(meta, tablet_uid, rowset_id, rowset_meta.get_rowset_pb());
    return status;
}

} // namespace doris
