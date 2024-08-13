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
#include "util/debug_points.h"

namespace doris {

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
        return Status::Error<META_KEY_NOT_FOUND>("rowset id: {} not found.", key);
    } else if (!s.ok()) {
        return Status::Error<IO_ERROR>("load rowset id: {} failed.", key);
    }
    bool ret = rowset_meta->init(value);
    if (!ret) {
        return Status::Error<SERIALIZE_PROTOBUF_ERROR>("parse rowset meta failed. rowset id: {}",
                                                       key);
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
        return Status::Error<SERIALIZE_PROTOBUF_ERROR>("get json rowset meta failed. rowset id:{}",
                                                       rowset_id.to_string());
    }
    return Status::OK();
}
Status RowsetMetaManager::save(OlapMeta* meta, TabletUid tablet_uid, const RowsetId& rowset_id,
                               const RowsetMetaPB& rowset_meta_pb, bool enable_binlog) {
    if (rowset_meta_pb.partition_id() <= 0) {
        LOG(WARNING) << "invalid partition id " << rowset_meta_pb.partition_id() << " tablet "
                     << rowset_meta_pb.tablet_id();
        // TODO(dx): after fix partition id eq 0 bug, fix it
        // return Status::InternalError("invaid partition id {} tablet {}",
        //  rowset_meta_pb.partition_id(), rowset_meta_pb.tablet_id());
    }
    DBUG_EXECUTE_IF("RowsetMetaManager::save::zero_partition_id", {
        long partition_id = rowset_meta_pb.partition_id();
        auto& rs_pb = const_cast<std::decay_t<decltype(rowset_meta_pb)>&>(rowset_meta_pb);
        rs_pb.set_partition_id(0);
        LOG(WARNING) << "set debug point RowsetMetaManager::save::zero_partition_id old="
                     << partition_id << " new=" << rowset_meta_pb.DebugString();
    });
    if (enable_binlog) {
        return _save_with_binlog(meta, tablet_uid, rowset_id, rowset_meta_pb);
    } else {
        return _save(meta, tablet_uid, rowset_id, rowset_meta_pb);
    }
}

Status RowsetMetaManager::_save(OlapMeta* meta, TabletUid tablet_uid, const RowsetId& rowset_id,
                                const RowsetMetaPB& rowset_meta_pb) {
    std::string key =
            fmt::format("{}{}_{}", ROWSET_PREFIX, tablet_uid.to_string(), rowset_id.to_string());
    std::string value;
    if (!rowset_meta_pb.SerializeToString(&value)) {
        return Status::Error<SERIALIZE_PROTOBUF_ERROR>("serialize rowset pb failed. rowset id:{}",
                                                       key);
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
        return Status::Error<SERIALIZE_PROTOBUF_ERROR>("serialize rowset pb failed. rowset id:{}",
                                                       rowset_key);
    }

    // create binlog write data
    // binlog_meta_key format: {kBinlogPrefix}meta_{tablet_uid}_{version}_{rowset_id}
    // binlog_data_key format: {kBinlogPrefix}data_{tablet_uid}_{version}_{rowset_id}
    // version is formatted to 20 bytes to avoid the problem of sorting, version is lower, timestamp is lower
    // binlog key is not supported for cumulative rowset
    if (rowset_meta_pb.start_version() != rowset_meta_pb.end_version()) {
        return Status::Error<ROWSET_BINLOG_NOT_ONLY_ONE_VERSION>(
                "binlog key is not supported for cumulative rowset. rowset id:{}", rowset_key);
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
    binlog_meta_entry_pb.set_rowset_id_v2(rowset_meta_pb.rowset_id_v2());
    std::string binlog_meta_value;
    if (!binlog_meta_entry_pb.SerializeToString(&binlog_meta_value)) {
        return Status::Error<SERIALIZE_PROTOBUF_ERROR>("serialize binlog pb failed. rowset id:{}",
                                                       binlog_meta_key);
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
    VLOG_DEBUG << fmt::format("prefix_key:{}", prefix_key);

    std::vector<std::string> binlog_files;
    std::string rowset_id;
    int64_t num_segments = -1;
    auto traverse_func = [&rowset_id, &num_segments](std::string_view key,
                                                     std::string_view value) -> bool {
        VLOG_DEBUG << fmt::format("key:{}, value:{}", key, value);
        // key is 'binlog_meta_6943f1585fe834b5-e542c2b83a21d0b7_00000000000000000069_020000000000000135449d7cd7eadfe672aa0f928fa99593', extract last part '020000000000000135449d7cd7eadfe672aa0f928fa99593'
        // check starts with "binlog_meta_"
        if (!starts_with_binlog_meta(key)) {
            LOG(WARNING) << fmt::format("invalid binlog meta key:{}", key);
            return false;
        }
        if (auto pos = key.rfind('_'); pos == std::string::npos) {
            LOG(WARNING) << fmt::format("invalid binlog meta key:{}", key);
            return false;
        } else {
            rowset_id = key.substr(pos + 1);
        }

        BinlogMetaEntryPB binlog_meta_entry_pb;
        if (!binlog_meta_entry_pb.ParseFromArray(value.data(), value.size())) {
            LOG(WARNING) << fmt::format("invalid binlog meta value:{}", value);
            return false;
        }
        num_segments = binlog_meta_entry_pb.num_segments();

        return false;
    };

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
    VLOG_DEBUG << fmt::format("tablet_uid:{}, binlog_version:{}", tablet_uid.to_string(),
                              binlog_version);
    auto prefix_key = make_binlog_filename_key(tablet_uid, binlog_version);
    VLOG_DEBUG << fmt::format("prefix_key:{}", prefix_key);

    std::string rowset_id;
    int64_t num_segments = -1;
    auto traverse_func = [&rowset_id, &num_segments](std::string_view key,
                                                     std::string_view value) -> bool {
        VLOG_DEBUG << fmt::format("key:{}, value:{}", key, value);
        // key is 'binlog_meta_6943f1585fe834b5-e542c2b83a21d0b7_00000000000000000069_020000000000000135449d7cd7eadfe672aa0f928fa99593', extract last part '020000000000000135449d7cd7eadfe672aa0f928fa99593'
        auto pos = key.rfind('_');
        if (pos == std::string::npos) {
            LOG(WARNING) << fmt::format("invalid binlog meta key:{}", key);
            return false;
        }
        rowset_id = key.substr(pos + 1);

        BinlogMetaEntryPB binlog_meta_entry_pb;
        binlog_meta_entry_pb.ParseFromArray(value.data(), value.size());
        num_segments = binlog_meta_entry_pb.num_segments();

        return false;
    };

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

std::string RowsetMetaManager::get_rowset_binlog_meta(OlapMeta* meta, TabletUid tablet_uid,
                                                      std::string_view binlog_version,
                                                      std::string_view rowset_id) {
    auto binlog_data_key = make_binlog_data_key(tablet_uid.to_string(), binlog_version, rowset_id);
    VLOG_DEBUG << fmt::format("get binlog_meta_key:{}", binlog_data_key);

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

Status RowsetMetaManager::get_rowset_binlog_metas(OlapMeta* meta, const TabletUid tablet_uid,
                                                  const std::vector<int64_t>& binlog_versions,
                                                  RowsetBinlogMetasPB* metas_pb) {
    if (binlog_versions.empty()) {
        return _get_all_rowset_binlog_metas(meta, tablet_uid, metas_pb);
    } else {
        return _get_rowset_binlog_metas(meta, tablet_uid, binlog_versions, metas_pb);
    }
}

Status RowsetMetaManager::_get_rowset_binlog_metas(OlapMeta* meta, const TabletUid tablet_uid,
                                                   const std::vector<int64_t>& binlog_versions,
                                                   RowsetBinlogMetasPB* metas_pb) {
    Status status;
    auto tablet_uid_str = tablet_uid.to_string();
    auto traverse_func = [meta, metas_pb, &status, &tablet_uid_str](
                                 std::string_view key, std::string_view value) -> bool {
        VLOG_DEBUG << fmt::format("key:{}, value:{}", key, value);
        if (!starts_with_binlog_meta(key)) {
            auto err_msg = fmt::format("invalid binlog meta key:{}", key);
            status = Status::InternalError(err_msg);
            LOG(WARNING) << err_msg;
            return false;
        }

        BinlogMetaEntryPB binlog_meta_entry_pb;
        if (!binlog_meta_entry_pb.ParseFromArray(value.data(), value.size())) {
            auto err_msg = fmt::format("fail to parse binlog meta value:{}", value);
            status = Status::InternalError(err_msg);
            LOG(WARNING) << err_msg;
            return false;
        }
        auto& rowset_id = binlog_meta_entry_pb.rowset_id_v2();

        auto binlog_meta_pb = metas_pb->add_rowset_binlog_metas();
        binlog_meta_pb->set_rowset_id(rowset_id);
        binlog_meta_pb->set_version(binlog_meta_entry_pb.version());
        binlog_meta_pb->set_num_segments(binlog_meta_entry_pb.num_segments());
        binlog_meta_pb->set_meta_key(std::string {key});
        binlog_meta_pb->set_meta(std::string {value});

        auto binlog_data_key =
                make_binlog_data_key(tablet_uid_str, binlog_meta_entry_pb.version(), rowset_id);
        std::string binlog_data;
        status = meta->get(META_COLUMN_FAMILY_INDEX, binlog_data_key, &binlog_data);
        if (!status.ok()) {
            LOG(WARNING) << status.to_string();
            return false;
        }
        binlog_meta_pb->set_data_key(binlog_data_key);
        binlog_meta_pb->set_data(binlog_data);

        return false;
    };

    for (auto& binlog_version : binlog_versions) {
        auto prefix_key = make_binlog_meta_key_prefix(tablet_uid, binlog_version);
        Status iterStatus = meta->iterate(META_COLUMN_FAMILY_INDEX, prefix_key, traverse_func);
        if (!iterStatus.ok()) {
            LOG(WARNING) << fmt::format("fail to iterate binlog meta. prefix_key:{}, status:{}",
                                        prefix_key, iterStatus.to_string());
            return iterStatus;
        }
        if (!status.ok()) {
            return status;
        }
    }
    return status;
}

Status RowsetMetaManager::_get_all_rowset_binlog_metas(OlapMeta* meta, const TabletUid tablet_uid,
                                                       RowsetBinlogMetasPB* metas_pb) {
    Status status;
    auto tablet_uid_str = tablet_uid.to_string();
    int64_t tablet_id = 0;
    auto traverse_func = [meta, metas_pb, &status, &tablet_uid_str, &tablet_id](
                                 std::string_view key, std::string_view value) -> bool {
        VLOG_DEBUG << fmt::format("key:{}, value:{}", key, value);
        if (!starts_with_binlog_meta(key)) {
            LOG(INFO) << fmt::format("end scan binlog meta. key:{}", key);
            return false;
        }

        BinlogMetaEntryPB binlog_meta_entry_pb;
        if (!binlog_meta_entry_pb.ParseFromArray(value.data(), value.size())) {
            auto err_msg = fmt::format("fail to parse binlog meta value:{}", value);
            status = Status::InternalError(err_msg);
            LOG(WARNING) << err_msg;
            return false;
        }
        if (tablet_id == 0) {
            tablet_id = binlog_meta_entry_pb.tablet_id();
        } else if (tablet_id != binlog_meta_entry_pb.tablet_id()) {
            // scan all binlog meta, so tablet_id should be same:
            return false;
        }
        auto& rowset_id = binlog_meta_entry_pb.rowset_id_v2();

        auto binlog_meta_pb = metas_pb->add_rowset_binlog_metas();
        binlog_meta_pb->set_rowset_id(rowset_id);
        binlog_meta_pb->set_version(binlog_meta_entry_pb.version());
        binlog_meta_pb->set_num_segments(binlog_meta_entry_pb.num_segments());
        binlog_meta_pb->set_meta_key(std::string {key});
        binlog_meta_pb->set_meta(std::string {value});

        auto binlog_data_key =
                make_binlog_data_key(tablet_uid_str, binlog_meta_entry_pb.version(), rowset_id);
        std::string binlog_data;
        status = meta->get(META_COLUMN_FAMILY_INDEX, binlog_data_key, &binlog_data);
        if (!status.ok()) {
            LOG(WARNING) << status;
            return false;
        }
        binlog_meta_pb->set_data_key(binlog_data_key);
        binlog_meta_pb->set_data(binlog_data);

        return true;
    };

    auto prefix_key = make_binlog_meta_key_prefix(tablet_uid);
    Status iterStatus = meta->iterate(META_COLUMN_FAMILY_INDEX, prefix_key, traverse_func);
    if (!iterStatus.ok()) {
        LOG(WARNING) << fmt::format("fail to iterate binlog meta. prefix_key:{}, status:{}",
                                    prefix_key, iterStatus.to_string());
        return iterStatus;
    }
    return status;
}

Status RowsetMetaManager::remove(OlapMeta* meta, TabletUid tablet_uid, const RowsetId& rowset_id) {
    std::string key = ROWSET_PREFIX + tablet_uid.to_string() + "_" + rowset_id.to_string();
    VLOG_NOTICE << "start to remove rowset, key:" << key;
    Status status = meta->remove(META_COLUMN_FAMILY_INDEX, key);
    VLOG_NOTICE << "remove rowset key:" << key << " finished";
    return status;
}

Status RowsetMetaManager::remove_binlog(OlapMeta* meta, const std::string& suffix) {
    // Please do not remove std::vector<std::string>, more info refer to pr#23190
    return meta->remove(META_COLUMN_FAMILY_INDEX,
                        std::vector<std::string> {kBinlogMetaPrefix.data() + suffix,
                                                  kBinlogDataPrefix.data() + suffix});
}

Status RowsetMetaManager::ingest_binlog_metas(OlapMeta* meta, TabletUid tablet_uid,
                                              RowsetBinlogMetasPB* metas_pb) {
    std::vector<OlapMeta::BatchEntry> entries;
    const auto tablet_uid_str = tablet_uid.to_string();

    for (auto& rowset_binlog_meta : *metas_pb->mutable_rowset_binlog_metas()) {
        auto& rowset_id = rowset_binlog_meta.rowset_id();
        auto version = rowset_binlog_meta.version();

        auto meta_key = rowset_binlog_meta.mutable_meta_key();
        *meta_key = make_binlog_meta_key(tablet_uid_str, version, rowset_id);
        auto data_key = rowset_binlog_meta.mutable_data_key();
        *data_key = make_binlog_data_key(tablet_uid_str, version, rowset_id);

        entries.emplace_back(*meta_key, rowset_binlog_meta.meta());
        entries.emplace_back(*data_key, rowset_binlog_meta.data());
    }

    return meta->put(META_COLUMN_FAMILY_INDEX, entries);
}

Status RowsetMetaManager::traverse_rowset_metas(
        OlapMeta* meta,
        std::function<bool(const TabletUid&, const RowsetId&, std::string_view)> const& func) {
    auto traverse_rowset_meta_func = [&func](std::string_view key, std::string_view value) -> bool {
        std::vector<std::string> parts;
        // key format: rst_uuid_rowset_id
        RETURN_IF_ERROR(split_string(key, '_', &parts));
        if (parts.size() != 3) {
            LOG(WARNING) << "invalid rowset key:" << key << ", splitted size:" << parts.size();
            return true;
        }
        RowsetId rowset_id;
        rowset_id.init(parts[2]);
        std::vector<std::string> uid_parts;
        RETURN_IF_ERROR(split_string(parts[1], '-', &uid_parts));
        TabletUid tablet_uid(uid_parts[0], uid_parts[1]);
        return func(tablet_uid, rowset_id, value);
    };
    Status status =
            meta->iterate(META_COLUMN_FAMILY_INDEX, ROWSET_PREFIX, traverse_rowset_meta_func);
    return status;
}

Status RowsetMetaManager::traverse_binlog_metas(
        OlapMeta* meta,
        std::function<bool(std::string_view, std::string_view, bool)> const& collector) {
    std::pair<std::string, bool> last_info = std::make_pair(kBinlogMetaPrefix.data(), false);
    bool seek_found = false;
    Status status;
    auto traverse_binlog_meta_func = [&last_info, &seek_found, &collector](
                                             std::string_view key, std::string_view value) -> bool {
        seek_found = true;
        auto& [last_prefix, need_collect] = last_info;
        size_t pos = key.find('_', kBinlogMetaPrefix.size());
        if (pos == std::string::npos) {
            LOG(WARNING) << "invalid binlog meta key: " << key;
            return true;
        }
        std::string_view key_view(key.data(), pos);
        std::string_view last_prefix_view(last_prefix.data(), last_prefix.size() - 1);

        if (last_prefix_view != key_view) {
            need_collect = collector(key, value, true);
            last_prefix = std::string(key_view) + "~";
        } else if (need_collect) {
            collector(key, value, false);
        }

        return need_collect;
    };

    do {
        seek_found = false;
        status = meta->iterate(META_COLUMN_FAMILY_INDEX, last_info.first, kBinlogMetaPrefix.data(),
                               traverse_binlog_meta_func);
    } while (status.ok() && seek_found);

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
        return Status::Error<SERIALIZE_PROTOBUF_ERROR>("parse json rowset meta failed.");
    }
    RowsetId rowset_id = rowset_meta.rowset_id();
    TabletUid tablet_uid = rowset_meta.tablet_uid();
    Status status = save(meta, tablet_uid, rowset_id, rowset_meta.get_rowset_pb(), false);
    return status;
}

Status RowsetMetaManager::save_partial_update_info(
        OlapMeta* meta, int64_t tablet_id, int64_t partition_id, int64_t txn_id,
        const PartialUpdateInfoPB& partial_update_info_pb) {
    std::string key =
            fmt::format("{}{}_{}_{}", PARTIAL_UPDATE_INFO_PREFIX, tablet_id, partition_id, txn_id);
    std::string value;
    if (!partial_update_info_pb.SerializeToString(&value)) {
        return Status::Error<SERIALIZE_PROTOBUF_ERROR>(
                "serialize partial update info failed. key={}", key);
    }
    VLOG_NOTICE << "save partial update info, key=" << key << ", value_size=" << value.size();
    return meta->put(META_COLUMN_FAMILY_INDEX, key, value);
}

Status RowsetMetaManager::try_get_partial_update_info(OlapMeta* meta, int64_t tablet_id,
                                                      int64_t partition_id, int64_t txn_id,
                                                      PartialUpdateInfoPB* partial_update_info_pb) {
    std::string key =
            fmt::format("{}{}_{}_{}", PARTIAL_UPDATE_INFO_PREFIX, tablet_id, partition_id, txn_id);
    std::string value;
    Status status = meta->get(META_COLUMN_FAMILY_INDEX, key, &value);
    if (status.is<META_KEY_NOT_FOUND>()) {
        return status;
    }
    if (!status.ok()) {
        LOG_WARNING("failed to get partial update info. tablet_id={}, partition_id={}, txn_id={}",
                    tablet_id, partition_id, txn_id);
        return status;
    }
    if (!partial_update_info_pb->ParseFromString(value)) {
        return Status::Error<ErrorCode::PARSE_PROTOBUF_ERROR>(
                "fail to parse partial update info content to protobuf object. tablet_id={}, "
                "partition_id={}, txn_id={}",
                tablet_id, partition_id, txn_id);
    }
    return Status::OK();
}

Status RowsetMetaManager::traverse_partial_update_info(
        OlapMeta* meta,
        std::function<bool(int64_t, int64_t, int64_t, std::string_view)> const& func) {
    auto traverse_partial_update_info_func = [&func](std::string_view key,
                                                     std::string_view value) -> bool {
        std::vector<std::string> parts;
        // key format: pui_{tablet_id}_{partition_id}_{txn_id}
        RETURN_IF_ERROR(split_string(key, '_', &parts));
        if (parts.size() != 4) {
            LOG_WARNING("invalid rowset key={}, splitted size={}", key, parts.size());
            return true;
        }
        int64_t tablet_id = std::stoll(parts[1]);
        int64_t partition_id = std::stoll(parts[2]);
        int64_t txn_id = std::stoll(parts[3]);
        return func(tablet_id, partition_id, txn_id, value);
    };
    return meta->iterate(META_COLUMN_FAMILY_INDEX, PARTIAL_UPDATE_INFO_PREFIX,
                         traverse_partial_update_info_func);
}

Status RowsetMetaManager::remove_partial_update_info(OlapMeta* meta, int64_t tablet_id,
                                                     int64_t partition_id, int64_t txn_id) {
    std::string key =
            fmt::format("{}{}_{}_{}", PARTIAL_UPDATE_INFO_PREFIX, tablet_id, partition_id, txn_id);
    Status res = meta->remove(META_COLUMN_FAMILY_INDEX, key);
    VLOG_NOTICE << "remove partial update info, key=" << key;
    return res;
}

Status RowsetMetaManager::remove_partial_update_infos(
        OlapMeta* meta, const std::vector<std::tuple<int64_t, int64_t, int64_t>>& keys) {
    std::vector<std::string> remove_keys;
    for (auto [tablet_id, partition_id, txn_id] : keys) {
        remove_keys.push_back(fmt::format("{}{}_{}_{}", PARTIAL_UPDATE_INFO_PREFIX, tablet_id,
                                          partition_id, txn_id));
    }
    Status res = meta->remove(META_COLUMN_FAMILY_INDEX, remove_keys);
    VLOG_NOTICE << "remove partial update info, remove_keys.size()=" << remove_keys.size();
    return res;
}

Status RowsetMetaManager::remove_tablet_related_partial_update_info(OlapMeta* meta,
                                                                    int64_t tablet_id) {
    std::string prefix = fmt::format("{}{}", PARTIAL_UPDATE_INFO_PREFIX, tablet_id);
    std::vector<std::string> remove_keys;
    auto get_remove_keys_func = [&](std::string_view key, std::string_view val) -> bool {
        remove_keys.emplace_back(key);
        return true;
    };
    VLOG_NOTICE << "remove tablet related partial update info, tablet_id: " << tablet_id
                << " removed keys size: " << remove_keys.size();
    RETURN_IF_ERROR(meta->iterate(META_COLUMN_FAMILY_INDEX, prefix, get_remove_keys_func));
    return meta->remove(META_COLUMN_FAMILY_INDEX, remove_keys);
}

} // namespace doris
