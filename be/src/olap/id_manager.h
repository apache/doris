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

#pragma once

#include <butil/macros.h>
#include <gen_cpp/BackendService_types.h>
#include <gen_cpp/Types_types.h>
#include <stddef.h>
#include <stdint.h>

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/status.h"
#include "olap/olap_common.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"

namespace doris {

enum class FileMappingType {
    INTERNAL, // for doris format file {tablet_id}{rowset_id}{segment_id}
    EXTERNAL, // for external table.
};

struct InternalFileMappingInfo {
    int64_t tablet_id;
    RowsetId rowset_id;
    uint32_t segment_id;

    std::string to_string() const {
        std::string value;
        value.resize(sizeof(tablet_id) + sizeof(rowset_id) + sizeof(segment_id));
        auto* ptr = value.data();

        memcpy(ptr, &tablet_id, sizeof(tablet_id));
        ptr += sizeof(tablet_id);
        memcpy(ptr, &rowset_id, sizeof(rowset_id));
        ptr += sizeof(rowset_id);
        memcpy(ptr, &segment_id, sizeof(segment_id));
        return value;
    }
};

struct ExternalFileMappingInfo {
    /* By recording the plan_node_id in fileMapping, the TFileScanRangeParams used in the scan phase can be found
    * from QueryContext according to the plan_node_id. Because there are some important information in
    * TFileScanRangeParams (needed when creating hdfs/s3 reader):
    *      8: optional THdfsParams hdfs_params;
    *      9: optional map<string, string> properties;
    */
    int plan_node_id;

    /*
     * Record TFileRangeDesc external_scan_range_desc in fileMapping, usage:
     * 1. If the file belongs to a partition, columns_from_path_keys and columns_from_path in TFileRangeDesc are needed when materializing the partition column
     * 2. path, file_type, modification_time,compress_type .... used to read the file
     * 3. TFileFormatType can distinguish whether it is iceberg/hive/hudi/paimon
     */
    TFileRangeDesc scan_range_desc;
    bool enable_file_meta_cache;

    ExternalFileMappingInfo(int plan_node_id, const TFileRangeDesc& scan_range,
                            bool file_meta_cache)
            : plan_node_id(plan_node_id),
              scan_range_desc(scan_range),
              enable_file_meta_cache(file_meta_cache) {}

    std::string to_string() const {
        std::string value;
        value.resize(scan_range_desc.path.size() + sizeof(plan_node_id) +
                     sizeof(scan_range_desc.start_offset));
        auto* ptr = value.data();

        memcpy(ptr, &plan_node_id, sizeof(plan_node_id));
        ptr += sizeof(plan_node_id);
        memcpy(ptr, &scan_range_desc.start_offset, sizeof(scan_range_desc.start_offset));
        ptr += sizeof(scan_range_desc.start_offset);
        memcpy(ptr, scan_range_desc.path.data(), scan_range_desc.path.size());
        return value;
    }
};

struct FileMapping {
    ENABLE_FACTORY_CREATOR(FileMapping);

    FileMappingType type;
    std::variant<InternalFileMappingInfo, ExternalFileMappingInfo> value;

    FileMapping(int64_t tablet_id, RowsetId rowset_id, uint32_t segment_id)
            : type(FileMappingType::INTERNAL),
              value(std::in_place_type<InternalFileMappingInfo>, tablet_id, rowset_id, segment_id) {
    }

    FileMapping(int plan_node_id, const TFileRangeDesc& scan_range, bool enable_file_meta_cache)
            : type(FileMappingType::EXTERNAL),
              value(std::in_place_type<ExternalFileMappingInfo>, plan_node_id, scan_range,
                    enable_file_meta_cache) {}

    std::tuple<int64_t, RowsetId, uint32_t> get_doris_format_info() const {
        DCHECK(type == FileMappingType::INTERNAL);
        auto info = std::get<InternalFileMappingInfo>(value);
        return std::make_tuple(info.tablet_id, info.rowset_id, info.segment_id);
    }

    ExternalFileMappingInfo& get_external_file_info() {
        DCHECK(type == FileMappingType::EXTERNAL);
        return std::get<ExternalFileMappingInfo>(value);
    }

    static std::string file_mapping_info_to_string(
            const std::variant<InternalFileMappingInfo, ExternalFileMappingInfo>& info) {
        return std::visit(
                [](const auto& info) -> std::string {
                    using T = std::decay_t<decltype(info)>;

                    if constexpr (std::is_same_v<T, InternalFileMappingInfo>) {
                        return info.to_string();

                    } else if constexpr (std::is_same_v<T, ExternalFileMappingInfo>) {
                        return info.to_string();
                    }
                },
                info);
    }

    std::string file_mapping_info_to_string() { return file_mapping_info_to_string(value); }
};

class IdFileMap {
public:
    IdFileMap(uint64_t expired_timestamp) : delayed_expired_timestamp(expired_timestamp) {}

    std::shared_ptr<FileMapping> get_file_mapping(uint32_t id) {
        std::shared_lock lock(_mtx);
        auto it = _id_map.find(id);
        if (it == _id_map.end()) {
            return nullptr;
        }
        return it->second;
    }

    uint32 get_file_mapping_id(const std::shared_ptr<FileMapping>& mapping) {
        DCHECK(mapping.get() != nullptr);
        auto value = mapping->file_mapping_info_to_string();

        std::unique_lock lock(_mtx);
        auto it = _mapping_to_id.find(value);
        if (it != _mapping_to_id.end()) {
            return it->second;
        }
        _id_map[_init_id++] = mapping;
        _mapping_to_id[value] = _init_id - 1;

        return _init_id - 1;
    }

    void add_temp_rowset(const RowsetSharedPtr& rowset) {
        std::unique_lock lock(_mtx);
        _temp_rowset_maps[{rowset->rowset_meta()->tablet_id(), rowset->rowset_id()}] = rowset;
    }

    RowsetSharedPtr get_temp_rowset(const int64_t tablet_id, const RowsetId& rowset_id) {
        std::shared_lock lock(_mtx);
        auto it = _temp_rowset_maps.find({tablet_id, rowset_id});
        if (it == _temp_rowset_maps.end()) {
            return nullptr;
        }
        return it->second;
    }

    int64_t get_delayed_expired_timestamp() { return delayed_expired_timestamp; }

private:
    std::shared_mutex _mtx;
    uint32_t _init_id = 0;
    std::unordered_map<std::string, uint32_t> _mapping_to_id;
    std::unordered_map<uint32_t, std::shared_ptr<FileMapping>> _id_map;

    // use in Doris Format to keep temp rowsets, preventing them from being deleted by compaction
    std::unordered_map<std::pair<int64_t, RowsetId>, RowsetSharedPtr> _temp_rowset_maps;
    uint64_t delayed_expired_timestamp = 0;
};

class IdManager {
public:
    static constexpr uint8_t ID_VERSION = 0;

    IdManager() = default;

    ~IdManager() {
        std::unique_lock lock(_query_to_id_file_map_mtx);
        _query_to_id_file_map.clear();
    }

    std::shared_ptr<IdFileMap> add_id_file_map(const UniqueId& query_id, int timeout) {
        std::unique_lock lock(_query_to_id_file_map_mtx);
        auto it = _query_to_id_file_map.find(query_id);
        if (it == _query_to_id_file_map.end()) {
            auto id_file_map = std::make_shared<IdFileMap>(UnixSeconds() + timeout + 10);
            _query_to_id_file_map[query_id] = id_file_map;
            return id_file_map;
        }
        return it->second;
    }

    void gc_expired_id_file_map(int64_t now) {
        std::unique_lock lock(_query_to_id_file_map_mtx);
        for (auto it = _query_to_id_file_map.begin(); it != _query_to_id_file_map.end();) {
            if (it->second->get_delayed_expired_timestamp() <= now) {
                LOG(INFO) << "gc expired id file map for query_id=" << it->first;
                it = _query_to_id_file_map.erase(it);
            } else {
                ++it;
            }
        }
    }

    void remove_id_file_map(const UniqueId& query_id) {
        std::unique_lock lock(_query_to_id_file_map_mtx);
        _query_to_id_file_map.erase(query_id);
    }

    std::shared_ptr<IdFileMap> get_id_file_map(const UniqueId& query_id) {
        std::shared_lock lock(_query_to_id_file_map_mtx);
        auto it = _query_to_id_file_map.find(query_id);
        if (it == _query_to_id_file_map.end()) {
            return nullptr;
        }
        return it->second;
    }

private:
    DISALLOW_COPY_AND_ASSIGN(IdManager);

    phmap::flat_hash_map<UniqueId, std::shared_ptr<IdFileMap>> _query_to_id_file_map;
    std::shared_mutex _query_to_id_file_map_mtx;
};

} // namespace doris