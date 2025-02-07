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
    DORIS_FORMAT, // for doris format file {tablet_id}{rowset_id}{segment_id}
    ORC,
    PARQUET
};

struct FileMapping {
    FileMappingType type;
    std::string value;

    FileMapping(int64_t tablet_id, RowsetId rowset_id, uint32_t segment_id)
            : type(FileMappingType::DORIS_FORMAT) {
        value.resize(sizeof(tablet_id) + sizeof(rowset_id) + sizeof(segment_id));
        auto* ptr = value.data();

        memcpy(ptr, &tablet_id, sizeof(tablet_id));
        ptr += sizeof(tablet_id);
        memcpy(ptr, &rowset_id, sizeof(rowset_id));
        ptr += sizeof(rowset_id);
        memcpy(ptr, &segment_id, sizeof(segment_id));
    }

    std::tuple<int64_t, RowsetId, uint32_t> get_doris_format_info() const {
        DCHECK(type == FileMappingType::DORIS_FORMAT);
        DCHECK(value.size() == sizeof(int64_t) + sizeof(RowsetId) + sizeof(uint32_t));

        auto* ptr = value.data();
        int64_t tablet_id;
        memcpy(&tablet_id, ptr, sizeof(tablet_id));
        ptr += sizeof(tablet_id);
        RowsetId rowset_id;
        memcpy(&rowset_id, ptr, sizeof(rowset_id));
        ptr += sizeof(rowset_id);
        uint32_t segment_id;
        memcpy(&segment_id, ptr, sizeof(segment_id));

        return std::make_tuple(tablet_id, rowset_id, segment_id);
    }
};

class IdFileMap {
public:
    std::shared_ptr<FileMapping> get_file_mapping(uint32_t id) {
        std::shared_lock lock(_mtx);
        auto it = _id_map.find(id);
        if (it == _id_map.end()) {
            return nullptr;
        }
        return it->second;
    }

    uint32 get_file_mapping_id(const std::shared_ptr<FileMapping>& mapping) {
        DCHECK(!mapping);
        std::unique_lock lock(_mtx);
        auto it = _mapping_to_id.find(mapping->value);
        if (it != _mapping_to_id.end()) {
            return it->second;
        }
        _id_map[_init_id++] = mapping;
        _mapping_to_id[mapping->value] = _init_id - 1;

        return _init_id - 1;
    }

private:
    std::shared_mutex _mtx;
    uint32_t _init_id = 0;
    std::unordered_map<std::string_view, uint32_t> _mapping_to_id;
    std::unordered_map<uint32_t, std::shared_ptr<FileMapping>> _id_map;
};

class IdManager {
public:
    static constexpr uint8_t ID_VERSION = 0;

    IdManager() = default;

    ~IdManager() {
        std::unique_lock lock(_query_to_id_file_map_mtx);
        _query_to_id_file_map.clear();
    }

    std::shared_ptr<IdFileMap> add_id_file_map(const UniqueId& query_id) {
        std::unique_lock lock(_query_to_id_file_map_mtx);
        auto it = _query_to_id_file_map.find(query_id);
        if (it == _query_to_id_file_map.end()) {
            auto id_file_map = std::make_shared<IdFileMap>();
            _query_to_id_file_map[query_id] = id_file_map;
            return id_file_map;
        }
        return it->second;
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