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

#include <fmt/format.h>

#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/logging.h" // DCHECK
#include "common/status.h"
#include "exec/sink/autoinc_buffer.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"          // DataWriteType
#include "storage/tablet/tablet_schema.h" // TabletSchemaSPtr
#include "storage/utils.h"                // BINLOG_TIMESTAMP_COL

namespace doris {

class AutoIncIDBuffer;
struct PartialUpdateInfo;
struct MowContext;

// Row binlog op type.
// NOTE: The value is persisted into row binlog data, so keep it stable.
static constexpr int64_t ROW_BINLOG_APPEND = 0;
static constexpr int64_t ROW_BINLOG_UPDATE = 1;
static constexpr int64_t ROW_BINLOG_DELETE = 2;

constexpr std::string_view kBinlogPrefix = "binlog_";
constexpr std::string_view kBinlogMetaPrefix = "binlog_meta_";
constexpr std::string_view kBinlogDataPrefix = "binlog_data_";
constexpr std::string_view kRowBinlogPrefix = "binlog_row_";
constexpr std::string_view kRowBinlogLsnColName = "__DORIS_BINLOG_LSN__";
// Alias to BINLOG_TIMESTAMP_COL in storage/utils.h to keep a single source of truth.
static const std::string& kRowBinlogTimestampColName = BINLOG_TIMESTAMP_COL;
constexpr std::string_view kRowBinlogOpColName = "__DORIS_BINLOG_OP__";

constexpr int64_t kBinlogLsnAutoIncId = -1;
// used in file directory
constexpr std::string_view FDRowBinlogSuffix = "_row_binlog";

inline auto make_binlog_meta_key(const std::string_view tablet, int64_t version,
                                 const std::string_view rowset) {
    return fmt::format("{}meta_{}_{:020d}_{}", kBinlogPrefix, tablet, version, rowset);
}

inline auto make_binlog_meta_key(const std::string_view tablet, const std::string_view version_str,
                                 const std::string_view rowset) {
    // TODO(Drogon): use fmt::format not convert to version_num, only string with length prefix '0'
    int64_t version = std::atoll(version_str.data());
    return make_binlog_meta_key(tablet, version, rowset);
}

inline auto make_binlog_meta_key(const TabletUid& tablet_uid, int64_t version,
                                 const RowsetId& rowset_id) {
    return make_binlog_meta_key(tablet_uid.to_string(), version, rowset_id.to_string());
}

inline auto make_binlog_meta_key_prefix(const TabletUid& tablet_uid) {
    return fmt::format("{}meta_{}_", kBinlogPrefix, tablet_uid.to_string());
}

inline auto make_binlog_meta_key_prefix(const TabletUid& tablet_uid, int64_t version) {
    return fmt::format("{}meta_{}_{:020d}_", kBinlogPrefix, tablet_uid.to_string(), version);
}

inline auto make_binlog_data_key(const std::string_view tablet, int64_t version,
                                 const std::string_view rowset) {
    return fmt::format("{}data_{}_{:020d}_{}", kBinlogPrefix, tablet, version, rowset);
}

inline auto make_binlog_data_key(const std::string_view tablet, const std::string_view version,
                                 const std::string_view rowset) {
    return fmt::format("{}data_{}_{:0>20}_{}", kBinlogPrefix, tablet, version, rowset);
}

inline auto make_binlog_data_key(const TabletUid& tablet_uid, int64_t version,
                                 const RowsetId& rowset_id) {
    return make_binlog_data_key(tablet_uid.to_string(), version, rowset_id.to_string());
}

inline auto make_binlog_data_key(const TabletUid& tablet_uid, int64_t version,
                                 const std::string_view rowset_id) {
    return make_binlog_data_key(tablet_uid.to_string(), version, rowset_id);
}

inline auto make_binlog_data_key_prefix(const TabletUid& tablet_uid, int64_t version) {
    return fmt::format("{}data_{}_{:020d}_", kBinlogPrefix, tablet_uid.to_string(), version);
}

inline auto make_binlog_filename_key(const TabletUid& tablet_uid, const std::string_view version) {
    return fmt::format("{}meta_{}_{:0>20}_", kBinlogPrefix, tablet_uid.to_string(), version);
}

inline bool starts_with_binlog_meta(const std::string_view str) {
    auto prefix = kBinlogMetaPrefix;
    if (prefix.length() > str.length()) {
        return false;
    }

    return str.compare(0, prefix.length(), prefix) == 0;
}

inline std::string get_binlog_data_key_from_meta_key(const std::string_view meta_key) {
    // like "binlog_meta_6943f1585fe834b5-e542c2b83a21d0b7" => "binlog_data-6943f1585fe834b5-e542c2b83a21d0b7"
    return fmt::format("{}data_{}", kBinlogPrefix, meta_key.substr(kBinlogMetaPrefix.length()));
}

inline auto make_row_binlog_key_prefix(const TabletUid& tablet_uid, const RowsetId& rowset_id) {
    return fmt::format("{}{}_{}_", kRowBinlogPrefix, tablet_uid.to_string(), rowset_id.to_string());
}

inline auto make_row_binlog_key(const TabletUid& tablet_uid, const RowsetId& rowset_id,
                                const RowsetId& binlog_rowset_id) {
    return fmt::format("{}{}_{}_{}", kRowBinlogPrefix, tablet_uid.to_string(),
                       rowset_id.to_string(), binlog_rowset_id.to_string());
}

// Allocate per-row LSNs for row-binlog data.
// The caller must provide a valid auto-inc buffer (typically from GlobalAutoIncBuffers).
inline Status allocate_binlog_lsn(const std::shared_ptr<AutoIncIDBuffer>& lsn_buffer,
                                  size_t num_rows, std::shared_ptr<std::vector<int64_t>>* lsn_ids) {
    if (lsn_buffer == nullptr) {
        return Status::InternalError("binlog<row> try to get lsn buffer but null");
    }
    DCHECK(lsn_ids != nullptr);
    DCHECK(num_rows > 0);

    std::vector<std::pair<int64_t, size_t>> ranges;
    RETURN_IF_ERROR(lsn_buffer->sync_request_ids(num_rows, &ranges));

    auto ids = std::make_shared<std::vector<int64_t>>();
    ids->reserve(num_rows);
    for (const auto& [start, length] : ranges) {
        for (size_t i = 0; i < length; ++i) {
            DCHECK_LE(start, std::numeric_limits<int64_t>::max() - static_cast<int64_t>(i));
            ids->push_back(start + static_cast<int64_t>(i));
        }
    }
    DCHECK_EQ(ids->size(), num_rows);
    *lsn_ids = std::move(ids);
    return Status::OK();
}

constexpr int64_t kTsoLogicalBits = 18;

inline int64_t extract_tso_physical_time(int64_t tso) {
    return tso <= 0 ? 0 : tso >> kTsoLogicalBits;
}

inline int128_t make_row_binlog_lsn(int64_t tso, int128_t row_id) {
    static constexpr int128_t kLow64Mask = (static_cast<int128_t>(1) << 64) - 1;
    return (static_cast<int128_t>(tso) << 64) | (row_id & kLow64Mask);
}

namespace segment_v2 {

class SegmentWriteBinlogLsnMap {
public:
    void insert_seg_lsn(int64_t seg_id, std::shared_ptr<std::vector<int64_t>> lsn_ids) {
        std::lock_guard<std::mutex> l(_mutex);
        _seg_id_to_lsn_ids.emplace(seg_id, std::move(lsn_ids));
    }

    void remove_seg(int64_t seg_id) {
        std::lock_guard<std::mutex> l(_mutex);
        _seg_id_to_lsn_ids.erase(seg_id);
    }

    std::shared_ptr<const std::vector<int64_t>> get_seg_lsn(int64_t seg_id) const {
        std::lock_guard<std::mutex> l(_mutex);
        auto it = _seg_id_to_lsn_ids.find(seg_id);
        CHECK(it != _seg_id_to_lsn_ids.end())
                << "SegmentWriteBinlogLsnMap::get_seg_lsn missing seg_id=" << seg_id
                << ", existing_seg_ids=[" << ([&] {
                       std::string s;
                       for (const auto& [id, _] : _seg_id_to_lsn_ids) {
                           if (!s.empty()) {
                               s.push_back(',');
                           }
                           s.append(std::to_string(id));
                       }
                       return s;
                   }())
                << "]";
        return it->second;
    }

private:
    mutable std::mutex _mutex;
    std::map<int64_t, std::shared_ptr<std::vector<int64_t>>> _seg_id_to_lsn_ids;
};

struct SegmentWriteBinlogOptions {
public:
    bool write_before = false;

    // source context, used for retrieving historical row and building binlog<row> block
    struct SourceWriteDataOptions {
        TabletSchemaSPtr tablet_schema = nullptr;
        std::shared_ptr<PartialUpdateInfo> partial_update_info;
        std::shared_ptr<MowContext> mow_context;
        bool is_transient_rowset_writer = false;
        DataWriteType source_write_type = DataWriteType::TYPE_DEFAULT;
    } source;

    void insert_seg_lsn(int64_t seg_id, std::shared_ptr<std::vector<int64_t>> lsn_ids) {
        DCHECK(lsn_map != nullptr);
        lsn_map->insert_seg_lsn(seg_id, std::move(lsn_ids));
    }

    void remove_seg(int64_t seg_id) {
        DCHECK(lsn_map != nullptr);
        lsn_map->remove_seg(seg_id);
    }

    std::shared_ptr<const std::vector<int64_t>> get_seg_lsn(int64_t seg_id) const {
        DCHECK(lsn_map != nullptr);
        return lsn_map->get_seg_lsn(seg_id);
    }

    // Shared LSN storage for row-binlog writers.
    // Keep it as a pointer so SegmentWriteBinlogOptions stays copyable.
    std::shared_ptr<SegmentWriteBinlogLsnMap> lsn_map =
            std::make_shared<SegmentWriteBinlogLsnMap>();
};
} // namespace segment_v2

} // namespace doris
