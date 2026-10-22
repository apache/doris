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
#include "storage/olap_define.h" // DataWriteType
#include "storage/rowset/rowset_fwd.h"
#include "storage/tablet/tablet_fwd.h"    // BaseTabletSPtr
#include "storage/tablet/tablet_schema.h" // TabletSchemaSPtr
#include "storage/utils.h"                // CHANGELOG_*_COL

namespace doris {

class AutoIncIDBuffer;
struct PartialUpdateInfo;
struct MowContext;

// Changelog op type.
// NOTE: The value is persisted into changelog data, so keep it stable.
static constexpr int64_t CHANGELOG_OP_APPEND = 0;
static constexpr int64_t CHANGELOG_OP_UPDATE = 1;
static constexpr int64_t CHANGELOG_OP_DELETE = 2;

namespace changelog {

// ATTN(changelog-rename): formerly "__BEFORE__<col>__"
inline std::string build_before_column_name(std::string_view name) {
    std::string before_name = "__DORIS_CHANGELOG_BEFORE_";
    before_name.append(name.data(), name.size());
    before_name.append("__");
    return before_name;
}

} // namespace changelog

constexpr int64_t kChangelogLsnAutoIncId = -1;

// Allocate per-row LSNs.
// The caller must provide a valid auto-inc buffer (typically from GlobalAutoIncBuffers).
inline Status allocate_lsn(const std::shared_ptr<AutoIncIDBuffer>& lsn_buffer, size_t num_rows,
                           std::vector<int64_t>& lsn_ids) {
    if (lsn_buffer == nullptr) {
        return Status::InternalError("try to get lsn buffer but null");
    }
    DCHECK(num_rows > 0);

    std::vector<std::pair<int64_t, size_t>> ranges;
    RETURN_IF_ERROR(lsn_buffer->sync_request_ids(num_rows, &ranges));

    lsn_ids.clear();
    lsn_ids.reserve(num_rows);
    for (const auto& [start, length] : ranges) {
        for (size_t i = 0; i < length; ++i) {
            DCHECK_LE(start, std::numeric_limits<int64_t>::max() - static_cast<int64_t>(i));
            lsn_ids.push_back(start + static_cast<int64_t>(i));
        }
    }
    DCHECK_EQ(lsn_ids.size(), num_rows);
    return Status::OK();
}

constexpr int64_t kTsoLogicalBits = 18;

inline int64_t extract_tso_physical_time(int64_t tso) {
    return tso <= 0 ? 0 : tso >> kTsoLogicalBits;
}

inline int64_t changelog_ttl_cutoff_tso(int64_t reference_tso, int64_t ttl_seconds) {
    DCHECK_GT(reference_tso, 0);
    DCHECK_GE(ttl_seconds, 0);
    const int64_t reference_ms = extract_tso_physical_time(reference_tso);
    if (ttl_seconds > reference_ms / 1000) {
        return 0;
    }
    constexpr int64_t kMaxLogical = (1L << kTsoLogicalBits) - 1;
    return ((reference_ms - ttl_seconds * 1000) << kTsoLogicalBits) | kMaxLogical;
}

namespace segment_v2 {

class SegmentAllocatedLsnMap {
public:
    void insert_segment_allocated_lsns(int64_t seg_id, ConstAllocatedLsnVectorSharedPtr lsn_ids) {
        std::lock_guard<std::mutex> l(_mutex);
        _seg_id_to_lsn_ids.emplace(seg_id, std::move(lsn_ids));
    }

    void remove_segment(int64_t seg_id) {
        std::lock_guard<std::mutex> l(_mutex);
        _seg_id_to_lsn_ids.erase(seg_id);
    }

    bool contains_segment(int64_t seg_id) const {
        std::lock_guard<std::mutex> l(_mutex);
        return _seg_id_to_lsn_ids.count(seg_id) > 0;
    }

    ConstAllocatedLsnVectorSharedPtr get_segment_allocated_lsns(int64_t seg_id) const {
        std::lock_guard<std::mutex> l(_mutex);
        auto it = _seg_id_to_lsn_ids.find(seg_id);
        DCHECK(it != _seg_id_to_lsn_ids.end())
                << "SegmentAllocatedLsnMap::get_segment_allocated_lsns missing seg_id=" << seg_id
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
    std::map<int64_t, ConstAllocatedLsnVectorSharedPtr> _seg_id_to_lsn_ids;
};

struct SegmentWriteChangelogOptions {
public:
    bool write_before = false;

    // source context, used for retrieving historical row and building changelog block
    struct SourceWriteDataOptions {
        BaseTabletSPtr base_tablet = nullptr;
        TabletSchemaSPtr tablet_schema = nullptr;
        std::shared_ptr<PartialUpdateInfo> partial_update_info;
        std::shared_ptr<MowContext> mow_context;
        bool is_transient_rowset_writer = false;
        DataWriteType source_write_type = DataWriteType::TYPE_DEFAULT;
        // input rowset's changelog rowset, read LSN from it at publish time
        RowsetSharedPtr changelog_rowset = nullptr;
    } source;
};
} // namespace segment_v2

} // namespace doris
