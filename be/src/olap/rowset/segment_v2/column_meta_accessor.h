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

#include <functional>
#include <memory>
#include <unordered_map>

#include "common/status.h"
#include "gen_cpp/segment_v2.pb.h"
#include "io/fs/file_reader.h"

namespace doris::segment_v2 {

// Centralized accessor for column metadata layout of a Segment.
// It selects V2 (inline footer.columns) or V3 (external Column Meta Region)
// implementation internally and exposes a unified API to callers, hiding
// the physical layout (inline vs external CMO).
class ColumnMetaAccessor {
public:
    class Impl;
    ColumnMetaAccessor();
    ~ColumnMetaAccessor();
    ColumnMetaAccessor(ColumnMetaAccessor&&) noexcept;
    ColumnMetaAccessor& operator=(ColumnMetaAccessor&&) noexcept;
    ColumnMetaAccessor(const ColumnMetaAccessor&) = delete;
    ColumnMetaAccessor& operator=(const ColumnMetaAccessor&) = delete;
    // Initialize accessor and choose V2 / V3 implementation based on footer.
    //
    // The caller is responsible for ensuring that `footer` outlives this accessor.
    Status init(const SegmentFooterPB& footer, io::FileReaderSPtr file_reader);

    // Get ColumnMetaPB for the given column ordinal id using the provided footer.
    //
    // Semantics:
    // - In legacy V2 segments, column_ordinal_id is the index into footer.columns().
    // - In V3 segments with external CMO, column_ordinal_id indexes the external
    //   Column Meta Region in column order; when external meta is missing or
    //   we fall back to inline columns, the same logical ordinal is reused.
    //
    // Error convention:
    // - If the ordinal is out of range or the corresponding meta is missing/corrupted,
    //   returns ErrorCode::CORRUPTION.
    Status get_column_meta_by_column_ordinal_id(const SegmentFooterPB& footer,
                                                uint32_t column_ordinal_id,
                                                ColumnMetaPB* out) const;

    // Get ColumnMetaPB for the given column unique id using the provided footer.
    // column_uid is TabletColumn.unique_id / ColumnMetaPB.unique_id.
    //
    // Mapping scope:
    // - Only top-level physical columns (including Variant root columns) are indexed.
    // - Variant extracted subcolumns (unique_id == -1) and path subcolumns are excluded.
    //
    // Error convention:
    // - If the uid does not exist in this segment (no physical column for this uid),
    //   returns ErrorCode::NOT_FOUND.
    Status get_column_meta_by_uid(const SegmentFooterPB& footer, int32_t column_uid,
                                  ColumnMetaPB* out) const;

    // Fast existence check for a given column unique id.
    //
    // Only checks the in-memory uid -> ordinal map; does not trigger any I/O and
    // does not parse ColumnMetaPB. Semantics are consistent with
    // get_column_meta_by_uid(...) in terms of uid presence (NOT_FOUND vs OK).
    bool has_column_uid(int32_t column_uid) const;

    // Traverse all ColumnMetaPBs using the provided footer.
    Status traverse_metas(const SegmentFooterPB& footer,
                          const std::function<void(const ColumnMetaPB&)>& visitor) const;

private:
    std::unique_ptr<Impl> _impl;
    // map column unique id ---> column ordinal id (footer ordinal in legacy mode, or external CMO column id)
    std::unordered_map<int32_t, size_t> _column_uid_to_column_ordinal;
};

} // namespace doris::segment_v2
