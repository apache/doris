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

#include <gen_cpp/segment_v2.pb.h>

#include <memory>
#include <string>

#include "common/status.h"
#include "olap/rowset/segment_v2/external_col_meta_util.h"
#include "olap/rowset/segment_v2/indexed_column_reader.h"
#include "olap/rowset/segment_v2/stream_reader.h"
#include "olap/rowset/segment_v2/variant/variant_statistics.h"
#include "util/once.h"

namespace doris::segment_v2 {

#include "common/compile_check_begin.h"

// Encapsulates reading of externalized Variant subcolumn metas.
// It discovers key/value indexed-columns from SegmentFooterPB, supports:
// - availability check
// - lookup ColumnMetaPB by relative path
// - one-time bulk load of all metas into SubcolumnColumnMetaInfo and VariantStatistics
class VariantExternalMetaReader {
public:
    VariantExternalMetaReader() = default;

    // Initialize by locating and opening ext meta key/value indexed columns.
    // root_uid is used to find suffixed keys like: variant_meta_keys.<root_uid>
    //
    // NOTE: footer is kept as shared_ptr to ensure its lifetime is at least as
    // long as this reader; callers should pass in the same footer shared_ptr
    // they own (e.g. from Segment::_get_segment_footer).
    Status init_from_footer(std::shared_ptr<const SegmentFooterPB> footer,
                            const io::FileReaderSPtr& file_reader, int32_t root_uid);

    bool available() const { return _key_reader != nullptr; }

    // Lookup a single ColumnMetaPB by relative path. Returns NOT_FOUND if missing/unavailable.
    Status lookup_meta_by_path(const std::string& rel_path, ColumnMetaPB* out_meta) const;

    // Check whether there exists any key in external meta that starts with `prefix`.
    // This performs a lower_bound (seek_at_or_after) on the sorted key column
    // and verifies the first key is prefixed by `prefix`.
    Status has_prefix(const std::string& prefix, bool* out) const;

    // Ensure external metas are loaded exactly once and merged into provided structures.
    Status load_all_once(SubcolumnColumnMetaInfo* out_meta_tree, VariantStatistics* out_stats);

    // Load and merge all external metas without call-once guard.
    Status load_all(SubcolumnColumnMetaInfo* out_meta_tree, VariantStatistics* out_stats);

private:
    // helpers
    Status _find_key_meta(const SegmentFooterPB& footer, int32_t root_uid,
                          const MetadataPairPB** keys_meta_pair) const;

    std::unique_ptr<IndexedColumnReader> _key_reader;

    // Shared footer to access Column Meta Region info (column_meta_entries).
    // Using shared_ptr prevents dangling pointer when the original owner releases it.
    std::shared_ptr<const SegmentFooterPB> _footer;
    ExternalColMetaUtil::ExternalMetaPointers _meta_ptrs;
    io::FileReaderSPtr _file_reader;

    // Global column_id of the variant root in external Column Meta Region.
    // For its non-sparse subcolumns, their column_id is derived as:
    //   root_col_id + 1 + key_ordinal
    uint32_t _root_col_id = 0;

    // call-once guard for bulk loading
    DorisCallOnce<Status> _load_once_call;
    bool _loaded = false;
};

#include "common/compile_check_end.h"

} // namespace doris::segment_v2
