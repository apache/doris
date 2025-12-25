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

#include "olap/rowset/segment_v2/column_meta_accessor.h"

#include <cstdint>
#include <string>

#include "io/io_common.h"
#include "olap/rowset/segment_v2/external_col_meta_util.h"
#include "olap/rowset/segment_v2/segment.h" // SEGMENT_FOOTER_VERSION_V3_EXT_COL_META

namespace doris::segment_v2 {

// Abstract base implementation shared by both V2 and V3 layouts.
class ColumnMetaAccessor::Impl {
public:
    virtual ~Impl() = default;
    // Build uid -> col_id mapping based on the provided footer.
    virtual Status build_uid_to_colid_map(
            const SegmentFooterPB& footer,
            std::unordered_map<int32_t, size_t>* uid_to_colid) const = 0;
    // Lookup ColumnMetaPB by logical ordinal id using the provided footer.
    virtual Status get_column_meta_by_column_ordinal_id(const SegmentFooterPB& footer,
                                                        uint32_t column_ordinal_id,
                                                        ColumnMetaPB* out) const = 0;
    // Traverse all ColumnMetaPBs using the provided footer.
    virtual Status traverse_metas(
            const SegmentFooterPB& footer,
            const std::function<void(const ColumnMetaPB&)>& visitor) const = 0;
};

// V2: inline layout using only footer.columns().
class ColumnMetaAccessorV2 : public ColumnMetaAccessor::Impl {
public:
    explicit ColumnMetaAccessorV2(io::FileReaderSPtr file_reader)
            : _file_reader(std::move(file_reader)) {}

    Status build_uid_to_colid_map(
            const SegmentFooterPB& footer,
            std::unordered_map<int32_t, size_t>* uid_to_colid) const override {
        uid_to_colid->clear();
        uint32_t ordinal = 0;
        for (const auto& column_meta : footer.columns()) {
            // 跳过 variant 子列（unique_id == -1）
            if (column_meta.unique_id() == -1) {
                ordinal++;
                continue;
            }
            uid_to_colid->try_emplace(column_meta.unique_id(), ordinal++);
        }
        return Status::OK();
    }

    Status get_column_meta_by_column_ordinal_id(const SegmentFooterPB& footer,
                                                uint32_t column_ordinal_id,
                                                ColumnMetaPB* out) const override {
        if (footer.columns_size() == 0 ||
            column_ordinal_id >= static_cast<uint32_t>(footer.columns_size())) {
            return Status::Corruption(
                    "no inline column meta available for column_id={} (columns_size={})",
                    column_ordinal_id, footer.columns_size());
        }
        *out = footer.columns(static_cast<int>(column_ordinal_id));
        return Status::OK();
    }

    Status traverse_metas(const SegmentFooterPB& footer,
                          const std::function<void(const ColumnMetaPB&)>& visitor) const override {
        if (footer.columns_size() == 0) {
            return Status::Corruption("no column meta found in footer (inline V2)");
        }
        for (const auto& column : footer.columns()) {
            visitor(column);
        }
        return Status::OK();
    }

private:
    io::FileReaderSPtr _file_reader;
};

// V3: use external Column Meta Region + column_meta_entries layout.
class ColumnMetaAccessorV3 : public ColumnMetaAccessor::Impl {
public:
    ColumnMetaAccessorV3(io::FileReaderSPtr file_reader,
                         const ExternalColMetaUtil::ExternalMetaPointers& ptrs)
            : _file_reader(std::move(file_reader)), _ptrs(ptrs) {}

    Status build_uid_to_colid_map(
            const SegmentFooterPB& footer,
            std::unordered_map<int32_t, size_t>* uid_to_colid) const override {
        uid_to_colid->clear();
        return ExternalColMetaUtil::parse_uid_to_colid_map(footer, _ptrs, uid_to_colid);
    }

    Status get_column_meta_by_column_ordinal_id(const SegmentFooterPB& footer,
                                                uint32_t column_ordinal_id,
                                                ColumnMetaPB* out) const override {
        // Prefer external Column Meta Region first.
        if (column_ordinal_id < _ptrs.num_columns) {
            return ExternalColMetaUtil::read_col_meta(_file_reader, footer, _ptrs,
                                                      column_ordinal_id, out);
        }
        return Status::Corruption(
                "no column meta available for column_id={} (inline/external missing)",
                column_ordinal_id);
    }

    Status traverse_metas(const SegmentFooterPB& footer,
                          const std::function<void(const ColumnMetaPB&)>& visitor) const override {
        const uint64_t region_size = (_ptrs.region_end > _ptrs.region_start)
                                             ? (_ptrs.region_end - _ptrs.region_start)
                                             : 0;
        if (region_size == 0) {
            return Status::Corruption("invalid external meta region size");
        }

        // Read entire meta region once to reduce random I/O.
        std::string region_buf;
        region_buf.resize(static_cast<size_t>(region_size));
        size_t br = 0;
        io::IOContext io_ctx {.is_index_data = true};
        RETURN_IF_ERROR(_file_reader->read_at(_ptrs.region_start, Slice(region_buf), &br, &io_ctx));
        if (br != region_size) {
            return Status::Corruption("short read on meta region");
        }

        if (footer.column_meta_entries_size() != _ptrs.num_columns) {
            return Status::Corruption("column_meta_entries size mismatch");
        }

        uint64_t offset = _ptrs.region_start;
        for (uint32_t i = 0; i < _ptrs.num_columns; ++i) {
            const auto& entry = footer.column_meta_entries(static_cast<int>(i));
            const auto sz = static_cast<uint64_t>(entry.length());
            const uint64_t pos = offset;
            if (!ExternalColMetaUtil::is_valid_meta_slice(pos, sz, _ptrs)) {
                return Status::Corruption("external meta entry out of region bounds");
            }
            const uint64_t rel = pos - _ptrs.region_start;
            ColumnMetaPB meta;
            if (!meta.ParseFromArray(region_buf.data() + rel, static_cast<int>(sz))) {
                return Status::Corruption("failed parse ColumnMetaPB from region");
            }
            visitor(meta);
            offset += sz;
        }
        return Status::OK();
    }

private:
    io::FileReaderSPtr _file_reader;
    ExternalColMetaUtil::ExternalMetaPointers _ptrs;
};

ColumnMetaAccessor::ColumnMetaAccessor() = default;
ColumnMetaAccessor::~ColumnMetaAccessor() = default;

Status ColumnMetaAccessor::init(const SegmentFooterPB& footer, io::FileReaderSPtr file_reader) {
    // First check footer version to see if external Column Meta Region might exist,
    // then try to parse external layout; if parsing fails, fall back to V2.
    if (footer.version() >= SEGMENT_FOOTER_VERSION_V3_EXT_COL_META) {
        ExternalColMetaUtil::ExternalMetaPointers ptrs;
        RETURN_IF_ERROR(ExternalColMetaUtil::parse_external_meta_pointers(footer, &ptrs));
        _impl = std::make_unique<ColumnMetaAccessorV3>(std::move(file_reader), ptrs);
    } else {
        _impl = std::make_unique<ColumnMetaAccessorV2>(std::move(file_reader));
    }
    // Build inner uid -> column ordinal id mapping once during init.
    _column_uid_to_column_ordinal.clear();
    RETURN_IF_ERROR(_impl->build_uid_to_colid_map(footer, &_column_uid_to_column_ordinal));
    return Status::OK();
}

Status ColumnMetaAccessor::get_column_meta_by_column_ordinal_id(const SegmentFooterPB& footer,
                                                                uint32_t column_ordinal_id,
                                                                ColumnMetaPB* out) const {
    return _impl->get_column_meta_by_column_ordinal_id(footer, column_ordinal_id, out);
}

Status ColumnMetaAccessor::get_column_meta_by_uid(const SegmentFooterPB& footer, int32_t column_uid,
                                                  ColumnMetaPB* out) const {
    auto it = _column_uid_to_column_ordinal.find(column_uid);
    if (it == _column_uid_to_column_ordinal.end()) {
        *out = ColumnMetaPB();
        return Status::Error<ErrorCode::NOT_FOUND, false>(
                "column not found in segment meta, col_uid={}", column_uid);
    }
    return _impl->get_column_meta_by_column_ordinal_id(footer, static_cast<uint32_t>(it->second),
                                                       out);
}

bool ColumnMetaAccessor::has_column_uid(int32_t column_uid) const {
    return _column_uid_to_column_ordinal.contains(column_uid);
}

Status ColumnMetaAccessor::traverse_metas(
        const SegmentFooterPB& footer,
        const std::function<void(const ColumnMetaPB&)>& visitor) const {
    return _impl->traverse_metas(footer, visitor);
}

} // namespace doris::segment_v2
