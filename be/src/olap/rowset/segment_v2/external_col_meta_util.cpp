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

#include "olap/rowset/segment_v2/external_col_meta_util.h"

#include <limits>
#include <memory>
#include <vector>

#include "io/io_common.h"
#include "olap/rowset/segment_v2/variant/variant_ext_meta_writer.h"

namespace doris::segment_v2 {

Status ExternalColMetaUtil::parse_external_meta_pointers(
        const SegmentFooterPB& footer, ExternalColMetaUtil::ExternalMetaPointers* out) {
    out->region_start = footer.col_meta_region_start();
    out->num_columns = static_cast<uint32_t>(footer.column_meta_entries_size());

    // Validate pointers and basic consistency to ensure external meta is actually present.
    if (out->region_start == 0) {
        return Status::Corruption("external meta region_start is 0");
    }
    if (out->num_columns == 0) {
        return Status::Corruption("external meta num_columns is 0");
    }

    // Compute region_end = region_start + sum(length_i) with overflow guards.
    uint64_t region_size = 0;
    for (int i = 0; i < footer.column_meta_entries_size(); ++i) {
        const auto& entry = footer.column_meta_entries(i);
        const auto len = static_cast<uint64_t>(entry.length());
        if (len == 0) {
            return Status::Corruption("column_meta_entries[{}] has zero length", i);
        }
        if (region_size + len < region_size) {
            return Status::Corruption("region_size overflow at column_meta_entries[{}]", i);
        }
        region_size += len;
    }

    out->region_end = out->region_start + region_size;
    if (out->region_end <= out->region_start) {
        return Status::Corruption("invalid region_end: start={}, end={}", out->region_start,
                                  out->region_end);
    }
    return Status::OK();
}

Status ExternalColMetaUtil::parse_uid_to_colid_map(
        const SegmentFooterPB& footer, const ExternalColMetaUtil::ExternalMetaPointers& ptrs,
        std::unordered_map<int32_t, size_t>* out) {
    out->clear();
    out->reserve(ptrs.num_columns);

    for (int i = 0; i < footer.column_meta_entries_size(); ++i) {
        const auto& entry = footer.column_meta_entries(i);
        // Skip subcolumns (unique_id == -1) for top-level map
        if (entry.unique_id() != -1) {
            (*out)[entry.unique_id()] = static_cast<size_t>(i);
        }
    }
    return Status::OK();
}

bool ExternalColMetaUtil::is_valid_meta_slice(uint64_t pos, uint64_t size,
                                              const ExternalColMetaUtil::ExternalMetaPointers& p) {
    if (size == 0) {
        return false;
    }
    if (pos < p.region_start) {
        return false;
    }
    if (pos + size < pos) {
        return false;
    } // overflow guard
    if (pos + size > p.region_end) {
        return false;
    }
    return true;
}

Status ExternalColMetaUtil::read_col_meta(const io::FileReaderSPtr& file_reader,
                                          const SegmentFooterPB& footer,
                                          const ExternalColMetaUtil::ExternalMetaPointers& p,
                                          uint32_t col_id, ColumnMetaPB* out_meta) {
    if (col_id >= p.num_columns) {
        return Status::Corruption("col_id {} out of range {}", col_id, p.num_columns);
    }
    if (footer.column_meta_entries_size() != p.num_columns) {
        return Status::Corruption("column_meta_entries size mismatch: entries={}, num_columns={}",
                                  footer.column_meta_entries_size(), p.num_columns);
    }
    // compute meta offset via prefix-sum of entry lengths.
    // TODO: prepare position and size in advance, and then read the meta.
    uint64_t pos = p.region_start;
    for (uint32_t i = 0; i < col_id; ++i) {
        pos += static_cast<uint64_t>(footer.column_meta_entries(i).length());
    }
    const auto size = static_cast<uint64_t>(footer.column_meta_entries(col_id).length());
    if (!ExternalColMetaUtil::is_valid_meta_slice(pos, size, p)) {
        return Status::Corruption(
                "invalid ColumnMetaPB bounds: col_id={}, pos={}, size={}, region_start={}, "
                "region_end={}",
                col_id, pos, size, p.region_start, p.region_end);
    }
    if (size > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        return Status::Corruption("ColumnMetaPB size too large to allocate: {}", size);
    }
    std::string buf;
    buf.resize(static_cast<size_t>(size));
    size_t meta_read = 0;
    io::IOContext io_ctx {.is_index_data = true};
    RETURN_IF_ERROR(file_reader->read_at(pos, Slice(buf.data(), buf.size()), &meta_read, &io_ctx));
    if (meta_read != size) {
        return Status::Corruption("short read ColumnMetaPB: expect={}, actual={}", size, meta_read);
    }
    if (!out_meta->ParseFromArray(buf.data(), static_cast<int>(buf.size()))) {
        return Status::Corruption("failed to parse ColumnMetaPB, col_id={}", col_id);
    }
    return Status::OK();
}

Status ExternalColMetaUtil::write_external_column_meta(
        io::FileWriter* file_writer, SegmentFooterPB* footer, CompressionTypePB compression_type,
        const std::function<Status(const std::vector<Slice>&)>& write_cb) {
    if (footer->columns_size() == 0) {
        return Status::OK();
    }

    std::vector<ColumnMetaPB> all_metas;
    // 0) Externalize variant subcolumns first, then proceed with per-column meta region.
    {
        auto variant_ext_meta_agg =
                std::make_unique<VariantExtMetaWriter>(file_writer, compression_type);
        RETURN_IF_ERROR(variant_ext_meta_agg->externalize_from_footer(footer, &all_metas));
    }

    // Use the reorganized metas (including subcolumns)
    const uint64_t meta_region_start = file_writer->bytes_appended();

    footer->clear_column_meta_entries();

    // 1) write contiguous meta blobs in col_id order and record lengths in footer.
    for (const auto& meta : all_metas) {
        std::string meta_bytes;
        if (!meta.SerializeToString(&meta_bytes)) {
            return Status::InternalError("failed to serialize column meta, unique_id={}",
                                         meta.unique_id());
        }
        Slice s(meta_bytes);
        RETURN_IF_ERROR(write_cb({s}));
        auto* entry = footer->add_column_meta_entries();
        entry->set_unique_id(static_cast<int32_t>(meta.unique_id()));
        entry->set_length(static_cast<uint32_t>(meta_bytes.size()));
    }

    // 2) write pointers via proto fields
    footer->set_col_meta_region_start(meta_region_start);

    // 3) clear inline columns to enable true on-demand meta loading
    // Note: footer->columns has already been pruned to only Top Level Columns in externalize_from_footer
    // But for full externalization, we might want to clear them all or keep only necessary info?
    // The original logic was footer->clear_columns().
    // If we clear columns, the Reader needs to know how to reconstruct the schema.
    // Currently, SegmentFooterPB.columns is used as the schema source if present.
    // If we clear it, Reader must rely on External Meta.
    // However, the Reader typically reads footer first. If columns is empty, it assumes V3 and reads external.
    // So yes, we should clear it.
    // But wait, in externalize_from_footer we carefully put Top Level columns back into footer->columns.
    // Why? Because in previous logic, we might want to keep roots in footer?
    // The previous logic: "replace Footer.columns with only the kept top-level columns".
    // BUT then `write_external_column_meta` calls `footer->clear_columns()` at the end!
    // So `footer->columns` will be empty anyway.
    // The only reason to reconstruct `footer->columns` in `externalize_from_footer` is if `write_external_column_meta` logic depended on it.
    // In my updated `write_external_column_meta`, I iterate over `all_metas` which is returned by `externalize_from_footer`.
    // So I don't strictly need `footer->columns` to be correct in between.
    // However, strictly following protocol: `externalize_from_footer` modifies footer to reflect "logical" columns (Top Level).
    // And then `write_external_column_meta` finalizes it by clearing them and setting pointers.

    footer->clear_columns();
    return Status::OK();
}

} // namespace doris::segment_v2
