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
#include <utility>
#include <vector>

#include "io/io_common.h"
#include "olap/rowset/segment_v2/variant/variant_ext_meta_writer.h"
#include "util/coding.h"
#include "util/faststring.h"

namespace doris::segment_v2 {

bool ExternalColMetaUtil::parse_external_meta_pointers(
        const SegmentFooterPB& footer, ExternalColMetaUtil::ExternalMetaPointers* out) {
    out->region_start = footer.col_meta_region_start();
    out->cmo_offset = footer.column_meta_offsets_start();
    out->num_columns = footer.num_columns();
    // Validate pointers to ensure external meta is actually present
    if (out->num_columns == 0) {
        return false;
    }
    if (out->cmo_offset <= out->region_start) {
        return false;
    }
    return true;
}

bool ExternalColMetaUtil::parse_uid_to_colid_map(
        const SegmentFooterPB& footer, const ExternalColMetaUtil::ExternalMetaPointers& ptrs,
        std::unordered_map<int32_t, size_t>* out) {
    const auto& uids = footer.top_level_column_uids();
    if (uids.size() != ptrs.num_columns) {
        return false;
    }
    out->reserve(ptrs.num_columns);
    for (int i = 0; i < uids.size(); ++i) {
        (*out)[uids.Get(i)] = static_cast<size_t>(i);
    }
    return true;
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
    if (pos + size > p.cmo_offset) {
        return false;
    }
    return true;
}

Status ExternalColMetaUtil::read_col_meta_from_cmo(
        const io::FileReaderSPtr& file_reader, const ExternalColMetaUtil::ExternalMetaPointers& p,
        uint32_t col_id, ColumnMetaPB* out_meta) {
    if (col_id >= p.num_columns) {
        return Status::Corruption("col_id {} out of range {}", col_id, p.num_columns);
    }
    uint8_t entry[16];
    size_t bytes_read = 0;
    const uint64_t cmo_entry_off = p.cmo_offset + static_cast<uint64_t>(col_id) * 16ULL;
    io::IOContext io_ctx {.is_index_data = true};
    RETURN_IF_ERROR(
            file_reader->read_at(cmo_entry_off, Slice(entry, sizeof(entry)), &bytes_read, &io_ctx));
    if (bytes_read != sizeof(entry)) {
        return Status::Corruption("bad CMO entry read: col_id={}, read={}", col_id, bytes_read);
    }
    const uint64_t pos = decode_fixed64_le(entry);
    const uint64_t size = decode_fixed64_le(entry + 8);
    if (!ExternalColMetaUtil::is_valid_meta_slice(pos, size, p)) {
        return Status::Corruption(
                "invalid ColumnMetaPB bounds: col_id={}, pos={}, size={}, region_start={}, "
                "cmo_offset={}",
                col_id, pos, size, p.region_start, p.cmo_offset);
    }
    if (size > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        return Status::Corruption("ColumnMetaPB size too large to allocate: {}", size);
    }
    std::string buf;
    buf.resize(static_cast<size_t>(size));
    size_t meta_read = 0;
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
        bool enabled, io::FileWriter* file_writer, SegmentFooterPB* footer,
        CompressionTypePB compression_type,
        const std::function<Status(const std::vector<Slice>&)>& write_cb) {
    if (!enabled) {
        return Status::OK();
    }
    if (footer->columns_size() == 0) {
        return Status::OK();
    }

    // 0) Externalize variant subcolumns first, then proceed with per-column meta region.
    {
        auto variant_ext_meta_agg =
                std::make_unique<VariantExtMetaWriter>(file_writer, compression_type);
        RETURN_IF_ERROR(variant_ext_meta_agg->externalize_from_footer(footer));
    }

    const auto ncols = static_cast<uint32_t>(footer->columns_size());
    const uint64_t meta_region_start = file_writer->bytes_appended();

    std::vector<std::pair<uint64_t, uint64_t>> cmo_entries;
    cmo_entries.reserve(ncols);

    // 1) write contiguous meta blobs in col_id order
    for (int i = 0; i < footer->columns_size(); ++i) {
        std::string meta_bytes;
        if (!footer->columns(i).SerializeToString(&meta_bytes)) {
            return Status::InternalError("failed to serialize column meta, col_id={}", i);
        }
        const uint64_t pos = file_writer->bytes_appended();
        Slice s(meta_bytes);
        RETURN_IF_ERROR(write_cb({s}));
        cmo_entries.emplace_back(pos, static_cast<uint64_t>(meta_bytes.size()));
    }

    // 2) write CMO entries
    const uint64_t cmo_offset = file_writer->bytes_appended();
    faststring cmo_buf;
    cmo_buf.reserve(static_cast<size_t>(cmo_entries.size() * 16));
    for (auto& e : cmo_entries) {
        put_fixed64_le(&cmo_buf, e.first);
        put_fixed64_le(&cmo_buf, e.second);
    }
    RETURN_IF_ERROR(write_cb({Slice(cmo_buf)}));

    // 3) write pointers via proto fields
    footer->set_col_meta_region_start(meta_region_start);
    footer->set_column_meta_offsets_start(cmo_offset);
    footer->set_num_columns(ncols);
    // 4) write uid->col_id mapping into proto field (top_level_column_uids in col_id order)
    footer->clear_top_level_column_uids();
    for (int i = 0; i < footer->columns_size(); ++i) {
        footer->add_top_level_column_uids(static_cast<int32_t>(footer->columns(i).unique_id()));
    }
    // 5) clear inline columns to enable true on-demand meta loading
    footer->clear_columns();
    return Status::OK();
}

} // namespace doris::segment_v2
