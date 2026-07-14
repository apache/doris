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

#include "storage/index/snii/writer/snii_compound_writer.h"

#include <utility>

#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/format/bootstrap_header.h"
#include "storage/index/snii/format/per_index_meta.h" // SectionRefs
#include "storage/index/snii/format/tail_meta_region.h"
#include "storage/index/snii/format/tail_pointer.h"

namespace doris::snii::writer {

using format::BootstrapHeader;
using format::SectionRefs;
using format::TailMetaRegionBuilder;
using format::TailPointer;

SniiCompoundWriter::SniiCompoundWriter(io::FileWriter* out) : out_(out) {}

Status SniiCompoundWriter::append(const std::vector<uint8_t>& bytes) {
    if (bytes.empty()) return Status::OK();
    return out_->append(Slice(bytes));
}

// The bootstrap header occupies offset 0 and must precede the first posting region,
// which streams straight into the output during build(). Written lazily exactly once
// (on the first add, or in finish() for an empty container).
Status SniiCompoundWriter::ensure_bootstrap() {
    if (bootstrap_written_) return Status::OK();
    bootstrap_written_ = true;
    return write_bootstrap();
}

Status SniiCompoundWriter::add_logical_index(const SniiIndexInput& in) {
    if (out_ == nullptr)
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("compound: null file writer");
    if (finished_)
        return Status::Error<ErrorCode::INTERNAL_ERROR, false>("compound: add after finish");
    RETURN_IF_ERROR(ensure_bootstrap());
    auto liw = std::make_unique<LogicalIndexWriter>(in);
    Placement p;
    // The posting region streams DIRECTLY into the container during build() -- no temp
    // round-trip for the bulk -- followed immediately by this index's compact DICT
    // trailer (produced interleaved into a temp, but laid out right after its posting
    // region, preserving the per-index [posting][dict] layout). Offsets are read off
    // the output writer (the single source of truth -- no separate cursor).
    p.post_off = out_->bytes_written();
    RETURN_IF_ERROR(liw->build(out_));
    p.post_len = out_->bytes_written() - p.post_off;
    p.dict_off = out_->bytes_written();
    RETURN_IF_ERROR(liw->stream_dict_region_into(out_));
    p.dict_len = out_->bytes_written() - p.dict_off;
    indexes_.push_back(std::move(liw));
    placements_.push_back(p);
    return Status::OK();
}

Status SniiCompoundWriter::write_bootstrap() {
    BootstrapHeader bh;
    bh.tail_pointer_size = static_cast<uint8_t>(format::tail_pointer_size());
    ByteSink sink;
    RETURN_IF_ERROR(format::encode_bootstrap_header(bh, &sink));
    return append(sink.buffer());
}

// Writes each index's norms POD then bsbf section (in add order), after all the
// per-index [posting][dict] regions.
Status SniiCompoundWriter::write_norms() {
    for (size_t i = 0; i < indexes_.size(); ++i) {
        const LogicalIndexWriter& w = *indexes_[i];
        if (!w.has_norms() || w.norms_bytes().empty()) continue;
        Placement& p = placements_[i];
        p.norms_off = out_->bytes_written();
        RETURN_IF_ERROR(append(w.norms_bytes()));
        p.norms_len = out_->bytes_written() - p.norms_off;
    }
    for (size_t i = 0; i < indexes_.size(); ++i) {
        const LogicalIndexWriter& w = *indexes_[i];
        if (!w.has_null_bitmap()) continue;
        Placement& p = placements_[i];
        p.null_off = out_->bytes_written();
        RETURN_IF_ERROR(append(w.null_bitmap_bytes()));
        p.null_len = out_->bytes_written() - p.null_off;
    }
    for (size_t i = 0; i < indexes_.size(); ++i) {
        const LogicalIndexWriter& w = *indexes_[i];
        if (!w.has_bsbf()) continue;
        Placement& p = placements_[i];
        p.bsbf_off = out_->bytes_written();
        RETURN_IF_ERROR(append(w.bsbf_bytes()));
        p.bsbf_len = out_->bytes_written() - p.bsbf_off;
    }
    return Status::OK();
}

Status SniiCompoundWriter::write_tail() {
    TailMetaRegionBuilder region;
    for (size_t i = 0; i < indexes_.size(); ++i) {
        const LogicalIndexWriter& w = *indexes_[i];
        const Placement& p = placements_[i];

        SectionRefs refs;
        refs.dict_region = {p.dict_off, p.dict_len};
        refs.posting_region = {p.post_off, p.post_len};
        refs.norms = {p.norms_off, p.norms_len};
        refs.null_bitmap = {p.null_off, p.null_len};
        refs.bsbf = {p.bsbf_off, p.bsbf_len};

        ByteSink meta;
        RETURN_IF_ERROR(w.finish_meta(refs, p.dict_off, &meta));
        region.add_index(w.index_id(), w.index_suffix(), meta.view());
    }

    ByteSink region_sink;
    region.finish(&region_sink);
    const uint64_t region_off = out_->bytes_written();
    RETURN_IF_ERROR(append(region_sink.buffer()));
    const uint64_t region_len = out_->bytes_written() - region_off;

    TailPointer tp;
    tp.meta_region_offset = region_off;
    tp.meta_region_length = region_len;
    tp.hot_off = 0;
    tp.meta_region_checksum = crc32c(region_sink.view());
    // Reserved: the bootstrap header carries (and decode_bootstrap_header verifies) its
    // OWN internal crc32c, so a tail-pointer copy is redundant. Left 0 until a cross-
    // region check needs it; the tail pointer's own tail_checksum still covers this
    // field's bytes.
    tp.bootstrap_header_checksum = 0;
    ByteSink tail_sink;
    RETURN_IF_ERROR(format::encode_tail_pointer(tp, &tail_sink));
    return append(tail_sink.buffer());
}

Status SniiCompoundWriter::finish() {
    if (out_ == nullptr)
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("compound: null file writer");
    if (finished_)
        return Status::Error<ErrorCode::INTERNAL_ERROR, false>("compound: finish called twice");
    finished_ = true;

    RETURN_IF_ERROR(ensure_bootstrap()); // empty container still gets a header
    RETURN_IF_ERROR(write_norms());
    RETURN_IF_ERROR(write_tail());
    return out_->finalize();
}

} // namespace doris::snii::writer
