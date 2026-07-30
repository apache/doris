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

#include <algorithm>
#include <utility>

#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/format/bootstrap_header.h"
#include "storage/index/snii/format/core_metadata.h"
#include "storage/index/snii/format/metadata_directory.h"
#include "storage/index/snii/format/tail_pointer.h"
#include "storage/index/snii/reader/snii_segment_reader.h"

namespace doris::snii::writer {

using format::BootstrapHeader;
using format::LogicalIndexMetadataRef;
using format::SectionRefs;
using format::TailPointer;

SniiCompoundWriter::SniiCompoundWriter(io::FileWriter* out) : out_(out) {}

Status SniiCompoundWriter::poison(Status status) {
    DCHECK(!status.ok());
    if (failed_.ok()) {
        failed_ = std::move(status);
    }
    return failed_;
}

Status SniiCompoundWriter::append(const std::vector<uint8_t>& bytes) {
    if (bytes.empty()) return Status::OK();
    return out_->append(Slice(bytes));
}

// The bootstrap header occupies offset 0 and must precede the first posting region,
// which streams straight into the output during build(). Written lazily exactly once
// (on the first add, or in finish() for an empty container).
Status SniiCompoundWriter::ensure_bootstrap() {
    if (!failed_.ok()) return failed_;
    if (bootstrap_written_) return Status::OK();
    const Status status = write_bootstrap();
    if (!status.ok()) return poison(status);
    bootstrap_written_ = true;
    return Status::OK();
}

Status SniiCompoundWriter::inherit(const reader::SniiRewriteSnapshot& snapshot,
                                   io::FileReader* source) {
    if (out_ == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("compound: null file writer");
    }
    if (source == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("compound: null inherit source");
    }
    if (!failed_.ok()) {
        return failed_;
    }
    // Every rejection below poisons the writer. The caller asked for a container
    // holding the inherited indexes; sealing one without them would silently drop
    // logical indexes the target schema requires.
    if (finished_) {
        return poison(
                Status::Error<ErrorCode::INTERNAL_ERROR, false>("compound: inherit after finish"));
    }
    if (inherited_prefix_) {
        return poison(Status::Error<ErrorCode::INTERNAL_ERROR, false>(
                "compound: inherit called twice; one source prefix is copied at most once"));
    }
    if (bootstrap_written_ || out_->bytes_written() != 0) {
        return poison(Status::Error<ErrorCode::INTERNAL_ERROR, false>(
                "compound: inherit must be the first data operation; the copied prefix owns the "
                "front of the container"));
    }

    // Sequential copy through a fixed-size buffer: peak memory is one chunk no
    // matter how large the source container is.
    const uint64_t prefix_end = snapshot.physical_prefix_end();
    std::vector<uint8_t> chunk;
    while (out_->bytes_written() < prefix_end) {
        const auto chunk_size = static_cast<size_t>(
                std::min<uint64_t>(kInheritCopyChunkBytes, prefix_end - out_->bytes_written()));
        Status status = source->read_at(out_->bytes_written(), chunk_size, &chunk);
        if (!status.ok()) {
            return poison(status);
        }
        DORIS_CHECK_EQ(chunk.size(), chunk_size);
        status = append(chunk);
        if (!status.ok()) {
            return poison(status);
        }
    }
    // The copied prefix starts with the source's bootstrap header, which the
    // snapshot validated, so the container must not get a second one.
    bootstrap_written_ = true;
    inherited_prefix_ = true;

    inherited_.reserve(snapshot.inherited().size());
    for (const reader::InheritedLogicalIndex& index : snapshot.inherited()) {
        InheritedGroup group;
        group.index_id = index.index_id;
        group.index_suffix = index.index_suffix;
        group.metadata_group = index.metadata_group;
        group.core_length = index.core_length;
        group.sampled_term_index_length = index.sampled_term_index_length;
        group.dict_block_directory_length = index.dict_block_directory_length;
        DORIS_CHECK_EQ(group.metadata_group.size(), group.core_length +
                                                            group.sampled_term_index_length +
                                                            group.dict_block_directory_length);
        inherited_.push_back(std::move(group));
    }
    return Status::OK();
}

Status SniiCompoundWriter::add_logical_index(const SniiIndexInput& in) {
    if (out_ == nullptr)
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("compound: null file writer");
    if (finished_)
        return Status::Error<ErrorCode::INTERNAL_ERROR, false>("compound: add after finish");
    if (!failed_.ok()) return failed_;
    if (has_active_session())
        return Status::Error<ErrorCode::INTERNAL_ERROR, false>(
                "compound: add_logical_index while a streamed index session is active (its "
                "posting region streams straight into the output; interleaving would corrupt "
                "both indexes)");
    for (const InheritedGroup& group : inherited_) {
        if (group.index_id == in.index_id && group.index_suffix == in.index_suffix) {
            return poison(Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "compound: new logical index reuses an inherited key; the final directory "
                    "must hold each key exactly once"));
        }
    }
    RETURN_IF_ERROR(ensure_bootstrap());
    auto liw = std::make_unique<LogicalIndexWriter>(in);
    Placement p;
    // The posting region streams DIRECTLY into the container during build() -- no temp
    // round-trip for the bulk -- followed immediately by this index's compact DICT
    // trailer (produced interleaved into a temp, but laid out right after its posting
    // region, preserving the per-index [posting][dict] layout). Offsets are read off
    // the output writer (the single source of truth -- no separate cursor).
    p.post_off = out_->bytes_written();
    Status status = liw->build(out_);
    if (!status.ok()) return poison(status);
    p.post_len = out_->bytes_written() - p.post_off;
    p.dict_off = out_->bytes_written();
    status = liw->stream_dict_region_into(out_);
    if (!status.ok()) return poison(status);
    p.dict_len = out_->bytes_written() - p.dict_off;
    indexes_.push_back(std::move(liw));
    placements_.push_back(p);
    return Status::OK();
}

SniiIndexInput SniiStreamedIndexSession::attach_encoded_norms(SniiIndexInput in,
                                                              TrackedEncodedNorms* encoded_norms,
                                                              uint64_t reserved_bytes) {
    DORIS_CHECK(encoded_norms != nullptr);
    DORIS_CHECK(in.encoded_norms.empty());
    if (in.mem_reporter != nullptr) {
        DORIS_CHECK_EQ(reserved_bytes, encoded_norms->norms_.capacity());
    } else {
        DORIS_CHECK_EQ(reserved_bytes, 0);
    }
    in.encoded_norms = std::move(encoded_norms->norms_);
    return in;
}

SniiStreamedIndexSession::SniiStreamedIndexSession(SniiCompoundWriter* owner, SniiIndexInput in,
                                                   TrackedNullDocids null_docids,
                                                   TrackedEncodedNorms encoded_norms)
        : owner_(owner),
          encoded_norms_reservation_(std::move(encoded_norms.reservation_)),
          input_(attach_encoded_norms(std::move(in), &encoded_norms,
                                      encoded_norms_reservation_.bytes())),
          // input_ (a member, initialized above) owns the vectors the writer
          // keeps references into -- NOT the caller's already-moved-from `in`.
          writer_(new LogicalIndexWriter(input_, std::move(null_docids))),
          semantic_token_count_required_(
                  input_.common_grams_metadata.has_value() &&
                  input_.common_grams_metadata->scoring_coverage ==
                          segment_v2::inverted_index::ScoringCoverage::kComplete) {}

Status SniiStreamedIndexSession::push_term(StreamedTermPostings&& tp) {
    if (!owner_->failed_.ok()) return owner_->failed_;
    if (finished_) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "compound: push_term on a finished streamed index session");
    }
    const Status status = writer_->push_term(std::move(tp));
    if (!status.ok()) return owner_->poison(status);
    return Status::OK();
}

Status SniiStreamedIndexSession::set_semantic_token_count(uint64_t token_count) {
    if (!owner_->failed_.ok()) return owner_->failed_;
    if (finished_) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "compound: semantic token count on a finished streamed index session");
    }
    if (!semantic_token_count_required_) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "compound: streamed index session has no complete semantic scoring metadata");
    }
    if (semantic_token_count_set_) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "compound: semantic token count was already set");
    }
    DORIS_CHECK(input_.common_grams_metadata.has_value());
    DORIS_CHECK(writer_->common_grams_metadata_.has_value());
    input_.common_grams_metadata->scoring_token_count = token_count;
    writer_->common_grams_metadata_->scoring_token_count = token_count;
    semantic_token_count_set_ = true;
    return Status::OK();
}

Status SniiStreamedIndexSession::finish() {
    if (!owner_->failed_.ok()) return owner_->failed_;
    if (finished_) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "compound: finish on an already-finished streamed index session");
    }
    if (semantic_token_count_required_ && !semantic_token_count_set_) {
        return owner_->poison(Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "compound: semantic token count must be set before streamed index finish"));
    }
    return owner_->finish_streamed_index(this);
}

void SniiStreamedIndexSession::abort(const Status& cause) {
    DCHECK(!cause.ok());
    static_cast<void>(owner_->poison(cause));
}

Status SniiCompoundWriter::begin_streamed_index(SniiIndexInput in,
                                                SniiStreamedIndexSession** session) {
    std::vector<uint32_t> null_docids;
    null_docids.swap(in.null_docids);
    MemoryReporter::Reservation null_docids_reservation =
            in.mem_reporter == nullptr ? MemoryReporter::Reservation()
                                       : in.mem_reporter->make_reservation();
    if (in.mem_reporter != nullptr) {
        RETURN_IF_ERROR(null_docids_reservation.set_bytes(
                static_cast<uint64_t>(null_docids.capacity()) * sizeof(uint32_t)));
    }
    return begin_streamed_index(
            std::move(in),
            TrackedNullDocids(std::move(null_docids_reservation), std::move(null_docids)), session);
}

Status SniiCompoundWriter::begin_streamed_index(SniiIndexInput in, TrackedNullDocids null_docids,
                                                SniiStreamedIndexSession** session) {
    std::vector<uint8_t> encoded_norms;
    encoded_norms.swap(in.encoded_norms);
    MemoryReporter::Reservation encoded_norms_reservation =
            in.mem_reporter == nullptr ? MemoryReporter::Reservation()
                                       : in.mem_reporter->make_reservation();
    if (in.mem_reporter != nullptr) {
        RETURN_IF_ERROR(encoded_norms_reservation.set_bytes(encoded_norms.capacity()));
    }
    return begin_streamed_index(
            std::move(in), std::move(null_docids),
            TrackedEncodedNorms(std::move(encoded_norms_reservation), std::move(encoded_norms)),
            session);
}

Status SniiCompoundWriter::begin_streamed_index(SniiIndexInput in, TrackedNullDocids null_docids,
                                                TrackedEncodedNorms encoded_norms,
                                                SniiStreamedIndexSession** session) {
    if (session == nullptr)
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "compound: null session out parameter");
    *session = nullptr;
    if (out_ == nullptr)
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("compound: null file writer");
    if (finished_)
        return Status::Error<ErrorCode::INTERNAL_ERROR, false>("compound: begin after finish");
    if (!failed_.ok()) return failed_;
    if (has_active_session())
        return Status::Error<ErrorCode::INTERNAL_ERROR, false>(
                "compound: a streamed index session is already active (one at a time: its "
                "posting region streams straight into the container output)");
    // A streamed session takes terms ONLY via push_term; a term source or a
    // materialized vector would silently be ignored by the streamed writer.
    if (in.term_source != nullptr || !in.terms.empty())
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "compound: a streamed index session must not carry a term source or "
                "materialized terms");
    if (!in.null_docids.empty())
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "compound: tracked streamed NULL docids must not also be present in input");
    if (!in.encoded_norms.empty())
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "compound: tracked streamed norms must not also be present in input");
    RETURN_IF_ERROR(ensure_bootstrap());
    auto s = std::unique_ptr<SniiStreamedIndexSession>(new SniiStreamedIndexSession(
            this, std::move(in), std::move(null_docids), std::move(encoded_norms)));
    s->post_off_ = out_->bytes_written();
    RETURN_IF_ERROR(s->writer_->begin_streamed(out_));
    sessions_.push_back(std::move(s));
    *session = sessions_.back().get();
    return Status::OK();
}

Status SniiCompoundWriter::finish_streamed_index(SniiStreamedIndexSession* session) {
    // Flushes the trailing DICT block and finalizes the stats / null-bitmap /
    // BSBF sections (poisoning the logical writer on failure).
    Status status = session->writer_->finish_streamed();
    if (!status.ok()) return poison(status);
    // finalize materialized the framed norms section. Drop the source vector and
    // its transferred charge before retaining that section for compound finish.
    std::vector<uint8_t>().swap(session->input_.encoded_norms);
    session->encoded_norms_reservation_.reset();
    Placement p;
    p.post_off = session->post_off_;
    p.post_len = out_->bytes_written() - p.post_off;
    p.dict_off = out_->bytes_written();
    status = session->writer_->stream_dict_region_into(out_);
    if (!status.ok()) return poison(status);
    p.dict_len = out_->bytes_written() - p.dict_off;
    // Only now does the index join the container. Any failure above leaves
    // finished_ false, so finish() keeps failing loudly instead of sealing a
    // tail that silently omits an index whose posting bytes are in the file.
    indexes_.push_back(std::move(session->writer_));
    placements_.push_back(p);
    session->finished_ = true;
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
        indexes_[i]->release_norms_bytes();
    }
    for (size_t i = 0; i < indexes_.size(); ++i) {
        LogicalIndexWriter& w = *indexes_[i];
        if (!w.has_null_bitmap()) continue;
        Placement& p = placements_[i];
        p.null_off = out_->bytes_written();
        RETURN_IF_ERROR(append(w.null_bitmap_bytes()));
        p.null_len = out_->bytes_written() - p.null_off;
        w.release_null_bitmap_bytes();
    }
    for (size_t i = 0; i < indexes_.size(); ++i) {
        LogicalIndexWriter& w = *indexes_[i];
        if (!w.has_bsbf()) continue;
        Placement& p = placements_[i];
        p.bsbf_off = out_->bytes_written();
        RETURN_IF_ERROR(append(w.bsbf_bytes()));
        p.bsbf_len = out_->bytes_written() - p.bsbf_off;
        w.release_bsbf_bytes();
    }
    return Status::OK();
}

Status SniiCompoundWriter::write_tail() {
    std::vector<LogicalIndexMetadataRef> directory_entries;
    directory_entries.reserve(inherited_.size() + indexes_.size());
    // Inherited metadata groups are re-emitted verbatim: their section references
    // already point into the copied prefix, which landed at identical offsets, so
    // no posting is decoded or re-encoded. Only the group's own position moves.
    for (const InheritedGroup& group : inherited_) {
        LogicalIndexMetadataRef entry;
        entry.index_id = group.index_id;
        entry.index_suffix = group.index_suffix;
        const uint64_t core_offset = out_->bytes_written();
        entry.core_metadata = {.offset = core_offset, .length = group.core_length};
        entry.sampled_term_index = {.offset = core_offset + group.core_length,
                                    .length = group.sampled_term_index_length};
        entry.dict_block_directory = {
                .offset = core_offset + group.core_length + group.sampled_term_index_length,
                .length = group.dict_block_directory_length};
        RETURN_IF_ERROR(append(group.metadata_group));
        DORIS_CHECK_EQ(out_->bytes_written(), core_offset + group.metadata_group.size());
        directory_entries.push_back(std::move(entry));
    }
    for (size_t i = 0; i < indexes_.size(); ++i) {
        const LogicalIndexWriter& w = *indexes_[i];
        const Placement& p = placements_[i];

        SectionRefs refs;
        refs.dict_region = {.offset = p.dict_off, .length = p.dict_len};
        refs.posting_region = {.offset = p.post_off, .length = p.post_len};
        refs.norms = {.offset = p.norms_off, .length = p.norms_len};
        refs.null_bitmap = {.offset = p.null_off, .length = p.null_len};
        refs.bsbf = {.offset = p.bsbf_off, .length = p.bsbf_len};

        SerializedMetadataGroup group;
        RETURN_IF_ERROR(w.finish_metadata(refs, p.dict_off, &group));

        LogicalIndexMetadataRef entry;
        entry.index_id = w.index_id();
        entry.index_suffix = w.index_suffix();
        entry.core_metadata = {.offset = out_->bytes_written(), .length = group.core.size()};
        RETURN_IF_ERROR(append(group.core));
        DORIS_CHECK_EQ(out_->bytes_written(),
                       entry.core_metadata.offset + entry.core_metadata.length);

        entry.sampled_term_index = {.offset = out_->bytes_written(),
                                    .length = group.sampled_term_index.size()};
        DORIS_CHECK_EQ(entry.sampled_term_index.offset,
                       entry.core_metadata.offset + entry.core_metadata.length);
        RETURN_IF_ERROR(append(group.sampled_term_index));
        DORIS_CHECK_EQ(out_->bytes_written(),
                       entry.sampled_term_index.offset + entry.sampled_term_index.length);

        entry.dict_block_directory = {.offset = out_->bytes_written(),
                                      .length = group.dict_block_directory.size()};
        DORIS_CHECK_EQ(entry.dict_block_directory.offset,
                       entry.sampled_term_index.offset + entry.sampled_term_index.length);
        RETURN_IF_ERROR(append(group.dict_block_directory));
        DORIS_CHECK_EQ(out_->bytes_written(),
                       entry.dict_block_directory.offset + entry.dict_block_directory.length);
        directory_entries.push_back(std::move(entry));
    }

    ByteSink directory_sink;
    RETURN_IF_ERROR(format::encode_metadata_directory(directory_entries, &directory_sink));
    const uint64_t directory_offset = out_->bytes_written();
    RETURN_IF_ERROR(append(directory_sink.buffer()));
    const uint64_t directory_length = out_->bytes_written() - directory_offset;

    TailPointer tp;
    tp.directory_offset = directory_offset;
    tp.directory_length = directory_length;
    tp.directory_crc32c = crc32c(directory_sink.view());
    ByteSink tail_sink;
    RETURN_IF_ERROR(format::encode_tail_pointer(tp, &tail_sink));
    return append(tail_sink.buffer());
}

Status SniiCompoundWriter::finish() {
    if (out_ == nullptr)
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("compound: null file writer");
    if (!failed_.ok()) {
        return failed_;
    }
    if (finished_)
        return Status::Error<ErrorCode::INTERNAL_ERROR, false>("compound: finish called twice");
    // Crash-safety invariant 6: a begun-but-unfinished streamed session already
    // streamed posting bytes into the file but recorded no placement; sealing
    // the container now would silently drop that index. Fail loudly -- the
    // whole compaction round must fail and be retried.
    if (has_active_session())
        return Status::Error<ErrorCode::INTERNAL_ERROR, false>(
                "compound: finish with an unfinished streamed index session; the half-fed "
                "index must never be sealed away silently");
    finished_ = true;

    RETURN_IF_ERROR(ensure_bootstrap()); // empty container still gets a header
    Status status = write_norms();
    if (!status.ok()) return poison(status);
    status = write_tail();
    if (!status.ok()) return poison(status);
    status = out_->finalize();
    if (!status.ok()) return poison(status);
    return Status::OK();
}

} // namespace doris::snii::writer
