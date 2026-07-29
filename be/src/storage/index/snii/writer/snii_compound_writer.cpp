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

#include <fmt/format.h>

#include <algorithm>
#include <utility>

#include "common/config.h"
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
    for (const PendingBlobIndex& blob : blobs_) {
        if (blob.index_id == in.index_id && blob.index_suffix == in.index_suffix) {
            return poison(Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "compound: new logical index reuses a registered blob index key"));
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
    // liw has been moved from; write_index_aux_sections works off indexes_.back().
    status = write_index_aux_sections(indexes_.size() - 1);
    if (!status.ok()) return poison(status);
    return Status::OK();
}

// Argument validation for one blob registration: the kind must be one this
// writer can emit, and the named-file table must be non-empty with unique,
// named, readable entries. Rejecting an unknown kind HERE (rather than letting
// the directory encoder catch it inside finish()) keeps a multi-GiB blob from
// being copied before the failure surfaces, and reports a caller bug as
// INVALID_ARGUMENT instead of an Unsupported format problem.
Status SniiCompoundWriter::validate_blob_registration(
        format::LogicalIndexKind kind, const std::vector<BlobFileSource>& cold_files,
        const std::vector<BlobFileSource>& hot_files) {
    if (kind != format::LogicalIndexKind::kBkd && kind != format::LogicalIndexKind::kAnn) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                fmt::format("compound: add_blob_index got kind {}; text indexes go through "
                            "add_logical_index and other kinds are not emittable",
                            static_cast<uint32_t>(kind)));
    }
    if (cold_files.empty() && hot_files.empty()) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "compound: blob index registered without files");
    }
    for (const std::vector<BlobFileSource>* files : {&cold_files, &hot_files}) {
        for (const BlobFileSource& file : *files) {
            if (file.name.empty()) {
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "compound: blob file with empty name");
            }
            if (file.length > 0 && !file.read_fn) {
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "compound: blob file without a read function");
            }
            size_t seen = 0;
            for (const std::vector<BlobFileSource>* other : {&cold_files, &hot_files}) {
                for (const BlobFileSource& candidate : *other) {
                    seen += candidate.name == file.name ? 1 : 0;
                }
            }
            if (seen != 1) {
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "compound: duplicate blob file name");
            }
        }
    }
    return Status::OK();
}

Status SniiCompoundWriter::add_blob_index(uint64_t index_id, std::string index_suffix,
                                          format::LogicalIndexKind kind,
                                          std::vector<BlobFileSource> cold_files,
                                          std::vector<BlobFileSource> hot_files) {
    if (out_ == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("compound: null file writer");
    }
    if (finished_) {
        return Status::Error<ErrorCode::INTERNAL_ERROR, false>("compound: add after finish");
    }
    if (!failed_.ok()) return failed_;
    // Registration writes nothing, so it is legal while a streamed session is
    // active. Rejections that only mean "bad arguments" leave the writer clean;
    // key collisions poison it (see below).
    RETURN_IF_ERROR(validate_blob_registration(kind, cold_files, hot_files));
    for (const PendingBlobIndex& blob : blobs_) {
        if (blob.index_id == index_id && blob.index_suffix == index_suffix) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "compound: blob index key registered twice");
        }
    }
    // A key collision against an already-registered index means the caller's plan
    // cannot produce a valid container (the directory must hold each key exactly
    // once, and MetadataDirectory::find would silently shadow one of the two).
    // Poison rather than reject cleanly, mirroring inherit() and
    // add_logical_index: sealing a container that silently omits an index the
    // schema requires is never acceptable. Catching it HERE also keeps a
    // multi-GiB blob from being copied before the failure surfaces, and reports
    // it as the caller bug it is instead of Corruption from the encoder's
    // self-check inside finish().
    for (const InheritedGroup& group : inherited_) {
        if (group.index_id == index_id && group.index_suffix == index_suffix) {
            return poison(Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "compound: blob index reuses an inherited key"));
        }
    }
    for (const std::unique_ptr<LogicalIndexWriter>& text : indexes_) {
        if (text->index_id() == index_id && text->index_suffix() == index_suffix) {
            return poison(Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "compound: blob index reuses a text logical index key"));
        }
    }
    PendingBlobIndex blob;
    blob.index_id = index_id;
    blob.index_suffix = std::move(index_suffix);
    blob.kind = kind;
    blob.cold_files = std::move(cold_files);
    blob.hot_files = std::move(hot_files);
    blobs_.push_back(std::move(blob));
    return Status::OK();
}

Status SniiCompoundWriter::write_blob_files(const std::vector<BlobFileSource>& files,
                                            std::vector<format::NamedBlobFileRef>* refs) {
    std::vector<uint8_t> chunk;
    for (const BlobFileSource& file : files) {
        format::NamedBlobFileRef ref;
        ref.name = file.name;
        ref.offset = out_->bytes_written();
        ref.length = file.length;
        uint32_t crc = 0;
        uint64_t copied = 0;
        while (copied < file.length) {
            const auto n = static_cast<size_t>(
                    std::min<uint64_t>(kBlobCopyChunkBytes, file.length - copied));
            chunk.resize(n);
            RETURN_IF_ERROR(file.read_fn(copied, n, chunk.data()));
            crc = crc32c_extend(crc, Slice(chunk.data(), n));
            RETURN_IF_ERROR(out_->append(Slice(chunk.data(), n)));
            copied += n;
        }
        ref.crc32c = crc;
        DORIS_CHECK_EQ(out_->bytes_written(), ref.offset + ref.length);
        refs->push_back(std::move(ref));
    }
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
    // The index joins the container (indexes_/placements_) here, but session->finished_
    // is not set until write_index_aux_sections below also succeeds. A failure ANYWHERE
    // in this function -- finish_streamed()/stream_dict_region_into() above, or
    // write_index_aux_sections below -- calls poison(), which sets failed_ before
    // returning. finish() checks "if (!failed_.ok()) return failed_;" ahead of its
    // has_active_session() gate, so a poisoned writer fails loudly on its own; it can
    // never fall through to sealing a tail that silently omits an index whose posting
    // bytes are already in the file.
    indexes_.push_back(std::move(session->writer_));
    placements_.push_back(p);
    status = write_index_aux_sections(indexes_.size() - 1);
    if (!status.ok()) return poison(status);
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

// Writes one index's norms / null bitmap / bsbf directly after its [posting][dict] pair.
// Bytes are released as soon as they are on disk rather than being held until finish(),
// which also lowers import peak memory -- a content column's bsbf runs to MBs.
Status SniiCompoundWriter::write_index_aux_sections(size_t index) {
    DORIS_CHECK_LT(index, indexes_.size());
    DORIS_CHECK_LT(index, placements_.size());
    LogicalIndexWriter& w = *indexes_[index];
    Placement& p = placements_[index];

    if (w.has_norms() && !w.norms_bytes().empty()) {
        p.norms_off = out_->bytes_written();
        RETURN_IF_ERROR(append(w.norms_bytes()));
        p.norms_len = out_->bytes_written() - p.norms_off;
        w.release_norms_bytes();
    }
    if (w.has_null_bitmap()) {
        p.null_off = out_->bytes_written();
        RETURN_IF_ERROR(append(w.null_bitmap_bytes()));
        p.null_len = out_->bytes_written() - p.null_off;
        w.release_null_bitmap_bytes();
    }
    if (w.has_bsbf()) {
        p.bsbf_off = out_->bytes_written();
        RETURN_IF_ERROR(append(w.bsbf_bytes()));
        p.bsbf_len = out_->bytes_written() - p.bsbf_off;
        w.release_bsbf_bytes();
    }
    return Status::OK();
}

// Streams every registered blob's HOT files -- after all text metadata groups,
// physically adjacent within each entry, so a future open can fetch an entry's
// hot set with one range read (mirroring Core/STI/DBD adjacency) -- then appends
// one directory entry per blob holding its cold refs followed by its hot refs.
Status SniiCompoundWriter::write_blob_hot_files_and_entries(
        std::vector<LogicalIndexMetadataRef>* directory_entries) {
    for (PendingBlobIndex& blob : blobs_) {
        RETURN_IF_ERROR(write_blob_files(blob.hot_files, &blob.hot_refs));
    }
    for (PendingBlobIndex& blob : blobs_) {
        LogicalIndexMetadataRef entry;
        entry.index_id = blob.index_id;
        entry.index_suffix = blob.index_suffix;
        entry.kind = blob.kind;
        entry.files.reserve(blob.cold_refs.size() + blob.hot_refs.size());
        entry.files.insert(entry.files.end(), blob.cold_refs.begin(), blob.cold_refs.end());
        entry.files.insert(entry.files.end(), blob.hot_refs.begin(), blob.hot_refs.end());
        directory_entries->push_back(std::move(entry));
    }
    return Status::OK();
}

Status SniiCompoundWriter::write_tail() {
    std::vector<LogicalIndexMetadataRef> directory_entries;
    directory_entries.reserve(inherited_.size() + indexes_.size() + blobs_.size());
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

    RETURN_IF_ERROR(write_blob_hot_files_and_entries(&directory_entries));

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

    // Pad the container up to a file-cache block boundary, but only when that padding is small
    // relative to the container.
    //
    // Padding at all: s_align_size clamps the aligned window to the file end, and back-pads by a
    // whole block when the clamp leaves it short (io/cache/cached_remote_file_reader.cpp). Ending
    // on a boundary makes that condition false, so a read confined to the final block fetches one
    // block instead of two.
    //
    // Only sometimes: the back-pad costs nothing when the query already fetches the preceding
    // block for other reasons. So the saving is one-shot and bounded (at most last_partial per
    // container) while the cost -- filler that every tail read pulls in -- scales with how much of
    // the container a query touches. Both signs are measured, on the same wikipedia corpus:
    //
    //   53-62 MiB containers, pad 0.09%-1.28%: -2.2% bytes fetched over a 13-case sweep,
    //     -21% when the sweep is one targeted case
    //   ~4 MiB containers, pad 13.3%: the sweep reads nearly the whole container, so the back-pad
    //     was already free and the filler is pure addition -> +13.3%
    //
    // kMinPaddingLeverage is a judgement call, not a derived constant: it admits the measured
    // 1.28% case with margin and rejects the 13.3% one.
    //
    // Expressed as a floor on the CONTAINER rather than a ratio on the padding. Since pad < block
    // the two bound the cost identically, but the floor cannot overflow, and it keeps containers
    // small enough to be PACKED out of the deal: in cloud mode a container below
    // cloud::config::small_file_threshold_bytes (1 MiB) is appended into a shared object at an
    // arbitrary offset (RowsetWriterContext wraps the fs in io::PackedFileSystem), where
    // s_align_size works in packed coordinates and aligning the sub-file buys nothing at all.
    // A 32-block floor clears that threshold by 32x.
    //
    // The padding goes BEFORE the tail pointer, which must stay the last thing in the file for the
    // reader to find it.
    //
    // Caveat: the block size is read at WRITE time but the saving is realised at READ time. If a
    // deployment changes file_cache_each_block_size afterwards, nothing breaks and no index becomes
    // unreadable -- but the outcome is not symmetric, so "neutral" would be the wrong word.
    // SHRINKING it is safe when the new size divides the old (256 KiB into 1 MiB keeps alignment).
    // GROWING it (1 MiB -> 4 MiB, a normal S3 throughput tuning move) brings the back-pad back
    // while the filler bytes stay on disk: strictly worse than never having padded, until
    // compaction rewrites the container.
    //
    // Gated on enable_file_cache because the saving is realised only by CachedRemoteFileReader.
    // That flag defaults to FALSE; without this check a storage-compute-coupled or local-filesystem
    // deployment appends up to a block of zeros per container and never reads through a block cache
    // at all. (exec_env_init only validates file_cache_each_block_size when the cache is on, so in
    // that configuration the value here would also be entirely unvalidated.)
    const int64_t block = config::file_cache_each_block_size;
    if (config::enable_file_cache && block > 0) {
        const uint64_t unpadded = out_->bytes_written() + tail_sink.buffer().size();
        const auto block_size = static_cast<uint64_t>(block);
        const uint64_t pad = (block_size - unpadded % block_size) % block_size;
        // 2*pad < block is the cost/benefit test itself, and it is exact rather than a proxy.
        // Measured in §6.7.1 of the design doc: the saving equals sum(last_partial) (2,290,341 vs
        // 2,290,330 predicted) and the cost equals the bytes added to disk (+45,207,390 written vs
        // +45,199,729 fetched). Since last_partial + pad == block, benefit = block - pad and
        // cost ~ pad -- perfectly anticorrelated, and NEITHER depends on the container size. A
        // 40 MiB container that overshoots a boundary by 100 B has last_partial = 100 and
        // pad = 1,048,476: it clears any size-based gate while saving 100 bytes for a megabyte.
        // On this project's own four measured containers the extra term trades 29% of the saving
        // for a 76% cut in padding written (1.20:1 -> 3.55:1 benefit:cost).
        if (pad > 0 && 2 * pad < block_size && unpadded / block_size >= kMinPaddingLeverage) {
            // Never referenced by any SectionRef, so no reader ever reads it.
            const std::vector<uint8_t> filler(pad, 0);
            RETURN_IF_ERROR(append(filler));
        }
    }

    RETURN_IF_ERROR(append(tail_sink.buffer()));
    return Status::OK();
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
    // Aux sections were written per index at add/finish_streamed time, right after each
    // index's [posting][dict] pair -- see write_index_aux_sections.
    // Blob COLD files follow all text physical sections and precede the first
    // metadata group, in registration order.
    Status status;
    for (PendingBlobIndex& blob : blobs_) {
        status = write_blob_files(blob.cold_files, &blob.cold_refs);
        if (!status.ok()) return poison(status);
    }
    status = write_tail();
    if (!status.ok()) return poison(status);
    status = out_->finalize();
    if (!status.ok()) return poison(status);
    return Status::OK();
}

} // namespace doris::snii::writer
