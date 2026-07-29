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

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/format/metadata_directory.h"
#include "storage/index/snii/io/file_reader.h"
#include "storage/index/snii/io/file_writer.h"
#include "storage/index/snii/writer/logical_index_writer.h"

namespace doris::snii::reader {
// Only ever named by reference below, so a declaration is enough. Including
// snii_segment_reader.h here would instead splice the whole reader into this
// header's dependents -- and this header sits upstream of runtime/exec_env.h,
// so that is most of the backend.
class SniiRewriteSnapshot;
} // namespace doris::snii::reader

// SniiCompoundWriter -- orchestrates a single-segment SNII container for one or
// more logical indexes, written front-to-back through an append-only
// io::FileWriter (no seek-back). It resolves all back-references by writing the
// metadata groups, raw directory, and fixed tail pointer LAST.
//
// CONTAINER LAYOUT PRODUCED (this is the on-disk contract the reader matches):
//   [bootstrap_header]                          (kBootstrapHeaderSize bytes)
//   for each logical index, in add order:
//     [posting region]       interleaved [prx][frq] per pod_ref term, term order
//                            (prx span empty when !has_prx)
//     [DICT blocks region]   concatenated DICT blocks, split by
//                            target_dict_block_bytes
//   for each logical index, in add order:
//     [norms POD]            NormsPodWriter::finish (scoring only; else absent)
//     [null bitmap POD]      NullBitmapWriter::finish (when nulls exist)
//   for each logical index, in add order:
//     [Core metadata][SampledTermIndex blob][DICT block directory blob]
//   [metadata directory]     raw SniiMetadataDirectoryPB bytes
//   [tail_pointer]           encode_tail_pointer at EOF
//
// (The posting region is streamed BEFORE the DICT region per index: postings are
// the large append-only term-ordered stream; the DICT region is the compact
// compressed trailer.)
//
// OFFSET CONVENTIONS (ABSOLUTE file offsets unless stated otherwise):
//   - SectionRefs in each Core metadata record ABSOLUTE file offset+length of
//     that index's posting, DICT, norms, null-bitmap, and BSBF regions. Absent
//     regions are (0,0); a present-but-empty posting region (all-INLINE index)
//     is (off, 0).
//   - DictBlockDirectory entries record each DICT block's ABSOLUTE file offset +
//     length.
//   - A windowed/slim pod_ref entry's absolute .frq offset =
//       section_refs.posting_region.offset + frq_base + frq_off_delta
//     where frq_base is the posting-region-relative running offset captured at the
//     block's open (see logical_index_writer.h). prx follows the identical rule
//     against the SAME region (prx_base == frq_base).
//   - tail_pointer.directory_offset/length point at the raw metadata directory.
namespace doris::snii::writer {

class SniiCompoundWriter;

// One opaque sub-file of a blob logical index, registered by add_blob_index.
// `read_fn` MUST fill exactly `len` bytes at blob-relative `offset` into `out`
// or return an error -- a short read reported as OK would be checksummed and
// sealed as if it were the real payload, since the crc is computed over the
// same buffer this call fills.
//
// The signature is PURE Status by design: the future Doris adapter that wraps a
// staged third-party index directory is required to convert that library's
// exceptions into Status BEFORE calling in, because the snii core has no
// try/catch on the sealing path (an escaping exception would skip poison()) and
// takes no third-party index-library dependency (a guard test enforces this).
struct BlobFileSource {
    std::string name;
    uint64_t length = 0;
    std::function<Status(uint64_t offset, size_t len, uint8_t* out)> read_fn;
};

// T2.2 (compaction index merge fast path): handle for ONE streamed logical-index
// session inside a SniiCompoundWriter, obtained from begin_streamed_index(). The
// caller pushes lexicographically sorted terms (the k-way merge output) and seals
// the index with finish(), which lays the [posting][dict] regions out exactly like
// add_logical_index. The handle is owned by the compound writer and stays valid
// for the writer's lifetime; it must not outlive it.
//
// CRASH SAFETY (invariant 6): a session that was begun but never successfully
// finished keeps the container permanently unsealable -- its posting bytes are
// already in the file, so SniiCompoundWriter::finish() fails loudly instead of
// writing a tail that silently omits the half-fed index.
class SniiStreamedIndexSession {
public:
    SniiStreamedIndexSession(const SniiStreamedIndexSession&) = delete;
    SniiStreamedIndexSession& operator=(const SniiStreamedIndexSession&) = delete;
    SniiStreamedIndexSession(SniiStreamedIndexSession&&) = delete;
    SniiStreamedIndexSession& operator=(SniiStreamedIndexSession&&) = delete;

    // Terms must arrive in strictly increasing lexicographic order with
    // ascending-docid postings. Unlike the standalone LogicalIndexWriter API,
    // every rejection is terminal here because posting bytes may already have
    // entered the compound output; all later calls return the first error.
    Status push_term(StreamedTermPostings&& tp);
    // Binds the semantic (plain-token) count after the merge's single postings
    // pass. Complete scoring metadata requires exactly one call, including for
    // an empty destination whose count is zero.
    Status set_semantic_token_count(uint64_t token_count);
    // Seals this index: flushes the trailing DICT block, streams the DICT region
    // right after the posting region and records the placements. A failed finish
    // leaves the session unfinished (and the container unsealable) -- there is
    // no retry, the whole compaction round must fail and be redone.
    Status finish();
    // Makes the owning compound permanently unsealable. Merge plans call this
    // on every destination when any source or sibling destination fails, because
    // a successfully written prefix is not a complete logical index.
    void abort(const Status& cause);
    bool finished() const { return finished_; }

private:
    friend class SniiCompoundWriter;
    SniiStreamedIndexSession(SniiCompoundWriter* owner, SniiIndexInput in,
                             TrackedNullDocids null_docids, TrackedEncodedNorms encoded_norms);
    static SniiIndexInput attach_encoded_norms(SniiIndexInput in,
                                               TrackedEncodedNorms* encoded_norms,
                                               uint64_t reserved_bytes);

    SniiCompoundWriter* owner_;
    // The reservation precedes input_ so input_.encoded_norms is destroyed
    // before its charge is released.
    MemoryReporter::Reservation encoded_norms_reservation_;
    // Owns the input: LogicalIndexWriter keeps references into it (terms /
    // encoded_norms), so it must live exactly as long as the writer.
    SniiIndexInput input_;
    std::unique_ptr<LogicalIndexWriter> writer_;
    uint64_t post_off_ = 0;
    bool semantic_token_count_required_ = false;
    bool semantic_token_count_set_ = false;
    bool finished_ = false;
};

class SniiCompoundWriter {
public:
    explicit SniiCompoundWriter(io::FileWriter* out);
    SniiCompoundWriter(const SniiCompoundWriter&) = delete;
    SniiCompoundWriter& operator=(const SniiCompoundWriter&) = delete;
    SniiCompoundWriter(SniiCompoundWriter&&) = delete;
    SniiCompoundWriter& operator=(SniiCompoundWriter&&) = delete;

    // Size of the buffer the inherit copy streams through. Fixed, so peak memory
    // of an inherit does not grow with the source container. Sized for sequential
    // local reads -- generous next to inverted_index_read_buffer_size (4 KiB), and
    // small enough that even a multi-GiB container costs a negligible number of
    // reads.
    static constexpr size_t kInheritCopyChunkBytes = 64U << 10;
    // Same rationale for the blob file copy in finish(): peak memory is one
    // chunk regardless of blob size (a GiB-scale ann.faiss must never be
    // buffered whole inside the compound writer).
    static constexpr size_t kBlobCopyChunkBytes = 64U << 10;

    // Carries a source container's unchanged logical indexes into this one
    // (BUILD INDEX on SNII). It copies the source's validated physical prefix --
    // bootstrap header plus every section the inherited indexes reference --
    // verbatim, then registers their metadata groups so finish() re-emits them
    // without decoding or re-encoding a single posting. Because the prefix lands
    // at the SAME offsets, the inherited section references stay valid unchanged.
    //
    // MUST be the writer's first data operation: the copy owns the front of the
    // file, so anything already written would be overwritten in meaning. A read
    // or write failure poisons the writer, so finish() can never seal a container
    // holding a partial prefix.
    Status inherit(const reader::SniiRewriteSnapshot& snapshot, io::FileReader* source);

    // Buffers one logical index: builds its section bytes and meta sub-sections.
    // The actual file writing happens in finish() (single front-to-back pass).
    // The key (index_id, suffix) must not collide with an inherited one.
    Status add_logical_index(const SniiIndexInput& in);

    // Registers one opaque BLOB logical index (kind must not be kInverted).
    // Registration is pure bookkeeping -- NOT A BYTE is written here, so it is
    // legal at any point before finish() (even while a streamed session is
    // active). finish() streams cold_files into the data area after all text
    // physical sections, and hot_files after the text metadata groups,
    // physically adjacent per entry, recording absolute offsets + crc32c into
    // the directory entry. A rejected registration leaves the writer clean; a
    // copy failure during finish() poisons the container for good.
    Status add_blob_index(uint64_t index_id, std::string index_suffix,
                          format::LogicalIndexKind kind, std::vector<BlobFileSource> cold_files,
                          std::vector<BlobFileSource> hot_files);

    // T2.2: begins a STREAMED logical-index session (the compaction merge fast
    // path) -- the caller pushes pre-merged terms through *session instead of
    // handing the writer a term source, so `in` must carry NO term_source and NO
    // materialized terms. Only ONE session may be active at a time (its posting
    // region streams straight into the container output, so a concurrent
    // add_logical_index or second session would interleave bytes); both are
    // rejected while a session is unfinished, as is finish(). The returned
    // handle is owned by this writer and valid for its lifetime.
    Status begin_streamed_index(SniiIndexInput in, SniiStreamedIndexSession** session);
    Status begin_streamed_index(SniiIndexInput in, TrackedNullDocids null_docids,
                                SniiStreamedIndexSession** session);
    Status begin_streamed_index(SniiIndexInput in, TrackedNullDocids null_docids,
                                TrackedEncodedNorms encoded_norms,
                                SniiStreamedIndexSession** session);

    // Writes bootstrap header + all index sections + adjacent metadata groups +
    // raw directory + tail pointer, then finalizes the underlying writer.
    Status finish();

private:
    // Absolute placement of one index's sections, resolved during finish().
    struct Placement {
        uint64_t dict_off = 0;
        uint64_t dict_len = 0;
        uint64_t post_off = 0; // interleaved [prx][frq] posting region (was frq + prx)
        uint64_t post_len = 0;
        uint64_t norms_off = 0;
        uint64_t norms_len = 0;
        uint64_t null_off = 0;
        uint64_t null_len = 0;
        uint64_t bsbf_off = 0;
        uint64_t bsbf_len = 0;
    };

    // One logical index carried over from a source container: its raw
    // [Core][STI][DBD] bytes plus the three lengths needed to rebuild the
    // directory entry once the group's new position is known.
    struct InheritedGroup {
        uint64_t index_id = 0;
        std::string index_suffix;
        std::vector<uint8_t> metadata_group;
        size_t core_length = 0;
        size_t sampled_term_index_length = 0;
        size_t dict_block_directory_length = 0;
    };

    // One registered blob logical index awaiting finish(). cold/hot refs are
    // resolved as the corresponding bytes stream out during finish().
    struct PendingBlobIndex {
        uint64_t index_id = 0;
        std::string index_suffix;
        format::LogicalIndexKind kind = format::LogicalIndexKind::kInverted;
        std::vector<BlobFileSource> cold_files;
        std::vector<BlobFileSource> hot_files;
        std::vector<format::NamedBlobFileRef> cold_refs;
        std::vector<format::NamedBlobFileRef> hot_refs;
    };

    friend class SniiStreamedIndexSession;

    Status ensure_bootstrap();
    Status write_bootstrap();
    // Writes indexes_[index]'s norms/null-bitmap/bsbf immediately after its
    // [posting][dict] pair and fills placements_[index]. Keeping one index's sections
    // contiguous is what makes a single-index cold query touch one cache block instead
    // of three; the previous layout grouped these by section type across all indexes.
    // Must be called after indexes_/placements_ have been pushed for this index.
    Status write_index_aux_sections(size_t index);
    Status write_tail();
    Status append(const std::vector<uint8_t>& bytes);
    Status poison(Status status);
    // Argument validation for one add_blob_index call; see the .cpp.
    static Status validate_blob_registration(format::LogicalIndexKind kind,
                                             const std::vector<BlobFileSource>& cold_files,
                                             const std::vector<BlobFileSource>& hot_files);
    // Streams `files` into the container at the current position through a
    // fixed-size chunk buffer, recording each file's absolute placement and
    // crc32c into *refs. Called from finish() only (cold then hot regions).
    Status write_blob_files(const std::vector<BlobFileSource>& files,
                            std::vector<format::NamedBlobFileRef>* refs);
    // Emits the blob hot-file region and the blob directory entries; see the .cpp.
    Status write_blob_hot_files_and_entries(
            std::vector<format::LogicalIndexMetadataRef>* directory_entries);
    // Seals one streamed session: records its placements and adopts its
    // LogicalIndexWriter into indexes_ (only on FULL success -- see the
    // crash-safety note on SniiStreamedIndexSession).
    Status finish_streamed_index(SniiStreamedIndexSession* session);
    // An unfinished streamed session (begun but not successfully sealed):
    // blocks add_logical_index, another begin_streamed_index and finish().
    bool has_active_session() const { return !sessions_.empty() && !sessions_.back()->finished(); }

    io::FileWriter* out_;
    std::vector<std::unique_ptr<LogicalIndexWriter>> indexes_;
    // Streamed sessions in begin order (at most the last one is unfinished).
    // Owned here so the raw handles returned to callers stay valid for the
    // writer's lifetime; a finished session is inert (its writer moved out).
    std::vector<std::unique_ptr<SniiStreamedIndexSession>> sessions_;
    // Per-index placement, fully resolved by the time add_logical_index /
    // finish_streamed_index returns: post_off/post_len and dict_off/dict_len as each
    // index's posting/DICT regions stream in, then norms/null/bsbf off+len via
    // write_index_aux_sections immediately after. finish() no longer fills any of
    // these fields -- it only reads placements_ to build the metadata directory. The
    // absolute write offset is out_->bytes_written() (the single source of truth --
    // no separate cursor).
    std::vector<Placement> placements_;
    // Logical indexes carried over by inherit(), in source directory order. They
    // own no LogicalIndexWriter: their sections are already in the copied prefix.
    std::vector<InheritedGroup> inherited_;
    // Blob logical indexes registered by add_blob_index(), in add order. Their
    // bytes stream out during finish() only.
    std::vector<PendingBlobIndex> blobs_;
    // inherit() ran successfully. Distinct from inherited_ being non-empty: a
    // rewrite may drop every old index and still copy the bootstrap header.
    bool inherited_prefix_ = false;
    bool bootstrap_written_ = false;
    bool finished_ = false;
    Status failed_ = Status::OK();
};

} // namespace doris::snii::writer
