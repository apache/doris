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
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/format/core_metadata.h"
#include "storage/index/snii/format/metadata_directory.h"
#include "storage/index/snii/io/file_reader.h"
#include "storage/index/snii/reader/logical_index_reader.h"

// SniiSegmentReader -- entry point for the SNII segment read path. It opens a
// single .idx container through a (possibly metered) io::FileReader and exposes
// its logical indexes. open() reads only the file tail:
//   1. the fixed tail pointer (last tail_pointer_size() bytes), which also gates
//      the container format_version ('TAIL' magic + format_version exact-match +
//      tail crc), and
//   2. the raw protobuf logical-index metadata directory.
// The bootstrap header at offset 0 is still WRITTEN on disk (for inspect tooling)
// but is intentionally NOT read at open: its only runtime role (the container
// version gate) is already covered, more strictly, by the tail pointer, so
// skipping it avoids a redundant offset-0 cache block / remote round-trip per
// segment on cold queries.
// Per-index metadata groups are read lazily by open_index() so opening one logical
// index does not read every other logical index's metadata.
//
// open_index() then materializes one LogicalIndexReader from the metadata group
// of a given (index_id, suffix); query functions operate on that reader.
namespace doris::snii::reader {

// Identifies one logical index inside a container.
struct LogicalIndexKey {
    uint64_t index_id = 0;
    std::string index_suffix;
};

// One logical index a rewrite inherits unchanged from its source container.
struct InheritedLogicalIndex {
    uint64_t index_id = 0;
    std::string index_suffix;
    // Section references as recorded on disk. They stay valid in the rewritten
    // container because the physical prefix is copied to the SAME offsets.
    format::SectionRefs section_refs;
    uint64_t doc_count = 0;
    // The on-disk [Core][STI][DBD] run, verbatim. A rewrite re-emits these bytes
    // without decoding or re-encoding any postings.
    std::vector<uint8_t> metadata_group;
    size_t core_length = 0;
    size_t sampled_term_index_length = 0;
    size_t dict_block_directory_length = 0;
};

// Immutable, fully validated view of one container as an inheritance source for a
// rewrite (BUILD INDEX on SNII). It exposes only what the writer needs: how many
// leading bytes to copy, and the metadata of the logical indexes carried over.
// Encoding details stay inside the reader and the writer.
class SniiRewriteSnapshot {
public:
    SniiRewriteSnapshot() = default;

    // Copying [0, physical_prefix_end) reproduces the bootstrap header and every
    // physical section the inherited indexes reference. It never covers a metadata
    // group, the directory, padding or the tail.
    uint64_t physical_prefix_end() const { return physical_prefix_end_; }
    const std::vector<InheritedLogicalIndex>& inherited() const { return inherited_; }

private:
    friend class SniiSegmentReader;

    uint64_t physical_prefix_end_ = 0;
    std::vector<InheritedLogicalIndex> inherited_;
};

class SniiSegmentReader {
public:
    SniiSegmentReader() = default;

    // Reads the tail pointer + raw metadata directory from reader (the offset-0
    // bootstrap header is not read; the tail pointer gates the container version).
    // reader must outlive the returned SniiSegmentReader and every
    // LogicalIndexReader opened from it. reader == nullptr / out == nullptr ->
    // InvalidArgument; structural problems -> Corruption / Unsupported.
    static Status open(io::FileReader* const reader, SniiSegmentReader* const out);

    uint32_t n_logical_indexes() const { return static_cast<uint32_t>(directory_.size()); }

    Status index_exists(uint64_t index_id, std::string_view suffix, bool* const exists) const;

    // Loads the adjacent Core/STI/DBD group for (index_id, suffix) and builds a
    // LogicalIndexReader bound to the same FileReader. Absent index -> NotFound.
    Status open_index(uint64_t index_id, std::string_view suffix, LogicalIndexReader* const out,
                      LogicalIndexOpenMode open_mode = LogicalIndexOpenMode::kQuery) const;
    Status section_refs_for_index(uint64_t index_id, std::string_view suffix,
                                  format::SectionRefs* const out) const;

    // Looks up a BLOB logical index entry (kind != kInverted) and exposes its
    // validated directory entry (kind + named-file table). Absent -> NotFound;
    // a text inverted entry under that key -> Unsupported (kind mismatch, not
    // a lookup miss). The pointer stays valid for this reader's lifetime.
    Status blob_entry(uint64_t index_id, std::string_view suffix,
                      const format::LogicalIndexMetadataRef** out) const;

    // True when the container holds at least one blob logical index. A rewrite
    // driven by the text index list must consult this BEFORE deciding it has
    // nothing to carry over, or it would drop those entries silently.
    bool has_blob_index() const;

    // Builds a rewrite snapshot describing exactly the logical indexes in `keep`.
    // `segment_doc_count` is the segment's row count: every kept logical index must
    // agree with it. Fails -- never silently drops an index -- on a missing or
    // duplicated key, a corrupt bootstrap header or metadata blob, a section
    // reference outside the physical area, or a doc-count disagreement.
    Status prepare_rewrite_snapshot(const std::vector<LogicalIndexKey>& keep,
                                    uint64_t segment_doc_count,
                                    SniiRewriteSnapshot* const out) const;

    io::FileReader* reader() const { return reader_; }

    // Exclusive upper bound of the container's data area: physical sections,
    // metadata groups and blob files all live strictly before it. Pass this to
    // SniiBlobDirectory::open so the shim bounds blob files against the same
    // limit open() validated them with, not against the whole file size.
    uint64_t directory_offset() const { return directory_offset_; }

private:
    // One kept index of a rewrite snapshot; see the .cpp for the contract.
    Status load_inherited_index(const LogicalIndexKey& key, uint64_t segment_doc_count,
                                uint64_t metadata_area_begin, InheritedLogicalIndex* const out,
                                uint64_t* const physical_prefix_end) const;

    io::FileReader* reader_ = nullptr;
    // Start of the raw metadata directory. Together with the directory entries it
    // bounds the metadata area, which is where the physical section area ends.
    uint64_t directory_offset_ = 0;
    format::MetadataDirectory directory_;
};

} // namespace doris::snii::reader
