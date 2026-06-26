#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "snii/common/slice.h"
#include "snii/common/status.h"
#include "snii/encoding/byte_sink.h"

namespace snii::format {

// Container-level directory entry: maps a logical index identity (index_id, index_suffix)
// to the physical location of its per-index meta block. Aligned with Doris key system
// (see design spec "footer meta region" logical index directory). The reader issues a
// single range read over [meta_off, meta_off + meta_len) to load that per-index meta.
struct LogicalIndexRef {
    uint64_t index_id = 0;    // logical index id (matches Doris InvertedIndexDescriptor key)
    std::string index_suffix; // UTF-8 sub-index suffix; may be empty for the primary index
    uint64_t meta_off = 0;    // absolute byte offset of the per-index meta block in the container
    uint64_t meta_len = 0;    // byte length of the per-index meta block
};

// Logical index directory: (index_id, index_suffix) -> per-index meta block reference.
//
// on-disk layout (framed by SectionFramer with a unified type+len+crc32c wrapper):
//   [u8 type=kLogicalIndexDirectory][varint64 payload_len][payload][fixed32 crc32c]
//   payload = varint32 n_entries
//             then n_entries x {
//               varint64 index_id,
//               varint32 suffix_len, suffix_bytes,
//               varint64 per_index_meta_off,
//               varint64 per_index_meta_len }
// The section-level crc covers the whole directory, so no per-entry crc is stored
// (the spec lists a per-entry crc32c as optional; it is folded into the framer crc here).
class LogicalIndexDirectoryBuilder {
public:
    void add(const LogicalIndexRef& ref) { refs_.push_back(ref); }

    // Encodes as a kLogicalIndexDirectory framed section (with embedded crc32c) and appends to sink.
    void finish(ByteSink* sink) const;

private:
    std::vector<LogicalIndexRef> refs_;
};

// Reads and verifies a kLogicalIndexDirectory framed section; provides ordinal access and
// (index_id, suffix) lookup. After parsing, all entries reside in the reader (entering the
// searcher cache along with the rest of the tail meta region).
class LogicalIndexDirectoryReader {
public:
    // Verifies the section crc and deserializes all entries.
    // crc mismatch / truncation / trailing bytes / oversized counts -> kCorruption;
    // wrong section type -> kInvalidArgument; null out -> kInvalidArgument.
    static Status open(Slice framed, LogicalIndexDirectoryReader* out);

    uint32_t size() const { return static_cast<uint32_t>(refs_.size()); }

    // Returns the i-th entry in encounter order; i >= size -> kNotFound.
    Status get(uint32_t i, LogicalIndexRef* out) const;

    // Looks up the entry for (index_id, suffix). On match, *found=true and *out is populated;
    // when absent, *found=false and *out is left untouched. Returns kInvalidArgument on null
    // output pointers. The pair (index_id, suffix) is the unique key.
    Status find(uint64_t index_id, std::string_view suffix, bool* found,
                LogicalIndexRef* out) const;

private:
    std::vector<LogicalIndexRef> refs_;
};

} // namespace snii::format
