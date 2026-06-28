#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "snii/common/slice.h"
#include "snii/common/status.h"
#include "snii/encoding/byte_sink.h"
#include "snii/format/logical_index_directory.h"

namespace snii::format {

struct TailMetaRegionHeader {
    uint64_t meta_region_len = 0;
    uint64_t directory_offset = 0;
    uint64_t directory_length = 0;
    uint32_t n_logical_indexes = 0;
};

size_t tail_meta_header_size();

// TailMetaRegion: the container's tail metadata region, located via the fixed
// tail pointer and read in one range. It bundles the per-logical-index meta
// blocks and the logical index directory so a reader can, after a single read,
// map (index_id, index_suffix) -> per-index meta block. See spec "footer meta
// region".
//
// On-disk layout (offsets are relative to the region start; the region is read
// whole into memory, so internal refs need not be file-absolute):
//   TailMetaHeader:
//     u32 meta_format_version (== kMetaFormatVersion)
//     u32 flags
//     u64 meta_region_len      (== total region byte length)
//     u32 n_logical_indexes
//     u64 directory_offset     (offset of the logical index directory in-region)
//     u64 directory_length
//     u32 header_crc32c        (covers the header fields above)
//   [per-index meta block #0][per-index meta block #1]...   (opaque payloads)
//   [logical index directory]  (framed via LogicalIndexDirectory)
//   u32 meta_region_checksum   (crc32c over everything before it)
class TailMetaRegionBuilder {
public:
    // Adds a per-index meta block (already serialized by PerIndexMetaBuilder) keyed
    // by (index_id, index_suffix). Bytes are copied.
    void add_index(uint64_t index_id, std::string index_suffix, Slice per_index_meta_bytes);

    // Serializes the whole region and appends it to sink.
    void finish(ByteSink* sink) const;

private:
    struct Entry {
        uint64_t index_id;
        std::string suffix;
        std::vector<uint8_t> bytes;
    };
    std::vector<Entry> entries_;
};

class TailMetaRegionReader {
public:
    TailMetaRegionReader() = default;

    // Parses and validates only the fixed tail-meta header. This is used by the
    // lazy segment open path to locate the directory without reading every
    // per-index meta block.
    static Status parse_header(Slice header, TailMetaRegionHeader* const out);

    // Parses and validates the region (header crc + region checksum + directory).
    // region must outlive this reader (find() returns sub-views of it).
    static Status open(Slice region, TailMetaRegionReader* const out);

    // Opens a reader from an already parsed header and a standalone framed
    // logical-index directory. This intentionally does not verify the whole-region
    // checksum because callers have not read the whole region; each per-index meta
    // block still carries its own framed-section CRCs.
    static Status open_directory(const TailMetaRegionHeader& header, Slice directory,
                                 TailMetaRegionReader* const out);

    uint32_t n_logical_indexes() const { return n_; }
    const LogicalIndexDirectoryReader& directory() const { return dir_; }

    Status find_ref(uint64_t index_id, std::string_view suffix, bool* const found,
                    LogicalIndexRef* const ref) const;

    // Locates the per-index meta block bytes for (index_id, suffix). On match,
    // *found=true and *per_index_meta_bytes views into the region; else *found=false.
    Status find(uint64_t index_id, std::string_view suffix, bool* const found,
                Slice* const per_index_meta_bytes) const;

private:
    Slice region_;
    LogicalIndexDirectoryReader dir_;
    uint64_t meta_region_len_ = 0;
    uint32_t n_ = 0;
};

} // namespace snii::format
