#pragma once

#include <cstdint>
#include <string_view>
#include <vector>

#include "snii/common/slice.h"
#include "common/status.h"
#include "snii/format/per_index_meta.h"
#include "snii/format/tail_meta_region.h"
#include "snii/io/file_reader.h"
#include "snii/reader/logical_index_reader.h"

// SniiSegmentReader -- entry point for the SNII segment read path. It opens a
// single .idx container through a (possibly metered) io::FileReader and exposes
// its logical indexes. open() performs the minimal bootstrap reads:
//   1. the fixed bootstrap header (front of the file),
//   2. the fixed tail pointer (last tail_pointer_size() bytes), and
//   3. the tail meta header + logical-index directory.
// Per-index meta blocks are read lazily by open_index() so opening one logical
// index does not read every other logical index's metadata.
//
// open_index() then materializes one LogicalIndexReader from the per-index meta
// block of a given (index_id, suffix); query functions operate on that reader.
namespace snii::reader {

class SniiSegmentReader {
public:
    SniiSegmentReader() = default;

    // Reads bootstrap header + tail pointer + tail meta region from reader.
    // reader must outlive the returned SniiSegmentReader and every
    // LogicalIndexReader opened from it. reader == nullptr / out == nullptr ->
    // InvalidArgument; structural problems -> Corruption / Unsupported.
    static doris::Status open(snii::io::FileReader* const reader, SniiSegmentReader* const out);

    uint32_t n_logical_indexes() const { return region_reader_.n_logical_indexes(); }

    // Reads the per-index meta block bytes for (index_id, suffix). The returned
    // vector owns the exact meta block and may be passed to open_index_from_meta().
    doris::Status read_index_meta(uint64_t index_id, std::string_view suffix,
                           std::vector<uint8_t>* const out) const;
    doris::Status index_exists(uint64_t index_id, std::string_view suffix, bool* const exists) const;

    doris::Status open_index_from_meta(Slice meta_bytes, LogicalIndexReader* const out) const;

    // Loads the per-index meta block for (index_id, suffix) and builds a
    // LogicalIndexReader bound to the same FileReader. Absent index -> NotFound.
    doris::Status open_index(uint64_t index_id, std::string_view suffix,
                      LogicalIndexReader* const out) const;
    doris::Status section_refs_for_index(uint64_t index_id, std::string_view suffix,
                                  snii::format::SectionRefs* const out) const;

    snii::io::FileReader* reader() const { return reader_; }

private:
    snii::io::FileReader* reader_ = nullptr;
    uint64_t meta_region_offset_ = 0;
    uint64_t meta_region_length_ = 0;
    snii::format::TailMetaRegionReader region_reader_;
};

} // namespace snii::reader
