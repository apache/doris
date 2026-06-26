#pragma once

#include <cstdint>
#include <string_view>
#include <vector>

#include "snii/common/slice.h"
#include "snii/common/status.h"
#include "snii/format/tail_meta_region.h"
#include "snii/io/file_reader.h"
#include "snii/reader/logical_index_reader.h"

// SniiSegmentReader -- entry point for the SNII segment read path. It opens a
// single .idx container through a (possibly metered) io::FileReader and exposes
// its logical indexes. open() performs the minimal bootstrap reads:
//   1. the fixed bootstrap header (front of the file),
//   2. the fixed tail pointer (last tail_pointer_size() bytes), and
//   3. the tail meta region (one range read located via the tail pointer).
// The meta region bytes are held resident by the reader so per-index meta blocks
// (returned as sub-views) remain valid for the reader's lifetime.
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
    static Status open(snii::io::FileReader* reader, SniiSegmentReader* out);

    uint32_t n_logical_indexes() const { return region_reader_.n_logical_indexes(); }

    // Loads the per-index meta block for (index_id, suffix) and builds a
    // LogicalIndexReader bound to the same FileReader. Absent index -> NotFound.
    Status open_index(uint64_t index_id, std::string_view suffix, LogicalIndexReader* out) const;

    snii::io::FileReader* reader() const { return reader_; }

private:
    snii::io::FileReader* reader_ = nullptr;
    std::vector<uint8_t> meta_region_; // owned resident copy of the tail meta region
    snii::format::TailMetaRegionReader region_reader_;
};

} // namespace snii::reader
