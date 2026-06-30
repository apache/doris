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
#include "storage/index/snii/format/per_index_meta.h"
#include "storage/index/snii/format/tail_meta_region.h"
#include "storage/index/snii/io/file_reader.h"
#include "storage/index/snii/reader/logical_index_reader.h"

// SniiSegmentReader -- entry point for the SNII segment read path. It opens a
// single .idx container through a (possibly metered) io::FileReader and exposes
// its logical indexes. open() reads only the file tail:
//   1. the fixed tail pointer (last tail_pointer_size() bytes), which also gates
//      the container format_version ('TAIL' magic + format_version exact-match +
//      tail crc), and
//   2. the tail meta header + logical-index directory.
// The bootstrap header at offset 0 is still WRITTEN on disk (for inspect tooling)
// but is intentionally NOT read at open: its only runtime role (the container
// version gate) is already covered, more strictly, by the tail pointer, so
// skipping it avoids a redundant offset-0 cache block / remote round-trip per
// segment on cold queries.
// Per-index meta blocks are read lazily by open_index() so opening one logical
// index does not read every other logical index's metadata.
//
// open_index() then materializes one LogicalIndexReader from the per-index meta
// block of a given (index_id, suffix); query functions operate on that reader.
namespace doris::snii::reader {

class SniiSegmentReader {
public:
    SniiSegmentReader() = default;

    // Reads the tail pointer + tail meta region from reader (the offset-0
    // bootstrap header is not read; the tail pointer gates the container version).
    // reader must outlive the returned SniiSegmentReader and every
    // LogicalIndexReader opened from it. reader == nullptr / out == nullptr ->
    // InvalidArgument; structural problems -> Corruption / Unsupported.
    static Status open(io::FileReader* const reader, SniiSegmentReader* const out);

    uint32_t n_logical_indexes() const { return region_reader_.n_logical_indexes(); }

    // Reads the per-index meta block bytes for (index_id, suffix). The returned
    // vector owns the exact meta block and may be passed to open_index_from_meta().
    Status read_index_meta(uint64_t index_id, std::string_view suffix,
                           std::vector<uint8_t>* const out) const;
    Status index_exists(uint64_t index_id, std::string_view suffix, bool* const exists) const;

    Status open_index_from_meta(Slice meta_bytes, LogicalIndexReader* const out) const;

    // Loads the per-index meta block for (index_id, suffix) and builds a
    // LogicalIndexReader bound to the same FileReader. Absent index -> NotFound.
    Status open_index(uint64_t index_id, std::string_view suffix,
                      LogicalIndexReader* const out) const;
    Status section_refs_for_index(uint64_t index_id, std::string_view suffix,
                                  format::SectionRefs* const out) const;

    io::FileReader* reader() const { return reader_; }

private:
    io::FileReader* reader_ = nullptr;
    uint64_t meta_region_offset_ = 0;
    uint64_t meta_region_length_ = 0;
    format::TailMetaRegionReader region_reader_;
};

} // namespace doris::snii::reader
