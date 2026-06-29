#include "snii/reader/snii_segment_reader.h"

#include <cstdint>
#include <vector>

#include "snii/encoding/crc32c.h"
#include "snii/format/bootstrap_header.h"
#include "snii/format/format_constants.h"
#include "snii/format/per_index_meta.h"
#include "snii/format/stats_block.h"
#include "snii/format/tail_pointer.h"

namespace snii::reader {
using doris::Status; // RETURN_IF_ERROR expands to bare Status

using snii::format::BootstrapHeader;
using snii::format::IndexTier;
using snii::format::PerIndexMetaReader;
using snii::format::StatsBlock;
using snii::format::TailMetaRegionReader;
using snii::format::TailPointer;

namespace {

// Reads the bootstrap header from the front of the file and validates it.
doris::Status ReadBootstrap(snii::io::FileReader* reader, BootstrapHeader* bh) {
    std::vector<uint8_t> buf;
    RETURN_IF_ERROR(reader->read_at(0, snii::format::kBootstrapHeaderSize, &buf));
    return snii::format::decode_bootstrap_header(Slice(buf), bh);
}

// Reads the fixed tail pointer (last tail_pointer_size() bytes) of the file.
doris::Status ReadTailPointer(snii::io::FileReader* reader, TailPointer* tp) {
    const size_t tp_size = snii::format::tail_pointer_size();
    const uint64_t total = reader->size();
    if (total < tp_size) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("segment: file smaller than tail pointer");
    }
    std::vector<uint8_t> buf;
    RETURN_IF_ERROR(reader->read_at(total - tp_size, tp_size, &buf));
    return snii::format::decode_tail_pointer(Slice(buf), tp);
}

doris::Status ReadTailMetaHeader(snii::io::FileReader* reader, const TailPointer& tp,
                          snii::format::TailMetaRegionHeader* header) {
    const size_t header_size = snii::format::tail_meta_header_size();
    if (tp.meta_region_length < header_size) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("segment: tail meta region smaller than header");
    }
    std::vector<uint8_t> buf;
    RETURN_IF_ERROR(reader->read_at(tp.meta_region_offset, header_size, &buf));
    return snii::format::TailMetaRegionReader::parse_header(Slice(buf), header);
}

} // namespace

doris::Status SniiSegmentReader::open(snii::io::FileReader* const reader, SniiSegmentReader* const out) {
    if (reader == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("segment: null reader");
    }
    if (out == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("segment: null out");
    }

    BootstrapHeader bh;
    RETURN_IF_ERROR(ReadBootstrap(reader, &bh));

    TailPointer tp;
    RETURN_IF_ERROR(ReadTailPointer(reader, &tp));
    if (tp.meta_region_length == 0) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("segment: empty tail meta region");
    }

    snii::format::TailMetaRegionHeader meta_header;
    RETURN_IF_ERROR(ReadTailMetaHeader(reader, tp, &meta_header));
    if (meta_header.meta_region_len != tp.meta_region_length) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("segment: tail meta length mismatch");
    }

    std::vector<uint8_t> directory;
    if (meta_header.directory_offset > UINT64_MAX - tp.meta_region_offset) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("segment: tail meta directory file offset overflow");
    }
    RETURN_IF_ERROR(reader->read_at(tp.meta_region_offset + meta_header.directory_offset,
                                         meta_header.directory_length, &directory));

    out->reader_ = reader;
    out->meta_region_offset_ = tp.meta_region_offset;
    out->meta_region_length_ = tp.meta_region_length;
    return TailMetaRegionReader::open_directory(meta_header, Slice(directory),
                                                &out->region_reader_);
}

doris::Status SniiSegmentReader::read_index_meta(uint64_t index_id, std::string_view suffix,
                                          std::vector<uint8_t>* const out) const {
    if (out == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("segment: null meta out");
    }
    if (reader_ == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("segment: not opened");
    }

    bool found = false;
    snii::format::LogicalIndexRef ref;
    RETURN_IF_ERROR(region_reader_.find_ref(index_id, suffix, &found, &ref));
    if (!found) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_SNII_NOT_FOUND, false>("segment: logical index not found");
    }
    if (ref.meta_off > meta_region_length_ || ref.meta_len > meta_region_length_ - ref.meta_off) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("segment: logical index meta out of tail region");
    }
    if (ref.meta_off > UINT64_MAX - meta_region_offset_) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("segment: logical index meta file offset overflow");
    }
    RETURN_IF_ERROR(reader_->read_at(meta_region_offset_ + ref.meta_off, ref.meta_len, out));
    return doris::Status::OK();
}

doris::Status SniiSegmentReader::index_exists(uint64_t index_id, std::string_view suffix,
                                       bool* const exists) const {
    if (exists == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("segment: null exists out");
    }
    if (reader_ == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("segment: not opened");
    }

    snii::format::LogicalIndexRef ref;
    return region_reader_.find_ref(index_id, suffix, exists, &ref);
}

doris::Status SniiSegmentReader::open_index_from_meta(Slice meta_bytes,
                                               LogicalIndexReader* const out) const {
    if (out == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("segment: null index out");
    }
    if (reader_ == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("segment: not opened");
    }
    // Determine tier / positions capability from the per-index meta. Positions
    // capability is read from the PERSISTED header flag (kHasPositions), NOT from
    // any region length: after the frq/prx merge, posting_region.length is non-zero
    // for ANY index with a pod_ref term -- docs-only included -- so a region-length
    // heuristic would mis-classify a docs-only index as positional and make
    // DictBlockReader::check_flags hard-fail. The "|| has_norms" is kept only as a
    // defensive belt-and-suspenders (a scoring index always has positions).
    PerIndexMetaReader meta;
    RETURN_IF_ERROR(PerIndexMetaReader::open(meta_bytes, &meta));
    const bool has_norms = meta.section_refs().norms.length > 0;
    const bool has_positions = meta.has_positions() || has_norms;
    IndexTier tier = IndexTier::kT1;
    if (has_norms) {
        tier = IndexTier::kT3;
    } else if (has_positions) {
        tier = IndexTier::kT2;
    }

    return LogicalIndexReader::open(reader_, tier, has_positions, meta_bytes, out);
}

doris::Status SniiSegmentReader::open_index(uint64_t index_id, std::string_view suffix,
                                     LogicalIndexReader* const out) const {
    std::vector<uint8_t> meta_bytes;
    RETURN_IF_ERROR(read_index_meta(index_id, suffix, &meta_bytes));
    return open_index_from_meta(Slice(meta_bytes), out);
}

doris::Status SniiSegmentReader::section_refs_for_index(uint64_t index_id, std::string_view suffix,
                                                 snii::format::SectionRefs* const out) const {
    if (out == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("segment: null section refs out");
    }
    if (reader_ == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("segment: not opened");
    }

    std::vector<uint8_t> meta_bytes;
    RETURN_IF_ERROR(read_index_meta(index_id, suffix, &meta_bytes));
    PerIndexMetaReader meta;
    RETURN_IF_ERROR(PerIndexMetaReader::open(Slice(meta_bytes), &meta));
    *out = meta.section_refs();
    return doris::Status::OK();
}

} // namespace snii::reader
