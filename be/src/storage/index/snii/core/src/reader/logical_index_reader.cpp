#include "snii/reader/logical_index_reader.h"

#include <cstdlib>
#include <limits>
#include <utility>
#include <vector>

#include "snii/encoding/crc32c.h"
#include "snii/encoding/zstd_codec.h"
#include "snii/format/dict_block.h"
#include "snii/format/dict_block_directory.h"

namespace snii::reader {
using doris::Status; // RETURN_IF_ERROR expands to bare Status

using snii::format::BlockRef;
using snii::format::bsbf_hash;
using snii::format::bsbf_probe;
using snii::format::DictBlockDirectoryReader;
using snii::format::DictBlockReader;
using snii::format::DictEntry;
using snii::format::IndexTier;
using snii::format::kBsbfBytesPerBlock;
using snii::format::kBsbfHeaderSize;
using snii::format::PerIndexMetaReader;
using snii::format::RegionRef;
using snii::format::SampledTermIndexReader;

namespace {
constexpr uint64_t kMaxDictBlockUncompBytes = 256ULL * 1024 * 1024;
constexpr uint64_t kDefaultDictResidentMaxBytes = 256ULL * 1024;

// L0/L1 tiering threshold (bytes). Defaults to kBsbfResidentMaxBytes; the env
// SNII_BSBF_RESIDENT_MAX overrides it for tuning and for exercising the
// on-demand L1 path in tests without a 250K-term corpus. Read fresh each open.
uint64_t bsbf_resident_max_bytes() {
    const char* s = std::getenv("SNII_BSBF_RESIDENT_MAX");
    if (s != nullptr) {
        char* end = nullptr;
        const unsigned long long v = std::strtoull(s, &end, 10);
        if (end != s) {
            return v;
        }
    }
    return snii::format::kBsbfResidentMaxBytes;
}

uint64_t dict_resident_max_bytes() {
    const char* s = std::getenv("SNII_DICT_RESIDENT_MAX");
    if (s != nullptr) {
        char* end = nullptr;
        const unsigned long long v = std::strtoull(s, &end, 10);
        if (end != s) {
            return v;
        }
    }
    return kDefaultDictResidentMaxBytes;
}

doris::Status checked_size(uint64_t value, const char* error, size_t* out) {
    if (value > std::numeric_limits<size_t>::max()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(error);
    }
    *out = static_cast<size_t>(value);
    return doris::Status::OK();
}

doris::Status dict_block_memory_bytes(const BlockRef& ref, uint64_t* out) {
    if ((ref.flags & snii::format::block_ref_flags::kZstd) == 0) {
        *out = ref.length;
        return doris::Status::OK();
    }
    if (ref.uncomp_len == 0 || ref.uncomp_len > kMaxDictBlockUncompBytes) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("dict block: zstd uncomp_len out of range");
    }
    *out = ref.uncomp_len;
    return doris::Status::OK();
}

doris::Status read_dict_block_bytes(snii::io::FileReader* reader, const BlockRef& ref,
                             std::vector<uint8_t>* out) {
    size_t read_len = 0;
    RETURN_IF_ERROR(
            checked_size(ref.length, "dict block: on-disk length out of range", &read_len));

    std::vector<uint8_t> block_bytes;
    RETURN_IF_ERROR(reader->read_at(ref.offset, read_len, &block_bytes));
    if (block_bytes.size() != read_len) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("dict block: short read");
    }

    if ((ref.flags & snii::format::block_ref_flags::kZstd) == 0) {
        *out = std::move(block_bytes);
        return doris::Status::OK();
    }

    uint64_t memory_bytes = 0;
    RETURN_IF_ERROR(dict_block_memory_bytes(ref, &memory_bytes));
    size_t uncomp_len = 0;
    RETURN_IF_ERROR(
            checked_size(memory_bytes, "dict block: zstd length out of range", &uncomp_len));
    return snii::zstd_decompress(Slice(block_bytes), uncomp_len, out);
}

doris::Status open_dict_block(snii::io::FileReader* reader, const BlockRef& ref, IndexTier tier,
                       bool has_positions, std::vector<uint8_t>* bytes, DictBlockReader* out) {
    RETURN_IF_ERROR(read_dict_block_bytes(reader, ref, bytes));
    return DictBlockReader::open(Slice(*bytes), tier, has_positions, out);
}
} // namespace

doris::Status LogicalIndexReader::load_resident_dict_blocks() {
    resident_dict_blocks_.clear();

    const uint64_t max_bytes = dict_resident_max_bytes();
    if (max_bytes == 0 || dbd_.n_blocks() == 0) {
        return doris::Status::OK();
    }

    uint64_t total_bytes = 0;
    for (uint32_t ord = 0; ord < dbd_.n_blocks(); ++ord) {
        BlockRef ref {};
        RETURN_IF_ERROR(dbd_.get(ord, &ref));
        uint64_t block_bytes = 0;
        RETURN_IF_ERROR(dict_block_memory_bytes(ref, &block_bytes));
        if (block_bytes > max_bytes - total_bytes) {
            return doris::Status::OK();
        }
        total_bytes += block_bytes;
    }

    resident_dict_blocks_.reserve(dbd_.n_blocks());
    for (uint32_t ord = 0; ord < dbd_.n_blocks(); ++ord) {
        BlockRef ref {};
        RETURN_IF_ERROR(dbd_.get(ord, &ref));
        ResidentDictBlock block;
        RETURN_IF_ERROR(
                open_dict_block(reader_, ref, tier_, has_positions_, &block.bytes, &block.reader));
        resident_dict_blocks_.push_back(std::move(block));
    }
    return doris::Status::OK();
}

doris::Status LogicalIndexReader::dict_block_reader_for_ordinal(uint32_t ordinal,
                                                         OnDemandDictBlock* on_demand,
                                                         const DictBlockReader** out) const {
    if (!resident_dict_blocks_.empty()) {
        if (resident_dict_blocks_.size() != dbd_.n_blocks() ||
            ordinal >= resident_dict_blocks_.size()) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("logical_index: incomplete resident dict");
        }
        *out = &resident_dict_blocks_[ordinal].reader;
        return doris::Status::OK();
    }

    BlockRef ref {};
    RETURN_IF_ERROR(dbd_.get(ordinal, &ref));
    RETURN_IF_ERROR(open_dict_block(reader_, ref, tier_, has_positions_, &on_demand->bytes,
                                         &on_demand->reader));
    *out = &on_demand->reader;
    return doris::Status::OK();
}

doris::Status LogicalIndexReader::open(snii::io::FileReader* file_reader, IndexTier tier,
                                bool has_positions, Slice meta_block, LogicalIndexReader* out) {
    if (file_reader == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("logical_index: null file reader");
    }
    if (out == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("logical_index: null out");
    }
    if (meta_block.empty()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("logical_index: empty meta block");
    }
    *out = LogicalIndexReader {};

    out->reader_ = file_reader;
    out->tier_ = tier;
    out->has_positions_ = has_positions;
    out->meta_block_.assign(meta_block.data(), meta_block.data() + meta_block.size());
    const Slice owned_meta(out->meta_block_);

    RETURN_IF_ERROR(PerIndexMetaReader::open(owned_meta, &out->meta_));
    RETURN_IF_ERROR(
            SampledTermIndexReader::open(out->meta_.sampled_term_index_bytes(), &out->sti_));
    RETURN_IF_ERROR(
            DictBlockDirectoryReader::open(out->meta_.dict_block_directory_bytes(), &out->dbd_));
    RETURN_IF_ERROR(out->load_resident_dict_blocks());

    // Block-split bloom XFilter. L0 reads the whole small filter so probes are
    // in-memory. L1 reads only the small header at open; the header is kept in
    // LogicalIndexReader and enters Doris searcher cache with the rest of the
    // logical-index metadata.
    const RegionRef& bsbf = out->meta_.section_refs().bsbf;
    if (bsbf.length > 0) {
        if (bsbf.length <= kBsbfHeaderSize) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("logical_index: bsbf section too small");
        }
        const uint64_t num_bytes = bsbf.length - kBsbfHeaderSize;
        const bool resident = bsbf.length <= bsbf_resident_max_bytes();
        std::vector<uint8_t> head;
        RETURN_IF_ERROR(
                file_reader->read_at(bsbf.offset, resident ? bsbf.length : kBsbfHeaderSize, &head));
        if (head.size() < kBsbfHeaderSize) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("logical_index: short bsbf header read");
        }
        RETURN_IF_ERROR(snii::format::BsbfHeader::parse(Slice(head.data(), kBsbfHeaderSize),
                                                             bsbf.offset, &out->bsbf_header_));
        // Cross-check the header geometry against the section ref.
        if (out->bsbf_header_.num_bytes != num_bytes) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("logical_index: bsbf header/section size mismatch");
        }
        out->has_bsbf_ = true;
        if (resident) {
            if (head.size() < bsbf.length) {
                return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("logical_index: short bsbf resident read");
            }
            const Slice bitset(head.data() + kBsbfHeaderSize, out->bsbf_header_.num_bytes);
            if (snii::crc32c(bitset) != out->bsbf_header_.bitset_crc) {
                return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("logical_index: bsbf bitset crc mismatch");
            }
            out->bsbf_resident_bitset_.assign(bitset.data(), bitset.data() + bitset.size());
            out->bsbf_resident_ = true;
        }
    }
    return doris::Status::OK();
}

size_t LogicalIndexReader::memory_usage() const {
    size_t bytes = sizeof(*this) + meta_block_.capacity() + bsbf_resident_bitset_.capacity();
    for (const auto& block : resident_dict_blocks_) {
        bytes += sizeof(block) + block.bytes.capacity();
    }
    return bytes;
}

doris::Status LogicalIndexReader::lookup(std::string_view term, bool* found, DictEntry* entry,
                                  uint64_t* frq_base, uint64_t* prx_base) const {
    *found = false;
    if (reader_ == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("logical_index: not opened");
    }

    // 1. XFilter fast rejection. DEFINITELY-ABSENT returns empty without the
    // DICT read. L0 probes the resident bitset; L1 reads one 32-byte block.
    if (has_bsbf_) {
        const uint64_t h = bsbf_hash(term);
        bool maybe = false;
        if (bsbf_resident_) {
            const uint32_t blk = snii::format::bsbf_block_index(h, bsbf_header_.num_blocks);
            maybe = snii::format::bsbf_block_contains(
                    h,
                    bsbf_resident_bitset_.data() + static_cast<size_t>(blk) * kBsbfBytesPerBlock);
        } else {
            RETURN_IF_ERROR(bsbf_probe(reader_, bsbf_header_, h, &maybe));
        }
        if (!maybe) {
            return doris::Status::OK();
        }
    }

    // 2. SampledTermIndex -> candidate block ordinal.
    bool maybe = false;
    uint32_t ordinal = 0;
    RETURN_IF_ERROR(sti_.locate(term, &maybe, &ordinal));
    if (!maybe) {
        return doris::Status::OK();
    }

    // 3. Use a resident small-DICT block when present; otherwise read the DICT
    //    block on demand and parse it with the same validation path used at open.
    const DictBlockReader* br = nullptr;
    OnDemandDictBlock on_demand;
    RETURN_IF_ERROR(dict_block_reader_for_ordinal(ordinal, &on_demand, &br));

    bool hit = false;
    RETURN_IF_ERROR(br->find_term(term, &hit, entry));
    if (!hit) {
        return doris::Status::OK();
    }

    *found = true;
    *frq_base = br->frq_base();
    *prx_base = br->prx_base();
    return doris::Status::OK();
}

doris::Status LogicalIndexReader::visit_prefix_terms(std::string_view prefix,
                                              const PrefixHitVisitor& visitor) const {
    if (!visitor) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("logical_index: null prefix visitor");
    }
    if (reader_ == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("logical_index: not opened");
    }

    // Seek the start block: the SampledTermIndex block whose first term <= prefix
    // (terms with `prefix` are >= prefix, so they begin in that block or later).
    // If the prefix sorts before every sample (or is empty), start at block 0.
    uint32_t start = 0;
    if (!prefix.empty()) {
        bool maybe = false;
        uint32_t ordinal = 0;
        RETURN_IF_ERROR(sti_.locate(prefix, &maybe, &ordinal));
        if (maybe) {
            start = ordinal;
        }
    }

    for (uint32_t ord = start; ord < dbd_.n_blocks(); ++ord) {
        const DictBlockReader* br = nullptr;
        OnDemandDictBlock on_demand;
        RETURN_IF_ERROR(dict_block_reader_for_ordinal(ord, &on_demand, &br));
        std::vector<DictEntry> entries;
        RETURN_IF_ERROR(br->decode_all(&entries));

        for (DictEntry& e : entries) {
            const std::string_view t(e.term);
            if (t < prefix) {
                continue; // not yet at the prefix range
            }
            const bool has_prefix =
                    t.size() >= prefix.size() && t.compare(0, prefix.size(), prefix) == 0;
            if (!has_prefix) {
                return doris::Status::OK(); // past the prefix range; sorted -> done
            }
            PrefixHit hit;
            hit.term = e.term;
            hit.entry = std::move(e);
            hit.frq_base = br->frq_base();
            hit.prx_base = br->prx_base();
            bool stop = false;
            RETURN_IF_ERROR(visitor(std::move(hit), &stop));
            if (stop) {
                return doris::Status::OK();
            }
        }
    }
    return doris::Status::OK();
}

doris::Status LogicalIndexReader::prefix_terms(std::string_view prefix, std::vector<PrefixHit>* const out,
                                        int32_t max_terms) const {
    if (out == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("logical_index: null out");
    }
    out->clear();
    return visit_prefix_terms(prefix, [&](PrefixHit&& hit, bool* stop) {
        out->push_back(std::move(hit));
        *stop = max_terms > 0 && out->size() >= static_cast<size_t>(max_terms);
        return doris::Status::OK();
    });
}

namespace {

// Validates a pod_ref window locator against the posting region and returns the
// absolute window range (after the prelude). Rejects corrupt locators rather
// than letting size_t underflow / uint64 overflow reach read_at.
doris::Status resolve_window(const snii::format::RegionRef& section, uint64_t base, uint64_t off_delta,
                      uint64_t total_len, uint64_t prelude_len, uint64_t* abs_off, uint64_t* len) {
    if (prelude_len > total_len) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("logical_index: prelude_len exceeds window len");
    }
    const uint64_t in_region = base + off_delta;
    if (in_region < base) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("logical_index: locator overflow");
    }
    if (in_region > section.length || total_len > section.length - in_region) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("logical_index: window past posting region");
    }
    *abs_off = section.offset + in_region + prelude_len;
    *len = total_len - prelude_len;
    return doris::Status::OK();
}

} // namespace

doris::Status LogicalIndexReader::resolve_frq_window(const snii::format::DictEntry& entry,
                                              uint64_t frq_base, uint64_t* abs_off,
                                              uint64_t* len) const {
    return resolve_window(section_refs().posting_region, frq_base, entry.frq_off_delta,
                          entry.frq_len, entry.prelude_len, abs_off, len);
}

doris::Status LogicalIndexReader::resolve_prx_window(const snii::format::DictEntry& entry,
                                              uint64_t prx_base, uint64_t* abs_off,
                                              uint64_t* len) const {
    // .prx windows carry no prelude (prelude_len = 0); both spans live in the
    // same posting region (prx span precedes frq span for the same term).
    return resolve_window(section_refs().posting_region, prx_base, entry.prx_off_delta,
                          entry.prx_len, 0, abs_off, len);
}

} // namespace snii::reader
