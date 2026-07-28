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

#include "storage/index/snii/reader/logical_index_reader.h"

#include <algorithm>
#include <cstdlib>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/encoding/section_framer.h"
#include "storage/index/snii/encoding/varint.h"
#include "storage/index/snii/encoding/zstd_codec.h"
#include "storage/index/snii/format/dict_block.h"
#include "storage/index/snii/format/dict_block_directory.h"
#include "storage/index/snii/format/metadata_blob.h"
#include "storage/index/snii/format/norms_pod.h"
#include "storage/index/snii/format/null_bitmap.h"
#include "storage/index/snii/io/batch_range_fetcher.h"
#include "storage/index/snii/reader/dict_block_cache.h"

namespace doris::snii::reader {

struct LogicalIndexReader::NormsCacheState {
    struct Data {
        std::vector<uint8_t> bytes;
        format::NormsPodReader reader;
    };

    std::mutex mutex;
    std::unique_ptr<Data> ready;
    std::shared_future<Status> in_flight;
};

using format::BlockRef;
using format::bsbf_hash;
using format::DictBlockDirectoryReader;
using format::DictBlockReader;
using format::DictEntry;
using format::IndexTier;
using format::kBsbfBytesPerBlock;
using format::kBsbfHeaderSize;
using format::RegionRef;
using format::SampledTermIndexReader;

namespace {
constexpr uint64_t kMaxDictBlockUncompBytes = 256ULL * 1024 * 1024;
constexpr uint64_t kDefaultDictResidentMaxBytes = 256ULL * 1024;
constexpr size_t kMaxDictLookupBatchRuns = 16;
constexpr uint64_t kMaxDictLookupBatchBytes = 4ULL * 1024 * 1024;
// Conservatively covers make_shared's control block and allocator bookkeeping
// for the state/data/vector allocations. The framed norms bytes are charged
// separately at their exact validated length.
constexpr size_t kNormsAllocationOverheadCharge = 128;

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
    return format::kBsbfResidentMaxBytes;
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

Status checked_size(uint64_t value, const char* error, size_t* out) {
    if (value > std::numeric_limits<size_t>::max()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(error);
    }
    *out = static_cast<size_t>(value);
    return Status::OK();
}

Status validate_norms_region(io::FileReader* reader, const RegionRef& norms, uint64_t doc_count,
                             size_t* length) {
    *length = 0;
    if (norms.length == 0) {
        return Status::OK();
    }
    const uint64_t file_size = reader->size();
    if (norms.offset > file_size || norms.length > file_size - norms.offset) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "logical_index: norms region past end of file");
    }
    if (doc_count > std::numeric_limits<uint32_t>::max()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "logical_index: norms doc count exceeds uint32");
    }
    const uint64_t payload_length = varint_len(doc_count) + doc_count;
    const uint64_t expected_length =
            1 + varint_len(payload_length) + payload_length + sizeof(uint32_t);
    if (norms.length != expected_length) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "logical_index: norms region length mismatch");
    }
    if (norms.length > std::numeric_limits<size_t>::max()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "logical_index: norms region exceeds cache charge bounds");
    }
    *length = static_cast<size_t>(norms.length);
    return Status::OK();
}

Status validate_null_bitmap_frame(Slice framed) {
    ByteSource source(framed);
    FramedSection section;
    RETURN_IF_ERROR(SectionFramer::read(source, &section));
    if (section.type != format::kNullBitmapSectionType) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "logical_index: invalid null bitmap section type");
    }
    if (source.remaining() != 0) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "logical_index: trailing null bitmap frame bytes");
    }

    // NullBitmapReader owns the Roaring validation. Parse only the two prefix
    // varints here to pin the region to exactly one complete payload; otherwise
    // a valid frame could silently carry ignored bytes after roaring_bytes.
    ByteSource payload(section.payload);
    uint64_t doc_count = 0;
    RETURN_IF_ERROR(payload.get_varint64(&doc_count));
    if (doc_count > std::numeric_limits<uint32_t>::max()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "logical_index: null bitmap doc count exceeds uint32");
    }
    uint64_t roaring_size = 0;
    RETURN_IF_ERROR(payload.get_varint64(&roaring_size));
    if (roaring_size != payload.remaining()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "logical_index: null bitmap payload length mismatch");
    }
    return Status::OK();
}

Status dict_block_memory_bytes(const BlockRef& ref, uint64_t* out) {
    if ((ref.flags & format::block_ref_flags::kZstd) == 0) {
        *out = ref.length;
        return Status::OK();
    }
    if (ref.uncomp_len == 0 || ref.uncomp_len > kMaxDictBlockUncompBytes) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "dict block: zstd uncomp_len out of range");
    }
    *out = ref.uncomp_len;
    return Status::OK();
}

Status checked_memory_add(uint64_t lhs, uint64_t rhs, const char* message, uint64_t* out) {
    if (rhs > std::numeric_limits<uint64_t>::max() - lhs) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(message);
    }
    *out = lhs + rhs;
    return Status::OK();
}

Status checked_memory_mul(uint64_t lhs, uint64_t rhs, const char* message, uint64_t* out) {
    if (lhs != 0 && rhs > std::numeric_limits<uint64_t>::max() / lhs) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(message);
    }
    *out = lhs * rhs;
    return Status::OK();
}

// Decompresses a zstd dict block from its on-disk bytes into *out. The decode
// buffer in zstd_decompress is resize_uninitialized'd (T19) then fully written.
Status zstd_decompress_dict_block(Slice on_disk, const BlockRef& ref, std::vector<uint8_t>* out) {
    uint64_t memory_bytes = 0;
    RETURN_IF_ERROR(dict_block_memory_bytes(ref, &memory_bytes));
    size_t uncomp_len = 0;
    RETURN_IF_ERROR(
            checked_size(memory_bytes, "dict block: zstd length out of range", &uncomp_len));
    return zstd_decompress(on_disk, uncomp_len, out);
}

// Materializes the usable (uncompressed) bytes of a dict block from a view over
// its on-disk bytes -- a raw block is copied, a zstd block is decompressed. Used
// by the resident single-range path, where on_disk is a sub-slice of the shared
// region buffer (so a raw block must be copied, not aliased).
Status decompress_dict_block_payload(Slice on_disk, const BlockRef& ref,
                                     std::vector<uint8_t>* out) {
    if ((ref.flags & format::block_ref_flags::kZstd) == 0) {
        out->assign(on_disk.data(), on_disk.data() + on_disk.size());
        return Status::OK();
    }
    return zstd_decompress_dict_block(on_disk, ref, out);
}

Status read_dict_block_bytes(io::FileReader* reader, const BlockRef& ref,
                             std::vector<uint8_t>* out) {
    size_t read_len = 0;
    RETURN_IF_ERROR(checked_size(ref.length, "dict block: on-disk length out of range", &read_len));

    std::vector<uint8_t> block_bytes;
    RETURN_IF_ERROR(reader->read_at(ref.offset, read_len, &block_bytes));
    if (block_bytes.size() != read_len) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "dict block: short read");
    }

    // Raw on-demand block: move the freshly read buffer in (no copy).
    if ((ref.flags & format::block_ref_flags::kZstd) == 0) {
        *out = std::move(block_bytes);
        return Status::OK();
    }
    return zstd_decompress_dict_block(Slice(block_bytes), ref, out);
}

Status open_dict_block(io::FileReader* reader, const BlockRef& ref, IndexTier tier,
                       bool has_positions, std::vector<uint8_t>* bytes, DictBlockReader* out) {
    RETURN_IF_ERROR(read_dict_block_bytes(reader, ref, bytes));
    return DictBlockReader::open(Slice(*bytes), tier, has_positions, out);
}

// Validates that block `ref` lies fully within dict_region and returns its byte
// range relative to the start of the region. Defends the single-range resident
// read against a corrupt directory ref (offset before the region, or a range
// that runs past it) before it is used to index the region buffer.
Status slice_dict_block_in_region(const BlockRef& ref, const RegionRef& dict_region,
                                  size_t region_len, size_t* rel_off, size_t* len) {
    if (ref.offset < dict_region.offset) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "dict block: ref before dict region");
    }
    size_t rel = 0;
    size_t block_len = 0;
    RETURN_IF_ERROR(
            checked_size(ref.offset - dict_region.offset, "dict block: ref offset OOR", &rel));
    RETURN_IF_ERROR(checked_size(ref.length, "dict block: ref length OOR", &block_len));
    if (rel > region_len || block_len > region_len - rel) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "dict block: ref past dict region");
    }
    *rel_off = rel;
    *len = block_len;
    return Status::OK();
}
} // namespace

Status LogicalIndexReader::load_resident_dict_blocks() {
    resident_dict_blocks_.clear();

    const uint64_t max_bytes = dict_resident_max_bytes();
    if (max_bytes == 0 || dbd_.n_blocks() == 0) {
        return Status::OK();
    }

    uint64_t total_bytes = 0;
    for (uint32_t ord = 0; ord < dbd_.n_blocks(); ++ord) {
        BlockRef ref {};
        RETURN_IF_ERROR(dbd_.get(ord, &ref));
        uint64_t block_bytes = 0;
        RETURN_IF_ERROR(dict_block_memory_bytes(ref, &block_bytes));
        if (block_bytes > max_bytes - total_bytes) {
            return Status::OK();
        }
        total_bytes += block_bytes;
    }

    // The resident blocks are physically contiguous within dict_region, so read
    // the whole region in a SINGLE range read (was one read_at per block -> up to
    // ~4 serial S3 rounds on a cold open) and decode each block from a sub-slice.
    // The region buffer is <= the resident byte cap (<=256KB) and freed on return;
    // each ResidentDictBlock keeps its own decoded copy.
    const RegionRef& dict_region = section_refs().dict_region;
    size_t region_len = 0;
    RETURN_IF_ERROR(
            checked_size(dict_region.length, "dict region: length out of range", &region_len));
    std::vector<uint8_t> region;
    RETURN_IF_ERROR(reader_->read_at(dict_region.offset, region_len, &region));
    if (region.size() != region_len) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "dict region: short read");
    }

    resident_dict_blocks_.reserve(dbd_.n_blocks());
    for (uint32_t ord = 0; ord < dbd_.n_blocks(); ++ord) {
        BlockRef ref {};
        RETURN_IF_ERROR(dbd_.get(ord, &ref));
        size_t rel_off = 0;
        size_t block_len = 0;
        RETURN_IF_ERROR(
                slice_dict_block_in_region(ref, dict_region, region_len, &rel_off, &block_len));
        const Slice on_disk(region.data() + rel_off, block_len);
        ResidentDictBlock block;
        RETURN_IF_ERROR(decompress_dict_block_payload(on_disk, ref, &block.bytes));
        RETURN_IF_ERROR(
                DictBlockReader::open(Slice(block.bytes), tier_, has_positions_, &block.reader));
        resident_dict_blocks_.push_back(std::move(block));
    }
    return Status::OK();
}

Status LogicalIndexReader::dict_block_reader_for_ordinal(
        uint32_t ordinal, DictBlockCache* cache, std::shared_ptr<const DecodedDictBlock>* pin,
        const DictBlockReader** out) const {
    pin->reset();
    if (!resident_dict_blocks_.empty()) {
        if (resident_dict_blocks_.size() != dbd_.n_blocks() ||
            ordinal >= resident_dict_blocks_.size()) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "logical_index: incomplete resident dict");
        }
        // Resident blocks live for the reader lifetime: no pin needed.
        *out = &resident_dict_blocks_[ordinal].reader;
        return Status::OK();
    }

    // On-demand: decode into a heap-allocated DecodedDictBlock held by *pin so the
    // reader's Slice never dangles. The loader (file read + optional zstd + CRC +
    // anchor parse) runs OUTSIDE any cache bookkeeping; on a cache hit it is not
    // called, so a block shared by several terms of one query decodes only once.
    DictBlockCache::Loader loader = [&](std::shared_ptr<const DecodedDictBlock>* slot) -> Status {
        BlockRef ref {};
        RETURN_IF_ERROR(dbd_.get(ordinal, &ref));
        auto block = std::make_shared<DecodedDictBlock>();
        RETURN_IF_ERROR(open_dict_block(reader_, ref, tier_, has_positions_, &block->bytes,
                                        &block->reader));
        *slot = std::move(block);
        return Status::OK();
    };
    if (cache != nullptr) {
        RETURN_IF_ERROR(cache->get_or_load(ordinal, loader, pin));
    } else {
        RETURN_IF_ERROR(loader(pin));
    }
    *out = &(*pin)->reader;
    return Status::OK();
}

Status LogicalIndexReader::load_resident_bsbf() {
    // Block-split bloom XFilter -- gated on RESIDENCY (P1 cold-read fix, see
    // docs/perf/P1-cold-read-amplification.md). The bloom is set up and used ONLY
    // when the whole (small) filter fits under the resident cap: it is read in
    // full, verified, and kept in memory so probes are in-memory and enter the
    // Doris searcher cache with the rest of the logical-index metadata.
    //
    // When NON-resident (the common case for a real text column, where the filter
    // is many MB) the bloom is skipped ENTIRELY: not even the 28B header is read,
    // and has_bsbf_ stays false. Every term then falls through to sti -> dict,
    // which yields the true found/absent. At 1 MiB cache-block granularity a
    // non-resident bloom never saves a physical block (an absent term still costs
    // one dict block either way), so its 28B header + per-term 32B probes were pure
    // cold read amplification.
    const RegionRef& bsbf = core_.section_refs.bsbf;
    if (open_mode_ != LogicalIndexOpenMode::kQuery || bsbf.length == 0 ||
        bsbf.length > bsbf_resident_max_bytes()) {
        return Status::OK();
    }
    if (bsbf.length <= kBsbfHeaderSize) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "logical_index: bsbf section too small");
    }
    const uint64_t num_bytes = bsbf.length - kBsbfHeaderSize;
    std::vector<uint8_t> head;
    RETURN_IF_ERROR(reader_->read_at(bsbf.offset, bsbf.length, &head));
    if (head.size() < bsbf.length) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "logical_index: short bsbf resident read");
    }
    RETURN_IF_ERROR(format::BsbfHeader::parse(Slice(head.data(), kBsbfHeaderSize), bsbf.offset,
                                              &bsbf_header_));
    // Cross-check the header geometry against the section ref.
    if (bsbf_header_.num_bytes != num_bytes) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "logical_index: bsbf header/section size mismatch");
    }
    const Slice bitset(head.data() + kBsbfHeaderSize, bsbf_header_.num_bytes);
    if (crc32c(bitset) != bsbf_header_.bitset_crc) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "logical_index: bsbf bitset crc mismatch");
    }
    bsbf_resident_bitset_.assign(bitset.data(), bitset.data() + bitset.size());
    has_bsbf_ = true;
    bsbf_resident_ = true;
    return Status::OK();
}

Status LogicalIndexReader::open(io::FileReader* file_reader, Slice core_frame, Slice sti_blob,
                                Slice dbd_blob, LogicalIndexReader* out,
                                LogicalIndexOpenMode open_mode) {
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("logical_index: null out");
    }
    *out = LogicalIndexReader {};
    if (file_reader == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("logical_index: null file reader");
    }
    if (core_frame.empty() || sti_blob.empty() || dbd_blob.empty()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "logical_index: empty mandatory metadata blob");
    }

    LogicalIndexReader candidate;
    candidate.reader_ = file_reader;
    candidate.open_mode_ = open_mode;
    RETURN_IF_ERROR(format::decode_core_metadata(core_frame, &candidate.core_));
    candidate.tier_ = format::tier_of(candidate.core_.index_config);
    candidate.has_positions_ = format::has_positions(candidate.core_.index_config);
    size_t norms_length = 0;
    RETURN_IF_ERROR(validate_norms_region(file_reader, candidate.core_.section_refs.norms,
                                          candidate.core_.stats.doc_count, &norms_length));
    if (norms_length != 0) {
        constexpr size_t fixed_charge = sizeof(NormsCacheState) + sizeof(NormsCacheState::Data) +
                                        kNormsAllocationOverheadCharge;
        if (norms_length > std::numeric_limits<size_t>::max() - fixed_charge) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "logical_index: norms cache charge overflow");
        }
        candidate.norms_reserved_charge_ = fixed_charge + norms_length;
        candidate.norms_cache_ = std::make_shared<NormsCacheState>();
    }
    // Raw frames alias the group read and compressed carriers materialize into
    // transient scratch. Both readers own their decoded state.
    {
        std::vector<uint8_t> scratch;
        Slice frame;
        RETURN_IF_ERROR(format::materialize_metadata_blob(
                sti_blob, format::SectionType::kSampledTermIndex,
                format::SectionType::kSampledTermIndexZstd, &scratch, &frame));
        RETURN_IF_ERROR(SampledTermIndexReader::open(frame, &candidate.sti_));
    }
    {
        std::vector<uint8_t> scratch;
        Slice frame;
        RETURN_IF_ERROR(format::materialize_metadata_blob(
                dbd_blob, format::SectionType::kDictBlockDirectory,
                format::SectionType::kDictBlockDirectoryZstd, &scratch, &frame));
        RETURN_IF_ERROR(DictBlockDirectoryReader::open(frame, &candidate.dbd_));
    }
    if (candidate.sti_.n_blocks() != candidate.dbd_.n_blocks()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "logical_index: sampled-term index and block directory count mismatch");
    }
    if (open_mode == LogicalIndexOpenMode::kQuery) {
        RETURN_IF_ERROR(candidate.load_resident_dict_blocks());
    }
    RETURN_IF_ERROR(candidate.load_resident_bsbf());
    *out = std::move(candidate);
    return Status::OK();
}

size_t LogicalIndexReader::memory_usage() const {
    size_t bytes = sizeof(*this) + bsbf_resident_bitset_.capacity();
    if (core_.common_grams_metadata) {
        const auto& common_grams = *core_.common_grams_metadata;
        bytes += format::std_string_heap_bytes(common_grams.common_grams_dictionary_identity);
        bytes += format::std_string_heap_bytes(common_grams.base_analyzer_fingerprint);
        bytes += format::std_string_heap_bytes(common_grams.common_grams_fingerprint);
    }
    bytes += sti_.heap_bytes();
    bytes += dbd_.heap_bytes();
    for (const auto& block : resident_dict_blocks_) {
        bytes += sizeof(block) + block.bytes.capacity() + block.reader.heap_bytes();
    }
    // Norms are loaded lazily, but the searcher-cache charge is fixed when this
    // reader is inserted. Reserve their complete framed size and heap state up
    // front so later allocation is already accounted for. Saturation prevents
    // an already-large resident metadata charge from wrapping around.
    if (norms_reserved_charge_ > std::numeric_limits<size_t>::max() - bytes) {
        return std::numeric_limits<size_t>::max();
    }
    bytes += norms_reserved_charge_;
    return bytes;
}

Status LogicalIndexReader::open_norms(format::NormsPodReader* out) const {
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "logical_index: null norms reader");
    }
    if (reader_ == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("logical_index: not opened");
    }
    const RegionRef& norms = section_refs().norms;
    if (norms.length == 0) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "logical_index: index has no norms");
    }
    DORIS_CHECK(norms_cache_ != nullptr);

    std::shared_future<Status> in_flight;
    std::shared_ptr<std::promise<Status>> completion;
    {
        std::lock_guard lock(norms_cache_->mutex);
        if (norms_cache_->ready != nullptr) {
            *out = norms_cache_->ready->reader;
            return Status::OK();
        }
        if (norms_cache_->in_flight.valid()) {
            in_flight = norms_cache_->in_flight;
        } else {
            completion = std::make_shared<std::promise<Status>>();
            in_flight = completion->get_future().share();
            norms_cache_->in_flight = in_flight;
        }
    }

    if (completion == nullptr) {
        const Status status = in_flight.get();
        RETURN_IF_ERROR(status);
        std::lock_guard lock(norms_cache_->mutex);
        DORIS_CHECK(norms_cache_->ready != nullptr);
        *out = norms_cache_->ready->reader;
        return Status::OK();
    }

    auto data = std::make_unique<NormsCacheState::Data>();
    const size_t read_len = static_cast<size_t>(norms.length);
    Status status = reader_->read_at(norms.offset, read_len, &data->bytes);
    if (status.ok() && data->bytes.size() != read_len) {
        status = Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "logical_index: short norms read");
    }
    if (status.ok()) {
        status = format::NormsPodReader::open(Slice(data->bytes), &data->reader);
    }
    {
        std::lock_guard lock(norms_cache_->mutex);
        if (status.ok()) {
            norms_cache_->ready = std::move(data);
            *out = norms_cache_->ready->reader;
        }
        norms_cache_->in_flight = std::shared_future<Status> {};
    }
    completion->set_value(status);
    return status;
}

void LogicalIndexReader::release_compaction_norms() const {
    DORIS_CHECK(open_mode_ == LogicalIndexOpenMode::kCompaction);
    DORIS_CHECK(norms_cache_ != nullptr);
    std::lock_guard lock(norms_cache_->mutex);
    DORIS_CHECK(!norms_cache_->in_flight.valid());
    norms_cache_->ready.reset();
}

Status LogicalIndexReader::read_null_docids(
        std::vector<uint32_t>* out, const NullDocidsDecodeReservation& reserve_decode) const {
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "logical_index: null null-docids output");
    }
    out->clear();
    if (reader_ == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("logical_index: not opened");
    }

    const RegionRef& ref = section_refs().null_bitmap;
    if (ref.length == 0) {
        if (stats().null_count != 0) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "logical_index: null bitmap section missing");
        }
        return Status::OK();
    }

    const uint64_t file_size = reader_->size();
    if (ref.offset > file_size || ref.length > file_size - ref.offset) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "logical_index: null bitmap region past end of file");
    }
    size_t read_len = 0;
    RETURN_IF_ERROR(
            checked_size(ref.length, "logical_index: null bitmap length out of range", &read_len));
    std::vector<uint8_t> bytes;
    RETURN_IF_ERROR(reader_->read_at(ref.offset, read_len, &bytes));
    if (bytes.size() != read_len) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "logical_index: short null bitmap read");
    }
    RETURN_IF_ERROR(validate_null_bitmap_frame(Slice(bytes)));

    uint64_t decoded_memory_bytes = 0;
    RETURN_IF_ERROR(
            format::NullBitmapReader::decoded_memory_bytes(Slice(bytes), &decoded_memory_bytes));
    if (reserve_decode) {
        RETURN_IF_ERROR(reserve_decode(decoded_memory_bytes));
    }

    format::NullBitmapReader null_bitmap_reader;
    RETURN_IF_ERROR(format::NullBitmapReader::open(Slice(bytes), &null_bitmap_reader));
    if (null_bitmap_reader.doc_count() != stats().doc_count) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "logical_index: null bitmap doc count mismatch");
    }
    if (null_bitmap_reader.null_count() != stats().null_count) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "logical_index: null bitmap cardinality mismatch");
    }

    out->reserve(null_bitmap_reader.null_count());
    null_bitmap_reader.append_docids(*out);
    for (uint32_t docid : *out) {
        if (docid >= stats().doc_count) {
            out->clear();
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "logical_index: null docid outside document domain");
        }
    }
    return Status::OK();
}

Status LogicalIndexReader::null_docids_scan_memory(NullDocidsScanMemory* out) const {
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "logical_index: null null-docids scan memory out");
    }
    *out = NullDocidsScanMemory {};
    if (reader_ == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("logical_index: not opened");
    }
    const RegionRef& ref = section_refs().null_bitmap;
    if (ref.length == 0) {
        if (stats().null_count != 0) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "logical_index: null bitmap section missing");
        }
        return Status::OK();
    }
    const uint64_t file_size = reader_->size();
    if (ref.offset > file_size || ref.length > file_size - ref.offset) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "logical_index: null bitmap region past end of file");
    }
    size_t frame_bytes = 0;
    RETURN_IF_ERROR(checked_size(ref.length, "logical_index: null bitmap length out of range",
                                 &frame_bytes));
    RETURN_IF_ERROR(checked_memory_mul(stats().null_count, sizeof(uint32_t),
                                       "logical_index: null docids output memory overflows",
                                       &out->output_bytes));

    out->frame_bytes = frame_bytes;
    return Status::OK();
}

Status LogicalIndexReader::lookup(std::string_view term, bool* found, DictEntry* entry,
                                  uint64_t* frq_base, uint64_t* prx_base,
                                  DictBlockCache* cache) const {
    *found = false;
    if (reader_ == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("logical_index: not opened");
    }

    bool maybe = false;
    uint32_t ordinal = 0;
    RETURN_IF_ERROR(locate_candidate_dict_block(term, &maybe, &ordinal));
    if (!maybe) {
        return Status::OK();
    }

    // Use a resident small-DICT block when present; otherwise read the DICT
    // block on demand and parse it with the same validation path used at open.
    // `pin` keeps an on-demand block alive through find_term (resident: null).
    const DictBlockReader* br = nullptr;
    std::shared_ptr<const DecodedDictBlock> pin;
    RETURN_IF_ERROR(dict_block_reader_for_ordinal(ordinal, cache, &pin, &br));

    bool hit = false;
    RETURN_IF_ERROR(br->find_term(term, &hit, entry));
    if (!hit) {
        return Status::OK();
    }

    *found = true;
    *frq_base = br->frq_base();
    *prx_base = br->prx_base();
    return Status::OK();
}

Status LogicalIndexReader::locate_candidate_dict_block(std::string_view term, bool* maybe_present,
                                                       uint32_t* ordinal) const {
    *maybe_present = false;
    // A DEFINITELY-ABSENT term returns without a DICT read. The bloom is
    // consulted only when resident; otherwise STI/DICT remains authoritative.
    if (has_bsbf_) {
        const uint64_t h = bsbf_hash(term);
        bool maybe = true;
        if (bsbf_resident_) {
            const uint32_t blk = format::bsbf_block_index(h, bsbf_header_.num_blocks);
            maybe = format::bsbf_block_contains(
                    h,
                    bsbf_resident_bitset_.data() + static_cast<size_t>(blk) * kBsbfBytesPerBlock);
        }
        if (!maybe) {
            return Status::OK();
        }
    }
    return sti_.locate(term, maybe_present, ordinal);
}

Status LogicalIndexReader::collect_batch_lookup_groups(
        const std::vector<std::string>& terms, std::vector<BatchLookupCandidate>* candidates,
        std::vector<BatchLookupGroup>* groups) const {
    candidates->clear();
    candidates->reserve(terms.size());
    for (size_t i = 0; i < terms.size(); ++i) {
        bool maybe = false;
        uint32_t ordinal = 0;
        RETURN_IF_ERROR(locate_candidate_dict_block(terms[i], &maybe, &ordinal));
        if (maybe) {
            candidates->push_back({i, ordinal});
        }
    }

    auto by_ordinal = [](const BatchLookupCandidate& lhs, const BatchLookupCandidate& rhs) {
        return lhs.ordinal < rhs.ordinal;
    };
    if (!std::ranges::is_sorted(*candidates, by_ordinal)) {
        std::ranges::sort(*candidates, by_ordinal);
    }

    groups->clear();
    for (size_t begin = 0; begin < candidates->size();) {
        const uint32_t ordinal = (*candidates)[begin].ordinal;
        size_t end = begin + 1;
        while (end < candidates->size() && (*candidates)[end].ordinal == ordinal) {
            ++end;
        }
        groups->push_back({ordinal, begin, end});
        begin = end;
    }
    return Status::OK();
}

Status LogicalIndexReader::resolve_batch_lookup_group(
        const std::vector<std::string>& terms, const std::vector<BatchLookupCandidate>& candidates,
        const BatchLookupGroup& group, const DictBlockReader& block_reader,
        std::vector<BatchLookupResult>* results) {
    for (size_t i = group.begin; i < group.end; ++i) {
        const size_t term_index = candidates[i].term_index;
        BatchLookupResult& result = (*results)[term_index];
        RETURN_IF_ERROR(block_reader.find_term(terms[term_index], &result.found, &result.entry));
        if (result.found) {
            result.frq_base = block_reader.frq_base();
            result.prx_base = block_reader.prx_base();
        }
    }
    return Status::OK();
}

Status LogicalIndexReader::lookup_batch_on_demand(
        const std::vector<std::string>& terms, const std::vector<BatchLookupCandidate>& candidates,
        const std::vector<BatchLookupGroup>& groups,
        std::vector<BatchLookupResult>* results) const {
    for (size_t wave_begin = 0; wave_begin < groups.size();) {
        std::vector<PendingBatchLookupBlock> pending;
        pending.reserve(kMaxDictLookupBatchRuns);
        uint64_t pending_bytes = 0;
        uint64_t pending_end = 0;
        size_t pending_runs = 0;
        size_t wave_end = wave_begin;
        while (wave_end < groups.size()) {
            BlockRef ref {};
            RETURN_IF_ERROR(dbd_.get(groups[wave_end].ordinal, &ref));
            const uint64_t ref_end = ref.offset + ref.length;
            const bool starts_new_run = pending.empty() || ref.offset > pending_end;
            if (!pending.empty() && ((starts_new_run && pending_runs == kMaxDictLookupBatchRuns) ||
                                     ref.length > kMaxDictLookupBatchBytes ||
                                     pending_bytes > kMaxDictLookupBatchBytes - ref.length)) {
                break;
            }
            pending.push_back({wave_end, ref, 0});
            pending_bytes += ref.length;
            if (starts_new_run) {
                ++pending_runs;
            }
            pending_end = std::max(pending_end, ref_end);
            ++wave_end;
        }
        DORIS_CHECK(!pending.empty());
        io::BatchRangeFetcher fetcher(reader_, /*coalesce_gap=*/0);
        for (PendingBatchLookupBlock& block : pending) {
            block.handle = fetcher.add(block.ref.offset, block.ref.length);
        }
        RETURN_IF_ERROR(fetcher.fetch());

        for (const PendingBatchLookupBlock& block : pending) {
            const Slice on_disk = fetcher.get(block.handle);
            std::vector<uint8_t> decoded;
            Slice payload = on_disk;
            if ((block.ref.flags & format::block_ref_flags::kZstd) != 0) {
                RETURN_IF_ERROR(zstd_decompress_dict_block(on_disk, block.ref, &decoded));
                payload = Slice(decoded);
            }
            DictBlockReader block_reader;
            RETURN_IF_ERROR(DictBlockReader::open(payload, tier_, has_positions_, &block_reader));
            RETURN_IF_ERROR(resolve_batch_lookup_group(terms, candidates, groups[block.group_index],
                                                       block_reader, results));
        }
        wave_begin = wave_end;
    }
    return Status::OK();
}

Status LogicalIndexReader::lookup_batch(const std::vector<std::string>& terms,
                                        std::vector<BatchLookupResult>* results) const {
    DORIS_CHECK(results != nullptr);
    DCHECK(std::ranges::is_sorted(terms));
    DCHECK(std::adjacent_find(terms.begin(), terms.end()) == terms.end());
    results->assign(terms.size(), BatchLookupResult {});
    if (reader_ == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("logical_index: not opened");
    }

    std::vector<BatchLookupCandidate> candidates;
    std::vector<BatchLookupGroup> groups;
    RETURN_IF_ERROR(collect_batch_lookup_groups(terms, &candidates, &groups));
    if (groups.empty()) {
        return Status::OK();
    }

    // Resident dictionaries always stay zero-I/O. One-block batches keep the
    // existing synchronous read-through path.
    if (!resident_dict_blocks_.empty() || groups.size() == 1) {
        for (const BatchLookupGroup& group : groups) {
            const DictBlockReader* block_reader = nullptr;
            std::shared_ptr<const DecodedDictBlock> pin;
            RETURN_IF_ERROR(dict_block_reader_for_ordinal(group.ordinal, /*cache=*/nullptr, &pin,
                                                          &block_reader));
            RETURN_IF_ERROR(
                    resolve_batch_lookup_group(terms, candidates, group, *block_reader, results));
        }
        return Status::OK();
    }

    return lookup_batch_on_demand(terms, candidates, groups, results);
}

Status LogicalIndexReader::decode_dict_block(uint32_t ordinal, std::vector<DictEntry>* entries,
                                             uint64_t* frq_base, uint64_t* prx_base) const {
    if (entries == nullptr || frq_base == nullptr || prx_base == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "logical_index: null decode_dict_block out");
    }
    if (reader_ == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("logical_index: not opened");
    }
    entries->clear();
    // Same resolution path lookup() uses (resident block or one on-demand range
    // read + zstd + CRC); `pin` keeps an on-demand block alive through
    // decode_all. No request-scoped cache: a sequential full scan touches each
    // block exactly once.
    const DictBlockReader* br = nullptr;
    std::shared_ptr<const DecodedDictBlock> pin;
    RETURN_IF_ERROR(dict_block_reader_for_ordinal(ordinal, /*cache=*/nullptr, &pin, &br));
    RETURN_IF_ERROR(br->decode_all(entries));
    *frq_base = br->frq_base();
    *prx_base = br->prx_base();
    return Status::OK();
}

Status LogicalIndexReader::dict_block_scan_memory(uint32_t ordinal,
                                                  DictBlockScanMemory* out) const {
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "logical_index: null dict block scan memory out");
    }
    *out = DictBlockScanMemory {};
    if (reader_ == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("logical_index: not opened");
    }

    BlockRef ref {};
    RETURN_IF_ERROR(dbd_.get(ordinal, &ref));
    uint64_t plain_bytes = 0;
    RETURN_IF_ERROR(dict_block_memory_bytes(ref, &plain_bytes));

    // During a compressed decode the fetched bytes coexist with the plain block.
    // The reader then owns the plain bytes plus anchor vectors/strings. Counting
    // every entry as an anchor is conservative for all valid anchor intervals.
    uint64_t anchor_slots = 0;
    RETURN_IF_ERROR(checked_memory_mul(ref.n_entries, sizeof(uint32_t) + sizeof(std::string),
                                       "logical_index: dict decode anchor memory overflows",
                                       &anchor_slots));
    uint64_t decode_bytes = 0;
    RETURN_IF_ERROR(checked_memory_add(ref.length, plain_bytes,
                                       "logical_index: dict decode bytes overflow", &decode_bytes));
    RETURN_IF_ERROR(checked_memory_add(decode_bytes, anchor_slots,
                                       "logical_index: dict decode slots overflow", &decode_bytes));
    RETURN_IF_ERROR(checked_memory_add(decode_bytes, plain_bytes,
                                       "logical_index: dict decode terms overflow", &decode_bytes));

    uint64_t entry_slots = 0;
    RETURN_IF_ERROR(checked_memory_mul(ref.n_entries, sizeof(DictEntry),
                                       "logical_index: dict entry slots overflow", &entry_slots));
    uint64_t entry_payload = 0;
    RETURN_IF_ERROR(checked_memory_mul(plain_bytes, 2, "logical_index: dict entry payload overflow",
                                       &entry_payload));
    RETURN_IF_ERROR(checked_memory_add(entry_slots, entry_payload,
                                       "logical_index: dict entries memory overflow",
                                       &out->entries_bytes));
    out->decode_bytes = decode_bytes;
    return Status::OK();
}

Status LogicalIndexReader::visit_prefix_terms(std::string_view prefix,
                                              const PrefixHitVisitor& visitor,
                                              DictBlockCache* cache) const {
    if (!visitor) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "logical_index: null prefix visitor");
    }
    if (reader_ == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("logical_index: not opened");
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
        std::shared_ptr<const DecodedDictBlock> pin;
        RETURN_IF_ERROR(dict_block_reader_for_ordinal(ord, cache, &pin, &br));

        // Stream this block's prefix range: anchor-jump past pre-prefix segments,
        // decode only the bodies we keep, and stop at the first term past the
        // range (decode_all materialized every entry of every scanned block).
        // The visitor still owns final term acceptance, so results are identical;
        // `br`/`pin` stay alive across this synchronous call.
        bool prefix_exhausted = false;
        bool visitor_stopped = false;
        RETURN_IF_ERROR(br->visit_prefix_range(
                prefix, /*accept_key=*/ {},
                [&](DictEntry&& e, bool* stop) -> Status {
                    PrefixHit hit;
                    hit.term = e.term;
                    hit.entry = std::move(e);
                    hit.frq_base = br->frq_base();
                    hit.prx_base = br->prx_base();
                    RETURN_IF_ERROR(visitor(std::move(hit), stop));
                    visitor_stopped = *stop;
                    return Status::OK();
                },
                &prefix_exhausted));
        if (visitor_stopped || prefix_exhausted) {
            return Status::OK();
        }
    }
    return Status::OK();
}

Status LogicalIndexReader::visit_term_range(std::string_view lower_inclusive,
                                            std::optional<std::string_view> upper_exclusive,
                                            const PrefixHitVisitor& visitor,
                                            DictBlockCache* cache) const {
    if (!visitor) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "logical_index: null range visitor");
    }
    if (reader_ == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("logical_index: not opened");
    }
    if (upper_exclusive.has_value() && *upper_exclusive <= lower_inclusive) {
        return Status::OK();
    }
    if (upper_exclusive.has_value()) {
        bool upper_reaches_dictionary = false;
        uint32_t upper_ordinal = 0;
        RETURN_IF_ERROR(sti_.locate(*upper_exclusive, &upper_reaches_dictionary, &upper_ordinal));
        if (!upper_reaches_dictionary) {
            return Status::OK();
        }
    }

    uint32_t start = 0;
    if (!lower_inclusive.empty()) {
        bool maybe = false;
        uint32_t ordinal = 0;
        RETURN_IF_ERROR(sti_.locate(lower_inclusive, &maybe, &ordinal));
        if (maybe) {
            start = ordinal;
        }
    }

    for (uint32_t ord = start; ord < dbd_.n_blocks(); ++ord) {
        const DictBlockReader* block_reader = nullptr;
        std::shared_ptr<const DecodedDictBlock> pin;
        RETURN_IF_ERROR(dict_block_reader_for_ordinal(ord, cache, &pin, &block_reader));

        bool range_exhausted = false;
        bool visitor_stopped = false;
        RETURN_IF_ERROR(block_reader->visit_term_range(
                lower_inclusive, upper_exclusive, /*accept_key=*/ {},
                [&](DictEntry&& entry, bool* stop) -> Status {
                    PrefixHit hit;
                    hit.term = entry.term;
                    hit.entry = std::move(entry);
                    hit.frq_base = block_reader->frq_base();
                    hit.prx_base = block_reader->prx_base();
                    RETURN_IF_ERROR(visitor(std::move(hit), stop));
                    visitor_stopped = *stop;
                    return Status::OK();
                },
                &range_exhausted));
        if (visitor_stopped || range_exhausted) {
            return Status::OK();
        }
    }
    return Status::OK();
}

Status LogicalIndexReader::prefix_terms(std::string_view prefix, std::vector<PrefixHit>* const out,
                                        int32_t max_terms, DictBlockCache* cache) const {
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("logical_index: null out");
    }
    out->clear();
    return visit_prefix_terms(
            prefix,
            [&](PrefixHit&& hit, bool* stop) {
                out->push_back(std::move(hit));
                *stop = max_terms > 0 && out->size() >= static_cast<size_t>(max_terms);
                return Status::OK();
            },
            cache);
}

namespace {

// Validates a pod_ref window locator against the posting region and returns the
// absolute window range (after the prelude). Rejects corrupt locators rather
// than letting size_t underflow / uint64 overflow reach read_at.
Status resolve_window(const format::RegionRef& section, uint64_t base, uint64_t off_delta,
                      uint64_t total_len, uint64_t prelude_len, uint64_t* abs_off, uint64_t* len) {
    if (prelude_len > total_len) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "logical_index: prelude_len exceeds window len");
    }
    const uint64_t in_region = base + off_delta;
    if (in_region < base) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "logical_index: locator overflow");
    }
    if (in_region > section.length || total_len > section.length - in_region) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "logical_index: window past posting region");
    }
    *abs_off = section.offset + in_region + prelude_len;
    *len = total_len - prelude_len;
    return Status::OK();
}

} // namespace

Status LogicalIndexReader::resolve_frq_window(const format::DictEntry& entry, uint64_t frq_base,
                                              uint64_t* abs_off, uint64_t* len) const {
    return resolve_window(section_refs().posting_region, frq_base, entry.frq_off_delta,
                          entry.frq_len, entry.prelude_len, abs_off, len);
}

Status LogicalIndexReader::resolve_prx_window(const format::DictEntry& entry, uint64_t prx_base,
                                              uint64_t* abs_off, uint64_t* len) const {
    // .prx windows carry no prelude (prelude_len = 0); both spans live in the
    // same posting region (prx span precedes frq span for the same term).
    return resolve_window(section_refs().posting_region, prx_base, entry.prx_off_delta,
                          entry.prx_len, 0, abs_off, len);
}

} // namespace doris::snii::reader
