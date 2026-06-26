#pragma once

#include <cstdint>
#include <string_view>
#include <vector>

#include "snii/common/slice.h"
#include "snii/common/status.h"
#include "snii/encoding/byte_sink.h"
#include "snii/io/file_reader.h"

// Block-split bloom filter (BSBF) -- Apache Parquet split-block spec, with an
// S3-native on-demand single-block probe that none of the reference implementations
// (Apache Parquet, Doris storage, Doris format/parquet) ship.
//
// BIT FORMAT IS PARQUET-CANONICAL (interoperable with Apache Parquet / Doris
// format/parquet for the bitset bytes):
//   - 256-bit (32-byte) blocks, 8 bits set per block.
//   - key = XXH64(term, seed=0); high 32 bits select the block via FASTRANGE
//     `block = ((hash>>32) * num_blocks) >> 32` (no power-of-2 requirement); low 32
//     bits select 8 in-block positions `1 << ((key * SALT[i]) >> 27)`.
//   - num_bytes via Parquet OptimalNumOfBytes: power of 2 in [32, 128 MiB].
//
// SNII WRAPPER (NOT Parquet's variable thrift header): a FIXED 28-byte header, then
// the contiguous, uncompressed, little-endian bitset. Because the header size is a
// constant, the bitset start is a constant offset (`section_base + 28`) and block i
// is at `section_base + 28 + i*32` -- so a single 32-byte block can be range-read on
// demand WITHOUT parsing a variable-length header and WITHOUT loading the whole blob.
namespace snii::format {

constexpr uint32_t kBsbfBytesPerBlock = 32;  // 256-bit block
constexpr uint32_t kBsbfBitsSetPerBlock = 8; // 8 uint32 words / block
constexpr uint32_t kBsbfMinBytes = 32;
constexpr uint32_t kBsbfMaxBytes = 128u * 1024 * 1024; // Parquet kMaximumBloomFilterBytes
constexpr uint32_t kBsbfHeaderSize = 28;               // FIXED (constant bitset offset)
// L0/L1 tiering threshold (design "不存在的term快速过滤"): a bsbf section whose total
// size is <= this is loaded WHOLE into the resident reader at open (L0 -> free
// in-memory probe, no per-lookup round); larger filters stay L1 (header-only, probed
// one 32-byte block on demand). 256 KiB fits in a single cloud FileCache block.
constexpr uint32_t kBsbfResidentMaxBytes = 256u * 1024;

// Canonical Parquet/Doris split-block SALT (8 odd 32-bit constants).
extern const uint32_t kBsbfSalt[kBsbfBitsSetPerBlock];

// XXH64(term, seed=0) -- the Parquet-canonical key (NOT XXH3, NOT Doris murmur).
uint64_t bsbf_hash(std::string_view term);

// Parquet OptimalNumOfBytes(ndv, fpp): power of 2 in [32, 128 MiB].
uint32_t bsbf_optimal_num_bytes(uint32_t ndv, double fpp);

// Fastrange block index from a 64-bit hash and the block count.
inline uint32_t bsbf_block_index(uint64_t hash, uint32_t num_blocks) {
    return static_cast<uint32_t>(((hash >> 32) * num_blocks) >> 32);
}

// Pure 32-byte-block kernel: does `block` contain the key's 8 bits? SIMD (AVX2)
// accelerated at runtime when available, scalar otherwise. Returns true => the term
// MAY be present (could be a false positive); false => DEFINITELY ABSENT.
bool bsbf_block_contains(uint64_t hash, const uint8_t block[kBsbfBytesPerBlock]);

// In-memory builder + serializer.
class BsbfBuilder {
public:
    BsbfBuilder() = default;

    // Sizes the filter for `ndv` distinct keys at target `fpp`. fpp in (0,1).
    static Status create(uint32_t ndv, double fpp, BsbfBuilder* out);

    // Insert a key / term. SIMD-accelerated.
    void insert(uint64_t hash);
    void insert_term(std::string_view term) { insert(bsbf_hash(term)); }

    // In-memory probe over the resident bitset (build/warm path). SIMD-accelerated.
    bool maybe_contains(uint64_t hash) const;
    bool maybe_contains_term(std::string_view term) const {
        return maybe_contains(bsbf_hash(term));
    }

    // Serialize [28-byte header][contiguous LE bitset] into `sink`. The header carries
    // magic/version/hash+index strategy/num_bytes/num_blocks/ndv + header & bitset
    // crc32c. The bitset is Parquet-canonical bytes.
    Status serialize(ByteSink* sink) const;

    uint32_t num_bytes() const { return num_bytes_; }
    uint32_t num_blocks() const { return num_blocks_; }

private:
    std::vector<uint32_t> words_; // num_bytes_/4, blocks of 8 words
    uint32_t num_bytes_ = 0;
    uint32_t num_blocks_ = 0;
    uint32_t ndv_ = 0;
};

// Resident header (28 bytes), parsed once at open. Validates magic/version/crc/bounds.
struct BsbfHeader {
    uint32_t num_bytes = 0;
    uint32_t num_blocks = 0;
    uint32_t bitset_crc = 0;  // stored crc32c of the bitset body (for L0 verification)
    uint64_t bitset_base = 0; // absolute file offset of block 0 = section_base + 28

    // Parse a 28-byte header located at `section_base` in the file. The bitset_base
    // is set to section_base + kBsbfHeaderSize.
    static Status parse(Slice header28, uint64_t section_base, BsbfHeader* out);

    // Absolute file offset of the 32-byte block this hash maps to.
    uint64_t block_offset(uint64_t hash) const {
        return bitset_base +
               static_cast<uint64_t>(bsbf_block_index(hash, num_blocks)) * kBsbfBytesPerBlock;
    }
};

// On-demand probe: read EXACTLY ONE 32-byte block via `reader`, then test. No whole
// blob load, no deep copy. *maybe_present=false means DEFINITELY ABSENT.
Status bsbf_probe(snii::io::FileReader* reader, const BsbfHeader& header, uint64_t hash,
                  bool* maybe_present);

} // namespace snii::format
