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

#include "snii/format/bsbf.h"

#include <cmath>

#include "snii/encoding/crc32c.h"

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#define SNII_BSBF_X86 1
#endif

#define XXH_INLINE_ALL
#include "xxhash.h"

namespace snii::format {
using doris::Status; // RETURN_IF_ERROR expands to bare Status

const uint32_t kBsbfSalt[kBsbfBitsSetPerBlock] = {0x47b6137bU, 0x44974d91U, 0x8824ad5bU,
                                                  0xa2b7289dU, 0x705495c7U, 0x2df1424bU,
                                                  0x9efc4947U, 0x5c6bfb31U};

namespace {

void store_le32(uint8_t* p, uint32_t v) {
    p[0] = static_cast<uint8_t>(v);
    p[1] = static_cast<uint8_t>(v >> 8);
    p[2] = static_cast<uint8_t>(v >> 16);
    p[3] = static_cast<uint8_t>(v >> 24);
}
uint32_t load_le32(const uint8_t* p) {
    return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}

bool cpu_has_avx2() {
#if defined(SNII_BSBF_X86)
    static const bool v = __builtin_cpu_supports("avx2");
    return v;
#else
    return false;
#endif
}

// --- scalar kernels ---
inline void masks_scalar(uint32_t key, uint32_t m[8]) {
    for (int i = 0; i < 8; ++i) m[i] = 1u << ((key * kBsbfSalt[i]) >> 27);
}
bool block_contains_scalar(uint64_t hash, const uint8_t* block) {
    const uint32_t* w = reinterpret_cast<const uint32_t*>(block); // LE
    uint32_t m[8];
    masks_scalar(static_cast<uint32_t>(hash), m);
    for (int i = 0; i < 8; ++i)
        if ((load_le32(reinterpret_cast<const uint8_t*>(w + i)) & m[i]) != m[i]) return false;
    return true;
}
void insert_scalar(uint32_t* words, uint32_t block, uint32_t key) {
    uint32_t m[8];
    masks_scalar(key, m);
    for (int i = 0; i < 8; ++i) words[block * 8 + i] |= m[i];
}
bool find_scalar(const uint32_t* words, uint32_t block, uint32_t key) {
    uint32_t m[8];
    masks_scalar(key, m);
    for (int i = 0; i < 8; ++i)
        if ((words[block * 8 + i] & m[i]) != m[i]) return false;
    return true;
}

#if defined(SNII_BSBF_X86)
// --- AVX2 kernels: a 256-bit block is one YMM register ---
__attribute__((target("avx2"))) __m256i mask_avx2(uint32_t key) {
    const __m256i salt =
            _mm256_setr_epi32(static_cast<int>(kBsbfSalt[0]), static_cast<int>(kBsbfSalt[1]),
                              static_cast<int>(kBsbfSalt[2]), static_cast<int>(kBsbfSalt[3]),
                              static_cast<int>(kBsbfSalt[4]), static_cast<int>(kBsbfSalt[5]),
                              static_cast<int>(kBsbfSalt[6]), static_cast<int>(kBsbfSalt[7]));
    const __m256i prod = _mm256_mullo_epi32(_mm256_set1_epi32(static_cast<int>(key)), salt);
    const __m256i shifts = _mm256_srli_epi32(prod, 27); // top 5 bits -> 0..31
    return _mm256_sllv_epi32(_mm256_set1_epi32(1), shifts);
}
__attribute__((target("avx2"))) bool block_contains_avx2(uint64_t hash, const uint8_t* block) {
    const __m256i m = mask_avx2(static_cast<uint32_t>(hash));
    const __m256i b = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(block));
    return _mm256_testc_si256(b, m) != 0; // (~b & m) == 0 -> b contains m
}
__attribute__((target("avx2"))) void insert_avx2(uint32_t* words, uint32_t block, uint32_t key) {
    __m256i* p = reinterpret_cast<__m256i*>(words + block * 8);
    _mm256_storeu_si256(p, _mm256_or_si256(_mm256_loadu_si256(p), mask_avx2(key)));
}
__attribute__((target("avx2"))) bool find_avx2(const uint32_t* words, uint32_t block,
                                               uint32_t key) {
    const __m256i m = mask_avx2(key);
    const __m256i b = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(words + block * 8));
    return _mm256_testc_si256(b, m) != 0;
}
#endif

} // namespace

uint64_t bsbf_hash(std::string_view term) {
    return XXH64(term.data(), term.size(), /*seed=*/0);
}

uint32_t bsbf_optimal_num_bytes(uint32_t ndv, double fpp) {
    // Parquet OptimalNumOfBits, then >>3 for bytes.
    const double m = -8.0 * ndv / std::log(1 - std::pow(fpp, 1.0 / 8));
    uint32_t num_bits;
    if (m < 0 || m > static_cast<double>(kBsbfMaxBytes) * 8) {
        num_bits = kBsbfMaxBytes << 3;
    } else {
        num_bits = static_cast<uint32_t>(m);
    }
    if (num_bits < (kBsbfMinBytes << 3)) num_bits = kBsbfMinBytes << 3;
    if (num_bits & (num_bits - 1)) { // next power of 2
        uint32_t p = 1;
        while (p < num_bits) p <<= 1;
        num_bits = p;
    }
    if (num_bits > (kBsbfMaxBytes << 3)) num_bits = kBsbfMaxBytes << 3;
    return num_bits >> 3;
}

bool bsbf_block_contains(uint64_t hash, const uint8_t block[kBsbfBytesPerBlock]) {
#if defined(SNII_BSBF_X86)
    if (cpu_has_avx2()) return block_contains_avx2(hash, block);
#endif
    return block_contains_scalar(hash, block);
}

doris::Status BsbfBuilder::create(uint32_t ndv, double fpp, BsbfBuilder* out) {
    if (out == nullptr) return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("bsbf: null out");
    if (!(fpp > 0.0 && fpp < 1.0)) return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("bsbf: fpp out of (0,1)");
    if (ndv == 0) ndv = 1;
    out->num_bytes_ = bsbf_optimal_num_bytes(ndv, fpp);
    out->num_blocks_ = out->num_bytes_ / kBsbfBytesPerBlock;
    out->ndv_ = ndv;
    out->words_.assign(out->num_bytes_ / 4, 0u);
    return doris::Status::OK();
}

void BsbfBuilder::insert(uint64_t hash) {
    const uint32_t block = bsbf_block_index(hash, num_blocks_);
    const uint32_t key = static_cast<uint32_t>(hash);
#if defined(SNII_BSBF_X86)
    if (cpu_has_avx2()) {
        insert_avx2(words_.data(), block, key);
        return;
    }
#endif
    insert_scalar(words_.data(), block, key);
}

bool BsbfBuilder::maybe_contains(uint64_t hash) const {
    const uint32_t block = bsbf_block_index(hash, num_blocks_);
    const uint32_t key = static_cast<uint32_t>(hash);
#if defined(SNII_BSBF_X86)
    if (cpu_has_avx2()) return find_avx2(words_.data(), block, key);
#endif
    return find_scalar(words_.data(), block, key);
}

doris::Status BsbfBuilder::serialize(ByteSink* sink) const {
    if (sink == nullptr) return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("bsbf: null sink");
    if (num_bytes_ == 0) return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("bsbf: not built");
    uint8_t hdr[kBsbfHeaderSize] = {0};
    hdr[0] = 'B';
    hdr[1] = 'S';
    hdr[2] = 'B';
    hdr[3] = 'F';
    hdr[4] = 1; // version
    hdr[5] = 0; // hash strategy: XXH64 seed 0
    hdr[6] = 0; // index strategy: fastrange
    hdr[7] = 0; // pad
    store_le32(hdr + 8, num_bytes_);
    store_le32(hdr + 12, num_blocks_);
    store_le32(hdr + 16, ndv_);
    store_le32(hdr + 20, crc32c(Slice(hdr, 20))); // header crc over [0,20)
    const uint8_t* bits = reinterpret_cast<const uint8_t*>(words_.data());
    store_le32(hdr + 24, crc32c(Slice(bits, num_bytes_))); // bitset crc
    sink->put_bytes(Slice(hdr, kBsbfHeaderSize));
    sink->put_bytes(Slice(bits, num_bytes_)); // contiguous, uncompressed, LE
    return doris::Status::OK();
}

doris::Status BsbfHeader::parse(Slice h, uint64_t section_base, BsbfHeader* out) {
    if (out == nullptr) return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("bsbf: null out");
    if (h.size() < kBsbfHeaderSize) return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("bsbf: short header");
    const uint8_t* p = h.data();
    if (p[0] != 'B' || p[1] != 'S' || p[2] != 'B' || p[3] != 'F')
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("bsbf: bad magic");
    if (p[4] != 1) return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("bsbf: bad version");
    if (p[5] != 0) return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("bsbf: unsupported hash strategy");
    if (p[6] != 0) return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("bsbf: unsupported index strategy");
    if (crc32c(Slice(p, 20)) != load_le32(p + 20))
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("bsbf: header crc mismatch");
    const uint32_t nb = load_le32(p + 8);
    const uint32_t nblk = load_le32(p + 12);
    if (nb < kBsbfMinBytes || nb > kBsbfMaxBytes || (nb & (nb - 1)) != 0)
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("bsbf: num_bytes out of range or not power of 2");
    if (nblk != nb / kBsbfBytesPerBlock) return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("bsbf: num_blocks mismatch");
    out->num_bytes = nb;
    out->num_blocks = nblk;
    out->bitset_crc = load_le32(p + 24);
    out->bitset_base = section_base + kBsbfHeaderSize;
    return doris::Status::OK();
}

doris::Status bsbf_probe(snii::io::FileReader* reader, const BsbfHeader& header, uint64_t hash,
                  bool* maybe_present) {
    if (reader == nullptr || maybe_present == nullptr)
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("bsbf: null arg");
    std::vector<uint8_t> blk;
    RETURN_IF_ERROR(reader->read_at(header.block_offset(hash), kBsbfBytesPerBlock, &blk));
    if (blk.size() < kBsbfBytesPerBlock) return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("bsbf: short block read");
    *maybe_present = bsbf_block_contains(hash, blk.data());
    return doris::Status::OK();
}

} // namespace snii::format
