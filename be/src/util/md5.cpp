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

#include "util/md5.h"

#include <algorithm>
#include <cstring>
#include <vector>

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include "exec/common/endian.h"

namespace doris {

namespace {

constexpr uint32_t MD5_A0 = 0x67452301;
constexpr uint32_t MD5_B0 = 0xefcdab89;
constexpr uint32_t MD5_C0 = 0x98badcfe;
constexpr uint32_t MD5_D0 = 0x10325476;
constexpr unsigned char MD5_DUMMY_INPUT = 0;

void md5_to_hex(const unsigned char* digest, char* out) {
    static constexpr char DIGITS[] = "0123456789abcdef";
    for (int i = 0; i < MD5_DIGEST_LENGTH; ++i) {
        *out++ = DIGITS[digest[i] >> 4];
        *out++ = DIGITS[digest[i] & 0x0F];
    }
}

size_t md5_num_blocks(size_t len) {
    return (len + 9 + 63) / 64;
}

size_t md5_pad_final_blocks(const unsigned char* data, size_t len, unsigned char* out) {
    size_t full_blocks = len / 64;
    size_t tail = len % 64;
    size_t num_blocks = md5_num_blocks(len);
    size_t final_count = num_blocks - full_blocks;

    std::memset(out, 0, final_count * 64);
    std::memcpy(out, data + full_blocks * 64, tail);
    out[tail] = 0x80;
    LittleEndian::Store64(out + final_count * 64 - 8, static_cast<uint64_t>(len) * 8);

    return final_count;
}

#ifdef __AVX2__

struct AVX2MD5Ops {
    using Vec = __m256i;
    static constexpr size_t LANES = 8;

    static Vec add(Vec a, Vec b) { return _mm256_add_epi32(a, b); }

    static Vec set1(uint32_t v) { return _mm256_set1_epi32(static_cast<int>(v)); }

    static Vec loadu(const void* p) {
        return _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p));
    }

    static void storeu(void* p, Vec v) { _mm256_storeu_si256(reinterpret_cast<__m256i*>(p), v); }

    template <int N>
    static Vec rotl(Vec x) {
        return _mm256_or_si256(_mm256_slli_epi32(x, N), _mm256_srli_epi32(x, 32 - N));
    }

    static Vec F(Vec b, Vec c, Vec d) {
        return _mm256_xor_si256(d, _mm256_and_si256(b, _mm256_xor_si256(c, d)));
    }

    static Vec G(Vec b, Vec c, Vec d) {
        return _mm256_xor_si256(c, _mm256_and_si256(d, _mm256_xor_si256(b, c)));
    }

    static Vec H(Vec b, Vec c, Vec d) { return _mm256_xor_si256(b, _mm256_xor_si256(c, d)); }

    static Vec I(Vec b, Vec c, Vec d) {
        return _mm256_xor_si256(c, _mm256_or_si256(b, _mm256_xor_si256(d, _mm256_set1_epi32(-1))));
    }

    static void gather_all_message_words(const unsigned char* const block_ptrs[], Vec msg[16]) {
        for (int half = 0; half < 2; ++half) {
            size_t off = half * 32;
            Vec r0 = loadu(block_ptrs[0] + off);
            Vec r1 = loadu(block_ptrs[1] + off);
            Vec r2 = loadu(block_ptrs[2] + off);
            Vec r3 = loadu(block_ptrs[3] + off);
            Vec r4 = loadu(block_ptrs[4] + off);
            Vec r5 = loadu(block_ptrs[5] + off);
            Vec r6 = loadu(block_ptrs[6] + off);
            Vec r7 = loadu(block_ptrs[7] + off);

            Vec t0 = _mm256_unpacklo_epi32(r0, r1);
            Vec t1 = _mm256_unpackhi_epi32(r0, r1);
            Vec t2 = _mm256_unpacklo_epi32(r2, r3);
            Vec t3 = _mm256_unpackhi_epi32(r2, r3);
            Vec t4 = _mm256_unpacklo_epi32(r4, r5);
            Vec t5 = _mm256_unpackhi_epi32(r4, r5);
            Vec t6 = _mm256_unpacklo_epi32(r6, r7);
            Vec t7 = _mm256_unpackhi_epi32(r6, r7);

            Vec u0 = _mm256_unpacklo_epi64(t0, t2);
            Vec u1 = _mm256_unpackhi_epi64(t0, t2);
            Vec u2 = _mm256_unpacklo_epi64(t1, t3);
            Vec u3 = _mm256_unpackhi_epi64(t1, t3);
            Vec u4 = _mm256_unpacklo_epi64(t4, t6);
            Vec u5 = _mm256_unpackhi_epi64(t4, t6);
            Vec u6 = _mm256_unpacklo_epi64(t5, t7);
            Vec u7 = _mm256_unpackhi_epi64(t5, t7);

            size_t base = half * 8;
            msg[base + 0] = _mm256_permute2x128_si256(u0, u4, 0x20);
            msg[base + 4] = _mm256_permute2x128_si256(u0, u4, 0x31);
            msg[base + 1] = _mm256_permute2x128_si256(u1, u5, 0x20);
            msg[base + 5] = _mm256_permute2x128_si256(u1, u5, 0x31);
            msg[base + 2] = _mm256_permute2x128_si256(u2, u6, 0x20);
            msg[base + 6] = _mm256_permute2x128_si256(u2, u6, 0x31);
            msg[base + 3] = _mm256_permute2x128_si256(u3, u7, 0x20);
            msg[base + 7] = _mm256_permute2x128_si256(u3, u7, 0x31);
        }
    }
};

#define MD5_STEP_X2(func, w1, x1, y1, z1, w2, x2, y2, z2, g, s, ti) \
    {                                                               \
        Vec t1 = Ops::func(x1, y1, z1);                             \
        Vec t2 = Ops::func(x2, y2, z2);                             \
        t1 = Ops::add(t1, w1);                                      \
        t2 = Ops::add(t2, w2);                                      \
        Vec k = Ops::set1(ti);                                      \
        t1 = Ops::add(t1, k);                                       \
        t2 = Ops::add(t2, k);                                       \
        t1 = Ops::add(t1, msg1[g]);                                 \
        t2 = Ops::add(t2, msg2[g]);                                 \
        (w1) = Ops::add(x1, Ops::template rotl<s>(t1));             \
        (w2) = Ops::add(x2, Ops::template rotl<s>(t2));             \
    }

template <typename Ops>
struct MD5X2State {
    typename Ops::Vec a1, b1, c1, d1, a2, b2, c2, d2;
};

template <typename Ops>
MD5X2State<Ops> md5_multi_buffer_block_x2(typename Ops::Vec a1, typename Ops::Vec b1,
                                          typename Ops::Vec c1, typename Ops::Vec d1,
                                          typename Ops::Vec a2, typename Ops::Vec b2,
                                          typename Ops::Vec c2, typename Ops::Vec d2,
                                          const typename Ops::Vec msg1[16],
                                          const typename Ops::Vec msg2[16]) {
    using Vec = typename Ops::Vec;
    Vec aa1 = a1;
    Vec bb1 = b1;
    Vec cc1 = c1;
    Vec dd1 = d1;
    Vec aa2 = a2;
    Vec bb2 = b2;
    Vec cc2 = c2;
    Vec dd2 = d2;

    MD5_STEP_X2(F, a1, b1, c1, d1, a2, b2, c2, d2, 0, 7, 0xd76aa478)
    MD5_STEP_X2(F, d1, a1, b1, c1, d2, a2, b2, c2, 1, 12, 0xe8c7b756)
    MD5_STEP_X2(F, c1, d1, a1, b1, c2, d2, a2, b2, 2, 17, 0x242070db)
    MD5_STEP_X2(F, b1, c1, d1, a1, b2, c2, d2, a2, 3, 22, 0xc1bdceee)
    MD5_STEP_X2(F, a1, b1, c1, d1, a2, b2, c2, d2, 4, 7, 0xf57c0faf)
    MD5_STEP_X2(F, d1, a1, b1, c1, d2, a2, b2, c2, 5, 12, 0x4787c62a)
    MD5_STEP_X2(F, c1, d1, a1, b1, c2, d2, a2, b2, 6, 17, 0xa8304613)
    MD5_STEP_X2(F, b1, c1, d1, a1, b2, c2, d2, a2, 7, 22, 0xfd469501)
    MD5_STEP_X2(F, a1, b1, c1, d1, a2, b2, c2, d2, 8, 7, 0x698098d8)
    MD5_STEP_X2(F, d1, a1, b1, c1, d2, a2, b2, c2, 9, 12, 0x8b44f7af)
    MD5_STEP_X2(F, c1, d1, a1, b1, c2, d2, a2, b2, 10, 17, 0xffff5bb1)
    MD5_STEP_X2(F, b1, c1, d1, a1, b2, c2, d2, a2, 11, 22, 0x895cd7be)
    MD5_STEP_X2(F, a1, b1, c1, d1, a2, b2, c2, d2, 12, 7, 0x6b901122)
    MD5_STEP_X2(F, d1, a1, b1, c1, d2, a2, b2, c2, 13, 12, 0xfd987193)
    MD5_STEP_X2(F, c1, d1, a1, b1, c2, d2, a2, b2, 14, 17, 0xa679438e)
    MD5_STEP_X2(F, b1, c1, d1, a1, b2, c2, d2, a2, 15, 22, 0x49b40821)

    MD5_STEP_X2(G, a1, b1, c1, d1, a2, b2, c2, d2, 1, 5, 0xf61e2562)
    MD5_STEP_X2(G, d1, a1, b1, c1, d2, a2, b2, c2, 6, 9, 0xc040b340)
    MD5_STEP_X2(G, c1, d1, a1, b1, c2, d2, a2, b2, 11, 14, 0x265e5a51)
    MD5_STEP_X2(G, b1, c1, d1, a1, b2, c2, d2, a2, 0, 20, 0xe9b6c7aa)
    MD5_STEP_X2(G, a1, b1, c1, d1, a2, b2, c2, d2, 5, 5, 0xd62f105d)
    MD5_STEP_X2(G, d1, a1, b1, c1, d2, a2, b2, c2, 10, 9, 0x02441453)
    MD5_STEP_X2(G, c1, d1, a1, b1, c2, d2, a2, b2, 15, 14, 0xd8a1e681)
    MD5_STEP_X2(G, b1, c1, d1, a1, b2, c2, d2, a2, 4, 20, 0xe7d3fbc8)
    MD5_STEP_X2(G, a1, b1, c1, d1, a2, b2, c2, d2, 9, 5, 0x21e1cde6)
    MD5_STEP_X2(G, d1, a1, b1, c1, d2, a2, b2, c2, 14, 9, 0xc33707d6)
    MD5_STEP_X2(G, c1, d1, a1, b1, c2, d2, a2, b2, 3, 14, 0xf4d50d87)
    MD5_STEP_X2(G, b1, c1, d1, a1, b2, c2, d2, a2, 8, 20, 0x455a14ed)
    MD5_STEP_X2(G, a1, b1, c1, d1, a2, b2, c2, d2, 13, 5, 0xa9e3e905)
    MD5_STEP_X2(G, d1, a1, b1, c1, d2, a2, b2, c2, 2, 9, 0xfcefa3f8)
    MD5_STEP_X2(G, c1, d1, a1, b1, c2, d2, a2, b2, 7, 14, 0x676f02d9)
    MD5_STEP_X2(G, b1, c1, d1, a1, b2, c2, d2, a2, 12, 20, 0x8d2a4c8a)

    MD5_STEP_X2(H, a1, b1, c1, d1, a2, b2, c2, d2, 5, 4, 0xfffa3942)
    MD5_STEP_X2(H, d1, a1, b1, c1, d2, a2, b2, c2, 8, 11, 0x8771f681)
    MD5_STEP_X2(H, c1, d1, a1, b1, c2, d2, a2, b2, 11, 16, 0x6d9d6122)
    MD5_STEP_X2(H, b1, c1, d1, a1, b2, c2, d2, a2, 14, 23, 0xfde5380c)
    MD5_STEP_X2(H, a1, b1, c1, d1, a2, b2, c2, d2, 1, 4, 0xa4beea44)
    MD5_STEP_X2(H, d1, a1, b1, c1, d2, a2, b2, c2, 4, 11, 0x4bdecfa9)
    MD5_STEP_X2(H, c1, d1, a1, b1, c2, d2, a2, b2, 7, 16, 0xf6bb4b60)
    MD5_STEP_X2(H, b1, c1, d1, a1, b2, c2, d2, a2, 10, 23, 0xbebfbc70)
    MD5_STEP_X2(H, a1, b1, c1, d1, a2, b2, c2, d2, 13, 4, 0x289b7ec6)
    MD5_STEP_X2(H, d1, a1, b1, c1, d2, a2, b2, c2, 0, 11, 0xeaa127fa)
    MD5_STEP_X2(H, c1, d1, a1, b1, c2, d2, a2, b2, 3, 16, 0xd4ef3085)
    MD5_STEP_X2(H, b1, c1, d1, a1, b2, c2, d2, a2, 6, 23, 0x04881d05)
    MD5_STEP_X2(H, a1, b1, c1, d1, a2, b2, c2, d2, 9, 4, 0xd9d4d039)
    MD5_STEP_X2(H, d1, a1, b1, c1, d2, a2, b2, c2, 12, 11, 0xe6db99e5)
    MD5_STEP_X2(H, c1, d1, a1, b1, c2, d2, a2, b2, 15, 16, 0x1fa27cf8)
    MD5_STEP_X2(H, b1, c1, d1, a1, b2, c2, d2, a2, 2, 23, 0xc4ac5665)

    MD5_STEP_X2(I, a1, b1, c1, d1, a2, b2, c2, d2, 0, 6, 0xf4292244)
    MD5_STEP_X2(I, d1, a1, b1, c1, d2, a2, b2, c2, 7, 10, 0x432aff97)
    MD5_STEP_X2(I, c1, d1, a1, b1, c2, d2, a2, b2, 14, 15, 0xab9423a7)
    MD5_STEP_X2(I, b1, c1, d1, a1, b2, c2, d2, a2, 5, 21, 0xfc93a039)
    MD5_STEP_X2(I, a1, b1, c1, d1, a2, b2, c2, d2, 12, 6, 0x655b59c3)
    MD5_STEP_X2(I, d1, a1, b1, c1, d2, a2, b2, c2, 3, 10, 0x8f0ccc92)
    MD5_STEP_X2(I, c1, d1, a1, b1, c2, d2, a2, b2, 10, 15, 0xffeff47d)
    MD5_STEP_X2(I, b1, c1, d1, a1, b2, c2, d2, a2, 1, 21, 0x85845dd1)
    MD5_STEP_X2(I, a1, b1, c1, d1, a2, b2, c2, d2, 8, 6, 0x6fa87e4f)
    MD5_STEP_X2(I, d1, a1, b1, c1, d2, a2, b2, c2, 15, 10, 0xfe2ce6e0)
    MD5_STEP_X2(I, c1, d1, a1, b1, c2, d2, a2, b2, 6, 15, 0xa3014314)
    MD5_STEP_X2(I, b1, c1, d1, a1, b2, c2, d2, a2, 13, 21, 0x4e0811a1)
    MD5_STEP_X2(I, a1, b1, c1, d1, a2, b2, c2, d2, 4, 6, 0xf7537e82)
    MD5_STEP_X2(I, d1, a1, b1, c1, d2, a2, b2, c2, 11, 10, 0xbd3af235)
    MD5_STEP_X2(I, c1, d1, a1, b1, c2, d2, a2, b2, 2, 15, 0x2ad7d2bb)
    MD5_STEP_X2(I, b1, c1, d1, a1, b2, c2, d2, a2, 9, 21, 0xeb86d391)

    return {Ops::add(a1, aa1), Ops::add(b1, bb1), Ops::add(c1, cc1), Ops::add(d1, dd1),
            Ops::add(a2, aa2), Ops::add(b2, bb2), Ops::add(c2, cc2), Ops::add(d2, dd2)};
}

#undef MD5_STEP_X2

template <typename Ops>
uint32_t extract_lane(typename Ops::Vec v, size_t lane) {
    alignas(32) uint32_t values[Ops::LANES];
    Ops::storeu(values, v);
    return values[lane];
}

template <typename Ops>
void md5_multi_buffer_compute(const unsigned char* const inputs[], const size_t lengths[],
                              unsigned char* outputs, size_t count) {
    constexpr size_t N = Ops::LANES;
    using Vec = typename Ops::Vec;
    size_t count1 = std::min(count, N);
    size_t count2 = count > N ? count - N : 0;

    size_t num_blocks[2 * N];
    size_t max_blocks = 0;
    for (size_t i = 0; i < count; ++i) {
        num_blocks[i] = md5_num_blocks(lengths[i]);
        max_blocks = std::max(max_blocks, num_blocks[i]);
    }
    for (size_t i = count; i < 2 * N; ++i) {
        num_blocks[i] = 1;
    }

    alignas(32) unsigned char final_buf[2 * N][128];
    size_t final_block_start[2 * N];
    size_t final_block_count[2 * N];
    for (size_t i = 0; i < count; ++i) {
        final_block_start[i] = lengths[i] / 64;
        final_block_count[i] = md5_pad_final_blocks(inputs[i], lengths[i], final_buf[i]);
    }
    for (size_t i = count; i < 2 * N; ++i) {
        final_block_start[i] = 0;
        final_block_count[i] = md5_pad_final_blocks(&MD5_DUMMY_INPUT, 0, final_buf[i]);
    }

    Vec a1 = Ops::set1(MD5_A0);
    Vec b1 = Ops::set1(MD5_B0);
    Vec c1 = Ops::set1(MD5_C0);
    Vec d1 = Ops::set1(MD5_D0);
    Vec a2 = Ops::set1(MD5_A0);
    Vec b2 = Ops::set1(MD5_B0);
    Vec c2 = Ops::set1(MD5_C0);
    Vec d2 = Ops::set1(MD5_D0);

    for (size_t block = 0; block < max_blocks; ++block) {
        const unsigned char* block_ptrs[2 * N];
        for (size_t i = 0; i < 2 * N; ++i) {
            if (block < final_block_start[i]) {
                block_ptrs[i] = inputs[i] + block * 64;
            } else {
                size_t final_index = block - final_block_start[i];
                block_ptrs[i] = final_index < final_block_count[i] ? final_buf[i] + final_index * 64
                                                                   : final_buf[i];
            }
        }

        Vec msg1[16];
        Vec msg2[16];
        Ops::gather_all_message_words(block_ptrs, msg1);
        Ops::gather_all_message_words(block_ptrs + N, msg2);

        auto st = md5_multi_buffer_block_x2<Ops>(a1, b1, c1, d1, a2, b2, c2, d2, msg1, msg2);
        a1 = st.a1;
        b1 = st.b1;
        c1 = st.c1;
        d1 = st.d1;
        a2 = st.a2;
        b2 = st.b2;
        c2 = st.c2;
        d2 = st.d2;

        for (size_t lane = 0; lane < count1; ++lane) {
            if (block + 1 == num_blocks[lane]) {
                unsigned char* out = outputs + lane * MD5_DIGEST_LENGTH;
                LittleEndian::Store32(out, extract_lane<Ops>(a1, lane));
                LittleEndian::Store32(out + 4, extract_lane<Ops>(b1, lane));
                LittleEndian::Store32(out + 8, extract_lane<Ops>(c1, lane));
                LittleEndian::Store32(out + 12, extract_lane<Ops>(d1, lane));
            }
        }
        for (size_t lane = 0; lane < count2; ++lane) {
            if (block + 1 == num_blocks[N + lane]) {
                unsigned char* out = outputs + (N + lane) * MD5_DIGEST_LENGTH;
                LittleEndian::Store32(out, extract_lane<Ops>(a2, lane));
                LittleEndian::Store32(out + 4, extract_lane<Ops>(b2, lane));
                LittleEndian::Store32(out + 8, extract_lane<Ops>(c2, lane));
                LittleEndian::Store32(out + 12, extract_lane<Ops>(d2, lane));
            }
        }
    }
}

void md5_binary_batch_avx2(const unsigned char* const inputs[], const size_t lengths[],
                           unsigned char* outputs, size_t count) {
    constexpr size_t BATCH = 2 * AVX2MD5Ops::LANES;
    for (size_t base = 0; base < count; base += BATCH) {
        size_t batch = std::min(BATCH, count - base);
        const unsigned char* batch_inputs[BATCH];
        size_t batch_lengths[BATCH];
        for (size_t i = 0; i < batch; ++i) {
            batch_inputs[i] = lengths[base + i] == 0 ? &MD5_DUMMY_INPUT : inputs[base + i];
            batch_lengths[i] = lengths[base + i];
        }
        for (size_t i = batch; i < BATCH; ++i) {
            batch_inputs[i] = &MD5_DUMMY_INPUT;
            batch_lengths[i] = 0;
        }
        md5_multi_buffer_compute<AVX2MD5Ops>(batch_inputs, batch_lengths,
                                             outputs + base * MD5_DIGEST_LENGTH, batch);
    }
}

#endif

} // namespace

Md5Digest::Md5Digest() {
    MD5_Init(&_md5_ctx);
}

void Md5Digest::update(const void* data, size_t length) {
    MD5_Update(&_md5_ctx, data, length);
}

void Md5Digest::digest() {
    unsigned char buf[MD5_DIGEST_LENGTH];
    MD5_Final(buf, &_md5_ctx);

    char hex_buf[MD5_HEX_LENGTH];
    md5_to_hex(buf, hex_buf);
    _hex.assign(hex_buf, MD5_HEX_LENGTH);
}

void md5_hex_batch(const unsigned char* const inputs[], const size_t lengths[], char* outputs,
                   size_t count) {
    if (count == 0) {
        return;
    }

#ifdef __AVX2__
    std::vector<unsigned char> digests(count * MD5_DIGEST_LENGTH);
    md5_binary_batch_avx2(inputs, lengths, digests.data(), count);
    for (size_t i = 0; i < count; ++i) {
        md5_to_hex(digests.data() + i * MD5_DIGEST_LENGTH, outputs + i * MD5_HEX_LENGTH);
    }
#else
    for (size_t i = 0; i < count; ++i) {
        unsigned char digest[MD5_DIGEST_LENGTH];
        MD5(lengths[i] == 0 ? &MD5_DUMMY_INPUT : inputs[i], lengths[i], digest);
        md5_to_hex(digest, outputs + i * MD5_HEX_LENGTH);
    }
#endif
}

} // namespace doris
