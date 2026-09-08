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

#include "meta-service/doris_txn.h"

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <gtest/gtest.h>

#include <array>
#include <bit>
#include <cstring>
#include <utility>

#include "common/config.h"
#include "common/util.h"
#include "meta-service/meta_service.h"
#include "meta-store/txn_kv.h"
#include "meta-store/versionstamp.h"

namespace {

#if defined(__x86_64__) || defined(__aarch64__)
// Preserve the original, unsafe load solely as a historical output oracle on
// hosts that support unaligned integer loads. Suppress only its known alignment
// violation, not instrumentation of the memcpy reference or production decoder.
// Matching its results does not establish that the old code was well-defined.
__attribute__((noinline, no_sanitize("alignment"))) int64_t legacy_aliased_load(const char* data) {
    return *reinterpret_cast<const int64_t*>(data);
}
#endif

int64_t legacy_memcpy_load(const char* data) {
    int64_t value;
    std::memcpy(&value, data, sizeof(value));
    return value;
}

// Freeze the pre-Versionstamp algorithm, including the overlapping eight-byte
// sequence read and its signed intermediate values. Do not use Versionstamp or
// its byte-swap helpers here: this must remain an independent regression oracle.
template <int64_t (*load)(const char*)>
int legacy_get_txn_id_from_fdb_ts(std::string_view fdb_vts, int64_t* txn_id) {
    if (fdb_vts.size() != 10) {
        return 1;
    }
    static_assert(std::endian::native == std::endian::little);
    auto to_little = [](int64_t v) {
        v = ((v & 0xffffffff00000000) >> 32) | ((v & 0x00000000ffffffff) << 32);
        v = ((v & 0xffff0000ffff0000) >> 16) | ((v & 0x0000ffff0000ffff) << 16);
        v = ((v & 0xff00ff00ff00ff00) >> 8) | ((v & 0x00ff00ff00ff00ff) << 8);
        return v;
    };
    int64_t ver = to_little(load(fdb_vts.data()));
    int64_t seq = to_little(load(fdb_vts.data() + 2));
    seq &= 0xffff;
    static constexpr int SEQ_RETAIN_BITS = 10;
    if (seq >= (1L << SEQ_RETAIN_BITS)) {
        return 2;
    }
    seq &= ((1L << SEQ_RETAIN_BITS) - 1L);
    ver <<= SEQ_RETAIN_BITS;
    ver |= seq;
    *txn_id = ver;
    return 0;
}

using TxnIdDecoder = int (*)(std::string_view, int64_t*);

void check_decoded_txn_id(TxnIdDecoder legacy_decode, std::string_view input, int expected_ret) {
    int64_t expected_txn_id = -1;
    int64_t actual_txn_id = -1;
    ASSERT_EQ(legacy_decode(input, &expected_txn_id), expected_ret);
    ASSERT_EQ(doris::cloud::get_txn_id_from_fdb_ts(input, &actual_txn_id), expected_ret);
    ASSERT_EQ(actual_txn_id, expected_txn_id);
    if (expected_ret != 0) {
        ASSERT_EQ(actual_txn_id, -1);
    }
}

void check_legacy_txn_id_compatibility(TxnIdDecoder legacy_decode) {
    // Include the positive txn_id limit, the sign transition, and discarded high
    // version bits. These latter cases preserve old behavior, not uniqueness.
    constexpr std::array<uint64_t, 8> versions = {0,
                                                  1,
                                                  0x00000182a5ed173f,
                                                  0x001f82a5ed173f80,
                                                  0x001fffffffffffff,
                                                  0x0020000000000000,
                                                  0x0040000000000000,
                                                  0x7fffffffffffffff};
    alignas(int64_t) std::array<char, 10 + alignof(int64_t) - 1> buffer {};
    for (uint64_t ver : versions) {
        SCOPED_TRACE(ver);
        std::array<uint8_t, 10> bytes {};
        for (size_t i = 0; i < 8; ++i) {
            bytes[i] = static_cast<uint8_t>(ver >> (8 * (7 - i)));
        }
        for (uint32_t seq = 0; seq <= 0xffff; ++seq) {
            bytes[8] = static_cast<uint8_t>(seq >> 8);
            bytes[9] = static_cast<uint8_t>(seq);
            // Also check the complete fields: txn_id packing discards version
            // bits and rejects most orders, so output equality alone misses them.
            const doris::cloud::Versionstamp versionstamp(bytes);
            ASSERT_EQ(std::make_pair(versionstamp.version(), versionstamp.order()),
                      std::make_pair(ver, static_cast<uint16_t>(seq)));
            for (size_t offset = 0; offset < alignof(int64_t); ++offset) {
                std::memcpy(buffer.data() + offset, bytes.data(), bytes.size());
                const std::string_view input(buffer.data() + offset, bytes.size());
                ASSERT_NO_FATAL_FAILURE(
                        check_decoded_txn_id(legacy_decode, input, seq < 1024 ? 0 : 2))
                        << "seq=" << seq << " offset=" << offset;
            }
        }
    }
}

void check_invalid_txn_id_inputs(TxnIdDecoder legacy_decode) {
    // Check short/long inputs and that failures leave the output untouched.
    std::array<char, 17> buffer {};
    for (size_t size = 0; size <= buffer.size(); ++size) {
        if (size == 10) {
            continue;
        }
        SCOPED_TRACE(size);
        const std::string_view input(buffer.data(), size);
        ASSERT_NO_FATAL_FAILURE(check_decoded_txn_id(legacy_decode, input, 1));
    }
    check_decoded_txn_id(legacy_decode, {}, 1);
}

} // namespace

int main(int argc, char** argv) {
    doris::cloud::config::init(nullptr, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

TEST(TxnIdConvert, TxnIdTest) {
    using namespace doris::cloud;

    // Correctness test
    {
        // 00000182a5ed173f0012
        std::string ts("\x00\x00\x01\x82\xa5\xed\x17\x3f\x00\x12", 10);
        ASSERT_EQ(ts.size(), 10);
        int64_t txn_id0;
        int ret = get_txn_id_from_fdb_ts(ts, &txn_id0);
        ASSERT_EQ(ret, 0);
        std::string str((char*)&txn_id0, sizeof(txn_id0));
        std::cout << "fdb_ts0: " << hex(ts) << " "
                  << "txn_id0: " << txn_id0 << " hex: " << hex(str) << std::endl;

        // 00000182a5ed173f0013
        ts = std::string("\x00\x00\x01\x82\xa5\xed\x17\x3f\x00\x13", 10);
        ASSERT_EQ(ts.size(), 10);
        int64_t txn_id1;
        ret = get_txn_id_from_fdb_ts(ts, &txn_id1);
        ASSERT_EQ(ret, 0);
        ASSERT_GT(txn_id1, txn_id0);
        str = std::string((char*)&txn_id1, sizeof(txn_id1));
        std::cout << "fdb_ts1: " << hex(ts) << " "
                  << "txn_id1: " << txn_id1 << " hex: " << hex(str) << std::endl;

        // 00000182a5ed174f0013
        ts = std::string("\x00\x00\x01\x82\xa5\xed\x17\x4f\x00\x13", 10);
        ASSERT_EQ(ts.size(), 10);
        int64_t txn_id2;
        ret = get_txn_id_from_fdb_ts(ts, &txn_id2);
        ASSERT_EQ(ret, 0);
        ASSERT_GT(txn_id2, txn_id1);
        str = std::string((char*)&txn_id2, sizeof(txn_id2));
        std::cout << "fdb_ts2: " << hex(ts) << " "
                  << "txn_id2: " << txn_id2 << " hex: " << hex(str) << std::endl;
    }

    // Boundary test
    {
        //                 1024
        // 00000182a5ed174f0400
        std::string ts("\x00\x00\x01\x82\xa5\xed\x17\x4f\x04\x00", 10);
        ASSERT_EQ(ts.size(), 10);
        int64_t txn_id;
        int ret = get_txn_id_from_fdb_ts(ts, &txn_id);
        ASSERT_EQ(ret, 2); // Exceed max seq

        //                 1023
        // 00000182a5ed174f03ff
        ts = std::string("\x00\x00\x01\x82\xa5\xed\x17\x4f\x03\xff", 10);
        ret = get_txn_id_from_fdb_ts(ts, &txn_id);
        ASSERT_EQ(ret, 0);

        //                 0000
        // 00000182a5ed174f0000
        ts = std::string("\x00\x00\x01\x82\xa5\xed\x17\x4f\x03\x00", 10);
        ret = get_txn_id_from_fdb_ts(ts, &txn_id);
        ASSERT_EQ(ret, 0);

        // Insufficient length
        ts = std::string("\x00\x00\x01\x82\xa5\xed\x17\x4f\x03\x00", 9);
        ret = get_txn_id_from_fdb_ts(ts, &txn_id);
        ASSERT_EQ(ret, 1);
    }
}

TEST(TxnIdConvert, UnalignedVersionstamp) {
    // Cover every input alignment, including odd addresses.
    // The payload has high bits set in both version bytes and sequence bytes.
    constexpr std::array<unsigned char, 10> versionstamp = {0x00, 0x1f, 0x82, 0xa5, 0xed,
                                                            0x17, 0x3f, 0x80, 0x03, 0xff};
    constexpr int64_t expected_txn_id = 0x7e0a97b45cfe03ff;
    alignas(int64_t) std::array<char, 10 + alignof(int64_t) - 1> buffer {};
    for (size_t offset = 0; offset < alignof(int64_t); ++offset) {
        SCOPED_TRACE(offset);
        std::memcpy(buffer.data() + offset, versionstamp.data(), versionstamp.size());
        int64_t txn_id = -1;
        ASSERT_EQ(doris::cloud::get_txn_id_from_fdb_ts(
                          std::string_view(buffer.data() + offset, versionstamp.size()), &txn_id),
                  0);
        EXPECT_EQ(txn_id, expected_txn_id);
    }
}

TEST(TxnIdConvert, SequenceValues) {
    // Exercise all two-byte sequences at an odd address. In particular, 0x00ff
    // must remain positive after decoding, and 0x0400 and above must fail.
    alignas(int64_t) std::array<unsigned char, 11> buffer = {0,    0x00, 0x00, 0x01, 0x82, 0xa5,
                                                             0xed, 0x17, 0x3f, 0,    0};
    const std::string_view versionstamp(reinterpret_cast<const char*>(buffer.data() + 1), 10);
    constexpr int64_t base_txn_id = 0x00060a97b45cfc00;
    for (uint32_t seq = 0; seq <= 0xffff; ++seq) {
        SCOPED_TRACE(seq);
        buffer[9] = static_cast<unsigned char>(seq >> 8);
        buffer[10] = static_cast<unsigned char>(seq);
        int64_t txn_id = -1;
        const int ret = doris::cloud::get_txn_id_from_fdb_ts(versionstamp, &txn_id);
        if (seq < 1024) {
            ASSERT_EQ(ret, 0);
            EXPECT_EQ(txn_id, base_txn_id + seq);
        } else {
            ASSERT_EQ(ret, 2);
            EXPECT_EQ(txn_id, -1);
        }
    }
}

TEST(TxnIdConvert, LegacyMemcpyCompatibility) {
    ASSERT_NO_FATAL_FAILURE(
            check_legacy_txn_id_compatibility(legacy_get_txn_id_from_fdb_ts<legacy_memcpy_load>));
    check_invalid_txn_id_inputs(legacy_get_txn_id_from_fdb_ts<legacy_memcpy_load>);
}

TEST(TxnIdConvert, LegacyReinterpretCastCompatibility) {
#if defined(__x86_64__) || defined(__aarch64__)
    ASSERT_NO_FATAL_FAILURE(
            check_legacy_txn_id_compatibility(legacy_get_txn_id_from_fdb_ts<legacy_aliased_load>));
    check_invalid_txn_id_inputs(legacy_get_txn_id_from_fdb_ts<legacy_aliased_load>);
#else
    GTEST_SKIP() << "The original decoder requires a host that supports unaligned integer loads";
#endif
}
