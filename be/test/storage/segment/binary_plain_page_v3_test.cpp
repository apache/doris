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

#include "storage/segment/binary_plain_page_v3.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "common/logging.h"
#include "core/column/column_string.h"
#include "storage/cache/page_cache.h"
#include "storage/olap_common.h"
#include "storage/segment/binary_plain_page_v3_pre_decoder.h"
#include "storage/segment/page_builder.h"
#include "storage/segment/page_decoder.h"
#include "storage/types.h"
#include "util/coding.h"

namespace doris {
namespace segment_v2 {

class BinaryPlainPageV3Test : public testing::Test {
public:
    BinaryPlainPageV3Test() = default;
    ~BinaryPlainPageV3Test() override = default;

    // Apply the V3 pre-decode step so the resulting Slice / DataPage matches the V1 layout
    // consumed by BinaryPlainPageDecoder. CHAR selects the IS_CHAR=true variant (which strips
    // '\0' padding on read), exactly as EncodingInfo does for (CHAR, PLAIN_ENCODING_V3).
    template <FieldType Type = FieldType::OLAP_FIELD_TYPE_VARCHAR>
    Status apply_pre_decode(Slice& page_slice, std::unique_ptr<DataPage>& decoded_page) {
        constexpr bool is_char = (Type == FieldType::OLAP_FIELD_TYPE_CHAR);
        BinaryPlainPageV3PreDecoder<is_char> pre_decoder;
        return pre_decoder.decode(&decoded_page, &page_slice, 0, false, PageTypePB::DATA_PAGE, "");
    }

    template <FieldType Type>
    std::unique_ptr<PageBuilder> make_builder(size_t data_page_size = 256 * 1024) {
        PageBuilderOptions opts;
        opts.data_page_size = data_page_size;

        PageBuilder* raw = nullptr;
        Status st = BinaryPlainPageV3Builder<Type>::create(&raw, opts);
        EXPECT_TRUE(st.ok()) << st;
        return std::unique_ptr<PageBuilder>(raw);
    }

    template <FieldType Type>
    OwnedSlice build_page(const std::vector<Slice>& slices) {
        auto builder = make_builder<Type>();
        size_t count = slices.size();
        Status st = builder->add(reinterpret_cast<const uint8_t*>(slices.data()), &count);
        EXPECT_TRUE(st.ok()) << st;
        EXPECT_EQ(slices.size(), count);

        OwnedSlice owned;
        st = builder->finish(&owned);
        EXPECT_TRUE(st.ok()) << st;
        return owned;
    }

    // Build the Slices fed to the builder. For CHAR, pad every value to a fixed declared
    // length with trailing '\0' (as OlapColumnDataConvertorChar does) so the IS_CHAR read path
    // is exercised; `backing` owns the padded bytes and must outlive the returned Slices.
    // Decoded values must still equal the logical src_strings (callers must not pass embedded
    // '\0' in CHAR inputs).
    template <FieldType Type>
    std::vector<Slice> make_input_slices(const std::vector<std::string>& src_strings,
                                         std::vector<std::string>& backing) {
        std::vector<Slice> slices;
        slices.reserve(src_strings.size());
        if constexpr (Type == FieldType::OLAP_FIELD_TYPE_CHAR) {
            size_t padded_len = 0;
            for (const auto& s : src_strings) {
                padded_len = std::max(padded_len, s.size());
            }
            padded_len += 3; // guarantee real padding even for the longest value
            backing.reserve(src_strings.size());
            for (const auto& s : src_strings) {
                std::string p = s;
                p.resize(padded_len, '\0');
                backing.push_back(std::move(p));
            }
            for (const auto& p : backing) {
                slices.emplace_back(p.data(), p.size());
            }
        } else {
            for (const auto& s : src_strings) {
                slices.emplace_back(s);
            }
        }
        return slices;
    }

    template <FieldType Type>
    void test_encode_decode_page(const std::vector<std::string>& src_strings) {
        std::vector<std::string> backing;
        std::vector<Slice> slices = make_input_slices<Type>(src_strings, backing);

        OwnedSlice owned = build_page<Type>(slices);
        Slice page_slice = owned.slice();
        std::unique_ptr<DataPage> decoded_page;
        ASSERT_TRUE(apply_pre_decode<Type>(page_slice, decoded_page).ok());

        PageDecoderOptions decoder_options;
        BinaryPlainPageV3Decoder<Type> decoder(page_slice, decoder_options);
        ASSERT_TRUE(decoder.init().ok());
        ASSERT_EQ(slices.size(), decoder.count());

        MutableColumnPtr column = ColumnString::create();
        size_t num_to_read = slices.size();
        ASSERT_TRUE(decoder.next_batch(&num_to_read, column).ok());
        ASSERT_EQ(slices.size(), num_to_read);
        ASSERT_EQ(slices.size(), column->size());

        auto* string_column = assert_cast<ColumnString*>(column.get());
        for (size_t i = 0; i < slices.size(); ++i) {
            EXPECT_EQ(src_strings[i], string_column->get_data_at(i).to_string())
                    << "Mismatch at index " << i;
        }
    }

    template <FieldType Type>
    void test_seek_in_page(const std::vector<std::string>& src_strings) {
        std::vector<std::string> backing;
        std::vector<Slice> slices = make_input_slices<Type>(src_strings, backing);

        OwnedSlice owned = build_page<Type>(slices);
        Slice page_slice = owned.slice();
        std::unique_ptr<DataPage> decoded_page;
        ASSERT_TRUE(apply_pre_decode<Type>(page_slice, decoded_page).ok());

        PageDecoderOptions decoder_options;
        BinaryPlainPageV3Decoder<Type> decoder(page_slice, decoder_options);
        ASSERT_TRUE(decoder.init().ok());

        const std::vector<size_t> seek_positions = {0, 2, slices.size() / 2, slices.size() - 1};
        for (size_t pos : seek_positions) {
            if (pos >= slices.size()) continue;

            ASSERT_TRUE(decoder.seek_to_position_in_page(pos).ok());
            EXPECT_EQ(pos, decoder.current_index());

            MutableColumnPtr column = ColumnString::create();
            size_t n = 1;
            ASSERT_TRUE(decoder.next_batch(&n, column).ok());
            EXPECT_EQ(1, n);
            auto* sc = assert_cast<ColumnString*>(column.get());
            EXPECT_EQ(src_strings[pos], sc->get_data_at(0).to_string())
                    << "Mismatch at seek position " << pos;
        }
    }

    template <FieldType Type>
    void test_read_by_rowids(const std::vector<std::string>& src_strings) {
        std::vector<std::string> backing;
        std::vector<Slice> slices = make_input_slices<Type>(src_strings, backing);

        OwnedSlice owned = build_page<Type>(slices);
        Slice page_slice = owned.slice();
        std::unique_ptr<DataPage> decoded_page;
        ASSERT_TRUE(apply_pre_decode<Type>(page_slice, decoded_page).ok());

        PageDecoderOptions decoder_options;
        BinaryPlainPageV3Decoder<Type> decoder(page_slice, decoder_options);
        ASSERT_TRUE(decoder.init().ok());

        std::vector<rowid_t> rowids;
        rowids.push_back(0);
        rowids.push_back(2);
        rowids.push_back(3);
        rowids.push_back(static_cast<rowid_t>(slices.size() - 1));
        ordinal_t page_first_ordinal = 0;

        MutableColumnPtr column = ColumnString::create();
        size_t num_to_read = rowids.size();
        ASSERT_TRUE(decoder.read_by_rowids(rowids.data(), page_first_ordinal, &num_to_read, column)
                            .ok());
        EXPECT_EQ(rowids.size(), num_to_read);

        auto* sc = assert_cast<ColumnString*>(column.get());
        for (size_t i = 0; i < rowids.size(); ++i) {
            EXPECT_EQ(src_strings[rowids[i]], sc->get_data_at(i).to_string())
                    << "Mismatch at rowid " << rowids[i];
        }
    }

    template <FieldType Type>
    void test_empty_page() {
        auto builder = make_builder<Type>();
        OwnedSlice owned;
        ASSERT_TRUE(builder->finish(&owned).ok());
        EXPECT_EQ(0, builder->count());

        Slice page_slice = owned.slice();
        std::unique_ptr<DataPage> decoded_page;
        ASSERT_TRUE(apply_pre_decode<Type>(page_slice, decoded_page).ok());

        PageDecoderOptions decoder_options;
        BinaryPlainPageV3Decoder<Type> decoder(page_slice, decoder_options);
        ASSERT_TRUE(decoder.init().ok());
        EXPECT_EQ(0, decoder.count());

        MutableColumnPtr column = ColumnString::create();
        size_t n = 1;
        ASSERT_TRUE(decoder.next_batch(&n, column).ok());
        EXPECT_EQ(0, n);
        EXPECT_EQ(0, column->size());
    }

    template <FieldType Type>
    void test_page_full() {
        // Tiny size_estimate budget triggers is_page_full() before we drain all input.
        auto builder = make_builder<Type>(/*data_page_size=*/128);

        std::vector<std::string> src_strings;
        for (int i = 0; i < 100; ++i) {
            src_strings.push_back("test_string_" + std::to_string(i));
        }
        std::vector<Slice> slices;
        slices.reserve(src_strings.size());
        for (const auto& s : src_strings) {
            slices.emplace_back(s);
        }

        size_t added = 0;
        for (size_t i = 0; i < slices.size(); ++i) {
            if (builder->is_page_full()) break;
            size_t n = 1;
            ASSERT_TRUE(builder->add(reinterpret_cast<const uint8_t*>(&slices[i]), &n).ok());
            if (n > 0) added++;
        }
        EXPECT_GT(added, 0);
        EXPECT_LT(added, slices.size());
        EXPECT_TRUE(builder->is_page_full());
    }

    template <FieldType Type>
    void test_various_length_strings() {
        std::vector<std::string> src_strings;
        src_strings.push_back("");
        src_strings.push_back("a");
        src_strings.push_back("ab");
        src_strings.push_back("Hello, World!");
        src_strings.push_back("Apache Doris is great");
        src_strings.push_back(std::string(1000, 'x'));
        src_strings.push_back("test\n\r\t");
        src_strings.push_back("中文测试");
        test_encode_decode_page<Type>(src_strings);
    }

    template <FieldType Type>
    void test_reset() {
        auto builder = make_builder<Type>();

        std::vector<std::string> src_strings = {"test1", "test2"};
        std::vector<Slice> slices;
        slices.reserve(src_strings.size());
        for (const auto& s : src_strings) {
            slices.emplace_back(s);
        }

        size_t count = slices.size();
        ASSERT_TRUE(builder->add(reinterpret_cast<const uint8_t*>(slices.data()), &count).ok());
        EXPECT_EQ(2, builder->count());

        ASSERT_TRUE(builder->reset().ok());
        EXPECT_EQ(0, builder->count());

        count = slices.size();
        ASSERT_TRUE(builder->add(reinterpret_cast<const uint8_t*>(slices.data()), &count).ok());
        EXPECT_EQ(2, builder->count());
    }
};

// -------- VARCHAR --------
TEST_F(BinaryPlainPageV3Test, TestEncodeDecodeVarchar) {
    test_encode_decode_page<FieldType::OLAP_FIELD_TYPE_VARCHAR>(
            {"Hello", "World", "Apache", "Doris"});
}
TEST_F(BinaryPlainPageV3Test, TestSeekVarchar) {
    test_seek_in_page<FieldType::OLAP_FIELD_TYPE_VARCHAR>({"a", "b", "c", "d", "e", "f"});
}
TEST_F(BinaryPlainPageV3Test, TestReadByRowidsVarchar) {
    test_read_by_rowids<FieldType::OLAP_FIELD_TYPE_VARCHAR>(
            {"first", "second", "third", "fourth", "fifth"});
}
TEST_F(BinaryPlainPageV3Test, TestEmptyPageVarchar) {
    test_empty_page<FieldType::OLAP_FIELD_TYPE_VARCHAR>();
}
TEST_F(BinaryPlainPageV3Test, TestPageFullVarchar) {
    test_page_full<FieldType::OLAP_FIELD_TYPE_VARCHAR>();
}
TEST_F(BinaryPlainPageV3Test, TestVariousLengthStringsVarchar) {
    test_various_length_strings<FieldType::OLAP_FIELD_TYPE_VARCHAR>();
}
TEST_F(BinaryPlainPageV3Test, TestResetVarchar) {
    test_reset<FieldType::OLAP_FIELD_TYPE_VARCHAR>();
}
TEST_F(BinaryPlainPageV3Test, TestLargeNumberOfStrings) {
    std::vector<std::string> v;
    for (int i = 0; i < 1000; ++i) v.push_back("string_" + std::to_string(i));
    test_encode_decode_page<FieldType::OLAP_FIELD_TYPE_VARCHAR>(v);
}
TEST_F(BinaryPlainPageV3Test, TestSingleString) {
    test_encode_decode_page<FieldType::OLAP_FIELD_TYPE_VARCHAR>({"single"});
}
// The last element's end boundary is the implicit sentinel offset — offset(N) is never
// stored; the decoder returns _offsets_pos (== data_block_size) for it. Exercise it at
// length 0: empty value at the tail, a single-empty page, and an all-empty page.
TEST_F(BinaryPlainPageV3Test, TestTrailingEmptyValue) {
    test_encode_decode_page<FieldType::OLAP_FIELD_TYPE_VARCHAR>({"abc", "", "de", ""});
    test_encode_decode_page<FieldType::OLAP_FIELD_TYPE_VARCHAR>({""});
    test_encode_decode_page<FieldType::OLAP_FIELD_TYPE_VARCHAR>({"", "", ""});
    // CHAR values are padded to a fixed length, so an empty tail value is all '\0' and the
    // IS_CHAR pre-decoder must strnlen it back to length 0 — the sentinel must still land
    // exactly at data_block_size.
    test_encode_decode_page<FieldType::OLAP_FIELD_TYPE_CHAR>({"x", "", "yz", ""});
}

// -------- STRING / CHAR --------
TEST_F(BinaryPlainPageV3Test, TestEncodeDecodeString) {
    test_encode_decode_page<FieldType::OLAP_FIELD_TYPE_STRING>({"String1", "String2", "String3"});
}
TEST_F(BinaryPlainPageV3Test, TestEncodeDecodeChar) {
    // Mixed lengths (including empty and multi-byte) so each value carries a different
    // amount of '\0' padding; test_encode_decode_page pads to a fixed length and the
    // IS_CHAR read path must strip it back to these logical values.
    test_encode_decode_page<FieldType::OLAP_FIELD_TYPE_CHAR>({"Hi", "", "abcdef", "x", "中文"});
}
// seek and read_by_rowids on a padded CHAR page (the inputs are padded to a fixed length,
// so the IS_CHAR pre-decoder strips the '\0' padding before these decode paths run).
TEST_F(BinaryPlainPageV3Test, TestSeekChar) {
    test_seek_in_page<FieldType::OLAP_FIELD_TYPE_CHAR>({"a", "bb", "", "dddd", "e", "ffffff"});
}
TEST_F(BinaryPlainPageV3Test, TestReadByRowidsChar) {
    test_read_by_rowids<FieldType::OLAP_FIELD_TYPE_CHAR>({"first", "", "third", "fourth", "fifth"});
}

// Aggregate binary types (HLL/BITMAP/QUANTILE_STATE/AGG_STATE) default to plain V3 in V3
// segments; verify the V3 page round-trips an opaque binary payload (including embedded
// '\0', which non-CHAR types must preserve verbatim).
TEST_F(BinaryPlainPageV3Test, TestEncodeDecodeAggState) {
    test_encode_decode_page<FieldType::OLAP_FIELD_TYPE_AGG_STATE>(
            {"agg_state_1", "", std::string("\x01\x02\x00\x03", 4), "another_state"});
}

// CHAR padding handling. OlapColumnDataConvertorChar pads CHAR values to their declared
// length with trailing '\0'. V3 (like V1/V2) stores the padded bytes verbatim; the padding
// is stripped on read by BinaryPlainPageV3PreDecoder<true>.
namespace {
// Build a fixed-length CHAR slice payload: each logical value padded with '\0' to
// padded_len, exactly as the convertor hands it to the page builder.
std::vector<std::string> make_padded_char_backing(const std::vector<std::string>& logical,
                                                  size_t padded_len) {
    std::vector<std::string> padded;
    padded.reserve(logical.size());
    for (const auto& s : logical) {
        std::string p = s;
        p.resize(padded_len, '\0'); // truncation never happens: callers keep s <= padded_len
        padded.push_back(std::move(p));
    }
    return padded;
}
} // namespace

// The V3 CHAR builder stores the padded bytes verbatim (same as V2); the IS_CHAR
// pre-decoder strips the trailing '\0' padding on read so the decoded value is logical.
TEST_F(BinaryPlainPageV3Test, TestCharBuilderKeepsPaddingStrippedOnRead) {
    constexpr size_t kPaddedLen = 10; // CHAR(10)
    const std::vector<std::string> logical = {"Hi", "", "abcdefghij", "x", "中文"};

    // Backing store must outlive the Slices that point into it.
    std::vector<std::string> padded = make_padded_char_backing(logical, kPaddedLen);
    std::vector<Slice> slices;
    slices.reserve(padded.size());
    for (auto& p : padded) {
        slices.emplace_back(p.data(), kPaddedLen); // full padded width, as the writer sees it
    }

    OwnedSlice owned = build_page<FieldType::OLAP_FIELD_TYPE_CHAR>(slices);
    Slice page = owned.slice();
    ASSERT_GE(page.size, 2 * sizeof(uint32_t));

    // 1. The on-disk data block keeps the padded bytes verbatim (same as V2).
    const auto* trailer = reinterpret_cast<const uint8_t*>(page.data + page.size - 8);
    uint32_t data_block_size = decode_fixed32_le(trailer);
    uint32_t num_elems = decode_fixed32_le(trailer + sizeof(uint32_t));
    EXPECT_EQ(logical.size(), num_elems);
    EXPECT_EQ(kPaddedLen * logical.size(), data_block_size) << "CHAR keeps padding on disk";

    // 2. The IS_CHAR pre-decoder strips the padding on read; decoded values are logical.
    std::unique_ptr<DataPage> decoded_page;
    BinaryPlainPageV3PreDecoder<true> char_pre_decoder;
    ASSERT_TRUE(char_pre_decoder.decode(&decoded_page, &page, 0, false, PageTypePB::DATA_PAGE, "")
                        .ok());

    PageDecoderOptions decoder_options;
    BinaryPlainPageV3Decoder<FieldType::OLAP_FIELD_TYPE_CHAR> decoder(page, decoder_options);
    ASSERT_TRUE(decoder.init().ok());
    ASSERT_EQ(logical.size(), decoder.count());

    MutableColumnPtr column = ColumnString::create();
    size_t n = logical.size();
    ASSERT_TRUE(decoder.next_batch(&n, column).ok());
    ASSERT_EQ(logical.size(), n);
    auto* sc = assert_cast<ColumnString*>(column.get());
    for (size_t i = 0; i < logical.size(); ++i) {
        EXPECT_EQ(logical[i], sc->get_data_at(i).to_string()) << "Mismatch at index " << i;
    }
}

// V3-specific: the IS_CHAR pre-decoder strips trailing '\0' padding at read time.
// This is the path used for CHAR dictionary word pages — they are written with the
// VARCHAR builder (no write-time strip) so the padding IS on disk, and the dict read
// path selects BinaryPlainPageV3PreDecoder<true> via EncodingInfo::get(CHAR, V3).
TEST_F(BinaryPlainPageV3Test, TestCharPreDecoderStripsPaddingOnRead) {
    constexpr size_t kPaddedLen = 12; // CHAR(12)
    const std::vector<std::string> logical = {"hi", "", "abcdefghijkl", "x", "中文"};

    std::vector<std::string> padded = make_padded_char_backing(logical, kPaddedLen);
    std::vector<Slice> slices;
    slices.reserve(padded.size());
    for (auto& p : padded) {
        slices.emplace_back(p.data(), kPaddedLen);
    }

    // Write with the VARCHAR builder so the padded bytes ARE stored on disk, exactly
    // like the dictionary word page does for a CHAR column.
    OwnedSlice owned = build_page<FieldType::OLAP_FIELD_TYPE_VARCHAR>(slices);
    Slice page = owned.slice();
    ASSERT_GE(page.size, 2 * sizeof(uint32_t));

    // The raw page keeps the full padded width.
    const auto* trailer = reinterpret_cast<const uint8_t*>(page.data + page.size - 8);
    uint32_t data_block_size = decode_fixed32_le(trailer);
    EXPECT_EQ(kPaddedLen * logical.size(), data_block_size) << "raw page should keep padding";

    // The IS_CHAR pre-decoder strips it on read.
    std::unique_ptr<DataPage> decoded_page;
    BinaryPlainPageV3PreDecoder<true> char_pre_decoder;
    ASSERT_TRUE(char_pre_decoder.decode(&decoded_page, &page, 0, false, PageTypePB::DATA_PAGE, "")
                        .ok());

    PageDecoderOptions decoder_options;
    BinaryPlainPageV3Decoder<FieldType::OLAP_FIELD_TYPE_CHAR> decoder(page, decoder_options);
    ASSERT_TRUE(decoder.init().ok());
    ASSERT_EQ(logical.size(), decoder.count());

    MutableColumnPtr column = ColumnString::create();
    size_t n = logical.size();
    ASSERT_TRUE(decoder.next_batch(&n, column).ok());
    ASSERT_EQ(logical.size(), n);
    auto* sc = assert_cast<ColumnString*>(column.get());
    for (size_t i = 0; i < logical.size(); ++i) {
        EXPECT_EQ(logical[i], sc->get_data_at(i).to_string()) << "Mismatch at index " << i;
    }
}

// -------- V3-specific: varint length boundaries --------
// Varint32 length encoding crosses byte boundaries at 128 and 16384. Probe
// the three width bands so we catch off-by-one bugs in the length-scan loop.
TEST_F(BinaryPlainPageV3Test, TestVarintBoundaryLengths) {
    std::vector<std::string> v;
    // 1-byte varint band (<128).
    v.push_back(std::string(0, 'a'));   // empty -> varint 0x00
    v.push_back(std::string(1, 'a'));   // 1
    v.push_back(std::string(127, 'a')); // last 1-byte varint
    // 2-byte varint band ([128, 16384)).
    v.push_back(std::string(128, 'b')); // first 2-byte varint
    v.push_back(std::string(255, 'b'));
    v.push_back(std::string(16383, 'b')); // last 2-byte varint
    // 3-byte varint band ([16384, 2M)).
    v.push_back(std::string(16384, 'c')); // first 3-byte varint
    v.push_back(std::string(20000, 'c'));

    test_encode_decode_page<FieldType::OLAP_FIELD_TYPE_STRING>(v);
}

// V3-specific: cross-check the raw trailer layout. data_block_size sits
// directly before num_elems, both little-endian uint32_t.
TEST_F(BinaryPlainPageV3Test, TestRawTrailerLayout) {
    std::vector<std::string> src = {"abc", "defgh", "ij"}; // sizes 3, 5, 2 = 10 bytes data
    std::vector<Slice> slices;
    for (const auto& s : src) slices.emplace_back(s);

    OwnedSlice owned = build_page<FieldType::OLAP_FIELD_TYPE_VARCHAR>(slices);
    Slice page = owned.slice();
    ASSERT_GE(page.size, 2 * sizeof(uint32_t));

    const auto* trailer = reinterpret_cast<const uint8_t*>(page.data + page.size - 8);
    uint32_t data_block_size = decode_fixed32_le(trailer);
    uint32_t num_elems = decode_fixed32_le(trailer + sizeof(uint32_t));

    EXPECT_EQ(3, num_elems);
    EXPECT_EQ(3 + 5 + 2, data_block_size);

    // Data bytes are contiguous from offset 0; spot-check the first byte of
    // each entry to confirm V3 does not interleave lengths.
    EXPECT_EQ('a', page.data[0]);
    EXPECT_EQ('d', page.data[3]);
    EXPECT_EQ('i', page.data[8]);
}

// V3-specific: corruption rejection. Truncated trailer must be detected.
TEST_F(BinaryPlainPageV3Test, TestCorruptionTooSmall) {
    OwnedSlice owned = build_page<FieldType::OLAP_FIELD_TYPE_VARCHAR>({Slice("x")});
    // Shrink the page below the V3 trailer size (2 * uint32_t).
    Slice page = owned.slice();
    page.size = sizeof(uint32_t); // intentionally short
    std::unique_ptr<DataPage> decoded_page;
    Status st = apply_pre_decode(page, decoded_page);
    EXPECT_FALSE(st.ok());
}

// V3-specific: a data_block_size close to UINT32_MAX must not pass the bounds
// check via uint32 wraparound. With the old `data_block_size + kV3TrailerSize`
// comparison, picking data_block_size = UINT32_MAX - 4 would overflow back to
// a small value (< data.size) and slip through. The subtraction form rejects
// it cleanly.
TEST_F(BinaryPlainPageV3Test, TestCorruptionDataBlockSizeOverflow) {
    OwnedSlice owned = build_page<FieldType::OLAP_FIELD_TYPE_VARCHAR>({Slice("hello")});
    Slice page = owned.slice();
    auto* trailer = reinterpret_cast<uint8_t*>(const_cast<char*>(page.data) + page.size - 8);
    encode_fixed32_le(trailer, std::numeric_limits<uint32_t>::max() - 4);
    std::unique_ptr<DataPage> decoded_page;
    Status st = apply_pre_decode(page, decoded_page);
    EXPECT_FALSE(st.ok());
}

// V3-specific: data_block_size lying about how much data is present must be
// rejected before we try to read past the page.
TEST_F(BinaryPlainPageV3Test, TestCorruptionInflatedDataBlockSize) {
    OwnedSlice owned = build_page<FieldType::OLAP_FIELD_TYPE_VARCHAR>({Slice("hello")});
    Slice page = owned.slice();
    // Rewrite the data_block_size field (8 bytes from end) to a value larger
    // than the actual data section.
    auto* trailer = reinterpret_cast<uint8_t*>(const_cast<char*>(page.data) + page.size - 8);
    encode_fixed32_le(trailer, static_cast<uint32_t>(page.size + 1));
    std::unique_ptr<DataPage> decoded_page;
    Status st = apply_pre_decode(page, decoded_page);
    EXPECT_FALSE(st.ok());
}

} // namespace segment_v2
} // namespace doris
