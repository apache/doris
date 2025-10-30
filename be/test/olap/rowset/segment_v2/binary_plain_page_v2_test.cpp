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

#include "olap/rowset/segment_v2/binary_plain_page_v2.h"

#include <gtest/gtest.h>

#include <iostream>
#include <memory>
#include <vector>

#include "common/logging.h"
#include "olap/olap_common.h"
#include "olap/page_cache.h"
#include "olap/rowset/segment_v2/binary_plain_page_v2_pre_decoder.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "olap/types.h"
#include "vec/columns/column_string.h"

namespace doris {
namespace segment_v2 {

class BinaryPlainPageV2Test : public testing::Test {
public:
    BinaryPlainPageV2Test() = default;
    virtual ~BinaryPlainPageV2Test() = default;

    // Helper method to apply pre-decode step for BinaryPlainPageV2
    // Similar to decode_bitshuffle_page in BinaryDictPageTest
    Status apply_pre_decode(Slice& page_slice, std::unique_ptr<DataPage>& decoded_page) {
        BinaryPlainPageV2PreDecoder pre_decoder;
        return pre_decoder.decode(&decoded_page, &page_slice, 0, false, PageTypePB::DATA_PAGE, "");
    }

    template <FieldType Type>
    void test_encode_decode_page(const std::vector<std::string>& src_strings) {
        std::vector<Slice> slices;
        for (const auto& str : src_strings) {
            slices.emplace_back(str);
        }

        // Build the page
        PageBuilderOptions builder_options;
        builder_options.data_page_size = 256 * 1024;

        PageBuilder* builder_ptr = nullptr;
        Status status = BinaryPlainPageV2Builder<Type>::create(&builder_ptr, builder_options);
        EXPECT_TRUE(status.ok());
        std::unique_ptr<PageBuilder> page_builder_wrapper(builder_ptr);
        auto* page_builder = static_cast<BinaryPlainPageV2Builder<Type>*>(builder_ptr);

        size_t count = slices.size();
        const Slice* ptr = slices.data();
        status = page_builder->add(reinterpret_cast<const uint8_t*>(ptr), &count);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(slices.size(), count);

        OwnedSlice owned_slice;
        status = page_builder->finish(&owned_slice);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(slices.size(), page_builder->count());

        // Check first and last value
        Slice first_value;
        status = page_builder->get_first_value(&first_value);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(slices[0], first_value);

        Slice last_value;
        status = page_builder->get_last_value(&last_value);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(slices[slices.size() - 1], last_value);

        // Decode the page
        // First apply pre-decode to convert V2 format to V1 format
        Slice page_slice = owned_slice.slice();
        std::unique_ptr<DataPage> decoded_page;
        status = apply_pre_decode(page_slice, decoded_page);
        EXPECT_TRUE(status.ok());

        PageDecoderOptions decoder_options;
        BinaryPlainPageV2Decoder<Type> page_decoder(page_slice, decoder_options);
        status = page_decoder.init();
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(slices.size(), page_decoder.count());

        // Read all values
        vectorized::MutableColumnPtr column = vectorized::ColumnString::create();
        size_t num_to_read = slices.size();
        status = page_decoder.next_batch(&num_to_read, column);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(slices.size(), num_to_read);
        EXPECT_EQ(slices.size(), column->size());

        // Verify values
        auto* string_column = assert_cast<vectorized::ColumnString*>(column.get());
        for (size_t i = 0; i < slices.size(); ++i) {
            EXPECT_EQ(src_strings[i], string_column->get_data_at(i).to_string())
                    << "Mismatch at index " << i;
        }
    }

    template <FieldType Type>
    void test_seek_in_page(const std::vector<std::string>& src_strings) {
        std::vector<Slice> slices;
        for (const auto& str : src_strings) {
            slices.emplace_back(str);
        }

        // Build the page
        PageBuilderOptions builder_options;
        builder_options.data_page_size = 256 * 1024;

        PageBuilder* builder_ptr = nullptr;
        Status status = BinaryPlainPageV2Builder<Type>::create(&builder_ptr, builder_options);
        EXPECT_TRUE(status.ok());
        std::unique_ptr<PageBuilder> page_builder_wrapper(builder_ptr);
        auto* page_builder = static_cast<BinaryPlainPageV2Builder<Type>*>(builder_ptr);

        size_t count = slices.size();
        const Slice* ptr = slices.data();
        status = page_builder->add(reinterpret_cast<const uint8_t*>(ptr), &count);
        EXPECT_TRUE(status.ok());

        OwnedSlice owned_slice;
        status = page_builder->finish(&owned_slice);
        EXPECT_TRUE(status.ok());

        // Decode and seek
        // First apply pre-decode to convert V2 format to V1 format
        Slice page_slice = owned_slice.slice();
        std::unique_ptr<DataPage> decoded_page;
        status = apply_pre_decode(page_slice, decoded_page);
        EXPECT_TRUE(status.ok());

        PageDecoderOptions decoder_options;
        BinaryPlainPageV2Decoder<Type> page_decoder(page_slice, decoder_options);
        status = page_decoder.init();
        EXPECT_TRUE(status.ok());

        // Seek to different positions
        std::vector<size_t> seek_positions = {0, 2, slices.size() / 2, slices.size() - 1};
        for (size_t seek_pos : seek_positions) {
            if (seek_pos >= slices.size()) continue;

            status = page_decoder.seek_to_position_in_page(seek_pos);
            EXPECT_TRUE(status.ok());
            EXPECT_EQ(seek_pos, page_decoder.current_index());

            vectorized::MutableColumnPtr column = vectorized::ColumnString::create();
            size_t num_to_read = 1;
            status = page_decoder.next_batch(&num_to_read, column);
            EXPECT_TRUE(status.ok());
            EXPECT_EQ(1, num_to_read);
            auto* string_column = assert_cast<vectorized::ColumnString*>(column.get());
            EXPECT_EQ(src_strings[seek_pos], string_column->get_data_at(0).to_string())
                    << "Mismatch at seek position " << seek_pos;
        }
    }

    template <FieldType Type>
    void test_read_by_rowids(const std::vector<std::string>& src_strings) {
        std::vector<Slice> slices;
        for (const auto& str : src_strings) {
            slices.emplace_back(str);
        }

        // Build the page
        PageBuilderOptions builder_options;
        builder_options.data_page_size = 256 * 1024;

        PageBuilder* builder_ptr = nullptr;
        Status status = BinaryPlainPageV2Builder<Type>::create(&builder_ptr, builder_options);
        EXPECT_TRUE(status.ok());
        std::unique_ptr<PageBuilder> page_builder_wrapper(builder_ptr);
        auto* page_builder = static_cast<BinaryPlainPageV2Builder<Type>*>(builder_ptr);

        size_t count = slices.size();
        const Slice* ptr = slices.data();
        status = page_builder->add(reinterpret_cast<const uint8_t*>(ptr), &count);
        EXPECT_TRUE(status.ok());

        OwnedSlice owned_slice;
        status = page_builder->finish(&owned_slice);
        EXPECT_TRUE(status.ok());

        // Decode
        // First apply pre-decode to convert V2 format to V1 format
        Slice page_slice = owned_slice.slice();
        std::unique_ptr<DataPage> decoded_page;
        status = apply_pre_decode(page_slice, decoded_page);
        EXPECT_TRUE(status.ok());

        PageDecoderOptions decoder_options;
        BinaryPlainPageV2Decoder<Type> page_decoder(page_slice, decoder_options);
        status = page_decoder.init();
        EXPECT_TRUE(status.ok());

        // Read by specific rowids
        std::vector<rowid_t> rowids;
        rowids.push_back(0);
        rowids.push_back(2);
        rowids.push_back(3);
        rowids.push_back(static_cast<rowid_t>(slices.size() - 1));
        ordinal_t page_first_ordinal = 0;

        vectorized::MutableColumnPtr column = vectorized::ColumnString::create();
        size_t num_to_read = rowids.size();
        status = page_decoder.read_by_rowids(rowids.data(), page_first_ordinal, &num_to_read,
                                             column);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(rowids.size(), num_to_read);

        // Verify values
        auto* string_column = assert_cast<vectorized::ColumnString*>(column.get());
        for (size_t i = 0; i < rowids.size(); ++i) {
            EXPECT_EQ(src_strings[rowids[i]], string_column->get_data_at(i).to_string())
                    << "Mismatch at rowid " << rowids[i];
        }
    }

    template <FieldType Type>
    void test_empty_page() {
        // Build empty page
        PageBuilderOptions builder_options;
        builder_options.data_page_size = 256 * 1024;

        PageBuilder* builder_ptr = nullptr;
        Status status = BinaryPlainPageV2Builder<Type>::create(&builder_ptr, builder_options);
        EXPECT_TRUE(status.ok());
        std::unique_ptr<PageBuilder> page_builder_wrapper(builder_ptr);
        auto* page_builder = static_cast<BinaryPlainPageV2Builder<Type>*>(builder_ptr);

        OwnedSlice owned_slice;
        status = page_builder->finish(&owned_slice);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(0, page_builder->count());

        // Try to get first/last value from empty page
        Slice value;
        status = page_builder->get_first_value(&value);
        EXPECT_FALSE(status.ok());

        status = page_builder->get_last_value(&value);
        EXPECT_FALSE(status.ok());

        // Decode empty page
        // First apply pre-decode to convert V2 format to V1 format
        Slice page_slice = owned_slice.slice();
        std::unique_ptr<DataPage> decoded_page;
        status = apply_pre_decode(page_slice, decoded_page);
        EXPECT_TRUE(status.ok());

        PageDecoderOptions decoder_options;
        BinaryPlainPageV2Decoder<Type> page_decoder(page_slice, decoder_options);
        status = page_decoder.init();
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(0, page_decoder.count());

        // Try to read from empty page
        vectorized::MutableColumnPtr column = vectorized::ColumnString::create();
        size_t num_to_read = 1;
        status = page_decoder.next_batch(&num_to_read, column);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(0, num_to_read);
        EXPECT_EQ(0, column->size());
    }

    template <FieldType Type>
    void test_page_full() {
        // Test page size limit
        PageBuilderOptions builder_options;
        builder_options.data_page_size = 128; // Small page size

        PageBuilder* builder_ptr = nullptr;
        Status status = BinaryPlainPageV2Builder<Type>::create(&builder_ptr, builder_options);
        EXPECT_TRUE(status.ok());
        std::unique_ptr<PageBuilder> page_builder_wrapper(builder_ptr);
        auto* page_builder = static_cast<BinaryPlainPageV2Builder<Type>*>(builder_ptr);

        std::vector<std::string> src_strings;
        for (int i = 0; i < 100; ++i) {
            src_strings.push_back("test_string_" + std::to_string(i));
        }

        std::vector<Slice> slices;
        for (const auto& str : src_strings) {
            slices.emplace_back(str);
        }

        size_t added = 0;
        for (size_t i = 0; i < slices.size(); ++i) {
            if (page_builder->is_page_full()) {
                break;
            }

            size_t count = 1;
            const Slice* ptr = &slices[i];
            status = page_builder->add(reinterpret_cast<const uint8_t*>(ptr), &count);
            EXPECT_TRUE(status.ok());

            if (count > 0) {
                added++;
            }
        }

        EXPECT_GT(added, 0);
        EXPECT_LT(added, slices.size()); // Should not add all strings due to page size limit
        EXPECT_TRUE(page_builder->is_page_full());
    }

    template <FieldType Type>
    void test_various_length_strings() {
        std::vector<std::string> src_strings;

        // Empty string
        src_strings.push_back("");

        // Short strings
        src_strings.push_back("a");
        src_strings.push_back("ab");

        // Medium strings
        src_strings.push_back("Hello, World!");
        src_strings.push_back("Apache Doris is great");

        // Long string
        src_strings.push_back(std::string(1000, 'x'));

        // String with special characters
        src_strings.push_back("test\n\r\t");
        src_strings.push_back("中文测试");

        test_encode_decode_page<Type>(src_strings);
    }

    template <FieldType Type>
    void test_get_dict_word() {
        std::vector<std::string> src_strings = {"apple", "banana", "cherry", "date"};
        std::vector<Slice> slices;
        for (const auto& str : src_strings) {
            slices.emplace_back(str);
        }

        // Build the page
        PageBuilderOptions builder_options;
        builder_options.data_page_size = 256 * 1024;

        PageBuilder* builder_ptr = nullptr;
        Status status = BinaryPlainPageV2Builder<Type>::create(&builder_ptr, builder_options);
        EXPECT_TRUE(status.ok());
        std::unique_ptr<PageBuilder> page_builder_wrapper(builder_ptr);
        auto* page_builder = static_cast<BinaryPlainPageV2Builder<Type>*>(builder_ptr);

        size_t count = slices.size();
        const Slice* ptr = slices.data();
        status = page_builder->add(reinterpret_cast<const uint8_t*>(ptr), &count);
        EXPECT_TRUE(status.ok());

        // Test get_dict_word before finish
        for (uint32_t i = 0; i < slices.size(); ++i) {
            Slice word;
            status = page_builder->get_dict_word(i, &word);
            EXPECT_TRUE(status.ok());
            EXPECT_EQ(src_strings[i], word.to_string()) << "Mismatch at index " << i;
        }

        // Test invalid value_code
        Slice word;
        status = page_builder->get_dict_word(slices.size(), &word);
        EXPECT_FALSE(status.ok());

        OwnedSlice owned_slice;
        status = page_builder->finish(&owned_slice);
        EXPECT_TRUE(status.ok());
    }

    template <FieldType Type>
    void test_reset() {
        PageBuilderOptions builder_options;
        builder_options.data_page_size = 256 * 1024;

        PageBuilder* builder_ptr = nullptr;
        Status status = BinaryPlainPageV2Builder<Type>::create(&builder_ptr, builder_options);
        EXPECT_TRUE(status.ok());
        std::unique_ptr<PageBuilder> page_builder_wrapper(builder_ptr);
        auto* page_builder = static_cast<BinaryPlainPageV2Builder<Type>*>(builder_ptr);

        // Add some data
        std::vector<std::string> src_strings = {"test1", "test2"};
        std::vector<Slice> slices;
        for (const auto& str : src_strings) {
            slices.emplace_back(str);
        }

        size_t count = slices.size();
        const Slice* ptr = slices.data();
        status = page_builder->add(reinterpret_cast<const uint8_t*>(ptr), &count);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(2, page_builder->count());

        // Reset
        status = page_builder->reset();
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(0, page_builder->count());

        // Add data again
        count = slices.size();
        status = page_builder->add(reinterpret_cast<const uint8_t*>(ptr), &count);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(2, page_builder->count());
    }

    template <FieldType Type>
    void test_get_dict_word_info() {
        std::vector<std::string> src_strings = {"dog", "cat", "bird"};
        std::vector<Slice> slices;
        for (const auto& str : src_strings) {
            slices.emplace_back(str);
        }

        // Build the page
        PageBuilderOptions builder_options;
        builder_options.data_page_size = 256 * 1024;

        PageBuilder* builder_ptr = nullptr;
        Status status = BinaryPlainPageV2Builder<Type>::create(&builder_ptr, builder_options);
        EXPECT_TRUE(status.ok());
        std::unique_ptr<PageBuilder> page_builder_wrapper(builder_ptr);
        auto* page_builder = static_cast<BinaryPlainPageV2Builder<Type>*>(builder_ptr);

        size_t count = slices.size();
        const Slice* ptr = slices.data();
        status = page_builder->add(reinterpret_cast<const uint8_t*>(ptr), &count);
        EXPECT_TRUE(status.ok());

        OwnedSlice owned_slice;
        status = page_builder->finish(&owned_slice);
        EXPECT_TRUE(status.ok());

        // Decode
        // First apply pre-decode to convert V2 format to V1 format
        Slice page_slice = owned_slice.slice();
        std::unique_ptr<DataPage> decoded_page;
        status = apply_pre_decode(page_slice, decoded_page);
        EXPECT_TRUE(status.ok());

        PageDecoderOptions decoder_options;
        BinaryPlainPageV2Decoder<Type> page_decoder(page_slice, decoder_options);
        status = page_decoder.init();
        EXPECT_TRUE(status.ok());

        // Get dict word info
        std::vector<StringRef> dict_word_info(page_decoder.count());
        status = page_decoder.get_dict_word_info(dict_word_info.data());
        EXPECT_TRUE(status.ok());

        // Verify
        for (size_t i = 0; i < src_strings.size(); ++i) {
            std::string actual(dict_word_info[i].data, dict_word_info[i].size);
            EXPECT_EQ(src_strings[i], actual) << "Mismatch at index " << i;
        }
    }
};

// Test cases for VARCHAR type
TEST_F(BinaryPlainPageV2Test, TestEncodeDecodeVarchar) {
    std::vector<std::string> src_strings = {"Hello", "World", "Apache", "Doris"};
    test_encode_decode_page<FieldType::OLAP_FIELD_TYPE_VARCHAR>(src_strings);
}

TEST_F(BinaryPlainPageV2Test, TestSeekVarchar) {
    std::vector<std::string> src_strings = {"a", "b", "c", "d", "e", "f"};
    test_seek_in_page<FieldType::OLAP_FIELD_TYPE_VARCHAR>(src_strings);
}

TEST_F(BinaryPlainPageV2Test, TestReadByRowidsVarchar) {
    std::vector<std::string> src_strings = {"first", "second", "third", "fourth", "fifth"};
    test_read_by_rowids<FieldType::OLAP_FIELD_TYPE_VARCHAR>(src_strings);
}

TEST_F(BinaryPlainPageV2Test, TestEmptyPageVarchar) {
    test_empty_page<FieldType::OLAP_FIELD_TYPE_VARCHAR>();
}

TEST_F(BinaryPlainPageV2Test, TestPageFullVarchar) {
    test_page_full<FieldType::OLAP_FIELD_TYPE_VARCHAR>();
}

TEST_F(BinaryPlainPageV2Test, TestVariousLengthStringsVarchar) {
    test_various_length_strings<FieldType::OLAP_FIELD_TYPE_VARCHAR>();
}

TEST_F(BinaryPlainPageV2Test, TestGetDictWordVarchar) {
    test_get_dict_word<FieldType::OLAP_FIELD_TYPE_VARCHAR>();
}

TEST_F(BinaryPlainPageV2Test, TestResetVarchar) {
    test_reset<FieldType::OLAP_FIELD_TYPE_VARCHAR>();
}

TEST_F(BinaryPlainPageV2Test, TestGetDictWordInfoVarchar) {
    test_get_dict_word_info<FieldType::OLAP_FIELD_TYPE_VARCHAR>();
}

// Test cases for STRING type
TEST_F(BinaryPlainPageV2Test, TestEncodeDecodeString) {
    std::vector<std::string> src_strings = {"String1", "String2", "String3"};
    test_encode_decode_page<FieldType::OLAP_FIELD_TYPE_STRING>(src_strings);
}

TEST_F(BinaryPlainPageV2Test, TestSeekString) {
    std::vector<std::string> src_strings = {"s1", "s2", "s3", "s4", "s5"};
    test_seek_in_page<FieldType::OLAP_FIELD_TYPE_STRING>(src_strings);
}

TEST_F(BinaryPlainPageV2Test, TestReadByRowidsString) {
    std::vector<std::string> src_strings = {"row1", "row2", "row3", "row4"};
    test_read_by_rowids<FieldType::OLAP_FIELD_TYPE_STRING>(src_strings);
}

TEST_F(BinaryPlainPageV2Test, TestEmptyPageString) {
    test_empty_page<FieldType::OLAP_FIELD_TYPE_STRING>();
}

// Test cases for CHAR type
TEST_F(BinaryPlainPageV2Test, TestEncodeDecodeChar) {
    std::vector<std::string> src_strings = {"char1", "char2", "char3"};
    test_encode_decode_page<FieldType::OLAP_FIELD_TYPE_CHAR>(src_strings);
}

// Test for large number of strings
TEST_F(BinaryPlainPageV2Test, TestLargeNumberOfStrings) {
    std::vector<std::string> src_strings;
    for (int i = 0; i < 1000; ++i) {
        src_strings.push_back("string_" + std::to_string(i));
    }
    test_encode_decode_page<FieldType::OLAP_FIELD_TYPE_VARCHAR>(src_strings);
}

// Test for single string
TEST_F(BinaryPlainPageV2Test, TestSingleString) {
    std::vector<std::string> src_strings = {"single"};
    test_encode_decode_page<FieldType::OLAP_FIELD_TYPE_VARCHAR>(src_strings);
}

// Test sequential read after seek
TEST_F(BinaryPlainPageV2Test, TestSeekAndRead) {
    std::vector<std::string> src_strings = {"a", "b", "c", "d", "e"};
    std::vector<Slice> slices;
    for (const auto& str : src_strings) {
        slices.emplace_back(str);
    }

    // Build the page
    PageBuilderOptions builder_options;
    builder_options.data_page_size = 256 * 1024;

    PageBuilder* builder_ptr = nullptr;
    Status status = BinaryPlainPageV2Builder<FieldType::OLAP_FIELD_TYPE_VARCHAR>::create(
            &builder_ptr, builder_options);
    EXPECT_TRUE(status.ok());
    std::unique_ptr<PageBuilder> page_builder_wrapper(builder_ptr);
    auto* page_builder =
            static_cast<BinaryPlainPageV2Builder<FieldType::OLAP_FIELD_TYPE_VARCHAR>*>(builder_ptr);

    size_t count = slices.size();
    const Slice* ptr = slices.data();
    status = page_builder->add(reinterpret_cast<const uint8_t*>(ptr), &count);
    EXPECT_TRUE(status.ok());

    OwnedSlice owned_slice;
    status = page_builder->finish(&owned_slice);
    EXPECT_TRUE(status.ok());

    // Decode
    // First apply pre-decode to convert V2 format to V1 format
    Slice page_slice = owned_slice.slice();
    std::unique_ptr<DataPage> decoded_page;
    status = apply_pre_decode(page_slice, decoded_page);
    EXPECT_TRUE(status.ok());

    PageDecoderOptions decoder_options;
    BinaryPlainPageV2Decoder<FieldType::OLAP_FIELD_TYPE_VARCHAR> page_decoder(page_slice,
                                                                              decoder_options);
    status = page_decoder.init();
    EXPECT_TRUE(status.ok());

    // Seek to position 2
    status = page_decoder.seek_to_position_in_page(2);
    EXPECT_TRUE(status.ok());

    // Read remaining 3 strings
    vectorized::MutableColumnPtr column = vectorized::ColumnString::create();
    size_t num_to_read = 10; // Request more than available
    status = page_decoder.next_batch(&num_to_read, column);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(3, num_to_read); // Should only read 3 (c, d, e)
    auto* string_column = assert_cast<vectorized::ColumnString*>(column.get());
    EXPECT_EQ("c", string_column->get_data_at(0).to_string());
    EXPECT_EQ("d", string_column->get_data_at(1).to_string());
    EXPECT_EQ("e", string_column->get_data_at(2).to_string());
}

} // namespace segment_v2
} // namespace doris
