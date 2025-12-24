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

#include "olap/rowset/segment_v2/binary_dict_page.h"

#include <gtest/gtest.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/binary_dict_page_pre_decoder.h"
#include "olap/rowset/segment_v2/binary_plain_page.h"
#include "olap/rowset/segment_v2/binary_plain_page_v2.h"
#include "olap/rowset/segment_v2/binary_plain_page_v2_pre_decoder.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "olap/types.h"
#include "runtime/exec_env.h"
#include "util/debug_util.h"
#include "vec/columns/column_string.h"

namespace doris {
namespace segment_v2 {

class BinaryDictPageTest : public testing::Test {
public:
    BinaryDictPageTest() {
        _resolver = std::make_unique<segment_v2::EncodingInfoResolver>();
        ExecEnv::GetInstance()->_encoding_info_resolver = _resolver.get();
    }
    ~BinaryDictPageTest() override { ExecEnv::GetInstance()->_encoding_info_resolver = nullptr; }

    // Generate test data with specified pattern
    std::vector<std::string> generate_test_data(size_t count, const std::string& prefix,
                                                size_t min_len = 5, size_t max_len = 20) {
        std::vector<std::string> result;
        result.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            size_t len = min_len + (i % (max_len - min_len + 1));
            std::string str = prefix + std::to_string(i);
            // Pad to desired length
            while (str.length() < len) {
                str += "_";
            }
            result.push_back(str);
        }
        return result;
    }

    // Create dict page decoder based on encoding type
    // The decoded_page parameter is passed by reference to ensure the decoded data's lifetime
    // is managed by the caller, preventing the data from being freed prematurely
    std::unique_ptr<PageDecoder> create_dict_page_decoder(Slice& dict_slice,
                                                          EncodingTypePB encoding_type,
                                                          std::unique_ptr<DataPage>& decoded_page) {
        // Apply pre-decode for BinaryPlainPageV2
        if (encoding_type == PLAIN_ENCODING_V2) {
            BinaryPlainPageV2PreDecoder pre_decoder;
            Status status = pre_decoder.decode(&decoded_page, &dict_slice, 0, false,
                                               PageTypePB::DATA_PAGE, "");
            if (!status.ok()) {
                return nullptr;
            }
        }

        PageDecoderOptions dict_decoder_options;
        std::unique_ptr<PageDecoder> dict_page_decoder;

        if (encoding_type == PLAIN_ENCODING) {
            dict_page_decoder.reset(new BinaryPlainPageDecoder<FieldType::OLAP_FIELD_TYPE_VARCHAR>(
                    dict_slice, dict_decoder_options));
        } else if (encoding_type == PLAIN_ENCODING_V2) {
            dict_page_decoder.reset(
                    new BinaryPlainPageV2Decoder<FieldType::OLAP_FIELD_TYPE_VARCHAR>(
                            dict_slice, dict_decoder_options));
        } else {
            return nullptr;
        }

        Status status = dict_page_decoder->init();
        if (!status.ok()) {
            return nullptr;
        }

        return dict_page_decoder;
    }

    // Apply pre-decode for BinaryDictPage data pages
    // This method handles all encoding types (bitshuffle, plain V1, plain V2)
    Status apply_pre_decode(Slice& page_slice, std::unique_ptr<DataPage>& decoded_page) {
        BinaryDictPagePreDecoder pre_decoder;
        return pre_decoder.decode(&decoded_page, &page_slice, 0, false, PageTypePB::DATA_PAGE, "");
    }

    // Create and setup a BinaryDictPageBuilder with data
    std::unique_ptr<BinaryDictPageBuilder> create_and_add_data(const std::vector<Slice>& slices,
                                                               const PageBuilderOptions& options,
                                                               size_t* added_count = nullptr) {
        PageBuilder* builder_ptr = nullptr;
        Status status = BinaryDictPageBuilder::create(&builder_ptr, options);
        if (!status.ok()) {
            return nullptr;
        }
        std::unique_ptr<BinaryDictPageBuilder> page_builder(
                static_cast<BinaryDictPageBuilder*>(builder_ptr));

        size_t count = slices.size();
        const Slice* ptr = &slices[0];
        status = page_builder->add(reinterpret_cast<const uint8_t*>(ptr), &count);
        if (!status.ok()) {
            return nullptr;
        }

        if (added_count) {
            *added_count = count;
        }

        return page_builder;
    }

    // Test encoding type for given preference and data
    void test_encoding_type(bool use_v2, const std::vector<Slice>& slices,
                            EncodingTypePB expected_encoding) {
        PageBuilderOptions options;
        options.data_page_size = 256 * 1024;
        options.dict_page_size = 256 * 1024;
        options.encoding_preference.binary_plain_encoding_default_impl =
                use_v2 ? BinaryPlainEncodingTypePB::BINARY_PLAIN_ENCODING_V2
                       : BinaryPlainEncodingTypePB::BINARY_PLAIN_ENCODING_V1;

        auto page_builder = create_and_add_data(slices, options);
        ASSERT_NE(nullptr, page_builder);

        // Get dictionary page
        OwnedSlice dict_slice;
        Status status = page_builder->get_dictionary_page(&dict_slice);
        EXPECT_TRUE(status.ok());

        // Check encoding type
        EncodingTypePB dict_encoding_type;
        status = page_builder->get_dictionary_page_encoding(&dict_encoding_type);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(expected_encoding, dict_encoding_type)
                << "Expected encoding type does not match when use_v2=" << use_v2;
    }

    void test_by_small_data_size(const std::vector<Slice>& slices,
                                 EncodingPreference encoding_preference = EncodingPreference()) {
        // Encode
        PageBuilderOptions options;
        options.data_page_size = 256 * 1024;
        options.dict_page_size = 256 * 1024;
        options.encoding_preference = encoding_preference;

        PageBuilder* builder_ptr = nullptr;
        Status ret0 = BinaryDictPageBuilder::create(&builder_ptr, options);
        EXPECT_TRUE(ret0.ok());
        std::unique_ptr<PageBuilder> builder_wrapper(builder_ptr);
        auto* page_builder = static_cast<BinaryDictPageBuilder*>(builder_ptr);

        size_t count = slices.size();

        const Slice* ptr = &slices[0];
        Status ret = page_builder->add(reinterpret_cast<const uint8_t*>(ptr), &count);
        EXPECT_TRUE(ret.ok());

        OwnedSlice s;
        ret = page_builder->finish(&s);
        EXPECT_TRUE(ret.ok());
        EXPECT_EQ(slices.size(), page_builder->count());
        EXPECT_FALSE(page_builder->is_page_full());

        // Check first value and last value
        Slice first_value;
        ret = page_builder->get_first_value(&first_value);
        EXPECT_TRUE(ret.ok());
        EXPECT_EQ(slices[0], first_value);
        Slice last_value;
        ret = page_builder->get_last_value(&last_value);
        EXPECT_TRUE(ret.ok());
        EXPECT_EQ(slices[count - 1], last_value);

        // Construct dict page
        OwnedSlice dict_slice;
        Status status = page_builder->get_dictionary_page(&dict_slice);
        EXPECT_TRUE(status.ok());

        // Get dict page encoding type
        EncodingTypePB dict_encoding_type;
        status = page_builder->get_dictionary_page_encoding(&dict_encoding_type);
        EXPECT_TRUE(status.ok());

        // Create dict decoder
        // decoded_dict_page must outlive dict_page_decoder since it holds the decoded data
        Slice dict_page_slice = dict_slice.slice();
        std::unique_ptr<DataPage> decoded_dict_page;
        auto dict_page_decoder =
                create_dict_page_decoder(dict_page_slice, dict_encoding_type, decoded_dict_page);
        ASSERT_NE(nullptr, dict_page_decoder) << "Failed to create dict page decoder";
        EXPECT_EQ(slices.size(), dict_page_decoder->count());

        // Get dict word info using vector instead of VLA
        std::vector<StringRef> dict_word_info(dict_page_decoder->count());
        status = dict_page_decoder->get_dict_word_info(dict_word_info.data());
        EXPECT_TRUE(status.ok());

        // Decode
        PageDecoderOptions decoder_options;

        Slice page_slice = s.slice();
        std::unique_ptr<DataPage> decoded_page;
        status = apply_pre_decode(page_slice, decoded_page);
        EXPECT_TRUE(status.ok());

        BinaryDictPageDecoder page_decoder(page_slice, decoder_options);
        status = page_decoder.init();
        EXPECT_TRUE(status.ok());
        page_decoder.set_dict_decoder(dict_page_decoder->count(), dict_word_info.data());
        EXPECT_EQ(slices.size(), page_decoder.count());

        // Check values using MutableColumnPtr
        vectorized::MutableColumnPtr column = vectorized::ColumnString::create();
        size_t size = slices.size();
        status = page_decoder.next_batch(&size, column);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(slices.size(), size);

        auto* string_column = assert_cast<vectorized::ColumnString*>(column.get());
        // Verify all values match
        for (size_t i = 0; i < slices.size(); ++i) {
            EXPECT_EQ(slices[i].to_string(), string_column->get_data_at(i).to_string())
                    << "Mismatch at index " << i;
        }

        // Test seek functionality with middle position
        if (slices.size() > 2) {
            size_t seek_pos = slices.size() / 2;
            status = page_decoder.seek_to_position_in_page(seek_pos);
            EXPECT_TRUE(status.ok());
            column = vectorized::ColumnString::create();
            size = slices.size() - seek_pos;
            status = page_decoder.next_batch(&size, column);
            EXPECT_TRUE(status.ok());
            EXPECT_EQ(slices.size() - seek_pos, size);
            string_column = assert_cast<vectorized::ColumnString*>(column.get());
            for (size_t i = 0; i < size; ++i) {
                EXPECT_EQ(slices[seek_pos + i].to_string(),
                          string_column->get_data_at(i).to_string())
                        << "Mismatch at seek position " << seek_pos << " + " << i;
            }
        }

        // Test read_by_rowids functionality
        if (slices.size() >= 4) {
            // Select specific rowids to read
            std::vector<rowid_t> rowids;
            rowids.push_back(0);                                       // First
            rowids.push_back(2);                                       // Middle
            rowids.push_back(slices.size() / 2);                       // Half
            rowids.push_back(static_cast<rowid_t>(slices.size() - 1)); // Last

            ordinal_t page_first_ordinal = 0;
            column = vectorized::ColumnString::create();
            size_t num_to_read = rowids.size();
            status = page_decoder.read_by_rowids(rowids.data(), page_first_ordinal, &num_to_read,
                                                 column);
            EXPECT_TRUE(status.ok());
            EXPECT_EQ(rowids.size(), num_to_read);

            // Verify values at specific rowids
            string_column = assert_cast<vectorized::ColumnString*>(column.get());
            for (size_t i = 0; i < rowids.size(); ++i) {
                EXPECT_EQ(slices[rowids[i]].to_string(), string_column->get_data_at(i).to_string())
                        << "Mismatch at rowid " << rowids[i] << " (index " << i << ")";
            }
        }
    }

    void test_with_large_data_size(const std::vector<Slice>& contents,
                                   EncodingPreference encoding_preference = EncodingPreference()) {
        // Encode
        PageBuilderOptions options;
        // Use smaller page sizes to ensure we trigger fallback scenario
        // where dictionary gets full and we switch to plain encoding
        options.data_page_size = 64 * 1024; // 64KB data page
        options.dict_page_size = 1024;      // 1KB dict page to trigger fallback
        options.encoding_preference = encoding_preference;

        PageBuilder* builder_ptr = nullptr;
        Status ret0 = BinaryDictPageBuilder::create(&builder_ptr, options);
        EXPECT_TRUE(ret0.ok());
        std::unique_ptr<PageBuilder> builder_wrapper(builder_ptr);
        auto* page_builder = static_cast<BinaryDictPageBuilder*>(builder_ptr);

        size_t count = contents.size();
        std::vector<OwnedSlice> results;
        std::vector<size_t> page_start_ids;
        size_t total_size = 0;
        page_start_ids.push_back(0);
        for (size_t i = 0; i < count;) {
            size_t add_num = 1;
            const Slice* ptr = &contents[i];
            Status ret = page_builder->add(reinterpret_cast<const uint8_t*>(ptr), &add_num);
            EXPECT_TRUE(ret.ok());
            if (page_builder->is_page_full()) {
                OwnedSlice s;
                ret = page_builder->finish(&s);
                EXPECT_TRUE(ret.ok());
                total_size += s.slice().size;
                results.emplace_back(std::move(s));
                ret = page_builder->reset();
                EXPECT_TRUE(ret.ok());
                page_start_ids.push_back(i + 1);
            }
            i += add_num;
        }
        OwnedSlice s;
        Status ret = page_builder->finish(&s);
        EXPECT_TRUE(ret.ok());
        total_size += s.slice().size;
        results.emplace_back(std::move(s));

        page_start_ids.push_back(count);

        OwnedSlice dict_slice;
        Status status = page_builder->get_dictionary_page(&dict_slice);
        size_t data_size = total_size;
        total_size += dict_slice.slice().size;
        EXPECT_TRUE(status.ok());

        // Get dict page encoding type
        EncodingTypePB dict_encoding_type;
        status = page_builder->get_dictionary_page_encoding(&dict_encoding_type);
        EXPECT_TRUE(status.ok());

        // Check if we have fallback scenario (both dict and plain pages)
        size_t dict_entries = 0;
        if (dict_slice.slice().size > 0) {
            Slice temp_dict_slice = dict_slice.slice();
            std::unique_ptr<DataPage> temp_decoded_page;
            auto temp_decoder = create_dict_page_decoder(temp_dict_slice, dict_encoding_type,
                                                         temp_decoded_page);
            if (temp_decoder) {
                dict_entries = temp_decoder->count();
            }
        }

        LOG(INFO) << "total size:" << total_size << ", data size:" << data_size
                  << ", dict size:" << dict_slice.slice().size << ", dict entries:" << dict_entries
                  << ", total entries:" << count << ", result page count:" << results.size()
                  << ", encoding type:"
                  << (dict_encoding_type == PLAIN_ENCODING ? "PLAIN" : "PLAIN_V2");

        // Verify we triggered fallback scenario: dict_entries < total entries
        EXPECT_GT(results.size(), 1) << "Should have multiple pages";
        EXPECT_LT(dict_entries, count)
                << "Should have fallback pages (dict entries < total entries)";

        // Create dict decoder for dictionary page
        // decoded_dict_page must outlive dict_page_decoder since it holds the decoded data
        Slice dict_page_slice = dict_slice.slice();
        std::unique_ptr<DataPage> decoded_dict_page;
        auto dict_page_decoder =
                create_dict_page_decoder(dict_page_slice, dict_encoding_type, decoded_dict_page);
        ASSERT_NE(nullptr, dict_page_decoder) << "Failed to create dict page decoder";

        // Get dict word info
        std::vector<StringRef> dict_word_info(dict_page_decoder->count());
        status = dict_page_decoder->get_dict_word_info(dict_word_info.data());
        EXPECT_TRUE(status.ok());

        // Validate by sequentially consuming all data
        size_t current_entry = 0;
        for (size_t page_idx = 0; page_idx < results.size(); ++page_idx) {
            PageDecoderOptions decoder_options;
            Slice page_slice = results[page_idx].slice();

            // First, apply pre-decode for all pages (handles bitshuffle, plain V1, plain V2)
            std::unique_ptr<DataPage> decoded_page;
            status = apply_pre_decode(page_slice, decoded_page);
            EXPECT_TRUE(status.ok()) << "Failed to apply pre-decode for page " << page_idx;

            // Create BinaryDictPageDecoder and check encoding type
            BinaryDictPageDecoder page_decoder(page_slice, decoder_options);
            status = page_decoder.init();
            EXPECT_TRUE(status.ok()) << "Failed to init decoder for page " << page_idx;

            // Check if this page is dict encoded or plain encoded (fallback)
            if (page_decoder.is_dict_encoding()) {
                // Dict encoded page - set dict decoder
                page_decoder.set_dict_decoder(dict_page_decoder->count(), dict_word_info.data());

                // Read all values from this page sequentially
                size_t page_entry_count = page_start_ids[page_idx + 1] - page_start_ids[page_idx];
                vectorized::MutableColumnPtr column = vectorized::ColumnString::create();
                size_t num_to_read = page_entry_count;
                status = page_decoder.next_batch(&num_to_read, column);
                EXPECT_TRUE(status.ok());
                EXPECT_EQ(page_entry_count, num_to_read);

                // Verify all values
                auto* string_column = assert_cast<vectorized::ColumnString*>(column.get());
                for (size_t i = 0; i < page_entry_count; ++i) {
                    std::string expect = contents[current_entry + i].to_string();
                    std::string actual = string_column->get_data_at(i).to_string();
                    EXPECT_EQ(expect, actual)
                            << "Dict page mismatch at page " << page_idx << ", entry " << i
                            << ", global entry " << (current_entry + i);
                }
            } else {
                // Plain encoded page (fallback) - no need to set dict decoder
                // Read all values from this page sequentially
                size_t page_entry_count = page_start_ids[page_idx + 1] - page_start_ids[page_idx];
                vectorized::MutableColumnPtr column = vectorized::ColumnString::create();
                size_t num_to_read = page_entry_count;
                status = page_decoder.next_batch(&num_to_read, column);
                EXPECT_TRUE(status.ok());
                EXPECT_EQ(page_entry_count, num_to_read);

                // Verify all values
                auto* string_column = assert_cast<vectorized::ColumnString*>(column.get());
                for (size_t i = 0; i < page_entry_count; ++i) {
                    std::string expect = contents[current_entry + i].to_string();
                    std::string actual = string_column->get_data_at(i).to_string();
                    EXPECT_EQ(expect, actual)
                            << "Plain page mismatch at page " << page_idx << ", entry " << i
                            << ", global entry " << (current_entry + i);
                }
            }

            current_entry += (page_start_ids[page_idx + 1] - page_start_ids[page_idx]);
        }

        // Verify we consumed all entries
        EXPECT_EQ(count, current_entry) << "Should have consumed all entries";

        // Test seek_to_position_in_page on all pages
        for (size_t page_idx = 0; page_idx < results.size(); ++page_idx) {
            size_t page_entry_count = page_start_ids[page_idx + 1] - page_start_ids[page_idx];
            if (page_entry_count <= 2) {
                continue; // Skip pages with too few entries
            }

            PageDecoderOptions decoder_options;
            Slice page_slice = results[page_idx].slice();

            // Apply pre-decode
            std::unique_ptr<DataPage> decoded_page;
            status = apply_pre_decode(page_slice, decoded_page);
            EXPECT_TRUE(status.ok())
                    << "Failed to apply pre-decode for page " << page_idx << " in seek test";

            // Create decoder
            BinaryDictPageDecoder page_decoder(page_slice, decoder_options);
            status = page_decoder.init();
            EXPECT_TRUE(status.ok())
                    << "Failed to init decoder for page " << page_idx << " in seek test";

            // Set dict decoder if needed
            if (page_decoder.is_dict_encoding()) {
                page_decoder.set_dict_decoder(dict_page_decoder->count(), dict_word_info.data());
            }

            // Seek to middle of page
            size_t seek_pos = page_entry_count / 2;
            status = page_decoder.seek_to_position_in_page(seek_pos);
            EXPECT_TRUE(status.ok()) << "Failed to seek in page " << page_idx;

            // Read from seek position
            vectorized::MutableColumnPtr column = vectorized::ColumnString::create();
            size_t num_to_read = page_entry_count - seek_pos;
            status = page_decoder.next_batch(&num_to_read, column);
            EXPECT_TRUE(status.ok()) << "Failed to read after seek in page " << page_idx;
            EXPECT_EQ(page_entry_count - seek_pos, num_to_read);

            // Verify values
            auto* string_column = assert_cast<vectorized::ColumnString*>(column.get());
            for (size_t i = 0; i < num_to_read; ++i) {
                std::string expect = contents[page_start_ids[page_idx] + seek_pos + i].to_string();
                std::string actual = string_column->get_data_at(i).to_string();
                EXPECT_EQ(expect, actual)
                        << "Seek test mismatch at page " << page_idx << ", position "
                        << (seek_pos + i)
                        << ", is_dict_encoding: " << page_decoder.is_dict_encoding();
            }
        }

        // Test read_by_rowids on all pages
        for (size_t page_idx = 0; page_idx < results.size(); ++page_idx) {
            size_t page_entry_count = page_start_ids[page_idx + 1] - page_start_ids[page_idx];
            if (page_entry_count < 4) {
                continue; // Skip pages with too few entries
            }

            PageDecoderOptions decoder_options;
            Slice page_slice = results[page_idx].slice();

            // Apply pre-decode
            std::unique_ptr<DataPage> decoded_page;
            status = apply_pre_decode(page_slice, decoded_page);
            EXPECT_TRUE(status.ok()) << "Failed to apply pre-decode for page " << page_idx
                                     << " in read_by_rowids test";

            // Create decoder
            BinaryDictPageDecoder page_decoder(page_slice, decoder_options);
            status = page_decoder.init();
            EXPECT_TRUE(status.ok())
                    << "Failed to init decoder for page " << page_idx << " in read_by_rowids test";

            // Set dict decoder if needed
            if (page_decoder.is_dict_encoding()) {
                page_decoder.set_dict_decoder(dict_page_decoder->count(), dict_word_info.data());
            }

            // Select specific rowids within the page
            // rowids should be global rowids (relative to the entire dataset), not page-relative offsets
            ordinal_t page_first_ordinal = page_start_ids[page_idx];
            std::vector<rowid_t> rowids;
            rowids.push_back(page_first_ordinal + 0);                    // First in page
            rowids.push_back(page_first_ordinal + 2);                    // Middle
            rowids.push_back(page_first_ordinal + page_entry_count / 2); // Half
            rowids.push_back(page_first_ordinal + page_entry_count - 1); // Last in page

            vectorized::MutableColumnPtr column = vectorized::ColumnString::create();
            size_t num_to_read = rowids.size();
            status = page_decoder.read_by_rowids(rowids.data(), page_first_ordinal, &num_to_read,
                                                 column);
            EXPECT_TRUE(status.ok()) << "Failed to read_by_rowids in page " << page_idx;
            EXPECT_EQ(rowids.size(), num_to_read)
                    << "Mismatched read count in page " << page_idx
                    << ", page_start_id:" << page_first_ordinal
                    << ", page_entry_count:" << page_entry_count
                    << ", is_dict_encoding: " << page_decoder.is_dict_encoding();

            // Verify values
            auto* string_column = assert_cast<vectorized::ColumnString*>(column.get());
            for (size_t i = 0; i < rowids.size(); ++i) {
                // rowids are global, so we use them directly to index into contents
                std::string expect = contents[rowids[i]].to_string();
                std::string actual = string_column->get_data_at(i).to_string();
                EXPECT_EQ(expect, actual)
                        << "read_by_rowids test mismatch at page " << page_idx << ", global rowid "
                        << rowids[i] << ", is_dict_encoding: " << page_decoder.is_dict_encoding();
            }
        }
    }

private:
    std::unique_ptr<segment_v2::EncodingInfoResolver> _resolver;
};

// Local behavior tests - test specific config behavior
TEST_F(BinaryDictPageTest, TestConfigUsePlainBinaryV2False) {
    std::vector<Slice> slices;
    slices.emplace_back("apple");
    slices.emplace_back("banana");
    slices.emplace_back("cherry");

    test_encoding_type(false, slices, PLAIN_ENCODING);
}

TEST_F(BinaryDictPageTest, TestConfigUsePlainBinaryV2True) {
    std::vector<Slice> slices;
    slices.emplace_back("apple");
    slices.emplace_back("banana");
    slices.emplace_back("cherry");

    test_encoding_type(true, slices, PLAIN_ENCODING_V2);
}

TEST_F(BinaryDictPageTest, TestConfigSwitchBetweenEncodings) {
    std::vector<Slice> slices;
    slices.emplace_back("test1");
    slices.emplace_back("test2");

    // Test with config = false
    test_encoding_type(false, slices, PLAIN_ENCODING);

    // Test with config = true
    test_encoding_type(true, slices, PLAIN_ENCODING_V2);
}

// Test that encoding preference affects the dictionary page encoding type
TEST_F(BinaryDictPageTest, TestConfigAffectsDictionaryPageEncoding) {
    std::vector<Slice> slices;
    slices.emplace_back("apple");
    slices.emplace_back("banana");
    slices.emplace_back("cherry");
    slices.emplace_back("date");
    slices.emplace_back("elderberry");

    // Test with V1 encoding preference
    {
        PageBuilderOptions options;
        options.data_page_size = 256 * 1024;
        options.dict_page_size = 256 * 1024;
        options.encoding_preference.binary_plain_encoding_default_impl =
                BinaryPlainEncodingTypePB::BINARY_PLAIN_ENCODING_V1;

        PageBuilder* builder_ptr = nullptr;
        Status status = BinaryDictPageBuilder::create(&builder_ptr, options);
        EXPECT_TRUE(status.ok());
        std::unique_ptr<PageBuilder> builder_wrapper(builder_ptr);
        auto* page_builder = static_cast<BinaryDictPageBuilder*>(builder_ptr);

        size_t count = slices.size();
        const Slice* ptr = &slices[0];
        status = page_builder->add(reinterpret_cast<const uint8_t*>(ptr), &count);
        EXPECT_TRUE(status.ok());

        // Get dictionary page
        OwnedSlice dict_slice;
        status = page_builder->get_dictionary_page(&dict_slice);
        EXPECT_TRUE(status.ok());

        // Verify dictionary uses PLAIN_ENCODING
        EncodingTypePB dict_encoding_type;
        status = page_builder->get_dictionary_page_encoding(&dict_encoding_type);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(PLAIN_ENCODING, dict_encoding_type)
                << "Dictionary should use PLAIN_ENCODING with V1 preference";

        // Decode dictionary page with BinaryPlainPageDecoder
        PageDecoderOptions dict_decoder_options;
        std::unique_ptr<PageDecoder> dict_page_decoder(
                new BinaryPlainPageDecoder<FieldType::OLAP_FIELD_TYPE_VARCHAR>(
                        dict_slice.slice(), dict_decoder_options));
        status = dict_page_decoder->init();
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(slices.size(), dict_page_decoder->count());
    }

    // Test with V2 encoding preference
    {
        PageBuilderOptions options;
        options.data_page_size = 256 * 1024;
        options.dict_page_size = 256 * 1024;
        options.encoding_preference.binary_plain_encoding_default_impl =
                BinaryPlainEncodingTypePB::BINARY_PLAIN_ENCODING_V2;

        PageBuilder* builder_ptr = nullptr;
        Status status = BinaryDictPageBuilder::create(&builder_ptr, options);
        EXPECT_TRUE(status.ok());
        std::unique_ptr<PageBuilder> builder_wrapper(builder_ptr);
        auto* page_builder = static_cast<BinaryDictPageBuilder*>(builder_ptr);

        size_t count = slices.size();
        const Slice* ptr = &slices[0];
        status = page_builder->add(reinterpret_cast<const uint8_t*>(ptr), &count);
        EXPECT_TRUE(status.ok());

        // Get dictionary page
        OwnedSlice dict_slice;
        status = page_builder->get_dictionary_page(&dict_slice);
        EXPECT_TRUE(status.ok());

        // Verify dictionary uses PLAIN_ENCODING_V2
        EncodingTypePB dict_encoding_type;
        status = page_builder->get_dictionary_page_encoding(&dict_encoding_type);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(PLAIN_ENCODING_V2, dict_encoding_type)
                << "Dictionary should use PLAIN_ENCODING_V2 with V2 preference";

        // Decode dictionary page with BinaryPlainPageV2Decoder
        // First apply pre-decode for BinaryPlainPageV2
        Slice dict_page_slice = dict_slice.slice();
        std::unique_ptr<DataPage> decoded_page;
        BinaryPlainPageV2PreDecoder pre_decoder;
        status = pre_decoder.decode(&decoded_page, &dict_page_slice, 0, false,
                                    PageTypePB::DATA_PAGE, "");
        EXPECT_TRUE(status.ok());

        PageDecoderOptions dict_decoder_options;
        std::unique_ptr<PageDecoder> dict_page_decoder(
                new BinaryPlainPageV2Decoder<FieldType::OLAP_FIELD_TYPE_VARCHAR>(
                        dict_page_slice, dict_decoder_options));
        status = dict_page_decoder->init();
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(slices.size(), dict_page_decoder->count());
    }
}

// Test that encoding preference affects fallback encoding when dictionary is full
TEST_F(BinaryDictPageTest, TestConfigAffectsFallbackEncoding) {
    // Create many unique strings to force dictionary overflow and fallback
    std::vector<std::string> src_strings;
    for (int i = 0; i < 10000; ++i) {
        src_strings.push_back("unique_string_" + std::to_string(i) + "_suffix");
    }

    std::vector<Slice> slices;
    for (const auto& str : src_strings) {
        slices.push_back(str);
    }

    // Test with V1 encoding preference
    {
        PageBuilderOptions options;
        options.data_page_size = 256 * 1024;
        options.dict_page_size = 128; // Small dict size to force fallback
        options.encoding_preference.binary_plain_encoding_default_impl =
                BinaryPlainEncodingTypePB::BINARY_PLAIN_ENCODING_V1;

        PageBuilder* builder_ptr = nullptr;
        Status status = BinaryDictPageBuilder::create(&builder_ptr, options);
        EXPECT_TRUE(status.ok());
        std::unique_ptr<PageBuilder> builder_wrapper(builder_ptr);
        auto* page_builder = static_cast<BinaryDictPageBuilder*>(builder_ptr);

        // Add strings until page is full or fallback happens
        size_t total_added = 0;
        for (size_t i = 0; i < slices.size() && !page_builder->is_page_full(); ++i) {
            size_t count = 1;
            const Slice* ptr = &slices[i];
            status = page_builder->add(reinterpret_cast<const uint8_t*>(ptr), &count);
            EXPECT_TRUE(status.ok());
            if (count > 0) {
                total_added++;
            }
        }

        EXPECT_GT(total_added, 0);
        LOG(INFO) << "Added " << total_added << " entries with V1 preference";

        // Call reset() to trigger fallback encoding setup
        OwnedSlice s;
        status = page_builder->finish(&s);
        EXPECT_TRUE(status.ok());

        status = page_builder->reset();
        EXPECT_TRUE(status.ok());

        // Access private member _fallback_binary_encoding_type to verify
        EXPECT_EQ(PLAIN_ENCODING, page_builder->_fallback_binary_encoding_type)
                << "Fallback encoding should be PLAIN_ENCODING with V1 preference";

        // Also check the dict word page encoding type
        EXPECT_EQ(PLAIN_ENCODING, page_builder->_dict_word_page_encoding_type)
                << "Dict word page encoding should be PLAIN_ENCODING with V1 preference";

        // Check the actual encoding type used (should have fallen back)
        EXPECT_EQ(PLAIN_ENCODING, page_builder->_encoding_type)
                << "Should have fallen back to PLAIN_ENCODING";
    }

    // Test with V2 encoding preference
    {
        PageBuilderOptions options;
        options.data_page_size = 256 * 1024;
        options.dict_page_size = 128; // Small dict size to force fallback
        options.encoding_preference.binary_plain_encoding_default_impl =
                BinaryPlainEncodingTypePB::BINARY_PLAIN_ENCODING_V2;

        PageBuilder* builder_ptr = nullptr;
        Status status = BinaryDictPageBuilder::create(&builder_ptr, options);
        EXPECT_TRUE(status.ok());
        std::unique_ptr<PageBuilder> builder_wrapper(builder_ptr);
        auto* page_builder = static_cast<BinaryDictPageBuilder*>(builder_ptr);

        // Add strings until page is full or fallback happens
        size_t total_added = 0;
        for (size_t i = 0; i < slices.size() && !page_builder->is_page_full(); ++i) {
            size_t count = 1;
            const Slice* ptr = &slices[i];
            status = page_builder->add(reinterpret_cast<const uint8_t*>(ptr), &count);
            EXPECT_TRUE(status.ok());
            if (count > 0) {
                total_added++;
            }
        }

        EXPECT_GT(total_added, 0);
        LOG(INFO) << "Added " << total_added << " entries with V2 preference";

        // Call reset() to trigger fallback encoding setup
        OwnedSlice s;
        status = page_builder->finish(&s);
        EXPECT_TRUE(status.ok());

        status = page_builder->reset();
        EXPECT_TRUE(status.ok());

        // Access private member _fallback_binary_encoding_type to verify
        EXPECT_EQ(PLAIN_ENCODING_V2, page_builder->_fallback_binary_encoding_type)
                << "Fallback encoding should be PLAIN_ENCODING_V2 with V2 preference";

        // Also check the dict word page encoding type
        EXPECT_EQ(PLAIN_ENCODING_V2, page_builder->_dict_word_page_encoding_type)
                << "Dict word page encoding should be PLAIN_ENCODING_V2 with V2 preference";

        // Check the actual encoding type used (should have fallen back)
        EXPECT_EQ(PLAIN_ENCODING_V2, page_builder->_encoding_type)
                << "Should have fallen back to PLAIN_ENCODING_V2";
    }
}

// End-to-end tests - test full encode/decode flow
TEST_F(BinaryDictPageTest, TestBySmallDataSize) {
    std::vector<Slice> slices;
    slices.emplace_back("Individual");
    slices.emplace_back("Lifetime");
    slices.emplace_back("Objective");
    slices.emplace_back("Value");
    slices.emplace_back("Evolution");
    slices.emplace_back("Nature");
    slices.emplace_back("Captain");
    slices.emplace_back("Xmas");
    test_by_small_data_size(slices);
}

TEST_F(BinaryDictPageTest, TestSmallDataWithConfigFalse) {
    auto src_strings = generate_test_data(50, "test_");
    std::vector<Slice> slices;
    for (const auto& str : src_strings) {
        slices.emplace_back(str);
    }

    EncodingPreference encoding_preference;
    encoding_preference.binary_plain_encoding_default_impl =
            BinaryPlainEncodingTypePB::BINARY_PLAIN_ENCODING_V1;
    test_by_small_data_size(slices, encoding_preference);
}

TEST_F(BinaryDictPageTest, TestSmallDataWithConfigTrue) {
    auto src_strings = generate_test_data(50, "test_");
    std::vector<Slice> slices;
    for (const auto& str : src_strings) {
        slices.emplace_back(str);
    }

    EncodingPreference encoding_preference;
    encoding_preference.binary_plain_encoding_default_impl =
            BinaryPlainEncodingTypePB::BINARY_PLAIN_ENCODING_V2;
    test_by_small_data_size(slices, encoding_preference);
}

TEST_F(BinaryDictPageTest, TestLargeDataWithConfigFalse) {
    // Generate large amount of data with some repetition to test dictionary efficiency
    std::vector<std::string> src_strings;
    // Generate 1000 unique strings
    auto unique_strings = generate_test_data(1000, "data_", 10, 50);
    // Repeat them 100 times to create 100k entries
    for (int i = 0; i < 100; ++i) {
        for (const auto& str : unique_strings) {
            src_strings.push_back(str);
        }
    }

    std::vector<Slice> slices;
    for (const auto& str : src_strings) {
        slices.push_back(str);
    }

    EncodingPreference encoding_preference;
    encoding_preference.binary_plain_encoding_default_impl =
            BinaryPlainEncodingTypePB::BINARY_PLAIN_ENCODING_V1;
    LOG(INFO) << "Testing large data with V1 preference, entry count: " << slices.size();
    test_with_large_data_size(slices, encoding_preference);
}

TEST_F(BinaryDictPageTest, TestLargeDataWithConfigTrue) {
    // Generate large amount of data with some repetition to test dictionary efficiency
    std::vector<std::string> src_strings;
    // Generate 1000 unique strings
    auto unique_strings = generate_test_data(1000, "data_", 10, 50);
    // Repeat them 100 times to create 100k entries
    for (int i = 0; i < 100; ++i) {
        for (const auto& str : unique_strings) {
            src_strings.push_back(str);
        }
    }

    std::vector<Slice> slices;
    for (const auto& str : src_strings) {
        slices.push_back(str);
    }

    EncodingPreference encoding_preference;
    encoding_preference.binary_plain_encoding_default_impl =
            BinaryPlainEncodingTypePB::BINARY_PLAIN_ENCODING_V2;
    LOG(INFO) << "Testing large data with V2 preference, entry count: " << slices.size();
    test_with_large_data_size(slices, encoding_preference);
}

} // namespace segment_v2
} // namespace doris
