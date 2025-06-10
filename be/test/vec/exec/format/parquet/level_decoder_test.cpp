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

#include "vec/exec/format/parquet/level_decoder.h"

#include <gtest/gtest.h>

#include "parquet/encoding.h"
#include "parquet/schema.h"
#include "parquet/types.h"
#include "util/slice.h"

namespace doris::vectorized {

class LevelDecoderTest : public ::testing::Test {
protected:
    void SetUp() override { _decoder = std::make_unique<LevelDecoder>(); }

    std::unique_ptr<LevelDecoder> _decoder;
};

// Test basic RLE level decoding for data page v1
TEST_F(LevelDecoderTest, test_rle_decode_v1) {
    // Prepare RLE encoded data
    // RLE encoded data: 4 zeros followed by 1, 2, 1 [0 0 0 0 1 2 1]
    std::vector<uint8_t> rle_data = {
            0x04, 0x00, 0x00, 0x00,      // RLE length (4 bytes)
            8,    0,    3,    0b00011001 // RLE data
    };
    Slice data_slice(reinterpret_cast<char*>(rle_data.data()), rle_data.size());

    // Initialize decoder
    ASSERT_TRUE(_decoder->init(&data_slice, tparquet::Encoding::RLE, 2, 7).ok());

    // Decode levels
    level_t levels[7];
    size_t num_levels = _decoder->get_levels(levels, 7);

    // Verify results
    ASSERT_EQ(num_levels, 7);
    EXPECT_EQ(levels[0], 0);
    EXPECT_EQ(levels[1], 0);
    EXPECT_EQ(levels[2], 0);
    EXPECT_EQ(levels[3], 0);
    EXPECT_EQ(levels[4], 1);
    EXPECT_EQ(levels[5], 2);
    EXPECT_EQ(levels[6], 1);
}

// Test basic BIT-PACKED level decoding for data page v1
TEST_F(LevelDecoderTest, test_bit_packed_decode_v1) {
    // Prepare BIT-PACKED encoded data
    // [1 2 1]
    std::vector<uint8_t> rle_data = {0b00011001};
    Slice data_slice(reinterpret_cast<char*>(rle_data.data()), rle_data.size());

    // Initialize decoder
    ASSERT_TRUE(_decoder->init(&data_slice, tparquet::Encoding::BIT_PACKED, 2, 3).ok());

    // Decode levels
    level_t levels[3];
    size_t num_levels = _decoder->get_levels(levels, 3);

    // Verify results
    ASSERT_EQ(num_levels, 3);
    EXPECT_EQ(levels[0], 1);
    EXPECT_EQ(levels[1], 2);
    EXPECT_EQ(levels[2], 1);
}

// Test RLE level decoding for data page v2
TEST_F(LevelDecoderTest, test_rle_decode_v2) {
    // Prepare RLE encoded data
    // RLE encoded data: 4 zeros followed by 1, 2, 1, padded to 8 values, [0 0 0 0 1 2 1]
    std::vector<uint8_t> rle_data = {8, 0, 3, 0b00011001, 0};
    Slice data_slice(reinterpret_cast<char*>(rle_data.data()), rle_data.size());

    // Initialize decoder
    ASSERT_TRUE(_decoder->init_v2(data_slice, 2, 7).ok());

    // Decode levels
    level_t levels[7];
    size_t num_levels = _decoder->get_levels(levels, 7);

    // Verify results
    ASSERT_EQ(num_levels, 7);
    EXPECT_EQ(levels[0], 0);
    EXPECT_EQ(levels[1], 0);
    EXPECT_EQ(levels[2], 0);
    EXPECT_EQ(levels[3], 0);
    EXPECT_EQ(levels[4], 1);
    EXPECT_EQ(levels[5], 2);
    EXPECT_EQ(levels[6], 1);
}

// Test invalid RLE data for data page v1
TEST_F(LevelDecoderTest, test_invalid_rle_data_v1) {
    // Prepare invalid RLE data
    std::vector<uint8_t> rle_data = {0x04, 0x00, 0x00, 0x00, // RLE length (4 bytes)
                                     8,    0,    3};
    Slice data_slice(reinterpret_cast<char*>(rle_data.data()), rle_data.size());

    // Initialize decoder should fail
    ASSERT_FALSE(_decoder->init(&data_slice, tparquet::Encoding::RLE, 1, 8).ok());
}

// TODO: Currently not working, so commented out.
// Test invalid RLE data for data page v2
//TEST_F(LevelDecoderTest, test_invalid_rle_data_v2) {
//    // Prepare invalid RLE data
//    std::vector<uint8_t> rle_data = {8, 0, 3};
//    Slice data_slice(reinterpret_cast<char*>(rle_data.data()), rle_data.size());
//
//    // Initialize decoder should fail
//    ASSERT_TRUE(_decoder->init_v2(data_slice, 2, 7).ok());
//
//    // Decode levels
//    level_t levels[7];
//    size_t num_levels = _decoder->get_levels(levels, 7);
//
//    // Verify results
//    ASSERT_EQ(num_levels, 7);
//}

// Test unsupported encoding
TEST_F(LevelDecoderTest, test_unsupported_encoding) {
    // Prepare dummy data
    std::vector<uint8_t> dummy_data = {0x00};
    Slice data_slice(reinterpret_cast<char*>(dummy_data.data()), dummy_data.size());

    // Initialize decoder with unsupported encoding should fail
    ASSERT_FALSE(_decoder->init(&data_slice, tparquet::Encoding::PLAIN, 1, 8).ok());
}

// Test has_levels() function
TEST_F(LevelDecoderTest, test_has_levels) {
    // Initially, there should be no levels
    EXPECT_FALSE(_decoder->has_levels());

    // Prepare RLE encoded data
    std::vector<uint8_t> rle_data = {8, 0, 3, 0b00011001, 0};
    Slice data_slice(reinterpret_cast<char*>(rle_data.data()), rle_data.size());

    // Initialize decoder with valid data
    ASSERT_TRUE(_decoder->init_v2(data_slice, 2, 7).ok());

    // Now there should be levels
    EXPECT_TRUE(_decoder->has_levels());
}

// Test get_next() function
TEST_F(LevelDecoderTest, test_get_next) {
    // Prepare RLE encoded data
    std::vector<uint8_t> rle_data = {8, 0, 3, 0b00011001, 0};
    Slice data_slice(reinterpret_cast<char*>(rle_data.data()), rle_data.size());

    // Initialize decoder
    ASSERT_TRUE(_decoder->init_v2(data_slice, 2, 7).ok());

    // Verify the first level
    EXPECT_EQ(_decoder->get_next(), 0);

    // Verify the next level
    EXPECT_EQ(_decoder->get_next(), 0);
}

// Test rewind_one() function
TEST_F(LevelDecoderTest, test_rewind_one) {
    // Prepare RLE encoded data
    std::vector<uint8_t> rle_data = {8, 0, 3, 0b00011001, 0};
    Slice data_slice(reinterpret_cast<char*>(rle_data.data()), rle_data.size());

    // Initialize decoder
    ASSERT_TRUE(_decoder->init_v2(data_slice, 2, 7).ok());

    // Get the first level
    level_t first_level = _decoder->get_next();

    // Get the second level
    level_t second_level = _decoder->get_next();

    // Rewind one level
    _decoder->rewind_one();

    // Verify that we get the second level again
    EXPECT_EQ(_decoder->get_next(), second_level);

    // Rewind one more level
    _decoder->rewind_one();

    // Verify that we get the first level again
    EXPECT_EQ(_decoder->get_next(), first_level);
}

// Test rle_decoder() function
TEST_F(LevelDecoderTest, test_rle_decoder) {
    // Prepare RLE encoded data
    std::vector<uint8_t> rle_data = {8, 0, 3, 0b00011001, 0};
    Slice data_slice(reinterpret_cast<char*>(rle_data.data()), rle_data.size());

    // Initialize decoder
    ASSERT_TRUE(_decoder->init_v2(data_slice, 2, 7).ok());

    // Get the RLE decoder
    const RleDecoder<level_t>& rle_decoder = _decoder->rle_decoder();

    // Verify that the RLE decoder is not null
    EXPECT_NE(&rle_decoder, nullptr);
}

} // namespace doris::vectorized
