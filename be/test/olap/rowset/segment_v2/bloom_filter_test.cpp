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
#include "olap/rowset/segment_v2/bloom_filter.h"

#include <gtest/gtest.h>

#include <iostream>
#include <random>
#include <unordered_set>
#include <vector>

using namespace doris;
using namespace segment_v2;

// Test class for BloomFilter
class BloomFilterTest : public testing::Test {
public:
    void SetUp() override {
        // Any setup required before each test
    }
    void TearDown() override {
        // Any cleanup required after each test
    }
};

// Test the creation of BloomFilter instances
TEST_F(BloomFilterTest, TestCreateBloomFilter) {
    // Create a BlockSplitBloomFilter
    std::unique_ptr<BloomFilter> bf;
    Status st = BloomFilter::create(BLOCK_BLOOM_FILTER, &bf);
    EXPECT_TRUE(st.ok());
    EXPECT_NE(bf, nullptr);

    // Create an NGramBloomFilter with a specific size
    std::unique_ptr<BloomFilter> ngram_bf;
    size_t bf_size = 1024;
    st = BloomFilter::create(NGRAM_BLOOM_FILTER, &ngram_bf, bf_size);
    EXPECT_TRUE(st.ok());
    EXPECT_NE(ngram_bf, nullptr);
}

// Test the initialization of the BloomFilter with filter size
TEST_F(BloomFilterTest, TestInitBloomFilterWithFilterSize) {
    std::unique_ptr<BloomFilter> bf;
    Status st = BloomFilter::create(BLOCK_BLOOM_FILTER, &bf);
    EXPECT_TRUE(st.ok());

    // Initialize with a specific filter size
    uint64_t filter_size = 1024; // Filter size in bytes
    st = bf->init(filter_size);
    EXPECT_TRUE(st.ok());

    // Verify the internal size is set correctly
    EXPECT_EQ(bf->num_bytes(), static_cast<uint32_t>(filter_size));
}

// Test the initialization of the BloomFilter with expected items and FPP
TEST_F(BloomFilterTest, TestInitBloomFilter) {
    std::unique_ptr<BloomFilter> bf;
    Status st = BloomFilter::create(BLOCK_BLOOM_FILTER, &bf);
    EXPECT_TRUE(st.ok());

    // Initialize with expected number of items and FPP
    uint64_t n = 1000; // Expected number of items
    double fpp = 0.01; // Desired false positive probability
    st = bf->init(n, fpp, HASH_MURMUR3_X64_64);
    EXPECT_TRUE(st.ok());

    // Verify the internal size is set correctly
    uint32_t expected_num_bits = BloomFilter::optimal_bit_num(n, fpp);
    uint32_t expected_num_bytes = expected_num_bits / 8;
    EXPECT_EQ(bf->num_bytes(), expected_num_bytes);
}

// Test the num_bytes() function
TEST_F(BloomFilterTest, TestNumBytesFunction) {
    std::unique_ptr<BloomFilter> bf;
    Status st = BloomFilter::create(BLOCK_BLOOM_FILTER, &bf);
    EXPECT_TRUE(st.ok());

    // Initialize with a specific filter size
    uint64_t filter_size = 2048; // Filter size in bytes
    st = bf->init(filter_size);
    EXPECT_TRUE(st.ok());

    // Verify num_bytes() returns the correct value
    EXPECT_EQ(bf->num_bytes(), static_cast<uint32_t>(filter_size));
}

// Test the contains() function
TEST_F(BloomFilterTest, TestContainsFunction) {
    // Create and initialize two BloomFilters
    std::unique_ptr<BloomFilter> bf1;
    Status st = BloomFilter::create(BLOCK_BLOOM_FILTER, &bf1);
    EXPECT_TRUE(st.ok());
    uint64_t n = 1000;
    double fpp = 0.01;
    st = bf1->init(n, fpp, HASH_MURMUR3_X64_64);
    EXPECT_TRUE(st.ok());

    std::unique_ptr<BloomFilter> bf2;
    st = BloomFilter::create(BLOCK_BLOOM_FILTER, &bf2);
    EXPECT_TRUE(st.ok());
    st = bf2->init(n, fpp, HASH_MURMUR3_X64_64);
    EXPECT_TRUE(st.ok());

    // Add elements to bf1
    for (int i = 0; i < n; ++i) {
        std::string elem = "bf1_element_" + std::to_string(i);
        bf1->add_bytes(elem.c_str(), elem.size());
    }

    // Add elements to bf2
    for (int i = 0; i < n; ++i) {
        std::string elem = "bf2_element_" + std::to_string(i);
        bf2->add_bytes(elem.c_str(), elem.size());
    }

    // Test the contains() function
    // Since the base class implementation always returns true, we expect true
    bool result = bf1->contains(*bf2);
    EXPECT_TRUE(result);
}

// Test adding elements and checking them
TEST_F(BloomFilterTest, TestAddAndTestElements) {
    std::unique_ptr<BloomFilter> bf;
    Status st = BloomFilter::create(BLOCK_BLOOM_FILTER, &bf);
    EXPECT_TRUE(st.ok());

    uint64_t n = 1000;
    double fpp = 0.01;
    st = bf->init(n, fpp, HASH_MURMUR3_X64_64);
    EXPECT_TRUE(st.ok());

    // Add elements to the BloomFilter
    std::vector<std::string> inserted_elements;
    for (int i = 0; i < n; ++i) {
        std::string elem = "element_" + std::to_string(i);
        bf->add_bytes(elem.c_str(), elem.size());
        inserted_elements.push_back(elem);
    }

    // Test that inserted elements are found
    for (const auto& elem : inserted_elements) {
        bool exists = bf->test_bytes(elem.c_str(), elem.size());
        EXPECT_TRUE(exists);
    }

    // Test that non-inserted elements are not found (allowing for false positives)
    int false_positives = 0;
    int test_count = 10000;
    for (int i = 0; i < test_count; ++i) {
        std::string elem = "nonexistent_element_" + std::to_string(i);
        bool exists = bf->test_bytes(elem.c_str(), elem.size());
        if (exists) {
            false_positives++;
        }
    }
    double actual_fpp = static_cast<double>(false_positives) / test_count;
    std::cout << "Expected FPP: " << fpp << ", Actual FPP: " << actual_fpp << std::endl;
    // Allow some margin in FPP
    EXPECT_LE(actual_fpp, fpp * 1.2);
}

// Test the merge function
TEST_F(BloomFilterTest, TestMergeBloomFilters) {
    // Create and initialize two BloomFilters
    std::unique_ptr<BloomFilter> bf1;
    Status st = BloomFilter::create(BLOCK_BLOOM_FILTER, &bf1);
    EXPECT_TRUE(st.ok());
    uint64_t n = 1000;
    double fpp = 0.01;
    st = bf1->init(n, fpp, HASH_MURMUR3_X64_64);
    EXPECT_TRUE(st.ok());

    std::unique_ptr<BloomFilter> bf2;
    st = BloomFilter::create(BLOCK_BLOOM_FILTER, &bf2);
    EXPECT_TRUE(st.ok());
    st = bf2->init(n, fpp, HASH_MURMUR3_X64_64);
    EXPECT_TRUE(st.ok());

    // Add elements to bf1
    std::vector<std::string> elements_bf1;
    for (int i = 0; i < n; ++i) {
        std::string elem = "bf1_element_" + std::to_string(i);
        bf1->add_bytes(elem.c_str(), elem.size());
        elements_bf1.push_back(elem);
    }

    // Add elements to bf2
    std::vector<std::string> elements_bf2;
    for (int i = 0; i < n; ++i) {
        std::string elem = "bf2_element_" + std::to_string(i);
        bf2->add_bytes(elem.c_str(), elem.size());
        elements_bf2.push_back(elem);
    }

    // Merge bf2 into bf1
    st = bf1->merge(bf2.get());
    EXPECT_TRUE(st.ok());

    // Test that elements from bf1 are found
    for (const auto& elem : elements_bf1) {
        bool exists = bf1->test_bytes(elem.c_str(), elem.size());
        EXPECT_TRUE(exists);
    }

    // Test that elements from bf2 are found in merged bf1
    for (const auto& elem : elements_bf2) {
        bool exists = bf1->test_bytes(elem.c_str(), elem.size());
        EXPECT_TRUE(exists);
    }
}

// Test null value handling
TEST_F(BloomFilterTest, TestNullValueHandling) {
    std::unique_ptr<BloomFilter> bf;
    Status st = BloomFilter::create(BLOCK_BLOOM_FILTER, &bf);
    EXPECT_TRUE(st.ok());

    uint64_t n = 1000;
    double fpp = 0.01;
    st = bf->init(n, fpp, HASH_MURMUR3_X64_64);
    EXPECT_TRUE(st.ok());

    // Initially, has_null should be false
    EXPECT_FALSE(bf->has_null());

    // Add null value
    bf->add_bytes(nullptr, 0);

    // Now, has_null should be true
    EXPECT_TRUE(bf->has_null());

    // Test for null value
    bool exists = bf->test_bytes(nullptr, 0);
    EXPECT_TRUE(exists);
}

// Test the optimal_bit_num function
TEST_F(BloomFilterTest, TestOptimalBitNum) {
    uint64_t n = 1000;
    double fpp = 0.01;
    uint32_t num_bits = BloomFilter::optimal_bit_num(n, fpp);

    // Expected num_bits should be within a reasonable range
    EXPECT_GT(num_bits, 0u);
    EXPECT_LE(num_bits, BloomFilter::MAXIMUM_BYTES * 8);

    // Verify that num_bits is a power of 2
    EXPECT_EQ(num_bits & (num_bits - 1), 0u);
}

// Test init function with invalid inputs to cover exception branches
TEST_F(BloomFilterTest, TestInitWithInvalidInputs) {
    std::unique_ptr<BloomFilter> bf;
    Status st = BloomFilter::create(BLOCK_BLOOM_FILTER, &bf);
    EXPECT_TRUE(st.ok());

    // Test with invalid hash strategy
    const char* buffer = "test_buffer";
    uint32_t size = 1024;
    HashStrategyPB invalid_strategy = static_cast<HashStrategyPB>(-1); // Invalid strategy
    st = bf->init(buffer, size, invalid_strategy);
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(st.code(), TStatusCode::INVALID_ARGUMENT);

    // Test with size zero
    st = bf->init(buffer, 0, HASH_MURMUR3_X64_64);
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(st.code(), TStatusCode::INVALID_ARGUMENT);

    // Test with nullptr buffer
    st = bf->init(nullptr, size, HASH_MURMUR3_X64_64);
    EXPECT_EQ(st.code(), TStatusCode::INVALID_ARGUMENT);

    // Test with size less than minimum allowed
    uint32_t invalid_size = 1; // Less than minimum bytes
    st = bf->init(buffer, invalid_size, HASH_MURMUR3_X64_64);
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(st.code(), TStatusCode::INVALID_ARGUMENT);

    // Test with size not a power of two
    uint32_t non_power_of_two_size = 1000; // Not a power of two
    st = bf->init(buffer, non_power_of_two_size, HASH_MURMUR3_X64_64);
    EXPECT_EQ(st.code(), TStatusCode::INVALID_ARGUMENT);
}