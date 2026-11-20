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

#include <gtest/gtest.h>

#include <barrier>
#include <string>
#include <thread>
#include <vector>

#include "core/value/bitmap_value.h"

namespace doris {

static void check_serialized_bytes(const BitmapValue& source, const std::string& expected,
                                   const std::string& expected_compact, std::string& bytes) {
    bytes.resize(source.getSizeInBytes());
    ASSERT_EQ(source.write_to(bytes.data()), bytes.size());
    ASSERT_EQ(bytes, expected);
    source.serialize_optimized(bytes);
    ASSERT_EQ(bytes, expected_compact);
}

TEST(BitmapValueConcurrencyTest, ConcurrentCopySizeAndSerialization) {
    BitmapValue source;
    for (uint64_t i = 0; i < 4096; ++i) {
        source.add(i);
        source.add((uint64_t {1} << 40) + i);
    }
    ASSERT_EQ(source.cardinality(), 8192);
    ASSERT_TRUE(source.needs_optimization());
    std::string expected;
    source.serialize(expected);
    BitmapValue optimized(source);
    optimized.optimize();
    std::string expected_compact;
    optimized.serialize(expected_compact);
    std::barrier start(4);
    std::vector<std::jthread> threads;
    for (int thread = 0; thread < 4; ++thread) {
        threads.emplace_back([&] {
            std::string bytes;
            start.arrive_and_wait();
            for (int repeat = 0; repeat < 1000; ++repeat) {
                BitmapValue constructed(source);
                BitmapValue assigned;
                assigned = source;
                EXPECT_EQ(constructed.cardinality(), 8192);
                check_serialized_bytes(source, expected, expected_compact, bytes);
                EXPECT_EQ(assigned.cardinality(), 8192);
            }
        });
    }
    threads.clear();
    EXPECT_EQ(source.cardinality(), 8192);
    EXPECT_TRUE(source.needs_optimization());
}

} // namespace doris
