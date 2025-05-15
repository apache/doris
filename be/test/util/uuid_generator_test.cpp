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

#include "util/uuid_generator.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <boost/uuid/uuid_io.hpp>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

namespace doris {

// Extract the timestamp component from a UUIDv7
uint64_t extract_timestamp_from_uuid(const boost::uuids::uuid& uuid) {
    // The first 6 bytes (48 bits) of UUIDv7 contain the timestamp
    uint64_t timestamp = 0;
    timestamp |= static_cast<uint64_t>(uuid.data[0]) << 40;
    timestamp |= static_cast<uint64_t>(uuid.data[1]) << 32;
    timestamp |= static_cast<uint64_t>(uuid.data[2]) << 24;
    timestamp |= static_cast<uint64_t>(uuid.data[3]) << 16;
    timestamp |= static_cast<uint64_t>(uuid.data[4]) << 8;
    timestamp |= static_cast<uint64_t>(uuid.data[5]);
    return timestamp;
}

// Extract the random component from a UUIDv7
std::string extract_random_from_uuid(const boost::uuids::uuid& uuid) {
    // Return the last 8 bytes as a string representation
    // (bytes 8-15, skipping the first byte which contains the variant)
    std::stringstream ss;
    ss << std::hex;
    for (int i = 8; i < 16; i++) {
        ss << std::setw(2) << std::setfill('0') << static_cast<int>(uuid.data[i]);
    }
    return ss.str();
}

// Test fixture for UUID generator tests
class UUIDGeneratorTest : public testing::Test {
protected:
    UUIDGenerator* _generator;

    void SetUp() override { _generator = UUIDGenerator::instance(); }
};

// Test that UUIDs are unique
TEST_F(UUIDGeneratorTest, TestUniqueness) {
    const int NUM_UUIDS = 10000;
    std::unordered_set<std::string> uuid_strings;

    for (int i = 0; i < NUM_UUIDS; i++) {
        boost::uuids::uuid uuid = _generator->next_uuid();
        std::string uuid_str = boost::uuids::to_string(uuid);
        // Make sure we haven't seen this UUID before
        ASSERT_TRUE(uuid_strings.find(uuid_str) == uuid_strings.end())
                << "Generated duplicate UUID: " << uuid_str;
        uuid_strings.insert(uuid_str);
    }
}

// Test that UUIDs are monotonically increasing based on timestamp
TEST_F(UUIDGeneratorTest, TestMonotonicIncrease) {
    const int NUM_UUIDS = 100;
    boost::uuids::uuid prev_uuid = _generator->next_uuid();
    uint64_t prev_timestamp = extract_timestamp_from_uuid(prev_uuid);

    // Sleep a bit to ensure timestamp changes
    std::this_thread::sleep_for(std::chrono::milliseconds(5));

    for (int i = 0; i < NUM_UUIDS; i++) {
        boost::uuids::uuid uuid = _generator->next_uuid();
        uint64_t timestamp = extract_timestamp_from_uuid(uuid);

        // Timestamp should be >= the previous one (either equal or greater)
        ASSERT_GE(timestamp, prev_timestamp) << "UUID timestamp not monotonically increasing";

        prev_uuid = uuid;
        prev_timestamp = timestamp;
    }
}

// Test that the random component is different even when timestamps are the same
TEST_F(UUIDGeneratorTest, TestRandomComponent) {
    const int NUM_UUIDS = 1000;
    std::vector<std::string> random_parts;

    for (int i = 0; i < NUM_UUIDS; i++) {
        boost::uuids::uuid uuid = _generator->next_uuid();
        random_parts.push_back(extract_random_from_uuid(uuid));
    }

    // Check that all random parts are unique
    std::sort(random_parts.begin(), random_parts.end());
    auto it = std::adjacent_find(random_parts.begin(), random_parts.end());
    ASSERT_EQ(it, random_parts.end()) << "Random component is not unique between UUIDs";
}

// Test parallel generation of UUIDs to ensure they're still unique
TEST_F(UUIDGeneratorTest, TestParallelGeneration) {
    const int NUM_THREADS = 8;
    const int UUIDS_PER_THREAD = 1000;
    std::vector<std::thread> threads;
    std::vector<std::vector<std::string>> thread_uuids(NUM_THREADS);

    for (int t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back([t, &thread_uuids, this]() {
            for (int i = 0; i < UUIDS_PER_THREAD; i++) {
                boost::uuids::uuid uuid = _generator->next_uuid();
                thread_uuids[t].push_back(boost::uuids::to_string(uuid));
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Combine all UUIDs and check uniqueness
    std::unordered_set<std::string> all_uuids;
    for (const auto& thread_uuid_list : thread_uuids) {
        for (const auto& uuid_str : thread_uuid_list) {
            ASSERT_TRUE(all_uuids.find(uuid_str) == all_uuids.end())
                    << "Duplicate UUID found in parallel generation: " << uuid_str;
            all_uuids.insert(uuid_str);
        }
    }
}

// Test that timestamps extracted from UUIDs correlate with real time
TEST_F(UUIDGeneratorTest, TestTimestampCorrelation) {
    auto before = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();

    // Generate a UUID
    boost::uuids::uuid uuid = _generator->next_uuid();

    auto after = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();

    uint64_t uuid_timestamp = extract_timestamp_from_uuid(uuid);

    // UUID timestamp should be between 'before' and 'after'
    ASSERT_GE(uuid_timestamp, before);
    ASSERT_LE(uuid_timestamp, after);
}

} // namespace doris