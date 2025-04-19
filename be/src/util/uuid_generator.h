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

#pragma once

#include <atomic>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <random>

namespace doris {

// Format:
// We use UUID v7 (RFC 4122) for generating UUIDs.
// UUIDv7 was chosen for the following benefits:
// 1. Time-ordered - Contains a timestamp component that makes UUIDs sortable by generation time,
//    which is valuable for query tracking, debugging, and performance analysis
// 2. High performance - Efficient generation with minimal overhead
// 3. Global uniqueness - Combines timestamp with random data to ensure uniqueness across
//    distributed systems without coordination
// 4. Database friendly - The time-ordered nature makes it more efficient for database indexing
//    and storage compared to purely random UUIDs (like v4)
// 5. Future-proof - Follows the latest UUID standard with improvements over older versions

// 0                   1                   2                   3
// 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |                           unix_ts_ms                          |
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |          unix_ts_ms           |  ver  |       rand_a          |
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |var|                        rand_b                             |
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |                            rand_b                             |
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

class UUIDGenerator {
public:
    boost::uuids::uuid next_uuid() {
        std::lock_guard<std::mutex> lock(_uuid_gen_lock);

        auto now = std::chrono::system_clock::now();
        uint64_t millis =
                std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch())
                        .count();

        uint16_t counter = _counter.fetch_add(1, std::memory_order_relaxed) & 0xFF;

        // Generate random bits
        std::random_device rd;
        std::mt19937_64 gen(rd());
        std::uniform_int_distribution<uint64_t> dis;
        uint64_t random = dis(gen);

        boost::uuids::uuid uuid;

        uuid.data[0] = static_cast<uint8_t>((millis >> 40) & 0xFF);
        uuid.data[1] = static_cast<uint8_t>((millis >> 32) & 0xFF);
        uuid.data[2] = static_cast<uint8_t>((millis >> 24) & 0xFF);
        uuid.data[3] = static_cast<uint8_t>((millis >> 16) & 0xFF);
        uuid.data[4] = static_cast<uint8_t>((millis >> 8) & 0xFF);
        uuid.data[5] = static_cast<uint8_t>(millis & 0xFF);

        // Next 4 bits: version (7)
        // Next 12 bits: counter
        uuid.data[6] = static_cast<uint8_t>(0x70 | ((counter >> 8) & 0x0F));
        uuid.data[7] = static_cast<uint8_t>(counter & 0xFF);

        // Next 2 bits: variant (2)
        // Remaining 62 bits: random
        uuid.data[8] = static_cast<uint8_t>(0x80 | ((random >> 56) & 0x3F));
        uuid.data[9] = static_cast<uint8_t>((random >> 48) & 0xFF);
        uuid.data[10] = static_cast<uint8_t>((random >> 40) & 0xFF);
        uuid.data[11] = static_cast<uint8_t>((random >> 32) & 0xFF);
        uuid.data[12] = static_cast<uint8_t>((random >> 24) & 0xFF);
        uuid.data[13] = static_cast<uint8_t>((random >> 16) & 0xFF);
        uuid.data[14] = static_cast<uint8_t>((random >> 8) & 0xFF);
        uuid.data[15] = static_cast<uint8_t>(random & 0xFF);

        return uuid;
    }

    static UUIDGenerator* instance() {
        static UUIDGenerator generator;
        return &generator;
    }

private:
    std::mutex _uuid_gen_lock;
    std::atomic<uint16_t> _counter {0};
};

} // namespace doris