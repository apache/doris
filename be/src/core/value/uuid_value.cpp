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

#include "core/value/uuid_value.h"

#include <atomic>
#include <boost/uuid/random_generator.hpp>
#include <chrono>

namespace doris {
namespace {
UUIDValueType uuid_from_bytes(const boost::uuids::uuid& bytes) {
    return UUIDValue::from_big_endian(bytes.data);
}

UUIDValueType generate_uuid_v4(boost::uuids::random_generator& generator) {
    return uuid_from_bytes(generator());
}

UUIDValueType generate_uuid_v7(boost::uuids::random_generator& generator) {
    static std::atomic<uint64_t> last_timestamp_and_counter {0};
    constexpr uint64_t COUNTER_MASK = (uint64_t {1} << 12) - 1;
    const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
    const auto random = uuid_from_bytes(generator());
    const uint64_t initial_counter = static_cast<uint64_t>(random >> 64) & COUNTER_MASK;

    uint64_t previous = last_timestamp_and_counter.load(std::memory_order_relaxed);
    uint64_t next;
    do {
        const uint64_t previous_timestamp = previous >> 12;
        next = static_cast<uint64_t>(now) > previous_timestamp
                       ? (static_cast<uint64_t>(now) << 12) | initial_counter
                       : previous + 1;
    } while (!last_timestamp_and_counter.compare_exchange_weak(
            previous, next, std::memory_order_relaxed, std::memory_order_relaxed));

    const auto timestamp = static_cast<UUIDValueType>(next >> 12);
    const auto counter = static_cast<UUIDValueType>(next & COUNTER_MASK);
    const UUIDValueType random_tail = random & ((UUIDValueType {1} << 62) - 1);
    return (timestamp << 80) | (UUIDValueType {7} << 76) | (counter << 64) |
           (UUIDValueType {2} << 62) | random_tail;
}

} // namespace

// NOLINTNEXTLINE(readability-non-const-parameter): clang-tidy misses writes through __int128 pointers.
void UUIDValue::generate(UUIDValueType* values, size_t count, bool version7) {
    boost::uuids::random_generator generator;
    for (size_t i = 0; i < count; ++i) {
        values[i] = version7 ? generate_uuid_v7(generator) : generate_uuid_v4(generator);
    }
}

} // namespace doris
