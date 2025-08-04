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

#include <byteswap.h>
#include <fmt/format.h>

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <string>

namespace doris::cloud {

// Versionstamp is a class that represents a versionstamp in the meta store.
//
// A versionstamp is a 10 byte, unique, monotonically (but not sequentially)
// increasing value for each committed transaction. The first 8 bytes are the
// committed version of the database (serialized in big-endian order). The last
// 2 bytes are monotonic in the serialization order for transactions (serialized
// in big-endian order).
class Versionstamp {
public:
    constexpr static uint64_t byteswap64(uint64_t x) {
        return ((x & 0xFF00000000000000ULL) >> 56) | ((x & 0x00FF000000000000ULL) >> 40) |
               ((x & 0x0000FF0000000000ULL) >> 24) | ((x & 0x000000FF00000000ULL) >> 8) |
               ((x & 0x00000000FF000000ULL) << 8) | ((x & 0x0000000000FF0000ULL) << 24) |
               ((x & 0x000000000000FF00ULL) << 40) | ((x & 0x00000000000000FFULL) << 56);
    }

    constexpr static uint16_t byteswap16(uint16_t x) { return (x << 8) | (x >> 8); }

    constexpr Versionstamp()
            : Versionstamp({0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}) {}
    constexpr Versionstamp(const std::array<uint8_t, 10>& data) : data_(data) {}
    constexpr Versionstamp(const uint8_t data[10]) { std::copy(data, data + 10, data_.begin()); }
    constexpr Versionstamp(uint64_t version, uint16_t order) {
        if constexpr (std::endian::native == std::endian::little) {
            // If the native endianness is little-endian, we need to convert to big-endian
            version = byteswap64(version);
            order = byteswap16(order);
        }

        for (size_t i = 0; i < 8; ++i) {
            data_[i] = static_cast<uint8_t>(version >> (i * 8));
        }
        for (size_t i = 0; i < 2; ++i) {
            data_[8 + i] = static_cast<uint8_t>(order >> (i * 8));
        }
    }
    constexpr Versionstamp(uint64_t version) : Versionstamp(version, 0) {}
    constexpr Versionstamp(const Versionstamp&) = default;
    constexpr Versionstamp& operator=(const Versionstamp&) = default;
    constexpr Versionstamp& operator=(const std::array<uint8_t, 10>& data) {
        data_ = data;
        return *this;
    }
    constexpr Versionstamp& operator=(const uint8_t data[10]) {
        std::copy(data, data + 10, data_.begin());
        return *this;
    }

    consteval static Versionstamp min() {
        return {{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}};
    }

    consteval static Versionstamp max() {
        return {{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}};
    }

    constexpr std::strong_ordering operator<=>(const Versionstamp& other) const {
        return data_ <=> other.data_;
    }

    constexpr bool operator==(const Versionstamp& other) const { return data_ == other.data_; }

    constexpr const std::array<uint8_t, 10>& data() const { return data_; }

    constexpr uint64_t version() const {
        // The first 8 bytes represent the version in big-endian order
        uint8_t data[8];
        std::copy(data_.begin(), data_.begin() + 8, data);
        uint64_t version = std::bit_cast<uint64_t>(data);
        if constexpr (std::endian::native == std::endian::little) {
            // If the native endianness is little-endian, we need to convert to big-endian
            version = byteswap64(version);
        }
        return version;
    }

    constexpr uint16_t order() const {
        // The last 2 bytes represent the order in big-endian order
        uint8_t data[2];
        std::copy(data_.begin() + 8, data_.end(), data);
        uint16_t order = std::bit_cast<uint16_t>(data);
        if constexpr (std::endian::native == std::endian::little) {
            order = byteswap16(order);
        }
        return order;
    }

    std::string to_string() const {
        std::string result;
        result.reserve(21); // 10 bytes * 2 hex digits + 1 for null terminator
        for (const auto& byte : data_) {
            result += fmt::format("{:02x}", byte);
        }
        return result;
    }

private:
    std::array<uint8_t, 10> data_;
};

} // namespace doris::cloud
