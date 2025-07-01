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

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>

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
    constexpr Versionstamp()
            : Versionstamp({0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}) {}
    constexpr Versionstamp(const std::array<uint8_t, 10>& data) : data_(data) {}
    constexpr Versionstamp(const uint8_t data[10]) { std::copy(data, data + 10, data_.begin()); }
    constexpr Versionstamp(uint64_t version, uint16_t order) {
        if constexpr (std::endian::native == std::endian::little) {
            // If the native endianness is little-endian, we need to convert to big-endian
            version = bswap_64(version);
            order = bswap_16(order);
        }

        // Copy the big-endian version and order into the data array
        for (size_t i = 0; i < 8; ++i) {
            data_[i] = (version >> ((7 - i) * 8)) & 0xff;
        }
        for (size_t i = 0; i < 2; ++i) {
            data_[8 + i] = (order >> ((1 - i) * 8)) & 0xff;
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
        uint64_t version = 0;
        for (size_t i = 0; i < 8; ++i) {
            version |= static_cast<uint64_t>(data_[i]) << ((7 - i) * 8);
        }
        if constexpr (std::endian::native == std::endian::little) {
            // If the native endianness is little-endian, we need to convert to big-endian
            version = bswap_64(version);
        }
        return version;
    }

    constexpr uint16_t order() const {
        // The last 2 bytes represent the order in big-endian order
        uint16_t order = 0;
        for (size_t i = 0; i < 2; ++i) {
            order |= static_cast<uint16_t>(data_[8 + i]) << ((1 - i) * 8);
        }
        if constexpr (std::endian::native == std::endian::little) {
            order = bswap_16(order);
        }
        return order;
    }

private:
    std::array<uint8_t, 10> data_;
};

} // namespace doris::cloud
