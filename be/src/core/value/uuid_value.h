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

#include <array>
#include <cstddef>
#include <string>

#include "core/types.h"

namespace doris {

class UUIDValue {
public:
    static constexpr size_t BINARY_LENGTH = 16;
    static constexpr size_t TEXT_LENGTH = 36;
    static constexpr size_t TEXT_LENGTH_WITHOUT_DASHES = 32;

    // Generate independent values for each row, sharing the SQL function's V7 sequence.
    static void generate(UUIDValueType* values, size_t count, bool version7);

    static UUIDValueType from_big_endian(const uint8_t* bytes) {
        UUIDValueType value = 0;
        for (size_t i = 0; i < BINARY_LENGTH; ++i) {
            value = (value << 8) | bytes[i];
        }
        return value;
    }

    static std::array<uint8_t, BINARY_LENGTH> to_big_endian(UUIDValueType value) {
        std::array<uint8_t, BINARY_LENGTH> bytes {};
        for (size_t i = bytes.size(); i > 0; --i) {
            bytes[i - 1] = static_cast<uint8_t>(value);
            value >>= 8;
        }
        return bytes;
    }

    UUIDValue() = default;
    explicit UUIDValue(UUIDValueType value) : _value(value) {}

    const UUIDValueType& value() const { return _value; }
    UUIDValueType& value() { return _value; }

    bool from_string(const std::string& str) { return from_string(_value, str.data(), str.size()); }

    static bool from_string(UUIDValueType& value, const std::string& str) {
        return from_string(value, str.data(), str.size());
    }

    static bool from_string(UUIDValueType& value, const char* data, size_t size) {
        if (size != TEXT_LENGTH && size != TEXT_LENGTH_WITHOUT_DASHES) {
            return false;
        }

        UUIDValueType parsed = 0;
        size_t hex_digits = 0;
        for (size_t i = 0; i < size; ++i) {
            if (size == TEXT_LENGTH && is_dash_position(i)) {
                if (data[i] != '-') {
                    return false;
                }
                continue;
            }

            const int digit = hex_digit(data[i]);
            if (digit < 0) {
                return false;
            }
            parsed = (parsed << 4) | static_cast<UUIDValueType>(digit);
            ++hex_digits;
        }

        if (hex_digits != TEXT_LENGTH_WITHOUT_DASHES) {
            return false;
        }
        value = parsed;
        return true;
    }

    std::string to_string() const { return to_string(_value); }

    static std::string to_string(UUIDValueType value) {
        static constexpr char HEX_DIGITS[] = "0123456789abcdef";
        std::string result(TEXT_LENGTH, '-');
        for (size_t i = TEXT_LENGTH; i > 0; --i) {
            const size_t position = i - 1;
            if (is_dash_position(position)) {
                continue;
            }
            result[position] = HEX_DIGITS[static_cast<unsigned>(value & 0x0f)];
            value >>= 4;
        }
        return result;
    }

    static bool is_valid_string(const char* data, size_t size) {
        UUIDValueType value;
        return from_string(value, data, size);
    }

    static uint8_t version(UUIDValueType value) {
        return static_cast<uint8_t>((value >> 76) & 0x0f);
    }

private:
    static constexpr bool is_dash_position(size_t position) {
        return position == 8 || position == 13 || position == 18 || position == 23;
    }

    static constexpr int hex_digit(char value) {
        if (value >= '0' && value <= '9') {
            return value - '0';
        }
        if (value >= 'a' && value <= 'f') {
            return value - 'a' + 10;
        }
        if (value >= 'A' && value <= 'F') {
            return value - 'A' + 10;
        }
        return -1;
    }

    UUIDValueType _value = 0;
};

} // namespace doris
