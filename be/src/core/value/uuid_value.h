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

    static UUIDValueType from_parts(uint64_t high, uint64_t low) {
        return (static_cast<UUIDValueType>(high) << 64) | low;
    }

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

        uint8_t combined_digits = 0;
        uint64_t high;
        uint64_t low;
        if (size == TEXT_LENGTH) {
            if (data[8] != '-' || data[13] != '-' || data[18] != '-' || data[23] != '-') {
                return false;
            }
            high = parse_hex_digits<8>(data, combined_digits);
            high = (high << 16) | parse_hex_digits<4>(data + 9, combined_digits);
            high = (high << 16) | parse_hex_digits<4>(data + 14, combined_digits);
            low = parse_hex_digits<4>(data + 19, combined_digits);
            low = (low << 48) | parse_hex_digits<12>(data + 24, combined_digits);
        } else {
            high = parse_hex_digits<16>(data, combined_digits);
            low = parse_hex_digits<16>(data + 16, combined_digits);
        }

        if (combined_digits > 0x0f) {
            return false;
        }
        value = from_parts(high, low);
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

    static constexpr auto HEX_TO_NIBBLE = [] {
        std::array<uint8_t, 256> digits {};
        digits.fill(0xff);
        for (size_t digit = 0; digit < 10; ++digit) {
            digits['0' + digit] = static_cast<uint8_t>(digit);
        }
        for (size_t digit = 0; digit < 6; ++digit) {
            digits['a' + digit] = static_cast<uint8_t>(digit + 10);
            digits['A' + digit] = static_cast<uint8_t>(digit + 10);
        }
        return digits;
    }();

    template <size_t Digits>
    static uint64_t parse_hex_digits(const char* data, uint8_t& combined_digits) {
        uint64_t parsed = 0;
        for (size_t offset = 0; offset < Digits; ++offset) {
            const uint8_t digit = HEX_TO_NIBBLE[static_cast<unsigned char>(data[offset])];
            combined_digits |= digit;
            parsed = (parsed << 4) | digit;
        }
        return parsed;
    }

    UUIDValueType _value = 0;
};

} // namespace doris
