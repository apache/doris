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
#include <cstdint>

#include "core/string_ref.h"

namespace doris {

// A validated, stack-only plan for one physical Variant scalar. The plan either owns a small
// inline textual fallback or borrows a StringRef until write() returns; it never allocates.
// Callers can therefore preflight a complete batch, reserve its final buffer once, and write
// directly into that buffer without constructing a VariantBuilder per row.
class VariantScalarEncodingPlan {
public:
    static VariantScalarEncodingPlan null_value() noexcept;
    static VariantScalarEncodingPlan boolean(bool value) noexcept;
    // width == 0 selects the narrowest signed integer encoding. Otherwise width must be one of
    // 1, 2, 4, or 8 and value must fit that signed width.
    static VariantScalarEncodingPlan integer(int64_t value, uint8_t width = 0);
    static VariantScalarEncodingPlan float32(float value) noexcept;
    static VariantScalarEncodingPlan float64(double value) noexcept;
    // width == 0 selects the narrowest decimal width. Otherwise width must be 4, 8, or 16.
    static VariantScalarEncodingPlan decimal(__int128 unscaled, uint8_t scale, uint8_t width = 0);
    static VariantScalarEncodingPlan date(int32_t days_since_epoch) noexcept;
    static VariantScalarEncodingPlan timestamp_micros(int64_t value, bool utc_adjusted) noexcept;
    static VariantScalarEncodingPlan timestamp_nanos(int64_t value, bool utc_adjusted) noexcept;
    static VariantScalarEncodingPlan time_ntz_micros(int64_t value) noexcept;
    static VariantScalarEncodingPlan binary(StringRef value);
    static VariantScalarEncodingPlan string(StringRef value);
    static VariantScalarEncodingPlan uuid(const std::array<uint8_t, 16>& value) noexcept;
    // Values outside decimal38 are encoded as their canonical decimal string representation.
    static VariantScalarEncodingPlan largeint(__int128 value) noexcept;

    size_t size() const noexcept { return _encoded_size; }
    bool used_string_fallback() const noexcept { return _used_string_fallback; }

    // Writes exactly size() bytes. Invalid destination/capacity fails before modifying memory.
    void write(char* destination, size_t capacity) const;

private:
    VariantScalarEncodingPlan() = default;

    enum class PayloadKind : uint8_t {
        HEADER_ONLY,
        UNSIGNED,
        SIGNED,
        BORROWED_BYTES,
        UUID,
        INLINE_STRING,
    };

    uint64_t _unsigned_value = 0;
    __int128 _signed_value = 0;
    StringRef _borrowed;
    std::array<uint8_t, 16> _uuid {};
    std::array<char, 40> _inline_string {};
    size_t _encoded_size = 0;
    uint8_t _header = 0;
    uint8_t _payload_width = 0;
    uint8_t _scale = 0;
    uint8_t _inline_size = 0;
    PayloadKind _kind = PayloadKind::HEADER_ONLY;
    bool _has_scale = false;
    bool _has_length = false;
    bool _used_string_fallback = false;
};

} // namespace doris
