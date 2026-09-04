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
#include "core/value/variant/variant_parquet_encoding.h"

namespace doris {

struct VariantScalarAdapter;

// Stack-only scalar view shared by physical encoding and canonical operations. String and binary
// values are borrowed until the synchronous call returns. The primitive id retains physical
// distinctions such as integer/decimal width, float32 versus float64, and timestamp unit.
class VariantScalarRef {
public:
    static VariantScalarRef null_value() noexcept;
    static VariantScalarRef boolean(bool value) noexcept;
    // width == 0 selects the narrowest signed integer encoding. Otherwise width must be one of
    // 1, 2, 4, or 8 and value must fit that signed width.
    static VariantScalarRef integer(int64_t value, uint8_t width = 0);
    // width == 0 selects the narrowest decimal width. Otherwise width must be 4, 8, or 16.
    static VariantScalarRef decimal(__int128 unscaled, uint8_t scale, uint8_t width = 0);
    static VariantScalarRef float32(float value) noexcept;
    static VariantScalarRef float64(double value) noexcept;
    static VariantScalarRef string(StringRef value);
    static VariantScalarRef binary(StringRef value);
    static VariantScalarRef date(int32_t days_since_epoch) noexcept;
    static VariantScalarRef timestamp_micros(int64_t value, bool utc_adjusted) noexcept;
    static VariantScalarRef timestamp_nanos(int64_t value, bool utc_adjusted) noexcept;
    static VariantScalarRef time_ntz_micros(int64_t value) noexcept;
    static VariantScalarRef uuid(const std::array<uint8_t, 16>& value) noexcept;

    size_t encoded_size() const noexcept;

    // Writes exactly encoded_size() bytes. Invalid destination/capacity fails before modifying
    // memory.
    void write_physical(char* destination, size_t capacity) const;

private:
    explicit VariantScalarRef(VariantPrimitiveId physical_id) noexcept
            : _physical_id(physical_id) {}

    __int128 _signed_value = 0;
    uint64_t _floating_bits = 0;
    StringRef _bytes;
    std::array<uint8_t, 16> _uuid {};
    VariantPrimitiveId _physical_id;
    uint8_t _scale = 0;

    friend struct VariantScalarAdapter;
};

} // namespace doris
