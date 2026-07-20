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
#include <memory>
#include <string>

#include "core/value/variant/variant_value.h"

namespace doris {

class SipHash;
class CanonicalScalarSerializationPlan;
class VariantCanonicalScalarRef;
struct VariantCanonicalScalarAdapter;

bool canonical_equals(VariantCanonicalScalarRef left, VariantCanonicalScalarRef right);

template <typename Sink>
void canonical_hash(VariantCanonicalScalarRef value, Sink& sink);

// Stack-only logical scalar view used by typed column adapters. String and binary values are
// borrowed until the synchronous hash/serialization call returns; no column or DataType
// dependency crosses into the codec.
class VariantCanonicalScalarRef {
public:
    enum class Kind : uint8_t {
        NULL_VALUE,
        BOOL,
        EXACT_INTEGER,
        DECIMAL,
        FLOATING,
        STRING,
        BINARY,
        DATE,
        TIMESTAMP_TZ,
        TIMESTAMP_NTZ,
        TIME,
        UUID,
    };

    static VariantCanonicalScalarRef null_value() noexcept;
    static VariantCanonicalScalarRef boolean(bool value) noexcept;
    static VariantCanonicalScalarRef exact_integer(__int128 value);
    static VariantCanonicalScalarRef decimal(__int128 unscaled, uint8_t scale);
    static VariantCanonicalScalarRef float32(float value) noexcept;
    static VariantCanonicalScalarRef float64(double value) noexcept;
    static VariantCanonicalScalarRef string(StringRef value);
    static VariantCanonicalScalarRef binary(StringRef value);
    static VariantCanonicalScalarRef date(int32_t days_since_epoch) noexcept;
    static VariantCanonicalScalarRef timestamp_micros(int64_t value, bool utc_adjusted) noexcept;
    static VariantCanonicalScalarRef timestamp_nanos(int64_t value, bool utc_adjusted) noexcept;
    static VariantCanonicalScalarRef time_ntz_micros(int64_t value) noexcept;
    static VariantCanonicalScalarRef uuid(const std::array<uint8_t, 16>& value) noexcept;

private:
    explicit VariantCanonicalScalarRef(Kind kind) noexcept : _kind(kind) {}

    __int128 _integer = 0;
    double _floating = 0;
    StringRef _bytes;
    std::array<uint8_t, 16> _uuid {};
    Kind _kind;
    uint8_t _scale = 0;
    bool _boolean = false;

    friend class CanonicalScalarSerializationPlan;
    friend bool canonical_equals(VariantCanonicalScalarRef left, VariantCanonicalScalarRef right);
    template <typename Sink>
    friend void canonical_hash(VariantCanonicalScalarRef value, Sink& sink);
    friend CanonicalScalarSerializationPlan prepare_canonical_serialize(
            VariantCanonicalScalarRef value);
    friend struct VariantCanonicalScalarAdapter;
};

// Incremental adapters used by canonical_hash(). Each update consumes one canonical token. The
// token traversal and token boundaries are shared by all sinks; the adapters only select the hash
// family and carry its seed/state.
class VariantXxHashSink {
public:
    explicit VariantXxHashSink(uint64_t seed = 0) noexcept : _state(seed) {}

    void update(const char* data, size_t size);
    uint64_t digest() const noexcept { return _state; }

private:
    uint64_t _state;
};

class VariantCrc32HashSink {
public:
    explicit VariantCrc32HashSink(uint32_t seed = 0) noexcept : _state(seed) {}

    void update(const char* data, size_t size);
    uint32_t digest() const noexcept { return _state; }

private:
    uint32_t _state;
};

class VariantCrc32cHashSink {
public:
    explicit VariantCrc32cHashSink(uint32_t seed = 0) noexcept : _state(seed) {}

    void update(const char* data, size_t size);
    uint32_t digest() const noexcept { return _state; }

private:
    uint32_t _state;
};

// Compares logical Variant values. Numeric normalization is deliberately limited to the exact
// integer domain that canonical Variant decimal16 can encode: [-(10^38-1), +(10^38-1)]. A finite
// integral float/double outside that domain remains floating so canonical arena bytes stay valid.
bool canonical_equals(VariantRef left, VariantRef right);

// Hashes canonical logical tokens without re-encoding the value. Supported production sinks are
// SipHash, VariantXxHashSink, VariantCrc32HashSink, and VariantCrc32cHashSink; all four are explicit
// instantiations of the same traversal skeleton.
template <typename Sink>
void canonical_hash(VariantRef value, Sink& sink);

extern template void canonical_hash<SipHash>(VariantRef value, SipHash& sink);
extern template void canonical_hash<VariantXxHashSink>(VariantRef value, VariantXxHashSink& sink);
extern template void canonical_hash<VariantCrc32HashSink>(VariantRef value,
                                                          VariantCrc32HashSink& sink);
extern template void canonical_hash<VariantCrc32cHashSink>(VariantRef value,
                                                           VariantCrc32cHashSink& sink);

extern template void canonical_hash<SipHash>(VariantCanonicalScalarRef value, SipHash& sink);
extern template void canonical_hash<VariantXxHashSink>(VariantCanonicalScalarRef value,
                                                       VariantXxHashSink& sink);
extern template void canonical_hash<VariantCrc32HashSink>(VariantCanonicalScalarRef value,
                                                          VariantCrc32HashSink& sink);
extern template void canonical_hash<VariantCrc32cHashSink>(VariantCanonicalScalarRef value,
                                                           VariantCrc32cHashSink& sink);

// No-heap canonical arena plan for one scalar. The output layout is identical to
// CanonicalSerializationPlan: [u32 payload_size][minimal metadata][canonical scalar].
class CanonicalScalarSerializationPlan {
public:
    size_t size() const noexcept { return _cell_size; }
    void write(char* destination, size_t capacity) const;

private:
    CanonicalScalarSerializationPlan(VariantCanonicalScalarRef value, size_t cell_size) noexcept
            : _value(value), _cell_size(cell_size) {}

    VariantCanonicalScalarRef _value;
    size_t _cell_size;

    friend CanonicalScalarSerializationPlan prepare_canonical_serialize(
            VariantCanonicalScalarRef value);
};

CanonicalScalarSerializationPlan prepare_canonical_serialize(VariantCanonicalScalarRef value);

// Owns a validated canonical serialization plan while borrowing the source Variant bytes. The
// source metadata/value buffers must remain stable until write() returns. This lets column adapters
// allocate their final contiguous storage exactly once without introducing an Arena dependency in
// the codec.
class CanonicalSerializationPlan {
public:
    CanonicalSerializationPlan(CanonicalSerializationPlan&&) noexcept;
    CanonicalSerializationPlan& operator=(CanonicalSerializationPlan&&) noexcept;
    ~CanonicalSerializationPlan();

    CanonicalSerializationPlan(const CanonicalSerializationPlan&) = delete;
    CanonicalSerializationPlan& operator=(const CanonicalSerializationPlan&) = delete;

    // Returns zero only for a moved-from plan.
    size_t size() const noexcept;

    // Writes exactly size() bytes. A null destination, moved-from plan, or insufficient capacity
    // fails before modifying caller-owned memory.
    void write(char* destination, size_t capacity) const;

private:
    struct Impl;

    explicit CanonicalSerializationPlan(std::unique_ptr<Impl> impl) noexcept;

    std::unique_ptr<Impl> _impl;

    friend CanonicalSerializationPlan prepare_canonical_serialize(VariantRef value);
};

// Validates and plans one canonical arena cell without allocating its final output bytes.
CanonicalSerializationPlan prepare_canonical_serialize(VariantRef value);

// Appends one canonical arena cell and returns the number of appended bytes. The layout is
// [u32 payload_size][canonical_metadata][canonical_value], where payload_size excludes its own
// four-byte prefix. On validation or size failure, destination is unchanged. This convenience
// overload delegates to CanonicalSerializationPlan.
size_t canonical_serialize(VariantRef value, std::string& destination);

// Splits one bounded canonical arena cell without allocating or canonicalizing it. The returned
// view borrows serialized and remains valid only while the input bytes remain stable. This checks
// the exact payload prefix, derives the self-delimiting metadata size with bounded arithmetic, and
// requires the remainder to contain exactly one complete Variant value.
VariantRef parse_canonical_serialized(StringRef serialized);

} // namespace doris
