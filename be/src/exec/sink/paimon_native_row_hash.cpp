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

#include "exec/sink/paimon_native_row_hash.h"

#include <algorithm>
#include <bit>
#include <cstring>
#include <limits>

namespace doris::paimon_native {
namespace {

constexpr uint32_t MURMUR_C1 = 0xcc9e2d51U;
constexpr uint32_t MURMUR_C2 = 0x1b873593U;
constexpr uint32_t MURMUR_SEED = 42U;

uint32_t rotate_left(uint32_t value, int distance) {
    return (value << distance) | (value >> (32 - distance));
}

uint32_t mix_k1(uint32_t value) {
    value *= MURMUR_C1;
    value = rotate_left(value, 15);
    value *= MURMUR_C2;
    return value;
}

uint32_t mix_h1(uint32_t hash, uint32_t value) {
    hash ^= value;
    hash = rotate_left(hash, 13);
    return hash * 5U + 0xe6546b64U;
}

uint32_t fmix(uint32_t hash, size_t length) {
    hash ^= static_cast<uint32_t>(length);
    hash ^= hash >> 16;
    hash *= 0x85ebca6bU;
    hash ^= hash >> 13;
    hash *= 0xc2b2ae35U;
    hash ^= hash >> 16;
    return hash;
}

template <typename T>
void put_native(std::vector<uint8_t>* bytes, size_t offset, T value) {
    std::memcpy(bytes->data() + offset, &value, sizeof(value));
}

size_t round_to_word(size_t size) {
    return (size + 7U) & ~size_t {7U};
}

} // namespace

BinaryRowEncoder::BinaryRowEncoder(size_t arity)
        : _arity(arity),
          _null_bits_size((arity + 63U + 8U) / 64U * 8U),
          _fixed_size(_null_bits_size + arity * 8U),
          _cursor(_fixed_size),
          _bytes(_fixed_size, 0) {}

void BinaryRowEncoder::reset() {
    _cursor = _fixed_size;
    _bytes.assign(_fixed_size, 0);
}

bool BinaryRowEncoder::_valid_position(size_t position) const {
    return position < _arity;
}

size_t BinaryRowEncoder::_field_offset(size_t position) const {
    return _null_bits_size + position * 8U;
}

void BinaryRowEncoder::_set_null_bit(size_t position) {
    const size_t bit_index = position + 8U;
    _bytes[bit_index >> 3U] |= static_cast<uint8_t>(1U << (bit_index & 7U));
}

void BinaryRowEncoder::_ensure_capacity(size_t size) {
    if (_bytes.size() < size) {
        _bytes.resize(size, 0);
    }
}

void BinaryRowEncoder::_set_offset_and_size(size_t position, uint32_t offset, uint32_t size) {
    uint64_t offset_and_size = (static_cast<uint64_t>(offset) << 32U) | size;
    put_native(&_bytes, _field_offset(position), offset_and_size);
}

bool BinaryRowEncoder::set_null(size_t position) {
    if (!_valid_position(position)) {
        return false;
    }
    _set_null_bit(position);
    std::fill_n(_bytes.begin() + _field_offset(position), 8, uint8_t {0});
    return true;
}

bool BinaryRowEncoder::write_boolean(size_t position, bool value) {
    return write_tinyint(position, value ? 1 : 0);
}

bool BinaryRowEncoder::write_tinyint(size_t position, int8_t value) {
    if (!_valid_position(position)) {
        return false;
    }
    put_native(&_bytes, _field_offset(position), value);
    return true;
}

bool BinaryRowEncoder::write_smallint(size_t position, int16_t value) {
    if (!_valid_position(position)) {
        return false;
    }
    put_native(&_bytes, _field_offset(position), value);
    return true;
}

bool BinaryRowEncoder::write_int(size_t position, int32_t value) {
    if (!_valid_position(position)) {
        return false;
    }
    put_native(&_bytes, _field_offset(position), value);
    return true;
}

bool BinaryRowEncoder::write_bigint(size_t position, int64_t value) {
    if (!_valid_position(position)) {
        return false;
    }
    put_native(&_bytes, _field_offset(position), value);
    return true;
}

bool BinaryRowEncoder::write_float(size_t position, float value) {
    if (!_valid_position(position)) {
        return false;
    }
    put_native(&_bytes, _field_offset(position), value);
    return true;
}

bool BinaryRowEncoder::write_double(size_t position, double value) {
    if (!_valid_position(position)) {
        return false;
    }
    put_native(&_bytes, _field_offset(position), value);
    return true;
}

bool BinaryRowEncoder::_write_bytes(size_t position, std::string_view bytes) {
    if (!_valid_position(position) || bytes.size() > std::numeric_limits<uint32_t>::max()) {
        return false;
    }
    if (bytes.size() <= 7U) {
        uint64_t inline_value = (static_cast<uint64_t>(bytes.size()) | 0x80U) << 56U;
        for (size_t index = 0; index < bytes.size(); ++index) {
            if constexpr (std::endian::native == std::endian::little) {
                inline_value |= static_cast<uint64_t>(static_cast<uint8_t>(bytes[index]))
                                << (index * 8U);
            } else {
                inline_value |= static_cast<uint64_t>(static_cast<uint8_t>(bytes[index]))
                                << ((6U - index) * 8U);
            }
        }
        put_native(&_bytes, _field_offset(position), inline_value);
        return true;
    }

    const size_t rounded_size = round_to_word(bytes.size());
    if (rounded_size > std::numeric_limits<uint32_t>::max() ||
        _cursor > std::numeric_limits<uint32_t>::max() - rounded_size) {
        return false;
    }
    _ensure_capacity(_cursor + rounded_size);
    std::fill_n(_bytes.begin() + _cursor, rounded_size, uint8_t {0});
    std::memcpy(_bytes.data() + _cursor, bytes.data(), bytes.size());
    _set_offset_and_size(position, static_cast<uint32_t>(_cursor),
                         static_cast<uint32_t>(bytes.size()));
    _cursor += rounded_size;
    return true;
}

bool BinaryRowEncoder::write_string(size_t position, std::string_view utf8) {
    return _write_bytes(position, utf8);
}

bool BinaryRowEncoder::write_binary(size_t position, std::string_view bytes) {
    return _write_bytes(position, bytes);
}

int32_t BinaryRowEncoder::hash() const {
    return binary_row_hash(std::string_view(reinterpret_cast<const char*>(_bytes.data()), _cursor));
}

int32_t binary_row_hash(std::string_view bytes) {
    uint32_t hash = MURMUR_SEED;
    size_t offset = 0;
    const size_t aligned_length = bytes.size() - bytes.size() % 4U;
    for (; offset < aligned_length; offset += 4U) {
        uint32_t word;
        std::memcpy(&word, bytes.data() + offset, sizeof(word));
        hash = mix_h1(hash, mix_k1(word));
    }
    for (; offset < bytes.size(); ++offset) {
        int32_t signed_byte = static_cast<int8_t>(bytes[offset]);
        hash = mix_h1(hash, mix_k1(static_cast<uint32_t>(signed_byte)));
    }
    uint32_t mixed = fmix(hash, bytes.size());
    int32_t result;
    std::memcpy(&result, &mixed, sizeof(result));
    return result;
}

std::optional<uint32_t> default_bucket(int32_t bucket_key_hash, int32_t num_buckets) {
    if (num_buckets <= 0) {
        return std::nullopt;
    }
    int32_t remainder = bucket_key_hash % num_buckets;
    return static_cast<uint32_t>(remainder < 0 ? -remainder : remainder);
}

std::optional<uint32_t> fixed_bucket_channel(int32_t partition_hash, uint32_t bucket,
                                             uint32_t num_channels) {
    if (num_channels == 0) {
        return std::nullopt;
    }
    if (partition_hash == std::numeric_limits<int32_t>::min()) {
        partition_hash = std::numeric_limits<int32_t>::max();
    }
    uint32_t start_channel =
            static_cast<uint32_t>(partition_hash < 0 ? -partition_hash : partition_hash) %
            num_channels;
    return static_cast<uint32_t>((static_cast<uint64_t>(start_channel) + bucket) % num_channels);
}

} // namespace doris::paimon_native
