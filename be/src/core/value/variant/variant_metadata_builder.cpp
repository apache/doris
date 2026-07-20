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

#include <parallel_hashmap/phmap.h>

#include <algorithm>
#include <cstdint>
#include <deque>
#include <functional>
#include <limits>
#include <ranges>
#include <utility>

#include "common/exception.h"
#include "core/value/variant/variant_block_builder.h"
#include "core/value/variant/variant_encoded_block.h"
#include "core/value/variant/variant_encoding.h"
#include "core/value/variant/variant_tracked_storage.h"
#include "util/utf8_check.h"

namespace doris {
namespace {

constexpr uint32_t INVALID_INDEX = std::numeric_limits<uint32_t>::max();

uint8_t minimum_unsigned_width(uint64_t value) {
    if (value <= std::numeric_limits<uint8_t>::max()) {
        return 1;
    }
    if (value <= std::numeric_limits<uint16_t>::max()) {
        return 2;
    }
    if (value <= 0xFFFFFFU) {
        return 3;
    }
    return 4;
}

void validate_utf8_string_ref(StringRef value, const char* description) {
    if (value.data == nullptr && value.size != 0) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant {} has a null data pointer",
                        description);
    }
    if (value.size != 0 && !validate_utf8(value.data, value.size)) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant {} is not valid UTF-8", description);
    }
}

template <typename String>
void append_unsigned(String& output, uint64_t value, uint8_t width) {
    for (uint8_t byte = 0; byte < width; ++byte) {
        output.push_back(static_cast<char>(value >> (byte * 8)));
    }
}

} // namespace

struct VariantMetadataBuilder::Impl {
    using KeyMapValue = std::pair<const StringRef, uint32_t>;
    using KeyMap = phmap::flat_hash_map<StringRef, uint32_t, StringRefHash, std::equal_to<>,
                                        CustomStdAllocator<KeyMapValue>>;
    using Keys = std::deque<VariantTrackedString, CustomStdAllocator<VariantTrackedString>>;

    Keys keys;
    KeyMap key_to_temporary_id;
    DorisVector<uint64_t> key_use_counts;
    DorisVector<uint32_t> temporary_to_final_id;
    VariantTrackedString encoded;
    uint64_t active_collectors = 0;
    size_t key_capacity_growths = 0;
    uint32_t sealed_key_count = 0;
    bool sealed = false;
};

VariantMetadataBuilder::VariantMetadataBuilder() : _impl(std::make_unique<Impl>()) {}

VariantMetadataBuilder::~VariantMetadataBuilder() = default;

uint32_t VariantMetadataBuilder::register_key(StringRef key) {
    if (_impl->sealed) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot register a Variant metadata key after seal");
    }
    validate_utf8_string_ref(key, "metadata key");
    const auto existing = _impl->key_to_temporary_id.find(key);
    if (existing != _impl->key_to_temporary_id.end()) {
        return existing->second;
    }
    if (_impl->keys.size() == std::numeric_limits<uint32_t>::max()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant metadata dictionary exceeds the uint32 key limit");
    }

    const auto temporary_id = static_cast<uint32_t>(_impl->keys.size());
    _impl->keys.emplace_back(key.data == nullptr ? "" : key.data, key.size);
    const size_t old_capacity = _impl->key_use_counts.capacity();
    _impl->key_use_counts.push_back(0);
    if (_impl->key_use_counts.capacity() != old_capacity) {
        ++_impl->key_capacity_growths;
    }
    const StringRef owned_key {_impl->keys.back().data(), _impl->keys.back().size()};
    const auto [unused, inserted] = _impl->key_to_temporary_id.emplace(owned_key, temporary_id);
    static_cast<void>(unused);
    DCHECK(inserted);
    return temporary_id;
}

void VariantMetadataBuilder::_begin_row() {
    if (_impl->sealed) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot create a Variant row builder after metadata seal");
    }
    ++_impl->active_collectors;
}

void VariantMetadataBuilder::_retain_key(uint32_t temporary_id) noexcept {
    DCHECK_LT(temporary_id, _impl->key_use_counts.size());
    DCHECK(!_impl->sealed);
    DCHECK_LT(_impl->key_use_counts[temporary_id], std::numeric_limits<uint64_t>::max());
    ++_impl->key_use_counts[temporary_id];
}

void VariantMetadataBuilder::_complete_row() noexcept {
    DCHECK(!_impl->sealed);
    DCHECK_GT(_impl->active_collectors, 0);
    --_impl->active_collectors;
}

void VariantMetadataBuilder::_abort_row(const uint32_t* temporary_ids, size_t count,
                                        bool was_collecting) noexcept {
    DCHECK(!_impl->sealed);
    for (size_t index = 0; index < count; ++index) {
        DCHECK_LT(temporary_ids[index], _impl->key_use_counts.size());
        DCHECK_GT(_impl->key_use_counts[temporary_ids[index]], 0);
        --_impl->key_use_counts[temporary_ids[index]];
    }
    if (was_collecting) {
        DCHECK_GT(_impl->active_collectors, 0);
        --_impl->active_collectors;
    }
}

void VariantMetadataBuilder::_reserve_keys(size_t count) {
    if (_impl->sealed) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot reserve Variant metadata keys after seal");
    }
    _impl->key_use_counts.reserve(count);
    _impl->key_to_temporary_id.reserve(count);
}

StringRef VariantMetadataBuilder::_temporary_key(uint32_t temporary_id) const noexcept {
    DCHECK_LT(temporary_id, _impl->keys.size());
    return {_impl->keys[temporary_id].data(), _impl->keys[temporary_id].size()};
}

size_t VariantMetadataBuilder::_key_capacity() const noexcept {
    return _impl->key_use_counts.capacity();
}

size_t VariantMetadataBuilder::_key_capacity_growths() const noexcept {
    return _impl->key_capacity_growths;
}

void VariantMetadataBuilder::seal() {
    if (_impl->sealed) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant metadata is already sealed");
    }
    if (_impl->active_collectors != 0) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot seal Variant metadata while {} row builders are incomplete",
                        _impl->active_collectors);
    }

    DorisVector<uint32_t> sorted_temporary_ids;
    sorted_temporary_ids.reserve(_impl->keys.size());
    for (uint32_t temporary_id = 0; temporary_id < _impl->keys.size(); ++temporary_id) {
        if (_impl->key_use_counts[temporary_id] != 0) {
            sorted_temporary_ids.push_back(temporary_id);
        }
    }
    std::ranges::sort(sorted_temporary_ids, [this](uint32_t left, uint32_t right) {
        const StringRef left_key {_impl->keys[left].data(), _impl->keys[left].size()};
        const StringRef right_key {_impl->keys[right].data(), _impl->keys[right].size()};
        return left_key.compare(right_key) < 0;
    });

    uint64_t strings_size = 0;
    for (uint32_t temporary_id : sorted_temporary_ids) {
        strings_size += _impl->keys[temporary_id].size();
        if (strings_size > std::numeric_limits<uint32_t>::max()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant metadata dictionary strings exceed the uint32 byte limit");
        }
    }

    const auto count = static_cast<uint32_t>(sorted_temporary_ids.size());
    _impl->temporary_to_final_id.assign(_impl->keys.size(), INVALID_INDEX);
    for (uint32_t final_id = 0; final_id < count; ++final_id) {
        _impl->temporary_to_final_id[sorted_temporary_ids[final_id]] = final_id;
    }

    const uint8_t offset_width = minimum_unsigned_width(std::max<uint64_t>(count, strings_size));
    const uint64_t encoded_size =
            1 + offset_width + (static_cast<uint64_t>(count) + 1) * offset_width + strings_size;
    if (encoded_size > std::numeric_limits<size_t>::max()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant metadata exceeds the addressable output size");
    }
    _impl->encoded.reserve(static_cast<size_t>(encoded_size));
    const uint8_t header =
            VARIANT_ENCODING_VERSION | VARIANT_METADATA_SORTED_STRINGS_MASK |
            static_cast<uint8_t>((offset_width - 1) << VARIANT_METADATA_OFFSET_SIZE_SHIFT);
    _impl->encoded.push_back(static_cast<char>(header));
    append_unsigned(_impl->encoded, count, offset_width);

    uint32_t offset = 0;
    append_unsigned(_impl->encoded, offset, offset_width);
    for (uint32_t temporary_id : sorted_temporary_ids) {
        offset += static_cast<uint32_t>(_impl->keys[temporary_id].size());
        append_unsigned(_impl->encoded, offset, offset_width);
    }
    for (uint32_t temporary_id : sorted_temporary_ids) {
        _impl->encoded.append(_impl->keys[temporary_id].data(), _impl->keys[temporary_id].size());
    }
    DCHECK_EQ(_impl->encoded.size(), encoded_size);
    _impl->sealed_key_count = count;
    _impl->sealed = true;
}

bool VariantMetadataBuilder::is_sealed() const noexcept {
    return _impl->sealed;
}

size_t VariantMetadataBuilder::num_keys() const noexcept {
    return _impl->sealed ? _impl->sealed_key_count : _impl->keys.size();
}

uint32_t VariantMetadataBuilder::final_id(uint32_t temporary_id) const {
    if (!_impl->sealed) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot resolve a Variant metadata id before seal");
    }
    if (temporary_id >= _impl->temporary_to_final_id.size()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant temporary metadata id {} is out of range [0, {})", temporary_id,
                        _impl->temporary_to_final_id.size());
    }
    if (_impl->temporary_to_final_id[temporary_id] == INVALID_INDEX) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant temporary metadata id {} is not referenced by this encoding unit",
                        temporary_id);
    }
    return _impl->temporary_to_final_id[temporary_id];
}

StringRef VariantMetadataBuilder::encoded_metadata() const {
    if (!_impl->sealed) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot access Variant metadata bytes before seal");
    }
    return {_impl->encoded.data(), _impl->encoded.size()};
}

VariantTrackedString VariantMetadataBuilder::_take_encoded_metadata() noexcept {
    DCHECK(_impl->sealed);
    VariantTrackedString encoded = std::move(_impl->encoded);
    release_variant_tracked_container(_impl->keys);
    release_variant_tracked_container(_impl->key_to_temporary_id);
    release_variant_tracked_container(_impl->key_use_counts);
    release_variant_tracked_container(_impl->temporary_to_final_id);
    return encoded;
}

VariantMetadataRef VariantMetadataBuilder::metadata_ref() const {
    const StringRef bytes = encoded_metadata();
    return {.data = bytes.data, .size = bytes.size};
}

} // namespace doris
