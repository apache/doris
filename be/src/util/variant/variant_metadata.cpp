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

#include "util/variant/variant_metadata.h"

#include <cstdint>

#include "common/exception.h"
#include "util/variant/variant_encoding.h"

namespace doris {
namespace {

uint32_t read_unsigned(const char* data, uint8_t width) {
    uint32_t result = 0;
    for (uint8_t i = 0; i < width; ++i) {
        result |= static_cast<uint32_t>(static_cast<uint8_t>(data[i])) << (i * 8);
    }
    return result;
}

void require_bytes(size_t available, size_t required, const char* field) {
    if (available < required) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Truncated Variant metadata while reading {}: need {} bytes, have {}",
                        field, required, available);
    }
}

} // namespace

uint8_t VariantMetadataRef::version() const {
    require_bytes(size, 1, "header");
    return static_cast<uint8_t>(data[0]) & VARIANT_METADATA_VERSION_MASK;
}

bool VariantMetadataRef::sorted_strings() const {
    require_bytes(size, 1, "header");
    return (static_cast<uint8_t>(data[0]) & VARIANT_METADATA_SORTED_STRINGS_MASK) != 0;
}

uint8_t VariantMetadataRef::offset_size() const {
    require_bytes(size, 1, "header");
    return static_cast<uint8_t>(
            ((static_cast<uint8_t>(data[0]) >> VARIANT_METADATA_OFFSET_SIZE_SHIFT) &
             VARIANT_METADATA_OFFSET_SIZE_MASK) +
            1);
}

VariantMetadataRef::Layout VariantMetadataRef::_layout() const {
    require_bytes(size, 1, "header");
    const auto header = static_cast<uint8_t>(data[0]);
    const uint8_t metadata_version = header & VARIANT_METADATA_VERSION_MASK;
    if (metadata_version != VARIANT_ENCODING_VERSION) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Unsupported Variant metadata version {}, expected {}", metadata_version,
                        VARIANT_ENCODING_VERSION);
    }

    const auto width = static_cast<uint8_t>(
            ((header >> VARIANT_METADATA_OFFSET_SIZE_SHIFT) & VARIANT_METADATA_OFFSET_SIZE_MASK) +
            1);
    require_bytes(size - 1, width, "dictionary size");
    const uint32_t count = read_unsigned(data + 1, width);

    constexpr size_t HEADER_SIZE = 1;
    const size_t dictionary_size_offset = HEADER_SIZE;
    const size_t offsets_offset = dictionary_size_offset + width;
    const size_t offsets_bytes = (static_cast<size_t>(count) + 1) * width;
    require_bytes(size - offsets_offset, offsets_bytes, "dictionary offsets");
    const size_t strings_offset = offsets_offset + offsets_bytes;

    const uint32_t first_offset = read_unsigned(data + offsets_offset, width);
    if (first_offset != 0) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Invalid Variant metadata first dictionary offset {}", first_offset);
    }
    const uint32_t final_offset =
            read_unsigned(data + offsets_offset + static_cast<size_t>(count) * width, width);
    if (final_offset != size - strings_offset) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Invalid Variant metadata final dictionary offset {} for {} bytes",
                        final_offset, size - strings_offset);
    }
    return {.offset_width = width,
            .num_keys = count,
            .offsets_offset = offsets_offset,
            .strings_offset = strings_offset};
}

uint32_t VariantMetadataRef::dict_size() const {
    return _layout().num_keys;
}

StringRef VariantMetadataRef::key_at(uint32_t id) const {
    const Layout layout = _layout();
    if (id >= layout.num_keys) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant metadata dictionary id {} is out of range [0, {})", id,
                        layout.num_keys);
    }
    const uint32_t begin = read_unsigned(
            data + layout.offsets_offset + static_cast<size_t>(id) * layout.offset_width,
            layout.offset_width);
    const uint32_t end = read_unsigned(
            data + layout.offsets_offset + (static_cast<size_t>(id) + 1) * layout.offset_width,
            layout.offset_width);
    const size_t strings_size = size - layout.strings_offset;
    if (begin > end || end > strings_size) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Invalid Variant metadata dictionary offsets [{}, {}) for {} bytes", begin,
                        end, strings_size);
    }
    return {data + layout.strings_offset + begin, end - begin};
}

int64_t VariantMetadataRef::find_key(StringRef key) const {
    const Layout layout = _layout();
    if (!sorted_strings()) {
        for (uint32_t id = 0; id < layout.num_keys; ++id) {
            if (key_at(id) == key) {
                return id;
            }
        }
        return -1;
    }

    uint32_t begin = 0;
    uint32_t end = layout.num_keys;
    while (begin < end) {
        const uint32_t middle = begin + (end - begin) / 2;
        const int comparison = key_at(middle).compare(key);
        if (comparison < 0) {
            begin = middle + 1;
        } else {
            end = middle;
        }
    }
    if (begin < layout.num_keys && key_at(begin) == key) {
        return begin;
    }
    return -1;
}

void VariantMetadataRef::validate() const {
    const Layout layout = _layout();
    StringRef previous;
    for (uint32_t id = 0; id < layout.num_keys; ++id) {
        const StringRef current = key_at(id);
        if (sorted_strings() && id != 0 && previous.compare(current) >= 0) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Variant metadata dictionary is not sorted and unique at id {}", id);
        }
        previous = current;
    }
}

} // namespace doris
