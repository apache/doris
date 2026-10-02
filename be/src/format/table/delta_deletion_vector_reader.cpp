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

#include "format/table/delta_deletion_vector_reader.h"

#include <algorithm>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <cctype>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string_view>

#include "exec/common/endian.h"
#include "roaring/roaring.hh"
#include "util/hash_util.hpp"

namespace doris {
namespace {

constexpr int64_t MAX_DELTA_DELETION_VECTOR_BYTES = 1L << 30;
constexpr size_t DELTA_DELETION_VECTOR_WRAPPER_BYTES = 8;
constexpr uint32_t DELTA_NATIVE_BITMAP_ARRAY_MAGIC = 1681511376;
constexpr uint32_t DELTA_PORTABLE_BITMAP_ARRAY_MAGIC = 1681511377;
constexpr std::string_view Z85_ALPHABET =
        "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";

Status validate_serialized_size(int64_t size_in_bytes) {
    if (size_in_bytes < 0 || size_in_bytes > MAX_DELTA_DELETION_VECTOR_BYTES) {
        return Status::DataQualityError("Invalid Delta deletion vector size: {}", size_in_bytes);
    }
    return Status::OK();
}

Status check_expected_cardinality(const DeletionVector& deletion_vector,
                                  int64_t expected_cardinality) {
    if (expected_cardinality >= 0 &&
        deletion_vector.cardinality() != static_cast<uint64_t>(expected_cardinality)) {
        return Status::DataQualityError(
                "Delta deletion vector cardinality mismatch, expected: {}, actual: {}",
                expected_cardinality, deletion_vector.cardinality());
    }
    return Status::OK();
}

Status append_roaring_bitmap(const char* data, size_t size, uint64_t high,
                             DeletionVector* deletion_vector, size_t* consumed) {
    if (data == nullptr || deletion_vector == nullptr || consumed == nullptr) {
        return Status::InvalidArgument("Invalid Delta Roaring bitmap decode arguments");
    }
    if (size == 0) {
        return Status::DataQualityError("Empty Delta Roaring bitmap");
    }

    try {
        roaring::Roaring bitmap = roaring::Roaring::readSafe(data, size);
        const size_t bitmap_size = bitmap.getSizeInBytes();
        if (bitmap_size > size) {
            return Status::DataQualityError("Delta Roaring bitmap exceeds serialized payload");
        }
        for (auto iter = bitmap.begin(); iter != bitmap.end(); ++iter) {
            deletion_vector->add((high << 32) | static_cast<uint32_t>(*iter));
        }
        *consumed = bitmap_size;
    } catch (const std::runtime_error& e) {
        return Status::DataQualityError("Decode Delta Roaring bitmap failed: {}", e.what());
    }
    return Status::OK();
}

Status decode_serialized_bitmap_array(const char* data, size_t size, int64_t expected_cardinality,
                                      DeletionVector* deletion_vector) {
    if (data == nullptr || deletion_vector == nullptr) {
        return Status::InvalidArgument("Invalid Delta deletion vector decode arguments");
    }
    if (size < sizeof(uint32_t)) {
        return Status::DataQualityError("Delta deletion vector payload is too small: {}", size);
    }

    const uint32_t magic = LittleEndian::Load32(data);
    size_t position = sizeof(uint32_t);
    if (magic == DELTA_NATIVE_BITMAP_ARRAY_MAGIC) {
        if (size - position < sizeof(uint32_t)) {
            return Status::DataQualityError("Delta native deletion vector misses bitmap count");
        }
        const uint32_t bitmap_count = LittleEndian::Load32(data + position);
        position += sizeof(uint32_t);
        if (bitmap_count > (size - position) / (sizeof(uint32_t) * 2)) {
            return Status::DataQualityError("Invalid Delta native deletion vector bitmap count: {}",
                                            bitmap_count);
        }
        for (uint32_t i = 0; i < bitmap_count; ++i) {
            if (size - position < sizeof(uint32_t)) {
                return Status::DataQualityError("Delta native deletion vector misses bitmap size");
            }
            const uint32_t bitmap_size = LittleEndian::Load32(data + position);
            position += sizeof(uint32_t);
            if (bitmap_size > size - position) {
                return Status::DataQualityError("Delta native bitmap exceeds payload");
            }
            size_t consumed = 0;
            RETURN_IF_ERROR(append_roaring_bitmap(data + position, bitmap_size, i, deletion_vector,
                                                  &consumed));
            if (consumed != bitmap_size) {
                return Status::DataQualityError(
                        "Delta Roaring bitmap size mismatch, expected: {}, actual: {}", bitmap_size,
                        consumed);
            }
            position += bitmap_size;
        }
    } else if (magic == DELTA_PORTABLE_BITMAP_ARRAY_MAGIC) {
        if (size - position < sizeof(uint64_t)) {
            return Status::DataQualityError("Delta portable deletion vector misses bitmap count");
        }
        const uint64_t bitmap_count = LittleEndian::Load64(data + position);
        position += sizeof(uint64_t);
        if (bitmap_count > (size - position) / sizeof(uint32_t)) {
            return Status::DataQualityError(
                    "Invalid Delta portable deletion vector bitmap count: {}", bitmap_count);
        }
        uint32_t last_key = 0;
        for (uint64_t i = 0; i < bitmap_count; ++i) {
            if (size - position < sizeof(uint32_t)) {
                return Status::DataQualityError("Delta portable deletion vector misses bitmap key");
            }
            const uint32_t key = LittleEndian::Load32(data + position);
            position += sizeof(uint32_t);
            if (key > std::numeric_limits<int32_t>::max() || (i > 0 && key <= last_key)) {
                return Status::DataQualityError("Invalid Delta portable deletion vector key: {}",
                                                key);
            }
            last_key = key;
            size_t consumed = 0;
            RETURN_IF_ERROR(append_roaring_bitmap(data + position, size - position, key,
                                                  deletion_vector, &consumed));
            position += consumed;
        }
    } else {
        return Status::DataQualityError("Unknown Delta deletion vector magic: {}", magic);
    }

    if (position != size) {
        return Status::DataQualityError("Trailing bytes in Delta deletion vector payload: {}",
                                        size - position);
    }
    return check_expected_cardinality(*deletion_vector, expected_cardinality);
}

Status decode_z85(std::string_view encoded, int64_t output_size, std::string* decoded) {
    if (decoded == nullptr) {
        return Status::InvalidArgument("Decoded Delta deletion vector output is null");
    }
    RETURN_IF_ERROR(validate_serialized_size(output_size));
    const auto expected_encoded_size = (static_cast<uint64_t>(output_size) + 3) / 4 * 5;
    if (encoded.size() != expected_encoded_size || encoded.size() % 5 != 0) {
        return Status::DataQualityError("Invalid Delta Z85 length: {}", encoded.size());
    }

    decoded->assign(static_cast<size_t>(output_size), '\0');
    size_t output_position = 0;
    for (size_t input_position = 0; input_position < encoded.size(); input_position += 5) {
        uint32_t value = 0;
        for (size_t i = 0; i < 5; ++i) {
            const size_t alphabet_position = Z85_ALPHABET.find(encoded[input_position + i]);
            if (alphabet_position == std::string_view::npos) {
                return Status::DataQualityError("Invalid character in Delta Z85 deletion vector");
            }
            value = value * 85 + static_cast<uint32_t>(alphabet_position);
        }
        char bytes[sizeof(uint32_t)];
        BigEndian::Store32(bytes, value);
        const size_t bytes_to_copy = std::min(sizeof(bytes), decoded->size() - output_position);
        std::memcpy(decoded->data() + output_position, bytes, bytes_to_copy);
        output_position += bytes_to_copy;
    }
    return Status::OK();
}

Status decode_uuid(std::string_view encoded, boost::uuids::uuid* uuid) {
    if (uuid == nullptr || encoded.size() != 20) {
        return Status::DataQualityError("Invalid Delta UUID deletion vector identifier");
    }
    std::string bytes;
    RETURN_IF_ERROR(decode_z85(encoded, 16, &bytes));
    std::copy(bytes.begin(), bytes.end(), uuid->begin());
    return Status::OK();
}

bool is_absolute_uri(std::string_view value) {
    const size_t colon = value.find(':');
    if (colon == std::string_view::npos || colon == 0 ||
        !std::isalpha(static_cast<unsigned char>(value.front()))) {
        return false;
    }
    return std::all_of(value.begin() + 1, value.begin() + colon, [](char character) {
        const auto value = static_cast<unsigned char>(character);
        return std::isalnum(value) || character == '+' || character == '-' || character == '.';
    });
}

} // namespace

Status validate_delta_deletion_vector_read_range(int64_t offset, int64_t size, size_t& bytes_read) {
    if (offset < 0) {
        return Status::DataQualityError("Invalid Delta deletion vector offset: {}", offset);
    }
    RETURN_IF_ERROR(validate_serialized_size(size));
    if (size > std::numeric_limits<int64_t>::max() - DELTA_DELETION_VECTOR_WRAPPER_BYTES) {
        return Status::DataQualityError("Delta deletion vector read size overflows");
    }
    bytes_read = static_cast<size_t>(size + DELTA_DELETION_VECTOR_WRAPPER_BYTES);
    return Status::OK();
}

Status decode_delta_deletion_vector_buffer(const char* buf, size_t buffer_size,
                                           int64_t expected_cardinality,
                                           DeletionVector* deletion_vector) {
    if (buf == nullptr || deletion_vector == nullptr) {
        return Status::InvalidArgument("Invalid Delta deletion vector decode arguments");
    }
    if (expected_cardinality == 0) {
        return Status::OK();
    }
    if (buffer_size < DELTA_DELETION_VECTOR_WRAPPER_BYTES) {
        return Status::DataQualityError("Delta deletion vector file is too small: {}", buffer_size);
    }
    const uint32_t serialized_size = BigEndian::Load32(buf);
    if (static_cast<uint64_t>(serialized_size) + DELTA_DELETION_VECTOR_WRAPPER_BYTES !=
        buffer_size) {
        return Status::DataQualityError(
                "Delta deletion vector size mismatch, expected: {}, actual: {}",
                static_cast<uint64_t>(serialized_size) + DELTA_DELETION_VECTOR_WRAPPER_BYTES,
                buffer_size);
    }
    const uint32_t expected_crc = BigEndian::Load32(buf + 4 + serialized_size);
    const uint32_t actual_crc = HashUtil::zlib_crc_hash(buf + 4, serialized_size, 0);
    if (actual_crc != expected_crc) {
        return Status::DataQualityError("Delta deletion vector CRC32 mismatch");
    }
    return decode_serialized_bitmap_array(buf + 4, serialized_size, expected_cardinality,
                                          deletion_vector);
}

Status decode_delta_inline_deletion_vector(const std::string& encoded, int64_t size_in_bytes,
                                           int64_t expected_cardinality,
                                           DeletionVector* deletion_vector) {
    if (deletion_vector == nullptr) {
        return Status::InvalidArgument("Delta deletion vector output is null");
    }
    if (expected_cardinality == 0) {
        return Status::OK();
    }
    std::string decoded;
    RETURN_IF_ERROR(decode_z85(encoded, size_in_bytes, &decoded));
    return decode_serialized_bitmap_array(decoded.data(), decoded.size(), expected_cardinality,
                                          deletion_vector);
}

Status resolve_delta_deletion_vector_path(const std::string& storage_type,
                                          const std::string& path_or_inline_dv,
                                          const std::string& table_path, std::string* path) {
    if (path == nullptr) {
        return Status::InvalidArgument("Delta deletion vector path output is null");
    }
    if (storage_type == "p") {
        if (!is_absolute_uri(path_or_inline_dv)) {
            return Status::DataQualityError("Delta path deletion vector must use an absolute URI");
        }
        *path = path_or_inline_dv;
        return Status::OK();
    }
    if (storage_type != "u") {
        return Status::DataQualityError("Unsupported Delta deletion vector storage type: {}",
                                        storage_type);
    }
    if (table_path.empty() || path_or_inline_dv.size() < 20) {
        return Status::DataQualityError("Delta UUID deletion vector misses table path or UUID");
    }
    const size_t prefix_size = path_or_inline_dv.size() - 20;
    boost::uuids::uuid uuid;
    RETURN_IF_ERROR(decode_uuid(std::string_view(path_or_inline_dv).substr(prefix_size), &uuid));
    const std::string prefix = path_or_inline_dv.substr(0, prefix_size);
    *path = table_path;
    if (!path->empty() && path->back() != '/') {
        path->push_back('/');
    }
    if (!prefix.empty()) {
        *path += prefix;
        if (path->back() != '/') {
            path->push_back('/');
        }
    }
    *path += "deletion_vector_" + boost::uuids::to_string(uuid) + ".bin";
    return Status::OK();
}

} // namespace doris
