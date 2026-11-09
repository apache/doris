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

#include "format/table/paimon_deletion_vector.h"

#include <fmt/format.h>

#include <cstring>
#include <stdexcept>

#include "common/status.h"
#include "exec/common/endian.h"
#include "format/table/deletion_vector_reader.h"
#include "format/table/iceberg_delete_file_reader_helper.h"

namespace doris {

namespace {

constexpr uint32_t PAIMON_BITMAP32_MAGIC = 0x5e43f2d0;
constexpr uint32_t PAIMON_BITMAP64_MAGIC = 0xd1d33964;
constexpr size_t PAIMON_DV_HEADER_BYTES = 8;
constexpr int64_t PAIMON_DV_LENGTH_PREFIX_BYTES = 4;
constexpr int64_t PAIMON_DV_CRC_BYTES = 4;

} // namespace

std::string build_paimon_deletion_vector_cache_key(const TPaimonDeletionFileDesc& deletion_file) {
    return fmt::format("paimon_dv_{}#{}#{}", deletion_file.path, deletion_file.offset,
                       deletion_file.length);
}

Status validate_paimon_deletion_vector_descriptor(const TPaimonDeletionFileDesc& deletion_file) {
    if (!deletion_file.__isset.path || !deletion_file.__isset.offset ||
        !deletion_file.__isset.length) {
        return Status::DataQualityError(
                "Paimon deletion file descriptor misses path/offset/length");
    }
    return validate_paimon_deletion_vector_read_range(deletion_file.offset, deletion_file.length);
}

Status read_paimon_deletion_vector_buffer(DeletionVectorReader& reader, const DeleteFileDesc& desc,
                                          std::vector<char>& buffer) {
    char header[PAIMON_DV_HEADER_BYTES];
    RETURN_IF_ERROR(reader.read_at(desc.start_offset, {header, sizeof(header)}));
    const uint32_t payload_size = BigEndian::Load32(header);
    const uint32_t magic = BigEndian::Load32(header + PAIMON_DV_LENGTH_PREFIX_BYTES);
    // The length prefix counts the magic itself, even for an empty bitmap.
    if (payload_size < sizeof(magic)) {
        return Status::DataQualityError(
                "Paimon deletion vector payload too small: path {}, offset {}, size {}", desc.path,
                desc.start_offset, payload_size);
    }
    if (magic != PAIMON_BITMAP32_MAGIC && magic != PAIMON_BITMAP64_MAGIC) {
        return Status::DataQualityError(
                "Paimon deletion vector magic number mismatch: path {}, offset {}, magic {}",
                desc.path, desc.start_offset, magic);
    }

    // Paimon may mix bitmap32 and bitmap64 entries in one index file. The bitmap32 descriptor
    // counts only magic and bitmap, while bitmap64 counts the prefix and CRC as well.
    const bool bitmap64 = magic == PAIMON_BITMAP64_MAGIC;
    const int64_t read_size =
            payload_size + PAIMON_DV_LENGTH_PREFIX_BYTES + (bitmap64 ? PAIMON_DV_CRC_BYTES : 0);
    const int64_t expected_length = bitmap64 ? read_size : payload_size;
    if (desc.size != expected_length) {
        return Status::DataQualityError(
                "Paimon deletion vector descriptor length mismatch: path {}, offset {}, expected "
                "{}, actual {}",
                desc.path, desc.start_offset, expected_length, desc.size);
    }
    RETURN_IF_ERROR(validate_paimon_deletion_vector_read_range(desc.start_offset, read_size));
    RETURN_IF_ERROR(reader.validate_file_range(desc.start_offset, read_size));
    buffer.resize(read_size);
    memcpy(buffer.data(), header, sizeof(header));
    return reader.read_at(desc.start_offset + sizeof(header),
                          {buffer.data() + sizeof(header), buffer.size() - sizeof(header)});
}

Status decode_paimon_deletion_vector_buffer(const char* buf, size_t buffer_size,
                                            DeletionVector* deletion_vector) {
    if (deletion_vector == nullptr) {
        return Status::InvalidArgument("deletion vector output must not be null");
    }
    if (buf == nullptr) {
        return Status::DataQualityError("Paimon deletion vector blob is null");
    }
    if (buffer_size < PAIMON_DV_HEADER_BYTES) [[unlikely]] {
        return Status::DataQualityError("Deletion vector file size too small: {}", buffer_size);
    }

    const uint32_t magic = BigEndian::Load32(buf + PAIMON_DV_LENGTH_PREFIX_BYTES);
    if (magic == PAIMON_BITMAP64_MAGIC) {
        // Bitmap64 uses the same on-disk framing and portable Roaring64 layout as Iceberg DVs.
        return decode_iceberg_deletion_vector_buffer(buf, buffer_size, deletion_vector);
    }

    const uint32_t actual_length = BigEndian::Load32(buf);
    const uint64_t expected_size =
            static_cast<uint64_t>(actual_length) + PAIMON_DV_LENGTH_PREFIX_BYTES;
    if (expected_size != buffer_size) [[unlikely]] {
        return Status::DataQualityError(
                "Paimon deletion vector length mismatch, expected: {}, actual: {}", expected_size,
                buffer_size);
    }

    if (magic != PAIMON_BITMAP32_MAGIC) [[unlikely]] {
        return Status::DataQualityError(
                "Paimon deletion vector magic number mismatch, expected: {}, actual: {}",
                PAIMON_BITMAP32_MAGIC, magic);
    }

    roaring::Roaring roaring_bitmap;
    try {
        roaring_bitmap = roaring::Roaring::readSafe(buf + PAIMON_DV_HEADER_BYTES,
                                                    buffer_size - PAIMON_DV_HEADER_BYTES);
    } catch (const std::runtime_error& e) {
        return Status::RuntimeError(
                "DeletionVector deserialize error: failed to deserialize roaring bitmap, {}",
                e.what());
    }

    *deletion_vector |= DeletionVector(std::move(roaring_bitmap));
    return Status::OK();
}

} // namespace doris
