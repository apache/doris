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

namespace doris {

namespace {

constexpr char PAIMON_BITMAP_MAGIC[] = {'\x5E', '\x43', '\xF2', '\xD0'};

} // namespace

std::string build_paimon_deletion_vector_cache_key(const TPaimonDeletionFileDesc& deletion_file) {
    return fmt::format("paimon_dv_{}#{}#{}", deletion_file.path, deletion_file.offset,
                       deletion_file.length);
}

Status validate_paimon_deletion_vector_descriptor(const TPaimonDeletionFileDesc& deletion_file,
                                                  size_t& bytes_read) {
    if (!deletion_file.__isset.path || !deletion_file.__isset.offset ||
        !deletion_file.__isset.length) {
        return Status::DataQualityError(
                "Paimon deletion file descriptor misses path/offset/length");
    }
    return validate_paimon_deletion_vector_read_range(deletion_file.offset, deletion_file.length,
                                                      bytes_read);
}

Status decode_paimon_deletion_vector_buffer(const char* buf, size_t buffer_size,
                                            DeletionVector* deletion_vector) {
    if (deletion_vector == nullptr) {
        return Status::InvalidArgument("deletion vector output must not be null");
    }
    if (buf == nullptr) {
        return Status::DataQualityError("Paimon deletion vector blob is null");
    }
    if (buffer_size < 8) [[unlikely]] {
        return Status::DataQualityError("Deletion vector file size too small: {}", buffer_size);
    }

    const uint32_t actual_length = BigEndian::Load32(buf);
    if (static_cast<uint64_t>(actual_length) + 4 != buffer_size) [[unlikely]] {
        return Status::DataQualityError(
                "Paimon deletion vector length mismatch, expected: {}, actual: {}",
                static_cast<uint64_t>(actual_length) + 4, buffer_size);
    }

    if (memcmp(buf + sizeof(actual_length), PAIMON_BITMAP_MAGIC, 4) != 0) [[unlikely]] {
        return Status::DataQualityError(
                "Paimon deletion vector magic number mismatch, expected: {}, actual: {}",
                BigEndian::Load32(PAIMON_BITMAP_MAGIC),
                BigEndian::Load32(buf + sizeof(actual_length)));
    }

    roaring::Roaring roaring_bitmap;
    try {
        roaring_bitmap = roaring::Roaring::readSafe(buf + 8, buffer_size - 8);
    } catch (const std::runtime_error& e) {
        return Status::RuntimeError(
                "DeletionVector deserialize error: failed to deserialize roaring bitmap, {}",
                e.what());
    }

    *deletion_vector |= DeletionVector(std::move(roaring_bitmap));
    return Status::OK();
}

} // namespace doris
