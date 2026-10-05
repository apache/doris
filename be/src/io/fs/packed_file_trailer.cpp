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

#include "io/fs/packed_file_trailer.h"

#include <array>
#include <fstream>

#include "common/status.h"
#include "util/coding.h"

namespace doris::io {

Status parse_packed_file_trailer(std::string_view data, cloud::PackedFileFooterPB* debug_pb,
                                 uint32_t* version) {
    if (debug_pb == nullptr || version == nullptr) {
        return Status::InvalidArgument("Output parameters must not be null");
    }
    if (data.size() < kPackedFileTrailerSuffixSize) {
        return Status::InternalError("Packed file too small to contain trailer");
    }

    const size_t suffix_offset = data.size() - kPackedFileTrailerSuffixSize;
    const auto* suffix_ptr = reinterpret_cast<const uint8_t*>(data.data() + suffix_offset);
    const uint32_t trailer_size = decode_fixed32_le(suffix_ptr);
    const uint32_t trailer_version = decode_fixed32_le(suffix_ptr + sizeof(uint32_t));

    // Preferred format: [PackedFileFooterPB][length][version]
    if (trailer_size > 0 && trailer_size <= data.size() - kPackedFileTrailerSuffixSize) {
        const size_t payload_offset = data.size() - kPackedFileTrailerSuffixSize - trailer_size;
        std::string_view payload(data.data() + payload_offset, trailer_size);
        if (payload.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
            return Status::InternalError("Packed file trailer payload too large");
        }
        cloud::PackedFileFooterPB parsed_pb;
        if (parsed_pb.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
            debug_pb->Swap(&parsed_pb);
            *version = trailer_version;
            return Status::OK();
        }
    }

    // Legacy format fallback: [PackedFileInfoPB][length]
    if (data.size() < sizeof(uint32_t)) {
        return Status::InternalError("Packed file trailer corrupted");
    }
    const size_t legacy_suffix_offset = data.size() - sizeof(uint32_t);
    const auto* legacy_ptr = reinterpret_cast<const uint8_t*>(data.data() + legacy_suffix_offset);
    const uint32_t legacy_size = decode_fixed32_le(legacy_ptr);
    if (legacy_size == 0 || legacy_size > data.size() - sizeof(uint32_t)) {
        return Status::InternalError("Packed file trailer corrupted");
    }
    const size_t legacy_payload_offset = data.size() - sizeof(uint32_t) - legacy_size;
    std::string_view legacy_payload(data.data() + legacy_payload_offset, legacy_size);
    cloud::PackedFileInfoPB packed_info;
    if (legacy_payload.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
        return Status::InternalError("Packed file legacy trailer payload too large");
    }
    if (!packed_info.ParseFromArray(legacy_payload.data(),
                                    static_cast<int>(legacy_payload.size()))) {
        return Status::InternalError("Failed to parse packed file trailer");
    }
    debug_pb->Clear();
    debug_pb->mutable_packed_file_info()->Swap(&packed_info);
    *version = 0;
    return Status::OK();
}

Status read_packed_file_trailer(const std::string& file_path, cloud::PackedFileFooterPB* debug_pb,
                                uint32_t* version) {
    if (debug_pb == nullptr || version == nullptr) {
        return Status::InvalidArgument("Output parameters must not be null");
    }

    std::ifstream file(file_path, std::ios::binary);
    if (!file.is_open()) {
        return Status::IOError("Failed to open packed file {}", file_path);
    }

    file.seekg(0, std::ios::end);
    const std::streamoff file_size = file.tellg();
    if (file_size < static_cast<std::streamoff>(sizeof(uint32_t))) {
        return Status::InternalError("Packed file {} is too small", file_path);
    }

    auto read_tail = [&](std::streamoff count, std::string* out) -> Status {
        out->assign(static_cast<size_t>(count), '\0');
        file.seekg(file_size - count);
        file.read(out->data(), count);
        if (!file) {
            return Status::IOError("Failed to read last {} bytes from {}", count, file_path);
        }
        return Status::OK();
    };

    // Try new format first.
    if (file_size >= static_cast<std::streamoff>(kPackedFileTrailerSuffixSize)) {
        std::array<char, kPackedFileTrailerSuffixSize> suffix {};
        file.seekg(file_size - static_cast<std::streamoff>(suffix.size()));
        file.read(suffix.data(), suffix.size());
        if (file) {
            const uint32_t trailer_size =
                    decode_fixed32_le(reinterpret_cast<uint8_t*>(suffix.data()));
            const uint32_t trailer_version =
                    decode_fixed32_le(reinterpret_cast<uint8_t*>(suffix.data()) + sizeof(uint32_t));
            const std::streamoff required =
                    static_cast<std::streamoff>(kPackedFileTrailerSuffixSize + trailer_size);
            if (trailer_size > 0 && file_size >= required) {
                std::string tail;
                RETURN_IF_ERROR(read_tail(required, &tail));
                Status st = parse_packed_file_trailer(tail, debug_pb, version);
                if (st.ok() && *version == trailer_version) {
                    return st;
                }
            }
        }
        file.clear();
    }

    // Legacy fallback: PackedFileInfoPB + length.
    std::array<char, sizeof(uint32_t)> legacy_suffix {};
    file.seekg(file_size - static_cast<std::streamoff>(legacy_suffix.size()));
    file.read(legacy_suffix.data(), legacy_suffix.size());
    if (!file) {
        return Status::IOError("Failed to read legacy trailer length from {}", file_path);
    }
    const uint32_t legacy_size =
            decode_fixed32_le(reinterpret_cast<uint8_t*>(legacy_suffix.data()));
    const std::streamoff required = static_cast<std::streamoff>(sizeof(uint32_t) + legacy_size);
    if (legacy_size == 0 || file_size < required) {
        return Status::InternalError("Packed file trailer corrupted for {}", file_path);
    }
    std::string tail;
    RETURN_IF_ERROR(read_tail(required, &tail));
    return parse_packed_file_trailer(tail, debug_pb, version);
}

} // namespace doris::io
