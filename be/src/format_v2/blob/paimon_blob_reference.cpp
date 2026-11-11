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

#include "format_v2/blob/paimon_blob_reference.h"

#include <limits>

#include "util/coding.h"

namespace doris::format::paimon {

// Serialized descriptor layout in the physical binary column (byte widths in parentheses).
// Fields are packed without padding; all multi-byte integers are little-endian.
//
// v1: | version (1) | uri_size (4) | URI (N) | offset (8) | length (8) |
// v2: | version (1) | magic (8) | uri_size (4) | URI (N) | offset (8) | length (8) |
//
// version: 1 or 2. Only v2 contains magic, the uint64 value 0x424C4F4244455343.
// uri_size: N, the nonzero number of URI bytes; no separate NUL terminator is stored.
// URI: the file containing the payload; this descriptor contains no payload bytes.
// offset/length: the absolute byte offset and byte length of the payload in that file.
// A zero length denotes an empty payload; column NULLs are handled before decode().
//
// Let H be the bytes before uri_size: 1 for v1, 9 for v2. Then:
//   URI starts at H + 4;
//   offset starts at H + 4 + N, and length starts at H + 12 + N;
//   the complete descriptor is exactly H + 4 + N + 16 bytes, with no trailing bytes.
// This is the descriptor encoding, separate from the .blob record/index/footer format.
Status PaimonBlobReference::decode(std::string_view descriptor_bytes,
                                   PaimonBlobReference* decoded_reference) {
    if (descriptor_bytes.empty()) {
        return Status::Corruption("Empty Paimon BLOB descriptor");
    }
    const auto* descriptor_data = reinterpret_cast<const uint8_t*>(descriptor_bytes.data());
    const uint8_t version = descriptor_data[0];
    if (version != 1 && version != 2) {
        return Status::NotSupported("Unsupported Paimon BLOB descriptor version {}", version);
    }
    // Both versions share the URI/range suffix after their version-specific header.
    const size_t header_size = version == 2 ? 9 : 1;
    if (descriptor_bytes.size() < header_size + 4 + 16) {
        return Status::Corruption("Truncated Paimon BLOB descriptor");
    }
    if (version == 2 && decode_fixed64_le(descriptor_data + 1) != 0x424C4F4244455343ULL) {
        return Status::Corruption("Invalid Paimon BLOB descriptor magic");
    }
    const uint32_t uri_size = decode_fixed32_le(descriptor_data + header_size);
    // Validate the complete suffix before accessing the URI or the two range fields.
    if (uri_size == 0 || uri_size > descriptor_bytes.size() - header_size - 4 - 16 ||
        descriptor_bytes.size() != header_size + 4 + uri_size + 16) {
        return Status::Corruption("Invalid Paimon BLOB descriptor URI length");
    }
    const size_t payload_range_offset = header_size + 4 + uri_size;
    const uint64_t offset = decode_fixed64_le(descriptor_data + payload_range_offset);
    const uint64_t length = decode_fixed64_le(descriptor_data + payload_range_offset + 8);
    // File I/O uses signed offsets; reject negative encodings and offset + length overflow.
    if (offset > uint64_t(std::numeric_limits<int64_t>::max()) ||
        length > uint64_t(std::numeric_limits<int64_t>::max()) - offset) {
        return Status::Corruption("Invalid Paimon managed BLOB descriptor range");
    }
    decoded_reference->uri = descriptor_bytes.substr(header_size + 4, uri_size);
    decoded_reference->offset = static_cast<int64_t>(offset);
    decoded_reference->length = static_cast<int64_t>(length);
    return Status::OK();
}

} // namespace doris::format::paimon
