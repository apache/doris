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

#include <zlib.h>

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace doris::format::blob_test {

// Small v1 fixtures for boundary/corruption tests. The reader test also pins bytes written
// by Paimon 1.4.2, so this helper is not the only oracle for the on-disk format.
inline void append_little_endian(std::string* bytes, uint64_t value, size_t width) {
    for (size_t i = 0; i < width; ++i) {
        bytes->push_back(static_cast<char>((value >> (i * 8)) & 0xff));
    }
}

inline std::string encode_record_lengths(const std::vector<int64_t>& lengths) {
    std::string index;
    int64_t previous = 0;
    for (const int64_t length : lengths) {
        const int64_t delta = length - previous;
        uint64_t encoded = (static_cast<uint64_t>(delta) << 1) ^
                           static_cast<uint64_t>(-static_cast<int64_t>(delta < 0));
        do {
            auto byte = static_cast<uint8_t>(encoded & 0x7f);
            encoded >>= 7;
            index.push_back(static_cast<char>(byte | (encoded == 0 ? 0 : 0x80)));
        } while (encoded != 0);
        previous = length;
    }
    return index;
}

inline std::string finish_blob_file(std::string records, const std::string& index) {
    records += index;
    append_little_endian(&records, index.size(), 4);
    records.push_back('\1');
    return records;
}

inline std::string make_blob_file(const std::vector<std::optional<std::string>>& payloads) {
    std::string records;
    std::vector<int64_t> lengths;
    for (const auto& payload : payloads) {
        if (!payload) {
            lengths.push_back(-1);
            continue;
        }
        std::string record;
        append_little_endian(&record, 1481511375, 4);
        record += *payload;
        const auto record_length = static_cast<int64_t>(payload->size()) + 16;
        append_little_endian(&record, record_length, 8);
        const auto checksum = crc32(0, reinterpret_cast<const Bytef*>(record.data()),
                                    static_cast<uInt>(record.size()));
        append_little_endian(&record, checksum, 4);
        records += record;
        lengths.push_back(record_length);
    }
    return finish_blob_file(std::move(records), encode_record_lengths(lengths));
}

} // namespace doris::format::blob_test
