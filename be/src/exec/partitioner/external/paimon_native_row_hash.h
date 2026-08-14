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

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

namespace doris::paimon_native {

// Builds the byte representation used by Paimon BinaryRowWriter for the supported routing
// types. MemorySegment primitive access is native-endian.
class BinaryRowEncoder {
public:
    explicit BinaryRowEncoder(size_t arity);

    void reset();
    bool set_null(size_t position);
    bool write_boolean(size_t position, bool value);
    bool write_tinyint(size_t position, int8_t value);
    bool write_smallint(size_t position, int16_t value);
    bool write_int(size_t position, int32_t value);
    bool write_bigint(size_t position, int64_t value);
    bool write_float(size_t position, float value);
    bool write_double(size_t position, double value);
    bool write_string(size_t position, std::string_view utf8);
    bool write_binary(size_t position, std::string_view bytes);

    int32_t hash() const;
    const std::vector<uint8_t>& bytes() const { return _bytes; }

private:
    bool _valid_position(size_t position) const;
    size_t _field_offset(size_t position) const;
    void _set_null_bit(size_t position);
    void _ensure_capacity(size_t size);
    void _set_offset_and_size(size_t position, uint32_t offset, uint32_t size);
    bool _write_bytes(size_t position, std::string_view bytes);

    const size_t _arity;
    const size_t _null_bits_size;
    const size_t _fixed_size;
    size_t _cursor;
    std::vector<uint8_t> _bytes;
};

int32_t binary_row_hash(std::string_view bytes);

// Reproduces Paimon DefaultBucketFunction. nullopt means invalid metadata.
std::optional<uint32_t> default_bucket(int32_t bucket_key_hash, int32_t num_buckets);

// Reproduces ChannelComputer.select(partition, bucket, numChannels).
std::optional<uint32_t> fixed_bucket_channel(int32_t partition_hash, uint32_t bucket,
                                             uint32_t num_channels);

// Paimon BucketAssigner.computeAssigner for HASH_DYNAMIC input routing.
std::optional<uint32_t> dynamic_bucket_assigner_channel(int32_t partition_hash,
                                                        int32_t primary_key_hash,
                                                        uint32_t num_channels,
                                                        uint32_t num_assigners);

} // namespace doris::paimon_native
