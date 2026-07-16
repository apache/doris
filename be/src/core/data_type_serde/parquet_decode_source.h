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
#include <limits>
#include <vector>

#include "common/status.h"
#include "core/column/column.h"
#include "core/string_ref.h"

namespace cctz {
class time_zone;
} // namespace cctz

namespace doris {

// These enums deliberately do not expose parquet thrift classes to the core type system. The
// format reader translates the thrift metadata once when it creates a column reader.
enum class ParquetPhysicalType {
    BOOLEAN,
    INT32,
    INT64,
    INT96,
    FLOAT,
    DOUBLE,
    BYTE_ARRAY,
    FIXED_LEN_BYTE_ARRAY,
};

enum class ParquetValueEncoding {
    PLAIN,
    DICTIONARY,
    RLE,
    BIT_PACKED,
    DELTA_BINARY_PACKED,
    DELTA_LENGTH_BYTE_ARRAY,
    DELTA_BYTE_ARRAY,
    BYTE_STREAM_SPLIT,
};

enum class ParquetTimeUnit {
    UNKNOWN,
    MILLIS,
    MICROS,
    NANOS,
};

enum class ParquetLogicalType {
    NONE,
    STRING,
    DECIMAL,
    DATE,
    TIME,
    TIMESTAMP,
    INTEGER,
    UUID,
    FLOAT16,
};

// Immutable metadata required to turn one Parquet physical value into the selected Doris type.
// Encoding describes how the value source is read; logical annotations describe its meaning.
struct ParquetDecodeContext {
    ParquetPhysicalType physical_type = ParquetPhysicalType::INT32;
    ParquetValueEncoding encoding = ParquetValueEncoding::PLAIN;
    ParquetLogicalType logical_type = ParquetLogicalType::NONE;
    ParquetTimeUnit time_unit = ParquetTimeUnit::UNKNOWN;

    int32_t type_length = -1;
    int32_t decimal_precision = -1;
    int32_t decimal_scale = -1;
    int32_t logical_integer_bit_width = -1;
    bool logical_integer_is_signed = true;
    bool timestamp_is_adjusted_to_utc = false;
    bool logical_float16 = false;
    bool logical_uuid = false;
    bool dictionary_index_only = false;

    const cctz::time_zone* timezone = nullptr;
};

// A decoder may produce multiple contiguous spans for one request (for example delta encodings).
// Consumers are invoked per span, never per value, keeping virtual dispatch out of the row loop.
class ParquetFixedValueConsumer {
public:
    virtual ~ParquetFixedValueConsumer() = default;
    virtual Status consume(const uint8_t* values, size_t num_values, size_t value_width) = 0;
};

class ParquetBinaryValueConsumer {
public:
    virtual ~ParquetBinaryValueConsumer() = default;
    virtual Status consume(const StringRef* values, size_t num_values) = 0;
};

// Encoding decoders implement this interface. They own encoded-stream cursors and dictionary
// storage, but they never know the destination Doris column type. DataTypeSerDe owns the consumer
// and therefore the physical/logical-to-Doris conversion.
class ParquetDecodeSource {
public:
    virtual ~ParquetDecodeSource() = default;

    virtual Status decode_fixed_values(size_t num_values, ParquetFixedValueConsumer& consumer) = 0;
    virtual Status decode_binary_values(size_t num_values,
                                        ParquetBinaryValueConsumer& consumer) = 0;
    virtual Status skip_values(size_t num_values) = 0;

    virtual bool has_dictionary() const { return false; }
    virtual uint64_t dictionary_generation() const { return 0; }
    virtual size_t dictionary_size() const { return 0; }
    virtual Status decode_dictionary(ParquetFixedValueConsumer& fixed_consumer,
                                     ParquetBinaryValueConsumer& binary_consumer) {
        return Status::NotSupported("Parquet dictionary is not supported by this decoder");
    }
    virtual Status decode_dictionary_indices(size_t num_values, std::vector<uint32_t>* indices) {
        return Status::NotSupported("Parquet dictionary indices are not supported by this decoder");
    }
};

// Dictionary values are materialized once into the selected Doris type. The state belongs to a
// column reader rather than DataTypeSerDe because a SerDe instance can be shared by many files.
struct ParquetMaterializationState {
    MutableColumnPtr typed_dictionary;
    std::vector<uint32_t> dictionary_indices;
    uint64_t dictionary_generation = std::numeric_limits<uint64_t>::max();

    void reset_dictionary() {
        typed_dictionary.reset();
        dictionary_indices.clear();
        dictionary_generation = std::numeric_limits<uint64_t>::max();
    }
};

} // namespace doris
