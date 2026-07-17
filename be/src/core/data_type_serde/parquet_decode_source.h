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

#include "common/check.h"
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

struct ParquetSelectionRange {
    size_t first = 0;
    size_t count = 0;
};

// A decoder may produce multiple contiguous spans for one request (for example delta encodings).
// Consumers are invoked per span, never per value, keeping virtual dispatch out of the row loop.
class ParquetFixedValueConsumer {
public:
    virtual ~ParquetFixedValueConsumer() = default;
    virtual Status consume(const uint8_t* values, size_t num_values, size_t value_width) = 0;
    virtual Status consume_selected(const uint8_t* values, size_t value_width,
                                    const std::vector<ParquetSelectionRange>& ranges) {
        for (const auto& range : ranges) {
            RETURN_IF_ERROR(consume(values + range.first * value_width, range.count, value_width));
        }
        return Status::OK();
    }
};

class ParquetBinaryValueConsumer {
public:
    virtual ~ParquetBinaryValueConsumer() = default;
    virtual Status consume(const StringRef* values, size_t num_values) = 0;

    // PLAIN BYTE_ARRAY decoders already have to parse every length prefix. Publish the parsed
    // source and destination offsets so string columns do not rebuild an equally large StringRef
    // array and rescan all lengths before copying. Spans are expressed in output coordinates and
    // preserve adjacent surviving runs for consumers that can amortize range setup.
    virtual Status consume_plain_byte_array(const char* encoded_data,
                                            const uint32_t* payload_offsets,
                                            const uint32_t* value_offsets, size_t num_values,
                                            const std::vector<ParquetSelectionRange>& value_spans) {
        std::vector<StringRef> values;
        values.reserve(num_values);
        for (size_t row = 0; row < num_values; ++row) {
            values.emplace_back(encoded_data + payload_offsets[row],
                                value_offsets[row + 1] - value_offsets[row]);
        }
        return consume(values.data(), values.size());
    }
};

// Physical value ranges selected from one page-bounded decode request. Definition-level NULLs are
// intentionally excluded: the native ColumnReader uses this plan only when the batch has no NULL
// leaf slots, so selected values can be appended in one pass without a temporary nullable column.
// Ranges are sorted, disjoint, and expressed in the physical value stream's coordinate space.
struct ParquetSelection {
    size_t total_values = 0;
    size_t selected_values = 0;
    std::vector<ParquetSelectionRange> ranges;
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

    // Batch-level sparse decode. The default implementation preserves every encoding's cursor
    // semantics while moving SerDe dispatch and consumer construction out of the selection-run
    // loop. Decoders with cheap random access or batch decode override these methods to remove the
    // remaining per-range virtual calls as well.
    virtual Status decode_selected_fixed_values(const ParquetSelection& selection,
                                                ParquetFixedValueConsumer& consumer) {
        size_t cursor = 0;
        for (const auto& range : selection.ranges) {
            DORIS_CHECK(range.first >= cursor);
            DORIS_CHECK(range.first + range.count <= selection.total_values);
            RETURN_IF_ERROR(skip_values(range.first - cursor));
            RETURN_IF_ERROR(decode_fixed_values(range.count, consumer));
            cursor = range.first + range.count;
        }
        return skip_values(selection.total_values - cursor);
    }
    virtual Status decode_selected_binary_values(const ParquetSelection& selection,
                                                 ParquetBinaryValueConsumer& consumer) {
        size_t cursor = 0;
        for (const auto& range : selection.ranges) {
            DORIS_CHECK(range.first >= cursor);
            DORIS_CHECK(range.first + range.count <= selection.total_values);
            RETURN_IF_ERROR(skip_values(range.first - cursor));
            RETURN_IF_ERROR(decode_binary_values(range.count, consumer));
            cursor = range.first + range.count;
        }
        return skip_values(selection.total_values - cursor);
    }

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
    virtual Status decode_selected_dictionary_indices(const ParquetSelection& selection,
                                                      std::vector<uint32_t>* indices) {
        DORIS_CHECK(indices != nullptr);
        indices->clear();
        indices->reserve(selection.selected_values);
        std::vector<uint32_t> range_indices;
        size_t cursor = 0;
        for (const auto& range : selection.ranges) {
            DORIS_CHECK(range.first >= cursor);
            DORIS_CHECK(range.first + range.count <= selection.total_values);
            RETURN_IF_ERROR(skip_values(range.first - cursor));
            RETURN_IF_ERROR(decode_dictionary_indices(range.count, &range_indices));
            indices->insert(indices->end(), range_indices.begin(), range_indices.end());
            cursor = range.first + range.count;
        }
        RETURN_IF_ERROR(skip_values(selection.total_values - cursor));
        DORIS_CHECK_EQ(indices->size(), selection.selected_values);
        return Status::OK();
    }
};

// Dictionary values are materialized once into the selected Doris type. The state belongs to a
// column reader rather than DataTypeSerDe because a SerDe instance can be shared by many files.
struct ParquetMaterializationState {
    MutableColumnPtr typed_dictionary;
    std::vector<uint32_t> dictionary_indices;
    ParquetSelection selection;
    uint64_t dictionary_generation = std::numeric_limits<uint64_t>::max();
    bool enable_strict_mode = false;
    IColumn::Filter* conversion_failure_null_map = nullptr;
    IColumn::Filter dictionary_conversion_failures;

    void reset_dictionary() {
        typed_dictionary.reset();
        dictionary_indices.clear();
        dictionary_conversion_failures.clear();
        dictionary_generation = std::numeric_limits<uint64_t>::max();
    }

    bool can_insert_null_on_conversion_failure() const {
        return !enable_strict_mode && conversion_failure_null_map != nullptr;
    }

    bool mark_conversion_failure(size_t output_row) {
        if (!can_insert_null_on_conversion_failure()) {
            return false;
        }
        DORIS_CHECK_LT(output_row, conversion_failure_null_map->size());
        (*conversion_failure_null_map)[output_row] = 1;
        return true;
    }

    IColumn::Filter* begin_dictionary_conversion(size_t dictionary_size) {
        auto* output_null_map = conversion_failure_null_map;
        dictionary_conversion_failures.clear();
        if (can_insert_null_on_conversion_failure()) {
            // Only nullable non-strict outputs may absorb a bad dictionary entry. Redirecting a
            // non-nullable decode here would silently turn a required error into a default value.
            dictionary_conversion_failures.resize_fill(dictionary_size, 0);
            conversion_failure_null_map = &dictionary_conversion_failures;
        }
        return output_null_map;
    }

    void end_dictionary_conversion(IColumn::Filter* output_null_map) {
        conversion_failure_null_map = output_null_map;
    }
};

} // namespace doris
