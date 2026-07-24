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

// Dictionary decoders publish validated IDs without knowing the destination Doris type. A
// cache-resident dictionary can therefore fuse RLE decode with target-column gathering, while a
// large sparse dictionary can still choose a compact ID buffer before random dictionary access.
class ParquetDictionaryValueConsumer {
public:
    virtual ~ParquetDictionaryValueConsumer() = default;
    virtual Status consume_indices(const uint32_t* indices, size_t num_values) = 0;
    virtual Status consume_repeated(uint32_t index, size_t num_values) {
        std::vector<uint32_t> indices(num_values, index);
        return consume_indices(indices.data(), indices.size());
    }
};

// Physical value ranges selected from one page-bounded decode request. Definition-level NULLs do
// not occupy the encoded value stream, so the native ColumnReader merges the logical selection
// with definition levels before constructing this plan. The SerDe sees only selected non-NULL
// payload, while the reader restores selected NULL slots after the compact decode. Ranges are
// sorted, disjoint, and expressed in the physical value stream's coordinate space.
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
    virtual Status decode_dictionary_values(size_t num_values,
                                            ParquetDictionaryValueConsumer& consumer) {
        std::vector<uint32_t> indices;
        RETURN_IF_ERROR(decode_dictionary_indices(num_values, &indices));
        return consumer.consume_indices(indices.data(), indices.size());
    }
    virtual Status decode_selected_dictionary_values(const ParquetSelection& selection,
                                                     ParquetDictionaryValueConsumer& consumer) {
        std::vector<uint32_t> indices;
        RETURN_IF_ERROR(decode_selected_dictionary_indices(selection, &indices));
        return consumer.consume_indices(indices.data(), indices.size());
    }
    virtual bool prefer_dictionary_index_materialization(size_t dictionary_bytes) const {
        return false;
    }
};

enum class ParquetDictionaryMaterializationStrategy : uint8_t { DIRECT, INDICES };

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
    bool capturing_dictionary_conversion_failures = false;
    bool dictionary_has_conversion_failures = false;
    size_t dictionary_failure_scan_rows = 0;
    ParquetDictionaryMaterializationStrategy dictionary_materialization_strategy =
            ParquetDictionaryMaterializationStrategy::INDICES;

    void reset_dictionary() {
        typed_dictionary.reset();
        dictionary_indices.clear();
        dictionary_conversion_failures.clear();
        capturing_dictionary_conversion_failures = false;
        dictionary_has_conversion_failures = false;
        dictionary_failure_scan_rows = 0;
        dictionary_generation = std::numeric_limits<uint64_t>::max();
    }

    bool can_insert_null_on_conversion_failure() const {
        return conversion_failure_null_map != nullptr &&
               (!enable_strict_mode || capturing_dictionary_conversion_failures);
    }

    bool mark_conversion_failure(size_t output_row) {
        if (!can_insert_null_on_conversion_failure()) {
            return false;
        }
        DORIS_CHECK_LT(output_row, conversion_failure_null_map->size());
        (*conversion_failure_null_map)[output_row] = 1;
        if (capturing_dictionary_conversion_failures) {
            dictionary_has_conversion_failures = true;
        }
        return true;
    }

    IColumn::Filter* begin_dictionary_conversion(size_t dictionary_size) {
        auto* output_null_map = conversion_failure_null_map;
        dictionary_conversion_failures.resize_fill(dictionary_size, 0);
        dictionary_has_conversion_failures = false;
        conversion_failure_null_map = &dictionary_conversion_failures;
        capturing_dictionary_conversion_failures = true;
        return output_null_map;
    }

    void end_dictionary_conversion(IColumn::Filter* output_null_map) {
        conversion_failure_null_map = output_null_map;
        capturing_dictionary_conversion_failures = false;
    }

    Status materialize_dictionary(IColumn& column) {
        const size_t old_size = column.size();
        dictionary_failure_scan_rows = 0;
        if (UNLIKELY(dictionary_has_conversion_failures)) {
            const bool insert_failure_as_null = can_insert_null_on_conversion_failure();
            if (!insert_failure_as_null) {
                for (size_t row = 0; row < dictionary_indices.size(); ++row) {
                    ++dictionary_failure_scan_rows;
                    const auto dictionary_id = dictionary_indices[row];
                    DORIS_CHECK_LT(dictionary_id, dictionary_conversion_failures.size());
                    if (dictionary_conversion_failures[dictionary_id] == 0) {
                        continue;
                    }
                    // A malformed dictionary entry is irrelevant until a selected row references
                    // it; failing while building the dictionary would reject valid pages.
                    return Status::DataQualityError(
                            "Parquet dictionary entry {} cannot be converted to the target type",
                            dictionary_id);
                }
            }
        }
        column.insert_indices_from(*typed_dictionary, dictionary_indices.data(),
                                   dictionary_indices.data() + dictionary_indices.size());
        if (UNLIKELY(dictionary_has_conversion_failures &&
                     can_insert_null_on_conversion_failure())) {
            for (size_t row = 0; row < dictionary_indices.size(); ++row) {
                ++dictionary_failure_scan_rows;
                const auto dictionary_id = dictionary_indices[row];
                DORIS_CHECK_LT(dictionary_id, dictionary_conversion_failures.size());
                if (dictionary_conversion_failures[dictionary_id] != 0) {
                    mark_conversion_failure(old_size + row);
                }
            }
        }
        return Status::OK();
    }

    Status materialize_dictionary(IColumn& column, ParquetDecodeSource& source, size_t num_values) {
        DORIS_CHECK(typed_dictionary);
        if (UNLIKELY(dictionary_has_conversion_failures) ||
            source.prefer_dictionary_index_materialization(typed_dictionary->byte_size())) {
            dictionary_materialization_strategy = ParquetDictionaryMaterializationStrategy::INDICES;
            RETURN_IF_ERROR(source.decode_dictionary_indices(num_values, &dictionary_indices));
            DORIS_CHECK_EQ(dictionary_indices.size(), num_values);
            return materialize_dictionary(column);
        }

        class ColumnConsumer final : public ParquetDictionaryValueConsumer {
        public:
            ColumnConsumer(IColumn& destination, const IColumn& dictionary)
                    : _destination(destination), _dictionary(dictionary) {}

            Status consume_indices(const uint32_t* indices, size_t num_values) override {
                _destination.insert_indices_from(_dictionary, indices, indices + num_values);
                return Status::OK();
            }

            Status consume_repeated(uint32_t index, size_t num_values) override {
                _destination.insert_many_from(_dictionary, index, num_values);
                return Status::OK();
            }

        private:
            IColumn& _destination;
            const IColumn& _dictionary;
        } consumer(column, *typed_dictionary);

        dictionary_materialization_strategy = ParquetDictionaryMaterializationStrategy::DIRECT;
        const size_t old_size = column.size();
        const Status status = source.decode_dictionary_values(num_values, consumer);
        if (!status.ok()) {
            // Streaming direct gather may discover a corrupt late ID after earlier valid runs;
            // restore the same all-or-nothing column invariant as the index-buffer path.
            column.resize(old_size);
        }
        return status;
    }
};

} // namespace doris
