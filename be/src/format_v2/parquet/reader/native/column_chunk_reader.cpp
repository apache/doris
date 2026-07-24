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

#include "format_v2/parquet/reader/native/column_chunk_reader.h"

#include <cctz/time_zone.h>
#include <gen_cpp/parquet_types.h>
#include <glog/logging.h>
#include <parquet/metadata.h>
#include <snappy.h>
#include <string.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "core/column/column.h"
#include "core/column/column_decimal.h"
#include "core/column/column_dictionary.h"
#include "core/column/column_varbinary.h"
#include "core/column/column_vector.h"
#include "core/custom_allocator.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/data_type_serde/parquet_timestamp.h"
#include "exprs/vexpr.h"
#include "format_v2/parquet/native_schema_desc.h"
#include "format_v2/parquet/reader/native/decoder.h"
#include "format_v2/parquet/reader/native/level_decoder.h"
#include "format_v2/parquet/reader/native/page_reader.h"
#include "io/fs/buffered_reader.h"
#include "runtime/runtime_profile.h"
#include "storage/cache/page_cache.h"
#include "util/bit_util.h"
#include "util/block_compression.h"
#include "util/cpu_info.h"
#include "util/simd/parquet_kernels.h"
#include "util/unaligned.h"

namespace cctz {
class time_zone;
} // namespace cctz
namespace doris {
namespace io {
class BufferedStreamReader;
struct IOContext;
} // namespace io
} // namespace doris

namespace doris::format::parquet::native {

bool can_prepare_page_cache_payload(bool session_cache_enabled, bool storage_cache_disabled,
                                    bool cache_available, bool header_available) {
    return session_cache_enabled && !storage_cache_disabled && cache_available && header_available;
}

Status validate_uncompressed_page_sizes(const tparquet::PageHeader& header,
                                        tparquet::CompressionCodec::type codec,
                                        bool data_page_v2_always_compressed) {
    const bool is_v2 = header.__isset.data_page_header_v2;
    const bool page_is_compressed =
            codec != tparquet::CompressionCodec::UNCOMPRESSED &&
            (!is_v2 || header.data_page_header_v2.is_compressed || data_page_v2_always_compressed);
    if (!page_is_compressed &&
        UNLIKELY(header.compressed_page_size != header.uncompressed_page_size)) {
        // An uncompressed payload has one physical representation, so accepting two lengths makes
        // cold reads and decompressed cache hits consume different byte boundaries.
        return Status::Corruption(
                "Uncompressed Parquet page sizes differ: compressed={}, uncompressed={}",
                header.compressed_page_size, header.uncompressed_page_size);
    }
    return Status::OK();
}

Status validate_fixed_width_page_size(const tparquet::PageHeader& header, int32_t type_length,
                                      level_t max_rep_level, level_t max_def_level,
                                      bool schema_is_required) {
    if (type_length <= 0) {
        return Status::OK();
    }
    const bool is_v2 = header.__isset.data_page_header_v2;
    if (!is_v2 && !header.__isset.data_page_header) {
        return Status::OK();
    }
    const auto encoding =
            is_v2 ? header.data_page_header_v2.encoding : header.data_page_header.encoding;
    if (encoding != tparquet::Encoding::PLAIN &&
        encoding != tparquet::Encoding::BYTE_STREAM_SPLIT) {
        return Status::OK();
    }
    int32_t num_physical_values = 0;
    int64_t level_bytes = 0;
    if (is_v2) {
        const auto& page = header.data_page_header_v2;
        if (UNLIKELY(page.num_values < 0 || page.num_nulls < 0 ||
                     page.num_nulls > page.num_values || page.repetition_levels_byte_length < 0 ||
                     page.definition_levels_byte_length < 0)) {
            return Status::Corruption("Parquet data page v2 has invalid value or level counts");
        }
        num_physical_values = page.num_values - page.num_nulls;
        level_bytes = static_cast<int64_t>(page.repetition_levels_byte_length) +
                      page.definition_levels_byte_length;
    } else {
        if (max_rep_level != 0 || max_def_level != 0 || !schema_is_required) {
            return Status::OK();
        }
        num_physical_values = header.data_page_header.num_values;
    }
    if (level_bytes > std::numeric_limits<int32_t>::max() || num_physical_values < 0 ||
        static_cast<uint64_t>(num_physical_values) >
                (static_cast<uint64_t>(std::numeric_limits<int32_t>::max()) - level_bytes) /
                        static_cast<uint32_t>(type_length)) {
        return Status::Corruption("Parquet fixed-width PLAIN page byte size overflows");
    }
    const int64_t expected = level_bytes + static_cast<int64_t>(num_physical_values) * type_length;
    if (UNLIKELY(header.uncompressed_page_size != expected)) {
        // V2 exposes null and level extents separately, so fixed-width payload size is known before
        // decompression even for optional columns and must gate attacker-controlled allocation.
        return Status::Corruption("Parquet fixed-width page has {} uncompressed bytes, expected {}",
                                  header.uncompressed_page_size, expected);
    }
    return Status::OK();
}

Status validate_dictionary_page_size(const tparquet::PageHeader& header, int32_t type_length) {
    DORIS_CHECK(header.__isset.dictionary_page_header);
    const int32_t num_values = header.dictionary_page_header.num_values;
    if (UNLIKELY(num_values < 0 || (num_values == 0 && header.uncompressed_page_size != 0))) {
        // An empty dictionary owns no payload; validate before allocating from its untrusted size.
        return Status::Corruption("Parquet dictionary has {} values and {} uncompressed bytes",
                                  num_values, header.uncompressed_page_size);
    }
    if (type_length > 0) {
        if (UNLIKELY(static_cast<uint64_t>(num_values) >
                     static_cast<uint64_t>(std::numeric_limits<int32_t>::max()) /
                             static_cast<uint32_t>(type_length))) {
            return Status::Corruption("Parquet fixed-width dictionary byte size overflows");
        }
        const int64_t expected = static_cast<int64_t>(num_values) * type_length;
        if (UNLIKELY(header.uncompressed_page_size != expected)) {
            // Fixed-width dictionaries have no level section, so reject forged extents before the
            // decoder allocates storage based on the untrusted page header.
            return Status::Corruption(
                    "Parquet fixed-width dictionary has {} uncompressed bytes, expected {}",
                    header.uncompressed_page_size, expected);
        }
    }
    return Status::OK();
}

Status validate_compressed_page_size(tparquet::CompressionCodec::type codec,
                                     const Slice& compressed_data,
                                     size_t expected_uncompressed_size) {
    if (codec != tparquet::CompressionCodec::SNAPPY) {
        return Status::OK();
    }
    size_t actual_uncompressed_size = 0;
    if (UNLIKELY(!snappy::GetUncompressedLength(compressed_data.data, compressed_data.size,
                                                &actual_uncompressed_size))) {
        return Status::Corruption("Invalid Snappy-compressed Parquet page");
    }
    if (UNLIKELY(actual_uncompressed_size != expected_uncompressed_size)) {
        // Snappy exposes its decoded extent without an output buffer. Check it before trusting the
        // page header so malformed variable-width pages cannot force a header-sized allocation.
        return Status::Corruption("Snappy Parquet page expands to {} bytes, expected {}",
                                  actual_uncompressed_size, expected_uncompressed_size);
    }
    return Status::OK();
}

ParquetReaderCompat parquet_reader_compat(const std::string& created_by) {
    if (created_by.empty()) {
        return {};
    }
    const ::parquet::ApplicationVersion version(created_by);
    return {.parquet_816_padding =
                    version.VersionLt(::parquet::ApplicationVersion::PARQUET_816_FIXED_VERSION()),
            .data_page_v2_always_compressed = version.VersionLt(
                    ::parquet::ApplicationVersion::PARQUET_CPP_10353_FIXED_VERSION())};
}

Status compute_column_chunk_range(const tparquet::ColumnMetaData& metadata, size_t file_size,
                                  bool parquet_816_padding, ColumnChunkRange* range) {
    DORIS_CHECK(range != nullptr);
    int64_t start = metadata.data_page_offset;
    if (metadata.__isset.dictionary_page_offset && metadata.dictionary_page_offset > 0 &&
        metadata.dictionary_page_offset < start) {
        // Some writers use dictionary_page_offset=0 as an absence sentinel. Range validation must
        // follow has_dict_page() or the native reader starts at the Parquet magic bytes.
        start = metadata.dictionary_page_offset;
    }
    const int64_t length = metadata.total_compressed_size;
    if (UNLIKELY(start < 0 || length < 0)) {
        return Status::Corruption("Parquet column chunk has a negative offset or length");
    }
    const uint64_t unsigned_start = static_cast<uint64_t>(start);
    const uint64_t unsigned_length = static_cast<uint64_t>(length);
    if (UNLIKELY(unsigned_start > file_size || unsigned_length > file_size - unsigned_start)) {
        // Thrift range fields are signed and untrusted; validate before converting them to the
        // unsigned stream-reader coordinates so overflow cannot wrap back into the file.
        return Status::Corruption("Parquet column chunk [{}, {}) exceeds file size {}", start,
                                  unsigned_start + unsigned_length, file_size);
    }
    size_t bounded_length = static_cast<size_t>(unsigned_length);
    if (parquet_816_padding) {
        // parquet-mr before PARQUET-816 under-reported the chunk by up to 100 bytes. Padding stays
        // file-bounded and is only enabled for the affected writer versions.
        bounded_length += std::min<size_t>(100, file_size - unsigned_start - unsigned_length);
    }
    range->offset = static_cast<size_t>(unsigned_start);
    range->length = bounded_length;
    return Status::OK();
}

bool validate_offset_index(const tparquet::OffsetIndex& index, const ColumnChunkRange& chunk_range,
                           int64_t data_page_offset, int64_t row_count) {
    if (index.page_locations.empty() || data_page_offset < 0 || row_count < 0 ||
        index.page_locations.front().first_row_index != 0 ||
        index.page_locations.front().offset != data_page_offset ||
        chunk_range.length > std::numeric_limits<size_t>::max() - chunk_range.offset) {
        return false;
    }
    // Row indexes alone cannot detect a uniformly shifted OffsetIndex. Anchor its first location
    // to the owning metadata so page-to-row mapping cannot silently move by one physical page.
    const uint64_t chunk_begin = chunk_range.offset;
    const uint64_t chunk_end = chunk_begin + chunk_range.length;
    uint64_t previous_end = chunk_begin;
    int64_t previous_row = -1;
    for (const auto& location : index.page_locations) {
        if (location.first_row_index <= previous_row || location.first_row_index >= row_count ||
            location.offset < 0 || location.compressed_page_size <= 0) {
            return false;
        }
        const uint64_t begin = static_cast<uint64_t>(location.offset);
        const uint64_t size = static_cast<uint64_t>(location.compressed_page_size);
        if (begin < chunk_begin || begin < previous_end || begin > chunk_end ||
            size > chunk_end - begin) {
            return false;
        }
        previous_row = location.first_row_index;
        previous_end = begin + size;
    }
    return true;
}

namespace {

class EmptyValueSectionDecoder final : public Decoder {
public:
    Status skip_values(size_t num_values) override {
        if (UNLIKELY(num_values != 0)) {
            return Status::Corruption(
                    "Parquet definition levels require {} values from an empty value section",
                    num_values);
        }
        return Status::OK();
    }
};

Status append_v2_int96_datetime(ColumnDateTimeV2::Container& data,
                                const ParquetInt96Timestamp& value,
                                const cctz::time_zone& timezone) {
    static constexpr int32_t JULIAN_EPOCH_OFFSET_DAYS = 2440588;
    static constexpr int64_t MICROS_PER_DAY = 86400000000LL;
    static constexpr int64_t MICROS_PER_SECOND = 1000000LL;

    // Arrow normalized out-of-day INT96 nanos before the native V2 path replaced it. Preserve that
    // legacy-writer compatibility here; rejecting the carrier loses valid pre-epoch/year-0 values
    // already accepted by Doris external-table scans.
    const __int128 days = static_cast<__int128>(value.julian_day) - JULIAN_EPOCH_OFFSET_DAYS;
    // Truncate the signed nanos field before day normalization. Flooring a negative value first
    // changes the historical result by one microsecond.
    const __int128 timestamp_micros = days * MICROS_PER_DAY + value.nanos_of_day / 1000;
    if (timestamp_micros < std::numeric_limits<int64_t>::min() ||
        timestamp_micros > std::numeric_limits<int64_t>::max()) {
        return Status::DataQualityError("Parquet INT96 timestamp overflows microseconds");
    }

    const int64_t micros = static_cast<int64_t>(timestamp_micros);
    int64_t epoch_seconds = micros / MICROS_PER_SECOND;
    int64_t micros_of_second = micros % MICROS_PER_SECOND;
    if (micros_of_second < 0) {
        micros_of_second += MICROS_PER_SECOND;
        --epoch_seconds;
    }
    DateV2Value<DateTimeV2ValueType> datetime;
    datetime.from_unixtime(epoch_seconds, timezone);
    datetime.set_microsecond(static_cast<uint32_t>(micros_of_second));
    if (!datetime.is_valid_date()) {
        return Status::DataQualityError("Parquet INT96 timestamp is outside the Doris range");
    }
    data.push_back(datetime);
    return Status::OK();
}

class V2Int96DateTimeConsumer final : public ParquetFixedValueConsumer {
public:
    V2Int96DateTimeConsumer(IColumn& column, const ParquetDecodeContext& context,
                            ParquetMaterializationState* state)
            : _data(assert_cast<ColumnDateTimeV2&>(column).get_data()), _state(state) {
        static const auto utc = cctz::utc_time_zone();
        _timezone = context.timezone == nullptr ? &utc : context.timezone;
    }

    Status consume(const uint8_t* values, size_t num_values, size_t value_width) override {
        DORIS_CHECK_EQ(value_width, sizeof(ParquetInt96Timestamp));
        const size_t old_size = _data.size();
        for (size_t row = 0; row < num_values; ++row) {
            const auto value = unaligned_load<ParquetInt96Timestamp>(
                    values + row * sizeof(ParquetInt96Timestamp));
            const auto status = append_v2_int96_datetime(_data, value, *_timezone);
            if (!status.ok()) {
                if (_state != nullptr && _state->mark_conversion_failure(_data.size())) {
                    _data.emplace_back();
                    continue;
                }
                _data.resize(old_size);
                return status;
            }
        }
        return Status::OK();
    }

private:
    ColumnDateTimeV2::Container& _data;
    ParquetMaterializationState* _state;
    const cctz::time_zone* _timezone = nullptr;
};

class RejectV2Int96BinaryConsumer final : public ParquetBinaryValueConsumer {
public:
    Status consume(const StringRef*, size_t) override {
        return Status::NotSupported("INT96 cannot be decoded from binary Parquet values");
    }
};

Status read_v2_int96_datetime(IColumn& column, ParquetDecodeSource& source,
                              const ParquetDecodeContext& context, size_t num_values,
                              ParquetMaterializationState& state) {
    V2Int96DateTimeConsumer consumer(column, context, &state);
    if (context.encoding != ParquetValueEncoding::DICTIONARY) {
        return source.decode_fixed_values(num_values, consumer);
    }
    if (state.dictionary_generation != source.dictionary_generation()) {
        state.typed_dictionary = column.clone_empty();
        auto* output_null_map = state.begin_dictionary_conversion(source.dictionary_size());
        V2Int96DateTimeConsumer dictionary_consumer(*state.typed_dictionary, context, &state);
        RejectV2Int96BinaryConsumer binary_consumer;
        const auto dictionary_status =
                source.decode_dictionary(dictionary_consumer, binary_consumer);
        state.end_dictionary_conversion(output_null_map);
        RETURN_IF_ERROR(dictionary_status);
        DORIS_CHECK_EQ(state.typed_dictionary->size(), source.dictionary_size());
        state.dictionary_generation = source.dictionary_generation();
    }
    RETURN_IF_ERROR(source.decode_dictionary_indices(num_values, &state.dictionary_indices));
    DORIS_CHECK_EQ(state.dictionary_indices.size(), num_values);
    return state.materialize_dictionary(column);
}

Status read_native_or_serde(IColumn& column, const DataTypeSerDe& serde,
                            ParquetDecodeSource& source, const ParquetDecodeContext& context,
                            size_t num_values, ParquetMaterializationState& state) {
    if (context.dictionary_index_only) {
        if (context.encoding != ParquetValueEncoding::DICTIONARY) {
            return Status::IOError("Dictionary filter requested for a non-dictionary page");
        }
        auto* output = check_and_get_column<ColumnInt32>(&column);
        if (output == nullptr) {
            return Status::InternalError("Dictionary indices require an INT32 output column");
        }
        // Dictionary IDs have the same RLE/bit-packed representation for every physical type.
        // Decode them before dispatching to a typed SerDe; otherwise a fixed-width SerDe treats
        // the IDs as values and the row filter indexes its bitmap with materialized data.
        RETURN_IF_ERROR(source.decode_dictionary_indices(num_values, &state.dictionary_indices));
        DORIS_CHECK_EQ(state.dictionary_indices.size(), num_values);
        const size_t old_size = output->size();
        auto& indices = output->get_data();
        indices.resize(old_size + num_values);
        for (size_t row = 0; row < num_values; ++row) {
            if (UNLIKELY(state.dictionary_indices[row] >
                         static_cast<uint32_t>(std::numeric_limits<int32_t>::max()))) {
                indices.resize(old_size);
                return Status::Corruption("Parquet dictionary index {} exceeds INT32",
                                          state.dictionary_indices[row]);
            }
            indices[old_size + row] = static_cast<int32_t>(state.dictionary_indices[row]);
        }
        return Status::OK();
    }
    if (context.physical_type == ParquetPhysicalType::INT96 &&
        check_and_get_column<ColumnDateTimeV2>(&column) != nullptr) {
        return read_v2_int96_datetime(column, source, context, num_values, state);
    }
    return serde.read_column_from_parquet(column, source, context, num_values, state);
}

Status translate_value_encoding(tparquet::Encoding::type encoding,
                                ParquetValueEncoding* translated) {
    DORIS_CHECK(translated != nullptr);
    switch (encoding) {
    case tparquet::Encoding::PLAIN:
        *translated = ParquetValueEncoding::PLAIN;
        return Status::OK();
    case tparquet::Encoding::RLE_DICTIONARY:
    case tparquet::Encoding::PLAIN_DICTIONARY:
        *translated = ParquetValueEncoding::DICTIONARY;
        return Status::OK();
    case tparquet::Encoding::RLE:
        *translated = ParquetValueEncoding::RLE;
        return Status::OK();
    case tparquet::Encoding::BIT_PACKED:
        *translated = ParquetValueEncoding::BIT_PACKED;
        return Status::OK();
    case tparquet::Encoding::DELTA_BINARY_PACKED:
        *translated = ParquetValueEncoding::DELTA_BINARY_PACKED;
        return Status::OK();
    case tparquet::Encoding::DELTA_LENGTH_BYTE_ARRAY:
        *translated = ParquetValueEncoding::DELTA_LENGTH_BYTE_ARRAY;
        return Status::OK();
    case tparquet::Encoding::DELTA_BYTE_ARRAY:
        *translated = ParquetValueEncoding::DELTA_BYTE_ARRAY;
        return Status::OK();
    case tparquet::Encoding::BYTE_STREAM_SPLIT:
        *translated = ParquetValueEncoding::BYTE_STREAM_SPLIT;
        return Status::OK();
    default:
        return Status::NotSupported("Unsupported Parquet encoding {}",
                                    tparquet::to_string(encoding));
    }
}

template <bool HAS_FILTER>
Status decode_selected_values(IColumn& column, const DataTypeSerDe& serde, Decoder& decoder,
                              const ParquetDecodeContext& context,
                              ParquetMaterializationState& state, ColumnSelectVector& select_vector,
                              int64_t* materialization_time) {
    SCOPED_RAW_TIMER(materialization_time);
    ColumnSelectVector::DataReadType read_type;
    while (const size_t run_length = select_vector.get_next_run<HAS_FILTER>(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT:
            RETURN_IF_ERROR(
                    read_native_or_serde(column, serde, decoder, context, run_length, state));
            break;
        case ColumnSelectVector::NULL_DATA:
            column.insert_many_defaults(run_length);
            break;
        case ColumnSelectVector::FILTERED_CONTENT:
            RETURN_IF_ERROR(decoder.skip_values(run_length));
            break;
        case ColumnSelectVector::FILTERED_NULL:
            break;
        }
    }
    return Status::OK();
}

// Presents one sparse page request as an ordinary sequential source to DataTypeSerDe. SerDe is
// entered once per page fragment; the concrete decoder decides whether to gather selected spans,
// batch-decode and compact, or use the cursor-preserving range fallback.
class SelectedDecodeSource final : public ParquetDecodeSource {
public:
    SelectedDecodeSource(Decoder& decoder, const ParquetSelection& selection)
            : _decoder(decoder), _selection(selection) {}

    Status decode_fixed_values(size_t num_values, ParquetFixedValueConsumer& consumer) override {
        DORIS_CHECK_EQ(num_values, _selection.selected_values);
        return _decoder.decode_selected_fixed_values(_selection, consumer);
    }

    Status decode_binary_values(size_t num_values, ParquetBinaryValueConsumer& consumer) override {
        DORIS_CHECK_EQ(num_values, _selection.selected_values);
        return _decoder.decode_selected_binary_values(_selection, consumer);
    }

    Status skip_values(size_t num_values) override {
        return Status::NotSupported("Selected Parquet source cannot be skipped, values={}",
                                    num_values);
    }

    bool has_dictionary() const override { return _decoder.has_dictionary(); }
    uint64_t dictionary_generation() const override { return _decoder.dictionary_generation(); }
    size_t dictionary_size() const override { return _decoder.dictionary_size(); }

    Status decode_dictionary(ParquetFixedValueConsumer& fixed_consumer,
                             ParquetBinaryValueConsumer& binary_consumer) override {
        return _decoder.decode_dictionary(fixed_consumer, binary_consumer);
    }

    Status decode_dictionary_indices(size_t num_values, std::vector<uint32_t>* indices) override {
        DORIS_CHECK_EQ(num_values, _selection.selected_values);
        return _decoder.decode_selected_dictionary_indices(_selection, indices);
    }

    Status decode_dictionary_values(size_t num_values,
                                    ParquetDictionaryValueConsumer& consumer) override {
        DORIS_CHECK_EQ(num_values, _selection.selected_values);
        return _decoder.decode_selected_dictionary_values(_selection, consumer);
    }

    bool prefer_dictionary_index_materialization(size_t dictionary_bytes) const override {
        // Avoid touching a dictionary larger than L2 for rows already removed by sparse selection.
        // Cache-resident or dense batches instead fuse RLE decode and gather, eliminating the
        // page-sized intermediate ID vector.
        return _selection.selected_values < _selection.total_values &&
               dictionary_bytes > static_cast<size_t>(CpuInfo::get_l2_cache_size());
    }

private:
    Decoder& _decoder;
    const ParquetSelection& _selection;
};

Status decode_selected_non_null_values(IColumn& column, const DataTypeSerDe& serde,
                                       Decoder& decoder, const ParquetDecodeContext& context,
                                       ParquetMaterializationState& state,
                                       ColumnSelectVector& select_vector,
                                       int64_t* materialization_time) {
    auto& selection = state.selection;
    selection.ranges.clear();
    selection.total_values = select_vector.num_values();
    selection.selected_values = 0;

    size_t cursor = 0;
    ColumnSelectVector::DataReadType read_type;
    while (const size_t run_length = select_vector.get_next_run<true>(&read_type)) {
        DORIS_CHECK(read_type == ColumnSelectVector::CONTENT ||
                    read_type == ColumnSelectVector::FILTERED_CONTENT);
        if (read_type == ColumnSelectVector::CONTENT) {
            selection.ranges.push_back({.first = cursor, .count = run_length});
            selection.selected_values += run_length;
        }
        cursor += run_length;
    }
    DORIS_CHECK_EQ(cursor, selection.total_values);
    if (selection.selected_values == 0) {
        return decoder.skip_values(selection.total_values);
    }

    SCOPED_RAW_TIMER(materialization_time);
    SelectedDecodeSource selected_source(decoder, selection);
    return read_native_or_serde(column, serde, selected_source, context, selection.selected_values,
                                state);
}

template <typename Visitor>
bool visit_nullable_expandable_column(IColumn& column, Visitor&& visitor) {
#define VISIT_COLUMN(TYPE)                                   \
    if (auto* typed = check_and_get_column<TYPE>(&column)) { \
        visitor(*typed);                                     \
        return true;                                         \
    }
    VISIT_COLUMN(ColumnUInt8)
    VISIT_COLUMN(ColumnInt8)
    VISIT_COLUMN(ColumnInt16)
    VISIT_COLUMN(ColumnInt32)
    VISIT_COLUMN(ColumnInt64)
    VISIT_COLUMN(ColumnInt128)
    VISIT_COLUMN(ColumnDate)
    VISIT_COLUMN(ColumnDateTime)
    VISIT_COLUMN(ColumnDateV2)
    VISIT_COLUMN(ColumnDateTimeV2)
    VISIT_COLUMN(ColumnFloat32)
    VISIT_COLUMN(ColumnFloat64)
    VISIT_COLUMN(ColumnIPv4)
    VISIT_COLUMN(ColumnIPv6)
    VISIT_COLUMN(ColumnTimeV2)
    VISIT_COLUMN(ColumnTimeStampTz)
    VISIT_COLUMN(ColumnOffset32)
    VISIT_COLUMN(ColumnOffset64)
    VISIT_COLUMN(ColumnDecimal32)
    VISIT_COLUMN(ColumnDecimal64)
    VISIT_COLUMN(ColumnDecimal128V2)
    VISIT_COLUMN(ColumnDecimal128V3)
    VISIT_COLUMN(ColumnDecimal256)
    VISIT_COLUMN(ColumnString)
    VISIT_COLUMN(ColumnString64)
    VISIT_COLUMN(ColumnVarbinary)
    VISIT_COLUMN(ColumnDictI32)
#undef VISIT_COLUMN
    return false;
}

template <typename ColumnType>
void expand_nullable_pod_values(ColumnType& column, size_t old_size, size_t compact_values,
                                const NullMap& selected_nulls) {
    auto& data = column.get_data();
    DORIS_CHECK_EQ(data.size(), old_size + compact_values);
    data.resize(old_size + selected_nulls.size());
    if constexpr (sizeof(typename ColumnType::value_type) == 4 ||
                  sizeof(typename ColumnType::value_type) == 8) {
        simd::expand_nullable_values(reinterpret_cast<uint8_t*>(data.data() + old_size),
                                     compact_values, selected_nulls.data(), selected_nulls.size(),
                                     sizeof(typename ColumnType::value_type));
        return;
    }
    size_t source = compact_values;
    for (size_t output = selected_nulls.size(); output > 0;) {
        --output;
        if (selected_nulls[output] != 0) {
            data[old_size + output] = typename ColumnType::value_type {};
        } else {
            DORIS_CHECK(source > 0);
            --source;
            data[old_size + output] = std::move(data[old_size + source]);
        }
    }
    DORIS_CHECK_EQ(source, 0);
}

template <typename Offset>
void expand_nullable_string_values(ColumnStr<Offset>& column, size_t old_size,
                                   size_t compact_values, const NullMap& selected_nulls) {
    auto& offsets = column.get_offsets();
    DORIS_CHECK_EQ(offsets.size(), old_size + compact_values);
    const Offset prefix_end = old_size == 0 ? 0 : offsets[old_size - 1];
    offsets.resize(old_size + selected_nulls.size());
    size_t source = compact_values;
    for (size_t output = selected_nulls.size(); output > 0;) {
        --output;
        if (selected_nulls[output] == 0) {
            DORIS_CHECK(source > 0);
            --source;
            offsets[old_size + output] = offsets[old_size + source];
        } else {
            offsets[old_size + output] = source == 0 ? prefix_end : offsets[old_size + source - 1];
        }
    }
    DORIS_CHECK_EQ(source, 0);
}

template <typename ColumnType>
void expand_nullable_values(ColumnType& column, size_t old_size, size_t compact_values,
                            const NullMap& selected_nulls) {
    expand_nullable_pod_values(column, old_size, compact_values, selected_nulls);
}

template <typename Offset>
void expand_nullable_values(ColumnStr<Offset>& column, size_t old_size, size_t compact_values,
                            const NullMap& selected_nulls) {
    expand_nullable_string_values(column, old_size, compact_values, selected_nulls);
}

void remap_nullable_conversion_failures(IColumn::Filter* conversion_failure_null_map,
                                        size_t old_size, size_t compact_values,
                                        const NullMap& selected_nulls) {
    if (conversion_failure_null_map == nullptr) {
        return;
    }
    DORIS_CHECK(conversion_failure_null_map->size() >= old_size + selected_nulls.size());
    size_t source = compact_values;
    // Walk backwards so writing an expanded row cannot overwrite an unread compact failure bit.
    for (size_t output = selected_nulls.size(); output > 0;) {
        --output;
        if (selected_nulls[output] != 0) {
            (*conversion_failure_null_map)[old_size + output] = 1;
        } else {
            DORIS_CHECK(source > 0);
            --source;
            (*conversion_failure_null_map)[old_size + output] =
                    (*conversion_failure_null_map)[old_size + source];
        }
    }
    DORIS_CHECK_EQ(source, 0);
}

Status decode_selected_nullable_values(IColumn& column, const DataTypeSerDe& serde,
                                       Decoder& decoder, const ParquetDecodeContext& context,
                                       ParquetMaterializationState& state,
                                       ColumnSelectVector& select_vector, NullMap& selected_nulls,
                                       int64_t* materialization_time) {
    auto& selection = state.selection;
    selection.ranges.clear();
    selection.total_values = 0;
    selection.selected_values = 0;
    selected_nulls.clear();
    selected_nulls.reserve(select_vector.num_values() - select_vector.num_filtered());

    size_t physical_cursor = 0;
    ColumnSelectVector::DataReadType read_type;
    while (const size_t run_length = select_vector.get_next_run<true>(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT:
            if (!selection.ranges.empty() &&
                selection.ranges.back().first + selection.ranges.back().count == physical_cursor) {
                selection.ranges.back().count += run_length;
            } else {
                selection.ranges.push_back({.first = physical_cursor, .count = run_length});
            }
            selection.selected_values += run_length;
            selected_nulls.resize_fill(selected_nulls.size() + run_length, 0);
            physical_cursor += run_length;
            break;
        case ColumnSelectVector::NULL_DATA:
            selected_nulls.resize_fill(selected_nulls.size() + run_length, 1);
            break;
        case ColumnSelectVector::FILTERED_CONTENT:
            physical_cursor += run_length;
            break;
        case ColumnSelectVector::FILTERED_NULL:
            break;
        }
    }
    selection.total_values = physical_cursor;
    DORIS_CHECK_EQ(selection.total_values, select_vector.num_values() - select_vector.num_nulls());
    DORIS_CHECK_EQ(selected_nulls.size(),
                   select_vector.num_values() - select_vector.num_filtered());

    const size_t old_size = column.size();
    SCOPED_RAW_TIMER(materialization_time);
    if (selection.selected_values == 0) {
        RETURN_IF_ERROR(decoder.skip_values(selection.total_values));
        column.insert_many_defaults(selected_nulls.size());
        return Status::OK();
    }

    if (state.conversion_failure_null_map != nullptr) {
        DORIS_CHECK(state.conversion_failure_null_map->size() >= old_size + selected_nulls.size());
        memset(state.conversion_failure_null_map->data() + old_size, 0, selection.selected_values);
    }
    SelectedDecodeSource selected_source(decoder, selection);
    const auto status = read_native_or_serde(column, serde, selected_source, context,
                                             selection.selected_values, state);
    if (!status.ok()) {
        if (state.conversion_failure_null_map != nullptr) {
            memcpy(state.conversion_failure_null_map->data() + old_size, selected_nulls.data(),
                   selected_nulls.size());
        }
        return status;
    }
    DORIS_CHECK_EQ(column.size(), old_size + selection.selected_values);

    remap_nullable_conversion_failures(state.conversion_failure_null_map, old_size,
                                       selection.selected_values, selected_nulls);
    const bool expanded = visit_nullable_expandable_column(column, [&](auto& typed_column) {
        // Decode into the final nested column compactly, then expand in place. This preserves the
        // V2 no-intermediate-column invariant while restoring the nullable sparse row layout.
        expand_nullable_values(typed_column, old_size, selection.selected_values, selected_nulls);
    });
    DORIS_CHECK(expanded);
    return Status::OK();
}

class FixedWidthPredicateConsumer final : public ParquetFixedValueConsumer {
public:
    FixedWidthPredicateConsumer(const VExprSPtrs& conjuncts, DataTypePtr data_type, int column_id,
                                IColumn::Filter* matches, IColumn* projected_column)
            : _conjuncts(conjuncts),
              _data_type(std::move(data_type)),
              _column_id(column_id),
              _matches(matches),
              _projected_column(projected_column) {
        DORIS_CHECK(_matches != nullptr);
    }

    Status consume(const uint8_t* values, size_t num_values, size_t value_width) override {
        const size_t old_size = _matches->size();
        _matches->resize_fill(old_size + num_values, 1);
        for (const auto& conjunct : _conjuncts) {
            RETURN_IF_ERROR(conjunct->execute_on_raw_fixed_values(values, num_values, value_width,
                                                                  _data_type, _column_id,
                                                                  _matches->data() + old_size));
        }
        if (_projected_column != nullptr) {
            size_t row = 0;
            while (row < num_values) {
                while (row < num_values && (*_matches)[old_size + row] == 0) {
                    ++row;
                }
                const size_t run_begin = row;
                while (row < num_values && (*_matches)[old_size + row] != 0) {
                    ++row;
                }
                if (row != run_begin) {
                    _projected_column->insert_many_raw_data(
                            reinterpret_cast<const char*>(values + run_begin * value_width),
                            row - run_begin);
                }
            }
        }
        return Status::OK();
    }

private:
    const VExprSPtrs& _conjuncts;
    DataTypePtr _data_type;
    int _column_id;
    IColumn::Filter* _matches;
    IColumn* _projected_column;
};

} // namespace

template <bool IN_COLLECTION, bool OFFSET_INDEX>
ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::ColumnChunkReader(
        io::BufferedStreamReader* reader, tparquet::ColumnChunk* column_chunk,
        NativeFieldSchema* field_schema, const tparquet::OffsetIndex* offset_index,
        size_t total_rows, io::IOContext* io_ctx, const ParquetPageReadContext& page_read_ctx,
        const ColumnChunkRange* chunk_range)
        : _field_schema(field_schema),
          _max_rep_level(field_schema->repetition_level),
          _max_def_level(field_schema->definition_level),
          _stream_reader(reader),
          _metadata(column_chunk->meta_data),
          _offset_index(offset_index),
          _total_rows(total_rows),
          _io_ctx(io_ctx),
          _page_read_ctx(page_read_ctx) {
    if (chunk_range != nullptr) {
        _chunk_range = *chunk_range;
        _has_validated_chunk_range = true;
    }
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::init() {
    size_t start_offset = _has_validated_chunk_range
                                  ? _chunk_range.offset
                                  : (has_dict_page(_metadata) ? _metadata.dictionary_page_offset
                                                              : _metadata.data_page_offset);
    size_t chunk_size =
            _has_validated_chunk_range ? _chunk_range.length : _metadata.total_compressed_size;
    // create page reader
    _page_reader = create_page_reader<IN_COLLECTION, OFFSET_INDEX>(
            _stream_reader, _io_ctx, start_offset, chunk_size, _total_rows, _metadata,
            _page_read_ctx, _offset_index);
    // get the block compression codec
    RETURN_IF_ERROR(get_block_compression_codec(_metadata.codec, &_block_compress_codec));
    _state = INITIALIZED;
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::skip_nested_values(
        const std::vector<level_t>& def_levels, size_t start_index) {
    size_t no_value_cnt = 0;
    size_t value_cnt = 0;

    DORIS_CHECK(start_index <= def_levels.size());
    for (size_t idx = start_index; idx < def_levels.size(); idx++) {
        level_t def_level = def_levels[idx];
        if (IN_COLLECTION && def_level < _field_schema->repeated_parent_def_level) {
            no_value_cnt++;
        } else if (def_level < _field_schema->definition_level) {
            no_value_cnt++;
        } else {
            value_cnt++;
        }
    }

    RETURN_IF_ERROR(skip_values(value_cnt, true));
    RETURN_IF_ERROR(skip_values(no_value_cnt, false));
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::read_levels(
        size_t num_values, std::vector<level_t>* rep_levels, std::vector<level_t>* def_levels) {
    DORIS_CHECK(rep_levels != nullptr);
    DORIS_CHECK(def_levels != nullptr);
    if (_remaining_num_values < num_values || _remaining_rep_nums < num_values ||
        _remaining_def_nums < num_values) {
        return Status::Corruption(
                "Parquet level reader requested {} slots with only {}/{}/{} remaining", num_values,
                _remaining_num_values, _remaining_rep_nums, _remaining_def_nums);
    }

    const size_t start_index = def_levels->size();
    rep_levels->resize(rep_levels->size() + num_values, 0);
    def_levels->resize(def_levels->size() + num_values, 0);
    if (_max_rep_level > 0) {
        const size_t decoded = _rep_level_decoder.get_levels(
                rep_levels->data() + rep_levels->size() - num_values, num_values);
        if (decoded != num_values) {
            return Status::Corruption("Parquet repetition level stream ended after {} of {} slots",
                                      decoded, num_values);
        }
    }
    if (_max_def_level > 0) {
        const size_t decoded = _def_level_decoder.get_levels(
                def_levels->data() + def_levels->size() - num_values, num_values);
        if (decoded != num_values) {
            return Status::Corruption("Parquet definition level stream ended after {} of {} slots",
                                      decoded, num_values);
        }
    }
    _remaining_rep_nums -= num_values;
    _remaining_def_nums -= num_values;
    return skip_nested_values(*def_levels, start_index);
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::_ensure_dictionary_page_loaded() {
    if (_dict_checked) {
        return Status::OK();
    }

    DORIS_CHECK(_state == INITIALIZED);
    while (true) {
        RETURN_IF_ERROR(_page_reader->parse_page_header());
        const tparquet::PageHeader* header = nullptr;
        RETURN_IF_ERROR(_page_reader->get_page_header(&header));
        if (header->type == tparquet::PageType::DATA_PAGE ||
            header->type == tparquet::PageType::DATA_PAGE_V2) {
            if constexpr (IN_COLLECTION && OFFSET_INDEX) {
                if (header->type == tparquet::PageType::DATA_PAGE &&
                    _page_reader->has_active_offset_index()) {
                    // V1 nested pages expose row boundaries only in repetition levels, so an
                    // indexed seek must not skip the first page before those levels are decoded.
                    _page_reader->discard_offset_index();
                    _offset_index = nullptr;
                }
            }
            _dict_checked = true;
            return Status::OK();
        }
        if (header->type != tparquet::PageType::DICTIONARY_PAGE) {
            RETURN_IF_ERROR(_page_reader->skip_auxiliary_page());
            continue;
        }
        RETURN_IF_ERROR(_decode_dict_page());
        RETURN_IF_ERROR(_page_reader->dict_next_page());
        _dict_checked = true;
        return Status::OK();
    }
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::load_dictionary_page(bool* has_dict) {
    RETURN_IF_ERROR(_ensure_dictionary_page_loaded());
    *has_dict = _has_dict;
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::parse_page_header() {
    if (_state == HEADER_PARSED || _state == DATA_LOADED) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_ensure_dictionary_page_loaded());
    const tparquet::PageHeader* header = nullptr;
    while (true) {
        RETURN_IF_ERROR(_page_reader->parse_page_header());
        RETURN_IF_ERROR(_page_reader->get_page_header(&header));
        if (header->type == tparquet::PageType::DATA_PAGE ||
            header->type == tparquet::PageType::DATA_PAGE_V2) {
            break;
        }
        if (header->type == tparquet::PageType::DICTIONARY_PAGE) {
            return Status::Corruption("Parquet dictionary page appears after data pages");
        }
        RETURN_IF_ERROR(_page_reader->skip_auxiliary_page());
    }
    int32_t page_num_values = _page_reader->is_header_v2() ? header->data_page_header_v2.num_values
                                                           : header->data_page_header.num_values;
    if constexpr (IN_COLLECTION && OFFSET_INDEX) {
        if (!_page_reader->is_header_v2() && _page_reader->has_active_offset_index()) {
            // V1 nested pages do not declare their logical row count. An OffsetIndex span cannot
            // be trusted until repetition levels are decoded, so keep the sequential cursor path.
            _page_reader->discard_offset_index();
            _offset_index = nullptr;
        }
    }
    const bool active_offset_index = _page_reader->has_active_offset_index();
    if (page_num_values < 0 || page_num_values > _metadata.num_values ||
        (!active_offset_index &&
         static_cast<uint64_t>(page_num_values) >
                 static_cast<uint64_t>(_metadata.num_values) - _chunk_parsed_values)) {
        // Page counts are untrusted and feed both level decoders and scratch sizing. Bound each
        // page by the column metadata before converting to unsigned counters.
        return Status::Corruption("Parquet data page value count {} exceeds column total {}",
                                  page_num_values, _metadata.num_values);
    }
    if constexpr (!IN_COLLECTION) {
        const size_t page_start_row = _page_reader->start_row();
        const size_t page_end_row = _page_reader->end_row();
        if (UNLIKELY(page_end_row < page_start_row ||
                     static_cast<size_t>(page_num_values) != page_end_row - page_start_row)) {
            // Flat columns have exactly one physical value slot per logical row. Rejecting a
            // divergent header/OffsetIndex span prevents every later page from shifting rows.
            return Status::Corruption(
                    "Parquet flat data page has {} values for logical row range [{}, {})",
                    page_num_values, page_start_row, page_end_row);
        }
    } else if (_page_reader->is_header_v2() && active_offset_index) {
        const size_t page_start_row = _page_reader->start_row();
        const size_t page_end_row = _page_reader->end_row();
        if (UNLIKELY(page_end_row < page_start_row ||
                     static_cast<size_t>(header->data_page_header_v2.num_rows) !=
                             page_end_row - page_start_row)) {
            // V2 is the only repeated-page format that states its logical row count in the page
            // header, so it must agree with the optional index before indexed seeking is allowed.
            return Status::Corruption(
                    "Parquet nested data page has {} rows for indexed row range [{}, {})",
                    header->data_page_header_v2.num_rows, page_start_row, page_end_row);
        }
    }
    _remaining_rep_nums = page_num_values;
    _remaining_def_nums = page_num_values;
    _remaining_num_values = page_num_values;

    // no offset will parse all header.
    if (!active_offset_index) {
        _chunk_parsed_values += _remaining_num_values;
    }
    _state = HEADER_PARSED;
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::next_page() {
    // Load dictionary state before advancing can jump past the physical dictionary page.
    RETURN_IF_ERROR(_ensure_dictionary_page_loaded());
    // Level parsing advances _page_data past the allocation base, so retain explicit ownership
    // state instead of inferring whether current decoders still reference decompressed storage.
    _page_uses_decompress_buf = false;
    _active_decompress_bytes = 0;
    if (_decompress_release_pending) {
        if (_decompress_buf_size > _decompress_release_threshold) {
            _decompress_buf.reset();
            _decompress_buf_size = 0;
        }
        _decompress_release_pending = false;
        _decompress_release_threshold = std::numeric_limits<size_t>::max();
    }
    RETURN_IF_ERROR(_page_reader->next_page());
    _state = INITIALIZED;
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::_get_uncompressed_levels(
        const tparquet::DataPageHeaderV2& page_v2, Slice& page_data) {
    const size_t rl = page_v2.repetition_levels_byte_length;
    const size_t dl = page_v2.definition_levels_byte_length;
    if (UNLIKELY(rl > page_data.size || dl > page_data.size - rl)) {
        // Validate the physical slice again because a cached entry may itself be truncated.
        return Status::Corruption("Parquet data page v2 level bytes exceed available payload");
    }
    _v2_rep_levels = Slice(page_data.data, rl);
    _v2_def_levels = Slice(page_data.data + rl, dl);
    page_data.data += dl + rl;
    page_data.size -= dl + rl;
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::load_page_data() {
    if (_state == DATA_LOADED) {
        return Status::OK();
    }
    if (UNLIKELY(_state != HEADER_PARSED)) {
        return Status::Corruption("Should parse page header");
    }

    const tparquet::PageHeader* header = nullptr;
    RETURN_IF_ERROR(_page_reader->get_page_header(&header));
    RETURN_IF_ERROR(validate_uncompressed_page_sizes(
            *header, _metadata.codec, _page_read_ctx.data_page_v2_always_compressed));
    // Zero levels alone are insufficient: test/protocol adapters can leave repetition unset, so
    // only an explicitly REQUIRED schema proves that every logical value has fixed-width bytes.
    const bool schema_is_required = _field_schema->parquet_schema.__isset.repetition_type &&
                                    _field_schema->parquet_schema.repetition_type ==
                                            tparquet::FieldRepetitionType::REQUIRED;
    RETURN_IF_ERROR(validate_fixed_width_page_size(*header, _get_type_length(), _max_rep_level,
                                                   _max_def_level, schema_is_required));
    int32_t uncompressed_size = header->uncompressed_page_size;
    bool page_loaded = false;
    _page_uses_decompress_buf = false;
    _active_decompress_bytes = 0;

    // First, try to reuse a cache handle previously discovered by PageReader
    // (header-only lookup) to avoid a second lookup here.
    if (_page_read_ctx.enable_parquet_file_page_cache && !config::disable_storage_page_cache &&
        StoragePageCache::instance() != nullptr) {
        if (_page_reader->has_page_cache_handle()) {
            const PageCacheHandle& handle = _page_reader->page_cache_handle();
            Slice cached = handle.data();
            size_t header_size = _page_reader->header_bytes().size();
            size_t levels_size = 0;
            if (header->__isset.data_page_header_v2) {
                const tparquet::DataPageHeaderV2& header_v2 = header->data_page_header_v2;
                size_t rl = header_v2.repetition_levels_byte_length;
                size_t dl = header_v2.definition_levels_byte_length;
                levels_size = rl + dl;
                if (UNLIKELY(header_size > cached.size ||
                             levels_size > cached.size - header_size)) {
                    return Status::Corruption("Cached Parquet page is shorter than its v2 levels");
                }
                _v2_rep_levels =
                        Slice(reinterpret_cast<const uint8_t*>(cached.data) + header_size, rl);
                _v2_def_levels =
                        Slice(reinterpret_cast<const uint8_t*>(cached.data) + header_size + rl, dl);
            }
            // payload_slice points to the bytes after header and levels
            if (UNLIKELY(header_size + levels_size > cached.size)) {
                return Status::Corruption("Cached Parquet page is shorter than its header");
            }
            Slice payload_slice(cached.data + header_size + levels_size,
                                cached.size - header_size - levels_size);

            bool cache_payload_is_decompressed = _page_reader->is_cache_payload_decompressed();
            const size_t expected_payload_size =
                    cache_payload_is_decompressed
                            ? static_cast<size_t>(header->uncompressed_page_size) - levels_size
                            : static_cast<size_t>(header->compressed_page_size) - levels_size;
            if (UNLIKELY(payload_slice.size != expected_payload_size)) {
                return Status::Corruption("Cached Parquet page payload has size {}, expected {}",
                                          payload_slice.size, expected_payload_size);
            }

            if (cache_payload_is_decompressed) {
                // Cached payload is already uncompressed
                _page_data = payload_slice;
            } else {
                CHECK(_block_compress_codec);
                // Decompress cached payload into _decompress_buf for decoding
                size_t uncompressed_payload_size =
                        header->__isset.data_page_header_v2
                                ? static_cast<size_t>(header->uncompressed_page_size) - levels_size
                                : static_cast<size_t>(header->uncompressed_page_size);
                RETURN_IF_ERROR(validate_compressed_page_size(_metadata.codec, payload_slice,
                                                              uncompressed_payload_size));
                _reserve_decompress_buf(uncompressed_payload_size);
                _page_data = Slice(_decompress_buf.get(), uncompressed_payload_size);
                _page_uses_decompress_buf = true;
                _active_decompress_bytes = uncompressed_payload_size;
                SCOPED_RAW_TIMER(&_chunk_statistics.decompress_time);
                _chunk_statistics.decompress_cnt++;
                RETURN_IF_ERROR(_block_compress_codec->decompress(payload_slice, &_page_data));
                if (UNLIKELY(_page_data.size != uncompressed_payload_size)) {
                    return Status::Corruption("Parquet page decompressed to {} bytes, expected {}",
                                              _page_data.size, uncompressed_payload_size);
                }
            }
            // page cache counters were incremented when PageReader did the header-only
            // cache lookup. Do not increment again to avoid double-counting.
            page_loaded = true;
        }
    }

    if (!page_loaded) {
        const bool prepare_cache_payload = can_prepare_page_cache_payload(
                _page_read_ctx.enable_parquet_file_page_cache, config::disable_storage_page_cache,
                StoragePageCache::instance() != nullptr, !_page_reader->header_bytes().empty());
        if (_block_compress_codec != nullptr) {
            Slice compressed_data;
            RETURN_IF_ERROR(_page_reader->get_page_data(compressed_data));
            std::vector<uint8_t> level_bytes;
            if (header->__isset.data_page_header_v2) {
                const tparquet::DataPageHeaderV2& header_v2 = header->data_page_header_v2;
                // uncompressed_size = rl + dl + uncompressed_data_size
                // compressed_size = rl + dl + compressed_data_size
                uncompressed_size -= header_v2.repetition_levels_byte_length +
                                     header_v2.definition_levels_byte_length;
                // copy level bytes (rl + dl) so that we can cache header + levels + uncompressed payload
                size_t rl = header_v2.repetition_levels_byte_length;
                size_t dl = header_v2.definition_levels_byte_length;
                size_t level_sz = rl + dl;
                if (prepare_cache_payload && level_sz > 0) {
                    level_bytes.resize(level_sz);
                    memcpy(level_bytes.data(), compressed_data.data, level_sz);
                }
                // now remove levels from compressed_data for decompression
                RETURN_IF_ERROR(_get_uncompressed_levels(header_v2, compressed_data));
            }
            bool is_v2_compressed = header->__isset.data_page_header_v2 &&
                                    (header->data_page_header_v2.is_compressed ||
                                     _page_read_ctx.data_page_v2_always_compressed);
            bool page_has_compression = header->__isset.data_page_header || is_v2_compressed;

            if (page_has_compression) {
                // Decompress payload for immediate decoding
                RETURN_IF_ERROR(validate_compressed_page_size(
                        _metadata.codec, compressed_data, static_cast<size_t>(uncompressed_size)));
                _reserve_decompress_buf(uncompressed_size);
                _page_data = Slice(_decompress_buf.get(), uncompressed_size);
                _page_uses_decompress_buf = true;
                _active_decompress_bytes = static_cast<size_t>(uncompressed_size);
                SCOPED_RAW_TIMER(&_chunk_statistics.decompress_time);
                _chunk_statistics.decompress_cnt++;
                RETURN_IF_ERROR(_block_compress_codec->decompress(compressed_data, &_page_data));
                if (UNLIKELY(_page_data.size != static_cast<size_t>(uncompressed_size))) {
                    return Status::Corruption("Parquet page decompressed to {} bytes, expected {}",
                                              _page_data.size, uncompressed_size);
                }

                // Decide whether to cache decompressed payload or compressed payload based on threshold
                bool cache_payload_decompressed = should_cache_decompressed(
                        header, _metadata, _page_read_ctx.data_page_v2_always_compressed);

                if (prepare_cache_payload) {
                    if (cache_payload_decompressed) {
                        _insert_page_into_cache(level_bytes, _page_data);
                        _chunk_statistics.page_cache_decompressed_write_counter += 1;
                    } else {
                        if (config::enable_parquet_cache_compressed_pages) {
                            // cache the compressed payload as-is (header | levels | compressed_payload)
                            _insert_page_into_cache(
                                    level_bytes, Slice(compressed_data.data, compressed_data.size));
                            _chunk_statistics.page_cache_compressed_write_counter += 1;
                        }
                    }
                }
            } else {
                // no compression on this page, use the data directly
                _page_data = Slice(compressed_data.data, compressed_data.size);
                if (prepare_cache_payload) {
                    _insert_page_into_cache(level_bytes, _page_data);
                    _chunk_statistics.page_cache_decompressed_write_counter += 1;
                }
            }
        } else {
            // For uncompressed page, we may still need to extract v2 levels
            std::vector<uint8_t> level_bytes;
            Slice uncompressed_data;
            RETURN_IF_ERROR(_page_reader->get_page_data(uncompressed_data));
            if (header->__isset.data_page_header_v2) {
                const tparquet::DataPageHeaderV2& header_v2 = header->data_page_header_v2;
                size_t rl = header_v2.repetition_levels_byte_length;
                size_t dl = header_v2.definition_levels_byte_length;
                size_t level_sz = rl + dl;
                if (prepare_cache_payload && level_sz > 0) {
                    level_bytes.resize(level_sz);
                    memcpy(level_bytes.data(), uncompressed_data.data, level_sz);
                }
                RETURN_IF_ERROR(_get_uncompressed_levels(header_v2, uncompressed_data));
            }
            // copy page data out
            _page_data = Slice(uncompressed_data.data, uncompressed_data.size);
            // Optionally cache uncompressed data for uncompressed pages
            if (prepare_cache_payload) {
                _insert_page_into_cache(level_bytes, _page_data);
                _chunk_statistics.page_cache_decompressed_write_counter += 1;
            }
        }
    }

    // Initialize repetition level and definition level. Skip when level = 0, which means required field.
    if (_max_rep_level > 0) {
        SCOPED_RAW_TIMER(&_chunk_statistics.decode_level_time);
        if (header->__isset.data_page_header_v2) {
            RETURN_IF_ERROR(_rep_level_decoder.init_v2(_v2_rep_levels, _max_rep_level,
                                                       _remaining_rep_nums));
        } else {
            RETURN_IF_ERROR(_rep_level_decoder.init(
                    &_page_data, header->data_page_header.repetition_level_encoding, _max_rep_level,
                    _remaining_rep_nums));
        }
    }
    if (_max_def_level > 0) {
        SCOPED_RAW_TIMER(&_chunk_statistics.decode_level_time);
        if (header->__isset.data_page_header_v2) {
            RETURN_IF_ERROR(_def_level_decoder.init_v2(_v2_def_levels, _max_def_level,
                                                       _remaining_def_nums));
        } else {
            RETURN_IF_ERROR(_def_level_decoder.init(
                    &_page_data, header->data_page_header.definition_level_encoding, _max_def_level,
                    _remaining_def_nums));
        }
    }
    auto encoding = header->__isset.data_page_header_v2 ? header->data_page_header_v2.encoding
                                                        : header->data_page_header.encoding;
    // change the deprecated encoding to RLE_DICTIONARY
    if (encoding == tparquet::Encoding::PLAIN_DICTIONARY) {
        encoding = tparquet::Encoding::RLE_DICTIONARY;
    }
    _current_encoding = encoding;

    // Reuse page decoder
    Decoder* encoding_decoder = nullptr;
    if (_decoders.find(static_cast<int>(encoding)) != _decoders.end()) {
        encoding_decoder = _decoders[static_cast<int>(encoding)].get();
    } else {
        std::unique_ptr<Decoder> page_decoder;
        RETURN_IF_ERROR(Decoder::get_decoder(_metadata.type, encoding, page_decoder));
        // Set type length
        page_decoder->set_type_length(_get_type_length());
        _decoders[static_cast<int>(encoding)] = std::move(page_decoder);
        encoding_decoder = _decoders[static_cast<int>(encoding)].get();
    }
    _empty_value_section = _page_data.empty() && _max_def_level > 0;
    if (_empty_value_section) {
        // Nullable all-NULL pages legally contain only definition levels. Keep them decodable for
        // every advertised encoding, but make a non-NULL definition level fail before stale decoder
        // state from the preceding page can be consumed.
        if (_empty_value_decoder == nullptr) {
            _empty_value_decoder = std::make_unique<EmptyValueSectionDecoder>();
        }
        _page_decoder = _empty_value_decoder.get();
    } else {
        _page_decoder = encoding_decoder;
        // Encoding headers cannot legitimately advertise more physical values than the data page's
        // logical value count; establish the bound before decoders inspect external counts.
        _page_decoder->set_expected_values(_remaining_num_values);
        RETURN_IF_ERROR(_page_decoder->set_data(&_page_data));
    }

    _state = DATA_LOADED;
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::_decode_dict_page() {
    const tparquet::PageHeader* header = nullptr;
    RETURN_IF_ERROR(_page_reader->get_page_header(&header));
    DCHECK_EQ(tparquet::PageType::DICTIONARY_PAGE, header->type);
    SCOPED_RAW_TIMER(&_chunk_statistics.decode_dict_time);

    // Using the PLAIN_DICTIONARY enum value is deprecated in the Parquet 2.0 specification.
    // Prefer using RLE_DICTIONARY in a data page and PLAIN in a dictionary page for Parquet 2.0+ files.
    // refer: https://github.com/apache/parquet-format/blob/master/Encodings.md
    tparquet::Encoding::type dict_encoding = header->dictionary_page_header.encoding;
    if (dict_encoding != tparquet::Encoding::PLAIN_DICTIONARY &&
        dict_encoding != tparquet::Encoding::PLAIN) {
        return Status::InternalError("Unsupported dictionary encoding {}",
                                     tparquet::to_string(dict_encoding));
    }

    // Prepare dictionary data
    int32_t uncompressed_size = header->uncompressed_page_size;
    RETURN_IF_ERROR(validate_uncompressed_page_sizes(
            *header, _metadata.codec, _page_read_ctx.data_page_v2_always_compressed));
    RETURN_IF_ERROR(validate_dictionary_page_size(*header, _get_type_length()));
    DorisUniqueBufferPtr<uint8_t> dict_data;
    bool dict_buffer_allocated = false;
    const auto allocate_dict_buffer = [&]() {
        if (!dict_buffer_allocated) {
            dict_data = make_unique_buffer<uint8_t>(uncompressed_size);
            dict_buffer_allocated = true;
        }
    };
    bool dict_loaded = false;

    // Try to load dictionary page from cache
    if (_page_read_ctx.enable_parquet_file_page_cache && !config::disable_storage_page_cache &&
        StoragePageCache::instance() != nullptr) {
        if (_page_reader->has_page_cache_handle()) {
            const PageCacheHandle& handle = _page_reader->page_cache_handle();
            Slice cached = handle.data();
            size_t header_size = _page_reader->header_bytes().size();
            if (UNLIKELY(header_size > cached.size)) {
                return Status::Corruption(
                        "Cached Parquet dictionary is shorter than its page header");
            }
            // Dictionary page layout in cache: header | payload (compressed or uncompressed)
            Slice payload_slice(cached.data + header_size, cached.size - header_size);

            bool cache_payload_is_decompressed = _page_reader->is_cache_payload_decompressed();

            if (cache_payload_is_decompressed) {
                // Use cached decompressed dictionary data
                if (UNLIKELY(payload_slice.size != static_cast<size_t>(uncompressed_size))) {
                    return Status::Corruption(
                            "Cached Parquet dictionary payload has size {}, expected {}",
                            payload_slice.size, uncompressed_size);
                }
                allocate_dict_buffer();
                memcpy(dict_data.get(), payload_slice.data, payload_slice.size);
                dict_loaded = true;
            } else {
                CHECK(_block_compress_codec);
                // Decompress cached compressed dictionary data
                RETURN_IF_ERROR(validate_compressed_page_size(
                        _metadata.codec, payload_slice, static_cast<size_t>(uncompressed_size)));
                allocate_dict_buffer();
                Slice dict_slice(dict_data.get(), uncompressed_size);
                {
                    SCOPED_RAW_TIMER(&_chunk_statistics.decompress_time);
                    ++_chunk_statistics.decompress_cnt;
                    RETURN_IF_ERROR(_block_compress_codec->decompress(payload_slice, &dict_slice));
                }
                if (UNLIKELY(dict_slice.size != static_cast<size_t>(uncompressed_size))) {
                    return Status::Corruption(
                            "Parquet dictionary decompressed to {} bytes, expected {}",
                            dict_slice.size, uncompressed_size);
                }
                dict_loaded = true;
            }

            // When dictionary page is loaded from cache, we need to skip the page data
            // to update the offset correctly (similar to calling get_page_data())
            if (dict_loaded) {
                _page_reader->skip_page_data();
            }
        }
    }

    if (!dict_loaded) {
        // Load and decompress dictionary page from file
        if (_block_compress_codec != nullptr) {
            auto dict_num = header->dictionary_page_header.num_values;
            Slice compressed_data;
            if (dict_num != 0) {
                RETURN_IF_ERROR(_page_reader->get_page_data(compressed_data));
                RETURN_IF_ERROR(validate_compressed_page_size(
                        _metadata.codec, compressed_data, static_cast<size_t>(uncompressed_size)));
                allocate_dict_buffer();
                Slice dict_slice(dict_data.get(), uncompressed_size);
                // Dictionary probes stop before data pages, so count their decompression here or
                // metadata pruning profiles will report no codec work for the scan.
                {
                    SCOPED_RAW_TIMER(&_chunk_statistics.decompress_time);
                    ++_chunk_statistics.decompress_cnt;
                    RETURN_IF_ERROR(
                            _block_compress_codec->decompress(compressed_data, &dict_slice));
                }
                if (UNLIKELY(dict_slice.size != static_cast<size_t>(uncompressed_size))) {
                    return Status::Corruption(
                            "Parquet dictionary decompressed to {} bytes, expected {}",
                            dict_slice.size, uncompressed_size);
                }
            }
            allocate_dict_buffer();
            Slice dict_slice(dict_data.get(), uncompressed_size);

            // Decide whether to cache decompressed or compressed dictionary based on threshold
            // If uncompressed_page_size == 0, should_cache_decompressed will return true
            bool cache_payload_decompressed = should_cache_decompressed(
                    header, _metadata, _page_read_ctx.data_page_v2_always_compressed);

            if (_page_read_ctx.enable_parquet_file_page_cache &&
                !config::disable_storage_page_cache && StoragePageCache::instance() != nullptr &&
                !_page_reader->header_bytes().empty()) {
                std::vector<uint8_t> empty_levels; // Dictionary pages don't have levels
                if (cache_payload_decompressed) {
                    // Cache the decompressed dictionary page
                    // If dict_num == 0, `dict_slice` will be empty
                    _insert_page_into_cache(empty_levels, dict_slice);
                    _chunk_statistics.page_cache_decompressed_write_counter += 1;
                } else {
                    if (config::enable_parquet_cache_compressed_pages) {
                        DCHECK(!compressed_data.empty());
                        // Cache the compressed dictionary page
                        _insert_page_into_cache(empty_levels,
                                                Slice(compressed_data.data, compressed_data.size));
                        _chunk_statistics.page_cache_compressed_write_counter += 1;
                    }
                }
            }
            // `get_page_data` not called, we should skip the page data
            // Because `_insert_page_into_cache` will use _page_reader, we should exec `skip_page_data` after `_insert_page_into_cache`
            if (dict_num == 0) {
                _page_reader->skip_page_data();
            }
        } else {
            Slice dict_slice;
            RETURN_IF_ERROR(_page_reader->get_page_data(dict_slice));
            if (UNLIKELY(dict_slice.size != static_cast<size_t>(uncompressed_size))) {
                return Status::Corruption("Parquet dictionary payload has size {}, expected {}",
                                          dict_slice.size, uncompressed_size);
            }
            allocate_dict_buffer();
            // The data is stored by BufferedStreamReader, we should copy it out
            memcpy(dict_data.get(), dict_slice.data, dict_slice.size);

            // Cache the uncompressed dictionary page
            if (_page_read_ctx.enable_parquet_file_page_cache &&
                !config::disable_storage_page_cache && StoragePageCache::instance() != nullptr &&
                !_page_reader->header_bytes().empty()) {
                std::vector<uint8_t> empty_levels;
                Slice payload(dict_data.get(), uncompressed_size);
                _insert_page_into_cache(empty_levels, payload);
                _chunk_statistics.page_cache_decompressed_write_counter += 1;
            }
        }
    }
    allocate_dict_buffer();

    // Cache page decoder
    std::unique_ptr<Decoder> page_decoder;
    RETURN_IF_ERROR(
            Decoder::get_decoder(_metadata.type, tparquet::Encoding::RLE_DICTIONARY, page_decoder));
    // Set type length
    page_decoder->set_type_length(_get_type_length());
    // Set the dictionary data
    RETURN_IF_ERROR(page_decoder->set_dict(dict_data, uncompressed_size,
                                           header->dictionary_page_header.num_values));
    _decoders[static_cast<int>(tparquet::Encoding::RLE_DICTIONARY)] = std::move(page_decoder);

    _has_dict = true;
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
void ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::_reserve_decompress_buf(size_t size) {
    if (size > _decompress_buf_size) {
        _decompress_buf_size = BitUtil::next_power_of_two(size);
        _decompress_buf = make_unique_buffer<uint8_t>(_decompress_buf_size);
    }
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
void ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::_insert_page_into_cache(
        const std::vector<uint8_t>& level_bytes, const Slice& payload) {
    StoragePageCache::CacheKey key =
            _page_reader->make_page_cache_key(_page_reader->header_start_offset());
    const std::vector<uint8_t>& header_bytes = _page_reader->header_bytes();
    size_t total = header_bytes.size() + level_bytes.size() + payload.size;
    auto page = std::make_unique<DataPage>(total, true, segment_v2::DATA_PAGE);
    size_t pos = 0;
    memcpy(page->data() + pos, header_bytes.data(), header_bytes.size());
    pos += header_bytes.size();
    if (!level_bytes.empty()) {
        memcpy(page->data() + pos, level_bytes.data(), level_bytes.size());
        pos += level_bytes.size();
    }
    if (payload.size > 0) {
        memcpy(page->data() + pos, payload.data, payload.size);
        pos += payload.size;
    }
    page->reset_size(total);
    PageCacheHandle handle;
    StoragePageCache::instance()->insert(key, page.get(), &handle, segment_v2::DATA_PAGE);
    page.release();
    _chunk_statistics.page_cache_write_counter += 1;
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::skip_values(size_t num_values,
                                                                   bool skip_data) {
    if (UNLIKELY(_remaining_num_values < num_values)) {
        return Status::IOError("Skip too many values in current page. {} vs. {}",
                               _remaining_num_values, num_values);
    }
    if (skip_data) {
        if (UNLIKELY(_empty_value_section && num_values != 0)) {
            return Status::Corruption(
                    "Parquet definition levels require {} values from an empty value section",
                    num_values);
        }
        SCOPED_RAW_TIMER(&_chunk_statistics.decode_value_time);
        RETURN_IF_ERROR(_page_decoder->skip_values(num_values));
    }
    // Commit logical page progress only after the physical decoder accepted the whole request.
    _remaining_num_values -= num_values;
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::materialize_values(
        MutableColumnPtr& doris_column, const DataTypeSerDe& serde, ParquetDecodeContext& context,
        ParquetMaterializationState& state, ColumnSelectVector& select_vector) {
    if (select_vector.num_values() == 0) {
        return Status::OK();
    }
    SCOPED_RAW_TIMER(&_chunk_statistics.decode_value_time);
    const size_t physical_values = select_vector.num_values() - select_vector.num_nulls();
    if (UNLIKELY(_empty_value_section && physical_values != 0)) {
        return Status::Corruption(
                "Parquet definition levels require {} values from an empty value section",
                physical_values);
    }
    if (UNLIKELY((doris_column->is_column_dictionary() || context.dictionary_index_only) &&
                 !_has_dict && physical_values != 0)) {
        return Status::IOError("Not dictionary coded");
    }
    if (UNLIKELY(_remaining_num_values < select_vector.num_values())) {
        return Status::IOError("Decode too many values in current page");
    }
    RETURN_IF_ERROR(translate_value_encoding(_current_encoding, &context.encoding));
    Status status;
    if (select_vector.has_filter()) {
        if (select_vector.num_nulls() == 0) {
            ++_chunk_statistics.hybrid_selection_batches;
            status = decode_selected_non_null_values(*doris_column, serde, *_page_decoder, context,
                                                     state, select_vector,
                                                     &_chunk_statistics.materialization_time);
            _chunk_statistics.hybrid_selection_ranges += state.selection.ranges.size();
        } else if (visit_nullable_expandable_column(*doris_column, [](auto&) {})) {
            // Parquet omits NULL leaf values from the physical stream. For example, the logical
            // DATE sequence [d0, NULL, d1, NULL, d2] is physically encoded as [d0, d1, d2]. The
            // generic fallback follows the logical selection runs, so those NULLs split one
            // contiguous physical span into decode(d0), insert-NULL, decode(d1), insert-NULL,
            // decode(d2). On nullable sparse scans this repeatedly enters SerDe and turns decimal
            // scaling, date conversion, timestamp/timezone conversion, or dictionary-ID
            // materialization into many tiny calls even though the physical values are adjacent.
            //
            // Build selected non-NULL ranges in physical coordinates instead. In the example this
            // decodes [d0, d1, d2] compactly with one SerDe consumer, records [0, 1, 0, 1, 0] as
            // the logical NULL layout, and expands the final nested column backwards so unread
            // compact values cannot be overwritten. Apply this to every expandable V2 scalar,
            // including ordinary PLAIN primitives: otherwise nullable numeric predicates still
            // fragment a physical span into millions of SerDe calls and defeat sparse decoding.
            ++_chunk_statistics.hybrid_selection_batches;
            status = decode_selected_nullable_values(
                    *doris_column, serde, *_page_decoder, context, state, select_vector,
                    _nullable_selection_nulls, &_chunk_statistics.materialization_time);
            _chunk_statistics.hybrid_selection_ranges += state.selection.ranges.size();
        } else {
            ++_chunk_statistics.hybrid_selection_null_fallback_batches;
            status = decode_selected_values<true>(*doris_column, serde, *_page_decoder, context,
                                                  state, select_vector,
                                                  &_chunk_statistics.materialization_time);
        }
    } else {
        status = decode_selected_values<false>(*doris_column, serde, *_page_decoder, context, state,
                                               select_vector,
                                               &_chunk_statistics.materialization_time);
    }
    RETURN_IF_ERROR(status);
    _remaining_num_values -= select_vector.num_values();
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
bool ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::can_filter_fixed_width_values(
        const VExprSPtrs& conjuncts, int column_id) const {
    if (conjuncts.empty() ||
        !supports_raw_fixed_filter_encoding(_current_encoding, _metadata.type)) {
        return false;
    }
    const auto primitive_type = remove_nullable(_field_schema->data_type)->get_primitive_type();
    const bool has_identity_width =
            (_metadata.type == tparquet::Type::INT32 && primitive_type == TYPE_INT) ||
            (_metadata.type == tparquet::Type::INT64 && primitive_type == TYPE_BIGINT) ||
            (_metadata.type == tparquet::Type::FLOAT && primitive_type == TYPE_FLOAT) ||
            (_metadata.type == tparquet::Type::DOUBLE && primitive_type == TYPE_DOUBLE);
    if (!has_identity_width) {
        // Raw predicates consume the physical Parquet width. Logical conversions such as UINT32
        // to BIGINT must stay on the typed path or a four-byte value is interpreted as eight bytes.
        return false;
    }
    return std::ranges::all_of(conjuncts, [&](const auto& conjunct) {
        return conjunct != nullptr &&
               conjunct->can_execute_on_raw_fixed_values(_field_schema->data_type, column_id);
    });
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::filter_fixed_width_values(
        const VExprSPtrs& conjuncts, int column_id, ColumnSelectVector& select_vector,
        NullMap* selected_nulls, IColumn::Filter* physical_matches, IColumn* projected_column,
        IColumn::Filter* row_filter, bool* used_filter) {
    DORIS_CHECK(selected_nulls != nullptr);
    DORIS_CHECK(physical_matches != nullptr);
    DORIS_CHECK(row_filter != nullptr);
    DORIS_CHECK(used_filter != nullptr);
    *used_filter = false;
    row_filter->clear();
    if (!can_filter_fixed_width_values(conjuncts, column_id)) {
        return Status::OK();
    }
    if (UNLIKELY(_remaining_num_values < select_vector.num_values())) {
        return Status::IOError("Decode too many values in current page");
    }

    ParquetSelection selection;
    selected_nulls->clear();
    selected_nulls->reserve(select_vector.num_values() - select_vector.num_filtered());
    size_t physical_cursor = 0;
    auto build_selection = [&]<bool HAS_FILTER>() {
        ColumnSelectVector::DataReadType read_type;
        while (const size_t run_length = select_vector.get_next_run<HAS_FILTER>(&read_type)) {
            switch (read_type) {
            case ColumnSelectVector::CONTENT:
                if (!selection.ranges.empty() &&
                    selection.ranges.back().first + selection.ranges.back().count ==
                            physical_cursor) {
                    selection.ranges.back().count += run_length;
                } else {
                    selection.ranges.push_back({.first = physical_cursor, .count = run_length});
                }
                selection.selected_values += run_length;
                selected_nulls->resize_fill(selected_nulls->size() + run_length, 0);
                physical_cursor += run_length;
                break;
            case ColumnSelectVector::NULL_DATA:
                selected_nulls->resize_fill(selected_nulls->size() + run_length, 1);
                break;
            case ColumnSelectVector::FILTERED_CONTENT:
                physical_cursor += run_length;
                break;
            case ColumnSelectVector::FILTERED_NULL:
                break;
            }
        }
    };
    if (select_vector.has_filter()) {
        build_selection.template operator()<true>();
    } else {
        build_selection.template operator()<false>();
    }
    selection.total_values = physical_cursor;
    DORIS_CHECK_EQ(selection.total_values, select_vector.num_values() - select_vector.num_nulls());
    DORIS_CHECK_EQ(selected_nulls->size(),
                   select_vector.num_values() - select_vector.num_filtered());
    if (UNLIKELY(_empty_value_section && selection.total_values != 0)) {
        return Status::Corruption(
                "Parquet definition levels require {} values from an empty value section",
                selection.total_values);
    }

    physical_matches->clear();
    if (selection.selected_values == 0) {
        RETURN_IF_ERROR(_page_decoder->skip_values(selection.total_values));
    } else {
        FixedWidthPredicateConsumer consumer(conjuncts, _field_schema->data_type, column_id,
                                             physical_matches, projected_column);
        RETURN_IF_ERROR(_page_decoder->decode_selected_fixed_values(selection, consumer));
        DORIS_CHECK_EQ(physical_matches->size(), selection.selected_values);
    }

    row_filter->reserve(selected_nulls->size());
    size_t physical_row = 0;
    for (const uint8_t is_null : *selected_nulls) {
        row_filter->push_back(is_null != 0 ? 0 : (*physical_matches)[physical_row++]);
    }
    DORIS_CHECK_EQ(physical_row, physical_matches->size());
    // Commit logical progress only after both the raw comparison and NULL remapping succeed.
    _remaining_num_values -= select_vector.num_values();
    *used_filter = true;
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::seek_to_nested_row(size_t left_row) {
    if constexpr (IN_COLLECTION && OFFSET_INDEX) {
        RETURN_IF_ERROR(_ensure_dictionary_page_loaded());
    }
    if constexpr (OFFSET_INDEX) {
        if (_page_reader->has_active_offset_index()) {
            while (true) {
                if (_page_reader->start_row() <= left_row && left_row < _page_reader->end_row()) {
                    break;
                } else if (has_next_page()) {
                    RETURN_IF_ERROR(next_page());
                    _current_row = _page_reader->start_row();
                } else [[unlikely]] {
                    return Status::InternalError("no match seek row {}, current row {}", left_row,
                                                 _current_row);
                }
            }

            RETURN_IF_ERROR(parse_page_header());
            RETURN_IF_ERROR(load_page_data());
            RETURN_IF_ERROR(_skip_nested_rows_in_page(left_row - _current_row));
            _current_row = left_row;
            return Status::OK();
        }
    }

    while (true) {
        RETURN_IF_ERROR(parse_page_header());
        if (_page_reader->is_header_v2() || !IN_COLLECTION) {
            if (_page_reader->start_row() <= left_row && left_row < _page_reader->end_row()) {
                RETURN_IF_ERROR(load_page_data());
                // this page contain this row.
                RETURN_IF_ERROR(_skip_nested_rows_in_page(left_row - _current_row));
                _current_row = left_row;
                break;
            }

            _current_row = _page_reader->end_row();
            if (has_next_page()) [[likely]] {
                RETURN_IF_ERROR(next_page());
            } else {
                return Status::InternalError("no match seek row {}, current row {}", left_row,
                                             _current_row);
            }
        } else {
            RETURN_IF_ERROR(load_page_data());
            std::vector<level_t> rep_levels;
            std::vector<level_t> def_levels;
            bool cross_page = false;

            size_t result_rows = 0;
            RETURN_IF_ERROR(load_page_nested_rows(rep_levels, left_row - _current_row, &result_rows,
                                                  &cross_page));
            RETURN_IF_ERROR(fill_def(def_levels));
            RETURN_IF_ERROR(skip_nested_values(def_levels));
            bool need_load_next_page = true;
            while (cross_page) {
                need_load_next_page = false;
                rep_levels.clear();
                def_levels.clear();
                RETURN_IF_ERROR(load_cross_page_nested_row(rep_levels, &cross_page));
                RETURN_IF_ERROR(fill_def(def_levels));
                RETURN_IF_ERROR(skip_nested_values(def_levels));
            }
            if (left_row == _current_row) {
                break;
            }
            if (need_load_next_page) {
                if (has_next_page()) [[likely]] {
                    RETURN_IF_ERROR(next_page());
                } else {
                    return Status::InternalError("no match seek row {}, current row {}", left_row,
                                                 _current_row);
                }
            }
        }
    };

    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::_skip_nested_rows_in_page(size_t num_rows) {
    if (num_rows == 0) {
        return Status::OK();
    }

    std::vector<level_t> rep_levels;
    std::vector<level_t> def_levels;

    bool cross_page = false;
    size_t result_rows = 0;
    RETURN_IF_ERROR(load_page_nested_rows(rep_levels, num_rows, &result_rows, &cross_page));
    RETURN_IF_ERROR(fill_def(def_levels));
    RETURN_IF_ERROR(skip_nested_values(def_levels));
    DCHECK(cross_page == false);
    if (num_rows != result_rows) [[unlikely]] {
        return Status::InternalError("no match skip rows, expect {} vs. real {}", num_rows,
                                     result_rows);
    }
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::load_page_nested_rows(
        std::vector<level_t>& rep_levels, size_t max_rows, size_t* result_rows, bool* cross_page) {
    if (_state != DATA_LOADED) [[unlikely]] {
        return Status::IOError("Should load page data first to load nested rows");
    }
    *cross_page = false;
    *result_rows = 0;
    // Reserve only the requested row frontier. One nested row may legitimately contain more
    // values and grow the vector incrementally, but a forged page count must not allocate gigabytes
    // before the level stream proves those values exist.
    const size_t requested_frontier =
            max_rows == std::numeric_limits<size_t>::max() ? max_rows : max_rows + 1;
    rep_levels.reserve(rep_levels.size() +
                       std::min<size_t>(_remaining_rep_nums, requested_frontier));
    while (_remaining_rep_nums) {
        level_t rep_level = _rep_level_get_next();
        if (UNLIKELY(rep_level < 0)) {
            return Status::Corruption("Parquet repetition level stream ended unexpectedly");
        }
        if constexpr (IN_COLLECTION) {
            // A continuation level is valid across later V1 pages only after this chunk has seen
            // a row start; accepting it on the first sequential page invents an orphan parent row.
            if (!_page_reader->has_active_offset_index() && !_nested_row_started &&
                rep_level != 0) {
                return Status::Corruption(
                        "First Parquet nested data page starts with repetition level {}",
                        rep_level);
            }
            if (!_page_reader->has_active_offset_index()) {
                _nested_row_started = true;
            }
        }
        if (rep_level == 0) {               // rep_level 0 indicates start of new row
            if (*result_rows == max_rows) { // this page contain max_rows, page no end.
                _current_row += max_rows;
                _rep_level_rewind_one();
                return Status::OK();
            }
            (*result_rows)++;
        }
        _remaining_rep_nums--;
        rep_levels.emplace_back(rep_level);
    }
    _current_row += *result_rows;

    if ((_page_reader->is_header_v2() || _page_reader->has_active_offset_index()) &&
        UNLIKELY(_current_row != _page_reader->end_row())) {
        // V2 and OffsetIndex advertise an exact logical row span. A page that exhausts its
        // repetition levels without that many row starts would otherwise make the caller retry
        // the same row forever.
        return Status::Corruption(
                "Parquet nested data page ended at row {}, expected page end row {}", _current_row,
                _page_reader->end_row());
    }

    auto need_check_cross_page = [&]() -> bool {
        return IN_COLLECTION && !_page_reader->has_active_offset_index() &&
               _remaining_rep_nums == 0 && !_page_reader->is_header_v2() && has_next_page();
    };
    *cross_page = need_check_cross_page();
    return Status::OK();
};

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::load_cross_page_nested_row(
        std::vector<level_t>& rep_levels, bool* cross_page) {
    RETURN_IF_ERROR(next_page());
    RETURN_IF_ERROR(parse_page_header());
    RETURN_IF_ERROR(load_page_data());

    *cross_page = has_next_page();
    while (_remaining_rep_nums) {
        level_t rep_level = _rep_level_get_next();
        if (UNLIKELY(rep_level < 0)) {
            return Status::Corruption("Parquet repetition level stream ended unexpectedly");
        }
        if constexpr (IN_COLLECTION) {
            if (!_page_reader->has_active_offset_index() && !_nested_row_started &&
                rep_level != 0) {
                return Status::Corruption(
                        "First Parquet nested data page starts with repetition level {}",
                        rep_level);
            }
            if (!_page_reader->has_active_offset_index()) {
                _nested_row_started = true;
            }
        }
        if (rep_level == 0) { // rep_level 0 indicates start of new row
            *cross_page = false;
            _rep_level_rewind_one();
            break;
        }
        _remaining_rep_nums--;
        rep_levels.emplace_back(rep_level);
    }
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
int32_t ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::_get_type_length() {
    switch (_field_schema->physical_type) {
    case tparquet::Type::INT32:
        [[fallthrough]];
    case tparquet::Type::FLOAT:
        return 4;
    case tparquet::Type::INT64:
        [[fallthrough]];
    case tparquet::Type::DOUBLE:
        return 8;
    case tparquet::Type::INT96:
        return 12;
    case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
        return _field_schema->parquet_schema.type_length;
    default:
        return -1;
    }
}

/**
 * Checks if the given column has a dictionary page.
 *
 * This function determines the presence of a dictionary page by checking the
 * dictionary_page_offset field in the column metadata. The dictionary_page_offset
 * must be set and greater than 0, and it must be less than the data_page_offset.
 *
 * The reason for these checks is based on the implementation in the Java version
 * of ORC, where dictionary_page_offset is used to indicate the absence of a dictionary.
 * Additionally, Parquet may write an empty row group, in which case the dictionary page
 * content would be empty, and thus the dictionary page should not be read.
 *
 * See https://github.com/apache/arrow/pull/2667/files
 */
bool has_dict_page(const tparquet::ColumnMetaData& column) {
    return column.__isset.dictionary_page_offset && column.dictionary_page_offset > 0 &&
           column.dictionary_page_offset < column.data_page_offset;
}

template class ColumnChunkReader<true, true>;
template class ColumnChunkReader<true, false>;
template class ColumnChunkReader<false, true>;
template class ColumnChunkReader<false, false>;

} // namespace doris::format::parquet::native
