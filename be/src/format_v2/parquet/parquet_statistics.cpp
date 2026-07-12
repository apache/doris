// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "format_v2/parquet/parquet_statistics.h"

#include <parquet/api/reader.h>
#include <parquet/bloom_filter.h>
#include <parquet/bloom_filter_reader.h>
#include <parquet/column_page.h>
#include <parquet/encoding.h>
#include <parquet/page_index.h>
#include <parquet/statistics.h>
#include <parquet/types.h>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstring>
#include <exception>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/config.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/field.h"
#include "exprs/expr_zonemap_filter.h"
#include "exprs/vexpr_context.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "runtime/runtime_profile.h"
#include "storage/index/zone_map/zone_map_index.h"
#include "storage/index/zone_map/zonemap_eval_context.h"

namespace doris::format::parquet {

namespace {

enum class ParquetRowGroupPruneReason {
    NONE,         // cannot prune; must read
    STATISTICS,   // excluded by ZoneMap statistics
    DICTIONARY,   // excluded by dictionary
    BLOOM_FILTER, // excluded by bloom filter
};

bool bloom_logical_type_supported(const ParquetColumnSchema& column_schema) {
    if (column_schema.type == nullptr) {
        return false;
    }
    switch (remove_nullable(column_schema.type)->get_primitive_type()) {
    case TYPE_BOOLEAN:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_STRING:
        return true;
    default:
        return false;
    }
}

DecodedTimeUnit decoded_time_unit(ParquetTimeUnit time_unit) {
    switch (time_unit) {
    case ParquetTimeUnit::MILLIS:
        return DecodedTimeUnit::MILLIS;
    case ParquetTimeUnit::MICROS:
        return DecodedTimeUnit::MICROS;
    case ParquetTimeUnit::NANOS:
        return DecodedTimeUnit::NANOS;
    default:
        return DecodedTimeUnit::UNKNOWN;
    }
}

Status read_decoded_field(const ParquetColumnSchema& column_schema, DecodedColumnView view,
                          Field* field, const cctz::time_zone* timezone) {
    DORIS_CHECK(column_schema.type != nullptr);
    DORIS_CHECK(field != nullptr);
    constexpr uint8_t not_null = 0;
    view.row_count = 1;
    view.null_map = &not_null;
    view.time_unit = decoded_time_unit(column_schema.type_descriptor.time_unit);
    view.logical_integer_bit_width = column_schema.type_descriptor.integer_bit_width;
    view.logical_integer_is_signed = !column_schema.type_descriptor.is_unsigned_integer;
    view.decimal_precision = column_schema.type_descriptor.decimal_precision;
    view.decimal_scale = column_schema.type_descriptor.decimal_scale;
    view.fixed_length = column_schema.type_descriptor.fixed_length;
    view.timestamp_is_adjusted_to_utc = column_schema.type_descriptor.timestamp_is_adjusted_to_utc;
    view.timezone = timezone;
    return column_schema.type->get_serde()->read_field_from_decoded_value(*column_schema.type,
                                                                          field, view);
}

template <typename NativeType>
bool set_decoded_field(const ParquetColumnSchema& column_schema, DecodedValueKind value_kind,
                       const NativeType& value, Field* field, const cctz::time_zone* timezone) {
    DecodedColumnView view;
    view.value_kind = value_kind;
    view.values = reinterpret_cast<const uint8_t*>(&value);
    return read_decoded_field(column_schema, view, field, timezone).ok();
}

template <typename NativeType>
bool valid_min_max(const NativeType& min_value, const NativeType& max_value) {
    if constexpr (std::is_floating_point_v<NativeType>) {
        // Parquet requires readers to ignore min/max statistics if either bound is NaN.
        return !std::isnan(min_value) && !std::isnan(max_value);
    }
    return true;
}

template <typename ParquetDType>
bool set_decoded_min_max(const std::shared_ptr<::parquet::Statistics>& statistics,
                         const ParquetColumnSchema& column_schema, DecodedValueKind value_kind,
                         ParquetColumnStatistics* column_statistics,
                         const cctz::time_zone* timezone) {
    auto typed_statistics =
            std::static_pointer_cast<::parquet::TypedStatistics<ParquetDType>>(statistics);
    const auto& min_value = typed_statistics->min();
    const auto& max_value = typed_statistics->max();
    if (!valid_min_max(min_value, max_value) ||
        !set_decoded_field(column_schema, value_kind, min_value, &column_statistics->min_value,
                           timezone) ||
        !set_decoded_field(column_schema, value_kind, max_value, &column_statistics->max_value,
                           timezone)) {
        return false;
    }
    return true;
}

bool set_decoded_binary_field(const ParquetColumnSchema& column_schema, DecodedValueKind value_kind,
                              const StringRef& value, Field* field,
                              const cctz::time_zone* timezone) {
    std::vector<StringRef> binary_values {value};
    DecodedColumnView view;
    view.value_kind = value_kind;
    view.binary_values = &binary_values;
    return read_decoded_field(column_schema, view, field, timezone).ok();
}

bool set_string_min_max(const std::shared_ptr<::parquet::Statistics>& statistics,
                        const ParquetColumnSchema& column_schema,
                        ParquetColumnStatistics* column_statistics,
                        const cctz::time_zone* timezone) {
    switch (statistics->physical_type()) {
    case ::parquet::Type::BYTE_ARRAY: {
        auto typed_statistics =
                std::static_pointer_cast<::parquet::TypedStatistics<::parquet::ByteArrayType>>(
                        statistics);
        const auto min = ::parquet::ByteArrayToString(typed_statistics->min());
        const auto max = ::parquet::ByteArrayToString(typed_statistics->max());
        if (!set_decoded_binary_field(column_schema, DecodedValueKind::BINARY,
                                      StringRef(min.data(), min.size()),
                                      &column_statistics->min_value, timezone) ||
            !set_decoded_binary_field(column_schema, DecodedValueKind::BINARY,
                                      StringRef(max.data(), max.size()),
                                      &column_statistics->max_value, timezone)) {
            return false;
        }
        return true;
    }
    case ::parquet::Type::FIXED_LEN_BYTE_ARRAY: {
        if (column_schema.descriptor == nullptr || column_schema.descriptor->type_length() <= 0) {
            return false;
        }
        auto typed_statistics =
                std::static_pointer_cast<::parquet::TypedStatistics<::parquet::FLBAType>>(
                        statistics);
        const int type_length = column_schema.descriptor->type_length();
        const std::string min(reinterpret_cast<const char*>(typed_statistics->min().ptr),
                              type_length);
        const std::string max(reinterpret_cast<const char*>(typed_statistics->max().ptr),
                              type_length);
        if (!set_decoded_binary_field(column_schema, DecodedValueKind::FIXED_BINARY,
                                      StringRef(min.data(), min.size()),
                                      &column_statistics->min_value, timezone) ||
            !set_decoded_binary_field(column_schema, DecodedValueKind::FIXED_BINARY,
                                      StringRef(max.data(), max.size()),
                                      &column_statistics->max_value, timezone)) {
            return false;
        }
        return true;
    }
    default:
        return false;
    }
}

template <typename T>
T load_predicate_value(const char* data) {
    T value;
    memcpy(&value, data, sizeof(T));
    return value;
}

std::optional<int64_t> load_predicate_integral_value(const char* buf, size_t size) {
    switch (size) {
    case sizeof(int8_t):
        return static_cast<int64_t>(load_predicate_value<int8_t>(buf));
    case sizeof(int16_t):
        return static_cast<int64_t>(load_predicate_value<int16_t>(buf));
    case sizeof(int32_t):
        return static_cast<int64_t>(load_predicate_value<int32_t>(buf));
    case sizeof(int64_t):
        return load_predicate_value<int64_t>(buf);
    default:
        return std::nullopt;
    }
}

bool logical_integer_fits_physical_int32(const ParquetTypeDescriptor& type_descriptor,
                                         int64_t value) {
    const int bit_width =
            type_descriptor.integer_bit_width > 0 ? type_descriptor.integer_bit_width : 32;
    if (type_descriptor.is_unsigned_integer) {
        const uint64_t max_value = bit_width >= 32 ? std::numeric_limits<uint32_t>::max()
                                                   : ((uint64_t {1} << bit_width) - 1);
        return value >= 0 && static_cast<uint64_t>(value) <= max_value;
    }
    const int64_t min_value = bit_width >= 32 ? std::numeric_limits<int32_t>::min()
                                              : -(int64_t {1} << (bit_width - 1));
    const int64_t max_value = bit_width >= 32 ? std::numeric_limits<int32_t>::max()
                                              : ((int64_t {1} << (bit_width - 1)) - 1);
    return value >= min_value && value <= max_value;
}

std::optional<int32_t> convert_logical_integer_to_physical_int32(
        const ParquetTypeDescriptor& type_descriptor, int64_t value) {
    if (!logical_integer_fits_physical_int32(type_descriptor, value)) {
        return std::nullopt;
    }
    if (!type_descriptor.is_unsigned_integer) {
        return static_cast<int32_t>(value);
    }
    const auto unsigned_value = static_cast<uint32_t>(value);
    int32_t physical_value;
    memcpy(&physical_value, &unsigned_value, sizeof(physical_value));
    return physical_value;
}

class ArrowParquetBloomFilterAdapter final : public segment_v2::BloomFilter {
public:
    ArrowParquetBloomFilterAdapter(const ParquetColumnSchema& column_schema,
                                   const ::parquet::BloomFilter& bloom_filter)
            : _column_schema(column_schema), _bloom_filter(bloom_filter) {}

    void add_bytes(const char* buf, size_t size) override { DORIS_CHECK(false); }

    bool test_bytes(const char* buf, size_t size) const override {
        if (buf == nullptr) {
            return true;
        }
        // Parquet bloom filters are populated from the physical column carrier, while VExpr
        // literals are materialized as Doris logical values. Keep the logical type in
        // BloomFilterEvalContext for expression compatibility, and normalize to the Parquet
        // physical representation only at this adapter boundary.
        switch (_column_schema.type_descriptor.physical_type) {
        case ::parquet::Type::BOOLEAN:
            return test_boolean(buf, size);
        case ::parquet::Type::INT32:
            return test_physical_int32(buf, size);
        case ::parquet::Type::INT64:
            return test_int64(buf, size);
        case ::parquet::Type::FLOAT:
            return test_float(buf, size);
        case ::parquet::Type::DOUBLE:
            return test_double(buf, size);
        case ::parquet::Type::BYTE_ARRAY:
            return test_byte_array(buf, size);
        case ::parquet::Type::FIXED_LEN_BYTE_ARRAY:
            return test_fixed_len_byte_array(buf, size);
        default:
            return true;
        }
    }

    void set_has_null(bool has_null) override { DORIS_CHECK(!has_null); }
    bool has_null() const override { return false; }
    void add_hash(uint64_t hash) override { DORIS_CHECK(false); }
    bool test_hash(uint64_t hash) const override { return _bloom_filter.FindHash(hash); }

private:
    bool test_boolean(const char* buf, size_t size) const {
        if (size == sizeof(bool)) {
            const int32_t value = load_predicate_value<bool>(buf) ? 1 : 0;
            return _bloom_filter.FindHash(_bloom_filter.Hash(value));
        }
        if (size == sizeof(int32_t)) {
            const int32_t value = load_predicate_value<int32_t>(buf);
            return _bloom_filter.FindHash(_bloom_filter.Hash(value != 0 ? 1 : 0));
        }
        return true;
    }

    bool test_physical_int32(const char* buf, size_t size) const {
        const auto logical_value = load_predicate_integral_value(buf, size);
        if (!logical_value.has_value()) {
            return true;
        }
        const auto physical_value = convert_logical_integer_to_physical_int32(
                _column_schema.type_descriptor, *logical_value);
        if (!physical_value.has_value()) {
            return false;
        }
        return find_int32(*physical_value);
    }

    bool test_int64(const char* buf, size_t size) const {
        if (size != sizeof(int64_t)) {
            return true;
        }
        const int64_t value = load_predicate_value<int64_t>(buf);
        return _bloom_filter.FindHash(_bloom_filter.Hash(value));
    }

    bool test_float(const char* buf, size_t size) const {
        if (size != sizeof(float)) {
            return true;
        }
        const float value = load_predicate_value<float>(buf);
        return _bloom_filter.FindHash(_bloom_filter.Hash(value));
    }

    bool test_double(const char* buf, size_t size) const {
        if (size != sizeof(double)) {
            return true;
        }
        const double value = load_predicate_value<double>(buf);
        return _bloom_filter.FindHash(_bloom_filter.Hash(value));
    }

    bool test_byte_array(const char* buf, size_t size) const {
        ::parquet::ByteArray value(static_cast<uint32_t>(size),
                                   reinterpret_cast<const uint8_t*>(buf));
        return _bloom_filter.FindHash(_bloom_filter.Hash(&value));
    }

    bool test_fixed_len_byte_array(const char* buf, size_t size) const {
        if (_column_schema.type_descriptor.fixed_length <= 0) {
            return true;
        }
        if (size != static_cast<size_t>(_column_schema.type_descriptor.fixed_length)) {
            return false;
        }
        ::parquet::FLBA value(reinterpret_cast<const uint8_t*>(buf));
        return _bloom_filter.FindHash(
                _bloom_filter.Hash(&value, _column_schema.type_descriptor.fixed_length));
    }

    bool find_int32(int32_t value) const {
        return _bloom_filter.FindHash(_bloom_filter.Hash(value));
    }

    const ParquetColumnSchema& _column_schema;
    const ::parquet::BloomFilter& _bloom_filter;
};

bool bloom_filter_supported(const ParquetColumnSchema& column_schema) {
    if (!bloom_logical_type_supported(column_schema)) {
        return false;
    }
    switch (column_schema.type_descriptor.physical_type) {
    case ::parquet::Type::BOOLEAN:
    case ::parquet::Type::INT32:
    case ::parquet::Type::INT64:
    case ::parquet::Type::FLOAT:
    case ::parquet::Type::DOUBLE:
    case ::parquet::Type::BYTE_ARRAY:
        return true;
    case ::parquet::Type::FIXED_LEN_BYTE_ARRAY:
        return column_schema.type_descriptor.is_string_like &&
               column_schema.type_descriptor.fixed_length > 0;
    default:
        return false;
    }
}

bool bloom_filter_excludes(const ParquetColumnSchema& column_schema, int slot_index,
                           const VExprContextSPtrs& conjuncts,
                           const ::parquet::BloomFilter& bloom_filter) {
    if (!bloom_filter_supported(column_schema)) {
        return false;
    }
    ArrowParquetBloomFilterAdapter adapter(column_schema, bloom_filter);
    BloomFilterEvalContext ctx;
    ctx.slots.emplace(slot_index, BloomFilterEvalContext::SlotBloomFilter {
                                          .data_type = column_schema.type,
                                          .bloom_filter = &adapter,
                                  });
    return VExprContext::evaluate_bloom_filter(conjuncts, ctx) == ZoneMapFilterResult::kNoMatch;
}

struct RowGroupBloomFilterCache {
    ::parquet::BloomFilterReader* bloom_filter_reader = nullptr;
    std::map<int, std::unique_ptr<::parquet::BloomFilter>> column_bloom_filters;
    std::set<int> loaded_columns;

    ::parquet::BloomFilter* get(int row_group_idx, int leaf_column_id,
                                ParquetPruningStats* pruning_stats) {
        if (bloom_filter_reader == nullptr || leaf_column_id < 0) {
            return nullptr;
        }
        if (loaded_columns.find(leaf_column_id) == loaded_columns.end()) {
            loaded_columns.insert(leaf_column_id);
            try {
                std::shared_ptr<::parquet::RowGroupBloomFilterReader> row_group_reader;
                if (pruning_stats != nullptr) {
                    SCOPED_RAW_TIMER(&pruning_stats->bloom_filter_read_time);
                    row_group_reader = bloom_filter_reader->RowGroup(row_group_idx);
                    if (row_group_reader != nullptr) {
                        column_bloom_filters[leaf_column_id] =
                                row_group_reader->GetColumnBloomFilter(leaf_column_id);
                    }
                } else {
                    row_group_reader = bloom_filter_reader->RowGroup(row_group_idx);
                    if (row_group_reader != nullptr) {
                        column_bloom_filters[leaf_column_id] =
                                row_group_reader->GetColumnBloomFilter(leaf_column_id);
                    }
                }
            } catch (const ::parquet::ParquetException&) {
                return nullptr;
            } catch (const std::exception&) {
                return nullptr;
            }
        }
        auto it = column_bloom_filters.find(leaf_column_id);
        return it == column_bloom_filters.end() ? nullptr : it->second.get();
    }
};

bool is_dictionary_data_encoding(::parquet::Encoding::type encoding) {
    return encoding == ::parquet::Encoding::PLAIN_DICTIONARY ||
           encoding == ::parquet::Encoding::RLE_DICTIONARY;
}

bool is_level_encoding(::parquet::Encoding::type encoding) {
    return encoding == ::parquet::Encoding::RLE || encoding == ::parquet::Encoding::BIT_PACKED;
}

bool is_data_page_type(::parquet::PageType::type page_type) {
    return page_type == ::parquet::PageType::DATA_PAGE ||
           page_type == ::parquet::PageType::DATA_PAGE_V2;
}

bool is_dictionary_encoded_chunk(const ::parquet::ColumnChunkMetaData& column_metadata) {
    if (!column_metadata.has_dictionary_page()) {
        return false;
    }

    const auto& encoding_stats = column_metadata.encoding_stats();
    if (!encoding_stats.empty()) {
        bool has_dictionary_data_page = false;
        for (const auto& encoding_stat : encoding_stats) {
            if (!is_data_page_type(encoding_stat.page_type) || encoding_stat.count <= 0) {
                continue;
            }
            if (!is_dictionary_data_encoding(encoding_stat.encoding)) {
                return false;
            }
            has_dictionary_data_page = true;
        }
        return has_dictionary_data_page;
    }

    bool has_dictionary_encoding = false;
    for (const auto encoding : column_metadata.encodings()) {
        if (is_dictionary_data_encoding(encoding)) {
            has_dictionary_encoding = true;
            continue;
        }
        if (!is_level_encoding(encoding)) {
            return false;
        }
    }
    return has_dictionary_encoding;
}

bool supports_dictionary_pruning(const ParquetColumnSchema& column_schema,
                                 const ::parquet::ColumnChunkMetaData& column_metadata) {
    if (column_schema.kind != ParquetColumnSchemaKind::PRIMITIVE ||
        column_schema.descriptor == nullptr || column_schema.type == nullptr) {
        return false;
    }
    if (!column_schema.type_descriptor.is_string_like) {
        return false;
    }
    if (column_metadata.type() != ::parquet::Type::BYTE_ARRAY &&
        column_metadata.type() != ::parquet::Type::FIXED_LEN_BYTE_ARRAY) {
        return false;
    }
    return true;
}

} // namespace

bool read_dictionary_words(::parquet::ParquetFileReader* file_reader, int row_group_idx,
                           int leaf_column_id, const ParquetColumnSchema& column_schema,
                           ParquetDictionaryWords* dict_words) {
    DORIS_CHECK(dict_words != nullptr);
    dict_words->clear();
    if (file_reader == nullptr || leaf_column_id < 0) {
        return false;
    }

    auto row_group_reader = file_reader->RowGroup(row_group_idx);
    if (row_group_reader == nullptr) {
        return false;
    }
    auto page_reader = row_group_reader->GetColumnPageReader(leaf_column_id);
    if (page_reader == nullptr) {
        return false;
    }

    std::shared_ptr<::parquet::Page> page;
    try {
        page = page_reader->NextPage();
    } catch (const ::parquet::ParquetException&) {
        return false;
    } catch (const std::exception&) {
        return false;
    }
    if (page == nullptr || page->type() != ::parquet::PageType::DICTIONARY_PAGE) {
        return false;
    }
    const auto* dictionary_page = static_cast<const ::parquet::DictionaryPage*>(page.get());
    if (dictionary_page->encoding() != ::parquet::Encoding::PLAIN &&
        dictionary_page->encoding() != ::parquet::Encoding::PLAIN_DICTIONARY) {
        return false;
    }
    const int32_t dictionary_length = dictionary_page->num_values();
    if (dictionary_length <= 0) {
        return false;
    }
    const auto* dictionary_data = dictionary_page->data();
    const int dictionary_size = dictionary_page->size();

    dict_words->values.reserve(static_cast<size_t>(dictionary_length));
    if (column_schema.descriptor->physical_type() == ::parquet::Type::BYTE_ARRAY) {
        auto decoder = ::parquet::MakeTypedDecoder<::parquet::ByteArrayType>(
                ::parquet::Encoding::PLAIN, column_schema.descriptor);
        decoder->SetData(dictionary_length, dictionary_data, dictionary_size);
        std::vector<::parquet::ByteArray> byte_array_values(static_cast<size_t>(dictionary_length));
        if (decoder->Decode(byte_array_values.data(), dictionary_length) != dictionary_length) {
            return false;
        }
        for (int32_t dict_idx = 0; dict_idx < dictionary_length; ++dict_idx) {
            dict_words->values.emplace_back(
                    reinterpret_cast<const char*>(byte_array_values[dict_idx].ptr),
                    byte_array_values[dict_idx].len);
        }
        dict_words->build_refs();
        return true;
    }
    if (column_schema.descriptor->physical_type() == ::parquet::Type::FIXED_LEN_BYTE_ARRAY) {
        const int type_length = column_schema.descriptor->type_length();
        if (type_length <= 0) {
            return false;
        }
        auto decoder = ::parquet::MakeTypedDecoder<::parquet::FLBAType>(::parquet::Encoding::PLAIN,
                                                                        column_schema.descriptor);
        decoder->SetData(dictionary_length, dictionary_data, dictionary_size);
        std::vector<::parquet::FixedLenByteArray> flba_values(
                static_cast<size_t>(dictionary_length));
        if (decoder->Decode(flba_values.data(), dictionary_length) != dictionary_length) {
            return false;
        }
        for (int32_t dict_idx = 0; dict_idx < dictionary_length; ++dict_idx) {
            dict_words->values.emplace_back(
                    reinterpret_cast<const char*>(flba_values[dict_idx].ptr), type_length);
        }
        dict_words->build_refs();
        return true;
    }
    return false;
}

std::vector<Field> dictionary_fields_from_words(const ParquetDictionaryWords& dict_words) {
    std::vector<Field> fields;
    fields.reserve(dict_words.refs.size());
    for (const auto& ref : dict_words.refs) {
        fields.push_back(Field::create_field<TYPE_STRING>(String(ref.data, ref.size)));
    }
    return fields;
}

namespace {

const ParquetColumnSchema* resolve_local_leaf_schema(
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& schema,
        const format::LocalColumnId file_column_id) {
    if (!file_column_id.is_valid() || file_column_id.value() >= static_cast<int>(schema.size())) {
        return nullptr;
    }
    const ParquetColumnSchema* column_schema = schema[file_column_id.value()].get();
    if (column_schema == nullptr || column_schema->kind != ParquetColumnSchemaKind::PRIMITIVE ||
        column_schema->leaf_column_id < 0 || column_schema->max_repetition_level > 0) {
        return nullptr;
    }
    return column_schema;
}

std::optional<format::LocalColumnId> file_column_id_by_block_position(
        const format::FileScanRequest& request, int block_position) {
    for (const auto& [file_column_id, local_index] : request.local_positions) {
        if (local_index.value() == block_position) {
            return file_column_id;
        }
    }
    return std::nullopt;
}

bool has_expr_zonemap_filter(const format::FileScanRequest& request,
                             const RuntimeState* runtime_state) {
    if (!expr_zonemap::is_expr_zonemap_filter_enabled(runtime_state)) {
        return false;
    }
    for (const auto& conjunct : request.conjuncts) {
        if (conjunct != nullptr && conjunct->root() != nullptr &&
            conjunct->root()->can_evaluate_zonemap_filter()) {
            return true;
        }
    }
    return false;
}

std::set<int> collect_expr_zonemap_slot_indexes(const VExprContextSPtrs& conjuncts) {
    std::set<int> slot_indexes;
    for (const auto& conjunct : conjuncts) {
        if (conjunct != nullptr && conjunct->root() != nullptr &&
            conjunct->root()->can_evaluate_zonemap_filter()) {
            conjunct->root()->collect_slot_column_ids(slot_indexes);
        }
    }
    return slot_indexes;
}

template <typename SlotIndexSelector>
std::map<int, VExprContextSPtrs> collect_conjuncts_by_single_slot(
        const VExprContextSPtrs& conjuncts, SlotIndexSelector slot_index_selector) {
    std::map<int, VExprContextSPtrs> conjuncts_by_slot;
    for (const auto& conjunct : conjuncts) {
        const auto slot_index = slot_index_selector(conjunct);
        if (slot_index >= 0) {
            conjuncts_by_slot[slot_index].push_back(conjunct);
        }
    }
    return conjuncts_by_slot;
}

std::shared_ptr<segment_v2::ZoneMap> make_zonemap_from_statistics(
        const ParquetColumnStatistics& statistics) {
    if (!statistics.has_null_count && !statistics.has_min_max) {
        return nullptr;
    }
    segment_v2::ZoneMap zone_map;
    zone_map.has_null = statistics.has_null;
    zone_map.has_not_null = statistics.has_not_null;
    if (!statistics.has_not_null) {
        return std::make_shared<segment_v2::ZoneMap>(std::move(zone_map));
    }
    if (!statistics.has_min_max) {
        return nullptr;
    }
    zone_map.min_value = statistics.min_value;
    zone_map.max_value = statistics.max_value;
    return std::make_shared<segment_v2::ZoneMap>(std::move(zone_map));
}

void add_slot_zonemap(ZoneMapEvalContext* ctx, int slot_index, const DataTypePtr& data_type,
                      std::shared_ptr<segment_v2::ZoneMap> zone_map) {
    DORIS_CHECK(ctx != nullptr);
    ZoneMapEvalContext::SlotZoneMap slot_zone_map;
    slot_zone_map.data_type = data_type;
    slot_zone_map.zone_map = std::move(zone_map);
    ctx->slots.emplace(slot_index, std::move(slot_zone_map));
}

void accumulate_zonemap_stats(const ZoneMapEvalContext& ctx, ParquetPruningStats* pruning_stats) {
    if (pruning_stats == nullptr) {
        return;
    }
    pruning_stats->expr_zonemap_unusable_evals += ctx.stats.unusable_zonemap_eval_count;
    pruning_stats->in_zonemap_point_check_count += ctx.stats.in_zonemap_point_check_count;
    pruning_stats->in_zonemap_range_only_count += ctx.stats.in_zonemap_range_only_count;
}

} // namespace

ParquetColumnStatistics ParquetStatisticsUtils::TransformColumnStatistics(
        const ParquetColumnSchema& column_schema,
        const std::shared_ptr<::parquet::Statistics>& statistics, const cctz::time_zone* timezone) {
    ParquetColumnStatistics result;
    if (statistics == nullptr) {
        return result;
    }

    result.has_null = statistics->HasNullCount() && statistics->null_count() > 0;
    result.has_not_null = statistics->num_values() > 0 || statistics->HasMinMax();
    result.has_null_count = statistics->HasNullCount();
    if (!result.has_not_null || !statistics->HasMinMax()) {
        return result;
    }

    DORIS_CHECK(column_schema.type != nullptr);
    switch (statistics->physical_type()) {
    case ::parquet::Type::BOOLEAN:
        result.has_min_max = set_decoded_min_max<::parquet::BooleanType>(
                statistics, column_schema, DecodedValueKind::BOOL, &result, timezone);
        return result;
    case ::parquet::Type::INT32:
        result.has_min_max = set_decoded_min_max<::parquet::Int32Type>(
                statistics, column_schema, decoded_value_kind(column_schema.type_descriptor),
                &result, timezone);
        return result;
    case ::parquet::Type::INT64:
        result.has_min_max = set_decoded_min_max<::parquet::Int64Type>(
                statistics, column_schema, decoded_value_kind(column_schema.type_descriptor),
                &result, timezone);
        return result;
    case ::parquet::Type::FLOAT:
        result.has_min_max = set_decoded_min_max<::parquet::FloatType>(
                statistics, column_schema, DecodedValueKind::FLOAT, &result, timezone);
        return result;
    case ::parquet::Type::DOUBLE:
        result.has_min_max = set_decoded_min_max<::parquet::DoubleType>(
                statistics, column_schema, DecodedValueKind::DOUBLE, &result, timezone);
        return result;
    case ::parquet::Type::BYTE_ARRAY:
    case ::parquet::Type::FIXED_LEN_BYTE_ARRAY:
        result.has_min_max = set_string_min_max(statistics, column_schema, &result, timezone);
        return result;
    default:
        return result;
    }
}

bool ParquetStatisticsUtils::BloomFilterExcludes(const ParquetColumnSchema& column_schema,
                                                 int slot_index, const VExprContextSPtrs& conjuncts,
                                                 const ::parquet::BloomFilter& bloom_filter) {
    return bloom_filter_excludes(column_schema, slot_index, conjuncts, bloom_filter);
}

namespace {

ParquetRowGroupPruneReason dictionary_prune_reason(
        const ::parquet::RowGroupMetaData& row_group, ::parquet::ParquetFileReader* file_reader,
        int row_group_idx, const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const format::FileScanRequest& request) {
    const auto conjuncts_by_slot = collect_conjuncts_by_single_slot(
            request.conjuncts, expr_zonemap::single_slot_dictionary_index);
    for (const auto& [slot_index, conjuncts] : conjuncts_by_slot) {
        const auto file_column_id = file_column_id_by_block_position(request, slot_index);
        if (!file_column_id.has_value()) {
            continue;
        }
        const auto* column_schema = resolve_local_leaf_schema(file_schema, *file_column_id);
        if (column_schema == nullptr || column_schema->type == nullptr) {
            continue;
        }
        DCHECK_LT(column_schema->leaf_column_id, row_group.num_columns());
        auto column_chunk = row_group.ColumnChunk(column_schema->leaf_column_id);
        if (column_chunk == nullptr ||
            !supports_dictionary_pruning(*column_schema, *column_chunk) ||
            !is_dictionary_encoded_chunk(*column_chunk)) {
            continue;
        }

        ParquetDictionaryWords dict_words;
        if (!read_dictionary_words(file_reader, row_group_idx, column_schema->leaf_column_id,
                                   *column_schema, &dict_words)) {
            continue;
        }
        DictionaryEvalContext ctx;
        ctx.slots.emplace(slot_index, DictionaryEvalContext::SlotDictionary {
                                              .data_type = column_schema->type,
                                              .values = dictionary_fields_from_words(dict_words),
                                      });
        if (VExprContext::evaluate_dictionary_filter(conjuncts, ctx) ==
            ZoneMapFilterResult::kNoMatch) {
            return ParquetRowGroupPruneReason::DICTIONARY;
        }
    }
    return ParquetRowGroupPruneReason::NONE;
}

ParquetRowGroupPruneReason bloom_filter_prune_reason(
        int row_group_idx, const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const format::FileScanRequest& request, RowGroupBloomFilterCache* bloom_filter_cache,
        ParquetPruningStats* pruning_stats) {
    if (bloom_filter_cache == nullptr) {
        return ParquetRowGroupPruneReason::NONE;
    }
    const auto conjuncts_by_slot = collect_conjuncts_by_single_slot(
            request.conjuncts, expr_zonemap::single_slot_bloom_filter_index);
    for (const auto& [slot_index, conjuncts] : conjuncts_by_slot) {
        const auto file_column_id = file_column_id_by_block_position(request, slot_index);
        if (!file_column_id.has_value()) {
            continue;
        }
        const auto* column_schema = resolve_local_leaf_schema(file_schema, *file_column_id);
        if (column_schema == nullptr || column_schema->type == nullptr ||
            !bloom_filter_supported(*column_schema)) {
            continue;
        }
        auto* bloom_filter = bloom_filter_cache->get(row_group_idx, column_schema->leaf_column_id,
                                                     pruning_stats);
        if (bloom_filter == nullptr) {
            continue;
        }
        if (ParquetStatisticsUtils::BloomFilterExcludes(*column_schema, slot_index, conjuncts,
                                                        *bloom_filter)) {
            return ParquetRowGroupPruneReason::BLOOM_FILTER;
        }
    }
    return ParquetRowGroupPruneReason::NONE;
}

void init_bloom_filter_cache(::parquet::ParquetFileReader* file_reader, bool enable_bloom_filter,
                             RowGroupBloomFilterCache* bloom_filter_cache) {
    DORIS_CHECK(bloom_filter_cache != nullptr);
    if (!enable_bloom_filter || file_reader == nullptr) {
        return;
    }
    try {
        bloom_filter_cache->bloom_filter_reader = &file_reader->GetBloomFilterReader();
    } catch (const ::parquet::ParquetException&) {
        bloom_filter_cache->bloom_filter_reader = nullptr;
    } catch (const std::exception&) {
        bloom_filter_cache->bloom_filter_reader = nullptr;
    }
}

bool check_statistics(const ::parquet::RowGroupMetaData& row_group,
                      const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
                      const format::FileScanRequest& request, ParquetPruningStats* pruning_stats,
                      const cctz::time_zone* timezone) {
    const auto slot_indexes = collect_expr_zonemap_slot_indexes(request.conjuncts);
    if (slot_indexes.empty()) {
        return false;
    }

    ZoneMapEvalContext ctx;
    for (const int slot_index : slot_indexes) {
        const auto file_column_id = file_column_id_by_block_position(request, slot_index);
        if (!file_column_id.has_value()) {
            continue;
        }
        const auto* column_schema = resolve_local_leaf_schema(file_schema, *file_column_id);
        if (column_schema == nullptr || column_schema->type == nullptr) {
            continue;
        }

        std::shared_ptr<segment_v2::ZoneMap> zone_map;
        DCHECK_LT(column_schema->leaf_column_id, row_group.num_columns());
        auto column_chunk = row_group.ColumnChunk(column_schema->leaf_column_id);
        if (column_chunk != nullptr) {
            zone_map =
                    make_zonemap_from_statistics(ParquetStatisticsUtils::TransformColumnStatistics(
                            *column_schema, column_chunk->statistics(), timezone));
        }
        add_slot_zonemap(&ctx, slot_index, column_schema->type, std::move(zone_map));
    }

    const auto result = VExprContext::evaluate_zonemap_filter(request.conjuncts, ctx);
    accumulate_zonemap_stats(ctx, pruning_stats);
    return result == ZoneMapFilterResult::kNoMatch;
}

Status select_row_groups_by_metadata_impl(
        const ::parquet::FileMetaData& metadata, ::parquet::ParquetFileReader* file_reader,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const format::FileScanRequest& request, const std::vector<int>* candidate_row_groups,
        std::vector<int>* selected_row_groups, bool enable_bloom_filter,
        ParquetPruningStats* pruning_stats, const cctz::time_zone* timezone,
        const RuntimeState* runtime_state) {
    int64_t row_group_filter_time_sink = 0;
    SCOPED_RAW_TIMER(pruning_stats == nullptr ? &row_group_filter_time_sink
                                              : &pruning_stats->row_group_filter_time);
    if (selected_row_groups == nullptr) {
        return Status::InvalidArgument("selected_row_groups is null");
    }
    selected_row_groups->clear();

    const int num_row_groups = metadata.num_row_groups();
    if (pruning_stats != nullptr) {
        pruning_stats->total_row_groups = num_row_groups;
    }
    const auto candidate_size = candidate_row_groups == nullptr
                                        ? static_cast<size_t>(num_row_groups)
                                        : candidate_row_groups->size();
    selected_row_groups->reserve(candidate_size);
    RowGroupBloomFilterCache bloom_filter_cache;
    init_bloom_filter_cache(file_reader, enable_bloom_filter, &bloom_filter_cache);
    for (size_t candidate_idx = 0; candidate_idx < candidate_size; ++candidate_idx) {
        const int row_group_idx = candidate_row_groups == nullptr
                                          ? static_cast<int>(candidate_idx)
                                          : (*candidate_row_groups)[candidate_idx];
        DORIS_CHECK(row_group_idx >= 0);
        DORIS_CHECK(row_group_idx < num_row_groups);
        auto row_group = metadata.RowGroup(row_group_idx);
        if (row_group == nullptr) {
            selected_row_groups->push_back(row_group_idx);
            continue;
        }
        ParquetRowGroupPruneReason prune_reason = ParquetRowGroupPruneReason::NONE;
        if (has_expr_zonemap_filter(request, runtime_state) &&
            check_statistics(*row_group, file_schema, request, pruning_stats, timezone)) {
            prune_reason = ParquetRowGroupPruneReason::STATISTICS;
        }

        if (prune_reason == ParquetRowGroupPruneReason::NONE) {
            prune_reason = dictionary_prune_reason(*row_group, file_reader, row_group_idx,
                                                   file_schema, request);
            if (prune_reason == ParquetRowGroupPruneReason::NONE) {
                prune_reason = bloom_filter_prune_reason(row_group_idx, file_schema, request,
                                                         &bloom_filter_cache, pruning_stats);
            }
        }

        if (prune_reason != ParquetRowGroupPruneReason::NONE) {
            if (pruning_stats != nullptr) {
                pruning_stats->filtered_group_rows += row_group->num_rows();
                if (prune_reason == ParquetRowGroupPruneReason::STATISTICS) {
                    ++pruning_stats->filtered_row_groups_by_statistics;
                } else if (prune_reason == ParquetRowGroupPruneReason::DICTIONARY) {
                    ++pruning_stats->filtered_row_groups_by_dictionary;
                } else if (prune_reason == ParquetRowGroupPruneReason::BLOOM_FILTER) {
                    ++pruning_stats->filtered_row_groups_by_bloom_filter;
                }
            }
            continue;
        }
        selected_row_groups->push_back(row_group_idx);
    }
    return Status::OK();
}

} // namespace

Status select_row_groups_by_metadata(
        const ::parquet::FileMetaData& metadata, ::parquet::ParquetFileReader* file_reader,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const format::FileScanRequest& request, const std::vector<int>* candidate_row_groups,
        std::vector<int>* selected_row_groups, bool enable_bloom_filter,
        ParquetPruningStats* pruning_stats, const cctz::time_zone* timezone,
        const RuntimeState* runtime_state) {
    return select_row_groups_by_metadata_impl(
            metadata, file_reader, file_schema, request, candidate_row_groups, selected_row_groups,
            enable_bloom_filter, pruning_stats, timezone, runtime_state);
}

namespace {

template <typename ParquetDType>
bool set_page_decoded_min_max(const std::shared_ptr<::parquet::ColumnIndex>& column_index,
                              const ParquetColumnSchema& column_schema, size_t page_idx,
                              DecodedValueKind value_kind, ParquetColumnStatistics* page_statistics,
                              const cctz::time_zone* timezone) {
    const auto typed_index =
            std::static_pointer_cast<::parquet::TypedColumnIndex<ParquetDType>>(column_index);
    if (page_idx >= typed_index->min_values().size() ||
        page_idx >= typed_index->max_values().size()) {
        return false;
    }
    const auto& min_value = typed_index->min_values()[page_idx];
    const auto& max_value = typed_index->max_values()[page_idx];
    if (!valid_min_max(min_value, max_value)) {
        // A NaN invalidates only this page's bounds, not the ColumnIndex itself. Keep the page
        // conservatively by returning usable null-count statistics with has_min_max=false, while
        // allowing later pages with finite bounds to remain eligible for pruning.
        return true;
    }
    if (!set_decoded_field(column_schema, value_kind, min_value, &page_statistics->min_value,
                           timezone) ||
        !set_decoded_field(column_schema, value_kind, max_value, &page_statistics->max_value,
                           timezone)) {
        return false;
    }
    page_statistics->has_min_max = true;
    return true;
}

bool set_page_string_min_max(const std::shared_ptr<::parquet::ColumnIndex>& column_index,
                             const ParquetColumnSchema& column_schema, size_t page_idx,
                             ParquetColumnStatistics* page_statistics,
                             const cctz::time_zone* timezone) {
    switch (column_schema.descriptor->physical_type()) {
    case ::parquet::Type::BYTE_ARRAY: {
        const auto typed_index =
                std::static_pointer_cast<::parquet::ByteArrayColumnIndex>(column_index);
        if (page_idx >= typed_index->min_values().size() ||
            page_idx >= typed_index->max_values().size()) {
            return false;
        }
        const auto min = ::parquet::ByteArrayToString(typed_index->min_values()[page_idx]);
        const auto max = ::parquet::ByteArrayToString(typed_index->max_values()[page_idx]);
        if (!set_decoded_binary_field(column_schema, DecodedValueKind::BINARY,
                                      StringRef(min.data(), min.size()),
                                      &page_statistics->min_value, timezone) ||
            !set_decoded_binary_field(column_schema, DecodedValueKind::BINARY,
                                      StringRef(max.data(), max.size()),
                                      &page_statistics->max_value, timezone)) {
            return false;
        }
        page_statistics->has_min_max = true;
        return true;
    }
    case ::parquet::Type::FIXED_LEN_BYTE_ARRAY: {
        const int type_length = column_schema.descriptor->type_length();
        if (type_length <= 0) {
            return false;
        }
        const auto typed_index = std::static_pointer_cast<::parquet::FLBAColumnIndex>(column_index);
        if (page_idx >= typed_index->min_values().size() ||
            page_idx >= typed_index->max_values().size()) {
            return false;
        }
        const std::string min(
                reinterpret_cast<const char*>(typed_index->min_values()[page_idx].ptr),
                type_length);
        const std::string max(
                reinterpret_cast<const char*>(typed_index->max_values()[page_idx].ptr),
                type_length);
        if (!set_decoded_binary_field(column_schema, DecodedValueKind::FIXED_BINARY,
                                      StringRef(min.data(), min.size()),
                                      &page_statistics->min_value, timezone) ||
            !set_decoded_binary_field(column_schema, DecodedValueKind::FIXED_BINARY,
                                      StringRef(max.data(), max.size()),
                                      &page_statistics->max_value, timezone)) {
            return false;
        }
        page_statistics->has_min_max = true;
        return true;
    }
    default:
        return false;
    }
}

bool set_page_min_max(const std::shared_ptr<::parquet::ColumnIndex>& column_index,
                      const ParquetColumnSchema& column_schema, size_t page_idx,
                      ParquetColumnStatistics* page_statistics, const cctz::time_zone* timezone) {
    DORIS_CHECK(column_schema.type != nullptr);
    switch (column_schema.descriptor->physical_type()) {
    case ::parquet::Type::BOOLEAN:
        return set_page_decoded_min_max<::parquet::BooleanType>(column_index, column_schema,
                                                                page_idx, DecodedValueKind::BOOL,
                                                                page_statistics, timezone);
    case ::parquet::Type::INT32:
        return set_page_decoded_min_max<::parquet::Int32Type>(
                column_index, column_schema, page_idx,
                decoded_value_kind(column_schema.type_descriptor), page_statistics, timezone);
    case ::parquet::Type::INT64:
        return set_page_decoded_min_max<::parquet::Int64Type>(
                column_index, column_schema, page_idx,
                decoded_value_kind(column_schema.type_descriptor), page_statistics, timezone);
    case ::parquet::Type::FLOAT:
        return set_page_decoded_min_max<::parquet::FloatType>(column_index, column_schema, page_idx,
                                                              DecodedValueKind::FLOAT,
                                                              page_statistics, timezone);
    case ::parquet::Type::DOUBLE:
        return set_page_decoded_min_max<::parquet::DoubleType>(column_index, column_schema,
                                                               page_idx, DecodedValueKind::DOUBLE,
                                                               page_statistics, timezone);
    case ::parquet::Type::BYTE_ARRAY:
    case ::parquet::Type::FIXED_LEN_BYTE_ARRAY:
        return set_page_string_min_max(column_index, column_schema, page_idx, page_statistics,
                                       timezone);
    default:
        return false;
    }
}

bool build_page_statistics(const std::shared_ptr<::parquet::ColumnIndex>& column_index,
                           const ParquetColumnSchema& column_schema, size_t page_idx,
                           ParquetColumnStatistics* page_statistics,
                           const cctz::time_zone* timezone) {
    DORIS_CHECK(page_statistics != nullptr);
    *page_statistics = ParquetColumnStatistics {};

    const auto& null_pages = column_index->null_pages();
    if (!column_index->has_null_counts() || page_idx >= null_pages.size() ||
        page_idx >= column_index->null_counts().size()) {
        return false;
    }

    page_statistics->has_null_count = true;
    page_statistics->has_null = column_index->null_counts()[page_idx] > 0;
    page_statistics->has_not_null = !null_pages[page_idx];
    if (!page_statistics->has_not_null) {
        return true;
    }
    return set_page_min_max(column_index, column_schema, page_idx, page_statistics, timezone);
}

std::vector<RowRange> intersect_ranges(const std::vector<RowRange>& left,
                                       const std::vector<RowRange>& right) {
    std::vector<RowRange> result;
    size_t left_idx = 0;
    size_t right_idx = 0;
    while (left_idx < left.size() && right_idx < right.size()) {
        const int64_t left_start = left[left_idx].start;
        const int64_t left_end = left_start + left[left_idx].length;
        const int64_t right_start = right[right_idx].start;
        const int64_t right_end = right_start + right[right_idx].length;
        const int64_t start = std::max(left_start, right_start);
        const int64_t end = std::min(left_end, right_end);
        if (start < end) {
            result.push_back(RowRange {start, end - start});
        }
        if (left_end < right_end) {
            ++left_idx;
        } else {
            ++right_idx;
        }
    }
    return result;
}

int64_t count_range_rows(const std::vector<RowRange>& ranges) {
    int64_t rows = 0;
    for (const auto& range : ranges) {
        rows += range.length;
    }
    return rows;
}

RowRange page_row_range(const ::parquet::OffsetIndex& offset_index, size_t page_idx,
                        int64_t row_group_rows) {
    const auto& page_locations = offset_index.page_locations();
    const int64_t start = page_locations[page_idx].first_row_index;
    const int64_t end = page_idx + 1 == page_locations.size()
                                ? row_group_rows
                                : page_locations[page_idx + 1].first_row_index;
    DORIS_CHECK(start >= 0);
    DORIS_CHECK(end >= start);
    DORIS_CHECK(end <= row_group_rows);
    return RowRange {start, end - start};
}

void append_row_range(const RowRange& range, std::vector<RowRange>* ranges) {
    if (range.length == 0) {
        return;
    }
    if (!ranges->empty()) {
        auto& previous = ranges->back();
        if (previous.start + previous.length == range.start) {
            previous.length += range.length;
            return;
        }
    }
    ranges->push_back(range);
}

std::optional<
        std::pair<std::shared_ptr<::parquet::ColumnIndex>, std::shared_ptr<::parquet::OffsetIndex>>>
load_page_indexes_for_slot(const std::shared_ptr<::parquet::RowGroupPageIndexReader>& row_group,
                           const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
                           const format::FileScanRequest& request, int slot_index,
                           const ParquetColumnSchema** column_schema) {
    DORIS_CHECK(column_schema != nullptr);
    *column_schema = nullptr;
    const auto file_column_id = file_column_id_by_block_position(request, slot_index);
    if (!file_column_id.has_value()) {
        return std::nullopt;
    }
    *column_schema = resolve_local_leaf_schema(file_schema, *file_column_id);
    if (*column_schema == nullptr || (*column_schema)->descriptor == nullptr) {
        return std::nullopt;
    }

    try {
        auto column_index = row_group->GetColumnIndex((*column_schema)->leaf_column_id);
        auto offset_index = row_group->GetOffsetIndex((*column_schema)->leaf_column_id);
        if (column_index == nullptr || offset_index == nullptr ||
            column_index->null_pages().size() != offset_index->page_locations().size()) {
            return std::nullopt;
        }
        return std::make_pair(std::move(column_index), std::move(offset_index));
    } catch (const ::parquet::ParquetException&) {
        return std::nullopt;
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

bool select_ranges_for_expr_zonemap(
        const std::shared_ptr<::parquet::RowGroupPageIndexReader>& row_group,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const format::FileScanRequest& request, int slot_index, const VExprContextSPtrs& conjuncts,
        int64_t row_group_rows, std::vector<RowRange>* ranges, ParquetPruningStats* pruning_stats,
        const cctz::time_zone* timezone) {
    DORIS_CHECK(ranges != nullptr);
    if (conjuncts.empty()) {
        return false;
    }
    const ParquetColumnSchema* column_schema = nullptr;
    const auto page_indexes =
            load_page_indexes_for_slot(row_group, file_schema, request, slot_index, &column_schema);
    if (!page_indexes.has_value()) {
        return false;
    }
    const auto& [column_index, offset_index] = *page_indexes;

    ranges->clear();
    ZoneMapEvalStats page_stats;
    const auto page_count = offset_index->page_locations().size();
    for (size_t page_idx = 0; page_idx < page_count; ++page_idx) {
        ParquetColumnStatistics page_statistics;
        if (!ParquetStatisticsUtils::TransformColumnIndexStatistics(
                    column_index, *column_schema, page_idx, &page_statistics, timezone)) {
            ranges->clear();
            return false;
        }

        ZoneMapEvalContext ctx;
        add_slot_zonemap(&ctx, slot_index, column_schema->type,
                         make_zonemap_from_statistics(page_statistics));
        const auto result = VExprContext::evaluate_zonemap_filter(conjuncts, ctx);
        page_stats.merge_page_eval_stats(ctx.stats);
        if (result == ZoneMapFilterResult::kNoMatch) {
            continue;
        }
        append_row_range(page_row_range(*offset_index, page_idx, row_group_rows), ranges);
    }
    if (pruning_stats != nullptr) {
        pruning_stats->expr_zonemap_unusable_evals += page_stats.unusable_zonemap_eval_count;
        pruning_stats->in_zonemap_point_check_count += page_stats.in_zonemap_point_check_count;
        pruning_stats->in_zonemap_range_only_count += page_stats.in_zonemap_range_only_count;
    }
    return true;
}

bool ranges_intersect(const std::vector<RowRange>& ranges, const RowRange& range) {
    const int64_t range_end = range.start + range.length;
    for (const auto& selected_range : ranges) {
        const int64_t selected_end = selected_range.start + selected_range.length;
        if (selected_end <= range.start) {
            continue;
        }
        if (selected_range.start >= range_end) {
            return false;
        }
        return true;
    }
    return false;
}

void collect_leaf_schemas(const ParquetColumnSchema& column_schema,
                          const format::LocalColumnIndex* projection,
                          std::vector<const ParquetColumnSchema*>* leaf_schemas) {
    if (column_schema.kind == ParquetColumnSchemaKind::PRIMITIVE) {
        leaf_schemas->push_back(&column_schema);
        return;
    }
    for (const auto& child_schema : column_schema.children) {
        if (!format::is_child_projected(projection, child_schema->local_id)) {
            continue;
        }
        const auto* child_projection =
                format::find_child_projection(projection, child_schema->local_id);
        collect_leaf_schemas(*child_schema, child_projection, leaf_schemas);
    }
}

void collect_request_leaf_schemas(
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const format::FileScanRequest& request,
        std::vector<const ParquetColumnSchema*>* leaf_schemas) {
    std::set<int> seen_leaf_ids;
    auto collect_projection = [&](const format::LocalColumnIndex& projection) {
        const int32_t local_id = projection.local_id();
        if (local_id < 0 || local_id >= static_cast<int32_t>(file_schema.size())) {
            return;
        }
        std::vector<const ParquetColumnSchema*> projection_leaf_schemas;
        collect_leaf_schemas(*file_schema[local_id], &projection, &projection_leaf_schemas);
        for (const auto* leaf_schema : projection_leaf_schemas) {
            DORIS_CHECK(leaf_schema != nullptr);
            if (seen_leaf_ids.insert(leaf_schema->leaf_column_id).second) {
                leaf_schemas->push_back(leaf_schema);
            }
        }
    };
    for (const auto& projection : request.predicate_columns) {
        collect_projection(projection);
    }
    for (const auto& projection : request.non_predicate_columns) {
        collect_projection(projection);
    }
}

bool build_page_skip_plan_for_leaf(
        const std::shared_ptr<::parquet::RowGroupPageIndexReader>& row_group,
        const ParquetColumnSchema& column_schema, const std::vector<RowRange>& selected_ranges,
        int64_t row_group_rows, ParquetPageSkipPlan* page_skip_plan) {
    DORIS_CHECK(page_skip_plan != nullptr);
    *page_skip_plan = ParquetPageSkipPlan {};
    if (column_schema.kind != ParquetColumnSchemaKind::PRIMITIVE ||
        column_schema.descriptor == nullptr || column_schema.leaf_column_id < 0 ||
        column_schema.descriptor->max_repetition_level() != 0) {
        return false;
    }

    std::shared_ptr<::parquet::OffsetIndex> offset_index;
    try {
        offset_index = row_group->GetOffsetIndex(column_schema.leaf_column_id);
    } catch (const ::parquet::ParquetException&) {
        return false;
    } catch (const std::exception&) {
        return false;
    }
    if (offset_index == nullptr) {
        return false;
    }

    const auto page_count = offset_index->page_locations().size();
    page_skip_plan->leaf_column_id = column_schema.leaf_column_id;
    page_skip_plan->skipped_pages.resize(page_count);
    page_skip_plan->skipped_page_compressed_sizes.resize(page_count);
    const auto& page_locations = offset_index->page_locations();
    for (size_t page_idx = 0; page_idx < page_count; ++page_idx) {
        const RowRange row_range = page_row_range(*offset_index, page_idx, row_group_rows);
        if (row_range.length == 0 || ranges_intersect(selected_ranges, row_range)) {
            continue;
        }
        page_skip_plan->skipped_pages[page_idx] = 1;
        page_skip_plan->skipped_page_compressed_sizes[page_idx] =
                page_locations[page_idx].compressed_page_size;
        append_row_range(row_range, &page_skip_plan->skipped_ranges);
    }
    if (page_skip_plan->empty()) {
        *page_skip_plan = ParquetPageSkipPlan {};
        return false;
    }
    return true;
}

void build_page_skip_plans(const std::shared_ptr<::parquet::RowGroupPageIndexReader>& row_group,
                           const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
                           const format::FileScanRequest& request,
                           const std::vector<RowRange>& selected_ranges, int64_t row_group_rows,
                           std::map<int, ParquetPageSkipPlan>* page_skip_plans) {
    DORIS_CHECK(page_skip_plans != nullptr);
    page_skip_plans->clear();
    std::vector<const ParquetColumnSchema*> leaf_schemas;
    collect_request_leaf_schemas(file_schema, request, &leaf_schemas);
    for (const auto* leaf_schema : leaf_schemas) {
        DORIS_CHECK(leaf_schema != nullptr);
        ParquetPageSkipPlan page_skip_plan;
        if (build_page_skip_plan_for_leaf(row_group, *leaf_schema, selected_ranges, row_group_rows,
                                          &page_skip_plan)) {
            page_skip_plans->emplace(page_skip_plan.leaf_column_id, std::move(page_skip_plan));
        }
    }
}

} // namespace

bool ParquetStatisticsUtils::TransformColumnIndexStatistics(
        const std::shared_ptr<::parquet::ColumnIndex>& column_index,
        const ParquetColumnSchema& column_schema, size_t page_idx,
        ParquetColumnStatistics* page_statistics, const cctz::time_zone* timezone) {
    return build_page_statistics(column_index, column_schema, page_idx, page_statistics, timezone);
}

Status select_row_group_ranges_by_page_index(
        ::parquet::ParquetFileReader* file_reader,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const format::FileScanRequest& request, int row_group_idx, int64_t row_group_rows,
        std::vector<RowRange>* selected_ranges, std::map<int, ParquetPageSkipPlan>* page_skip_plans,
        ParquetPruningStats* pruning_stats, const cctz::time_zone* timezone,
        const RuntimeState* runtime_state) {
    int64_t page_index_filter_time_sink = 0;
    SCOPED_RAW_TIMER(pruning_stats == nullptr ? &page_index_filter_time_sink
                                              : &pruning_stats->page_index_filter_time);
    DORIS_CHECK(selected_ranges != nullptr);
    selected_ranges->clear();
    if (page_skip_plans != nullptr) {
        page_skip_plans->clear();
    }
    if (row_group_rows <= 0) {
        return Status::OK();
    }
    selected_ranges->push_back(RowRange {0, row_group_rows});
    if (!config::enable_parquet_page_index || !has_expr_zonemap_filter(request, runtime_state) ||
        file_reader == nullptr) {
        return Status::OK();
    }

    std::shared_ptr<::parquet::PageIndexReader> page_index_reader;
    std::shared_ptr<::parquet::RowGroupPageIndexReader> row_group_index_reader;
    try {
        if (pruning_stats != nullptr) {
            ++pruning_stats->page_index_read_calls;
        }
        {
            int64_t read_page_index_time_sink = 0;
            SCOPED_RAW_TIMER(pruning_stats == nullptr ? &read_page_index_time_sink
                                                      : &pruning_stats->read_page_index_time);
            page_index_reader = file_reader->GetPageIndexReader();
            if (page_index_reader == nullptr) {
                return Status::OK();
            }
            row_group_index_reader = page_index_reader->RowGroup(row_group_idx);
        }
    } catch (const ::parquet::ParquetException&) {
        return Status::OK();
    } catch (const std::exception&) {
        return Status::OK();
    }
    if (row_group_index_reader == nullptr) {
        return Status::OK();
    }

    std::map<int, VExprContextSPtrs> conjuncts_by_slot;
    for (const auto& conjunct : request.conjuncts) {
        const auto slot_index = expr_zonemap::single_slot_zonemap_index(conjunct);
        if (slot_index >= 0) {
            conjuncts_by_slot[slot_index].push_back(conjunct);
        }
    }

    for (const auto& [slot_index, conjuncts] : conjuncts_by_slot) {
        std::vector<RowRange> filter_ranges;
        if (!select_ranges_for_expr_zonemap(row_group_index_reader, file_schema, request,
                                            slot_index, conjuncts, row_group_rows, &filter_ranges,
                                            pruning_stats, timezone)) {
            continue;
        }
        *selected_ranges = intersect_ranges(*selected_ranges, filter_ranges);
        if (selected_ranges->empty()) {
            if (page_skip_plans != nullptr) {
                page_skip_plans->clear();
            }
            if (pruning_stats != nullptr) {
                pruning_stats->filtered_page_rows += row_group_rows;
                ++pruning_stats->filtered_row_groups_by_page_index;
            }
            return Status::OK();
        }
    }
    if (page_skip_plans != nullptr) {
        build_page_skip_plans(row_group_index_reader, file_schema, request, *selected_ranges,
                              row_group_rows, page_skip_plans);
    }
    if (pruning_stats != nullptr) {
        const int64_t selected_rows = count_range_rows(*selected_ranges);
        DORIS_CHECK(selected_rows <= row_group_rows);
        pruning_stats->filtered_page_rows += row_group_rows - selected_rows;
    }
    return Status::OK();
}

} // namespace doris::format::parquet
