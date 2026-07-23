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

#include <gtest/gtest.h>
#include <parquet/encoding.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include <array>
#include <cstring>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <vector>

#include "core/custom_allocator.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type_serde/parquet_timestamp.h"
#include "exprs/vectorized_fn_call.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "format_v2/parquet/reader/native/byte_array_dict_decoder.h"
#include "format_v2/parquet/reader/native/column_reader.h"
#include "format_v2/parquet/reader/native/decoder.h"
#include "format_v2/parquet/reader/native/delta_bit_pack_decoder.h"
#include "format_v2/parquet/reader/native/fix_length_plain_decoder.h"
#include "format_v2/parquet/reader/native/level_decoder.h"
#include "format_v2/parquet/reader/native/level_reader.h"
#include "format_v2/parquet/reader/native/page_reader.h"
#include "io/fs/buffered_reader.h"
#include "io/fs/file_reader.h"
#include "util/block_compression.h"
#include "util/coding.h"
#include "util/faststring.h"
#include "util/rle_encoding.h"
#include "util/thrift_util.h"
#include "util/timezone_utils.h"

namespace doris::format::parquet::native {
namespace {

VExprSPtr create_int32_raw_comparison(int column_id, const std::string& function_name,
                                      TExprOpcode::type opcode, int32_t literal,
                                      bool literal_on_left = false) {
    const auto int_type = std::make_shared<DataTypeInt32>();
    TFunctionName fn_name;
    fn_name.__set_function_name(function_name);
    TFunction fn;
    fn.__set_name(fn_name);
    fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
    fn.__set_arg_types({int_type->to_thrift(), int_type->to_thrift()});
    fn.__set_ret_type(std::make_shared<DataTypeUInt8>()->to_thrift());
    fn.__set_has_var_args(false);
    TExprNode node;
    node.__set_node_type(TExprNodeType::BINARY_PRED);
    node.__set_opcode(opcode);
    node.__set_type(std::make_shared<DataTypeUInt8>()->to_thrift());
    node.__set_fn(fn);
    node.__set_num_children(2);
    node.__set_is_nullable(false);
    auto root = VectorizedFnCall::create_shared(node);
    auto slot = VSlotRef::create_shared(column_id, column_id, -1, int_type, "plain_int");
    auto value = VLiteral::create_shared(int_type, Field::create_field<TYPE_INT>(literal));
    if (literal_on_left) {
        root->add_child(value);
        root->add_child(slot);
    } else {
        root->add_child(slot);
        root->add_child(value);
    }
    return root;
}

VExprSPtr create_float_raw_comparison(int column_id, const std::string& function_name,
                                      TExprOpcode::type opcode, float literal) {
    const auto float_type = std::make_shared<DataTypeFloat32>();
    TFunctionName fn_name;
    fn_name.__set_function_name(function_name);
    TFunction fn;
    fn.__set_name(fn_name);
    fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
    fn.__set_arg_types({float_type->to_thrift(), float_type->to_thrift()});
    fn.__set_ret_type(std::make_shared<DataTypeUInt8>()->to_thrift());
    fn.__set_has_var_args(false);
    TExprNode node;
    node.__set_node_type(TExprNodeType::BINARY_PRED);
    node.__set_opcode(opcode);
    node.__set_type(std::make_shared<DataTypeUInt8>()->to_thrift());
    node.__set_fn(fn);
    node.__set_num_children(2);
    node.__set_is_nullable(false);
    auto root = VectorizedFnCall::create_shared(node);
    root->add_child(VSlotRef::create_shared(column_id, column_id, -1, float_type, "plain_float"));
    root->add_child(VLiteral::create_shared(float_type, Field::create_field<TYPE_FLOAT>(literal)));
    return root;
}

TEST(ParquetV2NativeDecoderTest, RawExprEvaluatesPhysicalValuesWithoutColumn) {
    const std::array<int32_t, 6> values = {1, 4, 7, 10, 13, 16};
    std::array<uint8_t, values.size()> matches {};
    matches.fill(1);
    const auto greater_equal = create_int32_raw_comparison(0, "ge", TExprOpcode::GE, 7);
    const auto less_than = create_int32_raw_comparison(0, "lt", TExprOpcode::LT, 16);
    const auto int_type = std::make_shared<DataTypeInt32>();

    ASSERT_TRUE(greater_equal->can_execute_on_raw_fixed_values(int_type, 0));
    ASSERT_TRUE(greater_equal
                        ->execute_on_raw_fixed_values(
                                reinterpret_cast<const uint8_t*>(values.data()), values.size(),
                                sizeof(int32_t), int_type, 0, matches.data())
                        .ok());
    ASSERT_TRUE(less_than
                        ->execute_on_raw_fixed_values(
                                reinterpret_cast<const uint8_t*>(values.data()), values.size(),
                                sizeof(int32_t), int_type, 0, matches.data())
                        .ok());
    EXPECT_EQ(matches, (std::array<uint8_t, values.size()> {0, 0, 1, 1, 1, 0}));

    matches.fill(1);
    const auto literal_less_than_slot =
            create_int32_raw_comparison(0, "lt", TExprOpcode::LT, 7, true);
    ASSERT_TRUE(literal_less_than_slot
                        ->execute_on_raw_fixed_values(
                                reinterpret_cast<const uint8_t*>(values.data()), values.size(),
                                sizeof(int32_t), int_type, 0, matches.data())
                        .ok());
    EXPECT_EQ(matches, (std::array<uint8_t, values.size()> {0, 0, 0, 1, 1, 1}));
}

TEST(ParquetV2NativeDecoderTest, RawExprPreservesFloatNanOrdering) {
    const float nan = std::numeric_limits<float>::quiet_NaN();
    const std::array<float, 4> values {-1, 0, 1, nan};
    const auto float_type = std::make_shared<DataTypeFloat32>();

    std::array<uint8_t, values.size()> matches {};
    matches.fill(1);
    const auto greater_than_zero = create_float_raw_comparison(0, "gt", TExprOpcode::GT, 0);
    ASSERT_TRUE(greater_than_zero
                        ->execute_on_raw_fixed_values(
                                reinterpret_cast<const uint8_t*>(values.data()), values.size(),
                                sizeof(float), float_type, 0, matches.data())
                        .ok());
    EXPECT_EQ(matches, (std::array<uint8_t, values.size()> {0, 0, 1, 1}));

    matches.fill(1);
    const auto equals_nan = create_float_raw_comparison(0, "eq", TExprOpcode::EQ, nan);
    ASSERT_TRUE(equals_nan
                        ->execute_on_raw_fixed_values(
                                reinterpret_cast<const uint8_t*>(values.data()), values.size(),
                                sizeof(float), float_type, 0, matches.data())
                        .ok());
    EXPECT_EQ(matches, (std::array<uint8_t, values.size()> {0, 0, 0, 1}));
}

class RejectFixedConsumer final : public ParquetFixedValueConsumer {
public:
    Status consume(const uint8_t* values, size_t num_values, size_t value_width) override {
        return Status::InternalError("Unexpected fixed dictionary");
    }
};

class CaptureBinaryConsumer final : public ParquetBinaryValueConsumer {
public:
    Status consume(const StringRef* values, size_t num_values) override {
        refs.assign(values, values + num_values);
        return Status::OK();
    }

    std::vector<StringRef> refs;
};

class CapturePlainBinaryLayoutConsumer final : public ParquetBinaryValueConsumer {
public:
    Status consume(const StringRef*, size_t) override {
        legacy_consume_called = true;
        return Status::InternalError("PLAIN BYTE_ARRAY used the legacy StringRef path");
    }

    Status consume_plain_byte_array(
            const char* encoded_data, const uint32_t* payload_offsets,
            const uint32_t* value_offsets, size_t num_values,
            const std::vector<ParquetSelectionRange>& value_spans) override {
        base = encoded_data;
        source_offsets.assign(payload_offsets, payload_offsets + num_values);
        output_offsets.assign(value_offsets, value_offsets + num_values + 1);
        spans = value_spans;
        return Status::OK();
    }

    const char* base = nullptr;
    bool legacy_consume_called = false;
    std::vector<uint32_t> source_offsets;
    std::vector<uint32_t> output_offsets;
    std::vector<ParquetSelectionRange> spans;
};

class CaptureFixedConsumer final : public ParquetFixedValueConsumer {
public:
    Status consume(const uint8_t* values, size_t num_values, size_t value_width) override {
        ++consume_calls;
        if (width == 0) {
            width = value_width;
        }
        DORIS_CHECK_EQ(width, value_width);
        bytes.insert(bytes.end(), values, values + num_values * value_width);
        return Status::OK();
    }

    template <typename T>
    std::vector<T> values() const {
        DORIS_CHECK_EQ(width, sizeof(T));
        DORIS_CHECK_EQ(bytes.size() % sizeof(T), 0);
        std::vector<T> result(bytes.size() / sizeof(T));
        memcpy(result.data(), bytes.data(), bytes.size());
        return result;
    }

    size_t width = 0;
    size_t consume_calls = 0;
    std::vector<uint8_t> bytes;
};

TEST(ParquetV2NativeDecoderTest, FragmentedPlainSelectionUsesOneConsumerBatch) {
    std::array<int32_t, 32> input {};
    std::iota(input.begin(), input.end(), 0);
    Slice data(reinterpret_cast<char*>(input.data()), input.size() * sizeof(int32_t));
    FixLengthPlainDecoder decoder;
    decoder.set_type_length(sizeof(int32_t));
    ASSERT_TRUE(decoder.set_data(&data).ok());

    ParquetSelection selection {
            .total_values = input.size(), .selected_values = input.size() / 2, .ranges = {}};
    for (size_t row = 0; row < input.size(); row += 2) {
        selection.ranges.push_back({.first = row, .count = 1});
    }
    CaptureFixedConsumer consumer;
    ASSERT_TRUE(decoder.decode_selected_fixed_values(selection, consumer).ok());

    EXPECT_EQ(consumer.consume_calls, 1);
    std::vector<int32_t> expected;
    for (int32_t value = 0; value < static_cast<int32_t>(input.size()); value += 2) {
        expected.push_back(value);
    }
    EXPECT_EQ(consumer.values<int32_t>(), expected);
}

class ScriptedDictionaryMaterializationSource final : public ParquetDecodeSource {
public:
    ScriptedDictionaryMaterializationSource(std::vector<uint32_t> ids, bool prefer_indices)
            : _ids(std::move(ids)), _prefer_indices(prefer_indices) {}

    Status decode_fixed_values(size_t, ParquetFixedValueConsumer&) override {
        return Status::NotSupported("scripted dictionary source has no fixed values");
    }

    Status decode_binary_values(size_t, ParquetBinaryValueConsumer&) override {
        return Status::NotSupported("scripted dictionary source has no binary values");
    }

    Status skip_values(size_t) override { return Status::OK(); }

    Status decode_dictionary_indices(size_t num_values, std::vector<uint32_t>* indices) override {
        DORIS_CHECK_EQ(num_values, _ids.size());
        ++index_batches;
        *indices = _ids;
        return Status::OK();
    }

    Status decode_dictionary_values(size_t num_values,
                                    ParquetDictionaryValueConsumer& consumer) override {
        DORIS_CHECK_EQ(num_values, _ids.size());
        ++direct_batches;
        return consumer.consume_indices(_ids.data(), _ids.size());
    }

    bool prefer_dictionary_index_materialization(size_t) const override { return _prefer_indices; }

    size_t direct_batches = 0;
    size_t index_batches = 0;

private:
    std::vector<uint32_t> _ids;
    bool _prefer_indices;
};

const RowRanges& scripted_row_ranges() {
    static const RowRanges ranges;
    return ranges;
}

class ScriptedColumnReader final : public ColumnReader {
public:
    ScriptedColumnReader(size_t rows, size_t values, bool eof, std::vector<level_t> rep_levels,
                         std::vector<level_t> def_levels)
            : ColumnReader(scripted_row_ranges(), rows, nullptr, nullptr),
              _rows(rows),
              _values(values),
              _eof(eof),
              _rep_levels(std::move(rep_levels)),
              _def_levels(std::move(def_levels)) {}

    Status read_column_data(ColumnPtr& column, const DataTypePtr&,
                            const std::shared_ptr<NativeSchemaNode>&, FilterMap&, size_t,
                            size_t* read_rows, bool* eof, bool, int64_t = -1) override {
        if (_used) {
            *read_rows = 0;
            *eof = true;
            return Status::OK();
        }
        column = IColumn::mutate(std::move(column));
        column->assert_mutable()->insert_many_defaults(_values);
        *read_rows = _rows;
        *eof = _eof;
        _used = true;
        return Status::OK();
    }

    Status read_column_levels(FilterMap&, size_t, size_t* read_rows, bool* eof) override {
        *read_rows = _rows;
        *eof = _eof;
        return Status::OK();
    }

    const std::vector<level_t>& get_rep_level() const override { return _rep_levels; }
    const std::vector<level_t>& get_def_level() const override { return _def_levels; }
    ColumnStatistics column_statistics() override { return {}; }
    void close() override {}
    void release_batch_scratch(size_t) override {}
    void reset_filter_map_index() override {}

private:
    size_t _rows;
    size_t _values;
    bool _eof;
    bool _used = false;
    std::vector<level_t> _rep_levels;
    std::vector<level_t> _def_levels;
};

class MemoryBufferedReader final : public io::BufferedStreamReader {
public:
    explicit MemoryBufferedReader(std::vector<uint8_t> data) : _data(std::move(data)) {}

    Status read_bytes(const uint8_t** buf, uint64_t offset, size_t bytes_to_read,
                      const io::IOContext*) override {
        if (offset > _data.size() || bytes_to_read > _data.size() - offset) {
            return Status::IOError("out of bounds");
        }
        *buf = _data.data() + offset;
        return Status::OK();
    }
    Status read_bytes(Slice& slice, uint64_t offset, const io::IOContext*) override {
        if (offset > _data.size() || slice.size > _data.size() - offset) {
            return Status::IOError("out of bounds");
        }
        slice.data = reinterpret_cast<char*>(_data.data() + offset);
        return Status::OK();
    }
    std::string path() override { return "memory.parquet"; }
    int64_t mtime() const override { return 0; }

private:
    std::vector<uint8_t> _data;
};

class NativeDecoderMemoryFileReader final : public io::FileReader {
public:
    explicit NativeDecoderMemoryFileReader(std::vector<uint8_t> data)
            : _data(std::move(data)), _path("native-decoder-memory.parquet") {}

    Status close() override {
        _closed = true;
        return Status::OK();
    }
    const io::Path& path() const override { return _path; }
    size_t size() const override { return _data.size(); }
    bool closed() const override { return _closed; }
    int64_t mtime() const override { return 1; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext*) override {
        if (offset > _data.size() || result.size > _data.size() - offset) {
            return Status::IOError("native decoder memory read exceeds file");
        }
        memcpy(result.data, _data.data() + offset, result.size);
        *bytes_read = result.size;
        return Status::OK();
    }

private:
    std::vector<uint8_t> _data;
    io::Path _path;
    bool _closed = false;
};

std::shared_ptr<::parquet::ColumnDescriptor> descriptor(::parquet::Type::type physical_type) {
    auto node = ::parquet::schema::PrimitiveNode::Make("value", ::parquet::Repetition::REQUIRED,
                                                       physical_type);
    return std::make_shared<::parquet::ColumnDescriptor>(node, 0, 0);
}

DorisUniqueBufferPtr<uint8_t> make_byte_array_dictionary(const std::vector<std::string>& values,
                                                         int32_t* length) {
    size_t total_size = 0;
    for (const auto& value : values) {
        total_size += sizeof(uint32_t) + value.size();
    }
    *length = static_cast<int32_t>(total_size);
    auto dictionary = make_unique_buffer<uint8_t>(total_size);
    size_t offset = 0;
    for (const auto& value : values) {
        encode_fixed32_le(dictionary.get() + offset, static_cast<uint32_t>(value.size()));
        offset += sizeof(uint32_t);
        memcpy(dictionary.get() + offset, value.data(), value.size());
        offset += value.size();
    }
    return dictionary;
}

std::vector<uint8_t> encode_plain_byte_arrays(const std::vector<std::string>& values) {
    size_t total_size = 0;
    for (const auto& value : values) {
        total_size += sizeof(uint32_t) + value.size();
    }
    std::vector<uint8_t> encoded(total_size);
    size_t offset = 0;
    for (const auto& value : values) {
        encode_fixed32_le(encoded.data() + offset, static_cast<uint32_t>(value.size()));
        offset += sizeof(uint32_t);
        memcpy(encoded.data() + offset, value.data(), value.size());
        offset += value.size();
    }
    return encoded;
}

std::vector<uint8_t> serialize_page(tparquet::PageHeader header,
                                    const std::vector<uint8_t>& payload) {
    std::vector<uint8_t> bytes;
    ThriftSerializer serializer(/*compact=*/true, 128);
    DORIS_CHECK(serializer.serialize(&header, &bytes).ok());
    bytes.insert(bytes.end(), payload.begin(), payload.end());
    return bytes;
}

Status materialize_level_only_page(bool data_page_v2, tparquet::Type::type physical_type,
                                   tparquet::Encoding::type encoding, bool all_null) {
    constexpr size_t VALUE_COUNT = 3;
    const std::vector<uint8_t> encoded_levels {static_cast<uint8_t>(VALUE_COUNT << 1),
                                               static_cast<uint8_t>(all_null ? 0 : 1)};
    std::vector<uint8_t> payload;
    tparquet::PageHeader header;
    if (data_page_v2) {
        payload = encoded_levels;
        header.type = tparquet::PageType::DATA_PAGE_V2;
        header.__isset.data_page_header_v2 = true;
        header.data_page_header_v2.__set_num_values(VALUE_COUNT);
        header.data_page_header_v2.__set_num_nulls(all_null ? VALUE_COUNT : 0);
        header.data_page_header_v2.__set_num_rows(VALUE_COUNT);
        header.data_page_header_v2.__set_encoding(encoding);
        header.data_page_header_v2.__set_definition_levels_byte_length(encoded_levels.size());
        header.data_page_header_v2.__set_repetition_levels_byte_length(0);
        header.data_page_header_v2.__set_is_compressed(false);
    } else {
        payload.resize(sizeof(uint32_t));
        encode_fixed32_le(payload.data(), encoded_levels.size());
        payload.insert(payload.end(), encoded_levels.begin(), encoded_levels.end());
        header.type = tparquet::PageType::DATA_PAGE;
        header.__isset.data_page_header = true;
        header.data_page_header.__set_num_values(VALUE_COUNT);
        header.data_page_header.__set_encoding(encoding);
        header.data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
        header.data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);
    }
    header.__set_compressed_page_size(payload.size());
    header.__set_uncompressed_page_size(payload.size());

    auto bytes = serialize_page(header, payload);
    MemoryBufferedReader reader(bytes);
    tparquet::ColumnChunk chunk;
    chunk.meta_data.__set_type(physical_type);
    chunk.meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
    chunk.meta_data.__set_num_values(VALUE_COUNT);
    chunk.meta_data.__set_total_compressed_size(bytes.size());
    chunk.meta_data.__set_data_page_offset(0);
    NativeFieldSchema field;
    field.physical_type = physical_type;
    if (physical_type == tparquet::Type::BOOLEAN) {
        field.data_type = std::make_shared<DataTypeUInt8>();
    } else {
        field.data_type = std::make_shared<DataTypeInt32>();
    }
    field.definition_level = 1;
    field.parquet_schema.__set_type(physical_type);
    field.parquet_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    ParquetPageReadContext page_context(false, "");
    ColumnChunkReader<false, false> chunk_reader(&reader, &chunk, &field, nullptr, VALUE_COUNT,
                                                 nullptr, page_context);
    RETURN_IF_ERROR(chunk_reader.init());
    const auto load_status = chunk_reader.load_page_data();
    EXPECT_TRUE(load_status.ok()) << load_status;
    RETURN_IF_ERROR(load_status);

    level_t level = -1;
    EXPECT_EQ(chunk_reader.def_level_decoder().get_next_run(&level, VALUE_COUNT), VALUE_COUNT);
    EXPECT_EQ(level, all_null ? 0 : 1);
    FilterMap filter;
    RETURN_IF_ERROR(filter.init(nullptr, VALUE_COUNT, false));
    NullMap null_map;
    ColumnSelectVector select_vector;
    const std::vector<uint16_t> null_runs {static_cast<uint16_t>(all_null ? 0 : VALUE_COUNT),
                                           static_cast<uint16_t>(all_null ? VALUE_COUNT : 0)};
    RETURN_IF_ERROR(select_vector.init(null_runs, VALUE_COUNT, &null_map, &filter, 0));
    auto column = field.data_type->create_column();
    ParquetDecodeContext decode_context;
    static const auto utc = cctz::utc_time_zone();
    RETURN_IF_ERROR(init_decode_context_for_test(field, &utc, &decode_context));
    ParquetMaterializationState state;
    state.enable_strict_mode = true;
    const auto status = chunk_reader.materialize_values(column, *field.data_type->get_serde(),
                                                        decode_context, state, select_vector);
    if (status.ok()) {
        EXPECT_EQ(column->size(), VALUE_COUNT);
        EXPECT_EQ(null_map, NullMap(VALUE_COUNT, all_null ? 1 : 0));
    }
    return status;
}

Status load_scripted_page(tparquet::PageHeader header, const std::vector<uint8_t>& payload,
                          tparquet::CompressionCodec::type codec, bool preload_page_cache = false) {
    std::vector<uint8_t> bytes;
    ThriftSerializer serializer(/*compact=*/true, 128);
    RETURN_IF_ERROR(serializer.serialize(&header, &bytes));
    bytes.insert(bytes.end(), payload.begin(), payload.end());
    const size_t chunk_size = bytes.size();
    const std::string page_cache_file_key = "native-scripted-page-" +
                                            std::to_string(static_cast<int>(header.type)) + "-" +
                                            std::to_string(header.compressed_page_size) + "-" +
                                            std::to_string(header.uncompressed_page_size);
    if (preload_page_cache) {
        auto* page = new DataPage(bytes.size(), true, segment_v2::DATA_PAGE);
        memcpy(page->data(), bytes.data(), bytes.size());
        page->reset_size(bytes.size());
        PageCacheHandle handle;
        StoragePageCache::instance()->insert(
                StoragePageCache::CacheKey(page_cache_file_key, chunk_size, 0), page, &handle,
                segment_v2::DATA_PAGE);
    }
    MemoryBufferedReader reader(std::move(bytes));

    tparquet::ColumnChunk chunk;
    chunk.meta_data.__set_type(tparquet::Type::INT32);
    chunk.meta_data.__set_codec(codec);
    chunk.meta_data.__set_num_values(1);
    chunk.meta_data.__set_total_compressed_size(chunk_size);
    if (header.type == tparquet::PageType::DICTIONARY_PAGE) {
        chunk.meta_data.__set_dictionary_page_offset(0);
        chunk.meta_data.__set_data_page_offset(0);
    } else {
        chunk.meta_data.__set_data_page_offset(0);
    }
    NativeFieldSchema field;
    field.physical_type = tparquet::Type::INT32;
    field.data_type = std::make_shared<DataTypeInt32>();
    field.repetition_level = 0;
    field.definition_level = 0;
    ParquetPageReadContext context(preload_page_cache, page_cache_file_key);
    ColumnChunkReader<false, false> chunk_reader(&reader, &chunk, &field, nullptr, 1, nullptr,
                                                 context);
    RETURN_IF_ERROR(chunk_reader.init());
    return chunk_reader.load_page_data();
}

Status load_malformed_nested_page(tparquet::PageHeader header, const std::vector<uint8_t>& payload,
                                  int64_t metadata_values = std::numeric_limits<int32_t>::max(),
                                  level_t max_repetition_level = 1) {
    auto bytes = serialize_page(header, payload);
    MemoryBufferedReader reader(bytes);
    tparquet::ColumnChunk chunk;
    chunk.meta_data.__set_type(tparquet::Type::INT32);
    chunk.meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
    chunk.meta_data.__set_num_values(metadata_values);
    chunk.meta_data.__set_total_compressed_size(bytes.size());
    chunk.meta_data.__set_data_page_offset(0);
    NativeFieldSchema field;
    field.physical_type = tparquet::Type::INT32;
    field.repetition_level = max_repetition_level;
    field.definition_level = 0;
    ParquetPageReadContext context(false, "");
    ColumnChunkReader<true, false> chunk_reader(&reader, &chunk, &field, nullptr, 1, nullptr,
                                                context);
    RETURN_IF_ERROR(chunk_reader.init());
    RETURN_IF_ERROR(chunk_reader.load_page_data());
    std::vector<level_t> rep_levels;
    size_t result_rows = 0;
    bool cross_page = false;
    return chunk_reader.load_page_nested_rows(rep_levels, 1, &result_rows, &cross_page);
}

Status materialize_plain_int96(const std::vector<ParquetInt96Timestamp>& values,
                               const std::vector<uint8_t>& filter_values = {}) {
    tparquet::PageHeader header;
    header.type = tparquet::PageType::DATA_PAGE;
    header.__set_compressed_page_size(values.size() * sizeof(ParquetInt96Timestamp));
    header.__set_uncompressed_page_size(values.size() * sizeof(ParquetInt96Timestamp));
    header.__isset.data_page_header = true;
    header.data_page_header.__set_num_values(values.size());
    header.data_page_header.__set_encoding(tparquet::Encoding::PLAIN);
    header.data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
    header.data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);
    auto bytes = serialize_page(
            header, std::vector<uint8_t>(reinterpret_cast<const uint8_t*>(values.data()),
                                         reinterpret_cast<const uint8_t*>(values.data()) +
                                                 values.size() * sizeof(ParquetInt96Timestamp)));
    MemoryBufferedReader reader(bytes);
    tparquet::ColumnChunk chunk;
    chunk.meta_data.__set_type(tparquet::Type::INT96);
    chunk.meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
    chunk.meta_data.__set_num_values(values.size());
    chunk.meta_data.__set_total_compressed_size(bytes.size());
    chunk.meta_data.__set_data_page_offset(0);
    NativeFieldSchema field;
    field.physical_type = tparquet::Type::INT96;
    ParquetPageReadContext page_context(false, "");
    ColumnChunkReader<false, false> chunk_reader(&reader, &chunk, &field, nullptr, values.size(),
                                                 nullptr, page_context);
    RETURN_IF_ERROR(chunk_reader.init());
    RETURN_IF_ERROR(chunk_reader.load_page_data());

    DataTypeDateTimeV2 type(6);
    auto column = type.create_column();
    ParquetDecodeContext decode_context;
    decode_context.physical_type = ParquetPhysicalType::INT96;
    static const auto utc = cctz::utc_time_zone();
    decode_context.timezone = &utc;
    ParquetMaterializationState state;
    state.enable_strict_mode = true;
    FilterMap filter;
    RETURN_IF_ERROR(filter.init(filter_values.empty() ? nullptr : filter_values.data(),
                                filter_values.size(), false));
    ColumnSelectVector select_vector;
    const std::vector<uint16_t> run_length_null_map {static_cast<uint16_t>(values.size()), 0};
    RETURN_IF_ERROR(select_vector.init(run_length_null_map, values.size(), nullptr, &filter, 0));
    return chunk_reader.materialize_values(column, *type.get_serde(), decode_context, state,
                                           select_vector);
}

template <typename PhysicalType, typename DataType>
Status materialize_selected_plain_fixed(
        const std::vector<PhysicalType>& physical_values, size_t logical_values,
        const std::vector<uint16_t>& run_length_null_map, const std::vector<uint8_t>& filter_values,
        tparquet::Type::type parquet_physical_type, ParquetPhysicalType decode_physical_type,
        const DataType& type, MutableColumnPtr* column, NullMap* null_map,
        ColumnChunkReaderStatistics* statistics, bool strict_mode = false,
        const ParquetDecodeContext* context_override = nullptr) {
    tparquet::PageHeader header;
    header.type = tparquet::PageType::DATA_PAGE;
    header.__set_compressed_page_size(physical_values.size() * sizeof(PhysicalType));
    header.__set_uncompressed_page_size(physical_values.size() * sizeof(PhysicalType));
    header.__isset.data_page_header = true;
    header.data_page_header.__set_num_values(static_cast<int32_t>(logical_values));
    header.data_page_header.__set_encoding(tparquet::Encoding::PLAIN);
    header.data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
    header.data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);
    std::vector<uint8_t> payload(physical_values.size() * sizeof(PhysicalType));
    if (!payload.empty()) {
        memcpy(payload.data(), physical_values.data(), payload.size());
    }
    auto bytes = serialize_page(header, payload);

    MemoryBufferedReader reader(bytes);
    tparquet::ColumnChunk chunk;
    chunk.meta_data.__set_type(parquet_physical_type);
    chunk.meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
    chunk.meta_data.__set_num_values(static_cast<int64_t>(logical_values));
    chunk.meta_data.__set_total_compressed_size(bytes.size());
    chunk.meta_data.__set_data_page_offset(0);
    NativeFieldSchema field;
    field.physical_type = parquet_physical_type;
    ParquetPageReadContext page_context(false, "");
    ColumnChunkReader<false, false> chunk_reader(&reader, &chunk, &field, nullptr, logical_values,
                                                 nullptr, page_context);
    RETURN_IF_ERROR(chunk_reader.init());
    RETURN_IF_ERROR(chunk_reader.load_page_data());

    ParquetDecodeContext decode_context;
    decode_context.physical_type = decode_physical_type;
    if (context_override != nullptr) {
        decode_context = *context_override;
    }
    ParquetMaterializationState state;
    state.enable_strict_mode = strict_mode;
    state.conversion_failure_null_map = null_map;
    FilterMap filter;
    RETURN_IF_ERROR(filter.init(filter_values.data(), filter_values.size(), false));
    ColumnSelectVector select_vector;
    RETURN_IF_ERROR(select_vector.init(run_length_null_map, logical_values, null_map, &filter, 0));
    RETURN_IF_ERROR(chunk_reader.materialize_values(*column, *type.get_serde(), decode_context,
                                                    state, select_vector));
    *statistics = chunk_reader.statistics();
    return Status::OK();
}

template <typename DataType>
Status materialize_selected_plain_int32(const std::vector<int32_t>& physical_values,
                                        size_t logical_values,
                                        const std::vector<uint16_t>& run_length_null_map,
                                        const std::vector<uint8_t>& filter_values,
                                        const DataType& type, MutableColumnPtr* column,
                                        NullMap* null_map, ColumnChunkReaderStatistics* statistics,
                                        bool strict_mode = false,
                                        const ParquetDecodeContext* context_override = nullptr) {
    return materialize_selected_plain_fixed(physical_values, logical_values, run_length_null_map,
                                            filter_values, tparquet::Type::INT32,
                                            ParquetPhysicalType::INT32, type, column, null_map,
                                            statistics, strict_mode, context_override);
}

template <typename DataType>
Status materialize_selected_plain_int64(const std::vector<int64_t>& physical_values,
                                        size_t logical_values,
                                        const std::vector<uint16_t>& run_length_null_map,
                                        const std::vector<uint8_t>& filter_values,
                                        const DataType& type, MutableColumnPtr* column,
                                        NullMap* null_map, ColumnChunkReaderStatistics* statistics,
                                        const ParquetDecodeContext& context) {
    return materialize_selected_plain_fixed(physical_values, logical_values, run_length_null_map,
                                            filter_values, tparquet::Type::INT64,
                                            ParquetPhysicalType::INT64, type, column, null_map,
                                            statistics, false, &context);
}

template <typename PhysicalType, typename DataType>
Status materialize_selected_dictionary_fixed(
        const std::vector<PhysicalType>& dictionary, const std::vector<uint32_t>& physical_ids,
        size_t logical_values, const std::vector<uint16_t>& run_length_null_map,
        const std::vector<uint8_t>& filter_values, const DataType& type, MutableColumnPtr* column,
        NullMap* null_map, ColumnChunkReaderStatistics* statistics,
        tparquet::Type::type parquet_physical_type, ParquetPhysicalType decode_physical_type,
        const ParquetDecodeContext* context_override = nullptr) {
    std::vector<uint8_t> dictionary_payload(dictionary.size() * sizeof(PhysicalType));
    memcpy(dictionary_payload.data(), dictionary.data(), dictionary_payload.size());
    tparquet::PageHeader dictionary_header;
    dictionary_header.type = tparquet::PageType::DICTIONARY_PAGE;
    dictionary_header.__set_compressed_page_size(dictionary_payload.size());
    dictionary_header.__set_uncompressed_page_size(dictionary_payload.size());
    dictionary_header.__isset.dictionary_page_header = true;
    dictionary_header.dictionary_page_header.__set_num_values(
            static_cast<int32_t>(dictionary.size()));
    dictionary_header.dictionary_page_header.__set_encoding(tparquet::Encoding::PLAIN);
    // Real Parquet files place column chunks after the file header. Keep the dictionary offset
    // non-zero because zero is the metadata sentinel for "no dictionary page".
    std::vector<uint8_t> bytes(1, 0);
    auto dictionary_page = serialize_page(dictionary_header, dictionary_payload);
    bytes.insert(bytes.end(), dictionary_page.begin(), dictionary_page.end());
    const size_t data_page_offset = bytes.size();

    faststring encoded_ids;
    RleEncoder<uint32_t> encoder(&encoded_ids, 4);
    for (const auto id : physical_ids) {
        encoder.Put(id);
    }
    // Parquet bit-packed dictionary runs declare groups of eight; emit the trailing padding so a
    // decoder that buffers the declared run never reads beyond the synthetic page.
    for (size_t padding = physical_ids.size(); padding % 8 != 0; ++padding) {
        encoder.Put(0);
    }
    encoder.Flush();
    std::vector<uint8_t> data_payload(encoded_ids.size() + 1);
    data_payload[0] = 4;
    memcpy(data_payload.data() + 1, encoded_ids.data(), encoded_ids.size());
    tparquet::PageHeader data_header;
    data_header.type = tparquet::PageType::DATA_PAGE;
    data_header.__set_compressed_page_size(data_payload.size());
    data_header.__set_uncompressed_page_size(data_payload.size());
    data_header.__isset.data_page_header = true;
    data_header.data_page_header.__set_num_values(static_cast<int32_t>(logical_values));
    data_header.data_page_header.__set_encoding(tparquet::Encoding::RLE_DICTIONARY);
    data_header.data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
    data_header.data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);
    auto data_page = serialize_page(data_header, data_payload);
    bytes.insert(bytes.end(), data_page.begin(), data_page.end());

    MemoryBufferedReader reader(bytes);
    tparquet::ColumnChunk chunk;
    chunk.meta_data.__set_type(parquet_physical_type);
    chunk.meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
    chunk.meta_data.__set_num_values(static_cast<int64_t>(logical_values));
    chunk.meta_data.__set_total_compressed_size(bytes.size() - 1);
    chunk.meta_data.__set_dictionary_page_offset(1);
    chunk.meta_data.__set_data_page_offset(static_cast<int64_t>(data_page_offset));
    NativeFieldSchema field;
    field.physical_type = parquet_physical_type;
    ParquetPageReadContext page_context(false, "");
    ColumnChunkReader<false, false> chunk_reader(&reader, &chunk, &field, nullptr, logical_values,
                                                 nullptr, page_context);
    RETURN_IF_ERROR(chunk_reader.init());
    RETURN_IF_ERROR(chunk_reader.load_page_data());

    ParquetDecodeContext decode_context;
    decode_context.physical_type = decode_physical_type;
    if (context_override != nullptr) {
        decode_context = *context_override;
    }
    ParquetMaterializationState state;
    state.conversion_failure_null_map = null_map;
    FilterMap filter;
    RETURN_IF_ERROR(filter.init(filter_values.data(), filter_values.size(), false));
    ColumnSelectVector select_vector;
    RETURN_IF_ERROR(select_vector.init(run_length_null_map, logical_values, null_map, &filter, 0));
    RETURN_IF_ERROR(chunk_reader.materialize_values(*column, *type.get_serde(), decode_context,
                                                    state, select_vector));
    *statistics = chunk_reader.statistics();
    return Status::OK();
}

template <typename DataType>
Status materialize_selected_dictionary_int32(
        const std::vector<int32_t>& dictionary, const std::vector<uint32_t>& physical_ids,
        size_t logical_values, const std::vector<uint16_t>& run_length_null_map,
        const std::vector<uint8_t>& filter_values, const DataType& type, MutableColumnPtr* column,
        NullMap* null_map, ColumnChunkReaderStatistics* statistics) {
    return materialize_selected_dictionary_fixed(
            dictionary, physical_ids, logical_values, run_length_null_map, filter_values, type,
            column, null_map, statistics, tparquet::Type::INT32, ParquetPhysicalType::INT32);
}

template <typename DataType>
Status materialize_selected_dictionary_int64(
        const std::vector<int64_t>& dictionary, const std::vector<uint32_t>& physical_ids,
        size_t logical_values, const std::vector<uint16_t>& run_length_null_map,
        const std::vector<uint8_t>& filter_values, const DataType& type, MutableColumnPtr* column,
        NullMap* null_map, ColumnChunkReaderStatistics* statistics,
        const ParquetDecodeContext& context) {
    return materialize_selected_dictionary_fixed(dictionary, physical_ids, logical_values,
                                                 run_length_null_map, filter_values, type, column,
                                                 null_map, statistics, tparquet::Type::INT64,
                                                 ParquetPhysicalType::INT64, &context);
}

Status materialize_selected_dictionary_strings(const std::vector<std::string>& dictionary,
                                               const std::vector<uint32_t>& physical_ids,
                                               size_t logical_values,
                                               const std::vector<uint16_t>& run_length_null_map,
                                               const std::vector<uint8_t>& filter_values,
                                               MutableColumnPtr* column, NullMap* null_map,
                                               ColumnChunkReaderStatistics* statistics) {
    auto dictionary_payload = encode_plain_byte_arrays(dictionary);
    tparquet::PageHeader dictionary_header;
    dictionary_header.type = tparquet::PageType::DICTIONARY_PAGE;
    dictionary_header.__set_compressed_page_size(dictionary_payload.size());
    dictionary_header.__set_uncompressed_page_size(dictionary_payload.size());
    dictionary_header.__isset.dictionary_page_header = true;
    dictionary_header.dictionary_page_header.__set_num_values(
            static_cast<int32_t>(dictionary.size()));
    dictionary_header.dictionary_page_header.__set_encoding(tparquet::Encoding::PLAIN);
    std::vector<uint8_t> bytes(1, 0);
    auto dictionary_page = serialize_page(dictionary_header, dictionary_payload);
    bytes.insert(bytes.end(), dictionary_page.begin(), dictionary_page.end());
    const size_t data_page_offset = bytes.size();

    faststring encoded_ids;
    RleEncoder<uint32_t> encoder(&encoded_ids, 2);
    for (const auto id : physical_ids) {
        encoder.Put(id);
    }
    for (size_t padding = physical_ids.size(); padding % 8 != 0; ++padding) {
        encoder.Put(0);
    }
    encoder.Flush();
    std::vector<uint8_t> data_payload(encoded_ids.size() + 1);
    data_payload[0] = 2;
    memcpy(data_payload.data() + 1, encoded_ids.data(), encoded_ids.size());
    tparquet::PageHeader data_header;
    data_header.type = tparquet::PageType::DATA_PAGE;
    data_header.__set_compressed_page_size(data_payload.size());
    data_header.__set_uncompressed_page_size(data_payload.size());
    data_header.__isset.data_page_header = true;
    data_header.data_page_header.__set_num_values(static_cast<int32_t>(logical_values));
    data_header.data_page_header.__set_encoding(tparquet::Encoding::RLE_DICTIONARY);
    data_header.data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
    data_header.data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);
    auto data_page = serialize_page(data_header, data_payload);
    bytes.insert(bytes.end(), data_page.begin(), data_page.end());

    MemoryBufferedReader reader(bytes);
    tparquet::ColumnChunk chunk;
    chunk.meta_data.__set_type(tparquet::Type::BYTE_ARRAY);
    chunk.meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
    chunk.meta_data.__set_num_values(static_cast<int64_t>(logical_values));
    chunk.meta_data.__set_total_compressed_size(bytes.size() - 1);
    chunk.meta_data.__set_dictionary_page_offset(1);
    chunk.meta_data.__set_data_page_offset(static_cast<int64_t>(data_page_offset));
    NativeFieldSchema field;
    field.physical_type = tparquet::Type::BYTE_ARRAY;
    ParquetPageReadContext page_context(false, "");
    ColumnChunkReader<false, false> chunk_reader(&reader, &chunk, &field, nullptr, logical_values,
                                                 nullptr, page_context);
    RETURN_IF_ERROR(chunk_reader.init());
    RETURN_IF_ERROR(chunk_reader.load_page_data());

    DataTypeString type;
    ParquetDecodeContext decode_context;
    decode_context.physical_type = ParquetPhysicalType::BYTE_ARRAY;
    ParquetMaterializationState state;
    state.conversion_failure_null_map = null_map;
    FilterMap filter;
    RETURN_IF_ERROR(filter.init(filter_values.data(), filter_values.size(), false));
    ColumnSelectVector select_vector;
    RETURN_IF_ERROR(select_vector.init(run_length_null_map, logical_values, null_map, &filter, 0));
    RETURN_IF_ERROR(chunk_reader.materialize_values(*column, *type.get_serde(), decode_context,
                                                    state, select_vector));
    *statistics = chunk_reader.statistics();
    return Status::OK();
}

TEST(ParquetV2NativeDecoderTest, RawExprMapsNullableSparseRowsDirectly) {
    const std::vector<int32_t> physical_values {1, 4, 7, 10, 13};
    constexpr size_t LOGICAL_VALUES = 7;
    tparquet::PageHeader header;
    header.type = tparquet::PageType::DATA_PAGE;
    header.__set_compressed_page_size(physical_values.size() * sizeof(int32_t));
    header.__set_uncompressed_page_size(physical_values.size() * sizeof(int32_t));
    header.__isset.data_page_header = true;
    header.data_page_header.__set_num_values(LOGICAL_VALUES);
    header.data_page_header.__set_encoding(tparquet::Encoding::PLAIN);
    header.data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
    header.data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);
    std::vector<uint8_t> payload(physical_values.size() * sizeof(int32_t));
    memcpy(payload.data(), physical_values.data(), payload.size());
    auto bytes = serialize_page(header, payload);

    MemoryBufferedReader reader(bytes);
    tparquet::ColumnChunk chunk;
    chunk.meta_data.__set_type(tparquet::Type::INT32);
    chunk.meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
    chunk.meta_data.__set_num_values(LOGICAL_VALUES);
    chunk.meta_data.__set_total_compressed_size(bytes.size());
    chunk.meta_data.__set_data_page_offset(0);
    NativeFieldSchema field;
    field.physical_type = tparquet::Type::INT32;
    field.data_type = std::make_shared<DataTypeInt32>();
    ParquetPageReadContext page_context(false, "");
    ColumnChunkReader<false, false> chunk_reader(&reader, &chunk, &field, nullptr, LOGICAL_VALUES,
                                                 nullptr, page_context);
    ASSERT_TRUE(chunk_reader.init().ok());
    ASSERT_TRUE(chunk_reader.load_page_data().ok());

    const std::vector<uint16_t> null_runs {1, 1, 2, 1, 2};
    const std::vector<uint8_t> input_filter {1, 0, 1, 1, 1, 0, 1};
    FilterMap filter;
    ASSERT_TRUE(filter.init(input_filter.data(), input_filter.size(), false).ok());
    ColumnSelectVector select_vector;
    ASSERT_TRUE(select_vector.init(null_runs, LOGICAL_VALUES, nullptr, &filter, 0).ok());
    const VExprSPtrs predicates {create_int32_raw_comparison(0, "ge", TExprOpcode::GE, 4),
                                 create_int32_raw_comparison(0, "lt", TExprOpcode::LT, 13)};
    NullMap selected_nulls;
    IColumn::Filter physical_matches;
    auto projected_column = make_nullable(field.data_type)->create_column();
    IColumn::Filter row_filter;
    bool used_filter = false;
    ASSERT_TRUE(chunk_reader
                        .filter_plain_values(predicates, 0, select_vector, &selected_nulls,
                                             &physical_matches, projected_column.get(), &row_filter,
                                             &used_filter)
                        .ok());

    EXPECT_TRUE(used_filter);
    EXPECT_EQ(selected_nulls, (NullMap {0, 0, 0, 1, 0}));
    EXPECT_EQ(physical_matches, (IColumn::Filter {0, 1, 1, 0}));
    EXPECT_EQ(row_filter, (IColumn::Filter {0, 1, 1, 0, 0}));
    ASSERT_EQ(projected_column->size(), 2);
    EXPECT_FALSE(projected_column->is_null_at(0));
    EXPECT_FALSE(projected_column->is_null_at(1));
    const auto& projected_values =
            assert_cast<const ColumnInt32&>(
                    assert_cast<const ColumnNullable&>(*projected_column).get_nested_column())
                    .get_data();
    EXPECT_EQ(projected_values[0], 4);
    EXPECT_EQ(projected_values[1], 7);
    EXPECT_EQ(chunk_reader.remaining_num_values(), 0);
}

TEST(ParquetV2NativeDecoderTest, NullableSparsePlainPrimitiveSelectionBatchesPhysicalPayload) {
    constexpr size_t LOGICAL_VALUES = 4095;
    std::vector<uint16_t> null_runs(LOGICAL_VALUES, 1);
    std::vector<int32_t> physical_values((LOGICAL_VALUES + 1) / 2);
    std::iota(physical_values.begin(), physical_values.end(), 0);
    std::vector<uint8_t> filter(LOGICAL_VALUES, 0);
    for (const size_t selected_row : {0, 1000, 1001, 2000, 4001}) {
        filter[selected_row] = 1;
    }

    DataTypeInt32 type;
    auto column = type.create_column();
    assert_cast<ColumnInt32&>(*column).get_data().push_back(-7);
    NullMap null_map;
    null_map.push_back(0);
    ColumnChunkReaderStatistics statistics;
    ASSERT_TRUE(materialize_selected_plain_int32(physical_values, LOGICAL_VALUES, null_runs, filter,
                                                 type, &column, &null_map, &statistics)
                        .ok());

    EXPECT_EQ(assert_cast<const ColumnInt32&>(*column).get_data(),
              (ColumnInt32::Container {-7, 0, 500, 0, 1000, 0}));
    EXPECT_EQ(null_map, (NullMap {0, 0, 0, 1, 0, 1}));
    EXPECT_EQ(statistics.hybrid_selection_batches, 1);
    EXPECT_EQ(statistics.hybrid_selection_ranges, 3);
    EXPECT_EQ(statistics.hybrid_selection_null_fallback_batches, 0);
}

TEST(ParquetV2NativeDecoderTest, NullableSparsePlainDecimalSelectionBatchesPhysicalPayload) {
    constexpr size_t LOGICAL_VALUES = 4095;
    std::vector<uint16_t> null_runs(LOGICAL_VALUES, 1);
    std::vector<int32_t> physical_values((LOGICAL_VALUES + 1) / 2);
    std::iota(physical_values.begin(), physical_values.end(), 0);
    std::vector<uint8_t> filter(LOGICAL_VALUES, 0);
    for (const size_t selected_row : {0, 1000, 1001, 2000, 4001}) {
        filter[selected_row] = 1;
    }

    DataTypeDecimal32 type(9, 0);
    auto column = type.create_column();
    NullMap null_map;
    ColumnChunkReaderStatistics statistics;
    const ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT32,
                                        .logical_type = ParquetLogicalType::DECIMAL,
                                        .decimal_precision = 9,
                                        .decimal_scale = 0};
    ASSERT_TRUE(materialize_selected_plain_int32(physical_values, LOGICAL_VALUES, null_runs, filter,
                                                 type, &column, &null_map, &statistics, false,
                                                 &context)
                        .ok());

    const auto& values = assert_cast<const ColumnDecimal32&>(*column).get_data();
    ASSERT_EQ(values.size(), 5);
    EXPECT_EQ(values[0].value, 0);
    EXPECT_EQ(values[1].value, 500);
    EXPECT_EQ(values[2].value, 0);
    EXPECT_EQ(values[3].value, 1000);
    EXPECT_EQ(values[4].value, 0);
    EXPECT_EQ(null_map, (NullMap {0, 0, 1, 0, 1}));
    EXPECT_EQ(statistics.hybrid_selection_batches, 1);
    EXPECT_EQ(statistics.hybrid_selection_ranges, 3);
    EXPECT_EQ(statistics.hybrid_selection_null_fallback_batches, 0);
}

TEST(ParquetV2NativeDecoderTest, NullableSparsePlainDateSelectionBatchesPhysicalPayload) {
    const std::vector<int32_t> physical_days {0, 1, 2, 3, 4};
    constexpr size_t LOGICAL_VALUES = 9;
    const std::vector<uint16_t> null_runs(LOGICAL_VALUES, 1);
    const std::vector<uint8_t> filter {1, 1, 1, 0, 0, 1, 1, 1, 0};

    DataTypeDateV2 type;
    auto column = type.create_column();
    NullMap null_map;
    ColumnChunkReaderStatistics statistics;
    const ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT32,
                                        .logical_type = ParquetLogicalType::DATE};
    ASSERT_TRUE(materialize_selected_plain_int32(physical_days, LOGICAL_VALUES, null_runs, filter,
                                                 type, &column, &null_map, &statistics, false,
                                                 &context)
                        .ok());

    ASSERT_EQ(column->size(), 6);
    EXPECT_EQ(type.to_string(*column, 0), "1970-01-01");
    EXPECT_EQ(type.to_string(*column, 2), "1970-01-02");
    EXPECT_EQ(type.to_string(*column, 4), "1970-01-04");
    EXPECT_EQ(null_map, (NullMap {0, 1, 0, 1, 0, 1}));
    EXPECT_EQ(statistics.hybrid_selection_batches, 1);
    EXPECT_EQ(statistics.hybrid_selection_ranges, 2);
    EXPECT_EQ(statistics.hybrid_selection_null_fallback_batches, 0);
}

TEST(ParquetV2NativeDecoderTest, NullableSparsePlainDateTimeSelectionBatchesPhysicalPayload) {
    const std::vector<int64_t> physical_micros {0, 1'000'000, 2'000'000, 3'000'000, 4'000'000};
    constexpr size_t LOGICAL_VALUES = 9;
    const std::vector<uint16_t> null_runs(LOGICAL_VALUES, 1);
    const std::vector<uint8_t> filter {1, 1, 1, 0, 0, 1, 1, 1, 0};

    DataTypeDateTimeV2 type(6);
    auto column = type.create_column();
    NullMap null_map;
    ColumnChunkReaderStatistics statistics;
    const ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT64,
                                        .logical_type = ParquetLogicalType::TIMESTAMP,
                                        .time_unit = ParquetTimeUnit::MICROS};
    ASSERT_TRUE(materialize_selected_plain_int64(physical_micros, LOGICAL_VALUES, null_runs, filter,
                                                 type, &column, &null_map, &statistics, context)
                        .ok());

    ASSERT_EQ(column->size(), 6);
    EXPECT_EQ(type.to_string(*column, 0), "1970-01-01 00:00:00.000000");
    EXPECT_EQ(type.to_string(*column, 2), "1970-01-01 00:00:01.000000");
    EXPECT_EQ(type.to_string(*column, 4), "1970-01-01 00:00:03.000000");
    EXPECT_EQ(null_map, (NullMap {0, 1, 0, 1, 0, 1}));
    EXPECT_EQ(statistics.hybrid_selection_batches, 1);
    EXPECT_EQ(statistics.hybrid_selection_ranges, 2);
    EXPECT_EQ(statistics.hybrid_selection_null_fallback_batches, 0);
}

TEST(ParquetV2NativeDecoderTest, NegativeNanosFloorAcrossPlainAndDictionaryTimestamps) {
    const std::vector<int64_t> nanos {-1, -999, -1000, -1001, 0, 1001};
    const std::vector<uint32_t> dictionary_ids {0, 1, 2, 3, 4, 5};
    const std::vector<uint16_t> no_nulls {static_cast<uint16_t>(nanos.size()), 0};
    const std::vector<uint8_t> select_all(nanos.size(), 1);
    const std::vector<std::string> expected {
            "1969-12-31 23:59:59.999999", "1969-12-31 23:59:59.999999",
            "1969-12-31 23:59:59.999999", "1969-12-31 23:59:59.999998",
            "1970-01-01 00:00:00.000000", "1970-01-01 00:00:00.000001"};
    const ParquetDecodeContext local_context {.physical_type = ParquetPhysicalType::INT64,
                                              .logical_type = ParquetLogicalType::TIMESTAMP,
                                              .time_unit = ParquetTimeUnit::NANOS};
    const ParquetDecodeContext utc_context {.physical_type = ParquetPhysicalType::INT64,
                                            .logical_type = ParquetLogicalType::TIMESTAMP,
                                            .time_unit = ParquetTimeUnit::NANOS,
                                            .timestamp_is_adjusted_to_utc = true};

    for (const bool dictionary : {false, true}) {
        SCOPED_TRACE(dictionary ? "dictionary" : "plain");
        DataTypeDateTimeV2 datetime_type(6);
        auto datetime_column = datetime_type.create_column();
        NullMap datetime_nulls;
        ColumnChunkReaderStatistics datetime_statistics;
        const auto datetime_status =
                dictionary ? materialize_selected_dictionary_int64(
                                     nanos, dictionary_ids, nanos.size(), no_nulls, select_all,
                                     datetime_type, &datetime_column, &datetime_nulls,
                                     &datetime_statistics, local_context)
                           : materialize_selected_plain_int64(nanos, nanos.size(), no_nulls,
                                                              select_all, datetime_type,
                                                              &datetime_column, &datetime_nulls,
                                                              &datetime_statistics, local_context);
        ASSERT_TRUE(datetime_status.ok()) << datetime_status;
        ASSERT_EQ(datetime_column->size(), expected.size());
        for (size_t row = 0; row < expected.size(); ++row) {
            EXPECT_EQ(datetime_type.to_string(*datetime_column, row), expected[row]);
        }

        DataTypeTimeStampTz timestamptz_type(6);
        auto timestamptz_column = timestamptz_type.create_column();
        NullMap timestamptz_nulls;
        ColumnChunkReaderStatistics timestamptz_statistics;
        const auto timestamptz_status =
                dictionary ? materialize_selected_dictionary_int64(
                                     nanos, dictionary_ids, nanos.size(), no_nulls, select_all,
                                     timestamptz_type, &timestamptz_column, &timestamptz_nulls,
                                     &timestamptz_statistics, utc_context)
                           : materialize_selected_plain_int64(
                                     nanos, nanos.size(), no_nulls, select_all, timestamptz_type,
                                     &timestamptz_column, &timestamptz_nulls,
                                     &timestamptz_statistics, utc_context);
        ASSERT_TRUE(timestamptz_status.ok()) << timestamptz_status;
        const auto& tz_column = assert_cast<const ColumnTimeStampTz&>(*timestamptz_column);
        const auto utc = cctz::utc_time_zone();
        for (size_t row = 0; row < expected.size(); ++row) {
            EXPECT_EQ(tz_column.get_element(row).to_string(utc, 6), expected[row] + "+00:00");
        }
    }
}

TEST(ParquetV2NativeDecoderTest, NullableSparseDictionarySelectionBatchesPhysicalPayload) {
    constexpr size_t LOGICAL_VALUES = 4095;
    std::vector<int32_t> dictionary(16);
    std::iota(dictionary.begin(), dictionary.end(), 100);
    // One value followed by nineteen NULLs exercises the sparse-null threshold (<10%).
    std::vector<uint16_t> null_runs;
    for (size_t row = 0; row < LOGICAL_VALUES;) {
        null_runs.push_back(1);
        ++row;
        const auto null_count = std::min<size_t>(19, LOGICAL_VALUES - row);
        null_runs.push_back(cast_set<uint16_t>(null_count));
        row += null_count;
    }
    std::vector<uint32_t> physical_ids((LOGICAL_VALUES + 19) / 20);
    for (size_t index = 0; index < physical_ids.size(); ++index) {
        physical_ids[index] = static_cast<uint32_t>(index % dictionary.size());
    }
    std::vector<uint8_t> filter(LOGICAL_VALUES, 0);
    for (const size_t selected_row : {0, 1000, 1001, 2000, 4001}) {
        filter[selected_row] = 1;
    }

    DataTypeInt32 type;
    auto column = type.create_column();
    assert_cast<ColumnInt32&>(*column).get_data().push_back(-7);
    NullMap null_map;
    null_map.push_back(0);
    ColumnChunkReaderStatistics statistics;
    const auto status = materialize_selected_dictionary_int32(
            dictionary, physical_ids, LOGICAL_VALUES, null_runs, filter, type, &column, &null_map,
            &statistics);
    ASSERT_TRUE(status.ok()) << status;

    EXPECT_EQ(assert_cast<const ColumnInt32&>(*column).get_data(),
              (ColumnInt32::Container {-7, 100, 102, 0, 104, 0}));
    EXPECT_EQ(null_map, (NullMap {0, 0, 0, 1, 0, 1}));
    EXPECT_EQ(statistics.hybrid_selection_batches, 1);
    EXPECT_EQ(statistics.hybrid_selection_ranges, 3);
    EXPECT_EQ(statistics.hybrid_selection_null_fallback_batches, 0);
}

TEST(ParquetV2NativeDecoderTest, DictionaryMaterializationSkipsFailureScanWhenDictionaryIsClean) {
    ParquetMaterializationState state;
    state.typed_dictionary = ColumnInt32::create();
    auto& dictionary = assert_cast<ColumnInt32&>(*state.typed_dictionary).get_data();
    dictionary = {10, 20, 30, 40};
    state.dictionary_indices = {3, 0, 2, 1, 3};
    state.dictionary_conversion_failures.resize_fill(dictionary.size(), 0);

    auto output = ColumnInt32::create();
    ASSERT_TRUE(state.materialize_dictionary(*output).ok());
    EXPECT_EQ(output->get_data(), (ColumnInt32::Container {40, 10, 30, 20, 40}));
    EXPECT_EQ(state.dictionary_failure_scan_rows, 0);
}

TEST(ParquetV2NativeDecoderTest, DictionaryMaterializationUsesCacheAwareExecutionShape) {
    auto verify_strategy = [](bool prefer_indices,
                              ParquetDictionaryMaterializationStrategy expected_strategy,
                              size_t expected_direct_batches, size_t expected_index_batches) {
        ParquetMaterializationState state;
        state.typed_dictionary = ColumnInt32::create();
        assert_cast<ColumnInt32&>(*state.typed_dictionary).get_data() = {10, 20, 30, 40};
        ScriptedDictionaryMaterializationSource source({3, 0, 2, 1, 3}, prefer_indices);
        auto output = ColumnInt32::create();

        const auto status = state.materialize_dictionary(*output, source, 5);
        EXPECT_TRUE(status.ok()) << status;
        EXPECT_EQ(output->get_data(), (ColumnInt32::Container {40, 10, 30, 20, 40}));
        EXPECT_EQ(state.dictionary_materialization_strategy, expected_strategy);
        EXPECT_EQ(source.direct_batches, expected_direct_batches);
        EXPECT_EQ(source.index_batches, expected_index_batches);
    };

    verify_strategy(false, ParquetDictionaryMaterializationStrategy::DIRECT, 1, 0);
    verify_strategy(true, ParquetDictionaryMaterializationStrategy::INDICES, 0, 1);
}

TEST(ParquetV2NativeDecoderTest, DictionaryRepeatedRunsGatherDirectlyIntoDestination) {
    const std::array<int32_t, 2> dictionary_values {10, 20};
    auto dictionary = make_unique_buffer<uint8_t>(sizeof(dictionary_values));
    memcpy(dictionary.get(), dictionary_values.data(), sizeof(dictionary_values));
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(
            Decoder::get_decoder(tparquet::Type::INT32, tparquet::Encoding::RLE_DICTIONARY, decoder)
                    .ok());
    decoder->set_type_length(sizeof(int32_t));
    ASSERT_TRUE(decoder->set_dict(dictionary, sizeof(dictionary_values), dictionary_values.size())
                        .ok());

    faststring encoded_ids;
    RleEncoder<uint32_t> encoder(&encoded_ids, 1);
    for (size_t row = 0; row < 64; ++row) {
        encoder.Put(1);
    }
    encoder.Flush();
    std::vector<uint8_t> payload(encoded_ids.size() + 1);
    payload[0] = 1;
    memcpy(payload.data() + 1, encoded_ids.data(), encoded_ids.size());
    Slice id_slice(payload.data(), payload.size());
    ASSERT_TRUE(decoder->set_data(&id_slice).ok());

    DataTypeInt32 type;
    auto output = type.create_column();
    ParquetMaterializationState state;
    ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT32,
                                  .encoding = ParquetValueEncoding::DICTIONARY};
    ASSERT_TRUE(
            type.get_serde()->read_column_from_parquet(*output, *decoder, context, 64, state).ok());
    EXPECT_EQ(state.dictionary_materialization_strategy,
              ParquetDictionaryMaterializationStrategy::DIRECT);
    ASSERT_EQ(output->size(), 64);
    for (size_t row = 0; row < output->size(); ++row) {
        EXPECT_EQ(assert_cast<const ColumnInt32&>(*output).get_element(row), 20);
    }
}

TEST(ParquetV2NativeDecoderTest, DictionaryDirectGatherRollsBackLateCorruptRun) {
    const std::array<int32_t, 2> dictionary_values {10, 20};
    auto dictionary = make_unique_buffer<uint8_t>(sizeof(dictionary_values));
    memcpy(dictionary.get(), dictionary_values.data(), sizeof(dictionary_values));
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(
            Decoder::get_decoder(tparquet::Type::INT32, tparquet::Encoding::RLE_DICTIONARY, decoder)
                    .ok());
    decoder->set_type_length(sizeof(int32_t));
    ASSERT_TRUE(decoder->set_dict(dictionary, sizeof(dictionary_values), dictionary_values.size())
                        .ok());

    faststring encoded_ids;
    RleEncoder<uint32_t> encoder(&encoded_ids, 2);
    for (size_t row = 0; row < 8; ++row) {
        encoder.Put(0);
    }
    for (size_t row = 0; row < 8; ++row) {
        encoder.Put(3);
    }
    encoder.Flush();
    std::vector<uint8_t> payload(encoded_ids.size() + 1);
    payload[0] = 2;
    memcpy(payload.data() + 1, encoded_ids.data(), encoded_ids.size());
    Slice id_slice(payload.data(), payload.size());
    ASSERT_TRUE(decoder->set_data(&id_slice).ok());

    DataTypeInt32 type;
    auto output = type.create_column();
    ParquetMaterializationState state;
    ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT32,
                                  .encoding = ParquetValueEncoding::DICTIONARY};
    const auto status =
            type.get_serde()->read_column_from_parquet(*output, *decoder, context, 16, state);
    EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
    EXPECT_TRUE(output->empty());
}

TEST(ParquetV2NativeDecoderTest, DictionaryIndexBoundsCheckHandlesVectorTailAndUnsignedIds) {
    const std::vector<uint32_t> valid {0, 7, 1, 6, 2, 5, 3, 4, 7, 0, 6};
    EXPECT_TRUE(native::dictionary_indices_in_bounds(valid.data(), valid.size(), 8));

    auto invalid_tail = valid;
    invalid_tail.back() = 8;
    EXPECT_FALSE(native::dictionary_indices_in_bounds(invalid_tail.data(), invalid_tail.size(), 8));

    auto invalid_unsigned = valid;
    invalid_unsigned[3] = std::numeric_limits<uint32_t>::max();
    EXPECT_FALSE(native::dictionary_indices_in_bounds(invalid_unsigned.data(),
                                                      invalid_unsigned.size(), 8));
}

TEST(ParquetV2NativeDecoderTest, NullableSparseSelectionRemapsConversionFailures) {
    const std::vector<int32_t> dictionary {1, 1000, 2, 3};
    const std::vector<uint32_t> physical_ids {0, 1, 2, 3};
    const std::vector<uint16_t> null_runs(7, 1);
    std::vector<uint8_t> filter(7, 0);
    for (const size_t selected_row : {1, 2, 4, 5}) {
        filter[selected_row] = 1;
    }

    DataTypeInt8 type;
    auto column = type.create_column();
    assert_cast<ColumnInt8&>(*column).get_data().push_back(9);
    NullMap null_map;
    null_map.push_back(0);
    ColumnChunkReaderStatistics statistics;
    ASSERT_TRUE(materialize_selected_dictionary_int32(dictionary, physical_ids, 7, null_runs,
                                                      filter, type, &column, &null_map, &statistics)
                        .ok());

    EXPECT_EQ(assert_cast<const ColumnInt8&>(*column).get_data(),
              (ColumnInt8::Container {9, 0, 0, 2, 0}));
    EXPECT_EQ(null_map, (NullMap {0, 1, 1, 0, 1}));
    EXPECT_EQ(statistics.hybrid_selection_batches, 1);
    EXPECT_EQ(statistics.hybrid_selection_null_fallback_batches, 0);
}

TEST(ParquetV2NativeDecoderTest, NullableSparseSelectionExpandsStringsInPlace) {
    const std::vector<std::string> dictionary {"a", "bbb", "cc", "dddd"};
    const std::vector<uint32_t> physical_ids {0, 1, 2, 3};
    const std::vector<uint16_t> null_runs(7, 1);
    std::vector<uint8_t> filter(7, 0);
    for (const size_t selected_row : {1, 2, 4, 5}) {
        filter[selected_row] = 1;
    }

    DataTypeString type;
    auto column = type.create_column();
    assert_cast<ColumnString&>(*column).insert_data("prefix", 6);
    NullMap null_map;
    null_map.push_back(0);
    ColumnChunkReaderStatistics statistics;
    ASSERT_TRUE(materialize_selected_dictionary_strings(dictionary, physical_ids, 7, null_runs,
                                                        filter, &column, &null_map, &statistics)
                        .ok());

    ASSERT_EQ(column->size(), 5);
    EXPECT_EQ(column->get_data_at(0).to_string_view(), "prefix");
    EXPECT_EQ(column->get_data_at(1).to_string_view(), "");
    EXPECT_EQ(column->get_data_at(2).to_string_view(), "bbb");
    EXPECT_EQ(column->get_data_at(3).to_string_view(), "cc");
    EXPECT_EQ(column->get_data_at(4).to_string_view(), "");
    EXPECT_EQ(null_map, (NullMap {0, 1, 0, 0, 1}));
    EXPECT_EQ(statistics.hybrid_selection_batches, 1);
    EXPECT_EQ(statistics.hybrid_selection_null_fallback_batches, 0);
}

TEST(ParquetV2NativeDecoderTest, NullableSparseSelectionHandlesEmptyOutputShapes) {
    DataTypeInt32 type;
    ColumnChunkReaderStatistics statistics;

    auto all_null_column = type.create_column();
    assert_cast<ColumnInt32&>(*all_null_column).get_data().push_back(8);
    NullMap all_null_map;
    all_null_map.push_back(0);
    const std::vector<uint8_t> select_alternating {1, 0, 1, 0, 1};
    ASSERT_TRUE(materialize_selected_plain_int32({}, 5, {0, 5}, select_alternating, type,
                                                 &all_null_column, &all_null_map, &statistics)
                        .ok());
    EXPECT_EQ(assert_cast<const ColumnInt32&>(*all_null_column).get_data(),
              (ColumnInt32::Container {8, 0, 0, 0}));
    EXPECT_EQ(all_null_map, (NullMap {0, 1, 1, 1}));
    EXPECT_EQ(statistics.hybrid_selection_batches, 1);
    EXPECT_EQ(statistics.hybrid_selection_ranges, 0);
    EXPECT_EQ(statistics.hybrid_selection_null_fallback_batches, 0);

    auto dictionary_all_null_column = type.create_column();
    assert_cast<ColumnInt32&>(*dictionary_all_null_column).get_data().push_back(8);
    NullMap dictionary_all_null_map;
    dictionary_all_null_map.push_back(0);
    statistics = {};
    ASSERT_TRUE(materialize_selected_dictionary_int32({10}, {}, 5, {0, 5}, select_alternating, type,
                                                      &dictionary_all_null_column,
                                                      &dictionary_all_null_map, &statistics)
                        .ok());
    EXPECT_EQ(assert_cast<const ColumnInt32&>(*dictionary_all_null_column).get_data(),
              (ColumnInt32::Container {8, 0, 0, 0}));
    EXPECT_EQ(dictionary_all_null_map, (NullMap {0, 1, 1, 1}));
    EXPECT_EQ(statistics.hybrid_selection_batches, 1);
    EXPECT_EQ(statistics.hybrid_selection_ranges, 0);
    EXPECT_EQ(statistics.hybrid_selection_null_fallback_batches, 0);

    auto all_filtered_column = type.create_column();
    assert_cast<ColumnInt32&>(*all_filtered_column).get_data().push_back(9);
    NullMap all_filtered_map;
    all_filtered_map.push_back(0);
    statistics = {};
    ASSERT_TRUE(materialize_selected_plain_int32(
                        {10, 20, 30}, 5, {1, 1, 1, 1, 1}, std::vector<uint8_t>(5, 0), type,
                        &all_filtered_column, &all_filtered_map, &statistics)
                        .ok());
    EXPECT_EQ(assert_cast<const ColumnInt32&>(*all_filtered_column).get_data(),
              (ColumnInt32::Container {9}));
    EXPECT_EQ(all_filtered_map, (NullMap {0}));
    EXPECT_EQ(statistics.hybrid_selection_batches, 1);
    EXPECT_EQ(statistics.hybrid_selection_ranges, 0);
    EXPECT_EQ(statistics.hybrid_selection_null_fallback_batches, 0);

    auto dictionary_all_filtered_column = type.create_column();
    assert_cast<ColumnInt32&>(*dictionary_all_filtered_column).get_data().push_back(9);
    NullMap dictionary_all_filtered_map;
    dictionary_all_filtered_map.push_back(0);
    statistics = {};
    ASSERT_TRUE(materialize_selected_dictionary_int32({10, 20, 30}, {0, 1, 2}, 5, {1, 1, 1, 1, 1},
                                                      std::vector<uint8_t>(5, 0), type,
                                                      &dictionary_all_filtered_column,
                                                      &dictionary_all_filtered_map, &statistics)
                        .ok());
    EXPECT_EQ(assert_cast<const ColumnInt32&>(*dictionary_all_filtered_column).get_data(),
              (ColumnInt32::Container {9}));
    EXPECT_EQ(dictionary_all_filtered_map, (NullMap {0}));
    EXPECT_EQ(statistics.hybrid_selection_batches, 1);
    EXPECT_EQ(statistics.hybrid_selection_ranges, 0);
    EXPECT_EQ(statistics.hybrid_selection_null_fallback_batches, 0);
}

TEST(ParquetV2NativeDecoderTest, ByteArrayDictionaryReferencesOwnedPageAndValidatesIndices) {
    int32_t dictionary_length = 0;
    auto dictionary = make_byte_array_dictionary({"alpha", "beta"}, &dictionary_length);
    const uint8_t* dictionary_address = dictionary.get();
    ByteArrayDictDecoder decoder;

    ASSERT_TRUE(decoder.set_dict(dictionary, dictionary_length, 2).ok());
    EXPECT_EQ(dictionary.get(), nullptr);

    RejectFixedConsumer fixed_consumer;
    CaptureBinaryConsumer binary_consumer;
    ASSERT_TRUE(decoder.decode_dictionary(fixed_consumer, binary_consumer).ok());
    ASSERT_EQ(binary_consumer.refs.size(), 2);
    EXPECT_EQ(binary_consumer.refs[0].to_string_view(), "alpha");
    EXPECT_EQ(binary_consumer.refs[1].to_string_view(), "beta");
    EXPECT_EQ(binary_consumer.refs[0].data,
              reinterpret_cast<const char*>(dictionary_address + sizeof(uint32_t)));

    // bit width 1, an RLE run of three values (header = 3 << 1), dictionary id 1.
    char valid_indices[] = {1, 6, 1};
    Slice valid_slice(valid_indices, sizeof(valid_indices));
    ASSERT_TRUE(decoder.set_data(&valid_slice).ok());
    std::vector<uint32_t> decoded_indices;
    ASSERT_TRUE(decoder.decode_dictionary_indices(3, &decoded_indices).ok());
    EXPECT_EQ(decoded_indices, std::vector<uint32_t>({1, 1, 1}));

    // bit width 2, one RLE value with dictionary id 3. Skipping still validates the encoded id so
    // filter selection cannot hide a corrupt dictionary stream.
    char invalid_indices[] = {2, 2, 3};
    Slice invalid_slice(invalid_indices, sizeof(invalid_indices));
    ASSERT_TRUE(decoder.set_data(&invalid_slice).ok());
    ParquetDecodeSource& source = decoder;
    EXPECT_TRUE(source.skip_values(1).is<ErrorCode::CORRUPTION>());

    // Sparse decode must validate filtered dictionary ids too. Predicate selection must never
    // turn a corrupt page into a successful read merely because the bad row was not selected.
    ASSERT_TRUE(decoder.set_data(&invalid_slice).ok());
    ParquetSelection filtered_corrupt {.total_values = 1, .selected_values = 0, .ranges = {}};
    EXPECT_TRUE(decoder.decode_selected_dictionary_indices(filtered_corrupt, &decoded_indices)
                        .is<ErrorCode::CORRUPTION>());

    Slice empty_indices;
    EXPECT_TRUE(decoder.set_data(&empty_indices).is<ErrorCode::CORRUPTION>());

    // Dictionary indices are uint32_t. Wider external bit widths would make the RLE decoder copy
    // a five-byte repeated value into four-byte state before it can validate an index.
    char oversized_bit_width[] = {33, 2, 0};
    Slice oversized_bit_width_slice(oversized_bit_width, sizeof(oversized_bit_width));
    EXPECT_TRUE(decoder.set_data(&oversized_bit_width_slice).is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, ByteArrayDictionaryBoundsEntryCountBeforeReserve) {
    auto dictionary = make_unique_buffer<uint8_t>(1);
    dictionary.get()[0] = 0;
    ByteArrayDictDecoder decoder;
    EXPECT_TRUE(decoder.set_dict(dictionary, 1, std::numeric_limits<int32_t>::max())
                        .is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, LegacyConvertedTimestampsRemainUtcAdjusted) {
    TimezoneUtils::load_timezones_to_cache();
    for (const auto converted_type :
         {tparquet::ConvertedType::TIMESTAMP_MILLIS, tparquet::ConvertedType::TIMESTAMP_MICROS}) {
        NativeFieldSchema field;
        field.physical_type = tparquet::Type::INT64;
        field.parquet_schema.__set_converted_type(converted_type);
        ParquetDecodeContext context;
        ASSERT_TRUE(init_decode_context_for_test(field, nullptr, &context).ok());
        EXPECT_EQ(context.logical_type, ParquetLogicalType::TIMESTAMP);
        EXPECT_TRUE(context.timestamp_is_adjusted_to_utc);

        cctz::time_zone shanghai;
        ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("Asia/Shanghai", shanghai));
        ASSERT_TRUE(init_decode_context_for_test(field, &shanghai, &context).ok());
        int64_t epoch = 0;
        Slice epoch_slice(reinterpret_cast<uint8_t*>(&epoch), sizeof(epoch));
        std::unique_ptr<Decoder> decoder;
        ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::INT64, tparquet::Encoding::PLAIN, decoder)
                            .ok());
        decoder->set_type_length(sizeof(epoch));
        ASSERT_TRUE(decoder->set_data(&epoch_slice).ok());
        ParquetMaterializationState state;
        DataTypeDateTimeV2 type(0);
        auto column = type.create_column();
        ASSERT_TRUE(type.get_serde()
                            ->read_column_from_parquet(*column, *decoder, context, 1, state)
                            .ok());
        EXPECT_EQ(type.to_string(*column, 0), "1970-01-01 08:00:00");
    }
}

TEST(ParquetV2NativeDecoderTest, GeospatialByteArrayAnnotationsDecodeAsRawBytes) {
    const std::string wkb("\x01\x01\x00\x00\x00", 5);
    for (const bool geometry : {true, false}) {
        tparquet::LogicalType logical;
        if (geometry) {
            logical.__set_GEOMETRY(tparquet::GeometryType());
        } else {
            logical.__set_GEOGRAPHY(tparquet::GeographyType());
        }

        NativeFieldSchema field;
        field.name = geometry ? "geometry" : "geography";
        field.physical_type = tparquet::Type::BYTE_ARRAY;
        field.parquet_schema.__set_logicalType(logical);
        ParquetDecodeContext context;
        ASSERT_TRUE(init_decode_context_for_test(field, nullptr, &context).ok());
        EXPECT_EQ(context.physical_type, ParquetPhysicalType::BYTE_ARRAY);
        EXPECT_EQ(context.logical_type, ParquetLogicalType::NONE);

        DataTypeString type;
        auto plain_column = type.create_column();
        auto encoded = encode_plain_byte_arrays({wkb});
        Slice plain_slice(encoded.data(), encoded.size());
        std::unique_ptr<Decoder> plain_decoder;
        ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::PLAIN,
                                         plain_decoder)
                            .ok());
        ASSERT_TRUE(plain_decoder->set_data(&plain_slice).ok());
        ParquetMaterializationState plain_state;
        ASSERT_TRUE(type.get_serde()
                            ->read_column_from_parquet(*plain_column, *plain_decoder, context, 1,
                                                       plain_state)
                            .ok());
        ASSERT_EQ(plain_column->size(), 1);
        EXPECT_EQ(plain_column->get_data_at(0).to_string_view(), wkb);

        int32_t dictionary_length = 0;
        auto dictionary = make_byte_array_dictionary({wkb, "unused"}, &dictionary_length);
        ByteArrayDictDecoder dictionary_decoder;
        ASSERT_TRUE(dictionary_decoder.set_dict(dictionary, dictionary_length, 2).ok());
        char dictionary_index[] = {1, 2, 0};
        Slice dictionary_slice(dictionary_index, sizeof(dictionary_index));
        ASSERT_TRUE(dictionary_decoder.set_data(&dictionary_slice).ok());
        context.encoding = ParquetValueEncoding::DICTIONARY;
        auto dictionary_column = type.create_column();
        ParquetMaterializationState dictionary_state;
        ASSERT_TRUE(type.get_serde()
                            ->read_column_from_parquet(*dictionary_column, dictionary_decoder,
                                                       context, 1, dictionary_state)
                            .ok());
        ASSERT_EQ(dictionary_column->size(), 1);
        EXPECT_EQ(dictionary_column->get_data_at(0).to_string_view(), wkb);

        field.physical_type = tparquet::Type::INT32;
        EXPECT_TRUE(
                init_decode_context_for_test(field, nullptr, &context).is<ErrorCode::CORRUPTION>());
    }
}

TEST(ParquetV2NativeDecoderTest, InvalidLogicalPhysicalPairsFailBeforeDecode) {
    auto invalid = [](tparquet::Type::type physical, const tparquet::LogicalType& logical) {
        NativeFieldSchema field;
        field.name = "bad";
        field.physical_type = physical;
        field.parquet_schema.__set_logicalType(logical);
        ParquetDecodeContext context;
        return init_decode_context_for_test(field, nullptr, &context);
    };

    tparquet::LogicalType date;
    date.__set_DATE(tparquet::DateType());
    EXPECT_FALSE(invalid(tparquet::Type::BOOLEAN, date).ok());

    tparquet::LogicalType timestamp;
    timestamp.__set_TIMESTAMP(tparquet::TimestampType());
    timestamp.TIMESTAMP.__set_unit(tparquet::TimeUnit());
    timestamp.TIMESTAMP.unit.__set_MICROS(tparquet::MicroSeconds());
    EXPECT_FALSE(invalid(tparquet::Type::INT32, timestamp).ok());

    tparquet::LogicalType integer;
    integer.__set_INTEGER(tparquet::IntType());
    integer.INTEGER.__set_bitWidth(64);
    integer.INTEGER.__set_isSigned(true);
    EXPECT_FALSE(invalid(tparquet::Type::INT32, integer).ok());

    tparquet::LogicalType string;
    string.__set_STRING(tparquet::StringType());
    EXPECT_FALSE(invalid(tparquet::Type::INT32, string).ok());

    tparquet::LogicalType uuid;
    uuid.__set_UUID(tparquet::UUIDType());
    EXPECT_FALSE(invalid(tparquet::Type::BYTE_ARRAY, uuid).ok());

    auto invalid_converted = [](tparquet::Type::type physical,
                                tparquet::ConvertedType::type converted, int type_length = -1,
                                int precision = -1, int scale = -1) {
        NativeFieldSchema field;
        field.name = "bad";
        field.physical_type = physical;
        field.parquet_schema.__set_converted_type(converted);
        if (type_length >= 0) {
            field.parquet_schema.__set_type_length(type_length);
        }
        if (precision >= 0) {
            field.parquet_schema.__set_precision(precision);
        }
        if (scale >= 0) {
            field.parquet_schema.__set_scale(scale);
        }
        ParquetDecodeContext context;
        return init_decode_context_for_test(field, nullptr, &context);
    };
    EXPECT_FALSE(invalid_converted(tparquet::Type::INT32, tparquet::ConvertedType::UTF8).ok());
    EXPECT_FALSE(
            invalid_converted(tparquet::Type::INT32, tparquet::ConvertedType::TIME_MICROS).ok());
    EXPECT_FALSE(invalid_converted(tparquet::Type::INT32, tparquet::ConvertedType::UINT_64).ok());
    EXPECT_FALSE(invalid_converted(tparquet::Type::FIXED_LEN_BYTE_ARRAY,
                                   tparquet::ConvertedType::INTERVAL, 8)
                         .ok());
    EXPECT_FALSE(
            invalid_converted(tparquet::Type::INT32, tparquet::ConvertedType::DECIMAL, -1, 10, 2)
                    .ok());

    NativeFieldSchema fixed;
    fixed.name = "fixed";
    fixed.physical_type = tparquet::Type::FIXED_LEN_BYTE_ARRAY;
    ParquetDecodeContext context;
    EXPECT_FALSE(init_decode_context_for_test(fixed, nullptr, &context).ok());
    fixed.parquet_schema.__set_type_length(4);
    EXPECT_TRUE(init_decode_context_for_test(fixed, nullptr, &context).ok());
}

TEST(ParquetV2NativeDecoderTest, NonStrictLegacyTimestampsKeepDefaultOnOverflow) {
    DataTypePtr datetime_type = make_nullable(std::make_shared<DataTypeDateTimeV2>(6));
    NativeFieldSchema int96_field;
    int96_field.physical_type = tparquet::Type::INT96;
    EXPECT_TRUE(preserves_timestamp_conversion_default_for_test(int96_field, datetime_type, false));
    EXPECT_FALSE(preserves_timestamp_conversion_default_for_test(int96_field, datetime_type, true));

    NativeFieldSchema utc_field;
    utc_field.physical_type = tparquet::Type::INT64;
    utc_field.parquet_schema.__set_logicalType(tparquet::LogicalType());
    utc_field.parquet_schema.logicalType.__set_TIMESTAMP(tparquet::TimestampType());
    utc_field.parquet_schema.logicalType.TIMESTAMP.__set_isAdjustedToUTC(true);
    EXPECT_TRUE(preserves_timestamp_conversion_default_for_test(utc_field, datetime_type, false));

    NativeFieldSchema converted_utc_field;
    converted_utc_field.physical_type = tparquet::Type::INT64;
    converted_utc_field.parquet_schema.__set_converted_type(
            tparquet::ConvertedType::TIMESTAMP_MICROS);
    EXPECT_TRUE(preserves_timestamp_conversion_default_for_test(converted_utc_field, datetime_type,
                                                                false));

    NativeFieldSchema local_field = utc_field;
    local_field.parquet_schema.logicalType.TIMESTAMP.__set_isAdjustedToUTC(false);
    EXPECT_FALSE(
            preserves_timestamp_conversion_default_for_test(local_field, datetime_type, false));

    NativeFieldSchema integer_field;
    integer_field.physical_type = tparquet::Type::INT64;
    EXPECT_FALSE(preserves_timestamp_conversion_default_for_test(
            integer_field, make_nullable(std::make_shared<DataTypeInt64>()), false));
}

TEST(ParquetV2NativeDecoderTest, FastInt96NormalizesLegacyOutOfDayNanos) {
    EXPECT_TRUE(materialize_plain_int96({{-1, 2440588}}).ok());
    EXPECT_TRUE(materialize_plain_int96({{86400000000000LL, 2440588}}).ok());
    EXPECT_TRUE(materialize_plain_int96({{0, 2440588}, {-1, 2440588}}, {0, 1}).ok());
}

TEST(ParquetV2NativeDecoderTest, NonStrictLocalTimestampDefaultsBecomeNull) {
    DataTypePtr datetime_type = make_nullable(std::make_shared<DataTypeDateTimeV2>(6));
    NativeFieldSchema local_field;
    local_field.physical_type = tparquet::Type::INT64;
    local_field.parquet_schema.__set_logicalType(tparquet::LogicalType());
    local_field.parquet_schema.logicalType.__set_TIMESTAMP(tparquet::TimestampType());
    local_field.parquet_schema.logicalType.TIMESTAMP.__set_isAdjustedToUTC(false);

    DataTypeDateTimeV2 type(6);
    auto column = type.create_column();
    const uint64_t invalid_datetime = 0;
    column->insert_data(reinterpret_cast<const char*>(&invalid_datetime), sizeof(invalid_datetime));
    IColumn::Filter null_map;
    null_map.resize_fill(1, 0);
    mark_local_timestamp_defaults_for_test(local_field, datetime_type, false, *column, &null_map,
                                           0);
    EXPECT_EQ(null_map[0], 1);

    null_map[0] = 0;
    local_field.parquet_schema.logicalType.TIMESTAMP.__set_isAdjustedToUTC(true);
    mark_local_timestamp_defaults_for_test(local_field, datetime_type, false, *column, &null_map,
                                           0);
    EXPECT_EQ(null_map[0], 0);
}

TEST(ParquetV2NativeDecoderTest, SparsePlainAndBooleanDecodeOnceAndPreserveCursor) {
    const ParquetSelection selection {
            .total_values = 7,
            .selected_values = 3,
            .ranges = {{.first = 1, .count = 2}, {.first = 6, .count = 1}}};

    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(
            Decoder::get_decoder(tparquet::Type::INT32, tparquet::Encoding::PLAIN, decoder).ok());
    decoder->set_type_length(sizeof(int32_t));
    std::vector<int32_t> integers {10, 11, 12, 13, 14, 15, 16, 17};
    Slice integer_slice(reinterpret_cast<const uint8_t*>(integers.data()),
                        integers.size() * sizeof(int32_t));
    ASSERT_TRUE(decoder->set_data(&integer_slice).ok());
    CaptureFixedConsumer selected_integers;
    ASSERT_TRUE(decoder->decode_selected_fixed_values(selection, selected_integers).ok());
    EXPECT_EQ(selected_integers.values<int32_t>(), std::vector<int32_t>({11, 12, 16}));
    CaptureFixedConsumer trailing_integer;
    ASSERT_TRUE(decoder->decode_fixed_values(1, trailing_integer).ok());
    EXPECT_EQ(trailing_integer.values<int32_t>(), std::vector<int32_t>({17}));

    const std::vector<std::string> strings {"zero", "one",  "two", "three",
                                            "four", "five", "six", "seven"};
    auto encoded_strings = encode_plain_byte_arrays(strings);
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::PLAIN, decoder)
                        .ok());
    Slice string_slice(encoded_strings.data(), encoded_strings.size());
    ASSERT_TRUE(decoder->set_data(&string_slice).ok());
    CaptureBinaryConsumer selected_strings;
    ASSERT_TRUE(decoder->decode_selected_binary_values(selection, selected_strings).ok());
    ASSERT_EQ(selected_strings.refs.size(), 3);
    EXPECT_EQ(selected_strings.refs[0].to_string_view(), "one");
    EXPECT_EQ(selected_strings.refs[1].to_string_view(), "two");
    EXPECT_EQ(selected_strings.refs[2].to_string_view(), "six");
    CaptureBinaryConsumer trailing_string;
    ASSERT_TRUE(decoder->decode_binary_values(1, trailing_string).ok());
    ASSERT_EQ(trailing_string.refs.size(), 1);
    EXPECT_EQ(trailing_string.refs[0].to_string_view(), "seven");

    ASSERT_TRUE(
            Decoder::get_decoder(tparquet::Type::BOOLEAN, tparquet::Encoding::PLAIN, decoder).ok());
    char booleans[] = {static_cast<char>(0b10001101)};
    Slice boolean_slice(booleans, sizeof(booleans));
    ASSERT_TRUE(decoder->set_data(&boolean_slice).ok());
    CaptureFixedConsumer selected_booleans;
    ASSERT_TRUE(decoder->decode_selected_fixed_values(selection, selected_booleans).ok());
    EXPECT_EQ(selected_booleans.values<uint8_t>(), std::vector<uint8_t>({0, 1, 0}));
    CaptureFixedConsumer trailing_boolean;
    ASSERT_TRUE(decoder->decode_fixed_values(1, trailing_boolean).ok());
    EXPECT_EQ(trailing_boolean.values<uint8_t>(), std::vector<uint8_t>({1}));

    ASSERT_TRUE(
            Decoder::get_decoder(tparquet::Type::BOOLEAN, tparquet::Encoding::RLE, decoder).ok());
    char rle_booleans[] = {0x02, 0x00, 0x00, 0x00, 0x03, static_cast<char>(0x8D)};
    Slice rle_boolean_slice(rle_booleans, sizeof(rle_booleans));
    ASSERT_TRUE(decoder->set_data(&rle_boolean_slice).ok());
    CaptureFixedConsumer selected_rle_booleans;
    ASSERT_TRUE(decoder->decode_selected_fixed_values(selection, selected_rle_booleans).ok());
    EXPECT_EQ(selected_rle_booleans.values<uint8_t>(), std::vector<uint8_t>({0, 1, 0}));
    CaptureFixedConsumer trailing_rle_boolean;
    ASSERT_TRUE(decoder->decode_fixed_values(1, trailing_rle_boolean).ok());
    EXPECT_EQ(trailing_rle_boolean.values<uint8_t>(), std::vector<uint8_t>({1}));
}

TEST(ParquetV2NativeDecoderTest, PlainByteArrayPublishesOffsetsAndCoalescedSelectionSpans) {
    const std::vector<std::string> strings {"zero", "one",  "two", "three",
                                            "four", "five", "six", "seven"};
    auto encoded = encode_plain_byte_arrays(strings);
    Slice slice(encoded.data(), encoded.size());
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::PLAIN, decoder)
                        .ok());
    ASSERT_TRUE(decoder->set_data(&slice).ok());

    const ParquetSelection selection {
            .total_values = 7,
            .selected_values = 4,
            .ranges = {{.first = 1, .count = 2}, {.first = 5, .count = 2}}};
    CapturePlainBinaryLayoutConsumer consumer;
    ASSERT_TRUE(decoder->decode_selected_binary_values(selection, consumer).ok());
    EXPECT_FALSE(consumer.legacy_consume_called);
    EXPECT_EQ(consumer.output_offsets, std::vector<uint32_t>({0, 3, 6, 10, 13}));
    ASSERT_EQ(consumer.source_offsets.size(), selection.selected_values);
    EXPECT_EQ(std::string_view(consumer.base + consumer.source_offsets[0], 3), "one");
    EXPECT_EQ(std::string_view(consumer.base + consumer.source_offsets[1], 3), "two");
    EXPECT_EQ(std::string_view(consumer.base + consumer.source_offsets[2], 4), "five");
    EXPECT_EQ(std::string_view(consumer.base + consumer.source_offsets[3], 3), "six");
    ASSERT_EQ(consumer.spans.size(), 2);
    EXPECT_EQ(consumer.spans[0].first, 0);
    EXPECT_EQ(consumer.spans[0].count, 2);
    EXPECT_EQ(consumer.spans[1].first, 2);
    EXPECT_EQ(consumer.spans[1].count, 2);

    CaptureBinaryConsumer trailing;
    ASSERT_TRUE(decoder->decode_binary_values(1, trailing).ok());
    ASSERT_EQ(trailing.refs.size(), 1);
    EXPECT_EQ(trailing.refs[0].to_string_view(), "seven");
}

TEST(ParquetV2NativeDecoderTest, SparsePlainFixedDecodeDoesNotRetainGatherBuffer) {
    constexpr size_t value_count = 1UL << 18;
    std::vector<int32_t> integers(value_count);
    std::iota(integers.begin(), integers.end(), 0);
    Slice integer_slice(reinterpret_cast<const uint8_t*>(integers.data()),
                        integers.size() * sizeof(int32_t));

    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(
            Decoder::get_decoder(tparquet::Type::INT32, tparquet::Encoding::PLAIN, decoder).ok());
    decoder->set_type_length(sizeof(int32_t));
    ASSERT_TRUE(decoder->set_data(&integer_slice).ok());
    const ParquetSelection selection {
            .total_values = value_count,
            .selected_values = 2,
            .ranges = {{.first = 1, .count = 1}, {.first = value_count - 1, .count = 1}}};
    CaptureFixedConsumer selected_integers;
    ASSERT_TRUE(decoder->decode_selected_fixed_values(selection, selected_integers).ok());
    EXPECT_EQ(selected_integers.values<int32_t>(),
              std::vector<int32_t>({1, static_cast<int32_t>(value_count - 1)}));
    // Sparse PLAIN spans can be consumed directly from the encoded page. Retaining a gather buffer
    // makes a highly selective batch allocate in proportion to its selected width for no benefit.
    EXPECT_EQ(decoder->retained_scratch_bytes(), 0);
}

TEST(ParquetV2NativeDecoderTest, FixedPlainLargeSkipCannotWrapPageOffset) {
    std::vector<int64_t> values {11, 22};
    Slice slice(reinterpret_cast<const uint8_t*>(values.data()), values.size() * sizeof(int64_t));
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(
            Decoder::get_decoder(tparquet::Type::INT64, tparquet::Encoding::PLAIN, decoder).ok());
    decoder->set_type_length(sizeof(int64_t));
    ASSERT_TRUE(decoder->set_data(&slice).ok());
    EXPECT_FALSE(decoder->skip_values(536870913).ok());
    CaptureFixedConsumer consumer;
    ASSERT_TRUE(decoder->decode_fixed_values(1, consumer).ok());
    EXPECT_EQ(consumer.values<int64_t>(), std::vector<int64_t>({11}));
}

TEST(ParquetV2NativeDecoderTest, PlainAndBooleanRleExposeRawValuesAndPreserveCursor) {
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(
            Decoder::get_decoder(tparquet::Type::INT32, tparquet::Encoding::PLAIN, decoder).ok());
    decoder->set_type_length(sizeof(int32_t));
    std::vector<int32_t> integers {11, 22, 33};
    Slice integer_slice(reinterpret_cast<const uint8_t*>(integers.data()),
                        integers.size() * sizeof(int32_t));
    ASSERT_TRUE(decoder->set_data(&integer_slice).ok());
    ASSERT_TRUE(decoder->skip_values(1).ok());
    CaptureFixedConsumer integer_consumer;
    ASSERT_TRUE(decoder->decode_fixed_values(2, integer_consumer).ok());
    EXPECT_EQ(integer_consumer.values<int32_t>(), std::vector<int32_t>({22, 33}));

    ASSERT_TRUE(
            Decoder::get_decoder(tparquet::Type::BOOLEAN, tparquet::Encoding::PLAIN, decoder).ok());
    char plain_boolean[] = {static_cast<char>(0b10001101)};
    Slice plain_boolean_slice(plain_boolean, sizeof(plain_boolean));
    ASSERT_TRUE(decoder->set_data(&plain_boolean_slice).ok());
    CaptureFixedConsumer plain_boolean_consumer;
    ASSERT_TRUE(decoder->decode_fixed_values(8, plain_boolean_consumer).ok());
    EXPECT_EQ(plain_boolean_consumer.values<uint8_t>(),
              std::vector<uint8_t>({1, 0, 1, 1, 0, 0, 0, 1}));

    ASSERT_TRUE(
            Decoder::get_decoder(tparquet::Type::BOOLEAN, tparquet::Encoding::RLE, decoder).ok());
    char rle_boolean[] = {0x02, 0x00, 0x00, 0x00, 0x03, static_cast<char>(0x8D)};
    Slice rle_boolean_slice(rle_boolean, sizeof(rle_boolean));
    ASSERT_TRUE(decoder->set_data(&rle_boolean_slice).ok());
    ASSERT_TRUE(decoder->skip_values(3).ok());
    CaptureFixedConsumer rle_boolean_consumer;
    ASSERT_TRUE(decoder->decode_fixed_values(5, rle_boolean_consumer).ok());
    EXPECT_EQ(rle_boolean_consumer.values<uint8_t>(), std::vector<uint8_t>({1, 0, 0, 0, 1}));
}

TEST(ParquetV2NativeDecoderTest, DeltaEncodingsExposeValuesAfterSkip) {
    const std::vector<int32_t> integers {100, 101, 99, 1000};
    auto int_descriptor = descriptor(::parquet::Type::INT32);
    auto int_encoder = ::parquet::MakeTypedEncoder<::parquet::Int32Type>(
            ::parquet::Encoding::DELTA_BINARY_PACKED, false, int_descriptor.get());
    int_encoder->Put(integers.data(), static_cast<int>(integers.size()));
    auto int_buffer = int_encoder->FlushValues();

    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::INT32, tparquet::Encoding::DELTA_BINARY_PACKED,
                                     decoder)
                        .ok());
    decoder->set_type_length(sizeof(int32_t));
    Slice int_slice(int_buffer->data(), int_buffer->size());
    ASSERT_TRUE(decoder->set_data(&int_slice).ok());
    CaptureFixedConsumer first_integer;
    ASSERT_TRUE(decoder->decode_fixed_values(1, first_integer).ok());
    ASSERT_TRUE(decoder->skip_values(1).ok());
    CaptureFixedConsumer remaining_integers;
    ASSERT_TRUE(decoder->decode_fixed_values(2, remaining_integers).ok());
    EXPECT_EQ(first_integer.values<int32_t>(), std::vector<int32_t>({100}));
    EXPECT_EQ(remaining_integers.values<int32_t>(), std::vector<int32_t>({99, 1000}));

    const std::vector<std::string> strings {"prefix-a", "prefix-b", "other", "other-tail"};
    std::vector<::parquet::ByteArray> byte_arrays;
    byte_arrays.reserve(strings.size());
    for (const auto& value : strings) {
        byte_arrays.emplace_back(static_cast<uint32_t>(value.size()),
                                 reinterpret_cast<const uint8_t*>(value.data()));
    }
    auto byte_descriptor = descriptor(::parquet::Type::BYTE_ARRAY);
    for (const auto encoding :
         {::parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY, ::parquet::Encoding::DELTA_BYTE_ARRAY}) {
        auto encoder = ::parquet::MakeTypedEncoder<::parquet::ByteArrayType>(encoding, false,
                                                                             byte_descriptor.get());
        encoder->Put(byte_arrays.data(), static_cast<int>(byte_arrays.size()));
        auto buffer = encoder->FlushValues();
        ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::BYTE_ARRAY,
                                         encoding == ::parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY
                                                 ? tparquet::Encoding::DELTA_LENGTH_BYTE_ARRAY
                                                 : tparquet::Encoding::DELTA_BYTE_ARRAY,
                                         decoder)
                            .ok());
        Slice slice(buffer->data(), buffer->size());
        ASSERT_TRUE(decoder->set_data(&slice).ok());
        ASSERT_TRUE(decoder->skip_values(1).ok());
        CaptureBinaryConsumer consumer;
        ASSERT_TRUE(decoder->decode_binary_values(3, consumer).ok());
        ASSERT_EQ(consumer.refs.size(), 3);
        EXPECT_EQ(consumer.refs[0].to_string_view(), "prefix-b");
        EXPECT_EQ(consumer.refs[1].to_string_view(), "other");
        EXPECT_EQ(consumer.refs[2].to_string_view(), "other-tail");
    }
}

TEST(ParquetV2NativeDecoderTest, SparseStatefulEncodingsBatchDecodeAndCompact) {
    const ParquetSelection selection {
            .total_values = 3,
            .selected_values = 2,
            .ranges = {{.first = 0, .count = 1}, {.first = 2, .count = 1}}};
    const std::vector<int32_t> integers {100, 101, 99, 1000};
    auto int_descriptor = descriptor(::parquet::Type::INT32);
    auto int_encoder = ::parquet::MakeTypedEncoder<::parquet::Int32Type>(
            ::parquet::Encoding::DELTA_BINARY_PACKED, false, int_descriptor.get());
    int_encoder->Put(integers.data(), static_cast<int>(integers.size()));
    auto int_buffer = int_encoder->FlushValues();

    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::INT32, tparquet::Encoding::DELTA_BINARY_PACKED,
                                     decoder)
                        .ok());
    decoder->set_type_length(sizeof(int32_t));
    Slice int_slice(int_buffer->data(), int_buffer->size());
    ASSERT_TRUE(decoder->set_data(&int_slice).ok());
    CaptureFixedConsumer selected_integers;
    ASSERT_TRUE(decoder->decode_selected_fixed_values(selection, selected_integers).ok());
    EXPECT_EQ(selected_integers.values<int32_t>(), std::vector<int32_t>({100, 99}));
    CaptureFixedConsumer trailing_integer;
    ASSERT_TRUE(decoder->decode_fixed_values(1, trailing_integer).ok());
    EXPECT_EQ(trailing_integer.values<int32_t>(), std::vector<int32_t>({1000}));

    const std::vector<std::string> strings {"prefix-a", "prefix-b", "other", "other-tail"};
    std::vector<::parquet::ByteArray> byte_arrays;
    byte_arrays.reserve(strings.size());
    for (const auto& value : strings) {
        byte_arrays.emplace_back(static_cast<uint32_t>(value.size()),
                                 reinterpret_cast<const uint8_t*>(value.data()));
    }
    auto byte_descriptor = descriptor(::parquet::Type::BYTE_ARRAY);
    for (const auto encoding :
         {::parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY, ::parquet::Encoding::DELTA_BYTE_ARRAY}) {
        auto encoder = ::parquet::MakeTypedEncoder<::parquet::ByteArrayType>(encoding, false,
                                                                             byte_descriptor.get());
        encoder->Put(byte_arrays.data(), static_cast<int>(byte_arrays.size()));
        auto buffer = encoder->FlushValues();
        ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::BYTE_ARRAY,
                                         encoding == ::parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY
                                                 ? tparquet::Encoding::DELTA_LENGTH_BYTE_ARRAY
                                                 : tparquet::Encoding::DELTA_BYTE_ARRAY,
                                         decoder)
                            .ok());
        Slice slice(buffer->data(), buffer->size());
        ASSERT_TRUE(decoder->set_data(&slice).ok());
        CaptureBinaryConsumer selected_strings;
        ASSERT_TRUE(decoder->decode_selected_binary_values(selection, selected_strings).ok());
        ASSERT_EQ(selected_strings.refs.size(), 2);
        EXPECT_EQ(selected_strings.refs[0].to_string_view(), "prefix-a");
        EXPECT_EQ(selected_strings.refs[1].to_string_view(), "other");
        CaptureBinaryConsumer trailing_string;
        ASSERT_TRUE(decoder->decode_binary_values(1, trailing_string).ok());
        ASSERT_EQ(trailing_string.refs.size(), 1);
        EXPECT_EQ(trailing_string.refs[0].to_string_view(), "other-tail");
    }

    const std::vector<float> floats {1.0F, -2.5F, 3.25F, 9.5F};
    std::vector<uint8_t> encoded_floats(floats.size() * sizeof(float));
    for (size_t row = 0; row < floats.size(); ++row) {
        const auto* bytes = reinterpret_cast<const uint8_t*>(&floats[row]);
        for (size_t byte = 0; byte < sizeof(float); ++byte) {
            encoded_floats[byte * floats.size() + row] = bytes[byte];
        }
    }
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::FLOAT, tparquet::Encoding::BYTE_STREAM_SPLIT,
                                     decoder)
                        .ok());
    decoder->set_type_length(sizeof(float));
    Slice float_slice(encoded_floats.data(), encoded_floats.size());
    ASSERT_TRUE(decoder->set_data(&float_slice).ok());
    CaptureFixedConsumer selected_floats;
    ASSERT_TRUE(decoder->decode_selected_fixed_values(selection, selected_floats).ok());
    EXPECT_EQ(selected_floats.values<float>(), std::vector<float>({1.0F, 3.25F}));
    CaptureFixedConsumer trailing_float;
    ASSERT_TRUE(decoder->decode_fixed_values(1, trailing_float).ok());
    EXPECT_EQ(trailing_float.values<float>(), std::vector<float>({9.5F}));
}

TEST(ParquetV2NativeDecoderTest, ByteStreamSplitRestoresFixedWidthRows) {
    const std::vector<float> values {1.0F, -2.5F, 3.25F};
    std::vector<uint8_t> encoded(values.size() * sizeof(float));
    for (size_t row = 0; row < values.size(); ++row) {
        const auto* bytes = reinterpret_cast<const uint8_t*>(&values[row]);
        for (size_t byte = 0; byte < sizeof(float); ++byte) {
            encoded[byte * values.size() + row] = bytes[byte];
        }
    }

    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::FLOAT, tparquet::Encoding::BYTE_STREAM_SPLIT,
                                     decoder)
                        .ok());
    decoder->set_type_length(sizeof(float));
    Slice slice(encoded.data(), encoded.size());
    ASSERT_TRUE(decoder->set_data(&slice).ok());
    ASSERT_TRUE(decoder->skip_values(1).ok());
    CaptureFixedConsumer consumer;
    ASSERT_TRUE(decoder->decode_fixed_values(2, consumer).ok());
    EXPECT_EQ(consumer.values<float>(), std::vector<float>({-2.5F, 3.25F}));
}

TEST(ParquetV2NativeDecoderTest, BitPackedLevelCursorOperationsPreservePosition) {
    char encoded[] = {static_cast<char>(0b00001101)};
    Slice levels(encoded, sizeof(encoded));
    LevelDecoder decoder;
    ASSERT_TRUE(decoder.init(&levels, tparquet::Encoding::BIT_PACKED, 1, 4).ok());

    level_t value = -1;
    EXPECT_EQ(decoder.get_next_run(&value, 4), 1);
    EXPECT_EQ(value, 1);
    EXPECT_EQ(decoder.get_next(), 0);
    decoder.rewind_one();
    EXPECT_EQ(decoder.get_next(), 0);
    level_t tail[2] = {-1, -1};
    EXPECT_EQ(decoder.get_levels(tail, 2), 2);
    EXPECT_EQ(tail[0], 1);
    EXPECT_EQ(tail[1], 1);

    char truncated_rle[] = {0x01, 0x00, 0x00, 0x00, 0x03};
    Slice truncated_levels(truncated_rle, sizeof(truncated_rle));
    LevelDecoder rle_decoder;
    ASSERT_TRUE(rle_decoder.init(&truncated_levels, tparquet::Encoding::RLE, 1, 1).ok());
    level_t truncated_value = -1;
    EXPECT_EQ(rle_decoder.get_levels(&truncated_value, 1), 0);
}

TEST(ParquetV2NativeDecoderTest, BitPackedLevelByteCountDoesNotWrapAtLargeCounts) {
    char placeholder[8] = {};
    constexpr uint32_t num_levels = 1'500'000'000;
    constexpr size_t expected_bytes = 562'500'000;
    Slice levels(placeholder, expected_bytes);
    LevelDecoder decoder;

    ASSERT_TRUE(decoder.init(&levels, tparquet::Encoding::BIT_PACKED, 4, num_levels).ok());
    EXPECT_EQ(levels.size, 0);
}

TEST(ParquetV2NativeDecoderTest, RejectsLevelsAboveSchemaMaximumOnEveryDecodePath) {
    auto make_decoder = [](tparquet::Encoding::type encoding) {
        static char encoded[] = {0x03}; // width=2 value=3, while the schema maximum is 2.
        static char rle_encoded[] = {0x02, 0x00, 0x00, 0x00, 0x02, 0x03};
        Slice levels = encoding == tparquet::Encoding::BIT_PACKED
                               ? Slice(encoded, sizeof(encoded))
                               : Slice(rle_encoded, sizeof(rle_encoded));
        LevelDecoder decoder;
        EXPECT_TRUE(decoder.init(&levels, encoding, 2, 1).ok());
        return decoder;
    };

    for (auto encoding : {tparquet::Encoding::BIT_PACKED, tparquet::Encoding::RLE}) {
        {
            auto decoder = make_decoder(encoding);
            level_t value = -1;
            EXPECT_EQ(decoder.get_levels(&value, 1), 0);
        }
        {
            auto decoder = make_decoder(encoding);
            level_t value = -1;
            EXPECT_EQ(decoder.get_next_run(&value, 1), 0);
        }
        {
            auto decoder = make_decoder(encoding);
            EXPECT_EQ(decoder.get_next(), -1);
        }
    }
}

TEST(ParquetV2NativeDecoderTest, NestedReadersRejectRleAndBitPackedLevelsAboveMaximum) {
    for (auto encoding : {tparquet::Encoding::RLE, tparquet::Encoding::BIT_PACKED}) {
        const std::vector<uint8_t> payload = encoding == tparquet::Encoding::RLE
                                                     ? std::vector<uint8_t> {2, 0, 0, 0, 2, 3}
                                                     : std::vector<uint8_t> {3};
        tparquet::PageHeader header;
        header.type = tparquet::PageType::DATA_PAGE;
        header.__set_compressed_page_size(payload.size());
        header.__set_uncompressed_page_size(payload.size());
        header.__isset.data_page_header = true;
        header.data_page_header.__set_num_values(1);
        header.data_page_header.__set_encoding(tparquet::Encoding::PLAIN);
        header.data_page_header.__set_repetition_level_encoding(encoding);
        header.data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);

        EXPECT_TRUE(load_malformed_nested_page(header, payload, 1, 2).is<ErrorCode::CORRUPTION>());
    }
}

TEST(ParquetV2NativeDecoderTest, TruncatedBooleanStreamsFailWhileSkipping) {
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(
            Decoder::get_decoder(tparquet::Type::BOOLEAN, tparquet::Encoding::PLAIN, decoder).ok());
    char plain_boolean[] = {static_cast<char>(0xFF)};
    Slice plain_slice(plain_boolean, sizeof(plain_boolean));
    ASSERT_TRUE(decoder->set_data(&plain_slice).ok());
    EXPECT_FALSE(decoder->skip_values(9).ok());

    ASSERT_TRUE(
            Decoder::get_decoder(tparquet::Type::BOOLEAN, tparquet::Encoding::RLE, decoder).ok());
    // A bit-packed run header for eight values without its required payload byte.
    char truncated_rle[] = {0x01, 0x00, 0x00, 0x00, 0x03};
    Slice rle_slice(truncated_rle, sizeof(truncated_rle));
    ASSERT_TRUE(decoder->set_data(&rle_slice).ok());
    EXPECT_FALSE(decoder->skip_values(1).ok());

    // The first literal group is complete but the second has only its header. Exact-count checks
    // must reject both dense and selected decodes instead of exposing the retained vector tail.
    char partially_truncated_rle[] = {0x03, 0x00, 0x00, 0x00, 0x03, static_cast<char>(0xFF), 0x03};
    Slice partially_truncated_slice(partially_truncated_rle, sizeof(partially_truncated_rle));
    ASSERT_TRUE(decoder->set_data(&partially_truncated_slice).ok());
    CaptureFixedConsumer dense_consumer;
    EXPECT_FALSE(decoder->decode_fixed_values(9, dense_consumer).ok());
    ASSERT_TRUE(decoder->set_data(&partially_truncated_slice).ok());
    CaptureFixedConsumer selected_consumer;
    const ParquetSelection select_tail {
            .total_values = 9, .selected_values = 1, .ranges = {{.first = 8, .count = 1}}};
    EXPECT_FALSE(decoder->decode_selected_fixed_values(select_tail, selected_consumer).ok());
}

TEST(ParquetV2NativeDecoderTest, CompactRleSkipsKeepScratchBounded) {
    constexpr uint32_t value_count = 1U << 20;
    uint8_t header[16];
    uint8_t* header_end = encode_varint32(header, value_count << 1);

    int32_t dictionary_length = 0;
    auto dictionary = make_byte_array_dictionary({"only"}, &dictionary_length);
    ByteArrayDictDecoder dictionary_decoder;
    ASSERT_TRUE(dictionary_decoder.set_dict(dictionary, dictionary_length, 1).ok());
    std::vector<char> dictionary_indices {0};
    dictionary_indices.insert(dictionary_indices.end(), reinterpret_cast<char*>(header),
                              reinterpret_cast<char*>(header_end));
    Slice dictionary_slice(dictionary_indices.data(), dictionary_indices.size());
    ASSERT_TRUE(dictionary_decoder.set_data(&dictionary_slice).ok());
    ParquetDecodeSource& dictionary_source = dictionary_decoder;
    ASSERT_TRUE(dictionary_source.skip_values(value_count).ok());
    EXPECT_LE(dictionary_decoder.retained_scratch_bytes(), 4096 * sizeof(uint32_t));

    std::vector<char> boolean_payload(reinterpret_cast<char*>(header),
                                      reinterpret_cast<char*>(header_end));
    boolean_payload.push_back(0);
    std::vector<char> boolean_page(sizeof(uint32_t) + boolean_payload.size());
    encode_fixed32_le(reinterpret_cast<uint8_t*>(boolean_page.data()), boolean_payload.size());
    memcpy(boolean_page.data() + sizeof(uint32_t), boolean_payload.data(), boolean_payload.size());
    Slice boolean_slice(boolean_page.data(), boolean_page.size());
    std::unique_ptr<Decoder> boolean_decoder;
    ASSERT_TRUE(
            Decoder::get_decoder(tparquet::Type::BOOLEAN, tparquet::Encoding::RLE, boolean_decoder)
                    .ok());
    ASSERT_TRUE(boolean_decoder->set_data(&boolean_slice).ok());
    ASSERT_TRUE(boolean_decoder->skip_values(value_count).ok());
    EXPECT_LE(boolean_decoder->retained_scratch_bytes(), 4096);
}

TEST(ParquetV2NativeDecoderTest, CompactDeltaSkipKeepsScratchBounded) {
    constexpr uint32_t delta_count = 1U << 20;
    std::vector<uint8_t> encoded(64);
    uint8_t* cursor = encoded.data();
    cursor = encode_varint32(cursor, delta_count);
    cursor = encode_varint32(cursor, 1);
    cursor = encode_varint32(cursor, delta_count + 1);
    cursor = encode_varint32(cursor, 0); // first value, zig-zag encoded
    cursor = encode_varint32(cursor, 0); // minimum delta
    *cursor++ = 0;                       // zero-width miniblock
    encoded.resize(cursor - encoded.data());

    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::INT32, tparquet::Encoding::DELTA_BINARY_PACKED,
                                     decoder)
                        .ok());
    decoder->set_expected_values(delta_count + 1);
    Slice slice(encoded.data(), encoded.size());
    ASSERT_TRUE(decoder->set_data(&slice).ok());
    ASSERT_TRUE(decoder->skip_values(delta_count + 1).ok());
    EXPECT_LE(decoder->retained_scratch_bytes(), 4096 * sizeof(int32_t) + 1);
}

TEST(ParquetV2NativeDecoderTest, DeltaPaddingBitCountIsWidenedBeforeCursorAdvance) {
    int64_t padding_bits = 0;
    ASSERT_TRUE(detail::checked_delta_padding_bits(64, std::numeric_limits<uint32_t>::max(),
                                                   &padding_bits)
                        .ok());
    EXPECT_EQ(padding_bits, 64LL * std::numeric_limits<uint32_t>::max());
    EXPECT_GT(padding_bits, std::numeric_limits<uint32_t>::max());
}

TEST(ParquetV2NativeDecoderTest, DeltaByteArraySkipsKeepScratchBounded) {
    constexpr size_t value_count = 1U << 16;
    std::vector<::parquet::ByteArray> values(value_count);
    auto byte_descriptor = descriptor(::parquet::Type::BYTE_ARRAY);
    for (const auto encoding :
         {::parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY, ::parquet::Encoding::DELTA_BYTE_ARRAY}) {
        auto encoder = ::parquet::MakeTypedEncoder<::parquet::ByteArrayType>(encoding, false,
                                                                             byte_descriptor.get());
        encoder->Put(values.data(), static_cast<int>(values.size()));
        auto encoded = encoder->FlushValues();

        std::unique_ptr<Decoder> decoder;
        ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::BYTE_ARRAY,
                                         static_cast<tparquet::Encoding::type>(encoding), decoder)
                            .ok());
        decoder->set_expected_values(value_count);
        Slice slice(encoded->data(), encoded->size());
        ASSERT_TRUE(decoder->set_data(&slice).ok());
        ASSERT_TRUE(decoder->skip_values(value_count).ok());
        EXPECT_LT(decoder->retained_scratch_bytes(), 1UL << 20);
    }
}

TEST(ParquetV2NativeDecoderTest, DeltaByteArrayLongPrefixSparseWorkIsByteBounded) {
    constexpr size_t value_count = 128;
    const std::string repeated_value(256UL << 10, 'x');
    std::vector<::parquet::ByteArray> values(
            value_count,
            ::parquet::ByteArray(static_cast<uint32_t>(repeated_value.size()),
                                 reinterpret_cast<const uint8_t*>(repeated_value.data())));
    auto byte_descriptor = descriptor(::parquet::Type::BYTE_ARRAY);
    auto encoder = ::parquet::MakeTypedEncoder<::parquet::ByteArrayType>(
            ::parquet::Encoding::DELTA_BYTE_ARRAY, false, byte_descriptor.get());
    encoder->Put(values.data(), static_cast<int>(values.size()));
    auto encoded = encoder->FlushValues();

    auto make_decoder = [&]() {
        std::unique_ptr<Decoder> decoder;
        EXPECT_TRUE(Decoder::get_decoder(tparquet::Type::BYTE_ARRAY,
                                         tparquet::Encoding::DELTA_BYTE_ARRAY, decoder)
                            .ok());
        decoder->set_expected_values(value_count);
        Slice slice(encoded->data(), encoded->size());
        EXPECT_TRUE(decoder->set_data(&slice).ok());
        return decoder;
    };

    auto skipped = make_decoder();
    ASSERT_TRUE(skipped->skip_values(value_count).ok());
    EXPECT_LT(skipped->retained_scratch_bytes(), 8UL << 20);

    auto sparse = make_decoder();
    CaptureBinaryConsumer consumer;
    const ParquetSelection selection {
            .total_values = value_count,
            .selected_values = 1,
            .ranges = {{.first = value_count - 1, .count = 1}},
    };
    ASSERT_TRUE(sparse->decode_selected_binary_values(selection, consumer).ok());
    ASSERT_EQ(consumer.refs.size(), 1);
    EXPECT_EQ(consumer.refs[0].to_string_view(), repeated_value);
    EXPECT_LT(sparse->retained_scratch_bytes(), 8UL << 20);
}

TEST(ParquetV2NativeDecoderTest, DeltaFixedWidthValidatesFilteredAndSkippedValues) {
    const std::vector<std::string> values {"good", "bad"};
    std::vector<::parquet::ByteArray> byte_arrays;
    for (const auto& value : values) {
        byte_arrays.emplace_back(static_cast<uint32_t>(value.size()),
                                 reinterpret_cast<const uint8_t*>(value.data()));
    }
    auto byte_descriptor = descriptor(::parquet::Type::BYTE_ARRAY);
    auto encoder = ::parquet::MakeTypedEncoder<::parquet::ByteArrayType>(
            ::parquet::Encoding::DELTA_BYTE_ARRAY, false, byte_descriptor.get());
    encoder->Put(byte_arrays.data(), static_cast<int>(byte_arrays.size()));
    auto buffer = encoder->FlushValues();

    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::FIXED_LEN_BYTE_ARRAY,
                                     tparquet::Encoding::DELTA_BYTE_ARRAY, decoder)
                        .ok());
    decoder->set_type_length(4);
    Slice slice(buffer->data(), buffer->size());
    ASSERT_TRUE(decoder->set_data(&slice).ok());
    CaptureFixedConsumer selected_consumer;
    const ParquetSelection select_good {
            .total_values = 2, .selected_values = 1, .ranges = {{.first = 0, .count = 1}}};
    EXPECT_TRUE(decoder->decode_selected_fixed_values(select_good, selected_consumer)
                        .is<ErrorCode::CORRUPTION>());

    ASSERT_TRUE(decoder->set_data(&slice).ok());
    EXPECT_TRUE(decoder->skip_values(2).is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, TruncatedFixedWidthPlainKeepsPublicErrorKeyword) {
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::FIXED_LEN_BYTE_ARRAY,
                                     tparquet::Encoding::PLAIN, decoder)
                        .ok());
    decoder->set_type_length(4);
    char truncated[] = {'a', 'b', 'c'};
    Slice slice(truncated, sizeof(truncated));
    ASSERT_TRUE(decoder->set_data(&slice).ok());
    CaptureFixedConsumer consumer;
    const auto status = decoder->decode_fixed_values(1, consumer);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Unexpected end of stream"), std::string::npos);
}

TEST(ParquetV2NativeDecoderTest, DeltaSkipRequiresTheRequestedValueCount) {
    const std::vector<int32_t> integers {7};
    auto int_descriptor = descriptor(::parquet::Type::INT32);
    auto encoder = ::parquet::MakeTypedEncoder<::parquet::Int32Type>(
            ::parquet::Encoding::DELTA_BINARY_PACKED, false, int_descriptor.get());
    encoder->Put(integers.data(), static_cast<int>(integers.size()));
    auto buffer = encoder->FlushValues();

    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::INT32, tparquet::Encoding::DELTA_BINARY_PACKED,
                                     decoder)
                        .ok());
    Slice slice(buffer->data(), buffer->size());
    ASSERT_TRUE(decoder->set_data(&slice).ok());
    EXPECT_FALSE(decoder->skip_values(2).ok());
}

TEST(ParquetV2NativeDecoderTest, DeltaHeadersAndLengthsAreBoundedBeforeAllocation) {
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::INT32, tparquet::Encoding::DELTA_BINARY_PACKED,
                                     decoder)
                        .ok());
    decoder->set_expected_values(1);
    // block_size=128, miniblocks=4, total_values=INT_MAX, first_value=0.
    char oversized_count[] = {static_cast<char>(0x80),
                              0x01,
                              0x04,
                              static_cast<char>(0xFF),
                              static_cast<char>(0xFF),
                              static_cast<char>(0xFF),
                              static_cast<char>(0xFF),
                              0x07,
                              0x00};
    Slice oversized_count_slice(oversized_count, sizeof(oversized_count));
    EXPECT_TRUE(decoder->set_data(&oversized_count_slice).is<ErrorCode::CORRUPTION>());

    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::BYTE_ARRAY,
                                     tparquet::Encoding::DELTA_LENGTH_BYTE_ARRAY, decoder)
                        .ok());
    decoder->set_expected_values(1);
    // A single decoded length of 1 GiB with no following payload bytes must fail before the
    // decoder resizes its backing data buffer.
    char oversized_length[] = {static_cast<char>(0x80),
                               0x01,
                               0x04,
                               0x01,
                               static_cast<char>(0x80),
                               static_cast<char>(0x80),
                               static_cast<char>(0x80),
                               static_cast<char>(0x80),
                               0x08};
    Slice oversized_length_slice(oversized_length, sizeof(oversized_length));
    CaptureBinaryConsumer consumer;
    EXPECT_TRUE(decoder->set_data(&oversized_length_slice).is<ErrorCode::CORRUPTION>());
    EXPECT_TRUE(consumer.refs.empty());

    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::BYTE_ARRAY,
                                     tparquet::Encoding::DELTA_BYTE_ARRAY, decoder)
                        .ok());
    decoder->set_expected_values(1);
    std::vector<uint8_t> huge_prefix(64);
    uint8_t* cursor = huge_prefix.data();
    cursor = encode_varint32(cursor, 128);
    cursor = encode_varint32(cursor, 4);
    cursor = encode_varint32(cursor, 1);
    cursor = encode_varint32(cursor, std::numeric_limits<uint32_t>::max() - 1);
    cursor = encode_varint32(cursor, 128);
    cursor = encode_varint32(cursor, 4);
    cursor = encode_varint32(cursor, 1);
    cursor = encode_varint32(cursor, 0); // one empty suffix
    huge_prefix.resize(cursor - huge_prefix.data());
    Slice huge_prefix_slice(huge_prefix.data(), huge_prefix.size());
    ASSERT_TRUE(decoder->set_data(&huge_prefix_slice).ok());
    EXPECT_TRUE(decoder->decode_binary_values(1, consumer).is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_LT(decoder->retained_scratch_bytes(), 1UL << 20);
}

TEST(ParquetV2NativeDecoderTest, EmptyDeltaLengthPageResetsDecoderState) {
    const std::vector<std::string> strings {"old", "state"};
    std::vector<::parquet::ByteArray> byte_arrays;
    for (const auto& value : strings) {
        byte_arrays.emplace_back(static_cast<uint32_t>(value.size()),
                                 reinterpret_cast<const uint8_t*>(value.data()));
    }
    auto byte_descriptor = descriptor(::parquet::Type::BYTE_ARRAY);
    auto encoder = ::parquet::MakeTypedEncoder<::parquet::ByteArrayType>(
            ::parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY, false, byte_descriptor.get());
    encoder->Put(byte_arrays.data(), static_cast<int>(byte_arrays.size()));
    auto buffer = encoder->FlushValues();

    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::BYTE_ARRAY,
                                     tparquet::Encoding::DELTA_LENGTH_BYTE_ARRAY, decoder)
                        .ok());
    Slice valid(buffer->data(), buffer->size());
    ASSERT_TRUE(decoder->set_data(&valid).ok());
    Slice empty;
    EXPECT_TRUE(decoder->set_data(&empty).is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, ByteStreamSplitRejectsPartialRowsAtPageBoundary) {
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::FLOAT, tparquet::Encoding::BYTE_STREAM_SPLIT,
                                     decoder)
                        .ok());
    decoder->set_type_length(sizeof(float));
    char partial_row[] = {0, 1, 2, 3, 4};
    Slice slice(partial_row, sizeof(partial_row));
    EXPECT_TRUE(decoder->set_data(&slice).is<ErrorCode::CORRUPTION>());
}

Status read_scripted_map(const DataTypePtr& key_type, size_t key_rows, size_t value_rows,
                         size_t key_values, size_t value_values,
                         std::vector<level_t> key_rep_levels, std::vector<level_t> value_rep_levels,
                         bool value_eof, std::vector<level_t> key_def_levels = {},
                         std::vector<level_t> value_def_levels = {}) {
    auto value_type = make_nullable(std::make_shared<DataTypeInt32>());
    auto map_type = std::make_shared<DataTypeMap>(key_type, value_type);
    ColumnPtr column = map_type->create_column();
    NativeFieldSchema field;
    field.name = "m";
    field.data_type = map_type;
    field.definition_level = 1;
    field.repetition_level = 1;
    field.repeated_parent_def_level = 0;

    if (key_def_levels.empty()) {
        key_def_levels.assign(key_rep_levels.size(), 1);
    }
    if (value_def_levels.empty()) {
        value_def_levels.assign(value_rep_levels.size(), 1);
    }
    auto key_reader = std::make_unique<ScriptedColumnReader>(
            key_rows, key_values, true, std::move(key_rep_levels), std::move(key_def_levels));
    auto value_reader = std::make_unique<ScriptedColumnReader>(value_rows, value_values, value_eof,
                                                               std::move(value_rep_levels),
                                                               std::move(value_def_levels));
    MapColumnReader reader(scripted_row_ranges(), key_rows, nullptr, nullptr);
    RETURN_IF_ERROR(reader.init(std::move(key_reader), std::move(value_reader), &field));

    auto root = std::make_shared<NativeMapSchemaNode>(std::make_shared<NativeScalarSchemaNode>(),
                                                      std::make_shared<NativeScalarSchemaNode>());
    FilterMap filter;
    size_t read_rows = 0;
    bool eof = false;
    return reader.read_column_data(column, map_type, root, filter, key_rows, &read_rows, &eof,
                                   false);
}

TEST(ParquetV2NativeDecoderTest, MapReaderUsesKeyShapeForNestedValues) {
    auto int_type = std::make_shared<DataTypeInt32>();
    EXPECT_TRUE(read_scripted_map(int_type, 1, 1, 2, 2, {0, 1}, {0, 2, 1}, true).ok());
}

TEST(ParquetV2NativeDecoderTest, MapReaderRejectsShiftedEntryDistribution) {
    auto int_type = std::make_shared<DataTypeInt32>();
    EXPECT_TRUE(read_scripted_map(int_type, 2, 2, 4, 4, {0, 1, 0, 1}, {0, 1, 1, 0}, true)
                        .is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, MapReaderRejectsShiftedEntryPresence) {
    auto int_type = std::make_shared<DataTypeInt32>();
    EXPECT_TRUE(read_scripted_map(int_type, 2, 2, 2, 2, {0, 0}, {0, 0}, true, {0, 1}, {1, 0})
                        .is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, ComplexReadersRejectMalformedSiblingCounts) {
    auto int_type = std::make_shared<DataTypeInt32>();
    EXPECT_TRUE(
            read_scripted_map(int_type, 2, 1, 2, 1, {0, 0}, {0}, true).is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, MapReaderPreservesNullableKeysWrittenByDoris) {
    auto nullable_key = make_nullable(std::make_shared<DataTypeInt32>());
    EXPECT_TRUE(read_scripted_map(nullable_key, 1, 1, 1, 1, {0}, {0}, true).ok());
}

TEST(ParquetV2NativeDecoderTest, StructReaderRejectsShortSibling) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto struct_type =
            std::make_shared<DataTypeStruct>(DataTypes {int_type, int_type}, Strings {"a", "b"});
    ColumnPtr column = struct_type->create_column();
    NativeFieldSchema field;
    field.name = "s";
    field.data_type = struct_type;
    field.children.resize(2);
    field.children[0].name = "a";
    field.children[1].name = "b";

    std::unordered_map<std::string, std::unique_ptr<ColumnReader>> children;
    children["a"] = std::make_unique<ScriptedColumnReader>(2, 2, true, std::vector<level_t> {0, 0},
                                                           std::vector<level_t> {0, 0});
    children["b"] = std::make_unique<ScriptedColumnReader>(1, 1, true, std::vector<level_t> {0},
                                                           std::vector<level_t> {0});
    StructColumnReader reader(scripted_row_ranges(), 2, nullptr, nullptr);
    ASSERT_TRUE(reader.init(std::move(children), &field).ok());
    auto root = std::make_shared<NativeStructSchemaNode>();
    root->add_child("a", "a", std::make_shared<NativeScalarSchemaNode>());
    root->add_child("b", "b", std::make_shared<NativeScalarSchemaNode>());
    FilterMap filter;
    size_t read_rows = 0;
    bool eof = false;
    EXPECT_TRUE(
            reader.read_column_data(column, struct_type, root, filter, 2, &read_rows, &eof, false)
                    .is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, StructReaderRejectsShiftedRepeatedParentShape) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto struct_type =
            std::make_shared<DataTypeStruct>(DataTypes {int_type, int_type}, Strings {"a", "b"});
    ColumnPtr column = struct_type->create_column();
    NativeFieldSchema field;
    field.name = "s";
    field.repetition_level = 1;
    field.children.resize(2);
    field.children[0].name = "a";
    field.children[1].name = "b";

    std::unordered_map<std::string, std::unique_ptr<ColumnReader>> children;
    children["a"] = std::make_unique<ScriptedColumnReader>(
            2, 3, true, std::vector<level_t> {0, 1, 0}, std::vector<level_t> {0, 0, 0});
    children["b"] = std::make_unique<ScriptedColumnReader>(
            2, 3, true, std::vector<level_t> {0, 0, 1}, std::vector<level_t> {0, 0, 0});
    StructColumnReader reader(scripted_row_ranges(), 2, nullptr, nullptr);
    ASSERT_TRUE(reader.init(std::move(children), &field).ok());
    auto root = std::make_shared<NativeStructSchemaNode>();
    root->add_child("a", "a", std::make_shared<NativeScalarSchemaNode>());
    root->add_child("b", "b", std::make_shared<NativeScalarSchemaNode>());
    FilterMap filter;
    size_t read_rows = 0;
    bool eof = false;
    EXPECT_TRUE(
            reader.read_column_data(column, struct_type, root, filter, 2, &read_rows, &eof, false)
                    .is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, StructReaderRejectsShiftedOptionalParentPresence) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto struct_type =
            std::make_shared<DataTypeStruct>(DataTypes {int_type, int_type}, Strings {"a", "b"});
    ColumnPtr column = struct_type->create_column();
    NativeFieldSchema field;
    field.name = "s";
    field.data_type = struct_type;
    field.definition_level = 1;
    field.children.resize(2);
    field.children[0].name = "a";
    field.children[1].name = "b";

    std::unordered_map<std::string, std::unique_ptr<ColumnReader>> children;
    children["a"] = std::make_unique<ScriptedColumnReader>(2, 2, true, std::vector<level_t> {0, 0},
                                                           std::vector<level_t> {0, 1});
    children["b"] = std::make_unique<ScriptedColumnReader>(2, 2, true, std::vector<level_t> {0, 0},
                                                           std::vector<level_t> {1, 0});
    StructColumnReader reader(scripted_row_ranges(), 2, nullptr, nullptr);
    ASSERT_TRUE(reader.init(std::move(children), &field).ok());
    auto root = std::make_shared<NativeStructSchemaNode>();
    root->add_child("a", "a", std::make_shared<NativeScalarSchemaNode>());
    root->add_child("b", "b", std::make_shared<NativeScalarSchemaNode>());
    FilterMap filter;
    size_t read_rows = 0;
    bool eof = false;
    EXPECT_TRUE(
            reader.read_column_data(column, struct_type, root, filter, 2, &read_rows, &eof, false)
                    .is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, DecoderOwnedHighWaterScratchIsReleased) {
    constexpr size_t value_count = 1UL << 20;
    std::vector<uint8_t> encoded(value_count * sizeof(float), 0);
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::FLOAT, tparquet::Encoding::BYTE_STREAM_SPLIT,
                                     decoder)
                        .ok());
    decoder->set_type_length(sizeof(float));
    Slice slice(encoded.data(), encoded.size());
    ASSERT_TRUE(decoder->set_data(&slice).ok());
    CaptureFixedConsumer consumer;
    ASSERT_TRUE(decoder->decode_fixed_values(value_count, consumer).ok());
    ASSERT_GT(decoder->retained_scratch_bytes(), 1UL << 20);
    EXPECT_EQ(decoder->active_scratch_bytes(), value_count * sizeof(float));

    std::vector<uint8_t> ordinary_encoded(sizeof(float), 0);
    Slice ordinary_slice(ordinary_encoded.data(), ordinary_encoded.size());
    ASSERT_TRUE(decoder->set_data(&ordinary_slice).ok());
    ASSERT_TRUE(decoder->decode_fixed_values(1, consumer).ok());
    // Capacity records the high-water allocation while size records this batch. The reader needs
    // both to distinguish stable large batches from a one-off outlier before releasing capacity.
    EXPECT_EQ(decoder->active_scratch_bytes(), sizeof(float));
    ASSERT_GT(decoder->retained_scratch_bytes(), 1UL << 20);
    decoder->release_scratch(64UL << 10);
    EXPECT_LE(decoder->retained_scratch_bytes(), 64UL << 10);
}

TEST(ParquetV2NativeDecoderTest, DeltaByteArrayScratchReleasePreservesPrefixState) {
    const std::vector<std::string> values {std::string(4096, 'x'), "shared-prefix-a",
                                           "shared-prefix-b", "shared-prefix-c"};
    std::vector<::parquet::ByteArray> byte_arrays;
    for (const auto& value : values) {
        byte_arrays.emplace_back(static_cast<uint32_t>(value.size()),
                                 reinterpret_cast<const uint8_t*>(value.data()));
    }
    auto byte_descriptor = descriptor(::parquet::Type::BYTE_ARRAY);
    auto encoder = ::parquet::MakeTypedEncoder<::parquet::ByteArrayType>(
            ::parquet::Encoding::DELTA_BYTE_ARRAY, false, byte_descriptor.get());
    encoder->Put(byte_arrays.data(), static_cast<int>(byte_arrays.size()));
    auto encoded = encoder->FlushValues();

    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::BYTE_ARRAY,
                                     tparquet::Encoding::DELTA_BYTE_ARRAY, decoder)
                        .ok());
    decoder->set_expected_values(values.size());
    Slice slice(encoded->data(), encoded->size());
    ASSERT_TRUE(decoder->set_data(&slice).ok());
    CaptureBinaryConsumer consumer;
    ASSERT_TRUE(decoder->decode_binary_values(1, consumer).ok());
    ASSERT_TRUE(decoder->decode_binary_values(1, consumer).ok());
    decoder->release_scratch(64);
    ASSERT_TRUE(decoder->decode_binary_values(2, consumer).ok());
    ASSERT_EQ(consumer.refs.size(), 2);
    EXPECT_EQ(consumer.refs[0].to_string_view(), values[2]);
    EXPECT_EQ(consumer.refs[1].to_string_view(), values[3]);
}

TEST(ParquetV2NativeDecoderTest, DeltaBitPackScratchReleasePreservesMiniblockState) {
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::INT32, tparquet::Encoding::DELTA_BINARY_PACKED,
                                     decoder)
                        .ok());

    std::vector<uint8_t> outlier_header(64);
    uint8_t* cursor = outlier_header.data();
    cursor = encode_varint32(cursor, 512); // values per block
    cursor = encode_varint32(cursor, 16);  // miniblocks per block
    cursor = encode_varint32(cursor, 2);   // total values
    cursor = encode_varint32(cursor, 0);   // first value, zig-zag encoded
    cursor = encode_varint32(cursor, 0);   // minimum delta, zig-zag encoded
    memset(cursor, 0, 16);                 // one zero bit width per miniblock
    cursor += 16;
    outlier_header.resize(cursor - outlier_header.data());
    decoder->set_expected_values(2);
    Slice outlier_slice(outlier_header.data(), outlier_header.size());
    ASSERT_TRUE(decoder->set_data(&outlier_slice).ok());
    CaptureFixedConsumer consumer;
    ASSERT_TRUE(decoder->decode_fixed_values(1, consumer).ok());

    std::vector<int32_t> values(40);
    std::iota(values.begin(), values.end(), 0);
    auto int_descriptor = descriptor(::parquet::Type::INT32);
    auto encoder = ::parquet::MakeTypedEncoder<::parquet::Int32Type>(
            ::parquet::Encoding::DELTA_BINARY_PACKED, false, int_descriptor.get());
    encoder->Put(values.data(), static_cast<int>(values.size()));
    auto encoded = encoder->FlushValues();
    decoder->set_expected_values(values.size());
    Slice slice(encoded->data(), encoded->size());
    ASSERT_TRUE(decoder->set_data(&slice).ok());
    ASSERT_TRUE(decoder->decode_fixed_values(1, consumer).ok());
    decoder->release_scratch(8);
    ASSERT_TRUE(decoder->decode_fixed_values(values.size() - 1, consumer).ok());
    EXPECT_EQ(consumer.values<int32_t>().size(), values.size() + 1);
}

TEST(ParquetV2NativeDecoderTest, PageHeaderRejectsSignedAndV2LevelSizeCorruption) {
    auto parse_header = [](tparquet::PageHeader header) {
        std::vector<uint8_t> bytes;
        ThriftSerializer serializer(/*compact=*/true, 128);
        DORIS_CHECK(serializer.serialize(&header, &bytes).ok());
        if (header.compressed_page_size > 0) {
            bytes.resize(bytes.size() + header.compressed_page_size);
        }
        MemoryBufferedReader reader(bytes);
        tparquet::ColumnMetaData metadata;
        ParquetPageReadContext context(false, "");
        PageReader<false, false> page_reader(&reader, nullptr, 0, bytes.size(), 1, metadata,
                                             context);
        return page_reader.parse_page_header();
    };

    tparquet::PageHeader negative;
    negative.type = tparquet::PageType::DATA_PAGE;
    negative.__set_compressed_page_size(-1);
    negative.__set_uncompressed_page_size(1);
    negative.__isset.data_page_header = true;
    negative.data_page_header.__set_num_values(1);
    EXPECT_TRUE(parse_header(negative).is<ErrorCode::CORRUPTION>());

    tparquet::PageHeader oversized_levels;
    oversized_levels.type = tparquet::PageType::DATA_PAGE_V2;
    oversized_levels.__set_compressed_page_size(1);
    oversized_levels.__set_uncompressed_page_size(1);
    oversized_levels.__isset.data_page_header_v2 = true;
    oversized_levels.data_page_header_v2.__set_num_values(1);
    oversized_levels.data_page_header_v2.__set_num_rows(1);
    oversized_levels.data_page_header_v2.__set_repetition_levels_byte_length(2);
    oversized_levels.data_page_header_v2.__set_definition_levels_byte_length(0);
    EXPECT_TRUE(parse_header(oversized_levels).is<ErrorCode::CORRUPTION>());

    tparquet::PageHeader impossible_counts = oversized_levels;
    impossible_counts.__set_compressed_page_size(0);
    impossible_counts.__set_uncompressed_page_size(0);
    impossible_counts.data_page_header_v2.__set_repetition_levels_byte_length(0);
    impossible_counts.data_page_header_v2.__set_num_nulls(2);
    EXPECT_TRUE(parse_header(impossible_counts).is<ErrorCode::CORRUPTION>());

    tparquet::PageHeader missing_layout;
    missing_layout.type = tparquet::PageType::DATA_PAGE;
    missing_layout.__set_compressed_page_size(0);
    missing_layout.__set_uncompressed_page_size(0);
    EXPECT_TRUE(parse_header(missing_layout).is<ErrorCode::CORRUPTION>());

    auto swapped_layout = oversized_levels;
    swapped_layout.type = tparquet::PageType::DATA_PAGE;
    EXPECT_TRUE(parse_header(swapped_layout).is<ErrorCode::CORRUPTION>());

    auto competing_layouts = negative;
    competing_layouts.__set_compressed_page_size(0);
    competing_layouts.__set_uncompressed_page_size(0);
    competing_layouts.__isset.data_page_header_v2 = true;
    competing_layouts.data_page_header_v2.__set_num_values(0);
    competing_layouts.data_page_header_v2.__set_num_rows(0);
    competing_layouts.data_page_header_v2.__set_num_nulls(0);
    competing_layouts.data_page_header_v2.__set_repetition_levels_byte_length(0);
    competing_layouts.data_page_header_v2.__set_definition_levels_byte_length(0);
    EXPECT_TRUE(parse_header(competing_layouts).is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, ShiftedOffsetIndexFallsBackToSequentialPages) {
    auto data_page = [](int32_t value) {
        tparquet::PageHeader header;
        header.type = tparquet::PageType::DATA_PAGE;
        header.__set_compressed_page_size(sizeof(value));
        header.__set_uncompressed_page_size(sizeof(value));
        header.__isset.data_page_header = true;
        header.data_page_header.__set_num_values(1);
        header.data_page_header.__set_encoding(tparquet::Encoding::PLAIN);
        header.data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
        header.data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);
        std::vector<uint8_t> payload(sizeof(value));
        memcpy(payload.data(), &value, sizeof(value));
        return serialize_page(header, payload);
    };
    auto verify_fallback = [&](bool cache_hit) {
        const auto first_page = data_page(10);
        const auto second_page = data_page(20);
        std::vector<uint8_t> bytes = first_page;
        bytes.insert(bytes.end(), second_page.begin(), second_page.end());
        bytes.push_back(0);

        tparquet::OffsetIndex offset_index;
        tparquet::PageLocation first_location;
        first_location.__set_offset(0);
        first_location.__set_compressed_page_size(cast_set<int32_t>(first_page.size() + 1));
        first_location.__set_first_row_index(0);
        tparquet::PageLocation second_location;
        second_location.__set_offset(cast_set<int64_t>(first_page.size() + 1));
        second_location.__set_compressed_page_size(cast_set<int32_t>(second_page.size()));
        second_location.__set_first_row_index(1);
        offset_index.__set_page_locations({first_location, second_location});
        EXPECT_TRUE(
                validate_offset_index(offset_index, {.offset = 0, .length = bytes.size()}, 0, 2));

        const std::string cache_key = cache_hit ? "shifted-index-cache-hit" : "";
        if (cache_hit) {
            auto* page = new DataPage(first_page.size(), true, segment_v2::DATA_PAGE);
            memcpy(page->data(), first_page.data(), first_page.size());
            page->reset_size(first_page.size());
            PageCacheHandle handle;
            StoragePageCache::instance()->insert(
                    StoragePageCache::CacheKey(cache_key, bytes.size(), 0), page, &handle,
                    segment_v2::DATA_PAGE);
        }

        tparquet::ColumnMetaData metadata;
        metadata.__set_type(tparquet::Type::INT32);
        metadata.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
        metadata.__set_num_values(2);
        metadata.__set_total_compressed_size(bytes.size());
        metadata.__set_data_page_offset(0);
        MemoryBufferedReader stream(std::move(bytes));
        PageReader<false, true> reader(&stream, nullptr, 0, metadata.total_compressed_size, 2,
                                       metadata, ParquetPageReadContext(cache_hit, cache_key),
                                       &offset_index);

        ASSERT_TRUE(reader.parse_page_header().ok());
        reader.skip_page_data();
        ASSERT_TRUE(reader.next_page().ok());
        // The rectangles above are non-overlapping but shifted one byte from the serialized pages.
        // Discarding the optional index must continue from the first page's real payload end.
        ASSERT_TRUE(reader.parse_page_header().ok());
        const tparquet::PageHeader* parsed = nullptr;
        ASSERT_TRUE(reader.get_page_header(&parsed).ok());
        EXPECT_EQ(parsed->data_page_header.num_values, 1);
    };
    verify_fallback(false);
    verify_fallback(true);
}

TEST(ParquetV2NativeDecoderTest, FlatPagesRejectLogicalAndPhysicalCardinalityMismatch) {
    auto init_chunk = [](tparquet::PageHeader header, bool with_offset_index) {
        std::vector<uint8_t> payload(static_cast<size_t>(header.compressed_page_size), 0);
        auto bytes = serialize_page(header, payload);
        MemoryBufferedReader reader(bytes);
        tparquet::ColumnChunk chunk;
        chunk.meta_data.__set_type(tparquet::Type::INT32);
        chunk.meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
        chunk.meta_data.__set_num_values(2);
        chunk.meta_data.__set_total_compressed_size(bytes.size());
        chunk.meta_data.__set_data_page_offset(0);
        NativeFieldSchema field;
        field.physical_type = tparquet::Type::INT32;
        field.repetition_level = 0;
        field.definition_level = 0;
        ParquetPageReadContext context(false, "");
        tparquet::OffsetIndex offset_index;
        tparquet::PageLocation location;
        location.__set_offset(0);
        location.__set_compressed_page_size(bytes.size());
        location.__set_first_row_index(0);
        offset_index.__set_page_locations({location});
        if (with_offset_index) {
            ColumnChunkReader<false, true> reader_with_index(&reader, &chunk, &field, &offset_index,
                                                             1, nullptr, context);
            return reader_with_index.init();
        }
        ColumnChunkReader<false, false> sequential_reader(&reader, &chunk, &field, nullptr, 1,
                                                          nullptr, context);
        return sequential_reader.init();
    };

    tparquet::PageHeader v2;
    v2.type = tparquet::PageType::DATA_PAGE_V2;
    v2.__set_compressed_page_size(8);
    v2.__set_uncompressed_page_size(8);
    v2.__isset.data_page_header_v2 = true;
    v2.data_page_header_v2.__set_num_values(2);
    v2.data_page_header_v2.__set_num_rows(1);
    v2.data_page_header_v2.__set_num_nulls(0);
    v2.data_page_header_v2.__set_encoding(tparquet::Encoding::PLAIN);
    v2.data_page_header_v2.__set_repetition_levels_byte_length(0);
    v2.data_page_header_v2.__set_definition_levels_byte_length(0);
    v2.data_page_header_v2.__set_is_compressed(false);
    auto status = init_chunk(v2, false);
    EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
    status = init_chunk(v2, true);
    EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;

    tparquet::PageHeader v1;
    v1.type = tparquet::PageType::DATA_PAGE;
    v1.__set_compressed_page_size(8);
    v1.__set_uncompressed_page_size(8);
    v1.__isset.data_page_header = true;
    v1.data_page_header.__set_num_values(2);
    v1.data_page_header.__set_encoding(tparquet::Encoding::RLE_DICTIONARY);
    v1.data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);
    v1.data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
    status = init_chunk(v1, true);
    EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
}

TEST(ParquetV2NativeDecoderTest, NestedV2PageRejectsOffsetIndexRowSpanMismatch) {
    tparquet::PageHeader header;
    header.type = tparquet::PageType::DATA_PAGE_V2;
    header.__set_compressed_page_size(8);
    header.__set_uncompressed_page_size(8);
    header.__isset.data_page_header_v2 = true;
    header.data_page_header_v2.__set_num_values(2);
    header.data_page_header_v2.__set_num_rows(1);
    header.data_page_header_v2.__set_num_nulls(0);
    header.data_page_header_v2.__set_encoding(tparquet::Encoding::PLAIN);
    header.data_page_header_v2.__set_repetition_levels_byte_length(0);
    header.data_page_header_v2.__set_definition_levels_byte_length(0);
    header.data_page_header_v2.__set_is_compressed(false);
    auto bytes = serialize_page(header, std::vector<uint8_t>(8));
    MemoryBufferedReader stream(bytes);

    tparquet::ColumnChunk chunk;
    chunk.meta_data.__set_type(tparquet::Type::INT32);
    chunk.meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
    chunk.meta_data.__set_num_values(2);
    chunk.meta_data.__set_total_compressed_size(bytes.size());
    chunk.meta_data.__set_data_page_offset(0);
    NativeFieldSchema field;
    field.physical_type = tparquet::Type::INT32;
    field.repetition_level = 1;
    field.definition_level = 1;
    tparquet::OffsetIndex offset_index;
    tparquet::PageLocation location;
    location.__set_offset(0);
    location.__set_compressed_page_size(bytes.size());
    location.__set_first_row_index(0);
    offset_index.__set_page_locations({location});
    ParquetPageReadContext context(false, "");

    ColumnChunkReader<true, true> reader(&stream, &chunk, &field, &offset_index,
                                         /*total_rows=*/2, nullptr, context);
    const auto status = reader.init();
    EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
}

TEST(ParquetV2NativeDecoderTest, LevelOnlyAllNullPagesInitializeZeroValueDecoders) {
    const std::array<std::pair<tparquet::Type::type, tparquet::Encoding::type>, 3> encodings {{
            {tparquet::Type::INT32, tparquet::Encoding::RLE_DICTIONARY},
            {tparquet::Type::INT32, tparquet::Encoding::DELTA_BINARY_PACKED},
            {tparquet::Type::BOOLEAN, tparquet::Encoding::RLE},
    }};
    for (const bool data_page_v2 : {false, true}) {
        for (const auto& [physical_type, encoding] : encodings) {
            const auto status =
                    materialize_level_only_page(data_page_v2, physical_type, encoding, true);
            EXPECT_TRUE(status.ok())
                    << "v2=" << data_page_v2 << ", encoding=" << tparquet::to_string(encoding)
                    << ": " << status;
        }
    }
}

TEST(ParquetV2NativeDecoderTest, LevelOnlyPagesRejectDefinitionLevelsThatRequireValues) {
    const std::array<std::pair<tparquet::Type::type, tparquet::Encoding::type>, 3> encodings {{
            {tparquet::Type::INT32, tparquet::Encoding::RLE_DICTIONARY},
            {tparquet::Type::INT32, tparquet::Encoding::DELTA_BINARY_PACKED},
            {tparquet::Type::BOOLEAN, tparquet::Encoding::RLE},
    }};
    for (const bool data_page_v2 : {false, true}) {
        for (const auto& [physical_type, encoding] : encodings) {
            const auto status =
                    materialize_level_only_page(data_page_v2, physical_type, encoding, false);
            EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>())
                    << "v2=" << data_page_v2 << ", encoding=" << tparquet::to_string(encoding)
                    << ": " << status;
        }
    }
}

TEST(ParquetV2NativeDecoderTest, LevelOnlyReaderSkipsLeadingZeroValuePages) {
    auto page = [](bool v2, std::optional<int32_t> value) {
        std::vector<uint8_t> payload;
        if (value.has_value()) {
            const std::vector<uint8_t> definition_levels {2, 1};
            if (v2) {
                payload = definition_levels;
            } else {
                payload.resize(sizeof(uint32_t));
                encode_fixed32_le(payload.data(), definition_levels.size());
                payload.insert(payload.end(), definition_levels.begin(), definition_levels.end());
            }
            const auto* value_bytes = reinterpret_cast<const uint8_t*>(&*value);
            payload.insert(payload.end(), value_bytes, value_bytes + sizeof(*value));
        } else if (!v2) {
            // V1 prefixes its RLE definition-level stream even when that stream has no values.
            payload.resize(sizeof(uint32_t));
            encode_fixed32_le(payload.data(), 0);
        }
        tparquet::PageHeader header;
        header.type = v2 ? tparquet::PageType::DATA_PAGE_V2 : tparquet::PageType::DATA_PAGE;
        header.__set_compressed_page_size(payload.size());
        header.__set_uncompressed_page_size(payload.size());
        if (v2) {
            header.__isset.data_page_header_v2 = true;
            header.data_page_header_v2.__set_num_values(value.has_value() ? 1 : 0);
            header.data_page_header_v2.__set_num_rows(value.has_value() ? 1 : 0);
            header.data_page_header_v2.__set_num_nulls(0);
            header.data_page_header_v2.__set_encoding(tparquet::Encoding::PLAIN);
            header.data_page_header_v2.__set_definition_levels_byte_length(value.has_value() ? 2
                                                                                             : 0);
            header.data_page_header_v2.__set_repetition_levels_byte_length(0);
            header.data_page_header_v2.__set_is_compressed(false);
        } else {
            header.__isset.data_page_header = true;
            header.data_page_header.__set_num_values(value.has_value() ? 1 : 0);
            header.data_page_header.__set_encoding(tparquet::Encoding::PLAIN);
            header.data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
            header.data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);
        }
        return serialize_page(header, payload);
    };

    static const auto utc = cctz::utc_time_zone();
    for (const bool v2 : {false, true}) {
        SCOPED_TRACE(v2 ? "v2" : "v1");
        auto bytes = page(v2, std::nullopt);
        const auto nonempty_page = page(v2, 7);
        bytes.insert(bytes.end(), nonempty_page.begin(), nonempty_page.end());
        auto file = std::make_shared<NativeDecoderMemoryFileReader>(bytes);
        tparquet::ColumnChunk chunk;
        chunk.meta_data.__set_type(tparquet::Type::INT32);
        chunk.meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
        chunk.meta_data.__set_num_values(1);
        chunk.meta_data.__set_total_compressed_size(bytes.size());
        chunk.meta_data.__set_data_page_offset(0);
        NativeFieldSchema field;
        field.physical_type = tparquet::Type::INT32;
        field.data_type = make_nullable(std::make_shared<DataTypeInt32>());
        field.definition_level = 1;
        field.parquet_schema.__set_type(tparquet::Type::INT32);
        field.parquet_schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);

        auto row_ranges = ::doris::RowRanges::create_single(1);
        ScalarColumnReader<false, false> scalar_reader(row_ranges, 1, chunk, nullptr, &utc,
                                                       nullptr);
        ASSERT_TRUE(
                scalar_reader
                        .init(file, &field, bytes.size(), nullptr, "", ParquetReaderCompat {}, true)
                        .ok());
        FilterMap filter;
        ASSERT_TRUE(filter.init(nullptr, 1, false).ok());
        ColumnPtr scalar_column = field.data_type->create_column();
        size_t scalar_rows = 0;
        bool scalar_eof = false;
        while (scalar_rows == 0 && !scalar_eof) {
            size_t read_now = 0;
            ASSERT_TRUE(scalar_reader
                                .read_column_data(scalar_column, field.data_type, nullptr, filter,
                                                  1, &read_now, &scalar_eof, false)
                                .ok());
            scalar_rows += read_now;
        }
        ASSERT_EQ(scalar_rows, 1);
        const auto& scalar_nullable = assert_cast<const ColumnNullable&>(*scalar_column);
        EXPECT_FALSE(scalar_nullable.is_null_at(0));
        EXPECT_EQ(
                assert_cast<const ColumnInt32&>(scalar_nullable.get_nested_column()).get_element(0),
                7);

        std::unique_ptr<LevelReader> reader;
        ASSERT_TRUE(LevelReader::create(file, chunk, &field, 1, bytes.size(), nullptr, false, "",
                                        ParquetReaderCompat {}, &reader)
                            .ok());

        std::vector<level_t> repetition_levels;
        std::vector<level_t> definition_levels;
        size_t rows_read = 0;
        const auto status =
                reader->read_rows(1, &repetition_levels, &definition_levels, &rows_read);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_EQ(rows_read, 1);
        EXPECT_EQ(repetition_levels, (std::vector<level_t> {0}));
        EXPECT_EQ(definition_levels, (std::vector<level_t> {1}));
    }
}

TEST(ParquetV2NativeDecoderTest, NestedV2PageRejectsMissingAdvertisedRowStarts) {
    tparquet::PageHeader header;
    header.type = tparquet::PageType::DATA_PAGE_V2;
    header.__set_compressed_page_size(12);
    header.__set_uncompressed_page_size(12);
    header.__isset.data_page_header_v2 = true;
    header.data_page_header_v2.__set_num_values(2);
    header.data_page_header_v2.__set_num_rows(2);
    header.data_page_header_v2.__set_num_nulls(0);
    header.data_page_header_v2.__set_encoding(tparquet::Encoding::PLAIN);
    header.data_page_header_v2.__set_repetition_levels_byte_length(4);
    header.data_page_header_v2.__set_definition_levels_byte_length(0);
    header.data_page_header_v2.__set_is_compressed(false);
    // Two values [0, 1] contain only one repetition-level zero, so they describe one row.
    EXPECT_TRUE(load_malformed_nested_page(header, {2, 0, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0}, 2)
                        .is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, FirstNestedV1PageRejectsOrphanContinuation) {
    tparquet::PageHeader header;
    header.type = tparquet::PageType::DATA_PAGE;
    // A two-value RLE run at repetition level 1 has no level-0 row start.
    const std::vector<uint8_t> payload {2, 0, 0, 0, 4, 1, 0, 0, 0, 0, 0, 0, 0, 0};
    header.__set_compressed_page_size(payload.size());
    header.__set_uncompressed_page_size(payload.size());
    header.__isset.data_page_header = true;
    header.data_page_header.__set_num_values(2);
    header.data_page_header.__set_encoding(tparquet::Encoding::PLAIN);
    header.data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);
    header.data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
    EXPECT_TRUE(load_malformed_nested_page(header, payload, 2).is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, NestedV1ContinuationRemainsValidAfterFirstRowStart) {
    auto make_page = [](int values, std::vector<uint8_t> payload) {
        tparquet::PageHeader header;
        header.type = tparquet::PageType::DATA_PAGE;
        header.__set_compressed_page_size(payload.size());
        header.__set_uncompressed_page_size(payload.size());
        header.__isset.data_page_header = true;
        header.data_page_header.__set_num_values(values);
        header.data_page_header.__set_encoding(tparquet::Encoding::PLAIN);
        header.data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);
        header.data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
        return serialize_page(header, payload);
    };
    auto first_page = make_page(2, {2, 0, 0, 0, 3, 2, 0, 0, 0, 0, 0, 0, 0, 0});
    auto second_page = make_page(1, {2, 0, 0, 0, 2, 1, 0, 0, 0, 0});
    first_page.insert(first_page.end(), second_page.begin(), second_page.end());

    MemoryBufferedReader reader(first_page);
    tparquet::ColumnChunk chunk;
    chunk.meta_data.__set_type(tparquet::Type::INT32);
    chunk.meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
    chunk.meta_data.__set_num_values(3);
    chunk.meta_data.__set_total_compressed_size(first_page.size());
    chunk.meta_data.__set_data_page_offset(0);
    NativeFieldSchema field;
    field.physical_type = tparquet::Type::INT32;
    field.repetition_level = 1;
    ParquetPageReadContext context(false, "");
    ColumnChunkReader<true, false> chunk_reader(&reader, &chunk, &field, nullptr, 1, nullptr,
                                                context);
    ASSERT_TRUE(chunk_reader.init().ok());
    ASSERT_TRUE(chunk_reader.load_page_data().ok());
    std::vector<level_t> levels;
    size_t rows = 0;
    bool cross_page = false;
    ASSERT_TRUE(chunk_reader.load_page_nested_rows(levels, 1, &rows, &cross_page).ok());
    ASSERT_TRUE(cross_page);
    ASSERT_TRUE(chunk_reader.load_cross_page_nested_row(levels, &cross_page).ok());
    EXPECT_FALSE(cross_page);
    EXPECT_EQ(levels, std::vector<level_t>({0, 1, 1}));
}

TEST(ParquetV2NativeDecoderTest, NestedV1IgnoresUnverifiableOffsetIndexRows) {
    auto make_page = [](int values, std::vector<uint8_t> payload) {
        tparquet::PageHeader header;
        header.type = tparquet::PageType::DATA_PAGE;
        header.__set_compressed_page_size(payload.size());
        header.__set_uncompressed_page_size(payload.size());
        header.__isset.data_page_header = true;
        header.data_page_header.__set_num_values(values);
        header.data_page_header.__set_encoding(tparquet::Encoding::PLAIN);
        header.data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);
        header.data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
        return serialize_page(header, payload);
    };
    const auto first_page = make_page(2, {2, 0, 0, 0, 3, 2, 0, 0, 0, 0, 0, 0, 0, 0});
    const auto second_page = make_page(1, {2, 0, 0, 0, 2, 1, 0, 0, 0, 0});
    std::vector<uint8_t> bytes = first_page;
    bytes.insert(bytes.end(), second_page.begin(), second_page.end());

    MemoryBufferedReader stream(bytes);
    tparquet::ColumnChunk chunk;
    chunk.meta_data.__set_type(tparquet::Type::INT32);
    chunk.meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
    chunk.meta_data.__set_num_values(3);
    chunk.meta_data.__set_total_compressed_size(bytes.size());
    chunk.meta_data.__set_data_page_offset(0);
    NativeFieldSchema field;
    field.physical_type = tparquet::Type::INT32;
    field.repetition_level = 1;
    tparquet::OffsetIndex offset_index;
    tparquet::PageLocation first_location;
    first_location.__set_offset(0);
    first_location.__set_compressed_page_size(first_page.size());
    first_location.__set_first_row_index(0);
    tparquet::PageLocation second_location;
    second_location.__set_offset(first_page.size());
    second_location.__set_compressed_page_size(second_page.size());
    // This V1 page continues row zero, but its index claims a new logical row. Repetition levels
    // are the only authoritative source, so indexed seeking must be disabled for this chunk.
    second_location.__set_first_row_index(1);
    offset_index.__set_page_locations({first_location, second_location});
    ParquetPageReadContext context(false, "");
    ColumnChunkReader<true, true> chunk_reader(&stream, &chunk, &field, &offset_index, 1, nullptr,
                                               context);
    ASSERT_TRUE(chunk_reader.init().ok());
    ASSERT_TRUE(chunk_reader.load_page_data().ok());
    std::vector<level_t> levels;
    size_t rows = 0;
    bool cross_page = false;
    ASSERT_TRUE(chunk_reader.load_page_nested_rows(levels, 1, &rows, &cross_page).ok());
    ASSERT_TRUE(cross_page);
    ASSERT_TRUE(chunk_reader.load_cross_page_nested_row(levels, &cross_page).ok());
    EXPECT_FALSE(cross_page);
    EXPECT_EQ(levels, std::vector<level_t>({0, 1, 1}));
}

TEST(ParquetV2NativeDecoderTest, NestedV1DiscardedOffsetIndexStopsAtLogicalChunkEnd) {
    tparquet::PageHeader header;
    header.type = tparquet::PageType::DATA_PAGE;
    const std::vector<uint8_t> payload {2, 0, 0, 0, 2, 0, 0, 0, 0, 0};
    header.__set_compressed_page_size(payload.size());
    header.__set_uncompressed_page_size(payload.size());
    header.__isset.data_page_header = true;
    header.data_page_header.__set_num_values(1);
    header.data_page_header.__set_encoding(tparquet::Encoding::PLAIN);
    header.data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);
    header.data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
    auto bytes = serialize_page(header, payload);
    const size_t page_size = bytes.size();
    // A compatibility reader may pad the validated physical range beyond the encoded chunk.
    bytes.resize(page_size + 16, 0);

    MemoryBufferedReader stream(bytes);
    tparquet::ColumnChunk chunk;
    chunk.meta_data.__set_type(tparquet::Type::INT32);
    chunk.meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
    chunk.meta_data.__set_num_values(1);
    chunk.meta_data.__set_total_compressed_size(page_size);
    chunk.meta_data.__set_data_page_offset(0);
    NativeFieldSchema field;
    field.physical_type = tparquet::Type::INT32;
    field.repetition_level = 1;
    tparquet::OffsetIndex offset_index;
    tparquet::PageLocation location;
    location.__set_offset(0);
    location.__set_compressed_page_size(page_size);
    location.__set_first_row_index(0);
    offset_index.__set_page_locations({location});
    ColumnChunkRange padded_range {.offset = 0, .length = bytes.size()};
    ParquetPageReadContext context(false, "");
    ColumnChunkReader<true, true> chunk_reader(&stream, &chunk, &field, &offset_index, 1, nullptr,
                                               context, &padded_range);
    ASSERT_TRUE(chunk_reader.init().ok());
    ASSERT_TRUE(chunk_reader.load_page_data().ok());
    std::vector<level_t> levels;
    size_t rows = 0;
    bool cross_page = false;
    ASSERT_TRUE(chunk_reader.load_page_nested_rows(levels, 1, &rows, &cross_page).ok());
    EXPECT_EQ(rows, 1);
    EXPECT_FALSE(cross_page);
    EXPECT_FALSE(chunk_reader.has_next_page());
}

TEST(ParquetV2NativeDecoderTest, HugeNestedPageCountsDoNotPreallocateFromHeaders) {
    for (auto page_type : {tparquet::PageType::DATA_PAGE, tparquet::PageType::DATA_PAGE_V2}) {
        tparquet::PageHeader header;
        header.type = page_type;
        header.__set_compressed_page_size(4);
        header.__set_uncompressed_page_size(4);
        if (page_type == tparquet::PageType::DATA_PAGE) {
            header.__isset.data_page_header = true;
            header.data_page_header.__set_num_values(std::numeric_limits<int32_t>::max());
            header.data_page_header.__set_encoding(tparquet::Encoding::PLAIN);
            header.data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);
            header.data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
        } else {
            header.__isset.data_page_header_v2 = true;
            header.data_page_header_v2.__set_num_values(std::numeric_limits<int32_t>::max());
            header.data_page_header_v2.__set_num_rows(1);
            header.data_page_header_v2.__set_num_nulls(0);
            header.data_page_header_v2.__set_encoding(tparquet::Encoding::PLAIN);
            header.data_page_header_v2.__set_repetition_levels_byte_length(0);
            header.data_page_header_v2.__set_definition_levels_byte_length(0);
            header.data_page_header_v2.__set_is_compressed(false);
        }
        // The four-byte V1 level length (or V2 value payload) contains no advertised levels. The
        // reader must report corruption without reserving INT_MAX level slots first.
        EXPECT_TRUE(load_malformed_nested_page(header, {0, 0, 0, 0}).is<ErrorCode::CORRUPTION>());
    }
}

TEST(ParquetV2NativeDecoderTest, PageDecompressionRejectsBothSizeMismatchDirections) {
    BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(tparquet::CompressionCodec::SNAPPY, &codec).ok());
    std::vector<uint8_t> input(8, 7);
    faststring compressed;
    ASSERT_TRUE(codec->compress(Slice(input.data(), input.size()), &compressed).ok());
    std::vector<uint8_t> payload(compressed.data(), compressed.data() + compressed.size());

    for (const int32_t advertised_size : {4, 12}) {
        tparquet::PageHeader header;
        header.type = tparquet::PageType::DATA_PAGE;
        header.__set_compressed_page_size(payload.size());
        header.__set_uncompressed_page_size(advertised_size);
        header.__isset.data_page_header = true;
        header.data_page_header.__set_num_values(1);
        header.data_page_header.__set_encoding(tparquet::Encoding::PLAIN);
        header.data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
        header.data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);
        EXPECT_FALSE(load_scripted_page(header, payload, tparquet::CompressionCodec::SNAPPY).ok());
    }
}

TEST(ParquetV2NativeDecoderTest, UncompressedDictionaryRequiresEqualPhysicalAndLogicalSizes) {
    tparquet::PageHeader header;
    header.type = tparquet::PageType::DICTIONARY_PAGE;
    header.__set_compressed_page_size(8);
    header.__set_uncompressed_page_size(1);
    header.__isset.dictionary_page_header = true;
    header.dictionary_page_header.__set_num_values(1);
    header.dictionary_page_header.__set_encoding(tparquet::Encoding::PLAIN);
    EXPECT_TRUE(load_scripted_page(header, std::vector<uint8_t>(8),
                                   tparquet::CompressionCodec::UNCOMPRESSED)
                        .is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, UncompressedDataPagesRequireEqualPhysicalAndLogicalSizes) {
    for (const auto page_type : {tparquet::PageType::DATA_PAGE, tparquet::PageType::DATA_PAGE_V2}) {
        tparquet::PageHeader header;
        header.type = page_type;
        header.__set_compressed_page_size(8);
        header.__set_uncompressed_page_size(4);
        if (page_type == tparquet::PageType::DATA_PAGE) {
            header.__isset.data_page_header = true;
        } else {
            header.__isset.data_page_header_v2 = true;
            header.data_page_header_v2.__set_is_compressed(false);
        }
        const std::vector<uint8_t> payload(8);
        EXPECT_TRUE(load_scripted_page(header, payload, tparquet::CompressionCodec::UNCOMPRESSED)
                            .is<ErrorCode::CORRUPTION>());
        EXPECT_TRUE(
                load_scripted_page(header, payload, tparquet::CompressionCodec::UNCOMPRESSED, true)
                        .is<ErrorCode::CORRUPTION>());
    }
}

TEST(ParquetV2NativeDecoderTest, LegacyV2CompressedOverrideAllowsDifferentSizes) {
    tparquet::PageHeader header;
    header.type = tparquet::PageType::DATA_PAGE_V2;
    header.__set_compressed_page_size(8);
    header.__set_uncompressed_page_size(16);
    header.__isset.data_page_header_v2 = true;
    header.data_page_header_v2.__set_is_compressed(false);
    EXPECT_TRUE(validate_uncompressed_page_sizes(header, tparquet::CompressionCodec::SNAPPY, true)
                        .ok());
    EXPECT_TRUE(validate_uncompressed_page_sizes(header, tparquet::CompressionCodec::SNAPPY, false)
                        .is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, EmptyOffsetIndexCannotSelectIndexedPageReader) {
    MemoryBufferedReader reader(std::vector<uint8_t> {0});
    tparquet::ColumnMetaData metadata;
    tparquet::OffsetIndex empty_index;
    ParquetPageReadContext context(false, "");
    PageReader<false, true> page_reader(&reader, nullptr, 0, 1, 1, metadata, context, &empty_index);
    EXPECT_FALSE(page_reader.has_next_page());
    EXPECT_TRUE(page_reader.next_page().is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, ColumnChunkRangeRejectsSignedOverflowAndBoundsLegacyPadding) {
    tparquet::ColumnMetaData metadata;
    metadata.__set_data_page_offset(-1);
    metadata.__set_total_compressed_size(10);
    ColumnChunkRange range;
    EXPECT_TRUE(
            compute_column_chunk_range(metadata, 100, false, &range).is<ErrorCode::CORRUPTION>());

    metadata.__set_data_page_offset(95);
    EXPECT_TRUE(
            compute_column_chunk_range(metadata, 100, false, &range).is<ErrorCode::CORRUPTION>());

    metadata.__set_data_page_offset(10);
    metadata.__set_total_compressed_size(std::numeric_limits<int64_t>::max());
    EXPECT_TRUE(
            compute_column_chunk_range(metadata, 100, false, &range).is<ErrorCode::CORRUPTION>());

    metadata.__set_total_compressed_size(20);
    ASSERT_TRUE(compute_column_chunk_range(metadata, 35, true, &range).ok());
    EXPECT_EQ(range.offset, 10);
    EXPECT_EQ(range.length, 25);

    metadata.__set_dictionary_page_offset(0);
    ASSERT_TRUE(compute_column_chunk_range(metadata, 35, false, &range).ok());
    EXPECT_EQ(range.offset, 10);
    EXPECT_EQ(range.length, 20);

    metadata.__set_dictionary_page_offset(5);
    ASSERT_TRUE(compute_column_chunk_range(metadata, 35, false, &range).ok());
    EXPECT_EQ(range.offset, 5);
    EXPECT_EQ(range.length, 20);
}

TEST(ParquetV2NativeDecoderTest, OffsetIndexValidationRejectsBackwardAndOverlappingLocations) {
    ColumnChunkRange range {.offset = 100, .length = 100};
    tparquet::OffsetIndex index;
    tparquet::PageLocation first;
    first.__set_offset(110);
    first.__set_compressed_page_size(20);
    first.__set_first_row_index(0);
    tparquet::PageLocation second;
    second.__set_offset(120);
    second.__set_compressed_page_size(20);
    second.__set_first_row_index(10);
    index.page_locations = {first, second};
    EXPECT_FALSE(validate_offset_index(index, range, 110, 20));

    second.__set_offset(140);
    second.__set_first_row_index(0);
    index.page_locations = {first, second};
    EXPECT_FALSE(validate_offset_index(index, range, 110, 20));

    second.__set_first_row_index(10);
    index.page_locations = {first, second};
    EXPECT_TRUE(validate_offset_index(index, range, 110, 20));

    range = {.offset = std::numeric_limits<size_t>::max(), .length = 2};
    EXPECT_FALSE(validate_offset_index(index, range, 110, 20));
}

TEST(ParquetV2NativeDecoderTest, OffsetIndexValidationRejectsShiftedFirstDataPage) {
    ColumnChunkRange range {.offset = 100, .length = 100};
    tparquet::OffsetIndex index;
    tparquet::PageLocation first;
    first.__set_offset(120);
    first.__set_compressed_page_size(20);
    first.__set_first_row_index(0);
    tparquet::PageLocation second;
    second.__set_offset(140);
    second.__set_compressed_page_size(20);
    second.__set_first_row_index(10);
    index.page_locations = {first, second};

    EXPECT_FALSE(validate_offset_index(index, range, 100, 20));
}

TEST(ParquetV2NativeDecoderTest, ColumnChunkSkipsIndexPageBeforeInitializingDataDecoder) {
    tparquet::PageHeader index_header;
    index_header.type = tparquet::PageType::INDEX_PAGE;
    index_header.__set_compressed_page_size(3);
    index_header.__set_uncompressed_page_size(3);
    index_header.__set_index_page_header(tparquet::IndexPageHeader());
    auto bytes = serialize_page(index_header, {1, 2, 3});

    tparquet::PageHeader data_header;
    data_header.type = tparquet::PageType::DATA_PAGE;
    data_header.__set_compressed_page_size(sizeof(int32_t));
    data_header.__set_uncompressed_page_size(sizeof(int32_t));
    data_header.__isset.data_page_header = true;
    data_header.data_page_header.__set_num_values(1);
    data_header.data_page_header.__set_encoding(tparquet::Encoding::PLAIN);
    data_header.data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
    data_header.data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);
    const int32_t value = 42;
    auto data_bytes = serialize_page(
            data_header,
            std::vector<uint8_t>(reinterpret_cast<const uint8_t*>(&value),
                                 reinterpret_cast<const uint8_t*>(&value) + sizeof(value)));
    bytes.insert(bytes.end(), data_bytes.begin(), data_bytes.end());

    MemoryBufferedReader reader(bytes);
    tparquet::ColumnChunk chunk;
    chunk.meta_data.__set_type(tparquet::Type::INT32);
    chunk.meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
    chunk.meta_data.__set_num_values(1);
    chunk.meta_data.__set_total_compressed_size(bytes.size());
    chunk.meta_data.__set_data_page_offset(0);
    NativeFieldSchema field;
    field.physical_type = tparquet::Type::INT32;
    ParquetPageReadContext context(false, "");
    ColumnChunkReader<false, false> chunk_reader(&reader, &chunk, &field, nullptr, 1, nullptr,
                                                 context);
    const auto init_status = chunk_reader.init();
    ASSERT_TRUE(init_status.ok()) << init_status;
    EXPECT_EQ(chunk_reader.remaining_num_values(), 1);
    ASSERT_TRUE(chunk_reader.load_page_data().ok());
}

TEST(ParquetV2NativeDecoderTest, ColumnChunkSkipsUnknownAuxiliaryPage) {
    tparquet::PageHeader unknown_header;
    unknown_header.type = static_cast<tparquet::PageType::type>(127);
    unknown_header.__set_compressed_page_size(3);
    unknown_header.__set_uncompressed_page_size(3);
    auto bytes = serialize_page(unknown_header, {1, 2, 3});

    tparquet::PageHeader data_header;
    data_header.type = tparquet::PageType::DATA_PAGE;
    data_header.__set_compressed_page_size(sizeof(int32_t));
    data_header.__set_uncompressed_page_size(sizeof(int32_t));
    data_header.__isset.data_page_header = true;
    data_header.data_page_header.__set_num_values(1);
    data_header.data_page_header.__set_encoding(tparquet::Encoding::PLAIN);
    data_header.data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
    data_header.data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);
    const int32_t value = 42;
    auto data_bytes = serialize_page(
            data_header,
            std::vector<uint8_t>(reinterpret_cast<const uint8_t*>(&value),
                                 reinterpret_cast<const uint8_t*>(&value) + sizeof(value)));
    bytes.insert(bytes.end(), data_bytes.begin(), data_bytes.end());

    MemoryBufferedReader reader(bytes);
    tparquet::ColumnChunk chunk;
    chunk.meta_data.__set_type(tparquet::Type::INT32);
    chunk.meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
    chunk.meta_data.__set_num_values(1);
    chunk.meta_data.__set_total_compressed_size(bytes.size());
    chunk.meta_data.__set_data_page_offset(0);
    NativeFieldSchema field;
    field.physical_type = tparquet::Type::INT32;
    ParquetPageReadContext context(false, "");
    ColumnChunkReader<false, false> chunk_reader(&reader, &chunk, &field, nullptr, 1, nullptr,
                                                 context);
    const auto init_status = chunk_reader.init();
    ASSERT_TRUE(init_status.ok()) << init_status;
    EXPECT_EQ(chunk_reader.remaining_num_values(), 1);
}

TEST(ParquetV2NativeDecoderTest, LegacyDataPageV2OverridesFalseCompressedFlag) {
    tparquet::PageHeader header;
    header.type = tparquet::PageType::DATA_PAGE_V2;
    header.__set_compressed_page_size(4);
    header.__set_uncompressed_page_size(1024);
    header.__isset.data_page_header_v2 = true;
    header.data_page_header_v2.__set_is_compressed(false);
    tparquet::ColumnMetaData metadata;
    metadata.__set_codec(tparquet::CompressionCodec::SNAPPY);
    EXPECT_TRUE(should_cache_decompressed(&header, metadata, false));
    EXPECT_FALSE(should_cache_decompressed(&header, metadata, true));

    EXPECT_TRUE(parquet_reader_compat("parquet-cpp version 1.5.0").data_page_v2_always_compressed);
    EXPECT_FALSE(parquet_reader_compat("parquet-cpp version 2.0.0").data_page_v2_always_compressed);
    EXPECT_TRUE(parquet_reader_compat("parquet-mr version 1.2.8").parquet_816_padding);
    EXPECT_FALSE(parquet_reader_compat("parquet-mr version 1.2.9").parquet_816_padding);
}

TEST(ParquetV2NativeDecoderTest, NullableNumericOverflowIsNullOnlyOutsideStrictMode) {
    const int32_t overflow = 1000;
    auto decode = [&](bool strict, MutableColumnPtr* column, IColumn::Filter* null_map) {
        std::unique_ptr<Decoder> decoder;
        RETURN_IF_ERROR(
                Decoder::get_decoder(tparquet::Type::INT32, tparquet::Encoding::PLAIN, decoder));
        decoder->set_type_length(sizeof(overflow));
        Slice slice(reinterpret_cast<const uint8_t*>(&overflow), sizeof(overflow));
        RETURN_IF_ERROR(decoder->set_data(&slice));
        ParquetDecodeContext context;
        context.physical_type = ParquetPhysicalType::INT32;
        ParquetMaterializationState state;
        state.enable_strict_mode = strict;
        state.conversion_failure_null_map = null_map;
        DataTypeInt8 type;
        *column = type.create_column();
        return type.get_serde()->read_column_from_parquet(**column, *decoder, context, 1, state);
    };

    MutableColumnPtr column;
    IColumn::Filter null_map;
    null_map.resize_fill(1, 0);
    ASSERT_TRUE(decode(false, &column, &null_map).ok());
    ASSERT_EQ(column->size(), 1);
    EXPECT_EQ(null_map[0], 1);

    null_map.clear();
    null_map.resize_fill(1, 0);
    EXPECT_FALSE(decode(true, &column, &null_map).ok());
    EXPECT_EQ(null_map[0], 0);
}

TEST(ParquetV2NativeDecoderTest, DictionaryConversionFailureIsDeferredUntilReferenced) {
    auto decode_dictionary_id = [](uint8_t dictionary_id, bool strict, IColumn::Filter* null_map,
                                   MutableColumnPtr* column) {
        const std::array<int32_t, 2> dictionary_values {1, 1000};
        auto dictionary = make_unique_buffer<uint8_t>(sizeof(dictionary_values));
        memcpy(dictionary.get(), dictionary_values.data(), sizeof(dictionary_values));
        std::unique_ptr<Decoder> decoder;
        RETURN_IF_ERROR(Decoder::get_decoder(tparquet::Type::INT32,
                                             tparquet::Encoding::RLE_DICTIONARY, decoder));
        decoder->set_type_length(sizeof(int32_t));
        RETURN_IF_ERROR(
                decoder->set_dict(dictionary, sizeof(dictionary_values), dictionary_values.size()));
        char encoded_id[] = {1, 2, static_cast<char>(dictionary_id)};
        Slice id_slice(encoded_id, sizeof(encoded_id));
        RETURN_IF_ERROR(decoder->set_data(&id_slice));

        ParquetDecodeContext context;
        context.physical_type = ParquetPhysicalType::INT32;
        context.encoding = ParquetValueEncoding::DICTIONARY;
        ParquetMaterializationState state;
        state.enable_strict_mode = strict;
        state.conversion_failure_null_map = null_map;
        DataTypeInt8 type;
        *column = type.create_column();
        return type.get_serde()->read_column_from_parquet(**column, *decoder, context, 1, state);
    };

    for (const bool strict : {false, true}) {
        IColumn::Filter null_map;
        IColumn::Filter* output_null_map = nullptr;
        if (strict) {
            null_map.resize_fill(1, 0);
            output_null_map = &null_map;
        }

        MutableColumnPtr column;
        const auto unused_bad = decode_dictionary_id(0, strict, output_null_map, &column);
        ASSERT_TRUE(unused_bad.ok()) << unused_bad;
        ASSERT_EQ(column->size(), 1);
        EXPECT_EQ(assert_cast<const ColumnInt8&>(*column).get_element(0), 1);

        const auto referenced_bad = decode_dictionary_id(1, strict, output_null_map, &column);
        EXPECT_TRUE(referenced_bad.is<ErrorCode::DATA_QUALITY_ERROR>()) << referenced_bad;
        EXPECT_TRUE(column->empty());
        if (output_null_map != nullptr) {
            EXPECT_EQ((*output_null_map)[0], 0);
        }
    }
}

TEST(ParquetV2NativeDecoderTest, FixedLengthStringsAppendAsOneContiguousSpan) {
    ColumnString column;
    const std::string values = "aaabbbccc";
    column.insert_many_fixed_length_data(values.data(), 3, 3);
    ASSERT_EQ(column.size(), 3);
    EXPECT_EQ(column.get_data_at(0).to_string_view(), "aaa");
    EXPECT_EQ(column.get_data_at(1).to_string_view(), "bbb");
    EXPECT_EQ(column.get_data_at(2).to_string_view(), "ccc");
}

TEST(ParquetV2NativeDecoderTest, ComplexPageStatisticsPreservePerLeafCrossings) {
    ColumnChunkReaderStatistics first_chunk;
    first_chunk.page_read_counter = 1;
    first_chunk.data_page_read_counter = 1;
    ColumnChunkReaderStatistics second_chunk;
    second_chunk.page_read_counter = 1;
    second_chunk.data_page_read_counter = 1;
    ColumnReader::ColumnStatistics combined;
    ColumnReader::ColumnStatistics first(first_chunk, 0);
    ColumnReader::ColumnStatistics second(second_chunk, 0);
    combined.merge(first);
    combined.merge(second);

    EXPECT_EQ(combined.page_read_counter, 2);
    ASSERT_EQ(combined.leaf_page_read_counters.size(), 2);
    EXPECT_EQ(combined.leaf_page_read_counters[0], 1);
    EXPECT_EQ(combined.leaf_page_read_counters[1], 1);
}

TEST(ParquetV2NativeDecoderTest, NativePageCacheUsesStableFileDescriptionIdentity) {
    ParquetPageCacheKeyBuilder first;
    ParquetPageCacheKeyBuilder replaced;
    first.init("s3://bucket/object::etag-v1::1234");
    replaced.init("s3://bucket/object::etag-v2::1234");
    EXPECT_EQ(first.make_key(4096, 128).fname, "s3://bucket/object::etag-v1::1234");
    EXPECT_NE(first.make_key(4096, 128).encode(), replaced.make_key(4096, 128).encode());
}

TEST(ParquetV2NativeDecoderTest, UncompressedV2PageCachePayloadIsAlwaysDecompressed) {
    tparquet::PageHeader header;
    header.type = tparquet::PageType::DATA_PAGE_V2;
    header.__set_compressed_page_size(100);
    header.__set_uncompressed_page_size(1000);
    header.__isset.data_page_header_v2 = true;
    header.data_page_header_v2.__set_is_compressed(false);
    tparquet::ColumnMetaData metadata;
    metadata.__set_codec(tparquet::CompressionCodec::SNAPPY);

    // The representation is explicit in V2; a codec and compression ratio cannot override it on
    // a warm cache hit.
    EXPECT_TRUE(should_cache_decompressed(&header, metadata));
}

TEST(ParquetV2NativeDecoderTest, CacheDisabledPagesDoNotPrepareCopiedPayload) {
    EXPECT_FALSE(can_prepare_page_cache_payload(false, false, true, true));
    EXPECT_FALSE(can_prepare_page_cache_payload(true, true, true, true));
    EXPECT_FALSE(can_prepare_page_cache_payload(true, false, false, true));
    EXPECT_FALSE(can_prepare_page_cache_payload(true, false, true, false));
    EXPECT_TRUE(can_prepare_page_cache_payload(true, false, true, true));
}

TEST(ParquetV2NativeDecoderTest, OversizedNestedBatchScratchUsesIdleBatchHysteresis) {
    ::doris::RowRanges row_ranges;
    tparquet::ColumnChunk chunk;
    ScalarColumnReader<true, false> reader(row_ranges, 1, chunk, nullptr, nullptr, nullptr);

    constexpr size_t max_retained_bytes = 64UL << 10;
    reader.reserve_batch_scratch_for_test(1UL << 16);
    const size_t oversized_bytes = reader.retained_batch_scratch_bytes_for_test();
    ASSERT_GT(oversized_bytes, max_retained_bytes);

    reader.release_batch_scratch(max_retained_bytes);
    EXPECT_EQ(reader.retained_batch_scratch_bytes_for_test(), oversized_bytes);
    reader.release_batch_scratch(max_retained_bytes);
    EXPECT_EQ(reader.retained_batch_scratch_bytes_for_test(), oversized_bytes);
    reader.release_batch_scratch(max_retained_bytes);
    const size_t released_bytes = reader.retained_batch_scratch_bytes_for_test();
    EXPECT_LT(released_bytes, oversized_bytes);
    EXPECT_LE(released_bytes, max_retained_bytes + sizeof(void*));
}

} // namespace
} // namespace doris::format::parquet::native
