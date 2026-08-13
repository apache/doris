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

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <optional>
#include <ranges>
#include <set>
#include <shared_mutex>
#include <thread>

#include "common/config.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "core/string_buffer.hpp"
#include "core/value/jsonb_value.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "exec/rowid_fetcher.h"
#include "gtest/gtest.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_profile.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/segment/column_meta_accessor.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/column_reader_cache.h"
#include "storage/segment/segment.h"
#include "storage/segment/variant/binary_column_extract_iterator.h"
#include "storage/segment/variant/hierarchical_data_iterator.h"
#include "storage/segment/variant/nested_group_path.h"
#include "storage/segment/variant/nested_group_provider.h"
#include "storage/segment/variant/nested_group_streaming_write_plan.h"
#include "storage/segment/variant/sparse_column_merge_iterator.h"
#include "storage/segment/variant/v2/variant_column_writer.h"
#include "storage/segment/variant/v2/variant_path_builder.h"
#include "storage/segment/variant/v2/variant_shredder.h"
#include "storage/segment/variant/variant_column_reader.h"
#include "storage/segment/variant/variant_column_writer_impl.h"
#include "storage/segment/variant/variant_doc_snpashot_compact_iterator.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet_manager.h"
#include "testutil/variant_util.h"
#include "util/jsonb_writer.h"

using namespace doris;

namespace doris {

constexpr static uint32_t MAX_PATH_LEN = 1024;
constexpr static std::string_view dest_dir = "/ut_dir/variant_column_writer_test";
constexpr static std::string_view tmp_dir = "./ut_dir/tmp";

enum class VariantWriterInput : uint8_t { V1, V2 };

enum class VariantIndexWritePolicy : uint8_t { NONE, BLOOM_AND_INVERTED };

static std::string variant_writer_input_name(VariantWriterInput input) {
    return input == VariantWriterInput::V1 ? "V1" : "V2";
}

static std::string variant_writer_test_name(
        const testing::TestParamInfo<VariantWriterInput>& info) {
    return variant_writer_input_name(info.param);
}

class ScopedDuplicateJsonPathCheck {
public:
    explicit ScopedDuplicateJsonPathCheck(bool enabled)
            : _old_value(config::variant_enable_duplicate_json_path_check) {
        config::variant_enable_duplicate_json_path_check = enabled;
    }
    ~ScopedDuplicateJsonPathCheck() {
        config::variant_enable_duplicate_json_path_check = _old_value;
    }

private:
    bool _old_value;
};

static Status create_variant_writer_source(VariantWriterInput input,
                                           const std::vector<std::string>& jsons,
                                           int max_subcolumns_count, bool enable_doc_mode,
                                           const std::vector<UInt8>& outer_nulls, ColumnPtr* source,
                                           DataTypePtr* source_type) {
    DCHECK(source != nullptr);
    DCHECK(source_type != nullptr);

    ColumnPtr values;
    DataTypePtr type;
    if (input == VariantWriterInput::V1) {
        auto variant = ColumnVariant::create(max_subcolumns_count, enable_doc_mode);
        auto json_column = ColumnString::create();
        for (const auto& json : jsons) {
            json_column->insert_data(json.data(), json.size());
        }
        ParseConfig parse_config;
        parse_config.parse_to = enable_doc_mode ? ParseConfig::ParseTo::OnlyDocValueColumn
                                                : ParseConfig::ParseTo::OnlySubcolumns;
        parse_config.check_duplicate_json_path = config::variant_enable_duplicate_json_path_check;
        variant_util::parse_json_to_variant(*variant, *json_column, parse_config);
        values = std::move(variant);
        type = std::make_shared<DataTypeVariant>(max_subcolumns_count, enable_doc_mode);
    } else {
        auto variant = ColumnVariantV2::create();
        DataTypeVariantV2SerDe serde;
        DataTypeSerDe::FormatOptions options;
        for (const auto& json : jsons) {
            Slice slice(json.data(), json.size());
            RETURN_IF_ERROR(serde.deserialize_one_cell_from_json(*variant, slice, options));
        }
        values = std::move(variant);
        type = std::make_shared<DataTypeVariantV2>(max_subcolumns_count, enable_doc_mode);
    }

    if (!outer_nulls.empty()) {
        if (outer_nulls.size() != jsons.size()) {
            return Status::InvalidArgument("Variant source has {} null flags for {} rows",
                                           outer_nulls.size(), jsons.size());
        }
        auto null_map = ColumnUInt8::create();
        for (UInt8 is_null : outer_nulls) {
            null_map->insert_value(is_null);
        }
        values = ColumnNullable::create(std::move(values), std::move(null_map));
        type = make_nullable(std::move(type));
    }

    *source = std::move(values);
    *source_type = std::move(type);
    return Status::OK();
}

static Status create_typed_int_extracted_source(VariantWriterInput input, ColumnPtr* source,
                                                DataTypePtr* source_type) {
    DCHECK(source != nullptr);
    DCHECK(source_type != nullptr);
    const std::vector<UInt8> inner_nulls {0, 1, 1, 0};
    MutableColumnPtr values;
    DataTypePtr type;
    if (input == VariantWriterInput::V1) {
        auto variant = ColumnVariant::create(0, false);
        auto json = ColumnString::create();
        for (const std::string_view value : {"1", "null", "null", "4"}) {
            json->insert_data(value.data(), value.size());
        }
        ParseConfig parse_config;
        parse_config.parse_to = ParseConfig::ParseTo::OnlySubcolumns;
        variant_util::parse_json_to_variant(*variant, *json, parse_config);
        variant->finalize();
        variant->ensure_root_node_type(make_nullable(std::make_shared<DataTypeInt32>()));
        values = std::move(variant);
        type = std::make_shared<DataTypeVariant>(0, false);
    } else {
        auto integers = ColumnInt32::create();
        for (Int32 value : {1, 0, 0, 4}) {
            integers->insert_value(value);
        }
        auto nulls = ColumnUInt8::create();
        for (UInt8 is_null : inner_nulls) {
            nulls->insert_value(is_null);
        }
        values = ColumnVariantV2::create_typed(
                ColumnNullable::create(std::move(integers), std::move(nulls)),
                std::make_shared<DataTypeInt32>());
        type = std::make_shared<DataTypeVariantV2>(0, false);
    }

    auto outer_nulls = ColumnUInt8::create();
    for (UInt8 is_null : {0, 0, 1, 0}) {
        outer_nulls->insert_value(is_null);
    }
    *source = ColumnNullable::create(std::move(values), std::move(outer_nulls));
    *source_type = make_nullable(std::move(type));
    return Status::OK();
}

static std::string variant_v2_json_at(const ColumnVariantV2& column, size_t row) {
    auto output = ColumnString::create();
    BufferWritable writer(*output);
    DataTypeSerDe::FormatOptions options;
    DataTypeVariantV2SerDe serde;
    EXPECT_TRUE(serde.serialize_one_cell_to_json(column, row, writer, options).ok());
    writer.commit();
    return output->get_data_at(0).to_string();
}

static std::string variant_json_at(const IColumn& column, size_t row) {
    std::string value;
    if (const auto* variant_v2 = check_and_get_column<ColumnVariantV2>(column)) {
        value = variant_v2_json_at(*variant_v2, row);
    } else {
        DataTypeSerDe::FormatOptions options;
        assert_cast<const ColumnVariant&>(column).serialize_one_row_to_string(row, &value, options);
    }

    JsonBinaryValue jsonb;
    if (jsonb.from_json_string(value).ok()) {
        return jsonb.to_json_string();
    }
    JsonbWriter writer;
    DORIS_CHECK(writer.writeStartString());
    DORIS_CHECK(writer.writeString(value.data(), value.size()));
    DORIS_CHECK(writer.writeEndString());
    return JsonbToJson::jsonb_to_json_string(writer.getOutput()->getBuffer(),
                                             writer.getOutput()->getSize());
}

TEST(VariantPathBuilderTest, PromotesValuesAndMaterializesMissingRows) {
    VariantBatchBuilder value_builder;
    auto integer_row = value_builder.begin_row();
    integer_row.add_float(1.0F);
    integer_row.finish();
    auto double_row = value_builder.begin_row();
    double_row.add_double(2.5);
    double_row.finish();
    auto null_row = value_builder.begin_row();
    null_row.add_null();
    null_row.finish();
    VariantBatchBuilder values = value_builder.finish_batch();

    segment_v2::VariantPathBuilder builder(PathInData("metric"), 1);
    ASSERT_TRUE(builder.append(values.value_at(0), 1).ok());
    const Status null_status = builder.append(values.value_at(2), 2);
    ASSERT_FALSE(null_status.ok());
    EXPECT_NE(null_status.to_string().find("must not append JSON null"), std::string::npos)
            << null_status.to_string();
    ASSERT_TRUE(builder.append(values.value_at(1), 3).ok());
    ASSERT_TRUE(builder.complete_rows(5).ok());

    EXPECT_EQ(builder.rows(), 5);
    EXPECT_EQ(builder.non_null_rows(), 2);
    EXPECT_EQ(builder.promotion_count(), 1);
    EXPECT_EQ(remove_nullable(builder.type())->get_primitive_type(), TYPE_DOUBLE);
    EXPECT_EQ(std::vector<uint32_t>(builder.rowids().begin(), builder.rowids().end()),
              (std::vector<uint32_t> {1, 3}));

    ColumnPtr materialized;
    ASSERT_TRUE(builder.materialize(&materialized).ok());
    const auto& nullable = assert_cast<const ColumnNullable&>(*materialized);
    const auto& data = assert_cast<const ColumnFloat64&>(nullable.get_nested_column()).get_data();
    const std::vector<std::optional<double>> expected {std::nullopt, 1.0, std::nullopt, 2.5,
                                                       std::nullopt};
    ASSERT_EQ(nullable.size(), expected.size());
    for (size_t row = 0; row < expected.size(); ++row) {
        SCOPED_TRACE(testing::Message() << "row=" << row);
        EXPECT_EQ(builder.is_null_at(row), !expected[row].has_value());
        EXPECT_EQ(nullable.is_null_at(row), !expected[row].has_value());
        if (expected[row].has_value()) {
            EXPECT_DOUBLE_EQ(data[row], *expected[row]);
        }
    }
}

TEST(VariantPathBuilderTest, UsesBigintForEncodedIntegersWithoutPromotion) {
    VariantBatchBuilder value_builder;
    auto tiny_row = value_builder.begin_row();
    tiny_row.add_int(1);
    tiny_row.finish();
    auto int_row = value_builder.begin_row();
    int_row.add_int(1 << 20);
    int_row.finish();
    VariantBatchBuilder values = value_builder.finish_batch();

    segment_v2::VariantPathBuilder builder(PathInData("metric"));
    ASSERT_TRUE(builder.append(values.value_at(0), 0).ok());
    EXPECT_EQ(remove_nullable(builder.type())->get_primitive_type(), TYPE_BIGINT);
    ASSERT_TRUE(builder.append(values.value_at(1), 1).ok());
    EXPECT_EQ(remove_nullable(builder.type())->get_primitive_type(), TYPE_BIGINT);
    EXPECT_EQ(builder.promotion_count(), 0);
}

TEST(VariantPathBuilderTest, MatchesLegacyMixedIntegerDoubleInferenceInEitherOrder) {
    for (const bool reverse : {false, true}) {
        SCOPED_TRACE(testing::Message() << "reverse=" << reverse);
        const std::vector<std::string> jsons =
                reverse ? std::vector<std::string> {R"({"metric":1.5})", R"({"metric":1})"}
                        : std::vector<std::string> {R"({"metric":1})", R"({"metric":1.5})"};

        auto legacy = ColumnVariant::create(0, false);
        auto json = ColumnString::create();
        for (const std::string& value : jsons) {
            json->insert_data(value.data(), value.size());
        }
        ParseConfig parse_config;
        parse_config.parse_to = ParseConfig::ParseTo::OnlySubcolumns;
        variant_util::parse_json_to_variant(*legacy, *json, parse_config);
        legacy->finalize();
        auto* legacy_metric = legacy->get_subcolumn(PathInData("metric"));
        ASSERT_NE(legacy_metric, nullptr);
        EXPECT_EQ(remove_nullable(legacy_metric->get_least_common_type())->get_primitive_type(),
                  TYPE_JSONB);

        VariantBatchBuilder value_builder;
        if (reverse) {
            auto double_row = value_builder.begin_row();
            double_row.add_double(1.5);
            double_row.finish();
            auto integer_row = value_builder.begin_row();
            integer_row.add_int(1);
            integer_row.finish();
        } else {
            auto integer_row = value_builder.begin_row();
            integer_row.add_int(1);
            integer_row.finish();
            auto double_row = value_builder.begin_row();
            double_row.add_double(1.5);
            double_row.finish();
        }
        VariantBatchBuilder values = value_builder.finish_batch();
        segment_v2::VariantPathBuilder builder(PathInData("metric"));
        ASSERT_TRUE(builder.append(values.value_at(0), 0).ok());
        ASSERT_TRUE(builder.append(values.value_at(1), 1).ok());
        ASSERT_EQ(remove_nullable(builder.type())->get_primitive_type(), TYPE_JSONB);

        for (size_t row = 0; row < jsons.size(); ++row) {
            auto legacy_keys = ColumnString::create();
            auto legacy_values = ColumnString::create();
            legacy_metric->serialize_to_binary_column(legacy_keys.get(), "metric",
                                                      legacy_values.get(), row);
            ASSERT_EQ(legacy_values->size(), 1);
            ColumnString::Chars v2_cell;
            ASSERT_TRUE(builder.write_sparse_cell(row, &v2_cell).ok());
            const StringRef legacy_cell = legacy_values->get_data_at(0);
            ASSERT_EQ(v2_cell.size(), legacy_cell.size);
            EXPECT_EQ(std::memcmp(v2_cell.data(), legacy_cell.data, legacy_cell.size), 0);
        }
    }
}

TEST(VariantPathBuilderTest, CachedBinarySerdeFollowsFloatingPromotion) {
    VariantBatchBuilder value_builder;
    auto tiny_row = value_builder.begin_row();
    tiny_row.add_float(1.0F);
    tiny_row.finish();
    auto int_row = value_builder.begin_row();
    int_row.add_double(2.0);
    int_row.finish();
    VariantBatchBuilder values = value_builder.finish_batch();

    segment_v2::VariantPathBuilder builder(PathInData("metric"));
    ASSERT_TRUE(builder.append(values.value_at(0), 0).ok());
    ColumnString::Chars binary;
    ASSERT_TRUE(builder.write_sparse_cell(0, &binary).ok());
    ASSERT_FALSE(binary.empty());
    EXPECT_EQ(static_cast<FieldType>(binary.front()), FieldType::OLAP_FIELD_TYPE_FLOAT);

    ASSERT_TRUE(builder.append(values.value_at(1), 1).ok());
    binary.clear();
    ASSERT_TRUE(builder.write_sparse_cell(1, &binary).ok());
    ASSERT_FALSE(binary.empty());
    EXPECT_EQ(static_cast<FieldType>(binary.front()), FieldType::OLAP_FIELD_TYPE_DOUBLE);

    ASSERT_TRUE(builder.convert_to(std::make_shared<DataTypeInt64>()).ok());
    binary.clear();
    ASSERT_TRUE(builder.write_sparse_cell(1, &binary).ok());
    ASSERT_FALSE(binary.empty());
    EXPECT_EQ(static_cast<FieldType>(binary.front()), FieldType::OLAP_FIELD_TYPE_BIGINT);
}

TEST(VariantPathBuilderTest, PromotesCompatibleDecimalScalesInEitherOrderAndInsideArrays) {
    const auto verify = [](bool reverse, bool array) {
        VariantBatchBuilder value_builder;
        const auto append = [&](int index) {
            auto row = value_builder.begin_row();
            std::optional<VariantBatchBuilder::Row::ArrayScope> array_scope;
            if (array) {
                array_scope.emplace(row.start_array());
            }
            if (index == 0) {
                row.add_decimal(123, 2, 16);
            } else {
                row.add_decimal(12300, 4, 16);
            }
            if (array_scope.has_value()) {
                array_scope->finish();
            }
            row.finish();
        };
        append(reverse ? 1 : 0);
        append(reverse ? 0 : 1);
        VariantBatchBuilder values = value_builder.finish_batch();

        segment_v2::VariantPathBuilder builder(PathInData("metric"));
        ASSERT_TRUE(builder.append(values.value_at(0), 0).ok());
        ASSERT_TRUE(builder.append(values.value_at(1), 1).ok());
        const DataTypePtr base = remove_nullable(builder.type());
        const DataTypePtr decimal =
                array ? remove_nullable(assert_cast<const DataTypeArray&>(*base).get_nested_type())
                      : base;
        ASSERT_EQ(decimal->get_primitive_type(), TYPE_DECIMAL128I);
        EXPECT_EQ(decimal->get_scale(), 4);

        ColumnPtr materialized;
        ASSERT_TRUE(builder.materialize(&materialized).ok());
        const std::string expected = array ? "[1.2300]" : "1.2300";
        EXPECT_EQ(builder.type()->to_string(*materialized, 0), expected);
        EXPECT_EQ(builder.type()->to_string(*materialized, 1), expected);
    };

    for (bool reverse : {false, true}) {
        verify(reverse, false);
        verify(reverse, true);
    }
}

TEST(VariantPathBuilderTest, EqualFullTypeDoesNotPromoteButDifferentDecimalScaleDoes) {
    VariantBatchBuilder value_builder;
    for (const uint8_t scale : {2, 2, 4}) {
        auto row = value_builder.begin_row();
        row.add_decimal(123, scale, 16);
        row.finish();
    }
    VariantBatchBuilder values = value_builder.finish_batch();

    segment_v2::VariantPathBuilder builder(PathInData("metric"));
    ASSERT_TRUE(builder.append(values.value_at(0), 0).ok());
    ASSERT_TRUE(builder.append(values.value_at(1), 1).ok());
    EXPECT_EQ(builder.promotion_count(), 0);
    ASSERT_TRUE(builder.append(values.value_at(2), 2).ok());
    EXPECT_EQ(builder.promotion_count(), 1);
    EXPECT_EQ(builder.stable_scalar_append_count(), 1);
    EXPECT_EQ(remove_nullable(builder.type())->get_scale(), 4);
}

TEST(VariantPathBuilderTest, StableScalarFastPathPreservesInferenceBoundaries) {
    const auto verify_encoded = []<typename Append>(Append append) {
        VariantBatchBuilder value_builder;
        for (size_t row = 0; row < 2; ++row) {
            auto value_row = value_builder.begin_row();
            append(value_row);
            value_row.finish();
        }
        VariantBatchBuilder values = value_builder.finish_batch();
        segment_v2::VariantPathBuilder builder(PathInData("metric"));
        ASSERT_TRUE(builder.append(values.value_at(0), 0).ok());
        ASSERT_TRUE(builder.append(values.value_at(1), 1).ok());
        EXPECT_EQ(builder.promotion_count(), 0);
        EXPECT_EQ(builder.stable_scalar_append_count(), 1);
    };

    verify_encoded([](auto& row) { row.add_bool(true); });
    verify_encoded([](auto& row) { row.add_int(1 << 20); });
    verify_encoded([](auto& row) { row.add_largeint(static_cast<__int128>(1) << 80); });
    verify_encoded([](auto& row) { row.add_float(1.25F); });
    verify_encoded([](auto& row) { row.add_double(2.5); });
    verify_encoded([](auto& row) { row.add_string(StringRef("stable")); });
    const std::string long_string(80, 'x');
    verify_encoded([&](auto& row) { row.add_string(StringRef(long_string)); });
    verify_encoded([](auto& row) { row.add_date(0); });
    verify_encoded([](auto& row) { row.add_timestamp_micros(1, true); });
    verify_encoded([](auto& row) { row.add_timestamp_micros(1, false); });
    verify_encoded([](auto& row) { row.add_decimal(12300, 4, 8); });

    VariantBatchBuilder widening_values_builder;
    for (const int64_t value : std::array<int64_t, 4> {1, 1LL << 20, 1LL << 40, 1}) {
        auto row = widening_values_builder.begin_row();
        row.add_int(value);
        row.finish();
    }
    VariantBatchBuilder widening_values = widening_values_builder.finish_batch();
    segment_v2::VariantPathBuilder widening(PathInData("widening"));
    ASSERT_TRUE(widening.append(widening_values.value_at(0), 0).ok());
    ASSERT_TRUE(widening.append(widening_values.value_at(1), 1).ok());
    EXPECT_EQ(widening.stable_scalar_append_count(), 1);
    EXPECT_EQ(widening.promotion_count(), 0);
    ASSERT_TRUE(widening.append(widening_values.value_at(2), 2).ok());
    EXPECT_EQ(widening.stable_scalar_append_count(), 2);
    ASSERT_TRUE(widening.append(widening_values.value_at(3), 3).ok());
    EXPECT_EQ(widening.stable_scalar_append_count(), 3);
    EXPECT_EQ(widening.promotion_count(), 0);
    EXPECT_EQ(widening.type()->to_string(*widening.column(), 3), "1");

    VariantBatchBuilder conflict_values_builder;
    auto integer_row = conflict_values_builder.begin_row();
    integer_row.add_int(1);
    integer_row.finish();
    for (size_t row = 0; row < 2; ++row) {
        auto bool_row = conflict_values_builder.begin_row();
        bool_row.add_bool(true);
        bool_row.finish();
    }
    VariantBatchBuilder conflict_values = conflict_values_builder.finish_batch();
    segment_v2::VariantPathBuilder conflict(PathInData("conflict"));
    for (size_t row = 0; row < conflict_values.num_rows(); ++row) {
        ASSERT_TRUE(conflict.append(conflict_values.value_at(row), row).ok());
    }
    EXPECT_EQ(remove_nullable(conflict.type())->get_primitive_type(), TYPE_JSONB);
    EXPECT_EQ(conflict.stable_scalar_append_count(), 1);

    VariantBatchBuilder binary_values_builder;
    for (size_t row = 0; row < 2; ++row) {
        auto binary_row = binary_values_builder.begin_row();
        binary_row.add_binary(StringRef("binary"));
        binary_row.finish();
    }
    VariantBatchBuilder binary_values = binary_values_builder.finish_batch();
    segment_v2::VariantPathBuilder binary(PathInData("binary"));
    ASSERT_TRUE(binary.append(binary_values.value_at(0), 0).ok());
    ASSERT_TRUE(binary.append(binary_values.value_at(1), 1).ok());
    EXPECT_EQ(remove_nullable(binary.type())->get_primitive_type(), TYPE_JSONB);
    EXPECT_EQ(binary.stable_scalar_append_count(), 1);

    VariantBatchBuilder array_values_builder;
    for (size_t row = 0; row < 2; ++row) {
        auto value_row = array_values_builder.begin_row();
        auto array = value_row.start_array();
        value_row.add_int(1);
        array.finish();
        value_row.finish();
    }
    VariantBatchBuilder array_values = array_values_builder.finish_batch();
    segment_v2::VariantPathBuilder arrays(PathInData("arrays"));
    ASSERT_TRUE(arrays.append(array_values.value_at(0), 0).ok());
    ASSERT_TRUE(arrays.append(array_values.value_at(1), 1).ok());
    EXPECT_EQ(arrays.stable_scalar_append_count(), 0);

    const auto verify_out_of_range_falls_back = []<typename Append>(Append append) {
        VariantBatchBuilder value_builder;
        auto valid_row = value_builder.begin_row();
        append(valid_row, false);
        valid_row.finish();
        auto invalid_row = value_builder.begin_row();
        append(invalid_row, true);
        invalid_row.finish();
        VariantBatchBuilder values = value_builder.finish_batch();

        segment_v2::VariantPathBuilder builder(PathInData("range"));
        ASSERT_TRUE(builder.append(values.value_at(0), 0).ok());
        ASSERT_TRUE(builder.append(values.value_at(1), 1).ok());
        EXPECT_EQ(remove_nullable(builder.type())->get_primitive_type(), TYPE_JSONB);
        EXPECT_EQ(builder.stable_scalar_append_count(), 0);
    };
    verify_out_of_range_falls_back(
            [](auto& row, bool invalid) { row.add_date(invalid ? 3'000'000 : 0); });
    verify_out_of_range_falls_back([](auto& row, bool invalid) {
        row.add_timestamp_micros(invalid ? 253'402'300'800'000'000LL : 0, true);
    });
}

TEST(VariantPathBuilderTest, StableScalarGuardRetainsDecimalAndAppendFailureFallbacks) {
    VariantBatchBuilder value_builder;
    auto valid_row = value_builder.begin_row();
    valid_row.add_decimal(1, 1, 16);
    valid_row.finish();
    VariantBatchBuilder values = value_builder.finish_batch();

    segment_v2::VariantPathBuilder decimal(PathInData("decimal"));
    ASSERT_TRUE(decimal.append(values.value_at(0), 0).ok());

    std::array<char, 18> overflow_decimal {};
    overflow_decimal[0] = static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::DECIMAL16)
                                            << VARIANT_VALUE_HEADER_SHIFT);
    overflow_decimal[1] = 1;
    unsigned __int128 unscaled = VARIANT_DECIMAL16_MAX + 1;
    for (size_t byte = 0; byte < 16; ++byte) {
        overflow_decimal[byte + 2] = static_cast<char>(unscaled >> (byte * 8));
    }
    ASSERT_TRUE(
            decimal.append(VariantRef {.metadata = {},
                                       .value = {overflow_decimal.data(), overflow_decimal.size()}},
                           1)
                    .ok());
    EXPECT_EQ(remove_nullable(decimal.type())->get_primitive_type(), TYPE_JSONB);
    EXPECT_EQ(decimal.promotion_count(), 1);
    EXPECT_EQ(decimal.stable_scalar_append_count(), 0);
    EXPECT_EQ(decimal.non_null_rows(), 2);

    VariantBatchBuilder narrow_values_builder;
    auto narrow_row = narrow_values_builder.begin_row();
    narrow_row.add_decimal(9999, 1, 4);
    narrow_row.finish();
    VariantBatchBuilder narrow_values = narrow_values_builder.finish_batch();
    segment_v2::VariantPathBuilder narrow_decimal(PathInData("narrow_decimal"));
    ASSERT_TRUE(narrow_decimal.append(values.value_at(0), 0).ok());
    ASSERT_TRUE(narrow_decimal.convert_to(std::make_shared<DataTypeDecimal128>(3, 1)).ok());
    ASSERT_TRUE(narrow_decimal.append(narrow_values.value_at(0), 1).ok());
    EXPECT_EQ(remove_nullable(narrow_decimal.type())->get_primitive_type(), TYPE_JSONB);
    EXPECT_EQ(narrow_decimal.promotion_count(), 2);
    EXPECT_EQ(narrow_decimal.stable_scalar_append_count(), 0);
    EXPECT_EQ(narrow_decimal.non_null_rows(), 2);

    VariantBatchBuilder integer_builder;
    auto integer_row = integer_builder.begin_row();
    integer_row.add_int(1);
    integer_row.finish();
    VariantBatchBuilder integer_values = integer_builder.finish_batch();

    segment_v2::VariantPathBuilder truncated(PathInData("truncated"));
    ASSERT_TRUE(truncated.append(integer_values.value_at(0), 0).ok());
    const char truncated_int8 = static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::INT8)
                                                  << VARIANT_VALUE_HEADER_SHIFT);
    const Status truncated_status =
            truncated.append(VariantRef {.metadata = {}, .value = {&truncated_int8, 1}}, 1);
    EXPECT_FALSE(truncated_status.ok());
    EXPECT_EQ(remove_nullable(truncated.type())->get_primitive_type(), TYPE_JSONB);
    EXPECT_EQ(truncated.promotion_count(), 1);
    EXPECT_EQ(truncated.stable_scalar_append_count(), 0);
    EXPECT_EQ(truncated.non_null_rows(), 1);
}

TEST(VariantPathBuilderTest, JsonbFallbackPreservesCanonicalNumericBytes) {
    VariantBatchBuilder value_builder;
    auto row = value_builder.begin_row();
    auto array = row.start_array();
    row.add_int(1);
    auto object = row.start_object();
    object.add_key(StringRef("x"));
    row.add_int(2);
    object.finish();
    array.finish();
    row.finish();
    VariantBatchBuilder values = value_builder.finish_batch();

    segment_v2::VariantPathBuilder builder(PathInData("metric"));
    ASSERT_TRUE(builder.append(values.value_at(0), 0).ok());
    ASSERT_EQ(remove_nullable(builder.type())->get_primitive_type(), TYPE_JSONB);
    const auto& compact = assert_cast<const ColumnNullable&>(*builder.column());
    const StringRef actual =
            assert_cast<const ColumnString&>(compact.get_nested_column()).get_data_at(0);

    JsonbWriter expected;
    ASSERT_TRUE(expected.writeStartArray());
    ASSERT_TRUE(expected.writeInt64(1));
    ASSERT_TRUE(expected.writeStartObject());
    ASSERT_TRUE(expected.writeKey("x", 1));
    ASSERT_TRUE(expected.writeInt8(2));
    ASSERT_TRUE(expected.writeEndObject());
    ASSERT_TRUE(expected.writeEndArray());
    ASSERT_EQ(actual.size, expected.getOutput()->getSize());
    EXPECT_EQ(std::memcmp(actual.data, expected.getOutput()->getBuffer(), actual.size), 0);
    EXPECT_EQ(builder.type()->to_string(*builder.column(), 0), R"([1,{"x":2}])");
}

TEST(VariantPathBuilderTest, MatchesLegacyComplexArrayInference) {
    auto legacy = ColumnVariant::create(0, false);
    auto json = ColumnString::create();
    constexpr std::string_view JSON =
            R"({"b":[123,{"xx":1}],"k5":[[123]],"mixed":[456,"789"],"objects":[{"x":1},{"x":2}]})";
    json->insert_data(JSON.data(), JSON.size());
    ParseConfig parse_config;
    parse_config.parse_to = ParseConfig::ParseTo::OnlySubcolumns;
    variant_util::parse_json_to_variant(*legacy, *json, parse_config);

    VariantBatchBuilder value_builder;
    auto row = value_builder.begin_row();
    auto object = row.start_object();
    object.add_key(StringRef("b"));
    auto mixed_object_array = row.start_array();
    row.add_int(123);
    auto child = row.start_object();
    child.add_key(StringRef("xx"));
    row.add_int(1);
    child.finish();
    mixed_object_array.finish();
    object.add_key(StringRef("k5"));
    auto outer = row.start_array();
    auto inner = row.start_array();
    row.add_int(123);
    inner.finish();
    outer.finish();
    object.add_key(StringRef("mixed"));
    auto mixed_scalar_array = row.start_array();
    row.add_int(456);
    row.add_string(StringRef("789"));
    mixed_scalar_array.finish();
    object.add_key(StringRef("objects"));
    auto object_array = row.start_array();
    auto first_object = row.start_object();
    first_object.add_key(StringRef("x"));
    row.add_int(1);
    first_object.finish();
    auto second_object = row.start_object();
    second_object.add_key(StringRef("x"));
    row.add_int(2);
    second_object.finish();
    object_array.finish();
    object.finish();
    row.finish();
    VariantBatchBuilder values = value_builder.finish_batch();

    const VariantRef root = values.value_at(0);
    for (std::string_view path : {"b", "k5", "mixed", "objects"}) {
        SCOPED_TRACE(path);
        const auto* legacy_subcolumn = legacy->get_subcolumn(PathInData(path));
        ASSERT_NE(legacy_subcolumn, nullptr);
        VariantRef value;
        ASSERT_TRUE(root.object_find(StringRef(path.data(), path.size()), &value));
        segment_v2::VariantPathBuilder builder {PathInData(path)};
        ASSERT_TRUE(builder.append(value, 0).ok());
        EXPECT_EQ(remove_nullable(builder.type())->get_name(),
                  remove_nullable(legacy_subcolumn->get_least_common_type())->get_name());
    }
}

TEST(VariantPathBuilderTest, PreservesRowsWhenInferredDecimalPromotionOverflows) {
    VariantBatchBuilder value_builder;
    auto large_row = value_builder.begin_row();
    large_row.add_decimal(static_cast<__int128>(VARIANT_DECIMAL16_MAX), 1, 16);
    large_row.finish();
    auto high_scale_row = value_builder.begin_row();
    high_scale_row.add_decimal(1, 38, 16);
    high_scale_row.finish();
    VariantBatchBuilder values = value_builder.finish_batch();

    segment_v2::VariantPathBuilder builder(PathInData("metric"));
    ASSERT_TRUE(builder.append(values.value_at(0), 0).ok());
    ASSERT_TRUE(builder.append(values.value_at(1), 1).ok());

    EXPECT_EQ(remove_nullable(builder.type())->get_primitive_type(), TYPE_JSONB);
    EXPECT_EQ(builder.non_null_rows(), 2);
    EXPECT_EQ(std::vector<uint32_t>(builder.rowids().begin(), builder.rowids().end()),
              (std::vector<uint32_t> {0, 1}));

    ColumnPtr materialized;
    ASSERT_TRUE(builder.materialize(&materialized).ok());
    const auto& nullable = assert_cast<const ColumnNullable&>(*materialized);
    ASSERT_EQ(nullable.size(), 2);
    EXPECT_FALSE(nullable.is_null_at(0));
    EXPECT_FALSE(nullable.is_null_at(1));
    EXPECT_EQ(builder.type()->to_string(*materialized, 0),
              "9999999999999999999999999999999999999.9");
    EXPECT_EQ(builder.type()->to_string(*materialized, 1),
              "0.00000000000000000000000000000000000001");
}

TEST(VariantPathBuilderTest, PreservesArrayElementsWhenInferredDecimalPromotionOverflows) {
    VariantBatchBuilder value_builder;
    auto large_row = value_builder.begin_row();
    auto large_array = large_row.start_array();
    large_row.add_decimal(static_cast<__int128>(VARIANT_DECIMAL16_MAX), 1, 16);
    large_array.finish();
    large_row.finish();
    auto high_scale_row = value_builder.begin_row();
    auto high_scale_array = high_scale_row.start_array();
    high_scale_row.add_decimal(1, 38, 16);
    high_scale_array.finish();
    high_scale_row.finish();
    VariantBatchBuilder values = value_builder.finish_batch();

    segment_v2::VariantPathBuilder builder(PathInData("metric"));
    ASSERT_TRUE(builder.append(values.value_at(0), 0).ok());
    ASSERT_TRUE(builder.append(values.value_at(1), 1).ok());

    EXPECT_EQ(remove_nullable(builder.type())->get_primitive_type(), TYPE_JSONB);
    ColumnPtr materialized;
    ASSERT_TRUE(builder.materialize(&materialized).ok());
    EXPECT_EQ(builder.type()->to_string(*materialized, 0),
              "[9999999999999999999999999999999999999.9]");
    EXPECT_EQ(builder.type()->to_string(*materialized, 1),
              "[0.00000000000000000000000000000000000001]");
}

TEST(VariantPathBuilderTest, PreservesIncomingArrayWhenInferredDecimalPromotionOverflows) {
    VariantBatchBuilder value_builder;
    auto high_scale_row = value_builder.begin_row();
    auto high_scale_array = high_scale_row.start_array();
    high_scale_row.add_decimal(1, 38, 16);
    high_scale_array.finish();
    high_scale_row.finish();
    auto large_row = value_builder.begin_row();
    auto large_array = large_row.start_array();
    large_row.add_decimal(static_cast<__int128>(VARIANT_DECIMAL16_MAX), 1, 16);
    large_array.finish();
    large_row.finish();
    VariantBatchBuilder values = value_builder.finish_batch();

    segment_v2::VariantPathBuilder builder(PathInData("metric"));
    ASSERT_TRUE(builder.append(values.value_at(0), 0).ok());
    ASSERT_TRUE(builder.append(values.value_at(1), 1).ok());

    EXPECT_EQ(remove_nullable(builder.type())->get_primitive_type(), TYPE_JSONB);
    ColumnPtr materialized;
    ASSERT_TRUE(builder.materialize(&materialized).ok());
    EXPECT_EQ(builder.type()->to_string(*materialized, 0),
              "[0.00000000000000000000000000000000000001]");
    EXPECT_EQ(builder.type()->to_string(*materialized, 1),
              "[9999999999999999999999999999999999999.9]");
}

TEST(VariantPathBuilderTest, StringifiesArrayWithoutTreatingExistingNullAsCastFailure) {
    VariantBatchBuilder value_builder;
    auto row = value_builder.begin_row();
    auto array = row.start_array();
    row.add_int(1);
    row.add_null();
    array.finish();
    row.finish();
    VariantBatchBuilder values = value_builder.finish_batch();

    segment_v2::VariantPathBuilder builder(PathInData("metric"));
    ASSERT_TRUE(builder.append(values.value_at(0), 0).ok());
    ASSERT_TRUE(builder.convert_to(std::make_shared<DataTypeString>()).ok());

    EXPECT_EQ(remove_nullable(builder.type())->get_primitive_type(), TYPE_STRING);
    EXPECT_EQ(builder.non_null_rows(), 1);
    ColumnPtr materialized;
    ASSERT_TRUE(builder.materialize(&materialized).ok());
    EXPECT_EQ(builder.type()->to_string(*materialized, 0), "[1,null]");
}

TEST(VariantPathBuilderTest, SelectsMaterializedAndSparsePathsInStableOrder) {
    VariantBatchBuilder value_builder;
    auto value_row = value_builder.begin_row();
    value_row.add_int(1);
    value_row.finish();
    VariantBatchBuilder values = value_builder.finish_batch();

    struct CandidateSpec {
        std::string path;
        size_t non_null_rows;
        bool is_typed_path;
    };
    const std::vector<CandidateSpec> specs {
            {"typed", 0, true}, {"z", 2, false},   {"b.c", 2, false},
            {"a", 3, false},    {"a.c", 2, false},
    };
    std::vector<segment_v2::VariantPathBuilder> builders;
    builders.reserve(specs.size());
    for (const auto& spec : specs) {
        builders.emplace_back(PathInData(spec.path));
        for (size_t row = 0; row < spec.non_null_rows; ++row) {
            ASSERT_TRUE(builders.back().append(values.value_at(0), row).ok());
        }
    }

    std::vector<segment_v2::VariantPathSelectionCandidate> candidates;
    candidates.reserve(specs.size());
    for (size_t index = 0; index < specs.size(); ++index) {
        candidates.push_back(
                {.builder = &builders[index], .is_typed_path = specs[index].is_typed_path});
    }
    const auto selected_paths = [&](const auto& indices) {
        std::vector<std::string> paths;
        paths.reserve(indices.size());
        for (size_t index : indices) {
            paths.push_back(candidates[index].builder->path().get_path());
        }
        return paths;
    };

    struct SelectionCase {
        bool typed_paths_to_sparse;
        std::vector<std::string> materialized;
        std::vector<std::string> sparse;
    };
    const std::vector<SelectionCase> cases {
            {false, {"a", "b.c", "typed"}, {"a.c", "z"}},
            {true, {"a", "b.c"}, {"a.c", "z"}},
    };
    for (const auto& test_case : cases) {
        SCOPED_TRACE(testing::Message()
                     << "typed_paths_to_sparse=" << test_case.typed_paths_to_sparse);
        const auto selection =
                segment_v2::select_variant_paths(candidates, 2, test_case.typed_paths_to_sparse);
        EXPECT_EQ(selected_paths(selection.materialized), test_case.materialized);
        EXPECT_EQ(selected_paths(selection.sparse), test_case.sparse);
    }
}

TEST(VariantPathBuilderTest, ShredderReusesCanonicalPathsAcrossAppends) {
    DataTypeVariantV2SerDe serde;
    DataTypeSerDe::FormatOptions format_options;
    auto dotted = ColumnVariantV2::create();
    const std::string_view dotted_json = R"({"a.b":1})";
    Slice dotted_slice(dotted_json.data(), dotted_json.size());
    ASSERT_TRUE(serde.deserialize_one_cell_from_json(*dotted, dotted_slice, format_options).ok());
    auto nested = ColumnVariantV2::create();
    const std::string_view nested_json = R"({"a":{"b":2}})";
    Slice nested_slice(nested_json.data(), nested_json.size());
    ASSERT_TRUE(serde.deserialize_one_cell_from_json(*nested, nested_slice, format_options).ok());

    segment_v2::VariantShredderOptions options;
    options.max_subcolumns_count = 0;
    options.sparse_bucket_count = 1;
    segment_v2::VariantShredder shredder(std::move(options));
    ASSERT_TRUE(shredder.append(dotted->read_view(), 0, dotted->size()).ok());
    ASSERT_TRUE(shredder.append(nested->read_view(), 0, nested->size()).ok());

    segment_v2::VariantShreddedColumns shredded;
    ASSERT_TRUE(shredder.finish(&shredded).ok());
    ASSERT_EQ(shredded.materialized.size(), 1);
    const auto& selected = shredded.materialized.front();
    EXPECT_EQ(selected.path.get_path(), "a.b");
    EXPECT_EQ(selected.path.get_parts().size(), 2);
    EXPECT_EQ(selected.rowids.size(), 2);
    ASSERT_TRUE(selected.column);
    ASSERT_EQ(selected.column->size(), 2);
    EXPECT_EQ(selected.type->to_string(*selected.column, 0), "1");
    EXPECT_EQ(selected.type->to_string(*selected.column, 1), "2");
}

TEST(VariantShredderTest, ReusesRootAndNestedPathsForSharedMetadata) {
    DataTypeVariantV2SerDe serde;
    DataTypeSerDe::FormatOptions format_options;
    auto values = ColumnVariantV2::create();
    for (const std::string_view json :
         {R"({"a":1,"nested":{"x":2,"y":3}})", R"({"a":4,"nested":{"x":5,"y":6}})"}) {
        Slice slice(json.data(), json.size());
        ASSERT_TRUE(serde.deserialize_one_cell_from_json(*values, slice, format_options).ok());
    }
    ASSERT_EQ(values->read_view().metadata_count(), 1);

    segment_v2::VariantShredderOptions options;
    options.max_subcolumns_count = 0;
    options.sparse_bucket_count = 1;
    segment_v2::VariantShredder shredder(std::move(options));
    ASSERT_TRUE(shredder.append(values->read_view(), 0, values->size()).ok());

    segment_v2::VariantShreddedColumns shredded;
    ASSERT_TRUE(shredder.finish(&shredded).ok());
    ASSERT_EQ(shredded.materialized.size(), 3);
    const auto expect_path = [&](std::string_view path, std::string_view first,
                                 std::string_view second) {
        const auto found = std::ranges::find_if(shredded.materialized, [&](const auto& column) {
            return column.path.get_path() == path;
        });
        ASSERT_NE(found, shredded.materialized.end()) << path;
        ASSERT_TRUE(found->column);
        EXPECT_EQ(found->rowids, (DorisVector<uint32_t> {0, 1}));
        EXPECT_EQ(found->type->to_string(*found->column, 0), first);
        EXPECT_EQ(found->type->to_string(*found->column, 1), second);
    };
    expect_path("a", "1", "4");
    expect_path("nested.x", "2", "5");
    expect_path("nested.y", "3", "6");
}

static void expect_variant_statistics_equal(const segment_v2::VariantStatistics& actual,
                                            const segment_v2::VariantStatistics& expected) {
    EXPECT_EQ(actual.subcolumns_non_null_size, expected.subcolumns_non_null_size);
    EXPECT_EQ(actual.sparse_column_non_null_size, expected.sparse_column_non_null_size);
    EXPECT_EQ(actual.doc_value_column_non_null_size, expected.doc_value_column_non_null_size);
    EXPECT_EQ(actual.has_nested_group, expected.has_nested_group);
}

static void expect_physical_column_data_equal(const IColumn& actual, const IColumn& expected) {
    ASSERT_EQ(actual.get_name(), expected.get_name());
    ASSERT_EQ(actual.size(), expected.size());
    if (const auto* actual_string = check_and_get_column<ColumnString>(actual)) {
        const auto& expected_string = assert_cast<const ColumnString&>(expected);
        EXPECT_EQ(actual_string->get_chars(), expected_string.get_chars());
        EXPECT_EQ(actual_string->get_offsets(), expected_string.get_offsets());
        return;
    }
    if (const auto* actual_nullable = check_and_get_column<ColumnNullable>(actual)) {
        const auto& expected_nullable = assert_cast<const ColumnNullable&>(expected);
        EXPECT_EQ(actual_nullable->get_null_map_data(), expected_nullable.get_null_map_data());
        expect_physical_column_data_equal(actual_nullable->get_nested_column(),
                                          expected_nullable.get_nested_column());
        return;
    }
    if (const auto* actual_map = check_and_get_column<ColumnMap>(actual)) {
        const auto& expected_map = assert_cast<const ColumnMap&>(expected);
        EXPECT_EQ(actual_map->get_offsets(), expected_map.get_offsets());
        expect_physical_column_data_equal(actual_map->get_keys(), expected_map.get_keys());
        expect_physical_column_data_equal(actual_map->get_values(), expected_map.get_values());
        return;
    }
    if (const auto* actual_array = check_and_get_column<ColumnArray>(actual)) {
        const auto& expected_array = assert_cast<const ColumnArray&>(expected);
        EXPECT_EQ(actual_array->get_offsets(), expected_array.get_offsets());
        expect_physical_column_data_equal(actual_array->get_data(), expected_array.get_data());
        return;
    }
    for (size_t row = 0; row < actual.size(); ++row) {
        EXPECT_EQ(actual.compare_at(row, row, expected, -1), 0) << "row=" << row;
    }
}

static void expect_physical_columns_equal(const ColumnPtr& actual, const ColumnPtr& expected) {
    ASSERT_TRUE(actual);
    ASSERT_TRUE(expected);
    expect_physical_column_data_equal(*actual, *expected);
}

static void expect_shredded_columns_equal(const segment_v2::VariantShreddedColumns& actual,
                                          const segment_v2::VariantShreddedColumns& expected) {
    ASSERT_EQ(actual.num_rows, expected.num_rows);
    expect_physical_columns_equal(actual.root_jsonb, expected.root_jsonb);

    ASSERT_EQ(actual.materialized.size(), expected.materialized.size());
    for (size_t index = 0; index < actual.materialized.size(); ++index) {
        const auto& actual_path = actual.materialized[index];
        const auto& expected_path = expected.materialized[index];
        EXPECT_EQ(actual_path.path, expected_path.path) << "index=" << index;
        ASSERT_TRUE(actual_path.type);
        ASSERT_TRUE(expected_path.type);
        EXPECT_TRUE(actual_path.type->equals(*expected_path.type)) << "index=" << index;
        EXPECT_EQ(actual_path.rowids, expected_path.rowids) << "index=" << index;
        expect_physical_columns_equal(actual_path.column, expected_path.column);
    }

    ASSERT_EQ(actual.binary_buckets.size(), expected.binary_buckets.size());
    for (size_t bucket = 0; bucket < actual.binary_buckets.size(); ++bucket) {
        expect_physical_columns_equal(actual.binary_buckets[bucket].column,
                                      expected.binary_buckets[bucket].column);
        expect_variant_statistics_equal(actual.binary_buckets[bucket].statistics,
                                        expected.binary_buckets[bucket].statistics);
    }
    expect_variant_statistics_equal(actual.statistics, expected.statistics);
}

TEST(VariantShredderTest, ChunkedBinaryTransposeMatchesSingleChunkForOrdinaryAndDoc) {
    const auto path_for_bucket = [](uint32_t bucket, std::string_view prefix) {
        for (uint32_t suffix = 0; suffix < 1024; ++suffix) {
            std::string path(prefix);
            path += std::to_string(suffix);
            if (variant_util::variant_binary_shard_of({path.data(), path.size()}, 2) == bucket) {
                return path;
            }
        }
        DORIS_CHECK(false) << "failed to find path for bucket " << bucket;
        return std::string {};
    };
    const std::array<std::string, 3> sparse_paths {
            path_for_bucket(0, "chunk_left_"),
            path_for_bucket(0, "chunk_middle_"),
            path_for_bucket(1, "chunk_right_"),
    };
    // Ordinary sparse cells per row are 3,1,0,2,3,1,0,2. A four-cell limit
    // crosses exact boundaries and empty rows; a two-cell limit also forces the
    // over-wide-row branch. DOC additionally publishes hot in the binary map, so
    // both layouts exercise multiple chunks.
    const std::array<uint8_t, 8> sparse_masks {0b111, 0b001, 0b000, 0b110,
                                               0b111, 0b100, 0b000, 0b011};
    auto values = ColumnVariantV2::create();
    DataTypeVariantV2SerDe serde;
    DataTypeSerDe::FormatOptions format_options;
    for (size_t row = 0; row < sparse_masks.size(); ++row) {
        std::string json = "{\"hot\":" + std::to_string(row);
        for (size_t path = 0; path < sparse_paths.size(); ++path) {
            if ((sparse_masks[row] & (1U << path)) != 0) {
                json += ",\"" + sparse_paths[path] + "\":" + std::to_string(row * 10 + path);
            }
        }
        json += "}";
        Slice slice(json.data(), json.size());
        ASSERT_TRUE(serde.deserialize_one_cell_from_json(*values, slice, format_options).ok());
    }

    for (const auto physical_layout : {segment_v2::VariantShredderPhysicalLayout::ORDINARY,
                                       segment_v2::VariantShredderPhysicalLayout::DOC}) {
        SCOPED_TRACE(physical_layout == segment_v2::VariantShredderPhysicalLayout::ORDINARY
                             ? "ordinary"
                             : "doc");
        const segment_v2::VariantShredderOptions options {
                .physical_layout = physical_layout,
                .max_subcolumns_count = 1,
                .sparse_bucket_count = 2,
                .doc_bucket_count = 2,
                .doc_materialization_min_rows = sparse_masks.size() + 1,
        };
        segment_v2::VariantShredder single_chunk(options);
        ASSERT_TRUE(single_chunk.append(values->read_view(), 0, values->size()).ok());
        segment_v2::VariantShreddedColumns expected;
        ASSERT_TRUE(single_chunk.finish(&expected).ok());

        for (const size_t chunk_limit : {size_t {4}, size_t {2}}) {
            SCOPED_TRACE(testing::Message() << "chunk_limit=" << chunk_limit);
            segment_v2::VariantShredder chunked(options);
            segment_v2::VariantShredder::TestAccess::set_binary_cells_per_chunk(chunked,
                                                                                chunk_limit);
            ASSERT_TRUE(chunked.append(values->read_view(), 0, values->size()).ok());
            segment_v2::VariantShreddedColumns actual;
            ASSERT_TRUE(chunked.finish(&actual).ok());

            const size_t expected_chunks =
                    physical_layout == segment_v2::VariantShredderPhysicalLayout::ORDINARY
                            ? (chunk_limit == 4 ? 4 : 6)
                            : (chunk_limit == 4 ? 6 : 8);
            EXPECT_EQ(segment_v2::VariantShredder::TestAccess::binary_chunk_count(chunked),
                      expected_chunks);
            expect_shredded_columns_equal(actual, expected);
            ASSERT_EQ(actual.binary_buckets.size(), 2);
            for (size_t bucket = 0; bucket < actual.binary_buckets.size(); ++bucket) {
                const auto& map =
                        assert_cast<const ColumnMap&>(*actual.binary_buckets[bucket].column);
                EXPECT_EQ(map.size(), sparse_masks.size());
                EXPECT_GT(map.get_keys().size(), 0) << "bucket=" << bucket;
            }
        }
    }
}

static void construct_column(ColumnPB* column_pb, int32_t col_unique_id,
                             const std::string& column_type, const std::string& column_name,
                             int variant_max_subcolumns_count = 3, bool is_key = false,
                             bool is_nullable = false, int variant_sparse_hash_shard_count = 0,
                             bool variant_enable_doc_mode = false,
                             int64_t variant_doc_materialization_min_rows = 0,
                             int variant_doc_hash_shard_count = 0,
                             bool variant_enable_nested_group = false) {
    column_pb->set_unique_id(col_unique_id);
    column_pb->set_name(column_name);
    column_pb->set_type(column_type);
    column_pb->set_is_key(is_key);
    column_pb->set_is_nullable(is_nullable);
    if (column_type == "VARIANT") {
        column_pb->set_variant_max_subcolumns_count(variant_max_subcolumns_count);
        column_pb->set_variant_max_sparse_column_statistics_size(10000);
        // 5 sparse hash shard
        column_pb->set_variant_sparse_hash_shard_count(variant_sparse_hash_shard_count);
        column_pb->set_variant_enable_doc_mode(variant_enable_doc_mode);
        column_pb->set_variant_doc_materialization_min_rows(variant_doc_materialization_min_rows);
        if (variant_doc_hash_shard_count > 0) {
            column_pb->set_variant_doc_hash_shard_count(variant_doc_hash_shard_count);
        }
        column_pb->set_variant_enable_nested_group(variant_enable_nested_group);
    }
}

static void construct_tablet_index(TabletIndexPB* tablet_index, int64_t index_id,
                                   const std::string& index_name, int32_t col_unique_id) {
    tablet_index->set_index_id(index_id);
    tablet_index->set_index_name(index_name);
    tablet_index->set_index_type(IndexType::INVERTED);
    tablet_index->add_col_unique_id(col_unique_id);
}

static bool nested_group_write_path_available() {
    auto provider = segment_v2::create_nested_group_read_provider();
    return provider != nullptr && provider->should_enable_nested_group_read_path();
}

static void fill_nullable_variant_block(Block* block,
                                        std::unordered_map<int, std::string>* inserted_jsonstr,
                                        variant_util::PathToNoneNullValues* path_with_size) {
    MutableColumnPtr column = IColumn::mutate(block->get_by_position(0).column);
    auto* nullable_object = assert_cast<ColumnNullable*>(column.get());
    for (int idx = 0; idx < 10; idx++) {
        nullable_object->insert_default(); // insert null
        {
            auto column_object = nullable_object->get_nested_column_ptr();
            auto res = VariantUtil::fill_object_column_with_test_data(column_object, 80,
                                                                      inserted_jsonstr);
            path_with_size->insert(res.begin(), res.end());
        }
        for (int j = 0; j < 80; ++j) {
            Field f = Field::create_field<TYPE_BOOLEAN>(UInt8(0));
            nullable_object->get_null_map_column().insert(f);
        }
        nullable_object->insert_many_defaults(17);
        {
            auto column_object = nullable_object->get_nested_column_ptr();
            auto res = VariantUtil::fill_object_column_with_test_data(column_object, 2,
                                                                      inserted_jsonstr);
            path_with_size->insert(res.begin(), res.end());
        }
        for (int j = 0; j < 2; ++j) {
            Field f = Field::create_field<TYPE_BOOLEAN>(UInt8(0));
            nullable_object->get_null_map_column().insert(f);
        }
    }
    block->replace_by_position(0, std::move(column));
}

struct VariantStorageParseWriteResult {
    size_t num_rows = 0;
    size_t parsed_subcolumns = 0;
    size_t parsed_allocated_bytes = 0;
    size_t doc_value_entries = 0;
    int footer_columns = 0;
    int materialized_columns = 0;
    int sparse_columns = 0;
    int doc_value_columns = 0;
    uint64_t segment_file_size = 0;
};

class ScopedVariantStorageParseMode {
public:
    explicit ScopedVariantStorageParseMode(int32_t value)
            : _old_value(config::variant_storage_parse_mode) {
        config::variant_storage_parse_mode = value;
    }
    ~ScopedVariantStorageParseMode() { config::variant_storage_parse_mode = _old_value; }

private:
    int32_t _old_value;
};

// MockColumnReaderCache class for testing
class MockColumnReaderCache : public segment_v2::ColumnReaderCache {
public:
    MockColumnReaderCache(const SegmentFooterPB& footer, const io::FileReaderSPtr& file_reader,
                          const std::shared_ptr<TabletSchema>& tablet_schema)
            : ColumnReaderCache(nullptr, nullptr, nullptr, 0,
                                [](std::shared_ptr<SegmentFooterPB>&, OlapReaderStatistics*,
                                   const io::IOContext*) { return Status::OK(); }),
              _footer(footer),
              _file_reader(file_reader),
              _tablet_schema(tablet_schema) {}

    Status get_path_column_reader(int32_t col_uid, PathInData relative_path,
                                  std::shared_ptr<segment_v2::ColumnReader>* column_reader,
                                  OlapReaderStatistics* stats,
                                  const SubcolumnColumnMetaInfo::Node* node_hint = nullptr,
                                  const io::IOContext* io_ctx = nullptr) override {
        DCHECK(node_hint != nullptr);
        // Use node_hint's footer_ordinal to locate the specific ColumnMeta
        int32_t footer_ordinal = node_hint->data.footer_ordinal;
        if (footer_ordinal < 0 || footer_ordinal >= _footer.columns_size()) {
            *column_reader = nullptr;
            return Status::OK();
        }

        // Create ColumnReaderOptions
        ColumnReaderOptions opts;
        opts.kept_in_memory = false;
        opts.be_exec_version = BeExecVersionManager::get_newest_version();
        opts.tablet_schema = _tablet_schema;

        // Use ColumnReader::create to generate the corresponding ColumnReader
        return segment_v2::ColumnReader::create(opts, _footer.columns(footer_ordinal),
                                                _footer.num_rows(), _file_reader, column_reader);
    }

private:
    const SegmentFooterPB& _footer;
    const io::FileReaderSPtr& _file_reader;
    const std::shared_ptr<TabletSchema>& _tablet_schema;
};

// Helper to create a root VariantColumnReader using ColumnMetaAccessor, which
// hides inline vs external column meta layout (V2 vs V3).
static Status create_variant_root_reader(const SegmentFooterPB& footer,
                                         const io::FileReaderSPtr& file_reader,
                                         const TabletSchemaSPtr& tablet_schema,
                                         std::shared_ptr<segment_v2::ColumnReader>* out) {
    segment_v2::ColumnMetaAccessor accessor;
    RETURN_IF_ERROR(accessor.init(footer, file_reader));

    segment_v2::ColumnReaderOptions opts;
    opts.kept_in_memory = false;
    opts.be_exec_version = BeExecVersionManager::get_newest_version();
    opts.tablet_schema = tablet_schema;

    auto variant_reader = std::make_shared<segment_v2::VariantColumnReader>();
    int32_t root_uid = tablet_schema->column(0).unique_id();
    auto footer_sp = std::make_shared<SegmentFooterPB>();
    footer_sp->CopyFrom(footer);
    RETURN_IF_ERROR(variant_reader->init(opts, &accessor, footer_sp, root_uid, footer.num_rows(),
                                         file_reader));
    *out = std::move(variant_reader);
    return Status::OK();
}

class VariantColumnWriterReaderTest : public testing::Test {
public:
    void SetUp() override {
        // absolute dir
        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        _current_dir = std::string(buffer);
        _absolute_dir = _current_dir + std::string(dest_dir);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(_absolute_dir).ok());

        // tmp dir
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(tmp_dir).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(std::string(tmp_dir), 1024000000);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        Status st = tmp_file_dirs->init();
        EXPECT_TRUE(st.ok()) << st.to_json();
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));

        // storage engine
        doris::EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        _engine_ref = engine.get();
        _data_dir = std::make_unique<DataDir>(*_engine_ref, _absolute_dir);
        static_cast<void>(_data_dir->update_capacity());
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
        _engine_ref = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    VariantColumnWriterReaderTest() = default;
    ~VariantColumnWriterReaderTest() override = default;

protected:
    void init_variant_tablet(int64_t tablet_id, int variant_max_subcolumns_count = 10,
                             bool variant_enable_nested_group = false, bool is_nullable = false,
                             bool variant_enable_doc_mode = false,
                             int64_t variant_doc_materialization_min_rows = 0,
                             int variant_doc_hash_shard_count = 0) {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);
        construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", variant_max_subcolumns_count,
                         false, is_nullable, 0, variant_enable_doc_mode,
                         variant_doc_materialization_min_rows, variant_doc_hash_shard_count,
                         variant_enable_nested_group);
        _tablet_schema = std::make_shared<TabletSchema>();
        _tablet_schema->init_from_pb(schema_pb);

        TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
        _tablet_schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3);
        tablet_meta->_tablet_id = tablet_id;
        _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());

        EXPECT_TRUE(_tablet->init().ok());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());
    }

    void init_tablet_from_current_schema(int64_t tablet_id,
                                         TabletStorageFormatPB storage_format =
                                                 TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2) {
        TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
        _tablet_schema->set_storage_format(storage_format);
        tablet_meta->_tablet_id = tablet_id;
        _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
        ASSERT_TRUE(_tablet->init().ok());
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());
    }

    RowsetSharedPtr create_variant_rowset(const std::vector<std::vector<std::string>>& batches,
                                          int64_t version, int64_t max_rows_per_segment = 200) {
        RowsetWriterContext ctx;
        RowsetId rowset_id;
        rowset_id.init(version + 1000);
        ctx.rowset_id = rowset_id;
        ctx.rowset_type = BETA_ROWSET;
        ctx.data_dir = _data_dir.get();
        ctx.rowset_state = VISIBLE;
        ctx.tablet_schema = _tablet_schema;
        ctx.tablet_path = _tablet->tablet_path();
        ctx.tablet_id = _tablet->tablet_id();
        ctx.tablet = _tablet;
        ctx.version = Version(version, version);
        ctx.segments_overlap = NONOVERLAPPING;
        ctx.max_rows_per_segment = max_rows_per_segment;
        ctx.write_type = DataWriteType::TYPE_DIRECT;

        auto res = RowsetFactory::create_rowset_writer(*_engine_ref, ctx, false);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto rowset_writer = std::move(res).value();

        for (const auto& batch : batches) {
            Block block = _tablet_schema->create_block();
            auto columns = std::move(block).mutate_columns();
            auto variant_col = ColumnVariant::create(
                    _tablet_schema->column(0).variant_max_subcolumns_count(), false);
            auto json_col = ColumnString::create();
            for (const auto& json : batch) {
                json_col->insert_data(json.data(), json.size());
            }
            ParseConfig config;
            variant_util::parse_json_to_variant(*variant_col, *json_col, config);
            columns[0] = std::move(variant_col);
            block.set_columns(std::move(columns));

            auto st = rowset_writer->add_block(&block);
            EXPECT_TRUE(st.ok()) << st.to_string();
            st = rowset_writer->flush();
            EXPECT_TRUE(st.ok()) << st.to_string();
        }

        RowsetSharedPtr rowset;
        EXPECT_TRUE(rowset_writer->build(rowset).ok());
        return rowset;
    }

    std::vector<RowsetReaderSharedPtr> create_rowset_readers(
            const std::vector<RowsetSharedPtr>& rowsets) const {
        std::vector<RowsetReaderSharedPtr> readers;
        readers.reserve(rowsets.size());
        for (const auto& rowset : rowsets) {
            RowsetReaderSharedPtr reader;
            EXPECT_TRUE(rowset->create_reader(&reader).ok());
            readers.push_back(std::move(reader));
        }
        return readers;
    }

    Status append_json_batch(ColumnWriter* writer, const std::vector<std::string>& jsons) {
        if (writer == nullptr) {
            return Status::InvalidArgument("writer is null");
        }

        Block block = _tablet_schema->create_block();
        auto columns = std::move(block).mutate_columns();
        auto variant_col = ColumnVariant::create(
                _tablet_schema->column(0).variant_max_subcolumns_count(), false);
        auto json_col = ColumnString::create();
        for (const auto& json : jsons) {
            json_col->insert_data(json.data(), json.size());
        }
        ParseConfig config;
        variant_util::parse_json_to_variant(*variant_col, *json_col, config);
        columns[0] = std::move(variant_col);
        block.set_columns(std::move(columns));

        auto converter = std::make_unique<OlapBlockDataConvertor>();
        converter->add_column_data_convertor(_tablet_schema->column(0));
        converter->set_source_content(&block, 0, jsons.size());
        auto [status, accessor] = converter->convert_column_data(0);
        RETURN_IF_ERROR(status);
        return writer->append(accessor->get_nullmap(), accessor->get_data(), jsons.size());
    }

    Status read_root_rows(const SegmentFooterPB& footer, const std::string& file_path,
                          std::vector<std::string>* out_rows) {
        io::FileReaderSPtr file_reader;
        RETURN_IF_ERROR(io::global_local_filesystem()->open_file(file_path, &file_reader));

        std::shared_ptr<ColumnReader> column_reader;
        RETURN_IF_ERROR(
                create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader));

        auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
        MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

        TabletColumn parent_column = _tablet_schema->column(0);
        StorageReadOptions storage_read_opts;
        storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
        OlapReaderStatistics stats;
        storage_read_opts.stats = &stats;

        ColumnIteratorUPtr iterator;
        RETURN_IF_ERROR(variant_column_reader->new_iterator(
                &iterator, &parent_column, &storage_read_opts, &column_reader_cache));

        ColumnIteratorOptions column_iter_opts;
        column_iter_opts.stats = &stats;
        column_iter_opts.file_reader = file_reader.get();
        RETURN_IF_ERROR(iterator->init(column_iter_opts));

        MutableColumnPtr dst =
                ColumnVariant::create(parent_column.variant_max_subcolumns_count(), false);
        size_t nrows = footer.num_rows();
        RETURN_IF_ERROR(iterator->seek_to_ordinal(0));
        RETURN_IF_ERROR(iterator->next_batch(&nrows, dst));

        out_rows->clear();
        out_rows->reserve(nrows);
        DataTypeSerDe::FormatOptions options;
        for (size_t i = 0; i < nrows; ++i) {
            std::string value;
            assert_cast<ColumnVariant*>(dst.get())->serialize_one_row_to_string(i, &value, options);
            out_rows->push_back(std::move(value));
        }
        return Status::OK();
    }

    Status read_variant_path_rows(const SegmentFooterPB& footer, const std::string& file_path,
                                  std::string_view relative_path, FieldType field_type,
                                  std::vector<std::string>* out_rows) {
        io::FileReaderSPtr file_reader;
        RETURN_IF_ERROR(io::global_local_filesystem()->open_file(file_path, &file_reader));

        std::shared_ptr<ColumnReader> column_reader;
        RETURN_IF_ERROR(
                create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader));

        auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
        MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

        const TabletColumn& parent_column = _tablet_schema->column(0);
        const std::string full_path =
                parent_column.name_lower_case() + "." + std::string(relative_path);
        TabletColumn path_column;
        path_column.set_name(full_path);
        path_column.set_type(field_type);
        path_column.set_parent_unique_id(parent_column.unique_id());
        path_column.set_path_info(PathInData(full_path));
        path_column.set_is_nullable(true);

        StorageReadOptions storage_read_opts;
        storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
        OlapReaderStatistics stats;
        storage_read_opts.stats = &stats;

        ColumnIteratorUPtr iterator;
        RETURN_IF_ERROR(variant_column_reader->new_iterator(
                &iterator, &path_column, &storage_read_opts, &column_reader_cache));

        ColumnIteratorOptions column_iter_opts;
        column_iter_opts.stats = &stats;
        column_iter_opts.file_reader = file_reader.get();
        RETURN_IF_ERROR(iterator->init(column_iter_opts));

        auto data_type = DataTypeFactory::instance().create_data_type(path_column, false);
        MutableColumnPtr dst = data_type->create_column();
        size_t nrows = footer.num_rows();
        RETURN_IF_ERROR(iterator->seek_to_ordinal(0));
        RETURN_IF_ERROR(iterator->next_batch(&nrows, dst));

        out_rows->clear();
        out_rows->reserve(nrows);
        for (size_t i = 0; i < nrows; ++i) {
            out_rows->push_back(data_type->to_string(*dst, i));
        }
        return Status::OK();
    }

    Status create_inverted_index_file_writer(
            std::string_view rowset_id, const std::string& file_path,
            std::unique_ptr<segment_v2::IndexFileWriter>* index_file_writer) {
        DORIS_CHECK(index_file_writer != nullptr);
        index_file_writer->reset();
        if (!_tablet_schema->has_inverted_index()) {
            return Status::OK();
        }

        const std::string index_path_prefix = std::string(
                segment_v2::InvertedIndexDescriptor::get_index_file_path_prefix(file_path));
        io::FileWriterPtr index_v2_file_writer;
        if (_tablet_schema->get_inverted_index_storage_format() !=
            InvertedIndexStorageFormatPB::V1) {
            RETURN_IF_ERROR(io::global_local_filesystem()->create_file(
                    segment_v2::InvertedIndexDescriptor::get_index_file_path_v2(index_path_prefix),
                    &index_v2_file_writer));
        }
        *index_file_writer = std::make_unique<segment_v2::IndexFileWriter>(
                io::global_local_filesystem(), index_path_prefix, std::string(rowset_id),
                0 /* seg_id */, _tablet_schema->get_inverted_index_storage_format(),
                std::move(index_v2_file_writer), true /* can_use_ram_dir */, _tablet->tablet_id());
        return Status::OK();
    }

    Status write_variant_segment(
            const ColumnPtr& source, const DataTypePtr& source_type, std::string_view rowset_id,
            SegmentFooterPB* footer, std::string* file_path, size_t first_batch_rows = 0,
            uint64_t* buffered_bytes = nullptr,
            VariantIndexWritePolicy index_write_policy = VariantIndexWritePolicy::NONE) {
        DCHECK(source);
        DCHECK(source_type != nullptr);
        DCHECK(footer != nullptr);
        DCHECK(file_path != nullptr);
        const size_t num_rows = source->size();
        DCHECK_GT(num_rows, 0);

        *file_path = local_segment_path(_tablet->tablet_path(), rowset_id, 0);
        static_cast<void>(io::global_local_filesystem()->delete_file(*file_path));
        io::FileWriterPtr file_writer;
        RETURN_IF_ERROR(io::global_local_filesystem()->create_file(*file_path, &file_writer));

        std::unique_ptr<segment_v2::IndexFileWriter> index_file_writer;
        if (index_write_policy == VariantIndexWritePolicy::BLOOM_AND_INVERTED) {
            RETURN_IF_ERROR(
                    create_inverted_index_file_writer(rowset_id, *file_path, &index_file_writer));
        }

        footer->Clear();
        RowsetWriterContext rowset_ctx;
        rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
        rowset_ctx.tablet_schema = _tablet_schema;
        rowset_ctx.tablet = _tablet;
        rowset_ctx.tablet_path = _tablet->tablet_path();

        TabletColumn column = _tablet_schema->column(0);
        ColumnWriterOptions opts;
        opts.meta = footer->add_columns();
        opts.index_file_writer = index_file_writer.get();
        opts.compression_type = CompressionTypePB::LZ4;
        opts.file_writer = file_writer.get();
        opts.footer = footer;
        opts.rowset_ctx = &rowset_ctx;
        opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
        _init_column_meta(opts.meta, 0, column, opts);

        std::unique_ptr<ColumnWriter> writer;
        RETURN_IF_ERROR(ColumnWriter::create(opts, &column, file_writer.get(), &writer));
        RETURN_IF_ERROR(writer->init());

        auto converter = std::make_unique<OlapBlockDataConvertor>();
        converter->add_column_data_convertor(column);
        const auto append_batch = [&](size_t row_pos, size_t rows) -> Status {
            RETURN_IF_ERROR(converter->set_source_content_with_specifid_column(
                    {source, source_type, column.name()}, row_pos, rows, 0));
            auto [convert_status, accessor] = converter->convert_column_data(0);
            RETURN_IF_ERROR(convert_status);
            DCHECK(accessor != nullptr);
            RETURN_IF_ERROR(writer->append(accessor->get_nullmap(), accessor->get_data(), rows));
            converter->clear_source_content(0);
            return Status::OK();
        };
        if (first_batch_rows > 0 && first_batch_rows < num_rows) {
            RETURN_IF_ERROR(append_batch(0, first_batch_rows));
            RETURN_IF_ERROR(append_batch(first_batch_rows, num_rows - first_batch_rows));
        } else {
            RETURN_IF_ERROR(append_batch(0, num_rows));
        }
        if (buffered_bytes != nullptr) {
            *buffered_bytes = writer->estimate_buffer_size();
        }

        RETURN_IF_ERROR(writer->finish());
        RETURN_IF_ERROR(writer->write_data());
        RETURN_IF_ERROR(writer->write_ordinal_index());
        RETURN_IF_ERROR(writer->write_zone_map());
        if (index_write_policy == VariantIndexWritePolicy::BLOOM_AND_INVERTED) {
            RETURN_IF_ERROR(writer->write_bloom_filter_index());
            RETURN_IF_ERROR(writer->write_inverted_index());
            if (index_file_writer != nullptr) {
                RETURN_IF_ERROR(index_file_writer->begin_close());
                RETURN_IF_ERROR(index_file_writer->finish_close());
            }
        }
        RETURN_IF_ERROR(file_writer->close());
        footer->set_num_rows(num_rows);
        return Status::OK();
    }

    Status write_extracted_variant_segment(const ColumnPtr& source, const DataTypePtr& source_type,
                                           std::string_view rowset_id, SegmentFooterPB* footer,
                                           std::string* file_path, size_t first_batch_rows = 0) {
        DORIS_CHECK(source.get() != nullptr);
        DORIS_CHECK(source_type != nullptr);
        DORIS_CHECK(footer != nullptr);
        DORIS_CHECK(file_path != nullptr);
        DORIS_CHECK_GT(source->size(), 0);
        DORIS_CHECK_GT(_tablet_schema->num_columns(), 1);

        *file_path = local_segment_path(_tablet->tablet_path(), rowset_id, 0);
        static_cast<void>(io::global_local_filesystem()->delete_file(*file_path));
        io::FileWriterPtr file_writer;
        RETURN_IF_ERROR(io::global_local_filesystem()->create_file(*file_path, &file_writer));

        footer->Clear();
        RowsetWriterContext rowset_ctx;
        rowset_ctx.write_type = DataWriteType::TYPE_COMPACTION;
        rowset_ctx.tablet_schema = _tablet_schema;
        TabletColumn column = _tablet_schema->column(1);

        ColumnWriterOptions opts;
        opts.meta = footer->add_columns();
        opts.compression_type = CompressionTypePB::LZ4;
        opts.file_writer = file_writer.get();
        opts.footer = footer;
        opts.rowset_ctx = &rowset_ctx;
        opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
        _init_column_meta(opts.meta, 0, column, opts);

        std::unique_ptr<ColumnWriter> writer;
        RETURN_IF_ERROR(ColumnWriter::create(opts, &column, file_writer.get(), &writer));
        DORIS_CHECK(dynamic_cast<VariantSubcolumnWriter*>(writer.get()) != nullptr);
        RETURN_IF_ERROR(writer->init());

        OlapBlockDataConvertor converter;
        converter.add_column_data_convertor(column);
        const auto append_batch = [&](size_t row_pos, size_t rows) -> Status {
            RETURN_IF_ERROR(converter.set_source_content_with_specifid_column(
                    {source, source_type, column.name()}, row_pos, rows, 0));
            auto [convert_status, accessor] = converter.convert_column_data(0);
            RETURN_IF_ERROR(convert_status);
            DORIS_CHECK(accessor != nullptr);
            RETURN_IF_ERROR(writer->append(accessor->get_nullmap(), accessor->get_data(), rows));
            converter.clear_source_content(0);
            return Status::OK();
        };
        if (first_batch_rows > 0 && first_batch_rows < source->size()) {
            RETURN_IF_ERROR(append_batch(0, first_batch_rows));
            RETURN_IF_ERROR(append_batch(first_batch_rows, source->size() - first_batch_rows));
        } else {
            RETURN_IF_ERROR(append_batch(0, source->size()));
        }

        RETURN_IF_ERROR(writer->finish());
        RETURN_IF_ERROR(writer->write_data());
        RETURN_IF_ERROR(writer->write_ordinal_index());
        RETURN_IF_ERROR(writer->write_zone_map());
        RETURN_IF_ERROR(file_writer->close());
        footer->set_num_rows(source->size());
        return Status::OK();
    }

    Status try_write_extracted_variant_with_layout(bool nested_group,
                                                   bool deprecated_flatten_nested,
                                                   int64_t tablet_id, std::string_view rowset_id) {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);
        schema_pb.set_enable_variant_flatten_nested(deprecated_flatten_nested);
        construct_column(schema_pb.add_column(), 1, "VARIANT", "v", 1, false, false, 0, false, 0, 0,
                         nested_group);
        _tablet_schema = std::make_shared<TabletSchema>();
        _tablet_schema->init_from_pb(schema_pb);
        const TabletColumn& parent_column = _tablet_schema->column_by_uid(1);
        TabletColumn extracted_column;
        extracted_column.set_name(parent_column.name_lower_case() + ".payload");
        extracted_column.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
        extracted_column.set_parent_unique_id(parent_column.unique_id());
        extracted_column.set_path_info(PathInData(parent_column.name_lower_case() + ".payload"));
        extracted_column.set_is_nullable(true);
        _tablet_schema->append_column(extracted_column);
        init_tablet_from_current_schema(tablet_id);

        auto source = ColumnVariantV2::create();
        DataTypeVariantV2SerDe serde;
        DataTypeSerDe::FormatOptions format_options;
        constexpr std::string_view JSON = R"({"nested":1})";
        Slice slice(JSON.data(), JSON.size());
        RETURN_IF_ERROR(serde.deserialize_one_cell_from_json(*source, slice, format_options));

        SegmentFooterPB footer;
        std::string file_path;
        return write_extracted_variant_segment(source->get_ptr(),
                                               std::make_shared<DataTypeVariantV2>(2, false),
                                               rowset_id, &footer, &file_path);
    }

    Status read_extracted_variant_rows(const SegmentFooterPB& footer, const std::string& file_path,
                                       std::vector<std::string>* out_rows) {
        DORIS_CHECK(out_rows != nullptr);
        DORIS_CHECK_EQ(footer.columns_size(), 1);
        io::FileReaderSPtr file_reader;
        RETURN_IF_ERROR(io::global_local_filesystem()->open_file(file_path, &file_reader));

        ColumnReaderOptions reader_options;
        reader_options.kept_in_memory = false;
        reader_options.be_exec_version = BeExecVersionManager::get_newest_version();
        reader_options.tablet_schema = _tablet_schema;
        std::shared_ptr<ColumnReader> reader;
        RETURN_IF_ERROR(ColumnReader::create(reader_options, footer.columns(0), footer.num_rows(),
                                             file_reader, &reader));

        TabletColumn read_column = _tablet_schema->column(1);
        read_column.set_type(static_cast<FieldType>(footer.columns(0).type()));
        auto read_type = DataTypeFactory::instance().create_data_type(read_column, false);
        ColumnIteratorUPtr iterator;
        RETURN_IF_ERROR(reader->new_iterator(&iterator, &read_column));
        OlapReaderStatistics stats;
        ColumnIteratorOptions iterator_options;
        iterator_options.file_reader = file_reader.get();
        iterator_options.stats = &stats;
        RETURN_IF_ERROR(iterator->init(iterator_options));
        RETURN_IF_ERROR(iterator->seek_to_ordinal(0));

        MutableColumnPtr result = read_type->create_column();
        size_t num_rows = footer.num_rows();
        RETURN_IF_ERROR(iterator->next_batch(&num_rows, result));
        out_rows->clear();
        out_rows->reserve(num_rows);
        for (size_t row = 0; row < num_rows; ++row) {
            out_rows->push_back(read_type->to_string(*result, row));
        }
        return Status::OK();
    }

    Status read_variant_root_rows(const SegmentFooterPB& footer, const std::string& file_path,
                                  std::vector<std::optional<std::string>>* out_rows,
                                  bool read_as_v2 = true) {
        DCHECK(out_rows != nullptr);
        io::FileReaderSPtr file_reader;
        RETURN_IF_ERROR(io::global_local_filesystem()->open_file(file_path, &file_reader));

        std::shared_ptr<ColumnReader> column_reader;
        RETURN_IF_ERROR(
                create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader));
        auto* variant_reader = assert_cast<VariantColumnReader*>(column_reader.get());
        MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

        TabletColumn parent_column = _tablet_schema->column(0);
        parent_column.set_variant_is_v2(read_as_v2);
        auto parent_type = DataTypeFactory::instance().create_data_type(parent_column, false);

        StorageReadOptions read_opts;
        read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
        read_opts.tablet_schema = _tablet_schema;
        OlapReaderStatistics stats;
        read_opts.stats = &stats;

        ColumnIteratorUPtr iterator;
        RETURN_IF_ERROR(variant_reader->new_iterator(&iterator, &parent_column, &read_opts,
                                                     &column_reader_cache));
        ColumnIteratorOptions iterator_opts;
        iterator_opts.stats = &stats;
        iterator_opts.file_reader = file_reader.get();
        RETURN_IF_ERROR(iterator->init(iterator_opts));
        RETURN_IF_ERROR(iterator->seek_to_ordinal(0));

        MutableColumnPtr result = parent_type->create_column();
        size_t num_rows = footer.num_rows();
        RETURN_IF_ERROR(iterator->next_batch(&num_rows, result));

        const IColumn* data = result.get();
        const ColumnNullable* nullable = check_and_get_column<ColumnNullable>(*data);
        if (nullable != nullptr) {
            data = &nullable->get_nested_column();
        }
        out_rows->clear();
        out_rows->reserve(num_rows);
        for (size_t row = 0; row < num_rows; ++row) {
            if (nullable != nullptr && nullable->is_null_at(row)) {
                out_rows->emplace_back(std::nullopt);
            } else {
                out_rows->emplace_back(variant_json_at(*data, row));
            }
        }
        return Status::OK();
    }

    void collect_variant_footer_stats(const SegmentFooterPB& footer, uint64_t file_size,
                                      VariantStorageParseWriteResult* result) {
        CHECK(result != nullptr);
        result->footer_columns = footer.columns_size();
        result->materialized_columns = 0;
        result->sparse_columns = 0;
        result->doc_value_columns = 0;
        result->segment_file_size = file_size;
        for (int i = 1; i < footer.columns_size(); ++i) {
            const auto& meta = footer.columns(i);
            if (!meta.has_column_path_info()) {
                continue;
            }
            PathInData path;
            path.from_protobuf(meta.column_path_info());
            const auto base_path = path.copy_pop_front().get_path();
            if (base_path == "__DORIS_VARIANT_SPARSE__" ||
                base_path.rfind("__DORIS_VARIANT_SPARSE__.b", 0) == 0) {
                ++result->sparse_columns;
            } else if (base_path == "__DORIS_VARIANT_DOC_VALUE__" ||
                       base_path.rfind("__DORIS_VARIANT_DOC_VALUE__.b", 0) == 0) {
                ++result->doc_value_columns;
            } else {
                ++result->materialized_columns;
            }
        }
    }

    Status write_storage_parsed_segment(const std::vector<std::string>& jsons,
                                        std::string_view rowset_id, SegmentFooterPB* footer,
                                        std::string* file_path, bool write_inverted_index = false,
                                        VariantStorageParseWriteResult* result = nullptr) {
        if (footer == nullptr || file_path == nullptr) {
            return Status::InvalidArgument("footer or file_path is null");
        }
        const size_t num_rows = jsons.size();
        *file_path = local_segment_path(_tablet->tablet_path(), rowset_id, 0);
        static_cast<void>(io::global_local_filesystem()->delete_file(*file_path));

        io::FileWriterPtr file_writer;
        RETURN_IF_ERROR(io::global_local_filesystem()->create_file(*file_path, &file_writer));

        std::unique_ptr<segment_v2::IndexFileWriter> index_file_writer;
        RETURN_IF_ERROR(
                create_inverted_index_file_writer(rowset_id, *file_path, &index_file_writer));

        footer->Clear();
        RowsetWriterContext rowset_ctx;
        rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
        rowset_ctx.tablet_schema = _tablet_schema;
        rowset_ctx.tablet = _tablet;
        rowset_ctx.tablet_path = _tablet->tablet_path();

        TabletColumn parent_column = _tablet_schema->column(0);
        ColumnWriterOptions opts;
        opts.meta = footer->add_columns();
        opts.index_file_writer = index_file_writer.get();
        opts.compression_type = CompressionTypePB::LZ4;
        opts.file_writer = file_writer.get();
        opts.footer = footer;
        opts.rowset_ctx = &rowset_ctx;
        opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
        _init_column_meta(opts.meta, 0, parent_column, opts);

        std::unique_ptr<ColumnWriter> writer;
        RETURN_IF_ERROR(ColumnWriter::create(opts, &parent_column, file_writer.get(), &writer));
        RETURN_IF_ERROR(writer->init());

        Block block = _tablet_schema->create_block();
        auto columns = std::move(block).mutate_columns();
        auto scalar_variant = ColumnVariant::create(0, parent_column.variant_enable_doc_mode());
        for (const auto& json : jsons) {
            VariantUtil::insert_root_scalar_field(*scalar_variant,
                                                  Field::create_field<TYPE_STRING>(String(json)));
        }
        columns[0] = std::move(scalar_variant);
        block.set_columns(std::move(columns));

        RETURN_IF_ERROR(
                variant_util::parse_and_materialize_variant_columns(block, *_tablet_schema, {0}));

        const auto& parsed_variant =
                assert_cast<const ColumnVariant&>(*block.get_by_position(0).column);
        if (result != nullptr) {
            result->num_rows = num_rows;
            result->parsed_subcolumns = parsed_variant.get_subcolumns().size();
            result->parsed_allocated_bytes = parsed_variant.allocated_bytes();
            const auto& doc_value_offsets = parsed_variant.serialized_doc_value_column_offsets();
            result->doc_value_entries = doc_value_offsets.empty() ? 0 : doc_value_offsets.back();
        }

        auto converter = std::make_unique<OlapBlockDataConvertor>();
        converter->add_column_data_convertor(parent_column);
        converter->set_source_content(&block, 0, num_rows);
        auto [convert_status, accessor] = converter->convert_column_data(0);
        RETURN_IF_ERROR(convert_status);
        RETURN_IF_ERROR(writer->append(accessor->get_nullmap(), accessor->get_data(), num_rows));

        RETURN_IF_ERROR(writer->finish());
        RETURN_IF_ERROR(writer->write_data());
        RETURN_IF_ERROR(writer->write_ordinal_index());
        RETURN_IF_ERROR(writer->write_zone_map());
        if (write_inverted_index) {
            RETURN_IF_ERROR(writer->write_inverted_index());
            if (index_file_writer != nullptr) {
                RETURN_IF_ERROR(index_file_writer->begin_close());
                RETURN_IF_ERROR(index_file_writer->finish_close());
            }
        }
        RETURN_IF_ERROR(file_writer->close());
        footer->set_num_rows(num_rows);
        if (result != nullptr) {
            int64_t file_size = 0;
            RETURN_IF_ERROR(io::global_local_filesystem()->file_size(*file_path, &file_size));
            collect_variant_footer_stats(*footer, cast_set<uint64_t>(file_size), result);
        }
        return Status::OK();
    }

    TabletSchemaSPtr _tablet_schema = nullptr;
    StorageEngine* _engine_ref = nullptr;
    std::unique_ptr<DataDir> _data_dir = nullptr;
    TabletSharedPtr _tablet = nullptr;
    std::string _absolute_dir;
    std::string _current_dir;
};

class VariantWriterCompatibilityTest : public VariantColumnWriterReaderTest,
                                       public testing::WithParamInterface<VariantWriterInput> {};

static const ColumnMetaPB* find_footer_column_meta_by_relative_path(const SegmentFooterPB& footer,
                                                                    std::string_view relative_path);

TEST_P(VariantWriterCompatibilityTest, ordinary_materialized_sparse_round_trip) {
    init_variant_tablet(10001, 2);

    const std::vector<std::string> jsons {
            R"({"arr":[1,2],"cold_a":"x","hot":1})",
            R"({"arr":[3,null],"cold_b":true,"hot":2})",
            R"({"arr":[],"cold_c":{"x":1},"hot":3})",
            "7",
            "[8,9]",
            "{}",
            R"({"json_null":null})",
            "null",
    };
    ColumnPtr source;
    DataTypePtr source_type;
    ASSERT_TRUE(create_variant_writer_source(GetParam(), jsons, 2, false, {}, &source, &source_type)
                        .ok());

    SegmentFooterPB footer;
    std::string file_path;
    uint64_t buffered_bytes = 0;
    const std::string rowset_id = "shared_ordinary_" + variant_writer_input_name(GetParam());
    ASSERT_TRUE(write_variant_segment(source, source_type, rowset_id, &footer, &file_path, 4,
                                      &buffered_bytes)
                        .ok());
    EXPECT_GT(buffered_bytes, jsons.size());

    VariantStorageParseWriteResult footer_stats;
    collect_variant_footer_stats(footer, 0, &footer_stats);
    EXPECT_GT(footer_stats.materialized_columns, 0);
    EXPECT_GT(footer_stats.sparse_columns, 0);

    const std::vector<std::string_view> expected {
            jsons[0],
            jsons[1],
            // Non-root semantically empty arrays are intentionally omitted by both readers.
            R"({"cold_c":{"x":1},"hot":3})",
            jsons[3],
            jsons[4],
            jsons[5],
            // A JSON-null object leaf is absent from both reconstructed objects.
            "{}",
            // V1 materializes a root JSON null as an empty Variant object after storage.
            "{}",
    };
    for (bool read_as_v2 : {false, true}) {
        SCOPED_TRACE(testing::Message() << "writer=" << variant_writer_input_name(GetParam())
                                        << ", reader=" << (read_as_v2 ? "V2" : "V1"));
        auto reader_expected = expected;
        if (!read_as_v2) {
            // The legacy reader serializes BOOL subcolumns as their numeric storage value.
            reader_expected[1] = R"({"arr":[3,null],"cold_b":1,"hot":2})";
        }
        std::vector<std::optional<std::string>> actual;
        ASSERT_TRUE(read_variant_root_rows(footer, file_path, &actual, read_as_v2).ok());
        ASSERT_EQ(actual.size(), jsons.size());
        for (size_t row = 0; row < jsons.size(); ++row) {
            ASSERT_TRUE(actual[row].has_value());
            EXPECT_EQ(*actual[row], reader_expected[row]);
        }
    }
}

TEST_P(VariantWriterCompatibilityTest, materialized_array_preserves_middle_row_gap) {
    init_variant_tablet(10021 + static_cast<int>(GetParam()), 1);

    const std::vector<std::string> jsons {
            R"({"arr":[1,2]})",
            "{}",
            R"({"arr":[3]})",
    };
    ColumnPtr source;
    DataTypePtr source_type;
    ASSERT_TRUE(create_variant_writer_source(GetParam(), jsons, 1, false, {}, &source, &source_type)
                        .ok());

    SegmentFooterPB footer;
    std::string file_path;
    const std::string rowset_id =
            "shared_array_middle_gap_" + variant_writer_input_name(GetParam());
    ASSERT_TRUE(write_variant_segment(source, source_type, rowset_id, &footer, &file_path).ok());

    const auto* array_meta = find_footer_column_meta_by_relative_path(footer, "arr");
    ASSERT_NE(array_meta, nullptr);
    EXPECT_EQ(array_meta->type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_ARRAY));

    std::vector<std::optional<std::string>> actual;
    ASSERT_TRUE(read_variant_root_rows(footer, file_path, &actual).ok());
    EXPECT_EQ(actual, (std::vector<std::optional<std::string>> {
                              R"({"arr":[1,2]})",
                              "{}",
                              R"({"arr":[3]})",
                      }));
}

TEST_P(VariantWriterCompatibilityTest, dotted_and_nested_paths_follow_writer_semantics) {
    ScopedDuplicateJsonPathCheck duplicate_path_check(false);
    init_variant_tablet(10015 + static_cast<int>(GetParam()), 2);

    const std::vector<std::string> jsons {
            R"({"a.b":1})",
            R"({"a":{"b":2}})",
            R"({"a":{"b":null},"a.b":3})",
    };
    ColumnPtr source;
    DataTypePtr source_type;
    ASSERT_TRUE(create_variant_writer_source(GetParam(), jsons, 2, false, {}, &source, &source_type)
                        .ok());

    SegmentFooterPB footer;
    std::string file_path;
    const std::string rowset_id =
            "shared_cross_row_dotted_" + variant_writer_input_name(GetParam());
    const Status write_status =
            write_variant_segment(source, source_type, rowset_id, &footer, &file_path);
    if (GetParam() == VariantWriterInput::V1) {
        // Preserve the legacy behavior when duplicate-path checking is disabled: V1 does not
        // canonicalize the plain dotted key during parse and rejects the conflicting paths later.
        EXPECT_EQ(write_status.code(), ErrorCode::INVALID_JSON_PATH) << write_status;
        EXPECT_NE(write_status.to_string().find("duplicated entry : a.b"), std::string::npos)
                << write_status;
        return;
    }
    ASSERT_TRUE(write_status.ok()) << write_status;
    VariantStorageParseWriteResult footer_stats;
    collect_variant_footer_stats(footer, 0, &footer_stats);
    EXPECT_EQ(footer_stats.materialized_columns, 1);

    std::vector<std::optional<std::string>> actual;
    ASSERT_TRUE(read_variant_root_rows(footer, file_path, &actual).ok());
    EXPECT_EQ(actual, (std::vector<std::optional<std::string>> {
                              R"({"a":{"b":1}})",
                              R"({"a":{"b":2}})",
                              R"({"a":{"b":3}})",
                      }));
}

TEST_P(VariantWriterCompatibilityTest, dotted_null_path_is_first_when_duplicate_check_enabled) {
    ScopedDuplicateJsonPathCheck duplicate_path_check(true);
    init_variant_tablet(10019 + static_cast<int>(GetParam()), 2);

    const std::vector<std::string> jsons {R"({"a":{"b":null},"a.b":5})"};
    ColumnPtr source;
    DataTypePtr source_type;
    ASSERT_TRUE(create_variant_writer_source(GetParam(), jsons, 2, false, {}, &source, &source_type)
                        .ok());

    SegmentFooterPB footer;
    std::string file_path;
    const std::string rowset_id =
            "shared_dotted_null_first_" + variant_writer_input_name(GetParam());
    ASSERT_TRUE(write_variant_segment(source, source_type, rowset_id, &footer, &file_path).ok());

    std::vector<std::optional<std::string>> actual;
    ASSERT_TRUE(read_variant_root_rows(footer, file_path, &actual).ok());
    EXPECT_EQ(actual, (std::vector<std::optional<std::string>> {"{}"}));
}

TEST_P(VariantWriterCompatibilityTest, nullable_round_trip) {
    init_variant_tablet(10002, 1, false, true);

    const std::vector<std::string> nested_jsons {
            R"({"cold_a":"x","hot":1})",  "{}",  R"({"hot":2})", "[1,2]", "42",
            R"({"cold_b":true,"hot":2})", "null"};
    const std::vector<UInt8> outer_nulls {0, 1, 0, 0, 0, 0, 0};
    auto expected_jsons = nested_jsons;
    // Keep V1's root JSON-null storage behavior while testing it in the same nullable batch as an
    // outer SQL NULL. The writer tracks only the latter in the physical nullable map.
    expected_jsons.back() = "{}";
    ColumnPtr source;
    DataTypePtr source_type;
    ASSERT_TRUE(create_variant_writer_source(GetParam(), nested_jsons, 1, false, outer_nulls,
                                             &source, &source_type)
                        .ok());

    SegmentFooterPB footer;
    std::string file_path;
    uint64_t buffered_bytes = 0;
    const std::string rowset_id = "shared_nullable_" + variant_writer_input_name(GetParam());
    const Status write_status = write_variant_segment(source, source_type, rowset_id, &footer,
                                                      &file_path, 3, &buffered_bytes);
    ASSERT_TRUE(write_status.ok()) << write_status;
    EXPECT_GT(buffered_bytes, nested_jsons.size());

    VariantStorageParseWriteResult footer_stats;
    collect_variant_footer_stats(footer, 0, &footer_stats);
    EXPECT_GT(footer_stats.materialized_columns, 0);
    EXPECT_GT(footer_stats.sparse_columns, 0);

    std::vector<std::optional<std::string>> actual;
    ASSERT_TRUE(read_variant_root_rows(footer, file_path, &actual).ok());
    ASSERT_EQ(actual.size(), nested_jsons.size());
    for (size_t row = 0; row < nested_jsons.size(); ++row) {
        if (outer_nulls[row] != 0) {
            EXPECT_FALSE(actual[row].has_value());
        } else {
            ASSERT_TRUE(actual[row].has_value());
            EXPECT_EQ(*actual[row], expected_jsons[row]);
        }
    }
}

TEST_P(VariantWriterCompatibilityTest, doc_mode_round_trip) {
    constexpr int kDocBuckets = 2;
    init_variant_tablet(10007, 1, false, false, true,
                        /*variant_doc_materialization_min_rows=*/100, kDocBuckets);

    const std::vector<std::string> jsons {
            R"({"alpha":1,"beta":"x"})",
            R"({"alpha":2,"gamma":3})",
            "{}",
    };
    ColumnPtr source;
    DataTypePtr source_type;
    ASSERT_TRUE(create_variant_writer_source(GetParam(), jsons, 1, true, {}, &source, &source_type)
                        .ok());

    SegmentFooterPB footer;
    std::string file_path;
    const std::string rowset_id = "shared_doc_" + variant_writer_input_name(GetParam());
    ASSERT_TRUE(write_variant_segment(source, source_type, rowset_id, &footer, &file_path).ok());

    VariantStorageParseWriteResult footer_stats;
    collect_variant_footer_stats(footer, 0, &footer_stats);
    EXPECT_EQ(footer_stats.doc_value_columns, kDocBuckets);
    EXPECT_EQ(footer_stats.materialized_columns, 0);
    EXPECT_EQ(footer_stats.sparse_columns, 0);

    std::vector<std::optional<std::string>> actual;
    ASSERT_TRUE(read_variant_root_rows(footer, file_path, &actual).ok());
    ASSERT_EQ(actual.size(), jsons.size());
    for (size_t row = 0; row < jsons.size(); ++row) {
        ASSERT_TRUE(actual[row].has_value());
        EXPECT_EQ(*actual[row], jsons[row]);
    }
}

TEST_F(VariantColumnWriterReaderTest, test_write_column_variant_v2_duplicate_dotted_paths) {
    ScopedDuplicateJsonPathCheck duplicate_path_check(true);
    init_variant_tablet(10003, 2);

    const std::vector<std::string> jsons {
            R"({"a.b":1,"a":{"b":2}})",
            R"({"a":{"b":3},"a.b":4})",
            R"({"a.b":null,"a":{"b":5}})",
            R"({"a.b":6})",
            R"({"a":{"b":7}})",
            R"({"a.b":{"c":8},"a":{"b":{"d":9}}})",
    };
    auto source = ColumnVariantV2::create();
    DataTypeVariantV2SerDe serde;
    DataTypeSerDe::FormatOptions serde_options;
    for (const auto& json : jsons) {
        Slice slice(json.data(), json.size());
        ASSERT_TRUE(serde.deserialize_one_cell_from_json(*source, slice, serde_options).ok());
    }

    SegmentFooterPB footer;
    std::string file_path;
    const auto source_type = std::make_shared<DataTypeVariantV2>(2, false);
    const Status write_status = write_variant_segment(source->get_ptr(), source_type,
                                                      "v2_duplicate_paths", &footer, &file_path, 3);
    ASSERT_TRUE(write_status.ok()) << write_status;

    std::vector<std::optional<std::string>> actual;
    ASSERT_TRUE(read_variant_root_rows(footer, file_path, &actual).ok());
    // ColumnVariantV2 stores object entries in metadata field-id order, so the nested form is the
    // first canonical leaf for a dotted-path collision regardless of the source JSON key order.
    const std::vector<std::optional<std::string>> expected {
            R"({"a":{"b":2}})", R"({"a":{"b":3}})", R"({"a":{"b":5}})",
            R"({"a":{"b":6}})", R"({"a":{"b":7}})", R"({"a":{"b":{"c":8,"d":9}}})",
    };
    EXPECT_EQ(actual, expected);
}

TEST_F(VariantColumnWriterReaderTest,
       test_write_column_variant_v2_rejects_duplicate_dotted_paths_without_check) {
    ScopedDuplicateJsonPathCheck duplicate_path_check(false);
    init_variant_tablet(10008, 2);

    auto source = ColumnVariantV2::create();
    DataTypeVariantV2SerDe serde;
    DataTypeSerDe::FormatOptions serde_options;
    constexpr std::string_view json = R"({"a.b":1,"a":{"b":2}})";
    Slice slice(json.data(), json.size());
    ASSERT_TRUE(serde.deserialize_one_cell_from_json(*source, slice, serde_options).ok());

    SegmentFooterPB footer;
    std::string file_path;
    const auto source_type = std::make_shared<DataTypeVariantV2>(2, false);
    const Status status = write_variant_segment(source->get_ptr(), source_type,
                                                "v2_duplicate_paths_rejected", &footer, &file_path);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("may contains duplicated entry"), std::string::npos)
            << status.to_string();
}

TEST_F(VariantColumnWriterReaderTest,
       test_write_column_variant_v2_rejects_deprecated_flatten_nested_layout) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_enable_variant_flatten_nested(true);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "v", 2);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);
    init_tablet_from_current_schema(10017);

    auto source = ColumnVariantV2::create();
    DataTypeVariantV2SerDe serde;
    DataTypeSerDe::FormatOptions serde_options;
    constexpr std::string_view json = R"({"items":[{"v":1},{"v":2}]})";
    Slice slice(json.data(), json.size());
    ASSERT_TRUE(serde.deserialize_one_cell_from_json(*source, slice, serde_options).ok());

    SegmentFooterPB footer;
    std::string file_path;
    const auto source_type = std::make_shared<DataTypeVariantV2>(2, false);
    const Status status = write_variant_segment(source->get_ptr(), source_type,
                                                "v2_flatten_nested_rejected", &footer, &file_path);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("deprecated flatten-nested"), std::string::npos)
            << status.to_string();
}

TEST_F(VariantColumnWriterReaderTest, test_write_column_variant_v2_accounts_root_scratch) {
    init_variant_tablet(10004, 1);

    const std::string large_json = "\"" + std::string(1 << 20, 'x') + "\"";
    const std::vector<std::string> jsons {large_json, "1"};
    auto source = ColumnVariantV2::create();
    DataTypeVariantV2SerDe serde;
    DataTypeSerDe::FormatOptions serde_options;
    for (const auto& json : jsons) {
        Slice slice(json.data(), json.size());
        ASSERT_TRUE(serde.deserialize_one_cell_from_json(*source, slice, serde_options).ok());
    }

    SegmentFooterPB footer;
    std::string file_path;
    uint64_t buffered_bytes = 0;
    const auto source_type = std::make_shared<DataTypeVariantV2>(1, false);
    ASSERT_TRUE(write_variant_segment(source->get_ptr(), source_type, "v2_root_scratch", &footer,
                                      &file_path, 0, &buffered_bytes)
                        .ok());
    EXPECT_GT(buffered_bytes, large_json.size() * 2);

    std::vector<std::optional<std::string>> actual;
    ASSERT_TRUE(read_variant_root_rows(footer, file_path, &actual).ok());
    ASSERT_EQ(actual.size(), jsons.size());
    EXPECT_EQ(actual[0], large_json);
    EXPECT_EQ(actual[1], "1");
}

TEST_F(VariantColumnWriterReaderTest, test_write_column_variant_v2_typed_state) {
    init_variant_tablet(10009, 1);

    auto values = ColumnInt64::create();
    for (Int64 value : {11, 0, 33}) {
        values->insert_value(value);
    }
    auto inner_nulls = ColumnUInt8::create();
    for (UInt8 is_null : {0, 1, 0}) {
        inner_nulls->insert_value(is_null);
    }
    auto source = ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(values), std::move(inner_nulls)),
            std::make_shared<DataTypeInt64>());
    ASSERT_TRUE(source->is_typed());

    SegmentFooterPB footer;
    std::string file_path;
    const auto source_type = std::make_shared<DataTypeVariantV2>(1, false);
    ASSERT_TRUE(write_variant_segment(source->get_ptr(), source_type, "v2_typed_state", &footer,
                                      &file_path, 1)
                        .ok());

    std::vector<std::optional<std::string>> actual;
    const Status read_status = read_variant_root_rows(footer, file_path, &actual);
    ASSERT_TRUE(read_status.ok()) << read_status;
    const std::vector<std::optional<std::string>> expected {"11", "{}", "33"};
    EXPECT_EQ(actual, expected);
}

TEST_F(VariantColumnWriterReaderTest, test_write_column_variant_v2_high_cardinality_is_linear) {
    init_variant_tablet(10005, 1);

    const auto make_source = [](size_t rows, std::vector<std::string>* jsons) {
        auto source = ColumnVariantV2::create();
        DataTypeVariantV2SerDe serde;
        DataTypeSerDe::FormatOptions serde_options;
        jsons->clear();
        jsons->reserve(rows);
        for (size_t row = 0; row < rows; ++row) {
            const std::string value = std::to_string(row);
            jsons->push_back("{\"k_" + value + "\":{\"leaf\":" + value + "},\"shared\":" + value +
                             "}");
            const std::string& json = jsons->back();
            Slice slice(json.data(), json.size());
            EXPECT_TRUE(serde.deserialize_one_cell_from_json(*source, slice, serde_options).ok());
        }
        return source;
    };

    constexpr size_t kRows = 128;
    std::vector<std::string> small_jsons;
    std::vector<std::string> large_jsons;
    auto small_source = make_source(kRows, &small_jsons);
    auto large_source = make_source(kRows * 2, &large_jsons);
    const auto source_type = std::make_shared<DataTypeVariantV2>(1, false);

    SegmentFooterPB small_footer;
    SegmentFooterPB large_footer;
    std::string small_file_path;
    std::string large_file_path;
    uint64_t small_buffered_bytes = 0;
    uint64_t large_buffered_bytes = 0;
    ASSERT_TRUE(write_variant_segment(small_source->get_ptr(), source_type,
                                      "v2_high_cardinality_small", &small_footer, &small_file_path,
                                      0, &small_buffered_bytes)
                        .ok());
    ASSERT_TRUE(write_variant_segment(large_source->get_ptr(), source_type,
                                      "v2_high_cardinality_large", &large_footer, &large_file_path,
                                      0, &large_buffered_bytes)
                        .ok());
    EXPECT_GT(large_buffered_bytes, small_buffered_bytes);
    EXPECT_LT(large_buffered_bytes, small_buffered_bytes * 3)
            << "N=" << small_buffered_bytes << ", 2N=" << large_buffered_bytes;

    std::vector<std::optional<std::string>> actual;
    ASSERT_TRUE(read_variant_root_rows(large_footer, large_file_path, &actual).ok());
    ASSERT_EQ(actual.size(), large_jsons.size());
    for (size_t row = 0; row < large_jsons.size(); ++row) {
        ASSERT_TRUE(actual[row].has_value());
        EXPECT_EQ(*actual[row], large_jsons[row]);
    }
}

TEST_F(VariantColumnWriterReaderTest, test_write_column_variant_v2_doc_empty_key) {
    init_variant_tablet(10006, 1, false, false, true);

    const std::vector<std::string> jsons {R"({"":1})", R"({"":2,"normal":3})"};
    auto source = ColumnVariantV2::create();
    DataTypeVariantV2SerDe serde;
    DataTypeSerDe::FormatOptions serde_options;
    for (const auto& json : jsons) {
        Slice slice(json.data(), json.size());
        ASSERT_TRUE(serde.deserialize_one_cell_from_json(*source, slice, serde_options).ok());
    }

    SegmentFooterPB footer;
    std::string file_path;
    const auto source_type = std::make_shared<DataTypeVariantV2>(1, false);
    ASSERT_TRUE(write_variant_segment(source->get_ptr(), source_type, "v2_doc_empty_key", &footer,
                                      &file_path)
                        .ok());
    VariantStorageParseWriteResult footer_stats;
    collect_variant_footer_stats(footer, 0, &footer_stats);
    EXPECT_GT(footer_stats.doc_value_columns, 0);

    std::vector<std::optional<std::string>> actual;
    ASSERT_TRUE(read_variant_root_rows(footer, file_path, &actual).ok());
    ASSERT_EQ(actual.size(), jsons.size());
    for (size_t row = 0; row < jsons.size(); ++row) {
        ASSERT_TRUE(actual[row].has_value());
        EXPECT_EQ(*actual[row], jsons[row]);
    }
}

void check_column_meta(const ColumnMetaPB& column_meta, auto& path_with_size) {
    EXPECT_TRUE(column_meta.has_column_path_info());
    auto path = std::make_shared<PathInData>();
    path->from_protobuf(column_meta.column_path_info());
    EXPECT_EQ(column_meta.column_path_info().parrent_column_unique_id(), 1);
    EXPECT_EQ(column_meta.none_null_size(), path_with_size[path->copy_pop_front().get_path()]);
}

void check_sparse_column_meta(const ColumnMetaPB& column_meta, auto& path_with_size) {
    EXPECT_TRUE(column_meta.has_column_path_info());
    auto path = std::make_shared<PathInData>();
    path->from_protobuf(column_meta.column_path_info());
    EXPECT_EQ(column_meta.column_path_info().parrent_column_unique_id(), 1);
    for (const auto& [pat, size] : column_meta.variant_statistics().sparse_column_non_null_size()) {
        EXPECT_EQ(size, path_with_size[pat]);
    }
    auto base_path = path->copy_pop_front().get_path();
    EXPECT_TRUE(base_path == "__DORIS_VARIANT_SPARSE__" ||
                base_path.rfind("__DORIS_VARIANT_SPARSE__.b", 0) == 0);
}

static const ColumnMetaPB* find_footer_column_meta_by_relative_path(
        const SegmentFooterPB& footer, std::string_view relative_path) {
    for (int i = 0; i < footer.columns_size(); ++i) {
        const auto& column_meta = footer.columns(i);
        if (!column_meta.has_column_path_info()) {
            continue;
        }
        PathInData path;
        path.from_protobuf(column_meta.column_path_info());
        if (path.copy_pop_front().get_path() == relative_path) {
            return &column_meta;
        }
    }
    return nullptr;
}

static TabletColumn make_int_typed_path_template(
        std::string_view path, PatternTypePB pattern_type = PatternTypePB::MATCH_NAME) {
    ColumnPB column_pb;
    column_pb.set_unique_id(-1);
    column_pb.set_name(std::string(path));
    column_pb.set_type("INT");
    column_pb.set_is_nullable(true);
    column_pb.set_pattern_type(pattern_type);

    TabletColumn column;
    column.init_from_pb(column_pb);
    return column;
}

static TabletColumn make_string_typed_path_template(std::string_view path) {
    ColumnPB column_pb;
    column_pb.set_unique_id(-1);
    column_pb.set_name(std::string(path));
    column_pb.set_type("STRING");
    column_pb.set_is_nullable(true);
    column_pb.set_pattern_type(PatternTypePB::MATCH_NAME);

    TabletColumn column;
    column.init_from_pb(column_pb);
    return column;
}

TEST_P(VariantWriterCompatibilityTest, typed_path_and_sparse_round_trip) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1",
                     /*variant_max_subcolumns_count=*/1);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);
    auto typed_path = make_int_typed_path_template("typed_i");
    _tablet_schema->mutable_column_by_uid(1).add_sub_column(typed_path);
    init_tablet_from_current_schema(10010);

    const std::vector<std::string> jsons {
            R"({"cold0":100,"hot":"a","typed_i":1})",
            R"({"cold1":101,"hot":"b","typed_i":2})",
            R"({"cold2":102,"hot":"c"})",
    };
    ColumnPtr source;
    DataTypePtr source_type;
    ASSERT_TRUE(create_variant_writer_source(GetParam(), jsons, 1, false, {}, &source, &source_type)
                        .ok());

    SegmentFooterPB footer;
    std::string file_path;
    const std::string rowset_id = "shared_typed_sparse_" + variant_writer_input_name(GetParam());
    ASSERT_TRUE(write_variant_segment(source, source_type, rowset_id, &footer, &file_path).ok());

    const auto* typed_meta = find_footer_column_meta_by_relative_path(footer, "typed_i");
    ASSERT_NE(typed_meta, nullptr);
    EXPECT_EQ(typed_meta->type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_INT));
    EXPECT_TRUE(typed_meta->is_nullable());
    EXPECT_FALSE(typed_meta->has_none_null_size());
    const auto* hot_meta = find_footer_column_meta_by_relative_path(footer, "hot");
    ASSERT_NE(hot_meta, nullptr);
    EXPECT_EQ(hot_meta->none_null_size(), jsons.size());

    VariantStorageParseWriteResult footer_stats;
    collect_variant_footer_stats(footer, 0, &footer_stats);
    EXPECT_GE(footer_stats.materialized_columns, 2);
    EXPECT_GT(footer_stats.sparse_columns, 0);

    std::vector<std::string> typed_values;
    ASSERT_TRUE(read_variant_path_rows(footer, file_path, "typed_i", FieldType::OLAP_FIELD_TYPE_INT,
                                       &typed_values)
                        .ok());
    EXPECT_EQ(typed_values, (std::vector<std::string> {"1", "2", "NULL"}));

    std::vector<std::optional<std::string>> actual;
    ASSERT_TRUE(read_variant_root_rows(footer, file_path, &actual).ok());
    ASSERT_EQ(actual.size(), jsons.size());
    for (size_t row = 0; row < jsons.size(); ++row) {
        ASSERT_TRUE(actual[row].has_value());
        EXPECT_EQ(*actual[row], jsons[row]);
    }
}

TEST_F(VariantColumnWriterReaderTest, v2_empty_typed_path_keeps_following_converter_column_id) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "v",
                     /*variant_max_subcolumns_count=*/1);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);
    auto all_cast_null = make_int_typed_path_template("all_cast_null");
    auto good = make_int_typed_path_template("good");
    _tablet_schema->mutable_column_by_uid(1).add_sub_column(all_cast_null);
    _tablet_schema->mutable_column_by_uid(1).add_sub_column(good);
    init_tablet_from_current_schema(11004);

    // Path ordering writes all_cast_null first. Its forced INT cast removes every compact value;
    // good then verifies that the next physical column still uses the matching convertor slot.
    const std::vector<std::string> jsons {
            R"({"all_cast_null":"bad","good":1})",
            R"({"all_cast_null":"still_bad","good":2})",
    };
    ColumnPtr source;
    DataTypePtr source_type;
    ASSERT_TRUE(create_variant_writer_source(VariantWriterInput::V2, jsons, 1, false, {}, &source,
                                             &source_type)
                        .ok());

    SegmentFooterPB footer;
    std::string file_path;
    ASSERT_TRUE(
            write_variant_segment(source, source_type, "v2_empty_typed_path", &footer, &file_path)
                    .ok());
    ASSERT_NE(find_footer_column_meta_by_relative_path(footer, "all_cast_null"), nullptr);
    ASSERT_NE(find_footer_column_meta_by_relative_path(footer, "good"), nullptr);

    std::vector<std::string> cast_null_values;
    ASSERT_TRUE(read_variant_path_rows(footer, file_path, "all_cast_null",
                                       FieldType::OLAP_FIELD_TYPE_INT, &cast_null_values)
                        .ok());
    EXPECT_EQ(cast_null_values, (std::vector<std::string> {"NULL", "NULL"}));

    std::vector<std::string> good_values;
    ASSERT_TRUE(read_variant_path_rows(footer, file_path, "good", FieldType::OLAP_FIELD_TYPE_INT,
                                       &good_values)
                        .ok());
    EXPECT_EQ(good_values, (std::vector<std::string> {"1", "2"}));
}

TEST_P(VariantWriterCompatibilityTest, typed_string_path_preserves_object_descendants) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "v",
                     /*variant_max_subcolumns_count=*/1);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);
    auto typed_path = make_string_typed_path_template("payload");
    _tablet_schema->mutable_column_by_uid(1).add_sub_column(typed_path);
    init_tablet_from_current_schema(10013 + static_cast<int>(GetParam()));

    const std::vector<std::string> jsons {
            R"({"payload":"x"})",
            R"({"payload":1})",
            R"({"payload":{"a":1}})",
    };
    ColumnPtr source;
    DataTypePtr source_type;
    ASSERT_TRUE(create_variant_writer_source(GetParam(), jsons, 1, false, {}, &source, &source_type)
                        .ok());

    SegmentFooterPB footer;
    std::string file_path;
    const std::string rowset_id =
            "shared_typed_string_object_" + variant_writer_input_name(GetParam());
    ASSERT_TRUE(write_variant_segment(source, source_type, rowset_id, &footer, &file_path).ok());

    const auto* typed_meta = find_footer_column_meta_by_relative_path(footer, "payload");
    ASSERT_NE(typed_meta, nullptr);
    EXPECT_EQ(typed_meta->type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_STRING));
    const auto* descendant_meta = find_footer_column_meta_by_relative_path(footer, "payload.a");
    ASSERT_NE(descendant_meta, nullptr);
    EXPECT_EQ(descendant_meta->type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_BIGINT));

    std::vector<std::optional<std::string>> actual;
    ASSERT_TRUE(read_variant_root_rows(footer, file_path, &actual).ok());
    EXPECT_EQ(actual, (std::vector<std::optional<std::string>> {
                              R"({"payload":"x"})",
                              R"({"payload":"1"})",
                              R"({"payload":{"a":1}})",
                      }));
}

TEST_P(VariantWriterCompatibilityTest, secondary_indexes_are_written) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "v",
                     /*variant_max_subcolumns_count=*/1);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    auto& variant_column = _tablet_schema->mutable_column_by_uid(1);
    auto hot_path = make_string_typed_path_template("hot");
    variant_column.add_sub_column(hot_path);
    variant_column.set_is_bf_column(true);
    TabletIndexPB index_pb;
    construct_tablet_index(&index_pb, 10011, "idx_v_hot", variant_column.unique_id());
    (*index_pb.mutable_properties())["field_pattern"] = "hot";
    TabletIndex hot_index;
    hot_index.init_from_pb(index_pb);
    _tablet_schema->append_index(std::move(hot_index));
    init_tablet_from_current_schema(10011 + static_cast<int>(GetParam()));

    const std::vector<std::string> jsons {
            R"({"cold_a":1,"hot":"alpha"})",
            R"({"cold_b":2,"hot":"beta"})",
            R"({"cold_c":3,"hot":"gamma"})",
    };
    ColumnPtr source;
    DataTypePtr source_type;
    ASSERT_TRUE(create_variant_writer_source(GetParam(), jsons, 1, false, {}, &source, &source_type)
                        .ok());

    SegmentFooterPB footer;
    std::string file_path;
    const std::string rowset_id =
            "shared_secondary_indexes_" + variant_writer_input_name(GetParam());
    ASSERT_TRUE(write_variant_segment(source, source_type, rowset_id, &footer, &file_path,
                                      /*first_batch_rows=*/0, /*buffered_bytes=*/nullptr,
                                      VariantIndexWritePolicy::BLOOM_AND_INVERTED)
                        .ok());

    const auto* hot_meta = find_footer_column_meta_by_relative_path(footer, "hot");
    ASSERT_NE(hot_meta, nullptr);
    EXPECT_TRUE(std::ranges::any_of(hot_meta->indexes(), [](const ColumnIndexMetaPB& index) {
        return index.type() == BLOOM_FILTER_INDEX;
    }));

    const std::string index_path_prefix =
            std::string(segment_v2::InvertedIndexDescriptor::get_index_file_path_prefix(file_path));
    const std::string index_file_path =
            segment_v2::InvertedIndexDescriptor::get_index_file_path_v2(index_path_prefix);
    int64_t index_file_size = 0;
    ASSERT_TRUE(io::global_local_filesystem()->file_size(index_file_path, &index_file_size).ok());
    EXPECT_GT(index_file_size, 0);
}

INSTANTIATE_TEST_SUITE_P(V1AndV2, VariantWriterCompatibilityTest,
                         testing::Values(VariantWriterInput::V1, VariantWriterInput::V2),
                         variant_writer_test_name);

class VariantSpecializedWriterCompatibilityTest
        : public VariantColumnWriterReaderTest,
          public testing::WithParamInterface<VariantWriterInput> {};

TEST_P(VariantSpecializedWriterCompatibilityTest,
       extracted_predefined_type_round_trip_and_rejects_mixed_input) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "v", 1);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);
    auto typed_path = make_int_typed_path_template("typed_i");
    _tablet_schema->mutable_column_by_uid(1).add_sub_column(typed_path);

    const TabletColumn& parent_column = _tablet_schema->column_by_uid(1);
    TabletColumn extracted_column;
    extracted_column.set_name(parent_column.name_lower_case() + ".typed_i");
    extracted_column.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    extracted_column.set_parent_unique_id(parent_column.unique_id());
    extracted_column.set_path_info(PathInData(parent_column.name_lower_case() + ".typed_i", true));
    extracted_column.set_variant_max_subcolumns_count(0);
    extracted_column.set_is_nullable(true);
    _tablet_schema->append_column(extracted_column);
    init_tablet_from_current_schema(11000 + static_cast<int>(GetParam()));

    const std::string rowset_id = "specialized_extracted_" + variant_writer_input_name(GetParam());
    const std::string file_path = local_segment_path(_tablet->tablet_path(), rowset_id, 0);
    static_cast<void>(io::global_local_filesystem()->delete_file(file_path));
    io::FileWriterPtr file_writer;
    ASSERT_TRUE(io::global_local_filesystem()->create_file(file_path, &file_writer).ok());

    SegmentFooterPB footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_COMPACTION;
    rowset_ctx.tablet_schema = _tablet_schema;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    opts.rowset_ctx = &rowset_ctx;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    TabletColumn writer_column = _tablet_schema->column(1);
    _init_column_meta(opts.meta, 0, writer_column, opts);

    std::unique_ptr<ColumnWriter> writer;
    ASSERT_TRUE(ColumnWriter::create(opts, &writer_column, file_writer.get(), &writer).ok());
    ASSERT_NE(dynamic_cast<segment_v2::VariantSubcolumnWriter*>(writer.get()), nullptr);
    ASSERT_TRUE(writer->init().ok());

    ColumnPtr source;
    DataTypePtr source_type;
    ASSERT_TRUE(create_typed_int_extracted_source(GetParam(), &source, &source_type).ok());
    const VariantWriterInput other_input =
            GetParam() == VariantWriterInput::V1 ? VariantWriterInput::V2 : VariantWriterInput::V1;
    ColumnPtr other_source;
    DataTypePtr other_source_type;
    ASSERT_TRUE(
            create_typed_int_extracted_source(other_input, &other_source, &other_source_type).ok());

    const auto append_range = [&](const ColumnPtr& batch_source, const DataTypePtr& batch_type,
                                  size_t row_pos, size_t num_rows) -> Status {
        auto converter = std::make_unique<OlapBlockDataConvertor>();
        converter->add_column_data_convertor(writer_column);
        RETURN_IF_ERROR(converter->set_source_content_with_specifid_column(
                {batch_source, batch_type, writer_column.name()}, row_pos, num_rows, 0));
        auto [status, accessor] = converter->convert_column_data(0);
        RETURN_IF_ERROR(status);
        DORIS_CHECK(accessor != nullptr);
        return writer->append(accessor->get_nullmap(), accessor->get_data(), num_rows);
    };

    const Status first_append_status = append_range(source, source_type, 0, 2);
    ASSERT_TRUE(first_append_status.ok()) << first_append_status;
    EXPECT_EQ(writer->get_next_rowid(), 2);
    const Status mixed_status = append_range(other_source, other_source_type, 0, 1);
    EXPECT_FALSE(mixed_status.ok());
    EXPECT_NE(mixed_status.to_string().find("representation changed within one segment"),
              std::string::npos);
    EXPECT_EQ(writer->get_next_rowid(), 2);
    const Status second_append_status = append_range(source, source_type, 2, 2);
    ASSERT_TRUE(second_append_status.ok()) << second_append_status;
    EXPECT_EQ(writer->get_next_rowid(), 4);

    ASSERT_TRUE(writer->finish().ok());
    ASSERT_TRUE(writer->write_data().ok());
    ASSERT_TRUE(writer->write_ordinal_index().ok());
    ASSERT_TRUE(writer->write_zone_map().ok());
    ASSERT_TRUE(file_writer->close().ok());
    footer.set_num_rows(4);

    ASSERT_EQ(footer.columns_size(), 1);
    EXPECT_EQ(footer.columns(0).type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_INT));
    EXPECT_TRUE(footer.columns(0).is_nullable());
    EXPECT_FALSE(footer.columns(0).has_none_null_size());

    io::FileReaderSPtr file_reader;
    ASSERT_TRUE(io::global_local_filesystem()->open_file(file_path, &file_reader).ok());
    ColumnReaderOptions reader_options;
    reader_options.kept_in_memory = false;
    reader_options.be_exec_version = BeExecVersionManager::get_newest_version();
    reader_options.tablet_schema = _tablet_schema;
    std::shared_ptr<ColumnReader> reader;
    ASSERT_TRUE(ColumnReader::create(reader_options, footer.columns(0), footer.num_rows(),
                                     file_reader, &reader)
                        .ok());

    TabletColumn read_column;
    read_column.set_name(writer_column.name());
    read_column.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    read_column.set_is_nullable(true);
    read_column.set_parent_unique_id(parent_column.unique_id());
    read_column.set_path_info(*writer_column.path_info_ptr());
    ColumnIteratorUPtr iterator;
    ASSERT_TRUE(reader->new_iterator(&iterator, &read_column).ok());
    OlapReaderStatistics stats;
    ColumnIteratorOptions iterator_options;
    iterator_options.file_reader = file_reader.get();
    iterator_options.stats = &stats;
    ASSERT_TRUE(iterator->init(iterator_options).ok());
    ASSERT_TRUE(iterator->seek_to_ordinal(0).ok());
    auto read_type = make_nullable(std::make_shared<DataTypeInt32>());
    MutableColumnPtr result = read_type->create_column();
    size_t num_rows = 4;
    ASSERT_TRUE(iterator->next_batch(&num_rows, result).ok());
    ASSERT_EQ(num_rows, 4);
    std::vector<std::string> actual;
    for (size_t row = 0; row < num_rows; ++row) {
        actual.push_back(read_type->to_string(*result, row));
    }
    EXPECT_EQ(actual, (std::vector<std::string> {"1", "NULL", "NULL", "4"}));
}

TEST_F(VariantColumnWriterReaderTest,
       v2_extracted_subcolumn_writer_keeps_whole_values_across_batches) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "v", 1);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);
    const TabletColumn& parent_column = _tablet_schema->column_by_uid(1);
    TabletColumn extracted_column;
    extracted_column.set_name(parent_column.name_lower_case() + ".payload");
    extracted_column.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    extracted_column.set_parent_unique_id(parent_column.unique_id());
    extracted_column.set_path_info(PathInData(parent_column.name_lower_case() + ".payload"));
    extracted_column.set_is_nullable(true);
    _tablet_schema->append_column(extracted_column);
    init_tablet_from_current_schema(11101);

    const std::vector<std::string> jsons {
            R"({"nested":1})", "[1,2]", "7", "null", "{}", "9",
    };
    const std::vector<UInt8> outer_nulls {0, 0, 0, 0, 0, 1};
    ColumnPtr source;
    DataTypePtr source_type;
    ASSERT_TRUE(create_variant_writer_source(VariantWriterInput::V2, jsons, 0, false, outer_nulls,
                                             &source, &source_type)
                        .ok());

    SegmentFooterPB footer;
    std::string file_path;
    ASSERT_TRUE(write_extracted_variant_segment(source, source_type, "v2_extracted_whole_values",
                                                &footer, &file_path,
                                                /*first_batch_rows=*/3)
                        .ok());
    ASSERT_EQ(footer.columns_size(), 1);
    EXPECT_EQ(footer.columns(0).type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_JSONB));

    std::vector<std::string> actual;
    ASSERT_TRUE(read_extracted_variant_rows(footer, file_path, &actual).ok());
    EXPECT_EQ(actual, (std::vector<std::string> {
                              R"({"nested":1})",
                              "[1,2]",
                              "7",
                              "NULL",
                              "{}",
                              "NULL",
                      }));
}

TEST_F(VariantColumnWriterReaderTest, v2_root_only_preserves_layout_validation) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "v", 1, false, false, 0, false, 0, 0,
                     true /* variant_enable_nested_group */);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);
    const TabletColumn& parent_column = _tablet_schema->column_by_uid(1);
    TabletColumn extracted_column;
    extracted_column.set_name(parent_column.name_lower_case() + ".payload");
    extracted_column.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    extracted_column.set_parent_unique_id(parent_column.unique_id());
    extracted_column.set_path_info(PathInData(parent_column.name_lower_case() + ".payload"));
    extracted_column.set_is_nullable(true);
    _tablet_schema->append_column(extracted_column);

    RowsetWriterContext rowset_ctx;
    rowset_ctx.tablet_schema = _tablet_schema;
    ColumnWriterOptions opts;
    opts.rowset_ctx = &rowset_ctx;
    segment_v2::VariantV2ColumnWriter writer(std::move(opts), &parent_column);
    const Status status = writer.init();
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("does not support nested-group layout"), std::string::npos);
}

TEST_F(VariantColumnWriterReaderTest, v2_extracted_subcolumn_writer_preserves_layout_validation) {
    struct LayoutCase {
        bool nested_group;
        bool deprecated_flatten_nested;
        int64_t tablet_id;
        std::string_view rowset_id;
        std::string_view expected_error;
    };
    constexpr std::array<LayoutCase, 2> CASES {{
            {.nested_group = true,
             .deprecated_flatten_nested = false,
             .tablet_id = 11104,
             .rowset_id = "v2_extracted_nested_group_rejected",
             .expected_error = "does not support nested-group layout"},
            {.nested_group = false,
             .deprecated_flatten_nested = true,
             .tablet_id = 11105,
             .rowset_id = "v2_extracted_flatten_nested_rejected",
             .expected_error = "deprecated flatten-nested"},
    }};

    for (const auto& test_case : CASES) {
        SCOPED_TRACE(test_case.rowset_id);
        const Status status = try_write_extracted_variant_with_layout(
                test_case.nested_group, test_case.deprecated_flatten_nested, test_case.tablet_id,
                test_case.rowset_id);
        ASSERT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find(test_case.expected_error), std::string::npos)
                << status.to_string();
    }
}

TEST_F(VariantColumnWriterReaderTest, v2_extracted_subcolumn_writer_all_null) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "v", 1);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);
    const TabletColumn& parent_column = _tablet_schema->column_by_uid(1);
    TabletColumn extracted_column;
    extracted_column.set_name(parent_column.name_lower_case() + ".payload");
    extracted_column.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    extracted_column.set_parent_unique_id(parent_column.unique_id());
    extracted_column.set_path_info(PathInData(parent_column.name_lower_case() + ".payload"));
    extracted_column.set_is_nullable(true);
    _tablet_schema->append_column(extracted_column);
    init_tablet_from_current_schema(11102);

    const std::vector<std::string> jsons {R"({"ignored":1})", "null", "null"};
    const std::vector<UInt8> outer_nulls {1, 0, 1};
    ColumnPtr source;
    DataTypePtr source_type;
    ASSERT_TRUE(create_variant_writer_source(VariantWriterInput::V2, jsons, 0, false, outer_nulls,
                                             &source, &source_type)
                        .ok());

    SegmentFooterPB footer;
    std::string file_path;
    ASSERT_TRUE(write_extracted_variant_segment(source, source_type, "v2_extracted_all_null",
                                                &footer, &file_path,
                                                /*first_batch_rows=*/1)
                        .ok());
    ASSERT_EQ(footer.columns_size(), 1);
    EXPECT_EQ(footer.columns(0).type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_TINYINT));

    std::vector<std::string> actual;
    ASSERT_TRUE(read_extracted_variant_rows(footer, file_path, &actual).ok());
    EXPECT_EQ(actual, (std::vector<std::string> {"NULL", "NULL", "NULL"}));
}

TEST_F(VariantColumnWriterReaderTest,
       v2_root_with_extracted_columns_round_trips_without_unwritten_columns) {
    init_variant_tablet(11002, 1, false, true);
    const TabletColumn& parent_column = _tablet_schema->column_by_uid(1);
    TabletColumn extracted_column;
    extracted_column.set_name(parent_column.name_lower_case() + ".hot");
    extracted_column.set_type(FieldType::OLAP_FIELD_TYPE_BIGINT);
    extracted_column.set_parent_unique_id(parent_column.unique_id());
    extracted_column.set_path_info(PathInData(parent_column.name_lower_case() + ".hot"));
    extracted_column.set_is_nullable(true);
    _tablet_schema->append_column(extracted_column);
    init_tablet_from_current_schema(11003);

    const std::vector<std::string> encoded_jsons {
            R"({"cold_a":1,"hot":10})", "7", "[1,2]", "null", R"({"cold_b":2,"hot":20})", "[3]",
    };
    const std::vector<UInt8> encoded_outer_nulls {0, 0, 0, 0, 1, 1};
    ColumnPtr encoded_source;
    DataTypePtr encoded_source_type;
    ASSERT_TRUE(create_variant_writer_source(VariantWriterInput::V2, encoded_jsons, 1, false,
                                             encoded_outer_nulls, &encoded_source,
                                             &encoded_source_type)
                        .ok());

    SegmentFooterPB encoded_footer;
    std::string encoded_file_path;
    ASSERT_TRUE(write_variant_segment(encoded_source, encoded_source_type, "v2_extracted_root_e",
                                      &encoded_footer, &encoded_file_path,
                                      /*first_batch_rows=*/4)
                        .ok());

    ASSERT_EQ(encoded_footer.columns_size(), 1);
    EXPECT_EQ(encoded_footer.num_rows(), encoded_jsons.size());
    EXPECT_EQ(encoded_footer.columns(0).type(),
              static_cast<int>(FieldType::OLAP_FIELD_TYPE_VARIANT));
    EXPECT_TRUE(encoded_footer.columns(0).is_nullable());
    EXPECT_FALSE(encoded_footer.columns(0).has_variant_statistics());
    VariantStorageParseWriteResult encoded_footer_stats;
    collect_variant_footer_stats(encoded_footer, 0, &encoded_footer_stats);
    EXPECT_EQ(encoded_footer_stats.materialized_columns, 0);
    EXPECT_EQ(encoded_footer_stats.sparse_columns, 0);
    EXPECT_EQ(encoded_footer_stats.doc_value_columns, 0);

    std::vector<std::optional<std::string>> encoded_rows;
    ASSERT_TRUE(read_variant_root_rows(encoded_footer, encoded_file_path, &encoded_rows).ok());
    EXPECT_EQ(encoded_rows, (std::vector<std::optional<std::string>> {
                                    "{}",
                                    "7",
                                    "[1,2]",
                                    "{}",
                                    std::nullopt,
                                    std::nullopt,
                            }));

    ColumnPtr typed_source;
    DataTypePtr typed_source_type;
    ASSERT_TRUE(create_typed_int_extracted_source(VariantWriterInput::V2, &typed_source,
                                                  &typed_source_type)
                        .ok());
    const auto& typed_nullable = assert_cast<const ColumnNullable&>(*typed_source);
    EXPECT_TRUE(assert_cast<const ColumnVariantV2&>(typed_nullable.get_nested_column()).is_typed());

    SegmentFooterPB typed_footer;
    std::string typed_file_path;
    ASSERT_TRUE(write_variant_segment(typed_source, typed_source_type, "v2_extracted_root_t",
                                      &typed_footer, &typed_file_path,
                                      /*first_batch_rows=*/2)
                        .ok());

    ASSERT_EQ(typed_footer.columns_size(), 1);
    EXPECT_EQ(typed_footer.num_rows(), typed_source->size());
    EXPECT_FALSE(typed_footer.columns(0).has_variant_statistics());
    VariantStorageParseWriteResult typed_footer_stats;
    collect_variant_footer_stats(typed_footer, 0, &typed_footer_stats);
    EXPECT_EQ(typed_footer_stats.materialized_columns, 0);
    EXPECT_EQ(typed_footer_stats.sparse_columns, 0);
    EXPECT_EQ(typed_footer_stats.doc_value_columns, 0);

    std::vector<std::optional<std::string>> typed_rows;
    ASSERT_TRUE(read_variant_root_rows(typed_footer, typed_file_path, &typed_rows).ok());
    EXPECT_EQ(typed_rows, (std::vector<std::optional<std::string>> {
                                  "1",
                                  "{}",
                                  std::nullopt,
                                  "4",
                          }));
}

TEST_F(VariantColumnWriterReaderTest, v2_shredder_uses_only_rows_in_requested_range) {
    DataTypeVariantV2SerDe serde;
    DataTypeSerDe::FormatOptions format_options;
    const auto make_source = [&](std::string_view json) {
        auto source = ColumnVariantV2::create();
        Slice slice(json.data(), json.size());
        EXPECT_TRUE(serde.deserialize_one_cell_from_json(*source, slice, format_options).ok());
        return source;
    };
    auto first = make_source(R"({"first":1})");
    auto second = make_source(R"({"second":2})");
    auto combined = ColumnVariantV2::create();
    combined->insert_range_from(*first, 0, 1);
    combined->insert_range_from(*second, 0, 1);
    ASSERT_EQ(combined->read_view().metadata_count(), 2);

    segment_v2::VariantShredder shredder({
            .max_subcolumns_count = 1,
            .sparse_bucket_count = 1,
            .check_duplicate_json_path = config::variant_enable_duplicate_json_path_check,
    });
    ASSERT_TRUE(shredder.append(combined->read_view(), 1, 1).ok());
    segment_v2::VariantShreddedColumns shredded;
    ASSERT_TRUE(shredder.finish(&shredded).ok());
    ASSERT_EQ(shredded.materialized.size(), 1);
    EXPECT_EQ(shredded.materialized.front().path.get_path(), "second");
    EXPECT_EQ(shredded.materialized.front().rowids, (DorisVector<uint32_t> {0}));
}

TEST_F(VariantColumnWriterReaderTest,
       v2_extracted_subcolumn_writer_applies_predefined_string_type) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "v", 1);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);
    auto typed_path = make_string_typed_path_template("payload");
    _tablet_schema->mutable_column_by_uid(1).add_sub_column(typed_path);
    const TabletColumn& parent_column = _tablet_schema->column_by_uid(1);
    TabletColumn extracted_column;
    extracted_column.set_name(parent_column.name_lower_case() + ".payload");
    extracted_column.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    extracted_column.set_parent_unique_id(parent_column.unique_id());
    extracted_column.set_path_info(PathInData(parent_column.name_lower_case() + ".payload", true));
    extracted_column.set_is_nullable(true);
    _tablet_schema->append_column(extracted_column);
    init_tablet_from_current_schema(11103);

    auto source = ColumnVariantV2::create();
    DataTypeVariantV2SerDe serde;
    DataTypeSerDe::FormatOptions format_options;
    for (const std::string_view json : {R"({"nested":1})", R"([1,2])"}) {
        Slice slice(json.data(), json.size());
        ASSERT_TRUE(serde.deserialize_one_cell_from_json(*source, slice, format_options).ok());
    }
    const auto source_type = std::make_shared<DataTypeVariantV2>(0, false);
    SegmentFooterPB footer;
    std::string file_path;
    ASSERT_TRUE(write_extracted_variant_segment(source->get_ptr(), source_type,
                                                "v2_extracted_predefined_string", &footer,
                                                &file_path,
                                                /*first_batch_rows=*/1)
                        .ok());
    ASSERT_EQ(footer.columns_size(), 1);
    EXPECT_EQ(footer.columns(0).type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_STRING));

    std::vector<std::string> actual;
    ASSERT_TRUE(read_extracted_variant_rows(footer, file_path, &actual).ok());
    EXPECT_EQ(actual, (std::vector<std::string> {R"({"nested":1})", "[1,2]"}));
}

TEST_F(VariantColumnWriterReaderTest, v2_shredder_drops_typed_cast_null_from_sparse_rows) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    auto* root_pb = schema_pb.add_column();
    construct_column(root_pb, 1, "VARIANT", "v", 1);
    root_pb->set_variant_enable_typed_paths_to_sparse(true);
    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(schema_pb);
    auto typed_path = make_int_typed_path_template("bad");
    tablet_schema->mutable_column_by_uid(1).add_sub_column(typed_path);

    auto source = ColumnVariantV2::create();
    DataTypeVariantV2SerDe serde;
    DataTypeSerDe::FormatOptions format_options;
    for (const std::string_view json : {R"({"bad":"not_int","hot":1})", R"({"bad":7,"hot":2})"}) {
        Slice slice(json.data(), json.size());
        ASSERT_TRUE(serde.deserialize_one_cell_from_json(*source, slice, format_options).ok());
    }

    segment_v2::VariantShredder shredder({
            .tablet_schema = tablet_schema.get(),
            .parent_column_unique_id = 1,
            .max_subcolumns_count = 1,
            .typed_paths_to_sparse = true,
            .sparse_bucket_count = 1,
            .check_duplicate_json_path = config::variant_enable_duplicate_json_path_check,
    });
    ASSERT_TRUE(shredder.append(source->read_view(), 0, source->size()).ok());
    segment_v2::VariantShreddedColumns shredded;
    ASSERT_TRUE(shredder.finish(&shredded).ok());

    ASSERT_EQ(shredded.binary_buckets.size(), 1);
    const auto& sparse = assert_cast<const ColumnMap&>(*shredded.binary_buckets[0].column);
    const auto& keys = assert_cast<const ColumnString&>(sparse.get_keys());
    ASSERT_EQ(keys.size(), 1);
    EXPECT_EQ(keys.get_data_at(0).to_string(), "bad");
    ASSERT_EQ(sparse.get_offsets().size(), 2);
    EXPECT_EQ(sparse.get_offsets()[0], 0);
    EXPECT_EQ(sparse.get_offsets()[1], 1);
    ASSERT_TRUE(shredded.statistics.sparse_column_non_null_size.contains("bad"));
    EXPECT_EQ(shredded.statistics.sparse_column_non_null_size.at("bad"), 1);
}

static TabletColumn make_jsonb_array_typed_path_template(std::string_view path) {
    ColumnPB column_pb;
    column_pb.set_unique_id(-1);
    column_pb.set_name(std::string(path));
    column_pb.set_type("ARRAY");
    column_pb.set_is_nullable(true);
    column_pb.set_pattern_type(PatternTypePB::MATCH_NAME);

    auto* item_pb = column_pb.add_children_columns();
    item_pb->set_unique_id(-1);
    item_pb->set_name(std::string(path) + ".item");
    item_pb->set_type("JSONB");
    item_pb->set_is_nullable(true);

    TabletColumn column;
    column.init_from_pb(column_pb);
    return column;
}

static void fill_variant_column_with_doc_value_only(
        MutableColumnPtr& column_object, int num_rows,
        std::unordered_map<int, std::string>* inserted) {
    auto type_string = std::make_shared<DataTypeString>();
    auto json_column = type_string->create_column();
    auto* column_string = assert_cast<ColumnString*>(json_column.get());
    VariantUtil::fill_string_column_with_test_data(column_string, num_rows, inserted);

    ParseConfig config;
    config.deprecated_enable_flatten_nested = false;
    config.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
    variant_util::parse_json_to_variant(*column_object, *column_string, config);
}

// DOC_COMPACT reads only one doc bucket column (e.g. "__DORIS_VARIANT_DOC_VALUE__.b0"), so it
// naturally returns only the subset of keys mapped into that bucket.
// This helper derives the expected JSON string for a given bucket from the full JSON produced by
// VariantUtil::fill_string_column_with_test_data, without parsing JSON.
static std::string expected_doc_bucket_json_from_full(const std::string& full_json, int bucket_num,
                                                      int bucket_index) {
    auto bucket_of = [&](const std::string& key) -> uint32_t {
        StringRef ref {key.data(), key.size()};
        return variant_util::variant_binary_shard_of(ref, bucket_num);
    };

    std::string out;
    out.reserve(full_json.size());
    out.push_back('{');

    bool first = true;
    // fill_string_column_with_test_data generates keys "key0".."key9" at most.
    for (int j = 0; j < 10; ++j) {
        const std::string key = "key" + std::to_string(j);
        const std::string needle = "\"" + key + "\":";
        if (full_json.find(needle) == std::string::npos) {
            continue;
        }
        if (bucket_of(key) != static_cast<uint32_t>(bucket_index)) {
            continue;
        }
        if (!first) {
            out.push_back(',');
        }
        first = false;
        out.append("\"");
        out.append(key);
        out.append("\":");
        if (j % 2 == 0) {
            out.append("88");
        } else {
            out.append("\"str99\"");
        }
    }

    out.push_back('}');
    return out;
}

static std::set<std::string> collect_regular_paths(
        const segment_v2::NestedGroupStreamingWritePlan& plan) {
    std::set<std::string> paths;
    for (const auto& entry : plan.regular_subcolumns) {
        paths.insert(entry.path);
    }
    return paths;
}

static std::vector<std::string> normalize_json_rows(const std::vector<std::string>& jsons,
                                                    int variant_max_subcolumns_count) {
    auto variant_col = ColumnVariant::create(variant_max_subcolumns_count, false);
    auto json_col = ColumnString::create();
    for (const auto& json : jsons) {
        json_col->insert_data(json.data(), json.size());
    }

    ParseConfig config;
    variant_util::parse_json_to_variant(*variant_col, *json_col, config);

    std::vector<std::string> normalized;
    normalized.reserve(jsons.size());
    DataTypeSerDe::FormatOptions options;
    for (size_t i = 0; i < jsons.size(); ++i) {
        std::string value;
        variant_col->serialize_one_row_to_string(i, &value, options);
        normalized.push_back(std::move(value));
    }
    return normalized;
}

static void append_variant_json_field(std::string* json, bool* first, std::string_view key,
                                      int64_t value) {
    if (!*first) {
        json->push_back(',');
    }
    *first = false;
    json->push_back('"');
    json->append(key.data(), key.size());
    json->append("\":");
    json->append(std::to_string(value));
}

static std::vector<std::string> make_variant_write_footprint_jsons(size_t num_rows,
                                                                   size_t dense_key_count,
                                                                   size_t sparse_keys_per_row,
                                                                   size_t sparse_key_pool) {
    std::vector<std::string> jsons;
    jsons.reserve(num_rows);
    for (size_t row = 0; row < num_rows; ++row) {
        std::string json;
        json.reserve((dense_key_count + sparse_keys_per_row) * 18);
        json.push_back('{');
        bool first = true;
        for (size_t i = 0; i < dense_key_count; ++i) {
            append_variant_json_field(&json, &first, "hot" + std::to_string(i),
                                      static_cast<int64_t>(row + i));
        }
        for (size_t i = 0; i < sparse_keys_per_row; ++i) {
            const size_t key_id = (row * sparse_keys_per_row + i) % sparse_key_pool;
            append_variant_json_field(&json, &first, "cold" + std::to_string(key_id),
                                      static_cast<int64_t>(row + key_id));
        }
        json.push_back('}');
        jsons.push_back(std::move(json));
    }
    return jsons;
}

// Regression test for legacy flat-dot-key compatibility.
//
// Old versions (e.g. cloud-4.1.2 with variant_max_subcolumns_count=0) stored
// a flat JSON key like {"a.b": 1} as a single PathInData part "a.b" in the
// segment's ColumnPathInfo protobuf. New master compaction schema builds
// query paths by splitting on dots (3+ parts including root), which does not
// match the 1-part tree node and causes silent data loss during compaction.
//
// This test writes a normal variant segment via the writer, then *mutates*
// the resulting footer to turn a subcolumn's `column_path_info` into the
// legacy 1-part form, then calls `VariantColumnReader::init()` and verifies
// that the normalization inside init() rebuilds a multi-level tree that can
// be queried via both `get_subcolumn_meta_by_path` and prefix-path lookup.
TEST_F(VariantColumnWriterReaderTest, test_legacy_flat_dot_key_reader_init) {
    // 1. create tablet_schema with a variant column that has nested subcolumns
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", /*max_subcolumns=*/10);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    _tablet_schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 20000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());

    // 5. write nested json so the writer naturally creates a subcolumn "a.b"
    // with a 2-part path ["a", "b"].
    std::vector<std::string> jsons;
    const int kNumRows = 8;
    for (int i = 0; i < kNumRows; ++i) {
        jsons.push_back(R"({"a": {"b": "v)" + std::to_string(i) + R"("}})");
    }
    EXPECT_TRUE(append_json_batch(writer.get(), jsons).ok());
    EXPECT_TRUE(writer->finish().ok());
    EXPECT_TRUE(writer->write_data().ok());
    EXPECT_TRUE(writer->write_ordinal_index().ok());
    EXPECT_TRUE(writer->write_zone_map().ok());
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(kNumRows);

    // 6. Locate the "V1.a.b" subcolumn in the footer and mutate its
    // column_path_info into the legacy 1-part form: pb.path = "V1.a.b" but
    // path_part_infos = [{"V1"}, {"a.b"}]. This is exactly what cloud-4.1.2
    // wrote for JSON key {"a.b": ...}.
    int target_idx = -1;
    for (int i = 1; i < footer.columns_size(); ++i) {
        const auto& col_meta = footer.columns(i);
        if (!col_meta.has_column_path_info()) {
            continue;
        }
        if (col_meta.column_path_info().path() == "v1.a.b") {
            target_idx = i;
            break;
        }
    }
    ASSERT_GT(target_idx, 0) << "failed to locate subcolumn V1.a.b in footer";

    auto* target_path_info = footer.mutable_columns(target_idx)->mutable_column_path_info();
    target_path_info->clear_path_part_infos();
    auto* root_part = target_path_info->add_path_part_infos();
    root_part->set_key("v1");
    root_part->set_is_nested(false);
    root_part->set_anonymous_array_level(0);
    auto* legacy_part = target_path_info->add_path_part_infos();
    legacy_part->set_key("a.b"); // single legacy part containing a dot
    legacy_part->set_is_nested(false);
    legacy_part->set_anonymous_array_level(0);
    target_path_info->set_has_nested(false);

    // 7. Now initialize a fresh VariantColumnReader with the mutated footer.
    // The init() path calls _subcolumns_meta_info->add() for each subcolumn;
    // our fix normalizes the legacy 1-part relative path "a.b" into a
    // 2-part path ["a", "b"] so the tree has root -> "a" -> "b".
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    ASSERT_TRUE(st.ok()) << st.msg();

    std::shared_ptr<segment_v2::ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    auto* variant_reader = assert_cast<segment_v2::VariantColumnReader*>(column_reader.get());
    ASSERT_NE(variant_reader, nullptr);

    // 8. Verify that queries against the normalized tree succeed.
    //    - Leaf lookup "a.b" (PathInData splits into 2 parts) should hit.
    //    - Intermediate lookup "a" should return the TUPLE parent, which
    //      has exactly one child "b".
    const auto* leaf_node = variant_reader->get_subcolumn_meta_by_path(PathInData("a.b"));
    ASSERT_NE(leaf_node, nullptr)
            << "normalized tree should be able to find leaf 'a.b' via multi-part query";
    EXPECT_TRUE(leaf_node->is_scalar());
    EXPECT_GE(leaf_node->data.footer_ordinal, 0);

    const auto* subtree = variant_reader->get_subcolumns_meta_info();
    ASSERT_NE(subtree, nullptr);
    const auto* intermediate = subtree->find_exact(PathInData("a"));
    ASSERT_NE(intermediate, nullptr)
            << "normalized tree should expose intermediate node 'a' as a TUPLE";
    EXPECT_FALSE(intermediate->is_scalar());
    EXPECT_EQ(intermediate->children.size(), 1U);
}

TEST_F(VariantColumnWriterReaderTest, test_statics) {
    // VariantStatisticsPB stats_pb;
    // auto* subcolumns_stats = stats_pb.mutable_sparse_column_non_null_size();
    // (*subcolumns_stats)["key0"] = 500;  // 50% of rows have key0
    // (*subcolumns_stats)["key1"] = 500;  // 50% of rows have key1
    // (*subcolumns_stats)["key2"] = 333;  // 33.3% of rows have key2
    // (*subcolumns_stats)["key3"] = 200;  // 20% of rows have key3
    // (*subcolumns_stats)["key4"] = 1000; // 100% of rows have key4

    // auto* sparse_stats = stats_pb.mutable_sparse_column_non_null_size();
    // (*sparse_stats)["key5"] = 100;
    // (*sparse_stats)["key6"] = 200;
    // (*sparse_stats)["key7"] = 300;

    // // 6.2 Test from_pb
    // segment_v2::VariantStatistics stats;
    // stats.from_pb(stats_pb);

    // // 6.3 Verify statistics
    // EXPECT_EQ(stats.sparse_column_non_null_size["key0"], 500);
    // EXPECT_EQ(stats.sparse_column_non_null_size["key1"], 500);
    // EXPECT_EQ(stats.sparse_column_non_null_size["key2"], 333);
    // EXPECT_EQ(stats.sparse_column_non_null_size["key3"], 200);
    // EXPECT_EQ(stats.sparse_column_non_null_size["key4"], 1000);

    // EXPECT_EQ(stats.sparse_column_non_null_size["key5"], 100);
    // EXPECT_EQ(stats.sparse_column_non_null_size["key6"], 200);
    // EXPECT_EQ(stats.sparse_column_non_null_size["key7"], 300);
}

TEST_F(VariantColumnWriterReaderTest, test_segment_rowid_read_by_reader_version) {
    init_variant_tablet(21000, 1);
    // "hot" is present in every row and consumes the only materialized slot. "z" remains in the
    // shared sparse column, including one physically missing row.
    const std::vector<std::string> jsons {R"({"hot":1,"z":101})", R"({"hot":2})",
                                          R"({"hot":3,"z":103})", R"({"hot":4,"z":104})"};
    auto rowset = create_variant_rowset({jsons}, 1, 100);
    auto beta_rowset = std::static_pointer_cast<BetaRowset>(rowset);
    std::vector<segment_v2::SegmentSharedPtr> segments;
    ASSERT_TRUE(beta_rowset->load_segments(&segments).ok());
    ASSERT_EQ(segments.size(), 1);

    TDescriptorTableBuilder descriptor_builder;
    TTupleDescriptorBuilder tuple_builder;
    auto make_variant_slot = [](bool use_v2, bool nullable, std::vector<std::string> column_paths) {
        auto slot = TSlotDescriptorBuilder()
                            .type(TYPE_VARIANT)
                            .nullable(nullable)
                            .column_name("v1")
                            .column_pos(0)
                            .build();
        slot.__set_col_unique_id(1);
        slot.__set_column_paths(std::move(column_paths));
        slot.__set_primitive_type(TPrimitiveType::VARIANT);
        auto& scalar = slot.slotType.types[0].scalar_type;
        scalar.__set_variant_max_subcolumns_count(1);
        scalar.__set_variant_enable_doc_mode(false);
        scalar.__set_variant_is_v2(use_v2);
        return slot;
    };
    for (const bool use_v2 : {false, true}) {
        tuple_builder.add_slot(make_variant_slot(use_v2, false, {}));
        tuple_builder.add_slot(make_variant_slot(use_v2, true, {"hot"}));
        tuple_builder.add_slot(make_variant_slot(use_v2, true, {"z"}));
    }
    tuple_builder.build(&descriptor_builder);

    ObjectPool object_pool;
    DescriptorTbl* descriptor_table = nullptr;
    ASSERT_TRUE(
            DescriptorTbl::create(&object_pool, descriptor_builder.desc_tbl(), &descriptor_table)
                    .ok());
    const auto& slots = descriptor_table->get_tuple_descriptor(0)->slots();
    ASSERT_EQ(slots.size(), 6);

    const std::vector<uint32_t> row_ids {0, 1, 2};
    const std::vector<uint32_t> second_row_ids {3};
    ASSERT_FALSE(_tablet_schema->column(0).variant_is_v2());
    for (size_t mode = 0; mode < 2; ++mode) {
        const bool use_v2 = mode != 0;
        const size_t slot_base = mode * 3;
        for (size_t slot_index = slot_base; slot_index < slot_base + 3; ++slot_index) {
            const auto type = remove_nullable(slots[slot_index]->type());
            EXPECT_EQ(typeid_cast<const DataTypeVariantV2*>(type.get()) != nullptr, use_v2);
            EXPECT_EQ(typeid_cast<const DataTypeVariant*>(type.get()) != nullptr, !use_v2);
        }

        OlapReaderStatistics stats;
        StorageReadOptions read_options;
        read_options.stats = &stats;
        read_options.io_ctx.reader_type = ReaderType::READER_QUERY;
        read_options.tablet_schema = _tablet_schema;

        MutableColumnPtr whole_result = slots[slot_base]->type()->create_column();
        ColumnIteratorUPtr whole_iterator;
        auto st = segments[0]->seek_and_read_by_rowid(*_tablet_schema, slots[slot_base], row_ids,
                                                      whole_result, read_options, whole_iterator);
        ASSERT_TRUE(st.ok()) << "use_v2=" << use_v2 << ": " << st.to_string();
        auto* const whole_iterator_address = whole_iterator.get();
        st = segments[0]->seek_and_read_by_rowid(*_tablet_schema, slots[slot_base], second_row_ids,
                                                 whole_result, read_options, whole_iterator);
        ASSERT_TRUE(st.ok()) << "use_v2=" << use_v2 << ": " << st.to_string();
        EXPECT_EQ(whole_iterator.get(), whole_iterator_address);
        ASSERT_EQ(whole_result->size(), jsons.size());
        for (size_t row = 0; row < jsons.size(); ++row) {
            EXPECT_EQ(variant_json_at(*whole_result, row), jsons[row])
                    << "use_v2=" << use_v2 << ", row=" << row;
        }

        MutableColumnPtr hot_result = slots[slot_base + 1]->type()->create_column();
        ColumnIteratorUPtr hot_iterator;
        st = segments[0]->seek_and_read_by_rowid(*_tablet_schema, slots[slot_base + 1], row_ids,
                                                 hot_result, read_options, hot_iterator);
        ASSERT_TRUE(st.ok()) << "use_v2=" << use_v2 << ": " << st.to_string();
        auto* const hot_iterator_address = hot_iterator.get();
        st = segments[0]->seek_and_read_by_rowid(*_tablet_schema, slots[slot_base + 1],
                                                 second_row_ids, hot_result, read_options,
                                                 hot_iterator);
        ASSERT_TRUE(st.ok()) << "use_v2=" << use_v2 << ": " << st.to_string();
        EXPECT_EQ(hot_iterator.get(), hot_iterator_address);
        const auto& nullable_hot = assert_cast<const ColumnNullable&>(*hot_result);
        const auto& hot_variant = nullable_hot.get_nested_column();
        ASSERT_EQ(nullable_hot.size(), jsons.size());
        for (size_t row = 0; row < jsons.size(); ++row) {
            EXPECT_FALSE(nullable_hot.is_null_at(row)) << "use_v2=" << use_v2 << ", row=" << row;
            EXPECT_EQ(variant_json_at(hot_variant, row), std::to_string(row + 1))
                    << "use_v2=" << use_v2 << ", row=" << row;
        }
        if (use_v2) {
            EXPECT_TRUE(assert_cast<const ColumnVariantV2&>(hot_variant).is_typed());
        }

        MutableColumnPtr subpath_result = slots[slot_base + 2]->type()->create_column();
        ColumnIteratorUPtr subpath_iterator;
        st = segments[0]->seek_and_read_by_rowid(*_tablet_schema, slots[slot_base + 2], row_ids,
                                                 subpath_result, read_options, subpath_iterator);
        ASSERT_TRUE(st.ok()) << "use_v2=" << use_v2 << ": " << st.to_string();
        auto* const subpath_iterator_address = subpath_iterator.get();
        st = segments[0]->seek_and_read_by_rowid(*_tablet_schema, slots[slot_base + 2],
                                                 second_row_ids, subpath_result, read_options,
                                                 subpath_iterator);
        ASSERT_TRUE(st.ok()) << "use_v2=" << use_v2 << ": " << st.to_string();
        EXPECT_EQ(subpath_iterator.get(), subpath_iterator_address);
        const auto& nullable_subpath = assert_cast<const ColumnNullable&>(*subpath_result);
        const auto& subpath_variant = nullable_subpath.get_nested_column();
        ASSERT_EQ(nullable_subpath.size(), jsons.size());
        for (size_t row = 0; row < jsons.size(); ++row) {
            const bool missing = row == 1;
            EXPECT_EQ(nullable_subpath.is_null_at(row), missing)
                    << "use_v2=" << use_v2 << ", row=" << row;
            if (!missing) {
                EXPECT_EQ(variant_json_at(subpath_variant, row), std::to_string(row + 101))
                        << "use_v2=" << use_v2 << ", row=" << row;
            }
        }
        if (use_v2) {
            EXPECT_NE(typeid_cast<BinaryColumnExtractIterator*>(subpath_iterator.get()), nullptr);
            EXPECT_TRUE(assert_cast<const ColumnVariantV2&>(subpath_variant).is_typed());
        }
        EXPECT_FALSE(_tablet_schema->column(0).variant_is_v2());
    }

    // Reuse the persisted-segment V1/V2 matrix for legacy empty-nested visibility. Declare "a" as
    // an ARRAY<JSONB> typed path so this exercises the materialized array merge rather than the
    // sparse-cell decoder.
    TabletSchemaPB empty_nested_schema_pb;
    empty_nested_schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(empty_nested_schema_pb.add_column(), 1, "VARIANT", "V1",
                     /*variant_max_subcolumns_count=*/1,
                     /*is_key=*/false,
                     /*is_nullable=*/false);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(empty_nested_schema_pb);
    auto empty_nested_typed_path = make_jsonb_array_typed_path_template("a");
    _tablet_schema->mutable_column_by_uid(1).add_sub_column(empty_nested_typed_path);
    init_tablet_from_current_schema(21001);

    const std::vector<std::string> empty_nested_jsons {R"({"a":[]})",   R"({"a":[null]})",
                                                       R"({"a":[{}]})", R"({"a":[{"L2":[]}]})",
                                                       R"({"a":[1]})",  R"({"a":null})"};
    auto empty_nested_rowset = create_variant_rowset({empty_nested_jsons}, 2, 100);
    auto empty_nested_beta = std::static_pointer_cast<BetaRowset>(empty_nested_rowset);
    std::vector<segment_v2::SegmentSharedPtr> empty_nested_segments;
    ASSERT_TRUE(empty_nested_beta->load_segments(&empty_nested_segments).ok());
    ASSERT_EQ(empty_nested_segments.size(), 1);

    TDescriptorTableBuilder empty_descriptor_builder;
    TTupleDescriptorBuilder empty_tuple_builder;
    for (const bool use_v2 : {false, true}) {
        empty_tuple_builder.add_slot(make_variant_slot(use_v2, false, {}));
        empty_tuple_builder.add_slot(make_variant_slot(use_v2, true, {"a"}));
    }
    empty_tuple_builder.build(&empty_descriptor_builder);
    ObjectPool empty_object_pool;
    DescriptorTbl* empty_descriptor_table = nullptr;
    ASSERT_TRUE(DescriptorTbl::create(&empty_object_pool, empty_descriptor_builder.desc_tbl(),
                                      &empty_descriptor_table)
                        .ok());
    const auto& empty_slots = empty_descriptor_table->get_tuple_descriptor(0)->slots();
    ASSERT_EQ(empty_slots.size(), 4);
    const std::vector<uint32_t> empty_row_ids {0, 1, 2, 3, 4, 5};
    const std::array<std::string_view, 6> expected_whole_v1 {"{}", "{}",           "{}",
                                                             "{}", R"({"a":[1]})", "{}"};
    const std::array<std::string_view, 6> expected_whole_v2 {
            "{}", "{}", R"({"a":[{}]})", R"({"a":[{"L2":[]}]})", R"({"a":[1]})", "{}"};
    const std::array<bool, 6> expected_subpath_null {true, true, false, false, false, true};
    const std::array<std::string_view, 3> expected_subpath_values {"[{}]", R"([{"L2":[]}])", "[1]"};

    for (size_t mode = 0; mode < 2; ++mode) {
        const bool use_v2 = mode != 0;
        const size_t slot_base = mode * 2;
        OlapReaderStatistics stats;
        StorageReadOptions read_options;
        read_options.stats = &stats;
        read_options.io_ctx.reader_type = ReaderType::READER_QUERY;
        read_options.tablet_schema = _tablet_schema;

        MutableColumnPtr whole_result = empty_slots[slot_base]->type()->create_column();
        ColumnIteratorUPtr whole_iterator;
        auto st = empty_nested_segments[0]->seek_and_read_by_rowid(
                *_tablet_schema, empty_slots[slot_base], empty_row_ids, whole_result, read_options,
                whole_iterator);
        ASSERT_TRUE(st.ok()) << "use_v2=" << use_v2 << ": " << st.to_string();
        const auto& expected_whole = use_v2 ? expected_whole_v2 : expected_whole_v1;
        ASSERT_EQ(whole_result->size(), expected_whole.size());
        for (size_t row = 0; row < expected_whole.size(); ++row) {
            EXPECT_EQ(variant_json_at(*whole_result, row), expected_whole[row])
                    << "use_v2=" << use_v2 << ", row=" << row;
        }

        MutableColumnPtr subpath_result = empty_slots[slot_base + 1]->type()->create_column();
        ColumnIteratorUPtr subpath_iterator;
        st = empty_nested_segments[0]->seek_and_read_by_rowid(
                *_tablet_schema, empty_slots[slot_base + 1], empty_row_ids, subpath_result,
                read_options, subpath_iterator);
        ASSERT_TRUE(st.ok()) << "use_v2=" << use_v2 << ": " << st.to_string();
        const auto& nullable = assert_cast<const ColumnNullable&>(*subpath_result);
        const auto& values = nullable.get_nested_column();
        ASSERT_EQ(nullable.size(), expected_subpath_null.size());
        for (size_t row = 0; row < expected_subpath_null.size(); ++row) {
            EXPECT_EQ(nullable.is_null_at(row), expected_subpath_null[row])
                    << "use_v2=" << use_v2 << ", row=" << row;
        }
        for (size_t row = 2; row <= 4; ++row) {
            EXPECT_EQ(variant_json_at(values, row), expected_subpath_values[row - 2])
                    << "use_v2=" << use_v2 << ", row=" << row;
        }
    }
}

TEST_F(VariantColumnWriterReaderTest, test_legacy_rowid_storage_reader_extracted_leaf) {
    init_variant_tablet(21002, 1);
    // "hot" consumes the only materialized slot. The sparse "z" path stays homogeneous, while
    // "mixed" exercises missing -> int -> string through the row-at-a-time caller.
    const std::vector<std::string> jsons {R"({"hot":0,"z":100})",
                                          R"({"hot":1,"z":101,"mixed":101})",
                                          R"({"hot":2,"z":102,"mixed":"mixed"})", R"({"hot":3})"};
    auto rowset = create_variant_rowset({jsons}, 1, 100);

    RuntimeProfile profile("RegisterVariantRowIdTablet");
    auto* tablet_manager = _engine_ref->tablet_manager();
    {
        std::lock_guard<std::shared_mutex> lock(
                tablet_manager->_get_tablets_shard_lock(_tablet->tablet_id()));
        ASSERT_TRUE(tablet_manager
                            ->_add_tablet_unlocked(_tablet->tablet_id(), _tablet,
                                                   /*update_meta=*/false, /*force=*/false, &profile)
                            .ok());
    }
    _engine_ref->add_quering_rowset(rowset);

    auto make_slot = [](std::string path, bool use_v2) {
        auto slot = TSlotDescriptorBuilder()
                            .type(TYPE_VARIANT)
                            .nullable(true)
                            .column_name("v1")
                            .column_pos(0)
                            .build();
        slot.__set_col_unique_id(1);
        slot.__set_column_paths({std::move(path)});
        slot.__set_primitive_type(TPrimitiveType::VARIANT);
        auto& scalar = slot.slotType.types[0].scalar_type;
        scalar.__set_variant_max_subcolumns_count(1);
        scalar.__set_variant_enable_doc_mode(false);
        scalar.__set_variant_is_v2(use_v2);
        return slot;
    };

    for (const bool use_v2 : {false, true}) {
        TDescriptorTableBuilder descriptor_builder;
        TTupleDescriptorBuilder tuple_builder;
        tuple_builder.add_slot(make_slot("z", use_v2));
        tuple_builder.add_slot(make_slot("mixed", use_v2));
        tuple_builder.build(&descriptor_builder);
        ObjectPool object_pool;
        DescriptorTbl* descriptor_table = nullptr;
        ASSERT_TRUE(DescriptorTbl::create(&object_pool, descriptor_builder.desc_tbl(),
                                          &descriptor_table)
                            .ok());
        const auto& slots = descriptor_table->get_tuple_descriptor(0)->slots();
        ASSERT_EQ(slots.size(), 2);

        PMultiGetRequest request;
        slots[0]->to_protobuf(request.add_slots());
        slots[1]->to_protobuf(request.add_slots());
        _tablet_schema->column(0).to_schema_pb(request.add_column_desc());
        request.set_fetch_row_store(false);
        request.mutable_query_id()->set_hi(1);
        request.mutable_query_id()->set_lo(use_v2 ? 2 : 1);
        for (uint32_t row_id = 0; row_id < 3; ++row_id) {
            auto* location = request.add_row_locs();
            location->set_tablet_id(_tablet->tablet_id());
            location->set_rowset_id(rowset->rowset_id().to_string());
            location->set_segment_id(0);
            location->set_ordinal_id(row_id);
        }

        PMultiGetResponse response;
        auto st = RowIdStorageReader::read_by_rowids(request, &response);
        ASSERT_TRUE(st.ok()) << "use_v2=" << use_v2 << ": " << st.to_string();
        ASSERT_TRUE(response.has_block());
        ASSERT_EQ(response.row_locs_size(), 3);

        Block result;
        size_t uncompressed_size = 0;
        int64_t uncompressed_time = 0;
        st = result.deserialize(response.block(), &uncompressed_size, &uncompressed_time);
        ASSERT_TRUE(st.ok()) << "use_v2=" << use_v2 << ": " << st.to_string();
        ASSERT_EQ(result.columns(), 2);
        ASSERT_EQ(result.rows(), 3);

        const auto& result_column = result.get_by_position(0);
        const auto result_type = remove_nullable(result_column.type);
        EXPECT_EQ(typeid_cast<const DataTypeVariantV2*>(result_type.get()) != nullptr, use_v2);
        EXPECT_EQ(typeid_cast<const DataTypeVariant*>(result_type.get()) != nullptr, !use_v2);
        const auto& nullable = assert_cast<const ColumnNullable&>(*result_column.column);
        const auto& variant = nullable.get_nested_column();
        for (size_t row = 0; row < 3; ++row) {
            EXPECT_FALSE(nullable.is_null_at(row)) << "use_v2=" << use_v2 << ", row=" << row;
            EXPECT_EQ(variant_json_at(variant, row), std::to_string(row + 100))
                    << "use_v2=" << use_v2 << ", row=" << row;
        }

        const auto& mixed_result_column = result.get_by_position(1);
        const auto mixed_type = remove_nullable(mixed_result_column.type);
        EXPECT_EQ(typeid_cast<const DataTypeVariantV2*>(mixed_type.get()) != nullptr, use_v2);
        EXPECT_EQ(typeid_cast<const DataTypeVariant*>(mixed_type.get()) != nullptr, !use_v2);
        const auto& mixed_nullable =
                assert_cast<const ColumnNullable&>(*mixed_result_column.column);
        const auto& mixed_variant = mixed_nullable.get_nested_column();
        ASSERT_EQ(mixed_nullable.size(), 3);
        EXPECT_TRUE(mixed_nullable.is_null_at(0)) << "use_v2=" << use_v2;
        EXPECT_FALSE(mixed_nullable.is_null_at(1)) << "use_v2=" << use_v2;
        EXPECT_FALSE(mixed_nullable.is_null_at(2)) << "use_v2=" << use_v2;
        EXPECT_EQ(variant_json_at(mixed_variant, 1), "101") << "use_v2=" << use_v2;
        EXPECT_EQ(variant_json_at(mixed_variant, 2), R"("mixed")") << "use_v2=" << use_v2;

        if (use_v2) {
            EXPECT_TRUE(assert_cast<const ColumnVariantV2&>(variant).is_typed());
            EXPECT_FALSE(assert_cast<const ColumnVariantV2&>(mixed_variant).is_typed());
        }
    }
}

TEST_F(VariantColumnWriterReaderTest, test_speculative_sparse_read_after_statistics_truncation) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    auto* root_pb = schema_pb.add_column();
    construct_column(root_pb, 1, "VARIANT", "V1",
                     /*variant_max_subcolumns_count=*/1,
                     /*is_key=*/false,
                     /*is_nullable=*/false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/false);
    root_pb->set_variant_max_sparse_column_statistics_size(1);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);
    init_tablet_from_current_schema(21001);

    // "hot" is materialized. The first sparse row contains only "aa_filler", which consumes the
    // single statistics slot before "zz_object.child" first appears in a later row.
    const std::vector<std::string> jsons {
            R"({"hot":0,"aa_filler":10})",
            R"({"hot":1,"zz_object":{"child":101}})",
            R"({"hot":2,"aa_filler":12,"zz_object":{"child":102}})",
            R"({"hot":3,"aa_filler":13})",
    };
    SegmentFooterPB footer;
    std::string file_path;
    auto st = write_storage_parsed_segment(jsons, "sparse_stats_truncated", &footer, &file_path);
    ASSERT_TRUE(st.ok()) << st.to_string();

    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    ASSERT_TRUE(st.ok()) << st.to_string();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    ASSERT_TRUE(st.ok()) << st.to_string();
    auto* variant_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    ASSERT_NE(variant_reader, nullptr);

    const auto* statistics = variant_reader->get_stats();
    ASSERT_NE(statistics, nullptr);
    ASSERT_EQ(statistics->sparse_column_non_null_size.size(), 1);
    EXPECT_TRUE(statistics->sparse_column_non_null_size.contains("aa_filler"));
    EXPECT_FALSE(statistics->sparse_column_non_null_size.contains("zz_object.child"));
    EXPECT_TRUE(variant_reader->is_exceeded_sparse_column_limit());
    EXPECT_NE(variant_reader->get_subcolumn_meta_by_path(PathInData("hot")), nullptr);
    EXPECT_EQ(variant_reader->get_subcolumn_meta_by_path(PathInData("zz_object")), nullptr);
    EXPECT_FALSE(variant_reader->exist_in_sparse_column(PathInData("zz_object")));

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);
    ColumnIteratorOptions iterator_options;
    iterator_options.file_reader = file_reader.get();

    const TabletColumn& root_column = _tablet_schema->column(0);
    ASSERT_FALSE(root_column.variant_is_v2());
    TabletColumn target_column;
    target_column.set_name(root_column.name_lower_case() + ".zz_object");
    target_column.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    target_column.set_parent_unique_id(root_column.unique_id());
    target_column.set_path_info(PathInData(target_column.name_lower_case()));
    target_column.set_variant_max_subcolumns_count(root_column.variant_max_subcolumns_count());
    target_column.set_is_nullable(true);

    const std::vector<std::string> expected {"", R"({"child":101})", R"({"child":102})", ""};
    for (const bool use_v2 : {false, true}) {
        TabletColumn read_column = target_column;
        read_column.set_variant_is_v2(use_v2);
        OlapReaderStatistics stats;
        StorageReadOptions read_options;
        read_options.stats = &stats;
        read_options.io_ctx.reader_type = ReaderType::READER_QUERY;
        read_options.tablet_schema = _tablet_schema;
        ColumnIteratorUPtr iterator;
        st = variant_reader->new_iterator(&iterator, &read_column, &read_options,
                                          &column_reader_cache);
        ASSERT_TRUE(st.ok()) << "use_v2=" << use_v2 << ": " << st.to_string();
        ASSERT_NE(dynamic_cast<HierarchicalDataIterator*>(iterator.get()), nullptr);
        iterator_options.stats = &stats;
        ASSERT_TRUE(iterator->init(iterator_options).ok());
        ASSERT_TRUE(iterator->seek_to_ordinal(0).ok());
        auto type = DataTypeFactory::instance().create_data_type(read_column, false);
        MutableColumnPtr result = type->create_column();
        size_t rows = jsons.size();
        ASSERT_TRUE(iterator->next_batch(&rows, result).ok());
        ASSERT_EQ(rows, jsons.size());
        ASSERT_EQ(stats.variant_subtree_hierarchical_iter_count, 1);
        const auto& nullable = assert_cast<const ColumnNullable&>(*result);
        const auto& variant = nullable.get_nested_column();
        for (size_t row = 0; row < jsons.size(); ++row) {
            const bool expect_null = expected[row].empty();
            EXPECT_EQ(nullable.is_null_at(row), expect_null)
                    << "use_v2=" << use_v2 << ", row=" << row;
            if (!expect_null) {
                EXPECT_EQ(variant_json_at(variant, row), expected[row])
                        << "use_v2=" << use_v2 << ", row=" << row;
            }
        }
    }

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_write_data_normal) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    int variant_sparse_hash_shard_count = rand() % 10 + 1;
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 3, false, false,
                     variant_sparse_hash_shard_count);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());

    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto column_object = (*std::move(block.get_by_position(0).column)).mutate();
    std::unordered_map<int, std::string> inserted_jsonstr;
    auto path_with_size =
            VariantUtil::fill_object_column_with_test_data(column_object, 1000, &inserted_jsonstr);
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), 1000).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    int expected_sparse_cols =
            variant_sparse_hash_shard_count > 1 ? variant_sparse_hash_shard_count : 1;
    EXPECT_EQ(footer.columns_size(), 1 + 3 + expected_sparse_cols);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);

    for (int i = 1; i < footer.columns_size() - 1; ++i) {
        auto column_met = footer.columns(i);
        check_column_meta(column_met, path_with_size);
    }
    check_sparse_column_meta(footer.columns(footer.columns_size() - 1), path_with_size);

    // 7. check variant reader
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    // create root variant reader using ColumnMetaAccessor (supports inline/external meta)
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    auto variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    auto subcolumn_meta = variant_column_reader->get_subcolumn_meta_by_path(PathInData("key0"));
    EXPECT_TRUE(subcolumn_meta != nullptr);
    subcolumn_meta = variant_column_reader->get_subcolumn_meta_by_path(PathInData("key1"));
    EXPECT_TRUE(subcolumn_meta != nullptr);
    subcolumn_meta = variant_column_reader->get_subcolumn_meta_by_path(PathInData("key2"));
    EXPECT_TRUE(subcolumn_meta != nullptr);
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("key3")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("key4")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("key5")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("key6")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("key7")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("key8")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("key9")));
    auto size = variant_column_reader->get_metadata_size();
    EXPECT_GT(size, 0);

    // 8. check statistics
    auto statistics = variant_column_reader->get_stats();
    for (const auto& [path, siz] : statistics->subcolumns_non_null_size) {
        EXPECT_EQ(path_with_size[path], siz);
    }
    for (const auto& [path, siz] : statistics->sparse_column_non_null_size) {
        EXPECT_EQ(path_with_size[path], siz);
    }

    // 9. check hier reader
    ColumnIteratorUPtr it;
    TabletColumn parent_column = _tablet_schema->column(0);
    StorageReadOptions storage_read_opts;
    OlapReaderStatistics stats;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    storage_read_opts.stats = &stats;
    st = variant_column_reader->new_iterator(&it, &parent_column, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<HierarchicalDataIterator*>(it.get()) != nullptr);
    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = it->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    MutableColumnPtr new_column_object = ColumnVariant::create(3, false);
    size_t nrows = 1000;
    st = it->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = it->next_batch(&nrows, new_column_object);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(stats.bytes_read > 0);

    // seek_to_first for HierarchicalDataIterator no need to implement
    {
        auto iter = assert_cast<HierarchicalDataIterator*>(it.get());
        std::shared_ptr<ColumnReader> column_reader1;
        st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader1);
        EXPECT_TRUE(st.ok()) << st.msg();
        std::cout << "hier:" << iter->get_current_ordinal() << std::endl;
        //  now we can find exist
        auto exist_node = std::make_unique<SubcolumnColumnMetaInfo::Node>(
                SubcolumnColumnMetaInfo::Node::Kind::SCALAR);
        exist_node->path = PathInData("key0");
        OlapReaderStatistics stats;
        Status sts = iter->add_stream(0, exist_node.get(), &column_reader_cache, &stats);
        EXPECT_TRUE(sts.ok());
        auto jsonb_type = std::make_shared<DataTypeJsonb>();
        // if node path is emtpy we will meet error
        auto variant_column_reader1 = assert_cast<VariantColumnReader*>(column_reader1.get());
        EXPECT_TRUE(variant_column_reader1 != nullptr);
        auto r = variant_column_reader1->get_subcolumns_meta_info()->get_leaves()[1];
        r->path = PathInData("");
        // if we clear the parts manually, we will meet error, but it can be handled, and should not happen
        r->path.parts.clear();
        sts = iter->add_stream(0, r.get(), &column_reader_cache, &stats);
        EXPECT_FALSE(sts.ok());
    }

    DataTypeSerDe::FormatOptions options;
    auto tz = cctz::utc_time_zone();
    options.timezone = &tz;
    for (int i = 0; i < 1000; ++i) {
        std::string value;
        assert_cast<ColumnVariant*>(new_column_object.get())
                ->serialize_one_row_to_string(i, &value, options);

        EXPECT_EQ(value, inserted_jsonstr[i]);
    }

    // The segment is still written with ColumnVariant. A query destination can request
    // ColumnVariantV2 and assemble the same hierarchical streams directly.
    TabletColumn parent_column_v2 = parent_column;
    parent_column_v2.set_variant_is_v2(true);
    ColumnIteratorUPtr variant_v2_it;
    st = variant_column_reader->new_iterator(&variant_v2_it, &parent_column_v2, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<HierarchicalDataIterator*>(variant_v2_it.get()) != nullptr);
    st = variant_v2_it->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();
    MutableColumnPtr variant_v2_column = DataTypeVariantV2(3, false).create_column();
    nrows = 1000;
    st = variant_v2_it->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = variant_v2_it->next_batch(&nrows, variant_v2_column);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_EQ(nrows, 1000);
    auto& variant_v2 = assert_cast<ColumnVariantV2&>(*variant_v2_column);
    for (int i = 0; i < 1000; ++i) {
        EXPECT_EQ(variant_v2_json_at(variant_v2, i), inserted_jsonstr[i]);
    }

    std::vector<rowid_t> row_ids;
    for (int i = 0; i < 1000; ++i) {
        if (i % 7 == 0) {
            row_ids.push_back(i);
        }
    }
    MutableColumnPtr variant_v2_rowid_column = DataTypeVariantV2(3, false).create_column();
    st = variant_v2_it->read_by_rowids(row_ids.data(), row_ids.size(), variant_v2_rowid_column);
    EXPECT_TRUE(st.ok()) << st.msg();
    auto& variant_v2_rowids = assert_cast<ColumnVariantV2&>(*variant_v2_rowid_column);
    for (int i = 0; i < row_ids.size(); ++i) {
        EXPECT_EQ(variant_v2_json_at(variant_v2_rowids, i), inserted_jsonstr[row_ids[i]]);
    }

    new_column_object = ColumnVariant::create(3, false);
    st = it->read_by_rowids(row_ids.data(), row_ids.size(), new_column_object);
    EXPECT_TRUE(st.ok()) << st.msg();
    for (int i = 0; i < row_ids.size(); ++i) {
        std::string value;
        assert_cast<ColumnVariant*>(new_column_object.get())
                ->serialize_one_row_to_string(i, &value, options);
        EXPECT_EQ(value, inserted_jsonstr[row_ids[i]]);
    }

    auto read_to_column_object = [&](ColumnIteratorUPtr& it) {
        new_column_object = ColumnVariant::create(3, false);
        nrows = 1000;
        st = it->seek_to_ordinal(0);
        EXPECT_TRUE(st.ok()) << st.msg();
        st = it->next_batch(&nrows, new_column_object);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(stats.bytes_read > 0);
        EXPECT_EQ(nrows, 1000);
    };

    // 10. check sparse extract reader
    PathToBinaryColumnCacheUPtr sparse_column_cache =
            std::make_unique<std::unordered_map<std::string, BinaryColumnCacheSPtr>>();
    stats.bytes_read = 0;
    for (int i = 3; i < 10; ++i) {
        std::string key = ".key" + std::to_string(i);
        TabletColumn subcolumn_in_sparse;
        subcolumn_in_sparse.set_name(parent_column.name_lower_case() + key);
        subcolumn_in_sparse.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
        subcolumn_in_sparse.set_parent_unique_id(parent_column.unique_id());
        subcolumn_in_sparse.set_path_info(PathInData(parent_column.name_lower_case() + key));
        subcolumn_in_sparse.set_variant_max_subcolumns_count(
                parent_column.variant_max_subcolumns_count());
        subcolumn_in_sparse.set_is_nullable(true);

        ColumnIteratorUPtr it;
        st = variant_column_reader->new_iterator(&it, &subcolumn_in_sparse, &storage_read_opts,
                                                 &column_reader_cache, sparse_column_cache.get());
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(assert_cast<BinaryColumnExtractIterator*>(it.get()) != nullptr);
        st = it->init(column_iter_opts);
        EXPECT_TRUE(st.ok()) << st.msg();

        int64_t before_bytes_read = stats.bytes_read;
        read_to_column_object(it);
        // In bucketized mode, different keys may map to different buckets and trigger extra IO.
        if (variant_sparse_hash_shard_count <= 1 && before_bytes_read != 0) {
            EXPECT_EQ(stats.bytes_read, before_bytes_read);
        }

        for (int row = 0; row < 1000; ++row) {
            std::string value;
            assert_cast<ColumnVariant*>(new_column_object.get())
                    ->serialize_one_row_to_string(row, &value, options);
            if (inserted_jsonstr[row].find(key) != std::string::npos) {
                if (i % 2 == 0) {
                    EXPECT_EQ(value, "88");
                } else {
                    EXPECT_EQ(value, "str99");
                }
            }
        }

        TabletColumn subcolumn_in_sparse_v2 = subcolumn_in_sparse;
        subcolumn_in_sparse_v2.set_variant_is_v2(true);
        ColumnIteratorUPtr variant_v2_sparse_it;
        st = variant_column_reader->new_iterator(&variant_v2_sparse_it, &subcolumn_in_sparse_v2,
                                                 &storage_read_opts, &column_reader_cache,
                                                 sparse_column_cache.get());
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(assert_cast<BinaryColumnExtractIterator*>(variant_v2_sparse_it.get()) !=
                    nullptr);
        st = variant_v2_sparse_it->init(column_iter_opts);
        EXPECT_TRUE(st.ok()) << st.msg();

        MutableColumnPtr nullable_variant_v2 =
                make_nullable(std::make_shared<DataTypeVariantV2>(3, false))->create_column();
        nrows = 1000;
        st = variant_v2_sparse_it->seek_to_ordinal(0);
        EXPECT_TRUE(st.ok()) << st.msg();
        st = variant_v2_sparse_it->next_batch(&nrows, nullable_variant_v2);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_EQ(nrows, 1000);
        auto& nullable = assert_cast<ColumnNullable&>(*nullable_variant_v2);
        auto& sparse_variant_v2 = assert_cast<ColumnVariantV2&>(nullable.get_nested_column());
        EXPECT_TRUE(sparse_variant_v2.is_typed());
        const std::string json_key = "\"" + key.substr(1) + "\":";
        for (int row = 0; row < 1000; ++row) {
            const bool present = inserted_jsonstr[row].find(json_key) != std::string::npos;
            EXPECT_EQ(nullable.is_null_at(row), !present);
            if (present) {
                const std::string expected = i % 2 == 0 ? "88" : "\"str99\"";
                EXPECT_EQ(variant_v2_json_at(sparse_variant_v2, row), expected);
            }
        }
    }

    // 11. check leaf reader
    auto check_leaf_reader = [&]() {
        for (int i = 0; i < 3; ++i) {
            std::string key = ".key" + std::to_string(i);
            TabletColumn subcolumn;
            subcolumn.set_name(parent_column.name_lower_case() + key);
            subcolumn.set_type((FieldType)(int)footer.columns(i + 1).type());
            subcolumn.set_parent_unique_id(parent_column.unique_id());
            subcolumn.set_path_info(PathInData(parent_column.name_lower_case() + key));
            subcolumn.set_variant_max_subcolumns_count(
                    parent_column.variant_max_subcolumns_count());
            subcolumn.set_is_nullable(true);

            ColumnIteratorUPtr it;
            st = variant_column_reader->new_iterator(&it, &subcolumn, &storage_read_opts,
                                                     &column_reader_cache);
            EXPECT_TRUE(st.ok()) << st.msg();
            std::cout << "key " << key << std::endl;
            EXPECT_TRUE(dynamic_cast<FileColumnIterator*>(it.get()) != nullptr);
            st = it->init(column_iter_opts);
            EXPECT_TRUE(st.ok()) << st.msg();

            auto column_type = DataTypeFactory::instance().create_data_type(subcolumn, false);
            auto read_column = column_type->create_column();
            nrows = 1000;
            st = it->seek_to_ordinal(0);
            EXPECT_TRUE(st.ok()) << st.msg();
            st = it->next_batch(&nrows, read_column);
            EXPECT_TRUE(st.ok()) << st.msg();
            EXPECT_TRUE(stats.bytes_read > 0);

            for (int row = 0; row < 1000; ++row) {
                const std::string& value = column_type->to_string(*read_column, row);
                if (inserted_jsonstr[row].find(key) != std::string::npos) {
                    if (i % 2 == 0) {
                        EXPECT_EQ(value, "88");
                    } else {
                        EXPECT_EQ(value, "str99");
                    }
                }
            }
        }
    };
    check_leaf_reader();

    // 12. check empty
    TabletColumn subcolumn;
    subcolumn.set_name(parent_column.name_lower_case() + ".key10");
    subcolumn.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    subcolumn.set_parent_unique_id(parent_column.unique_id());
    subcolumn.set_path_info(PathInData(parent_column.name_lower_case() + ".key10"));
    subcolumn.set_is_nullable(true);
    ColumnIteratorUPtr it1;
    st = variant_column_reader->new_iterator(&it1, &subcolumn, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<DefaultValueColumnIterator*>(it1.get()) != nullptr);

    // 13. check statistics size == limit
    auto& variant_stats = variant_column_reader->_statistics;
    EXPECT_TRUE(variant_stats->sparse_column_non_null_size.size() <
                variant_column_reader->_variant_sparse_column_statistics_size);
    auto limit = variant_column_reader->_variant_sparse_column_statistics_size -
                 variant_stats->sparse_column_non_null_size.size();
    for (int i = 0; i < limit; ++i) {
        std::string key = parent_column.name_lower_case() + ".key10" + std::to_string(i);
        variant_stats->sparse_column_non_null_size[key] = 10000;
    }
    EXPECT_TRUE(variant_stats->sparse_column_non_null_size.size() ==
                variant_column_reader->_variant_sparse_column_statistics_size);
    EXPECT_TRUE(variant_column_reader->is_exceeded_sparse_column_limit());

    ColumnIteratorUPtr it2;
    st = variant_column_reader->new_iterator(&it2, &subcolumn, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<HierarchicalDataIterator*>(it2.get()) != nullptr);
    st = it2->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto check_empty_column = [&]() {
        for (int row = 0; row < 1000; ++row) {
            std::string value;
            assert_cast<ColumnVariant*>(new_column_object.get())
                    ->serialize_one_row_to_string(row, &value, options);

            EXPECT_EQ(value, "{}");
        }
    };

    read_to_column_object(it2);
    check_empty_column();

    // construct tablet schema for compaction
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_BASE_COMPACTION;
    storage_read_opts.tablet_schema = _tablet_schema;
    std::unordered_map<int32_t, TabletSchema::PathsSetInfo> uid_to_paths_set_info;
    TabletSchema::PathsSetInfo paths_set_info;
    paths_set_info.sub_path_set.insert("key0");
    paths_set_info.sub_path_set.insert("key3");
    paths_set_info.sub_path_set.insert("key4");
    paths_set_info.sparse_path_set.insert("key1");
    paths_set_info.sparse_path_set.insert("key2");
    paths_set_info.sparse_path_set.insert("key5");
    paths_set_info.sparse_path_set.insert("key6");
    paths_set_info.sparse_path_set.insert("key7");
    paths_set_info.sparse_path_set.insert("key8");
    paths_set_info.sparse_path_set.insert("key9");
    uid_to_paths_set_info[parent_column.unique_id()] = paths_set_info;
    _tablet_schema->set_path_set_info(std::move(uid_to_paths_set_info));

    // mock a subcolumn in compaction
    TabletColumn subcolumn_in_compaction;
    subcolumn_in_compaction.set_name(parent_column.name_lower_case() + ".key10");
    subcolumn_in_compaction.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    subcolumn_in_compaction.set_parent_unique_id(parent_column.unique_id());
    subcolumn_in_compaction.set_path_info(PathInData(parent_column.name_lower_case() + ".key10"));
    subcolumn_in_compaction.set_is_nullable(true);
    _tablet_schema->append_column(subcolumn_in_compaction);

    // 14. check compaction subcolumn reader
    check_leaf_reader();
    // 15. check compaction root reader
    ColumnIteratorUPtr it3;
    st = variant_column_reader->new_iterator(&it3, &parent_column, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<VariantRootColumnIterator*>(it3.get()) != nullptr);
    st = it3->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();
    // test VariantRootColumnIterator for next_batch and read_by_rowids
    {
        auto iter = assert_cast<VariantRootColumnIterator*>(it3.get());
        auto nullable_dt =
                std::make_shared<DataTypeNullable>(std::make_shared<DataTypeVariant>(3, false));
        MutableColumnPtr root_column_object = nullable_dt->create_column();
        nrows = 1000;
        st = iter->seek_to_ordinal(0);
        EXPECT_TRUE(st.ok()) << st.msg();
        st = iter->next_batch(&nrows, root_column_object);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(stats.bytes_read > 0);

        std::vector<rowid_t> row_ids1 = {0, 10, 100};
        root_column_object->clear();
        st = iter->read_by_rowids(row_ids1.data(), row_ids1.size(), root_column_object);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(root_column_object->size() == row_ids1.size());
        auto row_id = iter->get_current_ordinal();
        std::cout << "current row id: " << row_id << std::endl;
    }

    // 16. check compacton sparse column
    TabletColumn sparse_column =
            variant_sparse_hash_shard_count > 1
                    ? variant_util::create_sparse_shard_column(parent_column, 0)
                    : variant_util::create_sparse_column(parent_column);
    ColumnIteratorUPtr it4;
    st = variant_column_reader->new_iterator(&it4, &sparse_column, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<SparseColumnMergeIterator*>(it4.get()) != nullptr);
    st = it4->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();
    auto column_type = DataTypeFactory::instance().create_data_type(sparse_column, false);
    auto read_column = column_type->create_column();
    nrows = 1000;
    st = it4->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = it4->next_batch(&nrows, read_column);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(stats.bytes_read > 0);

    {
        // test SparseColumnMergeIterator seek_to_first
        auto iter = assert_cast<SparseColumnMergeIterator*>(it4.get());
        EXPECT_ANY_THROW(iter->get_current_ordinal());
        // and test read_by_rowids for 0 -> 1000
        std::vector<rowid_t> row_ids1;
        for (int i = 0; i < 1000; ++i) {
            row_ids1.push_back(i);
        }
        auto column_type1 = DataTypeFactory::instance().create_data_type(sparse_column, false);
        auto read_column1 = column_type1->create_column();
        st = iter->read_by_rowids(row_ids1.data(), row_ids1.size(), read_column1);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(read_column1->size() == row_ids1.size());
        // test _process_data_without_sparse_column
        std::cout << "_iter._src_subcolumn_map size : " << iter->_src_subcolumns_for_sparse.size()
                  << std::endl;
        std::cout << "_iter.root  " << iter->_src_subcolumns_for_sparse.empty() << std::endl;
        // fill with dst SparseMap
        MutableColumnPtr sparse_dst =
                ColumnMap::create(ColumnString::create(), ColumnString::create(),
                                  ColumnArray::ColumnOffsets::create());
        iter->_process_data_without_sparse_column(sparse_dst, 1);
        EXPECT_TRUE(sparse_dst->size() == 1);
    }
    //
    //    {
    //        // read with opt
    //        auto iter = assert_cast<SparseColumnMergeIterator*>(it4);
    //        StorageReadOptions storage_read_opts1;
    //        storage_read_opts1.io_ctx.reader_type = ReaderType::READER_QUERY;
    //        iter->_read_opts = &storage_read_opts1;
    //        auto read_column1 = column_type->create_column();
    //        st = iter->next_batch(&nrows, read_column1, nullptr);
    //        EXPECT_TRUE(st.ok()) << st.msg();
    //        EXPECT_TRUE(stats.bytes_read > 0);
    //        iter->_read_opts->io_ctx.reader_type = ReaderType::READER_BASE_COMPACTION;
    //        st = iter->next_batch(&nrows, read_column1, nullptr);
    //        EXPECT_TRUE(st.ok()) << st.msg();
    //    }

    for (int row = 0; row < 1000; ++row) {
        const std::string& value = column_type->to_string(*read_column, row);
        EXPECT_TRUE(value.find("key0") == std::string::npos)
                << "row: " << row << ", value: " << value;
        EXPECT_TRUE(value.find("key3") == std::string::npos)
                << "row: " << row << ", value: " << value;
        EXPECT_TRUE(value.find("key4") == std::string::npos)
                << "row: " << row << ", value: " << value;
    }

    // 17. check limit = 10000
    subcolumn.set_name(parent_column.name_lower_case() + ".key10");
    subcolumn.set_path_info(PathInData(parent_column.name_lower_case() + ".key10"));
    ColumnIteratorUPtr it5;
    st = variant_column_reader->new_iterator(&it5, &subcolumn, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<BinaryColumnExtractIterator*>(it5.get()) != nullptr);
    EXPECT_TRUE(it5->init(column_iter_opts).ok());

    {
        // test BinaryColumnExtractIterator seek_to_first
        auto iter = assert_cast<BinaryColumnExtractIterator*>(it5.get());
        EXPECT_TRUE(st.ok()) << st.msg();
        // and test read_by_rowids
        std::vector<rowid_t> row_ids1;
        for (int i = 0; i < 1000; ++i) {
            row_ids1.push_back(i);
        }
        MutableColumnPtr sparse_dst1 = ColumnVariant::create(3, false);
        st = iter->read_by_rowids(row_ids1.data(), row_ids1.size(), sparse_dst1);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(sparse_dst1->size() == row_ids1.size());
        // test to nullable column object
        std::cout << "test 2 " << std::endl;
        MutableColumnPtr sparse_dst2 =
                ColumnNullable::create(ColumnVariant::create(3, false), ColumnUInt8::create());
        st = iter->read_by_rowids(row_ids1.data(), row_ids1.size(), sparse_dst2);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(sparse_dst2->size() == row_ids1.size());
        std::cout << "test 3" << std::endl;
        MutableColumnPtr sparse_dst3 = ColumnVariant::create(3, false);
        size_t rs = 1000;
        bool has_null = false;
        st = iter->seek_to_ordinal(0);
        EXPECT_TRUE(st.ok()) << st.msg();
        st = iter->next_batch(&rs, sparse_dst3, &has_null);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(sparse_dst3->size() == row_ids1.size());
        // test _process_data_without_sparse_column
        // fill with dst SparseMap
        MutableColumnPtr sparse_dst =
                ColumnMap::create(ColumnString::create(), ColumnString::create(),
                                  ColumnArray::ColumnOffsets::create());
        iter->_process_data_without_sparse_column(sparse_dst, 1);
        EXPECT_TRUE(sparse_dst->size() == 1);
    }

    for (int i = 0; i < limit; ++i) {
        std::string key = parent_column.name_lower_case() + ".key10" + std::to_string(i);
        variant_stats->sparse_column_non_null_size.erase(key);
    }

    // 18. check compacton sparse extract column
    ColumnIteratorUPtr it6;
    subcolumn.set_name(parent_column.name_lower_case() + ".key3");
    subcolumn.set_path_info(PathInData(parent_column.name_lower_case() + ".key3"));
    st = variant_column_reader->new_iterator(&it6, &subcolumn, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<BinaryColumnExtractIterator*>(it6.get()) != nullptr);

    // 19. check compaction default column
    subcolumn.set_name(parent_column.name_lower_case() + ".key10");
    subcolumn.set_path_info(PathInData(parent_column.name_lower_case() + ".key10"));
    ColumnIteratorUPtr it7;
    st = variant_column_reader->new_iterator(&it7, &subcolumn, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<DefaultValueColumnIterator*>(it7.get()) != nullptr);
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_write_doc_and_read_hierarchical_doc) {
    constexpr int kRows = 200;
    constexpr int kDocBuckets = 2;

    // 1. create tablet_schema (enable doc mode, small shard count to keep footer small)
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 3, false, false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/true,
                     /*variant_doc_materialization_min_rows=*/100000,
                     /*variant_doc_hash_shard_count=*/kDocBuckets);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = false;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 31000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create variant writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn parent_column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, parent_column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &parent_column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write doc-value-only data into variant
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto column_object = (*std::move(block.get_by_position(0).column)).mutate();
    std::unordered_map<int, std::string> inserted_jsonstr;
    fill_variant_column_with_doc_value_only(column_object, kRows, &inserted_jsonstr);
    olap_data_convertor->add_column_data_convertor(parent_column);
    olap_data_convertor->set_source_content(&block, 0, kRows);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), kRows).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(kRows);

    // 6. validate footer contains doc snapshot bucket columns and per-bucket stats
    EXPECT_EQ(footer.columns_size(), 1 + kDocBuckets);
    for (int i = 1; i < footer.columns_size(); ++i) {
        const auto& col = footer.columns(i);
        EXPECT_TRUE(col.has_column_path_info());
        PathInData path;
        path.from_protobuf(col.column_path_info());
        auto rel = path.copy_pop_front().get_path();
        EXPECT_TRUE(rel.find(DOC_VALUE_COLUMN_PATH) != std::string::npos) << rel;
        EXPECT_TRUE(col.has_variant_statistics());
        EXPECT_GT(col.variant_statistics().doc_value_column_non_null_size_size(), 0);
    }

    // 7. open a VariantColumnReader on this segment
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);
    EXPECT_TRUE(variant_column_reader->get_stats()->has_doc_column_non_null_size());
    EXPECT_TRUE(variant_column_reader->get_subcolumn_meta_by_path(PathInData("key0")) == nullptr);

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    // 8. Read root with QUERY reader type: should choose ReadKind::HIERARCHICAL_DOC
    StorageReadOptions storage_read_opts;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    OlapReaderStatistics stats;
    storage_read_opts.stats = &stats;
    ColumnIteratorUPtr it;
    st = variant_column_reader->new_iterator(&it, &parent_column, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(dynamic_cast<HierarchicalDataIterator*>(it.get()) != nullptr);
    EXPECT_EQ(stats.variant_doc_value_column_iter_count, 1);

    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = it->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    MutableColumnPtr dst =
            ColumnVariant::create(parent_column.variant_max_subcolumns_count(), false);
    size_t nrows = kRows;
    st = it->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = it->next_batch(&nrows, dst);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_EQ(nrows, kRows);

    DataTypeSerDe::FormatOptions options;
    auto tz = cctz::utc_time_zone();
    options.timezone = &tz;
    for (int i = 0; i < kRows; ++i) {
        std::string value;
        assert_cast<ColumnVariant*>(dst.get())->serialize_one_row_to_string(i, &value, options);
        EXPECT_EQ(value, inserted_jsonstr[i]);
    }

    // The segment above is written by the legacy ColumnVariant writer. Read the same doc-value
    // streams into ColumnVariantV2 to cover the HIERARCHICAL_DOC query path.
    TabletColumn parent_column_v2 = parent_column;
    parent_column_v2.set_variant_is_v2(true);
    OlapReaderStatistics v2_stats;
    StorageReadOptions v2_read_opts;
    v2_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    v2_read_opts.stats = &v2_stats;
    ColumnIteratorUPtr v2_it;
    st = variant_column_reader->new_iterator(&v2_it, &parent_column_v2, &v2_read_opts,
                                             &column_reader_cache);
    ASSERT_TRUE(st.ok()) << st.msg();
    ASSERT_NE(dynamic_cast<HierarchicalDataIterator*>(v2_it.get()), nullptr);
    EXPECT_EQ(v2_stats.variant_doc_value_column_iter_count, 1);

    ColumnIteratorOptions v2_iter_opts;
    v2_iter_opts.stats = &v2_stats;
    v2_iter_opts.file_reader = file_reader.get();
    ASSERT_TRUE(v2_it->init(v2_iter_opts).ok());
    ASSERT_TRUE(v2_it->seek_to_ordinal(0).ok());
    MutableColumnPtr v2_dst =
            DataTypeVariantV2(parent_column.variant_max_subcolumns_count(), true).create_column();
    size_t v2_nrows = kRows;
    ASSERT_TRUE(v2_it->next_batch(&v2_nrows, v2_dst).ok());
    ASSERT_EQ(v2_nrows, kRows);
    const auto& v2_values = assert_cast<const ColumnVariantV2&>(*v2_dst);
    ASSERT_EQ(v2_values.size(), kRows);
    for (int i = 0; i < kRows; ++i) {
        EXPECT_EQ(variant_v2_json_at(v2_values, i), inserted_jsonstr[i]);
    }

    // Use a fresh iterator so random reads do not depend on the ordinal left by the sequential
    // scan above.
    OlapReaderStatistics v2_rowid_stats;
    StorageReadOptions v2_rowid_read_opts;
    v2_rowid_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    v2_rowid_read_opts.stats = &v2_rowid_stats;
    ColumnIteratorUPtr v2_rowid_it;
    st = variant_column_reader->new_iterator(&v2_rowid_it, &parent_column_v2, &v2_rowid_read_opts,
                                             &column_reader_cache);
    ASSERT_TRUE(st.ok()) << st.msg();
    ASSERT_NE(dynamic_cast<HierarchicalDataIterator*>(v2_rowid_it.get()), nullptr);
    EXPECT_EQ(v2_rowid_stats.variant_doc_value_column_iter_count, 1);

    ColumnIteratorOptions v2_rowid_iter_opts;
    v2_rowid_iter_opts.stats = &v2_rowid_stats;
    v2_rowid_iter_opts.file_reader = file_reader.get();
    ASSERT_TRUE(v2_rowid_it->init(v2_rowid_iter_opts).ok());
    const std::vector<rowid_t> rowids {0, 3, 57, kRows - 1};
    MutableColumnPtr v2_rowid_dst =
            DataTypeVariantV2(parent_column.variant_max_subcolumns_count(), true).create_column();
    ASSERT_TRUE(v2_rowid_it->read_by_rowids(rowids.data(), rowids.size(), v2_rowid_dst).ok());
    const auto& v2_rowid_values = assert_cast<const ColumnVariantV2&>(*v2_rowid_dst);
    ASSERT_EQ(v2_rowid_values.size(), rowids.size());
    for (size_t i = 0; i < rowids.size(); ++i) {
        EXPECT_EQ(variant_v2_json_at(v2_rowid_values, i), inserted_jsonstr[rowids[i]]);
    }
}

TEST_F(VariantColumnWriterReaderTest,
       test_write_doc_materialized_by_min_rows_and_read_metadata_and_data) {
    constexpr int kRows = 200;
    constexpr int kDocBuckets = 2;

    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 3, false, false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/true,
                     /*variant_doc_materialization_min_rows=*/0,
                     /*variant_doc_hash_shard_count=*/kDocBuckets);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    _tablet_schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 31002;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn parent_column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, parent_column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &parent_column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());

    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto column_object = (*std::move(block.get_by_position(0).column)).mutate();
    std::unordered_map<int, std::string> inserted_jsonstr;
    fill_variant_column_with_doc_value_only(column_object, kRows, &inserted_jsonstr);
    olap_data_convertor->add_column_data_convertor(parent_column);
    olap_data_convertor->set_source_content(&block, 0, kRows);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), kRows).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(kRows);

    EXPECT_GT(footer.columns_size(), 1 + kDocBuckets);

    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);
    EXPECT_TRUE(variant_column_reader->get_subcolumn_meta_by_path(PathInData("key0")) != nullptr);

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    StorageReadOptions query_read_opts;
    query_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    OlapReaderStatistics query_stats;
    query_read_opts.stats = &query_stats;
    ColumnIteratorUPtr root_it;
    st = variant_column_reader->new_iterator(&root_it, &parent_column, &query_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(dynamic_cast<HierarchicalDataIterator*>(root_it.get()) != nullptr);

    ColumnIteratorOptions root_iter_opts;
    root_iter_opts.stats = &query_stats;
    root_iter_opts.file_reader = file_reader.get();
    st = root_it->init(root_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    DataTypeSerDe::FormatOptions options;
    auto tz = cctz::utc_time_zone();
    options.timezone = &tz;
    MutableColumnPtr dst =
            ColumnVariant::create(parent_column.variant_max_subcolumns_count(), false);
    size_t nrows = kRows;
    st = root_it->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = root_it->next_batch(&nrows, dst);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_EQ(nrows, kRows);
    for (int i = 0; i < kRows; ++i) {
        std::string value;
        assert_cast<ColumnVariant*>(dst.get())->serialize_one_row_to_string(i, &value, options);
        EXPECT_EQ(value, inserted_jsonstr[i]);
    }

    TabletColumn parent_column_v2 = parent_column;
    parent_column_v2.set_variant_is_v2(true);
    StorageReadOptions v2_query_read_opts;
    v2_query_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    v2_query_read_opts.tablet_schema = _tablet_schema;
    OlapReaderStatistics v2_query_stats;
    v2_query_read_opts.stats = &v2_query_stats;
    ColumnIteratorUPtr v2_root_it;
    st = variant_column_reader->new_iterator(&v2_root_it, &parent_column_v2, &v2_query_read_opts,
                                             &column_reader_cache);
    ASSERT_TRUE(st.ok()) << st.msg();
    ASSERT_NE(dynamic_cast<HierarchicalDataIterator*>(v2_root_it.get()), nullptr);
    EXPECT_EQ(v2_query_stats.variant_doc_value_column_iter_count, 1);

    ColumnIteratorOptions v2_root_iter_opts;
    v2_root_iter_opts.stats = &v2_query_stats;
    v2_root_iter_opts.file_reader = file_reader.get();
    ASSERT_TRUE(v2_root_it->init(v2_root_iter_opts).ok());
    ASSERT_TRUE(v2_root_it->seek_to_ordinal(0).ok());

    MutableColumnPtr v2_dst =
            DataTypeVariantV2(parent_column.variant_max_subcolumns_count(), true).create_column();
    size_t v2_nrows = kRows;
    st = v2_root_it->next_batch(&v2_nrows, v2_dst);
    ASSERT_TRUE(st.ok()) << st.msg();
    ASSERT_EQ(v2_nrows, kRows);
    const auto& v2_variant = assert_cast<const ColumnVariantV2&>(*v2_dst);
    ASSERT_EQ(v2_variant.size(), kRows);
    ASSERT_FALSE(v2_variant.is_typed());
    for (int i = 0; i < kRows; ++i) {
        EXPECT_EQ(variant_v2_json_at(v2_variant, i), inserted_jsonstr[i]);
    }

    StorageReadOptions compact_read_opts;
    compact_read_opts.io_ctx.reader_type = ReaderType::READER_BASE_COMPACTION;
    compact_read_opts.tablet_schema = _tablet_schema;
    OlapReaderStatistics compact_stats;
    compact_read_opts.stats = &compact_stats;
    TabletColumn doc_bucket_col = variant_util::create_doc_value_column(parent_column, 0);
    ColumnIteratorUPtr bucket_it;
    st = variant_column_reader->new_iterator(&bucket_it, &doc_bucket_col, &compact_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(bucket_it != nullptr);

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

// Regression: materialized subcolumns in V3 doc-mode tablets must inherit the parent's
// storage_format and resolve V3 default encodings (e.g. integer family = PLAIN, not BIT_SHUFFLE).
// Without propagating base_opts.storage_format into the per-subcolumn ColumnWriterOptions,
// `_init_column_meta` falls back to the V2 default map and writes V2 encodings even for V3
// tablets, defeating the storage-format-based encoding policy.
TEST_F(VariantColumnWriterReaderTest, test_write_doc_materialized_v3_uses_v3_encoding) {
    constexpr int kRows = 200;
    constexpr int kDocBuckets = 2;

    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 3, false, false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/true,
                     /*variant_doc_materialization_min_rows=*/0,
                     /*variant_doc_hash_shard_count=*/kDocBuckets);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    _tablet_schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3);
    tablet_meta->_tablet_id = 31003;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn parent_column = _tablet_schema->column(0);
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3;
    _init_column_meta(opts.meta, 0, parent_column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &parent_column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());

    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto column_object = (*std::move(block.get_by_position(0).column)).mutate();
    std::unordered_map<int, std::string> inserted_jsonstr;
    fill_variant_column_with_doc_value_only(column_object, kRows, &inserted_jsonstr);
    olap_data_convertor->add_column_data_convertor(parent_column);
    olap_data_convertor->set_source_content(&block, 0, kRows);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), kRows).ok());
    EXPECT_TRUE(writer->finish().ok());
    EXPECT_TRUE(writer->write_data().ok());
    EXPECT_TRUE(writer->write_ordinal_index().ok());
    EXPECT_TRUE(file_writer->close().ok());

    // Materialization must have produced extra subcolumns beyond the doc-bucket columns.
    EXPECT_GT(footer.columns_size(), 1 + kDocBuckets) << "no subcolumns were materialized";

    // Locate materialized subcolumns. Doc bucket columns have DOC_VALUE_COLUMN_PATH in their
    // path; everything else (other than the root variant at index 0) is a materialized subcolumn.
    int integer_subcolumns_checked = 0;
    int string_subcolumns_checked = 0;
    for (int i = 1; i < footer.columns_size(); ++i) {
        const auto& col = footer.columns(i);
        if (!col.has_column_path_info()) continue;
        PathInData path;
        path.from_protobuf(col.column_path_info());
        std::string rel = path.copy_pop_front().get_path();
        if (rel.find(DOC_VALUE_COLUMN_PATH) != std::string::npos) continue;
        const auto field_type = static_cast<FieldType>(col.type());
        switch (field_type) {
        case FieldType::OLAP_FIELD_TYPE_TINYINT:
        case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        case FieldType::OLAP_FIELD_TYPE_INT:
        case FieldType::OLAP_FIELD_TYPE_BIGINT:
        case FieldType::OLAP_FIELD_TYPE_LARGEINT:
            EXPECT_EQ(col.encoding(), EncodingTypePB::PLAIN_ENCODING)
                    << "V3 integer subcolumn '" << rel << "' got "
                    << EncodingTypePB_Name(col.encoding()) << " instead of PLAIN_ENCODING";
            ++integer_subcolumns_checked;
            break;
        case FieldType::OLAP_FIELD_TYPE_CHAR:
        case FieldType::OLAP_FIELD_TYPE_VARCHAR:
        case FieldType::OLAP_FIELD_TYPE_STRING:
            EXPECT_EQ(col.encoding(), EncodingTypePB::DICT_ENCODING)
                    << "V3 string subcolumn '" << rel << "' got "
                    << EncodingTypePB_Name(col.encoding()) << " instead of DICT_ENCODING";
            ++string_subcolumns_checked;
            break;
        default:
            break;
        }
    }
    EXPECT_GT(integer_subcolumns_checked + string_subcolumns_checked, 0)
            << "no scalar materialized subcolumns were found to verify";

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_read_doc_compact_from_doc_value_bucket) {
    constexpr int kRows = 200;
    constexpr int kDocBuckets = 4;

    // 1. create tablet_schema (enable doc mode)
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 3, false, false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/true,
                     /*variant_doc_materialization_min_rows=*/0,
                     /*variant_doc_hash_shard_count=*/kDocBuckets);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    _tablet_schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 32000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. write doc-value-only segment
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn parent_column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, parent_column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &parent_column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());

    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto column_object = (*std::move(block.get_by_position(0).column)).mutate();
    std::unordered_map<int, std::string> inserted_jsonstr;
    fill_variant_column_with_doc_value_only(column_object, kRows, &inserted_jsonstr);
    olap_data_convertor->add_column_data_convertor(parent_column);
    olap_data_convertor->set_source_content(&block, 0, kRows);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), kRows).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(kRows);

    // 4. open reader
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    // 5. trigger flat-leaf planning by using compaction reader type + schema with extracted columns
    auto compaction_schema = std::make_shared<TabletSchema>();
    compaction_schema->init_from_pb(schema_pb);
    TabletColumn extracted;
    extracted.set_name(parent_column.name_lower_case() + ".dummy");
    extracted.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    extracted.set_parent_unique_id(parent_column.unique_id());
    extracted.set_path_info(PathInData(parent_column.name_lower_case() + ".dummy"));
    extracted.set_is_nullable(true);
    compaction_schema->append_column(extracted);

    StorageReadOptions storage_read_opts;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_BASE_COMPACTION;
    storage_read_opts.tablet_schema = compaction_schema;
    OlapReaderStatistics stats;
    storage_read_opts.stats = &stats;

    DataTypeSerDe::FormatOptions options;
    auto tz = cctz::utc_time_zone();
    options.timezone = &tz;

    ColumnIteratorUPtr root_it;
    st = variant_column_reader->new_iterator(&root_it, &parent_column, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(dynamic_cast<VariantRootColumnIterator*>(root_it.get()) != nullptr);

    // 6. Read and validate each doc value bucket column: should choose ReadKind::DOC_COMPACT.
    for (int bucket = 0; bucket < kDocBuckets; ++bucket) {
        TabletColumn doc_bucket_col = variant_util::create_doc_value_column(parent_column, bucket);
        ColumnIteratorUPtr it;
        st = variant_column_reader->new_iterator(&it, &doc_bucket_col, &storage_read_opts,
                                                 &column_reader_cache);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(dynamic_cast<segment_v2::VariantDocValueCompactIterator*>(it.get()) != nullptr);

        ColumnIteratorOptions column_iter_opts;
        column_iter_opts.stats = &stats;
        column_iter_opts.file_reader = file_reader.get();
        st = it->init(column_iter_opts);
        EXPECT_TRUE(st.ok()) << st.msg();

        MutableColumnPtr dst =
                ColumnVariant::create(parent_column.variant_max_subcolumns_count(), false);
        size_t nrows = kRows;
        st = it->seek_to_ordinal(0);
        EXPECT_TRUE(st.ok()) << st.msg();
        st = it->next_batch(&nrows, dst);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_EQ(nrows, kRows);

        for (int i = 0; i < kRows; ++i) {
            std::string value;
            assert_cast<ColumnVariant*>(dst.get())->serialize_one_row_to_string(i, &value, options);
            const std::string expected =
                    expected_doc_bucket_json_from_full(inserted_jsonstr[i], kDocBuckets, bucket);
            EXPECT_EQ(value, expected);
        }
    }
}

TEST_P(VariantSpecializedWriterCompatibilityTest, doc_compact_writer_round_trip) {
    constexpr int kRows = 200;
    constexpr int kDocBuckets = 4;
    constexpr int kBucket = 0;

    // 1. create tablet_schema: root variant is in doc mode; plus one extracted doc bucket column
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 3, false, false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/true,
                     /*variant_doc_materialization_min_rows=*/0,
                     /*variant_doc_hash_shard_count=*/kDocBuckets);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    TabletColumn parent_column = _tablet_schema->column(0);
    TabletColumn extracted_doc_bucket =
            variant_util::create_doc_value_column(parent_column, kBucket);
    // This matches VariantCompactionUtil::get_extended_compaction_schema behavior:
    // extracted doc bucket columns are represented as VARIANT to trigger VariantDocCompactWriter.
    extracted_doc_bucket.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    extracted_doc_bucket.set_is_nullable(false);
    _tablet_schema->append_column(extracted_doc_bucket);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    _tablet_schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 33000 + static_cast<int>(GetParam());
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(
            _tablet->tablet_path(),
            "specialized_doc_compact_" + variant_writer_input_name(GetParam()), 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column writers: root VariantColumnWriter + extracted VariantDocCompactWriter
    SegmentFooterPB footer;

    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    rowset_ctx.tablet_schema = _tablet_schema;

    ColumnWriterOptions root_opts;
    root_opts.meta = footer.add_columns();
    root_opts.compression_type = CompressionTypePB::LZ4;
    root_opts.file_writer = file_writer.get();
    root_opts.footer = &footer;
    root_opts.rowset_ctx = &rowset_ctx;
    root_opts.compression_type = CompressionTypePB::LZ4;
    root_opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(root_opts.meta, 0, parent_column, root_opts);

    std::unique_ptr<ColumnWriter> root_writer;
    EXPECT_TRUE(
            ColumnWriter::create(root_opts, &parent_column, file_writer.get(), &root_writer).ok());
    EXPECT_TRUE(root_writer->init().ok());

    TabletColumn extracted_doc_bucket_col = _tablet_schema->column(1);
    ColumnWriterOptions doc_compact_opts = root_opts;
    doc_compact_opts.meta = footer.add_columns();
    doc_compact_opts.compression_type = CompressionTypePB::LZ4;
    doc_compact_opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(doc_compact_opts.meta, 0, extracted_doc_bucket_col, doc_compact_opts);
    std::unique_ptr<ColumnWriter> doc_compact_writer;
    EXPECT_TRUE(ColumnWriter::create(doc_compact_opts, &extracted_doc_bucket_col, file_writer.get(),
                                     &doc_compact_writer)
                        .ok());
    EXPECT_TRUE(doc_compact_writer->init().ok());

    // 5. build doc-value-only data:
    // - root column uses the full JSON (doc values only is enough for this test)
    // - extracted doc bucket column uses bucket-filtered JSON so that doc bucket data matches
    //   the bucket index expected by VariantDocCompactWriter.
    std::unordered_map<int, std::string> inserted_full_json;
    auto type_string = std::make_shared<DataTypeString>();
    auto full_json_column = type_string->create_column();
    auto* full_strings = assert_cast<ColumnString*>(full_json_column.get());
    VariantUtil::fill_string_column_with_test_data(full_strings, kRows, &inserted_full_json);

    std::unordered_map<int, std::string> expected_bucket_json;
    for (int i = 0; i < kRows; ++i) {
        const std::string& full = inserted_full_json[i];
        std::string bucket_json = expected_doc_bucket_json_from_full(full, kDocBuckets, kBucket);
        expected_bucket_json.emplace(i, bucket_json);
    }

    std::vector<std::string> full_jsons;
    std::vector<std::string> bucket_jsons;
    full_jsons.reserve(kRows);
    bucket_jsons.reserve(kRows);
    for (int row = 0; row < kRows; ++row) {
        full_jsons.push_back(inserted_full_json[row]);
        bucket_jsons.push_back(expected_bucket_json[row]);
    }
    ColumnPtr root_variant;
    DataTypePtr root_type;
    ASSERT_TRUE(create_variant_writer_source(GetParam(), full_jsons,
                                             parent_column.variant_max_subcolumns_count(), true, {},
                                             &root_variant, &root_type)
                        .ok());
    ASSERT_NE(root_type, nullptr);
    ColumnPtr bucket_variant;
    DataTypePtr bucket_type;
    ASSERT_TRUE(create_variant_writer_source(GetParam(), bucket_jsons,
                                             parent_column.variant_max_subcolumns_count(), true, {},
                                             &bucket_variant, &bucket_type)
                        .ok());
    ASSERT_NE(bucket_type, nullptr);
    const VariantWriterInput other_input =
            GetParam() == VariantWriterInput::V1 ? VariantWriterInput::V2 : VariantWriterInput::V1;
    ColumnPtr other_bucket_variant;
    DataTypePtr other_bucket_type;
    ASSERT_TRUE(create_variant_writer_source(other_input, bucket_jsons,
                                             parent_column.variant_max_subcolumns_count(), true, {},
                                             &other_bucket_variant, &other_bucket_type)
                        .ok());
    ASSERT_NE(other_bucket_type, nullptr);

    // 6. append and write
    const auto append_batch = [](ColumnWriter* writer, const ColumnPtr& source, size_t row_pos,
                                 size_t num_rows) {
        VariantColumnData column_data {source.get(), row_pos};
        const auto* data = reinterpret_cast<const uint8_t*>(&column_data);
        return writer->append_data(&data, num_rows);
    };
    constexpr size_t kFirstBatchRows = 73;
    ASSERT_TRUE(append_batch(root_writer.get(), root_variant, 0, kFirstBatchRows).ok());
    ASSERT_TRUE(
            append_batch(root_writer.get(), root_variant, kFirstBatchRows, kRows - kFirstBatchRows)
                    .ok());
    ASSERT_TRUE(append_batch(doc_compact_writer.get(), bucket_variant, 0, kFirstBatchRows).ok());
    const Status mixed_status = append_batch(doc_compact_writer.get(), other_bucket_variant, 0, 1);
    EXPECT_FALSE(mixed_status.ok());
    EXPECT_NE(mixed_status.to_string().find("representation changed within one segment"),
              std::string::npos);
    EXPECT_EQ(doc_compact_writer->get_next_rowid(), kFirstBatchRows);
    ASSERT_TRUE(append_batch(doc_compact_writer.get(), bucket_variant, kFirstBatchRows,
                             kRows - kFirstBatchRows)
                        .ok());
    EXPECT_EQ(root_writer->get_next_rowid(), kRows);
    EXPECT_EQ(doc_compact_writer->get_next_rowid(), kRows);

    EXPECT_TRUE(root_writer->finish().ok());
    EXPECT_TRUE(doc_compact_writer->finish().ok());
    EXPECT_TRUE(root_writer->write_data().ok());
    EXPECT_TRUE(doc_compact_writer->write_data().ok());
    EXPECT_TRUE(root_writer->write_ordinal_index().ok());
    EXPECT_TRUE(doc_compact_writer->write_ordinal_index().ok());
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(kRows);

    const auto* doc_meta = find_footer_column_meta_by_relative_path(
            footer, DOC_VALUE_COLUMN_PATH + ".b" + std::to_string(kBucket));
    ASSERT_NE(doc_meta, nullptr);
    EXPECT_EQ(doc_meta->type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_MAP));
    EXPECT_FALSE(doc_meta->variant_statistics().doc_value_column_non_null_size().empty());

    // 7. open reader and validate:
    // - doc bucket can be read via DOC_COMPACT iterator in flat-leaf compaction mode
    // - materialized leaf meta exists for at least one key in this bucket
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    bool checked_one_key = false;
    for (int j = 0; j < 10; ++j) {
        const std::string key = "key" + std::to_string(j);
        StringRef ref {key.data(), key.size()};
        if (variant_util::variant_binary_shard_of(ref, kDocBuckets) ==
            static_cast<uint32_t>(kBucket)) {
            EXPECT_TRUE(variant_column_reader->get_subcolumn_meta_by_path(PathInData(key)) !=
                        nullptr);
            checked_one_key = true;
            break;
        }
    }
    EXPECT_TRUE(checked_one_key);

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);
    StorageReadOptions storage_read_opts;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_BASE_COMPACTION;
    storage_read_opts.tablet_schema = _tablet_schema;
    OlapReaderStatistics stats;
    storage_read_opts.stats = &stats;

    TabletColumn doc_bucket_map = variant_util::create_doc_value_column(parent_column, kBucket);
    ColumnIteratorUPtr it;
    st = variant_column_reader->new_iterator(&it, &doc_bucket_map, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(dynamic_cast<segment_v2::VariantDocValueCompactIterator*>(it.get()) != nullptr);

    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = it->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    DataTypeSerDe::FormatOptions options;
    auto tz = cctz::utc_time_zone();
    options.timezone = &tz;

    MutableColumnPtr dst =
            ColumnVariant::create(parent_column.variant_max_subcolumns_count(), false);
    size_t nrows = kRows;
    st = it->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = it->next_batch(&nrows, dst);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_EQ(nrows, kRows);

    for (int i = 0; i < kRows; ++i) {
        std::string value;
        assert_cast<ColumnVariant*>(dst.get())->serialize_one_row_to_string(i, &value, options);
        EXPECT_EQ(value, expected_bucket_json[i]);
    }

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

INSTANTIATE_TEST_SUITE_P(V1AndV2SpecializedWriters, VariantSpecializedWriterCompatibilityTest,
                         testing::Values(VariantWriterInput::V1, VariantWriterInput::V2),
                         variant_writer_test_name);

TEST_F(VariantColumnWriterReaderTest, test_doc_compact_sparse_write_array_gap) {
    constexpr int kRows = 2;
    constexpr int kDocBuckets = 1;
    constexpr int kBucket = 0;

    struct ConfigGuard {
        bool old_value;
        ~ConfigGuard() { config::enable_variant_doc_sparse_write_subcolumns = old_value; }
    };
    ConfigGuard guard {config::enable_variant_doc_sparse_write_subcolumns};
    config::enable_variant_doc_sparse_write_subcolumns = true;

    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 3, false, false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/true,
                     /*variant_doc_materialization_min_rows=*/0,
                     /*variant_doc_hash_shard_count=*/kDocBuckets);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    TabletColumn parent_column = _tablet_schema->column(0);
    TabletColumn extracted_doc_bucket =
            variant_util::create_doc_value_column(parent_column, kBucket);
    extracted_doc_bucket.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    extracted_doc_bucket.set_is_nullable(false);
    _tablet_schema->append_column(extracted_doc_bucket);

    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    _tablet_schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 33001;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    SegmentFooterPB footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    rowset_ctx.tablet_schema = _tablet_schema;

    TabletColumn extracted_doc_bucket_col = _tablet_schema->column(1);
    ColumnWriterOptions doc_compact_opts;
    doc_compact_opts.meta = footer.add_columns();
    doc_compact_opts.compression_type = CompressionTypePB::LZ4;
    doc_compact_opts.file_writer = file_writer.get();
    doc_compact_opts.footer = &footer;
    doc_compact_opts.rowset_ctx = &rowset_ctx;
    doc_compact_opts.compression_type = CompressionTypePB::LZ4;
    doc_compact_opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(doc_compact_opts.meta, 0, extracted_doc_bucket_col, doc_compact_opts);

    std::unique_ptr<ColumnWriter> doc_compact_writer;
    EXPECT_TRUE(ColumnWriter::create(doc_compact_opts, &extracted_doc_bucket_col, file_writer.get(),
                                     &doc_compact_writer)
                        .ok());
    EXPECT_TRUE(doc_compact_writer->init().ok());

    auto type_string = std::make_shared<DataTypeString>();
    auto json_column = type_string->create_column();
    auto* strings = assert_cast<ColumnString*>(json_column.get());
    const std::string row0 = R"({"arr":[1,2]})";
    const std::string row1 = R"({})";
    strings->insert_data(row0.data(), row0.size());
    strings->insert_data(row1.data(), row1.size());

    ParseConfig parse_cfg;
    parse_cfg.deprecated_enable_flatten_nested = false;
    parse_cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;

    MutableColumnPtr bucket_variant = ColumnVariant::create(
            parent_column.variant_max_subcolumns_count(), parent_column.variant_enable_doc_mode());
    variant_util::parse_json_to_variant(*bucket_variant, *strings, parse_cfg);

    auto bucket_data = std::make_unique<VariantColumnData>();
    bucket_data->column_data = bucket_variant.get();
    bucket_data->row_pos = 0;
    const auto* data = reinterpret_cast<const uint8_t*>(bucket_data.get());
    EXPECT_TRUE(doc_compact_writer->append_data(&data, kRows).ok());

    EXPECT_TRUE(doc_compact_writer->finish().ok());
    EXPECT_TRUE(doc_compact_writer->write_data().ok());
    EXPECT_TRUE(doc_compact_writer->write_ordinal_index().ok());
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(kRows);

    bool found_arr = false;
    for (int i = 0; i < footer.columns_size(); ++i) {
        const auto& col = footer.columns(i);
        if (!col.has_column_path_info()) {
            continue;
        }
        PathInData path;
        path.from_protobuf(col.column_path_info());
        if (path.copy_pop_front().get_path() == "arr") {
            EXPECT_EQ(col.type(), (int)FieldType::OLAP_FIELD_TYPE_ARRAY);
            EXPECT_TRUE(col.is_nullable());
            found_arr = true;
            break;
        }
    }
    EXPECT_TRUE(found_arr);

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_write_doc_sparse_write_array_gap_and_read) {
    constexpr int kRows = 2;
    constexpr int kDocBuckets = 1;

    struct ConfigGuard {
        bool old_value;
        ~ConfigGuard() { config::enable_variant_doc_sparse_write_subcolumns = old_value; }
    };
    ConfigGuard guard {config::enable_variant_doc_sparse_write_subcolumns};
    config::enable_variant_doc_sparse_write_subcolumns = true;

    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 3, false, false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/true,
                     /*variant_doc_materialization_min_rows=*/0,
                     /*variant_doc_hash_shard_count=*/kDocBuckets);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    _tablet_schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 33002;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    SegmentFooterPB footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    rowset_ctx.tablet_schema = _tablet_schema;

    TabletColumn parent_column = _tablet_schema->column(0);
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    opts.rowset_ctx = &rowset_ctx;
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, parent_column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &parent_column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());

    auto type_string = std::make_shared<DataTypeString>();
    auto json_column = type_string->create_column();
    auto* strings = assert_cast<ColumnString*>(json_column.get());
    std::unordered_map<int, std::string> inserted_json;
    inserted_json.emplace(0, R"({"arr":[1,2]})");
    inserted_json.emplace(1, R"({})");
    strings->insert_data(inserted_json[0].data(), inserted_json[0].size());
    strings->insert_data(inserted_json[1].data(), inserted_json[1].size());

    ParseConfig parse_cfg;
    parse_cfg.deprecated_enable_flatten_nested = false;
    parse_cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;

    MutableColumnPtr variant_column = ColumnVariant::create(
            parent_column.variant_max_subcolumns_count(), parent_column.variant_enable_doc_mode());
    variant_util::parse_json_to_variant(*variant_column, *strings, parse_cfg);

    auto variant_data = std::make_unique<VariantColumnData>();
    variant_data->column_data = variant_column.get();
    variant_data->row_pos = 0;
    const auto* data = reinterpret_cast<const uint8_t*>(variant_data.get());
    EXPECT_TRUE(writer->append_data(&data, kRows).ok());

    EXPECT_TRUE(writer->finish().ok());
    EXPECT_TRUE(writer->write_data().ok());
    EXPECT_TRUE(writer->write_ordinal_index().ok());
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(kRows);

    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    const auto* arr_node = variant_column_reader->get_subcolumn_meta_by_path(PathInData("arr"));
    EXPECT_TRUE(arr_node != nullptr);

    bool found_arr_meta = false;
    for (int i = 0; i < footer.columns_size(); ++i) {
        const auto& col = footer.columns(i);
        if (!col.has_column_path_info()) {
            continue;
        }
        PathInData path;
        path.from_protobuf(col.column_path_info());
        if (path.copy_pop_front().get_path() == "arr") {
            EXPECT_EQ(col.type(), (int)FieldType::OLAP_FIELD_TYPE_ARRAY);
            EXPECT_TRUE(col.is_nullable());
            found_arr_meta = true;
            break;
        }
    }
    EXPECT_TRUE(found_arr_meta);

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);
    StorageReadOptions storage_read_opts;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    OlapReaderStatistics stats;
    storage_read_opts.stats = &stats;
    ColumnIteratorUPtr it;
    st = variant_column_reader->new_iterator(&it, &parent_column, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();

    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = it->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    MutableColumnPtr dst =
            ColumnVariant::create(parent_column.variant_max_subcolumns_count(), false);
    size_t nrows = kRows;
    st = it->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = it->next_batch(&nrows, dst);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_EQ(nrows, kRows);

    DataTypeSerDe::FormatOptions options;
    auto tz = cctz::utc_time_zone();
    options.timezone = &tz;
    for (int i = 0; i < kRows; ++i) {
        std::string value;
        assert_cast<ColumnVariant*>(dst.get())->serialize_one_row_to_string(i, &value, options);
        if (i == 0) {
            EXPECT_EQ(value, "{\"arr\":[1, 2]}");
        } else {
            EXPECT_EQ(value, "{}");
        }
    }

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_storage_parse_kv_write_materialized_and_sparse) {
    constexpr int kRows = 4;

    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1",
                     /*variant_max_subcolumns_count=*/2,
                     /*is_key=*/false,
                     /*is_nullable=*/false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/false);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    _tablet_schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 33003;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    ASSERT_TRUE(_tablet->init().ok());
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    ASSERT_TRUE(st.ok()) << st.msg();

    SegmentFooterPB footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    rowset_ctx.tablet_schema = _tablet_schema;

    TabletColumn parent_column = _tablet_schema->column(0);
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    opts.rowset_ctx = &rowset_ctx;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, parent_column, opts);

    std::unique_ptr<ColumnWriter> writer;
    ASSERT_TRUE(ColumnWriter::create(opts, &parent_column, file_writer.get(), &writer).ok());
    ASSERT_TRUE(writer->init().ok());

    const std::vector<std::string> jsons = {
            R"({"hot":1,"warm":10,"cold0":100})",
            R"({"hot":2,"warm":20,"cold1":101})",
            R"({"hot":3,"warm":30,"cold2":102})",
            R"({"hot":4,"warm":40,"cold3":103})",
    };

    Block block = _tablet_schema->create_block();
    auto columns = std::move(block).mutate_columns();
    auto scalar_variant = ColumnVariant::create(0, false);
    for (const auto& json : jsons) {
        VariantUtil::insert_root_scalar_field(*scalar_variant,
                                              Field::create_field<TYPE_STRING>(String(json)));
    }
    columns[0] = std::move(scalar_variant);
    block.set_columns(std::move(columns));

    st = variant_util::parse_and_materialize_variant_columns(block, *_tablet_schema, {0});
    ASSERT_TRUE(st.ok()) << st.to_string();
    const auto& parsed_variant =
            assert_cast<const ColumnVariant&>(*block.get_by_position(0).column);
    EXPECT_EQ(parsed_variant.get_subcolumn(PathInData("hot")), nullptr);
    EXPECT_EQ(parsed_variant.get_subcolumn(PathInData("warm")), nullptr);
    ASSERT_FALSE(parsed_variant.serialized_doc_value_column_offsets().empty());
    EXPECT_EQ(parsed_variant.serialized_doc_value_column_offsets().back(), kRows * 3);

    auto converter = std::make_unique<OlapBlockDataConvertor>();
    converter->add_column_data_convertor(parent_column);
    converter->set_source_content(&block, 0, kRows);
    auto [convert_status, accessor] = converter->convert_column_data(0);
    ASSERT_TRUE(convert_status.ok()) << convert_status.to_string();
    ASSERT_NE(accessor, nullptr);
    ASSERT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), kRows).ok());

    ASSERT_TRUE(writer->finish().ok());
    ASSERT_TRUE(writer->write_data().ok());
    ASSERT_TRUE(writer->write_ordinal_index().ok());
    ASSERT_TRUE(writer->write_zone_map().ok());
    ASSERT_TRUE(file_writer->close().ok());
    footer.set_num_rows(kRows);

    EXPECT_EQ(footer.columns_size(), 4);

    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    ASSERT_NE(variant_column_reader, nullptr);

    EXPECT_NE(variant_column_reader->get_subcolumn_meta_by_path(PathInData("hot")), nullptr);
    EXPECT_NE(variant_column_reader->get_subcolumn_meta_by_path(PathInData("warm")), nullptr);
    for (int i = 0; i < kRows; ++i) {
        EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(
                PathInData("cold" + std::to_string(i))));
    }

    const auto* stats = variant_column_reader->get_stats();
    ASSERT_NE(stats, nullptr);
    EXPECT_EQ(stats->subcolumns_non_null_size.at("hot"), kRows);
    EXPECT_EQ(stats->subcolumns_non_null_size.at("warm"), kRows);
    for (int i = 0; i < kRows; ++i) {
        EXPECT_EQ(stats->sparse_column_non_null_size.at("cold" + std::to_string(i)), 1);
    }

    std::vector<std::string> actual_rows;
    st = read_root_rows(footer, file_path, &actual_rows);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(actual_rows,
              normalize_json_rows(jsons, parent_column.variant_max_subcolumns_count()));

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest,
       test_storage_parse_kv_write_typed_path_materialized_with_storage_type) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1",
                     /*variant_max_subcolumns_count=*/1,
                     /*is_key=*/false,
                     /*is_nullable=*/false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/false);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    auto typed_path = make_int_typed_path_template("typed_i");
    _tablet_schema->mutable_column_by_uid(1).add_sub_column(typed_path);
    init_tablet_from_current_schema(33007);

    const std::vector<std::string> jsons = {
            R"({"typed_i":1,"hot":"a","cold0":100})",
            R"({"typed_i":2,"hot":"b","cold1":101})",
            R"({"hot":"c","cold2":102})",
    };

    SegmentFooterPB footer;
    std::string file_path;
    auto st = write_storage_parsed_segment(jsons, "typed_materialized", &footer, &file_path);
    ASSERT_TRUE(st.ok()) << st.to_string();

    const auto* typed_meta = find_footer_column_meta_by_relative_path(footer, "typed_i");
    ASSERT_NE(typed_meta, nullptr);
    EXPECT_EQ(typed_meta->type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_INT));
    EXPECT_TRUE(typed_meta->is_nullable());
    EXPECT_FALSE(typed_meta->has_none_null_size());

    const auto* hot_meta = find_footer_column_meta_by_relative_path(footer, "hot");
    ASSERT_NE(hot_meta, nullptr);
    EXPECT_EQ(hot_meta->none_null_size(), jsons.size());

    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    ASSERT_NE(variant_column_reader, nullptr);
    EXPECT_NE(variant_column_reader->get_subcolumn_meta_by_path(PathInData("typed_i")), nullptr);
    EXPECT_NE(variant_column_reader->get_subcolumn_meta_by_path(PathInData("hot")), nullptr);
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("cold0")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("cold1")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("cold2")));
    const auto* stats = variant_column_reader->get_stats();
    ASSERT_NE(stats, nullptr);
    EXPECT_FALSE(stats->subcolumns_non_null_size.contains("typed_i"));
    EXPECT_EQ(stats->subcolumns_non_null_size.at("hot"), jsons.size());

    std::vector<std::string> actual_rows;
    st = read_root_rows(footer, file_path, &actual_rows);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(actual_rows,
              normalize_json_rows(jsons, _tablet_schema->column(0).variant_max_subcolumns_count()));

    std::vector<std::string> typed_values;
    st = read_variant_path_rows(footer, file_path, "typed_i", FieldType::OLAP_FIELD_TYPE_INT,
                                &typed_values);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(typed_values, (std::vector<std::string> {"1", "2", "NULL"}));

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_storage_parse_kv_write_typed_path_sparse_fallback) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    auto* root_pb = schema_pb.add_column();
    construct_column(root_pb, 1, "VARIANT", "V1",
                     /*variant_max_subcolumns_count=*/1,
                     /*is_key=*/false,
                     /*is_nullable=*/false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/false);
    root_pb->set_variant_enable_typed_paths_to_sparse(true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    auto typed_path = make_int_typed_path_template("typed_i");
    _tablet_schema->mutable_column_by_uid(1).add_sub_column(typed_path);
    init_tablet_from_current_schema(33008);

    const std::vector<std::string> jsons = {
            R"({"typed_i":1,"hot":"a"})",
            R"({"typed_i":2,"hot":"b"})",
            R"({"hot":"c"})",
    };

    SegmentFooterPB footer;
    std::string file_path;
    auto st = write_storage_parsed_segment(jsons, "typed_sparse", &footer, &file_path);
    ASSERT_TRUE(st.ok()) << st.to_string();

    EXPECT_EQ(find_footer_column_meta_by_relative_path(footer, "typed_i"), nullptr);
    const auto* hot_meta = find_footer_column_meta_by_relative_path(footer, "hot");
    ASSERT_NE(hot_meta, nullptr);
    EXPECT_EQ(hot_meta->none_null_size(), jsons.size());

    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    ASSERT_NE(variant_column_reader, nullptr);
    EXPECT_EQ(variant_column_reader->get_subcolumn_meta_by_path(PathInData("typed_i")), nullptr);
    EXPECT_NE(variant_column_reader->get_subcolumn_meta_by_path(PathInData("hot")), nullptr);
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("typed_i")));

    const auto* stats = variant_column_reader->get_stats();
    ASSERT_NE(stats, nullptr);
    EXPECT_EQ(stats->subcolumns_non_null_size.at("hot"), jsons.size());
    EXPECT_EQ(stats->sparse_column_non_null_size.at("typed_i"), 2);

    std::vector<std::string> actual_rows;
    st = read_root_rows(footer, file_path, &actual_rows);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(actual_rows,
              normalize_json_rows(jsons, _tablet_schema->column(0).variant_max_subcolumns_count()));

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest,
       test_storage_parse_kv_write_glob_typed_path_materialized_with_storage_type) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1",
                     /*variant_max_subcolumns_count=*/1,
                     /*is_key=*/false,
                     /*is_nullable=*/false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/false);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    auto typed_path = make_int_typed_path_template("typed_*", PatternTypePB::MATCH_NAME_GLOB);
    _tablet_schema->mutable_column_by_uid(1).add_sub_column(typed_path);
    init_tablet_from_current_schema(33010);

    const std::vector<std::string> jsons = {
            R"({"typed_g":1,"hot":"a","cold0":100})",
            R"({"typed_g":2,"hot":"b","cold1":101})",
            R"({"hot":"c","cold2":102})",
    };

    SegmentFooterPB footer;
    std::string file_path;
    auto st = write_storage_parsed_segment(jsons, "glob_typed_materialized", &footer, &file_path);
    ASSERT_TRUE(st.ok()) << st.to_string();

    const auto* typed_meta = find_footer_column_meta_by_relative_path(footer, "typed_g");
    ASSERT_NE(typed_meta, nullptr);
    EXPECT_EQ(typed_meta->type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_INT));
    EXPECT_TRUE(typed_meta->is_nullable());
    EXPECT_FALSE(typed_meta->has_none_null_size());

    const auto* hot_meta = find_footer_column_meta_by_relative_path(footer, "hot");
    ASSERT_NE(hot_meta, nullptr);
    EXPECT_EQ(hot_meta->none_null_size(), jsons.size());

    std::vector<std::string> actual_rows;
    st = read_root_rows(footer, file_path, &actual_rows);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(actual_rows,
              normalize_json_rows(jsons, _tablet_schema->column(0).variant_max_subcolumns_count()));

    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    ASSERT_NE(variant_column_reader, nullptr);
    const auto* stats = variant_column_reader->get_stats();
    ASSERT_NE(stats, nullptr);
    EXPECT_FALSE(stats->subcolumns_non_null_size.contains("typed_g"));
    EXPECT_EQ(stats->subcolumns_non_null_size.at("hot"), jsons.size());

    std::vector<std::string> typed_values;
    st = read_variant_path_rows(footer, file_path, "typed_g", FieldType::OLAP_FIELD_TYPE_INT,
                                &typed_values);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(typed_values, (std::vector<std::string> {"1", "2", "NULL"}));

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_storage_parse_kv_write_glob_typed_path_sparse_fallback) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    auto* root_pb = schema_pb.add_column();
    construct_column(root_pb, 1, "VARIANT", "V1",
                     /*variant_max_subcolumns_count=*/1,
                     /*is_key=*/false,
                     /*is_nullable=*/false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/false);
    root_pb->set_variant_enable_typed_paths_to_sparse(true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    auto typed_path = make_int_typed_path_template("typed_*", PatternTypePB::MATCH_NAME_GLOB);
    _tablet_schema->mutable_column_by_uid(1).add_sub_column(typed_path);
    init_tablet_from_current_schema(33011);

    const std::vector<std::string> jsons = {
            R"({"typed_g":1,"hot":"a"})",
            R"({"typed_g":2,"hot":"b"})",
            R"({"hot":"c"})",
    };

    SegmentFooterPB footer;
    std::string file_path;
    auto st = write_storage_parsed_segment(jsons, "glob_typed_sparse", &footer, &file_path);
    ASSERT_TRUE(st.ok()) << st.to_string();

    EXPECT_EQ(find_footer_column_meta_by_relative_path(footer, "typed_g"), nullptr);
    const auto* hot_meta = find_footer_column_meta_by_relative_path(footer, "hot");
    ASSERT_NE(hot_meta, nullptr);
    EXPECT_EQ(hot_meta->none_null_size(), jsons.size());

    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    ASSERT_NE(variant_column_reader, nullptr);
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("typed_g")));

    const auto* stats = variant_column_reader->get_stats();
    ASSERT_NE(stats, nullptr);
    EXPECT_EQ(stats->subcolumns_non_null_size.at("hot"), jsons.size());
    EXPECT_EQ(stats->sparse_column_non_null_size.at("typed_g"), 2);

    std::vector<std::string> actual_rows;
    st = read_root_rows(footer, file_path, &actual_rows);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(actual_rows,
              normalize_json_rows(jsons, _tablet_schema->column(0).variant_max_subcolumns_count()));

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest,
       test_storage_parse_kv_write_parent_index_topn_materialized_only) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1",
                     /*variant_max_subcolumns_count=*/1,
                     /*is_key=*/false,
                     /*is_nullable=*/false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/false);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    TabletIndexPB parent_index_pb;
    construct_tablet_index(&parent_index_pb, 10007, "idx_v1",
                           _tablet_schema->column(0).unique_id());
    TabletIndex parent_index;
    parent_index.init_from_pb(parent_index_pb);
    _tablet_schema->append_index(std::move(parent_index));
    init_tablet_from_current_schema(33009);

    const std::vector<std::string> jsons = {
            R"({"hot":"a","cold0":"x"})",
            R"({"hot":"b","cold1":"y"})",
            R"({"hot":"c","cold2":"z"})",
    };

    SegmentFooterPB footer;
    std::string file_path;
    auto st = write_storage_parsed_segment(jsons, "parent_index", &footer, &file_path,
                                           true /* write_inverted_index */);
    ASSERT_TRUE(st.ok()) << st.to_string();

    const auto* hot_meta = find_footer_column_meta_by_relative_path(footer, "hot");
    ASSERT_NE(hot_meta, nullptr);
    EXPECT_EQ(hot_meta->none_null_size(), jsons.size());
    EXPECT_EQ(find_footer_column_meta_by_relative_path(footer, "cold0"), nullptr);

    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    ASSERT_NE(variant_column_reader, nullptr);
    EXPECT_NE(variant_column_reader->get_subcolumn_meta_by_path(PathInData("hot")), nullptr);
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("cold0")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("cold1")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("cold2")));

    TabletColumn hot_subcolumn;
    hot_subcolumn.set_name("v1.hot");
    hot_subcolumn.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    hot_subcolumn.set_parent_unique_id(_tablet_schema->column(0).unique_id());
    hot_subcolumn.set_path_info(PathInData("v1.hot"));
    hot_subcolumn.set_is_nullable(true);
    auto indexes = variant_column_reader->find_subcolumn_tablet_indexes(
            hot_subcolumn, std::make_shared<DataTypeString>());
    ASSERT_EQ(indexes.size(), 1);
    EXPECT_EQ(indexes[0]->index_id(), 10007);
    EXPECT_EQ(indexes[0]->get_index_suffix(), "v1%2Ehot");

    std::vector<std::string> actual_rows;
    st = read_root_rows(footer, file_path, &actual_rows);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(actual_rows,
              normalize_json_rows(jsons, _tablet_schema->column(0).variant_max_subcolumns_count()));

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest,
       test_compaction_schema_excludes_materialized_typed_paths_from_topn_sparse_paths) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1",
                     /*variant_max_subcolumns_count=*/1,
                     /*is_key=*/false,
                     /*is_nullable=*/false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/false);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    auto typed_path = make_int_typed_path_template("a");
    _tablet_schema->mutable_column_by_uid(1).add_sub_column(typed_path);

    TabletIndexPB parent_index_pb;
    construct_tablet_index(&parent_index_pb, 10008, "idx_v1",
                           _tablet_schema->column(0).unique_id());
    TabletIndex parent_index;
    parent_index.init_from_pb(parent_index_pb);
    _tablet_schema->append_index(std::move(parent_index));
    init_tablet_from_current_schema(33012);

    auto rowset = create_variant_rowset({{R"({"a":1,"b":"x"})", R"({"a":2,"b":"y","c":"z"})"}}, 1);
    std::vector<RowsetSharedPtr> input_rowsets {rowset};

    auto compaction_schema = std::make_shared<TabletSchema>(*_tablet_schema);
    auto st = variant_util::VariantCompactionUtil::get_extended_compaction_schema(
            input_rowsets, compaction_schema);
    ASSERT_TRUE(st.ok()) << st.to_string();

    const auto* path_set_info = compaction_schema->try_path_set_info(1);
    ASSERT_NE(path_set_info, nullptr);
    ASSERT_TRUE(path_set_info->typed_path_set.contains("a"));
    EXPECT_FALSE(path_set_info->sub_path_set.contains(StringRef("a")));
    EXPECT_FALSE(path_set_info->sparse_path_set.contains(StringRef("a")));
    EXPECT_FALSE(path_set_info->subcolumn_indexes.contains("a"));
    EXPECT_TRUE(path_set_info->sub_path_set.contains(StringRef("b")));
    EXPECT_TRUE(path_set_info->sparse_path_set.contains(StringRef("c")));

    size_t typed_path_count = 0;
    size_t dynamic_path_count = 0;
    size_t sparse_path_count = 0;
    for (const auto& column : compaction_schema->columns()) {
        if (!column->is_extracted_column() || column->parent_unique_id() != 1) {
            continue;
        }
        const auto relative_path = column->path_info_ptr()->copy_pop_front().get_path();
        if (relative_path == "a") {
            ++typed_path_count;
            EXPECT_TRUE(column->path_info_ptr()->get_is_typed());
        } else if (relative_path == "b") {
            ++dynamic_path_count;
            EXPECT_FALSE(column->path_info_ptr()->get_is_typed());
        } else if (relative_path == "c") {
            ++sparse_path_count;
        }
    }
    EXPECT_EQ(typed_path_count, 1);
    EXPECT_EQ(dynamic_path_count, 1);
    EXPECT_EQ(sparse_path_count, 0);

    const auto& typed_info = path_set_info->typed_path_set.at("a");
    ASSERT_EQ(typed_info.indexes.size(), 1);
    EXPECT_EQ(typed_info.indexes[0]->index_id(), 10008);
    EXPECT_EQ(typed_info.indexes[0]->get_index_suffix(), "v1%2Ea");
}

TEST_F(VariantColumnWriterReaderTest,
       test_doc_value_staging_root_writer_skips_payload_with_extracted_columns) {
    constexpr int kRows = 2;

    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1",
                     /*variant_max_subcolumns_count=*/2,
                     /*is_key=*/false,
                     /*is_nullable=*/false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/false);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    TabletColumn parent_column = _tablet_schema->column(0);
    TabletColumn extracted;
    extracted.set_name(parent_column.name_lower_case() + ".hot");
    extracted.set_type(FieldType::OLAP_FIELD_TYPE_BIGINT);
    extracted.set_parent_unique_id(parent_column.unique_id());
    extracted.set_path_info(PathInData(parent_column.name_lower_case() + ".hot"));
    extracted.set_is_nullable(true);
    _tablet_schema->append_column(extracted);

    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    _tablet_schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 33006;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    ASSERT_TRUE(_tablet->init().ok());
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    ASSERT_TRUE(st.ok()) << st.msg();

    SegmentFooterPB footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    rowset_ctx.tablet_schema = _tablet_schema;

    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    opts.rowset_ctx = &rowset_ctx;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, parent_column, opts);

    std::unique_ptr<ColumnWriter> writer;
    ASSERT_TRUE(ColumnWriter::create(opts, &parent_column, file_writer.get(), &writer).ok());
    ASSERT_TRUE(writer->init().ok());

    auto strings = ColumnString::create();
    const std::vector<std::string> jsons = {R"({"hot":1,"cold":10})", R"({"hot":2,"cold":20})"};
    for (const auto& json : jsons) {
        strings->insert_data(json.data(), json.size());
    }

    ParseConfig parse_cfg;
    parse_cfg.deprecated_enable_flatten_nested = false;
    parse_cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
    auto variant_column =
            ColumnVariant::create(parent_column.variant_max_subcolumns_count(), false);
    variant_util::parse_json_to_variant(*variant_column, *strings, parse_cfg);
    ASSERT_FALSE(variant_column->serialized_doc_value_column_offsets().empty());
    ASSERT_EQ(variant_column->serialized_doc_value_column_offsets().back(), kRows * 2);

    auto variant_data = std::make_unique<VariantColumnData>();
    variant_data->column_data = variant_column.get();
    variant_data->row_pos = 0;
    const auto* data = reinterpret_cast<const uint8_t*>(variant_data.get());
    ASSERT_TRUE(writer->append_data(&data, kRows).ok());

    ASSERT_TRUE(writer->finish().ok());
    ASSERT_TRUE(writer->write_data().ok());
    ASSERT_TRUE(writer->write_ordinal_index().ok());
    ASSERT_TRUE(writer->write_zone_map().ok());
    ASSERT_TRUE(file_writer->close().ok());
    footer.set_num_rows(kRows);

    EXPECT_EQ(footer.columns_size(), 1);
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_storage_parse_kv_reduces_sparse_parse_write_footprint) {
    constexpr size_t kRows = 2048;
    constexpr size_t kDenseKeys = 2;
    constexpr size_t kSparseKeysPerRow = 30;
    constexpr size_t kSparseKeyPool = 1000;
    constexpr size_t kPathsPerRow = kDenseKeys + kSparseKeysPerRow;

    init_variant_tablet(33004, kDenseKeys);
    const auto jsons = make_variant_write_footprint_jsons(kRows, kDenseKeys, kSparseKeysPerRow,
                                                          kSparseKeyPool);

    VariantStorageParseWriteResult parse_time_result;
    {
        ScopedVariantStorageParseMode guard(1);
        SegmentFooterPB footer;
        std::string file_path;
        auto st = write_storage_parsed_segment(jsons, "force_subcolumns", &footer, &file_path,
                                               false, &parse_time_result);
        ASSERT_TRUE(st.ok()) << st.to_string();
    }

    VariantStorageParseWriteResult kv_result;
    {
        ScopedVariantStorageParseMode guard(2);
        SegmentFooterPB footer;
        std::string file_path;
        auto st = write_storage_parsed_segment(jsons, "force_doc_value", &footer, &file_path, false,
                                               &kv_result);
        ASSERT_TRUE(st.ok()) << st.to_string();
    }

    EXPECT_EQ(parse_time_result.num_rows, kRows);
    EXPECT_EQ(kv_result.num_rows, kRows);
    EXPECT_EQ(parse_time_result.doc_value_entries, static_cast<size_t>(0));
    EXPECT_EQ(kv_result.doc_value_entries, kRows * kPathsPerRow);
    EXPECT_GE(parse_time_result.parsed_subcolumns, kSparseKeyPool);
    EXPECT_LE(kv_result.parsed_subcolumns, static_cast<size_t>(1));
    EXPECT_GT(parse_time_result.parsed_subcolumns, kv_result.parsed_subcolumns);
    EXPECT_LT(kv_result.parsed_allocated_bytes, parse_time_result.parsed_allocated_bytes);

    // KV staging is only a parse-time shape for plain non-doc VARIANT. The writer still emits the
    // same top-N materialized subcolumns plus sparse fallback, with no persistent doc-value column.
    EXPECT_EQ(parse_time_result.footer_columns, kv_result.footer_columns);
    EXPECT_EQ(parse_time_result.materialized_columns, kv_result.materialized_columns);
    EXPECT_EQ(parse_time_result.sparse_columns, kv_result.sparse_columns);
    EXPECT_EQ(parse_time_result.doc_value_columns, 0);
    EXPECT_EQ(kv_result.doc_value_columns, 0);
    EXPECT_EQ(kv_result.materialized_columns, static_cast<int>(kDenseKeys));
    EXPECT_EQ(kv_result.sparse_columns, 1);
    EXPECT_GT(parse_time_result.segment_file_size, static_cast<uint64_t>(0));
    EXPECT_GT(kv_result.segment_file_size, static_cast<uint64_t>(0));
}

TEST_F(VariantColumnWriterReaderTest, test_write_data_advanced) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    int variant_sparse_hash_shard_count = rand() % 10 + 1;
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, false, false,
                     variant_sparse_hash_shard_count);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto column_object = (*std::move(block.get_by_position(0).column)).mutate();
    std::unordered_map<int, std::string> inserted_jsonstr;
    auto path_with_size = VariantUtil::fill_object_column_with_nested_test_data(column_object, 1000,
                                                                                &inserted_jsonstr);
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), 1000).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    int expected_sparse_cols =
            variant_sparse_hash_shard_count > 1 ? variant_sparse_hash_shard_count : 1;
    EXPECT_EQ(footer.columns_size(), 1 + 10 + expected_sparse_cols);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);

    for (int i = 1; i < footer.columns_size() - 1; ++i) {
        auto column_met = footer.columns(i);
        check_column_meta(column_met, path_with_size);
    }
    check_sparse_column_meta(footer.columns(footer.columns_size() - 1), path_with_size);

    // 7. check variant reader
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    // 8. check statistics
    auto statistics = variant_column_reader->get_stats();
    for (const auto& [path, size] : statistics->subcolumns_non_null_size) {
        EXPECT_EQ(path_with_size[path], size);
    }
    for (const auto& [path, size] : statistics->sparse_column_non_null_size) {
        EXPECT_EQ(path_with_size[path], size);
    }

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    // 9. check root
    ColumnIteratorUPtr it;
    TabletColumn parent_column = _tablet_schema->column(0);
    StorageReadOptions storage_read_opts;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    OlapReaderStatistics stats;
    storage_read_opts.stats = &stats;
    st = variant_column_reader->new_iterator(&it, &parent_column, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<HierarchicalDataIterator*>(it.get()) != nullptr);
    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = it->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    MutableColumnPtr new_column_object = ColumnVariant::create(3, false);
    size_t nrows = 1000;
    st = it->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = it->next_batch(&nrows, new_column_object);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(stats.bytes_read > 0);

    DataTypeSerDe::FormatOptions options;
    auto tz = cctz::utc_time_zone();
    options.timezone = &tz;
    for (int i = 0; i < 1000; ++i) {
        std::string value;
        assert_cast<ColumnVariant*>(new_column_object.get())
                ->serialize_one_row_to_string(i, &value, options);
        EXPECT_EQ(value, inserted_jsonstr[i]);
    }

    auto read_to_column_object = [&](ColumnIteratorUPtr& it) {
        new_column_object = ColumnVariant::create(10, false);
        nrows = 1000;
        st = it->seek_to_ordinal(0);
        EXPECT_TRUE(st.ok()) << st.msg();
        st = it->next_batch(&nrows, new_column_object);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(stats.bytes_read > 0);
        EXPECT_EQ(nrows, 1000);
    };

    auto check_key_stats = [&](const std::string& key_num) {
        std::string key = ".key" + key_num;
        TabletColumn subcolumn_in_nested;
        subcolumn_in_nested.set_name(parent_column.name_lower_case() + key);
        subcolumn_in_nested.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
        subcolumn_in_nested.set_parent_unique_id(parent_column.unique_id());
        subcolumn_in_nested.set_path_info(PathInData(parent_column.name_lower_case() + key));
        subcolumn_in_nested.set_variant_max_subcolumns_count(
                parent_column.variant_max_subcolumns_count());
        subcolumn_in_nested.set_is_nullable(true);

        ColumnIteratorUPtr it1;
        st = variant_column_reader->new_iterator(&it1, &subcolumn_in_nested, &storage_read_opts,
                                                 &column_reader_cache);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(assert_cast<HierarchicalDataIterator*>(it1.get()) != nullptr);
        st = it1->init(column_iter_opts);
        EXPECT_TRUE(st.ok()) << st.msg();
        read_to_column_object(it1);

        size_t key_count = 0;
        size_t key_nested_count = 0;
        for (int row = 0; row < 1000; ++row) {
            std::string value;
            assert_cast<ColumnVariant*>(new_column_object.get())
                    ->serialize_one_row_to_string(row, &value, options);
            if (value.find("nested" + key_num) != std::string::npos) {
                key_nested_count++;
            } else if (value.find("88") != std::string::npos) {
                key_count++;
            }
        }
        EXPECT_EQ(key_count, path_with_size["key" + key_num]);
        EXPECT_EQ(key_nested_count, path_with_size["key" + key_num + ".nested" + key_num]);
    };

    for (int i = 3; i < 10; ++i) {
        check_key_stats(std::to_string(i));
    }

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_write_sub_index) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "v", 2, false);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);
    TabletColumn& variant = _tablet_schema->mutable_column_by_uid(1);
    // add subcolumn
    TabletColumn subcolumn2;
    subcolumn2.set_name("v.b");
    subcolumn2.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    variant.add_sub_column(subcolumn2);
    variant.set_is_bf_column(true);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto column_object = VariantUtil::construct_basic_varint_column();
    auto vw = assert_cast<VariantColumnWriter*>(writer.get());

    std::unique_ptr<VariantColumnData> _variant_column_data = std::make_unique<VariantColumnData>();
    // pass the real ColumnVariant pointer instead of address of shared_ptr
    _variant_column_data->column_data = column_object.get();
    _variant_column_data->row_pos = 0;
    const uint8_t* data = (const uint8_t*)_variant_column_data.get();
    st = vw->append_data(&data, 10);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_bloom_filter_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(10);

    // 6. check footer
    std::cout << footer.columns_size() << std::endl;
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);
}

TEST_F(VariantColumnWriterReaderTest, test_find_subcolumn_tablet_indexes_inherits_full_path) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "v", 10, false);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    _tablet_schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10001;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    ASSERT_TRUE(_tablet->init().ok());
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    ASSERT_TRUE(st.ok()) << st.msg();

    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    rowset_ctx.tablet_schema = _tablet_schema;
    opts.rowset_ctx = &rowset_ctx;
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    ASSERT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    ASSERT_TRUE(writer->init().ok());
    ASSERT_TRUE(append_json_batch(writer.get(), {R"({"a": "x"})"}).ok());
    ASSERT_TRUE(writer->finish().ok());
    ASSERT_TRUE(writer->write_data().ok());
    ASSERT_TRUE(writer->write_ordinal_index().ok());
    ASSERT_TRUE(writer->write_zone_map().ok());
    ASSERT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1);

    TabletIndexPB index_pb;
    construct_tablet_index(&index_pb, 10001, "idx_v", column.unique_id());
    TabletIndex parent_index;
    parent_index.init_from_pb(index_pb);
    _tablet_schema->append_index(std::move(parent_index));

    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<segment_v2::ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    auto* variant_reader = assert_cast<segment_v2::VariantColumnReader*>(column_reader.get());
    ASSERT_NE(variant_reader, nullptr);

    TabletColumn subcolumn;
    subcolumn.set_name("v.a");
    subcolumn.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    subcolumn.set_parent_unique_id(column.unique_id());
    subcolumn.set_path_info(PathInData("v.a"));
    subcolumn.set_is_nullable(true);

    auto indexes = variant_reader->find_subcolumn_tablet_indexes(
            subcolumn, std::make_shared<DataTypeString>());
    ASSERT_EQ(indexes.size(), 1);
    EXPECT_EQ(indexes[0]->index_id(), 10001);
    EXPECT_EQ(indexes[0]->get_index_suffix(), "v%2Ea");
    EXPECT_NE(indexes[0]->get_index_suffix(), "a");
}

TEST_F(VariantColumnWriterReaderTest, test_nested_group_logical_index_path_uses_variant_root) {
    EXPECT_EQ(segment_v2::build_nested_group_logical_child_path("v", "arr", "x"), "v.arr.x");
    EXPECT_EQ(segment_v2::build_nested_group_logical_child_path("v", "arr.inner", "z"),
              "v.arr.inner.z");
    EXPECT_EQ(segment_v2::build_nested_group_logical_child_path(
                      "v", std::string(segment_v2::kRootNestedGroupPath), "x"),
              "v.x");
    EXPECT_EQ(segment_v2::build_nested_group_logical_child_path(
                      "v", std::string(segment_v2::kRootNestedGroupPath) + ".inner", "z"),
              "v.inner.z");
}

TEST_F(VariantColumnWriterReaderTest, test_find_subcolumn_tablet_indexes_branch_coverage) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "v", 10, false);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    _tablet_schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10002;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    ASSERT_TRUE(_tablet->init().ok());
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    ASSERT_TRUE(st.ok()) << st.msg();

    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    rowset_ctx.tablet_schema = _tablet_schema;
    opts.rowset_ctx = &rowset_ctx;
    TabletColumn root_column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, root_column, opts);

    std::unique_ptr<ColumnWriter> writer;
    ASSERT_TRUE(ColumnWriter::create(opts, &root_column, file_writer.get(), &writer).ok());
    ASSERT_TRUE(writer->init().ok());
    ASSERT_TRUE(append_json_batch(writer.get(), {R"({"a": "x", "own": "y"})"}).ok());
    ASSERT_TRUE(writer->finish().ok());
    ASSERT_TRUE(writer->write_data().ok());
    ASSERT_TRUE(writer->write_ordinal_index().ok());
    ASSERT_TRUE(writer->write_zone_map().ok());
    ASSERT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1);

    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<segment_v2::ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    auto* variant_reader = assert_cast<segment_v2::VariantColumnReader*>(column_reader.get());
    ASSERT_NE(variant_reader, nullptr);

    const int32_t root_unique_id = root_column.unique_id();
    auto make_subcolumn = [&](std::string name, FieldType type, std::string path,
                              int32_t parent_unique_id) {
        TabletColumn subcolumn;
        subcolumn.set_name(std::move(name));
        subcolumn.set_type(type);
        subcolumn.set_parent_unique_id(parent_unique_id);
        subcolumn.set_unique_id(2001);
        subcolumn.set_path_info(PathInData(std::move(path)));
        subcolumn.set_is_nullable(true);
        return subcolumn;
    };

    {
        auto no_parent_index = variant_reader->find_subcolumn_tablet_indexes(
                make_subcolumn("v.missing", FieldType::OLAP_FIELD_TYPE_STRING, "v.missing",
                               root_unique_id),
                std::make_shared<DataTypeString>());
        EXPECT_TRUE(no_parent_index.empty());
    }

    TabletIndexPB parent_index_pb;
    construct_tablet_index(&parent_index_pb, 10002, "idx_v", root_column.unique_id());
    TabletIndex parent_index;
    parent_index.init_from_pb(parent_index_pb);
    _tablet_schema->append_index(std::move(parent_index));

    {
        auto inherited = variant_reader->find_subcolumn_tablet_indexes(
                make_subcolumn("v.a", FieldType::OLAP_FIELD_TYPE_STRING, "v.a", root_unique_id),
                std::make_shared<DataTypeString>());
        ASSERT_EQ(inherited.size(), 1);
        EXPECT_EQ(inherited[0]->index_id(), 10002);
        EXPECT_EQ(inherited[0]->get_index_suffix(), "v%2Ea");
    }

    {
        auto plain_array = variant_reader->find_subcolumn_tablet_indexes(
                make_subcolumn("v.plainarr", FieldType::OLAP_FIELD_TYPE_ARRAY, "v.plainarr",
                               root_unique_id),
                std::make_shared<DataTypeNullable>(
                        std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())));
        ASSERT_EQ(plain_array.size(), 1);
        EXPECT_EQ(plain_array[0]->index_id(), 10002);
        EXPECT_EQ(plain_array[0]->get_index_suffix(), "v%2Eplainarr");
        EXPECT_NE(plain_array[0]->get_index_suffix(), "plainarr");
    }

    {
        auto variant_type = variant_reader->find_subcolumn_tablet_indexes(
                make_subcolumn("v.object", FieldType::OLAP_FIELD_TYPE_VARIANT, "v.object",
                               root_unique_id),
                std::make_shared<DataTypeVariant>(10, false));
        EXPECT_TRUE(variant_type.empty());
    }

    {
        auto sparse_map_type = variant_reader->find_subcolumn_tablet_indexes(
                make_subcolumn("v.__sparse", FieldType::OLAP_FIELD_TYPE_MAP, "v.__sparse",
                               root_unique_id),
                std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(),
                                              std::make_shared<DataTypeString>()));
        EXPECT_TRUE(sparse_map_type.empty());
    }

    TabletColumn indexed_subcolumn;
    indexed_subcolumn.set_name("own");
    indexed_subcolumn.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    _tablet_schema->mutable_column_by_uid(root_column.unique_id())
            .add_sub_column(indexed_subcolumn);

    TabletIndexPB own_index_pb;
    construct_tablet_index(&own_index_pb, 10003, "idx_v_own", root_column.unique_id());
    (*own_index_pb.mutable_properties())["field_pattern"] = "own";
    TabletIndex own_index;
    own_index.init_from_pb(own_index_pb);
    _tablet_schema->append_index(std::move(own_index));

    {
        auto own = variant_reader->find_subcolumn_tablet_indexes(
                make_subcolumn("v.own", FieldType::OLAP_FIELD_TYPE_STRING, "v.own", root_unique_id),
                std::make_shared<DataTypeString>());
        ASSERT_EQ(own.size(), 1);
        EXPECT_EQ(own[0]->index_id(), 10003);
        EXPECT_EQ(own[0]->get_index_suffix(), "v%2Eown");
    }

    auto group_reader = std::make_unique<segment_v2::NestedGroupReader>();
    group_reader->array_path = "arr";
    group_reader->offsets_reader = std::make_shared<segment_v2::ColumnReader>();
    group_reader->child_readers.emplace("x", nullptr);
    auto& nested_group_readers =
            const_cast<segment_v2::NestedGroupReaders&>(variant_reader->get_nested_group_readers());
    nested_group_readers.emplace("arr", std::move(group_reader));

    {
        auto nested = variant_reader->find_subcolumn_tablet_indexes(
                make_subcolumn("v.arr.x", FieldType::OLAP_FIELD_TYPE_ARRAY, "v.arr.x",
                               root_unique_id),
                std::make_shared<DataTypeNullable>(
                        std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())));
        ASSERT_EQ(nested.size(), 1);
        EXPECT_EQ(nested[0]->index_id(), 10002);
        EXPECT_EQ(nested[0]->get_index_suffix(), "v%2Earr%2Ex");
        EXPECT_NE(nested[0]->get_index_suffix(), "arr%2Ex");
    }

    auto nested_group_reader = std::make_unique<segment_v2::NestedGroupReader>();
    nested_group_reader->array_path = "inner";
    nested_group_reader->offsets_reader = std::make_shared<segment_v2::ColumnReader>();
    nested_group_reader->child_readers.emplace("z", nullptr);
    nested_group_readers.at("arr")->nested_group_readers.emplace("inner",
                                                                 std::move(nested_group_reader));

    {
        auto nested = variant_reader->find_subcolumn_tablet_indexes(
                make_subcolumn("v.arr.inner.z", FieldType::OLAP_FIELD_TYPE_ARRAY, "v.arr.inner.z",
                               root_unique_id),
                std::make_shared<DataTypeNullable>(std::make_shared<DataTypeArray>(
                        std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()))));
        ASSERT_EQ(nested.size(), 1);
        EXPECT_EQ(nested[0]->index_id(), 10002);
        EXPECT_EQ(nested[0]->get_index_suffix(), "v%2Earr%2Einner%2Ez");
        EXPECT_NE(nested[0]->get_index_suffix(), "arr%2Einner%2Ez");
    }
}

TEST_F(VariantColumnWriterReaderTest, test_write_data_nullable) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    // make nullable tablet_column
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, true, true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    // nullable variant column
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    // here is nullable variant
    auto block = _tablet_schema->create_block();
    std::unordered_map<int, std::string> inserted_jsonstr;
    variant_util::PathToNoneNullValues path_with_size;
    fill_nullable_variant_block(&block, &inserted_jsonstr, &path_with_size);
    const auto& source_nullable =
            assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& source_null_map = source_nullable.get_null_map_data();
    std::vector<uint8_t> expected_null_map(source_null_map.begin(), source_null_map.end());
    std::vector<std::string> expected_json(source_nullable.size());
    const auto& source_variant =
            assert_cast<const ColumnVariant&>(source_nullable.get_nested_column());
    DataTypeSerDe::FormatOptions serde_options;
    for (size_t row = 0; row < expected_json.size(); ++row) {
        if (expected_null_map[row] == 0) {
            source_variant.serialize_one_row_to_string(row, &expected_json[row], serde_options);
        }
    }
    // sort path_with_size with value
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    // variant do not implement append_nulls
    auto* vw = assert_cast<VariantColumnWriter*>(writer.get());
    const auto* ptr = (const uint8_t*)accessor->get_data();
    st = vw->append_nullable(accessor->get_nullmap(), &ptr, 1000);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    auto size = vw->estimate_buffer_size();
    std::cout << "size: " << size << std::endl;
    st = vw->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 12);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);

    for (int i = 1; i < footer.columns_size() - 1; ++i) {
        auto column_meta = footer.columns(i);
        EXPECT_TRUE(column_meta.has_column_path_info());
        auto path = std::make_shared<PathInData>();
        EXPECT_EQ(column_meta.column_path_info().parrent_column_unique_id(), 1);
        EXPECT_GT(column_meta.none_null_size(), path_with_size[path->copy_pop_front().get_path()]);
    }
    check_sparse_column_meta(footer.columns(footer.columns_size() - 1), path_with_size);

    // 7. check variant reader
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    // 8. check statistics
    auto statistics = variant_column_reader->get_stats();
    for (const auto& [path, size] : statistics->subcolumns_non_null_size) {
        EXPECT_GT(size, path_with_size[path]);
    }
    for (const auto& [path, size] : statistics->sparse_column_non_null_size) {
        EXPECT_EQ(path_with_size[path], size);
    }

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    // 9. check root
    ColumnIteratorUPtr it;
    TabletColumn parent_column = _tablet_schema->column(0);
    StorageReadOptions storage_read_opts;
    OlapReaderStatistics stats;
    storage_read_opts.stats = &stats;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    st = variant_column_reader->new_iterator(&it, &parent_column, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<HierarchicalDataIterator*>(it.get()) != nullptr);
    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = it->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    // The physical segment is written by ColumnVariant. Read it into a nullable ColumnVariantV2
    // destination and verify that the root null map and non-null values survive both scan modes.
    TabletColumn parent_column_v2 = parent_column;
    parent_column_v2.set_variant_is_v2(true);
    auto parent_type_v2 = DataTypeFactory::instance().create_data_type(parent_column_v2, false);

    OlapReaderStatistics v2_stats;
    StorageReadOptions v2_read_opts;
    v2_read_opts.stats = &v2_stats;
    v2_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    v2_read_opts.tablet_schema = _tablet_schema;
    ColumnIteratorUPtr v2_it;
    st = variant_column_reader->new_iterator(&v2_it, &parent_column_v2, &v2_read_opts,
                                             &column_reader_cache);
    ASSERT_TRUE(st.ok()) << st.msg();
    ASSERT_NE(dynamic_cast<HierarchicalDataIterator*>(v2_it.get()), nullptr);
    ColumnIteratorOptions v2_iter_opts;
    v2_iter_opts.stats = &v2_stats;
    v2_iter_opts.file_reader = file_reader.get();
    ASSERT_TRUE(v2_it->init(v2_iter_opts).ok());
    ASSERT_TRUE(v2_it->seek_to_ordinal(0).ok());

    MutableColumnPtr v2_result = parent_type_v2->create_column();
    size_t v2_rows = expected_null_map.size();
    st = v2_it->next_batch(&v2_rows, v2_result);
    ASSERT_TRUE(st.ok()) << st.msg();
    ASSERT_EQ(v2_rows, expected_null_map.size());
    const auto& v2_nullable = assert_cast<const ColumnNullable&>(*v2_result);
    const auto& v2_variant = assert_cast<const ColumnVariantV2&>(v2_nullable.get_nested_column());
    ASSERT_EQ(v2_nullable.size(), expected_null_map.size());
    for (size_t row = 0; row < expected_null_map.size(); ++row) {
        const bool expected_null = expected_null_map[row] != 0;
        EXPECT_EQ(v2_nullable.is_null_at(row), expected_null);
        if (!expected_null) {
            EXPECT_EQ(variant_v2_json_at(v2_variant, row), expected_json[row]);
        }
    }

    OlapReaderStatistics v2_rowid_stats;
    StorageReadOptions v2_rowid_read_opts;
    v2_rowid_read_opts.stats = &v2_rowid_stats;
    v2_rowid_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    v2_rowid_read_opts.tablet_schema = _tablet_schema;
    ColumnIteratorUPtr v2_rowid_it;
    st = variant_column_reader->new_iterator(&v2_rowid_it, &parent_column_v2, &v2_rowid_read_opts,
                                             &column_reader_cache);
    ASSERT_TRUE(st.ok()) << st.msg();
    ASSERT_NE(dynamic_cast<HierarchicalDataIterator*>(v2_rowid_it.get()), nullptr);
    ColumnIteratorOptions v2_rowid_iter_opts;
    v2_rowid_iter_opts.stats = &v2_rowid_stats;
    v2_rowid_iter_opts.file_reader = file_reader.get();
    ASSERT_TRUE(v2_rowid_it->init(v2_rowid_iter_opts).ok());

    const std::vector<rowid_t> rowids {0, 1, 80, 81, 97, 98, 199, 900, 999};
    MutableColumnPtr v2_rowid_result = parent_type_v2->create_column();
    st = v2_rowid_it->read_by_rowids(rowids.data(), rowids.size(), v2_rowid_result);
    ASSERT_TRUE(st.ok()) << st.msg();
    const auto& v2_rowid_nullable = assert_cast<const ColumnNullable&>(*v2_rowid_result);
    const auto& v2_rowid_variant =
            assert_cast<const ColumnVariantV2&>(v2_rowid_nullable.get_nested_column());
    ASSERT_EQ(v2_rowid_nullable.size(), rowids.size());
    for (size_t row = 0; row < rowids.size(); ++row) {
        const auto source_row = rowids[row];
        const bool expected_null = expected_null_map[source_row] != 0;
        EXPECT_EQ(v2_rowid_nullable.is_null_at(row), expected_null);
        if (!expected_null) {
            EXPECT_EQ(variant_v2_json_at(v2_rowid_variant, row), expected_json[source_row]);
        }
    }

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_write_data_nullable_without_finalize) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    // make nullable tablet_column
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, true, true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    // nullable variant column
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    // here is nullable variant
    auto block = _tablet_schema->create_block();
    std::unordered_map<int, std::string> inserted_jsonstr;
    variant_util::PathToNoneNullValues path_with_size;
    fill_nullable_variant_block(&block, &inserted_jsonstr, &path_with_size);
    // sort path_with_size with value
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    // variant do not implement append_nulls
    auto* vw = assert_cast<VariantColumnWriter*>(writer.get());
    const auto* ptr = (const uint8_t*)accessor->get_data();
    st = vw->append_nullable(accessor->get_nullmap(), &ptr, 1000);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 12);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);
}

TEST_F(VariantColumnWriterReaderTest, test_write_bm_with_finalize) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    // make nullable tablet_column
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, true, true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    // nullable variant column
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    // here is nullable variant
    auto block = _tablet_schema->create_block();
    std::unordered_map<int, std::string> inserted_jsonstr;
    variant_util::PathToNoneNullValues path_with_size;
    fill_nullable_variant_block(&block, &inserted_jsonstr, &path_with_size);
    // sort path_with_size with value
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    // variant do not implement append_nulls
    auto* vw = assert_cast<VariantColumnWriter*>(writer.get());
    const auto* ptr = (const uint8_t*)accessor->get_data();
    st = vw->append_nullable(accessor->get_nullmap(), &ptr, 1000);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->_impl->finalize();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 12);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);
}

TEST_F(VariantColumnWriterReaderTest, test_write_bf_with_finalize) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    // make nullable tablet_column
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, true, true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    // nullable variant column
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    // here is nullable variant
    auto block = _tablet_schema->create_block();
    std::unordered_map<int, std::string> inserted_jsonstr;
    variant_util::PathToNoneNullValues path_with_size;
    fill_nullable_variant_block(&block, &inserted_jsonstr, &path_with_size);
    // sort path_with_size with value
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    // variant do not implement append_nulls
    auto* vw = assert_cast<VariantColumnWriter*>(writer.get());
    const auto* ptr = (const uint8_t*)accessor->get_data();
    st = vw->append_nullable(accessor->get_nullmap(), &ptr, 1000);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->_impl->finalize();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_bloom_filter_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 12);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);
}

TEST_F(VariantColumnWriterReaderTest, test_write_zm_with_finalize) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    // make nullable tablet_column
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, true, true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    // nullable variant column
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    // here is nullable variant
    auto block = _tablet_schema->create_block();
    std::unordered_map<int, std::string> inserted_jsonstr;
    variant_util::PathToNoneNullValues path_with_size;
    fill_nullable_variant_block(&block, &inserted_jsonstr, &path_with_size);
    // sort path_with_size with value
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    // variant do not implement append_nulls
    auto* vw = assert_cast<VariantColumnWriter*>(writer.get());
    const auto* ptr = (const uint8_t*)accessor->get_data();
    st = vw->append_nullable(accessor->get_nullmap(), &ptr, 1000);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->_impl->finalize();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 12);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);
}

TEST_F(VariantColumnWriterReaderTest, test_write_inverted_with_finalize) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    // make nullable tablet_column
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, true, true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    // nullable variant column
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    // here is nullable variant
    auto block = _tablet_schema->create_block();
    std::unordered_map<int, std::string> inserted_jsonstr;
    variant_util::PathToNoneNullValues path_with_size;
    fill_nullable_variant_block(&block, &inserted_jsonstr, &path_with_size);
    // sort path_with_size with value
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    // variant do not implement append_nulls
    auto* vw = assert_cast<VariantColumnWriter*>(writer.get());
    const auto* ptr = (const uint8_t*)accessor->get_data();
    st = vw->append_nullable(accessor->get_nullmap(), &ptr, 1000);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->_impl->finalize();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_inverted_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 12);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);
}

TEST_F(VariantColumnWriterReaderTest, test_no_sub_in_sparse_column) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1");
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10001;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto column_object = (*std::move(block.get_by_position(0).column)).mutate();
    auto type_string = std::make_shared<DataTypeString>();
    auto json_column = type_string->create_column();
    auto column_string = assert_cast<ColumnString*>(json_column.get());
    // for some test data in json string to insert variant column
    // make list for json string
    for (int i = 0; i < 1000; ++i) {
        std::string inserted_jsonstr =
                (R"({"a": {"b": )" + std::to_string(i) + R"(, "c": )" + std::to_string(i) +
                 R"(}, "d": )" + std::to_string(i) + R"(})");
        // insert json string to variant column
        column_string->insert_data(inserted_jsonstr.data(), inserted_jsonstr.size());
    }

    ParseConfig config;
    config.deprecated_enable_flatten_nested = false;
    variant_util::parse_json_to_variant(*column_object, *column_string, config);
    std::cout << "column_object size: "
              << assert_cast<ColumnVariant*>(column_object.get())->debug_string() << std::endl;

    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), 1000).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // check footer
    EXPECT_EQ(footer.columns_size(), 5);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);

    // 6. create reader
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    std::shared_ptr<ColumnReader> reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    auto variant_column_reader = assert_cast<VariantColumnReader*>(reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    // 7. test exist_in_sparse_column
    auto* variant_reader = assert_cast<VariantColumnReader*>(reader.get());
    PathInData non_existent_path("non.existent.path");
    EXPECT_FALSE(variant_reader->exist_in_sparse_column(non_existent_path));

    // 8. test prefix_exist_in_sparse_column = true which means we have prefix in sparse column
    for (auto& path : variant_reader->get_stats()->sparse_column_non_null_size) {
        std::cout << "sparse_column_non_null_size path: " << path.first << ", size: " << path.second
                  << std::endl;
    }
    for (auto& path : variant_reader->get_stats()->subcolumns_non_null_size) {
        std::cout << "subcolumns_non_null_size path: " << path.first << ", size: " << path.second
                  << std::endl;
    }
    PathInData prefix_path("a");
    EXPECT_FALSE(variant_reader->exist_in_sparse_column(prefix_path));

    // 9. test get_metadata_size with null statistics
    EXPECT_GT(variant_reader->get_metadata_size(), 0);

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    // 10. test hierarchical reader with empty statistics
    ColumnIteratorUPtr iterator;
    StorageReadOptions read_opts;
    OlapReaderStatistics stats;
    read_opts.stats = &stats;
    st = variant_reader->new_iterator(&iterator, &column, &read_opts, &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(iterator != nullptr);
}

TEST_F(VariantColumnWriterReaderTest, test_prefix_in_sub_and_sparse) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1");
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10001;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto column_object = (*std::move(block.get_by_position(0).column)).mutate();
    auto type_string = std::make_shared<DataTypeString>();
    auto json_column = type_string->create_column();
    auto column_string = assert_cast<ColumnString*>(json_column.get());
    // for some test data in json string to insert variant column
    // insert some test data to json string
    for (int i = 0; i < 1000; ++i) {
        std::string inserted_jsonstr =
                (R"({"a": {"b": )" + std::to_string(i) + R"(, "c": )" + std::to_string(i) +
                 R"(}, "d": )" + std::to_string(i) + R"(})");
        // add some rand key for sparse column with 'a.b' prefix : {"a": {"b": 1, "c": 1, "e": 1}, "d": 1}
        if (i % 17 == 0) {
            inserted_jsonstr =
                    (R"({"a": {"b": )" + std::to_string(i) + R"(, "c": )" + std::to_string(i) +
                     R"(, "e": )" + std::to_string(i) + R"(}, "d": )" + std::to_string(i) + R"(})");
        }
        // add some rand key for spare column without prefix: {"a": {"b": 1, "c": 1}, "d": 1, "e": 1}
        if (i % 177 == 0) {
            inserted_jsonstr =
                    (R"({"a": {"b": )" + std::to_string(i) + R"(, "c": )" + std::to_string(i) +
                     R"(}, "d": )" + std::to_string(i) + R"(, "e": )" + std::to_string(i) + R"(})");
        }
        // insert json string to variant column
        column_string->insert_data(inserted_jsonstr.data(), inserted_jsonstr.size());
    }

    ParseConfig config;
    config.deprecated_enable_flatten_nested = false;
    variant_util::parse_json_to_variant(*column_object, *column_string, config);
    std::cout << "column_object size: "
              << assert_cast<ColumnVariant*>(column_object.get())->debug_string() << std::endl;

    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), 1000).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // check footer
    EXPECT_EQ(footer.columns_size(), 5);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);

    // 6. create reader
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    std::shared_ptr<ColumnReader> reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    auto variant_column_reader = assert_cast<VariantColumnReader*>(reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    // 7. test exist_in_sparse_column
    auto* variant_reader = assert_cast<VariantColumnReader*>(reader.get());
    PathInData non_existent_path("non.existent.path");
    EXPECT_FALSE(variant_reader->exist_in_sparse_column(non_existent_path));

    // 8. test prefix_exist_in_sparse_column = true which means we have prefix in sparse column
    for (auto& path : variant_reader->get_stats()->sparse_column_non_null_size) {
        std::cout << "sparse_column_non_null_size path: " << path.first << ", size: " << path.second
                  << std::endl;
    }
    for (auto& path : variant_reader->get_stats()->subcolumns_non_null_size) {
        std::cout << "subcolumns_non_null_size path: " << path.first << ", size: " << path.second
                  << std::endl;
    }
    PathInData prefix_path("a");
    EXPECT_TRUE(variant_reader->exist_in_sparse_column(prefix_path));

    // 9. test get_metadata_size with null statistics
    EXPECT_GT(variant_reader->get_metadata_size(), 0);

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    // 10. test hierarchical reader with empty statistics
    ColumnIteratorUPtr iterator;
    StorageReadOptions read_opts;
    OlapReaderStatistics stats;
    read_opts.stats = &stats;
    st = variant_reader->new_iterator(&iterator, &column, &read_opts, &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(iterator != nullptr);
}

void test_write_variant_column(StorageEngine* _engine_ref, std::string _absolute_dir,
                               std::string& file_path, SegmentFooterPB& footer,
                               std::shared_ptr<TabletSchema> _tablet_schema,
                               bool nullable = false) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "v", 3, false, nullable);
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10000;
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_absolute_dir).ok());
    std::unique_ptr<DataDir> _data_dir = std::make_unique<DataDir>(*_engine_ref, _absolute_dir);
    static_cast<void>(_data_dir->update_capacity());
    Status st1 = _data_dir->init(true);
    EXPECT_TRUE(st1.ok()) << st1.msg();
    std::shared_ptr<Tablet> _tablet =
            std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn tablet_column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, tablet_column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &tablet_column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. make test data for column_object
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto column_object = (*std::move(block.get_by_position(0).column)).mutate();
    VariantUtil::VariantStringCreator simple_column_object = [](ColumnString* column_string,
                                                                size_t size) {
        // for some test data in json string to insert variant column
        // insert some test data to json string: {"a" : {"b" : [{"c" : {"d" : i, "e": "a@b"}}]}, "x": "y"}}
        for (int i = 0; i < size; ++i) {
            std::string inserted_jsonstr = (R"({"a" : {"b" : [{"c" : {"d" : )" + std::to_string(i) +
                                            R"(, "e": "a@b"}}]}, "x": "y"})");
            // insert json string to variant column
            column_string->insert_data(inserted_jsonstr.data(), inserted_jsonstr.size());
        }
    };
    if (nullable) {
        auto null_object = assert_cast<ColumnNullable*>(column_object.get());
        auto _object = null_object->get_nested_column_ptr();
        null_object->get_null_map_column_ptr()->insert_many_defaults(1000);
        VariantUtil::fill_variant_column(_object, 1000, 1, true, &simple_column_object);
    } else {
        VariantUtil::fill_variant_column(column_object, 1000, 1, true, &simple_column_object);
    }
    EXPECT_TRUE(column_object->size() == 1000);
    olap_data_convertor->add_column_data_convertor(tablet_column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), 1000).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 5);
    auto column_m = footer.columns(0);
    EXPECT_EQ(column_m.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);

    for (int i = 1; i < footer.columns_size() - 1; ++i) {
        auto column_meta = footer.columns(i);
        EXPECT_TRUE(column_meta.has_column_path_info());
        auto path = std::make_shared<PathInData>();
        path->from_protobuf(column_meta.column_path_info());
        EXPECT_EQ(column_meta.column_path_info().parrent_column_unique_id(), 1);
    }
}

TEST_F(VariantColumnWriterReaderTest, test_nested_subcolumn) {
    // write data
    std::string absolute_dir = _current_dir + std::string("/ut_dir/variant_test_nested_subcolumn");
    // declare file_path and footer
    std::string file_path;
    SegmentFooterPB footer;
    std::shared_ptr<TabletSchema> _tablet_schema = std::make_shared<TabletSchema>();
    test_write_variant_column(_engine_ref, absolute_dir, file_path, footer, _tablet_schema);
    // reader data
    // check variant reader
    io::FileReaderSPtr file_reader;
    Status st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);
    // test read situation for compaction with should flat all sub column
    EXPECT_FALSE(variant_column_reader->get_subcolumns_meta_info()->empty());

    // create a nested column array<struct> which not exists in subcolumn
    TabletColumn struct_column;
    struct_column.set_name("b");
    struct_column.set_type(FieldType::OLAP_FIELD_TYPE_STRUCT);
    TabletColumn int_column;
    int_column.set_name("i");
    int_column.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    TabletColumn string_column;
    string_column.set_name("s");
    string_column.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    struct_column.add_sub_column(int_column);
    struct_column.add_sub_column(string_column);

    TabletColumn target_column;
    target_column.set_name("a");
    target_column.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
    target_column.add_sub_column(struct_column);

    // {"a" : {"b" : [{"i" : 1, "s": "abs"}]}}
    // DefaultNestedColumnIterator with sibling_iter
    PathInDataBuilder builder;
    builder.append("v", false); // First part is variant
    builder.append("a", false); //  Second part is struct
    builder.append("b", false); // Third part is struct
    builder.append("i", true); // Fourth part is int as array for b.i array<int> , b.s array<string>
    // this will be a.b.i and a.b.s
    PathInData path = builder.build();
    EXPECT_TRUE(path.has_nested_part());
    target_column.set_path_info(path);
    EXPECT_TRUE(target_column.is_nested_subcolumn())
            << target_column._column_path->has_nested_part();

    StorageReadOptions storageReadOptions;
    storageReadOptions.io_ctx.reader_type = ReaderType::READER_CUMULATIVE_COMPACTION;

    // DefaultNestedColumnIterator with nullptr parameter
    PathInDataBuilder builder1;
    builder1.append("v", false); // First part is variant
    builder1.append("v", false); // First part is variant
    builder1.append("a", false); //  Second part is struct
    builder1.append("b", false); // Third part is struct
    builder1.append("i",
                    true); // Fourth part is int as array for b.i array<int> , b.s array<string>
    // this will be a.b.i and a.b.s
    PathInData path1 = builder1.build();
    EXPECT_TRUE(path1.has_nested_part());
    target_column.set_path_info(path1);
    EXPECT_TRUE(target_column.is_nested_subcolumn())
            << target_column._column_path->has_nested_part();
}

TEST_F(VariantColumnWriterReaderTest, test_nested_iter) {
    // write data
    std::string absolute_dir = _current_dir + std::string("/ut_dir/variant_test_nested_iter");
    // declare file_path and footer
    std::string file_path;
    SegmentFooterPB footer;
    std::shared_ptr<TabletSchema> _tablet_schema = std::make_shared<TabletSchema>();
    test_write_variant_column(_engine_ref, absolute_dir, file_path, footer, _tablet_schema);
    // reader data
    // check variant reader
    io::FileReaderSPtr file_reader;
    Status st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);
    // test read situation for compaction with should flat all sub column
    EXPECT_FALSE(variant_column_reader->get_subcolumns_meta_info()->empty());

    StorageReadOptions storageReadOptions;
    storageReadOptions.io_ctx.reader_type = ReaderType::READER_QUERY;
    OlapReaderStatistics stats;
    storageReadOptions.stats = &stats;

    ColumnIteratorUPtr nested_column_iter;
    st = variant_column_reader->new_iterator(&nested_column_iter, &_tablet_schema->column(0),
                                             &storageReadOptions, &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    // this is nested column root
    auto* nested_iter = assert_cast<HierarchicalDataIterator*>(nested_column_iter.get());
    EXPECT_TRUE(nested_iter != nullptr);
    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = nested_iter->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();
    // fill with nullable ColumnVariant target
    MutableColumnPtr new_column_object1 = ColumnVariant::create(3, false);
    MutableColumnPtr null_object =
            ColumnNullable::create(std::move(new_column_object1), ColumnUInt8::create());
    size_t n = 1000;
    st = nested_iter->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    bool has_null = false;
    st = nested_iter->next_batch(&n, null_object, &has_null);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(stats.bytes_read > 0);
    {
        // fill with nullable ColumnVariant target
        MutableColumnPtr new_column_object12 = ColumnVariant::create(3, false);
        MutableColumnPtr null_object12 =
                ColumnNullable::create(std::move(new_column_object12), ColumnUInt8::create());
        st = nested_iter->seek_to_ordinal(0);
        EXPECT_TRUE(st.ok()) << st.msg();
        st = nested_iter->next_batch(&n, null_object12, &has_null);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(stats.bytes_read > 0);
    }
    // read dst is nullable column object
    {
        ColumnIteratorUPtr nested_column_iter1;
        TabletColumn target_column;
        target_column.set_name("a");
        target_column.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
        // {"a" : {"b" : [{"i" : 1, "s": "abs"}]}}
        PathInDataBuilder builder;
        builder.append("v", false); // First part is variant
        builder.append("a", false); //  Second part is struct
        builder.append("b", false); // Third part is struct
        PathInData path = builder.build();
        target_column.set_path_info(path);

        st = variant_column_reader->new_iterator(&nested_column_iter1, &target_column,
                                                 &storageReadOptions, &column_reader_cache);
        EXPECT_TRUE(st.ok()) << st.msg();
        // this is nested column root
        auto* nested_iter2 = assert_cast<HierarchicalDataIterator*>(nested_column_iter1.get());
        EXPECT_TRUE(nested_iter2 != nullptr);
        st = nested_iter2->init(column_iter_opts);
        EXPECT_TRUE(st.ok()) << st.msg();
        // fill with nullable ColumnVariant target
        MutableColumnPtr new_column_object2 = ColumnVariant::create(3, false);
        MutableColumnPtr null_object2 =
                ColumnNullable::create(std::move(new_column_object2), ColumnUInt8::create());
        size_t nrows = 1000;
        st = nested_iter2->seek_to_ordinal(0);
        EXPECT_TRUE(st.ok()) << st.msg();
        st = nested_iter2->next_batch(&nrows, null_object2, &has_null);
        EXPECT_TRUE(st.ok()) << st.msg();
    }
    // test _process_with_nested_column for offsets not equals
    {
        ColumnIteratorUPtr nested_column_iter1;
        TabletColumn target_column;
        target_column.set_name("a");
        target_column.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
        // {"a" : {"b" : [{"i" : 1, "s": "abs"}]}}
        PathInDataBuilder builder;
        builder.append("v", false); // First part is variant
        builder.append("a", false); //  Second part is struct
        builder.append("b", false); // Third part is struct
        PathInData path = builder.build();
        target_column.set_path_info(path);

        st = variant_column_reader->new_iterator(&nested_column_iter1, &target_column,
                                                 &storageReadOptions, &column_reader_cache);
        EXPECT_TRUE(st.ok()) << st.msg();
        // this is nested column root
        auto* nested_iter2 = assert_cast<HierarchicalDataIterator*>(nested_column_iter1.get());
        EXPECT_TRUE(nested_iter2 != nullptr);
        st = nested_iter2->init(column_iter_opts);
        EXPECT_TRUE(st.ok()) << st.msg();
        st = nested_iter2->seek_to_ordinal(0);
        EXPECT_TRUE(st.ok()) << st.msg();
        std::map<PathInData, PathsWithColumnAndType> nested_subcolumns;
        // fill nested_subcolumns with different offset of array
        PathInData parent_path("a");
        PathInData relative_path("b");
        DataTypePtr base_data_type =
                std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
        auto base_column = base_data_type->create_column();
        PathWithColumnAndType base = {relative_path, base_column->get_ptr(), base_data_type};
        nested_subcolumns[parent_path].emplace_back(base);
        DataTypePtr second_data_type = std::make_shared<DataTypeString>();
        auto second_column = second_data_type->create_column();
        PathWithColumnAndType second = {relative_path, second_column->get_ptr(), second_data_type};
        nested_subcolumns[parent_path].emplace_back(second);
        // test _process_with_nested_column with different type
        // init container which is ColumnVariant
        MutableColumnPtr nested_column_object = ColumnVariant::create(3, false);
        auto& container_variant = assert_cast<ColumnVariant&>(*nested_column_object);
        st = nested_iter2->_process_nested_columns(container_variant, nested_subcolumns, n);
        std::cout << st.msg() << std::endl;
        EXPECT_FALSE(st.ok()) << st.msg();

        // then delete second
        nested_subcolumns[parent_path].pop_back();
        // add new nested column with array but not same offset
        auto column_a = base_data_type->create_column();
        column_a->insert_default();
        PathWithColumnAndType new_column = {relative_path, column_a->get_ptr(), base_data_type};
        nested_subcolumns[parent_path].emplace_back(new_column);
        // test _process_with_nested_column with different offset
        st = nested_iter2->_process_nested_columns(container_variant, nested_subcolumns, n);
        std::cout << st.msg() << std::endl;
        EXPECT_FALSE(st.ok()) << st.msg();
    }
}

TEST_F(VariantColumnWriterReaderTest, test_nested_iter_nullable) {
    // write data
    std::string absolute_dir = _current_dir + std::string("/ut_dir/variant_test_nested_iter");
    // declare file_path and footer
    std::string file_path;
    SegmentFooterPB footer;
    std::shared_ptr<TabletSchema> _tablet_schema = std::make_shared<TabletSchema>();
    test_write_variant_column(_engine_ref, absolute_dir, file_path, footer, _tablet_schema, true);
    // reader data
    // check variant reader
    io::FileReaderSPtr file_reader;
    Status st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);
    // test read situation for compaction with should flat all sub column
    EXPECT_FALSE(variant_column_reader->get_subcolumns_meta_info()->empty());

    StorageReadOptions storageReadOptions;
    storageReadOptions.io_ctx.reader_type = ReaderType::READER_QUERY;
    OlapReaderStatistics stats;
    storageReadOptions.stats = &stats;

    ColumnIteratorUPtr nested_column_iter;
    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    st = variant_column_reader->new_iterator(&nested_column_iter, &_tablet_schema->column(0),
                                             &storageReadOptions, &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    // this is nested column root
    auto* nested_iter = assert_cast<HierarchicalDataIterator*>(nested_column_iter.get());
    EXPECT_TRUE(nested_iter != nullptr);
    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = nested_iter->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();
    // fill with nullable ColumnVariant target
    MutableColumnPtr new_column_object1 = ColumnVariant::create(3, false);
    MutableColumnPtr null_object =
            ColumnNullable::create(std::move(new_column_object1), ColumnUInt8::create());
    size_t nrows = 1000;
    st = nested_iter->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    bool has_null = false;
    st = nested_iter->next_batch(&nrows, null_object, &has_null);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(stats.bytes_read > 0);
}

TEST_F(VariantColumnWriterReaderTest, test_read_with_checksum) {
    auto fill_string_column_with_test_data =
            [&](auto& column_string, int size,
                std::unordered_map<int, std::string>* inserted_jsonstr,
                variant_util::PathToNoneNullValues* path_with_size) {
                for (int i = 0; i < size; ++i) {
                    std::string jsonstr;
                    if (i % 2 == 0) {
                        jsonstr = R"({"b" : 3})";
                        (*path_with_size)["b"] += 1;
                    } else {
                        jsonstr = R"({"b" : {"c" : 5}})";
                        (*path_with_size)["b.c"] += 1;
                    }
                    inserted_jsonstr->emplace(i, jsonstr);
                    column_string->insert_data(jsonstr.c_str(), jsonstr.size());
                }
            };

    auto fill_object_column_with_test_data =
            [&](auto& column_object, int size,
                std::unordered_map<int, std::string>* inserted_jsonstr,
                variant_util::PathToNoneNullValues* path_with_size) {
                auto type_string = std::make_shared<DataTypeString>();
                auto column = type_string->create_column();
                auto* column_string = assert_cast<ColumnString*>(column.get());
                fill_string_column_with_test_data(column_string, size, inserted_jsonstr,
                                                  path_with_size);
                ParseConfig config;
                config.deprecated_enable_flatten_nested = false;
                variant_util::parse_json_to_variant(*column_object, *column_string, config);
            };

    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1");
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());

    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto column_object = (*std::move(block.get_by_position(0).column)).mutate();
    variant_util::PathToNoneNullValues path_with_size;
    std::unordered_map<int, std::string> inserted_jsonstr;
    fill_object_column_with_test_data(column_object, 1000, &inserted_jsonstr, &path_with_size);

    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), 1000).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 4);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);

    for (int i = 1; i < footer.columns_size() - 1; ++i) {
        auto column_met = footer.columns(i);
        check_column_meta(column_met, path_with_size);
    }
    check_sparse_column_meta(footer.columns(footer.columns_size() - 1), path_with_size);

    // 7. check variant reader
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    const auto* subcolumn_meta = variant_column_reader->get_subcolumn_meta_by_path(PathInData("b"));
    EXPECT_TRUE(subcolumn_meta != nullptr);
    subcolumn_meta = variant_column_reader->get_subcolumn_meta_by_path(PathInData("b.c"));
    EXPECT_TRUE(subcolumn_meta != nullptr);

    TabletColumn parent_column = _tablet_schema->column(0);
    StorageReadOptions storage_read_opts;

    storage_read_opts.tablet_schema = _tablet_schema;

    TabletColumn subcolumn;
    subcolumn.set_name(parent_column.name_lower_case() + ".b");
    subcolumn.set_type((FieldType)(int)footer.columns(1).type());
    subcolumn.set_parent_unique_id(parent_column.unique_id());
    subcolumn.set_path_info(PathInData(parent_column.name_lower_case() + ".b"));
    subcolumn.set_variant_max_subcolumns_count(parent_column.variant_max_subcolumns_count());
    subcolumn.set_is_nullable(true);
    _tablet_schema->append_column(subcolumn);
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    OlapReaderStatistics stats;
    storage_read_opts.stats = &stats;
    ColumnIteratorUPtr hierarchical_it;
    st = variant_column_reader->new_iterator(&hierarchical_it, &subcolumn, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(dynamic_cast<HierarchicalDataIterator*>(hierarchical_it.get()) != nullptr);

    storage_read_opts.io_ctx.reader_type = ReaderType::READER_CHECKSUM;
    ColumnIteratorUPtr it;
    st = variant_column_reader->new_iterator(&it, &subcolumn, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(dynamic_cast<FileColumnIterator*>(it.get()) != nullptr);
    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = it->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto column_type = DataTypeFactory::instance().create_data_type(subcolumn, true);
    auto read_column = column_type->create_column();
    size_t nrows = 1000;
    st = it->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = it->next_batch(&nrows, read_column);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(stats.bytes_read > 0);

    for (int row = 0; row < 1000; ++row) {
        const std::string& value = column_type->to_string(*read_column, row);
        if (row % 2 == 0) {
            EXPECT_EQ(value, "3");
        }
    }
}

// Concurrently trigger external meta loading and subcolumn meta access to guard against
// data races between `load_external_meta_once` writer and readers like
// `get_subcolumn_meta_by_path` / `get_metadata_size`. This roughly simulates the
// production crash stack where one thread was loading external meta while another
// thread was reading from `_subcolumns_meta_info`.
TEST_F(VariantColumnWriterReaderTest, test_concurrent_load_external_meta_and_get_subcolumn_meta) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1");
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet with external segment meta explicitly enabled so that
    // VariantColumnReader builds a VariantExternalMetaReader.
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = true;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 20000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());

    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write a small amount of data to build some subcolumns
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto column_object = (*std::move(block.get_by_position(0).column)).mutate();
    std::unordered_map<int, std::string> inserted_jsonstr;
    auto path_with_size =
            VariantUtil::fill_object_column_with_test_data(column_object, 200, &inserted_jsonstr);
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 200);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), 200).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(200);

    // 6. open a VariantColumnReader on this segment
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    // 7. run load_external_meta_once and subcolumn meta access concurrently.
    const int rounds = 200;
    std::atomic<bool> failed {false};
    Status writer_status = Status::OK();

    std::thread writer_thread([&] {
        for (int i = 0; i < rounds && !failed.load(); ++i) {
            Status s = variant_column_reader->load_external_meta_once();
            if (!s.ok()) {
                writer_status = s;
                failed.store(true);
                break;
            }
        }
    });

    std::thread reader_thread([&] {
        for (int i = 0; i < rounds && !failed.load(); ++i) {
            // Access subcolumn meta and metadata size repeatedly.
            auto* node = variant_column_reader->get_subcolumn_meta_by_path(PathInData("key0"));
            (void)node;
            auto meta_size = variant_column_reader->get_metadata_size();
            (void)meta_size;
        }
    });

    writer_thread.join();
    reader_thread.join();

    EXPECT_TRUE(writer_status.ok());
}

TEST_F(VariantColumnWriterReaderTest,
       test_streaming_write_plan_collects_regular_paths_from_rowset_metadata) {
    if (!nested_group_write_path_available()) {
        GTEST_SKIP() << "NestedGroup write path is not available in this build";
    }

    init_variant_tablet(41000, 10, true);

    std::vector<RowsetSharedPtr> input_rowsets;
    input_rowsets.push_back(
            create_variant_rowset({{R"({"session_id": 1, "tags": ["a"], "score": 10})",
                                    R"({"session_id": 2, "tags": []})"}},
                                  1));
    input_rowsets.push_back(
            create_variant_rowset({{R"({"score": 30})", R"({"session_id": 4})"}}, 2));

    auto readers = create_rowset_readers(input_rowsets);
    segment_v2::NestedGroupStreamingWritePlan plan;
    auto st = segment_v2::build_nested_group_streaming_write_plan(readers,
                                                                  _tablet_schema->column(0), &plan);
    ASSERT_TRUE(st.ok()) << st.to_string();

    EXPECT_FALSE(plan.has_conflict_paths);
    EXPECT_FALSE(plan.has_root_nested_group);
    EXPECT_EQ(plan.conflict_policy, segment_v2::get_nested_group_conflict_policy());
    EXPECT_TRUE(plan.nested_groups.empty());
    EXPECT_EQ(collect_regular_paths(plan), std::set<std::string>({"score", "session_id", "tags"}));
    ASSERT_EQ(plan.regular_subcolumns.size(), 3);
    EXPECT_EQ(plan.regular_subcolumns[0].path, "score");
    EXPECT_EQ(plan.regular_subcolumns[1].path, "session_id");
    EXPECT_EQ(plan.regular_subcolumns[2].path, "tags");
    ASSERT_NE(plan.regular_subcolumns[2].data_type, nullptr);
    EXPECT_NE(plan.regular_subcolumns[2].data_type->get_name().find("Array"), std::string::npos);
}

TEST_F(VariantColumnWriterReaderTest,
       test_streaming_compaction_writer_streams_regular_array_paths_across_batches) {
    if (!nested_group_write_path_available()) {
        GTEST_SKIP() << "NestedGroup write path is not available in this build";
    }

    init_variant_tablet(41001, 10, true);

    std::vector<RowsetSharedPtr> input_rowsets;
    input_rowsets.push_back(
            create_variant_rowset({{R"({"session_id": 10, "tags": ["seed_10"]})"}}, 1));
    input_rowsets.push_back(create_variant_rowset({{R"({"session_id": 20})"}}, 2));
    auto input_readers = create_rowset_readers(input_rowsets);

    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "streaming_compaction", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    ASSERT_TRUE(st.ok()) << st.msg();

    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    opts.input_rs_readers = input_readers;

    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_COMPACTION;
    rowset_ctx.tablet_schema = _tablet_schema;
    rowset_ctx.input_rs_readers = input_readers;
    opts.rowset_ctx = &rowset_ctx;

    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    ASSERT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    ASSERT_TRUE(writer->init().ok());

    auto* variant_writer = assert_cast<VariantColumnWriter*>(writer.get());
    ASSERT_NE(variant_writer, nullptr);
    auto* writer_impl = variant_writer->impl_for_test();
    ASSERT_NE(writer_impl, nullptr);
    EXPECT_TRUE(writer_impl->has_streaming_compaction_writer_for_test());
    EXPECT_FALSE(writer_impl->is_finalized());

    const std::vector<std::string> batch1 = {R"({"session_id": 1, "tags": ["topic_1", "topic_2"]})",
                                             R"({"session_id": 2, "tags": []})"};
    const std::vector<std::string> batch2 = {R"({"session_id": 3})",
                                             R"({"session_id": 4, "tags": [null, "topic_4"]})"};

    std::vector<std::string> expected_rows =
            normalize_json_rows(batch1, column.variant_max_subcolumns_count());
    auto normalized_batch2 = normalize_json_rows(batch2, column.variant_max_subcolumns_count());
    expected_rows.insert(expected_rows.end(), normalized_batch2.begin(), normalized_batch2.end());

    ASSERT_TRUE(append_json_batch(writer.get(), batch1).ok());
    EXPECT_FALSE(writer_impl->is_finalized());
    ASSERT_TRUE(append_json_batch(writer.get(), batch2).ok());
    EXPECT_FALSE(writer_impl->is_finalized());

    ASSERT_TRUE(writer->finish().ok());
    EXPECT_TRUE(writer_impl->is_finalized());
    ASSERT_TRUE(writer->write_data().ok());
    ASSERT_TRUE(writer->write_ordinal_index().ok());
    ASSERT_TRUE(writer->write_zone_map().ok());
    ASSERT_TRUE(file_writer->close().ok());
    footer.set_num_rows(writer->get_next_rowid());

    EXPECT_EQ(footer.columns_size(), 3);

    std::vector<std::string> actual_rows;
    st = read_root_rows(footer, file_path, &actual_rows);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(actual_rows, expected_rows);
}

// Regression test: compaction on no-key duplicate table with variant column uid=0.
// TabletColumn::is_extracted_column() used "_parent_col_unique_id > 0" which
// incorrectly returned false for subcolumns whose parent has uid=0, causing
// VariantColumnWriterImpl to duplicate sparse column entries in segment footer.
// Without fix: DCHECK(uid >= 0) fires in segment_iterator.cpp because
// is_extracted_column() wrongly returns false for extracted cols with parent uid=0,
// making them take the non-extracted path where uid=-1 violates the check.
TEST_F(VariantColumnWriterReaderTest, test_compaction_nokey_variant_uid0) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), /*col_unique_id=*/0, "VARIANT", "v1",
                     /*variant_max_subcolumns_count=*/3);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    tablet_meta->_tablet_id = 99900;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    ASSERT_TRUE(_tablet->init().ok());
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    auto rs0 = create_variant_rowset(
            {{R"({"name":"Alice","age":30})", R"({"name":"Bob","age":25})"}}, 2);
    auto rs1 =
            create_variant_rowset({{R"({"name":"u1","age":10})", R"({"name":"u2","age":20})"}}, 3);

    std::vector<RowsetSharedPtr> input_rowsets {rs0, rs1};
    auto input_readers = create_rowset_readers(input_rowsets);

    auto compaction_schema = std::make_shared<TabletSchema>(*_tablet_schema);
    auto st = variant_util::VariantCompactionUtil::get_extended_compaction_schema(
            input_rowsets, compaction_schema);
    ASSERT_TRUE(st.ok()) << st.to_string();

    RowsetWriterContext ctx;
    RowsetId rowset_id;
    rowset_id.init(9999);
    ctx.rowset_id = rowset_id;
    ctx.rowset_type = BETA_ROWSET;
    ctx.data_dir = _data_dir.get();
    ctx.rowset_state = VISIBLE;
    ctx.tablet_schema = compaction_schema;
    ctx.tablet_path = _tablet->tablet_path();
    ctx.tablet_id = _tablet->tablet_id();
    ctx.tablet = _tablet;
    ctx.version = Version(2, 3);
    ctx.write_type = DataWriteType::TYPE_COMPACTION;
    auto res = RowsetFactory::create_rowset_writer(*_engine_ref, ctx, true);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto output_writer = std::move(res).value();

    Merger::Statistics stats;
    st = Merger::vertical_merge_rowsets(_tablet, ReaderType::READER_CUMULATIVE_COMPACTION,
                                        *compaction_schema, input_readers, output_writer.get(),
                                        10000, 2, &stats);
    ASSERT_TRUE(st.ok()) << st.to_string();

    RowsetSharedPtr output_rowset;
    ASSERT_TRUE(output_writer->build(output_rowset).ok());
    ASSERT_EQ(output_rowset->num_rows(), 4);
}

} // namespace doris
