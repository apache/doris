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

#include "format/table/paimon_jni_reader.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_variant.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/value/variant/variant_batch_builder.h"
#include "exec/common/variant_util.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "format/jni/jni_data_bridge.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state.h"

namespace doris {
namespace {

TFileRangeDesc make_legacy_paimon_jni_range() {
    TFileRangeDesc range;
    TTableFormatFileDesc table_format_params;
    table_format_params.__set_table_format_type("paimon");
    TPaimonFileDesc paimon_params;
    paimon_params.__set_paimon_split("serialized-split");
    table_format_params.__set_paimon_params(std::move(paimon_params));
    range.__set_table_format_params(std::move(table_format_params));
    return range;
}

struct JavaVarbinaryEntry {
    int64_t length;
    uint64_t address;
};

static_assert(sizeof(JavaVarbinaryEntry) == 16);

TEST(LegacyPaimonJniReaderTest, GeneratesMissingOrEmptySerializedTableCacheKey) {
    const auto range = make_legacy_paimon_jni_range();
    TFileScanRangeParams scan_params;
    scan_params.__set_serialized_table("serialized-table");
    scan_params.__set_paimon_predicate("serialized-predicate");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    const std::vector<SlotDescriptor*> file_slot_descs;

    PaimonJniReader missing_key_reader(file_slot_descs, &state, nullptr, range, &scan_params);
    EXPECT_EQ(missing_key_reader._scanner_params["serialized_table"], "serialized-table");
    const auto& missing_key = missing_key_reader._scanner_params["serialized_table_cache_key"];
    EXPECT_FALSE(missing_key.empty());

    scan_params.__set_serialized_table_cache_key("");
    PaimonJniReader empty_key_reader(file_slot_descs, &state, nullptr, range, &scan_params);
    EXPECT_EQ(empty_key_reader._scanner_params["serialized_table"], "serialized-table");
    const auto& empty_key = empty_key_reader._scanner_params["serialized_table_cache_key"];
    EXPECT_FALSE(empty_key.empty());
    EXPECT_NE(missing_key, empty_key);
}

TEST(LegacyPaimonJniReaderTest, PublishesVariantV2BinaryStructSchema) {
    const DataTypePtr type = std::make_shared<DataTypeVariantV2>();
    EXPECT_EQ(JniDataBridge::get_jni_type_with_different_string(type),
              "struct<value:varbinary,metadata:varbinary>");
    EXPECT_EQ(JniDataBridge::get_jni_type_with_encoded_struct_fields(type),
              "struct<$dmFsdWU=:varbinary,$bWV0YWRhdGE=:varbinary>");
}

TEST(LegacyPaimonJniReaderTest, DecodesVariantV2BinaryStructFromJavaMetadata) {
    const std::string json = R"({"id":7,"tags":["doris"]})";
    JsonStringToVariantEncoder encoder;
    encoder.add_json({json.data(), json.size()});
    VariantBatchBuilder source = encoder.finish_batch();
    const VariantRef expected = source.value_at(0);

    std::array<uint8_t, 2> outer_nulls {0, 1};
    std::array<uint8_t, 2> child_nulls {0, 1};
    std::array<JavaVarbinaryEntry, 2> value_entries {
            JavaVarbinaryEntry {static_cast<int64_t>(expected.value.size),
                                reinterpret_cast<uint64_t>(expected.value.data)},
            JavaVarbinaryEntry {0, 0}};
    std::array<JavaVarbinaryEntry, 2> metadata_entries {
            JavaVarbinaryEntry {static_cast<int64_t>(expected.metadata.size),
                                reinterpret_cast<uint64_t>(expected.metadata.data)},
            JavaVarbinaryEntry {0, 0}};
    std::array<long, 5> metadata {reinterpret_cast<long>(outer_nulls.data()),
                                  reinterpret_cast<long>(child_nulls.data()),
                                  reinterpret_cast<long>(value_entries.data()),
                                  reinterpret_cast<long>(child_nulls.data()),
                                  reinterpret_cast<long>(metadata_entries.data())};

    const DataTypePtr type = make_nullable(std::make_shared<DataTypeVariantV2>());
    ColumnPtr result = type->create_column();
    JniDataBridge::TableMetaAddress address(reinterpret_cast<long>(metadata.data()));
    ASSERT_TRUE(JniDataBridge::fill_column(address, result, type, 2).ok());

    const auto& nullable = assert_cast<const ColumnNullable&>(*result);
    EXPECT_EQ(nullable.get_null_map_data(), (NullMap {0, 1}));
    const auto& variants = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    ASSERT_EQ(variants.size(), 2);
    const VariantRef actual = variants.get_value_ref(0);
    EXPECT_EQ(actual.value, expected.value);
    EXPECT_EQ(std::string_view(actual.metadata.data, actual.metadata.size),
              std::string_view(expected.metadata.data, expected.metadata.size));
    EXPECT_TRUE(variants.get_value_ref(1).is_null());
}

TEST(LegacyPaimonJniReaderTest, DecodesLegacyVariantBinaryStructFromJavaMetadata) {
    const std::vector<std::string> json_rows {
            R"({"name":"alpha","score":12.5,"tags":["dts","fluss","paimon"]})",
            R"({"active":true,"name":"beta","nested":{"version":"2.0"}})", "null", "null", "123"};
    JsonStringToVariantEncoder encoder;
    for (const auto& json : json_rows) {
        encoder.add_json({json.data(), json.size()});
    }
    VariantBatchBuilder source = encoder.finish_batch();

    std::array<uint8_t, 5> outer_nulls {0, 0, 0, 1, 0};
    std::array<uint8_t, 5> child_nulls {0, 0, 0, 1, 0};
    std::array<JavaVarbinaryEntry, 5> value_entries;
    std::array<JavaVarbinaryEntry, 5> metadata_entries;
    for (size_t row = 0; row < json_rows.size(); ++row) {
        const VariantRef value = source.value_at(row);
        value_entries[row] = {static_cast<int64_t>(value.value.size),
                              reinterpret_cast<uint64_t>(value.value.data)};
        metadata_entries[row] = {static_cast<int64_t>(value.metadata.size),
                                 reinterpret_cast<uint64_t>(value.metadata.data)};
    }
    value_entries[3] = {0, 0};
    metadata_entries[3] = {0, 0};
    std::array<long, 5> metadata {reinterpret_cast<long>(outer_nulls.data()),
                                  reinterpret_cast<long>(child_nulls.data()),
                                  reinterpret_cast<long>(value_entries.data()),
                                  reinterpret_cast<long>(child_nulls.data()),
                                  reinterpret_cast<long>(metadata_entries.data())};

    const DataTypePtr type = make_nullable(std::make_shared<DataTypeVariant>(0, false));
    ColumnPtr result = type->create_column();
    JniDataBridge::TableMetaAddress address(reinterpret_cast<long>(metadata.data()));
    ASSERT_TRUE(JniDataBridge::fill_column(address, result, type, 5).ok());

    const auto& nullable = assert_cast<const ColumnNullable&>(*result);
    EXPECT_EQ(nullable.get_null_map_data(), (NullMap {0, 0, 0, 1, 0}));
    const auto& variants = assert_cast<const ColumnVariant&>(nullable.get_nested_column());
    ASSERT_EQ(variants.rows(), 5);
    auto finalized = variants.clone_finalized();
    const auto& decoded = assert_cast<const ColumnVariant&>(*finalized);
    DataTypeSerDe::FormatOptions options;
    std::string object_json;
    decoded.serialize_one_row_to_string(0, &object_json, options);
    const std::string expected_object_json =
            R"({"name":"alpha","score":12.5,"tags":["dts", "fluss", "paimon"]})";
    EXPECT_EQ(object_json, expected_object_json);
    std::string nested_object_json;
    decoded.serialize_one_row_to_string(1, &nested_object_json, options);
    EXPECT_EQ(nested_object_json, R"({"active":1,"name":"beta","nested":{"version":"2.0"}})");
    std::string encoded_json_null;
    decoded.serialize_one_row_to_string(2, &encoded_json_null, options);
    // Legacy VARIANT V1 materializes a root JSON null as an empty object.
    EXPECT_EQ(encoded_json_null, "{}");
    std::string scalar_number;
    decoded.serialize_one_row_to_string(4, &scalar_number, options);
    EXPECT_EQ(scalar_number, "123");

    IColumn::Permutation permutation {0, 1, 2, 3, 4};
    auto permuted_base = nullable.permute(permutation, 0);
    const auto& permuted_nullable = assert_cast<const ColumnNullable&>(*permuted_base);
    const auto& permuted_variants =
            assert_cast<const ColumnVariant&>(permuted_nullable.get_nested_column());
    auto finalized_permutation = permuted_variants.clone_finalized();
    const auto& permuted = assert_cast<const ColumnVariant&>(*finalized_permutation);
    std::string first_permuted;
    permuted.serialize_one_row_to_string(0, &first_permuted, options);
    EXPECT_EQ(first_permuted, expected_object_json);
    std::string second_permuted;
    permuted.serialize_one_row_to_string(1, &second_permuted, options);
    EXPECT_EQ(second_permuted, R"({"active":1,"name":"beta","nested":{"version":"2.0"}})");
    std::string encoded_null_permuted;
    permuted.serialize_one_row_to_string(2, &encoded_null_permuted, options);
    EXPECT_EQ(encoded_null_permuted, "{}");
    std::string fifth_permuted;
    permuted.serialize_one_row_to_string(4, &fifth_permuted, options);
    EXPECT_EQ(fifth_permuted, "123");

    auto copied_rows = type->create_column();
    for (size_t row = 0; row < result->size(); ++row) {
        copied_rows->insert_from(*result, row);
    }
    const auto& copied_nullable = assert_cast<const ColumnNullable&>(*copied_rows);
    EXPECT_EQ(copied_nullable.get_null_map_data(), (NullMap {0, 0, 0, 1, 0}));
    const auto& copied_variants =
            assert_cast<const ColumnVariant&>(copied_nullable.get_nested_column());
    auto finalized_copy = copied_variants.clone_finalized();
    const auto& copied = assert_cast<const ColumnVariant&>(*finalized_copy);
    std::string copied_nested_object;
    copied.serialize_one_row_to_string(1, &copied_nested_object, options);
    EXPECT_EQ(copied_nested_object, R"({"active":1,"name":"beta","nested":{"version":"2.0"}})");

    auto merged = ColumnVariant::create(0, false);
    ParseConfig parse_config;
    for (size_t row = 0; row < json_rows.size(); ++row) {
        auto source_column = ColumnVariant::create(0, false);
        if (outer_nulls[row]) {
            source_column->insert_default();
        } else {
            variant_util::parse_json_to_variant(*source_column,
                                                {json_rows[row].data(), json_rows[row].size()},
                                                nullptr, parse_config);
        }
        merged->insert_range_from(*source_column, 0, 1);
    }
    merged->finalize();
    auto merged_permuted = merged->permute(permutation, 0);
    const auto& merged_result = assert_cast<const ColumnVariant&>(*merged_permuted);
    std::string merged_second_row;
    merged_result.serialize_one_row_to_string(1, &merged_second_row, options);
    EXPECT_EQ(merged_second_row, R"({"active":1,"name":"beta","nested":{"version":"2.0"}})");

    auto merged_batch = ColumnVariant::create(0, false);
    merged_batch->insert_range_from(variants, 0, variants.rows());
    merged_batch->finalize();
    auto merged_batch_permuted = merged_batch->permute(permutation, 0);
    const auto& merged_batch_result = assert_cast<const ColumnVariant&>(*merged_batch_permuted);
    std::string merged_batch_second_row;
    merged_batch_result.serialize_one_row_to_string(1, &merged_batch_second_row, options);
    EXPECT_EQ(merged_batch_second_row, R"({"active":1,"name":"beta","nested":{"version":"2.0"}})");
}

} // namespace
} // namespace doris
