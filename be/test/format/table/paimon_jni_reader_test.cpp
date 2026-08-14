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
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/value/variant/variant_batch_builder.h"
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

} // namespace
} // namespace doris
