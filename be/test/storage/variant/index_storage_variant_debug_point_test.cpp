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

#include <algorithm>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_variant.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "testutil/index_storage_test_util.h"

namespace doris::index_storage_test {
namespace {

constexpr int32_t kVariantUid = 2;
constexpr int64_t kArrayPathIndexId = 210001;
constexpr std::string_view kArrayPath = "c_arr";

VariantColumnSpec array_text_variant_column() {
    VariantColumnSpec variant;
    variant.unique_id = kVariantUid;
    variant.name = "v";
    variant.nullable = true;
    variant.max_subcolumns_count = 10;
    variant.predefined_paths = {
            VariantPathSpec {.path = std::string(kArrayPath),
                             .type = FieldType::OLAP_FIELD_TYPE_ARRAY,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME,
                             .array_item_type = FieldType::OLAP_FIELD_TYPE_STRING,
                             .array_item_nullable = true},
    };
    return variant;
}

ColumnPtr single_array_variant_column(const std::vector<std::optional<std::string>>& elements,
                                      bool array_is_null = false) {
    auto values = ColumnString::create();
    auto item_null_map = ColumnUInt8::create();
    if (!array_is_null) {
        for (const auto& element : elements) {
            if (element.has_value()) {
                values->insert_data(element->data(), element->size());
                item_null_map->insert_value(0);
            } else {
                values->insert_default();
                item_null_map->insert_value(1);
            }
        }
    }

    auto nullable_items = ColumnNullable::create(std::move(values), std::move(item_null_map));
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(array_is_null ? 0 : elements.size());
    auto array = ColumnArray::create(std::move(nullable_items), std::move(offsets));
    auto array_null_map = ColumnUInt8::create();
    array_null_map->insert_value(array_is_null ? 1 : 0);
    auto nullable_array = ColumnNullable::create(std::move(array), std::move(array_null_map));

    auto variant = ColumnVariant::create(10, false);
    variant->insert_default();
    auto array_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())));
    CHECK(variant->add_sub_column(PathInData(std::string(kArrayPath), true),
                                  std::move(nullable_array), array_type));
    return variant;
}

ColumnPtr single_null_variant_column() {
    auto variant = ColumnVariant::create(10, false);
    variant->insert_default();
    auto null_map = ColumnUInt8::create();
    null_map->insert_value(1);
    return ColumnNullable::create(std::move(variant), std::move(null_map));
}

IndexRowsetSpec single_array_variant_rowset(int64_t version,
                                            std::vector<std::optional<std::string>> elements,
                                            bool array_is_null = false) {
    IndexRowsetSpec rowset;
    rowset.version = version;
    rowset.batches.push_back(IndexBatch::single_variant_column(
            single_array_variant_column(elements, array_is_null), static_cast<int32_t>(version)));
    return rowset;
}

IndexRowsetSpec single_null_variant_rowset(int64_t version) {
    IndexRowsetSpec rowset;
    rowset.version = version;
    rowset.batches.push_back(IndexBatch::single_variant_column(single_null_variant_column(),
                                                               static_cast<int32_t>(version)));
    return rowset;
}

} // namespace

class IndexStorageVariantDebugPointTest : public IndexStorageTestFixture {
protected:
    RowsetSharedPtr write_array_rowset_with_expected_field_count(
            int64_t version, std::vector<std::optional<std::string>> elements,
            int32_t expected_field_count, bool array_is_null = false) {
        ScopedDebugPoint debug_point(
                "array_inverted_index.write_index",
                {{"single_array_field_count", std::to_string(expected_field_count)}});
        auto rowset = write_rowset(
                single_array_variant_rowset(version, std::move(elements), array_is_null));
        EXPECT_GT(debug_point.execute_num(), 0);
        EXPECT_TRUE(rowset.has_value()) << rowset.error();
        if (!rowset.has_value()) {
            return nullptr;
        }

        auto probe = probe_rowset(rowset.value());
        EXPECT_TRUE(probe.has_value()) << probe.error();
        if (probe.has_value()) {
            expect_index_files(probe.value(), true);
        }
        return rowset.value();
    }

    RowsetSharedPtr write_null_variant_rowset_with_array_debug_point(int64_t version) {
        ScopedDebugPoint debug_point("array_inverted_index.write_index",
                                     {{"single_array_field_count", "0"}});
        auto rowset = write_rowset(single_null_variant_rowset(version));
        EXPECT_TRUE(rowset.has_value()) << rowset.error();
        if (!rowset.has_value()) {
            return nullptr;
        }
        return rowset.value();
    }
};

TEST_F(IndexStorageVariantDebugPointTest, ArrayPathIndexWriteCountsNonNullElements) {
    IndexTabletOptions options;
    options.tablet_id = 110029;
    options.variant_columns = {array_text_variant_column()};
    options.inverted_indexes.push_back(IndexSpec::field_pattern_index(
            kArrayPathIndexId, "index_inverted_c_arr", kVariantUid, std::string(kArrayPath)));
    ASSERT_TRUE(create_tablet(options).ok());

    std::vector<RowsetSharedPtr> rowsets;
    rowsets.push_back(
            write_array_rowset_with_expected_field_count(0, {"amory", "is", "committer"}, 3));
    rowsets.push_back(write_array_rowset_with_expected_field_count(1, {"amory", "better"}, 2));
    rowsets.push_back(write_array_rowset_with_expected_field_count(2, {"amory", std::nullopt}, 1));
    rowsets.push_back(
            write_array_rowset_with_expected_field_count(3, {std::nullopt, std::nullopt}, 0));
    rowsets.push_back(write_array_rowset_with_expected_field_count(4, {}, 0));
    rowsets.push_back(write_array_rowset_with_expected_field_count(5, {}, 0, true));
    rowsets.push_back(write_null_variant_rowset_with_array_debug_point(6));
    ASSERT_TRUE(std::all_of(rowsets.begin(), rowsets.end(),
                            [](const auto& rowset) { return rowset != nullptr; }));

    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets);
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    EXPECT_EQ(compacted.value()->num_rows(), 7);

    auto compacted_probe = probe_rowset(compacted.value());
    ASSERT_TRUE(compacted_probe.has_value()) << compacted_probe.error();
    expect_index_files(compacted_probe.value(), true);
}

TEST_F(IndexStorageVariantDebugPointTest, ArrayPathIndexAcceptsMixedTypedElements) {
    IndexTabletOptions options;
    options.tablet_id = 110030;
    options.variant_columns = {array_text_variant_column()};
    options.inverted_indexes.push_back(IndexSpec::field_pattern_index(
            kArrayPathIndexId, "index_inverted_c_arr", kVariantUid, std::string(kArrayPath)));
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.batches.push_back(IndexBatch::single_variant(
            {R"({"c_arr": ["text"]})", R"({"c_arr": [1.1]})", R"({"c_arr": [1.0]})",
             R"({"c_arr": [90]})", R"({"c_arr": [90999999999999]})"},
            0));
    rowset.batches.back().parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
    auto rowset_result = write_rowset(rowset);
    ASSERT_TRUE(rowset_result.has_value()) << rowset_result.error();

    auto probe = probe_rowset(rowset_result.value());
    ASSERT_TRUE(probe.has_value()) << probe.error();
    expect_index_files(probe.value(), true);
}

} // namespace doris::index_storage_test
