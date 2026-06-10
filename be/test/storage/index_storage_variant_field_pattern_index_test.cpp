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

#include <memory>
#include <string>
#include <string_view>

#include "core/data_type/data_type_number.h"
#include "storage/predicate/predicate_creator.h"
#include "testutil/index_storage_test_util.h"

namespace doris::index_storage_test {
namespace {

constexpr int32_t kVariantUid = 2;
constexpr int64_t kIntPatternIndexId = 210201;
constexpr int64_t kStringPatternIndexId = 210202;
constexpr std::string_view kIntPath = "int_1";

std::shared_ptr<ColumnPredicate> int_equals(int32_t column_id, std::string column_name,
                                            int32_t value) {
    return create_comparison_predicate<PredicateType::EQ>(
            column_id, std::move(column_name), std::make_shared<DataTypeInt32>(),
            Field::create_field<TYPE_INT>(value), false);
}

VariantColumnSpec typed_pattern_variant_column() {
    VariantColumnSpec variant;
    variant.unique_id = kVariantUid;
    variant.name = "v";
    variant.max_subcolumns_count = 2;
    variant.predefined_paths = {
            VariantPathSpec {.path = "int_*",
                             .type = FieldType::OLAP_FIELD_TYPE_INT,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME_GLOB,
                             .array_item_type = {},
                             .array_item_nullable = true},
            VariantPathSpec {.path = "string_*",
                             .type = FieldType::OLAP_FIELD_TYPE_STRING,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME_GLOB,
                             .array_item_type = {},
                             .array_item_nullable = true},
    };
    return variant;
}

} // namespace

class IndexStorageVariantFieldPatternIndexTest : public IndexStorageTestFixture {
protected:
    void run_typed_int_field_pattern_index_lifecycle(IndexCompactionKind compaction_kind,
                                                     int64_t tablet_id);
};

void IndexStorageVariantFieldPatternIndexTest::run_typed_int_field_pattern_index_lifecycle(
        IndexCompactionKind compaction_kind, int64_t tablet_id) {
    const auto int_index = IndexSpec::field_pattern_index(kIntPatternIndexId, "idx_v_int_glob",
                                                          kVariantUid, "int_*");
    const auto string_index = IndexSpec::field_pattern_index(
            kStringPatternIndexId, "idx_v_string_glob", kVariantUid, "string_*");
    const auto index_case = IndexStorageCaseBuilder("typed_int_field_pattern_index_lifecycle")
                                    .tablet_id(tablet_id)
                                    .variant_column(typed_pattern_variant_column())
                                    .inverted_index(int_index)
                                    .inverted_index(string_index)
                                    .rowset(0, IndexDataSourceSpec::inline_variant(
                                                       {R"({"int_1": 42, "string_1": "sample"})",
                                                        R"({"int_1": 7, "string_1": "other"})"},
                                                       0))
                                    .rowset(1, IndexDataSourceSpec::inline_variant(
                                                       {R"({"int_1": 42, "string_1": "sample"})",
                                                        R"({"int_1": 8, "string_1": "other"})"},
                                                       100))
                                    .build();
    ASSERT_TRUE(create_tablet(index_case.tablet_options).ok());
    auto rowsets = write_rowsets(index_case.rowsets);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto readable_rowsets = rowsets_with_variant_extended_schema(rowsets.value());
    ASSERT_TRUE(readable_rowsets.has_value()) << readable_rowsets.error();
    const int32_t path_column_id = column_id_by_path("v.int_1");
    ASSERT_GE(path_column_id, 0) << dump_schema_paths(*tablet_schema());
    const auto& path_column = tablet_schema()->column(path_column_id);

    IndexReadOptions read_options;
    read_options.return_columns = {0, static_cast<uint32_t>(path_column_id)};
    read_options.target_cast_type_for_variants[path_column.name()] =
            std::make_shared<DataTypeInt32>();
    read_options.predicates.push_back(int_equals(path_column_id, path_column.name(), 42));

    auto before_compaction = read_rowsets(readable_rowsets.value(), read_options);
    ASSERT_TRUE(before_compaction.has_value()) << before_compaction.error();
    EXPECT_EQ(before_compaction->rows_read, 2);
    expect_applied_variant_path_index(before_compaction.value(), kIntPath, kIntPatternIndexId, 2);
    expect_index_not_applied(before_compaction.value(), kStringPatternIndexId);

    auto compacted = compact_rowsets(compaction_kind, rowsets.value());
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    EXPECT_EQ(compacted.value()->num_rows(), 4);

    auto reloaded = reload_rowsets({compacted.value()});
    ASSERT_TRUE(reloaded.has_value()) << reloaded.error();
    auto readable_compacted = rowsets_with_variant_extended_schema(reloaded.value());
    ASSERT_TRUE(readable_compacted.has_value()) << readable_compacted.error();
    const int32_t compacted_path_column_id = column_id_by_path("v.int_1");
    ASSERT_EQ(compacted_path_column_id, path_column_id);

    auto after_compaction = read_rowsets(readable_compacted.value(), read_options);
    ASSERT_TRUE(after_compaction.has_value()) << after_compaction.error();
    EXPECT_EQ(after_compaction->rows_read, 2);
    expect_applied_variant_path_index(after_compaction.value(), kIntPath, kIntPatternIndexId, 2);
    expect_index_not_applied(after_compaction.value(), kStringPatternIndexId);
}

TEST_F(IndexStorageVariantFieldPatternIndexTest, TypedIntIndexAfterCumulativeCompaction) {
    run_typed_int_field_pattern_index_lifecycle(IndexCompactionKind::CUMULATIVE, 110032);
}

TEST_F(IndexStorageVariantFieldPatternIndexTest, TypedIntIndexAfterFullCompaction) {
    run_typed_int_field_pattern_index_lifecycle(IndexCompactionKind::FULL, 110033);
}

} // namespace doris::index_storage_test
