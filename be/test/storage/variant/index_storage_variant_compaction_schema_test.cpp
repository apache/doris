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
#include <rapidjson/document.h>

#include <random>

#include "common/config.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_variant.h"
#include "exec/common/variant_util.h"
#include "exprs/vectorized_fn_call.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "storage/compaction/cumulative_compaction.h"
#include "storage/segment/variant/variant_column_reader.h"
#include "storage/variant/index_storage_variant_test_base.h"
#include "testutil/mock/mock_slot_ref.h"

namespace doris::index_storage_test {

class IndexStorageVariantCompactionSchemaTest : public IndexStorageTestFixture {
protected:
};

TEST(IndexStorageVariantCompactionUtilTest, GetSubpathsHonorsZeroLimitAndTieOrdering) {
    variant_util::PathToNoneNullValues stats {
            {"alpha", 3},
            {"beta", 3},
            {"gamma", 1},
    };

    VariantCompactionPaths unlimited;
    variant_util::VariantCompactionUtil::get_subpaths(0, stats, unlimited);
    EXPECT_TRUE(unlimited.sub_path_set.contains(StringRef("alpha")));
    EXPECT_TRUE(unlimited.sub_path_set.contains(StringRef("beta")));
    EXPECT_TRUE(unlimited.sub_path_set.contains(StringRef("gamma")));
    EXPECT_TRUE(unlimited.sparse_path_set.empty());

    VariantCompactionPaths top_one;
    variant_util::VariantCompactionUtil::get_subpaths(1, stats, top_one);
    EXPECT_TRUE(top_one.sub_path_set.contains(StringRef("beta")));
    EXPECT_FALSE(top_one.sub_path_set.contains(StringRef("alpha")));
    EXPECT_TRUE(top_one.sparse_path_set.contains(StringRef("alpha")));
    EXPECT_TRUE(top_one.sparse_path_set.contains(StringRef("gamma")));
}

TEST(IndexStorageVariantCompactionUtilTest, GetSubpathsKeepsAllPathsAtLimitAndHandlesEmptyStats) {
    variant_util::PathToNoneNullValues exact_limit {
            {"alpha", 2},
            {"beta", 1},
    };

    VariantCompactionPaths at_limit;
    variant_util::VariantCompactionUtil::get_subpaths(2, exact_limit, at_limit);
    EXPECT_TRUE(at_limit.sub_path_set.contains(StringRef("alpha")));
    EXPECT_TRUE(at_limit.sub_path_set.contains(StringRef("beta")));
    EXPECT_TRUE(at_limit.sparse_path_set.empty());

    VariantCompactionPaths empty;
    variant_util::VariantCompactionUtil::get_subpaths(1, {}, empty);
    EXPECT_TRUE(empty.sub_path_set.empty());
    EXPECT_TRUE(empty.sparse_path_set.empty());
}

TEST(IndexStorageVariantCompactionUtilTest, EmptyInputsKeepVariantSchemaWithoutPathStats) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";

    IndexTabletOptions options;
    options.tablet_id = 110054;
    options.variant_columns = {std::move(variant)};
    auto base_schema = build_tablet_schema(options);
    ASSERT_NE(base_schema, nullptr);

    EXPECT_EQ(
            variant_util::VariantCompactionUtil::calculate_variant_extended_schema({}, base_schema),
            nullptr);

    auto compaction_schema = std::make_shared<TabletSchema>(*base_schema);
    VariantCompactionPathsMap compaction_paths;
    auto status = variant_util::VariantCompactionUtil::get_extended_compaction_schema(
            {}, compaction_schema, compaction_paths);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(compaction_schema->has_column_unique_id(2));

    const auto* path_set_info = compaction_paths.contains(2) ? &compaction_paths.at(2) : nullptr;
    ASSERT_NE(path_set_info, nullptr);
    EXPECT_TRUE(path_set_info->typed_path_set.empty());
    EXPECT_TRUE(path_set_info->sub_path_set.empty());
    EXPECT_TRUE(path_set_info->sparse_path_set.empty());
}

TEST_F(IndexStorageVariantCompactionSchemaTest,
       VariantMaxSubcolumnsCountZeroMaterializesAllObservedPaths) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 0;

    IndexTabletOptions options;
    options.tablet_id = 110049;
    options.variant_columns = {std::move(variant)};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.batches.push_back(IndexBatch::single_variant(
            {R"({"alpha": "a0", "beta": "b0"})", R"({"alpha": "a1", "gamma": "g1"})"}, 0));
    auto rowset_result = write_rowset(rowset);
    ASSERT_TRUE(rowset_result.has_value()) << rowset_result.error();

    auto compaction_schema = std::make_shared<TabletSchema>(*tablet_schema());
    VariantCompactionPathsMap compaction_paths;
    auto status = variant_util::VariantCompactionUtil::get_extended_compaction_schema(
            {rowset_result.value()}, compaction_schema, compaction_paths);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto* path_set_info = compaction_paths.contains(2) ? &compaction_paths.at(2) : nullptr;
    ASSERT_NE(path_set_info, nullptr);
    EXPECT_TRUE(path_set_info->sub_path_set.contains(StringRef("alpha")));
    EXPECT_TRUE(path_set_info->sub_path_set.contains(StringRef("beta")));
    EXPECT_TRUE(path_set_info->sub_path_set.contains(StringRef("gamma")));
    EXPECT_TRUE(path_set_info->sparse_path_set.empty());
    EXPECT_TRUE(schema_has_extracted_variant_path(*compaction_schema, 2, "alpha"));
    EXPECT_TRUE(schema_has_extracted_variant_path(*compaction_schema, 2, "beta"));
    EXPECT_TRUE(schema_has_extracted_variant_path(*compaction_schema, 2, "gamma"));
}

TEST_F(IndexStorageVariantCompactionSchemaTest,
       VariantPathCountAtLimitMaterializesAllObservedPaths) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 2;

    IndexTabletOptions options;
    options.tablet_id = 110050;
    options.variant_columns = {std::move(variant)};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.batches.push_back(IndexBatch::single_variant(
            {R"({"alpha": "a0", "beta": "b0"})", R"({"alpha": "a1", "beta": "b1"})"}, 0));
    auto rowset_result = write_rowset(rowset);
    ASSERT_TRUE(rowset_result.has_value()) << rowset_result.error();

    auto compaction_schema = std::make_shared<TabletSchema>(*tablet_schema());
    VariantCompactionPathsMap compaction_paths;
    auto status = variant_util::VariantCompactionUtil::get_extended_compaction_schema(
            {rowset_result.value()}, compaction_schema, compaction_paths);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto* path_set_info = compaction_paths.contains(2) ? &compaction_paths.at(2) : nullptr;
    ASSERT_NE(path_set_info, nullptr);
    EXPECT_TRUE(path_set_info->sub_path_set.contains(StringRef("alpha")));
    EXPECT_TRUE(path_set_info->sub_path_set.contains(StringRef("beta")));
    EXPECT_TRUE(path_set_info->sparse_path_set.empty());
    EXPECT_TRUE(schema_has_extracted_variant_path(*compaction_schema, 2, "alpha"));
    EXPECT_TRUE(schema_has_extracted_variant_path(*compaction_schema, 2, "beta"));
}

TEST_F(IndexStorageVariantCompactionSchemaTest,
       VariantTypedPathAndDynamicSubpathStaySeparatedInCompactionSchema) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 2;
    variant.predefined_paths = {
            VariantPathSpec {.path = "typed_i",
                             .type = FieldType::OLAP_FIELD_TYPE_INT,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME,
                             .array_item_type = {},
                             .array_item_nullable = true},
    };

    IndexTabletOptions options;
    options.tablet_id = 110052;
    options.variant_columns = {std::move(variant)};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.batches.push_back(IndexBatch::single_variant(
            {R"({"typed_i": 1, "hot": "h0"})", R"({"typed_i": 2, "cold": "c1"})"}, 0));
    auto rowset_result = write_rowset(rowset);
    ASSERT_TRUE(rowset_result.has_value()) << rowset_result.error();

    auto compaction_schema = std::make_shared<TabletSchema>(*tablet_schema());
    VariantCompactionPathsMap compaction_paths;
    auto status = variant_util::VariantCompactionUtil::get_extended_compaction_schema(
            {rowset_result.value()}, compaction_schema, compaction_paths);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto* path_set_info = compaction_paths.contains(2) ? &compaction_paths.at(2) : nullptr;
    ASSERT_NE(path_set_info, nullptr);
    EXPECT_TRUE(path_set_info->typed_path_set.contains("typed_i"));
    EXPECT_FALSE(path_set_info->sub_path_set.contains(StringRef("typed_i")));
    EXPECT_TRUE(path_set_info->sub_path_set.contains(StringRef("hot")));
    EXPECT_TRUE(path_set_info->sub_path_set.contains(StringRef("cold")));
    EXPECT_TRUE(schema_has_extracted_variant_path(*compaction_schema, 2, "typed_i"));
    EXPECT_TRUE(schema_has_extracted_variant_path(*compaction_schema, 2, "hot"));
    EXPECT_TRUE(schema_has_extracted_variant_path(*compaction_schema, 2, "cold"));
}

class VariantMasterPlanTest : public IndexStorageTestFixture,
                              public testing::WithParamInterface<int> {};

// The only master-plan exception is count=0 WITH old sparse input, tested separately by
// SparseOnlyOldPathSurvivesUnlimitedCompaction. An unaltered count=0 root has no sparse input.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_P(VariantMasterPlanTest, UnchangedTemplatesKeepExplicitMasterLayout) {
    const int mode = GetParam();
    const bool to_sparse = mode == 1;
    const bool unlimited = mode == 2 || mode == 4;
    IndexTabletOptions options;
    VariantColumnSpec variant;
    variant.max_subcolumns_count = unlimited ? 0 : 1;
    if (mode != 2) {
        variant.predefined_paths = {
                {.path = "typed_i", .type = FieldType::OLAP_FIELD_TYPE_INT, .array_item_type = {}}};
    }
    if (to_sparse) {
        variant.predefined_paths.push_back(
                {.path = "hot", .type = FieldType::OLAP_FIELD_TYPE_INT, .array_item_type = {}});
        variant.predefined_paths.push_back(
                {.path = "cold", .type = FieldType::OLAP_FIELD_TYPE_INT, .array_item_type = {}});
    }
    options.variant_columns = {variant};
    ASSERT_TRUE(create_tablet(options).ok());
    _tablet_schema->mutable_column_by_uid(2).set_variant_enable_typed_paths_to_sparse(to_sparse);
    IndexRowsetSpec input_spec;
    input_spec.batches.push_back(IndexBatch::single_variant(
            {R"({"typed_i":1,"hot":1,"cold":1})", R"({"typed_i":2,"hot":2})", R"({"hot":3})"}, 0));
    auto input = write_rowset(input_spec);
    ASSERT_TRUE(input.has_value()) << input.error();
    auto target = std::make_shared<TabletSchema>(*_tablet_schema);
    if (mode == 3) {
        target->mutable_column_by_uid(2).set_variant_max_subcolumns_count(2);
    }
    const auto base = std::make_shared<TabletSchema>(*target);
    std::unordered_map<int32_t, variant_util::VariantExtendedInfo> physical;
    ASSERT_TRUE(variant_util::VariantCompactionUtil::aggregate_variant_extended_info(input.value(),
                                                                                     &physical)
                        .ok());
    if (unlimited) {
        ASSERT_TRUE(physical.at(2).sparse_paths.empty());
    }
    VariantCompactionPathsMap paths;
    ASSERT_TRUE(variant_util::VariantCompactionUtil::get_extended_compaction_schema({input.value()},
                                                                                    target, paths)
                        .ok());
    ASSERT_TRUE(paths.contains(2));
    const auto& layout = paths.at(2);
    PathSet expected_sub {"hot"};
    PathSet expected_sparse;
    if (mode == 0) {
        expected_sparse.insert("cold");
    } else if (to_sparse) {
        expected_sparse = {"cold", "typed_i"};
    } else {
        expected_sub.insert("cold");
        if (mode == 2) {
            expected_sub.insert("typed_i");
        }
    }
    EXPECT_EQ(layout.sub_path_set, expected_sub);
    EXPECT_EQ(layout.sparse_path_set, expected_sparse);
    EXPECT_EQ(layout.typed_path_set.size(), mode == 2 || to_sparse ? 0 : 1);
    if (mode != 2 && !to_sparse) {
        EXPECT_TRUE(layout.typed_path_set.contains("typed_i"));
    }
    // Explicit master output column set and types, including root attributes, path flags and
    // the single sparse destination. No expected decision is obtained from the planner itself.
    std::map<std::string, FieldType> expected {
            {"k", FieldType::OLAP_FIELD_TYPE_INT},
            {"v", FieldType::OLAP_FIELD_TYPE_VARIANT},
            {"v.hot",
             to_sparse ? FieldType::OLAP_FIELD_TYPE_INT : FieldType::OLAP_FIELD_TYPE_BIGINT},
            {"v." + SPARSE_COLUMN_PATH, FieldType::OLAP_FIELD_TYPE_MAP}};
    if (!to_sparse) {
        expected["v.typed_i"] =
                mode == 2 ? FieldType::OLAP_FIELD_TYPE_BIGINT : FieldType::OLAP_FIELD_TYPE_INT;
    }
    if (mode >= 2) {
        expected["v.cold"] =
                mode == 3 ? FieldType::OLAP_FIELD_TYPE_VARIANT : FieldType::OLAP_FIELD_TYPE_BIGINT;
    }
    ASSERT_EQ(target->num_columns(), expected.size());
    std::set<std::string> seen;
    for (const auto& column : target->columns()) {
        ASSERT_TRUE(seen.insert(column->name()).second);
        ASSERT_TRUE(expected.contains(column->name())) << column->name();
        EXPECT_EQ(column->type(), expected.at(column->name())) << column->name();
        if (!column->is_extracted_column()) {
            EXPECT_EQ(*column, base->column_by_uid(column->unique_id()));
        } else {
            const auto& root = base->column_by_uid(2);
            TabletColumn expected_column;
            if (column->type() == FieldType::OLAP_FIELD_TYPE_MAP) {
                expected_column = variant_util::create_sparse_column(root);
            } else if ((column->name() == "v.typed_i" && mode != 2) || to_sparse) {
                TabletSchema::SubColumnInfo info;
                ASSERT_TRUE(variant_util::generate_sub_column_info(
                        *base, 2, column->name().substr(2), &info));
                expected_column = info.column;
                variant_util::inherit_column_attributes(root, expected_column);
            } else if (column->type() == FieldType::OLAP_FIELD_TYPE_VARIANT) {
                expected_column.set_name(column->name());
                expected_column.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
                expected_column.set_parent_unique_id(2);
                expected_column.set_path_info(PathInData(column->name()));
                expected_column.set_aggregation_method(root.aggregation());
                expected_column.set_variant_max_subcolumns_count(
                        root.variant_max_subcolumns_count());
                expected_column.set_variant_enable_doc_mode(root.variant_enable_doc_mode());
                expected_column.set_is_nullable(true);
            } else {
                expected_column = variant_util::get_column_by_type(
                        make_nullable(std::make_shared<DataTypeInt64>()), column->name(),
                        {.unique_id = -1,
                         .parent_unique_id = 2,
                         .path_info = PathInData(column->name())});
                variant_util::inherit_column_attributes(root, expected_column);
            }
            EXPECT_EQ(*column, expected_column) << column->name();
            EXPECT_EQ(column->parent_unique_id(), 2);
            EXPECT_EQ(column->is_nullable(), column->type() != FieldType::OLAP_FIELD_TYPE_MAP);
            EXPECT_EQ(column->path_info_ptr()->get_path(), column->name());
            EXPECT_EQ(column->path_info_ptr()->get_is_typed(),
                      column->name() == "v.typed_i" && mode != 2 && !to_sparse);
        }
    }
}

INSTANTIATE_TEST_SUITE_P(Policies, VariantMasterPlanTest, testing::Values(0, 1, 2, 3, 4));

TEST_F(IndexStorageVariantCompactionSchemaTest, VariantTemplatesComparison) {
    IndexTabletOptions options;
    VariantColumnSpec variant;
    variant.predefined_paths = {
            {.path = "a", .type = FieldType::OLAP_FIELD_TYPE_INT, .array_item_type = {}}};
    options.variant_columns = {variant};
    ASSERT_TRUE(create_tablet(options).ok());
    IndexRowsetSpec spec;
    spec.batches.push_back(IndexBatch::single_variant({R"({"a":5})"}, 0));
    auto input = write_rowset(spec);
    ASSERT_TRUE(input.has_value()) << input.error();
    const std::vector<RowsetSharedPtr> inputs {input.value()};
    auto changed = [&](const IndexTabletOptions& target_options) {
        return variant_util::VariantCompactionUtil::variant_templates_changed(
                inputs, *build_tablet_schema(target_options));
    };
    EXPECT_FALSE(changed(options));
    options.variant_columns[0].predefined_paths.push_back({.path = "b", .array_item_type = {}});
    EXPECT_TRUE(changed(options));
    options.variant_columns[0] = variant;
    options.variant_columns[0].max_subcolumns_count++;
    EXPECT_FALSE(changed(options));
    options.variant_columns[0] = variant;
    options.variant_columns[0].predefined_paths[0].pattern_type = PatternTypePB::MATCH_NAME_GLOB;
    EXPECT_TRUE(changed(options));
    options.variant_columns[0] = variant;
    options.variant_columns[0].predefined_paths[0].nullable = false;
    EXPECT_TRUE(changed(options));
    options.variant_columns[0] = variant;
    options.variant_columns[0].predefined_paths[0].type = FieldType::OLAP_FIELD_TYPE_BIGINT;
    EXPECT_TRUE(changed(options));
    options.variant_columns.clear();
    options.text_columns = {{.unique_id = 2, .name = "v"}};
    EXPECT_FALSE(changed(options));
    EXPECT_FALSE(variant_util::VariantCompactionUtil::variant_templates_changed(
            {}, *build_tablet_schema(options)));
}

TEST_F(IndexStorageVariantCompactionSchemaTest, NonVariantTemplatesAreUnchanged) {
    IndexTabletOptions options;
    options.text_columns = {{.unique_id = 2, .name = "text"}};
    ASSERT_TRUE(create_tablet(options).ok());
    IndexRowsetSpec spec;
    IndexBatch batch;
    batch.keys = {0};
    batch.text_values_by_column = {{"value"}};
    spec.batches.push_back(std::move(batch));
    auto input = write_rowset(spec);
    ASSERT_TRUE(input.has_value()) << input.error();
    EXPECT_FALSE(variant_util::VariantCompactionUtil::variant_templates_changed({input.value()},
                                                                                *tablet_schema()));
}

class VariantAlterCompactionTest : public IndexStorageTestFixture,
                                   public testing::WithParamInterface<bool> {};

// Keep both schema transitions and their physical/value assertions in one lifecycle test.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_P(VariantAlterCompactionTest, CastOldRowsOnExtendedCompaction) {
    const bool old_vertical = config::enable_vertical_compact_variant_subcolumns;
    struct ConfigGuard {
        bool value;
        ~ConfigGuard() { config::enable_vertical_compact_variant_subcolumns = value; }
    } guard {old_vertical};
    config::enable_vertical_compact_variant_subcolumns = GetParam();
    IndexTabletOptions options;
    VariantColumnSpec variant;
    variant.max_subcolumns_count = 2;
    options.variant_columns = {variant};
    ASSERT_TRUE(create_tablet(options).ok());
    IndexRowsetSpec first;
    first.version = 0;
    first.batches.push_back(IndexBatch::single_variant({R"({"a":5,"b":"keep0"})"}, 0));
    IndexRowsetSpec second;
    second.version = 1;
    second.batches.push_back(IndexBatch::single_variant({R"({"a":"x","b":"keep1"})"}, 1));
    auto inputs = write_rowsets({first, second});
    ASSERT_TRUE(inputs.has_value()) << inputs.error();
    options.variant_columns[0].max_subcolumns_count = 1;
    options.variant_columns[0].predefined_paths = {
            {.path = "a", .type = FieldType::OLAP_FIELD_TYPE_INT, .array_item_type = {}}};
    _tablet_schema = build_tablet_schema(options);
    _tablet_schema->set_schema_version(inputs->front()->tablet_schema()->schema_version() + 1);
    IndexRowsetSpec third;
    third.version = 2;
    third.batches.push_back(IndexBatch::single_variant({R"({"a":9,"b":"keep2"})"}, 2));
    auto new_rows = write_rowset(third);
    ASSERT_TRUE(new_rows.has_value()) << new_rows.error();
    inputs->push_back(new_rows.value());
    CumulativeCompaction compaction(*storage_engine(), tablet());
    compaction._input_rowsets = inputs.value();
    ASSERT_TRUE(compaction.build_basic_info().ok());
    EXPECT_EQ(compaction._cur_variant_compaction_paths != nullptr, GetParam());
    if (GetParam()) {
        const auto& layout = compaction._cur_variant_compaction_paths->at(2);
        EXPECT_TRUE(layout.typed_path_set.contains("a"));
        EXPECT_TRUE(layout.sub_path_set.contains(StringRef("a")));
        EXPECT_FALSE(layout.sparse_path_set.contains(StringRef("a")));
    }
    auto output = compact_rowsets(IndexCompactionKind::CUMULATIVE, inputs.value());
    ASSERT_TRUE(output.has_value()) << output.error();
    EXPECT_EQ(output.value()->tablet_schema()->column_by_uid(2), _tablet_schema->column_by_uid(2));
    EXPECT_EQ(output.value()->tablet_schema()->schema_version(), _tablet_schema->schema_version());
    EXPECT_FALSE(variant_util::VariantCompactionUtil::variant_templates_changed({output.value()},
                                                                                *_tablet_schema));
    EXPECT_EQ(schema_has_extracted_variant_path(*output.value()->tablet_schema(), 2, "a"),
              GetParam());

    std::unordered_map<int32_t, variant_util::VariantExtendedInfo> physical;
    const auto physical_status =
            variant_util::VariantCompactionUtil::aggregate_variant_extended_info(output.value(),
                                                                                 &physical);
    ASSERT_TRUE(physical_status.ok()) << physical_status;
    ASSERT_TRUE(physical.contains(2));
    ASSERT_TRUE(physical.at(2).typed_paths.contains("a"));
    ASSERT_TRUE(physical.at(2).path_to_data_types.contains(PathInData("a", true)));
    const auto& types = physical.at(2).path_to_data_types.at(PathInData("a", true));
    ASSERT_FALSE(types.empty());
    for (const auto& type : types) {
        EXPECT_TRUE(remove_nullable(type)->equals(DataTypeInt32()));
    }

    // A fresh load under the target policy is the oracle for cast failures and whole-root reads.
    IndexRowsetSpec reference;
    reference.version = 10;
    reference.add_to_tablet = false;
    reference.batches.push_back(IndexBatch::single_variant(
            {R"({"a":5,"b":"keep0"})", R"({"a":"x","b":"keep1"})", R"({"a":9,"b":"keep2"})"}, 0));
    auto fresh = write_rowset(reference);
    ASSERT_TRUE(fresh.has_value()) << fresh.error();
    IndexReadOptions read_options;
    read_options.collect_variant_values = true;
    read_options.need_ordered_result = true;
    auto actual = read_rowsets({output.value()}, read_options);
    auto expected = read_rowsets({fresh.value()}, read_options);
    ASSERT_TRUE(actual.has_value()) << actual.error();
    ASSERT_TRUE(expected.has_value()) << expected.error();
    EXPECT_EQ(actual->rows_read, 3);
    EXPECT_EQ(canonical_variant_values(actual->variant_values_by_uid.at(2)),
              canonical_variant_values(expected->variant_values_by_uid.at(2)));

    IndexRowsetSpec following;
    following.version = 3;
    following.batches.push_back(IndexBatch::single_variant({R"({"a":7,"b":"keep3"})"}, 3));
    auto next = write_rowset(following);
    ASSERT_TRUE(next.has_value()) << next.error();
    CumulativeCompaction converged(*storage_engine(), tablet());
    converged._input_rowsets = {output.value(), next.value()};
    ASSERT_TRUE(converged.build_basic_info().ok());
    EXPECT_EQ(converged._cur_variant_compaction_paths != nullptr, GetParam());
    auto next_output =
            compact_rowsets(IndexCompactionKind::CUMULATIVE, {output.value(), next.value()});
    ASSERT_TRUE(next_output.has_value()) << next_output.error();
    EXPECT_EQ(next_output.value()->num_rows(), 4);
}

INSTANTIATE_TEST_SUITE_P(VerticalModes, VariantAlterCompactionTest, testing::Bool());

// Linking must preserve the source policy because it does not rewrite the source segments.
// Keep schema, physical layout and value assertions in the same ordered/normal lifecycle.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_F(IndexStorageVariantCompactionSchemaTest, OrderedCompactionPreservesOldPolicyThenConverges) {
    const bool old_enabled = config::enable_ordered_data_compaction;
    const auto old_min_size = config::ordered_data_compaction_min_segment_size;
    struct ConfigGuard {
        bool enabled;
        decltype(config::ordered_data_compaction_min_segment_size) min_size;
        ~ConfigGuard() {
            config::enable_ordered_data_compaction = enabled;
            config::ordered_data_compaction_min_segment_size = min_size;
        }
    } guard {.enabled = old_enabled, .min_size = old_min_size};
    config::enable_ordered_data_compaction = true;
    config::ordered_data_compaction_min_segment_size = 0;
    IndexTabletOptions options;
    options.variant_columns = {VariantColumnSpec {}};
    options.variant_columns[0].max_subcolumns_count = 3;
    ASSERT_TRUE(create_tablet(options).ok());
    IndexRowsetSpec first;
    first.version = 0;
    first.batches.push_back(IndexBatch::single_variant({R"({"p":"001","a":1})"}, 0));
    IndexRowsetSpec second;
    second.version = 1;
    second.batches.push_back(IndexBatch::single_variant({R"({"p":"x7","b":2})"}, 1));
    auto inputs = write_rowsets({first, second});
    ASSERT_TRUE(inputs.has_value()) << inputs.error();
    const auto old_schema = inputs->front()->tablet_schema();
    options.variant_columns[0].max_subcolumns_count = 1;
    options.variant_columns[0].predefined_paths = {{.path = "p",
                                                    .type = FieldType::OLAP_FIELD_TYPE_INT,
                                                    .pattern_type = PatternTypePB::MATCH_NAME,
                                                    .array_item_type = {}}};
    _tablet_schema = build_tablet_schema(options);
    _tablet_schema->set_schema_version(old_schema->schema_version() + 1);
    tablet()->update_max_version_schema(_tablet_schema);
    CumulativeCompaction ordered(*storage_engine(), tablet());
    ordered._input_rowsets = inputs.value();
    ASSERT_TRUE(ordered.handle_ordered_data_compaction());
    const auto linked = ordered._output_rowset;
    ASSERT_NE(linked, nullptr);
    EXPECT_EQ(linked->num_segments(), 2);
    EXPECT_EQ(linked->tablet_schema()->schema_version(), old_schema->schema_version());
    EXPECT_EQ(*linked->tablet_schema(), *old_schema);
    EXPECT_TRUE(variant_util::VariantCompactionUtil::variant_templates_changed({linked},
                                                                               *_tablet_schema));
    // Read the linked segments under their original schema, independently of the new template.
    const auto target = _tablet_schema;
    _tablet_schema = old_schema;
    IndexReadOptions read;
    read.collect_variant_values = true;
    read.need_ordered_result = true;
    auto linked_values = read_rowsets({linked}, read);
    ASSERT_TRUE(linked_values.has_value()) << linked_values.error();
    EXPECT_EQ(canonical_variant_values(linked_values->variant_values_by_uid.at(2)),
              canonical_variant_values({R"({"a":1,"p":"001"})", R"({"b":2,"p":"x7"})"}));
    _tablet_schema = target;
    // handle_ordered_data_compaction builds the linked rowset; publish it before the next round.
    {
        std::lock_guard lock(tablet()->get_header_lock());
        std::vector<RowsetSharedPtr> added {linked};
        ASSERT_TRUE(tablet()->modify_rowsets(added, inputs.value(), true).ok());
    }
    IndexRowsetSpec next;
    next.version = 2;
    next.batches.push_back(IndexBatch::single_variant({R"({"p":7,"c":3})"}, 2));
    auto following = write_rowset(next);
    ASSERT_TRUE(following.has_value()) << following.error();
    auto converged = compact_rowsets(IndexCompactionKind::FULL, {linked, following.value()});
    ASSERT_TRUE(converged.has_value()) << converged.error();
    EXPECT_FALSE(variant_util::VariantCompactionUtil::variant_templates_changed({converged.value()},
                                                                                *target));
    IndexRowsetSpec control;
    control.version = 10;
    control.add_to_tablet = false;
    control.batches.push_back(IndexBatch::single_variant(
            {R"({"p":"001","a":1})", R"({"p":"x7","b":2})", R"({"p":7,"c":3})"}, 0));
    auto fresh = write_rowset(control);
    ASSERT_TRUE(fresh.has_value()) << fresh.error();
    auto actual = read_rowsets({converged.value()}, read);
    auto expected = read_rowsets({fresh.value()}, read);
    ASSERT_TRUE(actual.has_value()) << actual.error();
    ASSERT_TRUE(expected.has_value()) << expected.error();
    EXPECT_EQ(actual->rows_read, 3);
    EXPECT_EQ(canonical_variant_values(actual->variant_values_by_uid.at(2)),
              canonical_variant_values(expected->variant_values_by_uid.at(2)));
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_F(IndexStorageVariantCompactionSchemaTest, OldOnlyCompactionRetainsInputSchema) {
    IndexTabletOptions options;
    options.variant_columns = {VariantColumnSpec {}};
    options.variant_columns[0].max_subcolumns_count = 3;
    ASSERT_TRUE(create_tablet(options).ok());
    IndexRowsetSpec a;
    a.version = 0;
    a.batches.push_back(IndexBatch::single_variant({R"({"p":"001"})"}, 0));
    IndexRowsetSpec b;
    b.version = 1;
    b.batches.push_back(IndexBatch::single_variant({R"({"p":"x7"})"}, 1));
    auto inputs = write_rowsets({a, b});
    ASSERT_TRUE(inputs.has_value()) << inputs.error();
    const auto old = inputs->front()->tablet_schema();
    options.variant_columns[0].max_subcolumns_count = 1;
    options.variant_columns[0].predefined_paths = {
            {.path = "p", .type = FieldType::OLAP_FIELD_TYPE_INT, .array_item_type = {}}};
    _tablet_schema = build_tablet_schema(options);
    _tablet_schema->set_schema_version(old->schema_version() + 1);
    tablet()->update_max_version_schema(_tablet_schema);
    auto output = compact_rowsets(IndexCompactionKind::FULL, inputs.value());
    ASSERT_TRUE(output.has_value()) << output.error();
    EXPECT_EQ(output.value()->tablet_schema()->schema_version(), old->schema_version());
    EXPECT_EQ(output.value()->tablet_schema()->column_by_uid(2), old->column_by_uid(2));
    EXPECT_TRUE(output.value()->tablet_schema()->column_by_uid(2).get_sub_columns().empty());
    _tablet_schema = old;
    IndexReadOptions read;
    read.collect_variant_values = true;
    read.need_ordered_result = true;
    auto actual = read_rowsets({output.value()}, read);
    ASSERT_TRUE(actual.has_value()) << actual.error();
    EXPECT_EQ(canonical_variant_values(actual->variant_values_by_uid.at(2)),
              canonical_variant_values({R"({"p":"001"})", R"({"p":"x7"})"}));
}

class VariantBudgetTransitionTest : public IndexStorageTestFixture,
                                    public testing::WithParamInterface<int> {};

// Keep the sparse-input preconditions and the independent final-policy oracle together.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_P(VariantBudgetTransitionTest, CountOnlyChangePreservesPathsOutsideSparseStatistics) {
    const int old_count = GetParam();
    const std::map<int, int> transitions {{3, 0}, {0, 3}, {1, 5}, {5, 2}};
    const int new_count = transitions.at(old_count);
    IndexTabletOptions options;
    options.variant_columns = {VariantColumnSpec {}};
    options.variant_columns[0].max_subcolumns_count = old_count;
    options.variant_columns[0].max_sparse_column_statistics_size = 1;
    ASSERT_TRUE(create_tablet(options).ok());
    std::string json = R"({"a":1,"b":2,"c":3,"d":4,"e":5)";
    for (int i = 0; i < 30; ++i) {
        json += ",\"rare" + std::to_string(i) + "\":" + std::to_string(i);
    }
    json += R"(,"z_tail":"keep"})";
    IndexRowsetSpec old;
    old.version = 0;
    const std::string hot = R"({"a":1,"b":2,"c":3,"d":4,"e":5})";
    old.batches.push_back(IndexBatch::single_variant({json, hot, hot, hot, hot}, 0));
    auto input = write_rowset(old);
    ASSERT_TRUE(input.has_value()) << input.error();
    auto probe = probe_rowset(input.value());
    ASSERT_TRUE(probe.has_value()) << probe.error();
    if (old_count > 0) {
        // 34 observed paths exceed both the materialization budget and the one-entry statistics.
        EXPECT_FALSE(probe->contains_relative_path("z_tail"));
        EXPECT_FALSE(has_sparse_path_stat(probe.value(), "z_tail"));
        EXPECT_GT(sparse_stat_entry_count(probe.value()), 0);
        EXPECT_LT(sparse_stat_entry_count(probe.value()), 30);
    } else {
        EXPECT_TRUE(probe->contains_relative_path("z_tail"));
    }
    options.variant_columns[0].max_subcolumns_count = new_count;
    _tablet_schema = build_tablet_schema(options);
    _tablet_schema->set_schema_version(input.value()->tablet_schema()->schema_version() + 1);
    tablet()->update_max_version_schema(_tablet_schema);
    // The newest input supplies the compaction target schema, as after an ALTER and new load.
    IndexRowsetSpec next;
    next.version = 1;
    // The new row does not reveal any of the old rare paths or z_tail's physical type.
    next.batches.push_back(IndexBatch::single_variant({hot}, 5));
    auto following = write_rowset(next);
    ASSERT_TRUE(following.has_value()) << following.error();
    auto planned = std::make_shared<TabletSchema>(*_tablet_schema);
    VariantCompactionPathsMap paths;
    ASSERT_TRUE(variant_util::VariantCompactionUtil::get_extended_compaction_schema(
                        {input.value(), following.value()}, planned, paths)
                        .ok());
    EXPECT_TRUE(paths.contains(2));
    auto output = compact_rowsets(IndexCompactionKind::FULL, {input.value(), following.value()});
    ASSERT_TRUE(output.has_value()) << output.error();
    const auto stats_status = variant_util::VariantCompactionUtil::check_path_stats(
            {input.value(), following.value()}, output.value(), tablet());
    EXPECT_TRUE(stats_status.ok()) << stats_status;
    // A one-row output has deliberately wrong counts. This must fail even though a budget
    // changed; merely checking OK on the real output would not detect an early-return regression.
    EXPECT_FALSE(variant_util::VariantCompactionUtil::check_path_stats(
                         {input.value(), following.value()}, following.value(), tablet())
                         .ok());
    EXPECT_FALSE(variant_util::VariantCompactionUtil::variant_templates_changed({output.value()},
                                                                                *_tablet_schema));
    IndexRowsetSpec control;
    control.version = 10;
    control.add_to_tablet = false;
    control.batches.push_back(IndexBatch::single_variant({json, hot, hot, hot, hot, hot}, 0));
    auto fresh = write_rowset(control);
    ASSERT_TRUE(fresh.has_value()) << fresh.error();
    IndexReadOptions read;
    read.collect_variant_values = true;
    read.need_ordered_result = true;
    auto actual = read_rowsets({output.value()}, read);
    auto expected = read_rowsets({fresh.value()}, read);
    ASSERT_TRUE(actual.has_value()) << actual.error();
    ASSERT_TRUE(expected.has_value()) << expected.error();
    EXPECT_EQ(actual->rows_read, 6);
    EXPECT_EQ(canonical_variant_values(actual->variant_values_by_uid.at(2)),
              canonical_variant_values(expected->variant_values_by_uid.at(2)));
    ASSERT_EQ(actual->variant_values_by_uid.at(2).size(), 6);
    ASSERT_TRUE(actual->variant_values_by_uid.at(2)[0].has_value());
    EXPECT_NE(actual->variant_values_by_uid.at(2)[0]->find("keep"), std::string::npos);
}

INSTANTIATE_TEST_SUITE_P(Counts, VariantBudgetTransitionTest, testing::Values(3, 0, 1, 5));

// Independent old/new rowsets at each seeded budget step keep input schemas non-extended,
// so both the positive and deliberately-invalid count checks exercise statistics aggregation.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_F(IndexStorageVariantCompactionSchemaTest, SeededBudgetWalkBuildsExtendedPlansAndChecksStats) {
    IndexTabletOptions options;
    options.variant_columns = {VariantColumnSpec {}};
    ASSERT_TRUE(create_tablet(options).ok());
    std::mt19937 random(67714);
    const std::vector<int> budgets {0, 1, 2, 5};
    int old_budget = 3;
    int schema_version = _tablet_schema->schema_version();
    const std::string json = R"({"a":1,"b":2,"c":3,"d":4,"e":5,"keep":"old"})";
    for (int step = 0; step < 8; ++step) {
        SCOPED_TRACE(step);
        const int new_budget = budgets[random() % budgets.size()];
        options.variant_columns[0].max_subcolumns_count = old_budget;
        _tablet_schema = build_tablet_schema(options);
        _tablet_schema->set_schema_version(++schema_version);
        tablet()->update_max_version_schema(_tablet_schema);
        IndexRowsetSpec old;
        old.version = 2 * step;
        old.batches.push_back(IndexBatch::single_variant({json}, 2 * step));
        auto first = write_rowset(old);
        ASSERT_TRUE(first.has_value()) << first.error();
        options.variant_columns[0].max_subcolumns_count = new_budget;
        _tablet_schema = build_tablet_schema(options);
        _tablet_schema->set_schema_version(++schema_version);
        tablet()->update_max_version_schema(_tablet_schema);
        IndexRowsetSpec next;
        next.version = 2 * step + 1;
        next.batches.push_back(IndexBatch::single_variant({json}, 2 * step + 1));
        auto second = write_rowset(next);
        ASSERT_TRUE(second.has_value()) << second.error();
        const std::vector<RowsetSharedPtr> inputs {first.value(), second.value()};
        CumulativeCompaction compaction(*storage_engine(), tablet());
        compaction._input_rowsets = inputs;
        ASSERT_TRUE(compaction.build_basic_info().ok());
        ASSERT_NE(compaction._cur_variant_compaction_paths, nullptr);
        EXPECT_TRUE(compaction._cur_variant_compaction_paths->contains(2));
        EXPECT_TRUE(std::any_of(compaction._cur_tablet_schema->columns().begin(),
                                compaction._cur_tablet_schema->columns().end(),
                                [](const auto& c) { return c->is_extracted_column(); }));
        auto output = compact_rowsets(IndexCompactionKind::FULL, inputs);
        ASSERT_TRUE(output.has_value()) << output.error();
        EXPECT_TRUE(variant_util::VariantCompactionUtil::check_path_stats(inputs, output.value(),
                                                                          tablet())
                            .ok());
        EXPECT_FALSE(variant_util::VariantCompactionUtil::check_path_stats(inputs, second.value(),
                                                                           tablet())
                             .ok());
        IndexReadOptions read;
        read.collect_variant_values = true;
        read.need_ordered_result = true;
        auto actual = read_rowsets({output.value()}, read);
        ASSERT_TRUE(actual.has_value()) << actual.error();
        EXPECT_EQ(actual->rows_read, 2);
        IndexRowsetSpec control;
        control.version = 100 + step;
        control.add_to_tablet = false;
        control.batches.push_back(IndexBatch::single_variant({json, json}, 2 * step));
        auto fresh = write_rowset(control);
        ASSERT_TRUE(fresh.has_value()) << fresh.error();
        auto expected = read_rowsets({fresh.value()}, read);
        ASSERT_TRUE(expected.has_value()) << expected.error();
        EXPECT_EQ(canonical_variant_values(actual->variant_values_by_uid.at(2)),
                  canonical_variant_values(expected->variant_values_by_uid.at(2)));
        old_budget = new_budget;
    }
}

// An old sparse-only path must get a destination before sparse merge removes its copy.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_F(IndexStorageVariantCompactionSchemaTest, SparseOnlyOldPathSurvivesUnlimitedCompaction) {
    IndexTabletOptions options;
    options.variant_columns = {VariantColumnSpec {}};
    options.variant_columns[0].max_subcolumns_count = 1;
    ASSERT_TRUE(create_tablet(options).ok());
    IndexRowsetSpec old;
    old.version = 0;
    old.batches.push_back(IndexBatch::single_variant({R"({"row":0,"lost":"old"})"}, 0));
    auto input = write_rowset(old);
    ASSERT_TRUE(input.has_value()) << input.error();
    auto probe = probe_rowset(input.value());
    ASSERT_TRUE(probe.has_value()) << probe.error();
    ASSERT_TRUE(has_sparse_path_stat(probe.value(), "lost"));
    ASSERT_FALSE(has_variant_layout(probe.value(), 2, "lost"));
    options.variant_columns[0].max_subcolumns_count = 0;
    _tablet_schema = build_tablet_schema(options);
    _tablet_schema->set_schema_version(input.value()->tablet_schema()->schema_version() + 1);
    tablet()->update_max_version_schema(_tablet_schema);
    IndexRowsetSpec next;
    next.version = 1;
    next.batches.push_back(IndexBatch::single_variant({R"({"row":1,"fresh":7})"}, 1));
    auto following = write_rowset(next);
    ASSERT_TRUE(following.has_value()) << following.error();
    const std::vector<RowsetSharedPtr> inputs {input.value(), following.value()};
    auto schema = std::make_shared<TabletSchema>(*_tablet_schema);
    VariantCompactionPathsMap paths;
    ASSERT_TRUE(variant_util::VariantCompactionUtil::get_extended_compaction_schema(inputs, schema,
                                                                                    paths)
                        .ok());
    ASSERT_TRUE(paths.contains(2));
    EXPECT_TRUE(schema_has_extracted_variant_path(*schema, 2, "lost"));
    auto output = compact_rowsets(IndexCompactionKind::FULL, inputs);
    ASSERT_TRUE(output.has_value()) << output.error();
    const auto stats =
            variant_util::VariantCompactionUtil::check_path_stats(inputs, output.value(), tablet());
    EXPECT_TRUE(stats.ok()) << stats;
    IndexReadOptions read;
    read.collect_variant_values = true;
    read.need_ordered_result = true;
    auto actual = read_rowsets({output.value()}, read);
    ASSERT_TRUE(actual.has_value()) << actual.error();
    const std::vector<std::string> expected {R"({"row":0,"lost":"old"})", R"({"row":1,"fresh":7})"};
    const auto& values = actual->variant_values_by_uid.at(2);
    ASSERT_EQ(values.size(), expected.size());
    for (size_t i = 0; i < values.size(); ++i) {
        ASSERT_TRUE(values[i].has_value());
        rapidjson::Document actual_json;
        rapidjson::Document expected_json;
        actual_json.Parse(values[i]->c_str());
        expected_json.Parse(expected[i].c_str());
        ASSERT_FALSE(actual_json.HasParseError());
        ASSERT_FALSE(expected_json.HasParseError());
        EXPECT_TRUE(actual_json == expected_json) << *values[i];
    }
}

// The per-root map overrides the table-wide flag only for compaction/checksum reads.
TEST(IndexStorageVariantCompactionUtilTest, FlatLeafReadsFollowThisRootLayout) {
    segment_v2::VariantColumnReader reader;
    reader._root_unique_id = 2;
    OlapReaderStatistics stats;
    StorageReadOptions options(stats);
    EXPECT_FALSE(reader._need_read_flat_leaves(nullptr));
    options.tablet_has_extracted_variant_columns = true;
    options.io_ctx.reader_type = ReaderType::READER_QUERY;
    EXPECT_FALSE(reader._need_read_flat_leaves(&options));
    for (auto type : {ReaderType::READER_CUMULATIVE_COMPACTION, ReaderType::READER_CHECKSUM}) {
        options.io_ctx.reader_type = type;
        options.variant_compaction_paths = nullptr;
        EXPECT_TRUE(reader._need_read_flat_leaves(&options));
        auto paths = std::make_shared<VariantCompactionPathsMap>();
        paths->try_emplace(3); // Only the other root stays extended.
        options.variant_compaction_paths = paths;
        EXPECT_FALSE(reader._need_read_flat_leaves(&options));
        paths->try_emplace(2);
        EXPECT_TRUE(reader._need_read_flat_leaves(&options));
        options.tablet_has_extracted_variant_columns = false;
        EXPECT_TRUE(reader._need_read_flat_leaves(&options));
        options.variant_compaction_paths = nullptr;
        EXPECT_FALSE(reader._need_read_flat_leaves(&options));
        options.tablet_has_extracted_variant_columns = true;
    }
}

class VariantTemplateExtendedTest : public IndexStorageTestFixture,
                                    public testing::WithParamInterface<int> {};

// 0: MATCH_NAME, truncated; 1: new glob, truncated; 2: new glob, complete;
// 3: literal GLOB, truncated; 4: existing glob, truncated.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_P(VariantTemplateExtendedTest, PromoteOrFallbackPerColumnAndPreserveValues) {
    const bool truncated = GetParam() != 2;
    const bool wildcard = GetParam() == 1 || GetParam() == 2 || GetParam() == 4;
    const VariantPathSpec entry {.path = wildcard ? "z*" : "z_tail",
                                 .type = FieldType::OLAP_FIELD_TYPE_INT,
                                 .pattern_type = GetParam() == 0 ? PatternTypePB::MATCH_NAME
                                                                 : PatternTypePB::MATCH_NAME_GLOB,
                                 .array_item_type = {}};
    IndexTabletOptions options;
    VariantColumnSpec first;
    first.max_subcolumns_count = 1;
    first.max_sparse_column_statistics_size = truncated ? 1 : 10000;
    if (GetParam() == 4) {
        first.predefined_paths = {entry};
    }
    VariantColumnSpec second;
    second.unique_id = 3;
    second.name = "w";
    options.variant_columns = {first, second};
    ASSERT_TRUE(create_tablet(options).ok());
    const std::string json = R"({"hot":1,"a":2,"b":3,"z_tail":"7"})";
    IndexRowsetSpec old;
    old.version = 0;
    IndexBatch batch;
    batch.keys = {0, 1, 2, 3};
    batch.variant_jsons_by_column = {
            {json, R"({"hot":1})", R"({"hot":1})", R"({"hot":1})"},
            {R"({"keep":9})", R"({"keep":8})", R"({"keep":7})", R"({"keep":6})"}};
    old.batches.push_back(batch);
    auto input = write_rowset(old);
    ASSERT_TRUE(input.has_value()) << input.error();
    auto probe = probe_rowset(input.value());
    ASSERT_TRUE(probe.has_value()) << probe.error();
    if (truncated && GetParam() != 4) {
        ASSERT_FALSE(has_sparse_path_stat(probe.value(), "z_tail"));
        ASSERT_FALSE(has_variant_layout(probe.value(), 2, "z_tail"));
    }
    options.variant_columns[0].predefined_paths = {entry};
    _tablet_schema = build_tablet_schema(options);
    _tablet_schema->set_schema_version(input.value()->tablet_schema()->schema_version() + 1);
    tablet()->update_max_version_schema(_tablet_schema);
    IndexRowsetSpec next;
    next.version = 1;
    IndexBatch new_batch;
    new_batch.keys = {4};
    new_batch.variant_jsons_by_column = {{R"({"z_tail":8})"}, {R"({"keep":5})"}};
    next.batches.push_back(new_batch);
    auto following = write_rowset(next);
    ASSERT_TRUE(following.has_value()) << following.error();
    auto schema = std::make_shared<TabletSchema>(*_tablet_schema);
    const std::vector<RowsetSharedPtr> inputs {input.value(), following.value()};
    VariantCompactionPathsMap paths;
    const auto st = variant_util::VariantCompactionUtil::get_extended_compaction_schema(
            inputs, schema, paths);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_TRUE(paths.contains(3));
    EXPECT_TRUE(schema_has_extracted_variant_path(*schema, 3, "keep"));
    if (GetParam() == 1) {
        EXPECT_FALSE(paths.contains(2));
        for (const auto& column : schema->columns()) {
            EXPECT_FALSE(column->is_extracted_column() && column->parent_unique_id() == 2);
        }
    } else {
        ASSERT_TRUE(paths.contains(2));
        EXPECT_TRUE(paths.at(2).typed_path_set.contains("z_tail"));
        EXPECT_FALSE(paths.at(2).sparse_path_set.contains(StringRef("z_tail")));
        EXPECT_EQ(std::count_if(schema->columns().begin(), schema->columns().end(),
                                [](const auto& c) { return c->name() == "v.z_tail"; }),
                  1);
        EXPECT_EQ(schema->column(schema->field_index("v.z_tail")).type(),
                  FieldType::OLAP_FIELD_TYPE_INT);
    }
    auto output = compact_rowsets(IndexCompactionKind::FULL, inputs);
    ASSERT_TRUE(output.has_value()) << output.error();
    IndexRowsetSpec control;
    control.version = 10;
    control.add_to_tablet = false;
    batch.keys.push_back(4);
    batch.variant_jsons_by_column[0].emplace_back(R"({"z_tail":8})");
    batch.variant_jsons_by_column[1].emplace_back(R"({"keep":5})");
    control.batches.push_back(batch);
    auto fresh = write_rowset(control);
    ASSERT_TRUE(fresh.has_value()) << fresh.error();
    IndexReadOptions read;
    read.collect_variant_values = true;
    read.need_ordered_result = true;
    auto actual = read_rowsets({output.value()}, read);
    auto expected = read_rowsets({fresh.value()}, read);
    ASSERT_TRUE(actual.has_value()) << actual.error();
    ASSERT_TRUE(expected.has_value()) << expected.error();
    for (int uid : {2, 3}) {
        EXPECT_EQ(canonical_variant_values(actual->variant_values_by_uid.at(uid)),
                  canonical_variant_values(expected->variant_values_by_uid.at(uid)));
    }
    auto physical = probe_rowset(output.value());
    ASSERT_TRUE(physical.has_value()) << physical.error();
    EXPECT_FALSE(has_sparse_path_stat(physical.value(), "z_tail"));
}

INSTANTIATE_TEST_SUITE_P(Templates, VariantTemplateExtendedTest, testing::Values(0, 1, 2, 3, 4));

// V2 writes arrays as leaves, so construct the legacy nested metadata explicitly. Planning
// must retain its nested column, just as when the template existed at CREATE time.
TEST_F(IndexStorageVariantCompactionSchemaTest, NestedPathsDoNotBecomeTemplateHits) {
    IndexTabletOptions options;
    VariantColumnSpec variant;
    variant.predefined_paths = {{.path = "items.*",
                                 .type = FieldType::OLAP_FIELD_TYPE_INT,
                                 .pattern_type = PatternTypePB::MATCH_NAME_GLOB,
                                 .array_item_type = {}}};
    options.variant_columns = {variant};
    auto schema = build_tablet_schema(options);
    PathInDataBuilder builder;
    const auto path = builder.append("items", false).append("a", true).build();
    ASSERT_TRUE(path.has_nested_part());
    variant_util::VariantExtendedInfo info;
    info.nested_paths.insert(path);
    info.path_to_data_types[path] = {
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeInt32>()))};
    info.path_to_none_null_values[path.get_path()] = 1;
    std::unordered_set<std::string> typed_paths;
    EXPECT_TRUE(variant_util::VariantCompactionUtil::collect_compaction_template_paths(
            *schema, schema->column_by_uid(2), info, typed_paths));
    EXPECT_TRUE(typed_paths.empty());
    VariantCompactionPaths paths;
    const auto st = variant_util::VariantCompactionUtil::get_compaction_nested_columns(
            info.nested_paths, info.path_to_data_types, schema->columns()[1], schema, paths);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_TRUE(schema_has_extracted_variant_path(*schema, 2, path.get_path()));
    EXPECT_TRUE(schema->columns().back()->path_info_ptr()->has_nested_part());
}

TEST(IndexStorageVariantCompactionUtilTest, TypeGateOnlyRejectsConcreteStoredLeafMismatch) {
    IndexTabletOptions options;
    options.variant_columns = {VariantColumnSpec {}};
    auto schema = build_schema_with_variant_path_column(*build_tablet_schema(options), 2, "a",
                                                        FieldType::OLAP_FIELD_TYPE_INT);
    const auto& column = schema->column(schema->field_index("v.a"));
    const auto read_type = make_nullable(std::make_shared<DataTypeInt32>());
    const auto variant_type = make_nullable(std::make_shared<DataTypeVariant>());
    EXPECT_TRUE(variant_util::variant_path_type_matches(*schema, column, nullptr, read_type));
    EXPECT_TRUE(variant_util::variant_path_type_matches(*schema, column, variant_type, read_type));
    EXPECT_TRUE(variant_util::variant_path_type_matches(
            *schema, column, std::make_shared<DataTypeInt32>(), read_type));
    const DataTypes old_leaf_types {std::make_shared<DataTypeInt64>(),
                                    std::make_shared<DataTypeString>(),
                                    std::make_shared<DataTypeJsonb>()};
    for (const auto& stored_type : old_leaf_types) {
        EXPECT_FALSE(
                variant_util::variant_path_type_matches(*schema, column, stored_type, read_type));
        EXPECT_FALSE(variant_util::variant_path_type_matches(
                *schema, column, make_nullable(stored_type), read_type));
        EXPECT_TRUE(variant_util::variant_path_type_matches(*schema, column, stored_type,
                                                            variant_type));
    }
    // DOC/NG retain their pre-ALTER read/index type contracts, including flattened NG types.
    for (bool doc_mode : {true, false}) {
        options.variant_columns[0].enable_doc_mode = doc_mode;
        options.variant_columns[0].enable_nested_group = !doc_mode;
        auto exempt_schema = build_tablet_schema(options);
        EXPECT_TRUE(variant_util::variant_path_type_matches(
                *exempt_schema, column, std::make_shared<DataTypeString>(), read_type));
    }
}

class VariantTemplateShapeTest : public IndexStorageTestFixture,
                                 public testing::WithParamInterface<int> {};

// 0: wide complete glob, 1: mixed arrays, 2: dotted name, 3: glob metacharacters as a literal,
// 4: empty name. Every run mixes old sparse/dynamic data with a target-policy typed input.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_P(VariantTemplateShapeTest, AppendedTemplateHasOnePhysicalDestinationAndControlValues) {
    const int mode = GetParam();
    const std::vector<std::string> shape_paths {"z0", "a", "a.b", "x*[?%", ""};
    const auto& path = shape_paths.at(mode);
    const bool array = mode == 1;
    std::vector<std::string> old_rows;
    std::string extra;
    std::string next;
    std::vector<std::string> matched {path};
    if (mode == 0) {
        old_rows = {R"({"z0":"7","z1":"8","z2":"bad","hot":1})", R"({"z0":"9","hot":1})",
                    R"({"hot":1})"};
        extra = R"({"z0":10,"z2":10})";
        next = R"({"z0":11,"z3":12})";
        matched = {"z0", "z1", "z2", "z3"};
    } else if (array) {
        old_rows = {R"({"a":[1,"2","bad",null],"hot":1})", R"({"hot":1})"};
        extra = R"({"a":[3,"4"]})";
        next = R"({"a":[5]})";
    } else {
        old_rows = {R"({")" + path + R"(":"7","hot":1})", R"({"hot":1})"};
        extra = R"({")" + path + R"(":"8"})";
        next = "{\"" + path + "\":9}";
    }
    IndexTabletOptions options;
    options.variant_columns = {VariantColumnSpec {}};
    options.variant_columns[0].max_subcolumns_count = 1;
    ASSERT_TRUE(create_tablet(options).ok());
    IndexRowsetSpec first;
    first.version = 0;
    first.batches.push_back(IndexBatch::single_variant(old_rows, 0));
    IndexRowsetSpec second;
    second.version = 1;
    second.batches.push_back(IndexBatch::single_variant({extra}, old_rows.size()));
    auto inputs = write_rowsets({first, second});
    ASSERT_TRUE(inputs.has_value()) << inputs.error();
    options.variant_columns[0].predefined_paths = {
            {.path = mode == 0 ? "z*" : path,
             .type = array ? FieldType::OLAP_FIELD_TYPE_ARRAY : FieldType::OLAP_FIELD_TYPE_INT,
             .pattern_type = mode == 0 ? PatternTypePB::MATCH_NAME_GLOB : PatternTypePB::MATCH_NAME,
             .array_item_type = array ? std::optional<FieldType>(FieldType::OLAP_FIELD_TYPE_INT)
                                      : std::nullopt}};
    _tablet_schema = build_tablet_schema(options);
    _tablet_schema->set_schema_version(inputs->front()->tablet_schema()->schema_version() + 1);
    tablet()->update_max_version_schema(_tablet_schema);
    IndexRowsetSpec third;
    third.version = 2;
    third.batches.push_back(IndexBatch::single_variant({next}, old_rows.size() + 1));
    auto typed = write_rowset(third);
    ASSERT_TRUE(typed.has_value()) << typed.error();
    inputs->push_back(typed.value());
    auto planned = std::make_shared<TabletSchema>(*_tablet_schema);
    VariantCompactionPathsMap paths;
    ASSERT_TRUE(variant_util::VariantCompactionUtil::get_extended_compaction_schema(inputs.value(),
                                                                                    planned, paths)
                        .ok());
    ASSERT_TRUE(paths.contains(2)); // Complete sparse statistics must never cause glob fallback.
    for (const auto& hit : matched) {
        EXPECT_TRUE(paths.at(2).typed_path_set.contains(hit));
        EXPECT_TRUE(paths.at(2).sub_path_set.contains(hit));
        EXPECT_FALSE(paths.at(2).sparse_path_set.contains(hit));
    }
    auto output = compact_rowsets(IndexCompactionKind::FULL, inputs.value());
    ASSERT_TRUE(output.has_value()) << output.error();
    auto probe = probe_rowset(output.value());
    ASSERT_TRUE(probe.has_value()) << probe.error();
    std::unordered_map<int32_t, variant_util::VariantExtendedInfo> physical;
    ASSERT_TRUE(variant_util::VariantCompactionUtil::aggregate_variant_extended_info(output.value(),
                                                                                     &physical)
                        .ok());
    for (const auto& hit : matched) {
        EXPECT_TRUE(physical.at(2).typed_paths.contains(hit));
        EXPECT_FALSE(has_sparse_path_stat(probe.value(), hit));
        // Footer probing recursively visits ARRAY children: one ARRAY destination has both
        // its parent and its single element carrying this path. Scalars have one footer entry.
        for (const auto& segment : probe->segments) {
            EXPECT_EQ(std::count_if(segment.variant_columns.begin(), segment.variant_columns.end(),
                                    [&](const auto& c) {
                                        return c.parent_unique_id == 2 && !c.is_sparse_column &&
                                               c.relative_path == hit;
                                    }),
                      array ? 2 : 1);
        }
        bool found_type = false;
        for (const auto& [stored_path, types] : physical.at(2).path_to_data_types) {
            if (stored_path.get_is_typed() && stored_path.get_path() == hit) {
                found_type = true;
                for (const auto& type : types) {
                    DataTypePtr expected_type = std::make_shared<DataTypeInt32>();
                    if (array) {
                        expected_type = std::make_shared<DataTypeArray>(
                                make_nullable(std::make_shared<DataTypeInt32>()));
                    }
                    EXPECT_TRUE(remove_nullable(type)->equals(*expected_type)) << type->get_name();
                }
            }
        }
        EXPECT_TRUE(found_type);
    }
    old_rows.push_back(extra);
    old_rows.push_back(next);
    IndexRowsetSpec control;
    control.version = 10;
    control.add_to_tablet = false;
    control.batches.push_back(IndexBatch::single_variant(old_rows, 0));
    auto fresh = write_rowset(control);
    ASSERT_TRUE(fresh.has_value()) << fresh.error();
    IndexReadOptions read;
    read.collect_variant_values = true;
    read.need_ordered_result = true;
    auto actual = read_rowsets({output.value()}, read);
    auto expected = read_rowsets({fresh.value()}, read);
    ASSERT_TRUE(actual.has_value()) << actual.error();
    ASSERT_TRUE(expected.has_value()) << expected.error();
    EXPECT_EQ(canonical_variant_values(actual->variant_values_by_uid.at(2)),
              canonical_variant_values(expected->variant_values_by_uid.at(2)));
}

INSTANTIATE_TEST_SUITE_P(Shapes, VariantTemplateShapeTest, testing::Values(0, 1, 2, 3, 4));

class SegmentPredicateTemplateAlterTest : public IndexStorageTestFixture,
                                          public testing::WithParamInterface<bool> {};

// Exercise both predicates against the same old segments, including cast failures.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_P(SegmentPredicateTemplateAlterTest, CastAndFilterWithoutOldStorageIndexes) {
    IndexTabletOptions options;
    options.variant_columns = {VariantColumnSpec {}};
    options.variant_columns[0].is_bf_column = true;
    if (GetParam()) {
        options.variant_columns[0].predefined_paths = {
                {.path = "a", .type = FieldType::OLAP_FIELD_TYPE_INT, .array_item_type = {}}};
    } else {
        options.inverted_indexes = {IndexSpec::column_index(220900, "idx_old_v", 2)};
    }
    ASSERT_TRUE(create_tablet(options).ok());
    IndexRowsetSpec numeric;
    numeric.version = 0;
    numeric.batches.push_back(
            IndexBatch::single_variant({R"({"a":5})", R"({"a":2})", R"({"a":8})"}, 0));
    IndexRowsetSpec strings;
    strings.version = 1;
    strings.batches.push_back(
            IndexBatch::single_variant({R"({"a":"5"})", R"({"a":"x"})", R"({"a":"2"})"}, 3));
    auto inputs = write_rowsets({numeric, strings});
    ASSERT_TRUE(inputs.has_value()) << inputs.error();
    options.variant_columns[0].predefined_paths = {
            {.path = "a", .type = FieldType::OLAP_FIELD_TYPE_INT, .array_item_type = {}}};
    options.inverted_indexes.push_back(IndexSpec::field_pattern_index(220901, "idx_a", 2, "a"));
    auto target = build_schema_with_variant_path_column(*build_tablet_schema(options), 2, "a",
                                                        FieldType::OLAP_FIELD_TYPE_INT);
    auto readable = inject_reader_schema_for_rowsets(inputs.value(), target);
    ASSERT_TRUE(readable.has_value()) << readable.error();
    const int32_t path_id = column_id_by_path("v.a");
    ASSERT_GE(path_id, 0);
    const auto int_type = std::make_shared<DataTypeInt32>();
    for (bool equality : {true, false}) {
        const int value = equality ? 5 : 3;
        TFunctionName name;
        name.__set_function_name(equality ? "eq" : "gt");
        TFunction function;
        function.__set_name(name);
        function.__set_binary_type(TFunctionBinaryType::BUILTIN);
        function.__set_arg_types({int_type->to_thrift(), int_type->to_thrift()});
        function.__set_ret_type(std::make_shared<DataTypeUInt8>()->to_thrift());
        function.__set_has_var_args(false);
        TExprNode node;
        node.__set_node_type(TExprNodeType::BINARY_PRED);
        node.__set_opcode(equality ? TExprOpcode::EQ : TExprOpcode::GT);
        node.__set_type(std::make_shared<DataTypeUInt8>()->to_thrift());
        node.__set_fn(function);
        node.__set_num_children(2);
        node.__set_is_nullable(true);
        auto expr = VectorizedFnCall::create_shared(node);
        expr->add_child(std::make_shared<MockSlotRef>(1, make_nullable(int_type)));
        expr->add_child(VLiteral::create_shared(int_type, Field::create_field<TYPE_INT>(value)));
        IndexReadOptions read;
        read.return_columns = {0, static_cast<uint32_t>(path_id)};
        read.target_cast_type_for_variants["v.a"] = make_nullable(int_type);
        read.common_expr_ctxs_push_down = {VExprContext::create_shared(expr)};
        read.enable_fallback_on_missing_inverted_index = true;
        if (equality) {
            read.predicates = {create_comparison_predicate<PredicateType::EQ>(
                    path_id, "v.a", int_type, Field::create_field<TYPE_INT>(value), false)};
        } else {
            read.predicates = {create_comparison_predicate<PredicateType::GT>(
                    path_id, "v.a", int_type, Field::create_field<TYPE_INT>(value), false)};
        }
        // With the old INT template, only index metadata was added. The missing
        // file must retain its error contract when fallback is disabled.
        if (GetParam() && equality) {
            auto strict_read = read;
            strict_read.common_expr_ctxs_push_down.clear();
            strict_read.enable_fallback_on_missing_inverted_index = false;
            auto missing = read_rowsets(readable.value(), strict_read);
            ASSERT_FALSE(missing.has_value());
            EXPECT_TRUE(missing.error().is<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>())
                    << missing.error();
        }
        auto result = read_rowsets(readable.value(), read);
        ASSERT_TRUE(result.has_value()) << result.error();
        EXPECT_EQ(result->rows_read, equality ? 2 : 3);
        EXPECT_FALSE(result->inverted_index_used());
        if (GetParam()) {
            EXPECT_GT(result->stats.inverted_index_downgrade_count, 0);
        }
        if (!GetParam()) {
            EXPECT_EQ(result->stats.raw_rows_read, 6);
        }
        EXPECT_EQ(result->stats.rows_stats_filtered, 0);
        EXPECT_EQ(result->stats.rows_bf_filtered, 0);
    }
    _tablet_schema = build_tablet_schema(options);
    _tablet_schema->set_schema_version(inputs->front()->tablet_schema()->schema_version() + 1);
    tablet()->update_max_version_schema(_tablet_schema);
    IndexRowsetSpec fresh_row;
    fresh_row.version = 2;
    fresh_row.batches.push_back(IndexBatch::single_variant({R"({"a":5})"}, 6));
    auto latest = write_rowset(fresh_row);
    ASSERT_TRUE(latest.has_value()) << latest.error();
    inputs->push_back(latest.value());
    auto output = compact_rowsets(IndexCompactionKind::FULL, inputs.value());
    ASSERT_TRUE(output.has_value()) << output.error();
    auto expanded = rowsets_with_variant_extended_schema({output.value()});
    ASSERT_TRUE(expanded.has_value()) << expanded.error();
    const auto new_id = column_id_by_path("v.a");
    ASSERT_GE(new_id, 0);
    IndexReadOptions indexed;
    indexed.return_columns = {0, static_cast<uint32_t>(new_id)};
    indexed.target_cast_type_for_variants["v.a"] = make_nullable(int_type);
    indexed.predicates = {create_comparison_predicate<PredicateType::EQ>(
            new_id, "v.a", int_type, Field::create_field<TYPE_INT>(5), false)};
    for (bool enabled : {true, false}) {
        indexed.enable_inverted_index_query = enabled;
        auto result = read_rowsets(expanded.value(), indexed);
        ASSERT_TRUE(result.has_value()) << result.error();
        EXPECT_EQ(result->rows_read, 3);
        EXPECT_EQ(result->inverted_index_used(), enabled);
    }
    if (GetParam()) { // This fixture has only idx_a, so a dropped index cannot hide behind a root index.
        auto dropped = drop_inverted_indexes_and_reload({options.inverted_indexes.back()});
        ASSERT_TRUE(dropped.has_value()) << dropped.error();
        options.inverted_indexes.clear();
        _tablet_schema = build_tablet_schema(options);
        _tablet_schema->set_schema_version(output.value()->tablet_schema()->schema_version() + 1);
        tablet()->update_max_version_schema(_tablet_schema);
        IndexRowsetSpec after_drop;
        after_drop.version = 3;
        after_drop.batches.push_back(IndexBatch::single_variant({R"({"a":5})"}, 7));
        auto last = write_rowset(after_drop);
        ASSERT_TRUE(last.has_value()) << last.error();
        auto drop_inputs = dropped.value();
        drop_inputs.push_back(last.value());
        auto rewritten = compact_rowsets(IndexCompactionKind::FULL, drop_inputs);
        ASSERT_TRUE(rewritten.has_value()) << rewritten.error();
        auto probe = probe_rowset(rewritten.value());
        ASSERT_TRUE(probe.has_value()) << probe.error();
        EXPECT_EQ(probe->index_files.existing_files, 0);
        auto readable = rowsets_with_variant_extended_schema({rewritten.value()});
        ASSERT_TRUE(readable.has_value()) << readable.error();
        const auto dropped_id = column_id_by_path("v.a");
        ASSERT_GE(dropped_id, 0);
        indexed.return_columns = {0, static_cast<uint32_t>(dropped_id)};
        indexed.predicates = {create_comparison_predicate<PredicateType::EQ>(
                dropped_id, "v.a", int_type, Field::create_field<TYPE_INT>(5), false)};
        indexed.enable_inverted_index_query = true;
        auto result = read_rowsets(readable.value(), indexed);
        ASSERT_TRUE(result.has_value()) << result.error();
        EXPECT_EQ(result->rows_read, 4);
        EXPECT_FALSE(result->inverted_index_used());
    }
}

INSTANTIATE_TEST_SUITE_P(ExistingTemplate, SegmentPredicateTemplateAlterTest, testing::Bool());

} // namespace doris::index_storage_test
