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
#include <string>
#include <vector>

#include "exec/common/variant_util.h"
#include "storage/variant/index_storage_variant_test_base.h"

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
    variant_util::VariantCompactionUtil::get_subpaths(
            0, VariantSerializeTypePB::VARIANT_SERIALIZE_V1, stats, unlimited);
    EXPECT_TRUE(unlimited.sub_path_set.contains(StringRef("alpha")));
    EXPECT_TRUE(unlimited.sub_path_set.contains(StringRef("beta")));
    EXPECT_TRUE(unlimited.sub_path_set.contains(StringRef("gamma")));
    EXPECT_TRUE(unlimited.sparse_path_set.empty());

    // V2 reads a count of 0 as no dynamic subcolumn, so neither set gets a path
    VariantCompactionPaths none;
    variant_util::VariantCompactionUtil::get_subpaths(
            0, VariantSerializeTypePB::VARIANT_SERIALIZE_V2, stats, none);
    EXPECT_TRUE(none.sub_path_set.empty());
    EXPECT_TRUE(none.sparse_path_set.empty());

    VariantCompactionPaths top_one;
    variant_util::VariantCompactionUtil::get_subpaths(
            1, VariantSerializeTypePB::VARIANT_SERIALIZE_V1, stats, top_one);
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
    variant_util::VariantCompactionUtil::get_subpaths(
            2, VariantSerializeTypePB::VARIANT_SERIALIZE_V1, exact_limit, at_limit);
    EXPECT_TRUE(at_limit.sub_path_set.contains(StringRef("alpha")));
    EXPECT_TRUE(at_limit.sub_path_set.contains(StringRef("beta")));
    EXPECT_TRUE(at_limit.sparse_path_set.empty());

    VariantCompactionPaths empty;
    variant_util::VariantCompactionUtil::get_subpaths(
            1, VariantSerializeTypePB::VARIANT_SERIALIZE_V1, {}, empty);
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

// Every VARIANT parameter combination the FE accepts (PropertyAnalyzer.validateVariantProperties and
// LogicalPlanBuilder.visitVariantType): doc mode excludes the subcolumn count, typed paths to sparse
// and sparse shards; nested group excludes doc mode and a count above 0. For each one the writer's
// layout, the compaction schema and the compacted layout must follow the same rule, and compaction
// must keep every value.
namespace {

enum class Layout { ORDINARY, DOC, NESTED_GROUP };

struct MatrixCase {
    Layout layout = Layout::ORDINARY;
    VariantSerializeTypePB serialize_type = VariantSerializeTypePB::VARIANT_SERIALIZE_V1;
    int32_t max_subcolumns_count = 0;
    bool typed_paths_to_sparse = false;
    bool with_template = false;
    int32_t sparse_hash_shard_count = 0;
    int64_t doc_materialization_min_rows = 0;
    int32_t doc_hash_shard_count = 0;
};

std::string case_name(const testing::TestParamInfo<MatrixCase>& info) {
    const MatrixCase& c = info.param;
    std::string name = "nested_group";
    if (c.layout == Layout::ORDINARY) {
        name = "ordinary";
    } else if (c.layout == Layout::DOC) {
        name = "doc";
    }
    name += c.serialize_type == VariantSerializeTypePB::VARIANT_SERIALIZE_V1 ? "_v1" : "_v2";
    name += "_count" + std::to_string(c.max_subcolumns_count);
    name += c.typed_paths_to_sparse ? "_typed_to_sparse" : "";
    name += c.with_template ? "_template" : "";
    name += "_shard" + std::to_string(c.sparse_hash_shard_count);
    name += "_docrows" + std::to_string(c.doc_materialization_min_rows);
    name += "_docshard" + std::to_string(c.doc_hash_shard_count);
    return name;
}

std::vector<MatrixCase> all_cases() {
    std::vector<MatrixCase> cases;
    for (auto serialize_type : {VariantSerializeTypePB::VARIANT_SERIALIZE_V1,
                                VariantSerializeTypePB::VARIANT_SERIALIZE_V2}) {
        for (bool with_template : {false, true}) {
            for (int32_t count : {0, 3, 100}) {
                for (bool to_sparse : {false, true}) {
                    for (int32_t shards : {0, 2}) {
                        cases.push_back({.layout = Layout::ORDINARY,
                                         .serialize_type = serialize_type,
                                         .max_subcolumns_count = count,
                                         .typed_paths_to_sparse = to_sparse,
                                         .with_template = with_template,
                                         .sparse_hash_shard_count = shards});
                    }
                }
            }
            for (int64_t min_rows : {0, 100}) {
                for (int32_t doc_shards : {1, 2}) {
                    cases.push_back({.layout = Layout::DOC,
                                     .serialize_type = serialize_type,
                                     .with_template = with_template,
                                     .doc_materialization_min_rows = min_rows,
                                     .doc_hash_shard_count = doc_shards});
                }
            }
            cases.push_back({.layout = Layout::NESTED_GROUP,
                             .serialize_type = serialize_type,
                             .with_template = with_template});
        }
    }
    return cases;
}

// Paths of the ordinary data, most frequent first; no two share a non-null count, so the writer and
// compaction pick the same top paths whatever their tie-breaking.
const std::vector<std::string> ORDINARY_PATHS {"a", "b", "t", "c", "d", "e", "x.y"};

std::vector<std::string> ordinary_rows() {
    std::vector<std::string> rows;
    for (int row = 0; row < 7; ++row) {
        std::string json = R"({"a": "a)" + std::to_string(row) + "\"";
        const auto add = [&](int rows_with_path, const std::string& field) {
            if (row < rows_with_path) {
                json += ", " + field;
            }
        };
        add(6, R"("b": "b)" + std::to_string(row) + "\"");
        add(5, R"("t": )" + std::to_string(row));
        add(4, R"("c": "c)" + std::to_string(row) + "\"");
        add(3, R"("d": "d)" + std::to_string(row) + "\"");
        add(2, R"("e": "e)" + std::to_string(row) + "\"");
        add(1, R"("x": {"y": "y)" + std::to_string(row) + "\"}");
        rows.push_back(json + "}");
    }
    return rows;
}

std::vector<std::string> nested_group_rows() {
    return {R"({"owner": "alice", "profile": {"region": "us"}, "items": [{"sku": "a", "qty": 1}]})",
            R"({"owner": "bob", "profile": {"region": "eu"}, "items": [{"sku": "b", "qty": 2}]})",
            R"({"owner": "carol", "items": [{"sku": "c", "qty": 3}]})"};
}

// The paths an ordinary VARIANT materializes: the typed path `t` unless typed paths go to the
// sparse column (then it competes with the dynamic paths), and the most frequent dynamic paths up
// to the count, where a count of 0 means every one under V1 and none under V2.
std::vector<std::string> expected_materialized(const MatrixCase& c) {
    const bool typed_fixed = c.with_template && !c.typed_paths_to_sparse;
    std::vector<std::string> dynamic;
    for (const auto& path : ORDINARY_PATHS) {
        if (!(typed_fixed && path == "t")) {
            dynamic.push_back(path);
        }
    }
    size_t selected = std::min<size_t>(c.max_subcolumns_count, dynamic.size());
    if (c.max_subcolumns_count == 0) {
        selected = c.serialize_type == VariantSerializeTypePB::VARIANT_SERIALIZE_V1 ? dynamic.size()
                                                                                    : 0;
    }
    std::vector<std::string> result(dynamic.begin(), dynamic.begin() + selected);
    if (typed_fixed) {
        result.emplace_back("t");
    }
    return result;
}

size_t count_extracted_with_prefix(const TabletSchema& schema, std::string_view prefix) {
    size_t count = 0;
    for (int32_t column_id = 0; column_id < schema.num_columns(); ++column_id) {
        const auto& column = schema.column(column_id);
        if (column.is_extracted_column() && column.has_path_info() &&
            column.path_info_ptr()->copy_pop_front().get_path().starts_with(prefix)) {
            ++count;
        }
    }
    return count;
}

} // namespace

class VariantCompactionSchemaMatrixTest : public IndexStorageTestFixture,
                                          public testing::WithParamInterface<MatrixCase> {};

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest macros inflate the matrix.
TEST_P(VariantCompactionSchemaMatrixTest, WriterCompactionSchemaAndCompactedLayoutAgree) {
    const MatrixCase& c = GetParam();
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.serialize_type = c.serialize_type;
    variant.max_subcolumns_count = c.max_subcolumns_count;
    variant.enable_typed_paths_to_sparse = c.typed_paths_to_sparse;
    variant.sparse_hash_shard_count = c.sparse_hash_shard_count;
    variant.enable_doc_mode = c.layout == Layout::DOC;
    variant.doc_materialization_min_rows = c.doc_materialization_min_rows;
    variant.doc_hash_shard_count = c.doc_hash_shard_count;
    variant.enable_nested_group = c.layout == Layout::NESTED_GROUP;
    if (c.with_template) {
        variant.predefined_paths = {VariantPathSpec {.path = "t",
                                                     .type = FieldType::OLAP_FIELD_TYPE_INT,
                                                     .nullable = true,
                                                     .pattern_type = PatternTypePB::MATCH_NAME,
                                                     .array_item_type = {},
                                                     .array_item_nullable = true}};
    }
    IndexTabletOptions options;
    options.tablet_id = 120000;
    options.variant_columns = {std::move(variant)};
    ASSERT_TRUE(create_tablet(options).ok());

    const std::vector<std::string> rows =
            c.layout == Layout::NESTED_GROUP ? nested_group_rows() : ordinary_rows();
    std::vector<IndexRowsetSpec> specs(2);
    for (int64_t version = 0; version < 2; ++version) {
        specs[version].version = version;
        specs[version].batches.push_back(
                IndexBatch::single_variant(rows, static_cast<int32_t>(version * 100)));
    }
    auto rowsets = write_rowsets(specs);
    if (c.layout == Layout::NESTED_GROUP && !rowsets.has_value() &&
        rowsets.error().is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) {
        GTEST_SKIP() << "NestedGroup write path is not available in this build: "
                     << rowsets.error();
    }
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    // the compaction schema
    auto compaction_schema = std::make_shared<TabletSchema>(*tablet_schema());
    VariantCompactionPathsMap compaction_paths;
    auto status = variant_util::VariantCompactionUtil::get_extended_compaction_schema(
            rowsets.value(), compaction_schema, compaction_paths);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const size_t sparse_columns =
            count_extracted_with_prefix(*compaction_schema, SPARSE_COLUMN_PATH);
    switch (c.layout) {
    case Layout::ORDINARY: {
        const auto materialized = expected_materialized(c);
        for (const auto& path : ORDINARY_PATHS) {
            const bool expect = std::ranges::find(materialized, path) != materialized.end();
            EXPECT_EQ(schema_has_extracted_variant_path(*compaction_schema, 2, path), expect)
                    << "compaction schema " << path;
        }
        EXPECT_EQ(sparse_columns, std::max(1, c.sparse_hash_shard_count));
        break;
    }
    case Layout::DOC:
        for (const auto& path : ORDINARY_PATHS) {
            EXPECT_FALSE(schema_has_extracted_variant_path(*compaction_schema, 2, path)) << path;
        }
        EXPECT_EQ(sparse_columns, 0);
        break;
    case Layout::NESTED_GROUP:
        EXPECT_TRUE(schema_has_extracted_variant_path(*compaction_schema, 2, "owner"));
        EXPECT_TRUE(schema_has_extracted_variant_path(*compaction_schema, 2, "profile.region"));
        EXPECT_EQ(sparse_columns, 0);
        break;
    }

    // the layout the writer produced and the one compaction produces
    IndexReadOptions read_options;
    read_options.return_columns = {0, static_cast<uint32_t>(tablet_schema()->field_index("v"))};
    read_options.collect_variant_values = true;
    auto before = read_rowsets(rowsets.value(), read_options);
    ASSERT_TRUE(before.has_value()) << before.error();
    auto written = probe_rowset(rowsets.value()[0]);
    ASSERT_TRUE(written.has_value()) << written.error();
    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets.value());
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_EQ(compacted.value()->num_rows(), static_cast<int64_t>(rows.size() * 2));
    auto compacted_probe = probe_rowset(compacted.value());
    ASSERT_TRUE(compacted_probe.has_value()) << compacted_probe.error();
    for (const auto* probe : {&written.value(), &compacted_probe.value()}) {
        const std::string which = probe == &written.value() ? "written " : "compacted ";
        switch (c.layout) {
        case Layout::ORDINARY: {
            const auto materialized = expected_materialized(c);
            for (const auto& path : ORDINARY_PATHS) {
                const bool expect = std::ranges::find(materialized, path) != materialized.end();
                EXPECT_EQ(has_variant_layout(*probe, 2, path), expect) << which << path;
                EXPECT_EQ(has_sparse_path_stat(*probe, path), !expect) << which << path;
            }
            break;
        }
        case Layout::DOC:
            EXPECT_TRUE(has_doc_value_column(*probe)) << which;
            break;
        case Layout::NESTED_GROUP:
            EXPECT_TRUE(has_variant_layout(*probe, 2, "owner")) << which;
            break;
        }
    }

    // compaction keeps every value
    auto after = read_rowsets({compacted.value()}, read_options);
    ASSERT_TRUE(after.has_value()) << after.error();
    auto before_values = canonical_variant_values(before->variant_values_by_uid[2]);
    auto after_values = canonical_variant_values(after->variant_values_by_uid[2]);
    std::ranges::sort(before_values);
    std::ranges::sort(after_values);
    EXPECT_EQ(before_values.size(), rows.size() * 2);
    EXPECT_EQ(after_values, before_values);
}

INSTANTIATE_TEST_SUITE_P(AllFeVariantParameters, VariantCompactionSchemaMatrixTest,
                         testing::ValuesIn(all_cases()), case_name);

} // namespace doris::index_storage_test
