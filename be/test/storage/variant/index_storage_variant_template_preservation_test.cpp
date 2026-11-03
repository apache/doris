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
#include <array>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/config.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/value/variant/variant_batch_builder.h"
#include "exec/common/variant_util.h"
#include "runtime/exec_env.h"
#include "storage/compaction/base_compaction.h"
#include "storage/compaction/cumulative_compaction.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_reader_context.h"
#include "storage/segment/column_writer.h"
#include "storage/segment/segment_loader.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/variant/index_storage_variant_test_base.h"
#include "util/defer_op.h"

namespace doris::index_storage_test {
namespace {

constexpr int32_t kVariantUid = 2;

VariantColumnSpec variant_with_template(std::optional<FieldType> type, int32_t budget = 8) {
    VariantColumnSpec column;
    column.max_subcolumns_count = budget;
    if (type.has_value()) {
        column.predefined_paths.push_back(
                VariantPathSpec {.path = "p",
                                 .type = type.value(),
                                 .nullable = true,
                                 .pattern_type = PatternTypePB::MATCH_NAME,
                                 .array_item_type = {},
                                 .array_item_nullable = true});
    }
    return column;
}

std::vector<const TabletColumn*> extracted_p_columns(const TabletSchema& schema) {
    std::vector<const TabletColumn*> columns;
    for (const auto& column : schema.columns()) {
        if (column->is_extracted_column() && column->parent_unique_id() == kVariantUid &&
            column->has_path_info() && column->path_info_ptr()->get_path() == "v.p") {
            columns.push_back(column.get());
        }
    }
    return columns;
}

enum class CompactWhen { NEVER, BEFORE_ALTER, AFTER_ALTER_BEFORE_LOAD, AFTER_LOAD };

} // namespace

class IndexStorageVariantTemplatePreservationTest : public IndexStorageTestFixture {
protected:
    void initialize(const VariantColumnSpec& variant, int64_t tablet_id = 110101) {
        IndexTabletOptions options;
        options.tablet_id = tablet_id;
        options.variant_columns = {variant};
        ASSERT_TRUE(create_tablet(options).ok());
    }

    Result<RowsetSharedPtr> write(int64_t version, const std::vector<std::string>& jsons) {
        IndexRowsetSpec rowset;
        rowset.version = version;
        rowset.batches.push_back(
                IndexBatch::single_variant(jsons, static_cast<int32_t>(version * 2)));
        auto written = write_rowset(rowset);
        if (!written.has_value()) {
            return ResultError(written.error());
        }
        // The shared fixture derives rowset IDs from version. This matrix reuses versions on
        // distinct tablets; the segment cache key contains only rowset ID and segment ordinal.
        SegmentLoader::instance()->erase_segments(*written.value()->rowset_meta());
        return written;
    }

    TabletSchemaSPtr target_schema(const VariantColumnSpec& variant) {
        // Rebuild the same key/root schema with exactly the target template. The generic patch
        // helper retains old children and appends templates, which would duplicate pattern p.
        IndexTabletOptions options;
        options.tablet_id = tablet()->tablet_id();
        options.variant_columns = {variant};
        auto target = build_tablet_schema(options);
        target->set_schema_version(tablet_schema()->schema_version() + 1);
        return target;
    }

    void install_write_schema(const TabletSchemaSPtr& schema) {
        // BE UTs use -fno-access-control. Change the fixture's future-write/query schema only;
        // never inject_reader_schema_for_rowsets(), which relabels historical rowset metadata.
        _tablet_schema = schema;
        tablet()->tablet_meta()->_schema = schema;
        tablet()->update_max_version_schema(schema);
    }

    void expect_p_type(const RowsetSharedPtr& rowset, PrimitiveType expected) {
        std::unordered_map<int32_t, variant_util::VariantExtendedInfo> collected;
        auto status = variant_util::VariantCompactionUtil::aggregate_variant_extended_info(
                rowset, &collected);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_TRUE(collected.contains(kVariantUid));
        if (expected == TYPE_VARIANT) {
            // TYPE_VARIANT is the selected schema owner, not a ColumnMetaPB codec. Assert both
            // independently: one pinned VARIANT schema slot and one concrete JSONB file leaf.
            auto schema = std::make_shared<TabletSchema>(*tablet_schema());
            VariantCompactionPathsMap paths;
            status = variant_util::VariantCompactionUtil::get_extended_compaction_schema(
                    {rowset}, schema, paths);
            ASSERT_TRUE(status.ok()) << status;
            const auto columns = extracted_p_columns(*schema);
            ASSERT_EQ(columns.size(), 1) << dump_schema_paths(*schema);
            EXPECT_EQ(columns.front()->type(), FieldType::OLAP_FIELD_TYPE_VARIANT);
            EXPECT_TRUE(paths.at(kVariantUid).sub_path_set.contains(StringRef("p")));
            EXPECT_FALSE(paths.at(kVariantUid).sparse_path_set.contains(StringRef("p")));
            expected = TYPE_JSONB;
        }
        size_t matching_types = 0;
        for (const auto& [path, types] : collected.at(kVariantUid).path_to_data_types) {
            if (path.get_path() != "p") {
                continue;
            }
            for (const auto& type : types) {
                ++matching_types;
                EXPECT_EQ(remove_nullable(type)->get_primitive_type(), expected)
                        << type->get_name();
            }
        }
        EXPECT_GT(matching_types, 0);
    }

    void expect_single_destination(const RowsetSharedPtr& rowset) {
        auto probe = probe_rowset(rowset);
        ASSERT_TRUE(probe.has_value()) << probe.error();
        ASSERT_FALSE(probe->segments.empty());
        for (const auto& segment : probe->segments) {
            SCOPED_TRACE(segment.segment_id);
            std::map<std::string, size_t> destinations;
            for (const auto& column : segment.variant_columns) {
                if (column.parent_unique_id != kVariantUid) {
                    continue;
                }
                ASSERT_FALSE(column.is_doc_value_column); // D7 excludes DOC transitions.
                if (column.is_sparse_column) {
                    for (const auto& [path, count] : column.sparse_non_null_size) {
                        EXPECT_GT(count, 0);
                        ++destinations[path];
                    }
                } else if (column.none_null_size > 0 && !column.relative_path.empty()) {
                    ++destinations[column.relative_path];
                }
            }
            // This fixture has only p/z and complete (10000-entry) statistics. A path cannot
            // have values in two materialized columns, two sparse shards, or both destinations.
            EXPECT_FALSE(destinations.empty());
            for (const auto& [path, count] : destinations) {
                EXPECT_EQ(count, 1) << path;
            }
        }
    }

    Result<RowsetSharedPtr> compact(const std::vector<RowsetSharedPtr>& rowsets, bool base,
                                    bool vertical, bool extracted = true) {
        const bool ordered = config::enable_ordered_data_compaction;
        config::enable_ordered_data_compaction = false;
        Defer restore_ordered {[ordered] { config::enable_ordered_data_compaction = ordered; }};
        // The configured mode is read once at construction.
        const bool configured = config::enable_vertical_compact_variant_subcolumns;
        config::enable_vertical_compact_variant_subcolumns = extracted;
        Defer restore_extracted {
                [configured] { config::enable_vertical_compact_variant_subcolumns = configured; }};
        std::unique_ptr<CompactionMixin> compaction;
        if (base) {
            compaction = std::make_unique<BaseCompaction>(*storage_engine(), tablet());
        } else {
            compaction = std::make_unique<CumulativeCompaction>(*storage_engine(), tablet());
        }
        compaction->_is_vertical = vertical;
        compaction->_input_rowsets = rowsets;
        RETURN_IF_ERROR_RESULT(compaction->CompactionMixin::execute_compact());
        EXPECT_EQ(compaction->is_vertical(), vertical);
        EXPECT_FALSE(compaction->_is_ordered_data_compaction);
        return reload_rowset(compaction->_output_rowset);
    }

    void expect_values(const std::vector<RowsetSharedPtr>& rowsets,
                       const std::vector<std::string>& expected) {
        IndexReadOptions options;
        options.return_columns = {0, 1};
        options.need_ordered_result = true;
        options.collect_variant_values = true;
        auto read = read_rowsets(rowsets, options);
        ASSERT_TRUE(read.has_value()) << read.error();
        EXPECT_EQ(read->rows_read, expected.size());
        ASSERT_TRUE(read->variant_values_by_uid.contains(kVariantUid));
        std::vector<std::optional<std::string>> oracle(expected.begin(), expected.end());
        EXPECT_EQ(canonical_variant_values(read->variant_values_by_uid.at(kVariantUid)),
                  canonical_variant_values(oracle));
    }

    // Decode actual Variant cell identities, not JSON spellings that erase integer widths.
    void expect_p_tags(const std::vector<RowsetSharedPtr>& rowsets,
                       const std::vector<std::optional<VariantPrimitiveId>>& expected) {
        auto schema = std::make_shared<ReadSchema>(tablet_schema()->columns());
        ASSERT_TRUE(schema->init_from_tablet_schema(*tablet_schema(), false, false).ok());
        OlapReaderStatistics stats;
        RuntimeState runtime;
        runtime.set_exec_env(ExecEnv::GetInstance());
        TQueryOptions options;
        options.__set_enable_file_cache(false);
        options.__set_disable_file_cache(true);
        options.__set_enable_segment_cache(false);
        options.__set_batch_size(1024);
        runtime.set_query_options(options);
        RowsetReaderContext context;
        context.read_schema = schema;
        context.stats = &stats;
        context.runtime_state = &runtime;
        context.need_ordered_result = true;
        std::vector<std::optional<VariantPrimitiveId>> actual;
        for (const auto& rowset : rowsets) {
            RowsetReaderSharedPtr reader;
            ASSERT_TRUE(rowset->create_reader(&reader).ok());
            auto status = reader->init(&context);
            ASSERT_TRUE(status.ok()) << status;
            while (true) {
                Block block = schema->create_read_block();
                status = reader->next_batch(&block);
                if (status.is<ErrorCode::END_OF_FILE>()) {
                    break;
                }
                ASSERT_TRUE(status.ok()) << status;
                const auto& values = *block.get_by_position(1).column;
                ASSERT_NE(check_and_get_column<ColumnVariantV2>(values), nullptr);
                visit_variant_v2_values(
                        values, 0, values.size(), {},
                        [](size_t) { ADD_FAILURE() << "unexpected outer NULL"; },
                        [&](size_t, const VariantRef& value) {
                            VariantRef p;
                            if (value.object_find(StringRef("p"), &p)) {
                                actual.emplace_back(p.basic_type() == VariantBasicType::SHORT_STRING
                                                            ? VariantPrimitiveId::STRING
                                                            : p.primitive_id());
                            } else {
                                actual.emplace_back(std::nullopt);
                            }
                        });
            }
        }
        EXPECT_EQ(actual, expected);
    }

    void expect_schema(const std::vector<RowsetSharedPtr>& rowsets, TabletSchemaSPtr target,
                       FieldType expected) {
        VariantCompactionPathsMap paths;
        auto status = variant_util::VariantCompactionUtil::get_extended_compaction_schema(
                rowsets, target, paths);
        ASSERT_TRUE(status.ok()) << status;
        const auto p_columns = extracted_p_columns(*target);
        ASSERT_EQ(p_columns.size(), 1) << dump_schema_paths(*target);
        EXPECT_EQ(p_columns.front()->type(), expected);
    }
};

TEST_F(IndexStorageVariantTemplatePreservationTest, UnchangedTypedSchemaKeepsNativeIntAndInputs) {
    initialize(variant_with_template(FieldType::OLAP_FIELD_TYPE_INT));
    ASSERT_FALSE(HasFatalFailure());
    auto old = write(0, {R"({"p":2000000,"z":0})", R"({"p":2000001,"z":1})"});
    ASSERT_TRUE(old.has_value()) << old.error();
    expect_p_type(old.value(), TYPE_INT);
    TabletSchemaPB before;
    old.value()->tablet_schema()->to_schema_pb(&before);
    const std::string meta_before = old.value()->rowset_meta()->get_rowset_pb().SerializeAsString();
    expect_schema({old.value()}, std::make_shared<TabletSchema>(*tablet_schema()),
                  FieldType::OLAP_FIELD_TYPE_INT);
    TabletSchemaPB after;
    old.value()->tablet_schema()->to_schema_pb(&after);
    EXPECT_EQ(before.SerializeAsString(), after.SerializeAsString());
    EXPECT_EQ(meta_before, old.value()->rowset_meta()->get_rowset_pb().SerializeAsString());
    expect_single_destination(old.value());
}

// Seeded fuzz for lightweight variant_max_subcolumns_count changes on a templated column. The
// table keeps glob template t* BIGINT while the budget moves 0->N, N->0 and up/down, with loads
// and compactions in between. Template-owned typed paths never consume or lose a budget slot;
// only dynamic d* paths obey the budget, hottest first. Override with
// DORIS_VARIANT_BUDGET_SEED=<seed> to replay a failure.
TEST_F(IndexStorageVariantTemplatePreservationTest, BudgetTransitionsWithTemplatesFuzz) {
    uint64_t seed = 20261103;
    if (const char* env = std::getenv("DORIS_VARIANT_BUDGET_SEED"); env != nullptr) {
        seed = std::strtoull(env, nullptr, 10);
    }
    std::cout << "variant budget template fuzz seed=" << seed << std::endl;
    std::mt19937_64 rng(seed);
    auto uniform = [&](int64_t lo, int64_t hi) {
        return std::uniform_int_distribution<int64_t>(lo, hi)(rng);
    };
    auto spec_with_budget = [](int32_t budget) {
        VariantColumnSpec column;
        column.max_subcolumns_count = budget;
        column.predefined_paths.push_back(
                VariantPathSpec {.path = "t*",
                                 .type = FieldType::OLAP_FIELD_TYPE_BIGINT,
                                 .nullable = true,
                                 .pattern_type = PatternTypePB::MATCH_NAME_GLOB,
                                 .array_item_type = {},
                                 .array_item_nullable = true});
        return column;
    };
    constexpr int kTemplatePaths = 3;
    for (int iteration = 0; iteration < 6; ++iteration) {
        const auto dynamic_paths = uniform(2, 9);
        const auto n = uniform(1, dynamic_paths + 2);
        std::vector<int32_t> budgets = {static_cast<int32_t>(uniform(0, 1) == 0 ? 0 : n), 0,
                                        static_cast<int32_t>(n), 0, static_cast<int32_t>(n)};
        for (int i = 0; i < 4; ++i) {
            const auto next = budgets.back() + (uniform(0, 1) == 0 ? -1 : 1) * uniform(1, 3);
            budgets.push_back(static_cast<int32_t>(std::max<int64_t>(1, next)));
        }
        budgets.push_back(1);
        std::string trace = "seed=" + std::to_string(seed) +
                            " iteration=" + std::to_string(iteration) +
                            " dynamic_paths=" + std::to_string(dynamic_paths) + " budgets=";
        for (auto budget : budgets) {
            trace += std::to_string(budget) + ",";
        }
        SCOPED_TRACE(trace);
        std::cout << trace << std::endl;
        std::vector<double> presence;
        for (int64_t i = 0; i < dynamic_paths; ++i) {
            presence.push_back(static_cast<double>(uniform(5, 100)) / 100.0);
        }
        initialize(spec_with_budget(budgets.front()), 110901 + iteration);
        ASSERT_FALSE(HasFatalFailure());
        std::vector<RowsetSharedPtr> rowsets;
        std::vector<std::string> expected;
        std::map<std::string, int64_t> dynamic_counts;
        int64_t version = 0;
        for (size_t step = 0; step < budgets.size(); ++step) {
            SCOPED_TRACE(::testing::Message() << "step=" << step << " budget=" << budgets[step]);
            if (step > 0) {
                install_write_schema(target_schema(spec_with_budget(budgets[step])));
            }
            std::vector<std::string> jsons;
            const auto rows = uniform(1, 6);
            for (int64_t row = 0; row < rows; ++row) {
                // Keys are emitted sorted (d* < t*) so the expected text is canonical.
                std::string json = "{";
                auto add = [&](const std::string& key, int64_t value) {
                    json += (json.size() > 1 ? "," : "");
                    json += "\"" + key + "\":" + std::to_string(value);
                };
                for (int64_t i = 0; i < dynamic_paths; ++i) {
                    if (std::bernoulli_distribution(presence[i])(rng)) {
                        const auto key = "d" + std::to_string(i);
                        add(key, uniform(0, 1000));
                        ++dynamic_counts[key];
                    }
                }
                add("t0", uniform(0, 1000));
                for (int i = 1; i < kTemplatePaths; ++i) {
                    if (uniform(0, 1) == 1) {
                        add("t" + std::to_string(i), uniform(0, 1000));
                    }
                }
                jsons.push_back(json + "}");
            }
            // Keys must be unique across rowsets so the ordered read matches load order.
            IndexRowsetSpec spec;
            spec.version = version;
            spec.batches.push_back(
                    IndexBatch::single_variant(jsons, static_cast<int32_t>(expected.size())));
            auto written = write_rowset(spec);
            ASSERT_TRUE(written.has_value()) << written.error();
            SegmentLoader::instance()->erase_segments(*written.value()->rowset_meta());
            version += 1;
            rowsets.push_back(written.value());
            expected.insert(expected.end(), jsons.begin(), jsons.end());

            // Plan under the current budget: t* stays typed BIGINT and outside the budget.
            auto target = std::make_shared<TabletSchema>(*tablet_schema());
            VariantCompactionPathsMap planned;
            auto status = variant_util::VariantCompactionUtil::get_extended_compaction_schema(
                    rowsets, target, planned);
            ASSERT_TRUE(status.ok()) << status;
            const auto& paths = planned.at(kVariantUid);
            for (int i = 0; i < kTemplatePaths; ++i) {
                const auto key = "t" + std::to_string(i);
                EXPECT_FALSE(paths.sub_path_set.contains(StringRef(key))) << key;
                EXPECT_FALSE(paths.sparse_path_set.contains(StringRef(key))) << key;
                EXPECT_TRUE(paths.carrier_path_set.empty());
            }
            size_t typed_t0 = 0;
            for (const auto& column : target->columns()) {
                if (column->is_extracted_column() && column->parent_unique_id() == kVariantUid &&
                    column->has_path_info() && column->path_info_ptr()->get_path() == "v.t0") {
                    ++typed_t0;
                    EXPECT_EQ(column->type(), FieldType::OLAP_FIELD_TYPE_BIGINT);
                }
            }
            EXPECT_EQ(typed_t0, 1) << dump_schema_paths(*target);
            const int64_t budget = budgets[step];
            const auto observed = static_cast<int64_t>(dynamic_counts.size());
            EXPECT_EQ(paths.sub_path_set.size() + paths.sparse_path_set.size(), observed);
            const auto expected_sub = (budget == 0 || observed <= budget) ? observed : budget;
            EXPECT_EQ(static_cast<int64_t>(paths.sub_path_set.size()), expected_sub);
            int64_t min_sub = std::numeric_limits<int64_t>::max();
            int64_t max_sparse = 0;
            for (const auto& [key, count] : dynamic_counts) {
                const bool sub = paths.sub_path_set.contains(StringRef(key));
                EXPECT_NE(sub, paths.sparse_path_set.contains(StringRef(key))) << key;
                if (sub) {
                    min_sub = std::min(min_sub, count);
                } else {
                    max_sparse = std::max(max_sparse, count);
                }
            }
            EXPECT_GE(min_sub, max_sparse);
            ASSERT_FALSE(HasFailure());

            if (rowsets.size() > 1 && uniform(0, 2) == 0) {
                auto output = compact(rowsets, false, uniform(0, 1) == 1);
                ASSERT_TRUE(output.has_value()) << output.error();
                rowsets = {output.value()};
                expect_single_destination(output.value());
            }
            expect_values(rowsets, expected);
            ASSERT_FALSE(HasFailure());
        }
    }
}

TEST_F(IndexStorageVariantTemplatePreservationTest, DocUnchangedLayoutKeepsDocBuckets) {
    // DOC template/count changes are rejected. Only the existing unchanged-layout dispatch is tested.
    auto variant = variant_with_template(std::nullopt);
    variant.enable_doc_mode = true;
    variant.doc_hash_shard_count = 2;
    initialize(variant);
    ASSERT_FALSE(HasFatalFailure());
    auto schema = std::make_shared<TabletSchema>(*tablet_schema());
    VariantCompactionPathsMap paths;
    ASSERT_TRUE(
            variant_util::VariantCompactionUtil::get_extended_compaction_schema({}, schema, paths)
                    .ok());
    EXPECT_EQ(schema->num_columns(), tablet_schema()->num_columns() + 2);
    EXPECT_EQ(schema->column(2).path_info_ptr()->get_path(), "v.__DORIS_VARIANT_DOC_VALUE__.b0");
    EXPECT_EQ(schema->column(3).path_info_ptr()->get_path(), "v.__DORIS_VARIANT_DOC_VALUE__.b1");
}

TEST_F(IndexStorageVariantTemplatePreservationTest, AddUnrelatedTemplateKeepsExistingTypedOwner) {
    auto before = variant_with_template(FieldType::OLAP_FIELD_TYPE_INT);
    initialize(before);
    ASSERT_FALSE(HasFatalFailure());
    auto old = write(0, {R"({"p":2000000,"z":0})"});
    ASSERT_TRUE(old.has_value()) << old.error();
    auto after = before;
    after.predefined_paths.push_back(VariantPathSpec {.path = "q",
                                                      .type = FieldType::OLAP_FIELD_TYPE_STRING,
                                                      .nullable = true,
                                                      .pattern_type = PatternTypePB::MATCH_NAME,
                                                      .array_item_type = {},
                                                      .array_item_nullable = true});
    install_write_schema(target_schema(after));
    auto recent = write(1, {R"({"p":2000001,"q":"new","z":1})"});
    ASSERT_TRUE(recent.has_value()) << recent.error();
    expect_schema({old.value(), recent.value()}, std::make_shared<TabletSchema>(*tablet_schema()),
                  FieldType::OLAP_FIELD_TYPE_INT);
    expect_p_type(old.value(), TYPE_INT);
    expect_p_type(recent.value(), TYPE_INT);
}

// Typed/template disagreement has no legal ADD-only producer (D9). If it ever appears, keep
// master behavior: the typed owner follows the target template and compaction does not fail.
TEST_F(IndexStorageVariantTemplatePreservationTest, MismatchedTypedTemplateHistoryFollowsTarget) {
    initialize(variant_with_template(FieldType::OLAP_FIELD_TYPE_INT));
    ASSERT_FALSE(HasFatalFailure());
    auto old = write(0, {R"({"p":2000000,"z":0})"});
    ASSERT_TRUE(old.has_value()) << old.error();
    for (const auto changed_type :
         {FieldType::OLAP_FIELD_TYPE_BIGINT, FieldType::OLAP_FIELD_TYPE_STRING}) {
        expect_schema({old.value()}, target_schema(variant_with_template(changed_type)),
                      changed_type);
    }
}

TEST_F(IndexStorageVariantTemplatePreservationTest, AddOverDynamicAndSparseUsesOneCarrier) {
    for (int32_t old_budget : {1, 8}) {
        for (int32_t target_budget : {0, 1, 2}) {
            SCOPED_TRACE(::testing::Message()
                         << "old_budget=" << old_budget << " target_budget=" << target_budget);
            initialize(variant_with_template(std::nullopt, old_budget),
                       110110 + old_budget * 10 + target_budget);
            ASSERT_FALSE(HasFatalFailure());
            auto old = write(0, {R"({"p":"001","z":0})", R"({"z":1})"});
            ASSERT_TRUE(old.has_value()) << old.error();
            auto probe = probe_rowset(old.value());
            ASSERT_TRUE(probe.has_value()) << probe.error();
            EXPECT_EQ(has_sparse_path_stat(probe.value(), "p"), old_budget == 1);
            install_write_schema(target_schema(
                    variant_with_template(FieldType::OLAP_FIELD_TYPE_INT, target_budget)));
            auto recent = write(1, {R"({"p":2000000,"z":2})"});
            ASSERT_TRUE(recent.has_value()) << recent.error();
            expect_schema({old.value(), recent.value()},
                          std::make_shared<TabletSchema>(*tablet_schema()),
                          FieldType::OLAP_FIELD_TYPE_VARIANT);
        }
    }
}

// Existing sparse-only ADD derivation already retains both paths without template casting.
TEST_F(IndexStorageVariantTemplatePreservationTest, AddOverSparseOnlyAtUnlimitedBudget) {
    initialize(variant_with_template(std::nullopt, 1));
    ASSERT_FALSE(HasFatalFailure());
    auto old = write(0, {R"({"p":"001","z":0})", R"({"z":1})"});
    ASSERT_TRUE(old.has_value()) << old.error();
    auto target = target_schema(variant_with_template(FieldType::OLAP_FIELD_TYPE_INT, 0));
    VariantCompactionPathsMap paths;
    ASSERT_TRUE(variant_util::VariantCompactionUtil::get_extended_compaction_schema({old.value()},
                                                                                    target, paths)
                        .ok());
    EXPECT_TRUE(paths.at(kVariantUid).sub_path_set.contains(StringRef("p")));
    EXPECT_TRUE(paths.at(kVariantUid).sub_path_set.contains(StringRef("z")));
    EXPECT_TRUE(paths.at(kVariantUid).sparse_path_set.empty());
    const auto p_columns = extracted_p_columns(*target);
    ASSERT_EQ(p_columns.size(), 1) << dump_schema_paths(*target);
    EXPECT_EQ(p_columns.front()->type(), FieldType::OLAP_FIELD_TYPE_VARIANT);
}

// Pinned template carriers consume no dynamic-budget slot, including ADD without any new write.
TEST_F(IndexStorageVariantTemplatePreservationTest, TemplateCarriersArePinnedOutsideBudget) {
    for (int32_t old_budget : {1, 8}) {
        for (int32_t budget : {0, 1, 2}) {
            for (bool new_typed_rows : {false, true}) {
                SCOPED_TRACE(::testing::Message()
                             << "old_budget=" << old_budget << " budget=" << budget
                             << " new_typed_rows=" << new_typed_rows);
                initialize(variant_with_template(std::nullopt, old_budget),
                           111000 + old_budget * 100 + budget * 10 + new_typed_rows);
                ASSERT_FALSE(HasFatalFailure());
                auto old = write(0, {R"({"alpha":0,"p":"001","z":0})", R"({"alpha":1,"z":1})"});
                ASSERT_TRUE(old.has_value()) << old.error();
                auto target = target_schema(
                        variant_with_template(FieldType::OLAP_FIELD_TYPE_INT, budget));
                std::vector<RowsetSharedPtr> inputs {old.value()};
                if (new_typed_rows) {
                    install_write_schema(target);
                    auto recent = write(1, {R"({"alpha":2,"p":2000000,"z":2})"});
                    ASSERT_TRUE(recent.has_value()) << recent.error();
                    inputs.push_back(recent.value());
                }
                VariantCompactionPathsMap paths;
                auto status = variant_util::VariantCompactionUtil::get_extended_compaction_schema(
                        inputs, target, paths);
                ASSERT_TRUE(status.ok()) << status;
                const auto& layout = paths.at(kVariantUid);
                EXPECT_FALSE(layout.typed_path_set.contains("p"));
                EXPECT_TRUE(layout.sub_path_set.contains(StringRef("p")));
                EXPECT_FALSE(layout.sparse_path_set.contains(StringRef("p")));
                const auto columns = extracted_p_columns(*target);
                ASSERT_EQ(columns.size(), 1) << dump_schema_paths(*target);
                EXPECT_EQ(columns.front()->type(), FieldType::OLAP_FIELD_TYPE_VARIANT);
                EXPECT_FALSE(columns.front()->path_info_ptr()->get_is_typed());
                // z/alpha have equal frequencies: the existing lexical-descending tie break
                // selects z at count=1. Carrier p does not displace either ordinary budget slot.
                EXPECT_TRUE(layout.sub_path_set.contains(StringRef("z")));
                EXPECT_EQ(layout.sub_path_set.contains(StringRef("alpha")), budget != 1);
                EXPECT_EQ(layout.sparse_path_set.contains(StringRef("alpha")), budget == 1);
                EXPECT_EQ(layout.sub_path_set.size(), budget == 1 ? 2 : 3);
            }
        }
    }
}

TEST_F(IndexStorageVariantTemplatePreservationTest, NewTemplateDataKeepsNativeTemplateType) {
    initialize(variant_with_template(FieldType::OLAP_FIELD_TYPE_INT));
    ASSERT_FALSE(HasFatalFailure());
    auto recent = write(0, {R"({"p":2000000,"z":0})"});
    ASSERT_TRUE(recent.has_value()) << recent.error();
    expect_schema({recent.value()}, std::make_shared<TabletSchema>(*tablet_schema()),
                  FieldType::OLAP_FIELD_TYPE_INT);
}

// Old dynamic BIGINT under an added BIGINT template is an identity cast: keep the typed leaf.
TEST_F(IndexStorageVariantTemplatePreservationTest, SameTypeTypedAndDynamicKeepsTypedLeaf) {
    initialize(variant_with_template(std::nullopt));
    ASSERT_FALSE(HasFatalFailure());
    auto old = write(0, {R"({"p":2000000,"z":0})"});
    ASSERT_TRUE(old.has_value()) << old.error();
    expect_p_type(old.value(), TYPE_BIGINT);
    install_write_schema(target_schema(variant_with_template(FieldType::OLAP_FIELD_TYPE_BIGINT)));
    auto recent = write(1, {R"({"p":2000001,"z":1})"});
    ASSERT_TRUE(recent.has_value()) << recent.error();
    expect_p_type(recent.value(), TYPE_BIGINT);
    expect_schema({old.value(), recent.value()}, std::make_shared<TabletSchema>(*tablet_schema()),
                  FieldType::OLAP_FIELD_TYPE_BIGINT);
}

// Bounded/disabled statistics may hide p entirely; scan sparse KEYS, including glob matches.
TEST_F(IndexStorageVariantTemplatePreservationTest, AddFindsSparsePathsMissingFromStatistics) {
    for (bool external_meta : {false, true}) {
        for (int32_t shards : {0, 4}) {
            for (int32_t statistics_limit : {0, 1}) {
                for (bool new_typed_rows : {false, true}) {
                    SCOPED_TRACE(::testing::Message()
                                 << "external_meta=" << external_meta << " shards=" << shards
                                 << " statistics_limit=" << statistics_limit
                                 << " new_typed_rows=" << new_typed_rows);
                    auto before = variant_with_template(std::nullopt, 1);
                    before.sparse_hash_shard_count = shards;
                    before.max_sparse_column_statistics_size = statistics_limit;
                    IndexTabletOptions options;
                    options.tablet_id = 113000 + external_meta * 100 + shards * 10 +
                                        statistics_limit * 2 + new_typed_rows;
                    options.external_segment_meta = external_meta;
                    options.variant_columns = {before};
                    ASSERT_TRUE(create_tablet(options).ok());
                    std::string filler = "a";
                    if (shards > 1) {
                        const auto p_bucket =
                                variant_util::variant_binary_shard_of(StringRef("p"), shards);
                        // Put the lexically earlier filler in p's bucket so the per-bucket cap
                        // records the filler and omits p, rather than depending on hash luck.
                        for (int32_t candidate = 0;; ++candidate) {
                            ASSERT_LT(candidate, 10000);
                            filler = "a" + std::to_string(candidate);
                            if (variant_util::variant_binary_shard_of(StringRef(filler), shards) ==
                                p_bucket) {
                                break;
                            }
                        }
                    }
                    auto old = write(
                            0, {"{\"" + filler + R"(":"first","p":"001","z":0})", R"({"z":1})"});
                    ASSERT_TRUE(old.has_value()) << old.error();
                    auto probe = probe_rowset(old.value());
                    ASSERT_TRUE(probe.has_value()) << probe.error();
                    EXPECT_FALSE(has_sparse_path_stat(probe.value(), "p"));
                    auto after = before;
                    after.predefined_paths =
                            variant_with_template(FieldType::OLAP_FIELD_TYPE_INT).predefined_paths;
                    after.predefined_paths.front().path = "p*";
                    after.predefined_paths.front().pattern_type = PatternTypePB::MATCH_NAME_GLOB;
                    // Keep source/target physical format identical; change only the template.
                    options.variant_columns = {after};
                    auto target = build_tablet_schema(options);
                    target->set_schema_version(tablet_schema()->schema_version() + 1);
                    std::vector<RowsetSharedPtr> inputs {old.value()};
                    if (new_typed_rows) {
                        install_write_schema(target);
                        auto recent = write(1, {R"({"p":2000000,"z":2})"});
                        ASSERT_TRUE(recent.has_value()) << recent.error();
                        inputs.push_back(recent.value());
                    }
                    VariantCompactionPathsMap paths;
                    auto status =
                            variant_util::VariantCompactionUtil::get_extended_compaction_schema(
                                    inputs, target, paths);
                    ASSERT_TRUE(status.ok()) << status;
                    EXPECT_TRUE(paths.at(kVariantUid).sub_path_set.contains(StringRef("p")));
                    EXPECT_FALSE(paths.at(kVariantUid).sparse_path_set.contains(StringRef("p")));
                    const auto columns = extracted_p_columns(*target);
                    ASSERT_EQ(columns.size(), 1) << dump_schema_paths(*target);
                    EXPECT_EQ(columns.front()->type(), FieldType::OLAP_FIELD_TYPE_VARIANT);
                }
            }
        }
    }
}

// C7 selects directly from aggregate_variant_extended_info's count map. Ordinary typed paths
// never enter that map, so C8's ownership filter must leave unaltered-table budget choices intact.
TEST_F(IndexStorageVariantTemplatePreservationTest, UnalteredTemplatePreservesBudgetSelection) {
    for (bool external_meta : {false, true}) {
        for (int32_t shards : {0, 4}) {
            for (int32_t statistics_limit : {0, 1}) {
                for (int32_t budget : {0, 1, 2}) {
                    SCOPED_TRACE(::testing::Message()
                                 << "external_meta=" << external_meta << " shards=" << shards
                                 << " statistics_limit=" << statistics_limit
                                 << " budget=" << budget);
                    auto variant = variant_with_template(FieldType::OLAP_FIELD_TYPE_INT, budget);
                    variant.sparse_hash_shard_count = shards;
                    variant.max_sparse_column_statistics_size = statistics_limit;
                    IndexTabletOptions options;
                    options.tablet_id = 114000 + external_meta * 100 + shards * 10 +
                                        statistics_limit * 3 + budget;
                    options.external_segment_meta = external_meta;
                    options.variant_columns = {variant};
                    ASSERT_TRUE(create_tablet(options).ok());
                    auto rowset = write(0, {R"({"alpha":0,"p":2000000,"z":0})",
                                            R"({"alpha":1,"p":2000001,"z":1})"});
                    ASSERT_TRUE(rowset.has_value()) << rowset.error();
                    std::unordered_map<int32_t, variant_util::VariantExtendedInfo> collected;
                    auto status =
                            variant_util::VariantCompactionUtil::aggregate_variant_extended_info(
                                    rowset.value(), &collected);
                    ASSERT_TRUE(status.ok()) << status;
                    ASSERT_TRUE(collected.contains(kVariantUid));
                    const auto& input = collected.at(kVariantUid);
                    EXPECT_TRUE(input.typed_paths.contains("p"));
                    EXPECT_FALSE(input.path_to_none_null_values.contains("p"));
                    VariantCompactionPaths c7_budget;
                    variant_util::VariantCompactionUtil::get_subpaths(
                            budget, input.path_to_none_null_values, c7_budget);
                    auto target = std::make_shared<TabletSchema>(*tablet_schema());
                    VariantCompactionPathsMap paths;
                    status = variant_util::VariantCompactionUtil::get_extended_compaction_schema(
                            {rowset.value()}, target, paths);
                    ASSERT_TRUE(status.ok()) << status;
                    const auto& output = paths.at(kVariantUid);
                    EXPECT_EQ(output.sub_path_set, c7_budget.sub_path_set);
                    EXPECT_EQ(output.sparse_path_set, c7_budget.sparse_path_set);
                    EXPECT_EQ(output.typed_path_set.size(), 1);
                    ASSERT_TRUE(output.typed_path_set.contains("p"));
                    EXPECT_EQ(output.typed_path_set.at("p").column.type(),
                              FieldType::OLAP_FIELD_TYPE_INT);
                    const auto columns = extracted_p_columns(*target);
                    ASSERT_EQ(columns.size(), 1) << dump_schema_paths(*target);
                    EXPECT_EQ(columns.front()->type(), FieldType::OLAP_FIELD_TYPE_INT);
                }
            }
        }
    }
}

class IndexStorageVariantTemplateRewriteTest
        : public IndexStorageVariantTemplatePreservationTest,
          public testing::WithParamInterface<std::tuple<bool, bool>> {
protected:
    void verify_timings(const VariantColumnSpec& before, const VariantColumnSpec& after,
                        const std::vector<std::string>& old_jsons,
                        const std::vector<std::string>& new_jsons,
                        const std::vector<std::optional<VariantPrimitiveId>>& expected_tags,
                        bool old_sparse = false, bool expect_carrier = true) {
        const auto [base, vertical] = GetParam();
        std::vector<std::optional<std::string>> never_compacted;
        int64_t tablet_id = 110201;
        for (auto when : {CompactWhen::NEVER, CompactWhen::BEFORE_ALTER,
                          CompactWhen::AFTER_ALTER_BEFORE_LOAD, CompactWhen::AFTER_LOAD}) {
            SCOPED_TRACE(::testing::Message() << "timing=" << static_cast<int>(when)
                                              << " base=" << base << " vertical=" << vertical);
            initialize(before, tablet_id++);
            ASSERT_FALSE(HasFatalFailure());
            auto first = write(0, {old_jsons[0], old_jsons[1]});
            ASSERT_TRUE(first.has_value()) << first.error();
            auto second = write(1, {old_jsons[2], old_jsons[3]});
            ASSERT_TRUE(second.has_value()) << second.error();
            std::vector<RowsetSharedPtr> old_rowsets {first.value(), second.value()};
            for (const auto& rowset : old_rowsets) {
                auto probe = probe_rowset(rowset);
                ASSERT_TRUE(probe.has_value()) << probe.error();
                EXPECT_EQ(has_sparse_path_stat(probe.value(), "p"), old_sparse);
                expect_single_destination(rowset);
            }
            expect_values(old_rowsets, old_jsons);
            if (when == CompactWhen::BEFORE_ALTER) {
                auto output = compact(old_rowsets, base, vertical);
                ASSERT_TRUE(output.has_value()) << output.error();
                old_rowsets = {output.value()};
                expect_single_destination(output.value());
            }
            auto target = target_schema(after);
            install_write_schema(target);
            // Querying under the new policy must not cast historical values, even before compaction.
            expect_values(old_rowsets, old_jsons);
            if (when == CompactWhen::AFTER_ALTER_BEFORE_LOAD) {
                auto output = compact(old_rowsets, base, vertical);
                ASSERT_TRUE(output.has_value()) << output.error();
                old_rowsets = {output.value()};
                expect_single_destination(output.value());
            }
            auto recent = write(2, new_jsons);
            ASSERT_TRUE(recent.has_value()) << recent.error();
            expect_single_destination(recent.value());
            auto all = old_rowsets;
            all.push_back(recent.value());
            auto expected = old_jsons;
            expected.insert(expected.end(), new_jsons.begin(), new_jsons.end());
            expect_values(all, expected);
            expect_p_tags(all, expected_tags);
            if (when == CompactWhen::AFTER_LOAD) {
                auto output = compact(all, base, vertical);
                ASSERT_TRUE(output.has_value()) << output.error();
                all = {output.value()};
                expect_single_destination(output.value());
                // Mixed template/source types require a carrier, not a common target-type cast.
                if (expect_carrier) {
                    expect_p_type(output.value(), TYPE_VARIANT);
                }
            }
            expect_values(all, expected);
            expect_p_tags(all, expected_tags);
            IndexReadOptions options;
            options.return_columns = {0, 1};
            options.need_ordered_result = true;
            options.collect_variant_values = true;
            auto actual = read_rowsets(all, options);
            ASSERT_TRUE(actual.has_value()) << actual.error();
            const auto values =
                    canonical_variant_values(actual->variant_values_by_uid.at(kVariantUid));
            if (when == CompactWhen::NEVER) {
                never_compacted = values;
            } else {
                EXPECT_EQ(values, never_compacted);
            }
        }
    }
};

TEST_P(IndexStorageVariantTemplateRewriteTest, UnchangedTemplatePreservesValuesAndOwnership) {
    const auto variant = variant_with_template(FieldType::OLAP_FIELD_TYPE_INT);
    verify_timings(variant, variant,
                   {R"({"p":2000000,"z":0})", R"({"p":2000001,"z":1})", R"({"p":2000002,"z":2})",
                    R"({"p":2000003,"z":3})"},
                   {R"({"p":2000004,"z":4})", R"({"p":2000005,"z":5})"},
                   std::vector<std::optional<VariantPrimitiveId>>(6, VariantPrimitiveId::INT32),
                   false, false);
}

TEST_P(IndexStorageVariantTemplateRewriteTest, UnchangedSparseLayoutPreservesValuesAndOwnership) {
    for (int32_t shards : {0, 4}) {
        SCOPED_TRACE(shards);
        auto variant = variant_with_template(std::nullopt, 1);
        variant.sparse_hash_shard_count = shards;
        verify_timings(variant, variant,
                       {R"({"p":"001","z":0})", R"({"z":1})", R"({"p":"bad","z":2})", R"({"z":3})"},
                       {R"({"p":"0004","z":4})", R"({"p":"0005","z":5})"},
                       {VariantPrimitiveId::STRING, std::nullopt, VariantPrimitiveId::STRING,
                        std::nullopt, VariantPrimitiveId::STRING, VariantPrimitiveId::STRING},
                       true, false);
    }
}

TEST_P(IndexStorageVariantTemplateRewriteTest, AddOverDynamicPreservesTimingAndTags) {
    verify_timings(
            variant_with_template(std::nullopt),
            variant_with_template(FieldType::OLAP_FIELD_TYPE_INT),
            {R"({"p":"001","z":0})", R"({"p":"bad","z":1})", R"({"p":"2147483648","z":2})",
             R"({"p":"0003","z":3})"},
            {R"({"p":2000004,"z":4})", R"({"p":2000005,"z":5})"},
            {VariantPrimitiveId::STRING, VariantPrimitiveId::STRING, VariantPrimitiveId::STRING,
             VariantPrimitiveId::STRING, VariantPrimitiveId::INT32, VariantPrimitiveId::INT32});
}

TEST_P(IndexStorageVariantTemplateRewriteTest, AddOverSparsePreservesTimingAndTags) {
    for (int32_t shards : {0, 4}) {
        SCOPED_TRACE(shards);
        auto before = variant_with_template(std::nullopt, 1);
        auto after = variant_with_template(FieldType::OLAP_FIELD_TYPE_INT, 1);
        before.sparse_hash_shard_count = shards;
        after.sparse_hash_shard_count = shards;
        verify_timings(before, after,
                       {R"({"p":"001","z":0})", R"({"z":1})", R"({"p":"bad","z":2})", R"({"z":3})"},
                       {R"({"p":2000004,"z":4})", R"({"p":2000005,"z":5})"},
                       {VariantPrimitiveId::STRING, std::nullopt, VariantPrimitiveId::STRING,
                        std::nullopt, VariantPrimitiveId::INT32, VariantPrimitiveId::INT32},
                       true);
    }
}

// Newly typed rows after ADD have no none_null_size. Count their actual null gaps, not
// segment rows, when they share a carrier with historical dynamic values.
// Old dynamic p is stored as BIGINT, the added template type, so compaction keeps a typed leaf
// (no carrier); present-cell counts, values and tags must still equal the uncompacted baseline.
TEST_P(IndexStorageVariantTemplateRewriteTest, NullableSameTypeLeafCountsOnlyPresentCells) {
    verify_timings(variant_with_template(std::nullopt),
                   variant_with_template(FieldType::OLAP_FIELD_TYPE_BIGINT),
                   {R"({"p":2000000,"z":0})", R"({"z":1})", R"({"p":2000002,"z":2})", R"({"z":3})"},
                   {R"({"p":5000000000,"z":4})", R"({"z":5})"},
                   {VariantPrimitiveId::INT32, std::nullopt, VariantPrimitiveId::INT32,
                    std::nullopt, VariantPrimitiveId::INT64, std::nullopt},
                   false, false);
}

// Exercise physical reopening, not just schema derivation: carriers persist a concrete JSONB
// codec, so inline metadata must register them as ordinary extracted leaves too.
TEST_P(IndexStorageVariantTemplateRewriteTest, InlineCarrierPreservesValuesAndTags) {
    const auto [base, vertical] = GetParam();
    for (int32_t old_budget : {1, 8}) {
        for (int32_t shards : {0, 4}) {
            for (bool extracted : {false, true}) {
                SCOPED_TRACE(::testing::Message() << "old_budget=" << old_budget << " shards="
                                                  << shards << " extracted=" << extracted);
                IndexTabletOptions options;
                options.tablet_id = 115000 + old_budget * 100 + shards * 10 + extracted * 4 +
                                    base * 2 + vertical;
                options.external_segment_meta = false;
                auto before = variant_with_template(std::nullopt, old_budget);
                before.sparse_hash_shard_count = shards;
                options.variant_columns = {before};
                ASSERT_TRUE(create_tablet(options).ok());
                auto first = write(0, {R"({"p":"001","z":0})", R"({"z":1})"});
                auto second = write(1, {R"({"p":"bad","z":2})", R"({"z":3})"});
                ASSERT_TRUE(first.has_value()) << first.error();
                ASSERT_TRUE(second.has_value()) << second.error();
                auto after = variant_with_template(FieldType::OLAP_FIELD_TYPE_INT, 1);
                after.sparse_hash_shard_count = shards;
                options.variant_columns = {after};
                auto target = build_tablet_schema(options);
                target->set_schema_version(tablet_schema()->schema_version() + 1);
                install_write_schema(target);
                auto recent = write(2, {R"({"p":2000004,"z":4})", R"({"p":2000005,"z":5})"});
                ASSERT_TRUE(recent.has_value()) << recent.error();
                expect_p_type(recent.value(), TYPE_INT); // New writes still use the new template.
                segment_v2::VariantSubcolumnWriter::set_chunk_values_for_test(1);
                Defer restore_chunks {
                        [] { segment_v2::VariantSubcolumnWriter::set_chunk_values_for_test(0); }};
                auto output = compact({first.value(), second.value(), recent.value()}, base,
                                      vertical, extracted);
                ASSERT_TRUE(output.has_value()) << output.error();
                expect_p_type(output.value(),
                              TYPE_JSONB); // Physical codec, not a VARIANT ColumnMetaPB.
                expect_single_destination(output.value());
                expect_values({output.value()},
                              {R"({"p":"001","z":0})", R"({"z":1})", R"({"p":"bad","z":2})",
                               R"({"z":3})", R"({"p":2000004,"z":4})", R"({"p":2000005,"z":5})"});
                expect_p_tags({output.value()},
                              {VariantPrimitiveId::STRING, std::nullopt, VariantPrimitiveId::STRING,
                               std::nullopt, VariantPrimitiveId::INT32, VariantPrimitiveId::INT32});
                // Publication keeps the target root template and removes extracted schema
                // slots. Equal template lists must not hide this carrier on the next compaction.
                ColumnPB compacted_root;
                ColumnPB target_root;
                output.value()
                        ->tablet_schema()
                        ->column_by_uid(kVariantUid)
                        .to_schema_pb(&compacted_root);
                target->column_by_uid(kVariantUid).to_schema_pb(&target_root);
                ASSERT_EQ(compacted_root.SerializeAsString(), target_root.SerializeAsString());
                auto newest = write(3, {R"({"p":2000006,"z":6})", R"({"p":2000007,"z":7})"});
                ASSERT_TRUE(newest.has_value()) << newest.error();
                auto repeated =
                        compact({output.value(), newest.value()}, base, vertical, extracted);
                ASSERT_TRUE(repeated.has_value()) << repeated.error();
                expect_single_destination(repeated.value());
                expect_values({repeated.value()},
                              {R"({"p":"001","z":0})", R"({"z":1})", R"({"p":"bad","z":2})",
                               R"({"z":3})", R"({"p":2000004,"z":4})", R"({"p":2000005,"z":5})",
                               R"({"p":2000006,"z":6})", R"({"p":2000007,"z":7})"});
                expect_p_tags({repeated.value()},
                              {VariantPrimitiveId::STRING, std::nullopt, VariantPrimitiveId::STRING,
                               std::nullopt, VariantPrimitiveId::INT32, VariantPrimitiveId::INT32,
                               VariantPrimitiveId::INT32, VariantPrimitiveId::INT32});
            }
        }
    }
}

// Materialized carriers use the existing JSONB codec. UUID has no JSONB tag and therefore
// becomes its string spelling, just as on a mixed-type untemplated dynamic path.
TEST_P(IndexStorageVariantTemplateRewriteTest, AddOverDynamicUuidKeepsEveryValueAsJsonb) {
    const auto [base, vertical] = GetParam();
    initialize(variant_with_template(std::nullopt), 116000 + base * 2 + vertical);
    ASSERT_FALSE(HasFatalFailure());
    const std::array<uint8_t, 16> uuid {0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
                                        0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff};
    VariantBatchBuilder builder;
    auto row = builder.begin_row();
    auto object = row.start_object();
    object.add_key(StringRef("p"));
    row.add_uuid(uuid);
    object.finish();
    row.finish();
    const auto batch = builder.finish_batch();
    auto source = ColumnVariantV2::create();
    source->insert_encoded_batch(batch);
    IndexRowsetSpec spec;
    spec.version = 0;
    spec.batches.push_back(IndexBatch::single_variant_column(source->get_ptr(), 0));
    auto old = write_rowset(spec);
    ASSERT_TRUE(old.has_value()) << old.error();
    SegmentLoader::instance()->erase_segments(*old.value()->rowset_meta());
    expect_p_type(old.value(), TYPE_UUID);
    expect_p_tags({old.value()}, {VariantPrimitiveId::UUID});
    const std::vector<std::string> expected {R"({"p":"00112233-4455-6677-8899-aabbccddeeff"})",
                                             R"({"p":"00112233-4455-6677-8899-aabbccddeeff"})",
                                             R"({"p":"new string"})"};
    install_write_schema(target_schema(variant_with_template(FieldType::OLAP_FIELD_TYPE_STRING)));
    auto recent = write(1, {expected[1], expected[2]});
    ASSERT_TRUE(recent.has_value()) << recent.error();
    expect_values({old.value(), recent.value()}, expected);
    auto output = compact({old.value(), recent.value()}, base, vertical);
    ASSERT_TRUE(output.has_value()) << output.error();
    expect_p_type(output.value(), TYPE_JSONB);
    expect_single_destination(output.value());
    expect_values({output.value()}, expected);
    expect_p_tags({output.value()}, std::vector<std::optional<VariantPrimitiveId>>(
                                            expected.size(), VariantPrimitiveId::STRING));
}

constexpr auto kPlanningFailure = "Compaction.resolve_variant_write_schema.planning_failure";
constexpr auto kAddedTemplateKeyScan =
        "variant_util.collect_compaction_schema.added_template_key_scan";

// Separate column groups off + never-altered (schema_version 0): no carrier planning at all, so
// an injected planning failure is never reached and master's native full-root write remains.
TEST_F(IndexStorageVariantTemplatePreservationTest, VerticalOffUnalteredSkipsCarrierPlanning) {
    initialize(variant_with_template(FieldType::OLAP_FIELD_TYPE_INT), 117001);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_EQ(tablet_schema()->schema_version(), 0);
    const std::vector<std::string> expected {R"({"p":1,"z":0})", R"({"p":2,"z":1})",
                                             R"({"p":3,"z":2})"};
    auto first = write(0, {expected[0], expected[1]});
    ASSERT_TRUE(first.has_value()) << first.error();
    auto second = write(1, {expected[2]});
    ASSERT_TRUE(second.has_value()) << second.error();
    ScopedDebugPoint failure(kPlanningFailure);
    auto output = compact({first.value(), second.value()}, false, true, false);
    ASSERT_TRUE(output.has_value()) << output.error();
    EXPECT_EQ(failure.execute_num(), 0);
    expect_p_type(output.value(), TYPE_INT);
    expect_values({output.value()}, expected);
}

// Separate column groups off + ADD history: a planning failure must fail compaction rather than
// fall back to full-root shredding that would re-cast old dynamic values (D1).
TEST_F(IndexStorageVariantTemplatePreservationTest, VerticalOffAlteredPlanningFailureFails) {
    initialize(variant_with_template(std::nullopt), 117002);
    ASSERT_FALSE(HasFatalFailure());
    auto old = write(0, {R"({"p":"001"})", R"({"p":"bad"})"});
    ASSERT_TRUE(old.has_value()) << old.error();
    install_write_schema(target_schema(variant_with_template(FieldType::OLAP_FIELD_TYPE_INT)));
    auto recent = write(1, {R"({"p":7})"});
    ASSERT_TRUE(recent.has_value()) << recent.error();
    const std::vector<std::string> expected {R"({"p":"001"})", R"({"p":"bad"})", R"({"p":7})"};
    expect_values({old.value(), recent.value()}, expected);
    const std::string old_meta = old.value()->rowset_meta()->get_rowset_pb().SerializeAsString();
    const std::string recent_meta =
            recent.value()->rowset_meta()->get_rowset_pb().SerializeAsString();
    {
        ScopedDebugPoint failure(kPlanningFailure);
        auto output = compact({old.value(), recent.value()}, false, true, false);
        ASSERT_FALSE(output.has_value());
        EXPECT_NE(output.error().to_string().find("injected variant carrier planning failure"),
                  std::string::npos)
                << output.error();
        EXPECT_EQ(failure.execute_num(), 1);
    }
    EXPECT_EQ(old.value()->rowset_meta()->get_rowset_pb().SerializeAsString(), old_meta);
    EXPECT_EQ(recent.value()->rowset_meta()->get_rowset_pb().SerializeAsString(), recent_meta);
    expect_values({old.value(), recent.value()}, expected);
    // Without the injected failure the same inputs compact through the ADD carrier.
    auto output = compact({old.value(), recent.value()}, false, true, false);
    ASSERT_TRUE(output.has_value()) << output.error();
    expect_p_type(output.value(), TYPE_JSONB);
    expect_values({output.value()}, expected);
}

// A rowset schema persisted without pattern_type (field 26) decodes as MATCH_NAME_GLOB. The same
// metacharacter-free entry is unchanged, so it must not enter the ADD-only sparse key scan.
TEST_F(IndexStorageVariantTemplatePreservationTest, LegacySchemaWithoutPatternTypeSkipsKeyScan) {
    initialize(variant_with_template(FieldType::OLAP_FIELD_TYPE_INT), 117003);
    ASSERT_FALSE(HasFatalFailure());
    auto old = write(0, {R"({"p":1,"z":0})", R"({"p":2,"z":1})"});
    ASSERT_TRUE(old.has_value()) << old.error();
    TabletSchemaPB legacy_pb;
    old.value()->tablet_schema()->to_schema_pb(&legacy_pb);
    int cleared = 0;
    for (auto& column : *legacy_pb.mutable_column()) {
        for (auto& child : *column.mutable_children_columns()) {
            child.clear_pattern_type();
            ++cleared;
        }
    }
    ASSERT_EQ(cleared, 1);
    auto legacy = std::make_shared<TabletSchema>();
    legacy->init_from_pb(legacy_pb);
    const auto& legacy_templates = legacy->column_by_uid(kVariantUid).get_sub_columns();
    ASSERT_EQ(legacy_templates.size(), 1);
    ASSERT_EQ(legacy_templates.front()->pattern_type(), PatternTypePB::MATCH_NAME_GLOB);
    old.value()->rowset_meta()->set_tablet_schema(legacy);
    ScopedDebugPoint key_scan(kAddedTemplateKeyScan);
    expect_schema({old.value()},
                  target_schema(variant_with_template(FieldType::OLAP_FIELD_TYPE_INT)),
                  FieldType::OLAP_FIELD_TYPE_INT);
    EXPECT_EQ(key_scan.execute_num(), 0);
}

INSTANTIATE_TEST_SUITE_P(IndexStorageLocalCumulativeAndBase, IndexStorageVariantTemplateRewriteTest,
                         testing::Combine(testing::Bool(), testing::Bool()),
                         [](const testing::TestParamInfo<std::tuple<bool, bool>>& info) {
                             return std::string(std::get<0>(info.param) ? "Base" : "Cumulative") +
                                    (std::get<1>(info.param) ? "Vertical" : "Horizontal");
                         });

} // namespace doris::index_storage_test
