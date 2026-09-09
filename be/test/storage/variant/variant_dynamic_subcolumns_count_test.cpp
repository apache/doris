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

#include <map>
#include <random>
#include <set>
#include <sstream>
#include <tuple>

#include "common/config.h"
#include "core/data_type/data_type_variant_v2.h"
#include "cpp/sync_point.h"
#include "exprs/vcast_expr.h"
#include "exprs/vectorized_fn_call.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "storage/compaction/cumulative_compaction.h"
#include "storage/segment/segment_loader.h"
#include "storage/variant/index_storage_variant_test_base.h"
#include "testutil/mock/mock_slot_ref.h"
#include "util/defer_op.h"

namespace doris::index_storage_test {

// No template changes: changing the physical path budget must preserve every JSON value.
// The fixture uses real writers/readers and compaction, not synthetic rowset schema rewrites.
class VariantDynamicSubcolumnsCountTest
        : public IndexStorageTestFixture,
          public testing::WithParamInterface<std::tuple<int, int, bool, int>> {
protected:
    void SetUp() override {
        IndexStorageTestFixture::SetUp();
        auto* sync = SyncPoint::get_instance();
        _sync_was_enabled = sync->get_enable();
        // Only replace the FE RPC. The actual compaction still selects and applies the policy.
        sync->set_call_back("Compaction::fetch_latest_tablet_schema", [this](auto&& args) {
            *try_any_cast<TabletSchemaSPtr*>(args[0]) = tablet()->tablet_schema();
            auto* result = try_any_cast_ret<Status>(args);
            result->first = Status::OK();
            result->second = true;
        });
        sync->enable_processing();
    }

    void TearDown() override {
        auto* sync = SyncPoint::get_instance();
        sync->clear_call_back("Compaction::fetch_latest_tablet_schema");
        if (!_sync_was_enabled) {
            sync->disable_processing();
        }
        IndexStorageTestFixture::TearDown();
    }

    bool _sync_was_enabled = false;

    // seed, sparse statistics budget, external metadata, compaction mode
    int seed() const { return std::get<0>(GetParam()); }
    int stats_limit() const { return std::get<1>(GetParam()); }
    bool external_meta() const { return std::get<2>(GetParam()); }
    int mode() const { return std::get<3>(GetParam()); }

    static void count_paths(const rapidjson::Value& value, const std::string& prefix,
                            std::map<std::string, int64_t>& counts) {
        if (value.IsObject()) {
            for (auto it = value.MemberBegin(); it != value.MemberEnd(); ++it) {
                const std::string key(it->name.GetString(), it->name.GetStringLength());
                count_paths(it->value, prefix.empty() ? key : prefix + "." + key, counts);
            }
        } else if (!value.IsNull()) {
            ++counts[prefix];
        }
    }

    void set_count(IndexTabletOptions& options, int count) {
        // Repeating the same setting is a control operation, not an artificial schema change.
        if (options.variant_columns[0].max_subcolumns_count == count) {
            return;
        }
        options.variant_columns[0].max_subcolumns_count = count;
        auto next = build_tablet_schema(options);
        next->set_schema_version(tablet_schema()->schema_version() + 1);
        // UTs compile with -fno-access-control. Replace only the fixture's writer schema;
        // never mutate a schema already owned by an input rowset.
        _tablet_schema = next;
        tablet()->update_max_version_schema(next);
    }

    void check_values(const std::vector<RowsetSharedPtr>& rowsets,
                      const std::map<int, std::string>& expected) {
        const auto read_schema = tablet_schema();
        DEFER(_tablet_schema = read_schema);
        for (const auto& rowset : rowsets) {
            SegmentLoader::instance()->erase_segments(*rowset->rowset_meta());
        }
        auto reopened = reload_rowsets(rowsets);
        ASSERT_TRUE(reopened.has_value()) << reopened.error();
        IndexReadOptions read_options;
        read_options.collect_variant_values = true;
        auto result = read_rowsets(reopened.value(), read_options);
        ASSERT_TRUE(result.has_value()) << result.error();
        ASSERT_EQ(result->rows_read, expected.size());
        ASSERT_TRUE(result->variant_values_by_uid.contains(2));
        const auto& values = result->variant_values_by_uid.at(2);
        ASSERT_EQ(values.size(), expected.size());
        std::set<int> seen;
        for (const auto& value : values) {
            ASSERT_TRUE(value.has_value());
            rapidjson::Document actual;
            actual.Parse(value->c_str());
            ASSERT_FALSE(actual.HasParseError()) << *value;
            ASSERT_TRUE(actual.IsObject()) << *value;
            ASSERT_TRUE(actual.HasMember("row")) << *value;
            ASSERT_TRUE(actual["row"].IsInt()) << *value;
            const int id = actual["row"].GetInt();
            ASSERT_TRUE(seen.insert(id).second) << "duplicate row " << id;
            ASSERT_TRUE(expected.contains(id)) << "unexpected row " << id;
            rapidjson::Document reference;
            reference.Parse(expected.at(id).c_str());
            ASSERT_FALSE(reference.HasParseError());
            EXPECT_TRUE(actual == reference)
                    << "row=" << id << " expected=" << expected.at(id) << " actual=" << *value;
        }
    }

    static VExprContextSPtr string_equality_context(const std::string& value) {
        TFunctionName name;
        name.__set_function_name("eq");
        TFunction fn;
        fn.__set_name(name);
        fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
        fn.__set_arg_types({create_type_desc(PrimitiveType::TYPE_STRING),
                            create_type_desc(PrimitiveType::TYPE_STRING)});
        fn.__set_ret_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
        fn.__set_has_var_args(false);
        TExprNode node;
        node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
        node.__set_node_type(TExprNodeType::FUNCTION_CALL);
        node.__set_fn(fn);
        node.__set_num_children(2);
        node.__set_is_nullable(true);
        auto equality = VectorizedFnCall::create_shared(node);
        TExprNode cast_node;
        cast_node.__set_type(create_type_desc(PrimitiveType::TYPE_STRING));
        cast_node.__set_node_type(TExprNodeType::CAST_EXPR);
        cast_node.__set_num_children(1);
        cast_node.__set_is_nullable(true);
        auto cast = VCastExpr::create_shared(cast_node);
        cast->add_child(std::make_shared<MockSlotRef>(
                1, make_nullable(std::make_shared<DataTypeVariantV2>())));
        equality->add_child(cast);
        TExprNode literal;
        literal.__set_node_type(TExprNodeType::STRING_LITERAL);
        literal.__set_type(create_type_desc(PrimitiveType::TYPE_STRING));
        literal.__set_num_children(0);
        literal.__set_is_nullable(false);
        TStringLiteral text;
        text.__set_value(value);
        literal.__set_string_literal(text);
        equality->add_child(VLiteral::create_shared(literal));
        return VExprContext::create_shared(equality);
    }

    // GTest assertions expand to branches; keep the projection and its oracle together.
    // NOLINTNEXTLINE(readability-function-cognitive-complexity)
    void check_path(const std::vector<RowsetSharedPtr>& rowsets, const std::string& path,
                    const std::vector<std::optional<std::string>>& expected,
                    const std::optional<std::string>& predicate = std::nullopt) {
        const auto saved = tablet_schema();
        DEFER(_tablet_schema = saved);
        for (const auto& rowset : rowsets) {
            SegmentLoader::instance()->erase_segments(*rowset->rowset_meta());
        }
        auto schema = build_schema_with_variant_path_column(*saved, 2, "v." + path,
                                                            FieldType::OLAP_FIELD_TYPE_VARIANT);
        // Supply a query projection without changing tablet or input-rowset metadata.
        _tablet_schema = std::move(schema);
        const int cid = column_id_by_path("v." + path);
        ASSERT_GE(cid, 0);
        IndexReadOptions options;
        options.need_ordered_result = true;
        options.return_columns = {0, static_cast<uint32_t>(cid)};
        options.collect_variant_values = true;
        if (predicate.has_value()) {
            // Sparse paths can skip ColumnPredicate pushdown. Use an executable expression,
            // following index_storage_variant_expr_pushdown_test.cpp.
            options.common_expr_ctxs_push_down.push_back(string_equality_context(*predicate));
            // Retain row identity alongside the filtered path, not just its row count.
            options.return_columns.push_back(column_id_by_path("v"));
        }
        auto result = read_rowsets(rowsets, options);
        ASSERT_TRUE(result.has_value()) << result.error();
        ASSERT_TRUE(result->variant_values_by_uid.contains(-1));
        EXPECT_EQ(canonical_variant_values(result->variant_values_by_uid.at(-1)),
                  canonical_variant_values(expected));
        if (predicate.has_value()) {
            ASSERT_TRUE(result->variant_values_by_uid.contains(2));
            const auto& roots = result->variant_values_by_uid.at(2);
            ASSERT_EQ(roots.size(), 1);
            ASSERT_TRUE(roots.front().has_value());
            rapidjson::Document root;
            root.Parse(roots.front()->c_str());
            ASSERT_TRUE(root.IsObject());
            ASSERT_TRUE(root.HasMember("row"));
            EXPECT_EQ(root["row"].GetInt(), 1);
        }
    }

    // Expected counts come from the original JSON, independently of the storage statistics.
    // Rows have monotonically increasing primary keys; segment order partitions this map.
    void check_stats(const RowsetSharedPtr& rowset, const std::map<int, std::string>& expected,
                     int count, bool fresh_write) {
        EXPECT_EQ(rowset->tablet_schema()->column_by_uid(2).variant_max_subcolumns_count(), count);
        auto probe = probe_rowset(rowset);
        ASSERT_TRUE(probe.has_value()) << probe.error();
        ASSERT_EQ(probe->num_rows, expected.size());
        ASSERT_FALSE(probe->segments.empty());
        auto next = expected.begin();
        for (const auto& segment : probe->segments) {
            SCOPED_TRACE(testing::Message() << "rowset=" << rowset->rowset_id().to_string()
                                            << " segment=" << segment.segment_id);
            std::map<std::string, int64_t> reference;
            for (int64_t row = 0; row < segment.num_rows; ++row, ++next) {
                ASSERT_NE(next, expected.end());
                rapidjson::Document doc;
                doc.Parse(next->second.c_str());
                ASSERT_FALSE(doc.HasParseError());
                count_paths(doc, "", reference);
            }
            std::map<std::string, int64_t> materialized;
            std::map<std::string, int64_t> sparse;
            for (const auto& column : segment.variant_columns) {
                if (column.parent_unique_id != 2) {
                    continue;
                }
                if (column.is_sparse_column) {
                    EXPECT_LE(column.sparse_non_null_size.size(), stats_limit());
                    for (const auto& [path, size] : column.sparse_non_null_size) {
                        EXPECT_TRUE(sparse.emplace(path, size).second) << path;
                    }
                } else if (!column.is_doc_value_column && !column.relative_path.empty()) {
                    EXPECT_TRUE(materialized.emplace(column.relative_path, column.none_null_size)
                                        .second)
                            << column.relative_path;
                }
            }
            for (const auto& [path, size] : materialized) {
                // Compaction may retain an output-schema column that is all NULL in this segment.
                const auto it = reference.find(path);
                EXPECT_EQ(size, it == reference.end() ? 0 : it->second) << path;
                EXPECT_FALSE(sparse.contains(path)) << "path in both physical locations=" << path;
            }
            for (const auto& [path, size] : sparse) {
                ASSERT_TRUE(reference.contains(path)) << "unexpected sparse path=" << path;
                EXPECT_EQ(size, reference.at(path)) << path;
            }
            if (count > 0) {
                EXPECT_LE(materialized.size(), count);
            }
            // A fresh writer selects per segment. Compaction selects across all input segments,
            // so its physical column count need not equal this segment's non-null path count.
            if (fresh_write) {
                EXPECT_EQ(materialized.size(), count == 0
                                                       ? reference.size()
                                                       : std::min(reference.size(), size_t(count)));
                EXPECT_EQ(sparse.size(),
                          std::min(reference.size() - materialized.size(), size_t(stats_limit())));
            }
            if (stats_limit() == 64) {
                auto combined = materialized;
                combined.insert(sparse.begin(), sparse.end());
                std::erase_if(combined, [](const auto& entry) { return entry.second == 0; });
                EXPECT_EQ(combined, reference);
                if (count == 0) {
                    EXPECT_TRUE(sparse.empty());
                }
            }
        }
        EXPECT_EQ(next, expected.end());
    }

    // One reproducible state-machine trace owns the writes, selected versions and oracle.
    // NOLINTNEXTLINE(readability-function-cognitive-complexity,readability-function-size)
    void run_sequence(const std::vector<int>& counts, uint32_t random_seed, bool old_only) {
        std::mt19937 random(random_seed);
        const bool was_vertical = config::enable_vertical_compaction;
        const bool was_subcolumns = config::enable_vertical_compact_variant_subcolumns;
        const bool was_ordered = config::enable_ordered_data_compaction;
        DEFER({
            config::enable_vertical_compaction = was_vertical;
            config::enable_vertical_compact_variant_subcolumns = was_subcolumns;
            config::enable_ordered_data_compaction = was_ordered;
        });
        config::enable_vertical_compaction = mode() != 0;
        config::enable_vertical_compact_variant_subcolumns = mode() == 2;
        config::enable_ordered_data_compaction = false;
        IndexTabletOptions options;
        options.tablet_id = 119800 + random_seed % 100;
        options.external_segment_meta = external_meta();
        VariantColumnSpec variant;
        variant.max_subcolumns_count = counts.front();
        variant.max_sparse_column_statistics_size = stats_limit();
        options.variant_columns = {variant};
        ASSERT_TRUE(create_tablet(options).ok());
        std::vector<RowsetSharedPtr> active;
        std::map<int, std::string> expected;
        std::map<std::string, std::map<int, std::string>> contents;
        int64_t version = 0;
        int next_id = 0;
        std::ostringstream history;
        for (size_t step = 0; step < counts.size(); ++step) {
            history << counts[step] << ',';
            SCOPED_TRACE(testing::Message()
                         << "seed=" << random_seed << " external_meta=" << external_meta()
                         << " stats_limit=" << stats_limit() << " mode=" << mode()
                         << " old_only=" << old_only << " counts=" << history.str());
            set_count(options, counts[step]);
            const auto target = tablet_schema();
            // First build two old rowsets. Later old-only steps change the budget without a load.
            if (!old_only || step == 0) {
                for (int batch = 0, batches = 2 + random() % 3; batch < batches; ++batch) {
                    IndexRowsetSpec spec;
                    spec.version = version++;
                    spec.max_rows_per_segment = 3;
                    std::vector<std::string> rows;
                    std::map<int, std::string> written_values;
                    const int first_id = next_id;
                    for (int row = 0, rows_in_batch = 4 + random() % 6; row < rows_in_batch;
                         ++row) {
                        const int id = next_id++;
                        // Rotating rare paths exceed the statistics budget and change hotness.
                        std::string json = "{\"row\":" + std::to_string(id) +
                                           ",\"a\":" + std::to_string(random() % 1000) +
                                           R"(,"b":"001","c":true,"d":1.25,"rare_)" +
                                           std::to_string(random() % 19) + "\":\"r" +
                                           std::to_string(id) + R"(","nested":{"leaf":)" +
                                           std::to_string(id) + R"(},"arr":[1,2,3]})";
                        if (row % 4 != 0) {
                            json.pop_back();
                            json += ",\"heat_" + std::to_string(step % 3) + R"(":"hot"})";
                        }
                        written_values.emplace(id, json);
                        expected.emplace(id, json);
                        rows.push_back(std::move(json));
                    }
                    const size_t split = rows.size() / 2;
                    spec.batches.push_back(IndexBatch::single_variant(
                            std::vector<std::string>(rows.begin(), rows.begin() + split),
                            first_id));
                    spec.batches.push_back(IndexBatch::single_variant(
                            std::vector<std::string>(rows.begin() + split, rows.end()),
                            first_id + split));
                    auto written = write_rowset(spec);
                    ASSERT_TRUE(written.has_value()) << written.error();
                    active.push_back(written.value());
                    contents.emplace(written.value()->rowset_id().to_string(), written_values);
                    history << "write(v=" << spec.version << ",rows=" << written_values.size()
                            << ");";
                    ASSERT_NO_FATAL_FAILURE(
                            check_stats(written.value(), written_values, counts[step], true));
                }
            }
            ASSERT_NO_FATAL_FAILURE(check_values(active, expected));
            // Reading reopens input schemas in the fixture. Restore only the target writer schema.
            _tablet_schema = target;
            // Random contiguous version ranges, occasionally all inputs with FULL compaction.
            const bool full = step % 4 == 3;
            const size_t start = full || active.size() <= 2 ? 0 : random() % (active.size() - 1);
            const size_t length = full || active.size() <= 2
                                          ? active.size()
                                          : 2 + random() % (active.size() - start - 1);
            std::vector<RowsetSharedPtr> inputs(active.begin() + start,
                                                active.begin() + start + length);
            std::map<int, std::string> compacted_values;
            for (const auto& input : inputs) {
                const auto& values = contents.at(input->rowset_id().to_string());
                compacted_values.insert(values.begin(), values.end());
                history << "input(" << input->start_version() << '-' << input->end_version()
                        << ");";
            }
            SCOPED_TRACE(history.str());
            auto compacted = compact_rowsets_and_reload(
                    full ? IndexCompactionKind::FULL : IndexCompactionKind::CUMULATIVE, inputs);
            ASSERT_TRUE(compacted.has_value()) << compacted.error();
            EXPECT_EQ(compacted.value()->tablet_schema()->schema_version(),
                      target->schema_version());
            ASSERT_NO_FATAL_FAILURE(
                    check_stats(compacted.value(), compacted_values, counts[step], false));
            contents.emplace(compacted.value()->rowset_id().to_string(), compacted_values);
            for (const auto& input : inputs) {
                contents.erase(input->rowset_id().to_string());
            }
            active.erase(active.begin() + start, active.begin() + start + length);
            active.insert(active.begin() + start, compacted.value());
            ASSERT_NO_FATAL_FAILURE(check_values(active, expected));
            _tablet_schema = target;
        }
    }
};

TEST_P(VariantDynamicSubcolumnsCountTest, UnchangedCountControl) {
    run_sequence({1, 1, 1, 1}, seed(), false);
}

// Unequal frequencies make the expected materialized paths explicit, not a copy of
// the production top-N selection algorithm. Covers stats below/at/above the cap.
TEST_P(VariantDynamicSubcolumnsCountTest, ExactFreshWriteLayoutAndStatistics) {
    IndexTabletOptions options;
    options.tablet_id = 119910;
    options.external_segment_meta = external_meta();
    VariantColumnSpec variant;
    variant.max_sparse_column_statistics_size = stats_limit();
    options.variant_columns = {variant};
    ASSERT_TRUE(create_tablet(options).ok());
    int64_t version = 0;
    for (int count : {1, 2, 3, 0}) {
        SCOPED_TRACE(testing::Message() << "count=" << count << " stats=" << stats_limit());
        set_count(options, count);
        IndexRowsetSpec spec;
        spec.version = version++;
        const std::vector<std::string> rows = {
                R"({"row":0,"z":1,"m":2,"a":3})", R"({"row":1,"z":1,"m":2,"a":3})",
                R"({"row":2,"z":1,"m":2})", R"({"row":3,"z":1,"m":2})", R"({"row":4,"z":1})"};
        spec.batches.push_back(IndexBatch::single_variant(rows));
        auto written = write_rowset(spec);
        ASSERT_TRUE(written.has_value()) << written.error();
        std::map<int, std::string> expected;
        for (size_t i = 0; i < rows.size(); ++i) {
            expected.emplace(i, rows[i]);
        }
        ASSERT_NO_FATAL_FAILURE(check_values({written.value()}, expected));
        ASSERT_NO_FATAL_FAILURE(check_stats(written.value(), expected, count, true));
        auto probe = probe_rowset(written.value());
        ASSERT_TRUE(probe.has_value()) << probe.error();
        ASSERT_EQ(probe->segments.size(), 1);
        std::set<std::string> actual_paths;
        for (const auto& column : probe->segments.front().variant_columns) {
            if (column.parent_unique_id == 2 && !column.is_sparse_column &&
                !column.is_doc_value_column && !column.relative_path.empty()) {
                actual_paths.insert(column.relative_path);
            }
        }
        std::set<std::string> expected_paths = {"z"};
        if (count == 0 || count >= 2) {
            expected_paths.insert("row");
        }
        if (count == 0 || count >= 3) {
            expected_paths.insert("m");
        }
        if (count == 0) {
            expected_paths.insert("a");
        }
        EXPECT_EQ(actual_paths, expected_paths);
    }
}

TEST_P(VariantDynamicSubcolumnsCountTest, CountTransitionsWithWrites) {
    run_sequence({0, 1, 3, 1, 0, 12, 2}, seed(), false);
}

TEST_P(VariantDynamicSubcolumnsCountTest, CountTransitionsWithoutNewWrites) {
    run_sequence({3, 1, 0, 2, 12, 1}, seed(), true);
}

TEST_P(VariantDynamicSubcolumnsCountTest, SeededRandomCountTransitions) {
    std::mt19937 random(seed());
    std::vector<int> counts = {1, 0};
    for (int step = 0; step < 14; ++step) {
        counts.push_back(random() % 13);
    }
    run_sequence(counts, seed(), false);
}

// Minimal data-loss reproducer: `lost` is present only in an old sparse column.
// A new unlimited rowset does not provide its physical type to compaction.
// Keep setup and before/after assertions in one regression; GTest inflates branch counts.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_P(VariantDynamicSubcolumnsCountTest, SparseOnlyOldPathSurvivesUnlimitedCompaction) {
    const bool was_vertical = config::enable_vertical_compaction;
    const bool was_subcolumns = config::enable_vertical_compact_variant_subcolumns;
    const bool was_ordered = config::enable_ordered_data_compaction;
    DEFER({
        config::enable_vertical_compaction = was_vertical;
        config::enable_vertical_compact_variant_subcolumns = was_subcolumns;
        config::enable_ordered_data_compaction = was_ordered;
    });
    config::enable_vertical_compaction = mode() != 0;
    config::enable_vertical_compact_variant_subcolumns = mode() == 2;
    config::enable_ordered_data_compaction = false;
    IndexTabletOptions options;
    options.tablet_id = 119911;
    options.external_segment_meta = external_meta();
    VariantColumnSpec variant;
    variant.max_subcolumns_count = 1;
    variant.max_sparse_column_statistics_size = stats_limit();
    options.variant_columns = {variant};
    ASSERT_TRUE(create_tablet(options).ok());
    IndexRowsetSpec old_spec;
    old_spec.version = 0;
    old_spec.batches.push_back(IndexBatch::single_variant({R"({"row":0,"lost":"old"})"}, 0));
    auto old_rowset = write_rowset(old_spec);
    ASSERT_TRUE(old_rowset.has_value()) << old_rowset.error();
    auto old_probe = probe_rowset(old_rowset.value());
    ASSERT_TRUE(old_probe.has_value()) << old_probe.error();
    EXPECT_TRUE(has_sparse_path_stat(old_probe.value(), "lost"));
    EXPECT_FALSE(has_variant_layout(old_probe.value(), 2, "lost"));
    set_count(options, 0);
    IndexRowsetSpec new_spec;
    new_spec.version = 1;
    new_spec.batches.push_back(IndexBatch::single_variant({R"({"row":1,"fresh":7})"}, 1));
    auto new_rowset = write_rowset(new_spec);
    ASSERT_TRUE(new_rowset.has_value()) << new_rowset.error();
    const std::map<int, std::string> expected = {{0, R"({"row":0,"lost":"old"})"},
                                                 {1, R"({"row":1,"fresh":7})"}};
    const std::vector<RowsetSharedPtr> inputs = {old_rowset.value(), new_rowset.value()};
    ASSERT_NO_FATAL_FAILURE(check_values(inputs, expected));
    auto compacted = compact_rowsets_and_reload(IndexCompactionKind::CUMULATIVE, inputs);
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NO_FATAL_FAILURE(check_stats(compacted.value(), expected, 0, false));
    ASSERT_NO_FATAL_FAILURE(check_values({compacted.value()}, expected));
}

TEST_P(VariantDynamicSubcolumnsCountTest, OrderedControlThenExactCompactionLayout) {
    const auto was_ordered = config::enable_ordered_data_compaction;
    const auto min_size = config::ordered_data_compaction_min_segment_size;
    const auto was_vertical = config::enable_vertical_compaction;
    const auto was_subcolumns = config::enable_vertical_compact_variant_subcolumns;
    DEFER({
        config::enable_ordered_data_compaction = was_ordered;
        config::ordered_data_compaction_min_segment_size = min_size;
        config::enable_vertical_compaction = was_vertical;
        config::enable_vertical_compact_variant_subcolumns = was_subcolumns;
    });
    config::enable_ordered_data_compaction = true;
    config::ordered_data_compaction_min_segment_size = 0;
    config::enable_vertical_compaction = mode() != 0;
    config::enable_vertical_compact_variant_subcolumns = mode() == 2;
    IndexTabletOptions options;
    options.tablet_id = 119912;
    options.external_segment_meta = external_meta();
    VariantColumnSpec variant;
    variant.max_subcolumns_count = 1;
    variant.max_sparse_column_statistics_size = stats_limit();
    options.variant_columns = {variant};
    ASSERT_TRUE(create_tablet(options).ok());
    std::vector<RowsetSharedPtr> inputs;
    std::map<int, std::string> expected;
    for (int version = 0; version < 2; ++version) {
        IndexRowsetSpec spec;
        spec.version = version;
        std::vector<std::string> rows;
        for (int row = 0; row < 5; ++row) {
            const int id = version * 5 + row;
            std::string json = "{\"row\":" + std::to_string(id) + ",\"z\":1";
            if (row < 4) {
                json += ",\"m\":2";
            }
            if (row < 2) {
                json += ",\"a\":3";
            }
            json += "}";
            rows.push_back(json);
            expected.emplace(id, json);
        }
        spec.batches.push_back(IndexBatch::single_variant(rows, version * 5));
        auto written = write_rowset(spec);
        ASSERT_TRUE(written.has_value()) << written.error();
        inputs.push_back(written.value());
    }
    // Identical old schemas and disjoint keys really qualify for the link shortcut.
    // Afterwards every setting change must rewrite even with ordered compaction enabled.
    for (int count : {1, 3, 2, 0, 1}) {
        SCOPED_TRACE(testing::Message() << "count=" << count << " mode=" << mode());
        const bool changed = options.variant_columns[0].max_subcolumns_count != count;
        set_count(options, count);
        CumulativeCompaction compaction(*storage_engine(), tablet());
        compaction._input_rowsets = inputs;
        ASSERT_TRUE(compaction.CompactionMixin::execute_compact().ok());
        EXPECT_EQ(compaction._is_ordered_data_compaction, !changed);
        auto output = compaction._output_rowset;
        ASSERT_NE(output, nullptr);
        ASSERT_NO_FATAL_FAILURE(check_values({output}, expected));
        ASSERT_NO_FATAL_FAILURE(check_stats(output, expected, count, false));
        auto probe = probe_rowset(output);
        ASSERT_TRUE(probe.has_value()) << probe.error();
        // A rewrite of these ten rows fits one segment; linked control retains two.
        ASSERT_EQ(probe->segments.size(), changed ? 1 : 2);
        std::set<std::string> paths = {"z"};
        if (count == 0 || count >= 2) {
            paths.insert("row");
        }
        if (count == 0 || count >= 3) {
            paths.insert("m");
        }
        if (count == 0) {
            paths.insert("a");
        }
        for (const auto& segment : probe->segments) {
            std::set<std::string> actual;
            size_t sparse_entries = 0;
            for (const auto& column : segment.variant_columns) {
                if (column.parent_unique_id != 2) {
                    continue;
                }
                if (column.is_sparse_column) {
                    sparse_entries += column.sparse_non_null_size.size();
                } else if (!column.is_doc_value_column && !column.relative_path.empty()) {
                    actual.insert(column.relative_path);
                }
            }
            EXPECT_EQ(actual, paths);
            EXPECT_EQ(sparse_entries, std::min(size_t(stats_limit()), 4 - paths.size()));
        }
        inputs = {output};
    }
}

// Keep setup and before/after assertions in one regression; GTest inflates branch counts.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_P(VariantDynamicSubcolumnsCountTest, MixedPathTypesAndProjectedReads) {
    const auto was_ordered = config::enable_ordered_data_compaction;
    const auto was_vertical = config::enable_vertical_compaction;
    const auto was_subcolumns = config::enable_vertical_compact_variant_subcolumns;
    DEFER({
        config::enable_ordered_data_compaction = was_ordered;
        config::enable_vertical_compaction = was_vertical;
        config::enable_vertical_compact_variant_subcolumns = was_subcolumns;
    });
    config::enable_ordered_data_compaction = false;
    config::enable_vertical_compaction = mode() != 0;
    config::enable_vertical_compact_variant_subcolumns = mode() == 2;
    IndexTabletOptions options;
    options.tablet_id = 119913;
    options.external_segment_meta = external_meta();
    VariantColumnSpec variant;
    variant.max_subcolumns_count = 1;
    variant.max_sparse_column_statistics_size = stats_limit();
    options.variant_columns = {variant};
    ASSERT_TRUE(create_tablet(options).ok());
    const std::vector<std::string> rows = {
            R"({"row":0,"x":7,"a":"other"})",       R"({"row":1,"x":"001","a":"match"})",
            R"({"row":2,"x":[1,"a"],"a":"other"})", R"({"row":3,"x":{"leaf":9},"a":"other"})",
            R"({"row":4,"x":null,"a":"other"})",    R"({"row":5,"a":"other"})",
            R"({"row":6,"x":{},"a":"other"})",      R"({"row":7,"x":[],"a":"other"})"};
    std::vector<RowsetSharedPtr> inputs;
    std::map<int, std::string> expected;
    for (size_t row = 0; row < rows.size(); ++row) {
        // Each type originates in its own segment and alternates physical/sparse storage.
        set_count(options, row % 2 == 0 ? 1 : 0);
        IndexRowsetSpec spec;
        spec.version = row;
        spec.batches.push_back(IndexBatch::single_variant({rows[row]}, row));
        auto written = write_rowset(spec);
        ASSERT_TRUE(written.has_value()) << written.error();
        inputs.push_back(written.value());
        expected.emplace(row, rows[row]);
    }
    const std::vector<std::optional<std::string>> tags = {R"("other")", R"("match")", R"("other")",
                                                          R"("other")", R"("other")", R"("other")",
                                                          R"("other")", R"("other")"};
    // Existing shredder semantics: null/object-with-no-leaves are skipped in visit();
    // isolated [] has Array(Nothing) and is skipped in prepare_logical_candidates().
    // These expectations are explicit, never derived from data read back from the writer.
    expected.at(4) = R"({"row":4,"a":"other"})";
    expected.at(6) = R"({"row":6,"a":"other"})";
    expected.at(7) = R"({"row":7,"a":"other"})";
    auto verify = [&] {
        ASSERT_NO_FATAL_FAILURE(check_values(inputs, expected));
        ASSERT_NO_FATAL_FAILURE(check_path(inputs, "a", tags));
        ASSERT_NO_FATAL_FAILURE(check_path(inputs, "a", {R"("match")"}, "match"));
    };
    ASSERT_NO_FATAL_FAILURE(verify());
    for (int count : {1, 0, 2, 1, 0}) {
        SCOPED_TRACE(testing::Message() << "count=" << count << " mode=" << mode());
        set_count(options, count);
        auto compacted = compact_rowsets_and_reload(IndexCompactionKind::CUMULATIVE, inputs);
        ASSERT_TRUE(compacted.has_value()) << compacted.error();
        EXPECT_EQ(
                compacted.value()->tablet_schema()->column_by_uid(2).variant_max_subcolumns_count(),
                count);
        if (count == 0 || count == 1) {
            auto probe = probe_rowset(compacted.value());
            ASSERT_TRUE(probe.has_value()) << probe.error();
            EXPECT_EQ(has_variant_layout(probe.value(), 2, "a"), count == 0);
        }
        inputs = {compacted.value()};
        ASSERT_NO_FATAL_FAILURE(verify());
    }
}

INSTANTIATE_TEST_SUITE_P(StorageMatrix, VariantDynamicSubcolumnsCountTest,
                         testing::Combine(testing::Values(17, 20260909, 8675309),
                                          testing::Values(1, 2, 64), testing::Bool(),
                                          testing::Values(0, 1, 2)));

} // namespace doris::index_storage_test
