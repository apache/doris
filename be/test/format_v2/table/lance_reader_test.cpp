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

#include "format_v2/table/lance_reader.h"

#include <arrow/array/util.h>
#include <arrow/builder.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>
#include <lance/lance.h>

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <lance/lance.hpp>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "common/consts.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_varbinary.h"
#include "exec/common/endian.h"
#include "exprs/vexpr.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "storage/utils.h"
#include "util/defer_op.h"
#include "util/timezone_utils.h"
#include "util/url_coding.h"

namespace doris::format::lance {
namespace {

using Columns = std::vector<ColumnDefinition>;

class FailingResidualPredicate final : public VExpr {
public:
    FailingResidualPredicate() : VExpr(std::make_shared<DataTypeUInt8>(), false) {}

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::InternalError("Lance reader evaluated a scanner residual predicate");
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const std::string _expr_name = "FailingResidualPredicate";
};

struct LanceFixtureInfo {
    int64_t version = 0;
    std::vector<int64_t> fragment_ids;
};

void expect_lance_profile_hierarchy(RuntimeProfile* profile,
                                    const std::vector<std::string>& metric_names) {
    TRuntimeProfileTree tree;
    profile->to_thrift(&tree, 3);
    ASSERT_FALSE(tree.nodes.empty());
    const auto& children = tree.nodes[0].child_counters_map;
    ASSERT_TRUE(children.contains(RuntimeProfile::ROOT_COUNTER));
    EXPECT_TRUE(children.at(RuntimeProfile::ROOT_COUNTER).contains("FileScannerV2"));
    ASSERT_TRUE(children.contains("FileScannerV2"));
    EXPECT_TRUE(children.at("FileScannerV2").contains("TableReader"));
    ASSERT_TRUE(children.contains("TableReader"));
    EXPECT_TRUE(children.at("TableReader").contains("LanceReader"));
    ASSERT_TRUE(children.contains("LanceReader"));
    for (const auto& metric_name : metric_names) {
        EXPECT_TRUE(children.at("LanceReader").contains(metric_name)) << metric_name;
    }
}

Status get_fixture_info(const std::filesystem::path& dataset_uri, LanceFixtureInfo* info) {
    std::unique_ptr<LanceDataset, decltype(&lance_dataset_close)> dataset(
            lance_dataset_open(dataset_uri.c_str(), nullptr, 0), lance_dataset_close);
    if (dataset == nullptr) {
        return Status::InternalError("Failed to open Lance fixture: {}", dataset_uri.string());
    }

    info->version = static_cast<int64_t>(lance_dataset_version(dataset.get()));
    const auto fragment_count = lance_dataset_fragment_count(dataset.get());
    std::vector<uint64_t> raw_fragment_ids(fragment_count);
    if (lance_dataset_fragment_ids(dataset.get(), raw_fragment_ids.data()) != 0) {
        return Status::InternalError("Failed to list Lance fragments from {}",
                                     dataset_uri.string());
    }
    info->fragment_ids.clear();
    info->fragment_ids.reserve(raw_fragment_ids.size());
    for (const auto fragment_id : raw_fragment_ids) {
        info->fragment_ids.emplace_back(static_cast<int64_t>(fragment_id));
    }
    return Status::OK();
}

TFileRangeDesc make_lance_range(const std::filesystem::path& dataset_uri, int64_t version,
                                std::vector<int64_t> fragment_ids) {
    TFileRangeDesc range;
    range.__set_format_type(TFileFormatType::FORMAT_LANCE);
    TLanceFileDesc lance_params;
    lance_params.__set_dataset_uri(dataset_uri.string());
    lance_params.__set_version(version);
    lance_params.__set_fragment_ids(std::move(fragment_ids));
    TTableFormatFileDesc table_params;
    table_params.__set_table_format_type("lance");
    table_params.__set_lance_params(std::move(lance_params));
    range.__set_table_format_params(std::move(table_params));
    return range;
}

TFileRangeDesc make_latest_lance_range(const std::filesystem::path& dataset_uri) {
    auto range = make_lance_range(dataset_uri, 0, {});
    auto& lance_params = range.table_format_params.lance_params;
    lance_params.fragment_ids.clear();
    lance_params.__isset.fragment_ids = false;
    return range;
}

ColumnDefinition projected_column(std::string name, DataTypePtr type) {
    return {
            .identifier = Field::create_field<TYPE_STRING>(name),
            .name = std::move(name),
            .type = std::move(type),
    };
}

ColumnDefinition projected_column(std::string name, PrimitiveType type, bool nullable) {
    return projected_column(std::move(name),
                            DataTypeFactory::instance().create_data_type(type, nullable));
}

void add_output_columns(Block* block, const Columns& columns) {
    for (const auto& column : columns) {
        block->insert({column.type->create_column(), column.type, column.name});
    }
}

Status init_reader(LanceTableReader* reader, const Columns& projected_columns,
                   RuntimeState* runtime_state, RuntimeProfile* profile,
                   TFileScanRangeParams* scan_params, VExprContextSPtrs conjuncts = {}) {
    return reader->init({
            .projected_columns = projected_columns,
            .conjuncts = std::move(conjuncts),
            .format = FileFormat::LANCE,
            .scan_params = scan_params,
            .io_ctx = nullptr,
            .runtime_state = runtime_state,
            .scanner_profile = profile,
    });
}

Status prepare_range(LanceTableReader* reader, TFileRangeDesc range,
                     std::optional<GlobalRowIdContext> global_rowid_context = std::nullopt) {
    // Assign after value initialization so adding optional split state cannot break this fixture's
    // designated initializer under -Wmissing-designated-field-initializers.
    SplitReadOptions options;
    options.all_runtime_filters_applied = true;
    options.current_range = std::move(range);
    options.current_split_format = FileFormat::LANCE;
    options.global_rowid_context = global_rowid_context;
    return reader->prepare_split(options);
}

Status prepare_fixture(LanceTableReader* reader, const std::filesystem::path& dataset_uri,
                       const LanceFixtureInfo& fixture, std::vector<int64_t> fragment_ids,
                       std::optional<GlobalRowIdContext> global_rowid_context = std::nullopt) {
    return prepare_range(reader,
                         make_lance_range(dataset_uri, fixture.version, std::move(fragment_ids)),
                         global_rowid_context);
}

TFileScanRangeParams make_float32_vector_search_params(
        const std::array<float, 3>& query_values, int64_t top_k, int64_t offset,
        std::optional<std::string> filter = std::nullopt) {
    std::string encoded_values(query_values.size() * sizeof(float), '\0');
    for (size_t i = 0; i < query_values.size(); ++i) {
        LittleEndian::Store32(encoded_values.data() + i * sizeof(float),
                              std::bit_cast<uint32_t>(query_values[i]));
    }

    TSearchVector query_vector;
    query_vector.__set_element_type(TVectorElementType::FLOAT32);
    query_vector.__set_dimension(static_cast<int32_t>(query_values.size()));
    query_vector.__set_values(std::move(encoded_values));

    TVectorSearchParams vector_params;
    vector_params.__set_column("embedding");
    vector_params.__set_query_vector(std::move(query_vector));
    vector_params.__set_top_k(top_k);
    vector_params.__set_offset(offset);
    vector_params.__set_metric(TVectorMetric::L2);

    TExternalSearchQuery query;
    query.__set_vector_search(std::move(vector_params));
    TExternalSearchRequest request;
    request.__set_schema_version(1);
    request.__set_search_query(std::move(query));
    TVectorSearchOptions vector_search_options;
    vector_search_options.__set_use_index(false);
    request.__set_vector_search_options(std::move(vector_search_options));
    if (filter.has_value()) {
        TSearchFilter search_filter;
        search_filter.__set_format(TSearchFilterFormat::SQL);
        search_filter.__set_payload(*filter);
        request.__set_search_filter(std::move(search_filter));
    }

    TLanceScanParams lance_scan_params;
    lance_scan_params.__set_external_search_request(std::move(request));
    TFileScanRangeParams scan_params;
    scan_params.__set_lance_scan_params(std::move(lance_scan_params));
    return scan_params;
}

TEST(LanceTableReaderVectorSearchTest, RejectsMalformedVectorPayloadBeforeReadingIt) {
    const Columns columns {
            projected_column("row_id", TYPE_BIGINT, false),
            projected_column("_distance", TYPE_FLOAT, true),
    };
    TQueryGlobals query_globals;
    RuntimeState state(query_globals);
    RuntimeProfile profile("lance_vector_search_invalid_request");
    auto scan_params = make_float32_vector_search_params({0.0F, 0.0F, 0.0F}, 2, 0);
    scan_params.lance_scan_params.external_search_request.search_query.vector_search.query_vector
            .__set_dimension(4);

    LanceTableReader reader;
    const auto status = init_reader(&reader, columns, &state, &profile, &scan_params);

    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("query vector byte size"), std::string::npos);
}

TEST(LanceTableReaderVectorSearchTest, RejectsMalformedIndexSegmentUuid) {
    const std::filesystem::path dataset_uri =
            "./be/test/format_v2/table/lance/data/all_types.lance";
    LanceFixtureInfo fixture;
    ASSERT_TRUE(get_fixture_info(dataset_uri, &fixture).ok());

    const Columns columns {
            projected_column("row_id", TYPE_BIGINT, false),
            projected_column("_distance", TYPE_FLOAT, true),
    };
    TQueryGlobals query_globals;
    RuntimeState state(query_globals);
    RuntimeProfile profile("lance_vector_search_invalid_index_segment_uuid");
    auto scan_params = make_float32_vector_search_params({0.0F, 0.0F, 0.0F}, 2, 0);

    LanceTableReader reader;
    ASSERT_TRUE(init_reader(&reader, columns, &state, &profile, &scan_params).ok());
    auto range = make_lance_range(dataset_uri, fixture.version, fixture.fragment_ids);
    range.table_format_params.lance_params.__set_index_segment_uuids({"too-short"});

    const auto status = prepare_range(&reader, std::move(range));

    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("must contain 16 bytes"), std::string::npos);
    EXPECT_TRUE(reader.close().ok());
}

std::vector<std::pair<int64_t, float>> read_vector_search_rows(LanceTableReader* reader,
                                                               Block* block) {
    std::vector<std::pair<int64_t, float>> rows;
    bool eos = false;
    while (!eos) {
        EXPECT_TRUE(reader->get_block(block, &eos).ok());
        if (eos) {
            continue;
        }
        const auto& row_ids = assert_cast<const ColumnInt64&>(*block->get_by_position(0).column);
        const auto& distances =
                assert_cast<const ColumnNullable&>(*block->get_by_position(1).column);
        const auto& distance_values =
                assert_cast<const ColumnFloat32&>(distances.get_nested_column());
        for (size_t row = 0; row < block->rows(); ++row) {
            EXPECT_EQ(0, distances.get_null_map_data()[row]);
            rows.emplace_back(row_ids.get_data()[row], distance_values.get_data()[row]);
        }
    }
    return rows;
}

GlobalRowLoacationV2 decode_lance_row_id(const ColumnString& column, size_t row) {
    const auto encoded = column.get_data_at(row);
    EXPECT_EQ(sizeof(GlobalRowLoacationV2), encoded.size);
    GlobalRowLoacationV2 location(ROW_VERSION::LANCE_DATASET_ROW_ID, 0, 0, 0);
    if (encoded.size == sizeof(location)) {
        std::memcpy(&location, encoded.data, sizeof(location));
    }
    return location;
}

TEST(LanceTableReaderVectorSearchTest, SearchesWholeSnapshotWithOffsetAndDistance) {
    const std::filesystem::path dataset_uri =
            "./be/test/format_v2/table/lance/data/all_types.lance";
    LanceFixtureInfo fixture;
    ASSERT_TRUE(get_fixture_info(dataset_uri, &fixture).ok());

    const Columns columns {
            projected_column("row_id", TYPE_BIGINT, false),
            projected_column("_distance", TYPE_FLOAT, true),
    };
    TQueryOptions query_options;
    query_options.__set_batch_size(1);
    TQueryGlobals query_globals;
    RuntimeState state(query_globals);
    state.set_query_options(query_options);
    RuntimeProfile profile("lance_vector_search_fixture");
    auto scan_params = make_float32_vector_search_params({0.0F, 0.0F, 0.0F}, 2, 1);
    auto& search_options =
            scan_params.lance_scan_params.external_search_request.vector_search_options;
    search_options.__set_nprobes(4);
    search_options.__set_refine_factor(2);
    search_options.__set_ef(16);

    LanceTableReader reader;
    ASSERT_TRUE(init_reader(&reader, columns, &state, &profile, &scan_params).ok());
    ASSERT_TRUE(prepare_fixture(&reader, dataset_uri, fixture, fixture.fragment_ids).ok());

    Block block;
    add_output_columns(&block, columns);
    const auto rows = read_vector_search_rows(&reader, &block);
    ASSERT_EQ(2U, rows.size());
    EXPECT_EQ(2, rows[0].first);
    EXPECT_FLOAT_EQ(1.0F, rows[0].second);
    EXPECT_EQ(4, rows[1].first);
    EXPECT_FLOAT_EQ(8.25F, rows[1].second);
    ASSERT_NE(profile.get_info_string("LanceTopK"), nullptr);
    EXPECT_EQ("2", *profile.get_info_string("LanceTopK"));
    ASSERT_NE(profile.get_info_string("LanceOffset"), nullptr);
    EXPECT_EQ("1", *profile.get_info_string("LanceOffset"));
    ASSERT_NE(profile.get_info_string("LanceTopKPlusOffset"), nullptr);
    EXPECT_EQ("3", *profile.get_info_string("LanceTopKPlusOffset"));
    EXPECT_TRUE(reader.close().ok());
}

TEST(LanceTableReaderVectorSearchTest, AppliesSearchFilterBeforeTopK) {
    const std::filesystem::path dataset_uri =
            "./be/test/format_v2/table/lance/data/all_types.lance";
    LanceFixtureInfo fixture;
    ASSERT_TRUE(get_fixture_info(dataset_uri, &fixture).ok());

    const Columns columns {
            projected_column("row_id", TYPE_BIGINT, false),
            projected_column("_distance", TYPE_FLOAT, true),
    };
    TQueryOptions query_options;
    query_options.__set_batch_size(2);
    TQueryGlobals query_globals;
    RuntimeState state(query_globals);
    state.set_query_options(query_options);
    RuntimeProfile profile("lance_vector_search_prefilter_fixture");
    auto scan_params = make_float32_vector_search_params({0.0F, 0.0F, 0.0F}, 1, 0, "row_id >= 3");

    LanceTableReader reader;
    ASSERT_TRUE(init_reader(&reader, columns, &state, &profile, &scan_params).ok());
    ASSERT_TRUE(prepare_fixture(&reader, dataset_uri, fixture, fixture.fragment_ids).ok());

    Block block;
    add_output_columns(&block, columns);
    const auto rows = read_vector_search_rows(&reader, &block);
    ASSERT_EQ(1U, rows.size());
    EXPECT_EQ(4, rows[0].first);
    EXPECT_FLOAT_EQ(8.25F, rows[0].second);
    EXPECT_TRUE(reader.close().ok());
}

TEST(LanceTableReaderVectorSearchTest, SearchesMultipleFragmentSplits) {
    const std::filesystem::path dataset_uri =
            "./be/test/format_v2/table/lance/data/all_types.lance";
    LanceFixtureInfo fixture;
    ASSERT_TRUE(get_fixture_info(dataset_uri, &fixture).ok());
    ASSERT_GT(fixture.fragment_ids.size(), 1U);

    const Columns columns {
            projected_column("row_id", TYPE_BIGINT, false),
            projected_column("_distance", TYPE_FLOAT, true),
    };
    TQueryOptions query_options;
    query_options.__set_batch_size(2);
    TQueryGlobals query_globals;
    RuntimeState state(query_globals);
    state.set_query_options(query_options);
    RuntimeProfile profile("lance_vector_search_fragment_splits_fixture");
    auto scan_params = make_float32_vector_search_params({0.0F, 0.0F, 0.0F}, 4, 0);

    LanceTableReader reader;
    ASSERT_TRUE(init_reader(&reader, columns, &state, &profile, &scan_params).ok());
    std::vector<int64_t> row_ids;
    for (const auto fragment_id : fixture.fragment_ids) {
        ASSERT_TRUE(prepare_fixture(&reader, dataset_uri, fixture, {fragment_id}).ok());
        Block block;
        add_output_columns(&block, columns);
        const auto rows = read_vector_search_rows(&reader, &block);
        for (const auto& row : rows) {
            row_ids.emplace_back(row.first);
        }
    }
    std::ranges::sort(row_ids);
    EXPECT_EQ((std::vector<int64_t> {1, 2, 3, 4}), row_ids);
    ASSERT_NE(profile.get_counter("LancePlannedIndexSegmentCount"), nullptr);
    EXPECT_EQ(0, profile.get_counter("LancePlannedIndexSegmentCount")->value());
    ASSERT_NE(profile.get_counter("LancePlannedIndexedFragmentCount"), nullptr);
    EXPECT_EQ(0, profile.get_counter("LancePlannedIndexedFragmentCount")->value());
    ASSERT_NE(profile.get_counter("LancePlannedFlatSearchFragmentCount"), nullptr);
    EXPECT_EQ(profile.get_counter("LancePlannedFlatSearchFragmentCount")->value(),
              static_cast<int64_t>(fixture.fragment_ids.size()));
    ASSERT_NE(profile.get_info_string("LanceTopK"), nullptr);
    EXPECT_EQ("4", *profile.get_info_string("LanceTopK"));
    ASSERT_NE(profile.get_info_string("LanceOffset"), nullptr);
    EXPECT_EQ("0", *profile.get_info_string("LanceOffset"));
    ASSERT_NE(profile.get_info_string("LanceTopKPlusOffset"), nullptr);
    EXPECT_EQ("4", *profile.get_info_string("LanceTopKPlusOffset"));
    EXPECT_NE(profile.get_counter("LanceDatasetOpenTime"), nullptr);
    EXPECT_NE(profile.get_counter("LanceScannerConfigureTime"), nullptr);
    EXPECT_NE(profile.get_counter("LanceScannerReadTime"), nullptr);
    EXPECT_NE(profile.get_counter("LanceRowOffsetRangesScanned"), nullptr);
    EXPECT_NE(profile.get_counter("LanceTaskWaitTime"), nullptr);
    EXPECT_EQ(profile.get_counter("LanceExecutionIndexCacheMissLoads"), nullptr);
    EXPECT_EQ(profile.get_counter("LanceRowIdTakeReadTime"), nullptr);
    EXPECT_EQ(profile.get_counter("LanceRowIdFetchTotalTime"), nullptr);
    EXPECT_EQ(profile.get_counter("LanceScalarIndexQueryTime"), nullptr);
    EXPECT_EQ(profile.get_counter("LanceScalarIndexResultSerializationTime"), nullptr);
    expect_lance_profile_hierarchy(&profile, {"LanceDatasetOpenTime",
                                              "LanceScannerConfigureTime",
                                              "LanceScannerReadTime",
                                              "LanceArrowToDorisBlockTime",
                                              "LanceExecutionIOOps",
                                              "LanceExecutionIORequests",
                                              "LanceExecutionIOBytesRead",
                                              "LanceIndexPartitionCacheMissLoads",
                                              "LanceIndexComparisons",
                                              "LanceFragmentsScanned",
                                              "LanceRowOffsetRangesScanned",
                                              "LanceRowsScanned",
                                              "LanceIVFPartitionsRanked",
                                              "LanceIVFPartitionsSearched",
                                              "LanceVectorIndexSegmentsSearched",
                                              "LanceTaskWaitTime",
                                              "LanceIVFPartitionRankingTime",
                                              "LancePlannedIndexSegmentCount",
                                              "LancePlannedIndexedFragmentCount",
                                              "LancePlannedFlatSearchFragmentCount"});
    EXPECT_TRUE(reader.close().ok());
}

TEST(LanceTableReaderVectorSearchTest, ReturnsStableGlobalRowIdsAndFetchesPayload) {
    const std::filesystem::path dataset_uri =
            "./be/test/format_v2/table/lance/data/all_types.lance";
    LanceFixtureInfo fixture;
    ASSERT_TRUE(get_fixture_info(dataset_uri, &fixture).ok());

    const auto global_rowid_name = BeConsts::GLOBAL_ROWID_COL + std::string("topn_fetch_lance");
    const Columns columns {
            projected_column("row_id", TYPE_BIGINT, false),
            projected_column(global_rowid_name, TYPE_STRING, false),
    };
    TQueryOptions query_options;
    query_options.__set_batch_size(1);
    TQueryGlobals query_globals;
    RuntimeState state(query_globals);
    state.set_query_options(query_options);
    RuntimeProfile profile("lance_vector_search_global_rowid_fixture");
    auto scan_params = make_float32_vector_search_params({0.0F, 0.0F, 0.0F}, 4, 0);
    const GlobalRowIdContext context {.backend_id = 123456789, .file_id = 42};

    LanceTableReader reader;
    ASSERT_TRUE(init_reader(&reader, columns, &state, &profile, &scan_params).ok());
    const auto scan_row_ids = [&]() {
        std::map<int64_t, uint64_t> row_ids;
        const auto prepare_status =
                prepare_fixture(&reader, dataset_uri, fixture, fixture.fragment_ids, context);
        EXPECT_TRUE(prepare_status.ok()) << prepare_status.to_string();
        if (!prepare_status.ok()) {
            return row_ids;
        }
        Block block;
        add_output_columns(&block, columns);
        bool eos = false;
        while (!eos) {
            const auto read_status = reader.get_block(&block, &eos);
            EXPECT_TRUE(read_status.ok()) << read_status.to_string();
            if (!read_status.ok()) {
                break;
            }
            if (eos) {
                continue;
            }
            const auto& logical_row_ids =
                    assert_cast<const ColumnInt64&>(*block.get_by_position(0).column);
            const auto& global_row_ids =
                    assert_cast<const ColumnString&>(*block.get_by_position(1).column);
            for (size_t row = 0; row < block.rows(); ++row) {
                const auto location = decode_lance_row_id(global_row_ids, row);
                EXPECT_EQ(static_cast<uint8_t>(ROW_VERSION::LANCE_DATASET_ROW_ID),
                          location.version);
                EXPECT_EQ(context.backend_id, location.backend_id);
                EXPECT_EQ(context.file_id, location.lance_file_id);
                row_ids.emplace(logical_row_ids.get_data()[row], location.lance_row_id);
            }
        }
        return row_ids;
    };

    const auto first_scan = scan_row_ids();
    const auto second_scan = scan_row_ids();
    EXPECT_EQ(4U, first_scan.size());
    EXPECT_EQ(first_scan, second_scan);
    EXPECT_TRUE(reader.close().ok());

    ASSERT_TRUE(first_scan.contains(2));
    ASSERT_TRUE(first_scan.contains(4));
    const std::vector<uint64_t> fetch_row_ids {first_scan.at(4), first_scan.at(2),
                                               first_scan.at(4)};
    const Columns payload_columns {
            projected_column("row_id", TYPE_BIGINT, false),
            projected_column("label", TYPE_STRING, true),
    };
    RuntimeProfile fetch_profile("lance_vector_search_rowid_fetch_fixture");
    LanceTableReader payload_reader;
    ASSERT_TRUE(init_reader(&payload_reader, payload_columns, &state, &fetch_profile, &scan_params)
                        .ok());
    Block payload_block;
    add_output_columns(&payload_block, payload_columns);
    ASSERT_TRUE(payload_reader
                        .read_by_row_ids(make_lance_range(dataset_uri, fixture.version,
                                                          fixture.fragment_ids),
                                         fetch_row_ids, &payload_block)
                        .ok());

    ASSERT_EQ(3U, payload_block.rows());
    const auto& logical_row_ids =
            assert_cast<const ColumnInt64&>(*payload_block.get_by_position(0).column);
    const auto& labels =
            assert_cast<const ColumnNullable&>(*payload_block.get_by_position(1).column);
    const auto& label_values = assert_cast<const ColumnString&>(labels.get_nested_column());
    EXPECT_EQ((std::vector<int64_t> {4, 2, 4}),
              std::vector<int64_t>(logical_row_ids.get_data().begin(),
                                   logical_row_ids.get_data().end()));
    ASSERT_EQ(3U, labels.size());
    EXPECT_EQ(0, labels.get_null_map_data()[0]);
    EXPECT_EQ(0, labels.get_null_map_data()[1]);
    EXPECT_EQ(0, labels.get_null_map_data()[2]);
    EXPECT_EQ("extra", label_values.get_data_at(0).to_string());
    EXPECT_EQ("unit-x", label_values.get_data_at(1).to_string());
    EXPECT_EQ("extra", label_values.get_data_at(2).to_string());
    EXPECT_NE(fetch_profile.get_counter("LanceDatasetOpenTime"), nullptr);
    EXPECT_NE(fetch_profile.get_counter("LanceRowIdTakeReadTime"), nullptr);
    EXPECT_NE(fetch_profile.get_counter("LanceArrowToDorisBlockTime"), nullptr);
    EXPECT_NE(fetch_profile.get_counter("LanceRowIdFetchTotalTime"), nullptr);
    expect_lance_profile_hierarchy(&fetch_profile,
                                   {"LanceDatasetOpenTime", "LanceRowIdTakeReadTime",
                                    "LanceArrowToDorisBlockTime", "LanceRowIdFetchTotalTime"});
    EXPECT_TRUE(payload_reader.close().ok());
}

TEST(LanceTableReaderVectorSearchTest, ReadsOnlyGlobalRowIdVirtualColumn) {
    const std::filesystem::path dataset_uri =
            "./be/test/format_v2/table/lance/data/all_types.lance";
    LanceFixtureInfo fixture;
    ASSERT_TRUE(get_fixture_info(dataset_uri, &fixture).ok());

    const auto global_rowid_name = BeConsts::GLOBAL_ROWID_COL + std::string("topn_fetch_lance");
    const Columns columns {projected_column(global_rowid_name, TYPE_STRING, false)};
    TQueryOptions query_options;
    query_options.__set_batch_size(2);
    TQueryGlobals query_globals;
    RuntimeState state(query_globals);
    state.set_query_options(query_options);
    RuntimeProfile profile("lance_vector_search_only_global_rowid_fixture");
    auto scan_params = make_float32_vector_search_params({0.0F, 0.0F, 0.0F}, 2, 0);
    const GlobalRowIdContext context {.backend_id = 13579, .file_id = 24};

    LanceTableReader reader;
    ASSERT_TRUE(init_reader(&reader, columns, &state, &profile, &scan_params).ok());
    ASSERT_TRUE(prepare_fixture(&reader, dataset_uri, fixture, fixture.fragment_ids, context).ok());

    Block block;
    add_output_columns(&block, columns);
    std::set<uint64_t> native_row_ids;
    bool eos = false;
    while (!eos) {
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        if (eos) {
            continue;
        }
        const auto& global_row_ids =
                assert_cast<const ColumnString&>(*block.get_by_position(0).column);
        for (size_t row = 0; row < block.rows(); ++row) {
            const auto location = decode_lance_row_id(global_row_ids, row);
            EXPECT_EQ(static_cast<uint8_t>(ROW_VERSION::LANCE_DATASET_ROW_ID), location.version);
            EXPECT_EQ(context.backend_id, location.backend_id);
            EXPECT_EQ(context.file_id, location.lance_file_id);
            native_row_ids.emplace(location.lance_row_id);
        }
    }
    EXPECT_EQ(2U, native_row_ids.size());
    EXPECT_TRUE(reader.close().ok());
}

TEST(LanceTableReaderFilterTest, PushesFilterOnNonProjectedColumn) {
    const std::filesystem::path dataset_uri =
            "./be/test/format_v2/table/lance/data/all_types.lance";
    LanceFixtureInfo fixture;
    ASSERT_TRUE(get_fixture_info(dataset_uri, &fixture).ok());

    const Columns columns {projected_column("label", TYPE_STRING, true)};
    TQueryOptions query_options;
    query_options.__set_batch_size(4);
    TQueryGlobals query_globals;
    RuntimeState state(query_globals);
    state.set_query_options(query_options);
    RuntimeProfile profile("lance_filter_pushdown_fixture");
    TFileScanRangeParams scan_params;
    // Generated by the FE LancePredicateConverter for row_id >= 3 against this
    // fixture's complete Arrow schema. Keeping the FE-produced envelope here
    // verifies Java Substrait -> thrift binary -> lance-c interoperability.
    const std::string substrait_filter_base64 =
            "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSERoPCAEaC2d0ZTphbnlfYW55GisKHBoaGgQKAhACIg"
            "oa"
            "CBIGCgISACIAIgYaBAoCOAMaC2ZpbHRlcl9tYXNrIrkGCgZyb3dfaWQKCmJvb2xfdmFsdWUKDXRpbnlpbnRfdm"
            "Fs"
            "dWUKDnNtYWxsaW50X3ZhbHVlCglpbnRfdmFsdWUKDGJpZ2ludF92YWx1ZQoLZmxvYXRfdmFsdWUKDGRvdWJsZV"
            "92"
            "YWx1ZQoLZGVjaW1hbF8xXzAKC2RlY2ltYWxfOV8yCgxkZWNpbWFsXzEwXzAKDGRlY2ltYWxfMThfNAoMZGVjaW"
            "1h"
            "bF8xOV8wCg1kZWNpbWFsXzM4XzEwCiRfX3VubGlrZWx5X25hbWVfcGxhY2Vob2xkZXJfZG9yaXNfMTQKJF9fdW"
            "5s"
            "aWtlbHlfbmFtZV9wbGFjZWhvbGRlcl9kb3Jpc18xNQoKdGV4dF92YWx1ZQokX191bmxpa2VseV9uYW1lX3BsYW"
            "Nl"
            "aG9sZGVyX2RvcmlzXzE3CiRfX3VubGlrZWx5X25hbWVfcGxhY2Vob2xkZXJfZG9yaXNfMTgKJF9fdW5saWtlbH"
            "lf"
            "bmFtZV9wbGFjZWhvbGRlcl9kb3Jpc18xOQokX191bmxpa2VseV9uYW1lX3BsYWNlaG9sZGVyX2RvcmlzXzIwCi"
            "Rf"
            "X3VubGlrZWx5X25hbWVfcGxhY2Vob2xkZXJfZG9yaXNfMjEKJF9fdW5saWtlbHlfbmFtZV9wbGFjZWhvbGRlcl"
            "9k"
            "b3Jpc18yMgokX191bmxpa2VseV9uYW1lX3BsYWNlaG9sZGVyX2RvcmlzXzIzCiRfX3VubGlrZWx5X25hbWVfcG"
            "xh"
            "Y2Vob2xkZXJfZG9yaXNfMjQKBWxhYmVsCiRfX3VubGlrZWx5X25hbWVfcGxhY2Vob2xkZXJfZG9yaXNfMjYSxw"
            "EK"
            "BDoCEAIKBAoCEAEKBBICEAEKBBoCEAEKBCoCEAEKBDoCEAEKBFICEAEKBFoCEAEKB8IBBBABIAEKCcIBBggCEA"
            "kg"
            "AQoHwgEEEAogAQoJwgEGCAQQEiABCgfCAQQQEyABCgnCAQYIChAmIAEKBfIBAhgBCgXyAQIYAQoEYgIQAQoF8g"
            "EC"
            "GAEKBfIBAhgBCgXyAQIYAQoF8gECGAEKBfIBAhgBCgXyAQIYAQoF8gECGAEKBfIBAhgBCgRiAhABCgXyAQIYAR"
            "gC"
            "Og8QRioLZG9yaXMtbGFuY2U=";
    std::string substrait_filter;
    ASSERT_TRUE(base64_decode(substrait_filter_base64, &substrait_filter));
    TLanceScanParams lance_scan_params;
    lance_scan_params.__set_lance_substrait_filter(std::move(substrait_filter));
    scan_params.__set_lance_scan_params(std::move(lance_scan_params));

    LanceTableReader reader;
    ASSERT_TRUE(init_reader(&reader, columns, &state, &profile, &scan_params).ok());
    ASSERT_TRUE(prepare_fixture(&reader, dataset_uri, fixture, fixture.fragment_ids).ok());
    ASSERT_TRUE(reader.TEST_conjuncts_empty());

    Block block;
    add_output_columns(&block, columns);
    std::vector<std::string> labels;
    bool eos = false;
    while (!eos) {
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        if (eos) {
            continue;
        }
        const auto& nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& values = assert_cast<const ColumnString&>(nullable.get_nested_column());
        for (size_t row = 0; row < block.rows(); ++row) {
            ASSERT_EQ(0, nullable.get_null_map_data()[row]);
            labels.emplace_back(values.get_data_at(row).to_string());
        }
    }
    std::ranges::sort(labels);
    EXPECT_EQ((std::vector<std::string> {"extra", "mixed"}), labels);
    EXPECT_TRUE(reader.close().ok());
}

TEST(LanceTableReaderFilterTest, LeavesResidualPredicatesToScanner) {
    const std::filesystem::path dataset_uri =
            "./be/test/format_v2/table/lance/data/all_types.lance";
    LanceFixtureInfo fixture;
    ASSERT_TRUE(get_fixture_info(dataset_uri, &fixture).ok());

    const Columns columns {projected_column("row_id", TYPE_BIGINT, false)};
    TQueryOptions query_options;
    query_options.__set_batch_size(4);
    TQueryGlobals query_globals;
    RuntimeState state(query_globals);
    state.set_query_options(query_options);
    RuntimeProfile profile("lance_scanner_residual_fixture");
    TFileScanRangeParams scan_params;
    auto residual = VExprContext::create_shared(std::make_shared<FailingResidualPredicate>());

    LanceTableReader reader;
    ASSERT_TRUE(init_reader(&reader, columns, &state, &profile, &scan_params, {residual}).ok());
    ASSERT_TRUE(prepare_fixture(&reader, dataset_uri, fixture, fixture.fragment_ids).ok());
    ASSERT_FALSE(reader.TEST_conjuncts_empty());

    Block block;
    add_output_columns(&block, columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_FALSE(eos);
    EXPECT_GT(block.rows(), 0);
    EXPECT_TRUE(reader.close().ok());
}

void expect_filtered_row_ids(const char* profile_name, const std::string& substrait_filter_base64,
                             const std::vector<int64_t>& expected_row_ids) {
    const std::filesystem::path dataset_uri =
            "./be/test/format_v2/table/lance/data/all_types.lance";
    LanceFixtureInfo fixture;
    ASSERT_TRUE(get_fixture_info(dataset_uri, &fixture).ok());

    const Columns columns {projected_column("row_id", TYPE_BIGINT, false)};
    TQueryOptions query_options;
    query_options.__set_batch_size(4);
    TQueryGlobals query_globals;
    RuntimeState state(query_globals);
    state.set_query_options(query_options);
    RuntimeProfile profile(profile_name);
    TFileScanRangeParams scan_params;
    std::string substrait_filter;
    ASSERT_TRUE(base64_decode(substrait_filter_base64, &substrait_filter));
    TLanceScanParams lance_scan_params;
    lance_scan_params.__set_lance_substrait_filter(std::move(substrait_filter));
    scan_params.__set_lance_scan_params(std::move(lance_scan_params));

    LanceTableReader reader;
    ASSERT_TRUE(init_reader(&reader, columns, &state, &profile, &scan_params).ok());
    ASSERT_TRUE(prepare_fixture(&reader, dataset_uri, fixture, fixture.fragment_ids).ok());
    ASSERT_TRUE(reader.TEST_conjuncts_empty());

    Block block;
    add_output_columns(&block, columns);
    std::vector<int64_t> actual_row_ids;
    bool eos = false;
    while (!eos) {
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        if (eos) {
            continue;
        }
        const auto& values = assert_cast<const ColumnInt64&>(*block.get_by_position(0).column);
        actual_row_ids.insert(actual_row_ids.end(), values.get_data().begin(),
                              values.get_data().end());
    }
    auto sorted_expected_row_ids = expected_row_ids;
    std::ranges::sort(actual_row_ids);
    std::ranges::sort(sorted_expected_row_ids);
    EXPECT_EQ(sorted_expected_row_ids, actual_row_ids);
    EXPECT_TRUE(reader.close().ok());
}

TEST(LanceTableReaderFilterTest, PushesDecimalFilterFromFeSubstrait) {
    // Generated by the FE LancePredicateConverter for decimal_9_2 >= 0.00
    // against this fixture's complete Arrow schema.
    const std::string substrait_filter_base64 =
            "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSERoPCAEaC2d0ZTphbnlfYW55GkQKNRozGgQKAhACIg"
            "waChIICgQS"
            "AggJIgAiHRobChnCARYKEAAAAAAAAAAAAAAAAAAAAAAQCRgCGgtmaWx0ZXJfbWFzayKfBgoGcm93X2lkCgpib2"
            "9sX3ZhbHVl"
            "Cg10aW55aW50X3ZhbHVlCg5zbWFsbGludF92YWx1ZQoJaW50X3ZhbHVlCgxiaWdpbnRfdmFsdWUKC2Zsb2F0X3"
            "ZhbHVlCgxk"
            "b3VibGVfdmFsdWUKC2RlY2ltYWxfMV8wCgtkZWNpbWFsXzlfMgoMZGVjaW1hbF8xMF8wCgxkZWNpbWFsXzE4Xz"
            "QKDGRlY2lt"
            "YWxfMTlfMAoNZGVjaW1hbF8zOF8xMAokX191bmxpa2VseV9uYW1lX3BsYWNlaG9sZGVyX2RvcmlzXzE0CiRfX3"
            "VubGlrZWx5"
            "X25hbWVfcGxhY2Vob2xkZXJfZG9yaXNfMTUKCnRleHRfdmFsdWUKJF9fdW5saWtlbHlfbmFtZV9wbGFjZWhvbG"
            "Rlcl9kb3Jp"
            "c18xNwoKZGF0ZV92YWx1ZQokX191bmxpa2VseV9uYW1lX3BsYWNlaG9sZGVyX2RvcmlzXzE5CiRfX3VubGlrZW"
            "x5X25hbWVf"
            "cGxhY2Vob2xkZXJfZG9yaXNfMjAKJF9fdW5saWtlbHlfbmFtZV9wbGFjZWhvbGRlcl9kb3Jpc18yMQokX191bm"
            "xpa2VseV9u"
            "YW1lX3BsYWNlaG9sZGVyX2RvcmlzXzIyCiRfX3VubGlrZWx5X25hbWVfcGxhY2Vob2xkZXJfZG9yaXNfMjMKJF"
            "9fdW5saWtl"
            "bHlfbmFtZV9wbGFjZWhvbGRlcl9kb3Jpc18yNAoFbGFiZWwKJF9fdW5saWtlbHlfbmFtZV9wbGFjZWhvbGRlcl"
            "9kb3Jpc18y"
            "NhLHAQoEOgIQAgoECgIQAQoEEgIQAQoEGgIQAQoEKgIQAQoEOgIQAQoEUgIQAQoEWgIQAQoHwgEEEAEgAQoJwg"
            "EGCAIQCSAB"
            "CgfCAQQQCiABCgnCAQYIBBASIAEKB8IBBBATIAEKCcIBBggKECYgAQoF8gECGAEKBfIBAhgBCgRiAhABCgXyAQ"
            "IYAQoFggEC"
            "EAEKBfIBAhgBCgXyAQIYAQoF8gECGAEKBfIBAhgBCgXyAQIYAQoF8gECGAEKBGICEAEKBfIBAhgBGAI6DxBGKg"
            "tkb3Jpcy1s"
            "YW5jZQ==";
    expect_filtered_row_ids("lance_decimal_filter_fixture", substrait_filter_base64, {2, 4});
}

TEST(LanceTableReaderFilterTest, PushesDateFilterFromFeSubstrait) {
    // Generated by the FE LancePredicateConverter for
    // date_value >= DATE '1970-01-01' against this fixture's complete Arrow schema.
    const std::string substrait_filter_base64 =
            "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSERoPCAEaC2d0ZTphbnlfYW55Gi4KHxodGgQKAhACIg"
            "waChIICgQS"
            "AggSIgAiBxoFCgOAAQAaC2ZpbHRlcl9tYXNrIp8GCgZyb3dfaWQKCmJvb2xfdmFsdWUKDXRpbnlpbnRfdmFsdW"
            "UKDnNtYWxs"
            "aW50X3ZhbHVlCglpbnRfdmFsdWUKDGJpZ2ludF92YWx1ZQoLZmxvYXRfdmFsdWUKDGRvdWJsZV92YWx1ZQoLZG"
            "VjaW1hbF8x"
            "XzAKC2RlY2ltYWxfOV8yCgxkZWNpbWFsXzEwXzAKDGRlY2ltYWxfMThfNAoMZGVjaW1hbF8xOV8wCg1kZWNpbW"
            "FsXzM4XzEw"
            "CiRfX3VubGlrZWx5X25hbWVfcGxhY2Vob2xkZXJfZG9yaXNfMTQKJF9fdW5saWtlbHlfbmFtZV9wbGFjZWhvbG"
            "Rlcl9kb3Jp"
            "c18xNQoKdGV4dF92YWx1ZQokX191bmxpa2VseV9uYW1lX3BsYWNlaG9sZGVyX2RvcmlzXzE3CgpkYXRlX3ZhbH"
            "VlCiRfX3Vu"
            "bGlrZWx5X25hbWVfcGxhY2Vob2xkZXJfZG9yaXNfMTkKJF9fdW5saWtlbHlfbmFtZV9wbGFjZWhvbGRlcl9kb3"
            "Jpc18yMAok"
            "X191bmxpa2VseV9uYW1lX3BsYWNlaG9sZGVyX2RvcmlzXzIxCiRfX3VubGlrZWx5X25hbWVfcGxhY2Vob2xkZX"
            "JfZG9yaXNf"
            "MjIKJF9fdW5saWtlbHlfbmFtZV9wbGFjZWhvbGRlcl9kb3Jpc18yMwokX191bmxpa2VseV9uYW1lX3BsYWNlaG"
            "9sZGVyX2Rv"
            "cmlzXzI0CgVsYWJlbAokX191bmxpa2VseV9uYW1lX3BsYWNlaG9sZGVyX2RvcmlzXzI2EscBCgQ6AhACCgQKAh"
            "ABCgQSAhAB"
            "CgQaAhABCgQqAhABCgQ6AhABCgRSAhABCgRaAhABCgfCAQQQASABCgnCAQYIAhAJIAEKB8IBBBAKIAEKCcIBBg"
            "gEEBIgAQoH"
            "wgEEEBMgAQoJwgEGCAoQJiABCgXyAQIYAQoF8gECGAEKBGICEAEKBfIBAhgBCgWCAQIQAQoF8gECGAEKBfIBAh"
            "gBCgXyAQIY"
            "AQoF8gECGAEKBfIBAhgBCgXyAQIYAQoEYgIQAQoF8gECGAEYAjoPEEYqC2RvcmlzLWxhbmNl";
    expect_filtered_row_ids("lance_date_filter_fixture", substrait_filter_base64, {2, 3});
}

DataTypePtr nullable_type(PrimitiveType type, int precision = 0, int scale = 0) {
    return DataTypeFactory::instance().create_data_type(type, true, precision, scale);
}

// Creates an Arrow Duration array with one value and one null.
std::shared_ptr<arrow::Array> make_duration_array(arrow::TimeUnit::type unit, int64_t value) {
    arrow::DurationBuilder builder(arrow::duration(unit), arrow::default_memory_pool());
    EXPECT_TRUE(builder.Append(value).ok());
    EXPECT_TRUE(builder.AppendNull().ok());
    std::shared_ptr<arrow::DurationArray> array;
    EXPECT_TRUE(builder.Finish(&array).ok());
    return array;
}

// Creates a Lance BFloat16 array with one vector and one null.
std::shared_ptr<arrow::Array> make_bfloat16_vector_array() {
    auto values =
            std::make_shared<arrow::FixedSizeBinaryBuilder>(arrow::fixed_size_binary(2));
    arrow::FixedSizeListBuilder builder(arrow::default_memory_pool(), values, 2);
    EXPECT_TRUE(builder.Append().ok());
    const std::array<uint8_t, 2> one {0x80, 0x3F};
    const std::array<uint8_t, 2> two {0x00, 0x40};
    EXPECT_TRUE(values->Append(one.data()).ok());
    EXPECT_TRUE(values->Append(two.data()).ok());
    EXPECT_TRUE(builder.AppendNull().ok());
    std::shared_ptr<arrow::FixedSizeListArray> array;
    EXPECT_TRUE(builder.Finish(&array).ok());
    return array;
}

std::pair<size_t, size_t> array_range(const ColumnArray& array, size_t row) {
    const auto& offsets = array.get_offsets();
    return {row == 0 ? 0 : static_cast<size_t>(offsets[row - 1]),
            static_cast<size_t>(offsets[row])};
}

std::pair<size_t, size_t> map_range(const ColumnMap& map, size_t row) {
    const auto& offsets = map.get_offsets();
    return {row == 0 ? 0 : static_cast<size_t>(offsets[row - 1]),
            static_cast<size_t>(offsets[row])};
}

} // namespace

TEST(LanceTableReaderSchemaTest, FetchesSchemaWithoutFragmentIdsOrScanInitialization) {
    const std::filesystem::path dataset_uri =
            "./be/test/format_v2/table/lance/data/all_types.lance";

    // A local TVF schema request opens the latest dataset snapshot but returns only its schema.
    auto range = make_latest_lance_range(dataset_uri);

    TFileScanRangeParams scan_params;
    std::vector<std::string> column_names;
    std::vector<DataTypePtr> column_types;
    LanceTableReader reader;
    ASSERT_TRUE(reader.fetch_schema(range, scan_params, &column_names, &column_types).ok());
    ASSERT_EQ(column_names.size(), column_types.size());

    const auto row_id = std::find(column_names.begin(), column_names.end(), "row_id");
    ASSERT_NE(column_names.end(), row_id);
    const auto row_id_idx = static_cast<size_t>(std::distance(column_names.begin(), row_id));
    ASSERT_NE(nullptr, column_types[row_id_idx]);
    EXPECT_EQ(TYPE_BIGINT, column_types[row_id_idx]->get_primitive_type());

    const auto embedding = std::find(column_names.begin(), column_names.end(), "embedding");
    ASSERT_NE(column_names.end(), embedding);
    const auto embedding_idx = static_cast<size_t>(std::distance(column_names.begin(), embedding));
    ASSERT_NE(nullptr, column_types[embedding_idx]);
    EXPECT_EQ(TYPE_ARRAY, column_types[embedding_idx]->get_primitive_type());

    const auto binary_value = std::find(column_names.begin(), column_names.end(), "binary_value");
    ASSERT_NE(column_names.end(), binary_value);
    const auto binary_idx = static_cast<size_t>(std::distance(column_names.begin(), binary_value));
    ASSERT_NE(nullptr, column_types[binary_idx]);
    const auto binary_type = remove_nullable(column_types[binary_idx]);
    ASSERT_EQ(TYPE_VARBINARY, binary_type->get_primitive_type());
    EXPECT_EQ(std::numeric_limits<int32_t>::max(),
              assert_cast<const DataTypeVarbinary&>(*binary_type).len());
}

// Verifies the additional mappings and preserves unknown extensions as unsupported.
TEST(LanceTableReaderSchemaTest, MapsAdditionalTypesAndPreservesUnknownExtensions) {
    const auto unknown_extension_metadata =
            arrow::KeyValueMetadata::Make({"ARROW:extension:name"}, {"doris.test.extension"});
    const auto json_extension_metadata =
            arrow::KeyValueMetadata::Make({"ARROW:extension:name"}, {"arrow.json"});
    const auto bfloat16_extension_metadata =
            arrow::KeyValueMetadata::Make({"ARROW:extension:name"}, {"lance.bfloat16"});
    const auto blob_extension_metadata =
            arrow::KeyValueMetadata::Make({"ARROW:extension:name"}, {"lance.blob.v2"});
    const auto bfloat16_item =
            arrow::field("item", arrow::fixed_size_binary(2))->WithMetadata(
                    bfloat16_extension_metadata);
    const auto blob_type = arrow::struct_({
            arrow::field("kind", arrow::uint8(), false),
            arrow::field("position", arrow::uint64(), false),
            arrow::field("size", arrow::uint64(), false),
            arrow::field("blob_id", arrow::uint32(), false),
            arrow::field("blob_uri", arrow::utf8(), false),
    });
    const auto arrow_schema = arrow::schema({
            arrow::field("row_id", arrow::int64()),
            arrow::field("null_value", arrow::null()),
            arrow::field("duration", arrow::duration(arrow::TimeUnit::MILLI)),
            arrow::field("json", arrow::utf8())->WithMetadata(json_extension_metadata),
            arrow::field("blob", blob_type)->WithMetadata(blob_extension_metadata),
            arrow::field("bfloat16_vector", arrow::fixed_size_list(bfloat16_item, 4)),
            arrow::field("dictionary", arrow::dictionary(arrow::int16(), arrow::utf8())),
            arrow::field("unknown_extension", arrow::utf8())
                    ->WithMetadata(unknown_extension_metadata),
            arrow::field("name", arrow::utf8()),
    });

    std::vector<std::string> column_names;
    std::vector<DataTypePtr> column_types;
    ASSERT_TRUE(convert_arrow_schema_to_doris(arrow_schema, &column_names, &column_types).ok());

    EXPECT_EQ((std::vector<std::string> {"row_id", "null_value", "duration", "json", "blob",
                                         "bfloat16_vector", "dictionary", "unknown_extension",
                                         "name"}),
              column_names);
    ASSERT_EQ(column_names.size(), column_types.size());
    for (const auto& column_type : column_types) {
        ASSERT_NE(nullptr, column_type);
    }
    EXPECT_EQ(TYPE_BIGINT, column_types[0]->get_primitive_type());
    EXPECT_TRUE(column_types[1]->is_null_literal());
    EXPECT_EQ(TYPE_BIGINT, column_types[2]->get_primitive_type());
    EXPECT_EQ(TYPE_JSONB, column_types[3]->get_primitive_type());
    ASSERT_EQ(TYPE_STRUCT, column_types[4]->get_primitive_type());
    const auto& blob_struct =
            assert_cast<const DataTypeStruct&>(*remove_nullable(column_types[4]));
    ASSERT_EQ(5, blob_struct.get_elements().size());
    EXPECT_EQ((Strings {"kind", "position", "size", "blob_id", "blob_uri"}),
              blob_struct.get_element_names());
    EXPECT_EQ(TYPE_SMALLINT, blob_struct.get_element(0)->get_primitive_type());
    EXPECT_EQ(TYPE_LARGEINT, blob_struct.get_element(1)->get_primitive_type());
    EXPECT_EQ(TYPE_LARGEINT, blob_struct.get_element(2)->get_primitive_type());
    EXPECT_EQ(TYPE_BIGINT, blob_struct.get_element(3)->get_primitive_type());
    EXPECT_EQ(TYPE_STRING, blob_struct.get_element(4)->get_primitive_type());
    ASSERT_EQ(TYPE_ARRAY, column_types[5]->get_primitive_type());
    const auto& bfloat16_array =
            assert_cast<const DataTypeArray&>(*remove_nullable(column_types[5]));
    EXPECT_EQ(TYPE_FLOAT, bfloat16_array.get_nested_type()->get_primitive_type());
    EXPECT_EQ(INVALID_TYPE, column_types[6]->get_primitive_type());
    EXPECT_EQ(INVALID_TYPE, column_types[7]->get_primitive_type());
    EXPECT_EQ(TYPE_STRING, column_types[8]->get_primitive_type());
}

// Verifies malformed storage for known extensions remains unsupported.
TEST(LanceTableReaderSchemaTest, RejectsMalformedKnownExtensionStorage) {
    const auto json_extension_metadata =
            arrow::KeyValueMetadata::Make({"ARROW:extension:name"}, {"arrow.json"});
    const auto bfloat16_extension_metadata =
            arrow::KeyValueMetadata::Make({"ARROW:extension:name"}, {"lance.bfloat16"});
    const auto blob_extension_metadata =
            arrow::KeyValueMetadata::Make({"ARROW:extension:name"}, {"lance.blob.v2"});
    const auto arrow_schema = arrow::schema({
            arrow::field("json", arrow::binary())->WithMetadata(json_extension_metadata),
            arrow::field("bfloat16", arrow::fixed_size_binary(4))
                    ->WithMetadata(bfloat16_extension_metadata),
            arrow::field("blob", arrow::struct_({}))->WithMetadata(blob_extension_metadata),
    });

    std::vector<std::string> column_names;
    std::vector<DataTypePtr> column_types;
    ASSERT_TRUE(convert_arrow_schema_to_doris(arrow_schema, &column_names, &column_types).ok());
    ASSERT_EQ(3, column_types.size());
    for (const auto& column_type : column_types) {
        ASSERT_NE(nullptr, column_type);
        EXPECT_EQ(INVALID_TYPE, column_type->get_primitive_type());
    }
}

// Verifies nested Null fields remain unsupported.
TEST(LanceTableReaderSchemaTest, MarksNestedNullTypesAsUnsupported) {
    const auto arrow_schema = arrow::schema({
            arrow::field("null_list", arrow::list(arrow::field("item", arrow::null()))),
            arrow::field("null_struct",
                         arrow::struct_({arrow::field("value", arrow::null())})),
    });

    std::vector<std::string> column_names;
    std::vector<DataTypePtr> column_types;
    ASSERT_TRUE(convert_arrow_schema_to_doris(arrow_schema, &column_names, &column_types).ok());

    EXPECT_EQ((std::vector<std::string> {"null_list", "null_struct"}), column_names);
    ASSERT_EQ(2, column_types.size());
    for (const auto& column_type : column_types) {
        ASSERT_NE(nullptr, column_type);
        EXPECT_EQ(INVALID_TYPE, column_type->get_primitive_type());
    }
}

// Verifies values, nullability, and precision when reading the additional types.
TEST(LanceTableReaderTypeTest, ReadsAdditionalArrowAndLanceTypes) {
    const auto json_extension_metadata =
            arrow::KeyValueMetadata::Make({"ARROW:extension:name"}, {"arrow.json"});
    const auto bfloat16_extension_metadata =
            arrow::KeyValueMetadata::Make({"ARROW:extension:name"}, {"lance.bfloat16"});
    const auto bfloat16_item =
            arrow::field("item", arrow::fixed_size_binary(2))->WithMetadata(
                    bfloat16_extension_metadata);
    const auto schema = arrow::schema({
            arrow::field("null_value", arrow::null()),
            arrow::field("duration_s", arrow::duration(arrow::TimeUnit::SECOND)),
            arrow::field("duration_ms", arrow::duration(arrow::TimeUnit::MILLI)),
            arrow::field("duration_us", arrow::duration(arrow::TimeUnit::MICRO)),
            arrow::field("duration_ns", arrow::duration(arrow::TimeUnit::NANO)),
            arrow::field("json_value", arrow::utf8())->WithMetadata(json_extension_metadata),
            arrow::field("bfloat16_vector", arrow::fixed_size_list(bfloat16_item, 2)),
    });

    arrow::StringBuilder json_builder;
    ASSERT_TRUE(json_builder.Append(R"({"engine":"doris"})").ok());
    ASSERT_TRUE(json_builder.AppendNull().ok());
    std::shared_ptr<arrow::StringArray> json_array;
    ASSERT_TRUE(json_builder.Finish(&json_array).ok());

    const auto record_batch = arrow::RecordBatch::Make(
            schema, 2,
            {
                    std::make_shared<arrow::NullArray>(2),
                    make_duration_array(arrow::TimeUnit::SECOND, 1),
                    make_duration_array(arrow::TimeUnit::MILLI, 1000),
                    make_duration_array(arrow::TimeUnit::MICRO, 1000000),
                    make_duration_array(arrow::TimeUnit::NANO, 1000000000),
                    json_array,
                    make_bfloat16_vector_array(),
            });

    const auto bfloat16_array_type = make_nullable(
            std::make_shared<DataTypeArray>(nullable_type(TYPE_FLOAT)));
    const Columns columns {
            projected_column("null_value", nullable_type(TYPE_NULL)),
            projected_column("duration_s", TYPE_BIGINT, true),
            projected_column("duration_ms", TYPE_BIGINT, true),
            projected_column("duration_us", TYPE_BIGINT, true),
            projected_column("duration_ns", TYPE_BIGINT, true),
            projected_column("json_value", TYPE_JSONB, true),
            projected_column("bfloat16_vector", bfloat16_array_type),
    };
    TQueryGlobals query_globals;
    RuntimeState state(query_globals);
    RuntimeProfile profile("lance_additional_types");
    TFileScanRangeParams scan_params;
    LanceTableReader reader;
    ASSERT_TRUE(init_reader(&reader, columns, &state, &profile, &scan_params).ok());

    Block block;
    add_output_columns(&block, columns);
    size_t rows = 0;
    ASSERT_TRUE(reader._fill_block_from_record_batch(record_batch, &block, &rows).ok());
    ASSERT_EQ(2, rows);

    const auto& null_values =
            assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    EXPECT_EQ((ColumnUInt8::Container {1, 1}), null_values.get_null_map_data());

    const std::array<int64_t, 4> expected_durations {1, 1000, 1000000, 1000000000};
    for (size_t column_idx = 0; column_idx < expected_durations.size(); ++column_idx) {
        const auto& duration = assert_cast<const ColumnNullable&>(
                *block.get_by_position(column_idx + 1).column);
        const auto& values = assert_cast<const ColumnInt64&>(duration.get_nested_column());
        EXPECT_EQ(0, duration.get_null_map_data()[0]);
        EXPECT_EQ(1, duration.get_null_map_data()[1]);
        EXPECT_EQ(expected_durations[column_idx], values.get_data()[0]);
    }

    const auto& json_values =
            assert_cast<const ColumnNullable&>(*block.get_by_position(5).column);
    EXPECT_EQ(0, json_values.get_null_map_data()[0]);
    EXPECT_EQ(1, json_values.get_null_map_data()[1]);
    EXPECT_EQ(R"({"engine":"doris"})", columns[5].type->to_string(json_values, 0));

    const auto& vectors =
            assert_cast<const ColumnNullable&>(*block.get_by_position(6).column);
    EXPECT_EQ(0, vectors.get_null_map_data()[0]);
    EXPECT_EQ(1, vectors.get_null_map_data()[1]);
    const auto& vector_values = assert_cast<const ColumnArray&>(vectors.get_nested_column());
    EXPECT_EQ((ColumnArray::Offsets64 {2, 4}), vector_values.get_offsets());
    const auto& bfloat16_values =
            assert_cast<const ColumnNullable&>(vector_values.get_data());
    const auto& floats =
            assert_cast<const ColumnFloat32&>(bfloat16_values.get_nested_column());
    EXPECT_FLOAT_EQ(1.0F, floats.get_data()[0]);
    EXPECT_FLOAT_EQ(2.0F, floats.get_data()[1]);
    EXPECT_TRUE(reader.close().ok());
}

// Verifies the additional types through the full scan path.
TEST(LanceTableReaderTypeTest, ReadsAdditionalTypesFromCompatibilityFixture) {
    const std::filesystem::path dataset_uri =
            "./docker/thirdparties/docker-compose/iceberg/scripts/preinstalled_data/lance/"
            "all_types.lance";
    const auto bfloat16_array_type = make_nullable(
            std::make_shared<DataTypeArray>(nullable_type(TYPE_FLOAT)));
    const auto blob_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {nullable_type(TYPE_SMALLINT), nullable_type(TYPE_LARGEINT),
                       nullable_type(TYPE_LARGEINT), nullable_type(TYPE_BIGINT),
                       nullable_type(TYPE_STRING)},
            Strings {"kind", "position", "size", "blob_id", "blob_uri"}));
    const Columns columns {
            projected_column("row_id", TYPE_BIGINT, false),
            projected_column("null_col", nullable_type(TYPE_NULL)),
            projected_column("duration_s_col", TYPE_BIGINT, true),
            projected_column("duration_ms_col", TYPE_BIGINT, true),
            projected_column("duration_us_col", TYPE_BIGINT, true),
            projected_column("duration_ns_col", TYPE_BIGINT, true),
            projected_column("blob_col", blob_type),
            projected_column("json_col", TYPE_JSONB, true),
            projected_column("bfloat16_vector_col", bfloat16_array_type),
    };
    TQueryOptions query_options;
    query_options.__set_batch_size(4);
    TQueryGlobals query_globals;
    RuntimeState state(query_globals);
    state.set_query_options(query_options);
    RuntimeProfile profile("lance_additional_types_fixture");
    TFileScanRangeParams scan_params;
    LanceTableReader reader;
    ASSERT_TRUE(init_reader(&reader, columns, &state, &profile, &scan_params).ok());
    ASSERT_TRUE(prepare_range(&reader, make_latest_lance_range(dataset_uri)).ok());

    Block block;
    add_output_columns(&block, columns);
    bool found = false;
    bool eos = false;
    while (!eos) {
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        if (eos) {
            continue;
        }
        const auto& row_ids = assert_cast<const ColumnInt64&>(*block.get_by_position(0).column);
        for (size_t row = 0; row < block.rows(); ++row) {
            const auto& null_values =
                    assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
            EXPECT_EQ(1, null_values.get_null_map_data()[row]);
            if (row_ids.get_data()[row] != 1) {
                continue;
            }
            found = true;
            const std::array<int64_t, 4> expected_durations {1, 1000, 1000000, 1000000000};
            for (size_t duration_idx = 0; duration_idx < expected_durations.size();
                 ++duration_idx) {
                const auto& duration = assert_cast<const ColumnNullable&>(
                        *block.get_by_position(duration_idx + 2).column);
                const auto& values =
                        assert_cast<const ColumnInt64&>(duration.get_nested_column());
                EXPECT_EQ(0, duration.get_null_map_data()[row]);
                EXPECT_EQ(expected_durations[duration_idx], values.get_data()[row]);
            }

            // Blob v2 is exposed as its descriptor struct, never the payload bytes: the reader
            // only ever sees where a Blob lives and how large it is. size is the byte length of
            // the original "blob payload" content; the remaining descriptor fields depend on how
            // the fixture stored the Blob and are asserted for presence here.
            const auto& blobs =
                    assert_cast<const ColumnNullable&>(*block.get_by_position(6).column);
            const auto& blob_struct =
                    assert_cast<const ColumnStruct&>(blobs.get_nested_column());
            ASSERT_EQ(5, blob_struct.tuple_size());
            EXPECT_EQ(0, blobs.get_null_map_data()[row]);
            const auto& blob_sizes = assert_cast<const ColumnInt128&>(
                    assert_cast<const ColumnNullable&>(blob_struct.get_column(2))
                            .get_nested_column());
            EXPECT_EQ(std::string("blob payload").size(), blob_sizes.get_data()[row]);

            const auto& json_values =
                    assert_cast<const ColumnNullable&>(*block.get_by_position(7).column);
            EXPECT_EQ(R"({"engine":"doris","format":"lance"})",
                      columns[7].type->to_string(json_values, row));

            const auto& vectors =
                    assert_cast<const ColumnNullable&>(*block.get_by_position(8).column);
            const auto& vector_values =
                    assert_cast<const ColumnArray&>(vectors.get_nested_column());
            const auto [begin, end] = array_range(vector_values, row);
            ASSERT_EQ(4, end - begin);
            const auto& nullable_values =
                    assert_cast<const ColumnNullable&>(vector_values.get_data());
            const auto& floats =
                    assert_cast<const ColumnFloat32&>(nullable_values.get_nested_column());
            for (size_t index = 0; index < 4; ++index) {
                EXPECT_FLOAT_EQ(static_cast<float>(index + 1), floats.get_data()[begin + index]);
            }
        }
    }
    EXPECT_TRUE(found);
    EXPECT_TRUE(reader.close().ok());
}

TEST(LanceTableReaderSchemaTest, FetchesNegativeScaleDecimalAsUnsupported) {
    const auto unique_suffix = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto dataset_uri =
            std::filesystem::temp_directory_path() /
            ("doris_lance_negative_decimal_scale_" + std::to_string(unique_suffix) + ".lance");
    Defer cleanup {[&] {
        std::error_code error;
        std::filesystem::remove_all(dataset_uri, error);
    }};

    const auto decimal_type = arrow::decimal128(10, -2);
    const auto arrow_schema = arrow::schema({arrow::field("negative_scale_decimal", decimal_type)});
    auto array = arrow::MakeArrayOfNull(decimal_type, 1);
    ASSERT_TRUE(array.ok()) << array.status().ToString();
    const auto batch = arrow::RecordBatch::Make(arrow_schema, 1, {std::move(array).ValueUnsafe()});
    auto batch_reader = arrow::RecordBatchReader::Make({batch}, arrow_schema);
    ASSERT_TRUE(batch_reader.ok()) << batch_reader.status().ToString();

    ArrowArrayStream stream {};
    const auto export_status =
            arrow::ExportRecordBatchReader(std::move(batch_reader).ValueUnsafe(), &stream);
    ASSERT_TRUE(export_status.ok()) << export_status.ToString();
    static_cast<void>(
            ::lance::Dataset::write(dataset_uri.string(), &stream, ::lance::WriteMode::Create));

    TFileScanRangeParams scan_params;
    std::vector<std::string> column_names;
    std::vector<DataTypePtr> column_types;
    LanceTableReader reader;
    const auto status = reader.fetch_schema(make_latest_lance_range(dataset_uri), scan_params,
                                            &column_names, &column_types);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ((std::vector<std::string> {"negative_scale_decimal"}), column_names);
    ASSERT_EQ(1, column_types.size());
    ASSERT_NE(nullptr, column_types[0]);
    EXPECT_EQ(INVALID_TYPE, column_types[0]->get_primitive_type());
}

TEST(LanceTableReaderScanTest, ReadsLatestSnapshotWithoutFragmentIds) {
    const std::filesystem::path dataset_uri =
            "./be/test/format_v2/table/lance/data/all_types.lance";
    const Columns columns {projected_column("row_id", TYPE_BIGINT, false)};
    TQueryOptions query_options;
    query_options.__set_batch_size(2);
    TQueryGlobals query_globals;
    RuntimeState state(query_globals);
    state.set_query_options(query_options);
    RuntimeProfile profile("lance_latest_all_fragments_fixture");
    TFileScanRangeParams scan_params;

    LanceTableReader reader;
    ASSERT_TRUE(init_reader(&reader, columns, &state, &profile, &scan_params).ok());
    ASSERT_TRUE(prepare_range(&reader, make_latest_lance_range(dataset_uri)).ok());

    Block block;
    add_output_columns(&block, columns);
    std::array<bool, 5> seen_rows {};
    size_t total_rows = 0;
    bool eos = false;
    while (!eos) {
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        if (eos) {
            continue;
        }
        const auto& row_ids = assert_cast<const ColumnInt64&>(*block.get_by_position(0).column);
        for (size_t row = 0; row < block.rows(); ++row) {
            const auto row_id = row_ids.get_data()[row];
            ASSERT_GE(row_id, 1);
            ASSERT_LE(row_id, 4);
            EXPECT_FALSE(seen_rows[row_id]);
            seen_rows[row_id] = true;
        }
        total_rows += block.rows();
    }
    EXPECT_EQ(4U, total_rows);
    EXPECT_TRUE(seen_rows[1]);
    EXPECT_TRUE(seen_rows[2]);
    EXPECT_TRUE(seen_rows[3]);
    EXPECT_TRUE(seen_rows[4]);
    EXPECT_TRUE(reader.close().ok());
}

TEST(LanceTableReaderScanTest, RejectsStorageOptionWithEmbeddedNul) {
    const std::filesystem::path dataset_uri =
            "./be/test/format_v2/table/lance/data/all_types.lance";
    const Columns columns {projected_column("row_id", TYPE_BIGINT, false)};
    TQueryGlobals query_globals;
    RuntimeState state(query_globals);
    RuntimeProfile profile("lance_storage_option_embedded_nul");

    // lance-c reads these as C strings, so a NUL truncates the option here while the FE goes on
    // using the whole thing, leaving the two halves opening the dataset with different
    // configuration. Dropping it instead of failing would only move that divergence.
    TFileScanRangeParams scan_params;
    TLanceScanParams lance_scan_params;
    lance_scan_params.__set_lance_storage_options(
            {{std::string("aws_region\0ignored", 18), "us-east-1"}});
    scan_params.__set_lance_scan_params(std::move(lance_scan_params));

    LanceTableReader reader;
    ASSERT_TRUE(init_reader(&reader, columns, &state, &profile, &scan_params).ok());

    const auto status = prepare_range(&reader, make_latest_lance_range(dataset_uri));

    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("contains a NUL"), std::string::npos);
    EXPECT_TRUE(reader.close().ok());
}

TEST(LanceTableReaderTypeTest, ReadsNumericTypesFromAllTypesFixture) {
    // The committed fixture contains four rows covering values, nulls, and boundary cases.
    const std::filesystem::path dataset_uri =
            "./be/test/format_v2/table/lance/data/all_types.lance";
    LanceFixtureInfo fixture;
    ASSERT_TRUE(get_fixture_info(dataset_uri, &fixture).ok());
    ASSERT_FALSE(fixture.fragment_ids.empty());

    const Columns columns {
            projected_column("row_id", TYPE_BIGINT, false),
            projected_column("bool_value", TYPE_BOOLEAN, true),
            projected_column("tinyint_value", TYPE_TINYINT, true),
            projected_column("smallint_value", TYPE_SMALLINT, true),
            projected_column("int_value", TYPE_INT, true),
            projected_column("bigint_value", TYPE_BIGINT, true),
            projected_column("float_value", TYPE_FLOAT, true),
            projected_column("double_value", TYPE_DOUBLE, true),
    };
    TQueryOptions query_options;
    query_options.__set_batch_size(3);
    TQueryGlobals query_globals;
    RuntimeState state(query_globals);
    state.set_query_options(query_options);
    RuntimeProfile profile("lance_numeric_types_fixture");
    TFileScanRangeParams scan_params;
    LanceTableReader reader;
    ASSERT_TRUE(init_reader(&reader, columns, &state, &profile, &scan_params).ok());

    Block block;
    add_output_columns(&block, columns);
    std::array<bool, 5> seen_rows {};
    const auto read_split = [&](std::vector<int64_t> fragment_ids) {
        ASSERT_TRUE(prepare_fixture(&reader, dataset_uri, fixture, std::move(fragment_ids)).ok());
        bool eos = false;
        while (!eos) {
            ASSERT_TRUE(reader.get_block(&block, &eos).ok());
            if (eos) {
                continue;
            }
            const auto& row_ids = assert_cast<const ColumnInt64&>(*block.get_by_position(0).column);
            const auto& bools =
                    assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
            const auto& tinyints =
                    assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
            const auto& smallints =
                    assert_cast<const ColumnNullable&>(*block.get_by_position(3).column);
            const auto& ints = assert_cast<const ColumnNullable&>(*block.get_by_position(4).column);
            const auto& bigints =
                    assert_cast<const ColumnNullable&>(*block.get_by_position(5).column);
            const auto& floats =
                    assert_cast<const ColumnNullable&>(*block.get_by_position(6).column);
            const auto& doubles =
                    assert_cast<const ColumnNullable&>(*block.get_by_position(7).column);
            const auto& bool_values = assert_cast<const ColumnUInt8&>(bools.get_nested_column());
            const auto& tinyint_values =
                    assert_cast<const ColumnInt8&>(tinyints.get_nested_column());
            const auto& smallint_values =
                    assert_cast<const ColumnInt16&>(smallints.get_nested_column());
            const auto& int_values = assert_cast<const ColumnInt32&>(ints.get_nested_column());
            const auto& bigint_values =
                    assert_cast<const ColumnInt64&>(bigints.get_nested_column());
            const auto& float_values =
                    assert_cast<const ColumnFloat32&>(floats.get_nested_column());
            const auto& double_values =
                    assert_cast<const ColumnFloat64&>(doubles.get_nested_column());
            for (size_t row = 0; row < block.rows(); ++row) {
                const auto row_id = row_ids.get_data()[row];
                ASSERT_GE(row_id, 1);
                ASSERT_LE(row_id, 4);
                EXPECT_FALSE(seen_rows[row_id]);
                seen_rows[row_id] = true;
                if (row_id == 3) {
                    EXPECT_EQ(1, bools.get_null_map_data()[row]);
                    EXPECT_EQ(1, tinyints.get_null_map_data()[row]);
                    EXPECT_EQ(1, smallints.get_null_map_data()[row]);
                    EXPECT_EQ(1, ints.get_null_map_data()[row]);
                    EXPECT_EQ(1, bigints.get_null_map_data()[row]);
                    EXPECT_EQ(1, floats.get_null_map_data()[row]);
                    EXPECT_EQ(1, doubles.get_null_map_data()[row]);
                } else {
                    EXPECT_EQ(0, bools.get_null_map_data()[row]);
                    EXPECT_EQ(0, tinyints.get_null_map_data()[row]);
                    EXPECT_EQ(0, smallints.get_null_map_data()[row]);
                    EXPECT_EQ(0, ints.get_null_map_data()[row]);
                    EXPECT_EQ(0, bigints.get_null_map_data()[row]);
                    EXPECT_EQ(0, floats.get_null_map_data()[row]);
                    EXPECT_EQ(0, doubles.get_null_map_data()[row]);
                    if (row_id == 1) {
                        EXPECT_EQ(1, bool_values.get_data()[row]);
                        EXPECT_EQ(-128, tinyint_values.get_data()[row]);
                        EXPECT_EQ(-32768, smallint_values.get_data()[row]);
                        EXPECT_EQ(-2147483648, int_values.get_data()[row]);
                        EXPECT_EQ(-9223372036854775807LL, bigint_values.get_data()[row]);
                        EXPECT_FLOAT_EQ(-1.25F, float_values.get_data()[row]);
                        EXPECT_DOUBLE_EQ(-1.25, double_values.get_data()[row]);
                    } else if (row_id == 2) {
                        EXPECT_EQ(0, bool_values.get_data()[row]);
                        EXPECT_EQ(127, tinyint_values.get_data()[row]);
                        EXPECT_EQ(32767, smallint_values.get_data()[row]);
                        EXPECT_EQ(2147483647, int_values.get_data()[row]);
                        EXPECT_EQ(9223372036854775807LL, bigint_values.get_data()[row]);
                        EXPECT_FLOAT_EQ(3.5F, float_values.get_data()[row]);
                        EXPECT_DOUBLE_EQ(3.5, double_values.get_data()[row]);
                    } else {
                        EXPECT_EQ(0, bool_values.get_data()[row]);
                        EXPECT_EQ(0, tinyint_values.get_data()[row]);
                        EXPECT_EQ(0, smallint_values.get_data()[row]);
                        EXPECT_EQ(0, int_values.get_data()[row]);
                        EXPECT_EQ(0, bigint_values.get_data()[row]);
                        EXPECT_FLOAT_EQ(0.0F, float_values.get_data()[row]);
                        EXPECT_DOUBLE_EQ(0.0, double_values.get_data()[row]);
                    }
                }
            }
        }
    };
    for (const auto fragment_id : fixture.fragment_ids) {
        read_split({fragment_id});
    }
    EXPECT_TRUE(seen_rows[1]);
    EXPECT_TRUE(seen_rows[2]);
    EXPECT_TRUE(seen_rows[3]);
    EXPECT_TRUE(seen_rows[4]);
    EXPECT_TRUE(reader.close().ok());
}

TEST(LanceTableReaderTypeTest, ReadsDecimalTypesFromAllTypesFixture) {
    // The committed fixture contains four rows covering values, nulls, and boundary cases.
    const std::filesystem::path dataset_uri =
            "./be/test/format_v2/table/lance/data/all_types.lance";
    LanceFixtureInfo fixture;
    ASSERT_TRUE(get_fixture_info(dataset_uri, &fixture).ok());
    ASSERT_FALSE(fixture.fragment_ids.empty());

    const std::array<std::tuple<const char*, PrimitiveType, int, int>, 8> decimal_specs {{
            {"decimal_1_0", TYPE_DECIMAL32, 1, 0},
            {"decimal_9_2", TYPE_DECIMAL32, 9, 2},
            {"decimal_10_0", TYPE_DECIMAL64, 10, 0},
            {"decimal_18_4", TYPE_DECIMAL64, 18, 4},
            {"decimal_19_0", TYPE_DECIMAL128I, 19, 0},
            {"decimal_38_10", TYPE_DECIMAL128I, 38, 10},
            {"decimal_39_4", TYPE_DECIMAL256, 39, 4},
            {"decimal_76_38", TYPE_DECIMAL256, 76, 38},
    }};
    Columns columns {projected_column("row_id", TYPE_BIGINT, false)};
    for (const auto& [name, type, precision, scale] : decimal_specs) {
        columns.emplace_back(projected_column(name, nullable_type(type, precision, scale)));
    }

    TQueryOptions query_options;
    query_options.__set_batch_size(4);
    TQueryGlobals query_globals;
    RuntimeState state(query_globals);
    state.set_query_options(query_options);
    RuntimeProfile profile("lance_decimal_types_fixture");
    TFileScanRangeParams scan_params;
    LanceTableReader reader;
    ASSERT_TRUE(init_reader(&reader, columns, &state, &profile, &scan_params).ok());
    ASSERT_TRUE(prepare_fixture(&reader, dataset_uri, fixture, fixture.fragment_ids).ok());

    Block block;
    add_output_columns(&block, columns);
    const std::array<std::array<const char*, 3>, 8> expected_values {{
            {{"-9", "9", "0"}},
            {{"-9999999.99", "9999999.99", "0.00"}},
            {{"-9999999999", "9999999999", "0"}},
            {{"-99999999999999.9999", "99999999999999.9999", "0.0000"}},
            {{"-9999999999999999999", "9999999999999999999", "0"}},
            {{"-9999999999999999999999999999.9999999999", "9999999999999999999999999999.9999999999",
              "0.0000000000"}},
            {{"-99999999999999999999999999999999999.9999",
              "99999999999999999999999999999999999.9999", "0.0000"}},
            {{"-99999999999999999999999999999999999999.99999999999999999999999999999999999999",
              "99999999999999999999999999999999999999.99999999999999999999999999999999999999",
              "0.00000000000000000000000000000000000000"}},
    }};
    std::array<bool, 5> seen_rows {};
    size_t total_rows = 0;
    bool eos = false;
    while (!eos) {
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        if (eos) {
            continue;
        }
        const auto& row_ids = assert_cast<const ColumnInt64&>(*block.get_by_position(0).column);
        for (size_t row = 0; row < block.rows(); ++row) {
            const auto row_id = row_ids.get_data()[row];
            ASSERT_GE(row_id, 1);
            ASSERT_LE(row_id, 4);
            EXPECT_FALSE(seen_rows[row_id]);
            seen_rows[row_id] = true;
            for (size_t column = 0; column < decimal_specs.size(); ++column) {
                const auto& decimal_column = assert_cast<const ColumnNullable&>(
                        *block.get_by_position(column + 1).column);
                if (row_id == 3) {
                    EXPECT_EQ(1, decimal_column.get_null_map_data()[row]);
                } else {
                    EXPECT_EQ(0, decimal_column.get_null_map_data()[row]);
                    const auto expected_index = row_id == 1 ? 0 : row_id == 2 ? 1 : 2;
                    EXPECT_EQ(expected_values[column][expected_index],
                              columns[column + 1].type->to_string(decimal_column, row));
                }
            }
        }
        total_rows += block.rows();
    }
    EXPECT_EQ(4U, total_rows);
    EXPECT_TRUE(seen_rows[1]);
    EXPECT_TRUE(seen_rows[2]);
    EXPECT_TRUE(seen_rows[3]);
    EXPECT_TRUE(seen_rows[4]);
    EXPECT_TRUE(reader.close().ok());
}

TEST(LanceTableReaderTypeTest, ReadsStringAndBinaryTypesFromAllTypesFixture) {
    // The committed fixture contains four rows covering values, nulls, and boundary cases.
    const std::filesystem::path dataset_uri =
            "./be/test/format_v2/table/lance/data/all_types.lance";
    LanceFixtureInfo fixture;
    ASSERT_TRUE(get_fixture_info(dataset_uri, &fixture).ok());
    ASSERT_FALSE(fixture.fragment_ids.empty());
    const Columns columns {
            projected_column("row_id", TYPE_BIGINT, false),
            projected_column("text_value", TYPE_STRING, true),
            projected_column("binary_value", TYPE_VARBINARY, true),
    };

    TQueryOptions query_options;
    query_options.__set_batch_size(4);
    TQueryGlobals query_globals;
    RuntimeState state(query_globals);
    state.set_query_options(query_options);
    RuntimeProfile profile("lance_string_binary_types_fixture");
    TFileScanRangeParams scan_params;
    LanceTableReader reader;
    ASSERT_TRUE(init_reader(&reader, columns, &state, &profile, &scan_params).ok());
    ASSERT_TRUE(prepare_fixture(&reader, dataset_uri, fixture, fixture.fragment_ids).ok());

    Block block;
    add_output_columns(&block, columns);
    std::array<bool, 5> seen_rows {};
    size_t total_rows = 0;
    bool eos = false;
    while (!eos) {
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        if (eos) {
            continue;
        }
        const auto& row_ids = assert_cast<const ColumnInt64&>(*block.get_by_position(0).column);
        const auto& texts = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
        const auto& binaries = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
        for (size_t row = 0; row < block.rows(); ++row) {
            const auto row_id = row_ids.get_data()[row];
            ASSERT_GE(row_id, 1);
            ASSERT_LE(row_id, 4);
            EXPECT_FALSE(seen_rows[row_id]);
            seen_rows[row_id] = true;
            if (row_id == 3) {
                EXPECT_EQ(1, texts.get_null_map_data()[row]);
                EXPECT_EQ(1, binaries.get_null_map_data()[row]);
                continue;
            }
            EXPECT_EQ(0, texts.get_null_map_data()[row]);
            EXPECT_EQ(0, binaries.get_null_map_data()[row]);
            const auto text = texts.get_nested_column().get_data_at(row).to_string();
            const auto binary = binaries.get_nested_column().get_data_at(row).to_string();
            if (row_id == 1) {
                EXPECT_EQ("", text);
                EXPECT_EQ("", binary);
            } else if (row_id == 2) {
                EXPECT_EQ("Doris 与 Lance ��", text);
                EXPECT_EQ(std::string("\x00\x01\xFF\x7F", 4), binary);
            } else {
                EXPECT_EQ(
                        "a moderately long text value used to exercise variable-length Arrow "
                        "buffers",
                        text);
                EXPECT_EQ("lance", binary);
            }
        }
        total_rows += block.rows();
    }
    EXPECT_EQ(4U, total_rows);
    EXPECT_TRUE(seen_rows[1]);
    EXPECT_TRUE(seen_rows[2]);
    EXPECT_TRUE(seen_rows[3]);
    EXPECT_TRUE(seen_rows[4]);
    EXPECT_TRUE(reader.close().ok());
}

TEST(LanceTableReaderTypeTest, ReadsComplexTypesFromAllTypesFixture) {
    // The committed fixture contains four rows covering values, nulls, and boundary cases.
    const std::filesystem::path dataset_uri =
            "./be/test/format_v2/table/lance/data/all_types.lance";
    LanceFixtureInfo fixture;
    ASSERT_TRUE(get_fixture_info(dataset_uri, &fixture).ok());
    ASSERT_FALSE(fixture.fragment_ids.empty());

    const auto int_type = nullable_type(TYPE_INT);
    const auto string_type = nullable_type(TYPE_STRING);
    const auto int_array = make_nullable(std::make_shared<DataTypeArray>(int_type));
    const auto attributes = make_nullable(std::make_shared<DataTypeMap>(string_type, int_type));
    const auto profile = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {string_type, int_type}, Strings {"city", "level"}));
    const auto visit = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {string_type, int_type}, Strings {"page", "duration_seconds"}));
    const auto visits = make_nullable(std::make_shared<DataTypeArray>(visit));
    const auto scores = make_nullable(std::make_shared<DataTypeMap>(
            string_type, make_nullable(std::make_shared<DataTypeArray>(int_type))));
    const Columns columns {
            projected_column("row_id", TYPE_BIGINT, false),
            projected_column("int_array", int_array),
            projected_column("attributes", attributes),
            projected_column("profile", profile),
            projected_column("visits", visits),
            projected_column("scores_by_source", scores),
    };

    TQueryOptions query_options;
    query_options.__set_batch_size(3);
    TQueryGlobals query_globals;
    RuntimeState state(query_globals);
    state.set_query_options(query_options);
    RuntimeProfile profile_stats("lance_complex_types_fixture");
    TFileScanRangeParams scan_params;
    LanceTableReader reader;
    ASSERT_TRUE(init_reader(&reader, columns, &state, &profile_stats, &scan_params).ok());
    ASSERT_TRUE(prepare_fixture(&reader, dataset_uri, fixture, fixture.fragment_ids).ok());

    Block block;
    add_output_columns(&block, columns);
    std::array<bool, 5> seen_rows {};
    size_t total_rows = 0;
    bool eos = false;
    while (!eos) {
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        if (eos) {
            continue;
        }
        const auto& row_ids = assert_cast<const ColumnInt64&>(*block.get_by_position(0).column);
        const auto& int_arrays_column =
                assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
        const auto& attributes_column =
                assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
        const auto& profiles = assert_cast<const ColumnNullable&>(*block.get_by_position(3).column);
        const auto& visits_column =
                assert_cast<const ColumnNullable&>(*block.get_by_position(4).column);
        const auto& scores_column =
                assert_cast<const ColumnNullable&>(*block.get_by_position(5).column);
        const auto& int_arrays =
                assert_cast<const ColumnArray&>(int_arrays_column.get_nested_column());
        const auto& int_array_items = assert_cast<const ColumnNullable&>(int_arrays.get_data());
        const auto& int_array_values =
                assert_cast<const ColumnInt32&>(int_array_items.get_nested_column());
        const auto& attribute_map =
                assert_cast<const ColumnMap&>(attributes_column.get_nested_column());
        const auto& attribute_keys = assert_cast<const ColumnNullable&>(attribute_map.get_keys());
        const auto& attribute_key_values =
                assert_cast<const ColumnString&>(attribute_keys.get_nested_column());
        const auto& attribute_items =
                assert_cast<const ColumnNullable&>(attribute_map.get_values());
        const auto& attribute_item_values =
                assert_cast<const ColumnInt32&>(attribute_items.get_nested_column());
        const auto& profile_struct = assert_cast<const ColumnStruct&>(profiles.get_nested_column());
        const auto& profile_cities =
                assert_cast<const ColumnNullable&>(profile_struct.get_column(0));
        const auto& profile_city_values =
                assert_cast<const ColumnString&>(profile_cities.get_nested_column());
        const auto& profile_levels =
                assert_cast<const ColumnNullable&>(profile_struct.get_column(1));
        const auto& profile_level_values =
                assert_cast<const ColumnInt32&>(profile_levels.get_nested_column());
        const auto& visits = assert_cast<const ColumnArray&>(visits_column.get_nested_column());
        const auto& visit_items = assert_cast<const ColumnNullable&>(visits.get_data());
        const auto& visit_struct =
                assert_cast<const ColumnStruct&>(visit_items.get_nested_column());
        const auto& visit_pages = assert_cast<const ColumnNullable&>(visit_struct.get_column(0));
        const auto& visit_page_values =
                assert_cast<const ColumnString&>(visit_pages.get_nested_column());
        const auto& visit_durations =
                assert_cast<const ColumnNullable&>(visit_struct.get_column(1));
        const auto& visit_duration_values =
                assert_cast<const ColumnInt32&>(visit_durations.get_nested_column());
        const auto& score_map = assert_cast<const ColumnMap&>(scores_column.get_nested_column());
        const auto& score_keys = assert_cast<const ColumnNullable&>(score_map.get_keys());
        const auto& score_key_values =
                assert_cast<const ColumnString&>(score_keys.get_nested_column());
        const auto& score_items = assert_cast<const ColumnNullable&>(score_map.get_values());
        const auto& score_arrays = assert_cast<const ColumnArray&>(score_items.get_nested_column());
        const auto& score_array_items = assert_cast<const ColumnNullable&>(score_arrays.get_data());
        const auto& score_array_values =
                assert_cast<const ColumnInt32&>(score_array_items.get_nested_column());
        for (size_t row = 0; row < block.rows(); ++row) {
            const auto row_id = row_ids.get_data()[row];
            ASSERT_GE(row_id, 1);
            ASSERT_LE(row_id, 4);
            EXPECT_FALSE(seen_rows[row_id]);
            seen_rows[row_id] = true;
            if (row_id == 1) {
                EXPECT_EQ(0, int_arrays_column.get_null_map_data()[row]);
                const auto [int_begin, int_end] = array_range(int_arrays, row);
                ASSERT_EQ(3U, int_end - int_begin);
                EXPECT_EQ(0, int_array_items.get_null_map_data()[int_begin]);
                EXPECT_EQ(1, int_array_values.get_data()[int_begin]);
                EXPECT_EQ(1, int_array_items.get_null_map_data()[int_begin + 1]);
                EXPECT_EQ(0, int_array_items.get_null_map_data()[int_begin + 2]);
                EXPECT_EQ(3, int_array_values.get_data()[int_begin + 2]);

                EXPECT_EQ(0, attributes_column.get_null_map_data()[row]);
                const auto [attribute_begin, attribute_end] = map_range(attribute_map, row);
                ASSERT_EQ(2U, attribute_end - attribute_begin);
                EXPECT_EQ("views", attribute_key_values.get_data_at(attribute_begin).to_string());
                EXPECT_EQ(10, attribute_item_values.get_data()[attribute_begin]);
                EXPECT_EQ("likes",
                          attribute_key_values.get_data_at(attribute_begin + 1).to_string());
                EXPECT_EQ(2, attribute_item_values.get_data()[attribute_begin + 1]);

                EXPECT_EQ(0, profiles.get_null_map_data()[row]);
                EXPECT_EQ(0, profile_cities.get_null_map_data()[row]);
                EXPECT_EQ("Beijing", profile_city_values.get_data_at(row).to_string());
                EXPECT_EQ(0, profile_levels.get_null_map_data()[row]);
                EXPECT_EQ(7, profile_level_values.get_data()[row]);

                EXPECT_EQ(0, visits_column.get_null_map_data()[row]);
                const auto [visit_begin, visit_end] = array_range(visits, row);
                ASSERT_EQ(2U, visit_end - visit_begin);
                EXPECT_EQ(0, visit_items.get_null_map_data()[visit_begin]);
                EXPECT_EQ("home", visit_page_values.get_data_at(visit_begin).to_string());
                EXPECT_EQ(0, visit_durations.get_null_map_data()[visit_begin]);
                EXPECT_EQ(5, visit_duration_values.get_data()[visit_begin]);
                EXPECT_EQ(0, visit_items.get_null_map_data()[visit_begin + 1]);
                EXPECT_EQ("search", visit_page_values.get_data_at(visit_begin + 1).to_string());
                EXPECT_EQ(1, visit_durations.get_null_map_data()[visit_begin + 1]);

                EXPECT_EQ(0, scores_column.get_null_map_data()[row]);
                const auto [score_begin, score_end] = map_range(score_map, row);
                ASSERT_EQ(2U, score_end - score_begin);
                EXPECT_EQ("organic", score_key_values.get_data_at(score_begin).to_string());
                const auto [organic_begin, organic_end] = array_range(score_arrays, score_begin);
                ASSERT_EQ(2U, organic_end - organic_begin);
                EXPECT_EQ(1, score_array_values.get_data()[organic_begin]);
                EXPECT_EQ(2, score_array_values.get_data()[organic_begin + 1]);
                EXPECT_EQ("ad", score_key_values.get_data_at(score_begin + 1).to_string());
                const auto [ad_begin, ad_end] = array_range(score_arrays, score_begin + 1);
                ASSERT_EQ(1U, ad_end - ad_begin);
                EXPECT_EQ(3, score_array_values.get_data()[ad_begin]);
            } else if (row_id == 2) {
                EXPECT_EQ(0, int_arrays_column.get_null_map_data()[row]);
                const auto [int_begin, int_end] = array_range(int_arrays, row);
                EXPECT_EQ(int_begin, int_end);

                EXPECT_EQ(0, attributes_column.get_null_map_data()[row]);
                const auto [attribute_begin, attribute_end] = map_range(attribute_map, row);
                EXPECT_EQ(attribute_begin, attribute_end);

                EXPECT_EQ(0, profiles.get_null_map_data()[row]);
                EXPECT_EQ(0, profile_cities.get_null_map_data()[row]);
                EXPECT_EQ("", profile_city_values.get_data_at(row).to_string());
                EXPECT_EQ(0, profile_levels.get_null_map_data()[row]);
                EXPECT_EQ(0, profile_level_values.get_data()[row]);

                EXPECT_EQ(0, visits_column.get_null_map_data()[row]);
                const auto [visit_begin, visit_end] = array_range(visits, row);
                EXPECT_EQ(visit_begin, visit_end);

                EXPECT_EQ(0, scores_column.get_null_map_data()[row]);
                const auto [score_begin, score_end] = map_range(score_map, row);
                EXPECT_EQ(score_begin, score_end);
            } else {
                EXPECT_EQ(1, int_arrays_column.get_null_map_data()[row]);
                EXPECT_EQ(1, attributes_column.get_null_map_data()[row]);
                EXPECT_EQ(1, profiles.get_null_map_data()[row]);
                EXPECT_EQ(1, visits_column.get_null_map_data()[row]);
                EXPECT_EQ(1, scores_column.get_null_map_data()[row]);
            }
        }
        total_rows += block.rows();
    }
    EXPECT_EQ(4U, total_rows);
    EXPECT_TRUE(seen_rows[1]);
    EXPECT_TRUE(seen_rows[2]);
    EXPECT_TRUE(seen_rows[3]);
    EXPECT_TRUE(seen_rows[4]);
    EXPECT_TRUE(reader.close().ok());
}

TEST(LanceTableReaderTypeTest, ReadsVectorTypesFromAllTypesFixture) {
    // The committed fixture contains four rows covering values, nulls, and boundary cases.
    const std::filesystem::path dataset_uri =
            "./be/test/format_v2/table/lance/data/all_types.lance";
    LanceFixtureInfo fixture;
    ASSERT_TRUE(get_fixture_info(dataset_uri, &fixture).ok());
    ASSERT_FALSE(fixture.fragment_ids.empty());

    const auto embedding =
            make_nullable(std::make_shared<DataTypeArray>(nullable_type(TYPE_FLOAT)));
    const Columns columns {
            projected_column("row_id", TYPE_BIGINT, false),
            projected_column("label", TYPE_STRING, true),
            projected_column("embedding", embedding),
    };
    TQueryOptions query_options;
    query_options.__set_batch_size(3);
    TQueryGlobals query_globals;
    RuntimeState state(query_globals);
    state.set_query_options(query_options);
    RuntimeProfile profile("lance_vector_types_fixture");
    TFileScanRangeParams scan_params;
    LanceTableReader reader;
    ASSERT_TRUE(init_reader(&reader, columns, &state, &profile, &scan_params).ok());
    ASSERT_TRUE(prepare_fixture(&reader, dataset_uri, fixture, fixture.fragment_ids).ok());

    Block block;
    add_output_columns(&block, columns);
    std::array<bool, 5> seen_rows {};
    size_t total_rows = 0;
    bool eos = false;
    while (!eos) {
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        if (eos) {
            continue;
        }
        const auto& row_ids = assert_cast<const ColumnInt64&>(*block.get_by_position(0).column);
        const auto& labels = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
        const auto& label_values = assert_cast<const ColumnString&>(labels.get_nested_column());
        const auto& embeddings =
                assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
        const auto& embedding_arrays =
                assert_cast<const ColumnArray&>(embeddings.get_nested_column());
        const auto& embedding_items =
                assert_cast<const ColumnNullable&>(embedding_arrays.get_data());
        const auto& embedding_values =
                assert_cast<const ColumnFloat32&>(embedding_items.get_nested_column());
        const auto expect_embedding = [&](size_t row, const std::string& label,
                                          const std::array<float, 3>& expected) {
            EXPECT_EQ(0, labels.get_null_map_data()[row]);
            EXPECT_EQ(label, label_values.get_data_at(row).to_string());
            EXPECT_EQ(0, embeddings.get_null_map_data()[row]);
            const auto [begin, end] = array_range(embedding_arrays, row);
            ASSERT_EQ(3U, end - begin);
            for (size_t i = 0; i < expected.size(); ++i) {
                EXPECT_EQ(0, embedding_items.get_null_map_data()[begin + i]);
                EXPECT_FLOAT_EQ(expected[i], embedding_values.get_data()[begin + i]);
            }
        };
        for (size_t row = 0; row < block.rows(); ++row) {
            const auto row_id = row_ids.get_data()[row];
            ASSERT_GE(row_id, 1);
            ASSERT_LE(row_id, 4);
            EXPECT_FALSE(seen_rows[row_id]);
            seen_rows[row_id] = true;
            if (row_id == 1) {
                expect_embedding(row, "origin", {0.0F, 0.0F, 0.0F});
            } else if (row_id == 2) {
                expect_embedding(row, "unit-x", {1.0F, 0.0F, 0.0F});
            } else if (row_id == 3) {
                expect_embedding(row, "mixed", {-1.5F, 0.25F, 3.75F});
            } else {
                expect_embedding(row, "extra", {2.0F, -2.0F, 0.5F});
            }
        }
        total_rows += block.rows();
    }
    EXPECT_EQ(4U, total_rows);
    EXPECT_TRUE(seen_rows[1]);
    EXPECT_TRUE(seen_rows[2]);
    EXPECT_TRUE(seen_rows[3]);
    EXPECT_TRUE(seen_rows[4]);
    EXPECT_TRUE(reader.close().ok());
}

TEST(LanceTableReaderTypeTest, ReadsTemporalTypesFromAllTypesFixture) {
    // The committed fixture contains four rows covering values, nulls, and boundary cases.
    const std::filesystem::path dataset_uri =
            "./be/test/format_v2/table/lance/data/all_types.lance";
    LanceFixtureInfo fixture;
    ASSERT_TRUE(get_fixture_info(dataset_uri, &fixture).ok());
    ASSERT_FALSE(fixture.fragment_ids.empty());
    const Columns columns {
            projected_column("row_id", TYPE_BIGINT, false),
            projected_column("date_value", TYPE_DATEV2, true),
            projected_column("timestamp_value", nullable_type(TYPE_DATETIMEV2, 0, 6)),
    };

    // Spark generated this fixture with spark.sql.session.timeZone=Asia/Shanghai (+08:00).
    TimezoneUtils::load_offsets_to_cache();
    TQueryOptions query_options;
    query_options.__set_batch_size(4);
    TQueryGlobals query_globals;
    query_globals.__set_time_zone("+08:00");
    RuntimeState state(query_globals);
    state.set_query_options(query_options);
    RuntimeProfile profile("lance_temporal_types_fixture");
    TFileScanRangeParams scan_params;
    LanceTableReader reader;
    ASSERT_TRUE(init_reader(&reader, columns, &state, &profile, &scan_params).ok());
    ASSERT_TRUE(prepare_fixture(&reader, dataset_uri, fixture, fixture.fragment_ids).ok());

    Block block;
    add_output_columns(&block, columns);
    std::array<bool, 5> seen_rows {};
    size_t total_rows = 0;
    bool eos = false;
    while (!eos) {
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        if (eos) {
            continue;
        }
        const auto& row_ids = assert_cast<const ColumnInt64&>(*block.get_by_position(0).column);
        const auto& dates = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
        const auto& timestamps =
                assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
        for (size_t row = 0; row < block.rows(); ++row) {
            const auto row_id = row_ids.get_data()[row];
            ASSERT_GE(row_id, 1);
            ASSERT_LE(row_id, 4);
            EXPECT_FALSE(seen_rows[row_id]);
            seen_rows[row_id] = true;
            if (row_id == 1) {
                EXPECT_EQ("1969-12-31", columns[1].type->to_string(dates, row));
                EXPECT_EQ("1969-12-31 23:59:59.123456",
                          columns[2].type->to_string(timestamps, row));
            } else if (row_id == 2) {
                EXPECT_EQ("1970-01-01", columns[1].type->to_string(dates, row));
                EXPECT_EQ("1970-01-01 08:00:00.000001",
                          columns[2].type->to_string(timestamps, row));
            } else if (row_id == 3) {
                EXPECT_EQ("2024-02-29", columns[1].type->to_string(dates, row));
                EXPECT_EQ("2024-02-29 12:34:56.654321",
                          columns[2].type->to_string(timestamps, row));
            } else {
                EXPECT_EQ(1, dates.get_null_map_data()[row]);
                EXPECT_EQ(1, timestamps.get_null_map_data()[row]);
            }
        }
        total_rows += block.rows();
    }
    EXPECT_EQ(4U, total_rows);
    EXPECT_TRUE(seen_rows[1]);
    EXPECT_TRUE(seen_rows[2]);
    EXPECT_TRUE(seen_rows[3]);
    EXPECT_TRUE(seen_rows[4]);
    EXPECT_TRUE(reader.close().ok());
}

} // namespace doris::format::lance
