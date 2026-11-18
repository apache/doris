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

#include <bit>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <string>
#include <utility>
#include <vector>

#include "core/block/block.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/field.h"
#include "format_v2/table/paimon_rust_predicate_converter.h"
#include "format_v2/table/paimon_rust_table_reader.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/io_common.h"
#include "runtime/runtime_state.h"

namespace doris::format::paimon {
namespace {

constexpr int64_t kFixtureRows = 2048;

testing::AssertionResult is_ok(const Status& status) {
    if (status.ok()) {
        return testing::AssertionSuccess();
    }
    return testing::AssertionFailure() << status;
}

std::string read_fixture(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    EXPECT_TRUE(input.good()) << path;
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

class PaimonRustVectorReaderTest : public testing::Test {
protected:
    void SetUp() override {
        TQueryOptions options;
        options.__set_batch_size(2);
        _state = RuntimeState::create_unique(options, TQueryGlobals {});
        // run-be-ut.sh runs from the repository root; ccache can make __FILE__ relative
        // to the build directory instead, so it is not a stable fixture location.
        _fixture = std::filesystem::canonical("be/test/exec/test_data/paimon_rust_vector");
        set_request(false, TVectorMetric::L2, kFixtureRows + 1);
    }

    void set_request(bool tvf, TVectorMetric::type metric, int64_t top_k) {
        TSearchVector query;
        query.__set_element_type(TVectorElementType::FLOAT32);
        query.__set_dimension(2);
        std::string values;
        for (float value : {metric == TVectorMetric::DOT_PRODUCT ? 1.0F : 0.0F, 0.0F}) {
            uint32_t bits = std::bit_cast<uint32_t>(value);
            for (int byte = 0; byte < 4; ++byte) {
                values.push_back(static_cast<char>(bits >> (8 * byte)));
            }
        }
        query.__set_values(values);
        TVectorSearchParams vector;
        vector.__set_column("embedding");
        vector.__set_query_vector(query);
        vector.__set_top_k(top_k);
        vector.__set_offset(0);
        vector.__set_metric(metric);
        TExternalSearchQuery search;
        search.__set_vector_search(vector);
        TExternalSearchRequest request;
        request.__set_schema_version(1);
        request.__set_search_query(search);
        TPaimonVectorSearchOptions paimon;
        paimon.__set_tvf_distance(tvf);
        request.__set_paimon_options(paimon);
        _params.__set_external_search_request(request);
        _distance_name = tvf ? "_distance" : kPaimonSearchDistanceColumn;
    }

    Status init(PaimonRustTableReader& reader, bool score_only = false) {
        std::vector<ColumnDefinition> columns;
        if (!score_only) {
            columns.push_back({.identifier = Field {},
                               .name = "id",
                               .type = make_nullable(std::make_shared<DataTypeInt32>())});
            columns.push_back({.identifier = Field {},
                               .name = "embedding",
                               .type = make_nullable(std::make_shared<DataTypeArray>(
                                       make_nullable(std::make_shared<DataTypeFloat32>())))});
        }
        columns.push_back({.identifier = Field {},
                           .name = _distance_name,
                           .type = std::make_shared<DataTypeFloat32>()});
        return reader.init({.projected_columns = std::move(columns),
                            .conjuncts = {},
                            .format = FileFormat::JNI,
                            .scan_params = &_params,
                            .io_ctx = _io_context,
                            .runtime_state = _state.get(),
                            .scanner_profile = nullptr});
    }

    SplitReadOptions split(const std::string& name = "full", bool missing_bucket = false) const {
        auto bytes = read_fixture(_fixture / (name + ".bin"));
        auto placeholder = bytes.find("__BUCKET__");
        EXPECT_NE(placeholder, std::string::npos);
        EXPECT_GE(placeholder, 2);
        std::string bucket = (_fixture / (missing_bucket ? "missing-bucket" : "bucket-0")).string();
        const std::string length {static_cast<char>(bucket.size() >> 8),
                                  static_cast<char>(bucket.size() & 0xff)};
        bytes.replace(placeholder - 2, 2 + std::string("__BUCKET__").size(), length + bucket);
        TPaimonVectorPayload payload;
        payload.__set_payload_type(TPaimonVectorPayloadType::BUCKET_SPLIT_BYTES);
        payload.__set_bytes(bytes);
        TPaimonFileDesc paimon;
        paimon.__set_reader_type(TPaimonReaderType::PAIMON_RUST);
        paimon.__set_paimon_split("");
        paimon.__set_vector_payload(payload);
        paimon.__set_paimon_table(_fixture.string());
        paimon.__set_db_name("db");
        paimon.__set_table_name("vector_fixture");
        bool dot = _params.external_search_request.search_query.vector_search.metric ==
                   TVectorMetric::DOT_PRODUCT;
        paimon.__set_paimon_table_schema_json(
                read_fixture(_fixture / (dot ? "schema-dot.json" : "schema.json")));
        TTableFormatFileDesc format;
        format.__set_table_format_type("paimon");
        format.__set_paimon_params(paimon);
        SplitReadOptions options;
        options.current_range.__set_table_format_params(format);
        options.current_split_format = FileFormat::JNI;
        options.all_runtime_filters_applied = true;
        return options;
    }

    std::vector<std::pair<int64_t, float>> read_rows(PaimonRustTableReader& reader) {
        std::vector<std::pair<int64_t, float>> result;
        for (int batch = 0; batch <= kFixtureRows; ++batch) {
            Block block(
                    {ColumnWithTypeAndName(
                             make_nullable(std::make_shared<DataTypeInt32>())->create_column(),
                             make_nullable(std::make_shared<DataTypeInt32>()), "id"),
                     ColumnWithTypeAndName(
                             make_nullable(std::make_shared<DataTypeArray>(make_nullable(
                                                   std::make_shared<DataTypeFloat32>())))
                                     ->create_column(),
                             make_nullable(std::make_shared<DataTypeArray>(
                                     make_nullable(std::make_shared<DataTypeFloat32>()))),
                             "embedding"),
                     ColumnWithTypeAndName(std::make_shared<DataTypeFloat32>()->create_column(),
                                           std::make_shared<DataTypeFloat32>(), _distance_name)});
            bool eos = false;
            auto status = reader.get_block(&block, &eos);
            if (!status.ok()) {
                ADD_FAILURE() << status;
                return result;
            }
            for (size_t row = 0; row < block.rows(); ++row) {
                Field id, vector, distance;
                block.get_by_position(0).column->get(row, id);
                block.get_by_position(1).column->get(row, vector);
                block.get_by_position(2).column->get(row, distance);
                EXPECT_FALSE(id.is_null());
                EXPECT_FALSE(vector.is_null());
                if (id.is_null() || vector.is_null()) {
                    continue;
                }
                const auto& coordinates = vector.get<TYPE_ARRAY>();
                EXPECT_EQ(2, coordinates.size());
                if (coordinates.size() == 2) {
                    EXPECT_FLOAT_EQ(static_cast<float>(id.get<TYPE_INT>()),
                                    coordinates[0].get<TYPE_FLOAT>());
                    EXPECT_FLOAT_EQ(0, coordinates[1].get<TYPE_FLOAT>());
                }
                result.emplace_back(id.get<TYPE_INT>(),
                                    static_cast<float>(distance.get<TYPE_FLOAT>()));
            }
            if (eos) {
                return result;
            }
        }
        ADD_FAILURE() << "vector reader did not reach EOF";
        return result;
    }

    std::shared_ptr<io::IOContext> _io_context = std::make_shared<io::IOContext>();
    std::unique_ptr<RuntimeState> _state;
    std::filesystem::path _fixture;
    TFileScanRangeParams _params;
    std::string _distance_name;
};

TEST_F(PaimonRustVectorReaderTest, ExactReadMaterializesRowsAcrossCompactedFiles) {
    PaimonRustTableReader reader;
    ASSERT_TRUE(is_ok(init(reader)));
    ASSERT_TRUE(is_ok(reader.prepare_split(split())));
    auto rows = read_rows(reader);
    ASSERT_EQ(kFixtureRows, rows.size());
    for (int64_t id = 0; id < kFixtureRows; ++id) {
        EXPECT_EQ(id, rows[id].first);
        EXPECT_FLOAT_EQ(static_cast<float>(id), rows[id].second);
    }
    EXPECT_TRUE(read_rows(reader).empty()); // EOF is stable on repeated calls.
    EXPECT_TRUE(is_ok(reader.close()));
}

TEST_F(PaimonRustVectorReaderTest, SqlAndTvfMetricsRemainCorrectWhenReusingReader) {
    // In each SQL/TVF mode, reuse the reader across metric/schema generations.
    // A previous split's score transform must not leak into the next request.
    for (bool tvf : {false, true}) {
        PaimonRustTableReader reader;
        set_request(tvf, TVectorMetric::L2, 4);
        ASSERT_TRUE(is_ok(init(reader)));
        for (auto metric : {TVectorMetric::L2, TVectorMetric::DOT_PRODUCT, TVectorMetric::L2}) {
            set_request(tvf, metric, 4);
            ASSERT_TRUE(is_ok(reader.prepare_split(split())));
            auto rows = read_rows(reader);
            std::vector<int64_t> expected = metric == TVectorMetric::L2
                                                    ? std::vector<int64_t> {0, 1, 2, 3}
                                                    : std::vector<int64_t> {2047, 2046, 2045, 2044};
            ASSERT_EQ(expected.size(), rows.size());
            for (size_t i = 0; i < expected.size(); ++i) {
                EXPECT_EQ(expected[i], rows[i].first);
                float distance = static_cast<float>(expected[i]);
                if (tvf) {
                    distance = metric == TVectorMetric::L2 ? distance * distance : -distance;
                }
                EXPECT_FLOAT_EQ(distance, rows[i].second);
            }
            ASSERT_TRUE(is_ok(reader.abort_split()));
        }
        EXPECT_TRUE(is_ok(reader.close()));
    }
}

TEST_F(PaimonRustVectorReaderTest, RowRangesExcludeClosestRowsAndNullVectors) {
    // Only the TVF contract skips NULL vectors; SQL nullability is guarded by FE.
    _fixture /= "nullable";
    set_request(true, TVectorMetric::L2, kFixtureRows + 1);
    PaimonRustTableReader reader;
    ASSERT_TRUE(is_ok(init(reader)));
    ASSERT_TRUE(is_ok(reader.prepare_split(split())));
    EXPECT_EQ(kFixtureRows - 1, read_rows(reader).size());
    ASSERT_TRUE(is_ok(reader.abort_split()));
    ASSERT_TRUE(is_ok(reader.prepare_split(split("ranges"))));
    auto rows = read_rows(reader);
    std::vector<int64_t> expected {4, 6, 1001, 1002, 1025, 1026, 2025, 2026};
    ASSERT_EQ(expected.size(), rows.size());
    for (size_t row = 0; row < expected.size(); ++row) {
        EXPECT_EQ(expected[row], rows[row].first);
        float value = static_cast<float>(expected[row]);
        EXPECT_FLOAT_EQ(value * value, rows[row].second);
    }
    ASSERT_TRUE(is_ok(reader.abort_split()));
    ASSERT_TRUE(is_ok(reader.prepare_split(split("empty"))));
    EXPECT_TRUE(read_rows(reader).empty());
    EXPECT_TRUE(is_ok(reader.close()));
}

TEST_F(PaimonRustVectorReaderTest, FailedOpenDoesNotPoisonFollowingSplit) {
    PaimonRustTableReader reader;
    ASSERT_TRUE(is_ok(init(reader)));
    auto truncated = split();
    truncated.current_range.table_format_params.paimon_params.vector_payload.bytes.resize(4);
    auto status = reader.prepare_split(truncated);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos, status.to_string().find("deserialize")) << status;
    ASSERT_TRUE(is_ok(reader.abort_split()));
    ASSERT_TRUE(is_ok(reader.prepare_split(split())));
    EXPECT_EQ(kFixtureRows, read_rows(reader).size());
    ASSERT_TRUE(is_ok(reader.abort_split()));
    status = reader.prepare_split(split("full", true));
    EXPECT_FALSE(status.ok());
    ASSERT_TRUE(is_ok(reader.abort_split()));
    ASSERT_TRUE(is_ok(reader.abort_split()));
    ASSERT_TRUE(is_ok(reader.prepare_split(split())));
    EXPECT_EQ(kFixtureRows, read_rows(reader).size());
    EXPECT_TRUE(is_ok(reader.close()));
    EXPECT_TRUE(is_ok(reader.close()));
}

TEST_F(PaimonRustVectorReaderTest, CancellationClosesStreamAndAllowsNextSplit) {
    PaimonRustTableReader reader;
    ASSERT_TRUE(is_ok(init(reader)));
    ASSERT_TRUE(is_ok(reader.prepare_split(split())));
    _io_context->should_stop = true;
    EXPECT_TRUE(read_rows(reader).empty());
    EXPECT_TRUE(read_rows(reader).empty());
    ASSERT_TRUE(is_ok(reader.abort_split()));
    _io_context->should_stop = false;
    ASSERT_TRUE(is_ok(reader.prepare_split(split())));
    EXPECT_EQ(kFixtureRows, read_rows(reader).size());
    EXPECT_TRUE(is_ok(reader.close()));
}

TEST_F(PaimonRustVectorReaderTest, ScoreOnlyProjectionStillReturnsDistances) {
    PaimonRustTableReader reader;
    set_request(true, TVectorMetric::L2, 4);
    ASSERT_TRUE(is_ok(init(reader, true)));
    ASSERT_TRUE(is_ok(reader.prepare_split(split())));
    std::vector<float> distances;
    bool eos = false;
    for (int batch = 0; batch <= kFixtureRows && !eos; ++batch) {
        auto type = std::make_shared<DataTypeFloat32>();
        Block block({ColumnWithTypeAndName(type->create_column(), type, "_distance")});
        auto status = reader.get_block(&block, &eos);
        ASSERT_TRUE(status.ok()) << status;
        for (size_t row = 0; row < block.rows(); ++row) {
            Field distance;
            block.get_by_position(0).column->get(row, distance);
            distances.push_back(static_cast<float>(distance.get<TYPE_FLOAT>()));
        }
    }
    EXPECT_TRUE(eos);
    EXPECT_EQ((std::vector<float> {0, 1, 4, 9}), distances);
    EXPECT_TRUE(is_ok(reader.close()));
}

TEST_F(PaimonRustVectorReaderTest, RejectsMalformedRequestsWithoutOpeningDataFiles) {
    using Request = TExternalSearchRequest;
    std::vector<std::pair<std::string, std::function<void(Request&)>>> cases {
            {"schema version", [](Request& r) { r.__set_schema_version(2); }},
            {"vector query", [](Request& r) { r.__isset.search_query = false; }},
            {"vector query", [](Request& r) { r.search_query.__isset.vector_search = false; }},
            {"non-empty column", [](Request& r) { r.search_query.vector_search.__set_column(""); }},
            {"non-empty column",
             [](Request& r) { r.search_query.vector_search.__set_column(std::string("e\0x", 3)); }},
            {"query vector",
             [](Request& r) { r.search_query.vector_search.__isset.query_vector = false; }},
            {"element_type",
             [](Request& r) {
                 r.search_query.vector_search.query_vector.__isset.element_type = false;
             }},
            {"dimension",
             [](Request& r) { r.search_query.vector_search.query_vector.__set_dimension(0); }},
            {"FLOAT32",
             [](Request& r) {
                 r.search_query.vector_search.query_vector.__set_element_type(
                         TVectorElementType::INT8);
             }},
            {"byte size",
             [](Request& r) { r.search_query.vector_search.query_vector.__set_values("bad"); }},
            {"top_k", [](Request& r) { r.search_query.vector_search.__isset.top_k = false; }},
            {"top_k", [](Request& r) { r.search_query.vector_search.__set_top_k(-1); }},
            {"offset", [](Request& r) { r.search_query.vector_search.__set_offset(-1); }},
            {"metric", [](Request& r) { r.search_query.vector_search.__isset.metric = false; }},
            {"unsupported distance metric",
             [](Request& r) { r.search_query.vector_search.__set_metric(TVectorMetric::COSINE); }},
            {"embedded NUL", [](Request& r) {
                 r.paimon_options.__set_options({{"bad", std::string("a\0b", 3)}});
             }}};
    PaimonRustTableReader reader;
    ASSERT_TRUE(is_ok(init(reader)));
    for (const auto& [message, mutate] : cases) {
        SCOPED_TRACE(message);
        set_request(false, TVectorMetric::L2, 4);
        mutate(_params.external_search_request);
        auto status = reader.prepare_split(split("full", true));
        EXPECT_FALSE(status.ok());
        EXPECT_NE(std::string::npos, status.to_string().find(message)) << status;
        ASSERT_TRUE(is_ok(reader.abort_split()));
    }
    EXPECT_TRUE(is_ok(reader.close()));
}

} // namespace
} // namespace doris::format::paimon
