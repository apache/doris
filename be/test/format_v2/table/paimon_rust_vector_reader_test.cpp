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
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array/builder_primitive.h"
#include "arrow/record_batch.h"
#include "arrow/type.h"
#include "core/block/block.h"
#include "core/data_type/data_type_number.h"
#include "exprs/vectorized_fn_call.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "format_v2/table/paimon_rust_predicate_converter.h"
#include "format_v2/table/paimon_rust_table_reader.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state.h"

namespace doris::format::paimon {
namespace {

testing::AssertionResult is_ok(const Status& status) {
    if (status.ok()) {
        return testing::AssertionSuccess();
    }
    return testing::AssertionFailure() << status;
}

class PaimonRustVectorReaderTest : public testing::Test {
protected:
    void SetUp() override {
        TQueryOptions options;
        options.__set_batch_size(2);
        _state = RuntimeState::create_unique(options, TQueryGlobals {});
        set_request(false, TVectorMetric::L2, 4);
    }

    void set_request(bool tvf, TVectorMetric::type metric, int64_t top_k) {
        TSearchVector query;
        query.__set_element_type(TVectorElementType::FLOAT32);
        query.__set_dimension(2);
        std::string values;
        for (float value : {metric == TVectorMetric::DOT_PRODUCT ? 1.0F : 0.0F, 0.0F}) {
            auto bits = std::bit_cast<uint32_t>(value);
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

    Status init(PaimonRustTableReader& reader) {
        return reader.init({.projected_columns = {{.identifier = Field {},
                                                   .name = _distance_name,
                                                   .type = std::make_shared<DataTypeFloat32>()}},
                            .conjuncts = {},
                            .format = FileFormat::JNI,
                            .scan_params = &_params,
                            .io_ctx = nullptr,
                            .runtime_state = _state.get(),
                            .scanner_profile = nullptr});
    }

    SplitReadOptions split() const {
        TPaimonVectorPayload payload;
        payload.__set_payload_type(TPaimonVectorPayloadType::BUCKET_SPLIT_BYTES);
        payload.__set_bytes("malformed-bucket");
        TPaimonFileDesc paimon;
        paimon.__set_reader_type(TPaimonReaderType::PAIMON_RUST);
        paimon.__set_paimon_split("");
        paimon.__set_vector_payload(payload);
        paimon.__set_paimon_table("/unused/paimon-vector-protocol-test");
        paimon.__set_db_name("db");
        paimon.__set_table_name("vector_protocol_test");
        // Paimon 2.1 TableSchema input; no snapshot, manifest or data file is needed.
        // The malformed requests/bucket fail before any data scan can open files.
        paimon.__set_paimon_table_schema_json(R"({"version":3,"id":0,"fields":[
            {"id":0,"name":"id","type":"INT NOT NULL"},
            {"id":1,"name":"embedding","type":{"type":"VECTOR NOT NULL","element":"FLOAT","length":2}}],
            "highestFieldId":1,"partitionKeys":[],"primaryKeys":["id"],"options":{
            "bucket":"1","merge-engine":"first-row","file.format":"parquet",
            "pk-vector.index.columns":"embedding","fields.embedding.pk-vector.index.type":"ivf-rq",
            "fields.embedding.pk-vector.distance.metric":"l2","vector-index.search-mode":"full"},
            "timeMillis":0})");
        TTableFormatFileDesc format;
        format.__set_table_format_type("paimon");
        format.__set_paimon_params(paimon);
        SplitReadOptions options;
        options.current_range.__set_table_format_params(format);
        options.current_split_format = FileFormat::JNI;
        options.all_runtime_filters_applied = true;
        return options;
    }

    std::unique_ptr<RuntimeState> _state;
    TFileScanRangeParams _params;
    std::string _distance_name;
};

TEST_F(PaimonRustVectorReaderTest, FailedDecodeAndZeroLimitLeaveReaderReusable) {
    PaimonRustTableReader reader;
    ASSERT_TRUE(is_ok(init(reader)));
    for (int attempt = 0; attempt < 2; ++attempt) {
        set_request(false, TVectorMetric::L2, 0);
        auto status = reader.prepare_split(split());
        ASSERT_FALSE(status.ok());
        EXPECT_NE(std::string::npos, status.to_string().find("limit must be positive")) << status;
        ASSERT_TRUE(is_ok(reader.abort_split()));
        set_request(false, TVectorMetric::L2, 4);
        status = reader.prepare_split(split());
        ASSERT_FALSE(status.ok());
        EXPECT_NE(std::string::npos, status.to_string().find("deserialize")) << status;
        ASSERT_TRUE(is_ok(reader.abort_split()));
    }
    EXPECT_TRUE(is_ok(reader.close()));
    EXPECT_TRUE(is_ok(reader.close()));
}

TEST_F(PaimonRustVectorReaderTest, AbortAndCloseReleaseRetainedArrowBatch) {
    PaimonRustTableReader reader;
    ASSERT_TRUE(is_ok(init(reader)));
    arrow::FloatBuilder builder;
    ASSERT_TRUE(builder.AppendValues(std::vector<float>(8, 1.0F)).ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(builder.Finish(&values).ok());
    auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("score", arrow::float32())}),
                                          8, {values});
    reader._pending_record_batch = batch;
    reader._pending_record_batch_offset = 7;
    reader._split_eof = true;
    EXPECT_EQ(2, batch.use_count());
    ASSERT_TRUE(is_ok(reader.abort_split()));
    EXPECT_EQ(nullptr, reader._pending_record_batch);
    EXPECT_EQ(0, reader._pending_record_batch_offset);
    EXPECT_FALSE(reader._split_eof);
    EXPECT_EQ(1, batch.use_count());
    reader._pending_record_batch = batch;
    ASSERT_TRUE(is_ok(reader.close()));
    EXPECT_EQ(1, batch.use_count());
}

// Use the real comparison function so this checks typed partition-constant execution.
VExprContextSPtr greater_than_filter(const std::string& name, int position, int value) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto bool_type = std::make_shared<DataTypeUInt8>();
    TFunctionName function_name;
    function_name.__set_function_name("gt");
    TFunction function;
    function.__set_name(function_name);
    function.__set_binary_type(TFunctionBinaryType::BUILTIN);
    function.__set_arg_types({int_type->to_thrift(), int_type->to_thrift()});
    function.__set_ret_type(bool_type->to_thrift());
    function.__set_has_var_args(false);
    TExprNode node;
    node.__set_node_type(TExprNodeType::BINARY_PRED);
    node.__set_opcode(TExprOpcode::GT);
    node.__set_type(bool_type->to_thrift());
    node.__set_is_nullable(false);
    node.__set_fn(function);
    node.__set_num_children(2);
    auto expr = VectorizedFnCall::create_shared(node);
    expr->add_child(VSlotRef::create_shared(position, position, -1, int_type, name));
    expr->add_child(VLiteral::create_shared(int_type, Field::create_field<TYPE_INT>(value)));
    return VExprContext::create_shared(expr);
}

TEST_F(PaimonRustVectorReaderTest, PartitionPrefilterEvaluatesConstantsAndPreservesDataFilters) {
    PaimonRustTableReader reader;
    ASSERT_TRUE(is_ok(init(reader)));
    reader.TEST_set_projected_columns({{.identifier = Field::create_field<TYPE_STRING>("part"),
                                        .name = "part",
                                        .type = std::make_shared<DataTypeInt32>(),
                                        .is_partition_key = true},
                                       {.identifier = Field::create_field<TYPE_STRING>("id"),
                                        .name = "id",
                                        .type = std::make_shared<DataTypeInt32>()}});
    auto data_filter = greater_than_filter("id", 1, 0);
    for (int partition : {7, 11}) {
        reader._split_eof = false;
        reader.TEST_set_partition_values({{"part", Field::create_field<TYPE_INT>(partition)}});
        VExprContextSPtrs filters {greater_than_filter("part", 0, 10), data_filter};
        ASSERT_TRUE(is_ok(reader._prune_vector_partition_filters(&filters)));
        EXPECT_EQ(partition == 7, reader._split_eof);
        ASSERT_EQ(1, filters.size());
        EXPECT_EQ(data_filter, filters[0]);
    }
    reader.TEST_set_partition_values({});
    reader._split_eof = false;
    VExprContextSPtrs filters {greater_than_filter("part", 0, 10)};
    ASSERT_TRUE(is_ok(reader._prune_vector_partition_filters(&filters)));
    EXPECT_EQ(1, filters.size());
    EXPECT_FALSE(reader._split_eof);
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
        auto status = reader.prepare_split(split());
        EXPECT_FALSE(status.ok());
        EXPECT_NE(std::string::npos, status.to_string().find(message)) << status;
        ASSERT_TRUE(is_ok(reader.abort_split()));
    }
    EXPECT_TRUE(is_ok(reader.close()));
}

} // namespace
} // namespace doris::format::paimon
