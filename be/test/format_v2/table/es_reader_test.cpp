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

#include "format_v2/table/es_reader.h"

#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <atomic>
#include <cstdlib>
#include <set>
#include <tuple>
#include <utility>

#include "common/object_pool.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "format/table/es/es_scroll_parser.h"
#include "format_v2/column_mapper.h"
#include "runtime/runtime_state.h"
#include "service/http/ev_http_server.h"
#include "service/http/http_channel.h"
#include "service/http/http_handler.h"
#include "service/http/http_request.h"
#include "testutil/desc_tbl_builder.h"

namespace doris::format::es {
namespace {
struct StreamState {
    int opens = 0;
    int reads = 0;
    int closes = 0;
    bool fail_open = false;
    bool fail_next = false;
    bool fail_close = false;
    bool docvalues = false;
    std::string host;
    std::map<std::string, std::string> properties;
    std::vector<std::string> pages;
};
class ResponseStream final : public EsScrollStream {
public:
    explicit ResponseStream(std::shared_ptr<StreamState> state) : _state(std::move(state)) {}
    Status open() override {
        ++_state->opens;
        return _state->fail_open ? Status::IOError("open failed") : Status::OK();
    }
    Status next(bool* eof, std::unique_ptr<ScrollParser>& parser) override {
        ++_state->reads;
        if (_state->fail_next) {
            return Status::IOError("scroll failed");
        }
        *eof = _page == _state->pages.size();
        if (!*eof) {
            parser = std::make_unique<ScrollParser>(_state->docvalues);
            RETURN_IF_ERROR(parser->parse(_state->pages[_page++]));
        }
        return Status::OK();
    }
    Status close() override {
        ++_state->closes;
        return _state->fail_close ? Status::IOError("close failed") : Status::OK();
    }

private:
    std::shared_ptr<StreamState> _state;
    size_t _page = 0;
};

std::string page(const std::string& hits) {
    return R"({"_scroll_id":"scroll-token","hits":{"hits":[)" + hits + "]}}";
}
TFileRangeDesc es_range() {
    TTableFormatFileDesc table;
    table.__set_table_format_type("es");
    table.__set_es_params(
            {{"index", "example"}, {"shard_id", "0"}, {"host_port", "http://localhost:9200"}});
    TFileRangeDesc range;
    range.__set_format_type(TFileFormatType::FORMAT_ES_HTTP);
    range.__set_path("es://example/0");
    range.__set_table_format_params(table);
    return range;
}
EsScrollStreamFactory factory(const std::shared_ptr<StreamState>& state) {
    return [state](const auto& host, const auto& properties, bool docvalues) {
        state->host = host;
        state->properties = properties;
        state->docvalues = docvalues;
        return std::make_unique<ResponseStream>(state);
    };
}

class ScrollHttpHandler final : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        if (req->method() == DELETE) {
            EXPECT_EQ(req->get_request_body(), R"({"scroll_id":"scroll-token"})");
            ++clears;
            HttpChannel::send_reply(req, HttpStatus::OK, R"({"succeeded":true})");
        } else if (searches.fetch_add(1) == 0) {
            HttpChannel::send_reply(req, HttpStatus::OK,
                                    page(empty ? "" : R"({"_source":{"id":7}})"));
        } else {
            HttpChannel::send_reply(req, scroll_status, R"({"error":"scroll failed"})");
        }
    }

    bool empty = false;
    HttpStatus scroll_status = HttpStatus::NOT_FOUND;
    std::atomic<int> searches {0};
    std::atomic<int> clears {0};
};

class EsV2ReaderTest : public testing::Test {
protected:
    void SetUp() override {
        DescriptorTblBuilder builder(&_pool);
        builder.declare_tuple()
                << std::make_tuple(std::make_shared<DataTypeInt32>(), std::string("id"))
                << std::make_tuple(std::make_shared<DataTypeString>(), std::string("name"))
                << std::make_tuple(std::make_shared<DataTypeString>(), std::string("_id"))
                << std::make_tuple(std::make_shared<DataTypeArray>(
                                           make_nullable(std::make_shared<DataTypeInt32>())),
                                   std::string("values"));
        _slots = builder.build()->get_tuple_descriptor(0)->slots();
        _params.__set_es_properties({{"query_dsl", R"({"match_all":{}})"}});
        _responses = std::make_shared<StreamState>();
        _responses->pages = {
                page(R"({"_id":"a","_source":{"id":1,"name":"one","values":[1,2,3]}},
                                       {"_id":"b","_source":{"id":2,"name":null,"values":[]}})"),
                page(R"({"_id":"c","_source":{"id":3,"name":"three","values":null}})")};
    }
    std::unique_ptr<EsFileReader> reader(bool real_http = false) {
        auto props = std::make_shared<io::FileSystemProperties>();
        auto file = std::make_unique<io::FileDescription>();
        file->path = "es://example/0";
        auto result = std::make_unique<EsFileReader>(
                props, file, _io, &_profile, _range, _params, _slots,
                real_http ? EsScrollStreamFactory {} : factory(_responses));
        EXPECT_TRUE(result->init(&_state).ok());
        return result;
    }
    std::unique_ptr<EsFileReader> http_reader(EvHttpServer& server, HttpHandler* handler) {
        EXPECT_TRUE(server.register_handler(POST, "/example/_search", handler));
        EXPECT_TRUE(server.register_handler(POST, "/_search/scroll", handler));
        EXPECT_TRUE(server.register_handler(DELETE, "/_search/scroll", handler));
        server.start();
        EXPECT_GT(server.get_real_port(), 0);
        _range.table_format_params.es_params["host_port"] =
                "http://127.0.0.1:" + std::to_string(server.get_real_port());
        auto props = std::make_shared<io::FileSystemProperties>();
        auto file = std::make_unique<io::FileDescription>();
        auto result = std::make_unique<EsFileReader>(props, file, _io, &_profile, _range, _params,
                                                     _slots);
        EXPECT_TRUE(result->init(&_state).ok());
        result->set_batch_size(1);
        return result;
    }
    std::shared_ptr<FileScanRequest> request(std::initializer_list<int> ids) {
        auto result = std::make_shared<FileScanRequest>();
        FileScanRequestBuilder builder(result.get());
        for (int id : ids) {
            EXPECT_TRUE(builder.add_non_predicate_column(LocalColumnId(id)).ok());
        }
        return result;
    }
    Block block(const EsFileReader& reader, const FileScanRequest& request) {
        std::vector<ColumnDefinition> schema;
        EXPECT_TRUE(reader.get_schema(&schema).ok());
        ColumnsWithTypeAndName columns(request.block_column_count());
        for (auto [id, position] : request.local_positions) {
            const auto& column = schema[id.value()];
            columns[position.value()] = {column.type->create_column(), column.type, column.name};
        }
        return Block(columns);
    }
    int32_t int_at(const Block& block, size_t pos, size_t row) {
        const auto& nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(pos).column);
        return assert_cast<const ColumnInt32&>(nullable.get_nested_column()).get_data()[row];
    }
    std::string string_at(const Block& block, size_t pos, size_t row) {
        const auto& nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(pos).column);
        return nullable.get_nested_column().get_data_at(row).to_string();
    }
    ObjectPool _pool;
    RuntimeState _state;
    RuntimeProfile _profile {"es_v2_test"};
    std::shared_ptr<io::IOContext> _io = std::make_shared<io::IOContext>();
    TFileRangeDesc _range = es_range();
    TFileScanRangeParams _params;
    std::vector<SlotDescriptor*> _slots;
    std::shared_ptr<StreamState> _responses;
};
class NullableIntGreaterThanExpr final : public VExpr {
public:
    NullableIntGreaterThanExpr(size_t block_position, int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _block_position(block_position),
              _value(value) {}

    const std::string& expr_name() const override { return _name; }

    bool is_constant() const override { return false; }

    Status execute_column_impl(VExprContext*, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        DORIS_CHECK(block != nullptr);
        const auto& nullable =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_block_position).column);
        const auto& data = assert_cast<const ColumnInt32&>(nullable.get_nested_column());

        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const auto source_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] =
                    !nullable.is_null_at(source_row) && data.get_element(source_row) > _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    Status clone_node(VExprSPtr* cloned_expr) const override {
        DORIS_CHECK(cloned_expr != nullptr);
        *cloned_expr = std::make_shared<NullableIntGreaterThanExpr>(_block_position, _value);
        return Status::OK();
    }

private:
    size_t _block_position;
    int32_t _value;
    const std::string _name = "NullableIntGreaterThanExpr";
};

VExprContextSPtr prepared_conjunct(RuntimeState* state, const VExprSPtr& expr) {
    auto context = VExprContext::create_shared(expr);
    auto status = context->prepare(state, RowDescriptor());
    EXPECT_TRUE(status.ok()) << status;
    status = context->open(state);
    EXPECT_TRUE(status.ok()) << status;
    return context;
}

} // namespace

TEST_F(EsV2ReaderTest, ProjectsLocalColumnsAndCarriesPagesAcrossBatches) {
    auto scan = reader();
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(scan->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 4);
    ASSERT_EQ(schema[3].children.size(), 1);
    EXPECT_EQ(_responses->opens, 0);
    auto projection = request({1, 0, 2, 3});
    ASSERT_TRUE(scan->open(projection).ok());
    EXPECT_EQ(_responses->reads, 1);
    scan->set_batch_size(1);
    size_t rows = 0;
    bool eof = false;
    for (size_t row = 0; row < 3; ++row) {
        auto output = block(*scan, *projection);
        auto status = scan->get_block(&output, &rows, &eof);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(rows, 1);
        EXPECT_FALSE(eof);
        EXPECT_EQ(int_at(output, 1, 0), row + 1);
        EXPECT_EQ(string_at(output, 2, 0), std::string(1, 'a' + row));
        if (row == 1) {
            EXPECT_TRUE(output.get_by_position(0).column->is_null_at(0));
        } else {
            EXPECT_EQ(string_at(output, 0, 0), row == 0 ? "one" : "three");
        }
        if (row == 0) {
            const auto& values = assert_cast<const ColumnArray&>(
                    assert_cast<const ColumnNullable&>(*output.get_by_position(3).column)
                            .get_nested_column());
            EXPECT_EQ(values.get_offsets()[0], 3);
            EXPECT_FALSE(values.get_data().is_null_at(1));
        }
    }
    auto output = block(*scan, *projection);
    ASSERT_TRUE(scan->get_block(&output, &rows, &eof).ok());
    EXPECT_EQ(rows, 0);
    EXPECT_TRUE(eof);
    EXPECT_EQ(_responses->closes, 1);
    ASSERT_TRUE(scan->close().ok());
    scan.reset();
    EXPECT_EQ(_responses->closes, 1);
}

TEST_F(EsV2ReaderTest, UsesDocvalueAliasesAndRangeOverrides) {
    _params.es_properties["index"] = "node-index";
    _params.es_properties["doc_values_mode"] = "1";
    _params.__set_es_docvalue_context({{"id", "id"}, {"name", "name.keyword"}});
    _responses->pages = {page(R"({"fields":{"id":[4],"name.keyword":["four"]}})")};
    auto scan = reader();
    auto projection = request({1, 0});
    ASSERT_TRUE(scan->open(projection).ok());
    EXPECT_TRUE(_responses->docvalues);
    EXPECT_EQ(_responses->properties.at("index"), "example");
    EXPECT_NE(_responses->properties.at("query").find("name.keyword"), std::string::npos);
    auto output = block(*scan, *projection);
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(scan->get_block(&output, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(int_at(output, 1, 0), 4);
    EXPECT_EQ(string_at(output, 0, 0), "four");
}

TEST_F(EsV2ReaderTest, CountsRowsWithNoProjectedColumns) {
    auto scan = reader();
    auto projection = request({});
    ASSERT_TRUE(scan->open(projection).ok());
    Block output;
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(scan->get_block(&output, &rows, &eof).ok());
    EXPECT_EQ(rows, 3);
    ASSERT_TRUE(scan->get_block(&output, &rows, &eof).ok());
    EXPECT_TRUE(eof);
    EXPECT_EQ(rows, 0);
}

TEST_F(EsV2ReaderTest, AppliesResidualFiltersAndAccountsForFilteredRows) {
    auto scan = reader();
    auto projection = request({1, 0});
    projection->conjuncts.push_back(
            prepared_conjunct(&_state, std::make_shared<NullableIntGreaterThanExpr>(1, 1)));
    ASSERT_TRUE(scan->open(projection).ok());
    auto output = block(*scan, *projection);
    size_t rows = 0;
    bool eof = false;
    auto status = scan->get_block(&output, &rows, &eof);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(rows, 2);
    EXPECT_EQ(int_at(output, 1, 0), 2);
    EXPECT_EQ(int_at(output, 1, 1), 3);
    EXPECT_EQ(_io->predicate_filtered_rows, 1);
}

TEST_F(EsV2ReaderTest, ClosesAfterOpenFailureAndBeforeFirstBlock) {
    for (int mode = 0; mode < 3; ++mode) {
        _responses = std::make_shared<StreamState>();
        _responses->pages = {page("")};
        _responses->fail_open = mode == 0;
        _responses->fail_next = mode == 1;
        auto scan = reader();
        auto status = scan->open(request({0}));
        EXPECT_EQ(status.ok(), mode == 2);
        if (mode != 2) {
            EXPECT_EQ(_responses->closes, 1);
        }
        ASSERT_TRUE(scan->close().ok());
        scan.reset();
        EXPECT_EQ(_responses->closes, 1);
    }
}

TEST_F(EsV2ReaderTest, CancellationAndReadErrorsReleaseStream) {
    for (bool fail_next : {false, true}) {
        _io->should_stop = false;
        _responses = std::make_shared<StreamState>();
        _responses->pages = {page(R"({"_source":{"id":1}})")};
        auto scan = reader();
        auto projection = request({0});
        ASSERT_TRUE(scan->open(projection).ok());
        _responses->fail_next = fail_next;
        _io->should_stop = !fail_next;
        auto output = block(*scan, *projection);
        size_t rows = 0;
        bool eof = false;
        auto status = scan->get_block(&output, &rows, &eof);
        EXPECT_EQ(status.ok(), !fail_next);
        EXPECT_EQ(_responses->closes, 1);
        ASSERT_TRUE(scan->close().ok());
        EXPECT_EQ(_responses->closes, 1);
    }
}

TEST_F(EsV2ReaderTest, CloseFailureIsReportedOnce) {
    auto scan = reader();
    ASSERT_TRUE(scan->open(request({0})).ok());
    _responses->fail_close = true;
    EXPECT_FALSE(scan->close().ok());
    ASSERT_TRUE(scan->close().ok());
    scan.reset();
    EXPECT_EQ(_responses->closes, 1);
}

TEST_F(EsV2ReaderTest, RejectsMissingRangePropertiesAndRowidFetch) {
    _range.table_format_params.__isset.es_params = false;
    auto props = std::make_shared<io::FileSystemProperties>();
    auto file = std::make_unique<io::FileDescription>();
    EsFileReader invalid(props, file, _io, &_profile, _range, _params, _slots, factory(_responses));
    EXPECT_FALSE(invalid.init(&_state).ok());
    _range = es_range();
    auto scan = reader();
    auto projection = request({0});
    projection->row_ids = std::vector<int64_t> {0};
    EXPECT_FALSE(scan->open(projection).ok());
    EXPECT_EQ(_responses->opens, 0);
}

TEST_F(EsV2ReaderTest, TableReaderMapsProjectionAndAdvancesSplits) {
    EsReader table(factory(_responses));
    std::vector<ColumnDefinition> projected;
    for (int id : {1, 0}) {
        ColumnDefinition column;
        column.identifier = Field::create_field<TYPE_STRING>(_slots[id]->col_name());
        column.name = _slots[id]->col_name();
        column.type = _slots[id]->type();
        projected.push_back(std::move(column));
    }
    ColumnDefinition missing;
    missing.name = "missing";
    missing.type = make_nullable(std::make_shared<DataTypeString>());
    projected.push_back(std::move(missing));
    ASSERT_TRUE(table.init({.projected_columns = projected,
                            .conjuncts = {},
                            .format = FileFormat::ES_HTTP,
                            .scan_params = &_params,
                            .io_ctx = _io,
                            .runtime_state = &_state,
                            .scanner_profile = &_profile,
                            .file_slot_descs = &_slots})
                        .ok());
    for (int split = 0; split < 2; ++split) {
        SplitReadOptions options;
        options.current_range = _range;
        options.current_split_format = FileFormat::ES_HTTP;
        ASSERT_TRUE(table.prepare_split(options).ok());
        Block output;
        for (const auto& column : projected) {
            output.insert({column.type->create_column(), column.type, column.name});
        }
        bool eof = false;
        size_t total = 0;
        while (!eof) {
            const auto status = table.get_block(&output, &eof);
            ASSERT_TRUE(status.ok()) << status;
            if (output.rows() > 0) {
                ASSERT_EQ(output.columns(), 3);
                EXPECT_EQ(string_at(output, 0, 0), "one");
                EXPECT_EQ(int_at(output, 1, 0), 1);
                EXPECT_TRUE(output.get_by_position(2).column->is_null_at(0));
                total += output.rows();
            }
        }
        EXPECT_EQ(total, 3);
        EXPECT_EQ(_responses->closes, split + 1);
    }
    ASSERT_TRUE(table.close().ok());
}

TEST_F(EsV2ReaderTest, RealHttpTransportClearsLatestScrollId) {
    class Handler final : public HttpHandler {
    public:
        void handle(HttpRequest* req) override {
            if (req->method() == DELETE) {
                EXPECT_EQ(req->get_request_body(), R"({"scroll_id":"last-scroll"})");
                ++clears;
                HttpChannel::send_reply(req, HttpStatus::OK, R"({"succeeded":true})");
                return;
            }
            const int call = searches.fetch_add(1);
            const std::string response =
                    call < 2
                            ? R"({"_scroll_id":"scroll-page","hits":{"hits":[{"_source":{"id":7}}]}})"
                            : R"({"_scroll_id":"last-scroll","hits":{"hits":[]}})";
            HttpChannel::send_reply(req, HttpStatus::OK, response);
        }
        std::atomic<int> searches {0};
        std::atomic<int> clears {0};
    } handler;
    EvHttpServer server {0};
    ASSERT_TRUE(server.register_handler(POST, "/example/_search", &handler));
    ASSERT_TRUE(server.register_handler(POST, "/_search/scroll", &handler));
    ASSERT_TRUE(server.register_handler(DELETE, "/_search/scroll", &handler));
    server.start();
    ASSERT_GT(server.get_real_port(), 0);
    _range.table_format_params.es_params["host_port"] =
            "http://127.0.0.1:" + std::to_string(server.get_real_port());
    auto props = std::make_shared<io::FileSystemProperties>();
    auto file = std::make_unique<io::FileDescription>();
    EsFileReader scan(props, file, _io, &_profile, _range, _params, _slots);
    ASSERT_TRUE(scan.init(&_state).ok());
    scan.set_batch_size(1);
    auto projection = request({0});
    const auto open_status = scan.open(projection);
    ASSERT_TRUE(open_status.ok()) << open_status;
    size_t rows = 0;
    bool eof = false;
    size_t total = 0;
    while (!eof) {
        auto output = block(scan, *projection);
        const auto status = scan.get_block(&output, &rows, &eof);
        ASSERT_TRUE(status.ok()) << status;
        if (rows > 0) {
            EXPECT_EQ(int_at(output, 0, 0), 7);
        }
        total += rows;
    }
    EXPECT_EQ(total, 2);
    EXPECT_EQ(handler.searches.load(), 3);
    EXPECT_EQ(handler.clears.load(), 1);
    ASSERT_TRUE(scan.close().ok());
    EXPECT_EQ(handler.clears.load(), 1);
}

TEST_F(EsV2ReaderTest, SparseProjectionDoesNotRequestOtherFields) {
    auto scan = reader();
    auto projection = request({2});
    ASSERT_TRUE(scan->open(projection).ok());
    EXPECT_FALSE(_responses->docvalues);
    rapidjson::Document query;
    query.Parse(_responses->properties.at("query").c_str());
    ASSERT_FALSE(query.HasParseError());
    ASSERT_EQ(query["_source"].Size(), 1);
    EXPECT_STREQ(query["_source"][0].GetString(), "_id");
    auto output = block(*scan, *projection);
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(scan->get_block(&output, &rows, &eof).ok());
    ASSERT_EQ(rows, 3);
    EXPECT_EQ(string_at(output, 0, 2), "c");
}

TEST_F(EsV2ReaderTest, MalformedResponseReleasesStream) {
    _responses->pages = {"invalid-json"};
    auto scan = reader();
    EXPECT_FALSE(scan->open(request({0})).ok());
    EXPECT_EQ(_responses->closes, 1);
}

TEST_F(EsV2ReaderTest, FullyFilteredBatchesKeepScanningUntilEof) {
    auto scan = reader();
    scan->set_batch_size(1);
    auto projection = request({0});
    projection->conjuncts.push_back(
            prepared_conjunct(&_state, std::make_shared<NullableIntGreaterThanExpr>(0, 10)));
    ASSERT_TRUE(scan->open(projection).ok());
    for (int batch = 0; batch < 4; ++batch) {
        auto output = block(*scan, *projection);
        size_t rows = 0;
        bool eof = false;
        ASSERT_TRUE(scan->get_block(&output, &rows, &eof).ok());
        EXPECT_EQ(rows, 0);
        EXPECT_EQ(eof, batch == 3);
    }
    EXPECT_EQ(_io->predicate_filtered_rows, 3);
    EXPECT_EQ(_responses->closes, 1);
}

TEST_F(EsV2ReaderTest, SourceOnlyFieldsDisableDocvalues) {
    for (int source_column : {1, 2}) {
        _responses = std::make_shared<StreamState>();
        _responses->pages = {page(R"({"_id":"a","_source":{"id":1,"name":"one"}})")};
        _params.es_properties["doc_values_mode"] = "1";
        _params.__set_es_docvalue_context({{"id", "id"}, {"_id", "_id"}});
        auto scan = reader();
        auto projection = request({0, source_column});
        ASSERT_TRUE(scan->open(projection).ok());
        EXPECT_FALSE(_responses->docvalues);
        rapidjson::Document query;
        query.Parse(_responses->properties.at("query").c_str());
        ASSERT_FALSE(query.HasParseError());
        EXPECT_TRUE(query.HasMember("_source"));
        EXPECT_FALSE(query.HasMember("docvalue_fields"));
        auto output = block(*scan, *projection);
        size_t rows = 0;
        bool eof = false;
        ASSERT_TRUE(scan->get_block(&output, &rows, &eof).ok());
        ASSERT_EQ(rows, 1);
        EXPECT_EQ(int_at(output, 0, 0), 1);
        EXPECT_EQ(string_at(output, 1, 0), source_column == 1 ? "one" : "a");
        ASSERT_TRUE(scan->close().ok());
        EXPECT_EQ(_responses->closes, 1);
    }
}

TEST_F(EsV2ReaderTest, DocvaluesPreserveMissingFieldsAndEmptyArrays) {
    _params.es_properties["doc_values_mode"] = "1";
    _params.__set_es_docvalue_context(
            {{"id", "id"}, {"name", "name.keyword"}, {"values", "values"}});
    _responses->pages = {page(R"({"fields":{"id":[1],"name.keyword":["one"],"values":[1,3]}},
                                {"fields":{"id":[2],"values":[]}},
                                {"fields":{"id":[3],"name.keyword":null,"values":null}})")};
    auto scan = reader();
    auto projection = request({1, 3, 0});
    ASSERT_TRUE(scan->open(projection).ok());
    EXPECT_TRUE(_responses->docvalues);
    auto output = block(*scan, *projection);
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(scan->get_block(&output, &rows, &eof).ok());
    ASSERT_EQ(rows, 3);
    for (const auto& column : output) {
        EXPECT_EQ(column.column->size(), rows);
    }
    EXPECT_EQ(string_at(output, 0, 0), "one");
    EXPECT_TRUE(output.get_by_position(0).column->is_null_at(1));
    EXPECT_TRUE(output.get_by_position(0).column->is_null_at(2));
    EXPECT_FALSE(output.get_by_position(1).column->is_null_at(1));
    EXPECT_TRUE(output.get_by_position(1).column->is_null_at(2));
    const auto& values = assert_cast<const ColumnArray&>(
            assert_cast<const ColumnNullable&>(*output.get_by_position(1).column)
                    .get_nested_column());
    EXPECT_EQ(values.get_offsets()[0], 2);
    EXPECT_EQ(values.get_offsets()[1], 2);
    EXPECT_EQ(values.get_offsets()[2], 2);
    EXPECT_EQ(int_at(output, 2, 2), 3);
}

TEST_F(EsV2ReaderTest, RealHttpEarlyCloseClearsInitialScroll) {
    ScrollHttpHandler handler;
    EvHttpServer server {0};
    auto scan = http_reader(server, &handler);
    ASSERT_TRUE(scan->open(request({0})).ok());
    ASSERT_TRUE(scan->close().ok());
    scan.reset();
    EXPECT_EQ(handler.searches.load(), 1);
    EXPECT_EQ(handler.clears.load(), 1);
}

TEST_F(EsV2ReaderTest, RealHttpEmptyResultClearsScroll) {
    ScrollHttpHandler handler;
    handler.empty = true;
    EvHttpServer server {0};
    auto scan = http_reader(server, &handler);
    auto projection = request({0});
    ASSERT_TRUE(scan->open(projection).ok());
    auto output = block(*scan, *projection);
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(scan->get_block(&output, &rows, &eof).ok());
    EXPECT_EQ(rows, 0);
    EXPECT_TRUE(eof);
    ASSERT_TRUE(scan->close().ok());
    EXPECT_EQ(handler.searches.load(), 1);
    EXPECT_EQ(handler.clears.load(), 1);
}

TEST_F(EsV2ReaderTest, RealHttpScrollErrorsFailAndReleaseContext) {
    for (auto status : {HttpStatus::NOT_FOUND, HttpStatus::INTERNAL_SERVER_ERROR}) {
        ScrollHttpHandler handler;
        handler.scroll_status = status;
        EvHttpServer server {0};
        auto scan = http_reader(server, &handler);
        auto projection = request({0});
        ASSERT_TRUE(scan->open(projection).ok());
        auto output = block(*scan, *projection);
        size_t rows = 0;
        bool eof = false;
        ASSERT_TRUE(scan->get_block(&output, &rows, &eof).ok());
        ASSERT_EQ(rows, 1);
        output = block(*scan, *projection);
        const auto result = scan->get_block(&output, &rows, &eof);
        EXPECT_FALSE(result.ok());
        EXPECT_NE(result.to_string().find(status == HttpStatus::NOT_FOUND ? "No search context"
                                                                          : "scroll failed"),
                  std::string::npos);
        ASSERT_TRUE(scan->close().ok());
        scan.reset();
        EXPECT_EQ(handler.searches.load(), 2);
        EXPECT_EQ(handler.clears.load(), 1);
    }
}

// The opt-in fixture has integer id / keyword name and rows (1,"alpha"), (2,"beta"), (3,null).
TEST_F(EsV2ReaderTest, ConfiguredHttpSourceSupportsSourceAndDocvalues) {
    const char* host = std::getenv("DORIS_ES_TEST_HOST");
    if (host == nullptr) {
        GTEST_SKIP() << "Set DORIS_ES_TEST_HOST and DORIS_ES_TEST_INDEX for a live ES smoke test";
    }
    const char* index = std::getenv("DORIS_ES_TEST_INDEX");
    ASSERT_NE(index, nullptr);
    _range.table_format_params.es_params["host_port"] = host;
    _range.table_format_params.es_params["index"] = index;
    _range.table_format_params.es_params["shard_id"] = "-1";
    for (const auto& [property, variable] : {std::pair {"user", "DORIS_ES_TEST_USER"},
                                             std::pair {"password", "DORIS_ES_TEST_PASSWORD"}}) {
        if (const char* value = std::getenv(variable); value != nullptr) {
            _params.es_properties[property] = value;
        }
    }
    _params.__set_es_docvalue_context({{"id", "id"}, {"name", "name"}});
    for (bool docvalues : {false, true}) {
        _params.es_properties["doc_values_mode"] = docvalues ? "1" : "0";
        auto scan = reader(true);
        scan->set_batch_size(1);
        auto projection = request({1, 0});
        const auto opened = scan->open(projection);
        ASSERT_TRUE(opened.ok()) << opened;
        size_t total = 0;
        std::set<int32_t> ids;
        bool eof = false;
        while (!eof) {
            auto output = block(*scan, *projection);
            size_t rows = 0;
            const auto status = scan->get_block(&output, &rows, &eof);
            ASSERT_TRUE(status.ok()) << status;
            for (size_t row = 0; row < rows; ++row) {
                const int32_t id = int_at(output, 1, row);
                ids.insert(id);
                if (id == 3) {
                    EXPECT_TRUE(output.get_by_position(0).column->is_null_at(row));
                } else {
                    EXPECT_EQ(string_at(output, 0, row), id == 1 ? "alpha" : "beta");
                }
            }
            total += rows;
        }
        EXPECT_EQ(total, 3);
        EXPECT_EQ(ids, (std::set<int32_t> {1, 2, 3}));
        ASSERT_TRUE(scan->close().ok());
    }
}

} // namespace doris::format::es
