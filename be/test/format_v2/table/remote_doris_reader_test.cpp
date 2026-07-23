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

#include "format_v2/table/remote_doris_reader.h"

#include <arrow/api.h>
#include <arrow/flight/server.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "format_v2/file_reader.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/file_factory.h"
#include "io/io_common.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "testutil/desc_tbl_builder.h"

namespace doris::format::remote_doris {
namespace {

class BatchRemoteDorisStream final : public RemoteDorisStream {
public:
    BatchRemoteDorisStream(std::vector<std::shared_ptr<arrow::RecordBatch>> batches,
                           std::shared_ptr<int> close_count)
            : _batches(std::move(batches)), _close_count(std::move(close_count)) {}

    Status next(std::shared_ptr<arrow::RecordBatch>* batch) override {
        DORIS_CHECK(batch != nullptr);
        if (_next_batch >= _batches.size()) {
            *batch = nullptr;
            return Status::OK();
        }
        *batch = _batches[_next_batch++];
        return Status::OK();
    }

    Status close() override {
        ++(*_close_count);
        return Status::OK();
    }

private:
    std::vector<std::shared_ptr<arrow::RecordBatch>> _batches;
    std::shared_ptr<int> _close_count;
    size_t _next_batch = 0;
};

TFileRangeDesc remote_doris_range() {
    TRemoteDorisFileDesc remote_desc;
    remote_desc.__set_location_uri("grpc://127.0.0.1:9050");
    remote_desc.__set_ticket("ticket-bytes");

    TTableFormatFileDesc table_desc;
    table_desc.__set_table_format_type("remote_doris");
    table_desc.__set_remote_doris_params(std::move(remote_desc));

    TFileRangeDesc range;
    range.__set_format_type(TFileFormatType::FORMAT_ARROW);
    range.__set_path("/dummyPath");
    range.__set_table_format_params(std::move(table_desc));
    return range;
}

class BlockingRecordBatchReader final : public arrow::RecordBatchReader {
public:
    std::shared_ptr<arrow::Schema> schema() const override {
        return arrow::schema({arrow::field("id", arrow::int32())});
    }

    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override {
        {
            std::lock_guard lock(_mutex);
            _entered = true;
        }
        _cv.notify_all();
        std::unique_lock lock(_mutex);
        _cv.wait(lock, [this] { return _released; });
        *batch = nullptr;
        return arrow::Status::OK();
    }

    bool wait_until_entered(std::chrono::milliseconds timeout) {
        std::unique_lock lock(_mutex);
        return _cv.wait_for(lock, timeout, [this] { return _entered; });
    }

    void release() {
        {
            std::lock_guard lock(_mutex);
            _released = true;
        }
        _cv.notify_all();
    }

private:
    std::mutex _mutex;
    std::condition_variable _cv;
    bool _entered = false;
    bool _released = false;
};

class BlockingFlightServer final : public arrow::flight::FlightServerBase {
public:
    enum class Mode { DO_GET, NEXT };

    explicit BlockingFlightServer(Mode mode)
            : _mode(mode), _batch_reader(std::make_shared<BlockingRecordBatchReader>()) {}

    arrow::Status start() {
        auto location = arrow::flight::Location::ForGrpcTcp("localhost", 0);
        if (!location.ok()) {
            return location.status();
        }
        // FlightServerBase::Init starts serving immediately; the blocking Serve lifecycle wrapper
        // is unnecessary for an in-process unit test.
        return Init(arrow::flight::FlightServerOptions(*location));
    }

    ~BlockingFlightServer() override {
        release();
        static_cast<void>(Shutdown());
    }

    arrow::Status DoGet(const arrow::flight::ServerCallContext& context,
                        const arrow::flight::Ticket&,
                        std::unique_ptr<arrow::flight::FlightDataStream>* stream) override {
        if (_mode == Mode::DO_GET) {
            {
                std::lock_guard lock(_mutex);
                _entered = true;
            }
            _cv.notify_all();
            std::unique_lock lock(_mutex);
            while (!_released && !context.is_cancelled()) {
                _cv.wait_for(lock, std::chrono::milliseconds(5));
            }
            if (context.is_cancelled()) {
                return arrow::Status::Cancelled("client cancelled blocked DoGet");
            }
        }
        *stream = std::make_unique<arrow::flight::RecordBatchStream>(_batch_reader);
        return arrow::Status::OK();
    }

    bool wait_until_entered(std::chrono::milliseconds timeout) {
        if (_mode == Mode::NEXT) {
            return _batch_reader->wait_until_entered(timeout);
        }
        std::unique_lock lock(_mutex);
        return _cv.wait_for(lock, timeout, [this] { return _entered; });
    }

    void release() {
        {
            std::lock_guard lock(_mutex);
            _released = true;
        }
        _cv.notify_all();
        _batch_reader->release();
    }

private:
    Mode _mode;
    std::shared_ptr<BlockingRecordBatchReader> _batch_reader;
    std::mutex _mutex;
    std::condition_variable _cv;
    bool _entered = false;
    bool _released = false;
};

TFileRangeDesc remote_doris_range(const BlockingFlightServer& server) {
    auto range = remote_doris_range();
    auto& params = range.table_format_params.remote_doris_params;
    params.__set_location_uri("grpc://localhost:" + std::to_string(server.port()));
    arrow::flight::Ticket ticket {.ticket = "ticket"};
    params.__set_ticket(ticket.SerializeToString().ValueOrDie());
    return range;
}

std::vector<SlotDescriptor*> remote_slots(ObjectPool* pool, DescriptorTbl** desc_tbl) {
    DescriptorTblBuilder builder(pool);
    builder.declare_tuple() << std::make_tuple(std::make_shared<DataTypeInt32>(), std::string("id"))
                            << std::make_tuple(std::make_shared<DataTypeString>(),
                                               std::string("name"));
    *desc_tbl = builder.build();
    return (*desc_tbl)->get_tuple_descriptor(0)->slots();
}

TSlotDescriptor remote_complex_slot_descriptor(int id, const DataTypePtr& type,
                                               const std::string& name) {
    TSlotDescriptor slot_desc;
    slot_desc.__set_id(id);
    slot_desc.__set_parent(0);
    slot_desc.__set_slotType(type->to_thrift());
    slot_desc.__set_byteOffset(0);
    slot_desc.__set_nullIndicatorByte(id / 8);
    slot_desc.__set_nullIndicatorBit(id % 8);
    slot_desc.__set_slotIdx(id);
    slot_desc.__set_columnPos(id);
    slot_desc.__set_isMaterialized(true);
    slot_desc.__set_is_key(false);
    slot_desc.__set_colName(name);
    slot_desc.__set_col_unique_id(id);
    return slot_desc;
}

std::vector<SlotDescriptor*> remote_complex_slots(ObjectPool* pool, DescriptorTbl** desc_tbl) {
    const auto string_type = make_nullable(std::make_shared<DataTypeString>());
    const auto int_type = make_nullable(std::make_shared<DataTypeInt32>());
    const auto array_type = make_nullable(std::make_shared<DataTypeArray>(string_type));
    const auto map_type = make_nullable(std::make_shared<DataTypeMap>(string_type, int_type));
    const auto struct_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {int_type, make_nullable(std::make_shared<DataTypeFloat32>()), string_type},
            Strings {"f1", "f2", "f3"}));

    TDescriptorTable thrift_desc_tbl;
    TTupleDescriptor tuple_desc;
    tuple_desc.__set_id(0);
    tuple_desc.__set_byteSize(0);
    tuple_desc.__set_numNullBytes(1);
    thrift_desc_tbl.tupleDescriptors.push_back(std::move(tuple_desc));
    thrift_desc_tbl.slotDescriptors.push_back(
            remote_complex_slot_descriptor(0, array_type, "c_array_s"));
    thrift_desc_tbl.slotDescriptors.push_back(remote_complex_slot_descriptor(1, map_type, "c_map"));
    thrift_desc_tbl.slotDescriptors.push_back(
            remote_complex_slot_descriptor(2, struct_type, "c_struct"));
    auto status = DescriptorTbl::create(pool, thrift_desc_tbl, desc_tbl);
    EXPECT_TRUE(status.ok()) << status;
    return (*desc_tbl)->get_tuple_descriptor(0)->slots();
}

std::shared_ptr<arrow::RecordBatch> make_batch(const std::vector<std::string>& names) {
    arrow::Int32Builder id_builder;
    EXPECT_TRUE(id_builder.Append(10).ok());
    EXPECT_TRUE(id_builder.Append(20).ok());
    std::shared_ptr<arrow::Array> id_array;
    EXPECT_TRUE(id_builder.Finish(&id_array).ok());

    arrow::StringBuilder name_builder;
    EXPECT_TRUE(name_builder.Append("alice").ok());
    EXPECT_TRUE(name_builder.Append("bob").ok());
    std::shared_ptr<arrow::Array> name_array;
    EXPECT_TRUE(name_builder.Finish(&name_array).ok());

    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (const auto& name : names) {
        if (name == "id") {
            fields.push_back(arrow::field("id", arrow::int32()));
            arrays.push_back(id_array);
        } else if (name == "name") {
            fields.push_back(arrow::field("name", arrow::utf8()));
            arrays.push_back(name_array);
        } else {
            fields.push_back(arrow::field(name, arrow::int32()));
            arrays.push_back(id_array);
        }
    }
    return arrow::RecordBatch::Make(arrow::schema(std::move(fields)), 2, std::move(arrays));
}

std::unique_ptr<RemoteDorisFileReader> create_reader(
        RuntimeProfile* profile, const TFileRangeDesc& range,
        const std::vector<SlotDescriptor*>& slots,
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches, std::shared_ptr<int> close_count,
        std::shared_ptr<io::IOContext> io_ctx = nullptr) {
    auto system_properties = std::make_shared<io::FileSystemProperties>();
    auto file_description = std::make_unique<io::FileDescription>();
    file_description->path = "/dummyPath";
    auto factory = [batches = std::move(batches), close_count](
                           const TFileRangeDesc&,
                           std::unique_ptr<RemoteDorisStream>* stream) mutable {
        *stream = std::make_unique<BatchRemoteDorisStream>(std::move(batches), close_count);
        return Status::OK();
    };
    return std::make_unique<RemoteDorisFileReader>(system_properties, file_description,
                                                   std::move(io_ctx), profile, range, slots,
                                                   std::move(factory));
}

std::unique_ptr<RemoteDorisFileReader> create_flight_reader(
        RuntimeProfile* profile, const TFileRangeDesc& range,
        const std::vector<SlotDescriptor*>& slots, std::shared_ptr<io::IOContext> io_ctx) {
    auto system_properties = std::make_shared<io::FileSystemProperties>();
    auto file_description = std::make_unique<io::FileDescription>();
    file_description->path = "/dummyPath";
    return std::make_unique<RemoteDorisFileReader>(system_properties, file_description,
                                                   std::move(io_ctx), profile, range, slots);
}

Block make_request_block(const std::vector<ColumnDefinition>& schema,
                         const std::vector<int32_t>& local_ids) {
    Block block;
    for (const auto local_id : local_ids) {
        const auto it = std::find_if(schema.begin(), schema.end(), [&](const auto& column) {
            return column.local_id == local_id;
        });
        DORIS_CHECK(it != schema.end());
        block.insert({it->type->create_column(), it->type, it->name});
    }
    return block;
}

int32_t nullable_int_at(const IColumn& column, size_t row) {
    const auto& nullable = assert_cast<const ColumnNullable&>(column);
    const auto& nested = assert_cast<const ColumnInt32&>(nullable.get_nested_column());
    return nested.get_data()[row];
}

std::string nullable_string_at(const IColumn& column, size_t row) {
    const auto& nullable = assert_cast<const ColumnNullable&>(column);
    const auto& nested = assert_cast<const ColumnString&>(nullable.get_nested_column());
    return nested.get_data_at(row).to_string();
}

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

TEST(RemoteDorisV2ReaderTest, BuildsSchemaFromSlotsAndProjectsRequestedColumns) {
    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    const auto slots = remote_slots(&pool, &desc_tbl);
    RuntimeState state;
    RuntimeProfile profile("remote_doris_v2_reader_test");
    auto close_count = std::make_shared<int>(0);
    auto reader = create_reader(&profile, remote_doris_range(), slots, {make_batch({"id", "name"})},
                                close_count);
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
    EXPECT_EQ(schema[0].name, "id");
    EXPECT_EQ(schema[0].local_id, 0);
    EXPECT_EQ(schema[1].name, "name");
    EXPECT_EQ(schema[1].local_id, 1);

    auto request = std::make_shared<FileScanRequest>();
    FileScanRequestBuilder builder(request.get());
    ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(1)).ok());
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_request_block(schema, {1});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 2);
    EXPECT_FALSE(eof);
    EXPECT_EQ(nullable_string_at(*block.get_by_position(0).column, 0), "alice");
    EXPECT_EQ(nullable_string_at(*block.get_by_position(0).column, 1), "bob");

    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_EQ(rows, 0);
    EXPECT_TRUE(eof);
    ASSERT_TRUE(reader->close().ok());
    EXPECT_EQ(*close_count, 1);
}

TEST(RemoteDorisV2ReaderTest, BuildsComplexSchemaChildrenFromSlots) {
    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    const auto slots = remote_complex_slots(&pool, &desc_tbl);
    RuntimeState state;
    RuntimeProfile profile("remote_doris_v2_reader_complex_schema_test");
    auto close_count = std::make_shared<int>(0);
    auto reader = create_reader(&profile, remote_doris_range(), slots, {}, close_count);
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 3);

    ASSERT_EQ(schema[0].name, "c_array_s");
    ASSERT_EQ(schema[0].children.size(), 1);
    EXPECT_EQ(schema[0].children[0].name, "element");
    EXPECT_EQ(schema[0].children[0].local_id, 0);
    EXPECT_TRUE(schema[0].children[0].children.empty());

    ASSERT_EQ(schema[1].name, "c_map");
    ASSERT_EQ(schema[1].children.size(), 2);
    EXPECT_EQ(schema[1].children[0].name, "key");
    EXPECT_EQ(schema[1].children[0].local_id, 0);
    EXPECT_EQ(schema[1].children[1].name, "value");
    EXPECT_EQ(schema[1].children[1].local_id, 1);

    ASSERT_EQ(schema[2].name, "c_struct");
    ASSERT_EQ(schema[2].children.size(), 3);
    EXPECT_EQ(schema[2].children[0].name, "f1");
    EXPECT_EQ(schema[2].children[0].local_id, 0);
    EXPECT_EQ(schema[2].children[1].name, "f2");
    EXPECT_EQ(schema[2].children[1].local_id, 1);
    EXPECT_EQ(schema[2].children[2].name, "f3");
    EXPECT_EQ(schema[2].children[2].local_id, 2);
}

TEST(RemoteDorisV2ReaderTest, HandlesDifferentArrowColumnOrder) {
    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    const auto slots = remote_slots(&pool, &desc_tbl);
    RuntimeState state;
    RuntimeProfile profile("remote_doris_v2_reader_reordered_test");
    auto close_count = std::make_shared<int>(0);
    auto reader = create_reader(&profile, remote_doris_range(), slots, {make_batch({"name", "id"})},
                                close_count);
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<FileScanRequest>();
    FileScanRequestBuilder builder(request.get());
    ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(1)).ok());
    ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(0)).ok());
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_request_block(schema, {1, 0});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 2);
    EXPECT_EQ(nullable_string_at(*block.get_by_position(0).column, 0), "alice");
    EXPECT_EQ(nullable_int_at(*block.get_by_position(1).column, 1), 20);
}

TEST(RemoteDorisV2ReaderTest, AppliesConjunctsAndTracksPredicateFilteredRows) {
    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    const auto slots = remote_slots(&pool, &desc_tbl);
    RuntimeState state;
    RuntimeProfile profile("remote_doris_v2_reader_filter_test");
    auto close_count = std::make_shared<int>(0);
    auto io_ctx = std::make_shared<io::IOContext>();
    auto reader = create_reader(&profile, remote_doris_range(), slots, {make_batch({"id", "name"})},
                                close_count, io_ctx);
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<FileScanRequest>();
    FileScanRequestBuilder builder(request.get());
    ASSERT_TRUE(builder.add_predicate_column(LocalColumnId(0)).ok());
    ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(1)).ok());
    request->conjuncts = {
            prepared_conjunct(&state, std::make_shared<NullableIntGreaterThanExpr>(0, 10))};
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_request_block(schema, {0, 1});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_FALSE(eof);
    EXPECT_EQ(nullable_int_at(*block.get_by_position(0).column, 0), 20);
    EXPECT_EQ(nullable_string_at(*block.get_by_position(1).column, 0), "bob");
    EXPECT_EQ(io_ctx->predicate_filtered_rows, 1);
}

TEST(RemoteDorisV2ReaderTest, RejectsUnknownReturnedColumnAndMissingRequestedColumn) {
    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    const auto slots = remote_slots(&pool, &desc_tbl);
    RuntimeState state;
    RuntimeProfile profile("remote_doris_v2_reader_error_test");

    {
        auto close_count = std::make_shared<int>(0);
        auto reader = create_reader(&profile, remote_doris_range(), slots,
                                    {make_batch({"unknown"})}, close_count);
        ASSERT_TRUE(reader->init(&state).ok());
        std::vector<ColumnDefinition> schema;
        ASSERT_TRUE(reader->get_schema(&schema).ok());
        auto request = std::make_shared<FileScanRequest>();
        FileScanRequestBuilder builder(request.get());
        ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(0)).ok());
        ASSERT_TRUE(reader->open(request).ok());
        auto block = make_request_block(schema, {0});
        size_t rows = 0;
        bool eof = false;
        EXPECT_FALSE(reader->get_block(&block, &rows, &eof).ok());
    }

    {
        auto close_count = std::make_shared<int>(0);
        auto reader = create_reader(&profile, remote_doris_range(), slots, {make_batch({"id"})},
                                    close_count);
        ASSERT_TRUE(reader->init(&state).ok());
        std::vector<ColumnDefinition> schema;
        ASSERT_TRUE(reader->get_schema(&schema).ok());
        auto request = std::make_shared<FileScanRequest>();
        FileScanRequestBuilder builder(request.get());
        ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(1)).ok());
        ASSERT_TRUE(reader->open(request).ok());
        auto block = make_request_block(schema, {1});
        size_t rows = 0;
        bool eof = false;
        EXPECT_FALSE(reader->get_block(&block, &rows, &eof).ok());
    }
}

TEST(RemoteDorisV2ReaderTest, RejectsInvalidRemoteDorisRange) {
    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    const auto slots = remote_slots(&pool, &desc_tbl);
    RuntimeState state;
    RuntimeProfile profile("remote_doris_v2_reader_bad_range_test");
    auto range = remote_doris_range();
    range.table_format_params.__isset.remote_doris_params = false;
    auto close_count = std::make_shared<int>(0);
    auto reader = create_reader(&profile, range, slots, {}, close_count);
    EXPECT_FALSE(reader->init(&state).ok());
}

TEST(RemoteDorisV2ReaderTest, RuntimeCancellationInterruptsBlockedFlightDoGet) {
    BlockingFlightServer server(BlockingFlightServer::Mode::DO_GET);
    const auto server_status = server.start();
    ASSERT_TRUE(server_status.ok()) << server_status;
    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    const auto slots = remote_slots(&pool, &desc_tbl);
    RuntimeState state;
    RuntimeProfile profile("remote_doris_v2_blocked_doget_test");
    auto io_ctx = std::make_shared<io::IOContext>();
    auto reader = create_flight_reader(&profile, remote_doris_range(server), slots, io_ctx);
    ASSERT_TRUE(reader->init(&state).ok());
    auto request = std::make_shared<FileScanRequest>();
    FileScanRequestBuilder builder(request.get());
    ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(0)).ok());

    auto open_result =
            std::async(std::launch::async, [&] { return reader->open(std::move(request)); });
    const bool entered = server.wait_until_entered(std::chrono::seconds(2));
    if (!entered &&
        open_result.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
        FAIL() << "Flight DoGet failed before reaching the server: " << open_result.get();
    }
    ASSERT_TRUE(entered);
    state.cancel(Status::Cancelled("cancel blocked Flight DoGet"));
    const bool interrupted =
            open_result.wait_for(std::chrono::milliseconds(750)) == std::future_status::ready;
    if (!interrupted) {
        server.release();
    }
    EXPECT_TRUE(interrupted);
    EXPECT_FALSE(open_result.get().ok());
}

TEST(RemoteDorisV2ReaderTest, ScannerStopInterruptsBlockedFlightNext) {
    BlockingFlightServer server(BlockingFlightServer::Mode::NEXT);
    const auto server_status = server.start();
    ASSERT_TRUE(server_status.ok()) << server_status;
    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    const auto slots = remote_slots(&pool, &desc_tbl);
    RuntimeState state;
    RuntimeProfile profile("remote_doris_v2_blocked_next_test");
    auto io_ctx = std::make_shared<io::IOContext>();
    auto reader = create_flight_reader(&profile, remote_doris_range(server), slots, io_ctx);
    ASSERT_TRUE(reader->init(&state).ok());
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<FileScanRequest>();
    FileScanRequestBuilder builder(request.get());
    ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(0)).ok());
    const auto open_status = reader->open(std::move(request));
    ASSERT_TRUE(open_status.ok()) << open_status;

    auto block = make_request_block(schema, {0});
    size_t rows = 0;
    bool eof = false;
    auto next_result =
            std::async(std::launch::async, [&] { return reader->get_block(&block, &rows, &eof); });
    ASSERT_TRUE(server.wait_until_entered(std::chrono::seconds(2)));
    io_ctx->request_stop();
    const bool interrupted =
            next_result.wait_for(std::chrono::milliseconds(750)) == std::future_status::ready;
    if (!interrupted) {
        server.release();
    }
    EXPECT_TRUE(interrupted);
    const auto next_status = next_result.get();
    EXPECT_TRUE(!next_status.ok() || (rows == 0 && eof));
    server.release();
}

TEST(RemoteDorisV2ReaderTest, ProductionCancellationWatcherClosesPromptly) {
    BlockingFlightServer server(BlockingFlightServer::Mode::NEXT);
    const auto server_status = server.start();
    ASSERT_TRUE(server_status.ok()) << server_status;
    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    const auto slots = remote_slots(&pool, &desc_tbl);
    RuntimeState state;
    RuntimeProfile profile("remote_doris_v2_prompt_close_test");
    auto reader = create_flight_reader(&profile, remote_doris_range(server), slots,
                                       std::make_shared<io::IOContext>());
    ASSERT_TRUE(reader->init(&state).ok());
    auto request = std::make_shared<FileScanRequest>();
    FileScanRequestBuilder builder(request.get());
    ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(0)).ok());
    ASSERT_TRUE(reader->open(std::move(request)).ok());

    const auto start = std::chrono::steady_clock::now();
    ASSERT_TRUE(reader->close().ok());
    const auto elapsed = std::chrono::steady_clock::now() - start;
    // Closing a split must notify the stop-aware watcher rather than waiting for a polling sleep.
    EXPECT_LT(elapsed, std::chrono::milliseconds(100));
}

TEST(RemoteDorisV2ReaderTest, CancellationStopsBeforeFlightNext) {
    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    const auto slots = remote_slots(&pool, &desc_tbl);
    RuntimeState state;
    RuntimeProfile profile("remote_doris_v2_reader_cancel_test");
    auto close_count = std::make_shared<int>(0);
    auto io_ctx = std::make_shared<io::IOContext>();
    auto reader = create_reader(&profile, remote_doris_range(), slots, {make_batch({"id"})},
                                close_count, io_ctx);
    ASSERT_TRUE(reader->init(&state).ok());
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<FileScanRequest>();
    FileScanRequestBuilder builder(request.get());
    ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(0)).ok());
    ASSERT_TRUE(reader->open(request).ok());

    io_ctx->should_stop = true;
    auto block = make_request_block(schema, {0});
    size_t rows = 99;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_EQ(rows, 0);
    EXPECT_TRUE(eof);
    EXPECT_EQ(*close_count, 1);
}

} // namespace doris::format::remote_doris
