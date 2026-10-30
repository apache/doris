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

#include "exec/sink/writer/paimon/ffi_paimon_write_backend.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>

#include "common/config.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "util/defer_op.h"

namespace doris {

class FfiPaimonWriteBackendTest : public testing::Test {
protected:
    void SetUp() override {
        _root = std::filesystem::current_path() /
                ("paimon-rust-writer-" +
                 std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(_root);
        TUniqueId id;
        id.hi = 77;
        id.lo = 88;
        TQueryOptions options;
        options.__set_mem_limit(2L * 1024 * 1024 * 1024);
        options.__set_query_type(TQueryType::SELECT);
        TNetworkAddress address;
        address.hostname = "127.0.0.1";
        address.port = 9030;
        _query_ctx = QueryContext::create(id, ExecEnv::GetInstance(), options, address, true,
                                          address, QuerySource::INTERNAL_FRONTEND);
        _state = RuntimeState::create_unique(id, 0, options, _query_ctx->query_globals,
                                             ExecEnv::GetInstance(), _query_ctx.get());
        _state->set_task_num(1);
        TPaimonTableDesc desc;
        desc.__set_table_path(_root.string());
        desc.__set_schema_json(R"({
            "version":3,"id":0,
            "fields":[{"id":0,"name":"id","type":"INT"}],
            "highestFieldId":0,"partitionKeys":[],"primaryKeys":[],
            "options":{"bucket":"-1","file.format":"parquet",
                       "target-file-size":"1 b","write-buffer-size":"8 mb"},
            "timeMillis":0
        })");
        desc.__set_database_name("default");
        desc.__set_table_name("test");
        desc.__set_branch("main");
        desc.__set_storage_options({});
        desc.__set_commit_message_version(11);
        _sink.__set_table_desc(desc);
        _sink.__set_backend_type(TPaimonWriteBackendType::FFI);
        _sink.__set_write_mode(TPaimonWriteMode::APPEND);
        _sink.__set_commit_user("doris-rust-test");
        _sink.__set_transaction_id(1);
        _sink.__set_column_names({"id"});
    }

    void TearDown() override {
        _writer.reset();
        EXPECT_TRUE(_backend.close().ok());
        std::filesystem::remove_all(_root);
    }

    Status open() {
        RETURN_IF_ERROR(_backend.open(_sink, _state.get(), &_profile));
        return _backend.create_writer(&_writer);
    }

    Block input() {
        auto values = ColumnInt32::create();
        values->insert_value(10);
        values->insert_value(20);
        values->insert_value(30);
        return Block({{std::move(values), std::make_shared<DataTypeInt32>(), "id"}});
    }

    size_t file_count() const {
        size_t result = 0;
        for (const auto& entry : std::filesystem::recursive_directory_iterator(_root)) {
            result += entry.is_regular_file();
        }
        return result;
    }

    std::filesystem::path _root;
    std::shared_ptr<QueryContext> _query_ctx;
    std::unique_ptr<RuntimeState> _state;
    RuntimeProfile _profile {"rust-writer-test"};
    TPaimonTableSink _sink;
    FfiPaimonWriteBackend _backend;
    std::unique_ptr<IPaimonWriter> _writer;
};

TEST_F(FfiPaimonWriteBackendTest, WritesDpcmAndReleasesFixedBudget) {
    const auto before = _state->query_mem_tracker()->consumption();
    ASSERT_TRUE(open().ok());
    EXPECT_GT(_state->query_mem_tracker()->consumption(), before);
    auto block = input();
    ASSERT_TRUE(_writer->write(_state.get(), block).ok());
    std::vector<TPaimonCommitMessage> messages;
    ASSERT_TRUE(_writer->prepare_commit(messages).ok());
    ASSERT_EQ(messages.size(), 1);
    const auto& payload = messages.front().payload;
    ASSERT_GE(payload.size(), 16);
    EXPECT_EQ(payload.substr(0, 4), "DPCM");
    EXPECT_EQ(static_cast<unsigned char>(payload[7]), 11);
    EXPECT_EQ(file_count(), 1);
    _writer.reset();
    ASSERT_TRUE(_backend.close().ok());
    EXPECT_EQ(_state->query_mem_tracker()->consumption(), before);
    // Once prepared, normal shutdown must preserve files for the FE transaction.
    EXPECT_EQ(file_count(), 1);
}

TEST_F(FfiPaimonWriteBackendTest, AbortRemovesProducerOwnedRolledFiles) {
    ASSERT_TRUE(open().ok());
    auto block = input();
    ASSERT_TRUE(_writer->write(_state.get(), block).ok());
    EXPECT_EQ(file_count(), 1);
    ASSERT_TRUE(_writer->abort().ok());
    _writer.reset();
    ASSERT_TRUE(_backend.close().ok());
    EXPECT_EQ(file_count(), 0);
}

TEST_F(FfiPaimonWriteBackendTest, NullableInputUsesPinnedArrowSchema) {
    ASSERT_TRUE(open().ok());
    auto values = ColumnInt32::create();
    values->insert_value(10);
    values->insert_value(0);
    auto nulls = ColumnUInt8::create();
    nulls->insert_value(0);
    nulls->insert_value(1);
    Block block({{ColumnNullable::create(std::move(values), std::move(nulls)),
                  make_nullable(std::make_shared<DataTypeInt32>()), "id"}});
    ASSERT_TRUE(_writer->write(_state.get(), block).ok());
    std::vector<TPaimonCommitMessage> messages;
    ASSERT_TRUE(_writer->prepare_commit(messages).ok());
    EXPECT_EQ(messages.size(), 1);
    ASSERT_TRUE(_writer->abort().ok());
    EXPECT_EQ(file_count(), 0);
}

TEST_F(FfiPaimonWriteBackendTest, OversizedRowFailsBeforeArrowAllocation) {
    auto json = _sink.table_desc.schema_json;
    json.replace(json.find("\"INT\""), 5, "\"STRING\"");
    _sink.table_desc.__set_schema_json(json);
    ASSERT_TRUE(open().ok());
    const auto before = _state->query_mem_tracker()->consumption();
    auto values = ColumnString::create();
    std::string large(1024 * 1024, 'x');
    values->insert_data(large.data(), large.size());
    Block block({{std::move(values), std::make_shared<DataTypeString>(), "id"}});
    auto status = _writer->write(_state.get(), block);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("exceeds 1 MiB"), std::string::npos);
    EXPECT_EQ(_state->query_mem_tracker()->consumption(), before);
    ASSERT_TRUE(_writer->abort().ok());
    EXPECT_EQ(file_count(), 0);
}

TEST_F(FfiPaimonWriteBackendTest, LargeBlockIsSplitBeforeImportingIntoRust) {
    auto json = _sink.table_desc.schema_json;
    json.replace(json.find("\"INT\""), 5, "\"STRING\"");
    _sink.table_desc.__set_schema_json(json);
    ASSERT_TRUE(open().ok());
    auto values = ColumnString::create();
    std::string value(4096, 'x');
    for (int row = 0; row < 5000; ++row) {
        values->insert_data(value.data(), value.size());
    }
    Block block({{std::move(values), std::make_shared<DataTypeString>(), "id"}});
    ASSERT_TRUE(_writer->write(_state.get(), block).ok());
    // The 1-byte file target rolls each imported batch synchronously.
    EXPECT_EQ(file_count(), 3);
    std::vector<TPaimonCommitMessage> messages;
    ASSERT_TRUE(_writer->prepare_commit(messages).ok());
    EXPECT_EQ(messages.size(), 1);
    ASSERT_TRUE(_writer->abort().ok());
    EXPECT_EQ(file_count(), 0);
}

TEST_F(FfiPaimonWriteBackendTest, AbortBeforePublicationRemovesPreparedFiles) {
    ASSERT_TRUE(open().ok());
    auto block = input();
    ASSERT_TRUE(_writer->write(_state.get(), block).ok());
    std::vector<TPaimonCommitMessage> messages;
    ASSERT_TRUE(_writer->prepare_commit(messages).ok());
    EXPECT_EQ(file_count(), 1);
    ASSERT_TRUE(_writer->abort().ok());
    EXPECT_EQ(file_count(), 0);
}

TEST_F(FfiPaimonWriteBackendTest, ResourceExhaustionFailsAndReleasesBudget) {
    const auto previous = config::paimon_rust_writer_memory_limit_bytes;
    Defer restore([&] { config::paimon_rust_writer_memory_limit_bytes = previous; });
    config::paimon_rust_writer_memory_limit_bytes = 1;
    const auto before = _state->query_mem_tracker()->consumption();
    ASSERT_TRUE(open().ok());
    auto block = input();
    auto status = _writer->write(_state.get(), block);
    EXPECT_TRUE(status.is<ErrorCode::QUERY_MEMORY_EXCEEDED>()) << status;
    ASSERT_TRUE(_writer->abort().ok());
    _writer.reset();
    ASSERT_TRUE(_backend.close().ok());
    EXPECT_EQ(file_count(), 0);
    EXPECT_EQ(_state->query_mem_tracker()->consumption(), before);
}

TEST_F(FfiPaimonWriteBackendTest, CancellationDoesNotSendAnotherBatch) {
    ASSERT_TRUE(open().ok());
    _state->cancel(Status::Cancelled("test cancellation"));
    auto block = input();
    EXPECT_FALSE(_writer->write(_state.get(), block).ok());
    ASSERT_TRUE(_writer->abort().ok());
    EXPECT_EQ(file_count(), 0);
}

TEST_F(FfiPaimonWriteBackendTest, AdmissionSlotIsReturnedAfterClose) {
    const auto previous = config::paimon_rust_writer_max_concurrency;
    Defer restore([&] { config::paimon_rust_writer_max_concurrency = previous; });
    config::paimon_rust_writer_max_concurrency = 1;
    ASSERT_TRUE(open().ok());
    FfiPaimonWriteBackend second;
    EXPECT_FALSE(second.open(_sink, _state.get(), &_profile).ok());
    _writer.reset();
    ASSERT_TRUE(_backend.close().ok());
    RuntimeProfile third_profile {"third-writer"};
    FfiPaimonWriteBackend third;
    ASSERT_TRUE(third.open(_sink, _state.get(), &third_profile).ok());
    ASSERT_TRUE(third.close().ok());
}

TEST_F(FfiPaimonWriteBackendTest, QueryBudgetFailureReturnsAdmissionSlot) {
    const auto previous = config::paimon_rust_writer_max_concurrency;
    Defer restore([&] { config::paimon_rust_writer_max_concurrency = previous; });
    config::paimon_rust_writer_max_concurrency = 1;
    auto tracker = _state->query_mem_tracker();
    {
        const auto charge = tracker->limit();
        tracker->consume(charge);
        Defer release([&] { tracker->release(charge); });
        FfiPaimonWriteBackend rejected;
        auto status = rejected.open(_sink, _state.get(), &_profile);
        EXPECT_TRUE(status.is<ErrorCode::QUERY_MEMORY_EXCEEDED>()) << status;
        ASSERT_TRUE(rejected.close().ok());
    }
    ASSERT_TRUE(open().ok());
}

} // namespace doris
