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
#include "common/metrics/doris_metrics.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exec/sink/writer/paimon/jni_paimon_write_backend.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
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
        _initial_query_memory = _state->query_mem_tracker()->consumption();
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
        auto status = _backend.close();
        EXPECT_TRUE(status.ok()) << status;
        EXPECT_EQ(_state->query_mem_tracker()->consumption(), _initial_query_memory);
        std::filesystem::remove_all(_root);
    }

    Status open() {
        RETURN_IF_ERROR(_backend.open(_sink, _state.get(), &_profile));
        return _backend.create_writer(&_writer);
    }

    Status write(Block& block) {
        // Pipeline tasks attach the query before converting input to Arrow.
        // Keep the input Block outside that scope so these tests measure only
        // the backend's budget and Arrow buffers.
        SCOPED_ATTACH_TASK(_state.get());
        return _writer->write(_state.get(), block);
    }

    int64_t counter(const std::string& name) {
        return _profile.get_child("RustPaimonWriteBackend")->get_counter(name)->value();
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
    int64_t _initial_query_memory = 0;
};

TEST_F(FfiPaimonWriteBackendTest, WritesDpcmAndReleasesFixedBudget) {
    // Leave the file open until prepare_commit, unlike the rolled-file abort cases.
    auto json = _sink.table_desc.schema_json;
    json.replace(json.find("\"1 b\""), 5, "\"128 mb\"");
    _sink.table_desc.__set_schema_json(json);
    const auto before = _state->query_mem_tracker()->consumption();
    ASSERT_TRUE(open().ok());
    EXPECT_GT(_state->query_mem_tracker()->consumption(), before);
    auto block = input();
    ASSERT_TRUE(write(block).ok());
    EXPECT_EQ(counter("InputRows"), 3);
    EXPECT_EQ(counter("InputBatches"), 1);
    EXPECT_GT(counter("InputEstimatedBytes"), 0);
    EXPECT_GT(counter("PeakArrowBufferBytes"), 0);
    EXPECT_GT(counter("QueryReservedBytes"), 0);
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
    EXPECT_EQ(counter("QueryReservedBytes"), 0);
    EXPECT_EQ(counter("ReservedMemoryBytes"), 0);
    EXPECT_EQ(counter("ArrowBufferBytes"), 0);
    EXPECT_EQ(counter("PreparedCommitMessages"), 1);
    auto* entity = DorisMetrics::instance()->server_entity();
    EXPECT_EQ(static_cast<IntGauge*>(entity->get_metric("paimon_rust_active_writers"))->value(), 0);
    EXPECT_EQ(static_cast<IntGauge*>(entity->get_metric("paimon_rust_fixed_memory_bytes"))->value(),
              0);
    // Once prepared, normal shutdown must preserve files for the FE transaction.
    EXPECT_EQ(file_count(), 1);
}

TEST_F(FfiPaimonWriteBackendTest, AbortRemovesProducerOwnedRolledFiles) {
    ASSERT_TRUE(open().ok());
    auto block = input();
    ASSERT_TRUE(write(block).ok());
    EXPECT_EQ(file_count(), 1);
    ASSERT_TRUE(_writer->abort().ok());
    _writer.reset();
    ASSERT_TRUE(_backend.close().ok());
    EXPECT_EQ(file_count(), 0);
}

TEST_F(FfiPaimonWriteBackendTest, MissingDescriptorReturnsErrorForBothBackends) {
    _sink.__isset.table_desc = false;
    auto status = _backend.open(_sink, _state.get(), &_profile);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(counter("QueryReservedBytes"), 0);
    EXPECT_TRUE(_backend.close().ok());
    JniPaimonWriteBackend jni;
    status = jni.open(_sink, _state.get(), &_profile);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("upgrade the FE"), std::string::npos);
    EXPECT_TRUE(jni.close().ok());
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
    ASSERT_TRUE(write(block).ok());
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
    auto status = write(block);
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
    ASSERT_TRUE(write(block).ok());
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
    ASSERT_TRUE(write(block).ok());
    std::vector<TPaimonCommitMessage> messages;
    ASSERT_TRUE(_writer->prepare_commit(messages).ok());
    EXPECT_EQ(file_count(), 1);
    ASSERT_TRUE(_writer->abort().ok());
    EXPECT_EQ(file_count(), 0);
}

TEST_F(FfiPaimonWriteBackendTest, ResourceExhaustionFailsAndReleasesBudget) {
    const auto previous = config::paimon_rust_writer_memory_limit_bytes;
    Defer restore([&] { config::paimon_rust_writer_memory_limit_bytes = previous; });
    config::paimon_rust_writer_memory_limit_bytes = 128 * 1024;
    const auto before = _state->query_mem_tracker()->consumption();
    ASSERT_TRUE(open().ok());
    auto block = input();
    ASSERT_TRUE(write(block).ok());
    ASSERT_EQ(file_count(), 1);
    // A second batch exceeds the native budget after the first file has rolled.
    auto values = ColumnInt32::create();
    for (int row = 0; row < 40000; ++row) {
        values->insert_value(row);
    }
    Block over_budget({{std::move(values), std::make_shared<DataTypeInt32>(), "id"}});
    auto status = write(over_budget);
    EXPECT_TRUE(status.is<ErrorCode::QUERY_MEMORY_EXCEEDED>()) << status;
    EXPECT_EQ(counter("FailedOperations"), 1);
    ASSERT_TRUE(_writer->abort().ok());
    _writer.reset();
    ASSERT_TRUE(_backend.close().ok());
    EXPECT_EQ(file_count(), 0);
    EXPECT_EQ(_state->query_mem_tracker()->consumption(), before);
}

TEST_F(FfiPaimonWriteBackendTest, CancellationDoesNotSendAnotherBatch) {
    ASSERT_TRUE(open().ok());
    auto block = input();
    ASSERT_TRUE(write(block).ok());
    ASSERT_EQ(file_count(), 1);
    _state->cancel(Status::Cancelled("test cancellation"));
    EXPECT_FALSE(write(block).ok());
    EXPECT_EQ(file_count(), 1);
    ASSERT_TRUE(_writer->abort().ok());
    EXPECT_EQ(file_count(), 0);
}

TEST_F(FfiPaimonWriteBackendTest, AdmissionSlotIsReturnedAfterClose) {
    const auto previous = config::paimon_rust_writer_max_concurrency;
    Defer restore([&] { config::paimon_rust_writer_max_concurrency = previous; });
    config::paimon_rust_writer_max_concurrency = 1;
    ASSERT_TRUE(open().ok());
    RuntimeProfile second_profile {"rejected-writer"};
    FfiPaimonWriteBackend second;
    EXPECT_FALSE(second.open(_sink, _state.get(), &second_profile).ok());
    auto* rejected_profile = second_profile.get_child("RustPaimonWriteBackend");
    ASSERT_NE(rejected_profile, nullptr);
    EXPECT_EQ(rejected_profile->get_counter("AdmissionRejections")->value(), 1);
    ASSERT_TRUE(second.close().ok());
    _writer.reset();
    ASSERT_TRUE(_backend.close().ok());
    RuntimeProfile third_profile {"third-writer"};
    FfiPaimonWriteBackend third;
    ASSERT_TRUE(third.open(_sink, _state.get(), &third_profile).ok());
    ASSERT_TRUE(third.close().ok());
}

TEST_F(FfiPaimonWriteBackendTest, FailedOpenReturnsBudgetAndAdmissionSlot) {
    const auto previous = config::paimon_rust_writer_max_concurrency;
    Defer restore([&] { config::paimon_rust_writer_max_concurrency = previous; });
    config::paimon_rust_writer_max_concurrency = 1;
    auto tracker = _state->query_mem_tracker();
    const auto before = tracker->consumption();
    {
        const auto charge = tracker->limit();
        tracker->consume(charge);
        Defer release([&] { tracker->release(charge); });
        RuntimeProfile rejected_profile {"rejected-query-budget"};
        FfiPaimonWriteBackend rejected;
        auto status = rejected.open(_sink, _state.get(), &rejected_profile);
        EXPECT_TRUE(status.is<ErrorCode::QUERY_MEMORY_EXCEEDED>()) << status;
        ASSERT_TRUE(rejected.close().ok());
    }
    EXPECT_EQ(tracker->consumption(), before);
    {
        // Fail after admission has charged the budget and created native resources.
        auto invalid_sink = _sink;
        invalid_sink.table_desc.__set_schema_json("{");
        RuntimeProfile invalid_profile {"invalid-schema"};
        FfiPaimonWriteBackend rejected;
        EXPECT_FALSE(rejected.open(invalid_sink, _state.get(), &invalid_profile).ok());
        ASSERT_TRUE(rejected.close().ok());
        EXPECT_EQ(tracker->consumption(), before);
    }
    ASSERT_TRUE(open().ok());
}

} // namespace doris
