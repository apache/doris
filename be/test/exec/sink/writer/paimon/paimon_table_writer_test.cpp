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

#include "exec/sink/writer/paimon/paimon_table_writer.h"

#include <gtest/gtest.h>

#include <functional>

#include "runtime/runtime_state.h"
#include "util/debug_points.h"

namespace doris {
namespace {

// Observe ownership and injected exceptions without loading JNI or the native SDK.
struct Calls {
    int prepared = 0;
    int aborted = 0;
    int closed = 0;
    int writers_destroyed = 0;
    int backends_destroyed = 0;
    int outputs_discarded = 0;
    bool transferred = false;
    std::function<void()> on_prepare = [] {};
    std::function<void()> on_abort = [] {};
    std::function<void()> on_close = [] {};
};

class TestWriter final : public IPaimonWriter {
public:
    explicit TestWriter(Calls& calls) : _calls(calls) {}
    ~TestWriter() override { ++_calls.writers_destroyed; }
    Status write(RuntimeState*, Block&) override { return Status::OK(); }
    Status prepare_commit(std::vector<TPaimonCommitMessage>& messages) override {
        ++_calls.prepared;
        _calls.on_prepare();
        TPaimonCommitMessage message;
        message.__set_payload("test-payload");
        messages.push_back(std::move(message));
        return Status::OK();
    }
    Status abort() override {
        ++_calls.aborted;
        _calls.on_abort();
        return Status::OK();
    }

private:
    Calls& _calls;
};

class TestBackend final : public IPaimonWriteBackend {
public:
    explicit TestBackend(Calls& calls) : _calls(calls) {}
    ~TestBackend() override {
        ++_calls.backends_destroyed;
        if (!_calls.transferred) ++_calls.outputs_discarded;
    }
    Status open(const TPaimonTableSink&, RuntimeState*, RuntimeProfile*) override {
        return Status::OK();
    }
    Status create_writer(std::unique_ptr<IPaimonWriter>* writer) override {
        *writer = std::make_unique<TestWriter>(_calls);
        return Status::OK();
    }
    Status close() override {
        EXPECT_EQ(1, _calls.writers_destroyed);
        ++_calls.closed;
        _calls.on_close();
        return Status::OK();
    }
    void on_commit_messages_transferred() override {
        EXPECT_EQ(1, _calls.closed);
        _calls.transferred = true;
    }
    PaimonBackendType type() const override { return PaimonBackendType::CPP; }

private:
    Calls& _calls;
};
} // namespace

class PaimonTableWriterTest : public testing::Test {
protected:
    std::unique_ptr<PaimonTableWriter> make_writer(Calls& calls) {
        TDataSink sink;
        sink.__set_paimon_table_sink(TPaimonTableSink {});
        auto writer = std::make_unique<PaimonTableWriter>(std::move(sink), _expressions);
        writer->_state = &_state;
        writer->_backend = std::make_unique<TestBackend>(calls);
        writer->_writer = std::make_unique<TestWriter>(calls);
        writer->_close_timer = ADD_TIMER(&_profile, "CloseTime");
        writer->_prepare_commit_timer = ADD_TIMER(&_profile, "PrepareTime");
        writer->_commit_payload_count = ADD_COUNTER(&_profile, "PayloadCount", TUnit::UNIT);
        writer->_commit_payload_bytes_counter =
                ADD_COUNTER(&_profile, "PayloadBytes", TUnit::BYTES);
        return writer;
    }

    VExprContextSPtrs _expressions;
    RuntimeState _state;
    RuntimeProfile _profile {"PaimonTableWriterTest"};
};

TEST_F(PaimonTableWriterTest, SuccessfulHandoffAndRepeatedClose) {
    Calls calls;
    auto writer = make_writer(calls);
    EXPECT_TRUE(writer->close(Status::OK()).ok());
    EXPECT_EQ(1, calls.prepared);
    EXPECT_EQ(0, calls.aborted);
    EXPECT_EQ(1, calls.closed);
    EXPECT_EQ(1, calls.backends_destroyed);
    EXPECT_TRUE(calls.transferred);
    EXPECT_EQ(0, calls.outputs_discarded);
    ASSERT_EQ(1, _state.paimon_commit_messages().size());
    EXPECT_TRUE(writer->close(Status::OK()).ok());
    EXPECT_EQ(1, calls.prepared);
    EXPECT_EQ(1, calls.closed);
}

TEST_F(PaimonTableWriterTest, PrepareOomStillAbortsClosesAndDestroys) {
    Calls calls;
    calls.on_prepare = [] { throw std::bad_alloc(); };
    auto writer = make_writer(calls);
    EXPECT_TRUE(writer->close(Status::OK()).is<ErrorCode::MEM_LIMIT_EXCEEDED>());
    EXPECT_EQ(1, calls.aborted);
    EXPECT_EQ(1, calls.closed);
    EXPECT_EQ(1, calls.backends_destroyed);
    EXPECT_EQ(1, calls.outputs_discarded);
    EXPECT_TRUE(_state.paimon_commit_messages().empty());
}

TEST_F(PaimonTableWriterTest, AbortAndCloseExceptionsPreserveOriginalError) {
    Calls calls;
    calls.on_abort = [] { throw std::bad_alloc(); };
    calls.on_close = [] { throw doris::Exception(ErrorCode::IO_ERROR, "close failed"); };
    auto writer = make_writer(calls);
    auto result = writer->close(Status::Cancelled("original task failure"));
    EXPECT_TRUE(result.is<ErrorCode::CANCELLED>());
    EXPECT_NE(std::string::npos, result.to_string().find("original task failure"));
    EXPECT_EQ(0, calls.prepared);
    EXPECT_EQ(1, calls.aborted);
    EXPECT_EQ(1, calls.closed);
    EXPECT_EQ(1, calls.backends_destroyed);
    EXPECT_EQ(1, calls.outputs_discarded);
}

TEST_F(PaimonTableWriterTest, CloseExceptionDiscardsUntransferredOutputs) {
    Calls calls;
    calls.on_close = [] { throw std::bad_alloc(); };
    auto writer = make_writer(calls);
    EXPECT_TRUE(writer->close(Status::OK()).is<ErrorCode::MEM_LIMIT_EXCEEDED>());
    EXPECT_EQ(1, calls.prepared);
    EXPECT_EQ(1, calls.closed);
    EXPECT_EQ(1, calls.backends_destroyed);
    EXPECT_EQ(1, calls.outputs_discarded);
    EXPECT_FALSE(calls.transferred);
    EXPECT_TRUE(_state.paimon_commit_messages().empty());
}

TEST_F(PaimonTableWriterTest, MessageRetentionOomCleansUpAndNextWriterCanFinish) {
    Calls failed;
    auto writer = make_writer(failed);
    {
        const bool old_enabled = config::enable_debug_points;
        const std::string point = "PaimonTableWriter.close.store_messages_oom";
        Defer restore {[&] {
            DebugPoints::instance()->remove(point);
            config::enable_debug_points = old_enabled;
        }};
        config::enable_debug_points = true;
        DebugPoints::instance()->add(point);
        EXPECT_TRUE(writer->close(Status::OK()).is<ErrorCode::MEM_LIMIT_EXCEEDED>());
    }
    EXPECT_EQ(1, failed.closed);
    EXPECT_EQ(1, failed.backends_destroyed);
    EXPECT_EQ(1, failed.outputs_discarded);
    EXPECT_FALSE(failed.transferred);
    EXPECT_TRUE(_state.paimon_commit_messages().empty());

    Calls next;
    auto next_writer = make_writer(next);
    EXPECT_TRUE(next_writer->close(Status::OK()).ok());
    EXPECT_TRUE(next.transferred);
    EXPECT_EQ(0, next.outputs_discarded);
    EXPECT_EQ(1, _state.paimon_commit_messages().size());
}

} // namespace doris
