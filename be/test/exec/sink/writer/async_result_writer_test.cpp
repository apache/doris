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

#include "exec/sink/writer/async_result_writer.h"

#include <gtest/gtest.h>

#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "exec/pipeline/dependency.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "runtime/workload_management/resource_context.h"

namespace doris {

namespace {

const VExprContextSPtrs EMPTY_OUTPUT_EXPRS;

Block make_block() {
    auto values = ColumnInt32::create();
    values->insert_value(1);
    Block block;
    block.insert({std::move(values), std::make_shared<DataTypeInt32>(), "value"});
    return block;
}

class RecordingAsyncWriter final : public AsyncResultWriter {
public:
    RecordingAsyncWriter(std::shared_ptr<Dependency> dependency,
                         std::shared_ptr<Dependency> finish_dependency, Status open_status)
            : AsyncResultWriter(EMPTY_OUTPUT_EXPRS, std::move(dependency),
                                std::move(finish_dependency)),
              _open_status(std::move(open_status)) {}

    Status open(RuntimeState*, RuntimeProfile*) override { return _open_status; }

    Status write(RuntimeState*, Block&) override {
        reservation_seen_by_write = thread_context()->thread_mem_tracker_mgr->reserved_mem();
        return Status::OK();
    }

    Status finish(RuntimeState*) override {
        reservation_seen_by_finish = thread_context()->thread_mem_tracker_mgr->reserved_mem();
        return Status::OK();
    }

    Status close(Status) override { return Status::OK(); }

    int64_t reservation_seen_by_write = 0;
    int64_t reservation_seen_by_finish = 0;

private:
    Status _open_status;
};

struct AsyncWriterHarness {
    AsyncWriterHarness()
            : dependency(std::make_shared<Dependency>(0, 0, "writer", true)),
              finish_dependency(std::make_shared<Dependency>(0, 0, "finish", false)),
              common_profile("CommonCounters"),
              memory_usage(common_profile.AddHighWaterMarkCounter("MemoryUsage", TUnit::BYTES)) {}

    void prepare(AsyncResultWriter* writer) {
        writer->_operator_profile = &operator_profile;
        writer->_memory_used_counter = memory_usage;
    }

    void process(AsyncResultWriter* writer) { writer->process_block(nullptr, &operator_profile); }

    std::shared_ptr<Dependency> dependency;
    std::shared_ptr<Dependency> finish_dependency;
    RuntimeProfile operator_profile {"operator"};
    RuntimeProfile common_profile;
    RuntimeProfile::Counter* memory_usage;
};

} // namespace

class AsyncResultWriterTest : public testing::Test {
protected:
    void SetUp() override {
        _exec_env = ExecEnv::GetInstance();
        if (_exec_env->fragment_mgr() == nullptr) {
            _fragment_mgr = std::make_unique<FragmentMgr>(_exec_env);
            _exec_env->_fragment_mgr = _fragment_mgr.get();
        }
        _tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                    "UT-AsyncResultWriterReservation");
        _resource_context = ResourceContext::create_shared();
        _resource_context->memory_context()->set_mem_tracker(_tracker);
        thread_context()->attach_task(_resource_context);
    }

    void TearDown() override {
        thread_context()->thread_mem_tracker_mgr->shrink_reserved();
        thread_context()->detach_task();
        EXPECT_EQ(0, GlobalMemoryArbitrator::process_reserved_memory());
        if (_fragment_mgr != nullptr) {
            _fragment_mgr->stop();
            _exec_env->_fragment_mgr = nullptr;
            _fragment_mgr.reset();
        }
    }

    std::shared_ptr<MemTrackerLimiter> _tracker;
    std::shared_ptr<ResourceContext> _resource_context;
    ExecEnv* _exec_env = nullptr;
    std::unique_ptr<FragmentMgr> _fragment_mgr;
};

TEST_F(AsyncResultWriterTest, TransfersQueuedReservationIntoActualWrite) {
    AsyncWriterHarness harness;
    RecordingAsyncWriter writer(harness.dependency, harness.finish_dependency, Status::OK());
    harness.prepare(&writer);
    constexpr int64_t reservation = 4 * 1024 * 1024;
    Block block = make_block();
    ASSERT_TRUE(thread_context()->thread_mem_tracker_mgr->try_reserve(reservation).ok());

    ASSERT_TRUE(writer.sink(&block, true).ok());
    EXPECT_EQ(0, thread_context()->thread_mem_tracker_mgr->reserved_mem());
    harness.process(&writer);

    EXPECT_GT(writer.reservation_seen_by_write, 0);
    EXPECT_LE(writer.reservation_seen_by_write, reservation);
    EXPECT_EQ(0, thread_context()->thread_mem_tracker_mgr->reserved_mem());
}

TEST_F(AsyncResultWriterTest, RetainsEosReservationThroughActualFinish) {
    AsyncWriterHarness harness;
    RecordingAsyncWriter writer(harness.dependency, harness.finish_dependency, Status::OK());
    harness.prepare(&writer);
    constexpr int64_t reservation = 4 * 1024 * 1024;
    ASSERT_TRUE(thread_context()->thread_mem_tracker_mgr->try_reserve(reservation).ok());
    Block block;

    ASSERT_TRUE(writer.sink(&block, true).ok());
    harness.process(&writer);

    EXPECT_EQ(reservation, writer.reservation_seen_by_finish);
    EXPECT_EQ(0, thread_context()->thread_mem_tracker_mgr->reserved_mem());
}

TEST_F(AsyncResultWriterTest, OpenFailureDrainsQueuedReservation) {
    AsyncWriterHarness harness;
    RecordingAsyncWriter writer(harness.dependency, harness.finish_dependency,
                                Status::IOError("injected open failure"));
    harness.prepare(&writer);
    constexpr int64_t reservation = 4 * 1024 * 1024;
    Block block = make_block();
    ASSERT_TRUE(thread_context()->thread_mem_tracker_mgr->try_reserve(reservation).ok());

    ASSERT_TRUE(writer.sink(&block, true).ok());
    harness.process(&writer);

    EXPECT_FALSE(writer.get_writer_status().ok());
    EXPECT_EQ(0, thread_context()->thread_mem_tracker_mgr->reserved_mem());
    EXPECT_EQ(0, GlobalMemoryArbitrator::process_reserved_memory());
}

} // namespace doris
