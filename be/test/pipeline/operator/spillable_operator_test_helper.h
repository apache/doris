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

#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gmock/gmock-actions.h>
#include <gmock/gmock-function-mocker.h>
#include <gmock/gmock-spec-builders.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

#include "common/object_pool.h"
#include "pipeline/pipeline_task.h"
#include "runtime/fragment_mgr.h"
#include "testutil/mock/mock_runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {

class MockPartitioner : public vectorized::PartitionerBase {
public:
    MockPartitioner(size_t partition_count) : PartitionerBase(partition_count) {}
    Status init(const std::vector<TExpr>& texprs) override { return Status::OK(); }

    Status prepare(RuntimeState* state, const RowDescriptor& row_desc) override {
        return Status::OK();
    }

    Status open(RuntimeState* state) override { return Status::OK(); }

    Status close(RuntimeState* state) override { return Status::OK(); }

    Status do_partitioning(RuntimeState* state, vectorized::Block* block, bool eos,
                           bool* already_sent) const override {
        if (already_sent) {
            *already_sent = false;
        }
        return Status::OK();
    }

    Status clone(RuntimeState* state, std::unique_ptr<PartitionerBase>& partitioner) override {
        partitioner = std::make_unique<MockPartitioner>(_partition_count);
        return Status::OK();
    }

    vectorized::ChannelField get_channel_ids() const override { return {}; }
};

class MockExpr : public vectorized::VExpr {
public:
    Status prepare(RuntimeState* state, const RowDescriptor& row_desc,
                   vectorized::VExprContext* context) override {
        return Status::OK();
    }

    Status open(RuntimeState* state, vectorized::VExprContext* context,
                FunctionContext::FunctionStateScope scope) override {
        return Status::OK();
    }
};

class MockFragmentManager : public FragmentMgr {
public:
    MockFragmentManager(Status& status_, ExecEnv* exec_env)
            : FragmentMgr(exec_env), status(status_) {}
    void cancel_query(const TUniqueId query_id, const Status reason) override { status = reason; }

private:
    Status& status;
};

class SpillableDebugPointHelper {
public:
    SpillableDebugPointHelper(const std::string name)
            : _enable_debug_points(config::enable_debug_points),
              _fragment_mgr(ExecEnv::GetInstance()->_fragment_mgr) {
        config::enable_debug_points = true;
        ExecEnv::GetInstance()->_fragment_mgr =
                new MockFragmentManager(_spill_status, ExecEnv::GetInstance());
        DebugPoints::instance()->add(name);
    }

    ~SpillableDebugPointHelper() {
        config::enable_debug_points = _enable_debug_points;
        ExecEnv::GetInstance()->_fragment_mgr->stop();
        SAFE_DELETE(ExecEnv::GetInstance()->_fragment_mgr);
        ExecEnv::GetInstance()->_fragment_mgr = _fragment_mgr;
    }

    const Status& get_spill_status() const { return _spill_status; }

private:
    Status _spill_status;
    const bool _enable_debug_points;
    FragmentMgr* const _fragment_mgr;
};

class SpillableOperatorTestHelper {
public:
    virtual ~SpillableOperatorTestHelper() = default;
    void SetUp();
    void TearDown();

    virtual TPlanNode create_test_plan_node() = 0;
    virtual TDescriptorTable create_test_table_descriptor(bool nullable) = 0;

    std::unique_ptr<MockRuntimeState> runtime_state;
    std::unique_ptr<ObjectPool> obj_pool;
    std::shared_ptr<QueryContext> query_ctx;
    std::shared_ptr<RuntimeProfile> runtime_profile;
    std::shared_ptr<PipelineTask> pipeline_task;
    DescriptorTbl* desc_tbl;
    static constexpr uint32_t TEST_PARTITION_COUNT = 8;
};
} // namespace doris::pipeline