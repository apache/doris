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
#include <string>
#include <vector>

#include "common/object_pool.h"
#include "core/block/block.h"
#include "exec/pipeline/pipeline_task.h"
#include "exec/spill/spill_file_manager.h"
#include "runtime/runtime_profile.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {

class MockPartitioner : public PartitionerBase {
public:
    MockPartitioner(size_t partition_count) : PartitionerBase(partition_count) {}
    Status init(const std::vector<TExpr>& texprs) override { return Status::OK(); }

    Status prepare(RuntimeState* state, const RowDescriptor& row_desc) override {
        return Status::OK();
    }

    Status open(RuntimeState* state) override { return Status::OK(); }

    Status close(RuntimeState* state) override { return Status::OK(); }

    Status do_partitioning(RuntimeState* state, Block* block) const override {
        return Status::OK();
    }

    Status clone(RuntimeState* state, std::unique_ptr<PartitionerBase>& partitioner) override {
        partitioner = std::make_unique<MockPartitioner>(_partition_count);
        return Status::OK();
    }

    const std::vector<HashValType>& get_channel_ids() const override { return _mocked_hash_vals; }

private:
    std::vector<HashValType> _mocked_hash_vals;
};

class MockExpr : public VExpr {
public:
    Status prepare(RuntimeState* state, const RowDescriptor& row_desc,
                   VExprContext* context) override {
        return Status::OK();
    }

    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) override {
        return Status::OK();
    }
};

class SpillableDebugPointHelper {
public:
    SpillableDebugPointHelper(const std::string name)
            : _enable_debug_points(config::enable_debug_points), _debug_point_name(name) {
        config::enable_debug_points = true;
        DebugPoints::instance()->add(name);
    }

    ~SpillableDebugPointHelper() {
        DebugPoints::instance()->remove(_debug_point_name);
        config::enable_debug_points = _enable_debug_points;
    }

private:
    const bool _enable_debug_points;
    const std::string _debug_point_name;
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
    std::unique_ptr<RuntimeProfile> operator_profile;
    std::unique_ptr<RuntimeProfile> custom_profile;
    std::unique_ptr<RuntimeProfile> common_profile;

    std::shared_ptr<PipelineTask> pipeline_task;
    DescriptorTbl* desc_tbl;
    static constexpr uint32_t TEST_PARTITION_COUNT = 8;
};
} // namespace doris