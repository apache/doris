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

#pragma once
#include <memory>

#include "pipeline/exec/hashjoin_probe_operator.h"
#include "pipeline/exec/partitioned_hash_join_probe_operator.h"
#include "testutil/mock/mock_descriptors.h"

namespace doris::pipeline {

class MockChildOperator : public OperatorXBase {
public:
    void set_block(vectorized::Block&& block) { _block = std::move(block); }
    void set_eos() { _eos = true; }
    Status get_block_after_projects(RuntimeState* state, vectorized::Block* block,
                                    bool* eos) override {
        block->swap(_block);
        *eos = _eos;
        return Status::OK();
    }

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override {
        block->swap(_block);
        *eos = _eos;
        return Status::OK();
    }
    Status setup_local_state(RuntimeState* state, LocalStateInfo& info) override {
        return Status::OK();
    }

private:
    vectorized::Block _block;
    bool _eos = false;
};

class MockSourceOperator : public MockChildOperator {
public:
    bool is_source() const override { return true; }

    Status close(RuntimeState* state) override { return Status::OK(); }
};

class MockSinkOperator final : public DataSinkOperatorXBase {
public:
    Status sink(RuntimeState* state, vectorized::Block* block, bool eos) override {
        return Status::OK();
    }

    Status setup_local_state(RuntimeState* state, LocalSinkStateInfo& info) override {
        return Status::OK();
    }

    std::shared_ptr<BasicSharedState> create_shared_state() const override { return nullptr; }
};
} // namespace doris::pipeline