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
#include "mock_query_context.h"
#include "runtime/runtime_state.h"

namespace doris {

class MockContext : public TaskExecutionContext {};

class MockRuntimeState : public RuntimeState {
public:
    MockRuntimeState() {
        set_task_execution_context(_mock_context);
        _query_ctx = _query_ctx_uptr.get();
    }

    int batch_size() const override { return batsh_size; }

    bool enable_shared_exchange_sink_buffer() const override {
        return _enable_shared_exchange_sink_buffer;
    }

    bool enable_local_exchange() const override { return true; }

    // default batch size
    int batsh_size = 4096;
    bool _enable_shared_exchange_sink_buffer = true;
    std::shared_ptr<MockContext> _mock_context = std::make_shared<MockContext>();
    std::shared_ptr<MockQueryContext> _query_ctx_uptr = std::make_shared<MockQueryContext>();
};

} // namespace doris
