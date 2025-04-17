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

#include "common/status.h"
#include "pipeline/pipeline_task.h"
#include "runtime/runtime_state.h"
#include "runtime_filter/runtime_filter.h"
#include "runtime_filter/runtime_filter_mgr.h"
#include "runtime_filter/runtime_filter_producer_helper.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
#include "common/compile_check_begin.h"
// this class used in set sink node
class RuntimeFilterProducerHelperSet : public RuntimeFilterProducerHelper {
public:
    ~RuntimeFilterProducerHelperSet() override = default;

    RuntimeFilterProducerHelperSet() : RuntimeFilterProducerHelper(true, false) {}

    Status process(RuntimeState* state, const vectorized::Block* block, uint64_t cardinality) {
        if (_skip_runtime_filters_process) {
            return Status::OK();
        }

        RETURN_IF_ERROR(_init_filters(state, cardinality));
        if (cardinality != 0) {
            RETURN_IF_ERROR(_insert(block, 0));
        }

        for (const auto& filter : _producers) {
            filter->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
        }

        RETURN_IF_ERROR(_publish(state));
        return Status::OK();
    }
};
#include "common/compile_check_end.h"
} // namespace doris
