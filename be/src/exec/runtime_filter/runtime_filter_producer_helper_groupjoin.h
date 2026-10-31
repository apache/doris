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

#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "exec/runtime_filter/runtime_filter_producer_helper.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "runtime/runtime_state.h"

namespace doris {

// This helper is used by GroupJoin build sink. GroupJoin does not keep a full build block, so
// this helper caches only runtime-filter source columns and replays them after RF size is ready.
class RuntimeFilterProducerHelperGroupJoin final : public RuntimeFilterProducerHelper {
public:
    ~RuntimeFilterProducerHelperGroupJoin() override = default;

    RuntimeFilterProducerHelperGroupJoin() : RuntimeFilterProducerHelper(true, false) {}

    Status append_block(Block* block) {
        if (_skip_runtime_filters_process) {
            return Status::OK();
        }
        if (block->rows() == 0) {
            return Status::OK();
        }

        std::vector<ColumnPtr> filter_columns;
        filter_columns.reserve(_filter_expr_contexts.size());
        for (auto& ctx : _filter_expr_contexts) {
            ColumnPtr column;
            RETURN_IF_ERROR(ctx->execute(block, column));
            column = column->convert_to_full_column_if_const();
            filter_columns.emplace_back(std::move(column));
        }

        _build_rows += block->rows();
        _cached_filter_columns.emplace_back(std::move(filter_columns));
        return Status::OK();
    }

    uint64_t build_rows() const { return _build_rows; }

    Status build_and_publish(RuntimeState* state) {
        if (_skip_runtime_filters_process) {
            return Status::OK();
        }

        RETURN_IF_ERROR(_init_filters(state, _build_rows));
        for (const auto& filter_columns : _cached_filter_columns) {
            RETURN_IF_ERROR(_insert_columns(filter_columns));
        }

        for (const auto& filter : _producers) {
            filter->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
        }
        RETURN_IF_ERROR(_publish(state));
        return Status::OK();
    }

private:
    Status _insert_columns(const std::vector<ColumnPtr>& filter_columns) {
        DCHECK_EQ(filter_columns.size(), _producers.size());
        SCOPED_TIMER(_runtime_filter_compute_timer.get());
        for (size_t i = 0; i < _producers.size(); ++i) {
            RETURN_IF_ERROR(_producers[i]->insert(filter_columns[i], 0));
        }
        return Status::OK();
    }

    std::vector<std::vector<ColumnPtr>> _cached_filter_columns;
    uint64_t _build_rows = 0;
};

} // namespace doris
