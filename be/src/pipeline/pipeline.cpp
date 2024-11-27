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

#include "pipeline.h"

#include <memory>
#include <string>
#include <utility>

#include "pipeline/exec/operator.h"
#include "pipeline/pipeline_fragment_context.h"
#include "pipeline/pipeline_task.h"

namespace doris::pipeline {

void Pipeline::_init_profile() {
    auto s = fmt::format("Pipeline (pipeline id={})", _pipeline_id);
    _pipeline_profile = std::make_unique<RuntimeProfile>(std::move(s));
}

bool Pipeline::need_to_local_exchange(const DataDistribution target_data_distribution,
                                      const int idx) const {
    // If serial operator exists after `idx`-th operator, we should not improve parallelism.
    if (std::any_of(_operators.begin() + idx, _operators.end(),
                    [&](OperatorPtr op) -> bool { return op->is_serial_operator(); })) {
        return false;
    }
    // If all operators are serial and sink is not serial, we should improve parallelism for sink.
    if (std::all_of(_operators.begin(), _operators.end(),
                    [&](OperatorPtr op) -> bool { return op->is_serial_operator(); })) {
        if (!_sink->is_serial_operator()) {
            return true;
        }
    } else if (std::any_of(_operators.begin(), _operators.end(),
                           [&](OperatorPtr op) -> bool { return op->is_serial_operator(); })) {
        // If non-serial operators exist, we should improve parallelism for those.
        return true;
    }

    if (target_data_distribution.distribution_type != ExchangeType::BUCKET_HASH_SHUFFLE &&
        target_data_distribution.distribution_type != ExchangeType::HASH_SHUFFLE) {
        // Always do local exchange if non-hash-partition exchanger is required.
        // For example, `PASSTHROUGH` exchanger is always required to distribute data evenly.
        return true;
    } else if (_operators.front()->is_serial_operator()) {
        DCHECK(std::all_of(_operators.begin(), _operators.end(),
                           [&](OperatorPtr op) -> bool { return op->is_serial_operator(); }) &&
               _sink->is_serial_operator())
                << debug_string();
        // All operators and sink are serial in this path.
        return false;
    } else {
        return _data_distribution.distribution_type != target_data_distribution.distribution_type &&
               !(is_hash_exchange(_data_distribution.distribution_type) &&
                 is_hash_exchange(target_data_distribution.distribution_type));
    }
}

Status Pipeline::add_operator(OperatorPtr& op, const int parallelism) {
    if (parallelism > 0 && op->is_serial_operator()) {
        set_num_tasks(parallelism);
    }
    op->set_parallel_tasks(num_tasks());
    _operators.emplace_back(op);
    if (op->is_source()) {
        std::reverse(_operators.begin(), _operators.end());
    }
    return Status::OK();
}

Status Pipeline::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(_operators.back()->open(state));
    RETURN_IF_ERROR(_sink->open(state));
    _name.append(std::to_string(id()));
    _name.push_back('-');
    for (auto& op : _operators) {
        _name.append(std::to_string(op->node_id()));
        _name.append(op->get_name());
    }
    _name.push_back('-');
    _name.append(std::to_string(_sink->node_id()));
    _name.append(_sink->get_name());
    return Status::OK();
}

Status Pipeline::set_sink(DataSinkOperatorPtr& sink) {
    if (_sink) {
        return Status::InternalError("set sink twice");
    }
    if (!sink->is_sink()) {
        return Status::InternalError("should set a sink operator but {}", typeid(sink).name());
    }
    _sink = sink;
    return Status::OK();
}

void Pipeline::make_all_runnable() {
    if (_sink->count_down_destination()) {
        for (auto* task : _tasks) {
            if (task) {
                task->clear_blocking_state(true);
            }
        }
    }
}

} // namespace doris::pipeline
