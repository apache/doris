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

#include "scan_operator.h"

#include <fmt/format.h>

#include <memory>

#include "pipeline/exec/operator.h"
#include "vec/exec/scan/scanner_context.h"
#include "vec/exec/scan/vscan_node.h"

namespace doris::pipeline {

OPERATOR_CODE_GENERATOR(ScanOperator, SourceOperator)

bool ScanOperator::can_read() {
    if (!_node->_opened) {
        if (_node->_should_create_scanner || _node->ready_to_open()) {
            return true;
        } else {
            return false;
        }
    } else {
        if (_node->_eos || _node->_scanner_ctx->done()) {
            // _eos: need eos
            // _scanner_ctx->done(): need finish
            // _scanner_ctx->no_schedule(): should schedule _scanner_ctx
            return true;
        } else {
            if (_node->_scanner_ctx->get_num_running_scanners() == 0 &&
                _node->_scanner_ctx->should_be_scheduled()) {
                _node->_scanner_ctx->reschedule_scanner_ctx();
            }
            return _node->ready_to_read(); // there are some blocks to process
        }
    }
}

bool ScanOperator::is_pending_finish() const {
    return _node->_scanner_ctx && !_node->_scanner_ctx->no_schedule();
}

Status ScanOperator::try_close(RuntimeState* state) {
    return _node->try_close(state);
}

bool ScanOperator::runtime_filters_are_ready_or_timeout() {
    return _node->runtime_filters_are_ready_or_timeout();
}

std::string ScanOperator::debug_string() const {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}, scanner_ctx is null: {} ",
                   SourceOperator::debug_string(), _node->_scanner_ctx == nullptr);
    if (_node->_scanner_ctx) {
        fmt::format_to(debug_string_buffer, ", num_running_scanners = {}, num_scheduling_ctx = {} ",
                       _node->_scanner_ctx->get_num_running_scanners(),
                       _node->_scanner_ctx->get_num_scheduling_ctx());
    }
    return fmt::to_string(debug_string_buffer);
}

} // namespace doris::pipeline
