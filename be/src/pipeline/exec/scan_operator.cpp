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

#include "vec/exec/scan/scanner_context.h"
#include "vec/exec/scan/vscan_node.h"

namespace doris::pipeline {

OPERATOR_CODE_GENERATOR(ScanOperator, SourceOperator)

Status ScanOperator::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(SourceOperator::open(state));
    return _node->open(state);
}

bool ScanOperator::can_read() {
    if (_node->_eos || !_node->_scanner_ctx || _node->_scanner_ctx->done() ||
        _node->_scanner_ctx->can_finish()) {
        // _eos: need eos
        // !_scanner_ctx: need call open
        // _scanner_ctx->done(): need finish
        // _scanner_ctx->can_finish(): should be scheduled
        return true;
    } else {
        return !_node->_scanner_ctx->empty_in_queue(); // have block to process
    }
}

bool ScanOperator::is_pending_finish() const {
    return _node->_scanner_ctx && !_node->_scanner_ctx->can_finish();
}

Status ScanOperator::close(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::close(state));
    _node->close(state);
    return Status::OK();
}

} // namespace doris::pipeline
