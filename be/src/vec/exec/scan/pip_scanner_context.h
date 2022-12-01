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

#include "scanner_context.h"

namespace doris {

namespace pipeline {

class PipScannerContext : public vectorized::ScannerContext {
public:
    PipScannerContext(RuntimeState* state, vectorized::VScanNode* parent,
                      const TupleDescriptor* input_tuple_desc,
                      const TupleDescriptor* output_tuple_desc,
                      const std::list<vectorized::VScanner*>& scanners, int64_t limit,
                      int64_t max_bytes_in_blocks_queue)
            : vectorized::ScannerContext(state, parent, input_tuple_desc, output_tuple_desc,
                                         scanners, limit, max_bytes_in_blocks_queue) {}

    void _update_block_queue_empty() override { _blocks_queue_empty = _blocks_queue.empty(); }

    // We should make those method lock free.
    bool done() override { return _is_finished || _should_stop || _status_error; }
    bool can_finish() override { return _num_running_scanners == 0 && _num_scheduling_ctx == 0; }
    bool empty_in_queue() override { return _blocks_queue_empty; }

private:
    std::atomic_bool _blocks_queue_empty = true;
};
} // namespace pipeline
} // namespace doris
