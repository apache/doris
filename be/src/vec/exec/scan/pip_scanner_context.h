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

#include "pipeline/exec/scan_operator.h"
#include "runtime/descriptors.h"
#include "scanner_context.h"

namespace doris::pipeline {

class PipScannerContext final : public vectorized::ScannerContext {
    ENABLE_FACTORY_CREATOR(PipScannerContext);

public:
    PipScannerContext(RuntimeState* state, vectorized::VScanNode* parent,
                      const TupleDescriptor* output_tuple_desc,
                      const RowDescriptor* output_row_descriptor,
                      const std::list<std::shared_ptr<vectorized::ScannerDelegate>>& scanners,
                      int64_t limit_, int64_t max_bytes_in_blocks_queue,
                      const int num_parallel_instances)
            : vectorized::ScannerContext(state, parent, output_tuple_desc, output_row_descriptor,
                                         scanners, limit_, max_bytes_in_blocks_queue,
                                         num_parallel_instances) {}
};

class PipXScannerContext final : public vectorized::ScannerContext {
    ENABLE_FACTORY_CREATOR(PipXScannerContext);

public:
    PipXScannerContext(RuntimeState* state, ScanLocalStateBase* local_state,
                       const TupleDescriptor* output_tuple_desc,
                       const RowDescriptor* output_row_descriptor,
                       const std::list<std::shared_ptr<vectorized::ScannerDelegate>>& scanners,
                       int64_t limit_, int64_t max_bytes_in_blocks_queue,
                       std::shared_ptr<pipeline::Dependency> dependency,
                       const int num_parallel_instances)
            : vectorized::ScannerContext(state, output_tuple_desc, output_row_descriptor, scanners,
                                         limit_, max_bytes_in_blocks_queue, num_parallel_instances,
                                         local_state) {
        _dependency = dependency;
    }

    void append_block_to_queue(std::shared_ptr<vectorized::ScanTask> scan_task) override {
        vectorized::ScannerContext::append_block_to_queue(scan_task);
        if (_dependency) {
            _dependency->set_ready();
        }
    }

    Status get_block_from_queue(RuntimeState* state, vectorized::Block* block, bool* eos, int id,
                                bool wait = true) override {
        Status st = vectorized::ScannerContext::get_block_from_queue(state, block, eos, id, wait);
        std::lock_guard<std::mutex> l(_transfer_lock);
        if (_blocks_queue.empty()) {
            if (_dependency) {
                _dependency->block();
            }
        }
        return st;
    }

protected:
    void _set_scanner_done() override {
        if (_dependency) {
            _dependency->set_always_ready();
        }
    }

private:
    std::shared_ptr<pipeline::Dependency> _dependency = nullptr;
};

} // namespace doris::pipeline
