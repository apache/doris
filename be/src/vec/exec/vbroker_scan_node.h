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

#include "exec/broker_scan_node.h"
#include "exec/scan_node.h"
#include "runtime/descriptors.h"
namespace doris {

class RuntimeState;
class Status;

namespace vectorized {
class VBrokerScanNode final : public BrokerScanNode {
public:
    VBrokerScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~VBrokerScanNode() override = default;

    Status get_next(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    // Close the scanner, and report errors.
    Status close(RuntimeState* state) override;

private:
    Status start_scanners() override;

    void scanner_worker(int start_idx, int length);
    // Scan one range
    Status scanner_scan(const TBrokerScanRange& scan_range, ScannerCounter* counter);

    std::deque<std::shared_ptr<vectorized::Block>> _block_queue;
};
} // namespace vectorized
} // namespace doris