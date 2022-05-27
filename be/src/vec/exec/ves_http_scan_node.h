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

#include "exec/es_http_scan_node.h"
#include "exec/scan_node.h"
#include "runtime/descriptors.h"
#include "vec/exec/ves_http_scanner.h"
namespace doris {

class RuntimeState;
class Status;

namespace vectorized {

class VEsHttpScanNode : public EsHttpScanNode {
public:
    VEsHttpScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~VEsHttpScanNode();

    using EsHttpScanNode::get_next;
    virtual Status get_next(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    virtual Status close(RuntimeState* state) override;

private:
    virtual Status scanner_scan(std::unique_ptr<VEsHttpScanner> scanner) override;

    std::deque<std::shared_ptr<vectorized::Block>> _block_queue;
    std::mutex _block_queue_lock;
};
} // namespace vectorized
} // namespace doris