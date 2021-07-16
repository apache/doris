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

#include "exec/olap_scan_node.h"

namespace doris {
class ObjectPool;
class TPlanNode;
class DescriptorTbl;
class RowBatch;
namespace vectorized {

class VOlapScanner;

class VOlapScanNode : public OlapScanNode {
public:
    VOlapScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~VOlapScanNode();
    virtual void transfer_thread(RuntimeState* state);
    virtual void scanner_thread(VOlapScanner* scanner);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
        return Status::NotSupported("Not Implemented VOlapScanNode Node::get_next scalar");
    }
    virtual Status get_next(RuntimeState* state, Block* block, bool* eos);
    virtual Status add_one_block(Block* block);
    virtual Status start_scan_thread(RuntimeState* state);
    virtual Status close(RuntimeState* state);

    friend class VOlapScanner;

private:
    std::list<Block*> _scan_blocks;
    std::list<Block*> _materialized_blocks;
    std::mutex _blocks_lock;
    std::condition_variable _block_added_cv;
    std::condition_variable _block_consumed_cv;

    std::mutex _scan_blocks_lock;
    std::condition_variable _scan_block_added_cv;

    std::list<VOlapScanner*> _volap_scanners;

    int _max_materialized_blocks;
};
} // namespace vectorized
} // namespace doris
