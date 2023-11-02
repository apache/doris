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

#include "runtime/group_commit_mgr.h"
#include "vec/exec/format/format_common.h"
#include "vec/exec/scan/vscan_node.h"

namespace doris::vectorized {

class GroupCommitScanNode : public VScanNode {
public:
    GroupCommitScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;

    void set_scan_ranges(RuntimeState* state,
                         const std::vector<TScanRangeParams>& scan_ranges) override;

    std::string get_name() override;

    Status get_next(RuntimeState* state, vectorized::Block* block, bool* eos) override;

protected:
    Status _init_profile() override;
    Status _process_conjuncts() override;

private:
    int64_t _table_id;
    std::shared_ptr<LoadBlockQueue> load_block_queue;
};
} // namespace doris::vectorized
