
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

#include "exec/scan_node.h"
#include "olap/tablet.h"

namespace doris {
namespace vectorized {

class VOlapScanNode;
class MetaScanNode : public ScanNode {
public:
    MetaScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override {
        return Status::NotSupported("Not Implemented VOlapScanNode Node::get_next scalar");
    }
    Status get_next(RuntimeState* state, Block* block, bool* eos) override;
    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;
    Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;

private:
    Status init_tablets();
    TupleId _tuple_id;
    TMetaScanNode _meta_scan_node;
    const TupleDescriptor* _tuple_desc;
    std::map<int, int> _slot_to_dict;
    std::vector<int> _tablet_ids;
    std::vector<TabletSharedPtr> _tablets;
    int32_t _return_column_id;
    //metaScanNode.get_next() only output one block
    bool get_next_done = false;
};

} // namespace vectorized
} // namespace doris