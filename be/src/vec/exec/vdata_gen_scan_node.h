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
#include <vector>

#include "common/global_types.h"
#include "exec/scan_node.h"

namespace doris {

class TupleDescriptor;
class RuntimeState;
class Status;
class DescriptorTbl;
class ObjectPool;
class TPlanNode;
class TScanRangeParams;

namespace vectorized {
class Block;
class VDataGenFunctionInf;

class VDataGenFunctionScanNode : public ScanNode {
public:
    VDataGenFunctionScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~VDataGenFunctionScanNode() override = default;

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status get_next(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    Status close(RuntimeState* state) override;

    // No use
    Status set_scan_ranges(RuntimeState* state,
                           const std::vector<TScanRangeParams>& scan_ranges) override;

protected:
    std::shared_ptr<VDataGenFunctionInf> _table_func;
    bool _is_init;
    // Tuple id resolved in prepare() to set _tuple_desc;
    TupleId _tuple_id;

    // Descriptor of tuples generated
    const TupleDescriptor* _tuple_desc;

    std::vector<TRuntimeFilterDesc> _runtime_filter_descs;
};

} // namespace vectorized

} // namespace doris
