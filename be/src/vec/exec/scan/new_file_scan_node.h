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

#include "vec/exec/scan/vscan_node.h"

namespace doris::vectorized {

class NewFileScanNode : public VScanNode {
public:
    NewFileScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;

    void set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;

protected:
    Status _init_profile() override;
    Status _process_conjuncts() override;
    Status _init_scanners(std::list<VScanner*>* scanners) override;

private:
    VScanner* _create_scanner(const TFileScanRange& scan_range);

private:
    std::vector<TScanRangeParams> _scan_ranges;
};
} // namespace doris::vectorized
