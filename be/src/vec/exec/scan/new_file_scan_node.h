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

#include <gen_cpp/PaloInternalService_types.h>

#include <list>
#include <memory>
#include <vector>

#include "common/status.h"
#include "vec/exec/format/format_common.h"
#include "vec/exec/scan/vscan_node.h"

namespace doris {
class DescriptorTbl;
class ObjectPool;
class RuntimeState;
class TPlanNode;
namespace vectorized {
class VScanner;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

class NewFileScanNode : public VScanNode {
public:
    NewFileScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;

    void set_scan_ranges(RuntimeState* state,
                         const std::vector<TScanRangeParams>& scan_ranges) override;

    std::string get_name() override;

protected:
    Status _init_profile() override;
    Status _process_conjuncts() override;
    Status _init_scanners(std::list<VScannerSPtr>* scanners) override;

private:
    std::vector<TScanRangeParams> _scan_ranges;
    // A in memory cache to save some common components
    // of the this scan node. eg:
    // 1. iceberg delete file
    // 2. parquet file meta
    // KVCache<std::string> _kv_cache;
    std::unique_ptr<ShardedKVCache> _kv_cache;

    std::string _table_name;
};
} // namespace doris::vectorized
