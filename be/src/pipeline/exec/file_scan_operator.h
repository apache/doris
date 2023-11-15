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

#include <stdint.h>

#include <string>

#include "common/logging.h"
#include "common/status.h"
#include "operator.h"
#include "pipeline/exec/scan_operator.h"
#include "pipeline/pipeline_x/operator.h"
#include "vec/exec/format/format_common.h"
#include "vec/exec/scan/vscan_node.h"

namespace doris {
class ExecNode;
namespace vectorized {
class VFileScanner;
} // namespace vectorized
} // namespace doris

namespace doris::pipeline {

class FileScanOperatorX;
class FileScanLocalState final : public ScanLocalState<FileScanLocalState> {
public:
    using Parent = FileScanOperatorX;
    using Base = ScanLocalState<FileScanLocalState>;
    ENABLE_FACTORY_CREATOR(FileScanLocalState);
    FileScanLocalState(RuntimeState* state, OperatorXBase* parent)
            : ScanLocalState<FileScanLocalState>(state, parent) {}

    Status init(RuntimeState* state, LocalStateInfo& info) override;

    Status _process_conjuncts() override;
    Status _init_scanners(std::list<vectorized::VScannerSPtr>* scanners) override;
    void set_scan_ranges(RuntimeState* state,
                         const std::vector<TScanRangeParams>& scan_ranges) override;
    int parent_id() { return _parent->node_id(); }

private:
    std::vector<TScanRangeParams> _scan_ranges;
    // A in memory cache to save some common components
    // of the this scan node. eg:
    // 1. iceberg delete file
    // 2. parquet file meta
    // KVCache<std::string> _kv_cache;
    std::unique_ptr<vectorized::ShardedKVCache> _kv_cache;
    TupleId _output_tuple_id = -1;
};

class FileScanOperatorX final : public ScanOperatorX<FileScanLocalState> {
public:
    FileScanOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                      const DescriptorTbl& descs)
            : ScanOperatorX<FileScanLocalState>(pool, tnode, operator_id, descs) {
        _output_tuple_id = tnode.file_scan_node.tuple_id;
    }

    Status prepare(RuntimeState* state) override;

private:
    friend class FileScanLocalState;
};

} // namespace doris::pipeline
