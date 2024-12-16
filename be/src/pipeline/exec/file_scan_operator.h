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
#include "vec/exec/format/format_common.h"
#include "vec/exec/scan/split_source_connector.h"

namespace doris {
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

    Status _process_conjuncts(RuntimeState* state) override;
    Status _init_scanners(std::list<vectorized::VScannerSPtr>* scanners) override;
    void set_scan_ranges(RuntimeState* state,
                         const std::vector<TScanRangeParams>& scan_ranges) override;
    int parent_id() { return _parent->node_id(); }
    std::string name_suffix() const override;

private:
    std::shared_ptr<vectorized::SplitSourceConnector> _split_source = nullptr;
    int _max_scanners;
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
                      const DescriptorTbl& descs, int parallel_tasks)
            : ScanOperatorX<FileScanLocalState>(pool, tnode, operator_id, descs, parallel_tasks),
              _table_name(tnode.file_scan_node.__isset.table_name ? tnode.file_scan_node.table_name
                                                                  : "") {
        _output_tuple_id = tnode.file_scan_node.tuple_id;
    }

    Status open(RuntimeState* state) override;

    bool is_file_scan_operator() const override { return true; }

private:
    friend class FileScanLocalState;

    const std::string _table_name;
};

} // namespace doris::pipeline
