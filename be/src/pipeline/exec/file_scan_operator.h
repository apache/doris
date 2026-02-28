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
#include "common/compile_check_begin.h"
namespace vectorized {
class FileScanner;
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
    Status _init_scanners(std::list<vectorized::ScannerSPtr>* scanners) override;
    void set_scan_ranges(RuntimeState* state,
                         const std::vector<TScanRangeParams>& scan_ranges) override;
    int parent_id() { return _parent->node_id(); }
    std::string name_suffix() const override;
    int max_scanners_concurrency(RuntimeState* state) const override;
    int min_scanners_concurrency(RuntimeState* state) const override;
    vectorized::ScannerScheduler* scan_scheduler(RuntimeState* state) const override;

private:
    friend class vectorized::FileScanner;
    PushDownType _should_push_down_bloom_filter() const override {
        return PushDownType::UNACCEPTABLE;
    }
    PushDownType _should_push_down_topn_filter() const override {
        return PushDownType::PARTIAL_ACCEPTABLE;
    }
    bool _push_down_topn(const vectorized::RuntimePredicate& predicate) override {
        // For external table/ file scan, first try push down the predicate,
        // and then determine whether it can be pushed down within the (parquet/orc) reader.
        return true;
    }

    PushDownType _should_push_down_bitmap_filter() const override {
        return PushDownType::UNACCEPTABLE;
    }
    PushDownType _should_push_down_is_null_predicate(
            vectorized::VectorizedFnCall* fn_call) const override {
        return fn_call->fn().name.function_name == "is_null_pred" ||
                               fn_call->fn().name.function_name == "is_not_null_pred"
                       ? PushDownType::PARTIAL_ACCEPTABLE
                       : PushDownType::UNACCEPTABLE;
    }
    PushDownType _should_push_down_in_predicate() const override {
        return PushDownType::PARTIAL_ACCEPTABLE;
    }
    PushDownType _should_push_down_binary_predicate(
            vectorized::VectorizedFnCall* fn_call, vectorized::VExprContext* expr_ctx,
            vectorized::Field& constant_val, const std::set<std::string> fn_name) const override;
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

    Status prepare(RuntimeState* state) override;

    // There's only one scan range for each backend in batch split mode. Each backend only starts up one ScanNode instance.
    int parallelism(RuntimeState* state) const override {
        return _batch_split_mode ? 1 : ScanOperatorX<FileScanLocalState>::parallelism(state);
    }

    int get_column_id(const std::string& col_name) const override {
        int column_id_counter = 0;
        for (const auto& slot : _output_tuple_desc->slots()) {
            if (slot->col_name() == col_name) {
                return column_id_counter;
            }
            column_id_counter++;
        }
        return column_id_counter;
    }

private:
    friend class FileScanLocalState;

    const std::string _table_name;
    bool _batch_split_mode = false;
};

#include "common/compile_check_end.h"
} // namespace doris::pipeline
