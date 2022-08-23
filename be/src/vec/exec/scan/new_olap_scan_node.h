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

class NewOlapScanner;
class NewOlapScanNode : public VScanNode {
public:
    NewOlapScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    friend class NewOlapScanner;

    Status prepare(RuntimeState* state) override;

    void set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;

protected:
    Status _init_profile() override;
    Status _process_conjuncts() override;
    bool _is_key_column(const std::string& col_name) override;

    bool _should_push_down_binary_predicate(
            VectorizedFnCall* fn_call, VExprContext* expr_ctx, StringRef* constant_val,
            int* slot_ref_child,
            const std::function<bool(const std::string&)>& fn_checker) override;

    bool _should_push_down_in_predicate(VInPredicate* in_pred, VExprContext* expr_ctx,
                                        bool is_not_in) override;

    bool _should_push_down_function_filter(VectorizedFnCall* fn_call, VExprContext* expr_ctx,
                                           StringVal* constant_str,
                                           doris_udf::FunctionContext** fn_ctx) override;

    Status _init_scanners(std::list<VScanner*>* scanners) override;

private:
    Status _build_key_ranges_and_filters();

private:
    TOlapScanNode _olap_scan_node;
    std::vector<std::unique_ptr<TPaloScanRange>> _scan_ranges;
    OlapScanKeys _scan_keys;

    std::unique_ptr<MemTracker> _scanner_mem_tracker;
};

} // namespace doris::vectorized
