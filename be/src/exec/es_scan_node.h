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

#include "exec/scan_node.h"
#include "exprs/slot_ref.h"
#include "gen_cpp/PaloExternalDataSourceService_types.h"
#include "gen_cpp/TExtDataSourceService.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/tuple.h"

namespace doris {

class TupleDescriptor;
class RuntimeState;
class Status;

class EsScanNode : public ScanNode {
public:
    EsScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~EsScanNode();

    virtual Status prepare(RuntimeState* state) override;
    virtual Status open(RuntimeState* state) override;
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
    virtual Status close(RuntimeState* state) override;
    virtual Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;

protected:
    // Write debug string of this into out.
    virtual void debug_string(int indentation_level, std::stringstream* out) const override;

private:
    Status open_es(TNetworkAddress& address, TExtOpenResult& result, TExtOpenParams& params);
    Status materialize_row(MemPool* tuple_pool, Tuple* tuple, const vector<TExtColumnData>& cols,
                           int next_row_idx, vector<int>& cols_next_val_idx);
    Status get_next_from_es(TExtGetNextResult& result);

    bool get_disjuncts(ExprContext* context, Expr* conjunct, vector<TExtPredicate>& disjuncts);
    bool to_ext_literal(ExprContext* context, Expr* expr, TExtLiteral* literal);
    bool to_ext_literal(PrimitiveType node_type, void* value, TExtLiteral* literal);
    bool ignore_cast(SlotDescriptor* slot, Expr* expr);

    bool is_match_func(Expr* conjunct);

    SlotDescriptor* get_slot_desc(SlotRef* slotRef);

    // check if open result meets condition
    // 1. check if left conjuncts contain "match" function, since match function could only be executed on es
    bool check_left_conjuncts(Expr* conjunct);

private:
    TupleId _tuple_id;
    std::map<std::string, std::string> _properties;
    const TupleDescriptor* _tuple_desc;
    ExecEnv* _env;
    std::vector<TEsScanRange> _scan_ranges;

    // scan range's iterator, used in get_next()
    int _scan_range_idx;

    // store every scan range's netaddress/handle/offset
    std::vector<TNetworkAddress> _addresses;
    std::vector<std::string> _scan_handles;
    std::vector<int> _offsets;
    std::vector<ExprContext*> _pushdown_conjunct_ctxs;
};

} // namespace doris
