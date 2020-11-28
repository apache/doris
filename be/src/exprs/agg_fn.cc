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

#include "exprs/agg_fn.h"

#include "exprs/anyval_util.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/user_function_cache.h"

using namespace doris_udf;

namespace doris {

AggFn::AggFn(const TExprNode& tnode, const SlotDescriptor& intermediate_slot_desc,
             const SlotDescriptor& output_slot_desc)
        : Expr(tnode),
          is_merge_(tnode.agg_expr.is_merge_agg),
          intermediate_slot_desc_(intermediate_slot_desc),
          output_slot_desc_(output_slot_desc),
          _vararg_start_idx(tnode.__isset.vararg_start_idx ? tnode.vararg_start_idx : -1) {
    // TODO(pengyubing) arg_type_descs_ is used for codegen
    //    arg_type_descs_(AnyValUtil::column_type_to_type_desc(
    //        TypeDescriptor::from_thrift(tnode.agg_expr.arg_types))) {
    DCHECK(tnode.__isset.fn);
    DCHECK(tnode.fn.__isset.aggregate_fn);
    // TODO chenhao
    DCHECK_EQ(tnode.node_type, TExprNodeType::AGG_EXPR);
    DCHECK_EQ(TypeDescriptor::from_thrift(tnode.type).type,
              TypeDescriptor::from_thrift(_fn.ret_type).type);
    const std::string& fn_name = _fn.name.function_name;
    if (fn_name == "count") {
        agg_op_ = COUNT;
    } else if (fn_name == "min") {
        agg_op_ = MIN;
    } else if (fn_name == "max") {
        agg_op_ = MAX;
    } else if (fn_name == "sum" || fn_name == "sum_init_zero") {
        agg_op_ = SUM;
    } else if (fn_name == "avg") {
        agg_op_ = AVG;
    } else if (fn_name == "ndv" || fn_name == "ndv_no_finalize") {
        agg_op_ = NDV;
    } else if (fn_name == "multi_distinct_count") {
        agg_op_ = COUNT_DISTINCT;
    } else if (fn_name == "multi_distinct_sum") {
        agg_op_ = SUM_DISTINCT;
    } else {
        agg_op_ = OTHER;
    }
}

Status AggFn::Init(const RowDescriptor& row_desc, RuntimeState* state) {
    // TODO chenhao , calling expr's prepare in NewAggFnEvaluator create
    // Initialize all children (i.e. input exprs to this aggregate expr).
    //for (Expr* input_expr : children()) {
    //   RETURN_IF_ERROR(input_expr->prepare(row_desc, state));
    //}

    // Initialize the aggregate expressions' internals.
    const TAggregateFunction& aggregate_fn = _fn.aggregate_fn;
    DCHECK_EQ(intermediate_slot_desc_.type().type,
              TypeDescriptor::from_thrift(aggregate_fn.intermediate_type).type);
    DCHECK_EQ(output_slot_desc_.type().type, TypeDescriptor::from_thrift(_fn.ret_type).type);

    // Load the function pointers. Must have init() and update().
    if (aggregate_fn.init_fn_symbol.empty() || aggregate_fn.update_fn_symbol.empty() ||
        (aggregate_fn.merge_fn_symbol.empty() && !aggregate_fn.is_analytic_only_fn)) {
        // This path is only for partially implemented builtins.
        DCHECK_EQ(_fn.binary_type, TFunctionBinaryType::BUILTIN);
        std::stringstream ss;
        ss << "Function " << _fn.name.function_name << " is not implemented.";
        return Status::InternalError(ss.str());
    }

    RETURN_IF_ERROR(UserFunctionCache::instance()->get_function_ptr(
            _fn.id, aggregate_fn.init_fn_symbol, _fn.hdfs_location, _fn.checksum, &init_fn_,
            &_cache_entry));
    RETURN_IF_ERROR(UserFunctionCache::instance()->get_function_ptr(
            _fn.id, aggregate_fn.update_fn_symbol, _fn.hdfs_location, _fn.checksum, &update_fn_,
            &_cache_entry));

    // Merge() is not defined for purely analytic function.
    if (!aggregate_fn.is_analytic_only_fn) {
        RETURN_IF_ERROR(UserFunctionCache::instance()->get_function_ptr(
                _fn.id, aggregate_fn.merge_fn_symbol, _fn.hdfs_location, _fn.checksum, &merge_fn_,
                &_cache_entry));
    }
    // Serialize(), GetValue(), Remove() and Finalize() are optional
    if (!aggregate_fn.serialize_fn_symbol.empty()) {
        RETURN_IF_ERROR(UserFunctionCache::instance()->get_function_ptr(
                _fn.id, aggregate_fn.serialize_fn_symbol, _fn.hdfs_location, _fn.checksum,
                &serialize_fn_, &_cache_entry));
    }
    if (!aggregate_fn.get_value_fn_symbol.empty()) {
        RETURN_IF_ERROR(UserFunctionCache::instance()->get_function_ptr(
                _fn.id, aggregate_fn.get_value_fn_symbol, _fn.hdfs_location, _fn.checksum,
                &get_value_fn_, &_cache_entry));
    }
    if (!aggregate_fn.remove_fn_symbol.empty()) {
        RETURN_IF_ERROR(UserFunctionCache::instance()->get_function_ptr(
                _fn.id, aggregate_fn.remove_fn_symbol, _fn.hdfs_location, _fn.checksum, &remove_fn_,
                &_cache_entry));
    }
    if (!aggregate_fn.finalize_fn_symbol.empty()) {
        RETURN_IF_ERROR(UserFunctionCache::instance()->get_function_ptr(
                _fn.id, _fn.aggregate_fn.finalize_fn_symbol, _fn.hdfs_location, _fn.checksum,
                &finalize_fn_, &_cache_entry));
    }
    return Status::OK();
}

Status AggFn::Create(const TExpr& texpr, const RowDescriptor& row_desc,
                     const SlotDescriptor& intermediate_slot_desc,
                     const SlotDescriptor& output_slot_desc, RuntimeState* state, AggFn** agg_fn) {
    *agg_fn = nullptr;
    ObjectPool* pool = state->obj_pool();
    const TExprNode& texpr_node = texpr.nodes[0];
    //TODO chenhao
    DCHECK_EQ(texpr_node.node_type, TExprNodeType::AGG_EXPR);
    if (!texpr_node.__isset.fn) {
        return Status::InternalError("Function not set in thrift AGGREGATE_EXPR node");
    }
    AggFn* new_agg_fn = pool->add(new AggFn(texpr_node, intermediate_slot_desc, output_slot_desc));
    RETURN_IF_ERROR(Expr::create_tree(texpr, pool, new_agg_fn));
    Status status = new_agg_fn->Init(row_desc, state);
    if (UNLIKELY(!status.ok())) {
        new_agg_fn->Close();
        return status;
    }
    for (Expr* input_expr : new_agg_fn->children()) {
        int fn_ctx_idx = 0;
        input_expr->assign_fn_ctx_idx(&fn_ctx_idx);
    }
    *agg_fn = new_agg_fn;
    return Status::OK();
}

FunctionContext::TypeDesc AggFn::GetIntermediateTypeDesc() const {
    return AnyValUtil::column_type_to_type_desc(intermediate_slot_desc_.type());
}

FunctionContext::TypeDesc AggFn::GetOutputTypeDesc() const {
    return AnyValUtil::column_type_to_type_desc(output_slot_desc_.type());
}

void AggFn::Close() {
    // This also closes all the input expressions.
    Expr::close();
}

void AggFn::Close(const std::vector<AggFn*>& exprs) {
    for (AggFn* expr : exprs) expr->Close();
}

std::string AggFn::DebugString() const {
    std::stringstream out;
    out << "AggFn(op=" << agg_op_;
    for (Expr* input_expr : children()) {
        out << " " << input_expr->debug_string() << ")";
    }
    out << ")";
    return out.str();
}

std::string AggFn::DebugString(const std::vector<AggFn*>& agg_fns) {
    std::stringstream out;
    out << "[";
    for (int i = 0; i < agg_fns.size(); ++i) {
        out << (i == 0 ? "" : " ") << agg_fns[i]->DebugString();
    }
    out << "]";
    return out.str();
}

} // namespace doris
