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

#ifndef DORIS_BE_SRC_QUERY_NEW_EXPRS_AGG_FN_H
#define DORIS_BE_SRC_QUERY_NEW_EXPRS_AGG_FN_H

#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "udf/udf.h"

namespace doris {

using doris_udf::FunctionContext;

class MemPool;
class MemTracker;
class ObjectPool;
class RuntimeState;
class Tuple;
class TupleRow;
class TExprNode;

/// --- AggFn overview
///
/// An aggregate function generates an output over a set of tuple rows.
/// An example would be AVG() which computes the average of all input rows.
/// The built-in aggregate functions such as min, max, sum, avg, ndv etc are
/// in this category.
///
/// --- Implementation
///
/// AggFn contains the aggregation operations, pointers to the UDAF interface functions
/// implementing various states of aggregation and the descriptors for the intermediate
/// and output values. Please see udf/udf.h for details of the UDAF interfaces.
///
/// AggFnEvaluator is the interface for evaluating aggregate functions against input
/// tuple rows. It invokes the following functions at different phases of the aggregation:
///
/// init_fn_     : An initialization function that initializes the aggregate value.
///
/// update_fn_   : An update function that processes the arguments for each row in the
///                query result set and accumulates an intermediate result. For example,
///                this function might increment a counter, append to a string buffer or
///                add the input to a cumulative sum.
///
/// merge_fn_    : A merge function that combines multiple intermediate results into a
///                single value.
///
/// serialize_fn_: A serialization function that flattens any intermediate values
///                containing pointers, and frees any memory allocated during the init,
///                update and merge phases.
///
/// finalize_fn_ : A finalize function that either passes through the combined result
///                unchanged, or does one final transformation. Also frees the resources
///                allocated during init, update and merge phases.
///
/// get_value_fn_: Used by AnalyticEval node to obtain the current intermediate value.
///
/// remove_fn_   : Used by AnalyticEval node to undo the update to the intermediate value
///                by an input row as it falls out of a sliding window.
///
class AggFn : public Expr {
public:
    /// Override the base class' implementation.
    virtual bool IsAggFn() const { return true; }

    /// Enum for some built-in aggregation ops.
    enum AggregationOp {
        COUNT,
        MIN,
        MAX,
        SUM,
        AVG,
        NDV,
        SUM_DISTINCT,
        COUNT_DISTINCT,
        HLL_UNION_AGG,
        OTHER,
    };

    /// Creates and initializes an aggregate function from 'texpr' and returns it in
    /// 'agg_fn'. The returned AggFn lives in the ObjectPool of 'state'. 'row_desc' is
    /// the row descriptor of the input tuple row; 'intermediate_slot_desc' is the slot
    /// descriptor of the intermediate value; 'output_slot_desc' is the slot descriptor
    /// of the output value. On failure, returns error status and sets 'agg_fn' to nullptr.
    static Status Create(const TExpr& texpr, const RowDescriptor& row_desc,
                         const SlotDescriptor& intermediate_slot_desc,
                         const SlotDescriptor& output_slot_desc, RuntimeState* state,
                         AggFn** agg_fn) WARN_UNUSED_RESULT;

    bool is_merge() const { return is_merge_; }
    AggregationOp agg_op() const { return agg_op_; }
    bool is_count_star() const { return agg_op_ == COUNT && _children.empty(); }
    bool is_count_distinct() const { return agg_op_ == COUNT_DISTINCT; }
    bool is_sum_distinct() const { return agg_op_ == SUM_DISTINCT; }
    bool is_builtin() const { return _fn.binary_type == TFunctionBinaryType::BUILTIN; }
    const std::string& fn_name() const { return _fn.name.function_name; }
    const TypeDescriptor& intermediate_type() const { return intermediate_slot_desc_.type(); }
    const SlotDescriptor& intermediate_slot_desc() const { return intermediate_slot_desc_; }
    // Output type is the same as Expr::type().
    const SlotDescriptor& output_slot_desc() const { return output_slot_desc_; }
    void* remove_fn() const { return remove_fn_; }
    void* merge_or_update_fn() const { return is_merge_ ? merge_fn_ : update_fn_; }
    void* serialize_fn() const { return serialize_fn_; }
    void* get_value_fn() const { return get_value_fn_; }
    void* finalize_fn() const { return finalize_fn_; }
    bool SupportsRemove() const { return remove_fn_ != nullptr; }
    bool SupportsSerialize() const { return serialize_fn_ != nullptr; }
    FunctionContext::TypeDesc GetIntermediateTypeDesc() const;
    FunctionContext::TypeDesc GetOutputTypeDesc() const;
    const std::vector<FunctionContext::TypeDesc>& arg_type_descs() const { return arg_type_descs_; }

    /// Releases all cache entries to libCache for all nodes in the expr tree.
    virtual void Close();
    static void Close(const std::vector<AggFn*>& exprs);

    Expr* clone(ObjectPool* pool) const { return nullptr; }

    virtual std::string DebugString() const;
    static std::string DebugString(const std::vector<AggFn*>& exprs);

    const int get_vararg_start_idx() const { return _vararg_start_idx; }

private:
    friend class Expr;
    friend class NewAggFnEvaluator;

    /// True if this is a merging aggregation.
    const bool is_merge_;

    /// Slot into which Update()/Merge()/Serialize() write their result. Not owned.
    const SlotDescriptor& intermediate_slot_desc_;

    /// Slot into which Finalize() results are written. Not owned. Identical to
    /// intermediate_slot_desc_ if this agg fn has the same intermediate and result type.
    const SlotDescriptor& output_slot_desc_;

    /// The types of the arguments to the aggregate function.
    const std::vector<FunctionContext::TypeDesc> arg_type_descs_;

    /// The aggregation operation.
    AggregationOp agg_op_;

    /// Function pointers for the different phases of the aggregate function.
    void* init_fn_ = nullptr;
    void* update_fn_ = nullptr;
    void* remove_fn_ = nullptr;
    void* merge_fn_ = nullptr;
    void* serialize_fn_ = nullptr;
    void* get_value_fn_ = nullptr;
    void* finalize_fn_ = nullptr;

    int _vararg_start_idx;

    AggFn(const TExprNode& node, const SlotDescriptor& intermediate_slot_desc,
          const SlotDescriptor& output_slot_desc);

    /// Initializes the AggFn and its input expressions. May load the UDAF from LibCache
    /// if necessary.
    virtual Status Init(const RowDescriptor& desc, RuntimeState* state) WARN_UNUSED_RESULT;
};

} // namespace doris

#endif
