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

#ifndef IMPALA_EXPRS_AGG_FN_EVALUATOR_H
#define IMPALA_EXPRS_AGG_FN_EVALUATOR_H

#include <string>

#include "common/compiler_util.h"
#include "common/status.h"
#include "exprs/agg_fn.h"
#include "exprs/hybrid_map.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptors.h"
#include "runtime/tuple_row.h"
#include "runtime/types.h"
#include "udf/udf.h"
#include "udf/udf_internal.h"

namespace doris {

class MemPool;
class MemTracker;
class ObjectPool;
class RowDescriptor;
class RuntimeState;
class SlotDescriptor;
class Tuple;
class TupleRow;
class TExprNode;
class ExprContext;

/// NewAggFnEvaluator is the interface for evaluating aggregate functions during execution.
///
/// NewAggFnEvaluator contains runtime state and implements wrapper functions which convert
/// the input TupleRow into AnyVal format expected by UDAF functions defined in AggFn.
/// It also evaluates TupleRow against input expressions, stores the results in staging
/// input values which are passed to Update() function to update the intermediate value
/// and handles the merging of intermediate values in the merge phases of execution.
///
/// This class is not threadsafe. An evaluator can be cloned to isolate resource
/// consumption per partition in an aggregation node.
///
class NewAggFnEvaluator {
public:
    /// Creates an NewAggFnEvaluator object from the aggregate expression 'agg_fn'.
    /// The evaluator is added to 'pool' and returned in 'eval'. This will also
    /// create a single evaluator for each input expression. All allocations will come
    /// from 'mem_pool'. Note that it's the responsibility to call Close() all evaluators
    /// even if this function returns error status on initialization failure.
    static Status Create(const AggFn& agg_fn, RuntimeState* state, ObjectPool* pool,
                         MemPool* mem_pool, NewAggFnEvaluator** eval,
                         const std::shared_ptr<MemTracker>& tracker,
                         const RowDescriptor& row_desc) WARN_UNUSED_RESULT;

    /// Convenience functions for creating evaluators for multiple aggregate functions.
    static Status Create(const std::vector<AggFn*>& agg_fns, RuntimeState* state, ObjectPool* pool,
                         MemPool* mem_pool, std::vector<NewAggFnEvaluator*>* evals,
                         const std::shared_ptr<MemTracker>& tracker,
                         const RowDescriptor& row_desc) WARN_UNUSED_RESULT;

    ~NewAggFnEvaluator();

    /// Initializes the evaluator by calling Open() on all the input expressions' evaluators
    /// and caches all constant input arguments.
    /// TODO: Move the evaluation of constant input arguments to AggFn setup.
    Status Open(RuntimeState* state) WARN_UNUSED_RESULT;

    /// Convenience functions for opening multiple NewAggFnEvaluators.
    static Status Open(const std::vector<NewAggFnEvaluator*>& evals,
                       RuntimeState* state) WARN_UNUSED_RESULT;

    /// Used by PartitionedAggregation node to initialize one evaluator per partition.
    /// Avoid the overhead of re-initializing an evaluator (e.g. calling GetConstVal()
    /// on the input expressions). Cannot be called until after Open() has been called.
    /// 'cloned_eval' is a shallow copy of this evaluator: all input values, staging
    /// intermediate values and merge values are shared with the original evaluator. Only
    /// the FunctionContext 'agg_fn_ctx' is cloned for resource isolation per partition.
    /// So, it's not safe to use cloned evaluators concurrently.
    void ShallowClone(ObjectPool* pool, MemPool* mem_pool, NewAggFnEvaluator** cloned_eval) const;

    /// Convenience function for cloning multiple evaluators. The newly cloned evaluators
    /// are appended to 'cloned_evals'.
    static void ShallowClone(ObjectPool* pool, MemPool* mem_pool,
                             const std::vector<NewAggFnEvaluator*>& evals,
                             std::vector<NewAggFnEvaluator*>* cloned_evals);

    /// Free resources owned by the evaluator.
    void Close(RuntimeState* state);
    static void Close(const std::vector<NewAggFnEvaluator*>& evals, RuntimeState* state);

    const AggFn& agg_fn() const { return agg_fn_; }

    FunctionContext* agg_fn_ctx() const;

    ExprContext* const* input_evals() const;

    /// Call the initialization function of the AggFn. May update 'dst'.
    void Init(Tuple* dst);

    /// Updates the intermediate state dst based on adding the input src row. This can be
    /// called either to drive the UDA's Update() or Merge() function, depending on whether
    /// the AggFn is a merging aggregation.
    void Add(const TupleRow* src, Tuple* dst);

    /// Updates the intermediate state dst to remove the input src row, i.e. undo
    /// Add(src, dst). Only used internally for analytic fn builtins.
    void Remove(const TupleRow* src, Tuple* dst);

    /// Explicitly does a merge, even if this evaluator is not marked as merging.
    /// This is used by the partitioned agg node when it needs to merge spill results.
    /// In the non-spilling case, this node would normally not merge.
    void Merge(Tuple* src, Tuple* dst);

    /// Flattens any intermediate values containing pointers, and frees any memory
    /// allocated during the init, update and merge phases.
    void Serialize(Tuple* dst);

    /// Does one final transformation of the aggregated value in 'agg_val' and stores the
    /// result in 'output_val'. Also frees the resources allocated during init, update and
    /// merge phases.
    void Finalize(Tuple* agg_val, Tuple* output_val, bool add_null = false);

    /// Puts the finalized value from Tuple* src in Tuple* dst just as Finalize() does.
    /// However, unlike Finalize(), GetValue() does not clean up state in src.
    /// GetValue() can be called repeatedly with the same src. Only used internally for
    /// analytic fn builtins. Note that StringVal result is from local allocation (which
    /// will be freed in the next QueryMaintenance()) so it needs to be copied out if it
    /// needs to survive beyond QueryMaintenance() (e.g. if 'dst' lives in a row batch).
    void GetValue(Tuple* src, Tuple* dst);

    // TODO: implement codegen path. These functions would return IR functions with
    // the same signature as the interpreted ones above.
    // Function* GetIrInitFn();
    // Function* GetIrUpdateFn();
    // Function* GetIrMergeFn();
    // Function* GetIrSerializeFn();
    // Function* GetIrFinalizeFn();
    static const size_t TINYINT_SIZE = sizeof(int8_t);
    static const size_t SMALLINT_SIZE = sizeof(int16_t);
    static const size_t INT_SIZE = sizeof(int32_t);
    static const size_t BIGINT_SIZE = sizeof(int64_t);
    static const size_t FLOAT_SIZE = sizeof(float);
    static const size_t DOUBLE_SIZE = sizeof(double);
    static const size_t DECIMALV2_SIZE = sizeof(DecimalV2Value);
    static const size_t LARGEINT_SIZE = sizeof(__int128);

    // DATETIME VAL has two part: packet_time is 8 byte, and type is 4 byte
    // MySQL packet time : int64_t packed_time;
    // Indicate which type of this value : int type;
    static const size_t DATETIME_SIZE = 16;

    bool is_multi_distinct() { return _is_multi_distinct; }

    const std::vector<ExprContext*>& input_expr_ctxs() const { return input_evals_; }

    /// Helper functions for calling the above functions on many evaluators.
    static void Init(const std::vector<NewAggFnEvaluator*>& evals, Tuple* dst);
    static void Add(const std::vector<NewAggFnEvaluator*>& evals, const TupleRow* src, Tuple* dst);
    static void Remove(const std::vector<NewAggFnEvaluator*>& evals, const TupleRow* src,
                       Tuple* dst);
    static void Serialize(const std::vector<NewAggFnEvaluator*>& evals, Tuple* dst);
    static void GetValue(const std::vector<NewAggFnEvaluator*>& evals, Tuple* src, Tuple* dst);
    static void Finalize(const std::vector<NewAggFnEvaluator*>& evals, Tuple* src, Tuple* dst,
                         bool add_null = false);

    /// Free local allocations made in UDA functions and input arguments' evals.
    //void FreeLocalAllocations();
    //static void FreeLocalAllocations(const std::vector<NewAggFnEvaluator*>& evals);

    std::string DebugString() const;
    static std::string DebugString(const std::vector<NewAggFnEvaluator*>& evals);

private:
    uint64_t _total_mem_consumption;
    uint64_t _accumulated_mem_consumption;

    // index if has multi count distinct
    bool _is_multi_distinct;

    /// True if the evaluator has been initialized.
    bool opened_ = false;

    /// True if the evaluator has been closed.
    bool closed_ = false;

    /// True if this evaluator is created from a ShallowClone() call.
    const bool is_clone_;

    const AggFn& agg_fn_;

    /// Pointer to the MemPool which all allocations come from.
    /// Owned by the exec node which owns this evaluator.
    MemPool* mem_pool_ = nullptr;

    std::shared_ptr<MemTracker> _mem_tracker; // saved c'tor param

    /// This contains runtime state such as constant input arguments to the aggregate
    /// functions and a FreePool from which the intermediate values are allocated.
    /// Owned by this evaluator.
    std::unique_ptr<FunctionContext> agg_fn_ctx_;

    /// Evaluators for input expressions for this aggregate function.
    /// Empty if there is no input expression (e.g. count(*)).
    std::vector<ExprContext*> input_evals_;

    /// Staging input values used by the interpreted Update() / Merge() paths.
    /// It stores the evaluation results of input expressions to be passed to the
    /// Update() / Merge() function.
    std::vector<doris_udf::AnyVal*> staging_input_vals_;

    /// Staging intermediate and merged values used in the interpreted
    /// Update() / Merge() paths.
    doris_udf::AnyVal* staging_intermediate_val_ = nullptr;
    doris_udf::AnyVal* staging_merge_input_val_ = nullptr;

    /// Use Create() instead.
    NewAggFnEvaluator(const AggFn& agg_fn, MemPool* mem_pool,
                      const std::shared_ptr<MemTracker>& tracker, bool is_clone);

    /// Return the intermediate type of the aggregate function.
    inline const SlotDescriptor& intermediate_slot_desc() const;
    inline const TypeDescriptor& intermediate_type() const;

    /// The interpreted path for the UDA's Update() function. It sets up the arguments to
    /// call 'fn' is either the 'update_fn_' or 'merge_fn_' of agg_fn_, depending on whether
    /// agg_fn_ is a merging aggregation. This converts from the agg-expr signature, taking
    /// TupleRow to the UDA signature taking AnyVals by evaluating any input expressions
    /// and populating the staging input values.
    ///
    /// Note that this function may be superseded by the codegend Update() IR function
    /// generated by AggFn::CodegenUpdateOrMergeFunction() when codegen is enabled.
    void Update(const TupleRow* row, Tuple* dst, void* fn);

    /// Writes the result in src into dst pointed to by dst_slot_desc
    inline void SetDstSlot(const doris_udf::AnyVal* src, const SlotDescriptor& dst_slot_desc,
                           Tuple* dst);

    /// Sets up the arguments to call 'fn'. This converts from the agg-expr signature,
    /// taking TupleRow to the UDA signature taking AnyVals. Writes the serialize/finalize
    /// result to the given destination slot/tuple. 'fn' can be nullptr to indicate the src
    /// value should simply be written into the destination. Note that StringVal result is
    /// from local allocation (which will be freed in the next QueryMaintenance()) so it
    /// needs to be copied out if it needs to survive beyond QueryMaintenance() (e.g. if
    /// 'dst' lives in a row batch).
    void SerializeOrFinalize(Tuple* src, const SlotDescriptor& dst_slot_desc, Tuple* dst, void* fn,
                             bool add_null = false);

    // Sets 'dst' to the value from 'slot'.
    void set_any_val(const void* slot, const TypeDescriptor& type, doris_udf::AnyVal* dst);
};

inline void NewAggFnEvaluator::Add(const TupleRow* row, Tuple* dst) {
    agg_fn_ctx_->impl()->increment_num_updates();
    Update(row, dst, agg_fn_.merge_or_update_fn());
}

inline void NewAggFnEvaluator::Remove(const TupleRow* row, Tuple* dst) {
    agg_fn_ctx_->impl()->increment_num_removes();
    Update(row, dst, agg_fn_.remove_fn());
}

inline void NewAggFnEvaluator::Serialize(Tuple* tuple) {
    SerializeOrFinalize(tuple, agg_fn_.intermediate_slot_desc(), tuple, agg_fn_.serialize_fn());
}

inline void NewAggFnEvaluator::Finalize(Tuple* agg_val, Tuple* output_val, bool add_null) {
    SerializeOrFinalize(agg_val, agg_fn_.output_slot_desc(), output_val, agg_fn_.finalize_fn(),
                        add_null);
}

inline void NewAggFnEvaluator::GetValue(Tuple* src, Tuple* dst) {
    SerializeOrFinalize(src, agg_fn_.output_slot_desc(), dst, agg_fn_.get_value_fn());
}

inline void NewAggFnEvaluator::Init(const std::vector<NewAggFnEvaluator*>& evals, Tuple* dst) {
    for (int i = 0; i < evals.size(); ++i) evals[i]->Init(dst);
}

inline void NewAggFnEvaluator::Add(const std::vector<NewAggFnEvaluator*>& evals,
                                   const TupleRow* src, Tuple* dst) {
    for (int i = 0; i < evals.size(); ++i) evals[i]->Add(src, dst);
}

inline void NewAggFnEvaluator::Remove(const std::vector<NewAggFnEvaluator*>& evals,
                                      const TupleRow* src, Tuple* dst) {
    for (int i = 0; i < evals.size(); ++i) evals[i]->Remove(src, dst);
}

inline void NewAggFnEvaluator::Serialize(const std::vector<NewAggFnEvaluator*>& evals, Tuple* dst) {
    for (int i = 0; i < evals.size(); ++i) evals[i]->Serialize(dst);
}

inline void NewAggFnEvaluator::GetValue(const std::vector<NewAggFnEvaluator*>& evals, Tuple* src,
                                        Tuple* dst) {
    for (int i = 0; i < evals.size(); ++i) evals[i]->GetValue(src, dst);
}

inline void NewAggFnEvaluator::Finalize(const std::vector<NewAggFnEvaluator*>& evals,
                                        Tuple* agg_val, Tuple* output_val, bool add_null) {
    for (int i = 0; i < evals.size(); ++i) {
        evals[i]->Finalize(agg_val, output_val, add_null);
    }
}

} // namespace doris

#endif
