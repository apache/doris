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

#ifndef DORIS_BE_SRC_QUERY_EXPRS_AGG_FN_EVALUATOR_H
#define DORIS_BE_SRC_QUERY_EXPRS_AGG_FN_EVALUATOR_H

#include <string>
#include <vector>

#include "gen_cpp/Exprs_types.h"
#include "udf/udf.h"
//#include "exprs/opcode_registry.h"
#include "exprs/expr_context.h"
#include "exprs/hybrid_map.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"
#include "util/hash_util.hpp"

namespace doris {

class AggregationNode;
class TExprNode;

// This class evaluates aggregate functions. Aggregate functions can either be
// builtins or external UDAs. For both of types, they can either use codegen
// or not.
// This class provides an interface that's 1:1 with the UDA interface and serves
// as glue code between the TupleRow/Tuple signature used by the AggregationNode
// and the AnyVal signature of the UDA interface. It handles evaluating input
// slots from TupleRows and aggregating the result to the result tuple.
class AggFnEvaluator {
public:
    /// TODO: The aggregation node has custom codegen paths for a few of the builtins.
    /// That logic needs to be removed. For now, add some enums for those builtins.
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

    ~AggFnEvaluator();
    // Creates an AggFnEvaluator object from desc. The object is added to 'pool'
    // and returned in *result. This constructs the input Expr trees for
    // this aggregate function as specified in desc. The result is returned in
    // *result.
    static Status create(ObjectPool* pool, const TExpr& desc, AggFnEvaluator** result);

    static Status create(ObjectPool* pool, const TExpr& desc, bool is_analytic_fn,
                         AggFnEvaluator** result);

    // Initializes the agg expr. 'desc' must be the row descriptor for the input TupleRow.
    // It is used to get the input values in the Update() and Merge() functions.
    // 'output_slot_desc' is the slot that this aggregator should write to.
    // The underlying aggregate function allocates memory from the 'pool'. This is
    // either string data for intermediate results or whatever memory the UDA might
    // need.
    // TODO: should we give them their own pool?
    Status prepare(RuntimeState* state, const RowDescriptor& desc, MemPool* pool,
                   const SlotDescriptor* intermediate_slot_desc,
                   const SlotDescriptor* output_slot_desc,
                   const std::shared_ptr<MemTracker>& mem_tracker, FunctionContext** agg_fn_ctx);

    Status open(RuntimeState* state, FunctionContext* agg_fn_ctx);

    void close(RuntimeState* state);

    const TypeDescriptor& intermediate_type() const { return _intermediate_slot_desc->type(); }

    //PrimitiveType type() const { return _type.type; }
    AggregationOp agg_op() const { return _agg_op; }
    const std::vector<ExprContext*>& input_expr_ctxs() const { return _input_exprs_ctxs; }
    bool is_merge() const { return _is_merge; }
    bool is_count_star() const {
        return _agg_op == AggregationOp::COUNT && _input_exprs_ctxs.empty();
    }
    bool is_builtin() const { return _function_type == TFunctionBinaryType::BUILTIN; }
    bool supports_serialize() const { return _serialize_fn != nullptr; }

    static std::string debug_string(const std::vector<AggFnEvaluator*>& exprs);
    std::string debug_string() const;

    // Updates the intermediate state dst based on adding the input src row. This can be
    // called either to drive the UDA's update() or merge() function depending on
    // is_merge_. That is, from the caller, it doesn't mater.
    void add(doris_udf::FunctionContext* agg_fn_ctx, TupleRow* src, Tuple* dst);

    // Updates the intermediate state dst to remove the input src row, i.e. undoes
    // add(src, dst). Only used internally for analytic fn builtins.
    void remove(doris_udf::FunctionContext* agg_fn_ctx, TupleRow* src, Tuple* dst);
    // Puts the finalized value from Tuple* src in Tuple* dst just as finalize() does.
    // However, unlike finalize(), get_value() does not clean up state in src. get_value()
    // can be called repeatedly with the same src. Only used internally for analytic fn
    // builtins.
    void get_value(doris_udf::FunctionContext* agg_fn_ctx, Tuple* src, Tuple* dst);

    // Functions for different phases of the aggregation.
    void init(FunctionContext* agg_fn_ctx, Tuple* dst);
    void update(FunctionContext* agg_fn_ctx, TupleRow* src, Tuple* dst, void* fn, MemPool* pool);
    void merge(FunctionContext* agg_fn_ctx, TupleRow* src, Tuple* dst, MemPool* pool);
    // Explicitly does a merge, even if this evaluator is not marked as merging.
    // This is used by the partitioned agg node when it needs to merge spill results.
    // In the non-spilling case, this node would normally not merge.
    void merge(FunctionContext* agg_fn_ctx, Tuple* src, Tuple* dst);
    void serialize(FunctionContext* agg_fn_ctx, Tuple* dst);
    void finalize(FunctionContext* agg_fn_ctx, Tuple* src, Tuple* dst, bool add_null = false);

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

    inline void update_mem_limlits(int len);
    inline void update_mem_trackers(bool is_filter, bool is_add_buckets, int len);
    bool count_distinct_data_filter(TupleRow* row, Tuple* dst);
    bool sum_distinct_data_filter(TupleRow* row, Tuple* dst);
    bool is_multi_distinct() { return _is_multi_distinct; }
    bool is_in_hybridmap(void* input_val, Tuple* dst, bool* is_add_buckets);

    void choose_update_or_merge(FunctionContext* agg_fn_ctx, TupleRow* row, Tuple* dst);
    static void add(const std::vector<AggFnEvaluator*>& evaluators,
                    const std::vector<doris_udf::FunctionContext*>& fn_ctxs, TupleRow* src,
                    Tuple* dst);
    static void remove(const std::vector<AggFnEvaluator*>& evaluators,
                       const std::vector<doris_udf::FunctionContext*>& fn_ctxs, TupleRow* src,
                       Tuple* dst);
    static void get_value(const std::vector<AggFnEvaluator*>& evaluators,
                          const std::vector<doris_udf::FunctionContext*>& fn_ctxs, Tuple* src,
                          Tuple* dst);
    static void finalize(const std::vector<AggFnEvaluator*>& evaluators,
                         const std::vector<doris_udf::FunctionContext*>& fn_ctxs, Tuple* src,
                         Tuple* dst, bool add_null = false);
    static void init(const std::vector<AggFnEvaluator*>& evaluators,
                     const std::vector<doris_udf::FunctionContext*>& fn_ctxs, Tuple* dst);
    static void serialize(const std::vector<AggFnEvaluator*>& evaluators,
                          const std::vector<doris_udf::FunctionContext*>& fn_ctxs, Tuple* dst);

    const std::string& fn_name() const { return _fn.name.function_name; }

    const SlotDescriptor* output_slot_desc() const { return _output_slot_desc; }

private:
    const TFunction _fn;

    /// Indicates whether to Update() or Merge()
    const bool _is_merge;
    /// Indicates which functions must be loaded.
    const bool _is_analytic_fn;
    std::unique_ptr<HybridMap> _hybrid_map;
    bool _is_multi_distinct;
    std::vector<ExprContext*> _input_exprs_ctxs;
    std::unique_ptr<char[]> _string_buffer;   //for count distinct
    int _string_buffer_len;                   //for count distinct
    std::shared_ptr<MemTracker> _mem_tracker; // saved c'tor param

    const TypeDescriptor _return_type;
    const TypeDescriptor _intermediate_type;
    // Native (.so), IR (.ll) or builtin
    TFunctionBinaryType::type _function_type;

    // If it's a builtin, the opcode.
    AggregationOp _agg_op;

    uint64_t _total_mem_consumption;
    uint64_t _accumulated_mem_consumption;

    // Slot into which update()/merge()/serialize() write their result. Not owned.
    const SlotDescriptor* _intermediate_slot_desc;
    // Unowned
    const SlotDescriptor* _output_slot_desc;

    // Context to run the aggregate functions.
    // TODO: this and _pool make this not thread safe but they are easy to duplicate
    // per thread.
    // std::unique_ptr<doris_udf::FunctionContext> _ctx;

    // Created to a subclass of AnyVal for type(). We use this to convert values
    // from the UDA interface to the Expr interface.
    // These objects are allocated in the runtime state's object pool.
    // TODO: this is awful, remove this when exprs are updated.
    std::vector<doris_udf::AnyVal*> _staging_input_vals;
    doris_udf::AnyVal* _staging_intermediate_val;
    doris_udf::AnyVal* _staging_merge_input_val;
    // doris_udf::AnyVal* _staging_output_val;

    // Function ptrs to the aggregate function. This is either populated from the
    // opcode registry for builtins or from the external binary for native UDAs.
    // OpcodeRegistry::AggFnDescriptor _fn_ptrs;

    void* _init_fn;
    void* _update_fn;
    void* _remove_fn;
    void* _merge_fn;
    void* _serialize_fn;
    void* _get_value_fn;
    void* _finalize_fn;

    // Use create() instead.
    AggFnEvaluator(const TExprNode& desc);
    AggFnEvaluator(const TExprNode& desc, bool is_analytic_fn);

    std::string to_string(TAggregationOp::type index) {
        std::map<int, const char*>::const_iterator it =
                _TAggregationOp_VALUES_TO_NAMES.find(_agg_op);

        if (it == _TAggregationOp_VALUES_TO_NAMES.end()) {
            return "NULL";
        } else {
            return it->second;
        }
    }

    // TODO: these functions below are not extensible and we need to use codegen to
    // generate the calls into the UDA functions (like for UDFs).
    // Remove these functions when this is supported.

    // Sets up the arguments to call fn. This converts from the agg-expr signature,
    // taking TupleRow to the UDA signature taking AnvVals.
    void update_or_merge(FunctionContext* agg_fn_ctx, TupleRow* row, Tuple* dst, void* fn);

    // Sets up the arguments to call fn. This converts from the agg-expr signature,
    // taking TupleRow to the UDA signature taking AnvVals.
    // void serialize_or_finalize(FunctionContext* agg_fn_ctx, const SlotDescriptor* dst_slot_desc, Tuple* dst, void* fn);
    void serialize_or_finalize(FunctionContext* agg_fn_ctx, Tuple* src,
                               const SlotDescriptor* dst_slot_desc, Tuple* dst, void* fn,
                               bool add_null = false);

    // Writes the result in src into dst pointed to by _output_slot_desc
    void set_output_slot(const doris_udf::AnyVal* src, const SlotDescriptor* dst_slot_desc,
                         Tuple* dst);
    // Sets 'dst' to the value from 'slot'.
    void set_any_val(const void* slot, const TypeDescriptor& type, doris_udf::AnyVal* dst);
};

inline void AggFnEvaluator::add(doris_udf::FunctionContext* agg_fn_ctx, TupleRow* row, Tuple* dst) {
    agg_fn_ctx->impl()->increment_num_updates();
    update(agg_fn_ctx, row, dst, _is_merge ? _merge_fn : _update_fn, nullptr);
}
inline void AggFnEvaluator::remove(doris_udf::FunctionContext* agg_fn_ctx, TupleRow* row,
                                   Tuple* dst) {
    agg_fn_ctx->impl()->increment_num_removes();
    update(agg_fn_ctx, row, dst, _remove_fn, nullptr);
}

inline void AggFnEvaluator::finalize(doris_udf::FunctionContext* agg_fn_ctx, Tuple* src, Tuple* dst,
                                     bool add_null) {
    serialize_or_finalize(agg_fn_ctx, src, _output_slot_desc, dst, _finalize_fn, add_null);
}
inline void AggFnEvaluator::get_value(doris_udf::FunctionContext* agg_fn_ctx, Tuple* src,
                                      Tuple* dst) {
    serialize_or_finalize(agg_fn_ctx, src, _output_slot_desc, dst, _get_value_fn);
}

inline void AggFnEvaluator::init(const std::vector<AggFnEvaluator*>& evaluators,
                                 const std::vector<doris_udf::FunctionContext*>& fn_ctxs,
                                 Tuple* dst) {
    DCHECK_EQ(evaluators.size(), fn_ctxs.size());

    for (int i = 0; i < evaluators.size(); ++i) {
        evaluators[i]->init(fn_ctxs[i], dst);
    }
}
inline void AggFnEvaluator::add(const std::vector<AggFnEvaluator*>& evaluators,
                                const std::vector<doris_udf::FunctionContext*>& fn_ctxs,
                                TupleRow* src, Tuple* dst) {
    DCHECK_EQ(evaluators.size(), fn_ctxs.size());

    for (int i = 0; i < evaluators.size(); ++i) {
        evaluators[i]->add(fn_ctxs[i], src, dst);
    }
}
inline void AggFnEvaluator::remove(const std::vector<AggFnEvaluator*>& evaluators,
                                   const std::vector<doris_udf::FunctionContext*>& fn_ctxs,
                                   TupleRow* src, Tuple* dst) {
    DCHECK_EQ(evaluators.size(), fn_ctxs.size());

    for (int i = 0; i < evaluators.size(); ++i) {
        evaluators[i]->remove(fn_ctxs[i], src, dst);
    }
}
inline void AggFnEvaluator::serialize(const std::vector<AggFnEvaluator*>& evaluators,
                                      const std::vector<doris_udf::FunctionContext*>& fn_ctxs,
                                      Tuple* dst) {
    DCHECK_EQ(evaluators.size(), fn_ctxs.size());

    for (int i = 0; i < evaluators.size(); ++i) {
        evaluators[i]->serialize(fn_ctxs[i], dst);
    }
}
inline void AggFnEvaluator::get_value(const std::vector<AggFnEvaluator*>& evaluators,
                                      const std::vector<doris_udf::FunctionContext*>& fn_ctxs,
                                      Tuple* src, Tuple* dst) {
    DCHECK_EQ(evaluators.size(), fn_ctxs.size());

    for (int i = 0; i < evaluators.size(); ++i) {
        evaluators[i]->get_value(fn_ctxs[i], src, dst);
    }
}
inline void AggFnEvaluator::finalize(const std::vector<AggFnEvaluator*>& evaluators,
                                     const std::vector<doris_udf::FunctionContext*>& fn_ctxs,
                                     Tuple* src, Tuple* dst, bool add_null) {
    DCHECK_EQ(evaluators.size(), fn_ctxs.size());

    for (int i = 0; i < evaluators.size(); ++i) {
        evaluators[i]->finalize(fn_ctxs[i], src, dst, add_null);
    }
}

} // namespace doris

#endif
