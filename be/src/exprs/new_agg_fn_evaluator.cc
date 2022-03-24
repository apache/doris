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

#include "exprs/new_agg_fn_evaluator.h"

#include <thrift/protocol/TDebugProtocol.h>

#include <sstream>

#include "common/logging.h"
#include "exprs/agg_fn.h"
#include "exprs/aggregate_functions.h"
#include "exprs/anyval_util.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/scalar_fn_call.h"
#include "gutil/strings/substitute.h"
#include "runtime/mem_tracker.h"
#include "runtime/raw_value.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "udf/udf_internal.h"
#include "util/debug_util.h"

using namespace doris;
using namespace doris_udf;
using std::move;

// typedef for builtin aggregate functions. Unfortunately, these type defs don't
// really work since the actual builtin is implemented not in terms of the base
// AnyVal* type. Due to this, there are lots of casts when we use these typedefs.
// TODO: these typedefs exists as wrappers to go from (TupleRow, Tuple) to the
// types the aggregation functions need. This needs to be done with codegen instead.
typedef void (*InitFn)(FunctionContext*, AnyVal*);
typedef void (*UpdateFn0)(FunctionContext*, AnyVal*);
typedef void (*UpdateFn1)(FunctionContext*, const AnyVal&, AnyVal*);
typedef void (*UpdateFn2)(FunctionContext*, const AnyVal&, const AnyVal&, AnyVal*);
typedef void (*UpdateFn3)(FunctionContext*, const AnyVal&, const AnyVal&, const AnyVal&, AnyVal*);
typedef void (*UpdateFn4)(FunctionContext*, const AnyVal&, const AnyVal&, const AnyVal&,
                          const AnyVal&, AnyVal*);
typedef void (*UpdateFn5)(FunctionContext*, const AnyVal&, const AnyVal&, const AnyVal&,
                          const AnyVal&, const AnyVal&, AnyVal*);
typedef void (*UpdateFn6)(FunctionContext*, const AnyVal&, const AnyVal&, const AnyVal&,
                          const AnyVal&, const AnyVal&, const AnyVal&, AnyVal*);
typedef void (*UpdateFn7)(FunctionContext*, const AnyVal&, const AnyVal&, const AnyVal&,
                          const AnyVal&, const AnyVal&, const AnyVal&, const AnyVal&, AnyVal*);
typedef void (*UpdateFn8)(FunctionContext*, const AnyVal&, const AnyVal&, const AnyVal&,
                          const AnyVal&, const AnyVal&, const AnyVal&, const AnyVal&, const AnyVal&,
                          AnyVal*);

typedef void (*VarargUpdateFn0)(FunctionContext*, int num_varargs, const AnyVal*, AnyVal*);
typedef void (*VarargUpdateFn1)(FunctionContext*, const AnyVal&, int num_varargs, const AnyVal*,
                                AnyVal*);
typedef void (*VarargUpdateFn2)(FunctionContext*, const AnyVal&, const AnyVal&, int num_varargs,
                                const AnyVal*, AnyVal*);
typedef void (*VarargUpdateFn3)(FunctionContext*, const AnyVal&, const AnyVal&, const AnyVal&,
                                int num_varargs, const AnyVal*, AnyVal*);
typedef void (*VarargUpdateFn4)(FunctionContext*, const AnyVal&, const AnyVal&, const AnyVal&,
                                const AnyVal&, int num_varargs, const AnyVal*, AnyVal*);
typedef void (*VarargUpdateFn5)(FunctionContext*, const AnyVal&, const AnyVal&, const AnyVal&,
                                const AnyVal&, const AnyVal&, int num_varargs, const AnyVal*,
                                AnyVal*);
typedef void (*VarargUpdateFn6)(FunctionContext*, const AnyVal&, const AnyVal&, const AnyVal&,
                                const AnyVal&, const AnyVal&, const AnyVal&, int num_varargs,
                                const AnyVal*, AnyVal*);
typedef void (*VarargUpdateFn7)(FunctionContext*, const AnyVal&, const AnyVal&, const AnyVal&,
                                const AnyVal&, const AnyVal&, const AnyVal&, const AnyVal&,
                                int num_varargs, const AnyVal*, AnyVal*);
typedef void (*VarargUpdateFn8)(FunctionContext*, const AnyVal&, const AnyVal&, const AnyVal&,
                                const AnyVal&, const AnyVal&, const AnyVal&, const AnyVal&,
                                const AnyVal&, int num_varargs, const AnyVal*, AnyVal*);

typedef StringVal (*SerializeFn)(FunctionContext*, const StringVal&);
typedef AnyVal (*GetValueFn)(FunctionContext*, const AnyVal&);
typedef AnyVal (*FinalizeFn)(FunctionContext*, const AnyVal&);

NewAggFnEvaluator::NewAggFnEvaluator(const AggFn& agg_fn, MemPool* mem_pool, bool is_clone)
        : _accumulated_mem_consumption(0),
          is_clone_(is_clone),
          agg_fn_(agg_fn),
          mem_pool_(mem_pool) {}

NewAggFnEvaluator::~NewAggFnEvaluator() {
    DCHECK(closed_);
}

const SlotDescriptor& NewAggFnEvaluator::intermediate_slot_desc() const {
    return agg_fn_.intermediate_slot_desc();
}

const TypeDescriptor& NewAggFnEvaluator::intermediate_type() const {
    return agg_fn_.intermediate_type();
}

Status NewAggFnEvaluator::Create(const AggFn& agg_fn, RuntimeState* state, ObjectPool* pool,
                                 MemPool* mem_pool, NewAggFnEvaluator** result,
                                 const std::shared_ptr<MemTracker>& tracker,
                                 const RowDescriptor& row_desc) {
    *result = nullptr;

    // Create a new AggFn evaluator.
    NewAggFnEvaluator* agg_fn_eval =
            pool->add(new NewAggFnEvaluator(agg_fn, mem_pool, false));

    agg_fn_eval->agg_fn_ctx_.reset(FunctionContextImpl::create_context(
            state, mem_pool, agg_fn.GetIntermediateTypeDesc(), agg_fn.GetOutputTypeDesc(),
            agg_fn.arg_type_descs(), 0, false));

    Status status;
    // Create the evaluators for the input expressions.
    for (Expr* input_expr : agg_fn.children()) {
        // TODO chenhao replace ExprContext with ScalarFnEvaluator
        ExprContext* input_eval = pool->add(new ExprContext(input_expr));
        if (input_eval == nullptr) goto cleanup;
        input_eval->prepare(state, row_desc, tracker);
        agg_fn_eval->input_evals_.push_back(input_eval);
        Expr* root = input_eval->root();
        DCHECK(root == input_expr);
        AnyVal* staging_input_val;
        status = allocate_any_val(state, mem_pool, input_expr->type(),
                                  "Could not allocate aggregate expression input value",
                                  &staging_input_val);
        agg_fn_eval->staging_input_vals_.push_back(staging_input_val);
        if (UNLIKELY(!status.ok())) goto cleanup;
    }
    DCHECK_EQ(agg_fn.get_num_children(), agg_fn_eval->input_evals_.size());
    DCHECK_EQ(agg_fn_eval->staging_input_vals_.size(), agg_fn_eval->input_evals_.size());

    status = allocate_any_val(state, mem_pool, agg_fn.intermediate_type(),
                              "Could not allocate aggregate expression intermediate value",
                              &(agg_fn_eval->staging_intermediate_val_));
    if (UNLIKELY(!status.ok())) goto cleanup;
    status = allocate_any_val(state, mem_pool, agg_fn.intermediate_type(),
                              "Could not allocate aggregate expression merge input value",
                              &(agg_fn_eval->staging_merge_input_val_));
    if (UNLIKELY(!status.ok())) goto cleanup;

    if (agg_fn.is_merge()) {
        DCHECK_EQ(agg_fn_eval->staging_input_vals_.size(), 1) << "Merge should only have 1 input.";
    }

    *result = agg_fn_eval;
    return Status::OK();

cleanup:
    DCHECK(!status.ok());
    agg_fn_eval->Close(state);
    return status;
}

Status NewAggFnEvaluator::Create(const vector<AggFn*>& agg_fns, RuntimeState* state,
                                 ObjectPool* pool, MemPool* mem_pool,
                                 vector<NewAggFnEvaluator*>* evals,
                                 const std::shared_ptr<MemTracker>& tracker,
                                 const RowDescriptor& row_desc) {
    for (const AggFn* agg_fn : agg_fns) {
        NewAggFnEvaluator* agg_fn_eval;
        RETURN_IF_ERROR(NewAggFnEvaluator::Create(*agg_fn, state, pool, mem_pool, &agg_fn_eval,
                                                  tracker, row_desc));
        evals->push_back(agg_fn_eval);
    }
    return Status::OK();
}

Status NewAggFnEvaluator::Open(RuntimeState* state) {
    if (opened_) return Status::OK();
    opened_ = true;
    // TODO chenhao, ScalarFnEvaluator different from ExprContext
    RETURN_IF_ERROR(ExprContext::open(input_evals_, state));
    // Now that we have opened all our input exprs, it is safe to evaluate any constant
    // values for the UDA's FunctionContext (we cannot evaluate exprs before calling Open()
    // on them).
    vector<AnyVal*> constant_args(input_evals_.size(), nullptr);
    for (int i = 0; i < input_evals_.size(); ++i) {
        ExprContext* eval = input_evals_[i];
        RETURN_IF_ERROR(eval->get_const_value(state, *(agg_fn_.get_child(i)), &constant_args[i]));
    }
    agg_fn_ctx_->impl()->set_constant_args(move(constant_args));
    return Status::OK();
}

Status NewAggFnEvaluator::Open(const vector<NewAggFnEvaluator*>& evals, RuntimeState* state) {
    for (NewAggFnEvaluator* eval : evals) RETURN_IF_ERROR(eval->Open(state));
    return Status::OK();
}

void NewAggFnEvaluator::Close(RuntimeState* state) {
    if (closed_) return;
    closed_ = true;
    if (!is_clone_) Expr::close(input_evals_, state);
    // TODO chenhao
    //FreeLocalAllocations();
    agg_fn_ctx_->impl()->close();
    agg_fn_ctx_.reset();

    //TODO chenhao release ExprContext
    //for (int i = 0; i < input_evals_.size(); i++) {
    //    ExprContext* context = input_evals_[i];
    //    delete context;
    //}
    input_evals_.clear();
}

void NewAggFnEvaluator::Close(const vector<NewAggFnEvaluator*>& evals, RuntimeState* state) {
    for (NewAggFnEvaluator* eval : evals) eval->Close(state);
}

void NewAggFnEvaluator::SetDstSlot(const AnyVal* src, const SlotDescriptor& dst_slot_desc,
                                   Tuple* dst) {
    if (src->is_null && dst_slot_desc.is_nullable()) {
        dst->set_null(dst_slot_desc.null_indicator_offset());
        return;
    }

    dst->set_not_null(dst_slot_desc.null_indicator_offset());
    void* slot = dst->get_slot(dst_slot_desc.tuple_offset());
    switch (dst_slot_desc.type().type) {
    case TYPE_NULL:
        return;
    case TYPE_BOOLEAN:
        *reinterpret_cast<bool*>(slot) = reinterpret_cast<const BooleanVal*>(src)->val;
        return;
    case TYPE_TINYINT:
        *reinterpret_cast<int8_t*>(slot) = reinterpret_cast<const TinyIntVal*>(src)->val;
        return;
    case TYPE_SMALLINT:
        *reinterpret_cast<int16_t*>(slot) = reinterpret_cast<const SmallIntVal*>(src)->val;
        return;
    case TYPE_INT:
        *reinterpret_cast<int32_t*>(slot) = reinterpret_cast<const IntVal*>(src)->val;
        return;
    case TYPE_BIGINT:
        *reinterpret_cast<int64_t*>(slot) = reinterpret_cast<const BigIntVal*>(src)->val;
        return;
    case TYPE_LARGEINT:
        memcpy(slot, &reinterpret_cast<const LargeIntVal*>(src)->val, sizeof(__int128));
        return;
    case TYPE_FLOAT:
        *reinterpret_cast<float*>(slot) = reinterpret_cast<const FloatVal*>(src)->val;
        return;
    case TYPE_DOUBLE:
        *reinterpret_cast<double*>(slot) = reinterpret_cast<const DoubleVal*>(src)->val;
        return;
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_OBJECT:
    case TYPE_QUANTILE_STATE:
    case TYPE_STRING:
        *reinterpret_cast<StringValue*>(slot) =
                StringValue::from_string_val(*reinterpret_cast<const StringVal*>(src));
        return;
    case TYPE_DATE:
    case TYPE_DATETIME:
        *reinterpret_cast<DateTimeValue*>(slot) =
                DateTimeValue::from_datetime_val(*reinterpret_cast<const DateTimeVal*>(src));
        return;

    case TYPE_DECIMALV2:
        *reinterpret_cast<PackedInt128*>(slot) = reinterpret_cast<const DecimalV2Val*>(src)->val;
        return;
    default:
        DCHECK(false) << "NYI: " << dst_slot_desc.type();
    }
}

// This function would be replaced in codegen.
void NewAggFnEvaluator::Init(Tuple* dst) {
    DCHECK(opened_);
    DCHECK(agg_fn_.init_fn_ != nullptr);
    for (ExprContext* input_eval : input_evals_) {
        DCHECK(input_eval->opened());
    }

    const TypeDescriptor& type = intermediate_type();
    const SlotDescriptor& slot_desc = intermediate_slot_desc();
    if (type.type == TYPE_CHAR) {
        // The intermediate value is represented as a fixed-length buffer inline in the tuple.
        // The aggregate function writes to this buffer directly. staging_intermediate_val_
        // is a StringVal with a pointer to the slot and the length of the slot.
        void* slot = dst->get_slot(slot_desc.tuple_offset());
        StringVal* sv = reinterpret_cast<StringVal*>(staging_intermediate_val_);
        sv->is_null = dst->is_null(slot_desc.null_indicator_offset());
        sv->ptr = reinterpret_cast<uint8_t*>(slot);
        sv->len = type.len;
    }
    reinterpret_cast<InitFn>(agg_fn_.init_fn_)(agg_fn_ctx_.get(), staging_intermediate_val_);
    SetDstSlot(staging_intermediate_val_, slot_desc, dst);
    agg_fn_ctx_->impl()->set_num_updates(0);
    agg_fn_ctx_->impl()->set_num_removes(0);
}

static void SetAnyVal(const SlotDescriptor& desc, Tuple* tuple, AnyVal* dst) {
    bool is_null = tuple->is_null(desc.null_indicator_offset());
    void* slot = nullptr;
    if (!is_null) slot = tuple->get_slot(desc.tuple_offset());
    AnyValUtil::set_any_val(slot, desc.type(), dst);
}

// Utility to put val into an AnyVal struct
inline void NewAggFnEvaluator::set_any_val(const void* slot, const TypeDescriptor& type,
                                           AnyVal* dst) {
    if (slot == nullptr) {
        dst->is_null = true;
        return;
    }

    dst->is_null = false;

    switch (type.type) {
    case TYPE_NULL:
        return;

    case TYPE_BOOLEAN:
        reinterpret_cast<BooleanVal*>(dst)->val = *reinterpret_cast<const bool*>(slot);
        return;

    case TYPE_TINYINT:
        reinterpret_cast<TinyIntVal*>(dst)->val = *reinterpret_cast<const int8_t*>(slot);
        return;

    case TYPE_SMALLINT:
        reinterpret_cast<SmallIntVal*>(dst)->val = *reinterpret_cast<const int16_t*>(slot);
        return;

    case TYPE_INT:
        reinterpret_cast<IntVal*>(dst)->val = *reinterpret_cast<const int32_t*>(slot);
        return;

    case TYPE_BIGINT:
        reinterpret_cast<BigIntVal*>(dst)->val = *reinterpret_cast<const int64_t*>(slot);
        return;

    case TYPE_FLOAT:
        reinterpret_cast<FloatVal*>(dst)->val = *reinterpret_cast<const float*>(slot);
        return;

    case TYPE_DOUBLE:
        reinterpret_cast<DoubleVal*>(dst)->val = *reinterpret_cast<const double*>(slot);
        return;

    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_OBJECT:
    case TYPE_QUANTILE_STATE:
    case TYPE_STRING:
        reinterpret_cast<const StringValue*>(slot)->to_string_val(
                reinterpret_cast<StringVal*>(dst));
        return;

    case TYPE_DATE:
    case TYPE_DATETIME:
        reinterpret_cast<const DateTimeValue*>(slot)->to_datetime_val(
                reinterpret_cast<DateTimeVal*>(dst));
        return;

    case TYPE_DECIMALV2:
        reinterpret_cast<DecimalV2Val*>(dst)->val =
                reinterpret_cast<const PackedInt128*>(slot)->value;
        return;

    case TYPE_LARGEINT:
        memcpy(&reinterpret_cast<LargeIntVal*>(dst)->val, slot, sizeof(__int128));
        return;

    default:
        DCHECK(false) << "NYI";
    }
}

void NewAggFnEvaluator::Update(const TupleRow* row, Tuple* dst, void* fn) {
    if (fn == nullptr) return;

    const SlotDescriptor& slot_desc = intermediate_slot_desc();
    SetAnyVal(slot_desc, dst, staging_intermediate_val_);
    for (int i = 0; i < input_evals_.size(); ++i) {
        void* src_slot = input_evals_[i]->get_value(const_cast<TupleRow*>(row));
        DCHECK(input_evals_[i]->root() == agg_fn_.get_child(i));
        AnyValUtil::set_any_val(src_slot, agg_fn_.get_child(i)->type(), staging_input_vals_[i]);
    }
    if (agg_fn_.is_merge()) {
        reinterpret_cast<UpdateFn1>(fn)(agg_fn_ctx_.get(), *staging_input_vals_[0],
                                        staging_intermediate_val_);
        SetDstSlot(staging_intermediate_val_, slot_desc, dst);
        return;
    }

    // TODO: this part is not so good and not scalable. It can be replaced with
    // codegen but we can also consider leaving it for the first few cases for
    // debugging.
    if (agg_fn_.get_vararg_start_idx() == -1) {
        switch (input_evals_.size()) {
        case 0:
            reinterpret_cast<UpdateFn0>(fn)(agg_fn_ctx_.get(), staging_intermediate_val_);
            break;
        case 1:
            reinterpret_cast<UpdateFn1>(fn)(agg_fn_ctx_.get(), *staging_input_vals_[0],
                                            staging_intermediate_val_);
            break;
        case 2:
            reinterpret_cast<UpdateFn2>(fn)(agg_fn_ctx_.get(), *staging_input_vals_[0],
                                            *staging_input_vals_[1], staging_intermediate_val_);
            break;
        case 3:
            reinterpret_cast<UpdateFn3>(fn)(agg_fn_ctx_.get(), *staging_input_vals_[0],
                                            *staging_input_vals_[1], *staging_input_vals_[2],
                                            staging_intermediate_val_);
            break;
        case 4:
            reinterpret_cast<UpdateFn4>(fn)(agg_fn_ctx_.get(), *staging_input_vals_[0],
                                            *staging_input_vals_[1], *staging_input_vals_[2],
                                            *staging_input_vals_[3], staging_intermediate_val_);
            break;
        case 5:
            reinterpret_cast<UpdateFn5>(fn)(agg_fn_ctx_.get(), *staging_input_vals_[0],
                                            *staging_input_vals_[1], *staging_input_vals_[2],
                                            *staging_input_vals_[3], *staging_input_vals_[4],
                                            staging_intermediate_val_);
            break;
        case 6:
            reinterpret_cast<UpdateFn6>(fn)(agg_fn_ctx_.get(), *staging_input_vals_[0],
                                            *staging_input_vals_[1], *staging_input_vals_[2],
                                            *staging_input_vals_[3], *staging_input_vals_[4],
                                            *staging_input_vals_[5], staging_intermediate_val_);
            break;
        case 7:
            reinterpret_cast<UpdateFn7>(fn)(
                    agg_fn_ctx_.get(), *staging_input_vals_[0], *staging_input_vals_[1],
                    *staging_input_vals_[2], *staging_input_vals_[3], *staging_input_vals_[4],
                    *staging_input_vals_[5], *staging_input_vals_[6], staging_intermediate_val_);
            break;
        case 8:
            reinterpret_cast<UpdateFn8>(fn)(agg_fn_ctx_.get(), *staging_input_vals_[0],
                                            *staging_input_vals_[1], *staging_input_vals_[2],
                                            *staging_input_vals_[3], *staging_input_vals_[4],
                                            *staging_input_vals_[5], *staging_input_vals_[6],
                                            *staging_input_vals_[7], staging_intermediate_val_);
            break;
        default:
            DCHECK(false) << "NYI";
        }
    } else {
        int num_varargs = input_evals_.size() - agg_fn_.get_vararg_start_idx();
        const AnyVal* varargs = *(staging_input_vals_.data() + agg_fn_.get_vararg_start_idx());
        switch (agg_fn_.get_vararg_start_idx()) {
        case 0:
            reinterpret_cast<VarargUpdateFn0>(fn)(agg_fn_ctx_.get(), num_varargs, varargs,
                                                  staging_intermediate_val_);
            break;
        case 1:
            reinterpret_cast<VarargUpdateFn1>(fn)(agg_fn_ctx_.get(), *staging_input_vals_[0],
                                                  num_varargs, varargs, staging_intermediate_val_);
            break;
        case 2:
            reinterpret_cast<VarargUpdateFn2>(fn)(agg_fn_ctx_.get(), *staging_input_vals_[0],
                                                  *staging_input_vals_[1], num_varargs, varargs,
                                                  staging_intermediate_val_);
            break;
        case 3:
            reinterpret_cast<VarargUpdateFn3>(fn)(agg_fn_ctx_.get(), *staging_input_vals_[0],
                                                  *staging_input_vals_[1], *staging_input_vals_[2],
                                                  num_varargs, varargs, staging_intermediate_val_);
            break;
        case 4:
            reinterpret_cast<VarargUpdateFn4>(fn)(agg_fn_ctx_.get(), *staging_input_vals_[0],
                                                  *staging_input_vals_[1], *staging_input_vals_[2],
                                                  *staging_input_vals_[3], num_varargs, varargs,
                                                  staging_intermediate_val_);
            break;
        case 5:
            reinterpret_cast<VarargUpdateFn5>(fn)(agg_fn_ctx_.get(), *staging_input_vals_[0],
                                                  *staging_input_vals_[1], *staging_input_vals_[2],
                                                  *staging_input_vals_[3], *staging_input_vals_[4],
                                                  num_varargs, varargs, staging_intermediate_val_);
            break;
        case 6:
            reinterpret_cast<VarargUpdateFn6>(fn)(
                    agg_fn_ctx_.get(), *staging_input_vals_[0], *staging_input_vals_[1],
                    *staging_input_vals_[2], *staging_input_vals_[3], *staging_input_vals_[4],
                    *staging_input_vals_[5], num_varargs, varargs, staging_intermediate_val_);
            break;
        case 7:
            reinterpret_cast<VarargUpdateFn7>(fn)(agg_fn_ctx_.get(), *staging_input_vals_[0],
                                                  *staging_input_vals_[1], *staging_input_vals_[2],
                                                  *staging_input_vals_[3], *staging_input_vals_[4],
                                                  *staging_input_vals_[5], *staging_input_vals_[6],
                                                  num_varargs, varargs, staging_intermediate_val_);
            break;
        case 8:
            reinterpret_cast<VarargUpdateFn8>(fn)(
                    agg_fn_ctx_.get(), *staging_input_vals_[0], *staging_input_vals_[1],
                    *staging_input_vals_[2], *staging_input_vals_[3], *staging_input_vals_[4],
                    *staging_input_vals_[5], *staging_input_vals_[6], *staging_input_vals_[7],
                    num_varargs, varargs, staging_intermediate_val_);
            break;
        default:
            DCHECK(false) << "NYI";
        }
    }
    SetDstSlot(staging_intermediate_val_, slot_desc, dst);
}

void NewAggFnEvaluator::Merge(Tuple* src, Tuple* dst) {
    DCHECK(agg_fn_.merge_fn_ != nullptr);
    const SlotDescriptor& slot_desc = intermediate_slot_desc();
    SetAnyVal(slot_desc, dst, staging_intermediate_val_);
    SetAnyVal(slot_desc, src, staging_merge_input_val_);
    // The merge fn always takes one input argument.
    reinterpret_cast<UpdateFn1>(agg_fn_.merge_fn_)(agg_fn_ctx_.get(), *staging_merge_input_val_,
                                                   staging_intermediate_val_);
    SetDstSlot(staging_intermediate_val_, slot_desc, dst);
}

void NewAggFnEvaluator::SerializeOrFinalize(Tuple* src, const SlotDescriptor& dst_slot_desc,
                                            Tuple* dst, void* fn, bool add_null) {
    // No fn was given and the src and dst are identical. Nothing to be done.
    if (fn == nullptr && src == dst) return;
    // src != dst means we are performing a Finalize(), so even if fn == null we
    // still must copy the value of the src slot into dst.

    const SlotDescriptor& slot_desc = intermediate_slot_desc();
    bool src_slot_null = add_null || src->is_null(slot_desc.null_indicator_offset());
    void* src_slot = nullptr;
    if (!src_slot_null) src_slot = src->get_slot(slot_desc.tuple_offset());

    // No fn was given but the src and dst tuples are different (doing a Finalize()).
    // Just copy the src slot into the dst tuple.
    if (fn == nullptr) {
        DCHECK_EQ(intermediate_type(), dst_slot_desc.type());
        RawValue::write(src_slot, dst, &dst_slot_desc, nullptr);
        return;
    }

    AnyValUtil::set_any_val(src_slot, intermediate_type(), staging_intermediate_val_);
    switch (dst_slot_desc.type().type) {
    case TYPE_BOOLEAN: {
        typedef BooleanVal (*Fn)(FunctionContext*, AnyVal*);
        BooleanVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx_.get(), staging_intermediate_val_);
        SetDstSlot(&v, dst_slot_desc, dst);
        break;
    }
    case TYPE_TINYINT: {
        typedef TinyIntVal (*Fn)(FunctionContext*, AnyVal*);
        TinyIntVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx_.get(), staging_intermediate_val_);
        SetDstSlot(&v, dst_slot_desc, dst);
        break;
    }
    case TYPE_SMALLINT: {
        typedef SmallIntVal (*Fn)(FunctionContext*, AnyVal*);
        SmallIntVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx_.get(), staging_intermediate_val_);
        SetDstSlot(&v, dst_slot_desc, dst);
        break;
    }
    case TYPE_INT: {
        typedef IntVal (*Fn)(FunctionContext*, AnyVal*);
        IntVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx_.get(), staging_intermediate_val_);
        SetDstSlot(&v, dst_slot_desc, dst);
        break;
    }
    case TYPE_BIGINT: {
        typedef BigIntVal (*Fn)(FunctionContext*, AnyVal*);
        BigIntVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx_.get(), staging_intermediate_val_);
        SetDstSlot(&v, dst_slot_desc, dst);
        break;
    }
    case TYPE_LARGEINT: {
        typedef LargeIntVal (*Fn)(FunctionContext*, AnyVal*);
        LargeIntVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx_.get(), staging_intermediate_val_);
        SetDstSlot(&v, dst_slot_desc, dst);
        break;
    }
    case TYPE_FLOAT: {
        typedef FloatVal (*Fn)(FunctionContext*, AnyVal*);
        FloatVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx_.get(), staging_intermediate_val_);
        SetDstSlot(&v, dst_slot_desc, dst);
        break;
    }
    case TYPE_DOUBLE: {
        typedef DoubleVal (*Fn)(FunctionContext*, AnyVal*);
        DoubleVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx_.get(), staging_intermediate_val_);
        SetDstSlot(&v, dst_slot_desc, dst);
        break;
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_OBJECT:
    case TYPE_QUANTILE_STATE:
    case TYPE_STRING: {
        typedef StringVal (*Fn)(FunctionContext*, AnyVal*);
        StringVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx_.get(), staging_intermediate_val_);
        SetDstSlot(&v, dst_slot_desc, dst);
        break;
    }
    case TYPE_DECIMALV2: {
        typedef DecimalV2Val (*Fn)(FunctionContext*, AnyVal*);
        DecimalV2Val v = reinterpret_cast<Fn>(fn)(agg_fn_ctx_.get(), staging_intermediate_val_);
        SetDstSlot(&v, dst_slot_desc, dst);
        break;
    }
    case TYPE_DATE:
    case TYPE_DATETIME: {
        typedef DateTimeVal (*Fn)(FunctionContext*, AnyVal*);
        DateTimeVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx_.get(), staging_intermediate_val_);
        SetDstSlot(&v, dst_slot_desc, dst);
        break;
    }
    default:
        DCHECK(false) << "NYI";
    }
}

void NewAggFnEvaluator::ShallowClone(ObjectPool* pool, MemPool* mem_pool,
                                     NewAggFnEvaluator** cloned_eval) const {
    DCHECK(opened_);
    *cloned_eval = pool->add(new NewAggFnEvaluator(agg_fn_, mem_pool, true));
    (*cloned_eval)->agg_fn_ctx_.reset(agg_fn_ctx_->impl()->clone(mem_pool));
    DCHECK_EQ((*cloned_eval)->input_evals_.size(), 0);
    (*cloned_eval)->input_evals_ = input_evals_;
    (*cloned_eval)->staging_input_vals_ = staging_input_vals_;
    (*cloned_eval)->staging_intermediate_val_ = staging_intermediate_val_;
    (*cloned_eval)->staging_merge_input_val_ = staging_merge_input_val_;
    (*cloned_eval)->opened_ = true;
}

void NewAggFnEvaluator::ShallowClone(ObjectPool* pool, MemPool* mem_pool,
                                     const vector<NewAggFnEvaluator*>& evals,
                                     vector<NewAggFnEvaluator*>* cloned_evals) {
    for (const NewAggFnEvaluator* eval : evals) {
        NewAggFnEvaluator* cloned_eval;
        eval->ShallowClone(pool, mem_pool, &cloned_eval);
        cloned_evals->push_back(cloned_eval);
    }
}

//
//void NewAggFnEvaluator::FreeLocalAllocations() {
//  ExprContext::FreeLocalAllocations(input_evals_);
//  agg_fn_ctx_->impl()->FreeLocalAllocations();
//}

//void NewAggFnEvaluator::FreeLocalAllocations(const vector<NewAggFnEvaluator*>& evals) {
//  for (NewAggFnEvaluator* eval : evals) eval->FreeLocalAllocations();
//}
