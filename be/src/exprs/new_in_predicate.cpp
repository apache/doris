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

#include "exprs/new_in_predicate.h"

#include <sstream>

#include "exprs/anyval_util.h"
#include "runtime/string_value.hpp"

namespace doris {

void InPredicate::init() {}

// Templated getter functions for extracting 'SetType' values from AnyVals
template <typename T, typename SetType>
SetType get_val(const FunctionContext::TypeDesc* type, const T& x) {
    DCHECK(!x.is_null);
    return x.val;
}

template <>
StringValue get_val(const FunctionContext::TypeDesc* type, const StringVal& x) {
    DCHECK(!x.is_null);
    return StringValue::from_string_val(x);
}

template <>
DateTimeValue get_val(const FunctionContext::TypeDesc* type, const DateTimeVal& x) {
    return DateTimeValue::from_datetime_val(x);
}

template <>
DecimalV2Value get_val(const FunctionContext::TypeDesc* type, const DecimalV2Val& x) {
    return DecimalV2Value::from_decimal_val(x);
}

template <typename T, typename SetType>
void InPredicate::set_lookup_prepare(FunctionContext* ctx,
                                     FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return;
    }

    SetLookupState<SetType>* state = new SetLookupState<SetType>;
    state->type = ctx->get_arg_type(0);
    state->contains_null = false;
    for (int i = 1; i < ctx->get_num_args(); ++i) {
        DCHECK(ctx->is_arg_constant(i));
        T* arg = reinterpret_cast<T*>(ctx->get_constant_arg(i));
        if (arg->is_null) {
            state->contains_null = true;
        } else {
            state->val_set.insert(get_val<T, SetType>(state->type, *arg));
        }
    }
    ctx->set_function_state(scope, state);
}

template <typename SetType>
void InPredicate::set_lookup_close(FunctionContext* ctx,
                                   FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return;
    }
    SetLookupState<SetType>* state =
            reinterpret_cast<SetLookupState<SetType>*>(ctx->get_function_state(scope));
    delete state;
}

template <typename T, typename SetType, bool not_in, InPredicate::Strategy strategy>
BooleanVal InPredicate::templated_in(FunctionContext* ctx, const T& val, int num_args,
                                     const T* args) {
    if (val.is_null) {
        return BooleanVal::null();
    }

    BooleanVal found;
    if (strategy == SET_LOOKUP) {
        SetLookupState<SetType>* state = reinterpret_cast<SetLookupState<SetType>*>(
                ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        DCHECK(state != nullptr);
        found = set_lookup(state, val);
    } else {
        DCHECK_EQ(strategy, ITERATE);
        found = iterate(ctx->get_arg_type(0), val, num_args, args);
    }
    if (found.is_null) {
        return BooleanVal::null();
    }
    return BooleanVal(found.val ^ not_in);
}

template <typename T, typename SetType>
BooleanVal InPredicate::set_lookup(SetLookupState<SetType>* state, const T& v) {
    DCHECK(state != nullptr);
    SetType val = get_val<T, SetType>(state->type, v);
    bool found = state->val_set.find(val) != state->val_set.end();
    if (found) {
        return BooleanVal(true);
    }
    if (state->contains_null) {
        return BooleanVal::null();
    }
    return BooleanVal(false);
}

template <typename T>
BooleanVal InPredicate::iterate(const FunctionContext::TypeDesc* type, const T& val, int num_args,
                                const T* args) {
    bool found_null = false;
    for (int i = 0; i < num_args; ++i) {
        if (args[i].is_null) {
            found_null = true;
        } else if (AnyValUtil::equals(*type, val, args[i])) {
            return BooleanVal(true);
        }
    }
    if (found_null) {
        return BooleanVal::null();
    }
    return BooleanVal(false);
}

#define IN_FUNCTIONS(AnyValType, SetType, type_name)                                               \
    BooleanVal InPredicate::in_set_lookup(FunctionContext* context, const AnyValType& val,         \
                                          int num_args, const AnyValType* args) {                  \
        return templated_in<AnyValType, SetType, false, SET_LOOKUP>(context, val, num_args, args); \
    }                                                                                              \
                                                                                                   \
    BooleanVal InPredicate::not_in_set_lookup(FunctionContext* context, const AnyValType& val,     \
                                              int num_args, const AnyValType* args) {              \
        return templated_in<AnyValType, SetType, true, SET_LOOKUP>(context, val, num_args, args);  \
    }                                                                                              \
                                                                                                   \
    BooleanVal InPredicate::in_iterate(FunctionContext* context, const AnyValType& val,            \
                                       int num_args, const AnyValType* args) {                     \
        return templated_in<AnyValType, SetType, false, ITERATE>(context, val, num_args, args);    \
    }                                                                                              \
                                                                                                   \
    BooleanVal InPredicate::not_in_iterate(FunctionContext* context, const AnyValType& val,        \
                                           int num_args, const AnyValType* args) {                 \
        return templated_in<AnyValType, SetType, true, ITERATE>(context, val, num_args, args);     \
    }                                                                                              \
                                                                                                   \
    void InPredicate::set_lookup_prepare_##type_name(FunctionContext* ctx,                         \
                                                     FunctionContext::FunctionStateScope scope) {  \
        set_lookup_prepare<AnyValType, SetType>(ctx, scope);                                       \
    }                                                                                              \
                                                                                                   \
    void InPredicate::set_lookup_close_##type_name(FunctionContext* ctx,                           \
                                                   FunctionContext::FunctionStateScope scope) {    \
        set_lookup_close<SetType>(ctx, scope);                                                     \
    }

IN_FUNCTIONS(BooleanVal, bool, boolean_val)
IN_FUNCTIONS(TinyIntVal, int8_t, tiny_int_val)
IN_FUNCTIONS(SmallIntVal, int16_t, small_int_val)
IN_FUNCTIONS(IntVal, int32_t, int_val)
IN_FUNCTIONS(BigIntVal, int64_t, big_int_val)
IN_FUNCTIONS(FloatVal, float, float_val)
IN_FUNCTIONS(DoubleVal, double, double_val)
IN_FUNCTIONS(StringVal, StringValue, string_val)
IN_FUNCTIONS(DateTimeVal, DateTimeValue, datetime_val)
IN_FUNCTIONS(DecimalV2Val, DecimalV2Value, decimalv2_val)
IN_FUNCTIONS(LargeIntVal, __int128, large_int_val)

// Needed for in-predicate-benchmark to build
template BooleanVal InPredicate::iterate<IntVal>(const FunctionContext::TypeDesc*, const IntVal&,
                                                 int, const IntVal*);
} // namespace doris
