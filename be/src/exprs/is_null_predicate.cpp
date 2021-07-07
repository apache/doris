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

#include "exprs/is_null_predicate.h"

#include "udf/udf.h"

namespace doris {

void IsNullPredicate::init() {}

template <typename T>
BooleanVal IsNullPredicate::is_null(FunctionContext* ctx, const T& val) {
    return val.is_null;
}

template <typename T>
BooleanVal IsNullPredicate::is_not_null(FunctionContext* ctx, const T& val) {
    return !val.is_null;
}

template BooleanVal IsNullPredicate::is_null(FunctionContext*, const AnyVal&);
template BooleanVal IsNullPredicate::is_null(FunctionContext*, const BooleanVal&);
template BooleanVal IsNullPredicate::is_null(FunctionContext*, const TinyIntVal&);
template BooleanVal IsNullPredicate::is_null(FunctionContext*, const SmallIntVal&);
template BooleanVal IsNullPredicate::is_null(FunctionContext*, const IntVal&);
template BooleanVal IsNullPredicate::is_null(FunctionContext*, const BigIntVal&);
template BooleanVal IsNullPredicate::is_null(FunctionContext*, const LargeIntVal&);
template BooleanVal IsNullPredicate::is_null(FunctionContext*, const FloatVal&);
template BooleanVal IsNullPredicate::is_null(FunctionContext*, const DoubleVal&);
template BooleanVal IsNullPredicate::is_null(FunctionContext*, const StringVal&);
template BooleanVal IsNullPredicate::is_null(FunctionContext*, const DateTimeVal&);
template BooleanVal IsNullPredicate::is_null(FunctionContext*, const DecimalV2Val&);

template BooleanVal IsNullPredicate::is_not_null(FunctionContext*, const AnyVal&);
template BooleanVal IsNullPredicate::is_not_null(FunctionContext*, const BooleanVal&);
template BooleanVal IsNullPredicate::is_not_null(FunctionContext*, const TinyIntVal&);
template BooleanVal IsNullPredicate::is_not_null(FunctionContext*, const SmallIntVal&);
template BooleanVal IsNullPredicate::is_not_null(FunctionContext*, const IntVal&);
template BooleanVal IsNullPredicate::is_not_null(FunctionContext*, const BigIntVal&);
template BooleanVal IsNullPredicate::is_not_null(FunctionContext*, const LargeIntVal&);
template BooleanVal IsNullPredicate::is_not_null(FunctionContext*, const FloatVal&);
template BooleanVal IsNullPredicate::is_not_null(FunctionContext*, const DoubleVal&);
template BooleanVal IsNullPredicate::is_not_null(FunctionContext*, const StringVal&);
template BooleanVal IsNullPredicate::is_not_null(FunctionContext*, const DateTimeVal&);
template BooleanVal IsNullPredicate::is_not_null(FunctionContext*, const DecimalV2Val&);

} // namespace doris
