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

#ifndef DORIS_BE_SRC_QUERY_EXPRS_DECIMAL_OPERATORS_H
#define DORIS_BE_SRC_QUERY_EXPRS_DECIMAL_OPERATORS_H

#include <stdint.h>

#include "runtime/decimal_value.h"
#include "udf/udf.h"

namespace doris {

class Expr;
struct ExprValue;
class TupleRow;

/// Implementation of the decimal operators. These include the cast,
/// arithmetic and binary operators.
class DecimalOperators {
public:
    static void init();

    static DecimalVal cast_to_decimal_val(FunctionContext*, const TinyIntVal&);
    static DecimalVal cast_to_decimal_val(FunctionContext*, const SmallIntVal&);
    static DecimalVal cast_to_decimal_val(FunctionContext*, const IntVal&);
    static DecimalVal cast_to_decimal_val(FunctionContext*, const BigIntVal&);
    static DecimalVal cast_to_decimal_val(FunctionContext*, const LargeIntVal&);
    static DecimalVal cast_to_decimal_val(FunctionContext*, const FloatVal&);
    static DecimalVal cast_to_decimal_val(FunctionContext*, const DoubleVal&);
    static DecimalVal cast_to_decimal_val(FunctionContext*, const DateTimeVal&);
    static DecimalVal cast_to_decimal_val(FunctionContext*, const StringVal&);

    static BooleanVal cast_to_boolean_val(FunctionContext*, const DecimalVal&);
    static TinyIntVal cast_to_tiny_int_val(FunctionContext*, const DecimalVal&);
    static SmallIntVal cast_to_small_int_val(FunctionContext*, const DecimalVal&);
    static IntVal cast_to_int_val(FunctionContext*, const DecimalVal&);
    static BigIntVal cast_to_big_int_val(FunctionContext*, const DecimalVal&);
    static LargeIntVal cast_to_large_int_val(FunctionContext*, const DecimalVal&);
    static FloatVal cast_to_float_val(FunctionContext*, const DecimalVal&);
    static DoubleVal cast_to_double_val(FunctionContext*, const DecimalVal&);
    static StringVal cast_to_string_val(FunctionContext*, const DecimalVal&);
    static DateTimeVal cast_to_datetime_val(FunctionContext*, const DecimalVal&);

    static DecimalVal add_decimal_val_decimal_val(FunctionContext*, const DecimalVal&,
                                                  const DecimalVal&);
    static DecimalVal subtract_decimal_val_decimal_val(FunctionContext*, const DecimalVal&,
                                                       const DecimalVal&);
    static DecimalVal multiply_decimal_val_decimal_val(FunctionContext*, const DecimalVal&,
                                                       const DecimalVal&);
    static DecimalVal divide_decimal_val_decimal_val(FunctionContext*, const DecimalVal&,
                                                     const DecimalVal&);
    static DecimalVal mod_decimal_val_decimal_val(FunctionContext*, const DecimalVal&,
                                                  const DecimalVal&);

    static BooleanVal eq_decimal_val_decimal_val(FunctionContext*, const DecimalVal&,
                                                 const DecimalVal&);
    static BooleanVal ne_decimal_val_decimal_val(FunctionContext*, const DecimalVal&,
                                                 const DecimalVal&);
    static BooleanVal gt_decimal_val_decimal_val(FunctionContext*, const DecimalVal&,
                                                 const DecimalVal&);
    static BooleanVal lt_decimal_val_decimal_val(FunctionContext*, const DecimalVal&,
                                                 const DecimalVal&);
    static BooleanVal ge_decimal_val_decimal_val(FunctionContext*, const DecimalVal&,
                                                 const DecimalVal&);
    static BooleanVal le_decimal_val_decimal_val(FunctionContext*, const DecimalVal&,
                                                 const DecimalVal&);
};

} // namespace doris

#endif
