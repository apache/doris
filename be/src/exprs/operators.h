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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exprs/operators.h
// and modified by Doris

#ifndef DORIS_BE_SRC_QUERY_EXPRS_OPERATORS_H
#define DORIS_BE_SRC_QUERY_EXPRS_OPERATORS_H

#include "udf/udf.h"

namespace doris {

/// Operators written against the UDF interface.
class Operators {
public:
    // Do nothing, just get its symbols
    static void init();

    // Bit operator
    static TinyIntVal bitnot_tiny_int_val(FunctionContext*, const TinyIntVal&);
    static SmallIntVal bitnot_small_int_val(FunctionContext*, const SmallIntVal&);
    static IntVal bitnot_int_val(FunctionContext*, const IntVal&);
    static BigIntVal bitnot_big_int_val(FunctionContext*, const BigIntVal&);
    static LargeIntVal bitnot_large_int_val(FunctionContext*, const LargeIntVal&);

    static TinyIntVal bitand_tiny_int_val_tiny_int_val(FunctionContext*, const TinyIntVal&,
                                                       const TinyIntVal&);
    static SmallIntVal bitand_small_int_val_small_int_val(FunctionContext*, const SmallIntVal&,
                                                          const SmallIntVal&);
    static IntVal bitand_int_val_int_val(FunctionContext*, const IntVal&, const IntVal&);
    static BigIntVal bitand_big_int_val_big_int_val(FunctionContext*, const BigIntVal&,
                                                    const BigIntVal&);
    static LargeIntVal bitand_large_int_val_large_int_val(FunctionContext*, const LargeIntVal&,
                                                          const LargeIntVal&);

    static TinyIntVal bitxor_tiny_int_val_tiny_int_val(FunctionContext*, const TinyIntVal&,
                                                       const TinyIntVal&);
    static SmallIntVal bitxor_small_int_val_small_int_val(FunctionContext*, const SmallIntVal&,
                                                          const SmallIntVal&);
    static IntVal bitxor_int_val_int_val(FunctionContext*, const IntVal&, const IntVal&);
    static BigIntVal bitxor_big_int_val_big_int_val(FunctionContext*, const BigIntVal&,
                                                    const BigIntVal&);
    static LargeIntVal bitxor_large_int_val_large_int_val(FunctionContext*, const LargeIntVal&,
                                                          const LargeIntVal&);

    static TinyIntVal bitor_tiny_int_val_tiny_int_val(FunctionContext*, const TinyIntVal&,
                                                      const TinyIntVal&);
    static SmallIntVal bitor_small_int_val_small_int_val(FunctionContext*, const SmallIntVal&,
                                                         const SmallIntVal&);
    static IntVal bitor_int_val_int_val(FunctionContext*, const IntVal&, const IntVal&);
    static BigIntVal bitor_big_int_val_big_int_val(FunctionContext*, const BigIntVal&,
                                                   const BigIntVal&);
    static LargeIntVal bitor_large_int_val_large_int_val(FunctionContext*, const LargeIntVal&,
                                                         const LargeIntVal&);

    // Arithmetic
    static TinyIntVal add_tiny_int_val_tiny_int_val(FunctionContext*, const TinyIntVal&,
                                                    const TinyIntVal&);
    static SmallIntVal add_small_int_val_small_int_val(FunctionContext*, const SmallIntVal&,
                                                       const SmallIntVal&);
    static IntVal add_int_val_int_val(FunctionContext*, const IntVal&, const IntVal&);
    static BigIntVal add_big_int_val_big_int_val(FunctionContext*, const BigIntVal&,
                                                 const BigIntVal&);
    static LargeIntVal add_large_int_val_large_int_val(FunctionContext*, const LargeIntVal&,
                                                       const LargeIntVal&);
    static FloatVal add_float_val_float_val(FunctionContext*, const FloatVal&, const FloatVal&);
    static DoubleVal add_double_val_double_val(FunctionContext*, const DoubleVal&,
                                               const DoubleVal&);

    static TinyIntVal subtract_tiny_int_val_tiny_int_val(FunctionContext*, const TinyIntVal&,
                                                         const TinyIntVal&);
    static SmallIntVal subtract_small_int_val_small_int_val(FunctionContext*, const SmallIntVal&,
                                                            const SmallIntVal&);
    static IntVal subtract_int_val_int_val(FunctionContext*, const IntVal&, const IntVal&);
    static BigIntVal subtract_big_int_val_big_int_val(FunctionContext*, const BigIntVal&,
                                                      const BigIntVal&);
    static LargeIntVal subtract_large_int_val_large_int_val(FunctionContext*, const LargeIntVal&,
                                                            const LargeIntVal&);
    static FloatVal subtract_float_val_float_val(FunctionContext*, const FloatVal&,
                                                 const FloatVal&);
    static DoubleVal subtract_double_val_double_val(FunctionContext*, const DoubleVal&,
                                                    const DoubleVal&);

    static TinyIntVal multiply_tiny_int_val_tiny_int_val(FunctionContext*, const TinyIntVal&,
                                                         const TinyIntVal&);
    static SmallIntVal multiply_small_int_val_small_int_val(FunctionContext*, const SmallIntVal&,
                                                            const SmallIntVal&);
    static IntVal multiply_int_val_int_val(FunctionContext*, const IntVal&, const IntVal&);
    static BigIntVal multiply_big_int_val_big_int_val(FunctionContext*, const BigIntVal&,
                                                      const BigIntVal&);
    static LargeIntVal multiply_large_int_val_large_int_val(FunctionContext*, const LargeIntVal&,
                                                            const LargeIntVal&);
    static FloatVal multiply_float_val_float_val(FunctionContext*, const FloatVal&,
                                                 const FloatVal&);
    static DoubleVal multiply_double_val_double_val(FunctionContext*, const DoubleVal&,
                                                    const DoubleVal&);

    static DoubleVal divide_double_val_double_val(FunctionContext*, const DoubleVal&,
                                                  const DoubleVal&);

    static TinyIntVal int_divide_tiny_int_val_tiny_int_val(FunctionContext*, const TinyIntVal&,
                                                           const TinyIntVal&);
    static SmallIntVal int_divide_small_int_val_small_int_val(FunctionContext*, const SmallIntVal&,
                                                              const SmallIntVal&);
    static IntVal int_divide_int_val_int_val(FunctionContext*, const IntVal&, const IntVal&);
    static BigIntVal int_divide_big_int_val_big_int_val(FunctionContext*, const BigIntVal&,
                                                        const BigIntVal&);
    static LargeIntVal int_divide_large_int_val_large_int_val(FunctionContext*, const LargeIntVal&,
                                                              const LargeIntVal&);

    static TinyIntVal mod_tiny_int_val_tiny_int_val(FunctionContext*, const TinyIntVal&,
                                                    const TinyIntVal&);
    static SmallIntVal mod_small_int_val_small_int_val(FunctionContext*, const SmallIntVal&,
                                                       const SmallIntVal&);
    static IntVal mod_int_val_int_val(FunctionContext*, const IntVal&, const IntVal&);
    static BigIntVal mod_big_int_val_big_int_val(FunctionContext*, const BigIntVal&,
                                                 const BigIntVal&);
    static LargeIntVal mod_large_int_val_large_int_val(FunctionContext*, const LargeIntVal&,
                                                       const LargeIntVal&);

    // Binary predicate
    static BooleanVal eq_boolean_val_boolean_val(FunctionContext*, const BooleanVal&,
                                                 const BooleanVal&);
    static BooleanVal eq_tiny_int_val_tiny_int_val(FunctionContext*, const TinyIntVal&,
                                                   const TinyIntVal&);
    static BooleanVal eq_small_int_val_small_int_val(FunctionContext*, const SmallIntVal&,
                                                     const SmallIntVal&);
    static BooleanVal eq_int_val_int_val(FunctionContext*, const IntVal&, const IntVal&);
    static BooleanVal eq_big_int_val_big_int_val(FunctionContext*, const BigIntVal&,
                                                 const BigIntVal&);
    static BooleanVal eq_large_int_val_large_int_val(FunctionContext*, const LargeIntVal&,
                                                     const LargeIntVal&);
    static BooleanVal eq_float_val_float_val(FunctionContext*, const FloatVal&, const FloatVal&);
    static BooleanVal eq_double_val_double_val(FunctionContext*, const DoubleVal&,
                                               const DoubleVal&);
    static BooleanVal eq_string_val_string_val(FunctionContext*, const StringVal&,
                                               const StringVal&);
    static BooleanVal eq_datetime_val_datetime_val(FunctionContext*, const DateTimeVal&,
                                                   const DateTimeVal&);

    static BooleanVal ne_boolean_val_boolean_val(FunctionContext*, const BooleanVal&,
                                                 const BooleanVal&);
    static BooleanVal ne_tiny_int_val_tiny_int_val(FunctionContext*, const TinyIntVal&,
                                                   const TinyIntVal&);
    static BooleanVal ne_small_int_val_small_int_val(FunctionContext*, const SmallIntVal&,
                                                     const SmallIntVal&);
    static BooleanVal ne_int_val_int_val(FunctionContext*, const IntVal&, const IntVal&);
    static BooleanVal ne_big_int_val_big_int_val(FunctionContext*, const BigIntVal&,
                                                 const BigIntVal&);
    static BooleanVal ne_large_int_val_large_int_val(FunctionContext*, const LargeIntVal&,
                                                     const LargeIntVal&);
    static BooleanVal ne_float_val_float_val(FunctionContext*, const FloatVal&, const FloatVal&);
    static BooleanVal ne_double_val_double_val(FunctionContext*, const DoubleVal&,
                                               const DoubleVal&);
    static BooleanVal ne_string_val_string_val(FunctionContext*, const StringVal&,
                                               const StringVal&);
    static BooleanVal ne_datetime_val_datetime_val(FunctionContext*, const DateTimeVal&,
                                                   const DateTimeVal&);

    static BooleanVal gt_boolean_val_boolean_val(FunctionContext*, const BooleanVal&,
                                                 const BooleanVal&);
    static BooleanVal gt_tiny_int_val_tiny_int_val(FunctionContext*, const TinyIntVal&,
                                                   const TinyIntVal&);
    static BooleanVal gt_small_int_val_small_int_val(FunctionContext*, const SmallIntVal&,
                                                     const SmallIntVal&);
    static BooleanVal gt_int_val_int_val(FunctionContext*, const IntVal&, const IntVal&);
    static BooleanVal gt_big_int_val_big_int_val(FunctionContext*, const BigIntVal&,
                                                 const BigIntVal&);
    static BooleanVal gt_large_int_val_large_int_val(FunctionContext*, const LargeIntVal&,
                                                     const LargeIntVal&);
    static BooleanVal gt_float_val_float_val(FunctionContext*, const FloatVal&, const FloatVal&);
    static BooleanVal gt_double_val_double_val(FunctionContext*, const DoubleVal&,
                                               const DoubleVal&);
    static BooleanVal gt_string_val_string_val(FunctionContext*, const StringVal&,
                                               const StringVal&);
    static BooleanVal gt_datetime_val_datetime_val(FunctionContext*, const DateTimeVal&,
                                                   const DateTimeVal&);

    static BooleanVal lt_boolean_val_boolean_val(FunctionContext*, const BooleanVal&,
                                                 const BooleanVal&);
    static BooleanVal lt_tiny_int_val_tiny_int_val(FunctionContext*, const TinyIntVal&,
                                                   const TinyIntVal&);
    static BooleanVal lt_small_int_val_small_int_val(FunctionContext*, const SmallIntVal&,
                                                     const SmallIntVal&);
    static BooleanVal lt_int_val_int_val(FunctionContext*, const IntVal&, const IntVal&);
    static BooleanVal lt_big_int_val_big_int_val(FunctionContext*, const BigIntVal&,
                                                 const BigIntVal&);
    static BooleanVal lt_large_int_val_large_int_val(FunctionContext*, const LargeIntVal&,
                                                     const LargeIntVal&);
    static BooleanVal lt_float_val_float_val(FunctionContext*, const FloatVal&, const FloatVal&);
    static BooleanVal lt_double_val_double_val(FunctionContext*, const DoubleVal&,
                                               const DoubleVal&);
    static BooleanVal lt_string_val_string_val(FunctionContext*, const StringVal&,
                                               const StringVal&);
    static BooleanVal lt_datetime_val_datetime_val(FunctionContext*, const DateTimeVal&,
                                                   const DateTimeVal&);

    static BooleanVal ge_boolean_val_boolean_val(FunctionContext*, const BooleanVal&,
                                                 const BooleanVal&);
    static BooleanVal ge_tiny_int_val_tiny_int_val(FunctionContext*, const TinyIntVal&,
                                                   const TinyIntVal&);
    static BooleanVal ge_small_int_val_small_int_val(FunctionContext*, const SmallIntVal&,
                                                     const SmallIntVal&);
    static BooleanVal ge_int_val_int_val(FunctionContext*, const IntVal&, const IntVal&);
    static BooleanVal ge_big_int_val_big_int_val(FunctionContext*, const BigIntVal&,
                                                 const BigIntVal&);
    static BooleanVal ge_large_int_val_large_int_val(FunctionContext*, const LargeIntVal&,
                                                     const LargeIntVal&);
    static BooleanVal ge_float_val_float_val(FunctionContext*, const FloatVal&, const FloatVal&);
    static BooleanVal ge_double_val_double_val(FunctionContext*, const DoubleVal&,
                                               const DoubleVal&);
    static BooleanVal ge_string_val_string_val(FunctionContext*, const StringVal&,
                                               const StringVal&);
    static BooleanVal ge_datetime_val_datetime_val(FunctionContext*, const DateTimeVal&,
                                                   const DateTimeVal&);

    static BooleanVal le_boolean_val_boolean_val(FunctionContext*, const BooleanVal&,
                                                 const BooleanVal&);
    static BooleanVal le_tiny_int_val_tiny_int_val(FunctionContext*, const TinyIntVal&,
                                                   const TinyIntVal&);
    static BooleanVal le_small_int_val_small_int_val(FunctionContext*, const SmallIntVal&,
                                                     const SmallIntVal&);
    static BooleanVal le_int_val_int_val(FunctionContext*, const IntVal&, const IntVal&);
    static BooleanVal le_big_int_val_big_int_val(FunctionContext*, const BigIntVal&,
                                                 const BigIntVal&);
    static BooleanVal le_large_int_val_large_int_val(FunctionContext*, const LargeIntVal&,
                                                     const LargeIntVal&);
    static BooleanVal le_float_val_float_val(FunctionContext*, const FloatVal&, const FloatVal&);
    static BooleanVal le_double_val_double_val(FunctionContext*, const DoubleVal&,
                                               const DoubleVal&);
    static BooleanVal le_string_val_string_val(FunctionContext*, const StringVal&,
                                               const StringVal&);
    static BooleanVal le_datetime_val_datetime_val(FunctionContext*, const DateTimeVal&,
                                                   const DateTimeVal&);
};

} // namespace doris

#endif
