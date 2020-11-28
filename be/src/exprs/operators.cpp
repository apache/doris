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

#include "exprs/operators.h"

#include <boost/cstdint.hpp>

#include "exprs/anyval_util.h"
#include "runtime/datetime_value.h"
#include "runtime/string_value.h"
#include "util/debug_util.h"

namespace doris {

void Operators::init() {}

#define BINARY_OP_FN(NAME, TYPE_NAME, TYPE, OP)                                          \
    TYPE Operators::NAME##_##TYPE_NAME##_##TYPE_NAME(FunctionContext* c, const TYPE& v1, \
                                                     const TYPE& v2) {                   \
        if (v1.is_null || v2.is_null) return TYPE::null();                               \
        return TYPE(v1.val OP v2.val);                                                   \
    }

#define BINARY_OP_CHECK_ZERO_FN(NAME, TYPE_NAME, TYPE, OP)                               \
    TYPE Operators::NAME##_##TYPE_NAME##_##TYPE_NAME(FunctionContext* c, const TYPE& v1, \
                                                     const TYPE& v2) {                   \
        if (v1.is_null || v2.is_null || v2.val == 0) return TYPE::null();                \
        return TYPE(v1.val OP v2.val);                                                   \
    }

#define BITNOT_FN(TYPE, TYPE_NAME)                                          \
    TYPE Operators::bitnot_##TYPE_NAME(FunctionContext* c, const TYPE& v) { \
        if (v.is_null) return TYPE::null();                                 \
        return TYPE(~v.val);                                                \
    }

// Return infinity if overflow.
#define FACTORIAL_FN(TYPE)                                                     \
    BigIntVal Operators::Factorial_##TYPE(FunctionContext* c, const TYPE& v) { \
        if (v.is_null) return BigIntVal::null();                               \
        int64_t fact = ComputeFactorial(v.val);                                \
        if (fact < 0) {                                                        \
            return BigIntVal::null();                                          \
        }                                                                      \
        return BigIntVal(fact);                                                \
    }

#define BINARY_PREDICATE_NUMERIC_FN(NAME, TYPE_NAME, TYPE, OP)                                 \
    BooleanVal Operators::NAME##_##TYPE_NAME##_##TYPE_NAME(FunctionContext* c, const TYPE& v1, \
                                                           const TYPE& v2) {                   \
        if (v1.is_null || v2.is_null) return BooleanVal::null();                               \
        return BooleanVal(v1.val OP v2.val);                                                   \
    }

#define BINARY_PREDICATE_NONNUMERIC_FN(NAME, TYPE_NAME, FUNC_NAME, TYPE, DORIS_TYPE, OP)       \
    BooleanVal Operators::NAME##_##TYPE_NAME##_##TYPE_NAME(FunctionContext* c, const TYPE& v1, \
                                                           const TYPE& v2) {                   \
        if (v1.is_null || v2.is_null) return BooleanVal::null();                               \
        DORIS_TYPE iv1 = DORIS_TYPE::from_##FUNC_NAME(v1);                                     \
        DORIS_TYPE iv2 = DORIS_TYPE::from_##FUNC_NAME(v2);                                     \
        return BooleanVal(iv1 OP iv2);                                                         \
    }

#define BINARY_OP_NUMERIC_TYPES(NAME, OP)               \
    BINARY_OP_FN(NAME, tiny_int_val, TinyIntVal, OP);   \
    BINARY_OP_FN(NAME, small_int_val, SmallIntVal, OP); \
    BINARY_OP_FN(NAME, int_val, IntVal, OP);            \
    BINARY_OP_FN(NAME, big_int_val, BigIntVal, OP);     \
    BINARY_OP_FN(NAME, large_int_val, LargeIntVal, OP); \
    BINARY_OP_FN(NAME, float_val, FloatVal, OP);        \
    BINARY_OP_FN(NAME, double_val, DoubleVal, OP);

#define BINARY_OP_INT_TYPES(NAME, OP)                   \
    BINARY_OP_FN(NAME, tiny_int_val, TinyIntVal, OP);   \
    BINARY_OP_FN(NAME, small_int_val, SmallIntVal, OP); \
    BINARY_OP_FN(NAME, int_val, IntVal, OP);            \
    BINARY_OP_FN(NAME, big_int_val, BigIntVal, OP);     \
    BINARY_OP_FN(NAME, large_int_val, LargeIntVal, OP);

#define BINARY_OP_CHECK_ZERO_INT_TYPES(NAME, OP)                   \
    BINARY_OP_CHECK_ZERO_FN(NAME, tiny_int_val, TinyIntVal, OP);   \
    BINARY_OP_CHECK_ZERO_FN(NAME, small_int_val, SmallIntVal, OP); \
    BINARY_OP_CHECK_ZERO_FN(NAME, int_val, IntVal, OP);            \
    BINARY_OP_CHECK_ZERO_FN(NAME, big_int_val, BigIntVal, OP);     \
    BINARY_OP_CHECK_ZERO_FN(NAME, large_int_val, LargeIntVal, OP);

#define BINARY_PREDICATE_ALL_TYPES(NAME, OP)                                                     \
    BINARY_PREDICATE_NUMERIC_FN(NAME, boolean_val, BooleanVal, OP);                              \
    BINARY_PREDICATE_NUMERIC_FN(NAME, tiny_int_val, TinyIntVal, OP);                             \
    BINARY_PREDICATE_NUMERIC_FN(NAME, small_int_val, SmallIntVal, OP);                           \
    BINARY_PREDICATE_NUMERIC_FN(NAME, int_val, IntVal, OP);                                      \
    BINARY_PREDICATE_NUMERIC_FN(NAME, big_int_val, BigIntVal, OP);                               \
    BINARY_PREDICATE_NUMERIC_FN(NAME, large_int_val, LargeIntVal, OP);                           \
    BINARY_PREDICATE_NUMERIC_FN(NAME, float_val, FloatVal, OP);                                  \
    BINARY_PREDICATE_NUMERIC_FN(NAME, double_val, DoubleVal, OP);                                \
    BINARY_PREDICATE_NONNUMERIC_FN(NAME, string_val, string_val, StringVal, StringValue, OP);    \
    BINARY_PREDICATE_NONNUMERIC_FN(NAME, datetime_val, datetime_val, DateTimeVal, DateTimeValue, \
                                   OP);

BINARY_OP_NUMERIC_TYPES(add, +);
BINARY_OP_NUMERIC_TYPES(subtract, -);
BINARY_OP_NUMERIC_TYPES(multiply, *);

BINARY_OP_CHECK_ZERO_FN(divide, double_val, DoubleVal, /);

BINARY_OP_CHECK_ZERO_INT_TYPES(int_divide, /);
BINARY_OP_CHECK_ZERO_INT_TYPES(mod, %);

// Bit operator
BINARY_OP_INT_TYPES(bitand, &);
BINARY_OP_INT_TYPES(bitxor, ^);
BINARY_OP_INT_TYPES(bitor, |);
BITNOT_FN(TinyIntVal, tiny_int_val);
BITNOT_FN(SmallIntVal, small_int_val);
BITNOT_FN(IntVal, int_val);
BITNOT_FN(BigIntVal, big_int_val);
BITNOT_FN(LargeIntVal, large_int_val);

#if 0
static const int64_t FACTORIAL_MAX = 20;
static const int64_t FACTORIAL_LOOKUP[] = {
    1LL, // 0!
    1LL, // 1!
    2LL, // 2!
    6LL, // 3!
    24LL, // 4!
    120LL, // 5!
    720LL, // 6!
    5040LL, // 7!
    40320LL, // 8!
    362880LL, // 9!
    3628800LL, // 10!
    39916800LL, // 11!
    479001600LL, // 12!
    6227020800LL, // 13!
    87178291200LL, // 14!
    1307674368000LL, // 15!
    20922789888000LL, // 16!
    355687428096000LL, // 17!
    6402373705728000LL, // 18!
    121645100408832000LL, // 19!
    2432902008176640000LL, // 20!
};

// Compute factorial - return -1 if out of range
// Factorial of any number <= 1 returns 1
static int64_t ComputeFactorial(int64_t n) {
  // Check range based on arg: 20! < 2^63 -1 < 21!
  if (n > FACTORIAL_MAX) {
    return -1;
  } else if (n < 0) {
    return 1;
  }

  return FACTORIAL_LOOKUP[n];
}

FACTORIAL_FN(TinyIntVal);
FACTORIAL_FN(SmallIntVal);
FACTORIAL_FN(IntVal);
FACTORIAL_FN(BigIntVal);
#endif

BINARY_PREDICATE_ALL_TYPES(eq, ==);
BINARY_PREDICATE_ALL_TYPES(ne, !=);
BINARY_PREDICATE_ALL_TYPES(gt, >);
BINARY_PREDICATE_ALL_TYPES(lt, <);
BINARY_PREDICATE_ALL_TYPES(ge, >=);
BINARY_PREDICATE_ALL_TYPES(le, <=);
} // namespace doris
