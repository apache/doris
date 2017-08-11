// Copyright 2005 Google Inc.
//
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
//
// ---
//
//

#include "gutil/mathlimits.h"

#include "gutil/integral_types.h"

// MSVC++ 2005 thinks the header declaration was a definition, and
// erroneously flags these as a duplicate definition.
#ifdef _MSC_VER

#define DEF_COMMON_LIMITS(Type)
#define DEF_UNSIGNED_INT_LIMITS(Type)
#define DEF_SIGNED_INT_LIMITS(Type)
#define DEF_PRECISION_LIMITS(Type)

#else

#define DEF_COMMON_LIMITS(Type) \
const bool MathLimits<Type>::kIsSigned; \
const bool MathLimits<Type>::kIsInteger; \
const int MathLimits<Type>::kMin10Exp; \
const int MathLimits<Type>::kMax10Exp;

#define DEF_UNSIGNED_INT_LIMITS(Type) \
DEF_COMMON_LIMITS(Type) \
const Type MathLimits<Type>::kPosMin; \
const Type MathLimits<Type>::kPosMax; \
const Type MathLimits<Type>::kMin; \
const Type MathLimits<Type>::kMax; \
const Type MathLimits<Type>::kEpsilon; \
const Type MathLimits<Type>::kStdError;

#define DEF_SIGNED_INT_LIMITS(Type) \
DEF_UNSIGNED_INT_LIMITS(Type) \
const Type MathLimits<Type>::kNegMin; \
const Type MathLimits<Type>::kNegMax;

#define DEF_PRECISION_LIMITS(Type) \
const int MathLimits<Type>::kPrecisionDigits;

#endif  // not _MSC_VER

// http://en.wikipedia.org/wiki/Quadruple_precision_floating-point_format#Double-double_arithmetic
// With some compilers (gcc 4.6.x) on some platforms (powerpc64),
// "long double" is implemented as a pair of double: "double double" format.
// This causes a problem with epsilon (eps).
// eps is the smallest positive number such that 1.0 + eps > 1.0
//
// Normal format:  1.0 + e = 1.0...01      // N-1 zeros for N fraction bits
// D-D format:     1.0 + e = 1.000...0001  // epsilon can be very small
//
// In the normal format, 1.0 + e has to fit in one stretch of bits.
// The maximum rounding error is half of eps.
//
// In the double-double format, 1.0 + e splits across two doubles:
// 1.0 in the high double, e in the low double, and they do not have to
// be contiguous.  The maximum rounding error on a value close to 1.0 is
// much larger than eps.
//
// Some code checks for errors by comparing a computed value to a golden
// value +/- some multiple of the maximum rounding error.  The maximum
// rounding error is not available so we use eps as an approximation
// instead.  That fails when long double is in the double-double format.
// Therefore, we define kStdError as a multiple of
// max(DBL_EPSILON * DBL_EPSILON, kEpsilon) rather than a multiple of kEpsilon.

#define DEF_FP_LIMITS(Type, PREFIX) \
DEF_COMMON_LIMITS(Type) \
const Type MathLimits<Type>::kPosMin = PREFIX##_MIN; \
const Type MathLimits<Type>::kPosMax = PREFIX##_MAX; \
const Type MathLimits<Type>::kMin = -MathLimits<Type>::kPosMax; \
const Type MathLimits<Type>::kMax = MathLimits<Type>::kPosMax; \
const Type MathLimits<Type>::kNegMin = -MathLimits<Type>::kPosMin; \
const Type MathLimits<Type>::kNegMax = -MathLimits<Type>::kPosMax; \
const Type MathLimits<Type>::kEpsilon = PREFIX##_EPSILON; \
/* 32 is 5 bits of mantissa error; should be adequate for common errors */ \
const Type MathLimits<Type>::kStdError = \
  32 * (DBL_EPSILON * DBL_EPSILON > MathLimits<Type>::kEpsilon \
      ? DBL_EPSILON * DBL_EPSILON : MathLimits<Type>::kEpsilon); \
DEF_PRECISION_LIMITS(Type) \
const Type MathLimits<Type>::kNaN = HUGE_VAL - HUGE_VAL; \
const Type MathLimits<Type>::kPosInf = HUGE_VAL; \
const Type MathLimits<Type>::kNegInf = -HUGE_VAL;

DEF_SIGNED_INT_LIMITS(int8)
DEF_SIGNED_INT_LIMITS(int16)
DEF_SIGNED_INT_LIMITS(int32)
DEF_SIGNED_INT_LIMITS(int64)
DEF_UNSIGNED_INT_LIMITS(uint8)
DEF_UNSIGNED_INT_LIMITS(uint16)
DEF_UNSIGNED_INT_LIMITS(uint32)
DEF_UNSIGNED_INT_LIMITS(uint64)

DEF_FP_LIMITS(float, FLT)
DEF_FP_LIMITS(double, DBL)
DEF_FP_LIMITS(long double, LDBL);

#undef DEF_COMMON_LIMITS
#undef DEF_SIGNED_INT_LIMITS
#undef DEF_UNSIGNED_INT_LIMITS
#undef DEF_FP_LIMITS
#undef DEF_PRECISION_LIMITS
