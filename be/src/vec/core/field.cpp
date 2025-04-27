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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Field.cpp
// and modified by Doris

#include "vec/core/field.h"

#include "vec/core/accurate_comparison.h"
#include "vec/core/decimal_comparison.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/io/io_helper.h"
#include "vec/io/var_int.h"

namespace doris {
namespace vectorized {
class BufferReadable;
class BufferWritable;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

template <>
Decimal128V3 DecimalField<Decimal128V3>::get_scale_multiplier() const {
    return DataTypeDecimal<Decimal128V3>::get_scale_multiplier(scale);
}

template <typename T>
bool dec_equal(T x, T y, UInt32 x_scale, UInt32 y_scale) {
    using Comparator = DecimalComparison<T, T, EqualsOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

template <typename T>
bool dec_less(T x, T y, UInt32 x_scale, UInt32 y_scale) {
    using Comparator = DecimalComparison<T, T, LessOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

template <typename T>
bool dec_less_or_equal(T x, T y, UInt32 x_scale, UInt32 y_scale) {
    using Comparator = DecimalComparison<T, T, LessOrEqualsOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

#define DECLARE_DECIMAL_COMPARISON(TYPE)                               \
    template <>                                                        \
    bool decimal_equal(TYPE x, TYPE y, UInt32 xs, UInt32 ys) {         \
        return dec_equal(x, y, xs, ys);                                \
    }                                                                  \
    template <>                                                        \
    bool decimal_less(TYPE x, TYPE y, UInt32 xs, UInt32 ys) {          \
        return dec_less(x, y, xs, ys);                                 \
    }                                                                  \
    template <>                                                        \
    bool decimal_less_or_equal(TYPE x, TYPE y, UInt32 xs, UInt32 ys) { \
        return dec_less_or_equal(x, y, xs, ys);                        \
    }                                                                  \
    template <>                                                        \
    TYPE DecimalField<TYPE>::get_scale_multiplier() const {            \
        return DataTypeDecimal<TYPE>::get_scale_multiplier(scale);     \
    }

DECLARE_DECIMAL_COMPARISON(Decimal32)
DECLARE_DECIMAL_COMPARISON(Decimal64)
DECLARE_DECIMAL_COMPARISON(Decimal128V2)
DECLARE_DECIMAL_COMPARISON(Decimal256)

template <>
bool decimal_equal(Decimal128V3 x, Decimal128V3 y, UInt32 xs, UInt32 ys) {
    return dec_equal(x, y, xs, ys);
}
template <>
bool decimal_less(Decimal128V3 x, Decimal128V3 y, UInt32 xs, UInt32 ys) {
    return dec_less(x, y, xs, ys);
}
template <>
bool decimal_less_or_equal(Decimal128V3 x, Decimal128V3 y, UInt32 xs, UInt32 ys) {
    return dec_less_or_equal(x, y, xs, ys);
}
} // namespace doris::vectorized
