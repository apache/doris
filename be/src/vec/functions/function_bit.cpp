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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/bitAnd.cpp
// and modified by Doris

#include <utility>

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/number_traits.h"
#include "vec/functions/function_binary_arithmetic.h"
#include "vec/functions/function_totype.h"
#include "vec/functions/function_unary_arithmetic.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

struct NameBitAnd {
    static constexpr auto name = "bitand";
};

template <PrimitiveType A, PrimitiveType B>
struct BitAndImpl {
    static constexpr PrimitiveType ResultType =
            NumberTraits::ResultOfBit<typename PrimitiveTypeTraits<A>::CppNativeType,
                                      typename PrimitiveTypeTraits<B>::CppNativeType>::Type;

    template <PrimitiveType Result = ResultType>
    static inline typename PrimitiveTypeTraits<Result>::CppNativeType apply(
            typename PrimitiveTypeTraits<A>::CppNativeType a,
            typename PrimitiveTypeTraits<B>::CppNativeType b) {
        return static_cast<typename PrimitiveTypeTraits<Result>::CppNativeType>(a) &
               static_cast<typename PrimitiveTypeTraits<Result>::CppNativeType>(b);
    }
};

struct NameBitNot {
    static constexpr auto name = "bitnot";
};

template <typename A>
struct BitNotImpl {
    static constexpr PrimitiveType ResultType = NumberTraits::ResultOfBitNot<A>::Type;

    static inline typename PrimitiveTypeTraits<ResultType>::ColumnItemType apply(A a) {
        return ~static_cast<typename PrimitiveTypeTraits<ResultType>::ColumnItemType>(a);
    }
};

struct NameBitOr {
    static constexpr auto name = "bitor";
};

template <PrimitiveType A, PrimitiveType B>
struct BitOrImpl {
    static constexpr PrimitiveType ResultType =
            NumberTraits::ResultOfBit<typename PrimitiveTypeTraits<A>::CppNativeType,
                                      typename PrimitiveTypeTraits<B>::CppNativeType>::Type;

    template <PrimitiveType Result = ResultType>
    static inline typename PrimitiveTypeTraits<Result>::CppNativeType apply(
            typename PrimitiveTypeTraits<A>::CppNativeType a,
            typename PrimitiveTypeTraits<B>::CppNativeType b) {
        return static_cast<typename PrimitiveTypeTraits<Result>::CppNativeType>(a) |
               static_cast<typename PrimitiveTypeTraits<Result>::CppNativeType>(b);
    }
};

struct NameBitXor {
    static constexpr auto name = "bitxor";
};

template <PrimitiveType A, PrimitiveType B>
struct BitXorImpl {
    static constexpr PrimitiveType ResultType =
            NumberTraits::ResultOfBit<typename PrimitiveTypeTraits<A>::CppNativeType,
                                      typename PrimitiveTypeTraits<B>::CppNativeType>::Type;

    template <PrimitiveType Result = ResultType>
    static inline typename PrimitiveTypeTraits<Result>::CppNativeType apply(
            typename PrimitiveTypeTraits<A>::CppNativeType a,
            typename PrimitiveTypeTraits<B>::CppNativeType b) {
        return static_cast<typename PrimitiveTypeTraits<Result>::CppNativeType>(a) ^
               static_cast<typename PrimitiveTypeTraits<Result>::CppNativeType>(b);
    }
};

struct NameBitLength {
    static constexpr auto name = "bit_length";
};

struct BitLengthImpl {
    using ReturnType = DataTypeInt32;
    static constexpr auto PrimitiveTypeImpl = PrimitiveType::TYPE_STRING;
    using Type = String;
    using ReturnColumnType = ColumnInt32;

    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         PaddedPODArray<Int32>& res) {
        auto size = offsets.size();
        res.resize(size);
        for (int i = 0; i < size; ++i) {
            int str_size = offsets[i] - offsets[i - 1];
            res[i] = (str_size * 8);
        }
        return Status::OK();
    }
};

using FunctionBitAnd = FunctionBinaryArithmetic<BitAndImpl, NameBitAnd, false>;
using FunctionBitNot = FunctionUnaryArithmetic<BitNotImpl, NameBitNot>;
using FunctionBitOr = FunctionBinaryArithmetic<BitOrImpl, NameBitOr, false>;
using FunctionBitXor = FunctionBinaryArithmetic<BitXorImpl, NameBitXor, false>;
using FunctionBitLength = FunctionUnaryToType<BitLengthImpl, NameBitLength>;

void register_function_bit(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionBitAnd>();
    factory.register_function<FunctionBitNot>();
    factory.register_function<FunctionBitOr>();
    factory.register_function<FunctionBitXor>();
    factory.register_function<FunctionBitLength>();
}
} // namespace doris::vectorized
