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

#pragma once

#include "exprs/bloomfilter_predicate.h"
#include "exprs/hybrid_set.h"
#include "exprs/minmax_predicate.h"

namespace doris {

class MinmaxFunctionTraits {
public:
    using BasePtr = MinMaxFuncBase*;
    template <PrimitiveType type>
    static BasePtr get_function(bool is_runtime_filter) {
        if constexpr (type == PrimitiveType::TYPE_DECIMALV2) {
            if (is_runtime_filter) {
                return new (std::nothrow) MinMaxNumFunc<int128_t>();
            }
        }
        return new (std::nothrow) MinMaxNumFunc<typename PrimitiveTypeTraits<type>::CppType>();
    };
};

class HybridSetTraits {
public:
    using BasePtr = HybridSetBase*;
    template <PrimitiveType type>
    static BasePtr get_function(bool is_runtime_filter) {
        using CppType = typename PrimitiveTypeTraits<type>::CppType;
        using Set = std::conditional_t<std::is_same_v<CppType, StringValue>, StringValueSet,
                                       HybridSet<CppType>>;
        if constexpr (type == PrimitiveType::TYPE_DECIMALV2) {
            if (is_runtime_filter) {
                return new (std::nothrow) HybridSet<int128_t>();
            }
        }
        return new (std::nothrow) Set();
    };
};

class BloomFilterTraits {
public:
    using BasePtr = IBloomFilterFuncBase*;
    template <PrimitiveType type>
    static BasePtr get_function(bool is_runtime_filter) {
        if constexpr (type == PrimitiveType::TYPE_DECIMALV2) {
            if (is_runtime_filter) {
                return new BloomFilterFunc<PrimitiveType::TYPE_LARGEINT,
                                           CurrentBloomFilterAdaptor>();
            }
        }
        return new BloomFilterFunc<type, CurrentBloomFilterAdaptor>();
    };
};

template <class Traits>
class PredicateFunctionCreator {
public:
    template <PrimitiveType type>
    static typename Traits::BasePtr create(bool is_runtime_filter) {
        return Traits::template get_function<type>(is_runtime_filter);
    }
};

template <class Traits>
typename Traits::BasePtr create_predicate_function(PrimitiveType type, bool is_runtime_filter) {
    using Creator = PredicateFunctionCreator<Traits>;

    switch (type) {
    case TYPE_BOOLEAN:
        return Creator::template create<TYPE_BOOLEAN>(is_runtime_filter);
    case TYPE_TINYINT:
        return Creator::template create<TYPE_TINYINT>(is_runtime_filter);
    case TYPE_SMALLINT:
        return Creator::template create<TYPE_SMALLINT>(is_runtime_filter);
    case TYPE_INT:
        return Creator::template create<TYPE_INT>(is_runtime_filter);
    case TYPE_BIGINT:
        return Creator::template create<TYPE_BIGINT>(is_runtime_filter);
    case TYPE_LARGEINT:
        return Creator::template create<TYPE_LARGEINT>(is_runtime_filter);

    case TYPE_FLOAT:
        return Creator::template create<TYPE_FLOAT>(is_runtime_filter);
    case TYPE_DOUBLE:
        return Creator::template create<TYPE_DOUBLE>(is_runtime_filter);

    case TYPE_DECIMALV2:
        return Creator::template create<TYPE_DECIMALV2>(is_runtime_filter);

    case TYPE_DATE:
        return Creator::template create<TYPE_DATE>(is_runtime_filter);
    case TYPE_DATETIME:
        return Creator::template create<TYPE_DATETIME>(is_runtime_filter);
    case TYPE_DATEV2:
        return Creator::template create<TYPE_DATEV2>(is_runtime_filter);

    case TYPE_CHAR:
        return Creator::template create<TYPE_CHAR>(is_runtime_filter);
    case TYPE_VARCHAR:
        return Creator::template create<TYPE_VARCHAR>(is_runtime_filter);
    case TYPE_STRING:
        return Creator::template create<TYPE_STRING>(is_runtime_filter);

    default:
        DCHECK(false) << "Invalid type.";
    }

    return nullptr;
}

inline auto create_minmax_filter(PrimitiveType type, bool is_runtime_filter = false) {
    return create_predicate_function<MinmaxFunctionTraits>(type, is_runtime_filter);
}

inline auto create_set(PrimitiveType type, bool is_runtime_filter = false) {
    return create_predicate_function<HybridSetTraits>(type, is_runtime_filter);
}

inline auto create_bloom_filter(PrimitiveType type, bool is_runtime_filter = false) {
    return create_predicate_function<BloomFilterTraits>(type, is_runtime_filter);
}

} // namespace doris