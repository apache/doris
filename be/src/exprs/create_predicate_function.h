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
#include "runtime/mem_tracker.h"

namespace doris {

class MinmaxFunctionTraits {
public:
    using BasePtr = MinMaxFuncBase*;
    template <PrimitiveType type>
    static BasePtr get_function([[maybe_unused]] MemTracker* tracker) {
        return new (std::nothrow) MinMaxNumFunc<typename PrimitiveTypeTraits<type>::CppType>();
    };
};

template <bool is_vec>
class HybridSetTraits {
public:
    using BasePtr = HybridSetBase*;
    template <PrimitiveType type>
    static BasePtr get_function([[maybe_unused]] MemTracker* tracker) {
        if constexpr (is_vec) {
            using CppType = typename VecPrimitiveTypeTraits<type>::CppType;
            using Set = std::conditional_t<std::is_same_v<CppType, StringValue>, StringValueSet,
                                           HybridSet<CppType>>;
            return new (std::nothrow) Set();
        } else {
            using CppType = typename PrimitiveTypeTraits<type>::CppType;
            using Set = std::conditional_t<std::is_same_v<CppType, StringValue>, StringValueSet,
                                           HybridSet<CppType>>;
            return new (std::nothrow) Set();
        }
    };
};

class BloomFilterTraits {
public:
    using BasePtr = IBloomFilterFuncBase*;
    template <PrimitiveType type>
    static BasePtr get_function(MemTracker* tracker) {
        return new BloomFilterFunc<type, CurrentBloomFilterAdaptor>(tracker);
    };
};

template <class Traits>
class PredicateFunctionCreator {
public:
    template <PrimitiveType type>
    static typename Traits::BasePtr create(MemTracker* tracker = nullptr) {
        return Traits::template get_function<type>(tracker);
    }
};

template <class Traits>
typename Traits::BasePtr create_predicate_function(PrimitiveType type,
                                                   MemTracker* tracker = nullptr) {
    using Creator = PredicateFunctionCreator<Traits>;

    switch (type) {
    case TYPE_BOOLEAN:
        return Creator::template create<TYPE_BOOLEAN>(tracker);
    case TYPE_TINYINT:
        return Creator::template create<TYPE_TINYINT>(tracker);
    case TYPE_SMALLINT:
        return Creator::template create<TYPE_SMALLINT>(tracker);
    case TYPE_INT:
        return Creator::template create<TYPE_INT>(tracker);
    case TYPE_BIGINT:
        return Creator::template create<TYPE_BIGINT>(tracker);
    case TYPE_LARGEINT:
        return Creator::template create<TYPE_LARGEINT>(tracker);

    case TYPE_FLOAT:
        return Creator::template create<TYPE_FLOAT>(tracker);
    case TYPE_DOUBLE:
        return Creator::template create<TYPE_DOUBLE>(tracker);

    case TYPE_DECIMALV2:
        return Creator::template create<TYPE_DECIMALV2>(tracker);

    case TYPE_DATE:
        return Creator::template create<TYPE_DATE>(tracker);
    case TYPE_DATETIME:
        return Creator::template create<TYPE_DATETIME>(tracker);

    case TYPE_CHAR:
        return Creator::template create<TYPE_CHAR>(tracker);
    case TYPE_VARCHAR:
        return Creator::template create<TYPE_VARCHAR>(tracker);
    case TYPE_STRING:
        return Creator::template create<TYPE_STRING>(tracker);

    default:
        DCHECK(false) << "Invalid type.";
    }

    return nullptr;
}

inline auto create_minmax_filter(PrimitiveType type) {
    return create_predicate_function<MinmaxFunctionTraits>(type);
}

inline auto create_set(PrimitiveType type) {
    return create_predicate_function<HybridSetTraits<false>>(type);
}

// used for VInPredicate
inline auto vec_create_set(PrimitiveType type) {
    return create_predicate_function<HybridSetTraits<true>>(type);
}

inline auto create_bloom_filter(MemTracker* tracker, PrimitiveType type) {
    return create_predicate_function<BloomFilterTraits>(type, tracker);
}

} // namespace doris
