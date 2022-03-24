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
    static BasePtr get_function() {
        return new (std::nothrow) MinMaxNumFunc<typename PrimitiveTypeTraits<type>::CppType>();
    };
};

class HybridSetTraits {
public:
    using BasePtr = HybridSetBase*;
    template <PrimitiveType type>
    static BasePtr get_function() {
        using CppType = typename PrimitiveTypeTraits<type>::CppType;
        using Set = std::conditional_t<std::is_same_v<CppType, StringValue>, StringValueSet,
                                       HybridSet<CppType>>;
        return new (std::nothrow) Set();
    };
};

class BloomFilterTraits {
public:
    using BasePtr = IBloomFilterFuncBase*;
    template <PrimitiveType type>
    static BasePtr get_function() {
        return new BloomFilterFunc<type, CurrentBloomFilterAdaptor>();
    };
};

template <class Traits>
class PredicateFunctionCreator {
public:
    template <PrimitiveType type>
    static typename Traits::BasePtr create() {
        return Traits::template get_function<type>();
    }
};

template <class Traits>
typename Traits::BasePtr create_predicate_function(PrimitiveType type) {
    using Creator = PredicateFunctionCreator<Traits>;

    switch (type) {
    case TYPE_BOOLEAN:
        return Creator::template create<TYPE_BOOLEAN>();
    case TYPE_TINYINT:
        return Creator::template create<TYPE_TINYINT>();
    case TYPE_SMALLINT:
        return Creator::template create<TYPE_SMALLINT>();
    case TYPE_INT:
        return Creator::template create<TYPE_INT>();
    case TYPE_BIGINT:
        return Creator::template create<TYPE_BIGINT>();
    case TYPE_LARGEINT:
        return Creator::template create<TYPE_LARGEINT>();

    case TYPE_FLOAT:
        return Creator::template create<TYPE_FLOAT>();
    case TYPE_DOUBLE:
        return Creator::template create<TYPE_DOUBLE>();

    case TYPE_DECIMALV2:
        return Creator::template create<TYPE_DECIMALV2>();

    case TYPE_DATE:
        return Creator::template create<TYPE_DATE>();
    case TYPE_DATETIME:
        return Creator::template create<TYPE_DATETIME>();

    case TYPE_CHAR:
        return Creator::template create<TYPE_CHAR>();
    case TYPE_VARCHAR:
        return Creator::template create<TYPE_VARCHAR>();
    case TYPE_STRING:
        return Creator::template create<TYPE_STRING>();

    default:
        DCHECK(false) << "Invalid type.";
    }

    return nullptr;
}

inline auto create_minmax_filter(PrimitiveType type) {
    return create_predicate_function<MinmaxFunctionTraits>(type);
}

inline auto create_set(PrimitiveType type) {
    return create_predicate_function<HybridSetTraits>(type);
}

inline auto create_bloom_filter(PrimitiveType type) {
    return create_predicate_function<BloomFilterTraits>(type);
}

} // namespace doris