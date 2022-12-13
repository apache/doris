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
#include "olap/bitmap_filter_predicate.h"
#include "olap/bloom_filter_predicate.h"
#include "olap/column_predicate.h"
#include "olap/in_list_predicate.h"
#include "runtime/define_primitive_type.h"

namespace doris {

class MinmaxFunctionTraits {
public:
    using BasePtr = MinMaxFuncBase*;
    template <PrimitiveType type>
    static BasePtr get_function() {
        return new MinMaxNumFunc<typename PrimitiveTypeTraits<type>::CppType>();
    };
};

template <bool is_vec>
class HybridSetTraits {
public:
    using BasePtr = HybridSetBase*;
    template <PrimitiveType type>
    static BasePtr get_function() {
        using CppType = typename PrimitiveTypeTraits<type>::CppType;
        using Set = std::conditional_t<std::is_same_v<CppType, StringValue>, StringSet,
                                       HybridSet<type, is_vec>>;
        return new Set();
    };
};

class BloomFilterTraits {
public:
    using BasePtr = BloomFilterFuncBase*;
    template <PrimitiveType type>
    static BasePtr get_function() {
        return new BloomFilterFunc<type>();
    };
};

class BitmapFilterTraits {
public:
    using BasePtr = BitmapFilterFuncBase*;
    template <PrimitiveType type>
    static BasePtr get_function() {
        return new BitmapFilterFunc<type>();
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

#define APPLY_FOR_PRIMTYPE(M) \
    M(TYPE_TINYINT)           \
    M(TYPE_SMALLINT)          \
    M(TYPE_INT)               \
    M(TYPE_BIGINT)            \
    M(TYPE_LARGEINT)          \
    M(TYPE_FLOAT)             \
    M(TYPE_DOUBLE)            \
    M(TYPE_DATE)              \
    M(TYPE_DATETIME)          \
    M(TYPE_DATEV2)            \
    M(TYPE_DATETIMEV2)        \
    M(TYPE_CHAR)              \
    M(TYPE_VARCHAR)           \
    M(TYPE_STRING)            \
    M(TYPE_DECIMAL32)         \
    M(TYPE_DECIMAL64)         \
    M(TYPE_DECIMAL128I)

template <class Traits>
typename Traits::BasePtr create_predicate_function(PrimitiveType type) {
    using Creator = PredicateFunctionCreator<Traits>;

    switch (type) {
    case TYPE_BOOLEAN: {
        return Creator::template create<TYPE_BOOLEAN>();
    }
    case TYPE_DECIMALV2: {
        return Creator::template create<TYPE_DECIMALV2>();
    }
#define M(NAME)                                  \
    case NAME: {                                 \
        return Creator::template create<NAME>(); \
    }
        APPLY_FOR_PRIMTYPE(M)
#undef M
    default:
        DCHECK(false) << "Invalid type.";
    }

    return nullptr;
}

template <class Traits>
typename Traits::BasePtr create_bitmap_predicate_function(PrimitiveType type) {
    using Creator = PredicateFunctionCreator<Traits>;

    switch (type) {
    case TYPE_TINYINT:
        return Creator::template create<TYPE_TINYINT>();
    case TYPE_SMALLINT:
        return Creator::template create<TYPE_SMALLINT>();
    case TYPE_INT:
        return Creator::template create<TYPE_INT>();
    case TYPE_BIGINT:
        return Creator::template create<TYPE_BIGINT>();
    default:
        DCHECK(false) << "Invalid type.";
    }

    return nullptr;
}

inline auto create_minmax_filter(PrimitiveType type) {
    return create_predicate_function<MinmaxFunctionTraits>(type);
}

inline auto create_set(PrimitiveType type, bool is_vectorized = false) {
    if (is_vectorized) {
        return create_predicate_function<HybridSetTraits<true>>(type);
    } else {
        return create_predicate_function<HybridSetTraits<false>>(type);
    }
}

inline auto create_bloom_filter(PrimitiveType type) {
    return create_predicate_function<BloomFilterTraits>(type);
}

inline auto create_bitmap_filter(PrimitiveType type) {
    return create_bitmap_predicate_function<BitmapFilterTraits>(type);
}

template <PrimitiveType PT>
inline ColumnPredicate* create_olap_column_predicate(
        uint32_t column_id, const std::shared_ptr<BloomFilterFuncBase>& filter, int be_exec_version,
        const TabletColumn*) {
    std::shared_ptr<BloomFilterFuncBase> filter_olap;
    filter_olap.reset(create_bloom_filter(PT));
    filter_olap->light_copy(filter.get());
    return new BloomFilterColumnPredicate<PT>(column_id, filter, be_exec_version);
}

template <PrimitiveType PT>
inline ColumnPredicate* create_olap_column_predicate(
        uint32_t column_id, const std::shared_ptr<BitmapFilterFuncBase>& filter,
        int be_exec_version, const TabletColumn*) {
    if constexpr (PT == TYPE_TINYINT || PT == TYPE_SMALLINT || PT == TYPE_INT ||
                  PT == TYPE_BIGINT) {
        std::shared_ptr<BitmapFilterFuncBase> filter_olap;
        filter_olap.reset(create_bitmap_filter(PT));
        filter_olap->light_copy(filter.get());
        return new BitmapFilterColumnPredicate<PT>(column_id, filter, be_exec_version);
    } else {
        return nullptr;
    }
}

template <PrimitiveType PT>
inline ColumnPredicate* create_olap_column_predicate(uint32_t column_id,
                                                     const std::shared_ptr<HybridSetBase>& filter,
                                                     int, const TabletColumn* column = nullptr) {
    return new InListPredicateBase<PT, PredicateType::IN_LIST>(column_id, filter, column->length());
}

template <typename T>
inline ColumnPredicate* create_column_predicate(uint32_t column_id,
                                                const std::shared_ptr<T>& filter, FieldType type,
                                                int be_exec_version,
                                                const TabletColumn* column = nullptr) {
    switch (type) {
#define M(NAME)                                                                                \
    case OLAP_FIELD_##NAME: {                                                                  \
        return create_olap_column_predicate<NAME>(column_id, filter, be_exec_version, column); \
    }
        APPLY_FOR_PRIMTYPE(M)
#undef M
    case OLAP_FIELD_TYPE_DECIMAL: {
        return create_olap_column_predicate<TYPE_DECIMALV2>(column_id, filter, be_exec_version,
                                                            column);
    }
    case OLAP_FIELD_TYPE_BOOL: {
        return create_olap_column_predicate<TYPE_BOOLEAN>(column_id, filter, be_exec_version,
                                                          column);
    }
    default:
        return nullptr;
    }
}

} // namespace doris
