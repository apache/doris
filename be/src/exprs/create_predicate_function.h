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

#include "common/exception.h"
#include "common/status.h"
#include "exprs/hybrid_set.h"
#include "exprs/minmax_predicate.h"
#include "function_filter.h"
#include "olap/bitmap_filter_predicate.h"
#include "olap/bloom_filter_predicate.h"
#include "olap/column_predicate.h"
#include "olap/in_list_predicate.h"
#include "olap/like_column_predicate.h"
#include "runtime/define_primitive_type.h"

namespace doris {

class MinmaxFunctionTraits {
public:
    using BasePtr = MinMaxFuncBase*;
    template <PrimitiveType type, size_t N>
    static BasePtr get_function(bool null_aware) {
        using CppType = typename PrimitiveTypeTraits<type>::CppType;
        return new MinMaxNumFunc<
                std::conditional_t<std::is_same_v<CppType, StringRef>, std::string, CppType>>(
                null_aware);
    }
};

class HybridSetTraits {
public:
    using BasePtr = HybridSetBase*;
    template <PrimitiveType type, size_t N>
    static BasePtr get_function(bool null_aware) {
        using CppType = typename PrimitiveTypeTraits<type>::CppType;
        if constexpr (N >= 1 && N <= FIXED_CONTAINER_MAX_SIZE) {
            using Set = std::conditional_t<
                    std::is_same_v<CppType, StringRef>, StringSet<>,
                    HybridSet<type,
                              FixedContainer<typename PrimitiveTypeTraits<type>::CppType, N>>>;
            return new Set(null_aware);
        } else {
            using Set = std::conditional_t<
                    std::is_same_v<CppType, StringRef>, StringSet<>,
                    HybridSet<type, DynamicContainer<typename PrimitiveTypeTraits<type>::CppType>>>;
            return new Set(null_aware);
        }
    }
};

class BloomFilterTraits {
public:
    using BasePtr = BloomFilterFuncBase*;
    template <PrimitiveType type, size_t N>
    static BasePtr get_function(bool null_aware) {
        return new BloomFilterFunc<type>(null_aware);
    }
};

class BitmapFilterTraits {
public:
    using BasePtr = BitmapFilterFuncBase*;
    template <PrimitiveType type, size_t N>
    static BasePtr get_function(bool null_aware) {
        return new BitmapFilterFunc<type>(null_aware);
    }
};

template <class Traits>
class PredicateFunctionCreator {
public:
    template <PrimitiveType type, size_t N = 0>
    static typename Traits::BasePtr create(bool null_aware) {
        return Traits::template get_function<type, N>(null_aware);
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
    M(TYPE_DECIMAL128I)       \
    M(TYPE_DECIMAL256)        \
    M(TYPE_IPV4)              \
    M(TYPE_IPV6)

template <class Traits, size_t N = 0>
typename Traits::BasePtr create_predicate_function(PrimitiveType type, bool null_aware) {
    using Creator = PredicateFunctionCreator<Traits>;

    switch (type) {
    case TYPE_BOOLEAN: {
        return Creator::template create<TYPE_BOOLEAN, N>(null_aware);
    }
    case TYPE_DECIMALV2: {
        return Creator::template create<TYPE_DECIMALV2, N>(null_aware);
    }
#define M(NAME)                                               \
    case NAME: {                                              \
        return Creator::template create<NAME, N>(null_aware); \
    }
        APPLY_FOR_PRIMTYPE(M)
#undef M
    default:
        throw Exception(ErrorCode::INTERNAL_ERROR, "predicate with type " + type_to_string(type));
    }

    return nullptr;
}

template <class Traits>
typename Traits::BasePtr create_bitmap_predicate_function(PrimitiveType type) {
    using Creator = PredicateFunctionCreator<Traits>;

    switch (type) {
    case TYPE_TINYINT:
        return Creator::template create<TYPE_TINYINT>(false);
    case TYPE_SMALLINT:
        return Creator::template create<TYPE_SMALLINT>(false);
    case TYPE_INT:
        return Creator::template create<TYPE_INT>(false);
    case TYPE_BIGINT:
        return Creator::template create<TYPE_BIGINT>(false);
    default:
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "bitmap predicate with type " + type_to_string(type));
    }

    return nullptr;
}

inline auto create_minmax_filter(PrimitiveType type, bool null_aware) {
    return create_predicate_function<MinmaxFunctionTraits>(type, null_aware);
}

template <size_t N = 0>
inline auto create_set(PrimitiveType type, bool null_aware) {
    return create_predicate_function<HybridSetTraits, N>(type, null_aware);
}

inline auto create_set(PrimitiveType type, size_t size, bool null_aware) {
    if (size == 0) {
        return create_set<0>(type, null_aware);
    } else if (size == 1) {
        return create_set<1>(type, null_aware);
    } else if (size == 2) {
        return create_set<2>(type, null_aware);
    } else if (size == 3) {
        return create_set<3>(type, null_aware);
    } else if (size == 4) {
        return create_set<4>(type, null_aware);
    } else if (size == 5) {
        return create_set<5>(type, null_aware);
    } else if (size == 6) {
        return create_set<6>(type, null_aware);
    } else if (size == 7) {
        return create_set<7>(type, null_aware);
    } else if (size == FIXED_CONTAINER_MAX_SIZE) {
        return create_set<FIXED_CONTAINER_MAX_SIZE>(type, null_aware);
    } else {
        return create_set(type, null_aware);
    }
}

template <size_t N = 0>
inline HybridSetBase* create_string_value_set(bool null_aware) {
    if constexpr (N >= 1 && N <= FIXED_CONTAINER_MAX_SIZE) {
        return new StringValueSet<FixedContainer<StringRef, N>>(null_aware);
    } else {
        return new StringValueSet(null_aware);
    }
}

inline HybridSetBase* create_string_value_set(size_t size, bool null_aware) {
    if (size == 1) {
        return create_string_value_set<1>(null_aware);
    } else if (size == 2) {
        return create_string_value_set<2>(null_aware);
    } else if (size == 3) {
        return create_string_value_set<3>(null_aware);
    } else if (size == 4) {
        return create_string_value_set<4>(null_aware);
    } else if (size == 5) {
        return create_string_value_set<5>(null_aware);
    } else if (size == 6) {
        return create_string_value_set<6>(null_aware);
    } else if (size == 7) {
        return create_string_value_set<7>(null_aware);
    } else if (size == FIXED_CONTAINER_MAX_SIZE) {
        return create_string_value_set<FIXED_CONTAINER_MAX_SIZE>(null_aware);
    } else {
        return create_string_value_set(null_aware);
    }
}

inline auto create_bloom_filter(PrimitiveType type, bool null_aware) {
    return create_predicate_function<BloomFilterTraits>(type, null_aware);
}

inline auto create_bitmap_filter(PrimitiveType type) {
    return create_bitmap_predicate_function<BitmapFilterTraits>(type);
}

template <PrimitiveType PT>
ColumnPredicate* create_olap_column_predicate(uint32_t column_id,
                                              const std::shared_ptr<BloomFilterFuncBase>& filter,
                                              const TabletColumn*, bool null_aware) {
    std::shared_ptr<BloomFilterFuncBase> filter_olap;
    filter_olap.reset(create_bloom_filter(PT, null_aware));
    filter_olap->light_copy(filter.get());
    // create a new filter to match the input filter and PT. For example, filter may be varchar, but PT is char
    return new BloomFilterColumnPredicate<PT>(column_id, filter_olap);
}

template <PrimitiveType PT>
ColumnPredicate* create_olap_column_predicate(uint32_t column_id,
                                              const std::shared_ptr<BitmapFilterFuncBase>& filter,
                                              const TabletColumn*, bool) {
    if constexpr (PT == TYPE_TINYINT || PT == TYPE_SMALLINT || PT == TYPE_INT ||
                  PT == TYPE_BIGINT) {
        return new BitmapFilterColumnPredicate<PT>(column_id, filter);
    } else {
        throw Exception(ErrorCode::INTERNAL_ERROR, "bitmap filter do not support type {}", PT);
    }
}

template <PrimitiveType PT>
ColumnPredicate* create_olap_column_predicate(uint32_t column_id,
                                              const std::shared_ptr<HybridSetBase>& filter,
                                              const TabletColumn* column, bool) {
    return create_in_list_predicate<PT, PredicateType::IN_LIST>(column_id, filter,
                                                                column->length());
}

template <PrimitiveType PT>
ColumnPredicate* create_olap_column_predicate(uint32_t column_id,
                                              const std::shared_ptr<FunctionFilter>& filter,
                                              const TabletColumn* column, bool) {
    // currently only support like predicate
    if constexpr (PT == TYPE_CHAR) {
        return new LikeColumnPredicate<TYPE_CHAR>(filter->_opposite, column_id, filter->_fn_ctx,
                                                  filter->_string_param);
    } else if constexpr (PT == TYPE_VARCHAR || PT == TYPE_STRING) {
        return new LikeColumnPredicate<TYPE_STRING>(filter->_opposite, column_id, filter->_fn_ctx,
                                                    filter->_string_param);
    }
    throw Exception(ErrorCode::INTERNAL_ERROR, "function filter do not support type {}", PT);
}

template <typename T>
ColumnPredicate* create_column_predicate(uint32_t column_id, const std::shared_ptr<T>& filter,
                                         FieldType type, const TabletColumn* column,
                                         bool null_aware = false) {
    switch (type) {
#define M(NAME)                                                                           \
    case FieldType::OLAP_FIELD_##NAME: {                                                  \
        return create_olap_column_predicate<NAME>(column_id, filter, column, null_aware); \
    }
        APPLY_FOR_PRIMTYPE(M)
#undef M
    case FieldType::OLAP_FIELD_TYPE_DECIMAL: {
        return create_olap_column_predicate<TYPE_DECIMALV2>(column_id, filter, column, null_aware);
    }
    case FieldType::OLAP_FIELD_TYPE_BOOL: {
        return create_olap_column_predicate<TYPE_BOOLEAN>(column_id, filter, column, null_aware);
    }
    default:
        return nullptr;
    }
}

} // namespace doris
