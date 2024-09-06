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
    static BasePtr get_function() {
        using CppType = typename PrimitiveTypeTraits<type>::CppType;
        return new MinMaxNumFunc<
                std::conditional_t<std::is_same_v<CppType, StringRef>, std::string, CppType>>();
    }
};

class HybridSetTraits {
public:
    using BasePtr = HybridSetBase*;
    template <PrimitiveType type, size_t N>
    static BasePtr get_function() {
        using CppType = typename PrimitiveTypeTraits<type>::CppType;
        if constexpr (N >= 1 && N <= FIXED_CONTAINER_MAX_SIZE) {
            using Set = std::conditional_t<
                    std::is_same_v<CppType, StringRef>, StringSet<>,
                    HybridSet<type,
                              FixedContainer<typename PrimitiveTypeTraits<type>::CppType, N>>>;
            return new Set();
        } else {
            using Set = std::conditional_t<
                    std::is_same_v<CppType, StringRef>, StringSet<>,
                    HybridSet<type, DynamicContainer<typename PrimitiveTypeTraits<type>::CppType>>>;
            return new Set();
        }
    }
};

class BloomFilterTraits {
public:
    using BasePtr = BloomFilterFuncBase*;
    template <PrimitiveType type, size_t N>
    static BasePtr get_function() {
        return new BloomFilterFunc<type>();
    }
};

class BitmapFilterTraits {
public:
    using BasePtr = BitmapFilterFuncBase*;
    template <PrimitiveType type, size_t N>
    static BasePtr get_function() {
        return new BitmapFilterFunc<type>();
    }
};

template <class Traits>
class PredicateFunctionCreator {
public:
    template <PrimitiveType type, size_t N = 0>
    static typename Traits::BasePtr create() {
        return Traits::template get_function<type, N>();
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
typename Traits::BasePtr create_predicate_function(PrimitiveType type) {
    using Creator = PredicateFunctionCreator<Traits>;

    switch (type) {
    case TYPE_BOOLEAN: {
        return Creator::template create<TYPE_BOOLEAN, N>();
    }
    case TYPE_DECIMALV2: {
        return Creator::template create<TYPE_DECIMALV2, N>();
    }
#define M(NAME)                                     \
    case NAME: {                                    \
        return Creator::template create<NAME, N>(); \
    }
        APPLY_FOR_PRIMTYPE(M)
#undef M
    default:
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "predicate with type " + type_to_string(type));
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
        DCHECK(false) << "Invalid type: " << type_to_string(type);
    }

    return nullptr;
}

inline auto create_minmax_filter(PrimitiveType type) {
    return create_predicate_function<MinmaxFunctionTraits>(type);
}

template <size_t N = 0>
inline auto create_set(PrimitiveType type) {
    return create_predicate_function<HybridSetTraits, N>(type);
}

inline auto create_set(PrimitiveType type, size_t size) {
    if (size == 0) {
        return create_set<0>(type);
    } else if (size == 1) {
        return create_set<1>(type);
    } else if (size == 2) {
        return create_set<2>(type);
    } else if (size == 3) {
        return create_set<3>(type);
    } else if (size == 4) {
        return create_set<4>(type);
    } else if (size == 5) {
        return create_set<5>(type);
    } else if (size == 6) {
        return create_set<6>(type);
    } else if (size == 7) {
        return create_set<7>(type);
    } else if (size == FIXED_CONTAINER_MAX_SIZE) {
        return create_set<FIXED_CONTAINER_MAX_SIZE>(type);
    } else {
        return create_set(type);
    }
}

template <size_t N = 0>
inline HybridSetBase* create_string_value_set() {
    if constexpr (N >= 1 && N <= FIXED_CONTAINER_MAX_SIZE) {
        return new StringValueSet<FixedContainer<StringRef, N>>();
    } else {
        return new StringValueSet();
    }
}

inline HybridSetBase* create_string_value_set(size_t size) {
    if (size == 1) {
        return create_string_value_set<1>();
    } else if (size == 2) {
        return create_string_value_set<2>();
    } else if (size == 3) {
        return create_string_value_set<3>();
    } else if (size == 4) {
        return create_string_value_set<4>();
    } else if (size == 5) {
        return create_string_value_set<5>();
    } else if (size == 6) {
        return create_string_value_set<6>();
    } else if (size == 7) {
        return create_string_value_set<7>();
    } else if (size == FIXED_CONTAINER_MAX_SIZE) {
        return create_string_value_set<FIXED_CONTAINER_MAX_SIZE>();
    } else {
        return create_string_value_set();
    }
}

inline auto create_bloom_filter(PrimitiveType type) {
    return create_predicate_function<BloomFilterTraits>(type);
}

inline auto create_bitmap_filter(PrimitiveType type) {
    return create_bitmap_predicate_function<BitmapFilterTraits>(type);
}

template <PrimitiveType PT>
ColumnPredicate* create_olap_column_predicate(uint32_t column_id,
                                              const std::shared_ptr<BloomFilterFuncBase>& filter,
                                              int be_exec_version, const TabletColumn*) {
    std::shared_ptr<BloomFilterFuncBase> filter_olap;
    filter_olap.reset(create_bloom_filter(PT));
    filter_olap->light_copy(filter.get());
    return new BloomFilterColumnPredicate<PT>(column_id, filter);
}

template <PrimitiveType PT>
ColumnPredicate* create_olap_column_predicate(uint32_t column_id,
                                              const std::shared_ptr<BitmapFilterFuncBase>& filter,
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
ColumnPredicate* create_olap_column_predicate(uint32_t column_id,
                                              const std::shared_ptr<HybridSetBase>& filter, int,
                                              const TabletColumn* column = nullptr) {
    return create_in_list_predicate<PT, PredicateType::IN_LIST>(column_id, filter,
                                                                column->length());
}

template <PrimitiveType PT>
ColumnPredicate* create_olap_column_predicate(uint32_t column_id,
                                              const std::shared_ptr<FunctionFilter>& filter, int,
                                              const TabletColumn* column = nullptr) {
    // currently only support like predicate
    if constexpr (PT == TYPE_CHAR || PT == TYPE_VARCHAR || PT == TYPE_STRING) {
        if constexpr (PT == TYPE_CHAR) {
            return new LikeColumnPredicate<TYPE_CHAR>(filter->_opposite, column_id, filter->_fn_ctx,
                                                      filter->_string_param);
        } else {
            return new LikeColumnPredicate<TYPE_STRING>(filter->_opposite, column_id,
                                                        filter->_fn_ctx, filter->_string_param);
        }
    } else {
        return nullptr;
    }
}

template <typename T>
ColumnPredicate* create_column_predicate(uint32_t column_id, const std::shared_ptr<T>& filter,
                                         FieldType type, int be_exec_version,
                                         const TabletColumn* column = nullptr) {
    switch (type) {
#define M(NAME)                                                                                \
    case FieldType::OLAP_FIELD_##NAME: {                                                       \
        return create_olap_column_predicate<NAME>(column_id, filter, be_exec_version, column); \
    }
        APPLY_FOR_PRIMTYPE(M)
#undef M
    case FieldType::OLAP_FIELD_TYPE_DECIMAL: {
        return create_olap_column_predicate<TYPE_DECIMALV2>(column_id, filter, be_exec_version,
                                                            column);
    }
    case FieldType::OLAP_FIELD_TYPE_BOOL: {
        return create_olap_column_predicate<TYPE_BOOLEAN>(column_id, filter, be_exec_version,
                                                          column);
    }
    default:
        return nullptr;
    }
}

} // namespace doris
