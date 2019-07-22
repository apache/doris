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

#include "olap/hll.h"
#include "olap/types.h"
#include "util/arena.h"

namespace doris {

using AggeInitFunc = void (*)(char* left, Arena* arena);
using AggUpdateFunc = void (*)(char* left, const char* right, Arena* arena);
using AggFinalizeFunc = void (*)(char* data, Arena* arena);

// This class contains information about aggregate operation.
class AggregateInfo {
public:
    // Init function will initialize aggregation execute environment in dst.
    // For example: for sum, we just initial dst to 0. For HLL column, it will
    // allocate and init context used to compute HLL.
    //
    // Memory Note: For plain memory can be allocated from arena, whose lifetime
    // will last util finalize function is called. Memory allocated from heap should
    // be freed in finalize functioin to avoid memory leak.
    inline void init(void* dst, Arena* arena) const {
        _init_fn((char*)dst, arena);
    }

    // Actually do the aggregate operation. dst is the context which is initialized
    // by init function, src is the current value which is to be aggregated.
    // For example: For sum, dst is the current sum, and src is the next value which
    // will be added to sum.
    // This function usually is used when load function.
    //
    // Memory Note: Same with init function.
    inline void update(void* dst, const void* src, Arena* arena) const {
        _update_fn((char*)dst, (const char*)src, arena);
    }

    // Merge aggregated intermediate data. Data stored in engine is aggregated,
    // because storage has done some aggregate when loading or compaction.
    // So this function is often used in read operation.
    // 
    // Memory Note: Same with init function.
    inline void merge(void* dst, const void* src, Arena* arena) const {
        _merge_fn((char*)dst, (const char*)src, arena);
    }

    // Finalize function convert intermediate context into final format. For example:
    // For HLL type, finalize function will serialize the aggregate context into a slice.
    // For input src points to the context, and when function is finished, result will be
    // saved in src.
    //
    // Memory Note: All heap memory allocated in init and update function should be freed
    // before this function return. Memory allocated from arena will be still available
    // and will be freed by client.
    inline void finalize(void* src, Arena* arena) const {
        _finalize_fn((char*)src, arena);
    }

private:
    void (*_init_fn)(char* dst, Arena* arena);
    void (*_update_fn)(char* dst, const char* src, Arena* arena);
    void (*_merge_fn)(char* dst, const char* src, Arena* arena);
    void (*_finalize_fn)(char* dst, Arena* arena);

    friend class AggregateFuncResolver;

    template<typename Traits>
    AggregateInfo(const Traits& traits);
};

struct BaseAggregateFuncs {
    static void init(char* dst, Arena* arena) { }
    static void update(char* dst, const char* src, Arena* arena) { }
    AggUpdateFunc merge = nullptr;
    static void finalize(char* src, Arena* arena) { }
};

template<FieldAggregationMethod agg_method, FieldType field_type>
struct AggregateFuncTraits : public BaseAggregateFuncs {
};

template <FieldType field_type>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MIN, field_type> : public BaseAggregateFuncs {
    static void update(char* left, const char* right, Arena* arena) {
        typedef typename FieldTypeTraits<field_type>::CppType CppType;
        bool l_null = *reinterpret_cast<bool*>(left);
        bool r_null = *reinterpret_cast<const bool*>(right);
        if (l_null) {
            return;
        } else if (r_null) {
            *reinterpret_cast<bool*>(left) = true;
        } else {
            CppType* l_val = reinterpret_cast<CppType*>(left + 1);
            const CppType* r_val = reinterpret_cast<const CppType*>(right + 1);
            if (*r_val < *l_val) { *l_val = *r_val; }
        }
    }
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MIN, OLAP_FIELD_TYPE_LARGEINT> : public BaseAggregateFuncs {
    static void update(char* left, const char* right, Arena* arena) {
        typedef typename FieldTypeTraits<OLAP_FIELD_TYPE_LARGEINT>::CppType CppType;
        bool l_null = *reinterpret_cast<bool*>(left);
        bool r_null = *reinterpret_cast<const bool*>(right);
        if (l_null) {
            return;
        } else if (r_null) {
            *reinterpret_cast<bool*>(left) = true;
        } else {
            CppType l_val, r_val;
            memcpy(&l_val, left + 1, sizeof(CppType));
            memcpy(&r_val, right + 1, sizeof(CppType));
            if (r_val < l_val) {
                memcpy(left + 1, right + 1, sizeof(CppType));
            }
        }
    }
};

template <FieldType field_type>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MAX, field_type> : public BaseAggregateFuncs {
    static void update(char* left, const char* right, Arena* arena) {
        typedef typename FieldTypeTraits<field_type>::CppType CppType;
        bool l_null = *reinterpret_cast<bool*>(left);
        bool r_null = *reinterpret_cast<const bool*>(right);
        if (r_null) {
            return;
        }

        CppType* l_val = reinterpret_cast<CppType*>(left + 1);
        const CppType* r_val = reinterpret_cast<const CppType*>(right + 1);
        if (l_null) {
            *reinterpret_cast<bool*>(left) = false;
            *l_val = *r_val;
        } else {
            if (*r_val > *l_val) { *l_val = *r_val; }
        }
    }
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MAX, OLAP_FIELD_TYPE_LARGEINT> : public BaseAggregateFuncs {
    static void update(char* left, const char* right, Arena* arena) {
        typedef typename FieldTypeTraits<OLAP_FIELD_TYPE_LARGEINT>::CppType CppType;
        bool l_null = *reinterpret_cast<bool*>(left);
        bool r_null = *reinterpret_cast<const bool*>(right);
        if (r_null) {
            return;
        }

        if (l_null) {
            *reinterpret_cast<bool*>(left) = false;
            memcpy(left + 1, right + 1, sizeof(CppType));
        } else {
            CppType l_val, r_val;
            memcpy(&l_val, left + 1, sizeof(CppType));
            memcpy(&r_val, right + 1, sizeof(CppType));
            if (r_val > l_val) {
                memcpy(left + 1, right + 1, sizeof(CppType));
            }
        }
    }
};

template <FieldType field_type>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_SUM, field_type> : public BaseAggregateFuncs {
    static void update(char* left, const char* right, Arena* arena) {
        typedef typename FieldTypeTraits<field_type>::CppType CppType;
        bool l_null = *reinterpret_cast<bool*>(left);
        bool r_null = *reinterpret_cast<const bool*>(right);
        if (r_null) {
            return;
        }

        CppType* l_val = reinterpret_cast<CppType*>(left + 1);
        const CppType* r_val = reinterpret_cast<const CppType*>(right + 1);
        if (l_null) {
            *reinterpret_cast<bool*>(left) = false;
            *l_val = *r_val;
        } else {
            *l_val += *r_val;
        }
    }
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_SUM, OLAP_FIELD_TYPE_LARGEINT> : public BaseAggregateFuncs {
    static void update(char* left, const char* right, Arena* arena) {
        typedef typename FieldTypeTraits<OLAP_FIELD_TYPE_LARGEINT>::CppType CppType;
        bool l_null = *reinterpret_cast<bool*>(left);
        bool r_null = *reinterpret_cast<const bool*>(right);
        if (r_null) {
            return;
        }

        if (l_null) {
            *reinterpret_cast<bool*>(left) = false;
            memcpy(left + 1, right + 1, sizeof(CppType));
        } else {
            CppType l_val, r_val;
            memcpy(&l_val, left + 1, sizeof(CppType));
            memcpy(&r_val, right + 1, sizeof(CppType));
            l_val += r_val;
            memcpy(left + 1, &l_val, sizeof(CppType));
        }
    }
};

template <FieldType field_type>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_REPLACE, field_type> : public BaseAggregateFuncs {
    static void update(char* left, const char* right, Arena* arena) {
        typedef typename FieldTypeTraits<field_type>::CppType CppType;
        bool r_null = *reinterpret_cast<const bool*>(right);
        *reinterpret_cast<bool*>(left) = r_null;

        if (!r_null) {
            CppType* l_val = reinterpret_cast<CppType*>(left + 1);
            const CppType* r_val = reinterpret_cast<const CppType*>(right + 1);
            *l_val = *r_val;
        }
    }
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_REPLACE, OLAP_FIELD_TYPE_LARGEINT> : public BaseAggregateFuncs {
    static void update(char* left, const char* right, Arena* arena) {
        typedef typename FieldTypeTraits<OLAP_FIELD_TYPE_LARGEINT>::CppType CppType;
        bool r_null = *reinterpret_cast<const bool*>(right);
        *reinterpret_cast<bool*>(left) = r_null;

        if (!r_null) {
            memcpy(left + 1, right + 1, sizeof(CppType));
        }
    }
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_REPLACE, OLAP_FIELD_TYPE_CHAR> : public BaseAggregateFuncs {
    static void update(char* left, const char* right, Arena* arena) {
        bool l_null = *reinterpret_cast<bool*>(left);
        bool r_null = *reinterpret_cast<const bool*>(right);
        *reinterpret_cast<bool*>(left) = r_null;
        if (!r_null) {
            Slice* l_slice = reinterpret_cast<Slice*>(left + 1);
            const Slice* r_slice = reinterpret_cast<const Slice*>(right + 1);
            if (arena == nullptr || (!l_null && l_slice->size >= r_slice->size)) {
                memory_copy(l_slice->data, r_slice->data, r_slice->size);
                l_slice->size = r_slice->size;
            } else {
                l_slice->data = arena->Allocate(r_slice->size);
                memory_copy(l_slice->data, r_slice->data, r_slice->size);
                l_slice->size = r_slice->size;
            }
        }
    }
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_REPLACE, OLAP_FIELD_TYPE_VARCHAR>
    : public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_REPLACE, OLAP_FIELD_TYPE_CHAR> {
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_HLL_UNION, OLAP_FIELD_TYPE_HLL> : public BaseAggregateFuncs {
    static void update(char* left, const char* right, Arena* arena) {
        Slice* l_slice = reinterpret_cast<Slice*>(left + 1);
        size_t hll_ptr = *(size_t*)(l_slice->data - sizeof(HllContext*));
        HllContext* context = (reinterpret_cast<HllContext*>(hll_ptr));
        HllSetHelper::fill_set(right + 1, context);
    }
    static void finalize(char* data, Arena* arena) {
        Slice* slice = reinterpret_cast<Slice*>(data);
        size_t hll_ptr = *(size_t*)(slice->data - sizeof(HllContext*));
        HllContext* context = (reinterpret_cast<HllContext*>(hll_ptr));
        std::map<int, uint8_t> index_to_value;
        if (context->has_sparse_or_full ||
                context->hash64_set->size() > HLL_EXPLICLIT_INT64_NUM) {
            HllSetHelper::set_max_register(context->registers, HLL_REGISTERS_COUNT,
                                           *(context->hash64_set));
            for (int i = 0; i < HLL_REGISTERS_COUNT; i++) {
                if (context->registers[i] != 0) {
                    index_to_value[i] = context->registers[i];
                }
            }
        }
        int sparse_set_len = index_to_value.size() *
            (sizeof(HllSetResolver::SparseIndexType)
             + sizeof(HllSetResolver::SparseValueType))
            + sizeof(HllSetResolver::SparseLengthValueType);
        int result_len = 0;

        if (sparse_set_len >= HLL_COLUMN_DEFAULT_LEN) {
            // full set
            HllSetHelper::set_full(slice->data, context->registers,
                                   HLL_REGISTERS_COUNT, result_len);
        } else if (index_to_value.size() > 0) {
            // sparse set
            HllSetHelper::set_sparse(slice->data, index_to_value, result_len);
        } else if (context->hash64_set->size() > 0) {
            // expliclit set
            HllSetHelper::set_explicit(slice->data, *(context->hash64_set), result_len);
        }

        slice->size = result_len & 0xffff;

        delete context->hash64_set;
    }
};

template<FieldAggregationMethod aggMethod, FieldType fieldType>
struct AggregateTraits : public AggregateFuncTraits<aggMethod, fieldType> {
    static const FieldAggregationMethod agg_method = aggMethod;
    static const FieldType type = fieldType;
};

const AggregateInfo* get_aggregate_info(const FieldAggregationMethod agg_method,
                                        const FieldType field_type);
} // namespace doris
