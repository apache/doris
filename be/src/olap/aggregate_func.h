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

using AggeInitFunc = void (*)(char* dst, Arena* arena);
using AggUpdateFunc = void (*)(char* dst, const char* src, Arena* arena);
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

    FieldAggregationMethod agg_method() const { return _agg_method; }

private:
    void (*_init_fn)(char* dst, Arena* arena);
    void (*_update_fn)(char* dst, const char* src, Arena* arena);
    void (*_merge_fn)(char* dst, const char* src, Arena* arena);
    void (*_finalize_fn)(char* dst, Arena* arena);

    friend class AggregateFuncResolver;

    template<typename Traits>
    AggregateInfo(const Traits& traits);

    FieldAggregationMethod _agg_method;
};

struct BaseAggregateFuncs {
    // Default init function will set to null
    static void init(char* dst, Arena* arena) {
        *reinterpret_cast<bool*>(dst) = true;
    }

    // Default update do nothing.
    static void update(char* dst, const char* src, Arena* arena) {
    }

    // For most aggregate method, its merge and update are same. If merge
    // is same with update, keep merge nullptr to avoid duplicate code.
    // AggregateInfo constructor will set merge function to update function
    // when merge is nullptr.
    AggUpdateFunc merge = nullptr;

    // Default finalize do nothing.
    static void finalize(char* src, Arena* arena) {
    }
};

template<FieldAggregationMethod agg_method, FieldType field_type>
struct AggregateFuncTraits : public BaseAggregateFuncs {
};

template <FieldType field_type>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MIN, field_type> : public BaseAggregateFuncs {
    typedef typename FieldTypeTraits<field_type>::CppType CppType;

    static void update(char* dst, const char* src, Arena* arena) {
        bool src_null = *reinterpret_cast<const bool*>(src);
        // ignore null value
        if (src_null) return;

        bool dst_null = *reinterpret_cast<bool*>(dst);
        CppType* dst_val = reinterpret_cast<CppType*>(dst + 1);
        const CppType* src_val = reinterpret_cast<const CppType*>(src + 1);
        if (dst_null || *src_val < *dst_val) {
            *reinterpret_cast<bool*>(dst) = false;
            *dst_val = *src_val;
        }
    }
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MIN, OLAP_FIELD_TYPE_LARGEINT> : public BaseAggregateFuncs {
    typedef typename FieldTypeTraits<OLAP_FIELD_TYPE_LARGEINT>::CppType CppType;

    static void update(char* dst, const char* src, Arena* arena) {
        bool src_null = *reinterpret_cast<const bool*>(src);
        // ignore null value
        if (src_null) return;

        bool dst_null = *reinterpret_cast<bool*>(dst);
        if (dst_null) {
            *reinterpret_cast<bool*>(dst) = false;
            memcpy(dst + 1, src + 1, sizeof(CppType));
            return;
        }

        CppType dst_val, src_val;
        memcpy(&dst_val, dst + 1, sizeof(CppType));
        memcpy(&src_val, src + 1, sizeof(CppType));
        if (src_val < dst_val) {
            *reinterpret_cast<bool*>(dst) = false;
            memcpy(dst + 1, src + 1, sizeof(CppType));
        }
    }
};

template <FieldType field_type>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MAX, field_type> : public BaseAggregateFuncs {
    typedef typename FieldTypeTraits<field_type>::CppType CppType;

    static void update(char* dst, const char* src, Arena* arena) {
        bool src_null = *reinterpret_cast<const bool*>(src);
        // ignore null value
        if (src_null) return;

        bool dst_null = *reinterpret_cast<bool*>(dst);
        CppType* dst_val = reinterpret_cast<CppType*>(dst + 1);
        const CppType* src_val = reinterpret_cast<const CppType*>(src + 1);
        if (dst_null || *src_val > *dst_val) {
            *reinterpret_cast<bool*>(dst) = false;
            *dst_val = *src_val;
        }
    }
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MAX, OLAP_FIELD_TYPE_LARGEINT> : public BaseAggregateFuncs {
    typedef typename FieldTypeTraits<OLAP_FIELD_TYPE_LARGEINT>::CppType CppType;

    static void update(char* dst, const char* src, Arena* arena) {
        bool src_null = *reinterpret_cast<const bool*>(src);
        // ignore null value
        if (src_null) return;

        bool dst_null = *reinterpret_cast<bool*>(dst);
        CppType dst_val, src_val;
        memcpy(&dst_val, dst + 1, sizeof(CppType));
        memcpy(&src_val, src + 1, sizeof(CppType));
        if (dst_null || src_val > dst_val) {
            *reinterpret_cast<bool*>(dst) = false;
            memcpy(dst + 1, src + 1, sizeof(CppType));
        }
    }
};

template <FieldType field_type>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_SUM, field_type> : public BaseAggregateFuncs {
    typedef typename FieldTypeTraits<field_type>::CppType CppType;

    static void update(char* dst, const char* src, Arena* arena) {
        bool src_null = *reinterpret_cast<const bool*>(src);
        if (src_null) {
            return;
        }

        CppType* dst_val = reinterpret_cast<CppType*>(dst + 1);
        bool dst_null = *reinterpret_cast<bool*>(dst);
        if (dst_null) {
            *reinterpret_cast<bool*>(dst) = false;
            *dst_val = *reinterpret_cast<const CppType*>(src + 1);
        } else {
            *dst_val += *reinterpret_cast<const CppType*>(src + 1);
        }
    }
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_SUM, OLAP_FIELD_TYPE_LARGEINT> : public BaseAggregateFuncs {
    typedef typename FieldTypeTraits<OLAP_FIELD_TYPE_LARGEINT>::CppType CppType;

    static void update(char* dst, const char* src, Arena* arena) {
        bool src_null = *reinterpret_cast<const bool*>(src);
        if (src_null) {
            return;
        }

        bool dst_null = *reinterpret_cast<bool*>(dst);
        if (dst_null) {
            *reinterpret_cast<bool*>(dst) = false;
            memcpy(dst + 1, src + 1, sizeof(CppType));
        } else {
            CppType dst_val, src_val;
            memcpy(&dst_val, dst + 1, sizeof(CppType));
            memcpy(&src_val, src + 1, sizeof(CppType));
            dst_val += src_val;
            memcpy(dst + 1, &dst_val, sizeof(CppType));
        }
    }
};

template <FieldType field_type>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_REPLACE, field_type> : public BaseAggregateFuncs {
    typedef typename FieldTypeTraits<field_type>::CppType CppType;

    static void update(char* dst, const char* src, Arena* arena) {
        bool src_null = *reinterpret_cast<const bool*>(src);
        *reinterpret_cast<bool*>(dst) = src_null;
        if (!src_null) {
            memcpy(dst + 1, src + 1, sizeof(CppType));
        }
    }
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_REPLACE, OLAP_FIELD_TYPE_CHAR> : public BaseAggregateFuncs {
    static void update(char* dst, const char* src, Arena* arena) {
        bool dst_null = *reinterpret_cast<bool*>(dst);
        bool src_null = *reinterpret_cast<const bool*>(src);
        *reinterpret_cast<bool*>(dst) = src_null;
        if (!src_null) {
            Slice* dst_slice = reinterpret_cast<Slice*>(dst + 1);
            const Slice* src_slice = reinterpret_cast<const Slice*>(src + 1);
            if (arena == nullptr || (!dst_null && dst_slice->size >= src_slice->size)) {
                memory_copy(dst_slice->data, src_slice->data, src_slice->size);
                dst_slice->size = src_slice->size;
            } else {
                dst_slice->data = arena->Allocate(src_slice->size);
                memory_copy(dst_slice->data, src_slice->data, src_slice->size);
                dst_slice->size = src_slice->size;
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
    static void init(char* dst, Arena* arena) {
        // TODO(zc): refactor HLL implementation
        *reinterpret_cast<bool*>(dst) = false;
        Slice* slice = reinterpret_cast<Slice*>(dst + 1);
        size_t hll_ptr = *(size_t*)(slice->data - sizeof(HllContext*));
        HllContext* context = (reinterpret_cast<HllContext*>(hll_ptr));
        HllSetHelper::init_context(context);
        context->has_value = true;
    }

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
