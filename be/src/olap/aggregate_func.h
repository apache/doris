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

#include "common/object_pool.h"
#include "olap/hll.h"
#include "olap/row_cursor_cell.h"
#include "olap/types.h"
#include "runtime/datetime_value.h"
#include "runtime/decimalv2_value.h"
#include "runtime/mem_pool.h"
#include "runtime/string_value.h"
#include "util/bitmap_value.h"
#include "util/quantile_state.h"

namespace doris {

using AggInitFunc = void (*)(RowCursorCell* dst, const char* src, bool src_null, MemPool* mem_pool,
                             ObjectPool* agg_pool);
using AggUpdateFunc = void (*)(RowCursorCell* dst, const RowCursorCell& src, MemPool* mem_pool);
using AggFinalizeFunc = void (*)(RowCursorCell* src, MemPool* mem_pool);

// This class contains information about aggregate operation.
class AggregateInfo {
public:
    // todo(kks): Unify this AggregateInfo::init method and Field::agg_init method

    // Init function will initialize aggregation execute environment in dst with source
    // and convert the source raw data to storage aggregate format
    //
    // Memory Note: For plain memory can be allocated from *mem_pool, whose lifetime
    // will last util finalize function is called. Memory allocated from heap should
    // be freed in finalize function to avoid memory leak.
    void init(RowCursorCell* dst, const char* src, bool src_null, MemPool* mem_pool,
              ObjectPool* agg_pool) const {
        _init_fn(dst, src, src_null, mem_pool, agg_pool);
    }

    // Update aggregated intermediate data. Data stored in engine is aggregated.
    // Actually do the aggregate operation. dst is the context which is initialized
    // by init function, src is the current value which is to be aggregated.
    // For example: For sum, dst is the current sum, and src is the next value which
    // will be added to sum.

    // Memory Note: Same with init function.
    void update(RowCursorCell* dst, const RowCursorCell& src, MemPool* mem_pool) const {
        _update_fn(dst, src, mem_pool);
    }

    // Finalize function convert intermediate context into final format. For example:
    // For HLL type, finalize function will serialize the aggregate context into a slice.
    // For input src points to the context, and when function is finished, result will be
    // saved in src.
    //
    // Memory Note: All heap memory allocated in init and update function should be freed
    // before this function return. Memory allocated from *mem_pool will be still available
    // and will be freed by client.
    void finalize(RowCursorCell* src, MemPool* mem_pool) const { _finalize_fn(src, mem_pool); }

    FieldAggregationMethod agg_method() const { return _agg_method; }

private:
    void (*_init_fn)(RowCursorCell* dst, const char* src, bool src_null, MemPool* mem_pool,
                     ObjectPool* agg_pool);
    void (*_update_fn)(RowCursorCell* dst, const RowCursorCell& src, MemPool* mem_pool);
    void (*_finalize_fn)(RowCursorCell* src, MemPool* mem_pool);

    friend class AggregateFuncResolver;

    template <typename Traits>
    AggregateInfo(const Traits& traits);

    FieldAggregationMethod _agg_method;
};

template <FieldType field_type, FieldType sub_type = OLAP_FIELD_TYPE_NONE>
struct BaseAggregateFuncs {
    static void init(RowCursorCell* dst, const char* src, bool src_null, MemPool* mem_pool,
                     ObjectPool* agg_pool) {
        dst->set_is_null(src_null);
        if (src_null) {
            return;
        }
        const auto* type_info = get_scalar_type_info<field_type>();
        type_info->deep_copy(dst->mutable_cell_ptr(), src, mem_pool);
    }

    // Default update do nothing.
    static void update(RowCursorCell* dst, const RowCursorCell& src, MemPool* mem_pool) {}

    // Default finalize do nothing.
    static void finalize(RowCursorCell* src, MemPool* mem_pool) {}
};

template <FieldType sub_type>
struct BaseAggregateFuncs<OLAP_FIELD_TYPE_ARRAY, sub_type> {
    static void init(RowCursorCell* dst, const char* src, bool src_null, MemPool* mem_pool,
                     ObjectPool* agg_pool) {
        dst->set_is_null(src_null);
        if (src_null) {
            return;
        }
        // nested array type is unsupported for base aggregate function now
        if (sub_type != OLAP_FIELD_TYPE_ARRAY) {
            const auto* type_info = get_collection_type_info<sub_type>();
            type_info->deep_copy(dst->mutable_cell_ptr(), src, mem_pool);
        }
    }

    // Default update do nothing.
    static void update(RowCursorCell* dst, const RowCursorCell& src, MemPool* mem_pool) {}

    // Default finalize do nothing.
    static void finalize(RowCursorCell* src, MemPool* mem_pool) {}
};

template <FieldAggregationMethod agg_method, FieldType field_type,
          FieldType sub_type = OLAP_FIELD_TYPE_NONE>
struct AggregateFuncTraits : public BaseAggregateFuncs<field_type, sub_type> {};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_DECIMAL>
        : public BaseAggregateFuncs<OLAP_FIELD_TYPE_DECIMAL> {
    static void init(RowCursorCell* dst, const char* src, bool src_null, MemPool* mem_pool,
                     ObjectPool* agg_pool) {
        dst->set_is_null(src_null);
        if (src_null) {
            return;
        }

        auto* decimal_value = reinterpret_cast<const DecimalV2Value*>(src);
        auto* storage_decimal_value = reinterpret_cast<decimal12_t*>(dst->mutable_cell_ptr());
        storage_decimal_value->integer = decimal_value->int_value();
        storage_decimal_value->fraction = decimal_value->frac_value();
    }
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_DATETIME>
        : public BaseAggregateFuncs<OLAP_FIELD_TYPE_DECIMAL> {
    static void init(RowCursorCell* dst, const char* src, bool src_null, MemPool* mem_pool,
                     ObjectPool* agg_pool) {
        dst->set_is_null(src_null);
        if (src_null) {
            return;
        }

        auto* datetime_value = reinterpret_cast<const DateTimeValue*>(src);
        auto* storage_datetime_value = reinterpret_cast<uint64_t*>(dst->mutable_cell_ptr());
        *storage_datetime_value = datetime_value->to_olap_datetime();
    }
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_DATE>
        : public BaseAggregateFuncs<OLAP_FIELD_TYPE_DECIMAL> {
    static void init(RowCursorCell* dst, const char* src, bool src_null, MemPool* mem_pool,
                     ObjectPool* agg_pool) {
        dst->set_is_null(src_null);
        if (src_null) {
            return;
        }

        auto* date_value = reinterpret_cast<const DateTimeValue*>(src);
        auto* storage_date_value = reinterpret_cast<uint24_t*>(dst->mutable_cell_ptr());
        *storage_date_value = static_cast<int64_t>(date_value->to_olap_date());
    }
};

template <FieldType field_type>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MIN, field_type>
        : public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_NONE, field_type> {
    typedef typename FieldTypeTraits<field_type>::CppType CppType;

    static void update(RowCursorCell* dst, const RowCursorCell& src, MemPool* mem_pool) {
        bool src_null = src.is_null();
        // ignore null value
        if (src_null) return;

        bool dst_null = dst->is_null();
        CppType* dst_val = reinterpret_cast<CppType*>(dst->mutable_cell_ptr());
        const CppType* src_val = reinterpret_cast<const CppType*>(src.cell_ptr());
        if (dst_null || *src_val < *dst_val) {
            dst->set_is_null(false);
            *dst_val = *src_val;
        }
    }
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MIN, OLAP_FIELD_TYPE_LARGEINT>
        : public BaseAggregateFuncs<OLAP_FIELD_TYPE_LARGEINT> {
    typedef typename FieldTypeTraits<OLAP_FIELD_TYPE_LARGEINT>::CppType CppType;

    static void update(RowCursorCell* dst, const RowCursorCell& src, MemPool* mem_pool) {
        bool src_null = src.is_null();
        // ignore null value
        if (src_null) return;

        bool dst_null = dst->is_null();
        if (dst_null) {
            dst->set_is_null(false);
            memcpy(dst->mutable_cell_ptr(), src.cell_ptr(), sizeof(CppType));
            return;
        }

        CppType dst_val, src_val;
        memcpy(&dst_val, dst->cell_ptr(), sizeof(CppType));
        memcpy(&src_val, src.cell_ptr(), sizeof(CppType));
        if (src_val < dst_val) {
            dst->set_is_null(false);
            memcpy(dst->mutable_cell_ptr(), src.cell_ptr(), sizeof(CppType));
        }
    }
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MIN, OLAP_FIELD_TYPE_VARCHAR>
        : public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_VARCHAR> {
    static void update(RowCursorCell* dst, const RowCursorCell& src, MemPool* mem_pool) {
        bool src_null = src.is_null();
        // ignore null value
        if (src_null) return;

        bool dst_null = dst->is_null();

        Slice* dst_slice = reinterpret_cast<Slice*>(dst->mutable_cell_ptr());
        const Slice* src_slice = reinterpret_cast<const Slice*>(src.cell_ptr());
        if (dst_null || src_slice->compare(*dst_slice) < 0) {
            if (mem_pool == nullptr || (!dst_null && dst_slice->size >= src_slice->size)) {
                memory_copy(dst_slice->data, src_slice->data, src_slice->size);
                dst_slice->size = src_slice->size;
            } else {
                dst_slice->data = (char*)mem_pool->allocate(src_slice->size);
                memory_copy(dst_slice->data, src_slice->data, src_slice->size);
                dst_slice->size = src_slice->size;
            }
            dst->set_is_null(false);
        }
    }
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MIN, OLAP_FIELD_TYPE_STRING>
        : public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_VARCHAR> {};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MIN, OLAP_FIELD_TYPE_CHAR>
        : public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MIN, OLAP_FIELD_TYPE_VARCHAR> {};

template <FieldType field_type>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MAX, field_type>
        : public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_NONE, field_type> {
    typedef typename FieldTypeTraits<field_type>::CppType CppType;

    static void update(RowCursorCell* dst, const RowCursorCell& src, MemPool* mem_pool) {
        bool src_null = src.is_null();
        // ignore null value
        if (src_null) return;

        bool dst_null = dst->is_null();
        CppType* dst_val = reinterpret_cast<CppType*>(dst->mutable_cell_ptr());
        const CppType* src_val = reinterpret_cast<const CppType*>(src.cell_ptr());
        if (dst_null || *src_val > *dst_val) {
            dst->set_is_null(false);
            *dst_val = *src_val;
        }
    }
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MAX, OLAP_FIELD_TYPE_LARGEINT>
        : public BaseAggregateFuncs<OLAP_FIELD_TYPE_LARGEINT> {
    typedef typename FieldTypeTraits<OLAP_FIELD_TYPE_LARGEINT>::CppType CppType;

    static void update(RowCursorCell* dst, const RowCursorCell& src, MemPool* mem_pool) {
        bool src_null = src.is_null();
        // ignore null value
        if (src_null) return;

        bool dst_null = dst->is_null();
        CppType dst_val, src_val;
        memcpy(&dst_val, dst->cell_ptr(), sizeof(CppType));
        memcpy(&src_val, src.cell_ptr(), sizeof(CppType));
        if (dst_null || src_val > dst_val) {
            dst->set_is_null(false);
            memcpy(dst->mutable_cell_ptr(), src.cell_ptr(), sizeof(CppType));
        }
    }
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MAX, OLAP_FIELD_TYPE_VARCHAR>
        : public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_VARCHAR> {
    static void update(RowCursorCell* dst, const RowCursorCell& src, MemPool* mem_pool) {
        bool src_null = src.is_null();
        // ignore null value
        if (src_null) return;

        bool dst_null = dst->is_null();

        Slice* dst_slice = reinterpret_cast<Slice*>(dst->mutable_cell_ptr());
        const Slice* src_slice = reinterpret_cast<const Slice*>(src.cell_ptr());
        if (dst_null || src_slice->compare(*dst_slice) > 0) {
            if (mem_pool == nullptr || (!dst_null && dst_slice->size >= src_slice->size)) {
                memory_copy(dst_slice->data, src_slice->data, src_slice->size);
                dst_slice->size = src_slice->size;
            } else {
                dst_slice->data = (char*)mem_pool->allocate(src_slice->size);
                memory_copy(dst_slice->data, src_slice->data, src_slice->size);
                dst_slice->size = src_slice->size;
            }
            dst->set_is_null(false);
        }
    }
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MAX, OLAP_FIELD_TYPE_CHAR>
        : public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MAX, OLAP_FIELD_TYPE_VARCHAR> {};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MAX, OLAP_FIELD_TYPE_STRING>
        : public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MAX, OLAP_FIELD_TYPE_VARCHAR> {};

template <FieldType field_type>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_SUM, field_type>
        : public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_NONE, field_type> {
    typedef typename FieldTypeTraits<field_type>::CppType CppType;

    static void update(RowCursorCell* dst, const RowCursorCell& src, MemPool* mem_pool) {
        bool src_null = src.is_null();
        if (src_null) {
            return;
        }

        CppType* dst_val = reinterpret_cast<CppType*>(dst->mutable_cell_ptr());
        bool dst_null = dst->is_null();
        if (dst_null) {
            dst->set_is_null(false);
            *dst_val = *reinterpret_cast<const CppType*>(src.cell_ptr());
        } else {
            *dst_val += *reinterpret_cast<const CppType*>(src.cell_ptr());
        }
    }
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_SUM, OLAP_FIELD_TYPE_LARGEINT>
        : public BaseAggregateFuncs<OLAP_FIELD_TYPE_LARGEINT> {
    typedef typename FieldTypeTraits<OLAP_FIELD_TYPE_LARGEINT>::CppType CppType;

    static void update(RowCursorCell* dst, const RowCursorCell& src, MemPool* mem_pool) {
        bool src_null = src.is_null();
        if (src_null) {
            return;
        }

        bool dst_null = dst->is_null();
        if (dst_null) {
            dst->set_is_null(false);
            memcpy(dst->mutable_cell_ptr(), src.cell_ptr(), sizeof(CppType));
        } else {
            CppType dst_val, src_val;
            memcpy(&dst_val, dst->cell_ptr(), sizeof(CppType));
            memcpy(&src_val, src.cell_ptr(), sizeof(CppType));
            dst_val += src_val;
            memcpy(dst->mutable_cell_ptr(), &dst_val, sizeof(CppType));
        }
    }
};

template <FieldType field_type>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_REPLACE, field_type>
        : public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_NONE, field_type> {
    typedef typename FieldTypeTraits<field_type>::CppType CppType;

    static void update(RowCursorCell* dst, const RowCursorCell& src, MemPool* mem_pool) {
        bool src_null = src.is_null();
        dst->set_is_null(src_null);
        if (!src_null) {
            memcpy(dst->mutable_cell_ptr(), src.cell_ptr(), sizeof(CppType));
        }
    }
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_REPLACE, OLAP_FIELD_TYPE_VARCHAR>
        : public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_VARCHAR> {
    static void update(RowCursorCell* dst, const RowCursorCell& src, MemPool* mem_pool) {
        bool dst_null = dst->is_null();
        bool src_null = src.is_null();
        dst->set_is_null(src_null);
        if (src_null) {
            return;
        }

        Slice* dst_slice = reinterpret_cast<Slice*>(dst->mutable_cell_ptr());
        const Slice* src_slice = reinterpret_cast<const Slice*>(src.cell_ptr());
        if (mem_pool == nullptr || (!dst_null && dst_slice->size >= src_slice->size)) {
            memory_copy(dst_slice->data, src_slice->data, src_slice->size);
            dst_slice->size = src_slice->size;
        } else {
            dst_slice->data = (char*)mem_pool->allocate(src_slice->size);
            memory_copy(dst_slice->data, src_slice->data, src_slice->size);
            dst_slice->size = src_slice->size;
        }
    }
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_REPLACE, OLAP_FIELD_TYPE_CHAR>
        : public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_REPLACE, OLAP_FIELD_TYPE_VARCHAR> {};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_REPLACE, OLAP_FIELD_TYPE_STRING>
        : public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_REPLACE, OLAP_FIELD_TYPE_VARCHAR> {};

// REPLACE_IF_NOT_NULL

template <FieldType field_type>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL, field_type>
        : public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_NONE, field_type> {
    typedef typename FieldTypeTraits<field_type>::CppType CppType;

    static void update(RowCursorCell* dst, const RowCursorCell& src, MemPool* mem_pool) {
        bool src_null = src.is_null();
        if (src_null) {
            // Ignore it if src is nullptr
            return;
        }

        dst->set_is_null(false);
        memcpy(dst->mutable_cell_ptr(), src.cell_ptr(), sizeof(CppType));
    }
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL, OLAP_FIELD_TYPE_VARCHAR>
        : public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_VARCHAR> {
    static void update(RowCursorCell* dst, const RowCursorCell& src, MemPool* mem_pool) {
        bool src_null = src.is_null();
        if (src_null) {
            // Ignore it if src is nullptr
            return;
        }

        bool dst_null = dst->is_null();
        dst->set_is_null(false);
        Slice* dst_slice = reinterpret_cast<Slice*>(dst->mutable_cell_ptr());
        const Slice* src_slice = reinterpret_cast<const Slice*>(src.cell_ptr());
        if (mem_pool == nullptr || (!dst_null && dst_slice->size >= src_slice->size)) {
            memory_copy(dst_slice->data, src_slice->data, src_slice->size);
            dst_slice->size = src_slice->size;
        } else {
            dst_slice->data = (char*)mem_pool->allocate(src_slice->size);
            memory_copy(dst_slice->data, src_slice->data, src_slice->size);
            dst_slice->size = src_slice->size;
        }
    }
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL, OLAP_FIELD_TYPE_CHAR>
        : public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL,
                                     OLAP_FIELD_TYPE_VARCHAR> {};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL, OLAP_FIELD_TYPE_STRING>
        : public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL,
                                     OLAP_FIELD_TYPE_VARCHAR> {};

// when data load, after hll_hash function, hll_union column won't be null
// so when init, update hll, the src is not null
template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_HLL_UNION, OLAP_FIELD_TYPE_HLL> {
    static void init(RowCursorCell* dst, const char* src, bool src_null, MemPool* mem_pool,
                     ObjectPool* agg_pool) {
        DCHECK_EQ(src_null, false);
        dst->set_not_null();

        auto* src_slice = reinterpret_cast<const Slice*>(src);
        auto* dst_slice = reinterpret_cast<Slice*>(dst->mutable_cell_ptr());

        // we use zero size represent this slice is a agg object
        dst_slice->size = 0;
        auto* hll = new HyperLogLog(*src_slice);

        dst_slice->data = reinterpret_cast<char*>(hll);

        agg_pool->add(hll);
    }

    static void update(RowCursorCell* dst, const RowCursorCell& src, MemPool* mem_pool) {
        DCHECK_EQ(src.is_null(), false);

        auto* dst_slice = reinterpret_cast<Slice*>(dst->mutable_cell_ptr());
        auto* src_slice = reinterpret_cast<const Slice*>(src.cell_ptr());
        auto* dst_hll = reinterpret_cast<HyperLogLog*>(dst_slice->data);

        // fixme(kks): trick here, need improve
        if (mem_pool == nullptr) { // for query
            HyperLogLog src_hll(*src_slice);
            dst_hll->merge(src_hll);
        } else { // for stream load
            auto* src_hll = reinterpret_cast<HyperLogLog*>(src_slice->data);
            dst_hll->merge(*src_hll);
        }
    }

    // The HLL object memory will be released by ObjectPool
    static void finalize(RowCursorCell* src, MemPool* mem_pool) {
        auto* slice = reinterpret_cast<Slice*>(src->mutable_cell_ptr());
        auto* hll = reinterpret_cast<HyperLogLog*>(slice->data);

        slice->data = (char*)mem_pool->allocate(hll->max_serialized_size());
        slice->size = hll->serialize((uint8_t*)slice->data);
        hll->clear();
    }
};
// when data load, after bitmap_init function, bitmap_union column won't be null
// so when init, update bitmap, the src is not null
template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_BITMAP_UNION, OLAP_FIELD_TYPE_OBJECT> {
    static void init(RowCursorCell* dst, const char* src, bool src_null, MemPool* mem_pool,
                     ObjectPool* agg_pool) {
        DCHECK_EQ(src_null, false);
        dst->set_not_null();
        auto* src_slice = reinterpret_cast<const Slice*>(src);
        auto* dst_slice = reinterpret_cast<Slice*>(dst->mutable_cell_ptr());

        // we use zero size represent this slice is a agg object
        dst_slice->size = 0;
        auto bitmap = new BitmapValue(src_slice->data);
        dst_slice->data = (char*)bitmap;

        agg_pool->add(bitmap);
    }

    static void update(RowCursorCell* dst, const RowCursorCell& src, MemPool* mem_pool) {
        DCHECK_EQ(src.is_null(), false);

        auto dst_slice = reinterpret_cast<Slice*>(dst->mutable_cell_ptr());
        DCHECK_EQ(dst_slice->size, 0);
        auto dst_bitmap = reinterpret_cast<BitmapValue*>(dst_slice->data);
        auto src_slice = reinterpret_cast<const Slice*>(src.cell_ptr());

        // fixme(kks): trick here, need improve
        if (mem_pool == nullptr) { // for query
            (*dst_bitmap) |= BitmapValue(src_slice->data);
        } else { // for stream load
            DCHECK_EQ(src_slice->size, 0);
            (*dst_bitmap) |= *reinterpret_cast<BitmapValue*>(src_slice->data);
        }
    }

    // The BitmapValue object memory will be released by ObjectPool
    static void finalize(RowCursorCell* src, MemPool* mem_pool) {
        auto slice = reinterpret_cast<Slice*>(src->mutable_cell_ptr());
        DCHECK_EQ(slice->size, 0);
        auto bitmap = reinterpret_cast<BitmapValue*>(slice->data);

        slice->size = bitmap->getSizeInBytes();
        slice->data = (char*)mem_pool->allocate(slice->size);
        bitmap->write(slice->data);
    }
};

// for backward compatibility
template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_BITMAP_UNION, OLAP_FIELD_TYPE_VARCHAR>
        : public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_BITMAP_UNION, OLAP_FIELD_TYPE_OBJECT> {
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_BITMAP_UNION, OLAP_FIELD_TYPE_STRING>
        : public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_BITMAP_UNION, OLAP_FIELD_TYPE_OBJECT> {
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_QUANTILE_UNION, OLAP_FIELD_TYPE_QUANTILE_STATE> {
    static void init(RowCursorCell* dst, const char* src, bool src_null, MemPool* mem_pool,
                     ObjectPool* agg_pool) {
        DCHECK_EQ(src_null, false);
        dst->set_not_null();

        auto* src_slice = reinterpret_cast<const Slice*>(src);
        auto* dst_slice = reinterpret_cast<Slice*>(dst->mutable_cell_ptr());

        // we use zero size represent this slice is a agg object
        dst_slice->size = 0;
        auto* dst_quantile_state = new QuantileState<double>(*src_slice);

        dst_slice->data = reinterpret_cast<char*>(dst_quantile_state);

        agg_pool->add(dst_quantile_state);
    }

    static void update(RowCursorCell* dst, const RowCursorCell& src, MemPool* mem_pool) {
        DCHECK_EQ(src.is_null(), false);

        auto* dst_slice = reinterpret_cast<Slice*>(dst->mutable_cell_ptr());
        auto* src_slice = reinterpret_cast<const Slice*>(src.cell_ptr());
        auto* dst_quantile_state = reinterpret_cast<QuantileState<double>*>(dst_slice->data);

        if (mem_pool == nullptr) { // for query
            QuantileState<double> src_state(*src_slice);
            dst_quantile_state->merge(src_state);
        } else { // for stream load
            auto* src_state = reinterpret_cast<QuantileState<double>*>(src_slice->data);
            dst_quantile_state->merge(*src_state);
        }
    }

    // The quantile_state object memory will be released by ObjectPool
    static void finalize(RowCursorCell* src, MemPool* mem_pool) {
        auto* slice = reinterpret_cast<Slice*>(src->mutable_cell_ptr());
        auto* quantile_state = reinterpret_cast<QuantileState<double>*>(slice->data);

        slice->data = (char*)mem_pool->allocate(quantile_state->get_serialized_size());
        slice->size = quantile_state->serialize((uint8_t*)slice->data);
        quantile_state->clear();
    }
};
template <FieldAggregationMethod aggMethod, FieldType fieldType,
          FieldType subType = OLAP_FIELD_TYPE_NONE>
struct AggregateTraits : public AggregateFuncTraits<aggMethod, fieldType, subType> {
    static const FieldAggregationMethod agg_method = aggMethod;
    static const FieldType type = fieldType;
    static const FieldType sub_type = subType;
};

const AggregateInfo* get_aggregate_info(const FieldAggregationMethod agg_method,
                                        const FieldType field_type,
                                        const FieldType sub_type = OLAP_FIELD_TYPE_NONE);
} // namespace doris
