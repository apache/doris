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
#include "olap/row_cursor_cell.h"
#include "runtime/datetime_value.h"
#include "runtime/decimalv2_value.h"
#include "runtime/string_value.h"
#include "util/arena.h"
#include "util/bitmap.h"

namespace doris {

using AggInitFunc = void (*)(RowCursorCell* dst, const char* src, bool src_null, Arena* arena);
using AggUpdateFunc = void (*)(RowCursorCell* dst, const RowCursorCell& src, Arena* arena);
using AggFinalizeFunc = void (*)(RowCursorCell* src, Arena* arena);

// This class contains information about aggregate operation.
class AggregateInfo {
public:
    // todo(kks): Unify this AggregateInfo::init method and Field::agg_init method

    // Init function will initialize aggregation execute environment in dst with source
    // and convert the source raw data to storage aggregate format
    //
    // Memory Note: For plain memory can be allocated from arena, whose lifetime
    // will last util finalize function is called. Memory allocated from heap should
    // be freed in finalize functioin to avoid memory leak.
    inline void init(RowCursorCell* dst, const char* src, bool src_null, Arena* arena) const {
        _init_fn(dst, src, src_null, arena);
    }

    // Update aggregated intermediate data. Data stored in engine is aggregated.
    // Actually do the aggregate operation. dst is the context which is initialized
    // by init function, src is the current value which is to be aggregated.
    // For example: For sum, dst is the current sum, and src is the next value which
    // will be added to sum.

    // Memory Note: Same with init function.
    inline void update(RowCursorCell* dst, const RowCursorCell& src, Arena* arena) const {
        _update_fn(dst, src, arena);
    }

    // Finalize function convert intermediate context into final format. For example:
    // For HLL type, finalize function will serialize the aggregate context into a slice.
    // For input src points to the context, and when function is finished, result will be
    // saved in src.
    //
    // Memory Note: All heap memory allocated in init and update function should be freed
    // before this function return. Memory allocated from arena will be still available
    // and will be freed by client.
    inline void finalize(RowCursorCell* src, Arena* arena) const {
        _finalize_fn(src, arena);
    }

    FieldAggregationMethod agg_method() const { return _agg_method; }

private:
    void (*_init_fn)(RowCursorCell* dst, const char* src, bool src_null, Arena* arena);
    void (*_update_fn)(RowCursorCell* dst, const RowCursorCell& src, Arena* arena);
    void (*_finalize_fn)(RowCursorCell* src, Arena* arena);

    friend class AggregateFuncResolver;

    template<typename Traits>
    AggregateInfo(const Traits& traits);

    FieldAggregationMethod _agg_method;
};

template<FieldType field_type>
struct BaseAggregateFuncs {
    static void init(RowCursorCell* dst, const char* src, bool src_null, Arena* arena) {
        dst->set_is_null(src_null);
        if (src_null) {
            return;
        }

        const TypeInfo* _type_info = get_type_info(field_type);
        _type_info->deep_copy_with_arena(dst->mutable_cell_ptr(), src, arena);
    }

    // Default update do nothing.
    static void update(RowCursorCell* dst, const RowCursorCell& src, Arena* arena) {
    }

    // Default finalize do nothing.
    static void finalize(RowCursorCell* src, Arena* arena) {
    }
};

template<FieldAggregationMethod agg_method, FieldType field_type>
struct AggregateFuncTraits : public BaseAggregateFuncs<field_type> {
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_DECIMAL> :
        public BaseAggregateFuncs<OLAP_FIELD_TYPE_DECIMAL>  {
    static void init(RowCursorCell* dst, const char* src, bool src_null, Arena* arena) {
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
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_DATETIME> :
        public BaseAggregateFuncs<OLAP_FIELD_TYPE_DECIMAL> {
    static void init(RowCursorCell* dst, const char* src, bool src_null, Arena* arena) {
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
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_DATE> :
        public BaseAggregateFuncs<OLAP_FIELD_TYPE_DECIMAL> {
    static void init(RowCursorCell* dst, const char* src, bool src_null, Arena* arena) {
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
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MIN, field_type> :
        public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_NONE, field_type> {
    typedef typename FieldTypeTraits<field_type>::CppType CppType;

    static void update(RowCursorCell* dst, const RowCursorCell& src, Arena* arena) {
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
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MIN, OLAP_FIELD_TYPE_LARGEINT> :
        public BaseAggregateFuncs<OLAP_FIELD_TYPE_LARGEINT> {
    typedef typename FieldTypeTraits<OLAP_FIELD_TYPE_LARGEINT>::CppType CppType;

    static void update(RowCursorCell* dst, const RowCursorCell& src, Arena* arena) {
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
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MIN, OLAP_FIELD_TYPE_VARCHAR> :
        public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_VARCHAR> {
    static void update(RowCursorCell* dst, const RowCursorCell& src, Arena* arena) {
        bool src_null = src.is_null();
        // ignore null value
        if (src_null) return;

        bool dst_null = dst->is_null();

        Slice* dst_slice = reinterpret_cast<Slice*>(dst->mutable_cell_ptr());
        const Slice* src_slice = reinterpret_cast<const Slice*>(src.cell_ptr());
        if (dst_null || src_slice->compare(*dst_slice) < 0) {
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
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MIN, OLAP_FIELD_TYPE_CHAR>
        : public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MIN, OLAP_FIELD_TYPE_VARCHAR> {
};

template <FieldType field_type>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MAX, field_type> :
        public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_NONE, field_type> {
    typedef typename FieldTypeTraits<field_type>::CppType CppType;

    static void update(RowCursorCell* dst, const RowCursorCell& src, Arena* arena) {
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
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MAX, OLAP_FIELD_TYPE_LARGEINT> :
        public BaseAggregateFuncs<OLAP_FIELD_TYPE_LARGEINT> {
    typedef typename FieldTypeTraits<OLAP_FIELD_TYPE_LARGEINT>::CppType CppType;

    static void update(RowCursorCell* dst, const RowCursorCell& src, Arena* arena) {
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
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MAX, OLAP_FIELD_TYPE_VARCHAR> :
        public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_VARCHAR> {
    static void update(RowCursorCell* dst, const RowCursorCell& src, Arena* arena) {
        bool src_null = src.is_null();
        // ignore null value
        if (src_null) return;

        bool dst_null = dst->is_null();

        Slice* dst_slice = reinterpret_cast<Slice*>(dst->mutable_cell_ptr());
        const Slice* src_slice = reinterpret_cast<const Slice*>(src.cell_ptr());
        if (dst_null || src_slice->compare(*dst_slice) > 0) {
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
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MAX, OLAP_FIELD_TYPE_CHAR>
        : public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_MAX, OLAP_FIELD_TYPE_VARCHAR> {
};

template <FieldType field_type>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_SUM, field_type> :
        public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_NONE, field_type>  {
    typedef typename FieldTypeTraits<field_type>::CppType CppType;

    static void update(RowCursorCell* dst, const RowCursorCell& src, Arena* arena) {
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
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_SUM, OLAP_FIELD_TYPE_LARGEINT> :
        public BaseAggregateFuncs<OLAP_FIELD_TYPE_LARGEINT> {
    typedef typename FieldTypeTraits<OLAP_FIELD_TYPE_LARGEINT>::CppType CppType;

    static void update(RowCursorCell* dst, const RowCursorCell& src, Arena* arena) {
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
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_REPLACE, field_type> :
        public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_NONE, field_type>  {
    typedef typename FieldTypeTraits<field_type>::CppType CppType;

    static void update(RowCursorCell* dst, const RowCursorCell& src, Arena* arena) {
        bool src_null = src.is_null();
        dst->set_is_null(src_null);
        if (!src_null) {
            memcpy(dst->mutable_cell_ptr(), src.cell_ptr(), sizeof(CppType));
        }
    }
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_REPLACE, OLAP_FIELD_TYPE_VARCHAR> :
        public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_VARCHAR> {
    static void update(RowCursorCell* dst, const RowCursorCell& src, Arena* arena) {
        bool dst_null = dst->is_null();
        bool src_null = src.is_null();
        dst->set_is_null(src_null);
        if (src_null) {
            return;
        }

        Slice* dst_slice = reinterpret_cast<Slice*>(dst->mutable_cell_ptr());
        const Slice* src_slice = reinterpret_cast<const Slice*>(src.cell_ptr());
        if (arena == nullptr || (!dst_null && dst_slice->size >= src_slice->size)) {
            memory_copy(dst_slice->data, src_slice->data, src_slice->size);
            dst_slice->size = src_slice->size;
        } else {
            dst_slice->data = arena->Allocate(src_slice->size);
            memory_copy(dst_slice->data, src_slice->data, src_slice->size);
            dst_slice->size = src_slice->size;
        }
    }
};

template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_REPLACE, OLAP_FIELD_TYPE_CHAR>
        : public AggregateFuncTraits<OLAP_FIELD_AGGREGATION_REPLACE, OLAP_FIELD_TYPE_VARCHAR> {
};

// when data load, after hll_hash fucntion, hll_union column won't be null
// so when init, update hll, the src is not null
template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_HLL_UNION, OLAP_FIELD_TYPE_HLL> {
    static void init(RowCursorCell* dst, const char* src, bool src_null, Arena* arena) {
        DCHECK_EQ(src_null, false);
        dst->set_not_null();

        auto* src_slice = reinterpret_cast<const Slice*>(src);
        auto* dst_slice = reinterpret_cast<Slice*>(dst->mutable_cell_ptr());

        dst_slice->size = sizeof(HyperLogLog);
        // use 'placement new' to allocate HyperLogLog on arena, so that we can control the memory usage.
        char* mem = arena->Allocate(dst_slice->size);
        if (src_slice->empty()) {
            dst_slice->data = (char*) new (mem) HyperLogLog();
        } else {
            dst_slice->data = (char*) new (mem) HyperLogLog(src_slice->data);
        }
    }

    static void update(RowCursorCell* dst, const RowCursorCell& src, Arena* arena) {
        DCHECK_EQ(src.is_null(), false);

        auto* dst_slice = reinterpret_cast<Slice*>(dst->mutable_cell_ptr());
        auto* src_slice = reinterpret_cast<const Slice*>(src.cell_ptr());
        auto* dst_hll = reinterpret_cast<HyperLogLog*>(dst_slice->data);

        // fixme(kks): trick here, need improve
        if (arena == nullptr) { // for query
            if (src_slice->empty()) {
                HyperLogLog src_hll = HyperLogLog();
                dst_hll->merge(src_hll);
            } else {
                HyperLogLog src_hll = HyperLogLog(src_slice->data);
                dst_hll->merge(src_hll);
            }
        } else {   // for stream load
            auto* src_hll = reinterpret_cast<HyperLogLog*>(src_slice->data);
            dst_hll->merge(*src_hll);
            // NOT use 'delete src_hll' because the memory is managed by arena
            src_hll->~HyperLogLog();
        }
    }

    static void finalize(RowCursorCell* src, Arena* arena) {
        auto *slice = reinterpret_cast<Slice*>(src->mutable_cell_ptr());
        auto *hll = reinterpret_cast<HyperLogLog*>(slice->data);

        slice->data = arena->Allocate(HLL_COLUMN_DEFAULT_LEN);
        slice->size = hll->serialize(slice->data);
        // NOT using 'delete hll' because the memory is managed by arena
        hll->~HyperLogLog();
    }
};
// when data load, after bitmap_init fucntion, bitmap_union column won't be null
// so when init, update bitmap, the src is not null
template <>
struct AggregateFuncTraits<OLAP_FIELD_AGGREGATION_BITMAP_UNION, OLAP_FIELD_TYPE_VARCHAR> {
    static void init(RowCursorCell* dst, const char* src, bool src_null, Arena* arena) {
        DCHECK_EQ(src_null, false);
        dst->set_not_null();
        auto* src_slice = reinterpret_cast<const Slice*>(src);
        auto* dst_slice = reinterpret_cast<Slice*>(dst->mutable_cell_ptr());

        dst_slice->size = sizeof(RoaringBitmap);
        dst_slice->data = (char*)new RoaringBitmap(src_slice->data);
    }

    static void update(RowCursorCell* dst, const RowCursorCell& src, Arena* arena) {
        DCHECK_EQ(src.is_null(), false);

        auto* dst_slice = reinterpret_cast<Slice*>(dst->mutable_cell_ptr());
        auto* src_slice = reinterpret_cast<const Slice*>(src.cell_ptr());
        auto* dst_bitmap = reinterpret_cast<RoaringBitmap*>(dst_slice->data);

        // fixme(kks): trick here, need improve
        if (arena == nullptr) { // for query
            RoaringBitmap src_bitmap = RoaringBitmap(src_slice->data);
            dst_bitmap->merge(src_bitmap);
        } else {   // for stream load
            auto* src_bitmap = reinterpret_cast<RoaringBitmap*>(src_slice->data);
            dst_bitmap->merge(*src_bitmap);

            delete src_bitmap;
        }
    }

    static void finalize(RowCursorCell* src, Arena *arena) {
        auto *slice = reinterpret_cast<Slice*>(src->mutable_cell_ptr());
        auto *bitmap = reinterpret_cast<RoaringBitmap*>(slice->data);

        slice->size = bitmap->size();
        slice->data = arena->Allocate(slice->size);
        bitmap->serialize(slice->data);

        delete bitmap;
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
