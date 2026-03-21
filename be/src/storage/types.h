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

#include <fmt/format.h>
#include <glog/logging.h>

#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/consts.h"
#include "common/status.h"
#include "core/arena.h"
#include "core/decimal12.h"
#include "core/extended_types.h"
#include "core/packed_int128.h"
#include "core/type_limit.h"
#include "core/uint24.h"
#include "core/value/ipv4_value.h"
#include "core/value/ipv6_value.h"
#include "core/value/map_value.h"
#include "core/value/struct_value.h"
#include "core/value/vdatetime_value.h"
#include "exprs/function/cast/cast_to_timestamptz.h"
#include "runtime/collection_value.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "util/slice.h"

namespace doris {
#include "common/compile_check_begin.h"

namespace segment_v2 {
class ColumnMetaPB;
}

class TabletColumn;

class TypeInfo;

using TypeInfoPtr = std::unique_ptr<const TypeInfo, void (*)(const TypeInfo*)>;

TypeInfoPtr create_static_type_info_ptr(const TypeInfo* type_info);
TypeInfoPtr create_dynamic_type_info_ptr(const TypeInfo* type_info);

class TypeInfo {
public:
    virtual ~TypeInfo() = default;
    virtual int cmp(const void* left, const void* right) const = 0;

    virtual void deep_copy(void* dest, const void* src, Arena& arena) const = 0;

    virtual void set_to_max(void* buf) const = 0;
    virtual void set_to_min(void* buf) const = 0;

    virtual size_t size() const = 0;

    virtual FieldType type() const = 0;
};

class ScalarTypeInfo : public TypeInfo {
public:
    int cmp(const void* left, const void* right) const override { return _cmp(left, right); }

    void deep_copy(void* dest, const void* src, Arena& arena) const override {
        _deep_copy(dest, src, arena);
    }

    void set_to_max(void* buf) const override { _set_to_max(buf); }
    void set_to_min(void* buf) const override { _set_to_min(buf); }
    size_t size() const override { return _size; }

    FieldType type() const override { return _field_type; }

    template <typename TypeTraitsClass>
    ScalarTypeInfo(TypeTraitsClass t)
            : _cmp(TypeTraitsClass::cmp),
              _deep_copy(TypeTraitsClass::deep_copy),
              _set_to_max(TypeTraitsClass::set_to_max),
              _set_to_min(TypeTraitsClass::set_to_min),
              _size(TypeTraitsClass::size),
              _field_type(TypeTraitsClass::type) {}

private:
    int (*_cmp)(const void* left, const void* right);

    void (*_deep_copy)(void* dest, const void* src, Arena& arena);

    void (*_set_to_max)(void* buf);
    void (*_set_to_min)(void* buf);

    const size_t _size;
    const FieldType _field_type;

    friend class ScalarTypeInfoResolver;
};

class ArrayTypeInfo : public TypeInfo {
public:
    explicit ArrayTypeInfo(TypeInfoPtr item_type_info)
            : _item_type_info(std::move(item_type_info)), _item_size(_item_type_info->size()) {}
    ~ArrayTypeInfo() override = default;

    int cmp(const void* left, const void* right) const override {
        auto l_value = reinterpret_cast<const CollectionValue*>(left);
        auto r_value = reinterpret_cast<const CollectionValue*>(right);
        size_t l_length = l_value->length();
        size_t r_length = r_value->length();
        size_t cur = 0;

        if (!l_value->has_null() && !r_value->has_null()) {
            while (cur < l_length && cur < r_length) {
                int result = _item_type_info->cmp((uint8_t*)(l_value->data()) + cur * _item_size,
                                                  (uint8_t*)(r_value->data()) + cur * _item_size);
                if (result != 0) {
                    return result;
                }
                ++cur;
            }
        } else {
            while (cur < l_length && cur < r_length) {
                if (l_value->is_null_at(cur)) {
                    if (!r_value->is_null_at(cur)) { // left is null & right is not null
                        return -1;
                    }
                } else if (r_value->is_null_at(cur)) { // left is not null & right is null
                    return 1;
                } else { // both are not null
                    int result =
                            _item_type_info->cmp((uint8_t*)(l_value->data()) + cur * _item_size,
                                                 (uint8_t*)(r_value->data()) + cur * _item_size);
                    if (result != 0) {
                        return result;
                    }
                }
                ++cur;
            }
        }

        if (l_length < r_length) {
            return -1;
        } else if (l_length > r_length) {
            return 1;
        } else {
            return 0;
        }
    }

    void deep_copy(void* dest, const void* src, Arena& arena) const override {
        auto dest_value = reinterpret_cast<CollectionValue*>(dest);
        auto src_value = reinterpret_cast<const CollectionValue*>(src);

        if (src_value->length() == 0) {
            new (dest_value) CollectionValue(src_value->length());
            return;
        }

        dest_value->set_length(src_value->length());

        size_t item_size = src_value->length() * _item_size;
        size_t nulls_size = src_value->has_null() ? src_value->length() : 0;
        dest_value->set_data(arena.alloc(item_size + nulls_size));
        dest_value->set_has_null(src_value->has_null());
        dest_value->set_null_signs(src_value->has_null()
                                           ? reinterpret_cast<bool*>(dest_value->mutable_data()) +
                                                     item_size
                                           : nullptr);

        // copy null_signs
        if (src_value->has_null()) {
            dest_value->copy_null_signs(src_value);
        }

        // copy item
        for (uint32_t i = 0; i < src_value->length(); ++i) {
            if (dest_value->is_null_at(i)) continue;
            _item_type_info->deep_copy((uint8_t*)(dest_value->mutable_data()) + i * _item_size,
                                       (uint8_t*)(src_value->data()) + i * _item_size, arena);
        }
    }

    void set_to_max(void* buf) const override {
        DCHECK(false) << "set_to_max of list is not implemented.";
    }

    void set_to_min(void* buf) const override {
        DCHECK(false) << "set_to_min of list is not implemented.";
    }

    size_t size() const override { return sizeof(CollectionValue); }

    FieldType type() const override { return FieldType::OLAP_FIELD_TYPE_ARRAY; }

    inline const TypeInfo* item_type_info() const { return _item_type_info.get(); }

private:
    TypeInfoPtr _item_type_info;
    const size_t _item_size;
};
///====================== MapType Info ==========================///
class MapTypeInfo : public TypeInfo {
public:
    explicit MapTypeInfo(TypeInfoPtr key_type_info, TypeInfoPtr value_type_info)
            : _key_type_info(std::move(key_type_info)),
              _value_type_info(std::move(value_type_info)) {}
    ~MapTypeInfo() override = default;

    int cmp(const void* left, const void* right) const override {
        auto l_value = reinterpret_cast<const MapValue*>(left);
        auto r_value = reinterpret_cast<const MapValue*>(right);
        uint32_t l_size = l_value->size();
        uint32_t r_size = r_value->size();
        if (l_size < r_size) {
            return -1;
        } else if (l_size > r_size) {
            return 1;
        } else {
            // now we use collection value in array to pack map k-v
            auto l_k = reinterpret_cast<const CollectionValue*>(l_value->key_data());
            auto l_v = reinterpret_cast<const CollectionValue*>(l_value->value_data());
            auto r_k = reinterpret_cast<const CollectionValue*>(r_value->key_data());
            auto r_v = reinterpret_cast<const CollectionValue*>(r_value->value_data());
            auto key_arr = new ArrayTypeInfo(create_static_type_info_ptr(_key_type_info.get()));
            auto val_arr = new ArrayTypeInfo(create_static_type_info_ptr(_value_type_info.get()));
            if (int kc = key_arr->cmp(l_k, r_k) != 0) {
                return kc;
            } else {
                return val_arr->cmp(l_v, r_v);
            }
        }
    }

    void deep_copy(void* dest, const void* src, Arena& arena) const override { DCHECK(false); }

    void set_to_max(void* buf) const override {
        DCHECK(false) << "set_to_max of list is not implemented.";
    }

    void set_to_min(void* buf) const override {
        DCHECK(false) << "set_to_min of list is not implemented.";
    }

    size_t size() const override { return sizeof(MapValue); }

    FieldType type() const override { return FieldType::OLAP_FIELD_TYPE_MAP; }

    inline const TypeInfo* get_key_type_info() const { return _key_type_info.get(); }
    inline const TypeInfo* get_value_type_info() const { return _value_type_info.get(); }

private:
    TypeInfoPtr _key_type_info;
    TypeInfoPtr _value_type_info;
};

class StructTypeInfo : public TypeInfo {
public:
    explicit StructTypeInfo(std::vector<TypeInfoPtr>& type_infos) {
        for (TypeInfoPtr& type_info : type_infos) {
            _type_infos.push_back(std::move(type_info));
        }
    }
    ~StructTypeInfo() override = default;

    int cmp(const void* left, const void* right) const override {
        auto l_value = reinterpret_cast<const StructValue*>(left);
        auto r_value = reinterpret_cast<const StructValue*>(right);
        uint32_t l_size = l_value->size();
        uint32_t r_size = r_value->size();
        uint32_t cur = 0;

        if (!l_value->has_null() && !r_value->has_null()) {
            while (cur < l_size && cur < r_size) {
                int result =
                        _type_infos[cur]->cmp(l_value->child_value(cur), r_value->child_value(cur));
                if (result != 0) {
                    return result;
                }
                ++cur;
            }
        } else {
            while (cur < l_size && cur < r_size) {
                if (l_value->is_null_at(cur)) {
                    if (!r_value->is_null_at(cur)) { // left is null & right is not null
                        return -1;
                    }
                } else if (r_value->is_null_at(cur)) { // left is not null & right is null
                    return 1;
                } else { // both are not null
                    int result = _type_infos[cur]->cmp(l_value->child_value(cur),
                                                       r_value->child_value(cur));
                    if (result != 0) {
                        return result;
                    }
                }
                ++cur;
            }
        }

        if (l_size < r_size) {
            return -1;
        } else if (l_size > r_size) {
            return 1;
        } else {
            return 0;
        }
    }

    void deep_copy(void* dest, const void* src, Arena& arena) const override {
        auto dest_value = reinterpret_cast<StructValue*>(dest);
        auto src_value = reinterpret_cast<const StructValue*>(src);

        if (src_value->size() == 0) {
            new (dest_value) StructValue(src_value->size());
            return;
        }

        dest_value->set_size(src_value->size());
        dest_value->set_has_null(src_value->has_null());

        size_t allocate_size = src_value->size() * sizeof(*src_value->values());
        // allocate memory for children value
        for (uint32_t i = 0; i < src_value->size(); ++i) {
            if (src_value->is_null_at(i)) continue;
            allocate_size += _type_infos[i]->size();
        }

        dest_value->set_values((void**)arena.alloc(allocate_size));
        auto ptr = reinterpret_cast<uint8_t*>(dest_value->mutable_values());
        ptr += dest_value->size() * sizeof(*dest_value->values());

        for (uint32_t i = 0; i < src_value->size(); ++i) {
            dest_value->set_child_value(nullptr, i);
            if (src_value->is_null_at(i)) continue;
            dest_value->set_child_value(ptr, i);
            ptr += _type_infos[i]->size();
        }

        // copy children value
        for (uint32_t i = 0; i < src_value->size(); ++i) {
            if (src_value->is_null_at(i)) continue;
            _type_infos[i]->deep_copy(dest_value->mutable_child_value(i), src_value->child_value(i),
                                      arena);
        }
    }

    void set_to_max(void* buf) const override {
        DCHECK(false) << "set_to_max of list is not implemented.";
    }

    void set_to_min(void* buf) const override {
        DCHECK(false) << "set_to_min of list is not implemented.";
    }

    size_t size() const override { return sizeof(StructValue); }

    FieldType type() const override { return FieldType::OLAP_FIELD_TYPE_STRUCT; }

    inline const std::vector<TypeInfoPtr>* type_infos() const { return &_type_infos; }

private:
    std::vector<TypeInfoPtr> _type_infos;
};

bool is_scalar_type(FieldType field_type);

const TypeInfo* get_scalar_type_info(FieldType field_type);

TypeInfoPtr get_type_info(const segment_v2::ColumnMetaPB* column_meta_pb);

TypeInfoPtr get_type_info(const TabletColumn* col);

TypeInfoPtr clone_type_info(const TypeInfo* type_info);

// support following formats when convert varchar to date
static const std::vector<std::string> DATE_FORMATS {
        "%Y-%m-%d", "%y-%m-%d", "%Y%m%d", "%y%m%d", "%Y/%m/%d", "%y/%m/%d",
};

template <FieldType field_type>
struct CppTypeTraits {};

template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_BOOL> {
    using CppType = uint8_t;
    using UnsignedCppType = uint8_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_TINYINT> {
    using CppType = int8_t;
    using UnsignedCppType = uint8_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_SMALLINT> {
    using CppType = int16_t;
    using UnsignedCppType = uint16_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_INT> {
    using CppType = int32_t;
    using UnsignedCppType = uint32_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT> {
    using CppType = uint32_t;
    using UnsignedCppType = uint32_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_BIGINT> {
    using CppType = int64_t;
    using UnsignedCppType = uint64_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT> {
    using CppType = uint64_t;
    using UnsignedCppType = uint64_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_LARGEINT> {
    using CppType = int128_t;
    using UnsignedCppType = uint128_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_FLOAT> {
    using CppType = float;
    using UnsignedCppType = uint32_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DOUBLE> {
    using CppType = double;
    using UnsignedCppType = uint64_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DECIMAL> {
    using CppType = decimal12_t;
    using UnsignedCppType = decimal12_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DECIMAL32> {
    using CppType = int32_t;
    using UnsignedCppType = uint32_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DECIMAL64> {
    using CppType = int64_t;
    using UnsignedCppType = uint64_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DECIMAL128I> {
    using CppType = int128_t;
    using UnsignedCppType = uint128_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DECIMAL256> {
    using CppType = wide::Int256;
    using UnsignedCppType = wide::UInt256;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DATE> {
    using CppType = uint24_t;
    using UnsignedCppType = uint24_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DATEV2> {
    using CppType = uint32_t;
    using UnsignedCppType = uint32_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_TIMEV2> {
    using CppType = uint64_t;
    using UnsignedCppType = uint64_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DATETIMEV2> {
    using CppType = uint64_t;
    using UnsignedCppType = uint64_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ> {
    using CppType = uint64_t;
    using UnsignedCppType = uint64_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DATETIME> {
    using CppType = int64_t;
    using UnsignedCppType = uint64_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_IPV4> {
    using CppType = uint32_t;
    using UnsignedCppType = uint32_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_IPV6> {
    using CppType = uint128_t;
    using UnsignedCppType = uint128_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_CHAR> {
    using CppType = Slice;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_VARCHAR> {
    using CppType = Slice;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_STRING> {
    using CppType = Slice;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_JSONB> {
    using CppType = Slice;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_VARIANT> {
    using CppType = Slice;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_HLL> {
    using CppType = Slice;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_BITMAP> {
    using CppType = Slice;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE> {
    using CppType = Slice;
};

template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_AGG_STATE> {
    using CppType = Slice;
};

template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_STRUCT> {
    using CppType = StructValue;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_ARRAY> {
    using CppType = CollectionValue;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_MAP> {
    using CppType = MapValue;
};
template <FieldType field_type>
struct BaseFieldTypeTraits : public CppTypeTraits<field_type> {
    using CppType = typename CppTypeTraits<field_type>::CppType;

    static inline CppType get_cpp_type_value(const void* address) {
        if constexpr (field_type == FieldType::OLAP_FIELD_TYPE_LARGEINT) {
            return get_int128_from_unalign(address);
        }
        return *reinterpret_cast<const CppType*>(address);
    }

    static inline void set_cpp_type_value(void* address, const CppType& value) {
        memcpy(address, &value, sizeof(CppType));
    }

    static inline int cmp(const void* left, const void* right) {
        CppType left_value = get_cpp_type_value(left);
        CppType right_value = get_cpp_type_value(right);
        if (left_value < right_value) {
            return -1;
        } else if (left_value > right_value) {
            return 1;
        } else {
            return 0;
        }
    }

    static inline void deep_copy(void* dest, const void* src, Arena& arena) {
        memcpy(dest, src, sizeof(CppType));
    }

    static inline void set_to_max(void* buf) {
        set_cpp_type_value(buf, type_limit<CppType>::max());
    }

    static inline void set_to_min(void* buf) {
        set_cpp_type_value(buf, type_limit<CppType>::min());
    }
};

// Using NumericFieldtypeTraits to Derived code for FieldType::OLAP_FIELD_TYPE_XXXINT, FieldType::OLAP_FIELD_TYPE_FLOAT,
// FieldType::OLAP_FIELD_TYPE_DOUBLE, to reduce redundant code
template <FieldType fieldType, bool isArithmetic>
struct NumericFieldTypeTraits : public BaseFieldTypeTraits<fieldType> {
    using CppType = typename CppTypeTraits<fieldType>::CppType;
};

template <FieldType fieldType>
struct NumericFieldTypeTraits<fieldType, false> : public BaseFieldTypeTraits<fieldType> {};

template <FieldType fieldType>
struct FieldTypeTraits
        : public NumericFieldTypeTraits<
                  fieldType, IsArithmeticV<typename BaseFieldTypeTraits<fieldType>::CppType> &&
                                     IsSignedV<typename BaseFieldTypeTraits<fieldType>::CppType>> {
};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_BOOL>
        : public BaseFieldTypeTraits<FieldType::OLAP_FIELD_TYPE_BOOL> {
    static void set_to_max(void* buf) { (*(uint8_t*)buf) = 1; }
    static void set_to_min(void* buf) { (*(uint8_t*)buf) = 0; }
};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_LARGEINT>
        : public NumericFieldTypeTraits<FieldType::OLAP_FIELD_TYPE_LARGEINT, true> {
    // GCC7.3 will generate movaps instruction, which will lead to SEGV when buf is
    // not aligned to 16 byte
    static void deep_copy(void* dest, const void* src, Arena& arena) {
        *reinterpret_cast<PackedInt128*>(dest) = *reinterpret_cast<const PackedInt128*>(src);
    }

    static void set_to_max(void* buf) {
        *reinterpret_cast<PackedInt128*>(buf) = ~((int128_t)(1) << 127);
    }
    static void set_to_min(void* buf) {
        *reinterpret_cast<PackedInt128*>(buf) = (int128_t)(1) << 127;
    }
};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_IPV4>
        : public BaseFieldTypeTraits<FieldType::OLAP_FIELD_TYPE_IPV4> {
    static void set_to_max(void* buf) {
        *reinterpret_cast<uint32_t*>(buf) = 0xFFFFFFFF; // 255.255.255.255
    }

    static void set_to_min(void* buf) {
        *reinterpret_cast<uint32_t*>(buf) = 0; // 0.0.0.0
    }
};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_IPV6>
        : public BaseFieldTypeTraits<FieldType::OLAP_FIELD_TYPE_IPV6> {
    static void set_to_max(void* buf) {
        *reinterpret_cast<int128_t*>(buf) = -1; // ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff
    }

    static void set_to_min(void* buf) {
        *reinterpret_cast<uint128_t*>(buf) = 0; // ::
    }
};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DECIMAL>
        : public BaseFieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DECIMAL> {
    static void set_to_max(void* buf) {
        CppType* data = reinterpret_cast<CppType*>(buf);
        data->integer = 999999999999999999L;
        data->fraction = 999999999;
    }
    static void set_to_min(void* buf) {
        CppType* data = reinterpret_cast<CppType*>(buf);
        data->integer = -999999999999999999;
        data->fraction = -999999999;
    }
};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DATE>
        : public BaseFieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DATE> {
    static void set_to_max(void* buf) {
        // max is 9999 * 16 * 32 + 12 * 32 + 31;
        *reinterpret_cast<CppType*>(buf) = 5119903;
    }
    static void set_to_min(void* buf) {
        // min is 0 * 16 * 32 + 1 * 32 + 1;
        *reinterpret_cast<CppType*>(buf) = 33;
    }
};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DATEV2>
        : public BaseFieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DATEV2> {
    static void set_to_max(void* buf) {
        // max is 9999 * 16 * 32 + 12 * 32 + 31;
        *reinterpret_cast<CppType*>(buf) = MAX_DATE_V2;
    }
    static void set_to_min(void* buf) {
        // min is 0 * 16 * 32 + 1 * 32 + 1;
        *reinterpret_cast<CppType*>(buf) = MIN_DATE_V2;
    }
};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DATETIMEV2>
        : public BaseFieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DATETIMEV2> {
    static void set_to_max(void* buf) {
        // max is 9999 * 16 * 32 + 12 * 32 + 31;
        *reinterpret_cast<CppType*>(buf) = MAX_DATETIME_V2;
    }
    static void set_to_min(void* buf) {
        // min is 0 * 16 * 32 + 1 * 32 + 1;
        *reinterpret_cast<CppType*>(buf) = MIN_DATETIME_V2;
    }
};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DATETIME>
        : public BaseFieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DATETIME> {
    static void set_to_max(void* buf) {
        // 9999-12-31 23:59:59
        *reinterpret_cast<CppType*>(buf) = 99991231235959L;
    }
    static void set_to_min(void* buf) { *reinterpret_cast<CppType*>(buf) = 101000000; }
};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ>
        : public BaseFieldTypeTraits<FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ> {
    static void set_to_max(void* buf) {
        // max is 9999 * 16 * 32 + 12 * 32 + 31;
        *reinterpret_cast<CppType*>(buf) = MAX_DATETIME_V2;
    }
    static void set_to_min(void* buf) {
        // min is 0 * 16 * 32 + 1 * 32 + 1;
        *reinterpret_cast<CppType*>(buf) = MIN_DATETIME_V2;
    }
};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_CHAR>
        : public BaseFieldTypeTraits<FieldType::OLAP_FIELD_TYPE_CHAR> {
    static int cmp(const void* left, const void* right) {
        auto l_slice = reinterpret_cast<const Slice*>(left);
        auto r_slice = reinterpret_cast<const Slice*>(right);
        return l_slice->compare(*r_slice);
    }
    static void deep_copy(void* dest, const void* src, Arena& arena) {
        auto l_slice = reinterpret_cast<Slice*>(dest);
        auto r_slice = reinterpret_cast<const Slice*>(src);
        l_slice->data = arena.alloc(r_slice->size);
        memcpy(l_slice->data, r_slice->data, r_slice->size);
        l_slice->size = r_slice->size;
    }

    // Using field.set_to_max to set varchar/char,not here.
    static void (*set_to_max)(void*);

    static void set_to_min(void* buf) {
        auto slice = reinterpret_cast<Slice*>(buf);
        memset(slice->data, 0, slice->size);
    }
};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_VARCHAR>
        : public FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_CHAR> {
    static void set_to_min(void* buf) {
        auto slice = reinterpret_cast<Slice*>(buf);
        slice->size = 0;
    }
};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_STRING>
        : public FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_CHAR> {
    static void set_to_min(void* buf) {
        auto slice = reinterpret_cast<Slice*>(buf);
        slice->size = 0;
    }
};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_JSONB>
        : public FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_VARCHAR> {
    static int cmp(const void* left, const void* right) {
        LOG(WARNING) << "can not compare JSONB values";
        return -1; // always update ?
    }

    static void set_to_min(void* buf) {
        auto slice = reinterpret_cast<Slice*>(buf);
        slice->size = 0;
    }

    static void set_to_max(void* buf) {
        auto slice = reinterpret_cast<Slice*>(buf);
        slice->size = 0;
    }
};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_VARIANT>
        : public FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_JSONB> {
    static int cmp(const void* left, const void* right) {
        LOG(WARNING) << "can not compare VARIANT values";
        return -1; // always update ?
    }
};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_HLL>
        : public FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_VARCHAR> {};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_BITMAP>
        : public FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_VARCHAR> {};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE>
        : public FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_VARCHAR> {};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_AGG_STATE>
        : public FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_VARCHAR> {};

// Instantiate this template to get static access to the type traits.
template <FieldType field_type>
struct TypeTraits : public FieldTypeTraits<field_type> {
    using CppType = typename CppTypeTraits<field_type>::CppType;

    static const FieldType type = field_type;
    static const int32_t size = sizeof(CppType);
};

template <FieldType field_type>
const TypeInfo* get_scalar_type_info() {
    static constexpr TypeTraits<field_type> traits;
    static ScalarTypeInfo scalar_type_info(traits);
    return &scalar_type_info;
}

template <FieldType field_type>
inline const TypeInfo* get_collection_type_info() {
    static ArrayTypeInfo collection_type_info(
            create_static_type_info_ptr(get_scalar_type_info<field_type>()));
    return &collection_type_info;
}

// nested array type is unsupported for sub_type of collection
template <>
inline const TypeInfo* get_collection_type_info<FieldType::OLAP_FIELD_TYPE_ARRAY>() {
    return nullptr;
}

#include "common/compile_check_end.h"
} // namespace doris
