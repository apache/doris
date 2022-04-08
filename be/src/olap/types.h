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

#ifndef DORIS_BE_SRC_OLAP_TYPES_H
#define DORIS_BE_SRC_OLAP_TYPES_H

#include <math.h>
#include <stdio.h>

#include <limits>
#include <sstream>
#include <string>

#include "gen_cpp/segment_v2.pb.h" // for ColumnMetaPB
#include "gutil/strings/numbers.h"
#include "olap/decimal12.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/tablet_schema.h" // for TabletColumn
#include "olap/uint24.h"
#include "runtime/collection_value.h"
#include "runtime/datetime_value.h"
#include "runtime/mem_pool.h"
#include "util/hash_util.hpp"
#include "util/mem_util.hpp"
#include "util/slice.h"
#include "util/string_parser.hpp"
#include "util/types.h"

namespace doris {
class TabletColumn;

extern bool is_olap_string_type(FieldType field_type);

class TypeInfo {
public:
    virtual ~TypeInfo() = default;
    virtual bool equal(const void* left, const void* right) const = 0;
    virtual int cmp(const void* left, const void* right) const = 0;

    virtual void shallow_copy(void* dest, const void* src) const = 0;

    virtual void deep_copy(void* dest, const void* src, MemPool* mem_pool) const = 0;

    // See `copy_row_in_memtable()` in `olap/row.h`, will be removed in the future.
    // It is same with deep_copy() for all type except for HLL and OBJECT type.
    virtual void copy_object(void* dest, const void* src, MemPool* mem_pool) const = 0;

    virtual void direct_copy(void* dest, const void* src) const = 0;

    // Use only in zone map to cut data.
    virtual void direct_copy_may_cut(void* dest, const void* src) const = 0;

    // Convert and deep copy value from other type's source.
    virtual OLAPStatus convert_from(void* dest, const void* src, const TypeInfo* src_type,
                                    MemPool* mem_pool, size_t variable_len = 0) const = 0;

    virtual OLAPStatus from_string(void* buf, const std::string& scan_key) const = 0;

    virtual std::string to_string(const void* src) const = 0;

    virtual void set_to_max(void* buf) const = 0;
    virtual void set_to_min(void* buf) const = 0;

    virtual uint32_t hash_code(const void* data, uint32_t seed) const = 0;
    virtual const size_t size() const = 0;

    virtual FieldType type() const = 0;
};

class ScalarTypeInfo : public TypeInfo {
public:
    bool equal(const void* left, const void* right) const override {
        return _equal(left, right);
    }

    int cmp(const void* left, const void* right) const override { return _cmp(left, right); }

    void shallow_copy(void* dest, const void* src) const override {
        _shallow_copy(dest, src);
    }

    void deep_copy(void* dest, const void* src, MemPool* mem_pool) const override {
        _deep_copy(dest, src, mem_pool);
    }

    // See `copy_row_in_memtable()` in olap/row.h, will be removed in the future.
    // It is same with `deep_copy()` for all type except for HLL and OBJECT type.
    void copy_object(void* dest, const void* src, MemPool* mem_pool) const override {
        _copy_object(dest, src, mem_pool);
    }

    void direct_copy(void* dest, const void* src) const override { _direct_copy(dest, src); }

    void direct_copy_may_cut(void* dest, const void* src) const override {
        _direct_copy_may_cut(dest, src);
    }

    // Convert and deep copy value from other type's source.
    OLAPStatus convert_from(void* dest, const void* src, const TypeInfo* src_type,
                            MemPool* mem_pool, size_t variable_len = 0) const override {
        return _convert_from(dest, src, src_type, mem_pool, variable_len);
    }

    OLAPStatus from_string(void* buf, const std::string& scan_key) const override {
        return _from_string(buf, scan_key);
    }

    std::string to_string(const void* src) const override { return _to_string(src); }

    void set_to_max(void* buf) const override { _set_to_max(buf); }
    void set_to_min(void* buf) const override { _set_to_min(buf); }

    uint32_t hash_code(const void* data, uint32_t seed) const override {
        return _hash_code(data, seed);
    }
    const size_t size() const override { return _size; }

    FieldType type() const override { return _field_type; }

    template <typename TypeTraitsClass>
    ScalarTypeInfo(TypeTraitsClass t)
            : _equal(TypeTraitsClass::equal),
              _cmp(TypeTraitsClass::cmp),
              _shallow_copy(TypeTraitsClass::shallow_copy),
              _deep_copy(TypeTraitsClass::deep_copy),
              _copy_object(TypeTraitsClass::copy_object),
              _direct_copy(TypeTraitsClass::direct_copy),
              _direct_copy_may_cut(TypeTraitsClass::direct_copy_may_cut),
              _convert_from(TypeTraitsClass::convert_from),
              _from_string(TypeTraitsClass::from_string),
              _to_string(TypeTraitsClass::to_string),
              _set_to_max(TypeTraitsClass::set_to_max),
              _set_to_min(TypeTraitsClass::set_to_min),
              _hash_code(TypeTraitsClass::hash_code),
              _size(TypeTraitsClass::size),
              _field_type(TypeTraitsClass::type) {}

private:
    bool (*_equal)(const void* left, const void* right);
    int (*_cmp)(const void* left, const void* right);

    void (*_shallow_copy)(void* dest, const void* src);
    void (*_deep_copy)(void* dest, const void* src, MemPool* mem_pool);
    void (*_copy_object)(void* dest, const void* src, MemPool* mem_pool);
    void (*_direct_copy)(void* dest, const void* src);
    void (*_direct_copy_may_cut)(void* dest, const void* src);
    OLAPStatus (*_convert_from)(void* dest, const void* src, const TypeInfo* src_type,
                                MemPool* mem_pool, size_t variable_len);

    OLAPStatus (*_from_string)(void* buf, const std::string& scan_key);
    std::string (*_to_string)(const void* src);

    void (*_set_to_max)(void* buf);
    void (*_set_to_min)(void* buf);

    uint32_t (*_hash_code)(const void* data, uint32_t seed);

    const size_t _size;
    const FieldType _field_type;

    friend class ScalarTypeInfoResolver;
};

class ArrayTypeInfo : public TypeInfo {
public:
    explicit ArrayTypeInfo(const TypeInfo* item_type_info)
            : _item_type_info(item_type_info), _item_size(item_type_info->size()) {}
    ~ArrayTypeInfo() = default;
    bool equal(const void* left, const void* right) const override {
        auto l_value = reinterpret_cast<const CollectionValue*>(left);
        auto r_value = reinterpret_cast<const CollectionValue*>(right);
        if (l_value->length() != r_value->length()) {
            return false;
        }
        size_t len = l_value->length();

        if (!l_value->has_null() && !r_value->has_null()) {
            for (size_t i = 0; i < len; ++i) {
                if (!_item_type_info->equal((uint8_t*)(l_value->data()) + i * _item_size,
                                            (uint8_t*)(r_value->data()) + i * _item_size)) {
                    return false;
                }
            }
        } else {
            for (size_t i = 0; i < len; ++i) {
                if (l_value->is_null_at(i)) {
                    if (r_value->is_null_at(i)) { // both are null
                        continue;
                    } else { // left is null & right is not null
                        return false;
                    }
                } else if (r_value->is_null_at(i)) { // left is not null & right is null
                    return false;
                }
                if (!_item_type_info->equal((uint8_t*)(l_value->data()) + i * _item_size,
                                            (uint8_t*)(r_value->data()) + i * _item_size)) {
                    return false;
                }
            }
        }
        return true;
    }

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

    void shallow_copy(void* dest, const void* src) const override {
        auto dest_value = reinterpret_cast<CollectionValue*>(dest);
        auto src_value = reinterpret_cast<const CollectionValue*>(src);
        dest_value->shallow_copy(src_value);
    }

    void deep_copy(void* dest, const void* src, MemPool* mem_pool) const override {
        auto dest_value = reinterpret_cast<CollectionValue*>(dest);
        auto src_value = reinterpret_cast<const CollectionValue*>(src);

        if (src_value->length() == 0) {
            new (dest_value) CollectionValue(src_value->length());
            return;
        }

        dest_value->set_length(src_value->length());

        size_t item_size = src_value->length() * _item_size;
        size_t nulls_size = src_value->has_null() ? src_value->length() : 0;
        dest_value->set_data(mem_pool->allocate(item_size + nulls_size));
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
                                       (uint8_t*)(src_value->data()) + i * _item_size, mem_pool);
        }
    }

    void copy_object(void* dest, const void* src, MemPool* mem_pool) const override {
        deep_copy(dest, src, mem_pool);
    }

    void direct_copy(void* dest, const void* src) const override {
        auto dest_value = reinterpret_cast<CollectionValue*>(dest);
        // NOTICE: The address pointed by null_signs of the dest_value can NOT be modified here.
        auto base = reinterpret_cast<uint8_t*>(dest_value->mutable_null_signs());
        direct_copy(&base, dest, src);
    }

    void direct_copy(uint8_t** base, void* dest, const void* src) const {
        auto dest_value = reinterpret_cast<CollectionValue*>(dest);
        auto src_value = reinterpret_cast<const CollectionValue*>(src);

        auto nulls_size = src_value->has_null() ? src_value->length() : 0;
        dest_value->set_data(src_value->length() ? (*base + nulls_size) : nullptr);
        dest_value->set_length(src_value->length());
        dest_value->set_has_null(src_value->has_null());
        if (src_value->has_null()) {
            // direct copy null_signs
            dest_value->set_null_signs(reinterpret_cast<bool*>(*base));
            memory_copy(dest_value->mutable_null_signs(), src_value->null_signs(),
                        src_value->length());
        }
        *base += nulls_size + src_value->length() * _item_type_info->size();
        // Direct copy item.
        if (_item_type_info->type() == OLAP_FIELD_TYPE_ARRAY) {
            for (uint32_t i = 0; i < src_value->length(); ++i) {
                if (dest_value->is_null_at(i)) continue;
                dynamic_cast<const ArrayTypeInfo*>(_item_type_info)
                        ->direct_copy(base, (uint8_t*)(dest_value->mutable_data()) + i * _item_size,
                                      (uint8_t*)(src_value->data()) + i * _item_size);
            }
        } else {
            for (uint32_t i = 0; i < src_value->length(); ++i) {
                if (dest_value->is_null_at(i)) continue;
                auto dest_address = (uint8_t*)(dest_value->mutable_data()) + i * _item_size;
                auto src_address = (uint8_t*)(src_value->data()) + i * _item_size;
                if (is_olap_string_type(_item_type_info->type())) {
                    auto dest_slice = reinterpret_cast<Slice*>(dest_address);
                    auto src_slice = reinterpret_cast<const Slice*>(src_address);
                    dest_slice->data = reinterpret_cast<char*>(*base);
                    dest_slice->size = src_slice->size;
                    *base += src_slice->size;
                }
                _item_type_info->direct_copy(dest_address, src_address);
            }
        }
    }

    void direct_copy_may_cut(void* dest, const void* src) const override {
        direct_copy(dest, src);
    }

    OLAPStatus convert_from(void* dest, const void* src, const TypeInfo* src_type,
                            MemPool* mem_pool, size_t variable_len = 0) const override {
        return OLAPStatus::OLAP_ERR_FUNC_NOT_IMPLEMENTED;
    }

    OLAPStatus from_string(void* buf, const std::string& scan_key) const override {
        return OLAPStatus::OLAP_ERR_FUNC_NOT_IMPLEMENTED;
    }

    std::string to_string(const void* src) const override {
        auto src_value = reinterpret_cast<const CollectionValue*>(src);
        std::string result = "[";

        for (size_t i = 0; i < src_value->length(); ++i) {
            std::string item =
                    _item_type_info->to_string((uint8_t*)(src_value->data()) + i * _item_size);
            result += item;
            if (i != src_value->length() - 1) {
                result += ", ";
            }
        }
        result += "]";
        return result;
    }

    void set_to_max(void* buf) const override {
        DCHECK(false) << "set_to_max of list is not implemented.";
    }

    void set_to_min(void* buf) const override {
        DCHECK(false) << "set_to_min of list is not implemented.";
    }

    uint32_t hash_code(const void* data, uint32_t seed) const override {
        auto value = reinterpret_cast<const CollectionValue*>(data);
        auto len = value->length();
        uint32_t result = HashUtil::hash(&len, sizeof(len), seed);
        for (size_t i = 0; i < len; ++i) {
            if (value->is_null_at(i)) {
                result = seed * result;
            } else {
                result = seed * result + _item_type_info->hash_code(
                                                 (uint8_t*)(value->data()) + i * _item_size, seed);
            }
        }
        return result;
    }

    const size_t size() const override { return sizeof(CollectionValue); }

    FieldType type() const override { return OLAP_FIELD_TYPE_ARRAY; }

    const TypeInfo* item_type_info() const { return _item_type_info; }

private:
    const TypeInfo* _item_type_info;
    const size_t _item_size;
};

extern bool is_scalar_type(FieldType field_type);

extern const TypeInfo* get_scalar_type_info(FieldType field_type);

extern const TypeInfo* get_type_info(segment_v2::ColumnMetaPB* column_meta_pb);

extern const TypeInfo* get_type_info(const TabletColumn* col);

// support following formats when convert varchar to date
static const std::vector<std::string> DATE_FORMATS {
        "%Y-%m-%d", "%y-%m-%d", "%Y%m%d", "%y%m%d", "%Y/%m/%d", "%y/%m/%d",
};

template <FieldType field_type>
struct CppTypeTraits {};

template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_BOOL> {
    using CppType = bool;
    using UnsignedCppType = bool;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_TINYINT> {
    using CppType = int8_t;
    using UnsignedCppType = uint8_t;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_SMALLINT> {
    using CppType = int16_t;
    using UnsignedCppType = uint16_t;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_INT> {
    using CppType = int32_t;
    using UnsignedCppType = uint32_t;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_UNSIGNED_INT> {
    using CppType = uint32_t;
    using UnsignedCppType = uint32_t;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_BIGINT> {
    using CppType = int64_t;
    using UnsignedCppType = uint64_t;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_UNSIGNED_BIGINT> {
    using CppType = uint64_t;
    using UnsignedCppType = uint64_t;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_LARGEINT> {
    using CppType = int128_t;
    using UnsignedCppType = uint128_t;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_FLOAT> {
    using CppType = float;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_DOUBLE> {
    using CppType = double;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_DECIMAL> {
    using CppType = decimal12_t;
    using UnsignedCppType = decimal12_t;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_DATE> {
    using CppType = uint24_t;
    using UnsignedCppType = uint24_t;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_DATETIME> {
    using CppType = int64_t;
    using UnsignedCppType = uint64_t;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_CHAR> {
    using CppType = Slice;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_VARCHAR> {
    using CppType = Slice;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_STRING> {
    using CppType = Slice;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_HLL> {
    using CppType = Slice;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_OBJECT> {
    using CppType = Slice;
};

template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_QUANTILE_STATE> {
    using CppType = Slice;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_ARRAY> {
    using CppType = CollectionValue;
};
template <FieldType field_type>
struct BaseFieldtypeTraits : public CppTypeTraits<field_type> {
    using CppType = typename CppTypeTraits<field_type>::CppType;

    static inline bool equal(const void* left, const void* right) {
        CppType l_value = *reinterpret_cast<const CppType*>(left);
        CppType r_value = *reinterpret_cast<const CppType*>(right);
        return l_value == r_value;
    }

    static inline int cmp(const void* left, const void* right) {
        CppType left_int = *reinterpret_cast<const CppType*>(left);
        CppType right_int = *reinterpret_cast<const CppType*>(right);
        if (left_int < right_int) {
            return -1;
        } else if (left_int > right_int) {
            return 1;
        } else {
            return 0;
        }
    }

    static inline void shallow_copy(void* dest, const void* src) {
        *reinterpret_cast<CppType*>(dest) = *reinterpret_cast<const CppType*>(src);
    }

    static inline void deep_copy(void* dest, const void* src, MemPool* mem_pool) {
        *reinterpret_cast<CppType*>(dest) = *reinterpret_cast<const CppType*>(src);
    }

    static inline void copy_object(void* dest, const void* src, MemPool* mem_pool) {
        *reinterpret_cast<CppType*>(dest) = *reinterpret_cast<const CppType*>(src);
    }

    static inline void direct_copy(void* dest, const void* src) {
        *reinterpret_cast<CppType*>(dest) = *reinterpret_cast<const CppType*>(src);
    }

    static inline void direct_copy_may_cut(void* dest, const void* src) { direct_copy(dest, src); }

    static OLAPStatus convert_from(void* dest, const void* src, const TypeInfo* src_type,
                                   MemPool* mem_pool, size_t variable_len = 0) {
        return OLAPStatus::OLAP_ERR_FUNC_NOT_IMPLEMENTED;
    }

    static inline void set_to_max(void* buf) {
        *reinterpret_cast<CppType*>(buf) = std::numeric_limits<CppType>::max();
    }

    static inline void set_to_min(void* buf) {
        *reinterpret_cast<CppType*>(buf) = std::numeric_limits<CppType>::min();
    }

    static inline uint32_t hash_code(const void* data, uint32_t seed) {
        return HashUtil::hash(data, sizeof(CppType), seed);
    }

    static std::string to_string(const void* src) {
        return std::to_string(*reinterpret_cast<const CppType*>(src));
    }

    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        CppType value = 0;
        if (scan_key.length() > 0) {
            value = static_cast<CppType>(strtol(scan_key.c_str(), nullptr, 10));
        }
        *reinterpret_cast<CppType*>(buf) = value;
        return OLAP_SUCCESS;
    }
};

static void prepare_char_before_convert(const void* src) {
    Slice* slice = const_cast<Slice*>(reinterpret_cast<const Slice*>(src));
    char* buf = slice->data;
    int64_t p = slice->size - 1;
    while (p >= 0 && buf[p] == '\0') {
        p--;
    }
    slice->size = p + 1;
}

template <typename T>
T convert_from_varchar(const Slice* src_value, StringParser::ParseResult& parse_res,
                       std::true_type) {
    return StringParser::string_to_int<T>(src_value->get_data(), src_value->get_size(), &parse_res);
}

template <typename T>
T convert_from_varchar(const Slice* src_value, StringParser::ParseResult& parse_res,
                       std::false_type) {
    return StringParser::string_to_float<T>(src_value->get_data(), src_value->get_size(),
                                            &parse_res);
}

template <typename T>
OLAPStatus arithmetic_convert_from_varchar(void* dest, const void* src) {
    auto src_value = reinterpret_cast<const Slice*>(src);
    StringParser::ParseResult parse_res;
    //TODO: use C++17 if constexpr to replace label assignment
    auto result = convert_from_varchar<T>(src_value, parse_res, std::is_integral<T>());
    if (UNLIKELY(parse_res != StringParser::PARSE_SUCCESS)) {
        return OLAPStatus::OLAP_ERR_INVALID_SCHEMA;
    }
    memcpy(dest, &result, sizeof(T));
    return OLAPStatus::OLAP_SUCCESS;
}

template <typename T>
OLAPStatus numeric_convert_from_char(void* dest, const void* src) {
    prepare_char_before_convert(src);
    return arithmetic_convert_from_varchar<T>(dest, src);
}

// Using NumericFieldtypeTraits to Derived code for OLAP_FIELD_TYPE_XXXINT, OLAP_FIELD_TYPE_FLOAT,
// OLAP_FIELD_TYPE_DOUBLE, to reduce redundant code
template <FieldType fieldType, bool isArithmetic>
struct NumericFieldtypeTraits : public BaseFieldtypeTraits<fieldType> {
    using CppType = typename CppTypeTraits<fieldType>::CppType;

    static std::string to_string(const void* src) {
        return std::to_string(*reinterpret_cast<const CppType*>(src));
    }

    static OLAPStatus convert_from(void* dest, const void* src, const TypeInfo* src_type,
                                   MemPool* mem_pool, size_t variable_len = 0) {
        if (src_type->type() == OLAP_FIELD_TYPE_VARCHAR ||
            src_type->type() == OLAP_FIELD_TYPE_STRING) {
            return arithmetic_convert_from_varchar<CppType>(dest, src);
        } else if (src_type->type() == OLAP_FIELD_TYPE_CHAR) {
            return numeric_convert_from_char<CppType>(dest, src);
        }
        return OLAPStatus::OLAP_ERR_INVALID_SCHEMA;
    }
};

template <FieldType fieldType>
struct NumericFieldtypeTraits<fieldType, false> : public BaseFieldtypeTraits<fieldType> {};

template <FieldType fieldType>
struct FieldTypeTraits
        : public NumericFieldtypeTraits<
                  fieldType,
                  std::is_arithmetic<typename BaseFieldtypeTraits<fieldType>::CppType>::value &&
                          std::is_signed<typename BaseFieldtypeTraits<fieldType>::CppType>::value> {
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_BOOL> : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_BOOL> {
    static std::string to_string(const void* src) {
        char buf[1024] = {'\0'};
        snprintf(buf, sizeof(buf), "%d", *reinterpret_cast<const bool*>(src));
        return std::string(buf);
    }
    static void set_to_max(void* buf) { (*(bool*)buf) = true; }
    static void set_to_min(void* buf) { (*(bool*)buf) = false; }
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_LARGEINT>
        : public NumericFieldtypeTraits<OLAP_FIELD_TYPE_LARGEINT, true> {
    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        int128_t value = 0;

        const char* value_string = scan_key.c_str();
        char* end = nullptr;
        value = strtol(value_string, &end, 10);
        if (*end != 0) {
            value = 0;
        } else if (value > LONG_MIN && value < LONG_MAX) {
            // use strtol result directly
        } else {
            bool is_negative = false;
            if (*value_string == '-' || *value_string == '+') {
                if (*(value_string++) == '-') {
                    is_negative = true;
                }
            }

            uint128_t current = 0;
            uint128_t max_int128 = ~((int128_t)(1) << 127);
            while (*value_string != 0) {
                if (current > max_int128 / 10) {
                    break;
                }

                current = current * 10 + (*(value_string++) - '0');
            }
            if (*value_string != 0 || (!is_negative && current > max_int128) ||
                (is_negative && current > max_int128 + 1)) {
                current = 0;
            }

            value = is_negative ? -current : current;
        }

        *reinterpret_cast<PackedInt128*>(buf) = value;

        return OLAP_SUCCESS;
    }
    static std::string to_string(const void* src) {
        char buf[1024];
        int128_t value = reinterpret_cast<const PackedInt128*>(src)->value;
        if (value >= std::numeric_limits<int64_t>::min() &&
            value <= std::numeric_limits<int64_t>::max()) {
            snprintf(buf, sizeof(buf), "%ld", (int64_t)value);
        } else {
            char* current = buf;
            uint128_t abs_value = value;
            if (value < 0) {
                *(current++) = '-';
                abs_value = -value;
            }

            // the max value of uint64_t is 18446744073709551615UL,
            // so use Z19_UINT64 to divide uint128_t
            const static uint64_t Z19_UINT64 = 10000000000000000000ULL;
            uint64_t suffix = abs_value % Z19_UINT64;
            uint64_t middle = abs_value / Z19_UINT64 % Z19_UINT64;
            uint64_t prefix = abs_value / Z19_UINT64 / Z19_UINT64;

            char* end = buf + sizeof(buf);
            if (prefix > 0) {
                current += snprintf(current, end - current, "%" PRIu64, prefix);
                current += snprintf(current, end - current, "%.19" PRIu64, middle);
                current += snprintf(current, end - current, "%.19" PRIu64, suffix);
            } else if (OLAP_LIKELY(middle > 0)) {
                current += snprintf(current, end - current, "%" PRIu64, middle);
                current += snprintf(current, end - current, "%.19" PRIu64, suffix);
            } else {
                current += snprintf(current, end - current, "%" PRIu64, suffix);
            }
        }

        return std::string(buf);
    }

    // GCC7.3 will generate movaps instruction, which will lead to SEGV when buf is
    // not aligned to 16 byte
    static void shallow_copy(void* dest, const void* src) {
        *reinterpret_cast<PackedInt128*>(dest) = *reinterpret_cast<const PackedInt128*>(src);
    }
    static void deep_copy(void* dest, const void* src, MemPool* mem_pool) {
        *reinterpret_cast<PackedInt128*>(dest) = *reinterpret_cast<const PackedInt128*>(src);
    }

    static void copy_object(void* dest, const void* src, MemPool* mem_pool) {
        *reinterpret_cast<PackedInt128*>(dest) = *reinterpret_cast<const PackedInt128*>(src);
    }
    static void direct_copy(void* dest, const void* src) {
        *reinterpret_cast<PackedInt128*>(dest) = *reinterpret_cast<const PackedInt128*>(src);
    }

    static inline void direct_copy_may_cut(void* dest, const void* src) { direct_copy(dest, src); }

    static void set_to_max(void* buf) {
        *reinterpret_cast<PackedInt128*>(buf) = ~((int128_t)(1) << 127);
    }
    static void set_to_min(void* buf) {
        *reinterpret_cast<PackedInt128*>(buf) = (int128_t)(1) << 127;
    }
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_FLOAT>
        : public NumericFieldtypeTraits<OLAP_FIELD_TYPE_FLOAT, true> {
    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        CppType value = 0.0f;
        if (scan_key.length() > 0) {
            value = static_cast<CppType>(atof(scan_key.c_str()));
        }
        *reinterpret_cast<CppType*>(buf) = value;
        return OLAP_SUCCESS;
    }
    static std::string to_string(const void* src) {
        char buf[1024] = {'\0'};
        int length =
                FloatToBuffer(*reinterpret_cast<const CppType*>(src), MAX_FLOAT_STR_LENGTH, buf);
        DCHECK(length >= 0) << "gcvt float failed, float value="
                            << *reinterpret_cast<const CppType*>(src);
        return std::string(buf);
    }
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_DOUBLE>
        : public NumericFieldtypeTraits<OLAP_FIELD_TYPE_DOUBLE, true> {
    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        CppType value = 0.0;
        if (scan_key.length() > 0) {
            value = atof(scan_key.c_str());
        }
        *reinterpret_cast<CppType*>(buf) = value;
        return OLAP_SUCCESS;
    }
    static std::string to_string(const void* src) {
        char buf[1024] = {'\0'};
        int length =
                DoubleToBuffer(*reinterpret_cast<const CppType*>(src), MAX_DOUBLE_STR_LENGTH, buf);
        DCHECK(length >= 0) << "gcvt float failed, float value="
                            << *reinterpret_cast<const CppType*>(src);
        return std::string(buf);
    }
    static OLAPStatus convert_from(void* dest, const void* src, const TypeInfo* src_type,
                                   MemPool* mem_pool, size_t variable_len = 0) {
        //only support float now
        if (src_type->type() == OLAP_FIELD_TYPE_FLOAT) {
            using SrcType = typename CppTypeTraits<OLAP_FIELD_TYPE_FLOAT>::CppType;
            //http://www.softelectro.ru/ieee754_en.html
            //According to the definition of IEEE754, the effect of converting a float binary to a double binary
            //is the same as that of static_cast . Data precision cannot be guaranteed, but the progress of
            //decimal system can be guaranteed by converting a float to a char buffer and then to a double.
            //float v2 = static_cast<double>(v1),
            //float 0.3000000 is: 0 | 01111101 | 00110011001100110011010
            //double 0.300000011920929 is: 0 | 01111111101 | 0000000000000000000001000000000000000000000000000000
            //==float to char buffer to strtod==
            //float 0.3000000 is: 0 | 01111101 | 00110011001100110011010
            //double 0.300000000000000 is: 0 | 01111111101 | 0011001100110011001100110011001100110011001100110011
            char buf[64] = {0};
            snprintf(buf, 64, "%f", *reinterpret_cast<const SrcType*>(src));
            char* tg;
            *reinterpret_cast<CppType*>(dest) = strtod(buf, &tg);
            return OLAPStatus::OLAP_SUCCESS;
        }

        return NumericFieldtypeTraits<OLAP_FIELD_TYPE_DOUBLE, true>::convert_from(
                dest, src, src_type, mem_pool);
    }
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_DECIMAL>
        : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_DECIMAL> {
    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        CppType* data_ptr = reinterpret_cast<CppType*>(buf);
        return data_ptr->from_string(scan_key);
    }
    static std::string to_string(const void* src) {
        const CppType* data_ptr = reinterpret_cast<const CppType*>(src);
        return data_ptr->to_string();
    }
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
struct FieldTypeTraits<OLAP_FIELD_TYPE_DATE> : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_DATE> {
    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        tm time_tm;
        char* res = strptime(scan_key.c_str(), "%Y-%m-%d", &time_tm);

        if (nullptr != res) {
            int value = (time_tm.tm_year + 1900) * 16 * 32 + (time_tm.tm_mon + 1) * 32 +
                        time_tm.tm_mday;
            *reinterpret_cast<CppType*>(buf) = value;
        } else {
            // 1400 - 01 - 01
            *reinterpret_cast<CppType*>(buf) = 716833;
        }

        return OLAP_SUCCESS;
    }
    static std::string to_string(const void* src) {
        return reinterpret_cast<const CppType*>(src)->to_string();
    }
    static OLAPStatus convert_from(void* dest, const void* src, const TypeInfo* src_type,
                                   MemPool* mem_pool, size_t variable_len = 0) {
        if (src_type->type() == FieldType::OLAP_FIELD_TYPE_DATETIME) {
            using SrcType = typename CppTypeTraits<OLAP_FIELD_TYPE_DATETIME>::CppType;
            SrcType src_value = *reinterpret_cast<const SrcType*>(src);
            //only need part one
            SrcType part1 = (src_value / 1000000L);
            CppType year = static_cast<CppType>((part1 / 10000L) % 10000);
            CppType mon = static_cast<CppType>((part1 / 100) % 100);
            CppType mday = static_cast<CppType>(part1 % 100);
            *reinterpret_cast<CppType*>(dest) = (year << 9) + (mon << 5) + mday;
            return OLAPStatus::OLAP_SUCCESS;
        }

        if (src_type->type() == FieldType::OLAP_FIELD_TYPE_INT) {
            using SrcType = typename CppTypeTraits<OLAP_FIELD_TYPE_INT>::CppType;
            SrcType src_value = *reinterpret_cast<const SrcType*>(src);
            DateTimeValue dt;
            if (!dt.from_date_int64(src_value)) {
                return OLAPStatus::OLAP_ERR_INVALID_SCHEMA;
            }
            CppType year = static_cast<CppType>(src_value / 10000);
            CppType month = static_cast<CppType>((src_value % 10000) / 100);
            CppType day = static_cast<CppType>(src_value % 100);
            *reinterpret_cast<CppType*>(dest) = (year << 9) + (month << 5) + day;
            return OLAPStatus::OLAP_SUCCESS;
        }

        if (src_type->type() == OLAP_FIELD_TYPE_VARCHAR ||
            src_type->type() == OLAP_FIELD_TYPE_CHAR ||
            src_type->type() == OLAP_FIELD_TYPE_STRING) {
            if (src_type->type() == OLAP_FIELD_TYPE_CHAR) {
                prepare_char_before_convert(src);
            }
            using SrcType = typename CppTypeTraits<OLAP_FIELD_TYPE_VARCHAR>::CppType;
            auto src_value = *reinterpret_cast<const SrcType*>(src);
            DateTimeValue dt;
            for (const auto& format : DATE_FORMATS) {
                if (dt.from_date_format_str(format.c_str(), format.length(), src_value.get_data(),
                                            src_value.get_size())) {
                    *reinterpret_cast<CppType*>(dest) =
                            (dt.year() << 9) + (dt.month() << 5) + dt.day();
                    return OLAPStatus::OLAP_SUCCESS;
                }
            }
            return OLAPStatus::OLAP_ERR_INVALID_SCHEMA;
        }

        return OLAPStatus::OLAP_ERR_INVALID_SCHEMA;
    }
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
struct FieldTypeTraits<OLAP_FIELD_TYPE_DATETIME>
        : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_DATETIME> {
    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        tm time_tm;
        char* res = strptime(scan_key.c_str(), "%Y-%m-%d %H:%M:%S", &time_tm);

        if (nullptr != res) {
            CppType value = ((time_tm.tm_year + 1900) * 10000L + (time_tm.tm_mon + 1) * 100L +
                             time_tm.tm_mday) *
                                    1000000L +
                            time_tm.tm_hour * 10000L + time_tm.tm_min * 100L + time_tm.tm_sec;
            *reinterpret_cast<CppType*>(buf) = value;
        } else {
            // 1400 - 01 - 01
            *reinterpret_cast<CppType*>(buf) = 14000101000000L;
        }

        return OLAP_SUCCESS;
    }
    static std::string to_string(const void* src) {
        tm time_tm;
        CppType tmp = *reinterpret_cast<const CppType*>(src);
        CppType part1 = (tmp / 1000000L);
        CppType part2 = (tmp - part1 * 1000000L);

        time_tm.tm_year = static_cast<int>((part1 / 10000L) % 10000) - 1900;
        time_tm.tm_mon = static_cast<int>((part1 / 100) % 100) - 1;
        time_tm.tm_mday = static_cast<int>(part1 % 100);

        time_tm.tm_hour = static_cast<int>((part2 / 10000L) % 10000);
        time_tm.tm_min = static_cast<int>((part2 / 100) % 100);
        time_tm.tm_sec = static_cast<int>(part2 % 100);

        char buf[20] = {'\0'};
        strftime(buf, 20, "%Y-%m-%d %H:%M:%S", &time_tm);
        return std::string(buf);
    }
    static OLAPStatus convert_from(void* dest, const void* src, const TypeInfo* src_type,
                                   MemPool* memPool, size_t variable_len = 0) {
        // when convert date to datetime, automatic padding zero
        if (src_type->type() == FieldType::OLAP_FIELD_TYPE_DATE) {
            using SrcType = typename CppTypeTraits<OLAP_FIELD_TYPE_DATE>::CppType;
            auto value = *reinterpret_cast<const SrcType*>(src);
            int day = static_cast<int>(value & 31);
            int mon = static_cast<int>(value >> 5 & 15);
            int year = static_cast<int>(value >> 9);
            *reinterpret_cast<CppType*>(dest) = (year * 10000L + mon * 100L + day) * 1000000;
            return OLAPStatus::OLAP_SUCCESS;
        }
        return OLAPStatus::OLAP_ERR_INVALID_SCHEMA;
    }
    static void set_to_max(void* buf) {
        // 设置为最大时间，其含义为：9999-12-31 23:59:59
        *reinterpret_cast<CppType*>(buf) = 99991231235959L;
    }
    static void set_to_min(void* buf) { *reinterpret_cast<CppType*>(buf) = 101000000; }
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_CHAR> : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_CHAR> {
    static bool equal(const void* left, const void* right) {
        auto l_slice = reinterpret_cast<const Slice*>(left);
        auto r_slice = reinterpret_cast<const Slice*>(right);
        return *l_slice == *r_slice;
    }
    static int cmp(const void* left, const void* right) {
        auto l_slice = reinterpret_cast<const Slice*>(left);
        auto r_slice = reinterpret_cast<const Slice*>(right);
        return l_slice->compare(*r_slice);
    }
    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        size_t value_len = scan_key.length();
        if (value_len > OLAP_VARCHAR_MAX_LENGTH) {
            LOG(WARNING) << "the len of value string is too long, len=" << value_len
                         << ", max_len=" << OLAP_VARCHAR_MAX_LENGTH;
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }

        auto slice = reinterpret_cast<Slice*>(buf);
        memory_copy(slice->data, scan_key.c_str(), value_len);
        if (slice->size < value_len) {
            /*
             * CHAR type is of fixed length. Size in slice can be modified
             * only if value_len is greater than the fixed length. ScanKey
             * inputted by user may be greater than fixed length.
             */
            slice->size = value_len;
        } else {
            // append \0 to the tail
            memset(slice->data + value_len, 0, slice->size - value_len);
        }
        return OLAP_SUCCESS;
    }
    static std::string to_string(const void* src) {
        auto slice = reinterpret_cast<const Slice*>(src);
        return slice->to_string();
    }

    static void deep_copy(void* dest, const void* src, MemPool* mem_pool) {
        auto l_slice = reinterpret_cast<Slice*>(dest);
        auto r_slice = reinterpret_cast<const Slice*>(src);
        l_slice->data = reinterpret_cast<char*>(mem_pool->allocate(r_slice->size));
        memory_copy(l_slice->data, r_slice->data, r_slice->size);
        l_slice->size = r_slice->size;
    }

    static void copy_object(void* dest, const void* src, MemPool* mem_pool) {
        deep_copy(dest, src, mem_pool);
    }

    static void direct_copy(void* dest, const void* src) {
        auto l_slice = reinterpret_cast<Slice*>(dest);
        auto r_slice = reinterpret_cast<const Slice*>(src);
        memory_copy(l_slice->data, r_slice->data, r_slice->size);
        l_slice->size = r_slice->size;
    }

    // Using field.set_to_max to set varchar/char,not here.
    static void (*set_to_max)(void*);

    static void set_to_min(void* buf) {
        auto slice = reinterpret_cast<Slice*>(buf);
        memset(slice->data, 0, slice->size);
    }

    static void direct_copy_may_cut(void* dest, const void* src) {
        auto l_slice = reinterpret_cast<Slice*>(dest);
        auto r_slice = reinterpret_cast<const Slice*>(src);

        auto min_size =
                MAX_ZONE_MAP_INDEX_SIZE >= r_slice->size ? r_slice->size : MAX_ZONE_MAP_INDEX_SIZE;
        memory_copy(l_slice->data, r_slice->data, min_size);
        l_slice->size = min_size;
    }

    static uint32_t hash_code(const void* data, uint32_t seed) {
        auto slice = reinterpret_cast<const Slice*>(data);
        return HashUtil::hash(slice->data, slice->size, seed);
    }
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_VARCHAR> : public FieldTypeTraits<OLAP_FIELD_TYPE_CHAR> {
    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        size_t value_len = scan_key.length();
        if (value_len > OLAP_VARCHAR_MAX_LENGTH) {
            LOG(WARNING) << "the len of value string is too long, len=" << value_len
                         << ", max_len=" << OLAP_VARCHAR_MAX_LENGTH;
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }

        auto slice = reinterpret_cast<Slice*>(buf);
        memory_copy(slice->data, scan_key.c_str(), value_len);
        slice->size = value_len;
        return OLAP_SUCCESS;
    }

    static OLAPStatus convert_from(void* dest, const void* src, const TypeInfo* src_type,
                                   MemPool* mem_pool, size_t variable_len = 0) {
        assert(variable_len > 0);
        switch (src_type->type()) {
        case OLAP_FIELD_TYPE_TINYINT:
        case OLAP_FIELD_TYPE_SMALLINT:
        case OLAP_FIELD_TYPE_INT:
        case OLAP_FIELD_TYPE_BIGINT:
        case OLAP_FIELD_TYPE_LARGEINT:
        case OLAP_FIELD_TYPE_FLOAT:
        case OLAP_FIELD_TYPE_DOUBLE:
        case OLAP_FIELD_TYPE_DECIMAL: {
            auto result = src_type->to_string(src);
            if (result.size() > variable_len) return OLAP_ERR_INPUT_PARAMETER_ERROR;
            auto slice = reinterpret_cast<Slice*>(dest);
            slice->data = reinterpret_cast<char*>(mem_pool->allocate(result.size()));
            memcpy(slice->data, result.c_str(), result.size());
            slice->size = result.size();
            return OLAP_SUCCESS;
        }
        case OLAP_FIELD_TYPE_CHAR:
            prepare_char_before_convert(src);
            deep_copy(dest, src, mem_pool);
            return OLAP_SUCCESS;
        default:
            return OLAP_ERR_INVALID_SCHEMA;
        }
    }

    static void set_to_min(void* buf) {
        auto slice = reinterpret_cast<Slice*>(buf);
        slice->size = 0;
    }
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_STRING> : public FieldTypeTraits<OLAP_FIELD_TYPE_CHAR> {
    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        size_t value_len = scan_key.length();
        if (value_len > config::string_type_length_soft_limit_bytes) {
            LOG(WARNING) << "the len of value string is too long, len=" << value_len
                         << ", max_len=" << config::string_type_length_soft_limit_bytes;
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }

        auto slice = reinterpret_cast<Slice*>(buf);
        memory_copy(slice->data, scan_key.c_str(), value_len);
        slice->size = value_len;
        return OLAP_SUCCESS;
    }

    static OLAPStatus convert_from(void* dest, const void* src, const TypeInfo* src_type,
                                   MemPool* mem_pool, size_t variable_len = 0) {
        switch (src_type->type()) {
        case OLAP_FIELD_TYPE_TINYINT:
        case OLAP_FIELD_TYPE_SMALLINT:
        case OLAP_FIELD_TYPE_INT:
        case OLAP_FIELD_TYPE_BIGINT:
        case OLAP_FIELD_TYPE_LARGEINT:
        case OLAP_FIELD_TYPE_FLOAT:
        case OLAP_FIELD_TYPE_DOUBLE:
        case OLAP_FIELD_TYPE_DECIMAL: {
            auto result = src_type->to_string(src);
            auto slice = reinterpret_cast<Slice*>(dest);
            slice->data = reinterpret_cast<char*>(mem_pool->allocate(result.size()));
            memcpy(slice->data, result.c_str(), result.size());
            slice->size = result.size();
            return OLAP_SUCCESS;
        }
        case OLAP_FIELD_TYPE_CHAR:
            prepare_char_before_convert(src);
        case OLAP_FIELD_TYPE_VARCHAR:
            deep_copy(dest, src, mem_pool);
            return OLAP_SUCCESS;
        default:
            return OLAP_ERR_INVALID_SCHEMA;
        }
    }

    static void set_to_min(void* buf) {
        auto slice = reinterpret_cast<Slice*>(buf);
        slice->size = 0;
    }
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_HLL> : public FieldTypeTraits<OLAP_FIELD_TYPE_VARCHAR> {
    /*
     * Hyperloglog type only used as value, so
     * cmp/from_string/set_to_max/set_to_min function
     * in this struct has no significance
     */

    // See copy_row_in_memtable() in olap/row.h, will be removed in future.
    static void copy_object(void* dest, const void* src, MemPool* mem_pool) {
        auto dst_slice = reinterpret_cast<Slice*>(dest);
        auto src_slice = reinterpret_cast<const Slice*>(src);
        DCHECK_EQ(src_slice->size, 0);
        dst_slice->data = src_slice->data;
        dst_slice->size = 0;
    }
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_OBJECT> : public FieldTypeTraits<OLAP_FIELD_TYPE_VARCHAR> {
    /*
     * Object type only used as value, so
     * cmp/from_string/set_to_max/set_to_min function
     * in this struct has no significance
     */

    // See `copy_row_in_memtable()` in olap/row.h, will be removed in the future.
    static void copy_object(void* dest, const void* src, MemPool* mem_pool) {
        auto dst_slice = reinterpret_cast<Slice*>(dest);
        auto src_slice = reinterpret_cast<const Slice*>(src);
        DCHECK_EQ(src_slice->size, 0);
        dst_slice->data = src_slice->data;
        dst_slice->size = 0;
    }
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_QUANTILE_STATE>
        : public FieldTypeTraits<OLAP_FIELD_TYPE_VARCHAR> {
    /*
     * quantile_state type only used as value, so
     * cmp/from_string/set_to_max/set_to_min function
     * in this struct has no significance
     */

    // See copy_row_in_memtable() in olap/row.h, will be removed in future.
    static void copy_object(void* dest, const void* src, MemPool* mem_pool) {
        auto dst_slice = reinterpret_cast<Slice*>(dest);
        auto src_slice = reinterpret_cast<const Slice*>(src);
        DCHECK_EQ(src_slice->size, 0);
        dst_slice->data = src_slice->data;
        dst_slice->size = 0;
    }
};

// Instantiate this template to get static access to the type traits.
template <FieldType field_type>
struct TypeTraits : public FieldTypeTraits<field_type> {
    using CppType = typename CppTypeTraits<field_type>::CppType;

    static const FieldType type = field_type;
    static const int32_t size = sizeof(CppType);
};

template <FieldType field_type>
inline const TypeInfo* get_scalar_type_info() {
    static constexpr TypeTraits<field_type> traits;
    static ScalarTypeInfo scalar_type_info(traits);
    return &scalar_type_info;
}

template <FieldType field_type>
inline const TypeInfo* get_collection_type_info() {
    static ArrayTypeInfo collection_type_info(get_scalar_type_info<field_type>());
    return &collection_type_info;
}

// nested array type is unsupported for sub_type of collection
template <>
inline const TypeInfo* get_collection_type_info<OLAP_FIELD_TYPE_ARRAY>() {
    return nullptr;
}

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_TYPES_H
