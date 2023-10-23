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
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <algorithm>
#include <cinttypes>
#include <limits>
#include <memory>
#include <new>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "gutil/stringprintf.h"
#include "gutil/strings/numbers.h"
#include "olap/decimal12.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/uint24.h"
#include "runtime/collection_value.h"
#include "runtime/map_value.h"
#include "runtime/struct_value.h"
#include "util/binary_cast.hpp"
#include "util/mysql_global.h"
#include "util/slice.h"
#include "util/string_parser.hpp"
#include "util/types.h"
#include "vec/common/arena.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {

namespace segment_v2 {
class ColumnMetaPB;
}

class TabletColumn;

extern bool is_olap_string_type(FieldType field_type);

class TypeInfo;

using TypeInfoPtr = std::unique_ptr<const TypeInfo, void (*)(const TypeInfo*)>;

TypeInfoPtr create_static_type_info_ptr(const TypeInfo* type_info);
TypeInfoPtr create_dynamic_type_info_ptr(const TypeInfo* type_info);

class TypeInfo {
public:
    virtual ~TypeInfo() = default;
    virtual int cmp(const void* left, const void* right) const = 0;

    virtual void deep_copy(void* dest, const void* src, vectorized::Arena* arena) const = 0;

    virtual void direct_copy(void* dest, const void* src) const = 0;

    // Use only in zone map to cut data.
    virtual void direct_copy_may_cut(void* dest, const void* src) const = 0;

    virtual Status from_string(void* buf, const std::string& scan_key, const int precision = 0,
                               const int scale = 0) const = 0;

    virtual std::string to_string(const void* src) const = 0;

    virtual void set_to_max(void* buf) const = 0;
    virtual void set_to_min(void* buf) const = 0;

    virtual size_t size() const = 0;

    virtual FieldType type() const = 0;
};

class ScalarTypeInfo : public TypeInfo {
public:
    int cmp(const void* left, const void* right) const override { return _cmp(left, right); }

    void deep_copy(void* dest, const void* src, vectorized::Arena* arena) const override {
        _deep_copy(dest, src, arena);
    }

    void direct_copy(void* dest, const void* src) const override { _direct_copy(dest, src); }

    void direct_copy_may_cut(void* dest, const void* src) const override {
        _direct_copy_may_cut(dest, src);
    }

    Status from_string(void* buf, const std::string& scan_key, const int precision = 0,
                       const int scale = 0) const override {
        return _from_string(buf, scan_key, precision, scale);
    }

    std::string to_string(const void* src) const override { return _to_string(src); }

    void set_to_max(void* buf) const override { _set_to_max(buf); }
    void set_to_min(void* buf) const override { _set_to_min(buf); }
    size_t size() const override { return _size; }

    FieldType type() const override { return _field_type; }

    template <typename TypeTraitsClass>
    ScalarTypeInfo(TypeTraitsClass t)
            : _cmp(TypeTraitsClass::cmp),
              _deep_copy(TypeTraitsClass::deep_copy),
              _direct_copy(TypeTraitsClass::direct_copy),
              _direct_copy_may_cut(TypeTraitsClass::direct_copy_may_cut),
              _from_string(TypeTraitsClass::from_string),
              _to_string(TypeTraitsClass::to_string),
              _set_to_max(TypeTraitsClass::set_to_max),
              _set_to_min(TypeTraitsClass::set_to_min),
              _size(TypeTraitsClass::size),
              _field_type(TypeTraitsClass::type) {}

private:
    int (*_cmp)(const void* left, const void* right);

    void (*_deep_copy)(void* dest, const void* src, vectorized::Arena* arena);
    void (*_direct_copy)(void* dest, const void* src);
    void (*_direct_copy_may_cut)(void* dest, const void* src);

    Status (*_from_string)(void* buf, const std::string& scan_key, const int precision,
                           const int scale);
    std::string (*_to_string)(const void* src);

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

    void deep_copy(void* dest, const void* src, vectorized::Arena* arena) const override {
        auto dest_value = reinterpret_cast<CollectionValue*>(dest);
        auto src_value = reinterpret_cast<const CollectionValue*>(src);

        if (src_value->length() == 0) {
            new (dest_value) CollectionValue(src_value->length());
            return;
        }

        dest_value->set_length(src_value->length());

        size_t item_size = src_value->length() * _item_size;
        size_t nulls_size = src_value->has_null() ? src_value->length() : 0;
        dest_value->set_data(arena->alloc(item_size + nulls_size));
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

    void direct_copy(void* dest, const void* src) const override {
        auto dest_value = static_cast<CollectionValue*>(dest);
        // NOTICE: The address pointed by null_signs of the dest_value can NOT be modified here.
        auto base = reinterpret_cast<uint8_t*>(dest_value->mutable_null_signs());
        direct_copy(&base, dest, src);
    }

    void direct_copy(uint8_t** base, void* dest, const void* src) const {
        auto dest_value = static_cast<CollectionValue*>(dest);
        auto src_value = static_cast<const CollectionValue*>(src);

        auto nulls_size = src_value->has_null() ? src_value->length() : 0;
        dest_value->set_data(src_value->length() ? (*base + nulls_size) : nullptr);
        dest_value->set_length(src_value->length());
        dest_value->set_has_null(src_value->has_null());
        if (src_value->has_null()) {
            // direct copy null_signs
            dest_value->set_null_signs(reinterpret_cast<bool*>(*base));
            memcpy(dest_value->mutable_null_signs(), src_value->null_signs(), src_value->length());
        }
        *base += nulls_size + src_value->length() * _item_type_info->size();

        // Direct copy item.
        if (_item_type_info->type() == FieldType::OLAP_FIELD_TYPE_ARRAY) {
            for (uint32_t i = 0; i < src_value->length(); ++i) {
                if (dest_value->is_null_at(i)) {
                    continue;
                }
                dynamic_cast<const ArrayTypeInfo*>(_item_type_info.get())
                        ->direct_copy(base, (uint8_t*)(dest_value->mutable_data()) + i * _item_size,
                                      (uint8_t*)(src_value->data()) + i * _item_size);
            }
        } else {
            for (uint32_t i = 0; i < src_value->length(); ++i) {
                if (dest_value->is_null_at(i)) {
                    continue;
                }
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

    void direct_copy_may_cut(void* dest, const void* src) const override { direct_copy(dest, src); }

    Status from_string(void* buf, const std::string& scan_key, const int precision = 0,
                       const int scale = 0) const override {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "ArrayTypeInfo not support from_string");
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

    void deep_copy(void* dest, const void* src, vectorized::Arena* arena) const override {
        DCHECK(false);
    }

    void direct_copy(void* dest, const void* src) const override { CHECK(false); }

    void direct_copy(uint8_t** base, void* dest, const void* src) const { CHECK(false); }

    void direct_copy_may_cut(void* dest, const void* src) const override { direct_copy(dest, src); }

    Status from_string(void* buf, const std::string& scan_key, const int precision = 0,
                       const int scale = 0) const override {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "MapTypeInfo not support from_string");
    }

    std::string to_string(const void* src) const override {
        auto src_ = reinterpret_cast<const MapValue*>(src);
        auto src_key = reinterpret_cast<const CollectionValue*>(src_->key_data());
        auto src_val = reinterpret_cast<const CollectionValue*>(src_->value_data());
        size_t key_slot_size = _key_type_info->size();
        size_t val_slot_size = _value_type_info->size();
        std::string result = "{";

        for (size_t i = 0; i < src_key->length(); ++i) {
            std::string k_s =
                    _key_type_info->to_string((uint8_t*)(src_key->data()) + key_slot_size);
            std::string v_s =
                    _key_type_info->to_string((uint8_t*)(src_val->data()) + val_slot_size);
            result += k_s + ":" + v_s;
            if (i != src_key->length() - 1) {
                result += ", ";
            }
        }
        result += "}";
        return result;
    }

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
        size_t cur = 0;

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

    void deep_copy(void* dest, const void* src, vectorized::Arena* arena) const override {
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
        for (size_t i = 0; i < src_value->size(); ++i) {
            if (src_value->is_null_at(i)) continue;
            allocate_size += _type_infos[i]->size();
        }

        dest_value->set_values((void**)arena->alloc(allocate_size));
        auto ptr = reinterpret_cast<uint8_t*>(dest_value->mutable_values());
        ptr += dest_value->size() * sizeof(*dest_value->values());

        for (size_t i = 0; i < src_value->size(); ++i) {
            dest_value->set_child_value(nullptr, i);
            if (src_value->is_null_at(i)) continue;
            dest_value->set_child_value(ptr, i);
            ptr += _type_infos[i]->size();
        }

        // copy children value
        for (size_t i = 0; i < src_value->size(); ++i) {
            if (src_value->is_null_at(i)) continue;
            _type_infos[i]->deep_copy(dest_value->mutable_child_value(i), src_value->child_value(i),
                                      arena);
        }
    }

    void direct_copy(void* dest, const void* src) const override {
        auto dest_value = static_cast<StructValue*>(dest);
        auto base = reinterpret_cast<uint8_t*>(dest_value->mutable_values());
        direct_copy(&base, dest, src);
    }

    void direct_copy(uint8_t** base, void* dest, const void* src) const {
        auto dest_value = static_cast<StructValue*>(dest);
        auto src_value = static_cast<const StructValue*>(src);

        dest_value->set_size(src_value->size());
        dest_value->set_has_null(src_value->has_null());
        *base += src_value->size() * sizeof(*src_value->values());

        for (size_t i = 0; i < src_value->size(); ++i) {
            dest_value->set_child_value(nullptr, i);
            if (src_value->is_null_at(i)) continue;
            dest_value->set_child_value(*base, i);
            *base += _type_infos[i]->size();
        }

        for (size_t i = 0; i < src_value->size(); ++i) {
            if (dest_value->is_null_at(i)) {
                continue;
            }
            auto dest_address = dest_value->mutable_child_value(i);
            auto src_address = src_value->child_value(i);
            if (_type_infos[i]->type() == FieldType::OLAP_FIELD_TYPE_STRUCT) {
                dynamic_cast<const StructTypeInfo*>(_type_infos[i].get())
                        ->direct_copy(base, dest_address, src_address);
            } else if (_type_infos[i]->type() == FieldType::OLAP_FIELD_TYPE_ARRAY) {
                dynamic_cast<const ArrayTypeInfo*>(_type_infos[i].get())
                        ->direct_copy(base, dest_address, src_address);
            } else {
                if (is_olap_string_type(_type_infos[i]->type())) {
                    auto dest_slice = reinterpret_cast<Slice*>(dest_address);
                    auto src_slice = reinterpret_cast<const Slice*>(src_address);
                    dest_slice->data = reinterpret_cast<char*>(*base);
                    dest_slice->size = src_slice->size;
                    *base += src_slice->size;
                }
                _type_infos[i]->direct_copy(dest_address, src_address);
            }
        }
    }

    void direct_copy_may_cut(void* dest, const void* src) const override { direct_copy(dest, src); }

    Status from_string(void* buf, const std::string& scan_key, const int precision = 0,
                       const int scale = 0) const override {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "StructTypeInfo not support from_string");
    }

    std::string to_string(const void* src) const override {
        auto src_value = reinterpret_cast<const StructValue*>(src);
        std::string result = "{";

        for (size_t i = 0; i < src_value->size(); ++i) {
            std::string field_value = _type_infos[i]->to_string(src_value->child_value(i));
            result += field_value;
            if (i < src_value->size() - 1) {
                result += ", ";
            }
        }
        result += "}";
        return result;
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
    using CppType = bool;
    using UnsignedCppType = bool;
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
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DOUBLE> {
    using CppType = double;
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
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DATETIME> {
    using CppType = int64_t;
    using UnsignedCppType = uint64_t;
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
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_OBJECT> {
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
struct BaseFieldtypeTraits : public CppTypeTraits<field_type> {
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

    static inline void deep_copy(void* dest, const void* src, vectorized::Arena* arena) {
        memcpy(dest, src, sizeof(CppType));
    }

    static inline void direct_copy(void* dest, const void* src) {
        memcpy(dest, src, sizeof(CppType));
    }

    static inline void direct_copy_may_cut(void* dest, const void* src) { direct_copy(dest, src); }

    static inline void set_to_max(void* buf) {
        set_cpp_type_value(buf, type_limit<CppType>::max());
    }

    static inline void set_to_min(void* buf) {
        set_cpp_type_value(buf, type_limit<CppType>::min());
    }

    static std::string to_string(const void* src) {
        return std::to_string(get_cpp_type_value(src));
    }

    static Status from_string(void* buf, const std::string& scan_key, const int precision,
                              const int scale) {
        CppType value = 0;
        if (scan_key.length() > 0) {
            value = static_cast<CppType>(strtol(scan_key.c_str(), nullptr, 10));
        }
        set_cpp_type_value(buf, value);
        return Status::OK();
    }
};

// Using NumericFieldtypeTraits to Derived code for FieldType::OLAP_FIELD_TYPE_XXXINT, FieldType::OLAP_FIELD_TYPE_FLOAT,
// FieldType::OLAP_FIELD_TYPE_DOUBLE, to reduce redundant code
template <FieldType fieldType, bool isArithmetic>
struct NumericFieldtypeTraits : public BaseFieldtypeTraits<fieldType> {
    using CppType = typename CppTypeTraits<fieldType>::CppType;

    static std::string to_string(const void* src) {
        return std::to_string(*reinterpret_cast<const CppType*>(src));
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
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_BOOL>
        : public BaseFieldtypeTraits<FieldType::OLAP_FIELD_TYPE_BOOL> {
    static std::string to_string(const void* src) {
        char buf[1024] = {'\0'};
        snprintf(buf, sizeof(buf), "%d", *reinterpret_cast<const bool*>(src));
        return std::string(buf);
    }
    static void set_to_max(void* buf) { (*(bool*)buf) = true; }
    static void set_to_min(void* buf) { (*(bool*)buf) = false; }
};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_LARGEINT>
        : public NumericFieldtypeTraits<FieldType::OLAP_FIELD_TYPE_LARGEINT, true> {
    static Status from_string(void* buf, const std::string& scan_key, const int precision,
                              const int scale) {
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

        return Status::OK();
    }
    static std::string to_string(const void* src) {
        char buf[1024];
        int128_t value = reinterpret_cast<const PackedInt128*>(src)->value;
        if (value >= std::numeric_limits<int64_t>::min() &&
            value <= std::numeric_limits<int64_t>::max()) {
            snprintf(buf, sizeof(buf), "%" PRId64, (int64_t)value);
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
            } else if (LIKELY(middle > 0)) {
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
    static void deep_copy(void* dest, const void* src, vectorized::Arena* arena) {
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
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_FLOAT>
        : public NumericFieldtypeTraits<FieldType::OLAP_FIELD_TYPE_FLOAT, true> {
    static Status from_string(void* buf, const std::string& scan_key, const int precision,
                              const int scale) {
        CppType value = 0.0f;
        if (scan_key.length() > 0) {
            value = static_cast<CppType>(atof(scan_key.c_str()));
        }
        *reinterpret_cast<CppType*>(buf) = value;
        return Status::OK();
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
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DOUBLE>
        : public NumericFieldtypeTraits<FieldType::OLAP_FIELD_TYPE_DOUBLE, true> {
    static Status from_string(void* buf, const std::string& scan_key, const int precision,
                              const int scale) {
        CppType value = 0.0;
        if (scan_key.length() > 0) {
            value = atof(scan_key.c_str());
        }
        *reinterpret_cast<CppType*>(buf) = value;
        return Status::OK();
    }
    static std::string to_string(const void* src) {
        char buf[1024] = {'\0'};
        int length =
                DoubleToBuffer(*reinterpret_cast<const CppType*>(src), MAX_DOUBLE_STR_LENGTH, buf);
        DCHECK(length >= 0) << "gcvt float failed, float value="
                            << *reinterpret_cast<const CppType*>(src);
        return std::string(buf);
    }
};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DECIMAL>
        : public BaseFieldtypeTraits<FieldType::OLAP_FIELD_TYPE_DECIMAL> {
    static Status from_string(void* buf, const std::string& scan_key, const int precision,
                              const int scale) {
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
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DECIMAL32>
        : public BaseFieldtypeTraits<FieldType::OLAP_FIELD_TYPE_DECIMAL32> {
    static Status from_string(void* buf, const std::string& scan_key, const int precision,
                              const int scale) {
        StringParser::ParseResult result = StringParser::PARSE_SUCCESS;
        int32_t value = StringParser::string_to_decimal<TYPE_DECIMAL32>(
                scan_key.c_str(), scan_key.size(), 9, scale, &result);

        if (result == StringParser::PARSE_FAILURE) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                    "FieldTypeTraits<OLAP_FIELD_TYPE_DECIMAL32>::from_string meet PARSE_FAILURE");
        }
        *reinterpret_cast<int32_t*>(buf) = value;
        return Status::OK();
    }
};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DECIMAL64>
        : public BaseFieldtypeTraits<FieldType::OLAP_FIELD_TYPE_DECIMAL64> {
    static Status from_string(void* buf, const std::string& scan_key, const int precision,
                              const int scale) {
        StringParser::ParseResult result = StringParser::PARSE_SUCCESS;
        int64_t value = StringParser::string_to_decimal<TYPE_DECIMAL64>(
                scan_key.c_str(), scan_key.size(), 18, scale, &result);
        if (result == StringParser::PARSE_FAILURE) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                    "FieldTypeTraits<OLAP_FIELD_TYPE_DECIMAL64>::from_string meet PARSE_FAILURE");
        }
        *reinterpret_cast<int64_t*>(buf) = value;
        return Status::OK();
    }
};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DECIMAL128I>
        : public BaseFieldtypeTraits<FieldType::OLAP_FIELD_TYPE_DECIMAL128I> {
    static Status from_string(void* buf, const std::string& scan_key, const int precision,
                              const int scale) {
        StringParser::ParseResult result = StringParser::PARSE_SUCCESS;
        int128_t value = StringParser::string_to_decimal<TYPE_DECIMAL128I>(
                scan_key.c_str(), scan_key.size(), 38, scale, &result);
        if (result == StringParser::PARSE_FAILURE) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                    "FieldTypeTraits<OLAP_FIELD_TYPE_DECIMAL128I>::from_string meet PARSE_FAILURE");
        }
        *reinterpret_cast<PackedInt128*>(buf) = value;
        return Status::OK();
    }
    static std::string to_string(const void* src) {
        int128_t value = reinterpret_cast<const PackedInt128*>(src)->value;
        fmt::memory_buffer buffer;
        fmt::format_to(buffer, "{}", value);
        return std::string(buffer.data(), buffer.size());
    }
};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DATE>
        : public BaseFieldtypeTraits<FieldType::OLAP_FIELD_TYPE_DATE> {
    static Status from_string(void* buf, const std::string& scan_key, const int precision,
                              const int scale) {
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

        return Status::OK();
    }
    static std::string to_string(const void* src) {
        return reinterpret_cast<const CppType*>(src)->to_string();
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
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DATEV2>
        : public BaseFieldtypeTraits<FieldType::OLAP_FIELD_TYPE_DATEV2> {
    static Status from_string(void* buf, const std::string& scan_key, const int precision,
                              const int scale) {
        tm time_tm;
        char* res = strptime(scan_key.c_str(), "%Y-%m-%d", &time_tm);

        if (nullptr != res) {
            uint32_t value =
                    ((time_tm.tm_year + 1900) << 9) | ((time_tm.tm_mon + 1) << 5) | time_tm.tm_mday;
            *reinterpret_cast<CppType*>(buf) = value;
        } else {
            *reinterpret_cast<CppType*>(buf) = MIN_DATE_V2;
        }

        return Status::OK();
    }
    static std::string to_string(const void* src) {
        CppType tmp = *reinterpret_cast<const CppType*>(src);
        DateV2Value<DateV2ValueType> value =
                binary_cast<CppType, DateV2Value<DateV2ValueType>>(tmp);
        string format = "%Y-%m-%d";
        string res;
        res.resize(12);
        res.reserve(12);
        value.to_format_string(format.c_str(), format.size(), res.data());
        return res;
    }

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
        : public BaseFieldtypeTraits<FieldType::OLAP_FIELD_TYPE_DATETIMEV2> {
    static Status from_string(void* buf, const std::string& scan_key, const int precision,
                              const int scale) {
        DateV2Value<DateTimeV2ValueType> datetimev2_value;
        std::string date_format = "%Y-%m-%d %H:%i:%s.%f";

        if (datetimev2_value.from_date_format_str(date_format.data(), date_format.size(),
                                                  scan_key.data(), scan_key.size())) {
            *reinterpret_cast<CppType*>(buf) = datetimev2_value.to_date_int_val();
        } else {
            *reinterpret_cast<CppType*>(buf) = MIN_DATETIME_V2;
        }

        return Status::OK();
    }
    static std::string to_string(const void* src) {
        CppType tmp = *reinterpret_cast<const CppType*>(src);
        DateV2Value<DateTimeV2ValueType> value =
                binary_cast<CppType, DateV2Value<DateTimeV2ValueType>>(tmp);
        string format = "%Y-%m-%d %H:%i:%s.%f";
        string res;
        res.resize(30);
        res.reserve(30);
        value.to_format_string(format.c_str(), format.size(), res.data());
        return res;
    }

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
        : public BaseFieldtypeTraits<FieldType::OLAP_FIELD_TYPE_DATETIME> {
    static Status from_string(void* buf, const std::string& scan_key, const int precision,
                              const int scale) {
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

        return Status::OK();
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

    static void set_to_max(void* buf) {
        // 设置为最大时间，其含义为：9999-12-31 23:59:59
        *reinterpret_cast<CppType*>(buf) = 99991231235959L;
    }
    static void set_to_min(void* buf) { *reinterpret_cast<CppType*>(buf) = 101000000; }
};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_CHAR>
        : public BaseFieldtypeTraits<FieldType::OLAP_FIELD_TYPE_CHAR> {
    static int cmp(const void* left, const void* right) {
        auto l_slice = reinterpret_cast<const Slice*>(left);
        auto r_slice = reinterpret_cast<const Slice*>(right);
        return l_slice->compare(*r_slice);
    }
    static Status from_string(void* buf, const std::string& scan_key, const int precision,
                              const int scale) {
        size_t value_len = scan_key.length();
        if (value_len > OLAP_VARCHAR_MAX_LENGTH) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                    "the len of value string is too long, len={}, max_len={}", value_len,
                    OLAP_VARCHAR_MAX_LENGTH);
        }

        auto slice = reinterpret_cast<Slice*>(buf);
        memcpy(slice->data, scan_key.c_str(), value_len);
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
        return Status::OK();
    }
    static std::string to_string(const void* src) {
        auto slice = reinterpret_cast<const Slice*>(src);
        return slice->to_string();
    }

    static void deep_copy(void* dest, const void* src, vectorized::Arena* arena) {
        auto l_slice = reinterpret_cast<Slice*>(dest);
        auto r_slice = reinterpret_cast<const Slice*>(src);
        l_slice->data = arena->alloc(r_slice->size);
        memcpy(l_slice->data, r_slice->data, r_slice->size);
        l_slice->size = r_slice->size;
    }

    static void direct_copy(void* dest, const void* src) {
        auto l_slice = reinterpret_cast<Slice*>(dest);
        auto r_slice = reinterpret_cast<const Slice*>(src);
        memcpy(l_slice->data, r_slice->data, r_slice->size);
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
        memcpy(l_slice->data, r_slice->data, min_size);
        l_slice->size = min_size;
    }
};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_VARCHAR>
        : public FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_CHAR> {
    static Status from_string(void* buf, const std::string& scan_key, const int precision,
                              const int scale) {
        size_t value_len = scan_key.length();
        if (value_len > OLAP_VARCHAR_MAX_LENGTH) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                    "the len of value string is too long, len={}, max_len={}", value_len,
                    OLAP_VARCHAR_MAX_LENGTH);
        }

        auto slice = reinterpret_cast<Slice*>(buf);
        memcpy(slice->data, scan_key.c_str(), value_len);
        slice->size = value_len;
        return Status::OK();
    }

    static void set_to_min(void* buf) {
        auto slice = reinterpret_cast<Slice*>(buf);
        slice->size = 0;
    }
};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_STRING>
        : public FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_CHAR> {
    static Status from_string(void* buf, const std::string& scan_key, const int precision,
                              const int scale) {
        size_t value_len = scan_key.length();
        if (value_len > config::string_type_length_soft_limit_bytes) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                    "the len of value string is too long, len={}, max_len={}", value_len,
                    config::string_type_length_soft_limit_bytes);
        }

        auto slice = reinterpret_cast<Slice*>(buf);
        memcpy(slice->data, scan_key.c_str(), value_len);
        slice->size = value_len;
        return Status::OK();
    }

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

    static Status from_string(void* buf, const std::string& scan_key, const int precision,
                              const int scale) {
        // TODO support schema change
        return Status::Error<ErrorCode::INVALID_SCHEMA>(
                "FieldTypeTraits<OLAP_FIELD_TYPE_JSONB> not support from_string");
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
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_OBJECT>
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

} // namespace doris
