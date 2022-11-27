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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/types.h
// and modified by Doris

#pragma once

#include <string>
#include <vector>

#include "common/config.h"
#include "runtime/primitive_type.h"

namespace doris {

extern const int HLL_COLUMN_DEFAULT_LEN;

struct TPrimitiveType;
class PTypeDesc;

// Describes a type. Includes the enum, children types, and any type-specific metadata
// (e.g. precision and scale for decimals).
// TODO for 2.3: rename to TypeDescriptor
struct TypeDescriptor {
    PrimitiveType type;
    /// Only set if type == TYPE_CHAR or type == TYPE_VARCHAR
    int len;
    static constexpr int MAX_VARCHAR_LENGTH = OLAP_VARCHAR_MAX_LENGTH;
    static constexpr int MAX_CHAR_LENGTH = 255;
    static constexpr int MAX_CHAR_INLINE_LENGTH = 128;

    /// Only set if type == TYPE_DECIMAL
    int precision;
    int scale;

    /// Must be kept in sync with FE's max precision/scale.
    static constexpr int MAX_PRECISION = 38;
    static constexpr int MAX_SCALE = MAX_PRECISION;

    /// The maximum precision representable by a 4-byte decimal (Decimal4Value)
    static constexpr int MAX_DECIMAL4_PRECISION = 9;
    /// The maximum precision representable by a 8-byte decimal (Decimal8Value)
    static constexpr int MAX_DECIMAL8_PRECISION = 18;

    // Empty for scalar types
    std::vector<TypeDescriptor> children;

    // Only set if type == TYPE_STRUCT. The field name of each child.
    std::vector<std::string> field_names;

    // Used for complex types only.
    bool contains_null = true;

    TypeDescriptor() : type(INVALID_TYPE), len(-1), precision(-1), scale(-1) {}

    // explicit TypeDescriptor(PrimitiveType type) :
    TypeDescriptor(PrimitiveType type) : type(type), len(-1), precision(-1), scale(-1) {
        if (type == TYPE_DECIMALV2) {
            precision = 27;
            scale = 9;
        }
    }

    static TypeDescriptor create_char_type(int len) {
        DCHECK_GE(len, 1);
        DCHECK_LE(len, MAX_CHAR_LENGTH);
        TypeDescriptor ret;
        ret.type = TYPE_CHAR;
        ret.len = len;
        return ret;
    }

    static TypeDescriptor create_varchar_type(int len) {
        DCHECK_GE(len, 1);
        DCHECK_LE(len, MAX_VARCHAR_LENGTH);
        TypeDescriptor ret;
        ret.type = TYPE_VARCHAR;
        ret.len = len;
        return ret;
    }

    static TypeDescriptor create_string_type() {
        TypeDescriptor ret;
        ret.type = TYPE_STRING;
        ret.len = config::string_type_length_soft_limit_bytes;
        return ret;
    }

    static TypeDescriptor create_hll_type() {
        TypeDescriptor ret;
        ret.type = TYPE_HLL;
        ret.len = HLL_COLUMN_DEFAULT_LEN;
        return ret;
    }

    static TypeDescriptor create_decimalv2_type(int precision, int scale) {
        DCHECK_LE(precision, MAX_PRECISION);
        DCHECK_LE(scale, MAX_SCALE);
        DCHECK_GE(precision, 0);
        DCHECK_LE(scale, precision);
        TypeDescriptor ret;
        ret.type = TYPE_DECIMALV2;
        ret.precision = precision;
        ret.scale = scale;
        return ret;
    }

    static TypeDescriptor from_thrift(const TTypeDesc& t) {
        int idx = 0;
        TypeDescriptor result(t.types, &idx);
        DCHECK_EQ(idx, t.types.size() - 1);
        return result;
    }

    static TypeDescriptor from_protobuf(const PTypeDesc& ptype) {
        int idx = 0;
        TypeDescriptor result(ptype.types(), &idx);
        DCHECK_EQ(idx, ptype.types_size() - 1);
        return result;
    }

    bool operator==(const TypeDescriptor& o) const {
        if (type != o.type) {
            return false;
        }
        if (children != o.children) {
            return false;
        }
        if (type == TYPE_CHAR) {
            return len == o.len;
        }
        if (type == TYPE_DECIMALV2) {
            return precision == o.precision && scale == o.scale;
        }
        return true;
    }

    bool operator!=(const TypeDescriptor& other) const { return !(*this == other); }

    TTypeDesc to_thrift() const {
        TTypeDesc thrift_type;
        to_thrift(&thrift_type);
        return thrift_type;
    }

    void to_protobuf(PTypeDesc* ptype) const;

    bool is_string_type() const {
        return type == TYPE_VARCHAR || type == TYPE_CHAR || type == TYPE_HLL ||
               type == TYPE_OBJECT || type == TYPE_QUANTILE_STATE || type == TYPE_STRING;
    }

    bool is_date_type() const { return type == TYPE_DATE || type == TYPE_DATETIME; }

    bool is_date_v2_type() const { return type == TYPE_DATEV2; }
    bool is_datetime_v2_type() const { return type == TYPE_DATETIMEV2; }

    bool is_decimal_v2_type() const { return type == TYPE_DECIMALV2; }

    bool is_decimal_v3_type() const {
        return (type == TYPE_DECIMAL32) || (type == TYPE_DECIMAL64) || (type == TYPE_DECIMAL128I);
    }

    bool is_datetime_type() const { return type == TYPE_DATETIME; }

    bool is_var_len_string_type() const {
        return type == TYPE_VARCHAR || type == TYPE_HLL || type == TYPE_CHAR ||
               type == TYPE_OBJECT || type == TYPE_QUANTILE_STATE || type == TYPE_STRING;
    }

    bool is_complex_type() const {
        return type == TYPE_STRUCT || type == TYPE_ARRAY || type == TYPE_MAP;
    }

    bool is_collection_type() const { return type == TYPE_ARRAY || type == TYPE_MAP; }

    bool is_array_type() const { return type == TYPE_ARRAY; }

    bool is_bitmap_type() const { return type == TYPE_OBJECT; }

    /// Returns the byte size of this type.  Returns 0 for variable length types.
    int get_byte_size() const { return ::doris::get_byte_size(type); }

    int get_slot_size() const { return ::doris::get_slot_size(type); }

    static inline int get_decimal_byte_size(int precision) {
        DCHECK_GT(precision, 0);
        if (precision <= MAX_DECIMAL4_PRECISION) {
            return 4;
        }
        if (precision <= MAX_DECIMAL8_PRECISION) {
            return 8;
        }
        return 16;
    }

    std::string debug_string() const;

private:
    /// Used to create a possibly nested type from the flattened Thrift representation.
    ///
    /// 'idx' is an in/out parameter that is initially set to the index of the type in
    /// 'types' being constructed, and is set to the index of the next type in 'types' that
    /// needs to be processed (or the size 'types' if all nodes have been processed).
    TypeDescriptor(const std::vector<TTypeNode>& types, int* idx);
    TypeDescriptor(const google::protobuf::RepeatedPtrField<PTypeNode>& types, int* idx);

    /// Recursive implementation of ToThrift() that populates 'thrift_type' with the
    /// TTypeNodes for this type and its children.
    void to_thrift(TTypeDesc* thrift_type) const;
};

std::ostream& operator<<(std::ostream& os, const TypeDescriptor& type);

TTypeDesc create_type_desc(PrimitiveType type, int precision = 0, int scale = 0);

} // namespace doris
