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

#ifndef DORIS_BE_RUNTIME_TYPES_H
#define DORIS_BE_RUNTIME_TYPES_H

#include <string>
#include <vector>

#include "common/config.h"
#include "gen_cpp/Types_types.h" // for TPrimitiveType
#include "gen_cpp/types.pb.h"    // for PTypeDesc
#include "olap/hll.h"
#include "runtime/collection_value.h"
#include "runtime/primitive_type.h"
#include "thrift/protocol/TDebugProtocol.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris {

// Describes a type. Includes the enum, children types, and any type-specific metadata
// (e.g. precision and scale for decimals).
// TODO for 2.3: rename to TypeDescriptor
struct TypeDescriptor {
    PrimitiveType type;
    /// Only set if type == TYPE_CHAR or type == TYPE_VARCHAR
    int len;
    static const int MAX_VARCHAR_LENGTH = OLAP_VARCHAR_MAX_LENGTH;
    static const int MAX_CHAR_LENGTH = 255;
    static const int MAX_CHAR_INLINE_LENGTH = 128;

    /// Only set if type == TYPE_DECIMAL
    int precision;
    int scale;

    /// Must be kept in sync with FE's max precision/scale.
    static const int MAX_PRECISION = 38;
    static const int MAX_SCALE = MAX_PRECISION;

    /// The maximum precision representable by a 4-byte decimal (Decimal4Value)
    static const int MAX_DECIMAL4_PRECISION = 9;
    /// The maximum precision representable by a 8-byte decimal (Decimal8Value)
    static const int MAX_DECIMAL8_PRECISION = 18;

    /// Empty for scalar types
    std::vector<TypeDescriptor> children;

    /// Only set if type == TYPE_STRUCT. The field name of each child.
    std::vector<std::string> field_names;

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

    inline bool is_string_type() const {
        return type == TYPE_VARCHAR || type == TYPE_CHAR || type == TYPE_HLL ||
               type == TYPE_OBJECT || type == TYPE_QUANTILE_STATE || type == TYPE_STRING;
    }

    inline bool is_date_type() const { return type == TYPE_DATE || type == TYPE_DATETIME; }

    inline bool is_decimal_type() const { return (type == TYPE_DECIMALV2); }

    inline bool is_datetime_type() const { return type == TYPE_DATETIME; }

    inline bool is_var_len_string_type() const {
        return type == TYPE_VARCHAR || type == TYPE_HLL || type == TYPE_CHAR ||
               type == TYPE_OBJECT || type == TYPE_QUANTILE_STATE || type == TYPE_STRING;
    }

    inline bool is_complex_type() const {
        return type == TYPE_STRUCT || type == TYPE_ARRAY || type == TYPE_MAP;
    }

    inline bool is_collection_type() const { return type == TYPE_ARRAY || type == TYPE_MAP; }

    /// Returns the byte size of this type.  Returns 0 for variable length types.
    inline int get_byte_size() const {
        switch (type) {
        case TYPE_ARRAY:
        case TYPE_MAP:
        case TYPE_VARCHAR:
        case TYPE_HLL:
        case TYPE_OBJECT:
        case TYPE_QUANTILE_STATE:
        case TYPE_STRING:
            return 0;

        case TYPE_NULL:
        case TYPE_BOOLEAN:
        case TYPE_TINYINT:
            return 1;

        case TYPE_SMALLINT:
            return 2;

        case TYPE_INT:
        case TYPE_FLOAT:
            return 4;

        case TYPE_BIGINT:
        case TYPE_DOUBLE:
            return 8;

        case TYPE_LARGEINT:
        case TYPE_DATETIME:
        case TYPE_DATE:
        case TYPE_DECIMALV2:
            return 16;

        case INVALID_TYPE:
        default:
            DCHECK(false);
        }
        return 0;
    }

    /// Returns the size of a slot for this type.
    inline int get_slot_size() const {
        switch (type) {
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_HLL:
        case TYPE_OBJECT:
        case TYPE_QUANTILE_STATE:
        case TYPE_STRING:
            return sizeof(StringValue);

        case TYPE_NULL:
        case TYPE_BOOLEAN:
        case TYPE_TINYINT:
            return 1;

        case TYPE_SMALLINT:
            return 2;

        case TYPE_INT:
        case TYPE_FLOAT:
            return 4;

        case TYPE_BIGINT:
        case TYPE_DOUBLE:
        case TYPE_TIME:
            return 8;

        case TYPE_LARGEINT:
            return sizeof(__int128);

        case TYPE_DATE:
        case TYPE_DATETIME:
            // This is the size of the slot, the actual size of the data is 12.
            return sizeof(DateTimeValue);

        case TYPE_DECIMALV2:
            return 16;

        case TYPE_ARRAY:
            return sizeof(CollectionValue);

        case INVALID_TYPE:
        default:
            DCHECK(false);
        }
        // For llvm complain
        return -1;
    }

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

} // namespace doris

#endif
