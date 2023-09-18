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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/IDataType.h
// and modified by Doris

#pragma once

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Types_types.h>
#include <stddef.h>
#include <stdint.h>

#include <boost/core/noncopyable.hpp>
#include <memory>
#include <string>
#include <vector>

#include "common/exception.h"
#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "vec/common/cow.h"
#include "vec/core/types.h"
#include "vec/data_types/serde/data_type_serde.h"

namespace doris {
class PColumnMeta;
enum PGenericType_TypeId : int;

namespace vectorized {

class IDataType;
class IColumn;
class BufferWritable;
class ReadBuffer;

using ColumnPtr = COW<IColumn>::Ptr;
using MutableColumnPtr = COW<IColumn>::MutablePtr;

class Field;

using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = std::vector<DataTypePtr>;

/** Properties of data type.
  * Contains methods for serialization/deserialization.
  * Implementations of this interface represent a data type (example: UInt8)
  *  or parametric family of data types (example: Array(...)).
  *
  * DataType is totally immutable object. You can always share them.
  */
class IDataType : private boost::noncopyable {
public:
    IDataType();
    virtual ~IDataType();

    /// Name of data type (examples: UInt64, Array(String)).
    String get_name() const;

    /// Name of data type family (example: FixedString, Array).
    virtual const char* get_family_name() const = 0;

    /// Data type id. It's used for runtime type checks.
    virtual TypeIndex get_type_id() const = 0;

    virtual TypeDescriptor get_type_as_type_descriptor() const = 0;
    virtual TPrimitiveType::type get_type_as_tprimitive_type() const = 0;
    virtual doris::FieldType get_type_as_field_type() const = 0;

    virtual void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const;
    virtual std::string to_string(const IColumn& column, size_t row_num) const;
    // only for compound type now.
    virtual Status from_string(ReadBuffer& rb, IColumn* column) const;

    // get specific serializer or deserializer
    virtual DataTypeSerDeSPtr get_serde(int nesting_level = 1) const = 0;

protected:
    virtual String do_get_name() const;

public:
    /** Create empty column for corresponding type.
      */
    virtual MutableColumnPtr create_column() const = 0;

    /** Create ColumnConst for corresponding type, with specified size and value.
      */
    ColumnPtr create_column_const(size_t size, const Field& field) const;
    ColumnPtr create_column_const_with_default_value(size_t size) const;

    /** Get default value of data type.
      * It is the "default" default, regardless the fact that a table could contain different user-specified default.
      */
    virtual Field get_default() const = 0;

    virtual Field get_field(const TExprNode& node) const = 0;

    /** Directly insert default value into a column. Default implementation use method IColumn::insert_default.
      * This should be overridden if data type default value differs from column default value (example: Enum data types).
      */
    virtual void insert_default_into(IColumn& column) const;

    /// Checks that two instances belong to the same type
    virtual bool equals(const IDataType& rhs) const = 0;

    /// Various properties on behaviour of data type.

    /** The data type is dependent on parameters and types with different parameters are different.
      * Examples: FixedString(N), Tuple(T1, T2), Nullable(T).
      * Otherwise all instances of the same class are the same types.
      */
    virtual bool get_is_parametric() const = 0;

    /** The data type is dependent on parameters and at least one of them is another type.
      * Examples: Tuple(T1, T2), Nullable(T). But FixedString(N) is not.
      */
    virtual bool have_subtypes() const = 0;

    /** In text formats that render "pretty" tables,
      *  is it better to align value right in table cell.
      * Examples: numbers, even nullable.
      */
    virtual bool should_align_right_in_pretty_formats() const { return false; }

    /** Does formatted value in any text format can contain anything but valid UTF8 sequences.
      * Example: String (because it can contain arbitrary bytes).
      * Counterexamples: numbers, Date, DateTime.
      * For Enum, it depends.
      */
    virtual bool text_can_contain_only_valid_utf8() const { return false; }

    /** Is it possible to compare for less/greater, to calculate min/max?
      * Not necessarily totally comparable. For example, floats are comparable despite the fact that NaNs compares to nothing.
      * The same for nullable of comparable types: they are comparable (but not totally-comparable).
      */
    virtual bool is_comparable() const { return false; }
    /** Values of data type can be summed (possibly with overflow, within the same data type).
      * Example: numbers, even nullable. Not Date/DateTime. Not Enum.
      * Enums can be passed to aggregate function 'sum', but the result is Int64, not Enum, so they are not summable.
      */
    virtual bool is_summable() const { return false; }

    /** Can be used in operations like bit and, bit shift, bit not, etc.
      */
    virtual bool can_be_used_in_bit_operations() const { return false; }

    /** Can be used in boolean context (WHERE, HAVING).
      * UInt8, maybe nullable.
      */
    virtual bool can_be_used_in_boolean_context() const { return false; }

    /** Numbers, Enums, Date, DateTime. Not nullable.
      */
    virtual bool is_value_represented_by_number() const { return false; }

    /** Integers, Enums, Date, DateTime. Not nullable.
      */
    virtual bool is_value_represented_by_integer() const { return false; }

    virtual bool is_object() const { return false; }

    /** Unsigned Integers, Date, DateTime. Not nullable.
      */
    virtual bool is_value_represented_by_unsigned_integer() const { return false; }

    /** Values are unambiguously identified by contents of contiguous memory region,
      *  that can be obtained by IColumn::get_data_at method.
      * Examples: numbers, Date, DateTime, String, FixedString,
      *  and Arrays of numbers, Date, DateTime, FixedString, Enum, but not String.
      *  (because Array(String) values became ambiguous if you concatenate Strings).
      * Counterexamples: Nullable, Tuple.
      */
    virtual bool is_value_unambiguously_represented_in_contiguous_memory_region() const {
        return false;
    }

    virtual bool is_value_unambiguously_represented_in_fixed_size_contiguous_memory_region() const {
        return is_value_represented_by_number();
    }

    /** Example: numbers, Date, DateTime, FixedString, Enum... Nullable and Tuple of such types.
      * Counterexamples: String, Array.
      * It's Ok to return false for AggregateFunction despite the fact that some of them have fixed size state.
      */
    virtual bool have_maximum_size_of_value() const { return false; }

    /** Size in amount of bytes in memory. Throws an exception if not have_maximum_size_of_value.
      */
    virtual size_t get_maximum_size_of_value_in_memory() const {
        return get_size_of_value_in_memory();
    }

    /** Throws an exception if value is not of fixed size.
      */
    virtual size_t get_size_of_value_in_memory() const;

    virtual bool is_nullable() const { return false; }

    /** Is this type can represent only NULL value? (It also implies is_nullable)
      */
    virtual bool only_null() const { return false; }

    /* the data type create from type_null, NULL literal*/
    virtual bool is_null_literal() const { return false; }

    /** If this data type cannot be wrapped in Nullable data type.
      */
    virtual bool can_be_inside_nullable() const { return false; }

    virtual bool low_cardinality() const { return false; }

    /// Strings, Numbers, Date, DateTime, Nullable
    virtual bool can_be_inside_low_cardinality() const { return false; }

    /// Updates avg_value_size_hint for newly read column. Uses to optimize deserialization. Zero expected for first column.
    static void update_avg_value_size_hint(const IColumn& column, double& avg_value_size_hint);

    virtual int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                                      int be_exec_version) const = 0;
    virtual char* serialize(const IColumn& column, char* buf, int be_exec_version) const = 0;
    virtual const char* deserialize(const char* buf, IColumn* column,
                                    int be_exec_version) const = 0;

    virtual void to_pb_column_meta(PColumnMeta* col_meta) const;

    static PGenericType_TypeId get_pdata_type(const IDataType* data_type);

    [[nodiscard]] virtual UInt32 get_precision() const {
        throw Exception(ErrorCode::INTERNAL_ERROR, "type {} not support get_precision", get_name());
    }
    [[nodiscard]] virtual UInt32 get_scale() const {
        throw Exception(ErrorCode::INTERNAL_ERROR, "type {} not support get_scale", get_name());
    }

private:
    friend class DataTypeFactory;
};

/// Some sugar to check data type of IDataType
struct WhichDataType {
    TypeIndex idx;

    WhichDataType(TypeIndex idx_ = TypeIndex::Nothing) : idx(idx_) {}

    WhichDataType(const IDataType& data_type) : idx(data_type.get_type_id()) {}

    WhichDataType(const IDataType* data_type) : idx(data_type->get_type_id()) {}

    WhichDataType(const DataTypePtr& data_type) : idx(data_type->get_type_id()) {}

    bool is_uint8() const { return idx == TypeIndex::UInt8; }
    bool is_uint16() const { return idx == TypeIndex::UInt16; }
    bool is_uint32() const { return idx == TypeIndex::UInt32; }
    bool is_uint64() const { return idx == TypeIndex::UInt64; }
    bool is_uint128() const { return idx == TypeIndex::UInt128; }
    bool is_uint() const {
        return is_uint8() || is_uint16() || is_uint32() || is_uint64() || is_uint128();
    }
    bool is_native_uint() const { return is_uint8() || is_uint16() || is_uint32() || is_uint64(); }

    bool is_int8() const { return idx == TypeIndex::Int8; }
    bool is_int16() const { return idx == TypeIndex::Int16; }
    bool is_int32() const { return idx == TypeIndex::Int32; }
    bool is_int64() const { return idx == TypeIndex::Int64; }
    bool is_int128() const { return idx == TypeIndex::Int128; }
    bool is_int() const {
        return is_int8() || is_int16() || is_int32() || is_int64() || is_int128();
    }
    bool is_int_or_uint() const { return is_int() || is_uint(); }
    bool is_native_int() const { return is_int8() || is_int16() || is_int32() || is_int64(); }

    bool is_decimal32() const { return idx == TypeIndex::Decimal32; }
    bool is_decimal64() const { return idx == TypeIndex::Decimal64; }
    bool is_decimal128() const { return idx == TypeIndex::Decimal128; }
    bool is_decimal128i() const { return idx == TypeIndex::Decimal128I; }
    bool is_decimal256() const { return idx == TypeIndex::Decimal256; }
    bool is_decimal() const {
        return is_decimal32() || is_decimal64() || is_decimal128() || is_decimal128i() ||
               is_decimal256();
    }

    bool is_float32() const { return idx == TypeIndex::Float32; }
    bool is_float64() const { return idx == TypeIndex::Float64; }
    bool is_float() const { return is_float32() || is_float64(); }

    bool is_date() const { return idx == TypeIndex::Date; }
    bool is_date_time() const { return idx == TypeIndex::DateTime; }
    bool is_date_v2() const { return idx == TypeIndex::DateV2; }
    bool is_date_time_v2() const { return idx == TypeIndex::DateTimeV2; }
    bool is_date_or_datetime() const { return is_date() || is_date_time(); }
    bool is_date_v2_or_datetime_v2() const { return is_date_v2() || is_date_time_v2(); }

    bool is_string() const { return idx == TypeIndex::String; }
    bool is_fixed_string() const { return idx == TypeIndex::FixedString; }
    bool is_string_or_fixed_string() const { return is_string() || is_fixed_string(); }

    bool is_json() const { return idx == TypeIndex::JSONB; }

    bool is_array() const { return idx == TypeIndex::Array; }
    bool is_tuple() const { return idx == TypeIndex::Tuple; }
    bool is_struct() const { return idx == TypeIndex::Struct; }
    bool is_map() const { return idx == TypeIndex::Map; }
    bool is_set() const { return idx == TypeIndex::Set; }

    bool is_nothing() const { return idx == TypeIndex::Nothing; }
    bool is_nullable() const { return idx == TypeIndex::Nullable; }
    bool is_function() const { return idx == TypeIndex::Function; }
    bool is_aggregate_function() const { return idx == TypeIndex::AggregateFunction; }
    bool is_variant_type() const { return idx == TypeIndex::VARIANT; }
    bool is_simple() const { return is_int() || is_uint() || is_float() || is_string(); }
};

/// IDataType helpers (alternative for IDataType virtual methods with single point of truth)

inline bool is_date(const DataTypePtr& data_type) {
    return WhichDataType(data_type).is_date();
}
inline bool is_date_v2(const DataTypePtr& data_type) {
    return WhichDataType(data_type).is_date_v2();
}
inline bool is_date_time_v2(const DataTypePtr& data_type) {
    return WhichDataType(data_type).is_date_time_v2();
}
inline bool is_date_or_datetime(const DataTypePtr& data_type) {
    return WhichDataType(data_type).is_date_or_datetime();
}
inline bool is_date_v2_or_datetime_v2(const DataTypePtr& data_type) {
    return WhichDataType(data_type).is_date_v2_or_datetime_v2();
}
inline bool is_decimal(const DataTypePtr& data_type) {
    return WhichDataType(data_type).is_decimal();
}
inline bool is_decimal_v2(const DataTypePtr& data_type) {
    return WhichDataType(data_type).is_decimal128();
}
inline bool is_tuple(const DataTypePtr& data_type) {
    return WhichDataType(data_type).is_tuple();
}
inline bool is_array(const DataTypePtr& data_type) {
    return WhichDataType(data_type).is_array();
}
inline bool is_map(const DataTypePtr& data_type) {
    return WhichDataType(data_type).is_map();
}
inline bool is_struct(const DataTypePtr& data_type) {
    return WhichDataType(data_type).is_struct();
}
inline bool is_nothing(const DataTypePtr& data_type) {
    return WhichDataType(data_type).is_nothing();
}

template <typename T>
bool is_uint8(const T& data_type) {
    return WhichDataType(data_type).is_uint8();
}

template <typename T>
bool is_unsigned_integer(const T& data_type) {
    return WhichDataType(data_type).is_uint();
}

template <typename T>
bool is_integer(const T& data_type) {
    WhichDataType which(data_type);
    return which.is_int() || which.is_uint();
}

template <typename T>
bool is_float(const T& data_type) {
    WhichDataType which(data_type);
    return which.is_float();
}

template <typename T>
bool is_native_number(const T& data_type) {
    WhichDataType which(data_type);
    return which.is_native_int() || which.is_native_uint() || which.is_float();
}

template <typename T>
bool is_number(const T& data_type) {
    WhichDataType which(data_type);
    return which.is_int() || which.is_uint() || which.is_float() || which.is_decimal();
}

template <typename T>
bool is_columned_as_number(const T& data_type) {
    WhichDataType which(data_type);
    return which.is_int() || which.is_uint() || which.is_float() || which.is_date_or_datetime() ||
           which.is_date_v2_or_datetime_v2();
}

template <typename T>
bool is_string(const T& data_type) {
    return WhichDataType(data_type).is_string();
}

template <typename T>
bool is_fixed_string(const T& data_type) {
    return WhichDataType(data_type).is_fixed_string();
}

template <typename T>
bool is_string_or_fixed_string(const T& data_type) {
    return WhichDataType(data_type).is_string_or_fixed_string();
}

inline bool is_not_decimal_but_comparable_to_decimal(const DataTypePtr& data_type) {
    WhichDataType which(data_type);
    return which.is_int() || which.is_uint();
}

inline bool is_compilable_type(const DataTypePtr& data_type) {
    return data_type->is_value_represented_by_number() && !is_decimal(data_type);
}

inline bool is_complex_type(const DataTypePtr& data_type) {
    WhichDataType which(data_type);
    return which.is_array() || which.is_map() || which.is_struct();
}

} // namespace vectorized
} // namespace doris
