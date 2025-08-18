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
#include <type_traits>
#include <vector>

#include "common/exception.h"
#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nothing.h"
#include "vec/columns/column_string.h"
#include "vec/common/cow.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/serde/data_type_serde.h"

namespace doris {
class PColumnMeta;
enum PGenericType_TypeId : int;

namespace vectorized {
#include "common/compile_check_begin.h"
class IDataType;
class IColumn;
class BufferWritable;

using ColumnPtr = COW<IColumn>::Ptr;
using MutableColumnPtr = COW<IColumn>::MutablePtr;

class Field;

using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = std::vector<DataTypePtr>;
constexpr auto SERIALIZED_MEM_SIZE_LIMIT = 256;

template <typename T>
T upper_int32(T size) {
    static_assert(std::is_unsigned_v<T>);
    return T(static_cast<double>(3 + size) / 4.0);
}

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
    virtual const std::string get_family_name() const = 0;
    virtual PrimitiveType get_primitive_type() const = 0;

    virtual doris::FieldType get_storage_field_type() const = 0;

    virtual void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const;
    virtual std::string to_string(const IColumn& column, size_t row_num) const;

    virtual void to_string_batch(const IColumn& column, ColumnString& column_to) const;
    // get specific serializer or deserializer
    virtual DataTypeSerDeSPtr get_serde(int nesting_level = 1) const = 0;

    virtual Status check_column(const IColumn& column) const = 0;

protected:
    virtual String do_get_name() const;

    template <typename Type>
    Status check_column_non_nested_type(const IColumn& column) const {
        if (check_and_get_column_with_const<Type>(column) != nullptr ||
            check_and_get_column<ColumnNothing>(column) != nullptr) {
            return Status::OK();
        }
        return Status::InternalError("Column type {} is not compatible with data type {}",
                                     column.get_name(), get_name());
    }

    template <typename Type>
    Result<const Type*> check_column_nested_type(const IColumn& column) const {
        if (const auto* col = check_and_get_column_with_const<Type>(column)) {
            return col;
        }
        return ResultError(
                Status::InternalError("Column type {} is not compatible with data type {}",
                                      column.get_name(), get_name()));
    }

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

    /// Checks that two instances belong to the same type
    virtual bool equals(const IDataType& rhs) const = 0;

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

    /** Numbers, Enums, Date, DateTime. Not nullable.
      */
    virtual bool is_value_represented_by_number() const { return false; }

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

    /** Example: numbers, Date, DateTime, FixedString, Enum... Nullable and Tuple of such types.
      * Counterexamples: String, Array.
      * It's Ok to return false for AggregateFunction despite the fact that some of them have fixed size state.
      */
    virtual bool have_maximum_size_of_value() const { return false; }

    /** Throws an exception if value is not of fixed size.
      */
    virtual size_t get_size_of_value_in_memory() const;

    virtual bool is_nullable() const { return false; }

    /* the data type create from type_null, NULL literal*/
    virtual bool is_null_literal() const { return false; }

    virtual bool low_cardinality() const { return false; }

    /// Strings, Numbers, Date, DateTime, Nullable
    virtual bool can_be_inside_low_cardinality() const { return false; }

    virtual int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                                      int be_exec_version) const = 0;
    virtual char* serialize(const IColumn& column, char* buf, int be_exec_version) const = 0;
    virtual const char* deserialize(const char* buf, MutableColumnPtr* column,
                                    int be_exec_version) const = 0;

    virtual void to_pb_column_meta(PColumnMeta* col_meta) const;

    static PGenericType_TypeId get_pdata_type(const IDataType* data_type);

    // Return wrapped field with precision and scale, only use in Variant type to get the detailed type info
    virtual FieldWithDataType get_field_with_data_type(const IColumn& column, size_t row_num) const;

    [[nodiscard]] virtual UInt32 get_precision() const { return 0; }
    [[nodiscard]] virtual UInt32 get_scale() const { return 0; }
    virtual void to_protobuf(PTypeDesc* ptype, PTypeNode* node, PScalarType* scalar_type) const {}
    void to_protobuf(PTypeDesc* ptype) const {
        auto node = ptype->add_types();
        node->set_type(TTypeNodeType::SCALAR);
        auto scalar_type = node->mutable_scalar_type();
        scalar_type->set_type(doris::to_thrift(get_primitive_type()));
        to_protobuf(ptype, node, scalar_type);
    }
#ifdef BE_TEST
    // only used in beut
    Status from_string(StringRef& str, IColumn* column) const {
        return get_serde()->default_from_string(str, *column);
    }

    TTypeDesc to_thrift() const {
        TTypeDesc thrift_type;
        to_thrift(thrift_type);
        return thrift_type;
    }
    void to_thrift(TTypeDesc& thrift_type) const {
        thrift_type.types.push_back(TTypeNode());
        TTypeNode& node = thrift_type.types.back();
        to_thrift(thrift_type, node);
    }
    virtual void to_thrift(TTypeDesc& thrift_type, TTypeNode& node) const {
        if (!is_complex_type(get_primitive_type())) {
            node.type = TTypeNodeType::SCALAR;
            node.__set_scalar_type(TScalarType());
            TScalarType& scalar_type = node.scalar_type;
            scalar_type.__set_type(doris::to_thrift(get_primitive_type()));
            if (get_primitive_type() == TYPE_DECIMALV2 || get_primitive_type() == TYPE_DECIMAL32 ||
                get_primitive_type() == TYPE_DECIMAL64 ||
                get_primitive_type() == TYPE_DECIMAL128I ||
                get_primitive_type() == TYPE_DECIMAL256) {
                scalar_type.__set_precision(get_precision());
                scalar_type.__set_scale(get_scale());
            } else if (get_primitive_type() == TYPE_DATETIMEV2) {
                scalar_type.__set_scale(get_scale());
            }
        }
    }
#endif

private:
    friend class DataTypeFactory;
};

// write const_flag and row_num to buf, and return real_need_copy_num
char* serialize_const_flag_and_row_num(const IColumn** column, char* buf,
                                       size_t* real_need_copy_num);
const char* deserialize_const_flag_and_row_num(const char* buf, MutableColumnPtr* column,
                                               size_t* real_have_saved_num);
} // namespace vectorized

#include "common/compile_check_end.h"
} // namespace doris
