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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/convert_field_to_type.cpp
// and modified by Doris

#include "vec/data_types/convert_field_to_type.h"

#include <fmt/format.h>
#include <glog/logging.h>
#include <stddef.h>

#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <vector>

#include "common/exception.h"
#include "common/status.h"
#include "vec/common/field_visitors.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/accurate_comparison.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris {
namespace vectorized {
struct UInt128;
} // namespace vectorized
} // namespace doris

// #include "vec/data_types/data_type_tuple.h"
namespace doris::vectorized {
/** Checking for a `Field from` of `From` type falls to a range of values of type `To`.
  * `From` and `To` - numeric types. They can be floating-point types.
  * `From` is one of UInt64, Int64, Float64,
  *  whereas `To` can also be 8, 16, 32 bit.
  *
  * If falls into a range, then `from` is converted to the `Field` closest to the `To` type.
  * If not, return Field(Null).
  */

/** simple types of implementation of visitor to string*/
// TODO support more types
class FieldVisitorToStringSimple : public StaticVisitor<String> {
public:
    String operator()(const Null& x) const { return "NULL"; }
    String operator()(const UInt64& x) const { return std::to_string(x); }
    String operator()(const Int64& x) const { return std::to_string(x); }
    String operator()(const Float64& x) const { return std::to_string(x); }
    String operator()(const String& x) const { return x; }
    [[noreturn]] String operator()(const UInt128& x) const { LOG(FATAL) << "not implemeted"; }
    [[noreturn]] String operator()(const Array& x) const { LOG(FATAL) << "not implemeted"; }
    [[noreturn]] String operator()(const Tuple& x) const { LOG(FATAL) << "not implemeted"; }
    [[noreturn]] String operator()(const DecimalField<Decimal32>& x) const {
        LOG(FATAL) << "not implemeted";
    }
    [[noreturn]] String operator()(const DecimalField<Decimal64>& x) const {
        LOG(FATAL) << "not implemeted";
    }
    [[noreturn]] String operator()(const DecimalField<Decimal128>& x) const {
        LOG(FATAL) << "not implemeted";
    }
    [[noreturn]] String operator()(const DecimalField<Decimal128I>& x) const {
        LOG(FATAL) << "not implemeted";
    }
    [[noreturn]] String operator()(const DecimalField<Decimal256>& x) const {
        LOG(FATAL) << "not implemeted";
    }
};

namespace {
template <typename From, typename To>
Field convert_numeric_type_impl(const Field& from) {
    To result;
    if (!accurate::convertNumeric(from.get<From>(), result)) {
        return {};
    }
    return result;
}

template <typename To>
void convert_numric_type(const Field& from, const IDataType& type, Field* to) {
    if (from.get_type() == Field::Types::UInt64) {
        *to = convert_numeric_type_impl<UInt64, To>(from);
    } else if (from.get_type() == Field::Types::Int64) {
        *to = convert_numeric_type_impl<Int64, To>(from);
    } else if (from.get_type() == Field::Types::Float64) {
        *to = convert_numeric_type_impl<Float64, To>(from);
    } else if (from.get_type() == Field::Types::UInt128) {
        // *to = convert_numeric_type_impl<UInt128, To>(from);
    } else if (from.get_type() == Field::Types::Int128) {
        *to = convert_numeric_type_impl<Int128, To>(from);
    } else {
        throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                               "Type mismatch in IN or VALUES section. Expected: {}. Got: {}",
                               type.get_name(), from.get_type());
    }
}

void convert_field_to_typeImpl(const Field& src, const IDataType& type,
                               const IDataType* from_type_hint, Field* to) {
    if (from_type_hint && from_type_hint->equals(type)) {
        *to = src;
        return;
    }
    WhichDataType which_type(type);
    // TODO add more types
    if (type.is_value_represented_by_number() && src.get_type() != Field::Types::String) {
        if (which_type.is_uint8()) {
            return convert_numric_type<UInt8>(src, type, to);
        }
        if (which_type.is_uint16()) {
            return convert_numric_type<UInt16>(src, type, to);
        }
        if (which_type.is_uint32()) {
            return convert_numric_type<UInt32>(src, type, to);
        }
        if (which_type.is_uint64()) {
            return convert_numric_type<UInt64>(src, type, to);
        }
        if (which_type.is_uint128()) {
            // return convert_numric_type<UInt128>(src, type, to);
        }
        if (which_type.is_int8()) {
            return convert_numric_type<Int8>(src, type, to);
        }
        if (which_type.is_int16()) {
            return convert_numric_type<Int16>(src, type, to);
        }
        if (which_type.is_int32()) {
            return convert_numric_type<Int32>(src, type, to);
        }
        if (which_type.is_int64()) {
            return convert_numric_type<Int64>(src, type, to);
        }
        if (which_type.is_int128()) {
            return convert_numric_type<Int128>(src, type, to);
        }
        if (which_type.is_float32()) {
            return convert_numric_type<Float32>(src, type, to);
        }
        if (which_type.is_float64()) {
            return convert_numric_type<Float64>(src, type, to);
        }
        if ((which_type.is_date() || which_type.is_date_time()) &&
            src.get_type() == Field::Types::UInt64) {
            /// We don't need any conversion UInt64 is under type of Date and DateTime
            *to = src;
            return;
        }
    } else if (which_type.is_string_or_fixed_string()) {
        if (src.get_type() == Field::Types::String) {
            *to = src;
            return;
        }
        // TODO this is a very simple translator, support more complex types
        *to = apply_visitor(FieldVisitorToStringSimple(), src);
        return;
    } else if (const DataTypeArray* type_array = typeid_cast<const DataTypeArray*>(&type)) {
        if (src.get_type() == Field::Types::Array) {
            const Array& src_arr = src.get<Array>();
            size_t src_arr_size = src_arr.size();
            const auto& element_type = *(type_array->get_nested_type());
            Array res(src_arr_size);
            for (size_t i = 0; i < src_arr_size; ++i) {
                convert_field_to_type(src_arr[i], element_type, &res[i]);
                if (res[i].is_null() && !element_type.is_nullable()) {
                    throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "Cannot convert NULL to {}",
                                           element_type.get_name());
                }
            }
            *to = Field(res);
            return;
        }
    }
    // else if (const DataTypeTuple* type_tuple = typeid_cast<const DataTypeTuple*>(&type)) {
    //     if (src.get_type() == Field::Types::Tuple) {
    //         const auto& src_tuple = src.get<Tuple>();
    //         size_t src_tuple_size = src_tuple.size();
    //         size_t dst_tuple_size = type_tuple->get_elements().size();
    //         if (dst_tuple_size != src_tuple_size) {
    //             return Status::InvalidArgument("Bad size of tuple in IN or VALUES section");
    //         }
    //         Tuple res(dst_tuple_size);
    //         bool have_unconvertible_element = false;
    //         for (size_t i = 0; i < dst_tuple_size; ++i) {
    //             const auto& element_type = *(type_tuple->get_elements()[i]);
    //             RETURN_IF_ERROR(convert_field_to_type(src_tuple[i], element_type, &res[i]));
    //             if (!res[i].is_null() || element_type.is_nullable()) continue;
    //             /*
    //              * Either the source element was Null, or the conversion did not
    //              * succeed, because the source and the requested types of the
    //              * element are compatible, but the value is not convertible
    //              * (e.g. trying to convert -1 from Int8 to UInt8). In these
    //              * cases, consider the whole tuple also compatible but not
    //              * convertible. According to the specification of this function,
    //              * we must return Null in this case.
    //              *
    //              * The following elements might be not even compatible, so it
    //              * makes sense to check them to detect user errors. Remember
    //              * that there is an unconvertible element, and try to process
    //              * the remaining ones. The convert_field_to_type for each element
    //              * will throw if it detects incompatibility.
    //              */
    //             have_unconvertible_element = true;
    //         }
    //         if (have_unconvertible_element) {
    //             return Status::InvalidArgument(fmt::format("Cannot convert {} to {}",
    //                                                        src.get_type_name(), type.get_name()));
    //         }
    //         *to = Field(res);
    //         return Status::OK();
    //     }
    // }
    throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                           "Type mismatch in IN or VALUES section. Expected: {}. Got: {}",
                           type.get_name(), src.get_type());
}
} // namespace
void convert_field_to_type(const Field& from_value, const IDataType& to_type, Field* to,
                           const IDataType* from_type_hint) {
    if (from_value.is_null()) {
        *to = from_value;
        return;
    }
    if (from_type_hint && from_type_hint->equals(to_type)) {
        *to = from_value;
        return;
    }
    if (const auto* nullable_type = typeid_cast<const DataTypeNullable*>(&to_type)) {
        const IDataType& nested_type = *nullable_type->get_nested_type();
        /// NULL remains NULL after any conversion.
        if (WhichDataType(nested_type).is_nothing()) {
            *to = {};
            return;
        }
        if (from_type_hint && from_type_hint->equals(nested_type)) {
            *to = from_value;
            return;
        }
        return convert_field_to_typeImpl(from_value, nested_type, from_type_hint, to);
    } else {
        return convert_field_to_typeImpl(from_value, to_type, from_type_hint, to);
    }
}
} // namespace doris::vectorized