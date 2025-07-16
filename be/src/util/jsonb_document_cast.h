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

#include "jsonb_document.h"
#include "runtime/primitive_type.h"
#include "vec/common/string_ref.h"

namespace doris {

struct JsonbCast {
    // 目前这个只是一个demo
    // 如果我们未来有了行级别的 doris cast ， 那我们就可以在这里调用
    // 如果没有，那在这里枚举一下也不是不行
    template <PrimitiveType from, PrimitiveType to>
    typename PrimitiveTypeTraits<to>::ColumnItemType static fake_doris_cast(
            const typename PrimitiveTypeTraits<from>::CppType& value) {
        if constexpr (from == to) {
            if constexpr (from == TYPE_STRING) {
                return value.to_string();
            } else {
                return value;
            }
        } else {
            return typename PrimitiveTypeTraits<to>::ColumnItemType {};
        }
    }

    constexpr bool static check_get_column_type(PrimitiveType PT) {
        return is_int_or_bool(PT) || is_float_or_double(PT) || is_decimal(PT) || PT == TYPE_STRING;
    }

    // Get a doris value from a jsonb
    // For example, get an int32_t from a JsonbValue::T_Int32
    // For example, get a std::string from a JsonbValue::T_String
    // At present, only support
    // JsonbValue::T_Int8, JsonbValue::T_Int16, JsonbValue::T_Int32,
    // JsonbValue::T_Int64, JsonbValue::T_Int128, JsonbValue::T_Float,
    // JsonbValue::T_Double, JsonbValue::T_Decimal32,
    // JsonbValue::T_Decimal64, JsonbValue::T_Decimal128,
    // JsonbValue::T_Decimal256
    // JsonbValue::T_String
    // That is, the scare type plus a String type
    template <PrimitiveType PT>
    typename PrimitiveTypeTraits<PT>::ColumnItemType static cast_jsonb_to_doris_value(
            const JsonbValue* jsonb_value) {
        static_assert(check_get_column_type(PT),
                      "Unsupported PrimitiveType for cast_json_to_doris_type");

        switch (jsonb_value->type) {
        case JsonbType::T_True:
        case JsonbType::T_False: {
            return fake_doris_cast<TYPE_BOOLEAN, PT>(jsonb_value->isTrue());
        }
        case JsonbType::T_Int8: {
            auto val = jsonb_value->unpack<JsonbInt8Val>()->val();
            return fake_doris_cast<TYPE_TINYINT, PT>(val);
        }
        case JsonbType::T_Int16: {
            auto val = jsonb_value->unpack<JsonbInt16Val>()->val();
            return fake_doris_cast<TYPE_SMALLINT, PT>(val);
        }
        case JsonbType::T_Int32: {
            auto val = jsonb_value->unpack<JsonbInt32Val>()->val();
            return fake_doris_cast<TYPE_INT, PT>(val);
        }
        case JsonbType::T_Int64: {
            auto val = jsonb_value->unpack<JsonbInt64Val>()->val();
            return fake_doris_cast<TYPE_BIGINT, PT>(val);
        }
        case JsonbType::T_Int128: {
            auto val = jsonb_value->unpack<JsonbInt128Val>()->val();
            return fake_doris_cast<TYPE_LARGEINT, PT>(val);
        }
        case JsonbType::T_Double: {
            auto val = jsonb_value->unpack<JsonbDoubleVal>()->val();
            return fake_doris_cast<TYPE_DOUBLE, PT>(val);
        }
        case JsonbType::T_Float: {
            auto val = jsonb_value->unpack<JsonbFloatVal>()->val();
            return fake_doris_cast<TYPE_FLOAT, PT>(val);
        }
        case JsonbType::T_String: {
            const auto* str_val = jsonb_value->unpack<JsonbStringVal>();
            StringRef str_ref(str_val->getBlob(), str_val->getBlobLen());
            return fake_doris_cast<TYPE_STRING, PT>(str_ref);
        }
        case JsonbType::T_Decimal32: {
            auto val = jsonb_value->unpack<JsonbDecimal32>()->val();
            return fake_doris_cast<TYPE_DECIMAL32, PT>(vectorized::Decimal32 {val});
        }
        case JsonbType::T_Decimal64: {
            auto val = jsonb_value->unpack<JsonbDecimal64>()->val();
            return fake_doris_cast<TYPE_DECIMAL64, PT>(vectorized::Decimal64 {val});
        }
        case JsonbType::T_Decimal128: {
            auto val = jsonb_value->unpack<JsonbDecimal128>()->val();
            return fake_doris_cast<TYPE_DECIMAL128I, PT>(vectorized::Decimal128V3 {val});
        }
        case JsonbType::T_Decimal256: {
            auto val = jsonb_value->unpack<JsonbDecimal256>()->val();
            return fake_doris_cast<TYPE_DECIMAL256, PT>(vectorized::Decimal256 {val});
        }
        default: {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "Unsupported JsonbType: {} for cast_json_to_doris_type",
                            static_cast<int>(jsonb_value->type));
            return {};
        }
        }
    }
};
} // namespace doris
