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
#include "util/jsonb_utils.h"
#include "vec/core/types.h"
#include "vec/functions/cast/cast_base.h"
#include "vec/functions/cast/cast_to_basic_number_common.h"
#include "vec/functions/cast/cast_to_boolean.h"
#include "vec/functions/cast/cast_to_decimal.h"
namespace doris::vectorized {

struct JsonbCast {
    static Status report_error(const JsonbValue* jsonb_value, PrimitiveType to_type) {
        return Status::InvalidArgument("Cannot cast from jsonb value type {} to doris type {}",
                                       JsonbToJson {}.to_json_string(jsonb_value),
                                       type_to_string(to_type));
    }

    static bool cast_from_json_to_boolean(const JsonbValue* jsonb_value, UInt8& to,
                                          CastParameters& params) {
        switch (jsonb_value->type) {
        case JsonbType::T_True:
        case JsonbType::T_False: {
            return CastToBool::from_number((UInt8)jsonb_value->isTrue(), to, params);
        }
        case JsonbType::T_Int8: {
            auto val = jsonb_value->unpack<JsonbInt8Val>()->val();
            return CastToBool::from_number(val, to, params);
        }
        case JsonbType::T_Int16: {
            auto val = jsonb_value->unpack<JsonbInt16Val>()->val();
            return CastToBool::from_number(val, to, params);
        }
        case JsonbType::T_Int32: {
            auto val = jsonb_value->unpack<JsonbInt32Val>()->val();
            return CastToBool::from_number(val, to, params);
        }
        case JsonbType::T_Int64: {
            auto val = jsonb_value->unpack<JsonbInt64Val>()->val();
            return CastToBool::from_number(val, to, params);
        }
        case JsonbType::T_Int128: {
            auto val = jsonb_value->unpack<JsonbInt128Val>()->val();
            return CastToBool::from_number(val, to, params);
        }
        case JsonbType::T_Double: {
            auto val = jsonb_value->unpack<JsonbDoubleVal>()->val();
            return CastToBool::from_number(val, to, params);
        }
        case JsonbType::T_Float: {
            auto val = jsonb_value->unpack<JsonbFloatVal>()->val();
            return CastToBool::from_number(val, to, params);
        }
        case JsonbType::T_Decimal32: {
            auto val = jsonb_value->unpack<JsonbDecimal32>()->val();
            UInt32 precision = jsonb_value->unpack<JsonbDecimal32>()->precision;
            UInt32 scale = jsonb_value->unpack<JsonbDecimal32>()->scale;
            return CastToBool::from_decimal(vectorized::Decimal32 {val}, to, precision, scale,
                                            params);
        }
        case JsonbType::T_Decimal64: {
            auto val = jsonb_value->unpack<JsonbDecimal64>()->val();
            UInt32 precision = jsonb_value->unpack<JsonbDecimal64>()->precision;
            UInt32 scale = jsonb_value->unpack<JsonbDecimal64>()->scale;
            return CastToBool::from_decimal(vectorized::Decimal64 {val}, to, precision, scale,
                                            params);
        }
        case JsonbType::T_Decimal128: {
            auto val = jsonb_value->unpack<JsonbDecimal128>()->val();
            UInt32 precision = jsonb_value->unpack<JsonbDecimal128>()->precision;
            UInt32 scale = jsonb_value->unpack<JsonbDecimal128>()->scale;
            return CastToBool::from_decimal(vectorized::Decimal128V3 {val}, to, precision, scale,
                                            params);
        }
        case JsonbType::T_Decimal256: {
            auto val = jsonb_value->unpack<JsonbDecimal256>()->val();
            UInt32 precision = jsonb_value->unpack<JsonbDecimal256>()->precision;
            UInt32 scale = jsonb_value->unpack<JsonbDecimal256>()->scale;
            return CastToBool::from_decimal(vectorized::Decimal256 {val}, to, precision, scale,
                                            params);
        }
        default: {
            return false;
        }
        }
    }

    template <typename T>
    static bool cast_from_json_to_int(const JsonbValue* jsonb_value, T& to,
                                      CastParameters& params) {
        switch (jsonb_value->type) {
        case JsonbType::T_True:
        case JsonbType::T_False: {
            auto val = (UInt8)jsonb_value->isTrue();
            return CastToInt::from_bool(val, to, params);
        }
        case JsonbType::T_Int8: {
            auto val = jsonb_value->unpack<JsonbInt8Val>()->val();
            return CastToInt::from_int(val, to, params);
        }
        case JsonbType::T_Int16: {
            auto val = jsonb_value->unpack<JsonbInt16Val>()->val();
            return CastToInt::from_int(val, to, params);
        }
        case JsonbType::T_Int32: {
            auto val = jsonb_value->unpack<JsonbInt32Val>()->val();
            return CastToInt::from_int(val, to, params);
        }
        case JsonbType::T_Int64: {
            auto val = jsonb_value->unpack<JsonbInt64Val>()->val();
            return CastToInt::from_int(val, to, params);
        }
        case JsonbType::T_Int128: {
            auto val = jsonb_value->unpack<JsonbInt128Val>()->val();
            return CastToInt::from_int(val, to, params);
        }
        case JsonbType::T_Double: {
            auto val = jsonb_value->unpack<JsonbDoubleVal>()->val();
            return CastToInt::from_float(val, to, params);
        }
        case JsonbType::T_Float: {
            auto val = jsonb_value->unpack<JsonbFloatVal>()->val();
            return CastToInt::from_float(val, to, params);
        }
        case JsonbType::T_Decimal32: {
            auto val = jsonb_value->unpack<JsonbDecimal32>()->val();
            UInt32 precision = jsonb_value->unpack<JsonbDecimal32>()->precision;
            UInt32 scale = jsonb_value->unpack<JsonbDecimal32>()->scale;
            return CastToInt::from_decimal(vectorized::Decimal32 {val}, precision, scale, to,
                                           params);
        }
        case JsonbType::T_Decimal64: {
            auto val = jsonb_value->unpack<JsonbDecimal64>()->val();
            UInt32 precision = jsonb_value->unpack<JsonbDecimal64>()->precision;
            UInt32 scale = jsonb_value->unpack<JsonbDecimal64>()->scale;
            return CastToInt::from_decimal(vectorized::Decimal64 {val}, precision, scale, to,
                                           params);
        }
        case JsonbType::T_Decimal128: {
            auto val = jsonb_value->unpack<JsonbDecimal128>()->val();
            UInt32 precision = jsonb_value->unpack<JsonbDecimal128>()->precision;
            UInt32 scale = jsonb_value->unpack<JsonbDecimal128>()->scale;
            return CastToInt::from_decimal(vectorized::Decimal128V3 {val}, precision, scale, to,
                                           params);
        }
        case JsonbType::T_Decimal256: {
            auto val = jsonb_value->unpack<JsonbDecimal256>()->val();
            UInt32 precision = jsonb_value->unpack<JsonbDecimal256>()->precision;
            UInt32 scale = jsonb_value->unpack<JsonbDecimal256>()->scale;
            return CastToInt::from_decimal(vectorized::Decimal256 {val}, precision, scale, to,
                                           params);
        }
        default: {
            return false;
        }
        }
    }

    template <typename T>
    static bool cast_from_json_to_float(const JsonbValue* jsonb_value, T& to,
                                        CastParameters& params) {
        switch (jsonb_value->type) {
        case JsonbType::T_True:
        case JsonbType::T_False: {
            auto val = (UInt8)jsonb_value->isTrue();
            return CastToFloat::from_bool(val, to, params);
        }
        case JsonbType::T_Int8: {
            auto val = jsonb_value->unpack<JsonbInt8Val>()->val();
            return CastToFloat::from_int(val, to, params);
        }
        case JsonbType::T_Int16: {
            auto val = jsonb_value->unpack<JsonbInt16Val>()->val();
            return CastToFloat::from_int(val, to, params);
        }
        case JsonbType::T_Int32: {
            auto val = jsonb_value->unpack<JsonbInt32Val>()->val();
            return CastToFloat::from_int(val, to, params);
        }
        case JsonbType::T_Int64: {
            auto val = jsonb_value->unpack<JsonbInt64Val>()->val();
            return CastToFloat::from_int(val, to, params);
        }
        case JsonbType::T_Int128: {
            auto val = jsonb_value->unpack<JsonbInt128Val>()->val();
            return CastToFloat::from_int(val, to, params);
        }
        case JsonbType::T_Double: {
            auto val = jsonb_value->unpack<JsonbDoubleVal>()->val();
            return CastToFloat::from_float(val, to, params);
        }
        case JsonbType::T_Float: {
            auto val = jsonb_value->unpack<JsonbFloatVal>()->val();
            return CastToFloat::from_float(val, to, params);
        }
        case JsonbType::T_Decimal32: {
            auto val = jsonb_value->unpack<JsonbDecimal32>()->val();
            UInt32 scale = jsonb_value->unpack<JsonbDecimal32>()->scale;
            return CastToFloat::from_decimal(vectorized::Decimal32 {val}, scale, to, params);
        }
        case JsonbType::T_Decimal64: {
            auto val = jsonb_value->unpack<JsonbDecimal64>()->val();
            UInt32 scale = jsonb_value->unpack<JsonbDecimal64>()->scale;
            return CastToFloat::from_decimal(vectorized::Decimal64 {val}, scale, to, params);
        }
        case JsonbType::T_Decimal128: {
            auto val = jsonb_value->unpack<JsonbDecimal128>()->val();
            UInt32 scale = jsonb_value->unpack<JsonbDecimal128>()->scale;
            return CastToFloat::from_decimal(vectorized::Decimal128V3 {val}, scale, to, params);
        }
        case JsonbType::T_Decimal256: {
            auto val = jsonb_value->unpack<JsonbDecimal256>()->val();
            UInt32 scale = jsonb_value->unpack<JsonbDecimal256>()->scale;
            return CastToFloat::from_decimal(vectorized::Decimal256 {val}, scale, to, params);
        }
        default: {
            return false;
        }
        }
    }

    template <typename T>
    static bool cast_from_json_to_decimal(const JsonbValue* jsonb_value, T& to, UInt32 to_precision,
                                          UInt32 to_scale, CastParameters& params) {
        switch (jsonb_value->type) {
        case JsonbType::T_True:
        case JsonbType::T_False: {
            UInt8 val = jsonb_value->isTrue();
            return CastToDecimal::from_bool(val, to, to_precision, to_scale, params);
        }
        case JsonbType::T_Int8: {
            auto val = jsonb_value->unpack<JsonbInt8Val>()->val();
            return CastToDecimal::from_int(val, to, to_precision, to_scale, params);
        }
        case JsonbType::T_Int16: {
            auto val = jsonb_value->unpack<JsonbInt16Val>()->val();
            return CastToDecimal::from_int(val, to, to_precision, to_scale, params);
        }
        case JsonbType::T_Int32: {
            auto val = jsonb_value->unpack<JsonbInt32Val>()->val();
            return CastToDecimal::from_int(val, to, to_precision, to_scale, params);
        }
        case JsonbType::T_Int64: {
            auto val = jsonb_value->unpack<JsonbInt64Val>()->val();
            return CastToDecimal::from_int(val, to, to_precision, to_scale, params);
        }
        case JsonbType::T_Int128: {
            auto val = jsonb_value->unpack<JsonbInt128Val>()->val();
            return CastToDecimal::from_int(val, to, to_precision, to_scale, params);
        }
        case JsonbType::T_Double: {
            auto val = jsonb_value->unpack<JsonbDoubleVal>()->val();
            return CastToDecimal::from_float(val, to, to_precision, to_scale, params);
        }
        case JsonbType::T_Float: {
            auto val = jsonb_value->unpack<JsonbFloatVal>()->val();
            return CastToDecimal::from_float(val, to, to_precision, to_scale, params);
        }
        case JsonbType::T_Decimal32: {
            auto val = Decimal32 {jsonb_value->unpack<JsonbDecimal32>()->val()};
            UInt32 from_precision = jsonb_value->unpack<JsonbDecimal32>()->precision;
            UInt32 from_scale = jsonb_value->unpack<JsonbDecimal32>()->scale;
            return CastToDecimal::from_decimalv3(val, from_precision, from_scale, to, to_precision,
                                                 to_scale, params);
        }
        case JsonbType::T_Decimal64: {
            auto val = Decimal64 {jsonb_value->unpack<JsonbDecimal64>()->val()};
            UInt32 from_precision = jsonb_value->unpack<JsonbDecimal64>()->precision;
            UInt32 from_scale = jsonb_value->unpack<JsonbDecimal64>()->scale;
            return CastToDecimal::from_decimalv3(val, from_precision, from_scale, to, to_precision,
                                                 to_scale, params);
        }
        case JsonbType::T_Decimal128: {
            auto val = Decimal128V3 {jsonb_value->unpack<JsonbDecimal128>()->val()};
            UInt32 from_precision = jsonb_value->unpack<JsonbDecimal128>()->precision;
            UInt32 from_scale = jsonb_value->unpack<JsonbDecimal128>()->scale;
            return CastToDecimal::from_decimalv3(val, from_precision, from_scale, to, to_precision,
                                                 to_scale, params);
        }
        case JsonbType::T_Decimal256: {
            auto val = Decimal256 {jsonb_value->unpack<JsonbDecimal256>()->val()};
            UInt32 from_precision = jsonb_value->unpack<JsonbDecimal256>()->precision;
            UInt32 from_scale = jsonb_value->unpack<JsonbDecimal256>()->scale;
            return CastToDecimal::from_decimalv3(val, from_precision, from_scale, to, to_precision,
                                                 to_scale, params);
        }
        default: {
            return false;
        }
        }
    }
};
} // namespace doris::vectorized
