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

#include "runtime/primitive_type.h"
#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"
#include "vec/core/field.h"
#include "vec/json/path_in_data.h"

namespace doris::vectorized {

// now only support the following basic types when flattening jsonb to variant field:
// 1. bigint
// 2. largeint
// 3. double
// 4. string
// 5. boolean
// 6. null
inline PrimitiveType getBasicPrimitiveType(const JsonbValue* value) {
    if (value->isInt8() || value->isInt16() || value->isInt32() || value->isInt64()) {
        return PrimitiveType::TYPE_BIGINT;
    } else if (value->isInt128()) {
        return PrimitiveType::TYPE_LARGEINT;
    } else if (value->isDouble() || value->isFloat()) {
        return PrimitiveType::TYPE_DOUBLE;
    } else if (value->isString()) {
        return PrimitiveType::TYPE_STRING;
    } else if (value->isTrue() || value->isFalse()) {
        return PrimitiveType::TYPE_BOOLEAN;
    } else if (value->isNull()) {
        return PrimitiveType::TYPE_NULL;
    } else {
        throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                        "Unsupported jsonb value type in variant: {}", value->type);
    }
}

// field type is the nearest field primitive type of the jsonb value's primitive type
// PrimitiveTypeTraits<value->get_primitive_type()>::NearestFieldType
inline Field parseAsField(const JsonbValue* value) {
    if (value->isNull()) {
        return Field();
    } else if (value->isTrue()) {
        return Field::create_field<PrimitiveType::TYPE_BIGINT>(true);
    } else if (value->isFalse()) {
        return Field::create_field<PrimitiveType::TYPE_BIGINT>(false);
    } else if (value->isInt8()) {
        return Field::create_field<PrimitiveType::TYPE_BIGINT>(
                value->unpack<JsonbInt8Val>()->val());
    } else if (value->isInt16()) {
        return Field::create_field<PrimitiveType::TYPE_BIGINT>(
                value->unpack<JsonbInt16Val>()->val());
    } else if (value->isInt32()) {
        return Field::create_field<PrimitiveType::TYPE_BIGINT>(
                value->unpack<JsonbInt32Val>()->val());
    } else if (value->isInt64()) {
        return Field::create_field<PrimitiveType::TYPE_BIGINT>(
                value->unpack<JsonbInt64Val>()->val());
    } else if (value->isInt128()) {
        return Field::create_field<PrimitiveType::TYPE_LARGEINT>(
                value->unpack<JsonbInt128Val>()->val());
    } else if (value->isDouble()) {
        return Field::create_field<PrimitiveType::TYPE_DOUBLE>(
                value->unpack<JsonbDoubleVal>()->val());
    } else if (value->isFloat()) {
        return Field::create_field<PrimitiveType::TYPE_DOUBLE>(
                value->unpack<JsonbFloatVal>()->val());
    } else if (value->isString()) {
        return Field::create_field<PrimitiveType::TYPE_STRING>(
                String(value->unpack<JsonbStringVal>()->getBlob(),
                       value->unpack<JsonbStringVal>()->length()));
    } else {
        throw Exception(ErrorCode::INTERNAL_ERROR, "Unsupported jsonb value type in variant: {}",
                        value->type);
    }
}

struct FlattenResult {
    std::vector<PathInData> paths;
    std::vector<FieldWithDataType> values;
};

enum class ParseArrayType : uint8_t {
    NULL_ARRAY = 0,
    JSONB = 1,
    ARRAY_JSONB = 2,
    ARRAY_NESTED_TYPE = 3
};

// flatten jsonb to path and variant field
class FlattenJsonbUtil {
public:
    void flatten(const char* jsonb_str, size_t jsonb_len, FlattenResult* result);

private:
    struct ParseContext {
        PathInDataBuilder builder;
        std::vector<PathInData::Parts> paths;
        std::vector<FieldWithDataType> values;
    };

    void flatten(const JsonbValue* element, ParseContext& ctx);
    // flatten object
    void flattenObject(const JsonbValue* object, ParseContext& ctx);

    // not implemented yet
    void flattenArray(const JsonbValue* array, ParseContext& ctx);

    // if array contains object or array, we should parse it as jsonb
    // otherwise, we can parse it as array
    FieldWithDataType parseArrayValue(const JsonbValue* array);
    FieldWithDataType parseSingleValue(const JsonbValue* value);

    // parse as jsonb
    void parseAsJsonb(const JsonbValue* element, JsonbWriter& writer);
    void parseObjectAsJsonb(const JsonbValue* object, JsonbWriter& writer);
    void parseArrayAsJsonb(const JsonbValue* array, JsonbWriter& writer);
    void parseSingleValueAsJsonb(const JsonbValue* value, JsonbWriter& writer);

    // 1. if array contains only null, we should parse it as null
    // 2. if array contains object or array, we should parse it as jsonb
    // 3. if all elements in array are the same type, we can parse it as array<nested_type>
    // 4. otherwise, we should parse it as array<jsonb>
    ParseArrayType getParseArrayType(const JsonbValue* array);
};

} // namespace doris::vectorized
