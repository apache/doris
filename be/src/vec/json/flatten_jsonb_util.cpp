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

#include "vec/json/flatten_jsonb_util.h"

namespace doris::vectorized {

// Helpers to determine whether a jsonb object/array is "empty" in the sense that
// it contains only nulls and recursively empty objects/arrays.
static bool is_empty_object(const JsonbValue* value);
static bool is_empty_array(const JsonbValue* value);

static bool is_empty_object(const JsonbValue* value) {
    const auto* obj = value->unpack<ObjectVal>();
    if (obj->numElem() == 0) {
        return true;
    }
    for (const auto& it : *obj) {
        if (it.value()->type == JsonbType::T_Object) {
            if (!is_empty_object(it.value())) {
                return false;
            } else {
                continue;
            }
        } else if (it.value()->type == JsonbType::T_Array) {
            if (!is_empty_array(it.value())) {
                return false;
            } else {
                continue;
            }
        } else if (it.value()->type != JsonbType::T_Null) {
            return false;
        }
    }
    return true;
}

static bool is_empty_array(const JsonbValue* value) {
    const auto* arr = value->unpack<ArrayVal>();
    if (arr->numElem() == 0) {
        return true;
    }
    for (int i = 0; i < arr->numElem(); ++i) {
        if (arr->get(i)->type == JsonbType::T_Object) {
            if (!is_empty_object(arr->get(i))) {
                return false;
            } else {
                continue;
            }
        } else if (arr->get(i)->type == JsonbType::T_Array) {
            if (!is_empty_array(arr->get(i))) {
                return false;
            } else {
                continue;
            }
        } else if (arr->get(i)->type != JsonbType::T_Null) {
            return false;
        }
    }
    return true;
}

void FlattenJsonbUtil::flatten(const char* jsonb_str, size_t jsonb_len, FlattenResult* result) {
    JsonbDocument* document = nullptr;
    THROW_IF_ERROR(JsonbDocument::checkAndCreateDocument(jsonb_str, jsonb_len, &document));
    JsonbValue* value = document->getValue();

    ParseContext ctx;
    flatten(value, ctx);
    result->values = std::move(ctx.values);
    result->paths.reserve(ctx.paths.size());
    for (auto&& path : ctx.paths) {
        result->paths.emplace_back(std::move(path));
    }
}

void FlattenJsonbUtil::flatten(const JsonbValue* value, ParseContext& ctx) {
    // flatten object
    if (value->isObject()) {
        flattenObject(value, ctx);
    } else {
        ctx.paths.push_back(ctx.builder.get_parts());
        if (value->isArray()) {
            ctx.values.push_back(parseArrayValue(value));
        } else {
            ctx.values.push_back(parseSingleValue(value));
        }
    }
}

void FlattenJsonbUtil::flattenObject(const JsonbValue* object, ParseContext& ctx) {
    const auto* obj = object->unpack<ObjectVal>();

    for (const auto& it : *obj) {
        std::string_view key(it.getKeyStr(), it.klen());

        ctx.builder.append(key, false);
        flatten(it.value(), ctx);
        ctx.builder.pop_back();
    }
}

FieldWithDataType FlattenJsonbUtil::parseArrayValue(const JsonbValue* array) {
    ParseArrayType parse_type = getParseArrayType(array);

    switch (parse_type) {
    case ParseArrayType::NULL_ARRAY: {
        return FieldWithDataType {.field = Field(),
                                  .base_scalar_type_id = PrimitiveType::TYPE_NULL,
                                  .num_dimensions = 0};
    }
    case ParseArrayType::JSONB: {
        JsonbWriter writer;
        parseArrayAsJsonb(array, writer);
        return FieldWithDataType {
                .field = Field::create_field<TYPE_JSONB>(
                        JsonbField(writer.getOutput()->getBuffer(), writer.getOutput()->getSize())),
                .base_scalar_type_id = PrimitiveType::TYPE_JSONB,
                .num_dimensions = 0};
    }
    case ParseArrayType::ARRAY_JSONB: {
        const auto* arr = array->unpack<ArrayVal>();
        const int array_size = arr->numElem();
        Array array_field;
        for (int i = 0; i < array_size; ++i) {
            JsonbWriter writer;
            parseAsJsonb(arr->get(i), writer);
            array_field.push_back(Field::create_field<TYPE_JSONB>(
                    JsonbField(writer.getOutput()->getBuffer(), writer.getOutput()->getSize())));
        }
        return FieldWithDataType {.field = Field::create_field<TYPE_ARRAY>(array_field),
                                  .base_scalar_type_id = PrimitiveType::TYPE_JSONB,
                                  .num_dimensions = 1};
    }
    case ParseArrayType::ARRAY_NESTED_TYPE: {
        Array array_field;
        PrimitiveType type = PrimitiveType::TYPE_NULL;
        const auto* arr = array->unpack<ArrayVal>();
        const int array_size = arr->numElem();
        for (int i = 0; i < array_size; ++i) {
            array_field.emplace_back(parseAsField(arr->get(i)));
            if (type == PrimitiveType::TYPE_NULL) {
                type = getBasicPrimitiveType(arr->get(i));
            }
        }
        return FieldWithDataType {.field = Field::create_field<TYPE_ARRAY>(array_field),
                                  .base_scalar_type_id = type,
                                  .num_dimensions = 1};
    }
    default:
        throw Exception(ErrorCode::INTERNAL_ERROR, "Invalid parse array type");
    }
}

void FlattenJsonbUtil::parseObjectAsJsonb(const JsonbValue* object, JsonbWriter& writer) {
    writer.writeStartObject();
    const auto* obj = object->unpack<ObjectVal>();
    for (const auto& it : *obj) {
        // write key
        std::string_view key(it.getKeyStr(), it.klen());
        writer.writeKey(key.data(), cast_set<uint8_t>(key.size()));

        // write value
        parseAsJsonb(it.value(), writer);
    }
    writer.writeEndObject();
}

void FlattenJsonbUtil::parseAsJsonb(const JsonbValue* value, JsonbWriter& writer) {
    if (value->isObject()) {
        parseObjectAsJsonb(value, writer);
    } else if (value->isArray()) {
        parseArrayAsJsonb(value, writer);
    } else {
        parseSingleValueAsJsonb(value, writer);
    }
}

void FlattenJsonbUtil::parseArrayAsJsonb(const JsonbValue* value, JsonbWriter& writer) {
    writer.writeStartArray();
    const auto* array = value->unpack<ArrayVal>();
    const int array_size = array->numElem();
    for (int i = 0; i < array_size; ++i) {
        parseAsJsonb(array->get(i), writer);
    }
    writer.writeEndArray();
}

ParseArrayType FlattenJsonbUtil::getParseArrayType(const JsonbValue* array) {
    const auto* arr = array->unpack<ArrayVal>();
    const int arr_size = arr->numElem();
    if (arr_size == 0) {
        return ParseArrayType::NULL_ARRAY;
    }

    PrimitiveType nested_type = PrimitiveType::TYPE_NULL;
    for (int i = 0; i < arr_size; ++i) {
        // array<object> is parsed as jsonb
        if (arr->get(i)->type == JsonbType::T_Object) {
            if (!is_empty_object(arr->get(i))) {
                return ParseArrayType::JSONB;
            } else {
                continue;
            }
        }
        // array<array> is parsed as jsonb
        else if (arr->get(i)->type == JsonbType::T_Array) {
            if (!is_empty_array(arr->get(i))) {
                return ParseArrayType::JSONB;
            } else {
                continue;
            }
        }
        // null is ignored
        else if (arr->get(i)->type == JsonbType::T_Null) {
            continue;
        }
        // update nested type
        else if (nested_type == PrimitiveType::TYPE_NULL) {
            nested_type = getBasicPrimitiveType(arr->get(i));
        }
        // if nested type is not null and has different type, we should parse it as array<jsonb>
        else if (nested_type != getBasicPrimitiveType(arr->get(i))) {
            return ParseArrayType::ARRAY_JSONB;
        } else {
            continue;
        }
    }
    // if all elements have the same type, we can parse it as array<nested_type>
    return nested_type == PrimitiveType::TYPE_NULL ? ParseArrayType::NULL_ARRAY
                                                   : ParseArrayType::ARRAY_NESTED_TYPE;
}

FieldWithDataType FlattenJsonbUtil::parseSingleValue(const JsonbValue* value) {
    return FieldWithDataType {.field = parseAsField(value),
                              .base_scalar_type_id = getBasicPrimitiveType(value),
                              .num_dimensions = 0};
}

void FlattenJsonbUtil::parseSingleValueAsJsonb(const JsonbValue* value, JsonbWriter& writer) {
    if (value->isNull()) {
        writer.writeNull();
    } else if (value->isTrue()) {
        writer.writeBool(true);
    } else if (value->isFalse()) {
        writer.writeBool(false);
    } else if (value->isInt8()) {
        writer.writeInt64(value->unpack<JsonbInt8Val>()->val());
    } else if (value->isInt16()) {
        writer.writeInt64(value->unpack<JsonbInt16Val>()->val());
    } else if (value->isInt32()) {
        writer.writeInt64(value->unpack<JsonbInt32Val>()->val());
    } else if (value->isInt64()) {
        writer.writeInt64(value->unpack<JsonbInt64Val>()->val());
    } else if (value->isInt128()) {
        writer.writeInt128(value->unpack<JsonbInt128Val>()->val());
    } else if (value->isDouble()) {
        writer.writeDouble(value->unpack<JsonbDoubleVal>()->val());
    } else if (value->isFloat()) {
        writer.writeFloat(value->unpack<JsonbFloatVal>()->val());
    } else if (value->isString()) {
        writer.writeStartString();
        writer.writeString(value->unpack<JsonbStringVal>()->getBlob(),
                           value->unpack<JsonbStringVal>()->length());
        writer.writeEndString();
    } else {
        throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                        "Unsupported jsonb value type in variant: {}", value->type);
    }
}

} // namespace doris::vectorized
