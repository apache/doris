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

#include <rapidjson/document.h>

#include <unordered_map>

#include "common/status.h"
#include "exprs/anyval_util.h"
#include "runtime/collection_value.h"
#include "runtime/primitive_type.h"
#include "runtime/types.h"
#include "util/mem_util.hpp"

namespace doris {

template <typename Encoding>
using ConstArray = typename rapidjson::GenericValue<Encoding>::ConstArray;

template <typename Encoding>
using ConstArrayIterator = typename ConstArray<Encoding>::ValueIterator;

class ArrayParser {
public:
    static Status parse(CollectionVal& array_val, FunctionContext* context,
                        const StringVal& str_val) {
        rapidjson::Document document;
        if (document.Parse(reinterpret_cast<char*>(str_val.ptr), str_val.len).HasParseError() ||
            !document.IsArray()) {
            return Status::RuntimeError("Failed to parse the json to array.");
        }
        if (document.IsNull()) {
            array_val = CollectionVal::null();
            return Status::OK();
        }
        auto type_desc = _convert_to_type_descriptor(context->get_return_type());
        return _parse<rapidjson::UTF8<>>(
                array_val, context,
                reinterpret_cast<const rapidjson::Document*>(&document)->GetArray(), type_desc);
    }

private:
    static TypeDescriptor _convert_to_type_descriptor(
            FunctionContext::TypeDesc function_type_desc) {
        auto iterator = _types_mapping.find(function_type_desc.type);
        if (iterator == _types_mapping.end()) {
            return TypeDescriptor();
        }
        auto type_desc = TypeDescriptor(iterator->second);
        type_desc.len = function_type_desc.len;
        type_desc.precision = function_type_desc.precision;
        type_desc.scale = function_type_desc.scale;
        for (auto child_type_desc : function_type_desc.children) {
            type_desc.children.push_back(_convert_to_type_descriptor(child_type_desc));
        }
        return type_desc;
    }

    template <typename Encoding>
    static Status _parse(CollectionVal& array_val, FunctionContext* context,
                         const ConstArray<Encoding>& array, const TypeDescriptor& type_desc) {
        if (array.Empty()) {
            CollectionValue(0).to_collection_val(&array_val);
            return Status::OK();
        }
        auto child_type_desc = type_desc.children[0];
        auto item_type = child_type_desc.type;
        CollectionValue collection_value;
        CollectionValue::init_collection(context, array.Size(), item_type, &collection_value);
        int index = 0;
        for (auto it = array.Begin(); it != array.End(); ++it) {
            if (it->IsNull()) {
                auto null = AnyVal(true);
                collection_value.set(index++, item_type, &null);
                continue;
            } else if (!_is_type_valid<Encoding>(it, item_type)) {
                return Status::RuntimeError("Failed to parse the json to array.");
            }
            AnyVal* val = nullptr;
            Status status = _parse<Encoding>(&val, context, it, child_type_desc);
            if (!status.ok()) {
                return status;
            }
            collection_value.set(index++, item_type, val);
        }
        collection_value.to_collection_val(&array_val);
        return Status::OK();
    }

    template <typename Encoding>
    static bool _is_type_valid(const ConstArrayIterator<Encoding> iterator,
                               const PrimitiveType type) {
        switch (type) {
        case TYPE_NULL:
            return iterator->IsNull();
        case TYPE_BOOLEAN:
            return iterator->IsBool();
        case TYPE_TINYINT:
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_BIGINT:
        case TYPE_LARGEINT:
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
            return iterator->IsNumber();
        case TYPE_DATE:
        case TYPE_DATETIME:
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_HLL:
        case TYPE_STRING:
            return iterator->IsString();
        case TYPE_OBJECT:
            return iterator->IsObject();
        case TYPE_ARRAY:
            return iterator->IsArray();
        default:
            return false;
        }
    }

    template <typename Encoding>
    static Status _parse(AnyVal** val, FunctionContext* context,
                         const ConstArrayIterator<Encoding> iterator,
                         const TypeDescriptor& type_desc) {
        switch (type_desc.type) {
        case TYPE_ARRAY:
            *val = reinterpret_cast<AnyVal*>(context->allocate(sizeof(CollectionVal)));
            new (*val) CollectionVal();
            return _parse<Encoding>(*reinterpret_cast<CollectionVal*>(*val), context,
                                    iterator->GetArray(), type_desc);
        case TYPE_BOOLEAN:
            *val = reinterpret_cast<AnyVal*>(context->allocate(sizeof(BooleanVal)));
            new (*val) BooleanVal(iterator->GetBool());
            break;
        case TYPE_TINYINT:
            *val = reinterpret_cast<AnyVal*>(context->allocate(sizeof(TinyIntVal)));
            new (*val) TinyIntVal(iterator->GetInt());
            break;
        case TYPE_SMALLINT:
            *val = reinterpret_cast<AnyVal*>(context->allocate(sizeof(SmallIntVal)));
            new (*val) SmallIntVal(iterator->GetInt());
            break;
        case TYPE_INT:
            *val = reinterpret_cast<AnyVal*>(context->allocate(sizeof(IntVal)));
            new (*val) IntVal(iterator->GetInt());
            break;
        case TYPE_BIGINT:
            *val = reinterpret_cast<AnyVal*>(context->allocate(sizeof(BigIntVal)));
            new (*val) BigIntVal(iterator->GetInt64());
            break;
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_STRING: {
            *val = reinterpret_cast<AnyVal*>(context->allocate(sizeof(StringVal)));
            new (*val) StringVal(context->allocate(iterator->GetStringLength()),
                                 iterator->GetStringLength());
            auto string_val = reinterpret_cast<StringVal*>(*val);
            memory_copy(string_val->ptr, iterator->GetString(), iterator->GetStringLength());
            break;
        }
        default:
            return Status::RuntimeError("Failed to parse json to type (" +
                                        std::to_string(type_desc.type) + ").");
        }
        return Status::OK();
    }

private:
    static std::unordered_map<FunctionContext::Type, PrimitiveType> _types_mapping;
};

std::unordered_map<FunctionContext::Type, PrimitiveType> ArrayParser::_types_mapping = {
        {FunctionContext::INVALID_TYPE, PrimitiveType::INVALID_TYPE},
        {FunctionContext::TYPE_NULL, PrimitiveType::TYPE_NULL},
        {FunctionContext::TYPE_BOOLEAN, PrimitiveType::TYPE_BOOLEAN},
        {FunctionContext::TYPE_TINYINT, PrimitiveType::TYPE_TINYINT},
        {FunctionContext::TYPE_SMALLINT, PrimitiveType::TYPE_SMALLINT},
        {FunctionContext::TYPE_INT, PrimitiveType::TYPE_INT},
        {FunctionContext::TYPE_BIGINT, PrimitiveType::TYPE_BIGINT},
        {FunctionContext::TYPE_LARGEINT, PrimitiveType::TYPE_LARGEINT},
        {FunctionContext::TYPE_FLOAT, PrimitiveType::TYPE_FLOAT},
        {FunctionContext::TYPE_DOUBLE, PrimitiveType::TYPE_DOUBLE},
        {FunctionContext::TYPE_DECIMAL_DEPRACTED, PrimitiveType::TYPE_DECIMAL_DEPRACTED},
        {FunctionContext::TYPE_DATE, PrimitiveType::TYPE_DATE},
        {FunctionContext::TYPE_DATETIME, PrimitiveType::TYPE_DATETIME},
        {FunctionContext::TYPE_CHAR, PrimitiveType::TYPE_CHAR},
        {FunctionContext::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR},
        {FunctionContext::TYPE_HLL, PrimitiveType::TYPE_HLL},
        {FunctionContext::TYPE_STRING, PrimitiveType::TYPE_STRING},
        {FunctionContext::TYPE_DECIMALV2, PrimitiveType::TYPE_DECIMALV2},
        {FunctionContext::TYPE_OBJECT, PrimitiveType::TYPE_OBJECT},
        {FunctionContext::TYPE_ARRAY, PrimitiveType::TYPE_ARRAY},
};

} // namespace doris
