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
#include "runtime/collection_value.h"
#include "runtime/large_int_value.h"
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
        auto iterator = collection_value.iterator(item_type);
        for (auto it = array.Begin(); it != array.End(); ++it, iterator.next()) {
            if (it->IsNull()) {
                auto null = AnyVal(true);
                iterator.set(&null);
                continue;
            } else if (!_is_type_valid<Encoding>(it, item_type)) {
                return Status::RuntimeError("Failed to parse the json to array.");
            }
            AnyVal* val = nullptr;
            Status status = _parse<Encoding>(&val, context, it, child_type_desc);
            if (!status.ok()) {
                return status;
            }
            iterator.set(val);
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
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
            return iterator->IsNumber();
        case TYPE_LARGEINT:
            return iterator->IsNumber() || iterator->IsString();
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
        case TYPE_DECIMALV2:
            return iterator->IsNumber() || iterator->IsString();
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
        case TYPE_LARGEINT: {
            __int128 value = 0;
            if (iterator->IsNumber()) {
                if (iterator->IsUint64()) {
                    value = iterator->GetUint64();
                } else {
                    return Status::RuntimeError(
                            "rapidjson can't parse the number larger than Uint64, please use "
                            "String to parse as LARGEINT");
                }
            } else {
                std::string_view view(iterator->GetString(), iterator->GetStringLength());
                std::stringstream stream;
                stream << view;
                stream >> value;
            }
            *val = reinterpret_cast<AnyVal*>(context->aligned_allocate(16, sizeof(LargeIntVal)));
            new (*val) LargeIntVal(value);
            break;
        }
        case TYPE_FLOAT:
            *val = reinterpret_cast<AnyVal*>(context->allocate(sizeof(FloatVal)));
            new (*val) FloatVal(iterator->GetFloat());
            break;
        case TYPE_DOUBLE:
            *val = reinterpret_cast<AnyVal*>(context->allocate(sizeof(DoubleVal)));
            new (*val) DoubleVal(iterator->GetDouble());
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
        case TYPE_DATE:
        case TYPE_DATETIME: {
            DateTimeValue value;
            value.from_date_str(iterator->GetString(), iterator->GetStringLength());
            *val = reinterpret_cast<AnyVal*>(context->allocate(sizeof(DateTimeVal)));
            new (*val) DateTimeVal();
            value.to_datetime_val(static_cast<DateTimeVal*>(*val));
            break;
        }
        case TYPE_DECIMALV2: {
            *val = reinterpret_cast<AnyVal*>(context->aligned_allocate(16, sizeof(DecimalV2Val)));
            new (*val) DecimalV2Val();

            if (iterator->IsNumber()) {
                if (iterator->IsUint64()) {
                    DecimalV2Value(iterator->GetUint64(), 0)
                            .to_decimal_val(static_cast<DecimalV2Val*>(*val));
                } else {
                    DecimalV2Value value;
                    value.assign_from_double(iterator->GetDouble());
                    value.to_decimal_val(static_cast<DecimalV2Val*>(*val));
                }
            } else {
                std::string_view view(iterator->GetString(), iterator->GetStringLength());
                DecimalV2Value(view).to_decimal_val(static_cast<DecimalV2Val*>(*val));
            }
            break;
        }
        default:
            return Status::RuntimeError("Failed to parse json to type ({}).",
                                        std::to_string(type_desc.type));
        }
        return Status::OK();
    }

private:
    static std::unordered_map<FunctionContext::Type, PrimitiveType> _types_mapping;
};
} // namespace doris
