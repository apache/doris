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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/JSONParsers/SimdJSONParser.h
// and modified by Doris

#pragma once

#include <parallel_hashmap/phmap.h>
#include <stddef.h>

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "util/jsonb_writer.h"
#include "vec/columns/column.h"
#include "vec/common/string_ref.h"
#include "vec/common/uint128.h"
#include "vec/core/field.h"
#include "vec/json/path_in_data.h"
#include "vec/json/simd_json_parser.h"

namespace doris::vectorized {

template <typename Element>
Field getValueAsField(const Element& element) {
    // bool will convert to type FiledType::UInt64
    if (element.isBool()) {
        return element.getBool();
    }
    if (element.isInt64()) {
        return element.getInt64();
    }
    // doris only support signed integers at present
    if (element.isUInt64()) {
        return element.getInt64();
    }
    if (element.isDouble()) {
        return element.getDouble();
    }
    if (element.isString()) {
        return element.getString();
    }
    if (element.isNull()) {
        return Field();
    }
    return Field();
}

template <typename Element>
void writeValueAsJsonb(const Element& element, JsonbWriter& writer) {
    // bool will convert to type FiledType::UInt64
    if (element.isBool()) {
        writer.writeBool(element.getBool());
        return;
    }
    if (element.isInt64()) {
        writer.writeInt64(element.getInt64());
        return;
    }
    // doris only support signed integers at present
    if (element.isUInt64()) {
        writer.writeInt64(element.getInt64());
        return;
    }
    if (element.isDouble()) {
        writer.writeDouble(element.getDouble());
        return;
    }
    if (element.isString()) {
        writer.writeStartString();
        std::string_view str = element.getString();
        writer.writeString(str.data(), str.size());
        writer.writeEndString();
        return;
    }
    if (element.isNull()) {
        writer.writeNull();
        return;
    }
}

template <typename Element>
std::string castValueAsString(const Element& element) {
    if (element.isBool()) {
        return element.getBool() ? "1" : "0";
    }
    if (element.isInt64()) {
        return std::to_string(element.getInt64());
    }
    if (element.isUInt64()) {
        return std::to_string(element.getUInt64());
    }
    if (element.isDouble()) {
        return std::to_string(element.getDouble());
    }
    if (element.isNull()) {
        return "";
    }
    return "";
}

enum class ExtractType {
    ToString = 0,
    // ...
};
template <typename ParserImpl, bool parse_nested = false>
class JSONDataParser {
public:
    using Element = typename ParserImpl::Element;
    using JSONObject = typename ParserImpl::Object;
    using JSONArray = typename ParserImpl::Array;
    std::optional<ParseResult> parse(const char* begin, size_t length);

    // extract keys's element into columns
    bool extract_key(MutableColumns& columns, StringRef json, const std::vector<StringRef>& keys,
                     const std::vector<ExtractType>& types);

private:
    struct ParseContext {
        PathInDataBuilder builder;
        std::vector<PathInData::Parts> paths;
        std::vector<Field> values;
    };
    using PathPartsWithArray = std::pair<PathInData::Parts, Array>;
    using PathToArray = phmap::flat_hash_map<UInt128, PathPartsWithArray, UInt128TrivialHash>;
    using KeyToSizes = phmap::flat_hash_map<StringRef, std::vector<size_t>, StringRefHash>;
    struct ParseArrayContext {
        size_t current_size = 0;
        size_t total_size = 0;
        PathToArray arrays_by_path;
        KeyToSizes nested_sizes_by_key;
    };
    void traverse(const Element& element, ParseContext& ctx);
    void traverseObject(const JSONObject& object, ParseContext& ctx);
    void traverseArray(const JSONArray& array, ParseContext& ctx);
    void traverseArrayElement(const Element& element, ParseArrayContext& ctx);
    static void fillMissedValuesInArrays(ParseArrayContext& ctx);
    static bool tryInsertDefaultFromNested(ParseArrayContext& ctx, const PathInData::Parts& path,
                                           Array& array);
    static StringRef getNameOfNested(const PathInData::Parts& path, const Field& value);

    bool has_nested = false;
    void checkHasNested(const Element& element);
    void traverseAsJsonb(const Element& element, JsonbWriter& writer);
    void traverseObjectAsJsonb(const JSONObject& object, JsonbWriter& writer);
    void traverseArrayAsJsonb(const JSONArray& array, JsonbWriter& writer);

    ParserImpl parser;
};

} // namespace doris::vectorized
