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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/JSONParsers/SimdJSONParser.cpp
// and modified by Doris

#include "vec/json/json_parser.h"

#include <assert.h>
#include <fmt/format.h>
#include <glog/logging.h>

#include <algorithm>
#include <string_view>

#include "common/cast_set.h"
#include "common/config.h"
#include "common/status.h"
#include "vec/json/path_in_data.h"
#include "vec/json/simd_json_parser.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <typename ParserImpl>
std::optional<ParseResult> JSONDataParser<ParserImpl>::parse(const char* begin, size_t length,
                                                             const ParseConfig& config) {
    Element document;
    if (!parser.parse(begin, length, document)) {
        return {};
    }
    ParseContext context;
    context.enable_flatten_nested = config.enable_flatten_nested;
    context.is_top_array = document.isArray();
    traverse(document, context);
    ParseResult result;
    result.values = std::move(context.values);
    result.paths.reserve(context.paths.size());
    for (auto&& path : context.paths) {
        result.paths.emplace_back(std::move(path));
    }
    return result;
}

template <typename ParserImpl>
void JSONDataParser<ParserImpl>::traverse(const Element& element, ParseContext& ctx) {
    // checkStackSize();
    if (element.isObject()) {
        traverseObject(element.getObject(), ctx);
    } else if (element.isArray()) {
        if (ctx.has_nested_in_flatten) {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                   "Nesting of array in Nested array within variant subcolumns is "
                                   "currently not supported.");
        }
        has_nested = false;
        check_has_nested_object(element);
        ctx.has_nested_in_flatten = has_nested && ctx.enable_flatten_nested;
        if (has_nested && !ctx.enable_flatten_nested) {
            // Parse nested arrays to JsonbField
            JsonbWriter writer;
            traverseArrayAsJsonb(element.getArray(), writer);
            ctx.paths.push_back(ctx.builder.get_parts());
            ctx.values.push_back(Field::create_field<TYPE_JSONB>(
                    JsonbField(writer.getOutput()->getBuffer(), writer.getOutput()->getSize())));
        } else {
            traverseArray(element.getArray(), ctx);
        }
        // we should set has_nested_in_flatten to false when traverse array finished for next array otherwise it will be true for next array
        ctx.has_nested_in_flatten = false;
    } else {
        ctx.paths.push_back(ctx.builder.get_parts());
        ctx.values.push_back(getValueAsField(element));
    }
}
template <typename ParserImpl>
void JSONDataParser<ParserImpl>::traverseObject(const JSONObject& object, ParseContext& ctx) {
    ctx.paths.reserve(ctx.paths.size() + object.size());
    ctx.values.reserve(ctx.values.size() + object.size());
    for (auto it = object.begin(); it != object.end(); ++it) {
        const auto& [key, value] = *it;
        if (key.size() >= std::numeric_limits<uint8_t>::max()) {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                   "Key length exceeds maximum allowed size of 255 bytes.");
        }
        ctx.builder.append(key, false);
        traverse(value, ctx);
        ctx.builder.pop_back();
    }
}

template <typename ParserImpl>
void JSONDataParser<ParserImpl>::check_has_nested_object(const Element& element) {
    if (element.isArray()) {
        const JSONArray& array = element.getArray();
        for (auto it = array.begin(); it != array.end(); ++it) {
            check_has_nested_object(*it);
        }
    }
    if (element.isObject()) {
        has_nested = true;
    }
}

template <typename ParserImpl>
void JSONDataParser<ParserImpl>::traverseAsJsonb(const Element& element, JsonbWriter& writer) {
    if (element.isObject()) {
        traverseObjectAsJsonb(element.getObject(), writer);
    } else if (element.isArray()) {
        traverseArrayAsJsonb(element.getArray(), writer);
    } else {
        writeValueAsJsonb(element, writer);
    }
}

template <typename ParserImpl>
void JSONDataParser<ParserImpl>::traverseObjectAsJsonb(const JSONObject& object,
                                                       JsonbWriter& writer) {
    writer.writeStartObject();
    for (auto it = object.begin(); it != object.end(); ++it) {
        const auto& [key, value] = *it;
        if (key.size() >= std::numeric_limits<uint8_t>::max()) {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                   "Key length exceeds maximum allowed size of 255 bytes.");
        }
        writer.writeKey(key.data(), cast_set<uint8_t>(key.size()));
        traverseAsJsonb(value, writer);
    }
    writer.writeEndObject();
}

template <typename ParserImpl>
void JSONDataParser<ParserImpl>::traverseArrayAsJsonb(const JSONArray& array, JsonbWriter& writer) {
    writer.writeStartArray();
    for (auto it = array.begin(); it != array.end(); ++it) {
        traverseAsJsonb(*it, writer);
    }
    writer.writeEndArray();
}

// check isPrefix in PathInData::Parts. like : [{"a": {"c": {"b": 1}}}, {"a": {"c": 2.2}}], "a.c" is prefix of "a.c.b"
// return true if prefix is a prefix of parts
static bool is_prefix(const PathInData::Parts& prefix, const PathInData::Parts& parts) {
    if (prefix.size() >= parts.size()) {
        return false;
    }
    for (size_t i = 0; i < prefix.size(); ++i) {
        if (prefix[i].key != parts[i].key) {
            return false;
        }
    }
    return true;
}

template <typename ParserImpl>
void JSONDataParser<ParserImpl>::traverseArray(const JSONArray& array, ParseContext& ctx) {
    /// Traverse elements of array and collect an array of fields by each path.
    ParseArrayContext array_ctx;
    array_ctx.has_nested_in_flatten = ctx.has_nested_in_flatten;
    array_ctx.is_top_array = ctx.is_top_array;
    array_ctx.total_size = array.size();
    for (auto it = array.begin(); it != array.end(); ++it) {
        traverseArrayElement(*it, array_ctx);
        ++array_ctx.current_size;
    }
    auto&& arrays_by_path = array_ctx.arrays_by_path;
    if (arrays_by_path.empty()) {
        ctx.paths.push_back(ctx.builder.get_parts());
        ctx.values.push_back(Field::create_field<TYPE_ARRAY>(Array()));
    } else {
        ctx.paths.reserve(ctx.paths.size() + arrays_by_path.size());
        ctx.values.reserve(ctx.values.size() + arrays_by_path.size());
        for (auto it = arrays_by_path.begin(); it != arrays_by_path.end(); ++it) {
            auto&& [path, path_array] = it->second;
            /// Merge prefix path and path of array element.
            ctx.paths.push_back(ctx.builder.append(path, true).get_parts());
            ctx.values.push_back(Field::create_field<TYPE_ARRAY>(std::move(path_array)));
            ctx.builder.pop_back(path.size());
        }
    }
}

template <typename ParserImpl>
void JSONDataParser<ParserImpl>::traverseArrayElement(const Element& element,
                                                      ParseArrayContext& ctx) {
    ParseContext element_ctx;
    element_ctx.has_nested_in_flatten = ctx.has_nested_in_flatten;
    element_ctx.is_top_array = ctx.is_top_array;
    traverse(element, element_ctx);
    auto& [_, paths, values, flatten_nested, __, is_top_array] = element_ctx;

    if (element_ctx.has_nested_in_flatten && is_top_array) {
        checkAmbiguousStructure(ctx, paths);
    }

    size_t size = paths.size();
    size_t keys_to_update = ctx.arrays_by_path.size();

    for (size_t i = 0; i < size; ++i) {
        if (values[i].is_null()) {
            continue;
        }

        UInt128 hash = PathInData::get_parts_hash(paths[i]);
        auto found = ctx.arrays_by_path.find(hash);

        if (found != ctx.arrays_by_path.end()) {
            handleExistingPath(found->second, paths[i], values[i], ctx, keys_to_update);
        } else {
            handleNewPath(hash, paths[i], values[i], ctx);
        }
    }

    if (keys_to_update && !is_top_array) {
        fillMissedValuesInArrays(ctx);
    }
}

// check if the structure of top_array is ambiguous like:
// [{"a": {"b": {"c": 1}}}, {"a": {"b": 1}}] a.b is ambiguous
template <typename ParserImpl>
void JSONDataParser<ParserImpl>::checkAmbiguousStructure(
        const ParseArrayContext& ctx, const std::vector<PathInData::Parts>& paths) {
    for (auto&& current_path : paths) {
        for (auto it = ctx.arrays_by_path.begin(); it != ctx.arrays_by_path.end(); ++it) {
            auto&& [p, _] = it->second;
            if (is_prefix(p, current_path) || is_prefix(current_path, p)) {
                throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                       "Ambiguous structure of top_array nested subcolumns: {}, {}",
                                       PathInData(p).to_jsonpath(),
                                       PathInData(current_path).to_jsonpath());
            }
        }
    }
}

template <typename ParserImpl>
void JSONDataParser<ParserImpl>::handleExistingPath(std::pair<PathInData::Parts, Array>& path_data,
                                                    const PathInData::Parts& path, Field& value,
                                                    ParseArrayContext& ctx,
                                                    size_t& keys_to_update) {
    auto& path_array = path_data.second;
    // For top_array structure we no need to check cur array size equals ctx.current_size
    // because we do not need to maintain the association information between Nested in array
    if (!ctx.is_top_array) {
        assert(path_array.size() == ctx.current_size);
    }
    // If current element of array is part of Nested,
    // collect its size or check it if the size of
    // the Nested has been already collected.
    auto nested_key = getNameOfNested(path, value);
    if (!nested_key.empty()) {
        size_t array_size = get<const Array&>(value).size();
        auto& current_nested_sizes = ctx.nested_sizes_by_key[nested_key];
        if (current_nested_sizes.size() == ctx.current_size) {
            current_nested_sizes.push_back(array_size);
        } else if (array_size != current_nested_sizes.back()) {
            throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                                   "Array sizes mismatched ({} and {})", array_size,
                                   current_nested_sizes.back());
        }
    }

    path_array.push_back(std::move(value));
    --keys_to_update;
}

template <typename ParserImpl>
void JSONDataParser<ParserImpl>::handleNewPath(UInt128 hash, const PathInData::Parts& path,
                                               Field& value, ParseArrayContext& ctx) {
    Array path_array;
    path_array.reserve(ctx.total_size);

    // For top_array structure we no need to resize array
    // because we no need to fill default values for maintaining the association information between Nested in array
    if (!ctx.is_top_array) {
        path_array.resize(ctx.current_size);
    }

    auto nested_key = getNameOfNested(path, value);
    if (!nested_key.empty()) {
        size_t array_size = get<const Array&>(value).size();
        auto& current_nested_sizes = ctx.nested_sizes_by_key[nested_key];
        if (current_nested_sizes.empty()) {
            current_nested_sizes.resize(ctx.current_size);
        } else {
            // If newly added element is part of the Nested then
            // resize its elements to keep correct sizes of Nested arrays.
            for (size_t j = 0; j < ctx.current_size; ++j) {
                path_array[j] = Field::create_field<TYPE_ARRAY>(Array(current_nested_sizes[j]));
            }
        }
        if (current_nested_sizes.size() == ctx.current_size) {
            current_nested_sizes.push_back(array_size);
        } else if (array_size != current_nested_sizes.back()) {
            throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                                   "Array sizes mismatched ({} and {})", array_size,
                                   current_nested_sizes.back());
        }
    }

    path_array.push_back(std::move(value));
    auto& elem = ctx.arrays_by_path[hash];
    elem.first = std::move(path);
    elem.second = std::move(path_array);
}

template <typename ParserImpl>
void JSONDataParser<ParserImpl>::fillMissedValuesInArrays(ParseArrayContext& ctx) {
    for (auto it = ctx.arrays_by_path.begin(); it != ctx.arrays_by_path.end(); ++it) {
        auto& [path, path_array] = it->second;
        assert(path_array.size() == ctx.current_size || path_array.size() == ctx.current_size + 1);
        if (path_array.size() == ctx.current_size) {
            bool inserted = tryInsertDefaultFromNested(ctx, path, path_array);
            if (!inserted) {
                path_array.emplace_back();
            }
        }
    }
}

template <typename ParserImpl>
bool JSONDataParser<ParserImpl>::tryInsertDefaultFromNested(ParseArrayContext& ctx,
                                                            const PathInData::Parts& path,
                                                            Array& array) {
    /// If there is a collected size of current Nested
    /// then insert array of this size as a default value.
    if (path.empty() || array.empty()) {
        return false;
    }
    /// Last element is not Null, because otherwise this path wouldn't exist.
    auto nested_key = getNameOfNested(path, array.back());
    if (nested_key.empty()) {
        return false;
    }
    auto mapped = ctx.nested_sizes_by_key.find(nested_key);
    if (mapped == ctx.nested_sizes_by_key.end()) {
        return false;
    }
    auto& current_nested_sizes = mapped->second;
    assert(current_nested_sizes.size() == ctx.current_size ||
           current_nested_sizes.size() == ctx.current_size + 1);
    /// If all keys of Nested were missed then add a zero length.
    if (current_nested_sizes.size() == ctx.current_size) {
        current_nested_sizes.push_back(0);
    }
    size_t array_size = current_nested_sizes.back();
    array.push_back(Field::create_field<TYPE_ARRAY>(Array(array_size)));
    return true;
}

template <typename ParserImpl>
StringRef JSONDataParser<ParserImpl>::getNameOfNested(const PathInData::Parts& path,
                                                      const Field& value) {
    if (value.get_type() != PrimitiveType::TYPE_ARRAY || path.empty()) {
        return {};
    }
    /// Find first key that is marked as nested,
    /// because we may have tuple of Nested and there could be
    /// several arrays with the same prefix, but with independent sizes.
    /// Consider we have array element with type `k2 Tuple(k3 Nested(...), k5 Nested(...))`
    /// Then subcolumns `k2.k3` and `k2.k5` may have indepented sizes and we should extract
    /// `k3` and `k5` keys instead of `k2`.
    for (const auto& part : path) {
        if (part.is_nested) {
            return {part.key.data(), part.key.size()};
        }
    }
    return {};
}

#include "common/compile_check_end.h"

template class JSONDataParser<SimdJSONParser>;
} // namespace doris::vectorized
