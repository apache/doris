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

#include "core/data_type_serde/bson_value_view.h"

#include <rapidjson/document.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <cstring>
#include <memory>
#include <string_view>

namespace doris {

const BsonBinding* BsonBinding::child(const std::string& child_name, size_t ordinal) const {
    if (ordinal < children.size() && children[ordinal].name == child_name) {
        return &children[ordinal];
    }
    auto position = child_positions.find(child_name);
    if (position != child_positions.end()) {
        return &children[position->second];
    }
    // Programmatically constructed bindings (including tests) may omit the derived index.
    if (child_positions.empty()) {
        for (const auto& candidate : children) {
            if (candidate.name == child_name) {
                return &candidate;
            }
        }
    }
    return nullptr;
}

BsonValueView BsonValueView::of(const bson_value_t& v) {
    BsonValueView result;
    result.value = v;
    result.missing = false;
    return result;
}

BsonValueView BsonValueView::document(const bson_t& document) {
    bson_value_t value {};
    value.value_type = BSON_TYPE_DOCUMENT;
    value.value.v_doc.data = const_cast<uint8_t*>(bson_get_data(&document));
    value.value.v_doc.data_len = document.len;
    return of(value);
}

BsonValueView BsonValueView::at_path(const std::vector<std::string>& path) const {
    BsonValueView current = *this;
    for (const auto& segment : path) {
        if (current.is_null()) {
            return {};
        }
        if (current.value.value_type != BSON_TYPE_DOCUMENT) {
            // For path [a,b], {a:1} is a type mismatch; {a:{}} is a genuinely missing child.
            BsonValueView mismatch;
            mismatch.path_mismatch = true;
            return mismatch;
        }
        bson_t document;
        bson_iter_t it;
        if (!bson_init_static(&document, current.value.value.v_doc.data,
                              current.value.value.v_doc.data_len) ||
            !bson_iter_init_find(&it, &document, segment.c_str())) {
            return {};
        }
        current = of(*bson_iter_value(&it));
    }
    return current;
}

BsonDecodeContext BsonDecodeContext::child(const BsonBinding* child_binding) const {
    return {.binding = child_binding, .mismatch_as_null = mismatch_as_null, .depth = depth + 1};
}

Status BsonDecodeContext::validate(const BsonValueView& value) const {
    if (value.path_mismatch) {
        return Status::InvalidArgument("BSON field path traverses a non-document value");
    }
    if (depth > MAX_DEPTH) {
        return Status::InvalidArgument("MongoDB BSON nesting exceeds the decoding limit");
    }
    if (value.is_null()) {
        return Status::OK();
    }
    if (binding && !binding->types.empty() &&
        std::ranges::find(binding->types, static_cast<int>(value.value.value_type)) ==
                binding->types.end()) {
        return Status::InvalidArgument("MongoDB BSON type does not match the pinned schema");
    }
    if ((value.value.value_type == BSON_TYPE_DOCUMENT ||
         value.value.value_type == BSON_TYPE_ARRAY) &&
        value.value.value.v_doc.data_len > MAX_DOCUMENT_BYTES) {
        return Status::InvalidArgument("MongoDB BSON document exceeds the decoding size limit");
    }
    return Status::OK();
}

namespace {
Status parse_binding(const rapidjson::Value& value, BsonBinding* out, int depth, size_t* count) {
    if (!value.IsObject() || depth > BsonDecodeContext::MAX_DEPTH || ++*count > 4096 ||
        !value.HasMember("name") || !value["name"].IsString() || !value.HasMember("path") ||
        !value["path"].IsArray() || !value.HasMember("bson_types") ||
        !value["bson_types"].IsArray() || !value.HasMember("children") ||
        !value["children"].IsArray()) {
        return Status::InvalidArgument("Invalid MongoDB binding descriptor");
    }
    out->name = value["name"].GetString();
    for (const auto& segment : value["path"].GetArray()) {
        if (!segment.IsString() || std::memchr(segment.GetString(), 0, segment.GetStringLength())) {
            return Status::InvalidArgument("Invalid MongoDB binding path");
        }
        out->path.emplace_back(segment.GetString(), segment.GetStringLength());
    }
    for (const auto& type : value["bson_types"].GetArray()) {
        if (!type.IsInt()) {
            return Status::InvalidArgument("Invalid MongoDB BSON tag");
        }
        out->types.push_back(type.GetInt());
    }
    if (value.HasMember("subtype") && value["subtype"].IsInt()) {
        out->subtype = value["subtype"].GetInt();
    }
    if (value.HasMember("special") && value["special"].IsString()) {
        out->special = value["special"].GetString();
    }
    for (const auto& child : value["children"].GetArray()) {
        BsonBinding binding;
        RETURN_IF_ERROR(parse_binding(child, &binding, depth + 1, count));
        if (!out->child_positions.emplace(binding.name, out->children.size()).second) {
            return Status::InvalidArgument("Duplicate BSON child binding");
        }
        out->children.push_back(std::move(binding));
    }
    return Status::OK();
}
} // namespace

Status parse_bson_bindings(const std::string& json, std::vector<BsonBinding>* bindings) {
    if (json.size() > 1024 * 1024) {
        return Status::InvalidArgument("MongoDB schema descriptor exceeds the size limit");
    }
    rapidjson::Document document;
    document.Parse(json.data(), json.size());
    if (document.HasParseError() || !document.IsObject() || !document.HasMember("columns") ||
        !document["columns"].IsArray() || document["columns"].Empty() ||
        !document.HasMember("version") || !document["version"].IsInt() ||
        document["version"].GetInt() != 1) {
        return Status::InvalidArgument("Invalid MongoDB schema descriptor");
    }
    size_t count = 0;
    std::vector<BsonBinding> result;
    for (const auto& column : document["columns"].GetArray()) {
        BsonBinding binding;
        RETURN_IF_ERROR(parse_binding(column, &binding, 0, &count));
        if (std::ranges::any_of(result, [&](const auto& b) { return b.name == binding.name; })) {
            return Status::InvalidArgument("Duplicate MongoDB binding name");
        }
        result.push_back(std::move(binding));
    }
    *bindings = std::move(result);
    return Status::OK();
}

Status bson_decimal_exact(const bson_decimal128_t& value, int precision, int scale,
                          std::string* text) {
    char raw[BSON_DECIMAL128_STRING];
    bson_decimal128_to_string(&value, raw);
    std::string source(raw);
    size_t pos = source[0] == '-' || source[0] == '+' ? 1 : 0;
    bool negative = source[0] == '-';
    std::string digits;
    int fractional = 0;
    bool point = false;
    for (; pos < source.size() && source[pos] != 'E' && source[pos] != 'e'; ++pos) {
        char c = source[pos];
        if (c == '.' && !point) {
            point = true;
        } else if (c >= '0' && c <= '9') {
            digits += c;
            fractional += point;
        } else {
            return Status::InvalidArgument(
                    "Non-finite BSON Decimal128 has no Doris DECIMAL representation");
        }
    }
    int exponent = 0;
    if (pos < source.size()) {
        ++pos;
        bool plus = source[pos] == '+';
        pos += plus;
        auto parsed = std::from_chars(source.data() + pos, source.data() + source.size(), exponent);
        if (parsed.ec != std::errc() || parsed.ptr != source.data() + source.size()) {
            return Status::InvalidArgument("Invalid BSON Decimal128 exponent");
        }
    }
    auto first = digits.find_first_not_of('0');
    if (first == std::string::npos) {
        *text = "0";
        return Status::OK();
    }
    digits.erase(0, first);
    int shift = scale + exponent - fractional;
    if (shift < 0) {
        auto removed = static_cast<size_t>(-shift);
        if (removed >= digits.size() || digits.find_last_not_of('0') >= digits.size() - removed) {
            return Status::InvalidArgument("BSON Decimal128 requires rounding at the target scale");
        }
        digits.resize(digits.size() - removed);
    } else {
        if (digits.size() + shift > static_cast<size_t>(precision)) {
            return Status::InvalidArgument("BSON Decimal128 exceeds the target precision");
        }
        digits.append(shift, '0');
    }
    if (digits.size() > static_cast<size_t>(precision)) {
        return Status::InvalidArgument("BSON Decimal128 exceeds the target precision");
    }
    if (scale > 0) {
        if (digits.size() <= static_cast<size_t>(scale)) {
            digits.insert(0, scale + 1 - digits.size(), '0');
        }
        digits.insert(digits.size() - scale, 1, '.');
    }
    *text = (negative ? "-" : "") + digits;
    return Status::OK();
}

Status validate_bson_tree(const BsonValueView& root) {
    auto is_container = [](bson_type_t type) {
        return type == BSON_TYPE_DOCUMENT || type == BSON_TYPE_ARRAY ||
               type == BSON_TYPE_CODEWSCOPE;
    };
    // Scalars use another union member and expose no child-document bytes to traverse.
    if (!is_container(root.value.value_type)) {
        return Status::OK();
    }
    // A wide array of empty documents can fit in 16 MiB yet queue tens of MiB of sibling
    // values. Retain only one iterator per ancestor: scratch is bounded by depth, not width.
    std::array<bson_iter_t, BsonDecodeContext::MAX_DEPTH + 1> pending;
    auto enter = [&](const bson_value_t& value, int depth) -> Status {
        // The root occupies depth zero; reject the next nested container before indexing the stack.
        if (depth > BsonDecodeContext::MAX_DEPTH) {
            return Status::InvalidArgument("BSON document exceeds the nesting limit");
        }
        bool scope = value.value_type == BSON_TYPE_CODEWSCOPE;
        const uint8_t* data = scope ? value.value.v_codewscope.scope_data : value.value.v_doc.data;
        uint32_t length = scope ? value.value.v_codewscope.scope_len : value.value.v_doc.data_len;
        bson_t document;
        if (length > BsonDecodeContext::MAX_DOCUMENT_BYTES ||
            !bson_init_static(&document, data, length) ||
            !bson_iter_init(&pending[depth], &document)) {
            return Status::Corruption("Invalid nested BSON document");
        }
        return Status::OK();
    };
    int depth = 0;
    RETURN_IF_ERROR(enter(root.value, depth));
    size_t elements = 0;
    while (depth >= 0) {
        // Resume the parent after this container ends instead of queueing all its siblings.
        if (!bson_iter_next(&pending[depth])) {
            --depth;
            continue;
        }
        if (++elements > BsonDecodeContext::MAX_ELEMENTS) {
            return Status::InvalidArgument("BSON document exceeds the element limit");
        }
        const auto* value = bson_iter_value(&pending[depth]);
        if (is_container(value->value_type)) {
            RETURN_IF_ERROR(enter(*value, ++depth));
        }
    }
    return Status::OK();
}

Status bson_canonical_json(const BsonValueView& value, std::string* json) {
    if (value.missing) {
        return Status::InvalidArgument("Missing BSON value cannot be materialized as JSON");
    }
    RETURN_IF_ERROR(validate_bson_tree(value));
    bson_t wrapper;
    bson_init(&wrapper);
    if (!bson_append_value(&wrapper, "v", 1, &value.value)) {
        bson_destroy(&wrapper);
        return Status::InvalidArgument(
                "BSON value cannot be represented as canonical Extended JSON");
    }
    size_t length = 0;
    // Copying the result can throw on memory limits; keep the driver buffer owned during that copy.
    std::unique_ptr<char, decltype(&bson_free)> encoded(
            bson_as_canonical_extended_json(&wrapper, &length), &bson_free);
    bson_destroy(&wrapper);
    if (!encoded) {
        return Status::InvalidArgument("BSON canonical Extended JSON encoding failed");
    }
    // libbson emits { "v" : VALUE }. The wrapper is fixed; VALUE may itself contain braces or quotes.
    std::string_view result(encoded.get(), length);
    size_t begin = result.find(':') + 1;
    size_t end = result.find_last_of('}');
    while (begin < end && result[begin] == ' ') {
        ++begin;
    }
    while (end > begin && result[end - 1] == ' ') {
        --end;
    }
    json->assign(result.substr(begin, end - begin));
    return Status::OK();
}

} // namespace doris
