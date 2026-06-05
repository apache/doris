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

#include <simdjson.h>

#include <cassert>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "core/types.h"

namespace doris {

/// This class can be used as an argument for the template class FunctionJSON.
/// It provides ability to parse JSONs using simdjson library.
class SimdJSONParser {
    struct Node {
        enum class Type {
            INT64,
            UINT64,
            BIG_INTEGER,
            DOUBLE,
            STRING,
            ARRAY,
            OBJECT,
            BOOL,
            NULL_VALUE,
        };

        Type type = Type::NULL_VALUE;
        Int64 int64_value = 0;
        UInt64 uint64_value = 0;
        double double_value = 0;
        bool bool_value = false;
        std::string string_value;
        std::string raw_number;
        std::vector<Node> array_values;
        std::vector<std::string> object_keys;
        std::vector<Node> object_values;
    };

public:
    class Array;
    class Object;
    /// References an element in a JSON document, representing a JSON null, boolean, string, number,
    /// array or object.
    class Element {
    public:
        ALWAYS_INLINE Element() {}                                         /// NOLINT
        ALWAYS_INLINE explicit Element(const Node* node_) : node(node_) {} /// NOLINT
        ALWAYS_INLINE bool isInt64() const {
            assert(node != nullptr);
            return node->type == Node::Type::INT64;
        }
        ALWAYS_INLINE bool isUInt64() const {
            assert(node != nullptr);
            return node->type == Node::Type::UINT64;
        }
        ALWAYS_INLINE bool isBigInteger() const {
            assert(node != nullptr);
            return node->type == Node::Type::BIG_INTEGER;
        }
        ALWAYS_INLINE bool isNumber() const {
            assert(node != nullptr);
            return node->type == Node::Type::INT64 || node->type == Node::Type::UINT64 ||
                   node->type == Node::Type::BIG_INTEGER || node->type == Node::Type::DOUBLE;
        }
        ALWAYS_INLINE bool isDouble() const {
            assert(node != nullptr);
            return node->type == Node::Type::DOUBLE;
        }
        ALWAYS_INLINE bool isString() const {
            assert(node != nullptr);
            return node->type == Node::Type::STRING;
        }
        ALWAYS_INLINE bool isArray() const {
            assert(node != nullptr);
            return node->type == Node::Type::ARRAY;
        }
        ALWAYS_INLINE bool isObject() const {
            assert(node != nullptr);
            return node->type == Node::Type::OBJECT;
        }
        ALWAYS_INLINE bool isBool() const {
            assert(node != nullptr);
            return node->type == Node::Type::BOOL;
        }
        ALWAYS_INLINE bool isNull() const {
            assert(node != nullptr);
            return node->type == Node::Type::NULL_VALUE;
        }
        ALWAYS_INLINE Int64 getInt64() const {
            assert(node != nullptr);
            return node->int64_value;
        }
        ALWAYS_INLINE double getDouble() const {
            assert(node != nullptr);
            return node->double_value;
        }
        ALWAYS_INLINE bool getBool() const {
            assert(node != nullptr);
            return node->bool_value;
        }
        ALWAYS_INLINE std::string_view getString() const {
            assert(node != nullptr);
            return node->string_value;
        }
        ALWAYS_INLINE UInt64 getUInt64() const {
            assert(node != nullptr);
            return node->uint64_value;
        }
        ALWAYS_INLINE std::string_view getRawNumber() const {
            assert(node != nullptr);
            return node->raw_number;
        }
        ALWAYS_INLINE Array getArray() const;
        ALWAYS_INLINE Object getObject() const;

    private:
        const Node* node = nullptr;
    };
    /// References an array in a JSON document.
    class Array {
    public:
        class Iterator {
        public:
            using NodeIterator = std::vector<Node>::const_iterator;
            ALWAYS_INLINE explicit Iterator(NodeIterator it_) : it(it_) {} /// NOLINT
            ALWAYS_INLINE Element operator*() const { return Element(&*it); }
            ALWAYS_INLINE Iterator& operator++() {
                ++it;
                return *this;
            }
            ALWAYS_INLINE friend bool operator!=(const Iterator& left, const Iterator& right) {
                return left.it != right.it;
            }

        private:
            NodeIterator it;
        };
        ALWAYS_INLINE explicit Array(const std::vector<Node>* array_) : array(array_) {} /// NOLINT
        ALWAYS_INLINE Iterator begin() const { return Iterator(array->begin()); }
        ALWAYS_INLINE Iterator end() const { return Iterator(array->end()); }
        ALWAYS_INLINE size_t size() const { return array->size(); }
        ALWAYS_INLINE Element operator[](size_t index) const {
            assert(index < size());
            return Element(&(*array)[index]);
        }

    private:
        const std::vector<Node>* array;
    };
    using KeyValuePair = std::pair<std::string_view, Element>;
    /// References an object in a JSON document.
    class Object {
    public:
        class Iterator {
        public:
            ALWAYS_INLINE explicit Iterator(const std::vector<std::string>* keys_,
                                            const std::vector<Node>* values_, size_t index_)
                    : index(index_), keys(keys_), values(values_) {} /// NOLINT
            ALWAYS_INLINE KeyValuePair operator*() const {
                return {(*keys)[index], Element(&(*values)[index])};
            }
            ALWAYS_INLINE Iterator& operator++() {
                ++index;
                return *this;
            }
            ALWAYS_INLINE Iterator operator++(int) {
                auto res = *this;
                ++(*this);
                return res;
            } /// NOLINT
            ALWAYS_INLINE friend bool operator!=(const Iterator& left, const Iterator& right) {
                return left.index != right.index;
            }
            ALWAYS_INLINE friend bool operator==(const Iterator& left, const Iterator& right) {
                return !(left != right);
            }

        private:
            size_t index;
            const std::vector<std::string>* keys;
            const std::vector<Node>* values;
        };
        ALWAYS_INLINE explicit Object(const std::vector<std::string>* keys_,
                                      const std::vector<Node>* values_)
                : keys(keys_), values(values_) {} /// NOLINT
        ALWAYS_INLINE Iterator begin() const { return Iterator(keys, values, 0); }
        ALWAYS_INLINE Iterator end() const { return Iterator(keys, values, size()); }
        ALWAYS_INLINE size_t size() const { return values->size(); }
        /// Optional: Provides access to an object's element by index.
        KeyValuePair operator[](size_t index) const {
            assert(index < size());
            return {(*keys)[index], Element(&(*values)[index])};
        }

    private:
        const std::vector<std::string>* keys;
        const std::vector<Node>* values;
    };
    /// Parses a JSON document, returns the reference to its root element if succeeded.
    bool parse(const char* data, size_t size, Element& result) {
        root = Node();
        return parse_ondemand(data, size, result);
    }
    void release() { root = Node(); }

private:
    bool parse_ondemand(const char* data, size_t size, Element& result) {
        simdjson::padded_string padded_json(data, size);
        simdjson::ondemand::document document;
        auto error = ondemand_parser.iterate(padded_json).get(document);
        if (error) {
            return false;
        }
        if (!build_node(document, &root)) {
            root = Node();
            return false;
        }
        result = Element(&root);
        return true;
    }

    static std::string_view trim_raw_number(std::string_view raw_number) {
        auto is_space = [](char ch) { return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'; };
        while (!raw_number.empty() && is_space(raw_number.front())) {
            raw_number.remove_prefix(1);
        }
        while (!raw_number.empty() && is_space(raw_number.back())) {
            raw_number.remove_suffix(1);
        }
        return raw_number;
    }

    template <typename RawNumber>
    static bool assign_raw_number(RawNumber&& raw_number, std::string* out) {
        if constexpr (std::is_same_v<std::decay_t<RawNumber>, std::string_view>) {
            *out = std::string(trim_raw_number(raw_number));
            return true;
        } else {
            std::string_view raw_number_view;
            auto error = std::move(raw_number).get(raw_number_view);
            if (error) {
                return false;
            }
            *out = std::string(trim_raw_number(raw_number_view));
            return true;
        }
    }

    template <typename Value>
    bool build_array_node(Value& value, Node* out) {
        simdjson::ondemand::array array;
        auto error = value.get_array().get(array);
        if (error) {
            return false;
        }
        out->type = Node::Type::ARRAY;
        for (auto element_result : array) {
            simdjson::ondemand::value element;
            error = std::move(element_result).get(element);
            if (error) {
                return false;
            }
            Node element_node;
            if (!build_node(element, &element_node)) {
                return false;
            }
            out->array_values.push_back(std::move(element_node));
        }
        return true;
    }

    template <typename Value>
    bool build_object_node(Value& value, Node* out) {
        simdjson::ondemand::object object;
        auto error = value.get_object().get(object);
        if (error) {
            return false;
        }
        out->type = Node::Type::OBJECT;
        for (auto field_result : object) {
            simdjson::ondemand::field field;
            error = std::move(field_result).get(field);
            if (error) {
                return false;
            }
            std::string_view key;
            error = field.unescaped_key().get(key);
            if (error) {
                return false;
            }
            std::string key_copy(key);
            simdjson::ondemand::value field_value = field.value();
            Node field_node;
            if (!build_node(field_value, &field_node)) {
                return false;
            }
            out->object_keys.push_back(std::move(key_copy));
            out->object_values.push_back(std::move(field_node));
        }
        return true;
    }

    template <typename Value>
    bool build_number_node(Value& value, Node* out) {
        simdjson::ondemand::number_type number_type;
        auto error = value.get_number_type().get(number_type);
        if (error) {
            return false;
        }
        switch (number_type) {
        case simdjson::ondemand::number_type::signed_integer:
            if (!assign_raw_number(value.raw_json_token(), &out->raw_number)) {
                return false;
            }
            out->type = Node::Type::INT64;
            error = value.get_int64().get(out->int64_value);
            return !error;
        case simdjson::ondemand::number_type::unsigned_integer:
            if (!assign_raw_number(value.raw_json_token(), &out->raw_number)) {
                return false;
            }
            out->type = Node::Type::UINT64;
            error = value.get_uint64().get(out->uint64_value);
            return !error;
        case simdjson::ondemand::number_type::floating_point_number:
            if (!assign_raw_number(value.raw_json_token(), &out->raw_number)) {
                return false;
            }
            out->type = Node::Type::DOUBLE;
            error = value.get_double().get(out->double_value);
            return !error;
        case simdjson::ondemand::number_type::big_integer: {
            if (!assign_raw_number(value.raw_json_token(), &out->raw_number)) {
                return false;
            }
            out->type = Node::Type::BIG_INTEGER;
            error = value.get_double().get(out->double_value);
            return !error;
        }
        }
        return false;
    }

    template <typename Value>
    bool build_string_node(Value& value, Node* out) {
        std::string_view str;
        auto error = value.get_string().get(str);
        if (error) {
            return false;
        }
        out->type = Node::Type::STRING;
        out->string_value = std::string(str);
        return true;
    }

    template <typename Value>
    bool build_node(Value& value, Node* out) {
        simdjson::ondemand::json_type type;
        auto error = value.type().get(type);
        if (error) {
            return false;
        }
        switch (type) {
        case simdjson::ondemand::json_type::array:
            return build_array_node(value, out);
        case simdjson::ondemand::json_type::object:
            return build_object_node(value, out);
        case simdjson::ondemand::json_type::number:
            return build_number_node(value, out);
        case simdjson::ondemand::json_type::string: {
            return build_string_node(value, out);
        }
        case simdjson::ondemand::json_type::boolean:
            out->type = Node::Type::BOOL;
            error = value.get_bool().get(out->bool_value);
            return !error;
        case simdjson::ondemand::json_type::null:
            out->type = Node::Type::NULL_VALUE;
            return true;
        }
        return false;
    }

    simdjson::ondemand::parser ondemand_parser;
    Node root;
};
inline ALWAYS_INLINE SimdJSONParser::Array SimdJSONParser::Element::getArray() const {
    assert(node != nullptr);
    return Array(&node->array_values);
}
inline ALWAYS_INLINE SimdJSONParser::Object SimdJSONParser::Element::getObject() const {
    assert(node != nullptr);
    return Object(&node->object_keys, &node->object_values);
}

} // namespace doris
