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

#include <rapidjson/document.h>
#include <simdjson.h>

#include "vec/core/types.h"

namespace doris::vectorized {

/// This class can be used as an argument for the template class FunctionJSON.
/// It provides ability to parse JSONs using simdjson library.
class SimdJSONParser {
public:
    class Array;
    class Object;
    /// References an element in a JSON document, representing a JSON null, boolean, string, number,
    /// array or object.
    class Element {
    public:
        ALWAYS_INLINE Element() {} /// NOLINT
        ALWAYS_INLINE Element(const simdjson::dom::element& element_)
                : element(element_) {} /// NOLINT
        ALWAYS_INLINE bool isInt64() const {
            return element.type() == simdjson::dom::element_type::INT64;
        }
        ALWAYS_INLINE bool isUInt64() const {
            return element.type() == simdjson::dom::element_type::UINT64;
        }
        ALWAYS_INLINE bool isDouble() const {
            return element.type() == simdjson::dom::element_type::DOUBLE;
        }
        ALWAYS_INLINE bool isString() const {
            return element.type() == simdjson::dom::element_type::STRING;
        }
        ALWAYS_INLINE bool isArray() const {
            return element.type() == simdjson::dom::element_type::ARRAY;
        }
        ALWAYS_INLINE bool isObject() const {
            return element.type() == simdjson::dom::element_type::OBJECT;
        }
        ALWAYS_INLINE bool isBool() const {
            return element.type() == simdjson::dom::element_type::BOOLEAN;
        }
        ALWAYS_INLINE bool isNull() const {
            return element.type() == simdjson::dom::element_type::NULL_VALUE;
        }
        ALWAYS_INLINE Int64 getInt64() const { return element.get_int64().value_unsafe(); }
        ALWAYS_INLINE double getDouble() const { return element.get_double().value_unsafe(); }
        ALWAYS_INLINE bool getBool() const { return element.get_bool().value_unsafe(); }
        ALWAYS_INLINE std::string_view getString() const {
            return element.get_string().value_unsafe();
        }
        ALWAYS_INLINE Array getArray() const;
        ALWAYS_INLINE Object getObject() const;

    private:
        simdjson::dom::element element;
    };
    /// References an array in a JSON document.
    class Array {
    public:
        class Iterator {
        public:
            ALWAYS_INLINE Iterator(const simdjson::dom::array::iterator& it_)
                    : it(it_) {} /// NOLINT
            ALWAYS_INLINE Element operator*() const { return *it; }
            ALWAYS_INLINE Iterator& operator++() {
                ++it;
                return *this;
            }
            ALWAYS_INLINE friend bool operator!=(const Iterator& left, const Iterator& right) {
                return left.it != right.it;
            }

        private:
            simdjson::dom::array::iterator it;
        };
        ALWAYS_INLINE Array(const simdjson::dom::array& array_) : array(array_) {} /// NOLINT
        ALWAYS_INLINE Iterator begin() const { return array.begin(); }
        ALWAYS_INLINE Iterator end() const { return array.end(); }
        ALWAYS_INLINE size_t size() const { return array.size(); }
        ALWAYS_INLINE Element operator[](size_t index) const {
            assert(index < size());
            return array.at(index).value_unsafe();
        }

    private:
        simdjson::dom::array array;
    };
    using KeyValuePair = std::pair<std::string_view, Element>;
    /// References an object in a JSON document.
    class Object {
    public:
        class Iterator {
        public:
            ALWAYS_INLINE Iterator(const simdjson::dom::object::iterator& it_)
                    : it(it_) {} /// NOLINT
            ALWAYS_INLINE KeyValuePair operator*() const {
                const auto& res = *it;
                return {res.key, res.value};
            }
            ALWAYS_INLINE Iterator& operator++() {
                ++it;
                return *this;
            }
            ALWAYS_INLINE Iterator operator++(int) {
                auto res = *this;
                ++it;
                return res;
            } /// NOLINT
            ALWAYS_INLINE friend bool operator!=(const Iterator& left, const Iterator& right) {
                return left.it != right.it;
            }
            ALWAYS_INLINE friend bool operator==(const Iterator& left, const Iterator& right) {
                return !(left != right);
            }

        private:
            simdjson::dom::object::iterator it;
        };
        ALWAYS_INLINE Object(const simdjson::dom::object& object_) : object(object_) {} /// NOLINT
        ALWAYS_INLINE Iterator begin() const { return object.begin(); }
        ALWAYS_INLINE Iterator end() const { return object.end(); }
        ALWAYS_INLINE size_t size() const { return object.size(); }
        /// Optional: Provides access to an object's element by index.
        KeyValuePair operator[](size_t index) const {
            assert(index < size());
            auto it = object.begin();
            while (index--) {
                ++it;
            }
            const auto& res = *it;
            return {res.key, res.value};
        }

    private:
        simdjson::dom::object object;
    };
    /// Parses a JSON document, returns the reference to its root element if succeeded.
    bool parse(const char* data, size_t size, Element& result) {
        auto document = parser.parse(data, size);
        if (document.error()) {
            return false;
        }
        result = document.value_unsafe();
        return true;
    }

private:
    simdjson::dom::parser parser;
};
inline ALWAYS_INLINE SimdJSONParser::Array SimdJSONParser::Element::getArray() const {
    return element.get_array().value_unsafe();
}
inline ALWAYS_INLINE SimdJSONParser::Object SimdJSONParser::Element::getObject() const {
    return element.get_object().value_unsafe();
}

} // namespace doris::vectorized