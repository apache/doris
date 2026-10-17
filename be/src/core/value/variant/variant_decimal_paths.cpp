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

#include "core/value/variant/variant_decimal_paths.h"

#include <cstddef>
#include <cstring>
#include <utility>

#include "exec/common/variant_util.h"

namespace doris {

VariantDecimalPathSet::VariantDecimalPathSet(std::vector<VariantDecimalPath> paths)
        : _paths(std::move(paths)) {
    for (const VariantDecimalPath& path : _paths) {
        if (path.is_glob) {
            _globs.push_back(path.pattern);
        } else {
            _exact.insert(path.pattern);
        }
    }
}

bool VariantDecimalPathSet::matches(std::string_view path) const {
    if (path.empty()) {
        return false;
    }
    const std::string candidate(path);
    if (_exact.find(candidate) != _exact.end()) {
        return true;
    }
    for (const std::string& glob : _globs) {
        if (variant_util::glob_match_re2(glob, candidate)) {
            return true;
        }
    }
    return false;
}

namespace {

// Deeper documents are left to the parser, which enforces its own nesting limit on the original text.
constexpr size_t kMaxQuoteDepth = 512;

// A single forward pass over the JSON text. It understands exactly the JSON grammar (objects, arrays,
// strings with escapes, numbers, the three literals, whitespace) and copies everything it reads; the
// only bytes it adds are the two quotes around a qualifying number.
class DecimalPathQuoter {
public:
    DecimalPathQuoter(StringRef json, const VariantDecimalPathSet& paths, std::string* out)
            : _data(json.data), _size(json.size), _paths(paths), _out(out) {}

    bool run() {
        _out->clear();
        _out->reserve(_size + 64);
        skip_whitespace();
        if (!value(0)) {
            return false;
        }
        skip_whitespace();
        return _pos == _size && _changed;
    }

private:
    static bool is_digit(char c) { return c >= '0' && c <= '9'; }

    bool at_end() const { return _pos >= _size; }
    char peek() const { return _data[_pos]; }
    void copy_byte() { _out->push_back(_data[_pos++]); }

    void skip_whitespace() {
        while (!at_end()) {
            const char c = peek();
            if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
                return;
            }
            copy_byte();
        }
    }

    // Copies one JSON string starting at its opening quote. When `key` is given, the raw bytes between
    // the quotes are stored in it, escapes left as written.
    bool string(std::string* key) {
        if (at_end() || peek() != '"') {
            return false;
        }
        copy_byte();
        const size_t begin = _pos;
        while (!at_end()) {
            const char c = peek();
            if (c == '\\') {
                if (_pos + 1 >= _size) {
                    return false;
                }
                copy_byte();
                copy_byte();
                continue;
            }
            if (c == '"') {
                if (key != nullptr) {
                    key->assign(_data + begin, _pos - begin);
                }
                copy_byte();
                return true;
            }
            copy_byte();
        }
        return false;
    }

    bool number() {
        const size_t begin = _pos;
        if (!at_end() && peek() == '-') {
            ++_pos;
        }
        size_t digits = 0;
        while (!at_end() && is_digit(peek())) {
            ++_pos;
            ++digits;
        }
        if (digits == 0) {
            return false;
        }
        bool fraction = false;
        if (!at_end() && peek() == '.') {
            ++_pos;
            size_t fraction_digits = 0;
            while (!at_end() && is_digit(peek())) {
                ++_pos;
                ++fraction_digits;
            }
            if (fraction_digits == 0) {
                return false;
            }
            fraction = true;
        }
        bool exponent = false;
        if (!at_end() && (peek() == 'e' || peek() == 'E')) {
            ++_pos;
            if (!at_end() && (peek() == '+' || peek() == '-')) {
                ++_pos;
            }
            size_t exponent_digits = 0;
            while (!at_end() && is_digit(peek())) {
                ++_pos;
                ++exponent_digits;
            }
            if (exponent_digits == 0) {
                return false;
            }
            exponent = true;
        }
        const bool quote = fraction && !exponent && _paths.matches(_path);
        if (quote) {
            _out->push_back('"');
        }
        _out->append(_data + begin, _pos - begin);
        if (quote) {
            _out->push_back('"');
            _changed = true;
        }
        return true;
    }

    bool literal(const char* text, size_t length) {
        if (_pos + length > _size || std::memcmp(_data + _pos, text, length) != 0) {
            return false;
        }
        _out->append(_data + _pos, length);
        _pos += length;
        return true;
    }

    bool value(size_t depth) {
        if (depth > kMaxQuoteDepth || at_end()) {
            return false;
        }
        switch (peek()) {
        case '{':
            return object(depth);
        case '[':
            return array(depth);
        case '"':
            return string(nullptr);
        case 't':
            return literal("true", 4);
        case 'f':
            return literal("false", 5);
        case 'n':
            return literal("null", 4);
        default:
            return number();
        }
    }

    bool object(size_t depth) {
        copy_byte(); // '{'
        skip_whitespace();
        if (!at_end() && peek() == '}') {
            copy_byte();
            return true;
        }
        while (true) {
            skip_whitespace();
            std::string key;
            if (!string(&key)) {
                return false;
            }
            skip_whitespace();
            if (at_end() || peek() != ':') {
                return false;
            }
            copy_byte();
            skip_whitespace();
            const size_t path_size = _path.size();
            if (!_path.empty()) {
                _path.push_back('.');
            }
            _path.append(key);
            const bool ok = value(depth + 1);
            _path.resize(path_size);
            if (!ok) {
                return false;
            }
            skip_whitespace();
            if (at_end()) {
                return false;
            }
            if (peek() == ',') {
                copy_byte();
                continue;
            }
            if (peek() == '}') {
                copy_byte();
                return true;
            }
            return false;
        }
    }

    // Array elements keep their array's path: `[1.5, 2.5]` under a DECIMAL `prices` template is
    // two values at `prices`, matching how the shredder types an ARRAY<DECIMAL> path.
    bool array(size_t depth) {
        copy_byte(); // '['
        skip_whitespace();
        if (!at_end() && peek() == ']') {
            copy_byte();
            return true;
        }
        while (true) {
            skip_whitespace();
            if (!value(depth + 1)) {
                return false;
            }
            skip_whitespace();
            if (at_end()) {
                return false;
            }
            if (peek() == ',') {
                copy_byte();
                continue;
            }
            if (peek() == ']') {
                copy_byte();
                return true;
            }
            return false;
        }
    }

    const char* _data;
    size_t _size;
    const VariantDecimalPathSet& _paths;
    std::string* _out;
    size_t _pos = 0;
    bool _changed = false;
    std::string _path;
};

} // namespace

bool quote_decimal_numbers_on_paths(StringRef json, const VariantDecimalPathSet& paths,
                                    std::string* out) {
    if (out == nullptr || paths.empty() || json.size == 0 || json.data == nullptr) {
        return false;
    }
    try {
        DecimalPathQuoter quoter(json, paths, out);
        return quoter.run();
    } catch (...) {
        return false;
    }
}

} // namespace doris
