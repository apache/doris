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

#include <string>
#include <vector>

namespace doris {

class OlapTuple {
public:
    OlapTuple() {}
    OlapTuple(const std::vector<std::string>& values)
            : _values(values), _nulls(values.size(), false) {}

    void add_null() {
        _values.push_back("");
        _nulls.push_back(true);
    }

    void add_value(const std::string& value, bool is_null = false) {
        _values.push_back(value);
        _nulls.push_back(is_null);
    }

    size_t size() const { return _values.size(); }

    void reserve(size_t size) {
        _values.reserve(size);
        _nulls.reserve(size);
    }

    void set_value(size_t i, const std::string& value, bool is_null = false) {
        _values[i] = value;
        _nulls[i] = is_null;
    }

    bool is_null(size_t i) const { return _nulls[i]; }
    const std::string& get_value(size_t i) const { return _values[i]; }
    const std::vector<std::string>& values() const { return _values; }

    void reset() {
        _values.clear();
        _nulls.clear();
    }

private:
    friend std::ostream& operator<<(std::ostream& os, const OlapTuple& tuple);

    std::vector<std::string> _values;
    std::vector<bool> _nulls;
};

inline std::ostream& operator<<(std::ostream& os, const OlapTuple& tuple) {
    for (int i = 0; i < tuple._values.size(); ++i) {
        if (i > 0) {
            os << ",";
        }
        if (tuple._nulls[i]) {
            os << "null";
        } else {
            os << tuple._values[i];
        }
    }
    return os;
}

} // namespace doris
