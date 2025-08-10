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

#include <list>
#include <unordered_map>
#include <utility>
#include <vector>

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

template <typename Key, typename Value>
class LinkedHashMap {
public:
    void insert(const Key& key, const Value& value) {
        auto it = _map.find(key);
        if (it != _map.end()) {
            it->second.first = value;
        } else {
            _order_list.push_back(key);
            auto listIt = std::prev(_order_list.end());
            _map[key] = {value, listIt};
        }
    }

    Value* find(const Key& key) {
        auto it = _map.find(key);
        if (it != _map.end()) {
            return &it->second.first;
        }
        return nullptr;
    }

    const Value* find(const Key& key) const {
        auto it = _map.find(key);
        if (it != _map.end()) {
            return &it->second.first;
        }
        return nullptr;
    }

    void erase(const Key& key) {
        auto it = _map.find(key);
        if (it != _map.end()) {
            _order_list.erase(it->second.second);
            _map.erase(it);
        }
    }

    bool contains(const Key& key) const { return _map.find(key) != _map.end(); }

    size_t size() const { return _map.size(); }

    bool empty() const { return _map.empty(); }

    std::vector<Key> to_vector() const {
        return std::vector<Key>(_order_list.begin(), _order_list.end());
    }

private:
    std::list<Key> _order_list;
    std::unordered_map<Key, std::pair<Value, typename std::list<Key>::iterator>> _map;
};

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index
