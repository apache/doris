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

#include <iterator>
#include <list>
#include <unordered_map>

namespace doris {

template <typename Key, typename Value>
class LruCache {
public:
    typedef typename std::pair<Key, Value> KeyValuePair;
    typedef typename std::list<KeyValuePair>::iterator ListIterator;

    class Iterator {
    public:
        using iterator_category = std::input_iterator_tag;
        using value_type = KeyValuePair;
        using difference_type = ptrdiff_t;
        using pointer = KeyValuePair*;
        using reference = KeyValuePair&;

        Iterator(typename std::unordered_map<Key, ListIterator>::iterator it) : _it(it) {}

        Iterator& operator++() {
            ++_it;
            return *this;
        }

        bool operator==(const Iterator& rhs) const { return _it == rhs._it; }

        bool operator!=(const Iterator& rhs) const { return _it != rhs._it; }

        KeyValuePair* operator->() { return _it->second.operator->(); }

        KeyValuePair& operator*() { return *_it->second; }

    private:
        typename std::unordered_map<Key, ListIterator>::iterator _it;
    };

    LruCache(size_t max_size) : _max_size(max_size) {}

    void put(const Key& key, const Value& value) {
        auto it = _cache_items_map.find(key);
        if (it != _cache_items_map.end()) {
            _cache_items_list.erase(it->second);
            _cache_items_map.erase(it);
        }

        _cache_items_list.push_front(KeyValuePair(key, value));
        _cache_items_map[key] = _cache_items_list.begin();

        if (_cache_items_map.size() > _max_size) {
            auto last = _cache_items_list.end();
            last--;
            _cache_items_map.erase(last->first);
            _cache_items_list.pop_back();
        }
    }

    void erase(const Key& key) {
        auto it = _cache_items_map.find(key);
        if (it != _cache_items_map.end()) {
            _cache_items_list.erase(it->second);
            _cache_items_map.erase(it);
        }
    }

    // Must copy value, because value maybe relased when caller used
    bool get(const Key& key, Value* value) {
        auto it = _cache_items_map.find(key);
        if (it == _cache_items_map.end()) {
            return false;
        }
        _cache_items_list.splice(_cache_items_list.begin(), _cache_items_list, it->second);
        *value = it->second->second;
        return true;
    }

    bool exists(const Key& key) const {
        return _cache_items_map.find(key) != _cache_items_map.end();
    }

    size_t size() const { return _cache_items_map.size(); }

    Iterator begin() { return Iterator(_cache_items_map.begin()); }

    Iterator end() { return Iterator(_cache_items_map.end()); }

private:
    std::list<KeyValuePair> _cache_items_list;
    std::unordered_map<Key, ListIterator> _cache_items_map;
    size_t _max_size;
};

} // namespace doris
