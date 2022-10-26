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

#include <map>
#include <memory>
#include <string>
#include <vector>

namespace doris {

// This tree is usd for manage restful api path.
template <class T>
class PathTrie {
public:
    PathTrie() : _root("/", "*"), _root_value(nullptr), _separator('/') {};

    ~PathTrie() {
        if (_root_value != nullptr) {
            _allocator.destroy(_root_value);
            _allocator.deallocate(_root_value, 1);
        }
    }

    class Allocator {
    public:
        using value_type = T;

        T* allocate(size_t n) { return static_cast<T*>(::operator new(sizeof(T) * n)); }

        template <typename... Args>
        void construct(T* p, Args&&... args) {
            new (p) T(std::forward<Args>(args)...);
        }

        void destroy(T* p) { p->~T(); }

        void deallocate(T* p, size_t n) { ::operator delete(p); }
    };

    class TrieNode {
    public:
        TrieNode(const std::string& key, const std::string& wildcard)
                : _value(nullptr), _wildcard(wildcard) {
            if (is_named_wildcard(key)) {
                _named_wildcard = extract_template(key);
            }
        }

        TrieNode(const std::string& key, const T& value, const std::string& wildcard)
                : _value(nullptr), _wildcard(wildcard) {
            _value = _allocator.allocate(1);
            _allocator.construct(_value, value);
            if (is_named_wildcard(key)) {
                _named_wildcard = extract_template(key);
            }
        }

        ~TrieNode() {
            for (auto& iter : _children) {
                delete iter.second;
                iter.second = nullptr;
            }
            if (_value != nullptr) {
                _allocator.destroy(_value);
                _allocator.deallocate(_value, 1);
            }
        }

        // Return true if insert success.
        bool insert(const std::vector<std::string> path, int index, const T& value) {
            if (index >= path.size()) {
                return false;
            }
            const std::string& token = path[index];
            std::string key = token;

            if (is_named_wildcard(token)) {
                key = _wildcard;
            }

            TrieNode* node = get_child(key);

            if (node == nullptr) {
                // no exist child for this key
                if (index == path.size() - 1) {
                    node = new TrieNode(token, value, _wildcard);
                    _children.insert(std::make_pair(key, node));
                    return true;
                } else {
                    node = new TrieNode(token, _wildcard);
                    _children.insert(std::make_pair(key, node));
                }
            } else {
                // If this is a template, set this to the node
                if (is_named_wildcard(token)) {
                    std::string temp = extract_template(token);
                    if (node->_named_wildcard.empty() || node->_named_wildcard.compare(temp) == 0) {
                        node->_named_wildcard = temp;
                    } else {
                        // Duplicated
                        return false;
                    }
                }
                if (index == path.size() - 1) {
                    if (node->_value == nullptr) {
                        node->_value = _allocator.allocate(1);
                        _allocator.construct(node->_value, value);
                        return true;
                    }
                    // Already register by other path
                    return false;
                }
            }
            return node->insert(path, index + 1, value);
        }

        bool retrieve(const std::vector<std::string> path, int index, T* value,
                      std::map<std::string, std::string>* params) {
            // check max index
            if (index >= path.size()) {
                return false;
            }
            bool use_wildcard = false;
            const std::string& token = path[index];
            TrieNode* node = get_child(token);
            if (node == nullptr) {
                node = get_child(_wildcard);
                if (node == nullptr) {
                    return false;
                }
                use_wildcard = true;
            } else {
                // If we the last one, but we have no value, check wildcard
                if (index == path.size() - 1 && node->_value == nullptr &&
                    get_child(_wildcard) != nullptr) {
                    node = get_child(_wildcard);
                    use_wildcard = true;
                } else {
                    use_wildcard = (token.compare(_wildcard) == 0);
                }
            }

            put(params, node, token);

            if (index == path.size() - 1) {
                if (node->_value == nullptr) {
                    return false;
                }
                _allocator.construct(value, *node->_value);
                return true;
            }

            // find exact
            if (node->retrieve(path, index + 1, value, params)) {
                return true;
            }

            // backtrace to test if wildcard can match
            if (!use_wildcard) {
                node = get_child(_wildcard);
                if (node != nullptr) {
                    put(params, node, token);
                    return node->retrieve(path, index + 1, value, params);
                }
            }
            return false;
        }

    private:
        bool is_named_wildcard(const std::string& key) {
            if (key.find('{') != std::string::npos && key.find('}') != std::string::npos) {
                return true;
            }
            return false;
        }

        std::string extract_template(const std::string& key) {
            std::size_t left = key.find_first_of('{') + 1;
            std::size_t right = key.find_last_of('}');
            return key.substr(left, right - left);
        }

        TrieNode* get_child(const std::string& key) {
            auto pair = _children.find(key);
            if (pair == _children.end()) {
                return nullptr;
            }
            return pair->second;
        }

        void put(std::map<std::string, std::string>* params, TrieNode* node,
                 const std::string& token) {
            if (params != nullptr && !node->_named_wildcard.empty()) {
                params->insert(std::make_pair(node->_named_wildcard, token));
            }
        }

        T* _value;
        std::string _wildcard;
        std::string _named_wildcard;
        std::map<std::string, TrieNode*> _children;
        Allocator _allocator;
    };

    bool insert(const std::string& path, const T& value) {
        std::vector<std::string> path_array;
        split(path, &path_array);
        if (path_array.empty()) {
            if (_root_value == nullptr) {
                _root_value = _allocator.allocate(1);
                _allocator.construct(_root_value, value);
                return true;
            } else {
                return false;
            }
        }
        int index = 0;
        if (path_array[0].empty()) {
            index = 1;
        }
        return _root.insert(path_array, index, value);
    }

    bool retrieve(const std::string& path, T* value) { return retrieve(path, value, nullptr); }

    bool retrieve(const std::string& path, T* value, std::map<std::string, std::string>* params) {
        if (path.empty()) {
            if (_root_value == nullptr) {
                return false;
            } else {
                _allocator.construct(value, *_root_value);
                return true;
            }
        }
        std::vector<std::string> path_array;
        split(path, &path_array);
        if (path_array.empty()) {
            if (_root_value == nullptr) {
                return false;
            } else {
                _allocator.construct(value, *_root_value);
                return true;
            }
        }
        int index = 0;
        if (path_array[0].empty()) {
            index = 1;
        }
        return _root.retrieve(path_array, index, value, params);
    }

private:
    void split(const std::string& path, std::vector<std::string>* array) {
        const char* path_str = path.c_str();
        std::size_t start = 0;
        std::size_t pos = 0;
        for (; pos < path.length(); ++pos) {
            if (path_str[pos] == _separator) {
                if (pos - start > 0) {
                    array->push_back(path.substr(start, pos - start));
                }
                start = pos + 1;
            }
        }
        if (pos - start > 0) {
            array->push_back(path.substr(start, pos - start));
        }
    }

    TrieNode _root;
    T* _root_value;
    char _separator;
    Allocator _allocator;
};

} // namespace doris
