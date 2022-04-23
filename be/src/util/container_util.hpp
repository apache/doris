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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/container-util.hpp
// and modified by Doris

#ifndef DORIS_BE_SRC_COMMON__UTIL_CONTAINER_UTIL_HPP
#define DORIS_BE_SRC_COMMON__UTIL_CONTAINER_UTIL_HPP

#include <map>
#include <unordered_map>

#include "gen_cpp/Types_types.h"
#include "util/hash_util.hpp"

namespace doris {

// Hash function for TNetworkAddress. This function must be called hash_value to be picked
// up properly by boost.
inline std::size_t hash_value(const TNetworkAddress& host_port) {
    uint32_t hash = HashUtil::hash(host_port.hostname.c_str(), host_port.hostname.length(), 0);
    return HashUtil::hash(&host_port.port, sizeof(host_port.port), hash);
}

struct HashTNetworkAddressPtr : public std::unary_function<TNetworkAddress*, size_t> {
    size_t operator()(const TNetworkAddress* const& p) const { return hash_value(*p); }
};

struct TNetworkAddressPtrEquals : public std::unary_function<TNetworkAddress*, bool> {
    bool operator()(const TNetworkAddress* const& p1, const TNetworkAddress* const& p2) const {
        return p1->hostname == p2->hostname && p1->port == p2->port;
    }
};

// find_or_insert(): if the key is present, return the value; if the key is not present,
// create a new entry (key, default_val) and return default_val.

template <typename K, typename V>
V* find_or_insert(std::map<K, V>* m, const K& key, const V& default_val) {
    typename std::map<K, V>::iterator it = m->find(key);

    if (it == m->end()) {
        it = m->insert(make_pair(key, default_val)).first;
    }

    return &it->second;
}

template <typename K, typename V>
V* find_or_insert(std::unordered_map<K, V>* m, const K& key, const V& default_val) {
    typename std::unordered_map<K, V>::iterator it = m->find(key);

    if (it == m->end()) {
        it = m->insert(make_pair(key, default_val)).first;
    }

    return &it->second;
}

// find_with_default: if the key is present, return the corresponding value; if the key
// is not present, return the supplied default value

template <typename K, typename V>
const V& find_with_default(const std::map<K, V>& m, const K& key, const V& default_val) {
    typename std::map<K, V>::const_iterator it = m.find(key);

    if (it == m.end()) {
        return default_val;
    }

    return it->second;
}

template <typename K, typename V>
const V& find_with_default(const std::unordered_map<K, V>& m, const K& key, const V& default_val) {
    typename std::unordered_map<K, V>::const_iterator it = m.find(key);

    if (it == m.end()) {
        return default_val;
    }

    return it->second;
}

} // namespace doris

#endif
