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

#include <unordered_map>

#include "common/object_pool.h"
#include "exprs/create_predicate_function.h"
#include "exprs/hybrid_set.h"
#include "runtime/primitive_type.h"

namespace doris {

class HybridMap {
public:
    HybridMap(PrimitiveType type) : _type(type) {}

    virtual ~HybridMap() {}

    virtual HybridSetBase* find_or_insert_set(uint64_t dst, bool* is_add_buckets) {
        HybridSetBase* _set_ptr = nullptr;
        typename std::unordered_map<uint64_t, HybridSetBase*>::const_iterator it = _map.find(dst);

        if (it == _map.end()) {
            _set_ptr = _pool.add(create_set(_type));
            std::pair<uint64_t, HybridSetBase*> insert_pair(dst, _set_ptr);
            _map.insert(insert_pair);
            *is_add_buckets = true;
        } else {
            _set_ptr = it->second;
            *is_add_buckets = false;
        }

        return _set_ptr;
    }

private:
    std::unordered_map<uint64_t, HybridSetBase*> _map;
    PrimitiveType _type;
    ObjectPool _pool;
};
} // namespace doris
