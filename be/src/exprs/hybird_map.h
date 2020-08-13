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

#ifndef DORIS_BE_SRC_QUERY_EXPRS_HYBIRD_MAP_H
#define DORIS_BE_SRC_QUERY_EXPRS_HYBIRD_MAP_H

#include <unordered_map>
#include "common/status.h"
#include "runtime/primitive_type.h"
#include "runtime/string_value.h"
#include "runtime/datetime_value.h"
#include "common/object_pool.h"
#include "exprs/hybird_set.h"

namespace doris {

class HybirdMap {
public:
    HybirdMap(PrimitiveType type) : _type(type) {
    }

    virtual ~HybirdMap() {
    }

    virtual HybirdSetBase* find_or_insert_set(uint64_t dst, bool* is_add_buckets) {
        HybirdSetBase* _set_ptr;
        typename std::unordered_map<uint64_t, HybirdSetBase*>::const_iterator it = _map.find(dst);

        if (it == _map.end()) {
            _set_ptr = _pool.add(HybirdSetBase::create_set(_type));
            std::pair<uint64_t, HybirdSetBase*> insert_pair(dst, _set_ptr);
            _map.insert(insert_pair);
            *is_add_buckets = true;
        } else {
            _set_ptr = it->second;
            *is_add_buckets = false;
        }

        return _set_ptr;
    }

private:
    std::unordered_map<uint64_t, HybirdSetBase*> _map;
    PrimitiveType _type;
    ObjectPool _pool;
};
}

#endif  // DORIS_BE_SRC_QUERY_EXPRS_HYBIRD_MAP_H
