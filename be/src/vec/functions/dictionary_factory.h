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

#include <mutex>

#include "vec/functions/dictionary.h"

namespace doris {
class MemTrackerLimiter;
}
namespace doris::vectorized {

class DictionaryFactory : private boost::noncopyable {
public:
    DictionaryFactory();
    ~DictionaryFactory();
    std::shared_ptr<const IDictionary> get(const std::string& name) {
        std::shared_lock lc(_mutex);
        if (_dict_map.contains(name)) {
            return _dict_map[name];
        }
        return nullptr;
    }

    void register_dict(DictionaryPtr dict) {
        std::unique_lock lc(_mutex);
        _dict_map[dict->dict_name()] = dict;
    }

    void register_dict_when_load_be();

    std::string current_all_dict() {
        std::shared_lock lc(_mutex);
        std::string all_dict_name;
        for (auto [name, _] : _dict_map) {
            all_dict_name += name + ", ";
        }
        return all_dict_name;
    }
    std::map<std::string, DictionaryPtr> _dict_map;

    std::shared_mutex _mutex;
    bool _has_load = false;

    std::shared_ptr<MemTrackerLimiter> _mem_tracker;
};

} // namespace doris::vectorized
