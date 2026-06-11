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

#include "exprs/function/dictionary_factory.h"

#include <gen_cpp/DataSinks_types.h>

#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"

namespace doris {

DictionaryFactory::DictionaryFactory()
        : _mem_tracker(MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::GLOBAL,
                                                        "GLOBAL_DICT_FACTORY")) {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_mem_tracker);
}

DictionaryFactory::~DictionaryFactory() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_mem_tracker);
    _dict_id_to_dict_map.clear();
    _dict_id_to_version_id_map.clear();
    _refreshing_dict_map.clear();
}

void DictionaryFactory::get_dictionary_status(std::vector<TDictionaryStatus>& result,
                                              std::vector<int64_t> dict_ids) {
    SharedLockGuard lock(_mutex);
    if (dict_ids.empty()) { // empty means ALL
        for (const auto& [dict_id, dict] : _dict_id_to_dict_map) {
            TDictionaryStatus status;
            status.__set_dictionary_id(dict_id);
            status.__set_version_id(_dict_id_to_version_id_map.at(dict_id));
            status.__set_dictionary_memory_size(dict->allocated_bytes());
            result.emplace_back(std::move(status));
        }
    } else {
        for (auto dict_id : dict_ids) {
            auto dict_iter = _dict_id_to_dict_map.find(dict_id);
            if (dict_iter != _dict_id_to_dict_map.end()) {
                TDictionaryStatus status;
                status.__set_dictionary_id(dict_id);
                status.__set_version_id(_dict_id_to_version_id_map.at(dict_id));
                status.__set_dictionary_memory_size(dict_iter->second->allocated_bytes());
                result.emplace_back(std::move(status));
            }
        }
    }
}

} // namespace doris