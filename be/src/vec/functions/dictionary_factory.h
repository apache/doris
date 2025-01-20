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

#include <fstream>
#include <mutex>

#include "common/config.h"
#include "common/logging.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/functions/dictionary.h"

namespace doris {
class MemTrackerLimiter;
}
namespace doris::vectorized {

class DictionaryFactory : private boost::noncopyable {
public:
    DictionaryFactory();
    ~DictionaryFactory();

    // Returns nullptr if failed
    std::shared_ptr<const IDictionary> get(int64_t dict_id, int64_t version_id) {
        std::shared_lock lc(_mutex);
        // dict_id and version_id must match
        if (_dict_id_to_dict_map.contains(dict_id) &&
            _dict_id_to_version_id_map[dict_id] == version_id) {
            return _dict_id_to_dict_map[dict_id];
        }
        return nullptr;
    }

    Status register_dict(int64_t dict_id, int64_t version_id, DictionaryPtr dict) {
        std::unique_lock lc(_mutex);
        if (_dict_id_to_version_id_map.contains(dict_id)) {
            // check version_id
            if (version_id <= _dict_id_to_version_id_map[dict_id]) {
                LOG_WARNING("DictionaryFactory Failed to register dictionary")
                        .tag("dict_id", dict_id)
                        .tag("version_id", version_id)
                        .tag("dict name", dict->dict_name());
                return Status::InvalidArgument(
                        "Version ID is not greater than the existing version ID for the "
                        "dictionary. {} : {}",
                        version_id, _dict_id_to_version_id_map[dict_id]);
            }
        }
        LOG_INFO("DictionaryFactory Successfully register dictionary")
                .tag("dict_id", dict_id)
                .tag("version_id", version_id)
                .tag("dict name", dict->dict_name());
        _dict_id_to_dict_map[dict_id] = dict;
        _dict_id_to_version_id_map[dict_id] = version_id;
        dict->_mem_tracker = _mem_tracker;
        return Status::OK();
    }

    Status delete_dict(int64_t dict_id) {
        std::unique_lock lc(_mutex);
        if (!_dict_id_to_dict_map.contains(dict_id)) {
            LOG_WARNING("DictionaryFactory Failed to delete dictionary").tag("dict_id", dict_id);
            return Status::OK(); // TODO: change to not ok when we have heartbeat.
        }
        auto dict = _dict_id_to_dict_map[dict_id];
        LOG_INFO("DictionaryFactory Successfully delete dictionary")
                .tag("dict_id", dict_id)
                .tag("dict name", dict->dict_name());
        _dict_id_to_dict_map.erase(dict_id);
        _dict_id_to_version_id_map.erase(dict_id);
        return Status::OK();
    }

    std::shared_ptr<MemTrackerLimiter> mem_tracker() const { return _mem_tracker; }

private:
    std::map<int64_t, DictionaryPtr> _dict_id_to_dict_map;
    std::map<int64_t, int64_t> _dict_id_to_version_id_map;

    std::shared_mutex _mutex;

    std::shared_ptr<MemTrackerLimiter> _mem_tracker;
};

} // namespace doris::vectorized
