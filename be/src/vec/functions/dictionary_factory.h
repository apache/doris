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

#include <gen_cpp/BackendService_types.h>

#include <mutex>

#include "common/config.h"
#include "common/logging.h"
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
        std::unique_lock lc(_mutex);
        // dict_id and version_id must match
        if (_dict_id_to_dict_map.contains(dict_id) &&
            _dict_id_to_version_id_map[dict_id] == version_id) {
            return _dict_id_to_dict_map[dict_id];
        }
        return nullptr;
    }

    Status refresh_dict(int64_t dict_id, int64_t version_id, DictionaryPtr dict) {
        VLOG_DEBUG << "DictionaryFactory refresh dictionary"
                   << " dict_id: " << dict_id << " version_id: " << version_id
                   << " dict name: " << dict->dict_name();
        std::unique_lock lc(_mutex);
        dict->_mem_tracker = _mem_tracker;
        _refreshing_dict_map[dict_id] = std::make_pair(version_id, dict);
        // Set the mem tracker for the dictionary
        return Status::OK();
    }

    Status abort_refresh_dict(int64_t dict_id, int64_t version_id) {
        VLOG_DEBUG << "DictionaryFactory abort refresh dictionary"
                   << " dict_id: " << dict_id << " version_id: " << version_id;
        std::unique_lock lc(_mutex);
        if (!_refreshing_dict_map.contains(dict_id)) {
            // FE will abort all, including succeed and failed.
            return Status::OK();
        }
        auto [refresh_version_id, dict] = _refreshing_dict_map[dict_id];
        if (version_id != refresh_version_id) {
            return Status::InvalidArgument(
                    "Version ID is not equal to the refreshing version ID. {} : {}", version_id,
                    refresh_version_id);
        }
        _refreshing_dict_map.erase(dict_id);
        return Status::OK();
    }

    Status commit_refresh_dict(int64_t dict_id, int64_t version_id) {
        VLOG_DEBUG << "DictionaryFactory commit refresh dictionary"
                   << " dict_id: " << dict_id << " version_id: " << version_id;
        std::unique_lock lc(_mutex);
        if (!_refreshing_dict_map.contains(dict_id)) {
            return Status::InvalidArgument("Dictionary is not refreshing dict_id: {}", dict_id);
        }
        auto [refresh_version_id, dict] = _refreshing_dict_map[dict_id];
        if (version_id != refresh_version_id) {
            return Status::InvalidArgument(
                    "Version ID is not equal to the refreshing version ID. {} : {}", version_id,
                    refresh_version_id);
        }
        {
            // commit the dictionary
            if (_dict_id_to_version_id_map.contains(dict_id)) {
                // check version_id
                if (version_id <= _dict_id_to_version_id_map[dict_id]) {
                    LOG_WARNING(
                            "DictionaryFactory Failed to commit dictionary because version ID "
                            "is not greater than the existing version ID")
                            .tag("dict_id", dict_id)
                            .tag("version_id", version_id)
                            .tag("dict name", dict->dict_name())
                            .tag("existing version ID", _dict_id_to_version_id_map[dict_id]);
                    return Status::InvalidArgument(
                            "Version ID is not greater than the existing version ID for the "
                            "dictionary. {} : {}",
                            version_id, _dict_id_to_version_id_map[dict_id]);
                }
            }
            LOG_INFO("DictionaryFactory Successfully commit dictionary")
                    .tag("dict_id", dict_id)
                    .tag("version_id", version_id)
                    .tag("dict name", dict->dict_name());
            _dict_id_to_dict_map[dict_id] = dict;
            _dict_id_to_version_id_map[dict_id] = version_id;
            _refreshing_dict_map.erase(dict_id);
        }
        return Status::OK();
    }

    Status delete_dict(int64_t dict_id) {
        VLOG_DEBUG << "DictionaryFactory delete dictionary, dict_id: " << dict_id;
        std::unique_lock lc(_mutex);
        if (!_dict_id_to_dict_map.contains(dict_id)) {
            LOG_WARNING("DictionaryFactory Failed to delete dictionary").tag("dict_id", dict_id);
            return Status::OK();
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

    void get_dictionary_status(std::vector<TDictionaryStatus>& result,
                               std::vector<int64_t> dict_ids);

private:
    std::map<int64_t, DictionaryPtr> _dict_id_to_dict_map;
    std::map<int64_t, int64_t> _dict_id_to_version_id_map;

    std::map<int64_t, std::pair<int64_t, DictionaryPtr>>
            _refreshing_dict_map; // dict_id -> (version_id, dict)

    std::shared_mutex _mutex;

    std::shared_ptr<MemTrackerLimiter> _mem_tracker;
};

} // namespace doris::vectorized
