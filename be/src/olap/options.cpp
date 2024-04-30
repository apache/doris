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

#include "olap/options.h"

#include <ctype.h>
#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/rapidjson.h>
#include <stdlib.h>

#include <algorithm>
#include <memory>
#include <ostream>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "gutil/strings/split.h"
#include "gutil/strings/strip.h"
#include "io/fs/local_file_system.h"
#include "olap/olap_define.h"
#include "olap/utils.h"
#include "util/path_util.h"
#include "util/string_util.h"

namespace doris {
using namespace ErrorCode;

using std::string;
using std::vector;

static std::string CAPACITY_UC = "CAPACITY";
static std::string MEDIUM_UC = "MEDIUM";
static std::string SSD_UC = "SSD";
static std::string HDD_UC = "HDD";
static std::string REMOTE_CACHE_UC = "REMOTE_CACHE";

static std::string CACHE_PATH = "path";
static std::string CACHE_TOTAL_SIZE = "total_size";
static std::string CACHE_QUERY_LIMIT_SIZE = "query_limit";
static std::string CACHE_NORMAL_PERCENT = "normal_percent";
static std::string CACHE_DISPOSABLE_PERCENT = "disposable_percent";
static std::string CACHE_INDEX_PERCENT = "index_percent";

// TODO: should be a general util method
// static std::string to_upper(const std::string& str) {
//     std::string out = str;
//     std::transform(out.begin(), out.end(), out.begin(), [](auto c) { return std::toupper(c); });
//     return out;
// }

// Currently, both of three following formats are supported(see be.conf), remote cache is the
// local cache path for remote storage.
//   format 1:   /home/disk1/palo.HDD,50
//   format 2:   /home/disk1/palo,medium:ssd,capacity:50
//   remote cache format:  /home/disk/palo/cache,medium:remote_cache,capacity:50
Status parse_root_path(const string& root_path, StorePath* path) {
    std::vector<string> tmp_vec = strings::Split(root_path, ",", strings::SkipWhitespace());

    // parse root path name
    StripWhiteSpace(&tmp_vec[0]);
    tmp_vec[0].erase(tmp_vec[0].find_last_not_of('/') + 1);
    if (tmp_vec[0].empty() || tmp_vec[0][0] != '/') {
        return Status::Error<INVALID_ARGUMENT>("invalid store path. path={}", tmp_vec[0]);
    }

    string canonicalized_path;
    RETURN_IF_ERROR(io::global_local_filesystem()->canonicalize(tmp_vec[0], &canonicalized_path));
    path->path = tmp_vec[0];

    // parse root path capacity and storage medium
    string capacity_str;
    string medium_str = HDD_UC;

    string extension = path_util::file_extension(canonicalized_path);
    if (!extension.empty()) {
        medium_str = to_upper(extension.substr(1));
    }

    for (int i = 1; i < tmp_vec.size(); i++) {
        // <property>:<value> or <value>
        string property;
        string value;
        std::pair<string, string> pair =
                strings::Split(tmp_vec[i], strings::delimiter::Limit(":", 1));
        if (pair.second.empty()) {
            // format_1: <value> only supports setting capacity
            property = CAPACITY_UC;
            value = tmp_vec[i];
        } else {
            // format_2
            property = to_upper(pair.first);
            value = pair.second;
        }

        StripWhiteSpace(&property);
        StripWhiteSpace(&value);
        if (property == CAPACITY_UC) {
            capacity_str = value;
        } else if (property == MEDIUM_UC) {
            // property 'medium' has a higher priority than the extension of
            // path, so it can override medium_str
            medium_str = to_upper(value);
        } else {
            return Status::Error<INVALID_ARGUMENT>("invalid property of store path, {}",
                                                   tmp_vec[i]);
        }
    }

    path->capacity_bytes = -1;
    if (!capacity_str.empty()) {
        if (!valid_signed_number<int64_t>(capacity_str) ||
            strtol(capacity_str.c_str(), nullptr, 10) < 0) {
            LOG(WARNING) << "invalid capacity of store path, capacity=" << capacity_str;
            return Status::Error<INVALID_ARGUMENT>("invalid capacity of store path, capacity={}",
                                                   capacity_str);
        }
        path->capacity_bytes = strtol(capacity_str.c_str(), nullptr, 10) * GB_EXCHANGE_BYTE;
    }

    path->storage_medium = TStorageMedium::HDD;
    if (!medium_str.empty()) {
        if (medium_str == SSD_UC) {
            path->storage_medium = TStorageMedium::SSD;
        } else if (medium_str == HDD_UC) {
            path->storage_medium = TStorageMedium::HDD;
        } else if (medium_str == REMOTE_CACHE_UC) {
            path->storage_medium = TStorageMedium::REMOTE_CACHE;
        } else {
            return Status::Error<INVALID_ARGUMENT>("invalid storage medium. medium={}", medium_str);
        }
    }

    return Status::OK();
}

Status parse_conf_store_paths(const string& config_path, std::vector<StorePath>* paths) {
    std::vector<string> path_vec = strings::Split(config_path, ";", strings::SkipWhitespace());
    if (path_vec.empty()) {
        // means compute node
        return Status::OK();
    }
    if (path_vec.back().empty()) {
        // deal with the case that user add `;` to the tail
        path_vec.pop_back();
    }

    std::set<std::string> real_paths;
    for (auto& item : path_vec) {
        StorePath path;
        auto res = parse_root_path(item, &path);
        if (res.ok()) {
            auto success = real_paths.emplace(path.path).second;
            if (success) {
                paths->emplace_back(std::move(path));
            } else {
                LOG(WARNING) << "a duplicated path is found " << path.path;
                return Status::Error<INVALID_ARGUMENT>("a duplicated path is found, path={}",
                                                       path.path);
            }
        } else {
            LOG(WARNING) << "failed to parse store path " << item << ", res=" << res;
        }
    }
    if ((path_vec.size() != paths->size() && !config::ignore_broken_disk)) {
        return Status::Error<INVALID_ARGUMENT>("fail to parse storage_root_path config. value={}",
                                               config_path);
    }
    return Status::OK();
}

void parse_conf_broken_store_paths(const string& config_path, std::set<std::string>* paths) {
    std::vector<string> path_vec = strings::Split(config_path, ";", strings::SkipWhitespace());
    if (path_vec.empty()) {
        return;
    }
    if (path_vec.back().empty()) {
        // deal with the case that user add `;` to the tail
        path_vec.pop_back();
    }
    for (auto& item : path_vec) {
        paths->emplace(item);
    }
    return;
}

/** format:   
 *  [
 *    {"path": "storage1", "total_size":53687091200,"query_limit": "10737418240"},
 *    {"path": "storage2", "total_size":53687091200},
 *    {"path": "storage3", "total_size":53687091200, "normal_percent":85, "disposable_percent":10, "index_percent":5}
 *  ]
 */
Status parse_conf_cache_paths(const std::string& config_path, std::vector<CachePath>& paths) {
    using namespace rapidjson;
    Document document;
    document.Parse(config_path.c_str());
    DCHECK(document.IsArray()) << config_path << " " << document.GetType();
    for (auto& config : document.GetArray()) {
        auto map = config.GetObject();
        DCHECK(map.HasMember(CACHE_PATH.c_str()));
        std::string path = map.FindMember(CACHE_PATH.c_str())->value.GetString();
        int64_t total_size = 0, query_limit_bytes = 0;
        if (map.HasMember(CACHE_TOTAL_SIZE.c_str())) {
            auto& value = map.FindMember(CACHE_TOTAL_SIZE.c_str())->value;
            if (value.IsInt64()) {
                total_size = value.GetInt64();
            } else {
                return Status::InvalidArgument("total_size should be int64");
            }
        }
        if (config::enable_file_cache_query_limit) {
            if (map.HasMember(CACHE_QUERY_LIMIT_SIZE.c_str())) {
                auto& value = map.FindMember(CACHE_QUERY_LIMIT_SIZE.c_str())->value;
                if (value.IsInt64()) {
                    query_limit_bytes = value.GetInt64();
                } else {
                    return Status::InvalidArgument("query_limit should be int64");
                }
            }
        }
        if (total_size <= 0 || (config::enable_file_cache_query_limit && query_limit_bytes <= 0)) {
            return Status::InvalidArgument(
                    "total_size or query_limit should not less than or equal to zero");
        }

        // percent
        auto get_percent_value = [&](const std::string& key, size_t& percent) {
            auto& value = map.FindMember(key.c_str())->value;
            if (value.IsUint()) {
                percent = value.GetUint();
            } else {
                return Status::InvalidArgument("percent should be uint");
            }
            return Status::OK();
        };

        size_t normal_percent = 85;
        size_t disposable_percent = 10;
        size_t index_percent = 5;
        bool has_normal_percent = map.HasMember(CACHE_NORMAL_PERCENT.c_str());
        bool has_disposable_percent = map.HasMember(CACHE_DISPOSABLE_PERCENT.c_str());
        bool has_index_percent = map.HasMember(CACHE_INDEX_PERCENT.c_str());
        if (has_normal_percent && has_disposable_percent && has_index_percent) {
            RETURN_IF_ERROR(get_percent_value(CACHE_NORMAL_PERCENT, normal_percent));
            RETURN_IF_ERROR(get_percent_value(CACHE_DISPOSABLE_PERCENT, disposable_percent));
            RETURN_IF_ERROR(get_percent_value(CACHE_INDEX_PERCENT, index_percent));
        } else if (has_normal_percent || has_disposable_percent || has_index_percent) {
            return Status::InvalidArgument(
                    "cache percent config must either be all set or all unset.");
        }
        if ((normal_percent + disposable_percent + index_percent) != 100) {
            return Status::InvalidArgument("The sum of cache percent config must equal 100.");
        }

        paths.emplace_back(std::move(path), total_size, query_limit_bytes, normal_percent,
                           disposable_percent, index_percent);
    }
    if (paths.empty()) {
        return Status::InvalidArgument("fail to parse storage_root_path config. value={}",
                                       config_path);
    }
    return Status::OK();
}

io::FileCacheSettings CachePath::init_settings() const {
    return io::get_file_cache_settings(total_bytes, query_limit_bytes, normal_percent,
                                       disposable_percent, index_percent);
}

} // end namespace doris
