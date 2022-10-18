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

#include <algorithm>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "env/env.h"
#include "gutil/strings/split.h"
#include "gutil/strings/substitute.h"
#include "olap/utils.h"
#include "util/path_util.h"

namespace doris {

using std::string;
using std::vector;

static std::string CAPACITY_UC = "CAPACITY";
static std::string MEDIUM_UC = "MEDIUM";
static std::string SSD_UC = "SSD";
static std::string HDD_UC = "HDD";
static std::string REMOTE_CACHE_UC = "REMOTE_CACHE";

// TODO: should be a general util method
static std::string to_upper(const std::string& str) {
    std::string out = str;
    std::transform(out.begin(), out.end(), out.begin(), [](auto c) { return std::toupper(c); });
    return out;
}

// Currently, both of three following formats are supported(see be.conf), remote cache is the
// local cache path for remote storage.
//   format 1:   /home/disk1/palo.HDD,50
//   format 2:   /home/disk1/palo,medium:ssd,capacity:50
//   remote cache format:  /home/disk/palo/cache,medium:remote_cache,capacity:50
Status parse_root_path(const string& root_path, StorePath* path) {
    std::vector<string> tmp_vec = strings::Split(root_path, ",", strings::SkipWhitespace());

    // parse root path name
    StripWhiteSpace(&tmp_vec[0]);
    tmp_vec[0].erase(tmp_vec[0].find_last_not_of("/") + 1);
    if (tmp_vec[0].empty() || tmp_vec[0][0] != '/') {
        LOG(WARNING) << "invalid store path. path=" << tmp_vec[0];
        return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
    }

    string canonicalized_path;
    Status status = Env::Default()->canonicalize(tmp_vec[0], &canonicalized_path);
    if (!status.ok()) {
        LOG(WARNING) << "path can not be canonicalized. may be not exist. path=" << tmp_vec[0];
        return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
    }
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
            LOG(WARNING) << "invalid property of store path, " << tmp_vec[i];
            return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
        }
    }

    path->capacity_bytes = -1;
    if (!capacity_str.empty()) {
        if (!valid_signed_number<int64_t>(capacity_str) ||
            strtol(capacity_str.c_str(), nullptr, 10) < 0) {
            LOG(WARNING) << "invalid capacity of store path, capacity=" << capacity_str;
            return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
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
            LOG(WARNING) << "invalid storage medium. medium=" << medium_str;
            return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
        }
    }

    return Status::OK();
}

Status parse_conf_store_paths(const string& config_path, std::vector<StorePath>* paths) {
    std::vector<string> path_vec = strings::Split(config_path, ";", strings::SkipWhitespace());
    for (auto& item : path_vec) {
        StorePath path;
        auto res = parse_root_path(item, &path);
        if (res.ok()) {
            paths->emplace_back(std::move(path));
        } else {
            LOG(WARNING) << "failed to parse store path " << item << ", res=" << res;
        }
    }
    if (paths->empty() || (path_vec.size() != paths->size() && !config::ignore_broken_disk)) {
        LOG(WARNING) << "fail to parse storage_root_path config. value=[" << config_path << "]";
        return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
    }
    return Status::OK();
}

} // end namespace doris
