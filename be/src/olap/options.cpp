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

#include "common/logging.h"
#include "olap/utils.h"

#include "gutil/strings/split.h"

namespace doris {

static std::string CAPACITY_UC = "CAPACITY";
static std::string MEDIUM_UC = "MEDIUM";
static std::string SSD_UC = "SSD";
static std::string HDD_UC = "HDD";

// TODO: should be a general util method
std::string to_upper(const std::string& str) {
    std::string out = str;
    std::transform(out.begin(), out.end(), out.begin(), ::toupper);
    return out;
}

// compatible with old multi path configuration:
// /path1,1024;/path2,2048
OLAPStatus parse_root_path(const std::string& root_path, StorePath* path) {
    try {
        std::vector<std::string> tmp_vec = strings::Split(root_path, ",", strings::SkipWhitespace());

        // parse root path name
        StripWhiteSpace(&tmp_vec[0]);
        tmp_vec[0].erase(tmp_vec[0].find_last_not_of("/") + 1);
        if (tmp_vec[0].empty() || tmp_vec[0][0] != '/') {
            LOG(WARNING) << "invalid store path. path=" << tmp_vec[0];
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }
        path->path = tmp_vec[0];

        // parse root path capacity and storage medium
        std::string capacity_str;
        std::string medium_str;

        boost::filesystem::path boost_path = tmp_vec[0];
        std::string extension = boost::filesystem::canonical(boost_path).extension().string();
        if (!extension.empty()) {
            medium_str = to_upper(extension.substr(1));
        }

        for (int i = 1; i < tmp_vec.size(); i++) {
            // <property>:<value> or <value>
            std::string property;
            std::string value;
            std::pair<std::string, std::string> pair = strings::Split(
                    tmp_vec[i], strings::delimiter::Limit(":", 1));
            if (!pair.second.empty()) {
                property = to_upper(pair.first);
                value = pair.second;
            } else {
                // <value> only supports setting capacity
                property = CAPACITY_UC;
                value = tmp_vec[i];
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
                LOG(WARNING) << "invalid property of store path, " << property;
                return OLAP_ERR_INPUT_PARAMETER_ERROR;
            }
        }

        path->capacity_bytes = -1;
        if (!capacity_str.empty()) {
            if (!valid_signed_number<int64_t>(capacity_str)
                    || strtol(capacity_str.c_str(), NULL, 10) < 0) {
                LOG(WARNING) << "invalid capacity of store path, capacity="
                             << capacity_str;
                return OLAP_ERR_INPUT_PARAMETER_ERROR;
            }
            path->capacity_bytes =
                strtol(capacity_str.c_str(), NULL, 10) * GB_EXCHANGE_BYTE;
        }

        path->storage_medium = TStorageMedium::HDD;
        if (!medium_str.empty()) {
            if (medium_str == SSD_UC) {
                path->storage_medium = TStorageMedium::SSD;
            } else if (medium_str == HDD_UC) {
                path->storage_medium = TStorageMedium::HDD;
            } else {
                LOG(WARNING) << "invalid storage medium. medium=" << medium_str;
                return OLAP_ERR_INPUT_PARAMETER_ERROR;
            }
        }
    } catch (...) {
        LOG(WARNING) << "invalid store path. path=" << root_path;
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    return OLAP_SUCCESS;
}

OLAPStatus parse_conf_store_paths(const std::string& config_path,
                                  std::vector<StorePath>* paths) {
    try {
        std::vector<std::string> path_vec = strings::Split(
                config_path, ";", strings::SkipWhitespace());
        for (auto& item : path_vec) {
            StorePath path;
            auto res = parse_root_path(item, &path);
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "get config store path failed. path="
                             << config_path;
                return OLAP_ERR_INPUT_PARAMETER_ERROR;
            }
            paths->emplace_back(std::move(path));
        }
    } catch (...) {
        LOG(WARNING) << "get config store path failed. path=" << config_path;
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    return OLAP_SUCCESS;
}
}
