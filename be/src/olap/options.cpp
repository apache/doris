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

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>

#include "common/logging.h"

#include "olap/utils.h"

namespace doris {

// compatible with old multi path configuration:
// /path1,2014;/path2,2048
OLAPStatus parse_conf_store_paths(
        const std::string& config_path,
        std::vector<StorePath>* paths) {
    try {
        std::vector<std::string> item_vec;
        boost::split(item_vec, config_path, boost::is_any_of(";"), boost::token_compress_on);
        for (auto& item : item_vec) {
            std::vector<std::string> tmp_vec;
            boost::split(tmp_vec, item, boost::is_any_of(","), boost::token_compress_on);

            // parse root path name
            boost::trim(tmp_vec[0]);
            tmp_vec[0].erase(tmp_vec[0].find_last_not_of("/") + 1);
            if (tmp_vec[0].empty() || tmp_vec[0][0] != '/') {
                LOG(WARNING) << "invalid store path. path=" << tmp_vec[0];
                return OLAP_ERR_INPUT_PARAMETER_ERROR;
            }

            // parse root path capacity and storage medium
            std::string capacity_str, medium_str;

            boost::filesystem::path boost_path = tmp_vec[0];
            std::string extension = boost::filesystem::canonical(boost_path).extension().string();
            if (!extension.empty()) {
                medium_str = extension.substr(1);
            }

            for (int i = 1; i < tmp_vec.size(); i++) {
                // <property>:<value> or <value>
                std::string property, value;
                std::size_t found = tmp_vec[i].find(':');
                if (found != std::string::npos) {
                    property = boost::trim_copy(tmp_vec[i].substr(0, found));
                    value = boost::trim_copy(tmp_vec[i].substr(found + 1));
                    LOG(INFO) << "get setting: "<< property <<": "<<value;
                } else {
                    // <value> only supports setting capacity
                    property = "capacity";
                    value = boost::trim_copy(tmp_vec[i]);
                }

                if (property == "capacity") {
                    capacity_str = value;
                } else if (property == "medium") {
                    // property 'medium' has a higher priority than the extension of path, so it can override medium_str
                    medium_str = value; 
                } else {
                    LOG(WARNING) << "invalid property of store path, " << property;
                    return OLAP_ERR_INPUT_PARAMETER_ERROR;
                }
            }

            int64_t capacity_bytes = -1;
            if (!capacity_str.empty()) {
                if (!valid_signed_number<int64_t>(capacity_str) ||
                    strtol(capacity_str.c_str(), NULL, 10) < 0) {
                    LOG(WARNING) << "invalid capacity of store path, capacity=" << capacity_str;
                    return OLAP_ERR_INPUT_PARAMETER_ERROR;
                }
                capacity_bytes = strtol(capacity_str.c_str(), NULL, 10) * GB_EXCHANGE_BYTE;
            }

            TStorageMedium::type storage_medium = TStorageMedium::HDD;
            if (!medium_str.empty()) {
                if (boost::iequals(medium_str, "ssd")) {
                    storage_medium = TStorageMedium::SSD;
                } else if (boost::iequals(medium_str, "hdd")) {
                    storage_medium = TStorageMedium::HDD;
                } else {
                    LOG(WARNING) << "invalid storage medium. medium=" << medium_str;
                    return OLAP_ERR_INPUT_PARAMETER_ERROR;
                }
            }

            paths->emplace_back(tmp_vec[0], capacity_bytes, storage_medium);
        }
    } catch (...) {
        LOG(WARNING) << "get config store path failed. path=" << config_path;
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    return OLAP_SUCCESS;
}

}
