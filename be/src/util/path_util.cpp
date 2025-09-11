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

#include "util/path_util.h"

// Use the POSIX version of dirname(3). See `man 3 dirname`
#include <libgen.h>

#include <cstdlib>
#include <filesystem>

#include "cloud/config.h"
#include "common/config.h"
#include "gutil/strings/split.h"
#include "gutil/strings/strip.h"
#include "runtime/plugin/cloud_plugin_downloader.h"

using std::string;
using std::vector;
using strings::SkipEmpty;
using strings::Split;

namespace doris {
namespace path_util {

std::string join_path_segments(const string& a, const string& b) {
    if (a.empty()) {
        return b;
    } else if (b.empty()) {
        return a;
    } else {
        return StripSuffixString(a, "/") + "/" + StripPrefixString(b, "/");
    }
}

// strdup use malloc to obtain memory for the new string, it should be freed with free.
// but std::unique_ptr use delete to free memory by default, so it should specify free memory using free

std::string dir_name(const string& path) {
    std::vector<char> path_copy(path.c_str(), path.c_str() + path.size() + 1);
    return dirname(&path_copy[0]);
}

std::string base_name(const string& path) {
    std::vector<char> path_copy(path.c_str(), path.c_str() + path.size() + 1);
    return basename(&path_copy[0]);
}

std::string file_extension(const string& path) {
    string file_name = base_name(path);
    if (file_name == "." || file_name == "..") {
        return "";
    }

    string::size_type pos = file_name.rfind(".");
    return pos == string::npos ? "" : file_name.substr(pos);
}

std::string get_real_plugin_url(const std::string& url, const std::string& plugin_dir_config_value,
                                const std::string& plugin_dir_name, const std::string& doris_home) {
    if (url.find(":/") == std::string::npos) {
        return check_and_return_default_plugin_url(url, plugin_dir_config_value, plugin_dir_name,
                                                   doris_home);
    }
    return url;
}

std::string check_and_return_default_plugin_url(const std::string& url,
                                                const std::string& plugin_dir_config_value,
                                                const std::string& plugin_dir_name,
                                                const std::string& doris_home) {
    std::string home_dir = doris_home;
    if (home_dir.empty()) {
        const char* env_home = std::getenv("DORIS_HOME");
        if (env_home) {
            home_dir = std::string(env_home);
        } else {
            return "file://" + plugin_dir_config_value + "/" + url;
        }
    }

    std::string default_url = home_dir + "/plugins/" + plugin_dir_name;
    std::string default_old_url = home_dir + "/" + plugin_dir_name;

    if (plugin_dir_config_value == default_url) {
        // If true, which means user does not set `jdbc_drivers_dir` and use the default one.
        // Because in 2.1.8, we change the default value of `jdbc_drivers_dir`
        // from `DORIS_HOME/jdbc_drivers` to `DORIS_HOME/plugins/jdbc_drivers`,
        // so we need to check the old default dir for compatibility.
        std::string target_path = default_url + "/" + url;
        if (std::filesystem::exists(target_path)) {
            // File exists in new default directory
            return "file://" + target_path;
        } else if (config::is_cloud_mode()) {
            // Cloud mode: try to download from cloud to new default directory
            CloudPluginDownloader::PluginType plugin_type;
            if (plugin_dir_name == "jdbc_drivers") {
                plugin_type = CloudPluginDownloader::PluginType::JDBC_DRIVERS;
            } else if (plugin_dir_name == "java_udf") {
                plugin_type = CloudPluginDownloader::PluginType::JAVA_UDF;
            } else {
                // Unknown plugin type, fallback to old directory
                return "file://" + default_old_url + "/" + url;
            }

            std::string downloaded_path;
            Status status = CloudPluginDownloader::download_from_cloud(
                    plugin_type, url, target_path, &downloaded_path);
            if (status.ok() && !downloaded_path.empty()) {
                return "file://" + downloaded_path;
            }
            // Download failed, log warning but continue to fallback
            LOG(WARNING) << "Failed to download plugin from cloud: " << status.to_string()
                         << ", fallback to old directory";
        }

        // Fallback to old default directory for compatibility
        return "file://" + default_old_url + "/" + url;
    } else {
        // User specified custom directory - use directly
        return "file://" + plugin_dir_config_value + "/" + url;
    }
}

} // namespace path_util
} // namespace doris
