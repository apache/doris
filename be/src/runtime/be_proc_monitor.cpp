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

#include "runtime/be_proc_monitor.h"

#include <fmt/format.h>
#include <glog/logging.h>
#include <sys/stat.h>
#include <unistd.h>

#include <deque>
#include <filesystem>
#include <fstream>
#include <map>
#include <nlohmann/json.hpp>

std::string BeProcMonitor::get_be_thread_info() {
    int32_t pid = getpid();
    std::string proc_path = fmt::format("/proc/{}/task", pid);
    if (access(proc_path.c_str(), F_OK) != 0) {
        LOG(WARNING) << "be proc path " << proc_path << " not exists.";
        return "";
    }

    std::map<std::string, int> thread_num_map;

    int total_thread_count = 0;
    int distinct_thread_name_count = 0;
    for (const auto& entry : std::filesystem::directory_iterator(proc_path)) {
        const std::string tid_path = entry.path().string();
        std::string thread_name_path = tid_path + "/comm";
        struct stat st;
        // == 0 means exists
        if (stat(thread_name_path.c_str(), &st) == 0) {
            // NOTE: there is no need to close std::ifstream, it's called during deconstruction.
            // refer:https://stackoverflow.com/questions/748014/do-i-need-to-manually-close-an-ifstream
            std::ifstream file(thread_name_path.c_str());
            if (!file.is_open()) {
                continue;
            }
            std::stringstream str_buf;
            str_buf << file.rdbuf();
            std::string thread_name = str_buf.str();
            thread_name.erase(std::remove(thread_name.begin(), thread_name.end(), '\n'),
                              thread_name.end());

            if (thread_num_map.find(thread_name) != thread_num_map.end()) {
                thread_num_map[thread_name]++;
            } else {
                distinct_thread_name_count++;
                thread_num_map.emplace(thread_name, 1);
            }
            total_thread_count++;
        }
    }

    std::deque<std::pair<std::string, int>> ordered_list(thread_num_map.begin(),
                                                         thread_num_map.end());
    std::sort(ordered_list.begin(), ordered_list.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.second > rhs.second; });

    ordered_list.push_front(
            std::make_pair("distinct_thread_name_count", distinct_thread_name_count));
    ordered_list.push_front(std::make_pair("total_thread_count", total_thread_count));
    ordered_list.push_front(std::make_pair("be_process_id", pid));

    nlohmann::json js = nlohmann::json::array();
    for (const auto& p : ordered_list) {
        js.push_back({p.first, p.second});
    }

    std::string output_json_str = js.dump();
    return output_json_str;
}