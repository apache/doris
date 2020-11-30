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

#pragma once

#include <iostream>
#include <sstream>

#include "common/status.h"

namespace doris {

class PprofUtils {
public:
    /// check and get "perf" cmd
    static Status get_perf_cmd(std::string* cmd);

    /// get current BE process cmdline from '/proc/self/cmdline'
    static Status get_self_cmdline(std::string* cmd);

    /// check and get "pprof" command, return the cmd abs path via "cmd".
    static Status get_pprof_cmd(std::string* cmd);

    /// get readable profile by `pprof --text palo_be perf.data`
    /// if is_file is true, the file_or_content is an abs path of perf file.
    /// if is_file is false, the file_or_content is the perf file content.
    /// the readable content is returned via "output"
    static Status get_readable_profile(const std::string& file_or_content, bool is_file,
                                       std::stringstream* output);

    /// generat flame graph of CPU profile of BE process.
    /// flame_graph_tool_dir is the dir will FlameGraph installed.
    /// if succeed, return return generated svg file path in "svg_file".
    static Status generate_flamegraph(int32_t sample_seconds,
                                      const std::string& flame_graph_tool_dir, bool return_file,
                                      std::string* svg_file_or_content);
};

} // namespace doris
