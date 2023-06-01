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

#include <gflags/gflags.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <set>
#include <sstream>
#include <string>

#include "common/status.h"
#include "tools/build_segment_tool/build_helper.h"
#include "util/uid_util.h"

DEFINE_string(meta_file, "", "tablet header meta file");
DEFINE_string(data_path, "", "this tablet's data to be build");
DEFINE_string(format, "parquet", "input data's format, currently just support parquet");

std::string get_usage(const std::string& progname) {
    std::stringstream ss;
    ss << progname << " tool for build segment file for a tablet.\n";
    ss << "Usage:\n";
    ss << "segment_builder --meta_file=/path/to/xxx.hdr --data_path=/path/to/input_data/"
          " --format=parquet \n";
    return ss.str();
}

int main(int argc, char** argv, char** envp) {
    std::string usage = get_usage(argv[0]);
    gflags::SetUsageMessage(usage);
    google::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_meta_file.empty() || FLAGS_data_path.empty()) {
        std::cerr << "Invalid arguments:" << usage;
        exit(1);
    }

    // for (char** env = envp; *env != nullptr; env++) {
    //     char* thisEnv = *env;
    //     LOG(INFO) << "got env:" << thisEnv;
    // }

    LOG(INFO) << "meta file:" << FLAGS_meta_file << " data path:" << FLAGS_data_path
              << " format:" << FLAGS_format;
    std::string build_dir = FLAGS_data_path;
    //
    auto t0 = std::chrono::steady_clock::now();
    doris::BuildHelper* instance = doris::BuildHelper::init_instance();
    instance->initial_build_env();
    instance->open(FLAGS_meta_file, build_dir, FLAGS_data_path, FLAGS_format);
    instance->build();
    auto t1 = std::chrono::steady_clock::now();
    std::chrono::duration<double, std::milli> d {t1 - t0};
    LOG(INFO) << "total cost:" << d.count() << " ms";
    std::exit(EXIT_SUCCESS);
    gflags::ShutDownCommandLineFlags();
    return 0;
}
