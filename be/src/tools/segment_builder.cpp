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

#include "common/object_pool.h"
#include "common/status.h"
#include "env/env.h"
#include "exec/parquet_scanner.h"
#include "exprs/cast_functions.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/segment_v2.pb.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/split.h"
#include "gutil/strings/substitute.h"
#include "io/buffered_reader.h"
#include "io/file_reader.h"
#include "io/local_file_reader.h"
#include "json2pb/pb_to_json.h"
#include "olap/data_dir.h"
#include "olap/olap_define.h"
#include "olap/options.h"
#include "olap/row.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_id_generator.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/rowset/segment_v2/binary_plain_page.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/schema_change.h"
#include "olap/storage_engine.h"
#include "olap/storage_policy_mgr.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "olap/tablet_schema.h"
#include "olap/tablet_schema_cache.h"
#include "olap/utils.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"
#include "runtime/user_function_cache.h"
#include "tools/builder_helper.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/file_utils.h"
#include "util/runtime_profile.h"
#include "util/time.h"
#include "vec/exec/format/parquet/vparquet_file_metadata.h"
#include "vec/exec/vbroker_scan_node.h"

DEFINE_string(meta_file, "", "tablet header meta file");
DEFINE_string(data_path, "", "this tablet's data to be build");
DEFINE_bool(is_remote, true, "input data's format, just support parquet");
DEFINE_string(format, "parquet", "input data's format, just support parquet");

std::string get_usage(const std::string& progname) {
    std::stringstream ss;
    ss << progname << " tool for build segment file for a tablet.\n";
    ss << "Usage:\n";
    ss << "segment_builder --meta_file=/path/to/xxx.hdr --data_path=/path/to/input_data/"
          " --format=parquet --is_remote=true\n";
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

    for (char** env = envp; *env != 0; env++) {
        char* thisEnv = *env;
        LOG(INFO) << "got env:" << thisEnv;
    }

    LOG(INFO) << "meta file:" << FLAGS_meta_file << " data path:" << FLAGS_data_path
              << " format:" << FLAGS_format << " is_remote:" << FLAGS_is_remote;
    bool isHDFS = FLAGS_is_remote; // only support hdfs
    std::string build_dir = FLAGS_data_path;
    if (isHDFS) {
        build_dir = std::string(get_current_dir_name());
        LOG(INFO) << "build hdfs file, using local build dir:" << build_dir;
    }

    //
    auto t0 = std::chrono::steady_clock::now();
    doris::BuilderHelper* instance = doris::BuilderHelper::init_instance();
    instance->initial_build_env();
    instance->open(FLAGS_meta_file, build_dir, FLAGS_data_path, FLAGS_format, isHDFS);
    instance->build();
    auto t1 = std::chrono::steady_clock::now();
    std::chrono::duration<double, std::milli> d {t1 - t0};
    LOG(INFO) << "total cost:" << d.count() << " ms";
    gflags::ShutDownCommandLineFlags();
    return 0;
}
