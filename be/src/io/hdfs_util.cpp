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

#include "io/hdfs_util.h"

#include <bthread/bthread.h>
#include <bthread/butex.h>
#include <bvar/latency_recorder.h>
#include <gen_cpp/cloud.pb.h>

#include <ostream>
#include <thread>

#include "common/logging.h"
#include "io/fs/err_utils.h"
#include "io/hdfs_builder.h"
#include "vec/common/string_ref.h"

namespace doris::io {

namespace hdfs_bvar {
bvar::LatencyRecorder hdfs_read_latency("hdfs_read");
bvar::LatencyRecorder hdfs_write_latency("hdfs_write");
bvar::LatencyRecorder hdfs_create_dir_latency("hdfs_create_dir");
bvar::LatencyRecorder hdfs_open_latency("hdfs_open");
bvar::LatencyRecorder hdfs_close_latency("hdfs_close");
bvar::LatencyRecorder hdfs_flush_latency("hdfs_flush");
bvar::LatencyRecorder hdfs_hflush_latency("hdfs_hflush");
bvar::LatencyRecorder hdfs_hsync_latency("hdfs_hsync");
}; // namespace hdfs_bvar

Path convert_path(const Path& path, const std::string& namenode) {
    std::string fs_path;
    if (path.native().find(namenode) != std::string::npos) {
        // `path` is uri format, remove the namenode part in `path`
        // FIXME(plat1ko): Not robust if `namenode` doesn't appear at the beginning of `path`
        fs_path = path.native().substr(namenode.size());
    } else {
        fs_path = path;
    }

    // Always use absolute path (start with '/') in hdfs
    if (fs_path.empty() || fs_path[0] != '/') {
        fs_path.insert(fs_path.begin(), '/');
    }
    return fs_path;
}

bool is_hdfs(const std::string& path_or_fs) {
    return path_or_fs.rfind("hdfs://") == 0;
}

THdfsParams to_hdfs_params(const cloud::HdfsVaultInfo& vault) {
    THdfsParams params;
    auto build_conf = vault.build_conf();
    params.__set_fs_name(build_conf.fs_name());
    if (build_conf.has_user()) {
        params.__set_user(build_conf.user());
    }
    if (build_conf.has_hdfs_kerberos_principal()) {
        params.__set_hdfs_kerberos_principal(build_conf.hdfs_kerberos_principal());
    }
    if (build_conf.has_hdfs_kerberos_keytab()) {
        params.__set_hdfs_kerberos_keytab(build_conf.hdfs_kerberos_keytab());
    }
    std::vector<THdfsConf> tconfs;
    for (const auto& confs : vault.build_conf().hdfs_confs()) {
        THdfsConf conf;
        conf.__set_key(confs.key());
        conf.__set_value(confs.value());
        tconfs.emplace_back(conf);
    }
    params.__set_hdfs_conf(tconfs);
    return params;
}

} // namespace doris::io
