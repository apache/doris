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

#include <gen_cpp/PlanNodes_types.h>

#include <map>
#include <string>

#include "common/status.h"
#include "io/fs/hdfs.h" // IWYU pragma: keep

struct hdfsBuilder;

namespace doris {

const std::string FS_KEY = "fs.defaultFS";
const std::string USER = "hadoop.username";
const std::string KERBEROS_PRINCIPAL = "hadoop.kerberos.principal";
const std::string KERBEROS_KEYTAB = "hadoop.kerberos.keytab";
const std::string TICKET_CACHE_PATH = "/tmp/krb5cc_doris_";

class HDFSCommonBuilder {
    friend Status create_hdfs_builder(const THdfsParams& hdfsParams, const std::string& fs_name,
                                      HDFSCommonBuilder* builder);
    friend Status create_hdfs_builder(const std::map<std::string, std::string>& properties,
                                      HDFSCommonBuilder* builder);

public:
    HDFSCommonBuilder() {}
    ~HDFSCommonBuilder() {
#ifdef USE_LIBHDFS3
        // for hadoop hdfs, the hdfs_builder will be freed in hdfsConnect
        if (hdfs_builder != nullptr) {
            hdfsFreeBuilder(hdfs_builder);
        }
#endif
    }

    // Must call this to init hdfs_builder first.
    Status init_hdfs_builder();

    hdfsBuilder* get() { return hdfs_builder; }
    bool is_kerberos() const { return kerberos_login; }
    Status check_krb_params();

private:
    hdfsBuilder* hdfs_builder = nullptr;
    bool kerberos_login {false};
    std::string hdfs_kerberos_keytab;
    std::string hdfs_kerberos_principal;
};

THdfsParams parse_properties(const std::map<std::string, std::string>& properties);

Status create_hdfs_builder(const THdfsParams& hdfsParams, HDFSCommonBuilder* builder);
Status create_hdfs_builder(const std::map<std::string, std::string>& properties,
                           HDFSCommonBuilder* builder);

} // namespace doris
