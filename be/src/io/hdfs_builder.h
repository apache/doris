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

#include <hdfs/hdfs.h>

#include "gen_cpp/PlanNodes_types.h"
#include "io/file_reader.h"

namespace doris {

const std::string FS_KEY = "fs.defaultFS";
const std::string USER = "hadoop.username";
const std::string KERBEROS_PRINCIPAL = "hadoop.kerberos.principal";
const std::string KERBEROS_KEYTAB = "hadoop.kerberos.keytab";
const std::string TICKET_CACHE_PATH = "/tmp/krb5cc_doris_";

class HDFSCommonBuilder {
    friend HDFSCommonBuilder createHDFSBuilder(const THdfsParams& hdfsParams);
    friend HDFSCommonBuilder createHDFSBuilder(
            const std::map<std::string, std::string>& properties);

public:
    HDFSCommonBuilder() : hdfs_builder(hdfsNewBuilder()) {};
    ~HDFSCommonBuilder() { hdfsFreeBuilder(hdfs_builder); };

    hdfsBuilder* get() { return hdfs_builder; };
    bool is_need_kinit() { return need_kinit; };
    Status run_kinit();

private:
    hdfsBuilder* hdfs_builder;
    bool need_kinit {false};
    std::string hdfs_kerberos_keytab;
    std::string hdfs_kerberos_principal;
};

THdfsParams parse_properties(const std::map<std::string, std::string>& properties);

HDFSCommonBuilder createHDFSBuilder(const THdfsParams& hdfsParams);
HDFSCommonBuilder createHDFSBuilder(const std::map<std::string, std::string>& properties);

} // namespace doris
