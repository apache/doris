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

#include "io/hdfs_builder.h"

#include <fmt/format.h>

#include <fstream>

#include "agent/utils.h"
#include "common/logging.h"
#include "util/string_util.h"
#include "util/uid_util.h"
#include "util/url_coding.h"
namespace doris {

Status HDFSCommonBuilder::run_kinit() {
    if (hdfs_kerberos_principal.empty() || hdfs_kerberos_keytab.empty()) {
        return Status::InvalidArgument("Invalid hdfs_kerberos_principal or hdfs_kerberos_keytab");
    }
    std::string ticket_path = TICKET_CACHE_PATH + generate_uuid_string();
    fmt::memory_buffer kinit_command;
    fmt::format_to(kinit_command, "kinit -c {} -R -t {} -k {}", ticket_path, hdfs_kerberos_keytab,
                   hdfs_kerberos_principal);
    VLOG_NOTICE << "kinit command: " << fmt::to_string(kinit_command);
    std::string msg;
    AgentUtils util;
    bool rc = util.exec_cmd(fmt::to_string(kinit_command), &msg);
    if (!rc) {
        return Status::InternalError("Kinit failed, errMsg: " + msg);
    }
    hdfsBuilderSetKerbTicketCachePath(hdfs_builder, ticket_path.c_str());
    return Status::OK();
}

THdfsParams parse_properties(const std::map<std::string, std::string>& properties) {
    StringCaseMap<std::string> prop(properties.begin(), properties.end());
    std::vector<THdfsConf> hdfs_configs;
    THdfsParams hdfsParams;
    for (auto iter = prop.begin(); iter != prop.end();) {
        if (iter->first.compare(FS_KEY) == 0) {
            hdfsParams.__set_fs_name(iter->second);
            iter = prop.erase(iter);
        } else if (iter->first.compare(USER) == 0) {
            hdfsParams.__set_user(iter->second);
            iter = prop.erase(iter);
        } else if (iter->first.compare(KERBEROS_PRINCIPAL) == 0) {
            hdfsParams.__set_hdfs_kerberos_principal(iter->second);
            iter = prop.erase(iter);
        } else if (iter->first.compare(KERBEROS_KEYTAB) == 0) {
            hdfsParams.__set_hdfs_kerberos_keytab(iter->second);
            iter = prop.erase(iter);
        } else {
            THdfsConf item;
            item.key = iter->first;
            item.value = iter->second;
            hdfs_configs.push_back(item);
            iter = prop.erase(iter);
        }
    }
    if (!hdfsParams.__isset.user && std::getenv("HADOOP_USER_NAME") != nullptr) {
        hdfsParams.__set_user(std::getenv("HADOOP_USER_NAME"));
    }
    hdfsParams.__set_hdfs_conf(hdfs_configs);
    return hdfsParams;
}

HDFSCommonBuilder createHDFSBuilder(const THdfsParams& hdfsParams) {
    HDFSCommonBuilder builder;
    hdfsBuilderSetNameNode(builder.get(), hdfsParams.fs_name.c_str());
    // set hdfs user
    if (hdfsParams.__isset.user) {
        hdfsBuilderSetUserName(builder.get(), hdfsParams.user.c_str());
    }
    // set kerberos conf
    if (hdfsParams.__isset.hdfs_kerberos_principal) {
        builder.need_kinit = true;
        builder.hdfs_kerberos_principal = hdfsParams.hdfs_kerberos_principal;
        hdfsBuilderSetPrincipal(builder.get(), hdfsParams.hdfs_kerberos_principal.c_str());
    }
    if (hdfsParams.__isset.hdfs_kerberos_keytab) {
        builder.need_kinit = true;
        builder.hdfs_kerberos_keytab = hdfsParams.hdfs_kerberos_keytab;
    }
    // set other conf
    if (hdfsParams.__isset.hdfs_conf) {
        for (const THdfsConf& conf : hdfsParams.hdfs_conf) {
            hdfsBuilderConfSetStr(builder.get(), conf.key.c_str(), conf.value.c_str());
        }
    }

    return builder;
}

HDFSCommonBuilder createHDFSBuilder(const std::map<std::string, std::string>& properties) {
    THdfsParams hdfsParams = parse_properties(properties);
    return createHDFSBuilder(hdfsParams);
}

} // namespace doris
