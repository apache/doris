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
#include <gen_cpp/PlanNodes_types.h>

#include <cstdlib>
#include <fstream>
#include <utility>
#include <vector>

#include "agent/utils.h"
#include "common/config.h"
#include "common/logging.h"
#include "io/fs/hdfs.h"
#include "util/string_util.h"
#include "util/uid_util.h"

namespace doris {

Status HDFSCommonBuilder::init_hdfs_builder() {
    hdfs_builder = hdfsNewBuilder();
    if (hdfs_builder == nullptr) {
        LOG(INFO) << "failed to init HDFSCommonBuilder, please check check be/conf/hdfs-site.xml";
        return Status::InternalError(
                "failed to init HDFSCommonBuilder, please check check be/conf/hdfs-site.xml");
    }
    hdfsBuilderSetForceNewInstance(hdfs_builder);
    return Status::OK();
}

Status HDFSCommonBuilder::run_kinit() {
    if (hdfs_kerberos_principal.empty() || hdfs_kerberos_keytab.empty()) {
        return Status::InvalidArgument("Invalid hdfs_kerberos_principal or hdfs_kerberos_keytab");
    }
    std::string ticket_path = TICKET_CACHE_PATH + generate_uuid_string();
    const char* krb_home = getenv("KRB_HOME");
    std::string krb_home_str(krb_home ? krb_home : "");
    fmt::memory_buffer kinit_command;
    if (krb_home_str.empty()) {
        fmt::format_to(kinit_command, "kinit -c {} -R -t {} -k {}", ticket_path,
                       hdfs_kerberos_keytab, hdfs_kerberos_principal);
    } else {
        // Assign kerberos home in env, get kinit in kerberos home
        fmt::format_to(kinit_command, krb_home_str + "/bin/kinit -c {} -R -t {} -k {}", ticket_path,
                       hdfs_kerberos_keytab, hdfs_kerberos_principal);
    }
    VLOG_NOTICE << "kinit command: " << fmt::to_string(kinit_command);
    std::string msg;
    AgentUtils util;
    bool rc = util.exec_cmd(fmt::to_string(kinit_command), &msg);
    if (!rc) {
        return Status::InternalError("Kinit failed, errMsg: " + msg);
    }
#ifdef USE_LIBHDFS3
    hdfsBuilderSetPrincipal(hdfs_builder, hdfs_kerberos_principal.c_str());
#endif
    hdfsBuilderConfSetStr(hdfs_builder, "hadoop.security.kerberos.ticket.cache.path",
                          ticket_path.c_str());
    LOG(INFO) << "finished to run kinit command: " << fmt::to_string(kinit_command);
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

Status create_hdfs_builder(const THdfsParams& hdfsParams, const std::string& fs_name,
                           HDFSCommonBuilder* builder) {
    RETURN_IF_ERROR(builder->init_hdfs_builder());
    hdfsBuilderSetNameNode(builder->get(), fs_name.c_str());
    // set kerberos conf
    if (hdfsParams.__isset.hdfs_kerberos_principal) {
        builder->need_kinit = true;
        builder->hdfs_kerberos_principal = hdfsParams.hdfs_kerberos_principal;
        hdfsBuilderSetUserName(builder->get(), hdfsParams.hdfs_kerberos_principal.c_str());
    } else if (hdfsParams.__isset.user) {
        hdfsBuilderSetUserName(builder->get(), hdfsParams.user.c_str());
#ifdef USE_HADOOP_HDFS
        hdfsBuilderSetKerb5Conf(builder->get(), nullptr);
        hdfsBuilderSetKeyTabFile(builder->get(), nullptr);
#endif
    }
    if (hdfsParams.__isset.hdfs_kerberos_keytab) {
        builder->need_kinit = true;
        builder->hdfs_kerberos_keytab = hdfsParams.hdfs_kerberos_keytab;
#ifdef USE_HADOOP_HDFS
        hdfsBuilderSetKeyTabFile(builder->get(), hdfsParams.hdfs_kerberos_keytab.c_str());
#endif
    }
    // set other conf
    if (hdfsParams.__isset.hdfs_conf) {
        for (const THdfsConf& conf : hdfsParams.hdfs_conf) {
            hdfsBuilderConfSetStr(builder->get(), conf.key.c_str(), conf.value.c_str());
            LOG(INFO) << "set hdfs config: " << conf.key << ", value: " << conf.value;
#ifdef USE_HADOOP_HDFS
            // Set krb5.conf, we should define java.security.krb5.conf in catalog properties
            if (strcmp(conf.key.c_str(), "java.security.krb5.conf") == 0) {
                hdfsBuilderSetKerb5Conf(builder->get(), conf.value.c_str());
            }
#endif
        }
    }

    hdfsBuilderConfSetStr(builder->get(), "ipc.client.fallback-to-simple-auth-allowed", "true");

    if (builder->is_need_kinit()) {
        RETURN_IF_ERROR(builder->run_kinit());
    }

    return Status::OK();
}

Status create_hdfs_builder(const std::map<std::string, std::string>& properties,
                           HDFSCommonBuilder* builder) {
    THdfsParams hdfsParams = parse_properties(properties);
    return create_hdfs_builder(hdfsParams, hdfsParams.fs_name, builder);
}

} // namespace doris
