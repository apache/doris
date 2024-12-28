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

#include <cstdarg>
#include <cstdlib>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#ifdef USE_HADOOP_HDFS
#include "hadoop_hdfs/hdfs.h"
#endif
#include "io/fs/hdfs.h"
#include "util/string_util.h"

namespace doris {

#ifdef USE_HADOOP_HDFS
void err_log_message(const char* fmt, ...) {
    va_list args;
    va_start(args, fmt);

    // First, call vsnprintf to get the required buffer size
    int size = vsnprintf(nullptr, 0, fmt, args) + 1; // +1 for '\0'
    if (size <= 0) {
        LOG(ERROR) << "Error formatting log message, invalid size";
        va_end(args);
        return;
    }

    va_end(args);
    va_start(args, fmt); // Reinitialize va_list

    // Allocate a buffer and format the string into it
    std::vector<char> buffer(size);
    vsnprintf(buffer.data(), size, fmt, args);

    va_end(args);

    // Use glog to log the message
    LOG(ERROR) << buffer.data();
}

void va_err_log_message(const char* fmt, va_list ap) {
    va_list args_copy;
    va_copy(args_copy, ap);

    // Call vsnprintf to get the required buffer size
    int size = vsnprintf(nullptr, 0, fmt, args_copy) + 1; // +1 for '\0'
    va_end(args_copy);                                    // Release the copied va_list

    if (size <= 0) {
        LOG(ERROR) << "Error formatting log message, invalid size";
        return;
    }

    // Reinitialize va_list for the second vsnprintf call
    va_copy(args_copy, ap);

    // Allocate a buffer and format the string into it
    std::vector<char> buffer(size);
    vsnprintf(buffer.data(), size, fmt, args_copy);

    va_end(args_copy);

    // Use glog to log the message
    LOG(ERROR) << buffer.data();
}

struct hdfsLogger logger = {.errLogMessage = err_log_message,
                            .vaErrLogMessage = va_err_log_message};
#endif // #ifdef USE_HADOOP_HDFS

Status HDFSCommonBuilder::init_hdfs_builder() {
#ifdef USE_HADOOP_HDFS
    static std::once_flag flag;
    std::call_once(flag, []() { hdfsSetLogger(&logger); });
#endif // #ifdef USE_HADOOP_HDFS

    hdfs_builder = hdfsNewBuilder();
    if (hdfs_builder == nullptr) {
        LOG(INFO) << "failed to init HDFSCommonBuilder, please check check be/conf/hdfs-site.xml";
        return Status::InternalError(
                "failed to init HDFSCommonBuilder, please check check be/conf/hdfs-site.xml");
    }
    hdfsBuilderSetForceNewInstance(hdfs_builder);
    return Status::OK();
}

Status HDFSCommonBuilder::check_krb_params() {
    std::string ticket_path = doris::config::kerberos_ccache_path;
    if (!ticket_path.empty()) {
        hdfsBuilderConfSetStr(hdfs_builder, "hadoop.security.kerberos.ticket.cache.path",
                              ticket_path.c_str());
        return Status::OK();
    }
    // we should check hdfs_kerberos_principal and hdfs_kerberos_keytab nonnull to login kdc.
    if (hdfs_kerberos_principal.empty() || hdfs_kerberos_keytab.empty()) {
        return Status::InvalidArgument("Invalid hdfs_kerberos_principal or hdfs_kerberos_keytab");
    }
    // enable auto-renew thread
    hdfsBuilderConfSetStr(hdfs_builder, "hadoop.kerberos.keytab.login.autorenewal.enabled", "true");
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
    if (hdfsParams.__isset.hdfs_kerberos_keytab) {
        builder->kerberos_login = true;
        builder->hdfs_kerberos_keytab = hdfsParams.hdfs_kerberos_keytab;
#ifdef USE_HADOOP_HDFS
        hdfsBuilderSetKerb5Conf(builder->get(), doris::config::kerberos_krb5_conf_path.c_str());
        hdfsBuilderSetKeyTabFile(builder->get(), hdfsParams.hdfs_kerberos_keytab.c_str());
#endif
    }
    if (hdfsParams.__isset.hdfs_kerberos_principal) {
        builder->kerberos_login = true;
        builder->hdfs_kerberos_principal = hdfsParams.hdfs_kerberos_principal;
        hdfsBuilderSetPrincipal(builder->get(), hdfsParams.hdfs_kerberos_principal.c_str());
    } else if (hdfsParams.__isset.user) {
        hdfsBuilderSetUserName(builder->get(), hdfsParams.user.c_str());
#ifdef USE_HADOOP_HDFS
        hdfsBuilderSetKerb5Conf(builder->get(), nullptr);
        hdfsBuilderSetKeyTabFile(builder->get(), nullptr);
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
    if (builder->is_kerberos()) {
        RETURN_IF_ERROR(builder->check_krb_params());
    }
    hdfsBuilderConfSetStr(builder->get(), "ipc.client.fallback-to-simple-auth-allowed", "true");
    return Status::OK();
}

Status create_hdfs_builder(const std::map<std::string, std::string>& properties,
                           HDFSCommonBuilder* builder) {
    THdfsParams hdfsParams = parse_properties(properties);
    return create_hdfs_builder(hdfsParams, hdfsParams.fs_name, builder);
}

} // namespace doris
