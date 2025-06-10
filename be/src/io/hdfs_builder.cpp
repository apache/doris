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
#include "common/kerberos/kerberos_ticket_mgr.h"
#include "common/logging.h"
#ifdef USE_HADOOP_HDFS
#include "hadoop_hdfs/hdfs.h"
#endif
#include "io/fs/hdfs.h"
#include "runtime/exec_env.h"
#include "util/string_util.h"

namespace doris {

#ifdef USE_DORIS_HADOOP_HDFS
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
#endif // #ifdef USE_DORIS_HADOOP_HDFS

Status HDFSCommonBuilder::init_hdfs_builder() {
#ifdef USE_DORIS_HADOOP_HDFS
    static std::once_flag flag;
    std::call_once(flag, []() { hdfsSetLogger(&logger); });
#endif // #ifdef USE_DORIS_HADOOP_HDFS

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

void HDFSCommonBuilder::set_hdfs_conf(const std::string& key, const std::string& val) {
    hdfs_conf[key] = val;
}

std::string HDFSCommonBuilder::get_hdfs_conf_value(const std::string& key,
                                                   const std::string& default_val) const {
    auto it = hdfs_conf.find(key);
    if (it != hdfs_conf.end()) {
        return it->second;
    } else {
        return default_val;
    }
}

void HDFSCommonBuilder::set_hdfs_conf_to_hdfs_builder() {
    for (const auto& pair : hdfs_conf) {
        hdfsBuilderConfSetStr(hdfs_builder, pair.first.c_str(), pair.second.c_str());
    }
}

// This method is deprecated, will be removed later
Status HDFSCommonBuilder::set_kerberos_ticket_cache() {
    // kerberos::KerberosConfig config;
    // config.set_principal_and_keytab(hdfs_kerberos_principal, hdfs_kerberos_keytab);
    // config.set_krb5_conf_path(config::kerberos_krb5_conf_path);
    // config.set_refresh_interval(config::kerberos_refresh_interval_second);
    // config.set_min_time_before_refresh(600);
    // kerberos::KerberosTicketMgr* ticket_mgr = ExecEnv::GetInstance()->kerberos_ticket_mgr();
    // RETURN_IF_ERROR(ticket_mgr->get_or_set_ticket_cache(config, &ticket_cache));
    // // ATTN, can't use ticket_cache->get_ticket_cache_path() directly,
    // // it may cause the kerberos ticket cache path in libhdfs is empty,
    // kerberos_ticket_path = ticket_cache->get_ticket_cache_path();
    // hdfsBuilderSetUserName(hdfs_builder, hdfs_kerberos_principal.c_str());
    // hdfsBuilderSetKerbTicketCachePath(hdfs_builder, kerberos_ticket_path.c_str());
    // hdfsBuilderSetForceNewInstance(hdfs_builder);
    // LOG(INFO) << "get kerberos ticket path: " << kerberos_ticket_path
    //           << " with principal: " << hdfs_kerberos_principal;
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
    builder->fs_name = fs_name;
    hdfsBuilderSetNameNode(builder->get(), builder->fs_name.c_str());
    LOG(INFO) << "set hdfs namenode: " << fs_name;

    std::string auth_type = "simple";
    // First, copy all hdfs conf and set to hdfs builder
    if (hdfsParams.__isset.hdfs_conf) {
        // set other conf
        for (const THdfsConf& conf : hdfsParams.hdfs_conf) {
            builder->set_hdfs_conf(conf.key, conf.value);
            LOG(INFO) << "set hdfs config key: " << conf.key << ", value: " << conf.value;
            if (strcmp(conf.key.c_str(), "hadoop.security.authentication") == 0) {
                auth_type = conf.value;
            }
        }
        builder->set_hdfs_conf_to_hdfs_builder();
    }

    if (auth_type == "kerberos") {
        // set kerberos conf
        if (!hdfsParams.__isset.hdfs_kerberos_principal ||
            !hdfsParams.__isset.hdfs_kerberos_keytab) {
            return Status::InvalidArgument("Must set both principal and keytab");
        }
        builder->kerberos_login = true;
        builder->hdfs_kerberos_principal = hdfsParams.hdfs_kerberos_principal;
        builder->hdfs_kerberos_keytab = hdfsParams.hdfs_kerberos_keytab;
        hdfsBuilderSetPrincipal(builder->get(), builder->hdfs_kerberos_principal.c_str());
#ifdef USE_HADOOP_HDFS
        hdfsBuilderSetKerb5Conf(builder->get(), doris::config::kerberos_krb5_conf_path.c_str());
        hdfsBuilderSetKeyTabFile(builder->get(), builder->hdfs_kerberos_keytab.c_str());
#endif
        hdfsBuilderConfSetStr(builder->get(), "hadoop.kerberos.keytab.login.autorenewal.enabled",
                              "true");
        // RETURN_IF_ERROR(builder->set_kerberos_ticket_cache());
    } else {
        if (hdfsParams.__isset.user) {
            builder->hadoop_user = hdfsParams.user;
            hdfsBuilderSetUserName(builder->get(), builder->hadoop_user.c_str());
        }
    }
    hdfsBuilderConfSetStr(builder->get(), FALLBACK_TO_SIMPLE_AUTH_ALLOWED.c_str(),
                          TRUE_VALUE.c_str());
    return Status::OK();
}

Status create_hdfs_builder(const std::map<std::string, std::string>& properties,
                           HDFSCommonBuilder* builder) {
    THdfsParams hdfsParams = parse_properties(properties);
    return create_hdfs_builder(hdfsParams, hdfsParams.fs_name, builder);
}

} // namespace doris
