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

#include "exec/hdfs_builder.h"

#include <fstream>

#include "agent/utils.h"
#include "common/logging.h"
#include "util/url_coding.h"

namespace doris {

Status HDFSCommonBuilder::runKinit() {
    if (!hdfs_kerberos_keytab_base64.empty()) {
        // write keytab file
        std::ofstream fp("./doris.keytab");
        if (!fp) {
            LOG(WARNING) << "create keytab file failed";
            return Status::InternalError("Create keytab file failed");
        }
        fp << hdfs_kerberos_keytab_base64 << std::endl;
        fp.close();
        hdfs_kerberos_keytab = "./doris.keytab";
    }
    if (hdfs_kerberos_principal.empty() || hdfs_kerberos_keytab.empty()) {
        return Status::InvalidArgument("Invalid hdfs_kerberos_principal or hdfs_kerberos_keytab");
    }
    std::stringstream ss;
    ss << "kinit -R -t \"" << hdfs_kerberos_keytab << "\" -k " << hdfs_kerberos_principal;
    LOG(INFO) << "kinit command: " << ss.str();
    std::string msg;
    AgentUtils util;
    bool rc = util.exec_cmd(ss.str(), &msg);
    if (!rc) {
        return Status::InternalError("Kinit failed, errMsg: " + msg);
    }
    return Status::OK();
}

HDFSCommonBuilder createHDFSBuilder(THdfsParams hdfsParams) {
    HDFSCommonBuilder builder;
    hdfsBuilderSetNameNode(builder.get(), hdfsParams.fs_name.c_str());
    // set hdfs user
    if (hdfsParams.__isset.user) {
        hdfsBuilderSetUserName(builder.get(), hdfsParams.user.c_str());
    }
    // set kerberos conf
    if (hdfsParams.__isset.hdfs_security_authentication) {
        if (hdfsParams.hdfs_security_authentication == "kerberos") {
            builder.needKinit = true;
        }
    }
    if (hdfsParams.__isset.hdfs_kerberos_principal) {
        builder.needKinit = true;
        builder.hdfs_kerberos_principal = hdfsParams.hdfs_kerberos_principal;
        hdfsBuilderSetPrincipal(builder.get(), hdfsParams.hdfs_kerberos_principal.c_str());
    }
    if (hdfsParams.__isset.hdfs_kerberos_keytab) {
        builder.needKinit = true;
        builder.hdfs_kerberos_keytab = hdfsParams.hdfs_kerberos_keytab;
    }
    if (hdfsParams.__isset.hdfs_kerberos_keytab_with_base64) {
        // decode base64-encoded keytab's content
        builder.needKinit = true;
        base64_decode(hdfsParams.hdfs_kerberos_keytab_with_base64, &builder.hdfs_kerberos_keytab_base64);
    }
    // set other conf
    if (hdfsParams.__isset.hdfs_conf) {
        for (const THdfsConf& conf : hdfsParams.hdfs_conf) {
            hdfsBuilderConfSetStr(builder.get(), conf.key.c_str(), conf.value.c_str());
        }
    }

    return builder;
}

} // namespace doris