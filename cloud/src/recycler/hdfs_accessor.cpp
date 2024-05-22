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

#include "recycler/hdfs_accessor.h"

#include <gen_cpp/cloud.pb.h>

#include <string_view>

#include "common/config.h"
#include "common/logging.h"
#include "recycler/obj_store_accessor.h"

namespace doris::cloud {
namespace {

std::string hdfs_error() {
#ifdef USE_HADOOP_HDFS
    const char* err_msg = hdfsGetLastExceptionRootCause();
#else
    const char* err_msg = hdfsGetLastError();
#endif
    return fmt::format("({}): {}", std::strerror(errno), err_msg ? err_msg : "");
}

} // namespace

class HDFSBuilder {
public:
    ~HDFSBuilder() {
        if (hdfs_builder_ != nullptr) {
            hdfsFreeBuilder(hdfs_builder_);
        }
    }

    // TODO(plat1ko): template <class Params>
    static hdfsFS create_fs(const HdfsBuildConf& params) {
        HDFSBuilder builder;
        int ret = builder.init_hdfs_builder();
        if (ret != 0) {
            return nullptr;
        }

        ret = builder.init(params);
        if (ret != 0) {
            return nullptr;
        }

        auto* fs = hdfsBuilderConnect(builder.hdfs_builder_);
#ifdef USE_HADOOP_HDFS
        // For hadoop hdfs, the `hdfs_builder_` will be freed in hdfsBuilderConnect
        builder.hdfs_builder_ = nullptr;
#endif
        if (!fs) {
            LOG(WARNING) << "failed to connect hdfs: " << hdfs_error();
        }

        return fs;
    }

private:
    HDFSBuilder() = default;

    // returns 0 for success otherwise error
    int init(const HdfsBuildConf& conf) {
        DCHECK(hdfs_builder_);
        hdfsBuilderSetNameNode(hdfs_builder_, conf.fs_name().c_str());
        // set kerberos conf
        bool kerberos_login = false;
        if (conf.has_hdfs_kerberos_keytab()) {
            kerberos_login = true;
#ifdef USE_HADOOP_HDFS
            hdfsBuilderSetKerb5Conf(hdfs_builder_, config::kerberos_krb5_conf_path.c_str());
            hdfsBuilderSetKeyTabFile(hdfs_builder_, conf.hdfs_kerberos_keytab().c_str());
#endif
        }

        if (conf.has_hdfs_kerberos_principal()) {
            kerberos_login = true;
            hdfsBuilderSetPrincipal(hdfs_builder_, conf.hdfs_kerberos_principal().c_str());
        } else if (conf.has_user()) {
            hdfsBuilderSetUserName(hdfs_builder_, conf.user().c_str());
#ifdef USE_HADOOP_HDFS
            hdfsBuilderSetKerb5Conf(hdfs_builder_, nullptr);
            hdfsBuilderSetKeyTabFile(hdfs_builder_, nullptr);
#endif
        }

        // set other conf
        for (const auto& kv : conf.hdfs_confs()) {
            hdfsBuilderConfSetStr(hdfs_builder_, kv.key().c_str(), kv.value().c_str());
#ifdef USE_HADOOP_HDFS
            // Set krb5.conf, we should define java.security.krb5.conf in catalog properties
            if (kv.key() == "java.security.krb5.conf") {
                hdfsBuilderSetKerb5Conf(hdfs_builder_, kv.value().c_str());
            }
#endif
        }

        if (kerberos_login) {
            int ret = check_krb_params(conf.hdfs_kerberos_principal(), conf.hdfs_kerberos_keytab());
            if (ret != 0) {
                return ret;
            }
        }

        hdfsBuilderConfSetStr(hdfs_builder_, "ipc.client.fallback-to-simple-auth-allowed", "true");
        return 0;
    }

    // returns 0 for success otherwise error
    int init_hdfs_builder() {
        hdfs_builder_ = hdfsNewBuilder();
        if (hdfs_builder_ == nullptr) {
            LOG(WARNING) << "failed to init HDFSBuilder, please check check be/conf/hdfs-site.xml";
            return -1;
        }
        hdfsBuilderSetForceNewInstance(hdfs_builder_);
        return 0;
    }

    // returns 0 for success otherwise error
    int check_krb_params(std::string_view hdfs_kerberos_principal,
                         std::string_view hdfs_kerberos_keytab) {
        const auto& ticket_path = config::kerberos_ccache_path;
        if (!ticket_path.empty()) {
            hdfsBuilderConfSetStr(hdfs_builder_, "hadoop.security.kerberos.ticket.cache.path",
                                  ticket_path.c_str());
            return 0;
        }

        // we should check hdfs_kerberos_principal and hdfs_kerberos_keytab nonnull to login kdc.
        if (hdfs_kerberos_principal.empty() || hdfs_kerberos_keytab.empty()) {
            LOG(WARNING) << "Invalid hdfs_kerberos_principal or hdfs_kerberos_keytab";
            return -1;
        }

        // enable auto-renew thread
        hdfsBuilderConfSetStr(hdfs_builder_, "hadoop.kerberos.keytab.login.autorenewal.enabled",
                              "true");
        return 0;
    }

    hdfsBuilder* hdfs_builder_ = nullptr;
};

HdfsAccessor::HdfsAccessor(const HdfsVaultInfo& info)
        : ObjStoreAccessor(AccessorType::HDFS), info_(info), prefix_(info.prefix()) {
    if (!prefix_.empty() && prefix_[0] != '/') {
        prefix_.insert(prefix_.begin(), '/');
    }
    uri_ = info.build_conf().fs_name() + prefix_;
}

HdfsAccessor::~HdfsAccessor() {
    if (fs_) {
        hdfsDisconnect(fs_);
    }
}

std::string HdfsAccessor::fs_path(const std::string& relative_path) {
    return prefix_ + '/' + relative_path;
}

std::string HdfsAccessor::uri(const std::string& relative_path) {
    return uri_ + '/' + relative_path;
}

int HdfsAccessor::init() {
    // TODO(plat1ko): Cache hdfsFS
    fs_ = HDFSBuilder::create_fs(info_.build_conf());
    if (!fs_) {
        LOG(WARNING) << "failed to init hdfs accessor. uri=" << uri_;
        return -1;
    }

    return 0;
}

int HdfsAccessor::delete_objects_by_prefix(const std::string& relative_path) {
    if (relative_path.empty() || relative_path.back() == '/') {
        // Is a directory
        // FIXME(plat1ko): Can we distinguish between RPC failure and the file not existing via
        // `hdfsDelete`'s return value or errno to avoid exist rpc?
        int ret = exist(relative_path);
        if (ret == 1) {
            return 0;
        } else if (ret < 0) {
            return ret;
        }

        // Path exists
        auto path = fs_path(relative_path);
        LOG_INFO("delete directory").tag("path", uri(relative_path)); // Audit log
        ret = hdfsDelete(fs_, path.c_str(), 1);
        if (ret != 0) {
            LOG_WARNING("failed to delete directory")
                    .tag("path", uri(relative_path))
                    .tag("error", hdfs_error());
            return ret;
        }

        return 0;
    }

    // TODO(plat1ko): Implement prefix delete for non-directory path
    return 0;
}

int HdfsAccessor::delete_objects(const std::vector<std::string>& relative_paths) {
    // FIXME(plat1ko): Inefficient!
    for (auto&& path : relative_paths) {
        int ret = delete_object(path);
        if (ret != 0) {
            return ret;
        }
    }

    return 0;
}

int HdfsAccessor::delete_object(const std::string& relative_path) {
    // FIXME(plat1ko): Can we distinguish between RPC failure and the file not existing via
    // `hdfsDelete`'s return value or errno to avoid exist rpc?
    int ret = exist(relative_path);
    if (ret == 1) {
        return 0;
    } else if (ret < 0) {
        return ret;
    }

    // Path exists
    auto path = fs_path(relative_path);
    LOG_INFO("delete object").tag("path", uri(relative_path)); // Audit log
    ret = hdfsDelete(fs_, path.c_str(), 0);
    if (ret != 0) {
        LOG_WARNING("failed to delete object")
                .tag("path", uri(relative_path))
                .tag("error", hdfs_error());
        return ret;
    }

    return 0;
}

int HdfsAccessor::put_object(const std::string& relative_path, const std::string& content) {
    auto path = fs_path(relative_path);
    auto* file = hdfsOpenFile(fs_, path.c_str(), O_WRONLY, 0, 0, 0);
    if (!file) {
        LOG_WARNING("failed to create file")
                .tag("path", uri(relative_path))
                .tag("error", hdfs_error());
        return -1;
    }

    std::unique_ptr<int, std::function<void(int*)>> defer((int*)0x01,
                                                          [&](int*) { hdfsCloseFile(fs_, file); });

    int64_t written_bytes = hdfsWrite(fs_, file, content.data(), content.size());
    if (written_bytes < content.size()) {
        LOG_WARNING("failed to write file")
                .tag("path", uri(relative_path))
                .tag("error", hdfs_error());
        return -1;
    }

    int ret = hdfsFlush(fs_, file);
    if (ret != 0) {
        LOG_WARNING("failed to flush file")
                .tag("path", uri(relative_path))
                .tag("error", hdfs_error());
        return -1;
    }

    return 0;
}

int HdfsAccessor::list(const std::string& relative_path, std::vector<ObjectMeta>* files) {
    auto path = fs_path(relative_path);

    int num_entries = 0;
    hdfsFileInfo* hdfs_file_info = hdfsListDirectory(fs_, path.c_str(), &num_entries);
    if (!hdfs_file_info) {
        LOG_WARNING("failed to list objects")
                .tag("path", uri(relative_path))
                .tag("error", hdfs_error());
        return -1;
    }

    for (int idx = 0; idx < num_entries; ++idx) {
        auto& file = hdfs_file_info[idx];
        std::string_view fname(file.mName);
        fname.remove_prefix(uri_.size() + 1);
        files->push_back({.path = std::string(fname), .size = file.mSize});
    }

    hdfsFreeFileInfo(hdfs_file_info, num_entries);

    return 0;
}

int HdfsAccessor::exist(const std::string& relative_path) {
    auto path = fs_path(relative_path);
    int ret = hdfsExists(fs_, path.c_str());
#ifdef USE_HADOOP_HDFS
    // when calling hdfsExists() and return non-zero code,
    // if errno is ENOENT, which means the file does not exist.
    // if errno is not ENOENT, which means it encounter other error, should return.
    // NOTE: not for libhdfs3 since it only runs on MaxOS, don't have to support it.
    //
    // See details:
    //  https://github.com/apache/hadoop/blob/5cda162a804fb0cfc2a5ac0058ab407662c5fb00/
    //  hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs/hdfs.c#L1923-L1924
    if (ret != 0 && errno != ENOENT) {
        LOG_WARNING("failed to check object existence")
                .tag("path", uri(relative_path))
                .tag("error", hdfs_error());
        return -1;
    }
#endif

    return ret != 0;
}

} // namespace doris::cloud
