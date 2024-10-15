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

#include <bvar/latency_recorder.h>
#include <gen_cpp/cloud.pb.h>

#include "common/stopwatch.h"
#include "recycler/util.h"

#ifdef USE_HADOOP_HDFS
#include <hadoop_hdfs/hdfs.h> // IWYU pragma: export
#else
#include <hdfs/hdfs.h> // IWYU pragma: export
#endif

#include <string_view>

#include "common/config.h"
#include "common/logging.h"
#include "common/string_util.h"
#include "cpp/sync_point.h"
#include "recycler/storage_vault_accessor.h"

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

bvar::LatencyRecorder hdfs_write_latency("hdfs_write");
bvar::LatencyRecorder hdfs_open_latency("hdfs_open");
bvar::LatencyRecorder hdfs_close_latency("hdfs_close");
bvar::LatencyRecorder hdfs_list_dir("hdfs_list_dir");
bvar::LatencyRecorder hdfs_exist_latency("hdfs_exist");
bvar::LatencyRecorder hdfs_delete_latency("hdfs_delete");
} // namespace

class HDFSBuilder {
public:
    ~HDFSBuilder() {
        if (hdfs_builder_ != nullptr) {
            hdfsFreeBuilder(hdfs_builder_);
        }
    }

    // TODO(plat1ko): template <class Params>
    static HdfsSPtr create_fs(const HdfsBuildConf& params) {
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
            return nullptr;
        }

        return {fs, [fs_name = params.fs_name()](auto fs) {
                    LOG_INFO("disconnect hdfs").tag("fs_name", fs_name);
                    hdfsDisconnect(fs);
                }};
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

class HdfsListIterator final : public ListIterator {
private:
    class DirEntries {
    public:
        DirEntries() = default;
        DirEntries(hdfsFileInfo* entries, size_t num_entries)
                : entries_(entries), num_entries_(num_entries) {
            DCHECK_EQ(!!entries, num_entries > 0);
            TEST_SYNC_POINT_CALLBACK("DirEntries", &num_entries_);
        }

        ~DirEntries() {
            if (entries_) {
                TEST_SYNC_POINT_CALLBACK("~DirEntries", &num_entries_);
                hdfsFreeFileInfo(entries_, num_entries_);
            }
        }

        DirEntries(const DirEntries&) = delete;
        DirEntries& operator=(const DirEntries&) = delete;
        DirEntries(DirEntries&& rhs) noexcept
                : entries_(rhs.entries_), num_entries_(rhs.num_entries_), offset_(rhs.offset_) {
            rhs.entries_ = nullptr;
            rhs.num_entries_ = 0;
            rhs.offset_ = 0;
        }
        DirEntries& operator=(DirEntries&& rhs) noexcept {
            std::swap(entries_, rhs.entries_);
            std::swap(num_entries_, rhs.num_entries_);
            std::swap(offset_, rhs.offset_);
            return *this;
        }

        bool empty() const { return offset_ >= num_entries_; }

        hdfsFileInfo& front() { return entries_[offset_]; }

        void pop_front() { ++offset_; }

    private:
        hdfsFileInfo* entries_ {nullptr};
        size_t num_entries_ {0};
        size_t offset_ {0};
    };

public:
    HdfsListIterator(HdfsSPtr hdfs, std::string dir_path, std::string uri)
            : hdfs_(std::move(hdfs)), dir_path_(std::move(dir_path)), uri_(std::move(uri)) {}

    ~HdfsListIterator() override = default;

    bool is_valid() override { return is_valid_; }

    bool has_next() override {
        if (!is_valid_) {
            return false;
        }

        if (!has_init_) {
            has_init_ = true;
            auto next_level_dir_entries = list_directory(dir_path_.c_str());
            if (!next_level_dir_entries) {
                is_valid_ = false;
                return false;
            }

            if (!next_level_dir_entries->empty()) {
                level_dirs_.emplace_back(std::move(next_level_dir_entries.value()));
            }
        }

        while (!level_dirs_.empty()) {
            auto& dir_entries = level_dirs_.back();
            auto& entry = dir_entries.front();
            if (entry.mKind == kObjectKindFile) {
                return true;
            }

            // entry is a dir
            auto next_level_dir_entries = list_directory(entry.mName);
            if (!next_level_dir_entries) {
                is_valid_ = false;
                return false;
            }

            dir_entries.pop_front();
            if (dir_entries.empty()) {
                level_dirs_.pop_back();
            }

            if (!next_level_dir_entries->empty()) {
                level_dirs_.emplace_back(std::move(next_level_dir_entries.value()));
            }
        }

        return false;
    }

    std::optional<FileMeta> next() override {
        std::optional<FileMeta> res;
        if (!has_next()) {
            return res;
        }

        auto& dir_entries = level_dirs_.back();
        auto& entry = dir_entries.front();
        DCHECK_EQ(entry.mKind, kObjectKindFile) << entry.mName;
        std::string_view path {entry.mName};
        DCHECK(path.starts_with(uri_)) << path << ' ' << uri_;
        path.remove_prefix(uri_.length());
        res = FileMeta {.path = std::string(path), .size = entry.mSize, .mtime_s = entry.mLastMod};
        dir_entries.pop_front();
        if (dir_entries.empty()) {
            level_dirs_.pop_back();
        }

        return res;
    }

private:
    // Return null if error occured, return emtpy DirEntries if dir is empty or doesn't exist.
    std::optional<DirEntries> list_directory(const char* dir_path) {
        int num_entries = 0;
        SCOPED_BVAR_LATENCY(hdfs_list_dir);
        auto* file_infos = hdfsListDirectory(hdfs_.get(), dir_path, &num_entries);
        if (errno != 0 && errno != ENOENT) {
            LOG_WARNING("failed to list hdfs directory")
                    .tag("uri", uri_)
                    .tag("dir_path", dir_path)
                    .tag("error", hdfs_error());
            return std::nullopt;
        }

        return DirEntries {file_infos, static_cast<size_t>(num_entries)};
    }

    HdfsSPtr hdfs_;
    std::string dir_path_; // absolute path start with '/'
    std::string uri_;      // {fs_name}/{prefix}/
    bool is_valid_ {true};
    bool has_init_ {false};
    std::vector<DirEntries> level_dirs_;
};

HdfsAccessor::HdfsAccessor(const HdfsVaultInfo& info)
        : StorageVaultAccessor(AccessorType::HDFS), info_(info), prefix_(info.prefix()) {
    if (!prefix_.empty() && prefix_[0] != '/') {
        prefix_.insert(prefix_.begin(), '/');
    }
    uri_ = info.build_conf().fs_name() + prefix_;
}

HdfsAccessor::~HdfsAccessor() = default;

std::string HdfsAccessor::to_fs_path(const std::string& relative_path) {
    return prefix_ + '/' + relative_path;
}

std::string HdfsAccessor::to_uri(const std::string& relative_path) {
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

int HdfsAccessor::delete_prefix(const std::string& path_prefix, int64_t expiration_time) {
    auto uri = to_uri(path_prefix);
    LOG(INFO) << "delete prefix, uri=" << uri;
    std::unique_ptr<ListIterator> list_iter;
    int ret = list_all(&list_iter);
    if (ret != 0) {
        LOG(WARNING) << "delete prefix, failed to list" << uri;
        return ret;
    }
    size_t num_listed = 0, num_deleted = 0;
    for (auto file = list_iter->next(); file; file = list_iter->next()) {
        ++num_listed;
        if (file->path.find(path_prefix) != 0) continue;
        if (int del_ret = delete_file(file->path); del_ret != 0) {
            ret = del_ret;
            break;
        }
        ++num_deleted;
    }
    LOG(INFO) << "delete prefix " << (ret != 0 ? "failed" : "succ") << " ret=" << ret
              << " uri=" << uri << " num_listed=" << num_listed << " num_deleted=" << num_deleted;
    return ret;
}

int HdfsAccessor::delete_directory_impl(const std::string& dir_path) {
    // FIXME(plat1ko): Can we distinguish between RPC failure and the file not existing via
    // `hdfsDelete`'s return value or errno to avoid exist rpc?
    int ret = exists(dir_path);
    if (ret == 1) {
        return 0;
    } else if (ret < 0) {
        return ret;
    }

    // Path exists
    auto path = to_fs_path(dir_path);
    LOG_INFO("delete directory").tag("uri", to_uri(dir_path)); // Audit log
    ret = hdfsDelete(fs_.get(), path.c_str(), 1);
    if (ret != 0) {
        LOG_WARNING("failed to delete directory")
                .tag("path", to_uri(dir_path))
                .tag("error", hdfs_error());
        return ret;
    }

    return 0;
}

int HdfsAccessor::delete_directory(const std::string& dir_path) {
    auto norm_dir_path = dir_path;
    strip_leading(norm_dir_path, "/");
    if (norm_dir_path.empty()) {
        LOG_WARNING("invalid dir_path {}", dir_path);
        return -1;
    }

    return delete_directory_impl(norm_dir_path);
}

int HdfsAccessor::delete_all(int64_t expiration_time) {
    if (expiration_time <= 0) {
        return delete_directory_impl("");
    }

    std::unique_ptr<ListIterator> list_iter;
    int ret = list_all(&list_iter);
    if (ret != 0) {
        return ret;
    }

    for (auto file = list_iter->next(); file; file = list_iter->next()) {
        if (file->mtime_s > expiration_time) {
            continue;
        }

        if (int del_ret = delete_file(file->path); del_ret != 0) {
            ret = del_ret;
        }
    }

    return ret;
}

int HdfsAccessor::delete_files(const std::vector<std::string>& paths) {
    int ret = 0;
    for (auto&& path : paths) {
        int del_ret = delete_file(path);
        if (del_ret != 0) {
            ret = del_ret;
        }
    }

    return ret;
}

int HdfsAccessor::delete_file(const std::string& relative_path) {
    // FIXME(plat1ko): Can we distinguish between RPC failure and the file not existing via
    // `hdfsDelete`'s return value or errno to avoid exist rpc?
    int ret = exists(relative_path);
    if (ret == 1) {
        return 0;
    } else if (ret < 0) {
        return ret;
    }

    // Path exists
    auto path = to_fs_path(relative_path);
    LOG_INFO("delete object").tag("uri", to_uri(relative_path)); // Audit log
    SCOPED_BVAR_LATENCY(hdfs_delete_latency);
    ret = hdfsDelete(fs_.get(), path.c_str(), 0);
    if (ret != 0) {
        LOG_WARNING("failed to delete object")
                .tag("path", to_uri(relative_path))
                .tag("error", hdfs_error());
        return ret;
    }

    return 0;
}

int HdfsAccessor::put_file(const std::string& relative_path, const std::string& content) {
    auto path = to_fs_path(relative_path);
    hdfsFile file;
    {
        SCOPED_BVAR_LATENCY(hdfs_open_latency);
        file = hdfsOpenFile(fs_.get(), path.c_str(), O_WRONLY, 0, 0, 0);
    }
    if (!file) {
        LOG_WARNING("failed to create file")
                .tag("uri", to_uri(relative_path))
                .tag("error", hdfs_error());
        return -1;
    }

    std::unique_ptr<int, std::function<void(int*)>> defer((int*)0x01, [&](int*) {
        if (file) {
            SCOPED_BVAR_LATENCY(hdfs_close_latency);
            hdfsCloseFile(fs_.get(), file);
        }
    });

    int64_t written_bytes = 0;
    {
        SCOPED_BVAR_LATENCY(hdfs_write_latency);
        written_bytes = hdfsWrite(fs_.get(), file, content.data(), content.size());
    }
    if (written_bytes < content.size()) {
        LOG_WARNING("failed to write file")
                .tag("uri", to_uri(relative_path))
                .tag("error", hdfs_error());
        return -1;
    }

    int ret = 0;
    {
        SCOPED_BVAR_LATENCY(hdfs_close_latency);
        ret = hdfsCloseFile(fs_.get(), file);
    }
    file = nullptr;
    if (ret != 0) {
        LOG_WARNING("failed to close file")
                .tag("uri", to_uri(relative_path))
                .tag("error", hdfs_error());
        return -1;
    }

    return 0;
}

int HdfsAccessor::list_directory(const std::string& dir_path, std::unique_ptr<ListIterator>* res) {
    auto norm_dir_path = dir_path;
    strip_leading(norm_dir_path, "/");
    if (norm_dir_path.empty()) {
        LOG_WARNING("invalid dir_path {}", dir_path);
        return -1;
    }

    *res = std::make_unique<HdfsListIterator>(fs_, to_fs_path(norm_dir_path), uri_ + '/');
    return 0;
}

int HdfsAccessor::list_all(std::unique_ptr<ListIterator>* res) {
    *res = std::make_unique<HdfsListIterator>(fs_, to_fs_path(""), uri_ + '/');
    return 0;
}

int HdfsAccessor::exists(const std::string& relative_path) {
    auto path = to_fs_path(relative_path);
    SCOPED_BVAR_LATENCY(hdfs_exist_latency);
    int ret = hdfsExists(fs_.get(), path.c_str());
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
                .tag("uri", to_uri(relative_path))
                .tag("error", hdfs_error());
        return -1;
    }
#endif

    return ret != 0;
}

} // namespace doris::cloud
