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

#include "io/fs/hdfs/hdfs_mgr.h"

#include <bthread/bthread.h>
#include <bthread/countdown_event.h>

#include <algorithm>
#include <chrono>
#include <thread>

#include "common/config.h"
#include "common/kerberos/kerberos_ticket_mgr.h"
#include "common/logging.h"
#include "core/string_ref.h"
#include "io/fs/err_utils.h"
#include "io/hdfs_builder.h"
#include "io/hdfs_tvf_policy.h"
#include "io/hdfs_util.h"
#include "runtime/exec_env.h"
#ifdef USE_HADOOP_HDFS
#include "util/jni-util.h"
#endif

namespace doris::io {

HdfsMgr::HdfsMgr() : _should_stop_cleanup_thread(false) {
    _start_cleanup_thread();
}

HdfsMgr::~HdfsMgr() {
    _stop_cleanup_thread();
}

void HdfsMgr::_start_cleanup_thread() {
    _cleanup_thread = std::make_unique<std::thread>(&HdfsMgr::_cleanup_loop, this);
}

void HdfsMgr::_stop_cleanup_thread() {
    if (_cleanup_thread) {
        _should_stop_cleanup_thread = true;
        _cleanup_thread->join();
        _cleanup_thread.reset();
    }
}

void HdfsMgr::_cleanup_loop() {
#ifdef BE_TEST
    static constexpr int64_t CHECK_INTERVAL_SECONDS = 1; // For testing purpose
#else
    static constexpr int64_t CHECK_INTERVAL_SECONDS = 5; // Check stop flag every 5 seconds
#endif
    uint64_t last_cleanup_time = std::time(nullptr);

    while (!_should_stop_cleanup_thread) {
        uint64_t current_time = std::time(nullptr);

        // Only perform cleanup if enough time has passed
        if (current_time - last_cleanup_time >= _cleanup_interval_seconds) {
            // Collect expired handlers under lock
            std::vector<std::shared_ptr<HdfsHandler>> handlers_to_cleanup;
            {
                std::lock_guard<std::mutex> lock(_mutex);

                // Find expired handlers
                for (auto it = _fs_handlers.begin(); it != _fs_handlers.end();) {
                    const auto& entry = *it;
                    bool is_expired = current_time - entry.second->last_access_time >=
                                      _instance_timeout_seconds;
                    // bool is_krb_expired =
                    //         entry.second->is_kerberos_auth &&
                    //         (current_time - entry.second->create_time >=
                    //          entry.second->ticket_cache->get_ticket_lifetime_sec() / 2);
                    if (is_expired) {
                        LOG(INFO) << "Found expired HDFS handler, hash_code=" << entry.first
                                  << ", last_access_time=" << entry.second->last_access_time
                                  << ", is_kerberos=" << entry.second->is_kerberos_auth
                                  << ", principal=" << entry.second->principal
                                  << ", fs_name=" << entry.second->fs_name
                                  << ", is_expired=" << is_expired;
                        // << ", is_krb_expire=" << is_krb_expired;
                        handlers_to_cleanup.push_back(entry.second);
                        it = _fs_handlers.erase(it);
                    } else {
                        ++it;
                    }
                }
            }

            // Cleanup handlers outside lock
            for (const auto& handler : handlers_to_cleanup) {
                LOG(INFO) << "Start to cleanup HDFS handler"
                          << ", is_kerberos=" << handler->is_kerberos_auth
                          << ", principal=" << handler->principal
                          << ", fs_name=" << handler->fs_name;

                // The kerberos ticket cache will be automatically cleaned up when the last reference is gone
                // DO NOT call hdfsDisconnect(), or we will meet "Filesystem closed"
                // even if we create a new one
                // hdfsDisconnect(handler->hdfs_fs);

                LOG(INFO) << "Finished cleanup HDFS handler"
                          << ", fs_name=" << handler->fs_name;
            }

            handlers_to_cleanup.clear();
            last_cleanup_time = current_time;
        }

        // Sleep for a short interval to check stop flag more frequently
        std::this_thread::sleep_for(std::chrono::seconds(CHECK_INTERVAL_SECONDS));
    }
}

Status HdfsMgr::get_or_create_fs(const THdfsParams& hdfs_params, const std::string& fs_name,
                                 std::shared_ptr<HdfsHandler>* fs_handler,
                                 const std::string& native_path) {
#ifdef USE_HADOOP_HDFS
    if (!config::enable_java_support) {
        return Status::InvalidArgument(
                "hdfs file system is not enabled; enable_java_support is required");
    }
#endif
    bool guarded = HdfsTvfPolicy::requested(hdfs_params);
    if (guarded && native_path.empty()) {
        return Status::InvalidArgument("TVF Hadoop IO requires its actual native path");
    }
    uint64_t hash_code = _hdfs_hash_code(hdfs_params, fs_name);
    auto matches_policy = [&](const std::shared_ptr<HdfsHandler>& handler) {
        return guarded ? handler->tvf_params && *handler->tvf_params == hdfs_params &&
                                 handler->fs_name == fs_name
                       : !handler->tvf_params;
    };
    auto validate = [&](const std::shared_ptr<HdfsHandler>& handler) {
        if (!guarded) {
            return Status::OK();
        }
        auto check = [&] { return _check_tvf_path_impl(*handler, fs_name, native_path); };
        // A direct remote provider never dispatches by path; avoid a pthread per cached file.
        if (handler->tvf_context && !handler->tvf_context->requires_jni()) {
            return check();
        }
        return HdfsTvfPolicy::on_pthread(check);
    };
    std::shared_ptr<HdfsHandler> selected;
    {
        std::scoped_lock lock(_mutex);
        auto [first, last] = _fs_handlers.equal_range(hash_code);
        auto it = std::find_if(first, last,
                               [&](const auto& entry) { return matches_policy(entry.second); });
        if (it != last) {
            selected = it->second;
        }
    }
    if (!selected) {
        THdfsParams creation = hdfs_params;
        if (guarded) {
            // Internal metadata is attached to the private connection request, never the cache key.
            for (const auto& entry : {std::make_pair(HdfsTvfPolicy::ROOT_KEY, fs_name),
                                      std::make_pair(HdfsTvfPolicy::PATH_KEY, native_path)}) {
                THdfsConf conf;
                conf.__set_key(entry.first);
                conf.__set_value(entry.second);
                creation.hdfs_conf.push_back(std::move(conf));
            }
            creation.__isset.hdfs_conf = true;
        }
        std::shared_ptr<HdfsHandler> created;
        RETURN_IF_ERROR(_create_hdfs_fs(creation, fs_name, &created));
        if (guarded) {
            created->tvf_params = std::make_shared<const THdfsParams>(hdfs_params);
        }
        RETURN_IF_ERROR(validate(created));
        {
            std::scoped_lock lock(_mutex);
            auto [first, last] = _fs_handlers.equal_range(hash_code);
            auto it = std::find_if(first, last,
                                   [&](const auto& entry) { return matches_policy(entry.second); });
            if (it != last) {
                selected = it->second;
            } else {
                selected = created;
                // Keep exact identities in the same hash bucket, including CRC collisions.
                // Each remains reusable and cleanup erases only the expired entry.
                _fs_handlers.emplace(hash_code, created);
            }
        }
    }
    RETURN_IF_ERROR(validate(selected));
    selected->update_access_time();
    *fs_handler = std::move(selected);
    return Status::OK();
}

Status HdfsMgr::_check_tvf_path_impl(const HdfsHandler& handler, const std::string& fs_name,
                                     const std::string& native_path) {
#ifdef USE_HADOOP_HDFS
    if (!handler.tvf_context) {
        return Status::NotSupported("TVF Hadoop connection has no runtime configuration context");
    }
    return handler.tvf_context->check(fs_name, native_path);
#else
    return Status::OK();                                 // libhdfs3 is a native HDFS client without Hadoop provider dispatch.
#endif
}

Status HdfsMgr::_create_hdfs_fs_impl(const THdfsParams& hdfs_params, const std::string& fs_name,
                                     std::shared_ptr<HdfsHandler>* fs_handler) {
#ifdef USE_HADOOP_HDFS
    bool guarded = HdfsTvfPolicy::requested(hdfs_params);
    if (guarded) {
        RETURN_IF_ERROR(HdfsTvfPolicy::prepare_runtime());
    }
#endif
    HDFSCommonBuilder builder;
    RETURN_IF_ERROR(create_hdfs_builder(hdfs_params, fs_name, &builder));
    hdfsFS hdfs_fs = hdfsBuilderConnect(builder.get());
    if (hdfs_fs == nullptr) {
#ifdef USE_HADOOP_HDFS
        if (guarded) {
            RETURN_IF_ERROR(HdfsTvfPolicy::clear_runtime());
        }
#endif
        return Status::InternalError("failed to connect to hdfs {}: {}", fs_name, hdfs_error());
    }
    std::shared_ptr<HdfsTvfPolicy> context;
#ifdef USE_HADOOP_HDFS
    if (guarded) {
        Status captured = HdfsTvfPolicy::capture(fs_name, &context);
        if (!captured.ok()) {
            hdfsDisconnect(hdfs_fs);
            return captured;
        }
    }
#endif

    bool is_kerberos = builder.is_kerberos();
    *fs_handler = std::make_shared<HdfsHandler>(
            hdfs_fs, is_kerberos, is_kerberos ? hdfs_params.hdfs_kerberos_principal : "",
            is_kerberos ? hdfs_params.hdfs_kerberos_keytab : "", fs_name);
    (*fs_handler)->tvf_context = std::move(context);
    // builder.get_ticket_cache());
    return Status::OK();
}

// https://brpc.apache.org/docs/server/basics/
// According to the brpc doc, JNI code checks stack layout and cannot be run in
// bthreads so create a pthread for creating hdfs connection if necessary.
Status HdfsMgr::_create_hdfs_fs(const THdfsParams& hdfs_params, const std::string& fs_name,
                                std::shared_ptr<HdfsHandler>* fs_handler) {
    return HdfsTvfPolicy::on_pthread(
            [&] { return _create_hdfs_fs_impl(hdfs_params, fs_name, fs_handler); });
}

uint64_t HdfsMgr::_hdfs_hash_code(const THdfsParams& hdfs_params, const std::string& fs_name) {
    uint64_t hash_code = 0;
    // The specified fsname is used first.
    // If there is no specified fsname, the default fsname is used
    if (!fs_name.empty()) {
        hash_code ^= crc32_hash(fs_name);
    } else if (hdfs_params.__isset.fs_name) {
        hash_code ^= crc32_hash(hdfs_params.fs_name);
    }

    if (hdfs_params.__isset.user) {
        hash_code ^= crc32_hash(hdfs_params.user);
    }
    if (hdfs_params.__isset.hdfs_kerberos_principal) {
        hash_code ^= crc32_hash(hdfs_params.hdfs_kerberos_principal);
    }
    if (hdfs_params.__isset.hdfs_kerberos_keytab) {
        hash_code ^= crc32_hash(hdfs_params.hdfs_kerberos_keytab);
    }
    if (hdfs_params.__isset.hdfs_conf) {
        std::map<std::string, std::string> conf_map;
        for (const auto& conf : hdfs_params.hdfs_conf) {
            conf_map[conf.key] = conf.value;
        }
        for (auto& conf : conf_map) {
            hash_code ^= crc32_hash(conf.first);
            hash_code ^= crc32_hash(conf.second);
        }
    }
    return hash_code;
}

} // namespace doris::io
