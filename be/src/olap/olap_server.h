// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef BDG_PALO_BE_SRC_OLAP_OLAP_SERVER_H
#define BDG_PALO_BE_SRC_OLAP_OLAP_SERVER_H

#include "olap/atomic.h"
#include "olap/olap_define.h"
#include "olap/utils.h"

namespace palo {
// @brief 基于ubserver的服务器类，负责接收发送外界给OLAPEngine的请求
class OLAPServer {
public:
    OLAPServer();

    // @brief 初始化server
    //
    // @param path 配置文件路径
    // @param file 配置文件名
    OLAPStatus init(const char* path, const char* file);

private:
    // Thread functions

    // base expansion thread process function
    static void* _be_thread_callback(void* arg);

    // garbage sweep thread process function. clear snapshot and trash folder
    static void* _garbage_sweeper_thread_callback(void* arg);

    // delete table with io error process function
    static void* _disk_stat_monitor_thread_callback(void* arg);

    // unused index process function
    static void* _unused_index_thread_callback(void* arg);

    // cumulative process function
    static void* _cumulative_thread_callback(void* arg);

    // clean file descriptors cache
    static void* _fd_cache_clean_callback(void* arg);

    // thread to monitor snapshot expiry
    pthread_t _garbage_sweeper_thread;
    static MutexLock _s_garbage_sweeper_mutex;
    static Condition _s_garbage_sweeper_cond;

    // thread to monitor disk stat
    pthread_t _disk_stat_monitor_thread;
    static MutexLock _s_disk_stat_monitor_mutex;
    static Condition _s_disk_stat_monitor_cond;

    // thread to check disks
    pthread_t _check_disks_thread;
    static MutexLock _s_check_disks_mutex;
    static Condition _s_check_disks_cond;

    // thread to monitor unused index
    pthread_t _unused_index_thread;
    static MutexLock _s_unused_index_mutex;
    static Condition _s_unused_index_cond;

    // thread to check session timeout
    pthread_t _session_timeout_thread;
    static MutexLock _s_session_timeout_mutex;
    static Condition _s_session_timeout_cond;

    // thread to run base expansion
    pthread_t* _be_threads;

    // thread to check cumulative
    pthread_t* _cumulative_threads;

    pthread_t* _fd_cache_clean_thread;

    static atomic_t _s_request_number;
};

}  // namespace palo

#endif // BDG_PALO_BE_SRC_OLAP_OLAP_SERVER_H
