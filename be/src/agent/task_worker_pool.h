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

#ifndef DORIS_BE_SRC_TASK_WORKER_POOL_H
#define DORIS_BE_SRC_TASK_WORKER_POOL_H

#include <atomic>
#include <deque>
#include <memory>
#include <utility>
#include <vector>

#include "agent/utils.h"
#include "common/status.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "gutil/ref_counted.h"
#include "olap/olap_define.h"
#include "olap/storage_engine.h"
#include "util/condition_variable.h"
#include "util/countdown_latch.h"
#include "util/mutex.h"
#include "util/thread.h"

namespace doris {

class ExecEnv;
class ThreadPool;

class TaskWorkerPool {
public:
    // You need to modify the content in TYPE_STRING at the same time,
    enum TaskWorkerType {
        CREATE_TABLE,
        DROP_TABLE,
        PUSH,
        REALTIME_PUSH,
        PUBLISH_VERSION,
        // Deprecated
        CLEAR_ALTER_TASK,
        CLEAR_TRANSACTION_TASK,
        DELETE,
        ALTER_TABLE,
        // Deprecated
        QUERY_SPLIT_KEY,
        CLONE,
        STORAGE_MEDIUM_MIGRATE,
        CHECK_CONSISTENCY,
        REPORT_TASK,
        REPORT_DISK_STATE,
        REPORT_OLAP_TABLE,
        UPLOAD,
        DOWNLOAD,
        MAKE_SNAPSHOT,
        RELEASE_SNAPSHOT,
        MOVE,
        RECOVER_TABLET,
        UPDATE_TABLET_META_INFO,
        SUBMIT_TABLE_COMPACTION
    };

    enum ReportType {
        TASK,
        DISK,
        TABLET
    };

    enum class ThreadModel {
        SINGLE_THREAD,      // Only 1 thread allowed in the pool
        MULTI_THREADS       // 1 or more threads allowed in the pool
    };

    inline const std::string TYPE_STRING(TaskWorkerType type) {
        switch (type) {
        case CREATE_TABLE:
            return "CREATE_TABLE";
        case DROP_TABLE:
            return "DROP_TABLE";
        case PUSH:
            return "PUSH";
        case REALTIME_PUSH:
            return "REALTIME_PUSH";
        case PUBLISH_VERSION:
            return "PUBLISH_VERSION";
        case CLEAR_ALTER_TASK:
            return "CLEAR_ALTER_TASK";
        case CLEAR_TRANSACTION_TASK:
            return "CLEAR_TRANSACTION_TASK";
        case DELETE:
            return "DELETE";
        case ALTER_TABLE:
            return "ALTER_TABLE";
        case QUERY_SPLIT_KEY:
            return "QUERY_SPLIT_KEY";
        case CLONE:
            return "CLONE";
        case STORAGE_MEDIUM_MIGRATE:
            return "STORAGE_MEDIUM_MIGRATE";
        case CHECK_CONSISTENCY:
            return "CHECK_CONSISTENCY";
        case REPORT_TASK:
            return "REPORT_TASK";
        case REPORT_DISK_STATE:
            return "REPORT_DISK_STATE";
        case REPORT_OLAP_TABLE:
            return "REPORT_OLAP_TABLE";
        case UPLOAD:
            return "UPLOAD";
        case DOWNLOAD:
            return "DOWNLOAD";
        case MAKE_SNAPSHOT:
            return "MAKE_SNAPSHOT";
        case RELEASE_SNAPSHOT:
            return "RELEASE_SNAPSHOT";
        case MOVE:
            return "MOVE";
        case RECOVER_TABLET:
            return "RECOVER_TABLET";
        case UPDATE_TABLET_META_INFO:
            return "UPDATE_TABLET_META_INFO";
        case SUBMIT_TABLE_COMPACTION:
            return "SUBMIT_TABLE_COMPACTION";
        default:
            return "Unknown";
        }
    }

    inline const std::string TYPE_STRING(ReportType type) {
        switch (type) {
        case TASK:
            return "TASK";
        case DISK:
            return "DISK";
        case TABLET:
            return "TABLET";
        default:
            return "Unknown";
        }
    }

    TaskWorkerPool(const TaskWorkerType task_worker_type, ExecEnv* env,
                   const TMasterInfo& master_info, ThreadModel thread_model);
    virtual ~TaskWorkerPool();

    // Start the task worker thread pool
    virtual void start();

    // Stop the task worker
    virtual void stop();

    // Submit task to task pool
    //
    // Input parameters:
    // * task: the task need callback thread to do
    virtual void submit_task(const TAgentTaskRequest& task);

    // notify the worker. currently for task/disk/tablet report thread
    void notify_thread();

private:
    bool _register_task_info(const TTaskType::type task_type, int64_t signature);
    void _remove_task_info(const TTaskType::type task_type, int64_t signature);
    void _finish_task(const TFinishTaskRequest& finish_task_request);
    uint32_t _get_next_task_index(int32_t thread_count, std::deque<TAgentTaskRequest>& tasks,
                                  TPriority::type priority);

    void _create_tablet_worker_thread_callback();
    void _drop_tablet_worker_thread_callback();
    void _push_worker_thread_callback();
    void _publish_version_worker_thread_callback();
    void _clear_transaction_task_worker_thread_callback();
    void _alter_tablet_worker_thread_callback();
    void _clone_worker_thread_callback();
    void _storage_medium_migrate_worker_thread_callback();
    void _check_consistency_worker_thread_callback();
    void _report_task_worker_thread_callback();
    void _report_disk_state_worker_thread_callback();
    void _report_tablet_worker_thread_callback();
    void _upload_worker_thread_callback();
    void _download_worker_thread_callback();
    void _make_snapshot_thread_callback();
    void _release_snapshot_thread_callback();
    void _move_dir_thread_callback();
    void _update_tablet_meta_worker_thread_callback();
    void _submit_table_compaction_worker_thread_callback();

    void _alter_tablet(const TAgentTaskRequest& alter_tablet_request, int64_t signature,
                       const TTaskType::type task_type, TFinishTaskRequest* finish_task_request);
    void _handle_report(TReportRequest& request, ReportType type);

    Status _get_tablet_info(const TTabletId tablet_id, const TSchemaHash schema_hash,
                                 int64_t signature, TTabletInfo* tablet_info);

    Status _move_dir(const TTabletId tablet_id, const TSchemaHash schema_hash,
                          const std::string& src, int64_t job_id, bool overwrite);

    OLAPStatus _check_migrate_requset(const TStorageMediumMigrateReq& req, TabletSharedPtr& tablet,
                                      DataDir** dest_store);

    // random sleep 1~second seconds
    void _random_sleep(int second);

private:
    std::string _name;

    // Reference to the ExecEnv::_master_info
    const TMasterInfo& _master_info;
    TBackend _backend;
    std::unique_ptr<AgentUtils> _agent_utils;
    std::unique_ptr<MasterServerClient> _master_client;
    ExecEnv* _env;

    // Protect task queue
    Mutex _worker_thread_lock;
    ConditionVariable _worker_thread_condition_variable;
    CountDownLatch _stop_background_threads_latch;
    bool _is_work;
    ThreadModel _thread_model;
    std::unique_ptr<ThreadPool> _thread_pool;
    // Only meaningful when _thread_model is MULTI_THREADS
    std::deque<TAgentTaskRequest> _tasks;
    // Only meaningful when _thread_model is SINGLE_THREAD
    std::atomic<bool> _is_doing_work;

    std::shared_ptr<MetricEntity> _metric_entity;
    UIntGauge* agent_task_queue_size;

    // Always 1 when _thread_model is SINGLE_THREAD
    uint32_t _worker_count;
    TaskWorkerType _task_worker_type;

    static FrontendServiceClientCache _master_service_client_cache;
    static std::atomic_ulong _s_report_version;

    static Mutex _s_task_signatures_lock;
    static std::map<TTaskType::type, std::set<int64_t>> _s_task_signatures;

    DISALLOW_COPY_AND_ASSIGN(TaskWorkerPool);
}; // class TaskWorkerPool
} // namespace doris
#endif // DORIS_BE_SRC_TASK_WORKER_POOL_H
