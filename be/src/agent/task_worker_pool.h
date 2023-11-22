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

#pragma once

#include <butil/macros.h>
#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Types_types.h>
#include <stdint.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>

#include "common/status.h"
#include "olap/tablet.h"
#include "util/countdown_latch.h"
#include "util/metrics.h"

namespace doris {

class ExecEnv;
class ThreadPool;
class AgentUtils;
class DataDir;
class TFinishTaskRequest;
class TMasterInfo;
class TReportRequest;
class TTabletInfo;

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
        CLEAR_ALTER_TASK [[deprecated]],
        CLEAR_TRANSACTION_TASK,
        DELETE,
        ALTER_TABLE,
        // Deprecated
        QUERY_SPLIT_KEY [[deprecated]],
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
        SUBMIT_TABLE_COMPACTION,
        PUSH_COOLDOWN_CONF,
        PUSH_STORAGE_POLICY,
        ALTER_INVERTED_INDEX,
        GC_BINLOG,
    };

    enum ReportType { TASK, DISK, TABLET };

    enum class ThreadModel {
        SINGLE_THREAD, // Only 1 thread allowed in the pool
        MULTI_THREADS  // 1 or more threads allowed in the pool
    };

    const std::string TYPE_STRING(TaskWorkerType type) {
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
        case CLEAR_TRANSACTION_TASK:
            return "CLEAR_TRANSACTION_TASK";
        case DELETE:
            return "DELETE";
        case ALTER_TABLE:
            return "ALTER_TABLE";
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
        case PUSH_COOLDOWN_CONF:
            return "PUSH_COOLDOWN_CONF";
        case PUSH_STORAGE_POLICY:
            return "PUSH_STORAGE_POLICY";
        case ALTER_INVERTED_INDEX:
            return "ALTER_INVERTED_INDEX";
        case GC_BINLOG:
            return "GC_BINLOG";
        default:
            return "Unknown";
        }
    }

    const std::string TYPE_STRING(ReportType type) {
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

    TaskWorkerType task_worker_type() const { return _task_worker_type; }

protected:
    bool _register_task_info(const TTaskType::type task_type, int64_t signature);
    void _remove_task_info(const TTaskType::type task_type, int64_t signature);
    void _finish_task(const TFinishTaskRequest& finish_task_request);

    void _alter_inverted_index_worker_thread_callback();
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
    void _push_cooldown_conf_worker_thread_callback();
    void _push_storage_policy_worker_thread_callback();
    void _gc_binlog_worker_thread_callback();

    void _alter_tablet(const TAgentTaskRequest& alter_tablet_request, int64_t signature,
                       const TTaskType::type task_type, TFinishTaskRequest* finish_task_request);
    void _alter_inverted_index(const TAgentTaskRequest& alter_inverted_index_request,
                               int64_t signature, const TTaskType::type task_type,
                               TFinishTaskRequest* finish_task_request);

    void _handle_report(const TReportRequest& request, ReportType type);

    Status _get_tablet_info(const TTabletId tablet_id, const TSchemaHash schema_hash,
                            int64_t signature, TTabletInfo* tablet_info);

    Status _move_dir(const TTabletId tablet_id, const std::string& src, int64_t job_id,
                     bool overwrite);

    // random sleep 1~second seconds
    void _random_sleep(int second);

protected:
    std::string _name;

    // Reference to the ExecEnv::_master_info
    const TMasterInfo& _master_info;
    std::unique_ptr<AgentUtils> _agent_utils;
    ExecEnv* _env;

    // Protect task queue
    std::mutex _worker_thread_lock;
    std::condition_variable _worker_thread_condition_variable;
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
    std::function<void()> _cb;

    static std::atomic_ulong _s_report_version;

    static std::mutex _s_task_signatures_lock;
    static std::map<TTaskType::type, std::set<int64_t>> _s_task_signatures;

    DISALLOW_COPY_AND_ASSIGN(TaskWorkerPool);
}; // class TaskWorkerPool

class CreateTableTaskPool : public TaskWorkerPool {
public:
    CreateTableTaskPool(ExecEnv* env, ThreadModel thread_model);
    void _create_tablet_worker_thread_callback();

    DISALLOW_COPY_AND_ASSIGN(CreateTableTaskPool);
};

class DropTableTaskPool : public TaskWorkerPool {
public:
    DropTableTaskPool(ExecEnv* env, ThreadModel thread_model);
    void _drop_tablet_worker_thread_callback();

    DISALLOW_COPY_AND_ASSIGN(DropTableTaskPool);
};

class PushTaskPool : public TaskWorkerPool {
public:
    enum class PushWokerType { LOAD_V2, DELETE };
    PushTaskPool(ExecEnv* env, ThreadModel thread_model, PushWokerType type);
    void _push_worker_thread_callback();

    DISALLOW_COPY_AND_ASSIGN(PushTaskPool);

private:
    PushWokerType _push_worker_type;
};

class PublishVersionTaskPool : public TaskWorkerPool {
public:
    PublishVersionTaskPool(ExecEnv* env, ThreadModel thread_model);
    void _publish_version_worker_thread_callback();

    DISALLOW_COPY_AND_ASSIGN(PublishVersionTaskPool);
};

class ClearTransactionTaskPool : public TaskWorkerPool {
public:
    ClearTransactionTaskPool(ExecEnv* env, ThreadModel thread_model);
    void _clear_transaction_task_worker_thread_callback();

    DISALLOW_COPY_AND_ASSIGN(ClearTransactionTaskPool);
};

class AlterTableTaskPool : public TaskWorkerPool {
public:
    AlterTableTaskPool(ExecEnv* env, ThreadModel thread_model);
    void _alter_tablet(const TAgentTaskRequest& alter_tablet_request, int64_t signature,
                       const TTaskType::type task_type, TFinishTaskRequest* finish_task_request);
    void _alter_tablet_worker_thread_callback();

    DISALLOW_COPY_AND_ASSIGN(AlterTableTaskPool);
};

class CloneTaskPool : public TaskWorkerPool {
public:
    CloneTaskPool(ExecEnv* env, ThreadModel thread_model);
    void _clone_worker_thread_callback();

    DISALLOW_COPY_AND_ASSIGN(CloneTaskPool);
};

class StorageMediumMigrateTaskPool : public TaskWorkerPool {
public:
    StorageMediumMigrateTaskPool(ExecEnv* env, ThreadModel thread_model);
    Status _check_migrate_request(const TStorageMediumMigrateReq& req, TabletSharedPtr& tablet,
                                  DataDir** dest_store);
    void _storage_medium_migrate_worker_thread_callback();

    DISALLOW_COPY_AND_ASSIGN(StorageMediumMigrateTaskPool);
};

} // namespace doris
