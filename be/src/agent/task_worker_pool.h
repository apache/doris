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

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>

#include "gutil/ref_counted.h"

namespace doris {

class ExecEnv;
class StorageEngine;
class Thread;
class ThreadPool;
class TMasterInfo;
class TReportRequest;
class TTabletInfo;
class TAgentTaskRequest;

class TaskWorkerPool {
public:
    TaskWorkerPool(std::string_view name, int worker_count,
                   std::function<void(const TAgentTaskRequest&)> callback);

    virtual ~TaskWorkerPool();

    void stop();

    void submit_task(const TAgentTaskRequest& task);

protected:
    std::atomic_bool _stopped {false};
    std::unique_ptr<ThreadPool> _thread_pool;
    std::function<void(const TAgentTaskRequest&)> _callback;
};

class PublishVersionWorkerPool final : public TaskWorkerPool {
public:
    PublishVersionWorkerPool(StorageEngine& engine);

    ~PublishVersionWorkerPool() override;

private:
    void publish_version_callback(const TAgentTaskRequest& task);

    StorageEngine& _engine;
};

class PriorTaskWorkerPool {
public:
    PriorTaskWorkerPool(std::string_view name, int normal_worker_count, int high_prior_worker_conut,
                        std::function<void(const TAgentTaskRequest& task)> callback);

    ~PriorTaskWorkerPool();

    void stop();

    void submit_task(const TAgentTaskRequest& task);

private:
    void normal_loop();

    void high_prior_loop();

    bool _stopped {false};

    std::mutex _mtx;
    std::condition_variable _normal_condv;
    std::deque<std::unique_ptr<TAgentTaskRequest>> _normal_queue;
    std::condition_variable _high_prior_condv;
    std::deque<std::unique_ptr<TAgentTaskRequest>> _high_prior_queue;

    std::unique_ptr<ThreadPool> _normal_pool;
    std::unique_ptr<ThreadPool> _high_prior_pool;

    std::function<void(const TAgentTaskRequest&)> _callback;
};

class ReportWorker {
public:
    ReportWorker(std::string name, const TMasterInfo& master_info, int report_interval_s,
                 std::function<void()> callback);

    ~ReportWorker();

    std::string_view name() const { return _name; }

    // Notify the worker to report immediately
    void notify();

    void stop();

private:
    std::string _name;
    scoped_refptr<Thread> _thread;

    std::mutex _mtx;
    std::condition_variable _condv;
    bool _stopped {false};
    bool _signal {false};
};

void alter_inverted_index_callback(StorageEngine& engine, const TAgentTaskRequest& req);

void check_consistency_callback(StorageEngine& engine, const TAgentTaskRequest& req);

void upload_callback(StorageEngine& engine, ExecEnv* env, const TAgentTaskRequest& req);

void download_callback(StorageEngine& engine, ExecEnv* env, const TAgentTaskRequest& req);

void make_snapshot_callback(StorageEngine& engine, const TAgentTaskRequest& req);

void release_snapshot_callback(StorageEngine& engine, const TAgentTaskRequest& req);

void move_dir_callback(StorageEngine& engine, ExecEnv* env, const TAgentTaskRequest& req);

void submit_table_compaction_callback(StorageEngine& engine, const TAgentTaskRequest& req);

void push_storage_policy_callback(StorageEngine& engine, const TAgentTaskRequest& req);

void push_cooldown_conf_callback(StorageEngine& engine, const TAgentTaskRequest& req);

void create_tablet_callback(StorageEngine& engine, const TAgentTaskRequest& req);

void drop_tablet_callback(StorageEngine& engine, const TAgentTaskRequest& req);

void clear_transaction_task_callback(StorageEngine& engine, const TAgentTaskRequest& req);

void push_callback(const TAgentTaskRequest& req);

void update_tablet_meta_callback(StorageEngine& engine, const TAgentTaskRequest& req);

void alter_tablet_callback(StorageEngine& engine, const TAgentTaskRequest& req);

void clone_callback(StorageEngine& engine, const TMasterInfo& master_info,
                    const TAgentTaskRequest& req);

void storage_medium_migrate_callback(StorageEngine& engine, const TAgentTaskRequest& req);

void gc_binlog_callback(StorageEngine& engine, const TAgentTaskRequest& req);

void report_task_callback(const TMasterInfo& master_info);

void report_disk_callback(StorageEngine& engine, const TMasterInfo& master_info);

void report_tablet_callback(StorageEngine& engine, const TMasterInfo& master_info);

} // namespace doris
