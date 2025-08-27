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
#include <boost/intrusive/detail/algo_type.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/list_hook.hpp>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "util/threadpool.h"
#include "vec/exec/executor/listenable_future.h"
#include "vec/exec/executor/task_executor.h"
#include "vec/exec/executor/ticker.h"
#include "vec/exec/executor/time_sharing/multilevel_split_queue.h"
#include "vec/exec/executor/time_sharing/prioritized_split_runner.h"

namespace doris {
namespace vectorized {

class TimeSharingTaskExecutor;
class SplitThreadPoolToken;

/**
 * @brief TimeSharingTaskExecutor
 *
 * This class implements a time-slice-based task scheduling and execution engine.
 * It is responsible for managing and scheduling multiple concurrent tasks (Task),
 * and the splits (Split) under each task. Each split is executed in a time-sharing
 * manner, running periodically until the split is either fully processed or fails.
 *
 * Main Features:
 *  - Supports dynamic registration, removal, and scheduling of tasks.
 *  - Provides multi-level priority scheduling for splits to prevent starvation of slow tasks.
 *  - Supports dynamic adjustment of task concurrency and resource isolation.
 *  - Offers automatic scheduling at the split level and fine-grained concurrency control.
 *  - Integrates task queues, thread pool management, blocking/wakeup mechanisms, and metrics collection.
 *
 * Implementation Details:
 *  - The thread pool is similar to the implementation in util/threadpool.h.
 *    In the original thread pool, each logical sub-pool is managed via a token with its own FIFO queue.
 *  - In TimeSharingTaskExecutor, there is no per-queue token; instead, only a single "tokenless" token is used.
 *    This token manages a SplitQueue, which by default is a MultilevelSplitQueue (a multi-level priority queue).
 *  - The design ensures that only one split is scheduled for execution in each time slice,
 *    and the scheduling is fair and adaptive to workload characteristics.
 *
 * Thread Safety:
 *  - This class is thread-safe.
 *
 */
class TimeSharingTaskExecutor : public TaskExecutor {
    ENABLE_FACTORY_CREATOR(TimeSharingTaskExecutor);

public:
    struct ThreadConfig {
        std::string thread_name;
        std::string workload_group;
        int max_thread_num;
        int min_thread_num;
        int max_queue_size = std::numeric_limits<int>::max();
        std::weak_ptr<CgroupCpuCtl> cgroup_cpu_ctl;
    };

    TimeSharingTaskExecutor(ThreadConfig config, int min_concurrency,
                            int guaranteed_concurrency_per_task, int max_concurrency_per_task,
                            std::shared_ptr<Ticker> ticker,
                            std::shared_ptr<SplitQueue> split_queue = nullptr,
                            bool enable_concurrency_control = true);

    ~TimeSharingTaskExecutor() override;

    Status init() override;

    Status start() override;
    void stop() override;

    Result<std::shared_ptr<TaskHandle>> create_task(
            const TaskId& task_id, std::function<double()> utilization_supplier,
            int initial_split_concurrency,
            std::chrono::nanoseconds split_concurrency_adjust_frequency,
            std::optional<int> max_concurrency_per_task) override;

    Status add_task(const TaskId& task_id, std::shared_ptr<TaskHandle> task_handle) override;

    Status remove_task(std::shared_ptr<TaskHandle> task_handle) override;

    Result<std::vector<SharedListenableFuture<Void>>> enqueue_splits(
            std::shared_ptr<TaskHandle> task_handle, bool intermediate,
            const std::vector<std::shared_ptr<SplitRunner>>& splits) override;

    Status re_enqueue_split(std::shared_ptr<TaskHandle> task_handle, bool intermediate,
                            const std::shared_ptr<SplitRunner>& split) override;

    size_t waiting_splits_size() const;

    size_t intermediate_splits_size() const {
        std::lock_guard<std::mutex> guard(_mutex);
        return _intermediate_splits.size();
    }

    size_t running_splits_size() const { return _running_splits.size(); }

    size_t blocked_splits_size() const { return _blocked_splits.size(); }

    size_t total_splits_size() const {
        std::lock_guard<std::mutex> guard(_mutex);
        return _all_splits.size();
    }

    size_t tasks_size() const {
        std::lock_guard<std::mutex> guard(_mutex);
        return _tasks.size();
    }

    int64_t completed_tasks_level0() const { return _completed_tasks_per_level[0]; }

    int64_t completed_tasks_level1() const { return _completed_tasks_per_level[1]; }

    int64_t completed_tasks_level2() const { return _completed_tasks_per_level[2]; }

    int64_t completed_tasks_level3() const { return _completed_tasks_per_level[3]; }

    int64_t completed_tasks_level4() const { return _completed_tasks_per_level[4]; }

    int64_t completed_splits_level0() const { return _completed_splits_per_level[0]; }

    int64_t completed_splits_level1() const { return _completed_splits_per_level[1]; }

    int64_t completed_splits_level2() const { return _completed_splits_per_level[2]; }

    int64_t completed_splits_level3() const { return _completed_splits_per_level[3]; }

    int64_t completed_splits_level4() const { return _completed_splits_per_level[4]; }

    int64_t running_tasks_level0() const { return _get_running_tasks_for_level(0); }

    int64_t running_tasks_level1() const { return _get_running_tasks_for_level(1); }

    int64_t running_tasks_level2() const { return _get_running_tasks_for_level(2); }

    int64_t running_tasks_level3() const { return _get_running_tasks_for_level(3); }

    int64_t running_tasks_level4() const { return _get_running_tasks_for_level(4); }

    // Allocates a new token for use in token-based task submission. All tokens
    // must be destroyed before their ThreadPool is destroyed.
    //
    // There is no limit on the number of tokens that may be allocated.
    enum class ExecutionMode {
        // Tasks submitted via this token will be executed serially.
        SERIAL,

        // Tasks submitted via this token may be executed concurrently.
        CONCURRENT
    };

    // Return the number of threads currently running (or in the process of starting up)
    // for this thread pool.
    int num_threads() const {
        std::lock_guard<std::mutex> l(_lock);
        return _num_threads + _num_threads_pending_start;
    }

    int max_threads() const {
        std::lock_guard<std::mutex> l(_lock);
        return _max_threads;
    }

    int min_threads() const {
        std::lock_guard<std::mutex> l(_lock);
        return _min_threads;
    }

    int num_threads_pending_start() const {
        std::lock_guard<std::mutex> l(_lock);
        return _num_threads_pending_start;
    }

    int num_active_threads() const {
        std::lock_guard<std::mutex> l(_lock);
        return _active_threads;
    }

    int get_queue_size() const {
        std::lock_guard<std::mutex> l(_lock);
        return _total_queued_tasks;
    }

    int get_max_queue_size() const {
        std::lock_guard<std::mutex> l(_lock);
        return _max_queue_size;
    }

    std::vector<int> debug_info() const {
        std::lock_guard<std::mutex> l(_lock);
        std::vector<int> arr = {_num_threads, static_cast<int>(_threads.size()), _min_threads,
                                _max_threads};
        return arr;
    }

    std::string get_info() const {
        std::lock_guard<std::mutex> l(_lock);
        return fmt::format(
                "SplitThreadPool(name={}, threads(active/pending)=({}/{}), queued_task={})",
                _thread_name, _active_threads, _num_threads_pending_start, _total_queued_tasks);
    }

    Status set_min_threads(int min_threads);
    Status set_max_threads(int max_threads);

private:
    Status _add_runner_thread();
    void _schedule_task_if_necessary(std::shared_ptr<TimeSharingTaskHandle> task_handle,
                                     std::unique_lock<std::mutex>& lock);
    void _add_new_entrants(std::unique_lock<std::mutex>& lock);
    void _start_intermediate_split(std::shared_ptr<PrioritizedSplitRunner> split,
                                   std::unique_lock<std::mutex>& lock);
    void _start_split(std::shared_ptr<PrioritizedSplitRunner> split,
                      std::unique_lock<std::mutex>& lock);
    std::shared_ptr<PrioritizedSplitRunner> _poll_next_split_worker(
            std::unique_lock<std::mutex>& lock);
    void _record_leaf_splits_size(std::unique_lock<std::mutex>& lock);
    void _split_finished(std::shared_ptr<PrioritizedSplitRunner> split, const Status& status);
    // Waits until all the tasks are completed.
    void wait();

    int64_t _get_running_tasks_for_level(int level) const;

    std::unique_ptr<ThreadPool> _thread_pool;
    const std::string _thread_name;
    const std::string _workload_group;
    int _min_threads;
    int _max_threads;
    const int _max_queue_size;
    std::weak_ptr<CgroupCpuCtl> _cgroup_cpu_ctl;
    const std::chrono::milliseconds _idle_timeout {std::chrono::milliseconds(500)};

    const int _min_concurrency;
    const int _guaranteed_concurrency_per_task;
    const int _max_concurrency_per_task;
    std::shared_ptr<Ticker> _ticker;

    mutable std::mutex _mutex;
    std::condition_variable _condition;
    std::atomic<bool> _stopped {false};

    std::unordered_map<TaskId, std::shared_ptr<TimeSharingTaskHandle>> _tasks;

    std::unordered_set<std::shared_ptr<PrioritizedSplitRunner>> _all_splits;
    std::unordered_set<std::shared_ptr<PrioritizedSplitRunner>> _intermediate_splits;
    std::unordered_set<std::shared_ptr<PrioritizedSplitRunner>> _running_splits;
    std::unordered_map<std::shared_ptr<PrioritizedSplitRunner>, SharedListenableFuture<Void>>
            _blocked_splits;
    std::array<std::atomic<int64_t>, 5> _completed_tasks_per_level = {0, 0, 0, 0, 0};
    std::array<std::atomic<int64_t>, 5> _completed_splits_per_level = {0, 0, 0, 0, 0};

    // friend class SplitThreadPoolBuilder;
    friend class SplitThreadPoolToken;

    // Dispatcher responsible for dequeueing and executing the tasks
    void _dispatch_thread();

    // Create new thread.
    //
    // REQUIRES: caller has incremented '_num_threads_pending_start' ahead of this call.
    // NOTE: For performance reasons, _lock should not be held.
    Status _create_thread();

    // Aborts if the current thread is a member of this thread pool.
    void check_not_pool_thread_unlocked();

    // // Submits a task to be run via token.
    Status _do_submit(std::shared_ptr<PrioritizedSplitRunner> split);

    //NOTE: not thread safe, caller should keep it thread-safe by using lock
    Status _try_create_thread(int thread_num, std::lock_guard<std::mutex>&);

    // Overall status of the pool. Set to an error when the pool is shut down.
    //
    // Protected by '_lock'.
    Status _pool_status {Status::Uninitialized("The pool was not initialized.")};

    // Synchronizes many of the members of the pool and all of its
    // condition variables.
    mutable std::mutex _lock;

    // Condition variable for "pool is idling". Waiters wake up when
    // _active_threads reaches zero.
    std::condition_variable _idle_cond;

    // Condition variable for "pool has no threads". Waiters wake up when
    // _num_threads and num_pending_threads_ are both 0.
    std::condition_variable _no_threads_cond;

    // Number of threads currently running.
    //
    // Protected by _lock.
    int _num_threads {0};

    // Number of threads which are in the process of starting.
    // When these threads start, they will decrement this counter and
    // accordingly increment '_num_threads'.
    //
    // Protected by _lock.
    int _num_threads_pending_start {0};

    // Number of threads currently running and executing client tasks.
    //
    // Protected by _lock.
    int _active_threads {0};

    // Total number of client tasks queued, either directly (_queue) or
    // indirectly (_tokens).
    //
    // Protected by _lock.
    int _total_queued_tasks {0};

    // Pointers to all running threads. Raw pointers are safe because a Thread
    // may only go out of scope after being removed from _threads.
    //
    // Protected by _lock.
    std::unordered_set<Thread*> _threads;

    // List of all threads currently waiting for work.
    //
    // A thread is added to the front of the list when it goes idle and is
    // removed from the front and signaled when new work arrives. This produces a
    // LIFO usage pattern that is more efficient than idling on a single
    //
    // Protected by _lock.
    struct IdleThread : public boost::intrusive::list_base_hook<> {
        explicit IdleThread() = default;

        // Condition variable for "queue is not empty". Waiters wake up when a new
        // task is queued.
        std::condition_variable not_empty;
        IdleThread(const IdleThread&) = delete;
        void operator=(const IdleThread&) = delete;
    };
    boost::intrusive::list<IdleThread> _idle_threads; // NOLINT(build/include_what_you_use)

    // ExecutionMode::CONCURRENT token used by the pool for tokenless submission.
    std::unique_ptr<SplitThreadPoolToken> _tokenless;

    const UniqueId _id {UniqueId::gen_uid()};

    std::shared_ptr<MetricEntity> _metric_entity;
    IntGauge* thread_pool_active_threads = nullptr;
    IntGauge* thread_pool_queue_size = nullptr;
    IntGauge* thread_pool_max_queue_size = nullptr;
    IntGauge* thread_pool_max_threads = nullptr;
    IntCounter* thread_pool_task_execution_time_ns_total = nullptr;
    IntCounter* thread_pool_task_execution_count_total = nullptr;
    IntCounter* thread_pool_task_wait_worker_time_ns_total = nullptr;
    IntCounter* thread_pool_task_wait_worker_count_total = nullptr;

    IntCounter* thread_pool_submit_failed = nullptr;

    bool _enable_concurrency_control = true;
};

// Entry point for token-based task submission and blocking for a particular
// thread pool. Tokens can only be created via ThreadPool::new_token().
//
// All functions are thread-safe. Mutable members are protected via the
// ThreadPool's lock.
class SplitThreadPoolToken {
public:
    // Destroys the token.
    //
    // May be called on a token with outstanding tasks, as Shutdown() will be
    // called first to take care of them.
    ~SplitThreadPoolToken();

    // Marks the token as unusable for future submissions. Any queued tasks not
    // yet running are destroyed. If tasks are in flight, Shutdown() will wait
    // on their completion before returning.
    void shutdown();

    // Waits until all the tasks submitted via this token are completed.
    void wait();

    // Waits for all submissions using this token are complete, or until 'delta'
    // time elapses.
    //
    // Returns true if all submissions are complete, false otherwise.
    template <class Rep, class Period>
    bool wait_for(const std::chrono::duration<Rep, Period>& delta) {
        std::unique_lock<std::mutex> l(_pool->_lock);
        _pool->check_not_pool_thread_unlocked();
        return _not_running_cond.wait_for(l, delta, [&]() { return !is_active(); });
    }

    bool need_dispatch();

    size_t num_tasks() {
        std::lock_guard<std::mutex> l(_pool->_lock);
        return _entries->size();
    }

    SplitThreadPoolToken(const SplitThreadPoolToken&) = delete;
    void operator=(const SplitThreadPoolToken&) = delete;

private:
    // All possible token states. Legal state transitions:
    //   IDLE      -> RUNNING: task is submitted via token
    //   IDLE      -> QUIESCED: token or pool is shut down
    //   RUNNING   -> IDLE: worker thread finishes executing a task and
    //                      there are no more tasks queued to the token
    //   RUNNING   -> QUIESCING: token or pool is shut down while worker thread
    //                           is executing a task
    //   RUNNING   -> QUIESCED: token or pool is shut down
    //   QUIESCING -> QUIESCED:  worker thread finishes executing a task
    //                           belonging to a shut down token or pool
    enum class State {
        // Token has no queued tasks.
        IDLE,

        // A worker thread is running one of the token's previously queued tasks.
        RUNNING,

        // No new tasks may be submitted to the token. A worker thread is still
        // running a previously queued task.
        QUIESCING,

        // No new tasks may be submitted to the token. There are no active tasks
        // either. At this state, the token may only be destroyed.
        QUIESCED,
    };

    // Writes a textual representation of the token state in 's' to 'o'.
    friend std::ostream& operator<<(std::ostream& o, SplitThreadPoolToken::State s);

    friend class TimeSharingTaskExecutor;

    // Returns a textual representation of 's' suitable for debugging.
    static const char* state_to_string(State s);

    // Constructs a new token.
    //
    // The token may not outlive its thread pool ('pool').
    SplitThreadPoolToken(TimeSharingTaskExecutor* pool, TimeSharingTaskExecutor::ExecutionMode mode,
                         std::shared_ptr<SplitQueue> split_queue, int max_concurrency = INT_MAX);

    // Changes this token's state to 'new_state' taking actions as needed.
    void transition(State new_state);

    // Returns true if this token has a task queued and ready to run, or if a
    // task belonging to this token is already running.
    bool is_active() const { return _state == State::RUNNING || _state == State::QUIESCING; }

    // Returns true if new tasks may be submitted to this token.
    bool may_submit_new_tasks() const {
        return _state != State::QUIESCING && _state != State::QUIESCED;
    }

    State state() const { return _state; }
    TimeSharingTaskExecutor::ExecutionMode mode() const { return _mode; }

    // Token's configured execution mode.
    TimeSharingTaskExecutor::ExecutionMode _mode;

    // Pointer to the token's thread pool.
    TimeSharingTaskExecutor* _pool = nullptr;

    // Token state machine.
    State _state;

    // Queued client tasks.
    std::shared_ptr<SplitQueue> _entries;

    // Condition variable for "token is idle". Waiters wake up when the token
    // transitions to IDLE or QUIESCED.
    std::condition_variable _not_running_cond;

    // Number of worker threads currently executing tasks belonging to this
    // token.
    int _active_threads;
    // The max number of tasks that can be ran concurrenlty. This is to limit
    // the concurrency of a thread pool token, and default is INT_MAX(no limited)
    int _max_concurrency;
    // Number of tasks which has been submitted to the thread pool's queue.
    int _num_submitted_tasks;
    // Number of tasks which has not been submitted to the thread pool's queue.
    int _num_unsubmitted_tasks;
};

} // namespace vectorized
} // namespace doris
