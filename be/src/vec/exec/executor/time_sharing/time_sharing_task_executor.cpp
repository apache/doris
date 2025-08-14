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

#include "vec/exec/executor/time_sharing/time_sharing_task_executor.h"

#include <functional>
#include <iostream>
#include <sstream>
#include <thread>
#include <vector>

#include "common/exception.h"
#include "util/debug_points.h"
#include "util/defer_op.h"
#include "util/doris_metrics.h"
#include "util/thread.h"
#include "util/threadpool.h"
#include "util/uid_util.h"
#include "vec/exec/executor/time_sharing/multilevel_split_queue.h"
#include "vec/exec/executor/time_sharing/time_sharing_task_handle.h"

namespace doris {
namespace vectorized {

// The name of these varialbs will be useds as metric name in prometheus.
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(split_thread_pool_active_threads, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(split_thread_pool_queue_size, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(split_thread_pool_max_queue_size, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(split_thread_pool_max_threads, MetricUnit::NOUNIT);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(split_thread_pool_submit_failed, MetricUnit::NOUNIT);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(split_thread_pool_task_execution_time_ns_total,
                                     MetricUnit::NANOSECONDS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(split_thread_pool_task_execution_count_total,
                                     MetricUnit::NOUNIT);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(split_thread_pool_task_wait_worker_time_ns_total,
                                     MetricUnit::NANOSECONDS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(split_thread_pool_task_wait_worker_count_total,
                                     MetricUnit::NOUNIT);

SplitThreadPoolToken::SplitThreadPoolToken(TimeSharingTaskExecutor* pool,
                                           TimeSharingTaskExecutor::ExecutionMode mode,
                                           std::shared_ptr<SplitQueue> split_queue,
                                           int max_concurrency)
        : _mode(mode),
          _pool(pool),
          _state(State::IDLE),
          _entries(std::move(split_queue)),
          _active_threads(0),
          _max_concurrency(max_concurrency),
          _num_submitted_tasks(0),
          _num_unsubmitted_tasks(0) {
    if (max_concurrency == 1 && mode != TimeSharingTaskExecutor::ExecutionMode::SERIAL) {
        _mode = TimeSharingTaskExecutor::ExecutionMode::SERIAL;
    }
}

SplitThreadPoolToken::~SplitThreadPoolToken() {
    shutdown();
}

void SplitThreadPoolToken::shutdown() {
    std::unique_lock<std::mutex> l(_pool->_lock);
    _pool->check_not_pool_thread_unlocked();

    // Clear the queue under the lock, but defer the releasing of the tasks
    // outside the lock, in case there are concurrent threads wanting to access
    // the ThreadPool. The task's destructors may acquire locks, etc, so this
    // also prevents lock inversions.
    _pool->_total_queued_tasks -= _entries->size();

    switch (state()) {
    case State::IDLE:
        // There were no tasks outstanding; we can quiesce the token immediately.
        transition(State::QUIESCED);
        break;
    case State::RUNNING:
        // There were outstanding tasks. If any are still running, switch to
        // QUIESCING and wait for them to finish (the worker thread executing
        // the token's last task will switch the token to QUIESCED). Otherwise,
        // we can quiesce the token immediately.

        _pool->_tokenless->_entries->clear();

        if (_active_threads == 0) {
            transition(State::QUIESCED);
            break;
        }
        transition(State::QUIESCING);
        [[fallthrough]];
    case State::QUIESCING:
        // The token is already quiescing. Just wait for a worker thread to
        // switch it to QUIESCED.
        _not_running_cond.wait(l, [this]() { return state() == State::QUIESCED; });
        break;
    default:
        break;
    }
}

void SplitThreadPoolToken::wait() {
    std::unique_lock<std::mutex> l(_pool->_lock);
    _pool->check_not_pool_thread_unlocked();
    _not_running_cond.wait(l, [this]() { return !is_active(); });
}

void SplitThreadPoolToken::transition(State new_state) {
#ifndef NDEBUG
    CHECK_NE(_state, new_state);

    switch (_state) {
    case State::IDLE:
        CHECK(new_state == State::RUNNING || new_state == State::QUIESCED);
        if (new_state == State::RUNNING) {
            CHECK(_entries->size() > 0);
        } else {
            CHECK(_entries->size() == 0);
            CHECK_EQ(_active_threads, 0);
        }
        break;
    case State::RUNNING:
        CHECK(new_state == State::IDLE || new_state == State::QUIESCING ||
              new_state == State::QUIESCED);
        CHECK(_entries->size() == 0);
        if (new_state == State::QUIESCING) {
            CHECK_GT(_active_threads, 0);
        }
        break;
    case State::QUIESCING:
        CHECK(new_state == State::QUIESCED);
        CHECK_EQ(_active_threads, 0);
        break;
    case State::QUIESCED:
        CHECK(false); // QUIESCED is a terminal state
        break;
    default:
        throw doris::Exception(Status::FatalError("Unknown token state: {}", _state));
    }
#endif

    // Take actions based on the state we're entering.
    switch (new_state) {
    case State::IDLE:
    case State::QUIESCED:
        _not_running_cond.notify_all();
        break;
    default:
        break;
    }

    _state = new_state;
}

const char* SplitThreadPoolToken::state_to_string(State s) {
    switch (s) {
    case State::IDLE:
        return "IDLE";
        break;
    case State::RUNNING:
        return "RUNNING";
        break;
    case State::QUIESCING:
        return "QUIESCING";
        break;
    case State::QUIESCED:
        return "QUIESCED";
        break;
    }
    return "<cannot reach here>";
}

bool SplitThreadPoolToken::need_dispatch() {
    return _state == SplitThreadPoolToken::State::IDLE ||
           (_mode == TimeSharingTaskExecutor::ExecutionMode::CONCURRENT &&
            _num_submitted_tasks < _max_concurrency);
}

TimeSharingTaskExecutor::TimeSharingTaskExecutor(ThreadConfig thread_config, int min_concurrency,
                                                 int guaranteed_concurrency_per_task,
                                                 int max_concurrency_per_task,
                                                 std::shared_ptr<Ticker> ticker,
                                                 std::shared_ptr<SplitQueue> split_queue,
                                                 bool enable_concurrency_control)
        : _thread_name(thread_config.thread_name),
          _workload_group(thread_config.workload_group),
          _min_threads(thread_config.min_thread_num),
          _max_threads(thread_config.max_thread_num),
          _max_queue_size(thread_config.max_queue_size),
          _cgroup_cpu_ctl(thread_config.cgroup_cpu_ctl),
          _min_concurrency(min_concurrency),
          _guaranteed_concurrency_per_task(guaranteed_concurrency_per_task),
          _max_concurrency_per_task(max_concurrency_per_task),
          _ticker(ticker != nullptr ? ticker : std::make_shared<SystemTicker>()),
          _tokenless(new SplitThreadPoolToken(this, ExecutionMode::CONCURRENT,
                                              split_queue != nullptr
                                                      ? std::move(split_queue)
                                                      : std::make_shared<MultilevelSplitQueue>(2),
                                              INT_MAX)),
          _enable_concurrency_control(enable_concurrency_control) {}

Status TimeSharingTaskExecutor::init() {
    if (!_pool_status.is<ErrorCode::UNINITIALIZED>()) {
        return Status::NotSupported("The thread pool {} is already initialized", _thread_name);
    }
    _pool_status = Status::OK();

    {
        std::lock_guard<std::mutex> l(_lock);
        // create thread failed should not cause SplitThreadPool init failed,
        // because thread can be created later such as when submit a task.
        static_cast<void>(_try_create_thread(_min_threads, l));
    }

    // _id of thread pool is used to make sure when we create thread pool with same name, we can
    // get different _metric_entity
    // If not, we will have problem when we deregister entity and register hook.
    _metric_entity = DorisMetrics::instance()->metric_registry()->register_entity(
            fmt::format("thread_pool_{}", _thread_name), {{"thread_pool_name", _thread_name},
                                                          {"workload_group", _workload_group},
                                                          {"id", _id.to_string()}});

    INT_GAUGE_METRIC_REGISTER(_metric_entity, split_thread_pool_active_threads);
    INT_GAUGE_METRIC_REGISTER(_metric_entity, split_thread_pool_max_threads);
    INT_GAUGE_METRIC_REGISTER(_metric_entity, split_thread_pool_queue_size);
    INT_GAUGE_METRIC_REGISTER(_metric_entity, split_thread_pool_max_queue_size);
    INT_COUNTER_METRIC_REGISTER(_metric_entity, split_thread_pool_task_execution_time_ns_total);
    INT_COUNTER_METRIC_REGISTER(_metric_entity, split_thread_pool_task_execution_count_total);
    INT_COUNTER_METRIC_REGISTER(_metric_entity, split_thread_pool_task_wait_worker_time_ns_total);
    INT_COUNTER_METRIC_REGISTER(_metric_entity, split_thread_pool_task_wait_worker_count_total);
    INT_COUNTER_METRIC_REGISTER(_metric_entity, split_thread_pool_submit_failed);

    _metric_entity->register_hook("update", [this]() {
        {
            std::lock_guard<std::mutex> l(_lock);
            if (!_pool_status.ok()) {
                return;
            }
        }

        split_thread_pool_active_threads->set_value(num_active_threads());
        split_thread_pool_queue_size->set_value(get_queue_size());
        split_thread_pool_max_queue_size->set_value(get_max_queue_size());
        split_thread_pool_max_threads->set_value(max_threads());
    });
    return Status::OK();
}

TimeSharingTaskExecutor::~TimeSharingTaskExecutor() {
    if (!_stopped.exchange(true)) {
        stop();
    }

    std::vector<std::shared_ptr<PrioritizedSplitRunner>> splits_to_destroy;
    {
        {
            std::unique_lock<std::mutex> lock(_mutex);
            for (auto& [task_id, handle] : _tasks) {
                if (handle->is_closed()) {
                    LOG(WARNING) << "task is already destroyed, task_id: " << task_id.to_string();
                    continue;
                }
                auto task_splits = handle->close();
                splits_to_destroy.insert(splits_to_destroy.end(),
                                         std::make_move_iterator(task_splits.begin()),
                                         std::make_move_iterator(task_splits.end()));
            }
        }
        {
            std::unique_lock<std::mutex> l(_lock);
            _tokenless->_entries->remove_all(splits_to_destroy);
        }
    }

    if (splits_to_destroy.empty()) {
        return;
    }

    for (auto& split : splits_to_destroy) {
        split->close(Status::OK());
    }
}

Status TimeSharingTaskExecutor::start() {
    return Status::OK();
}

void TimeSharingTaskExecutor::stop() {
    // Why access to doris_metrics is safe here?
    // Since DorisMetrics is a singleton, it will be destroyed only after doris_main is exited.
    // The shutdown/destroy of SplitThreadPool is guaranteed to take place before doris_main exits by
    // ExecEnv::destroy().
    DorisMetrics::instance()->metric_registry()->deregister_entity(_metric_entity);
    std::unique_lock<std::mutex> l(_lock);
    check_not_pool_thread_unlocked();

    // Note: this is the same error seen at submission if the pool is at
    // capacity, so clients can't tell them apart. This isn't really a practical
    // concern though because shutting down a pool typically requires clients to
    // be quiesced first, so there's no danger of a client getting confused.
    // Not print stack trace here
    _pool_status = Status::Error<ErrorCode::SERVICE_UNAVAILABLE, false>(
            "The thread pool {} has been shut down.", _thread_name);

    // Clear the various queues under the lock, but defer the releasing
    // of the tasks outside the lock, in case there are concurrent threads
    // wanting to access the SplitThreadPool. The task's destructors may acquire
    // locks, etc, so this also prevents lock inversions.

    std::deque<std::shared_ptr<SplitQueue>> to_release;
    if (_tokenless->_entries->size() > 0) {
        to_release.emplace_back(_tokenless->_entries);
        _tokenless->_entries->clear();
    }
    switch (_tokenless->state()) {
    case SplitThreadPoolToken::State::IDLE:
        // The token is idle; we can quiesce it immediately.
        _tokenless->transition(SplitThreadPoolToken::State::QUIESCED);
        break;
    case SplitThreadPoolToken::State::RUNNING:
        // The token has tasks associated with it. If they're merely queued
        // (i.e. there are no active threads), the tasks will have been removed
        // above and we can quiesce immediately. Otherwise, we need to wait for
        // the threads to finish.
        _tokenless->transition(_tokenless->_active_threads > 0
                                       ? SplitThreadPoolToken::State::QUIESCING
                                       : SplitThreadPoolToken::State::QUIESCED);
        break;
    default:
        break;
    }

    // The queues are empty. Wake any sleeping worker threads and wait for all
    // of them to exit. Some worker threads will exit immediately upon waking,
    // while others will exit after they finish executing an outstanding task.
    _total_queued_tasks = 0;
    while (!_idle_threads.empty()) {
        _idle_threads.front().not_empty.notify_one();
        _idle_threads.pop_front();
    }

    _no_threads_cond.wait(l, [this]() { return _num_threads + _num_threads_pending_start == 0; });

    // All the threads have exited. Check the state of each token.
    DCHECK(_tokenless->state() == SplitThreadPoolToken::State::IDLE ||
           _tokenless->state() == SplitThreadPoolToken::State::QUIESCED);
}

Status TimeSharingTaskExecutor::_try_create_thread(int thread_num, std::lock_guard<std::mutex>&) {
    for (int i = 0; i < thread_num; i++) {
        Status status = _create_thread();
        if (status.ok()) {
            _num_threads_pending_start++;
        } else {
            LOG(WARNING) << "Thread pool " << _thread_name
                         << " failed to create thread: " << status;
            return status;
        }
    }
    return Status::OK();
}

Status TimeSharingTaskExecutor::_do_submit(std::shared_ptr<PrioritizedSplitRunner> split) {
    std::unique_lock<std::mutex> l(_lock);
    if (!_pool_status.ok()) [[unlikely]] {
        return _pool_status;
    }

    if (!_tokenless->may_submit_new_tasks()) [[unlikely]] {
        return Status::Error<ErrorCode::SERVICE_UNAVAILABLE>("Thread pool({}) was shut down",
                                                             _thread_name);
    }

    // Size limit check.
    int64_t capacity_remaining = static_cast<int64_t>(_max_threads) - _active_threads +
                                 static_cast<int64_t>(_max_queue_size) - _total_queued_tasks;
    if (capacity_remaining < 1) {
        split_thread_pool_submit_failed->increment(1);
        return Status::Error<ErrorCode::SERVICE_UNAVAILABLE>(
                "Thread pool {} is at capacity ({}/{} tasks running, {}/{} tasks queued)",
                _thread_name, _num_threads + _num_threads_pending_start, _max_threads,
                _total_queued_tasks, _max_queue_size);
    }

    // Should we create another thread?

    // We assume that each current inactive thread will grab one item from the
    // queue.  If it seems like we'll need another thread, we create one.
    //
    // Rather than creating the thread here, while holding the lock, we defer
    // it to down below. This is because thread creation can be rather slow
    // (hundreds of milliseconds in some cases) and we'd like to allow the
    // existing threads to continue to process tasks while we do so.
    //
    // In theory, a currently active thread could finish immediately after this
    // calculation but before our new worker starts running. This would mean we
    // created a thread we didn't really need. However, this race is unavoidable
    // and harmless.
    //
    // Of course, we never create more than _max_threads threads no matter what.
    int threads_from_this_submit = 1;
    int inactive_threads = _num_threads + _num_threads_pending_start - _active_threads;
    int additional_threads = static_cast<int>(_tokenless->_entries->size()) +
                             threads_from_this_submit - inactive_threads;
    bool need_a_thread = false;
    if (additional_threads > 0 && _num_threads + _num_threads_pending_start < _max_threads) {
        need_a_thread = true;
        _num_threads_pending_start++;
    }

    // Add the split to the queue.
    SplitThreadPoolToken::State state = _tokenless->state();
    DCHECK(state == SplitThreadPoolToken::State::IDLE ||
           state == SplitThreadPoolToken::State::RUNNING);
    split->submit_time_watch().start();
    _tokenless->_entries->offer(std::move(split));
    if (state == SplitThreadPoolToken::State::IDLE) {
        _tokenless->transition(SplitThreadPoolToken::State::RUNNING);
    }
    // When we need to execute the task in the token, we submit the token object to the queue.
    // There are currently two places where tokens will be submitted to the queue:
    // 1. When submitting a new task, if the token is still in the IDLE state,
    //    or the concurrency of the token has not reached the online level, it will be added to the queue.
    // 2. When the dispatch thread finishes executing a task:
    //    1. If it is a SERIAL token, and there are unsubmitted tasks, submit them to the queue.
    //    2. If it is a CONCURRENT token, and there are still unsubmitted tasks, and the upper limit of concurrency is not reached,
    //       then submitted to the queue.
    _total_queued_tasks++;

    // Wake up an idle thread for this task. Choosing the thread at the front of
    // the list ensures LIFO semantics as idling threads are also added to the front.
    //
    // If there are no idle threads, the new task remains on the queue and is
    // processed by an active thread (or a thread we're about to create) at some
    // point in the future.
    if (!_idle_threads.empty()) {
        _idle_threads.front().not_empty.notify_one();
        _idle_threads.pop_front();
    }
    l.unlock();

    if (need_a_thread) {
        Status status = _create_thread();
        if (!status.ok()) {
            l.lock();
            _num_threads_pending_start--;
            if (_num_threads + _num_threads_pending_start == 0) {
                // If we have no threads, we can't do any work.
                return status;
            }
            // If we failed to create a thread, but there are still some other
            // worker threads, log a warning message and continue.
            LOG(WARNING) << "Thread pool " << _thread_name
                         << " failed to create thread: " << status.to_string();
        }
    }

    return Status::OK();
}

void TimeSharingTaskExecutor::wait() {
    std::unique_lock<std::mutex> l(_lock);
    check_not_pool_thread_unlocked();
    _idle_cond.wait(l, [this]() { return _total_queued_tasks == 0 && _active_threads == 0; });
}

void TimeSharingTaskExecutor::_dispatch_thread() {
    std::unique_lock<std::mutex> l(_lock);
    if (!_threads.insert(Thread::current_thread()).second) {
        throw doris::Exception(Status::InternalError("duplicate token"));
    }
    DCHECK_GT(_num_threads_pending_start, 0);
    _num_threads++;
    _num_threads_pending_start--;

    if (std::shared_ptr<CgroupCpuCtl> cg_cpu_ctl_sptr = _cgroup_cpu_ctl.lock()) {
        static_cast<void>(cg_cpu_ctl_sptr->add_thread_to_cgroup());
    }

    // Owned by this worker thread and added/removed from _idle_threads as needed.
    IdleThread me;

    while (true) {
        // Note: Status::Aborted() is used to indicate normal shutdown.
        if (!_pool_status.ok()) {
            VLOG_CRITICAL << "DispatchThread exiting: " << _pool_status.to_string();
            break;
        }

        if (_num_threads + _num_threads_pending_start > _max_threads) {
            break;
        }

        if (_tokenless->_entries->size() == 0) {
            // There's no work to do, let's go idle.
            //
            // Note: if FIFO behavior is desired, it's as simple as changing this to push_back().
            _idle_threads.push_front(me);
            Defer defer = [&] {
                // For some wake ups (i.e. shutdown or _do_submit) this thread is
                // guaranteed to be unlinked after being awakened. In others (i.e.
                // spurious wake-up or Wait timeout), it'll still be linked.
                if (me.is_linked()) {
                    _idle_threads.erase(_idle_threads.iterator_to(me));
                }
            };
            if (me.not_empty.wait_for(l, _idle_timeout) == std::cv_status::timeout) {
                // After much investigation, it appears that pthread condition variables have
                // a weird behavior in which they can return ETIMEDOUT from timed_wait even if
                // another thread did in fact signal. Apparently after a timeout there is some
                // brief period during which another thread may actually grab the internal mutex
                // protecting the state, signal, and release again before we get the mutex. So,
                // we'll recheck the empty queue case regardless.
                if (_tokenless->_entries->size() == 0 &&
                    _num_threads + _num_threads_pending_start > _min_threads) {
                    VLOG_NOTICE << "Releasing worker thread from pool " << _thread_name << " after "
                                << std::chrono::duration_cast<std::chrono::milliseconds>(
                                           _idle_timeout)
                                           .count()
                                << "ms of idle time.";
                    break;
                }
            }
            continue;
        }

        MonotonicStopWatch task_execution_time_watch;
        task_execution_time_watch.start();
        // // Get the next token and task to execute.
        DCHECK_EQ(SplitThreadPoolToken::State::RUNNING, _tokenless->state());
        DCHECK(_tokenless->_entries->size() > 0);
        std::shared_ptr<PrioritizedSplitRunner> split = _tokenless->_entries->take();
        split_thread_pool_task_wait_worker_time_ns_total->increment(
                split->submit_time_watch().elapsed_time());
        split_thread_pool_task_wait_worker_count_total->increment(1);
        _tokenless->_active_threads++;
        --_total_queued_tasks;
        ++_active_threads;
        l.unlock();

        // Execute the task
        {
            std::lock_guard<std::mutex> guard(_mutex);
            _running_splits.insert(split);
        }
        Defer defer {[&]() {
            std::lock_guard<std::mutex> guard(_mutex);
            _running_splits.erase(split);
        }};

        Result<SharedListenableFuture<Void>> blocked_future_result = split->process();

        if (!blocked_future_result.has_value()) {
            LOG(WARNING) << "split process failed, split_id: " << split->split_id()
                         << ", status: " << blocked_future_result.error();
            _split_finished(split, blocked_future_result.error());
        } else {
            auto blocked_future = blocked_future_result.value();

            if (split->is_finished()) {
                {
                    std::ostringstream _oss;
                    _oss << std::this_thread::get_id();
                }
                _split_finished(split, split->finished_status());
            } else {
                if (split->is_auto_reschedule()) {
                    std::unique_lock<std::mutex> lock(_mutex);
                    if (blocked_future.is_done()) {
                        lock.unlock();
                        l.lock();
                        if (_tokenless->state() == SplitThreadPoolToken::State::RUNNING) {
                            _tokenless->_entries->offer(split);
                        }
                        l.unlock();
                    } else {
                        _blocked_splits[split] = blocked_future;

                        _blocked_splits[split].add_callback([this, split](const Void& value,
                                                                          const Status& status) {
                            if (status.ok()) {
                                {
                                    std::unique_lock<std::mutex> lock(_mutex);
                                    _blocked_splits.erase(split);
                                }
                                split->reset_level_priority();
                                std::unique_lock<std::mutex> l(_lock);
                                if (_tokenless->state() == SplitThreadPoolToken::State::RUNNING) {
                                    _tokenless->_entries->offer(split);
                                }
                            } else {
                                LOG(WARNING) << "blocked split is failed, split_id: "
                                             << split->split_id() << ", status: " << status;
                                _split_finished(split, status);
                            }
                        });
                    }
                }
            }
        }

        // Destruct the task while we do not hold the lock.
        //
        // The task's destructor may be expensive if it has a lot of bound
        // objects, and we don't want to block submission of the SplitThreadPool.
        // In the worst case, the destructor might even try to do something
        // with this SplitThreadPool, and produce a deadlock.
        // task.runnable.reset();
        l.lock();
        split_thread_pool_task_execution_time_ns_total->increment(
                task_execution_time_watch.elapsed_time());
        split_thread_pool_task_execution_count_total->increment(1);
        // Possible states:
        // 1. The token was shut down while we ran its task. Transition to QUIESCED.
        // 2. The token has no more queued tasks. Transition back to IDLE.
        // 3. The token has more tasks. Requeue it and transition back to RUNNABLE.
        SplitThreadPoolToken::State state = _tokenless->state();
        DCHECK(state == SplitThreadPoolToken::State::RUNNING ||
               state == SplitThreadPoolToken::State::QUIESCING);
        --_tokenless->_active_threads;
        --_tokenless->_num_submitted_tasks;

        // handle shutdown && idle
        if (_tokenless->_active_threads == 0) {
            if (state == SplitThreadPoolToken::State::QUIESCING) {
                DCHECK(_tokenless->_entries->size() == 0);
                _tokenless->transition(SplitThreadPoolToken::State::QUIESCED);
            } else if (_tokenless->_entries->size() == 0) {
                _tokenless->transition(SplitThreadPoolToken::State::IDLE);
            }
        }

        // We decrease _num_submitted_tasks holding lock, so the following DCHECK works.
        DCHECK(_tokenless->_num_submitted_tasks < _tokenless->_max_concurrency);

        if (--_active_threads == 0) {
            _idle_cond.notify_all();
        }
    }

    // It's important that we hold the lock between exiting the loop and dropping
    // _num_threads. Otherwise it's possible someone else could come along here
    // and add a new task just as the last running thread is about to exit.
    CHECK(l.owns_lock());

    CHECK_EQ(_threads.erase(Thread::current_thread()), 1);
    _num_threads--;
    if (_num_threads + _num_threads_pending_start == 0) {
        _no_threads_cond.notify_all();

        // Sanity check: if we're the last thread exiting, the queue ought to be
        // empty. Otherwise it will never get processed.
        CHECK(_tokenless->_entries->size() == 0);
        DCHECK_EQ(0, _total_queued_tasks);
    }
}

Status TimeSharingTaskExecutor::_create_thread() {
    return Thread::create("thread pool", fmt::format("{} [worker]", _thread_name),
                          &TimeSharingTaskExecutor::_dispatch_thread, this, nullptr);
}

void TimeSharingTaskExecutor::check_not_pool_thread_unlocked() {
    Thread* current = Thread::current_thread();
    if (_threads.contains(current)) {
        throw doris::Exception(
                Status::FatalError("Thread belonging to thread pool {} with "
                                   "name {} called pool function that would result in deadlock",
                                   _thread_name, current->name()));
    }
}

Status TimeSharingTaskExecutor::set_min_threads(int min_threads) {
    std::lock_guard<std::mutex> l(_lock);
    if (min_threads > _max_threads) {
        // min threads can not be set greater than max threads
        return Status::InternalError("set thread pool {} min_threads failed", _thread_name);
    }
    _min_threads = min_threads;
    if (min_threads > _num_threads + _num_threads_pending_start) {
        int addition_threads = min_threads - _num_threads - _num_threads_pending_start;
        RETURN_IF_ERROR(_try_create_thread(addition_threads, l));
    }
    return Status::OK();
}

Status TimeSharingTaskExecutor::set_max_threads(int max_threads) {
    std::lock_guard<std::mutex> l(_lock);
    DBUG_EXECUTE_IF("SplitThreadPool.set_max_threads.force_set", {
        _max_threads = max_threads;
        return Status::OK();
    })
    if (_min_threads > max_threads) {
        // max threads can not be set less than min threads
        return Status::InternalError("set thread pool {} max_threads failed", _thread_name);
    }

    _max_threads = max_threads;
    if (_max_threads > _num_threads + _num_threads_pending_start) {
        int addition_threads = _max_threads - _num_threads - _num_threads_pending_start;
        addition_threads = std::min(addition_threads, _total_queued_tasks);
        RETURN_IF_ERROR(_try_create_thread(addition_threads, l));
    }
    return Status::OK();
}

std::ostream& operator<<(std::ostream& o, SplitThreadPoolToken::State s) {
    return o << SplitThreadPoolToken::state_to_string(s);
}

Result<std::shared_ptr<TaskHandle>> TimeSharingTaskExecutor::create_task(
        const TaskId& task_id, std::function<double()> utilization_supplier,
        int initial_split_concurrency, std::chrono::nanoseconds split_concurrency_adjust_frequency,
        std::optional<int> max_concurrency_per_task) {
    auto task_handle = std::make_shared<TimeSharingTaskHandle>(
            task_id, _tokenless->_entries, utilization_supplier, initial_split_concurrency,
            split_concurrency_adjust_frequency, max_concurrency_per_task);
    RETURN_IF_ERROR_RESULT(task_handle->init());

    std::lock_guard<std::mutex> lock(_mutex);

    _tasks[task_id] = task_handle;

    return task_handle;
}

Status TimeSharingTaskExecutor::add_task(const TaskId& task_id,
                                         std::shared_ptr<TaskHandle> task_handle) {
    std::lock_guard<std::mutex> lock(_mutex);
    _tasks[task_id] =
            std::dynamic_pointer_cast<doris::vectorized::TimeSharingTaskHandle>(task_handle);
    return Status::OK();
}

Status TimeSharingTaskExecutor::remove_task(std::shared_ptr<TaskHandle> task_handle) {
    auto handle = std::dynamic_pointer_cast<TimeSharingTaskHandle>(task_handle);
    std::vector<std::shared_ptr<PrioritizedSplitRunner>> splits_to_destroy;

    {
        {
            std::unique_lock<std::mutex> lock(_mutex);
            auto it = _tasks.find(handle->task_id());
            if (it == _tasks.end() || handle->is_closed()) {
                return Status::OK();
            }
            _tasks.erase(it);

            // Task is already closed
            if (task_handle->is_closed()) {
                return Status::OK();
            }

            splits_to_destroy = handle->close();

            for (const auto& split : splits_to_destroy) {
                _all_splits.erase(split);
                _intermediate_splits.erase(split);
                _blocked_splits.erase(split);
            }
        }
        {
            std::unique_lock<std::mutex> l(_lock);
            _tokenless->_entries->remove_all(splits_to_destroy);
        }
    }

    // call destroy outside of synchronized block as it is expensive and doesn't need a lock on the task executor
    for (auto& split : splits_to_destroy) {
        split->close(Status::OK());
    }

    // record completed stats
    int64_t thread_usage_nanos = handle->scheduled_nanos();
    int level = _tokenless->_entries->compute_level(thread_usage_nanos);
    _completed_tasks_per_level[level]++;

    if (splits_to_destroy.empty()) {
        return Status::OK();
    }

    // replace blocked splits that were terminated
    {
        std::unique_lock<std::mutex> lock(_mutex);
        _add_new_entrants(lock);
    }
    return Status::OK();
}

Result<std::vector<SharedListenableFuture<Void>>> TimeSharingTaskExecutor::enqueue_splits(
        std::shared_ptr<TaskHandle> task_handle, bool intermediate,
        const std::vector<std::shared_ptr<SplitRunner>>& splits) {
    std::vector<std::shared_ptr<PrioritizedSplitRunner>> splits_to_destroy;
    Defer defer {[&]() {
        for (auto& split : splits_to_destroy) {
            split->close(Status::OK());
        }
    }};
    std::vector<SharedListenableFuture<Void>> finished_futures;
    auto handle = std::dynamic_pointer_cast<TimeSharingTaskHandle>(task_handle);
    {
        std::unique_lock<std::mutex> lock(_mutex);
        for (const auto& task_split : splits) {
            TaskId task_id = handle->task_id();
            int split_id = handle->next_split_id();

            auto prioritized_split =
                    PrioritizedSplitRunner::create_shared(handle, split_id, task_split, _ticker);
            RETURN_IF_ERROR_RESULT(prioritized_split->init());
            if (intermediate) {
                if (handle->record_intermediate_split(prioritized_split)) {
                    _start_intermediate_split(prioritized_split, lock);
                } else {
                    splits_to_destroy.push_back(prioritized_split);
                }
            } else {
                if (handle->enqueue_split(prioritized_split)) {
                    _schedule_task_if_necessary(handle, lock);
                    _add_new_entrants(lock);
                } else {
                    splits_to_destroy.push_back(prioritized_split);
                }
            }
            finished_futures.push_back(prioritized_split->finished_future());
        }
    }
    return finished_futures;
}

Status TimeSharingTaskExecutor::re_enqueue_split(std::shared_ptr<TaskHandle> task_handle,
                                                 bool intermediate,
                                                 const std::shared_ptr<SplitRunner>& split) {
    auto handle = std::dynamic_pointer_cast<TimeSharingTaskHandle>(task_handle);
    std::shared_ptr<PrioritizedSplitRunner> prioritized_split =
            handle->get_split(split, intermediate);
    prioritized_split->reset_level_priority();
    return _do_submit(prioritized_split);
}

void TimeSharingTaskExecutor::_split_finished(std::shared_ptr<PrioritizedSplitRunner> split,
                                              const Status& status) {
    _completed_splits_per_level[split->priority().level()]++;
    {
        std::unique_lock<std::mutex> lock(_mutex);
        _all_splits.erase(split);

        auto task_handle = split->task_handle();
        task_handle->split_finished(split);

        _schedule_task_if_necessary(task_handle, lock);

        _add_new_entrants(lock);
    }
    // call close outside of synchronized block as it is expensive and doesn't need a lock on the task executor
    split->close(status);
}

void TimeSharingTaskExecutor::_schedule_task_if_necessary(
        std::shared_ptr<TimeSharingTaskHandle> task_handle, std::unique_lock<std::mutex>& lock) {
    int guaranteed_concurrency = std::min(
            _guaranteed_concurrency_per_task,
            task_handle->max_concurrency_per_task().value_or(std::numeric_limits<int>::max()));
    int splits_to_schedule = guaranteed_concurrency - task_handle->running_leaf_splits();
    int max_loops =
            !_enable_concurrency_control ? std::numeric_limits<int>::max() : splits_to_schedule;
    for (int i = 0; i < max_loops; ++i) {
        auto split = task_handle->poll_next_split();
        if (!split) {
            break;
        }
        _start_split(split, lock);
    }
}

void TimeSharingTaskExecutor::_add_new_entrants(std::unique_lock<std::mutex>& lock) {
    int running = _all_splits.size() - _intermediate_splits.size();
    int max_loops = !_enable_concurrency_control ? std::numeric_limits<int>::max()
                                                 : (_min_concurrency - running);
    for (int i = 0; i < max_loops; ++i) {
        auto split = _poll_next_split_worker(lock);
        if (!split) {
            break;
        }
        _start_split(split, lock);
    }
}

void TimeSharingTaskExecutor::_start_intermediate_split(
        std::shared_ptr<PrioritizedSplitRunner> split, std::unique_lock<std::mutex>& lock) {
    _start_split(split, lock);
    _intermediate_splits.insert(split);
}

void TimeSharingTaskExecutor::_start_split(std::shared_ptr<PrioritizedSplitRunner> split,
                                           std::unique_lock<std::mutex>& lock) {
    _all_splits.insert(split);
    lock.unlock();
    Status submit_status = _do_submit(split);
    if (!submit_status.ok()) {
        LOG(WARNING) << "_do_submit failed for split_id: " << split->split_id()
                     << ", status: " << submit_status.to_string();
    }
    lock.lock();
}

std::shared_ptr<PrioritizedSplitRunner> TimeSharingTaskExecutor::_poll_next_split_worker(
        std::unique_lock<std::mutex>& lock) {
    for (auto it = _tasks.begin(); it != _tasks.end();) {
        auto task = it->second;
        if (task->running_leaf_splits() >=
            task->max_concurrency_per_task().value_or(_max_concurrency_per_task)) {
            ++it;
            continue;
        }

        auto split = task->poll_next_split();
        if (split) {
            auto task_copy = task;
            auto task_id = it->first;
            it = _tasks.erase(it);
            _tasks[task_id] = task_copy;
            return split;
        }
        ++it;
    }
    return nullptr;
}

int64_t TimeSharingTaskExecutor::_get_running_tasks_for_level(int level) const {
    std::lock_guard<std::mutex> guard(_mutex);
    int64_t count = 0;
    for (const auto& [task_id, task] : _tasks) {
        if (task->priority().level() == level) {
            count++;
        }
    }
    return count;
}

size_t TimeSharingTaskExecutor::waiting_splits_size() const {
    return _tokenless->num_tasks();
}

} // namespace vectorized
} // namespace doris
