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

#ifndef DORIS_BE_SRC_UTIL_THREAD_POOL_H
#define DORIS_BE_SRC_UTIL_THREAD_POOL_H

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/list_hook.hpp>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>

#include "common/status.h"
#include "gutil/ref_counted.h"
#include "util/condition_variable.h"
#include "util/monotime.h"
#include "util/mutex.h"

namespace doris {

class Thread;
class ThreadPool;
class ThreadPoolToken;

class Runnable {
public:
    virtual void run() = 0;
    virtual ~Runnable() {}
};

// ThreadPool takes a lot of arguments. We provide sane defaults with a builder.
//
// name: Used for debugging output and default names of the worker threads.
//    Since thread names are limited to 16 characters on Linux, it's good to
//    choose a short name here.
//    Required.
//
// trace_metric_prefix: used to prefix the names of TraceMetric counters.
//    When a task on a thread pool has an associated trace, the thread pool
//    implementation will increment TraceMetric counters to indicate the
//    amount of time spent waiting in the queue as well as the amount of wall
//    and CPU time spent executing. By default, these counters are prefixed
//    with the name of the thread pool. For example, if the pool is named
//    'apply', then counters such as 'apply.queue_time_us' will be
//    incremented.
//
//    The TraceMetrics implementation relies on the number of distinct counter
//    names being small. Thus, if the thread pool name itself is dynamically
//    generated, the default behavior described above would result in an
//    unbounded number of distinct counter names. The 'trace_metric_prefix'
//    setting can be used to override the prefix used in generating the trace
//    metric names.
//
//    For example, the Raft thread pools are named "<tablet id>-raft" which
//    has unbounded cardinality (a server may have thousands of different
//    tablet IDs over its lifetime). In that case, setting the prefix to
//    "raft" will avoid any issues.
//
// min_threads: Minimum number of threads we'll have at any time.
//    Default: 0.
//
// max_threads: Maximum number of threads we'll have at any time.
//    Default: Number of CPUs detected on the system.
//
// max_queue_size: Maximum number of items to enqueue before returning a
//    Status::ServiceUnavailable message from Submit().
//    Default: INT_MAX.
//
// idle_timeout: How long we'll keep around an idle thread before timing it out.
//    We always keep at least min_threads.
//    Default: 500 milliseconds.
//
// metrics: Histograms, counters, etc. to update on various threadpool events.
//    Default: not set.
//
class ThreadPoolBuilder {
public:
    explicit ThreadPoolBuilder(std::string name);

    // Note: We violate the style guide by returning mutable references here
    // in order to provide traditional Builder pattern conveniences.
    ThreadPoolBuilder& set_min_threads(int min_threads);
    ThreadPoolBuilder& set_max_threads(int max_threads);
    ThreadPoolBuilder& set_max_queue_size(int max_queue_size);
    ThreadPoolBuilder& set_idle_timeout(const MonoDelta& idle_timeout);

    // Instantiate a new ThreadPool with the existing builder arguments.
    Status build(std::unique_ptr<ThreadPool>* pool) const;

private:
    friend class ThreadPool;
    const std::string _name;
    int _min_threads;
    int _max_threads;
    int _max_queue_size;
    MonoDelta _idle_timeout;

    DISALLOW_COPY_AND_ASSIGN(ThreadPoolBuilder);
};

// Thread pool with a variable number of threads.
//
// Tasks submitted directly to the thread pool enter a FIFO queue and are
// dispatched to a worker thread when one becomes free. Tasks may also be
// submitted via ThreadPoolTokens. The token Wait() and Shutdown() functions
// can then be used to block on logical groups of tasks.
//
// A token operates in one of two ExecutionModes, determined at token
// construction time:
// 1. SERIAL: submitted tasks are run one at a time.
// 2. CONCURRENT: submitted tasks may be run in parallel. This isn't unlike
//    tasks submitted without a token, but the logical grouping that tokens
//    impart can be useful when a pool is shared by many contexts (e.g. to
//    safely shut down one context, to derive context-specific metrics, etc.).
//
// Tasks submitted without a token or via ExecutionMode::CONCURRENT tokens are
// processed in FIFO order. On the other hand, ExecutionMode::SERIAL tokens are
// processed in a round-robin fashion, one task at a time. This prevents them
// from starving one another. However, tokenless (and CONCURRENT token-based)
// tasks can starve SERIAL token-based tasks.
//
// Usage Example:
//    static void Func(int n) { ... }
//    class Task : public Runnable { ... }
//
//    std::unique_ptr<ThreadPool> thread_pool;
//    CHECK_OK(
//        ThreadPoolBuilder("my_pool")
//            .set_min_threads(0)
//            .set_max_threads(5)
//            .set_max_queue_size(10)
//            .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
//            .Build(&thread_pool));
//    thread_pool->Submit(shared_ptr<Runnable>(new Task()));
//    thread_pool->SubmitFunc(std::bind(&Func, 10));
class ThreadPool {
public:
    ~ThreadPool();

    // Wait for the running tasks to complete and then shutdown the threads.
    // All the other pending tasks in the queue will be removed.
    // NOTE: That the user may implement an external abort logic for the
    //       runnables, that must be called before Shutdown(), if the system
    //       should know about the non-execution of these tasks, or the runnable
    //       require an explicit "abort" notification to exit from the run loop.
    void shutdown();

    // Submits a Runnable class.
    Status submit(std::shared_ptr<Runnable> r);

    // Submits a function bound using std::bind(&FuncName, args...).
    Status submit_func(std::function<void()> f);

    // Waits until all the tasks are completed.
    void wait();

    // Waits for the pool to reach the idle state, or until 'until' time is reached.
    // Returns true if the pool reached the idle state, false otherwise.
    bool wait_until(const MonoTime& until);

    // Waits for the pool to reach the idle state, or until 'delta' time elapses.
    // Returns true if the pool reached the idle state, false otherwise.
    bool wait_for(const MonoDelta& delta);

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
    std::unique_ptr<ThreadPoolToken> new_token(ExecutionMode mode, int max_concurrency = INT_MAX);

    // Return the number of threads currently running (or in the process of starting up)
    // for this thread pool.
    int num_threads() const {
        MutexLock l(&_lock);
        return _num_threads + _num_threads_pending_start;
    }

private:
    friend class ThreadPoolBuilder;
    friend class ThreadPoolToken;

    // Client-provided task to be executed by this pool.
    struct Task {
        std::shared_ptr<Runnable> runnable;

        // Time at which the entry was submitted to the pool.
        MonoTime submit_time;
    };

    // Creates a new thread pool using a builder.
    explicit ThreadPool(const ThreadPoolBuilder& builder);

    // Initializes the thread pool by starting the minimum number of threads.
    Status init();

    // Dispatcher responsible for dequeueing and executing the tasks
    void dispatch_thread();

    // Create new thread.
    //
    // REQUIRES: caller has incremented '_num_threads_pending_start' ahead of this call.
    // NOTE: For performance reasons, _lock should not be held.
    Status create_thread();

    // Aborts if the current thread is a member of this thread pool.
    void check_not_pool_thread_unlocked();

    // Submits a task to be run via token.
    Status do_submit(std::shared_ptr<Runnable> r, ThreadPoolToken* token);

    // Releases token 't' and invalidates it.
    void release_token(ThreadPoolToken* t);

    const std::string _name;
    const int _min_threads;
    const int _max_threads;
    const int _max_queue_size;
    const MonoDelta _idle_timeout;

    // Overall status of the pool. Set to an error when the pool is shut down.
    //
    // Protected by '_lock'.
    Status _pool_status;

    // Synchronizes many of the members of the pool and all of its
    // condition variables.
    mutable Mutex _lock;

    // Condition variable for "pool is idling". Waiters wake up when
    // _active_threads reaches zero.
    ConditionVariable _idle_cond;

    // Condition variable for "pool has no threads". Waiters wake up when
    // _num_threads and num_pending_threads_ are both 0.
    ConditionVariable _no_threads_cond;

    // Number of threads currently running.
    //
    // Protected by _lock.
    int _num_threads;

    // Number of threads which are in the process of starting.
    // When these threads start, they will decrement this counter and
    // accordingly increment '_num_threads'.
    //
    // Protected by _lock.
    int _num_threads_pending_start;

    // Number of threads currently running and executing client tasks.
    //
    // Protected by _lock.
    int _active_threads;

    // Total number of client tasks queued, either directly (_queue) or
    // indirectly (_tokens).
    //
    // Protected by _lock.
    int _total_queued_tasks;

    // All allocated tokens.
    //
    // Protected by _lock.
    std::unordered_set<ThreadPoolToken*> _tokens;

    // FIFO of tokens from which tasks should be executed. Does not own the
    // tokens; they are owned by clients and are removed from the FIFO on shutdown.
    //
    // Protected by _lock.
    std::deque<ThreadPoolToken*> _queue;

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
    // ConditionVariable (which yields FIFO semantics).
    //
    // Protected by _lock.
    struct IdleThread : public boost::intrusive::list_base_hook<> {
        explicit IdleThread(Mutex* m) : not_empty(m) {}

        // Condition variable for "queue is not empty". Waiters wake up when a new
        // task is queued.
        ConditionVariable not_empty;

        DISALLOW_COPY_AND_ASSIGN(IdleThread);
    };
    boost::intrusive::list<IdleThread> _idle_threads; // NOLINT(build/include_what_you_use)

    // ExecutionMode::CONCURRENT token used by the pool for tokenless submission.
    std::unique_ptr<ThreadPoolToken> _tokenless;

    DISALLOW_COPY_AND_ASSIGN(ThreadPool);
};

// Entry point for token-based task submission and blocking for a particular
// thread pool. Tokens can only be created via ThreadPool::new_token().
//
// All functions are thread-safe. Mutable members are protected via the
// ThreadPool's lock.
class ThreadPoolToken {
public:
    // Destroys the token.
    //
    // May be called on a token with outstanding tasks, as Shutdown() will be
    // called first to take care of them.
    ~ThreadPoolToken();

    // Submits a Runnable class.
    Status submit(std::shared_ptr<Runnable> r);

    // Submits a function bound using std::bind(&FuncName, args...).
    Status submit_func(std::function<void()> f);

    // Marks the token as unusable for future submissions. Any queued tasks not
    // yet running are destroyed. If tasks are in flight, Shutdown() will wait
    // on their completion before returning.
    void shutdown();

    // Waits until all the tasks submitted via this token are completed.
    void wait();

    // Waits for all submissions using this token are complete, or until 'until'
    // time is reached.
    //
    // Returns true if all submissions are complete, false otherwise.
    bool wait_until(const MonoTime& until);

    // Waits for all submissions using this token are complete, or until 'delta'
    // time elapses.
    //
    // Returns true if all submissions are complete, false otherwise.
    bool wait_for(const MonoDelta& delta);

    bool need_dispatch();

    size_t num_tasks() {
        MutexLock l(&_pool->_lock);
        return _entries.size();
    }

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
    friend std::ostream& operator<<(std::ostream& o, ThreadPoolToken::State s);

    friend class ThreadPool;

    // Returns a textual representation of 's' suitable for debugging.
    static const char* state_to_string(State s);

    // Constructs a new token.
    //
    // The token may not outlive its thread pool ('pool').
    ThreadPoolToken(ThreadPool* pool, ThreadPool::ExecutionMode mode, int max_concurrency = INT_MAX);

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
    ThreadPool::ExecutionMode mode() const { return _mode; }

    // Token's configured execution mode.
    ThreadPool::ExecutionMode _mode;

    // Pointer to the token's thread pool.
    ThreadPool* _pool;

    // Token state machine.
    State _state;

    // Queued client tasks.
    std::deque<ThreadPool::Task> _entries;

    // Condition variable for "token is idle". Waiters wake up when the token
    // transitions to IDLE or QUIESCED.
    ConditionVariable _not_running_cond;

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

    DISALLOW_COPY_AND_ASSIGN(ThreadPoolToken);
};

} // namespace doris

#endif //DORIS_BE_SRC_UTIL_THREAD_POOL_H
