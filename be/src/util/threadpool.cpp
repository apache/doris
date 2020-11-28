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

#include "util/threadpool.h"

#include <cstdint>
#include <limits>
#include <ostream>

#include "common/logging.h"
#include "gutil/macros.h"
#include "gutil/map-util.h"
#include "gutil/strings/substitute.h"
#include "gutil/sysinfo.h"
#include "util/scoped_cleanup.h"
#include "util/thread.h"

namespace doris {

using std::string;
using strings::Substitute;

class FunctionRunnable : public Runnable {
public:
    explicit FunctionRunnable(std::function<void()> func) : _func(std::move(func)) {}

    void run() OVERRIDE { _func(); }

private:
    std::function<void()> _func;
};

ThreadPoolBuilder::ThreadPoolBuilder(string name)
        : _name(std::move(name)),
          _min_threads(0),
          _max_threads(base::NumCPUs()),
          _max_queue_size(std::numeric_limits<int>::max()),
          _idle_timeout(MonoDelta::FromMilliseconds(500)) {}

ThreadPoolBuilder& ThreadPoolBuilder::set_min_threads(int min_threads) {
    CHECK_GE(min_threads, 0);
    _min_threads = min_threads;
    return *this;
}

ThreadPoolBuilder& ThreadPoolBuilder::set_max_threads(int max_threads) {
    CHECK_GT(max_threads, 0);
    _max_threads = max_threads;
    return *this;
}

ThreadPoolBuilder& ThreadPoolBuilder::set_max_queue_size(int max_queue_size) {
    _max_queue_size = max_queue_size;
    return *this;
}

ThreadPoolBuilder& ThreadPoolBuilder::set_idle_timeout(const MonoDelta& idle_timeout) {
    _idle_timeout = idle_timeout;
    return *this;
}

Status ThreadPoolBuilder::build(std::unique_ptr<ThreadPool>* pool) const {
    pool->reset(new ThreadPool(*this));
    RETURN_IF_ERROR((*pool)->init());
    return Status::OK();
}

ThreadPoolToken::ThreadPoolToken(ThreadPool* pool, ThreadPool::ExecutionMode mode)
        : _mode(mode),
          _pool(pool),
          _state(State::IDLE),
          _not_running_cond(&pool->_lock),
          _active_threads(0) {}

ThreadPoolToken::~ThreadPoolToken() {
    shutdown();
    _pool->release_token(this);
}

Status ThreadPoolToken::submit(std::shared_ptr<Runnable> r) {
    return _pool->do_submit(std::move(r), this);
}

Status ThreadPoolToken::submit_func(std::function<void()> f) {
    return submit(std::make_shared<FunctionRunnable>(std::move(f)));
}

void ThreadPoolToken::shutdown() {
    MutexLock unique_lock(&(_pool->_lock));
    _pool->check_not_pool_thread_unlocked();

    // Clear the queue under the lock, but defer the releasing of the tasks
    // outside the lock, in case there are concurrent threads wanting to access
    // the ThreadPool. The task's destructors may acquire locks, etc, so this
    // also prevents lock inversions.
    std::deque<ThreadPool::Task> to_release = std::move(_entries);
    _pool->_total_queued_tasks -= to_release.size();

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

        // Note: this is an O(n) operation, but it's expected to be infrequent.
        // Plus doing it this way (rather than switching to QUIESCING and waiting
        // for a worker thread to process the queue entry) helps retain state
        // transition symmetry with ThreadPool::shutdown.
        for (auto it = _pool->_queue.begin(); it != _pool->_queue.end();) {
            if (*it == this) {
                it = _pool->_queue.erase(it);
            } else {
                it++;
            }
        }

        if (_active_threads == 0) {
            transition(State::QUIESCED);
            break;
        }
        transition(State::QUIESCING);
        FALLTHROUGH_INTENDED;
    case State::QUIESCING:
        // The token is already quiescing. Just wait for a worker thread to
        // switch it to QUIESCED.
        while (state() != State::QUIESCED) {
            _not_running_cond.wait();
        }
        break;
    default:
        break;
    }
}

void ThreadPoolToken::wait() {
    MutexLock unique_lock(&(_pool->_lock));
    _pool->check_not_pool_thread_unlocked();
    while (is_active()) {
        _not_running_cond.wait();
    }
}

bool ThreadPoolToken::wait_until(const MonoTime& until) {
    MutexLock unique_lock(&(_pool->_lock));
    _pool->check_not_pool_thread_unlocked();
    while (is_active()) {
        if (!_not_running_cond.wait_until(until)) {
            return false;
        }
    }
    return true;
}

bool ThreadPoolToken::wait_for(const MonoDelta& delta) {
    return wait_until(MonoTime::Now() + delta);
}

void ThreadPoolToken::transition(State new_state) {
#ifndef NDEBUG
    CHECK_NE(_state, new_state);

    switch (_state) {
    case State::IDLE:
        CHECK(new_state == State::RUNNING || new_state == State::QUIESCED);
        if (new_state == State::RUNNING) {
            CHECK(!_entries.empty());
        } else {
            CHECK(_entries.empty());
            CHECK_EQ(_active_threads, 0);
        }
        break;
    case State::RUNNING:
        CHECK(new_state == State::IDLE || new_state == State::QUIESCING ||
              new_state == State::QUIESCED);
        CHECK(_entries.empty());
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
        LOG(FATAL) << "Unknown token state: " << _state;
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

const char* ThreadPoolToken::state_to_string(State s) {
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

ThreadPool::ThreadPool(const ThreadPoolBuilder& builder)
        : _name(builder._name),
          _min_threads(builder._min_threads),
          _max_threads(builder._max_threads),
          _max_queue_size(builder._max_queue_size),
          _idle_timeout(builder._idle_timeout),
          _pool_status(Status::Uninitialized("The pool was not initialized.")),
          _idle_cond(&_lock),
          _no_threads_cond(&_lock),
          _num_threads(0),
          _num_threads_pending_start(0),
          _active_threads(0),
          _total_queued_tasks(0),
          _tokenless(new_token(ExecutionMode::CONCURRENT)) {}

ThreadPool::~ThreadPool() {
    // There should only be one live token: the one used in tokenless submission.
    CHECK_EQ(1, _tokens.size()) << strings::Substitute(
            "Threadpool $0 destroyed with $1 allocated tokens", _name, _tokens.size());
    shutdown();
}

Status ThreadPool::init() {
    if (!_pool_status.is_uninitialized()) {
        return Status::NotSupported("The thread pool is already initialized");
    }
    _pool_status = Status::OK();
    _num_threads_pending_start = _min_threads;
    for (int i = 0; i < _min_threads; i++) {
        Status status = create_thread();
        if (!status.ok()) {
            shutdown();
            return status;
        }
    }
    return Status::OK();
}

void ThreadPool::shutdown() {
    MutexLock unique_lock(&_lock);
    check_not_pool_thread_unlocked();

    // Note: this is the same error seen at submission if the pool is at
    // capacity, so clients can't tell them apart. This isn't really a practical
    // concern though because shutting down a pool typically requires clients to
    // be quiesced first, so there's no danger of a client getting confused.
    _pool_status = Status::ServiceUnavailable("The pool has been shut down.");

    // Clear the various queues under the lock, but defer the releasing
    // of the tasks outside the lock, in case there are concurrent threads
    // wanting to access the ThreadPool. The task's destructors may acquire
    // locks, etc, so this also prevents lock inversions.
    _queue.clear();
    std::deque<std::deque<Task>> to_release;
    for (auto* t : _tokens) {
        if (!t->_entries.empty()) {
            to_release.emplace_back(std::move(t->_entries));
        }
        switch (t->state()) {
        case ThreadPoolToken::State::IDLE:
            // The token is idle; we can quiesce it immediately.
            t->transition(ThreadPoolToken::State::QUIESCED);
            break;
        case ThreadPoolToken::State::RUNNING:
            // The token has tasks associated with it. If they're merely queued
            // (i.e. there are no active threads), the tasks will have been removed
            // above and we can quiesce immediately. Otherwise, we need to wait for
            // the threads to finish.
            t->transition(t->_active_threads > 0 ? ThreadPoolToken::State::QUIESCING
                                                 : ThreadPoolToken::State::QUIESCED);
            break;
        default:
            break;
        }
    }

    // The queues are empty. Wake any sleeping worker threads and wait for all
    // of them to exit. Some worker threads will exit immediately upon waking,
    // while others will exit after they finish executing an outstanding task.
    _total_queued_tasks = 0;
    while (!_idle_threads.empty()) {
        _idle_threads.front().not_empty.notify_one();
        _idle_threads.pop_front();
    }
    while (_num_threads + _num_threads_pending_start > 0) {
        _no_threads_cond.wait();
    }

    // All the threads have exited. Check the state of each token.
    for (auto* t : _tokens) {
        DCHECK(t->state() == ThreadPoolToken::State::IDLE ||
               t->state() == ThreadPoolToken::State::QUIESCED);
    }
}

std::unique_ptr<ThreadPoolToken> ThreadPool::new_token(ExecutionMode mode) {
    MutexLock unique_lock(&_lock);
    std::unique_ptr<ThreadPoolToken> t(new ThreadPoolToken(this, mode));
    InsertOrDie(&_tokens, t.get());
    return t;
}

void ThreadPool::release_token(ThreadPoolToken* t) {
    MutexLock unique_lock(&_lock);
    CHECK(!t->is_active()) << strings::Substitute("Token with state $0 may not be released",
                                                  ThreadPoolToken::state_to_string(t->state()));
    CHECK_EQ(1, _tokens.erase(t));
}

Status ThreadPool::submit(std::shared_ptr<Runnable> r) {
    return do_submit(std::move(r), _tokenless.get());
}

Status ThreadPool::submit_func(std::function<void()> f) {
    return submit(std::make_shared<FunctionRunnable>(std::move(f)));
}

Status ThreadPool::do_submit(std::shared_ptr<Runnable> r, ThreadPoolToken* token) {
    DCHECK(token);
    MonoTime submit_time = MonoTime::Now();

    MutexLock unique_lock(&_lock);
    if (PREDICT_FALSE(!_pool_status.ok())) {
        return _pool_status;
    }

    if (PREDICT_FALSE(!token->may_submit_new_tasks())) {
        return Status::ServiceUnavailable("Thread pool token was shut down");
    }

    // Size limit check.
    int64_t capacity_remaining = static_cast<int64_t>(_max_threads) - _active_threads +
                                 static_cast<int64_t>(_max_queue_size) - _total_queued_tasks;
    if (capacity_remaining < 1) {
        return Status::ServiceUnavailable(strings::Substitute(
                "Thread pool is at capacity ($0/$1 tasks running, $2/$3 tasks queued)",
                _num_threads + _num_threads_pending_start, _max_threads, _total_queued_tasks,
                _max_queue_size));
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
    int threads_from_this_submit =
            token->is_active() && token->mode() == ExecutionMode::SERIAL ? 0 : 1;
    int inactive_threads = _num_threads + _num_threads_pending_start - _active_threads;
    int additional_threads =
            static_cast<int>(_queue.size()) + threads_from_this_submit - inactive_threads;
    bool need_a_thread = false;
    if (additional_threads > 0 && _num_threads + _num_threads_pending_start < _max_threads) {
        need_a_thread = true;
        _num_threads_pending_start++;
    }

    Task task;
    task.runnable = std::move(r);
    task.submit_time = submit_time;

    // Add the task to the token's queue.
    ThreadPoolToken::State state = token->state();
    DCHECK(state == ThreadPoolToken::State::IDLE || state == ThreadPoolToken::State::RUNNING);
    token->_entries.emplace_back(std::move(task));
    if (state == ThreadPoolToken::State::IDLE || token->mode() == ExecutionMode::CONCURRENT) {
        _queue.emplace_back(token);
        if (state == ThreadPoolToken::State::IDLE) {
            token->transition(ThreadPoolToken::State::RUNNING);
        }
    }
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
    unique_lock.unlock();

    if (need_a_thread) {
        Status status = create_thread();
        if (!status.ok()) {
            unique_lock.lock();
            _num_threads_pending_start--;
            if (_num_threads + _num_threads_pending_start == 0) {
                // If we have no threads, we can't do any work.
                return status;
            }
            // If we failed to create a thread, but there are still some other
            // worker threads, log a warning message and continue.
            LOG(ERROR) << "Thread pool failed to create thread: " << status.to_string();
        }
    }

    return Status::OK();
}

void ThreadPool::wait() {
    MutexLock unique_lock(&_lock);
    check_not_pool_thread_unlocked();
    while (_total_queued_tasks > 0 || _active_threads > 0) {
        _idle_cond.wait();
    }
}

bool ThreadPool::wait_until(const MonoTime& until) {
    MutexLock unique_lock(&_lock);
    check_not_pool_thread_unlocked();
    while (_total_queued_tasks > 0 || _active_threads > 0) {
        if (!_idle_cond.wait_until(until)) {
            return false;
        }
    }
    return true;
}

bool ThreadPool::wait_for(const MonoDelta& delta) {
    return wait_until(MonoTime::Now() + delta);
}

void ThreadPool::dispatch_thread() {
    MutexLock unique_lock(&_lock);
    InsertOrDie(&_threads, Thread::current_thread());
    DCHECK_GT(_num_threads_pending_start, 0);
    _num_threads++;
    _num_threads_pending_start--;
    // If we are one of the first '_min_threads' to start, we must be
    // a "permanent" thread.
    bool permanent = _num_threads <= _min_threads;

    // Owned by this worker thread and added/removed from _idle_threads as needed.
    IdleThread me(&_lock);

    while (true) {
        // Note: Status::Aborted() is used to indicate normal shutdown.
        if (!_pool_status.ok()) {
            VLOG(2) << "DispatchThread exiting: " << _pool_status.to_string();
            break;
        }

        if (_queue.empty()) {
            // There's no work to do, let's go idle.
            //
            // Note: if FIFO behavior is desired, it's as simple as changing this to push_back().
            _idle_threads.push_front(me);
            SCOPED_CLEANUP({
                // For some wake ups (i.e. shutdown or do_submit) this thread is
                // guaranteed to be unlinked after being awakened. In others (i.e.
                // spurious wake-up or Wait timeout), it'll still be linked.
                if (me.is_linked()) {
                    _idle_threads.erase(_idle_threads.iterator_to(me));
                }
            });
            if (permanent) {
                me.not_empty.wait();
            } else {
                if (!me.not_empty.wait_for(_idle_timeout)) {
                    // After much investigation, it appears that pthread condition variables have
                    // a weird behavior in which they can return ETIMEDOUT from timed_wait even if
                    // another thread did in fact signal. Apparently after a timeout there is some
                    // brief period during which another thread may actually grab the internal mutex
                    // protecting the state, signal, and release again before we get the mutex. So,
                    // we'll recheck the empty queue case regardless.
                    if (_queue.empty()) {
                        VLOG(3) << "Releasing worker thread from pool " << _name << " after "
                                << _idle_timeout.ToMilliseconds() << "ms of idle time.";
                        break;
                    }
                }
            }
            continue;
        }

        // Get the next token and task to execute.
        ThreadPoolToken* token = _queue.front();
        _queue.pop_front();
        DCHECK_EQ(ThreadPoolToken::State::RUNNING, token->state());
        DCHECK(!token->_entries.empty());
        Task task = std::move(token->_entries.front());
        token->_entries.pop_front();
        token->_active_threads++;
        --_total_queued_tasks;
        ++_active_threads;

        unique_lock.unlock();

        // Execute the task
        task.runnable->run();

        // Destruct the task while we do not hold the lock.
        //
        // The task's destructor may be expensive if it has a lot of bound
        // objects, and we don't want to block submission of the threadpool.
        // In the worst case, the destructor might even try to do something
        // with this threadpool, and produce a deadlock.
        task.runnable.reset();
        unique_lock.lock();

        // Possible states:
        // 1. The token was shut down while we ran its task. Transition to QUIESCED.
        // 2. The token has no more queued tasks. Transition back to IDLE.
        // 3. The token has more tasks. Requeue it and transition back to RUNNABLE.
        ThreadPoolToken::State state = token->state();
        DCHECK(state == ThreadPoolToken::State::RUNNING ||
               state == ThreadPoolToken::State::QUIESCING);
        if (--token->_active_threads == 0) {
            if (state == ThreadPoolToken::State::QUIESCING) {
                DCHECK(token->_entries.empty());
                token->transition(ThreadPoolToken::State::QUIESCED);
            } else if (token->_entries.empty()) {
                token->transition(ThreadPoolToken::State::IDLE);
            } else if (token->mode() == ExecutionMode::SERIAL) {
                _queue.emplace_back(token);
            }
        }
        if (--_active_threads == 0) {
            _idle_cond.notify_all();
        }
    }

    // It's important that we hold the lock between exiting the loop and dropping
    // _num_threads. Otherwise it's possible someone else could come along here
    // and add a new task just as the last running thread is about to exit.
    CHECK(unique_lock.own_lock());

    CHECK_EQ(_threads.erase(Thread::current_thread()), 1);
    _num_threads--;
    if (_num_threads + _num_threads_pending_start == 0) {
        _no_threads_cond.notify_all();

        // Sanity check: if we're the last thread exiting, the queue ought to be
        // empty. Otherwise it will never get processed.
        CHECK(_queue.empty());
        DCHECK_EQ(0, _total_queued_tasks);
    }
}

Status ThreadPool::create_thread() {
    return Thread::create("thread pool", strings::Substitute("$0 [worker]", _name),
                          &ThreadPool::dispatch_thread, this, nullptr);
}

void ThreadPool::check_not_pool_thread_unlocked() {
    Thread* current = Thread::current_thread();
    if (ContainsKey(_threads, current)) {
        LOG(FATAL) << strings::Substitute(
                "Thread belonging to thread pool '$0' with "
                "name '$1' called pool function that would result in deadlock",
                _name, current->name());
    }
}

std::ostream& operator<<(std::ostream& o, ThreadPoolToken::State s) {
    return o << ThreadPoolToken::state_to_string(s);
}

} // namespace doris
