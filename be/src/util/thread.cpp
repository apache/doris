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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/thread.cc
// and modified by Doris

#include "thread.h"

#include <sys/resource.h>

#ifndef __APPLE__
// IWYU pragma: no_include <bits/types/struct_sched_param.h>
#include <sched.h>
#include <sys/prctl.h>
#else
#include <pthread.h>

#include <cstdint>
#endif

// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <cstring>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <vector>

#include "common/logging.h"
#include "gutil/atomicops.h"
#include "gutil/dynamic_annotations.h"
#include "gutil/map-util.h"
#include "gutil/stringprintf.h"
#include "gutil/strings/substitute.h"
#include "http/web_page_handler.h"
#include "util/debug/sanitizer_scopes.h"
#include "util/easy_json.h"
#include "util/os_util.h"
#include "util/scoped_cleanup.h"
#include "util/url_coding.h"

namespace doris {

class ThreadMgr;

__thread Thread* Thread::_tls = nullptr;

// Singleton instance of ThreadMgr. Only visible in this file, used only by Thread.
// // The Thread class adds a reference to thread_manager while it is supervising a thread so
// // that a race between the end of the process's main thread (and therefore the destruction
// // of thread_manager) and the end of a thread that tries to remove itself from the
// // manager after the destruction can be avoided.
static std::shared_ptr<ThreadMgr> thread_manager;
//
// Controls the single (lazy) initialization of thread_manager.
static std::once_flag once;

// A singleton class that tracks all live threads, and groups them together for easy
// auditing. Used only by Thread.
class ThreadMgr {
public:
    ThreadMgr() : _threads_started_metric(0), _threads_running_metric(0) {}

    ~ThreadMgr() {
        std::unique_lock<std::mutex> lock(_lock);
        _thread_categories.clear();
    }

    static void set_thread_name(const std::string& name, int64_t tid);

#ifndef __APPLE__
    static void set_idle_sched(int64_t tid);

    static void set_low_priority(int64_t tid);
#endif

    // not the system TID, since pthread_t is less prone to being recycled.
    void add_thread(const pthread_t& pthread_id, const std::string& name,
                    const std::string& category, int64_t tid);

    // Removes a thread from the supplied category. If the thread has
    // already been removed, this is a no-op.
    void remove_thread(const pthread_t& pthread_id, const std::string& category);

    void display_thread_callback(const WebPageHandler::ArgumentMap& args, EasyJson* ej) const;

private:
    // Container class for any details we want to capture about a thread
    // TODO: Add start-time.
    // TODO: Track fragment ID.
    class ThreadDescriptor {
    public:
        ThreadDescriptor() {}
        ThreadDescriptor(std::string category, std::string name, int64_t thread_id)
                : _name(std::move(name)), _category(std::move(category)), _thread_id(thread_id) {}

        const std::string& name() const { return _name; }
        const std::string& category() const { return _category; }
        int64_t thread_id() const { return _thread_id; }

    private:
        std::string _name;
        std::string _category;
        int64_t _thread_id;
    };

    void summarize_thread_descriptor(const ThreadDescriptor& desc, EasyJson* ej) const;

    // A ThreadCategory is a set of threads that are logically related.
    // TODO: unordered_map is incompatible with pthread_t, but would be more
    // efficient here.
    typedef std::map<const pthread_t, ThreadDescriptor> ThreadCategory;

    // All thread categories, keyed on the category name.
    typedef std::map<std::string, ThreadCategory> ThreadCategoryMap;

    // Protects _thread_categories and thread metrics.
    mutable std::mutex _lock;

    // All thread categories that ever contained a thread, even if empty
    ThreadCategoryMap _thread_categories;

    // Counters to track all-time total number of threads, and the
    // current number of running threads.
    uint64_t _threads_started_metric;
    uint64_t _threads_running_metric;

    DISALLOW_COPY_AND_ASSIGN(ThreadMgr);
};

void ThreadMgr::set_thread_name(const std::string& name, int64_t tid) {
    if (tid == getpid()) {
        return;
    }
#ifdef __APPLE__
    int err = pthread_setname_np(name.c_str());
#else
    int err = prctl(PR_SET_NAME, name.c_str());
#endif
    if (err < 0 && errno != EPERM) {
        LOG(ERROR) << "set_thread_name";
    }
}

#ifndef __APPLE__
void ThreadMgr::set_idle_sched(int64_t tid) {
    if (tid == getpid()) {
        return;
    }
    struct sched_param sp = {.sched_priority = 0};
    int err = sched_setscheduler(0, SCHED_IDLE, &sp);
    if (err < 0 && errno != EPERM) {
        LOG(ERROR) << "set_thread_idle_sched";
    }
}

void ThreadMgr::set_low_priority(int64_t tid) {
    if (tid == getpid()) {
        return;
    }
    // From Linux kernel:
    // In the current implementation, each unit of difference in the nice values of two
    // processes results in a factor of 1.25 in the degree to which the
    // scheduler favors the higher priority process.  This causes very
    // low nice values (+19) to truly provide little CPU to a process
    // whenever there is any other higher priority load on the system,
    // and makes high nice values (-20) deliver most of the CPU to
    // applications that require it (e.g., some audio applications).

    // Choose 5 as lower priority value, default is 0
    constexpr static int s_low_priority_nice_value = 5;
    int err = setpriority(PRIO_PROCESS, 0, s_low_priority_nice_value);
    if (err < 0 && errno != EPERM) {
        LOG(ERROR) << "set_thread_low_priority";
    }
}
#endif

void ThreadMgr::add_thread(const pthread_t& pthread_id, const std::string& name,
                           const std::string& category, int64_t tid) {
    // These annotations cause TSAN to ignore the synchronization on lock_
    // without causing the subsequent mutations to be treated as data races
    // in and of themselves (that's what IGNORE_READS_AND_WRITES does).
    //
    // Why do we need them here and in SuperviseThread()? TSAN operates by
    // observing synchronization events and using them to establish "happens
    // before" relationships between threads. Where these relationships are
    // not built, shared state access constitutes a data race. The
    // synchronization events here, in RemoveThread(), and in
    // SuperviseThread() may cause TSAN to establish a "happens before"
    // relationship between thread functors, ignoring potential data races.
    // The annotations prevent this from happening.
    ANNOTATE_IGNORE_SYNC_BEGIN();
    debug::ScopedTSANIgnoreReadsAndWrites ignore_tsan;
    {
        std::unique_lock<std::mutex> l(_lock);
        _thread_categories[category][pthread_id] = ThreadDescriptor(category, name, tid);
        _threads_running_metric++;
        _threads_started_metric++;
    }
    ANNOTATE_IGNORE_SYNC_END();
}

void ThreadMgr::remove_thread(const pthread_t& pthread_id, const std::string& category) {
    ANNOTATE_IGNORE_SYNC_BEGIN();
    debug::ScopedTSANIgnoreReadsAndWrites ignore_tsan;
    {
        std::unique_lock<std::mutex> l(_lock);
        auto category_it = _thread_categories.find(category);
        DCHECK(category_it != _thread_categories.end());
        category_it->second.erase(pthread_id);
        _threads_running_metric--;
    }
    ANNOTATE_IGNORE_SYNC_END();
}

void ThreadMgr::display_thread_callback(const WebPageHandler::ArgumentMap& args,
                                        EasyJson* ej) const {
    const auto* category_name = FindOrNull(args, "group");
    if (category_name) {
        bool requested_all = (*category_name == "all");
        ej->Set("requested_thread_group", EasyJson::kObject);
        (*ej)["group_name"] = escape_for_html_to_string(*category_name);
        (*ej)["requested_all"] = requested_all;

        // The critical section is as short as possible so as to minimize the delay
        // imposed on new threads that acquire the lock in write mode.
        std::vector<ThreadDescriptor> descriptors_to_print;
        if (!requested_all) {
            std::unique_lock<std::mutex> l(_lock);
            const auto* category = FindOrNull(_thread_categories, *category_name);
            if (!category) {
                return;
            }
            for (const auto& elem : *category) {
                descriptors_to_print.emplace_back(elem.second);
            }
        } else {
            std::unique_lock<std::mutex> l(_lock);
            for (const auto& category : _thread_categories) {
                for (const auto& elem : category.second) {
                    descriptors_to_print.emplace_back(elem.second);
                }
            }
        }

        EasyJson found = (*ej).Set("found", EasyJson::kObject);
        EasyJson threads = found.Set("threads", EasyJson::kArray);
        for (const auto& desc : descriptors_to_print) {
            summarize_thread_descriptor(desc, &threads);
        }
    } else {
        // List all thread groups and the number of threads running in each.
        std::vector<std::pair<string, uint64_t>> thread_categories_info;
        uint64_t running;
        {
            std::unique_lock<std::mutex> l(_lock);
            running = _threads_running_metric;
            thread_categories_info.reserve(_thread_categories.size());
            for (const auto& category : _thread_categories) {
                thread_categories_info.emplace_back(category.first, category.second.size());
            }

            (*ej)["total_threads_running"] = running;
            EasyJson groups = ej->Set("groups", EasyJson::kArray);
            for (const auto& elem : thread_categories_info) {
                string category_arg;
                url_encode(elem.first, &category_arg);
                EasyJson group = groups.PushBack(EasyJson::kObject);
                group["encoded_group_name"] = category_arg;
                group["group_name"] = elem.first;
                group["threads_running"] = elem.second;
            }
        }
    }
}

void ThreadMgr::summarize_thread_descriptor(const ThreadMgr::ThreadDescriptor& desc,
                                            EasyJson* ej) const {
    ThreadStats stats;
    Status status = get_thread_stats(desc.thread_id(), &stats);
    if (!status.ok()) {
        LOG(WARNING) << "Could not get per-thread statistics: " << status.to_string();
    }

    EasyJson thread = ej->PushBack(EasyJson::kObject);
    thread["thread_name"] = desc.name();
    thread["user_sec"] = static_cast<double>(stats.user_ns) / 1e9;
    thread["kernel_sec"] = static_cast<double>(stats.kernel_ns) / 1e9;
    thread["iowait_sec"] = static_cast<double>(stats.iowait_ns) / 1e9;
}

Thread::~Thread() {
    if (_joinable) {
        int ret = pthread_detach(_thread);
        CHECK_EQ(ret, 0);
    }
}

void Thread::set_self_name(const std::string& name) {
    ThreadMgr::set_thread_name(name, current_thread_id());
}

#ifndef __APPLE__
void Thread::set_idle_sched() {
    ThreadMgr::set_idle_sched(current_thread_id());
}

void Thread::set_low_priority() {
    ThreadMgr::set_low_priority(current_thread_id());
}
#endif

void Thread::join() {
    auto status = ThreadJoiner(this).join();
    if (!status.ok()) {
        LOG(WARNING) << "Join thread failed, thread: " << to_string()
                     << ", err:" << status.to_string();
    }
}

int64_t Thread::tid() const {
    int64_t t = base::subtle::Acquire_Load(&_tid);
    if (t != PARENT_WAITING_TID) {
        return _tid;
    }
    return wait_for_tid();
}

pthread_t Thread::pthread_id() const {
    return _thread;
}

const std::string& Thread::name() const {
    return _name;
}

const std::string& Thread::category() const {
    return _category;
}

std::string Thread::to_string() const {
    return strings::Substitute("Thread $0 (name: \"$1\", category: \"$2\")", tid(), _name,
                               _category);
}

Thread* Thread::current_thread() {
    return _tls;
}

int64_t Thread::unique_thread_id() {
#ifdef __APPLE__
    uint64_t tid;
    pthread_threadid_np(pthread_self(), &tid);
    return tid;
#else
    return static_cast<int64_t>(pthread_self());
#endif
}

int64_t Thread::current_thread_id() {
#ifdef __APPLE__
    uint64_t tid;
    pthread_threadid_np(nullptr, &tid);
    return tid;
#else
    return syscall(SYS_gettid);
#endif
}

int64_t Thread::wait_for_tid() const {
    int loop_count = 0;
    while (true) {
        int64_t t = Acquire_Load(&_tid);
        if (t != PARENT_WAITING_TID) {
            return t;
        }
        // copied from boost::detail::yield
        int k = loop_count++;
        if (k < 32 || k & 1) {
            sched_yield();
        } else {
            // g++ -Wextra warns on {} or {0}
            struct timespec rqtp = {0, 0};

            // POSIX says that timespec has tv_sec and tv_nsec
            // But it doesn't guarantee order or placement

            rqtp.tv_sec = 0;
            rqtp.tv_nsec = 1000;

            nanosleep(&rqtp, 0);
        }
    }
}

Status Thread::start_thread(const std::string& category, const std::string& name,
                            const ThreadFunctor& functor, uint64_t flags,
                            scoped_refptr<Thread>* holder) {
    std::call_once(once, init_threadmgr);

    // Temporary reference for the duration of this function.
    scoped_refptr<Thread> t(new Thread(category, name, functor));

    // Optional, and only set if the thread was successfully created.
    //
    // We have to set this before we even start the thread because it's
    // allowed for the thread functor to access 'holder'.
    if (holder) {
        *holder = t;
    }

    t->_tid = PARENT_WAITING_TID;

    // Add a reference count to the thread since SuperviseThread() needs to
    // access the thread object, and we have no guarantee that our caller
    // won't drop the reference as soon as we return. This is dereferenced
    // in FinishThread().
    t->AddRef();

    auto cleanup = MakeScopedCleanup([&]() {
        // If we failed to create the thread, we need to undo all of our prep work.
        t->_tid = INVALID_TID;
        t->Release();
    });

    int ret = pthread_create(&t->_thread, nullptr, &Thread::supervise_thread, t.get());
    if (ret) {
        return Status::RuntimeError("Could not create thread. (error {}) {}", ret, strerror(ret));
    }

    // The thread has been created and is now joinable.
    //
    // Why set this in the parent and not the child? Because only the parent
    // (or someone communicating with the parent) can join, so joinable must
    // be set before the parent returns.
    t->_joinable = true;
    cleanup.cancel();

    VLOG_NOTICE << "Started thread " << t->tid() << " - " << category << ":" << name;
    return Status::OK();
}

void* Thread::supervise_thread(void* arg) {
    Thread* t = static_cast<Thread*>(arg);
    int64_t system_tid = Thread::current_thread_id();
    PCHECK(system_tid != -1);

    // Take an additional reference to the thread manager, which we'll need below.
    ANNOTATE_IGNORE_SYNC_BEGIN();
    std::shared_ptr<ThreadMgr> thread_mgr_ref = thread_manager;
    ANNOTATE_IGNORE_SYNC_END();

    // Set up the TLS.
    //
    // We could store a scoped_refptr in the TLS itself, but as its
    // lifecycle is poorly defined, we'll use a bare pointer. We
    // already incremented the reference count in StartThread.
    Thread::_tls = t;

    // Publish our tid to '_tid', which unblocks any callers waiting in
    // WaitForTid().
    Release_Store(&t->_tid, system_tid);

    std::string name = strings::Substitute("$0-$1", t->name(), system_tid);
    thread_manager->set_thread_name(name, t->_tid);
    thread_manager->add_thread(pthread_self(), name, t->category(), t->_tid);

    // FinishThread() is guaranteed to run (even if functor_ throws an
    // exception) because pthread_cleanup_push() creates a scoped object
    // whose destructor invokes the provided callback.
    pthread_cleanup_push(&Thread::finish_thread, t);
    t->_functor();
    pthread_cleanup_pop(true);

    return nullptr;
}

void Thread::finish_thread(void* arg) {
    Thread* t = static_cast<Thread*>(arg);

    // We're here either because of the explicit pthread_cleanup_pop() in
    // SuperviseThread() or through pthread_exit(). In either case,
    // thread_manager is guaranteed to be live because thread_mgr_ref in
    // SuperviseThread() is still live.
    thread_manager->remove_thread(pthread_self(), t->category());

    // Signal any Joiner that we're done.
    t->_done.count_down();

    VLOG_CRITICAL << "Ended thread " << t->_tid << " - " << t->category() << ":" << t->name();
    t->Release();
    // NOTE: the above 'Release' call could be the last reference to 'this',
    // so 'this' could be destructed at this point. Do not add any code
    // following here!
}

void Thread::init_threadmgr() {
    thread_manager.reset(new ThreadMgr());
}

ThreadJoiner::ThreadJoiner(Thread* thr)
        : _thread(CHECK_NOTNULL(thr)),
          _warn_after_ms(kDefaultWarnAfterMs),
          _warn_every_ms(kDefaultWarnEveryMs),
          _give_up_after_ms(kDefaultGiveUpAfterMs) {}

ThreadJoiner& ThreadJoiner::warn_after_ms(int ms) {
    _warn_after_ms = ms;
    return *this;
}

ThreadJoiner& ThreadJoiner::warn_every_ms(int ms) {
    _warn_every_ms = ms;
    return *this;
}

ThreadJoiner& ThreadJoiner::give_up_after_ms(int ms) {
    _give_up_after_ms = ms;
    return *this;
}

Status ThreadJoiner::join() {
    if (Thread::current_thread() && Thread::current_thread()->tid() == _thread->tid()) {
        return Status::InvalidArgument("Can't join on own thread. (error {}) {}", -1,
                                       _thread->_name);
    }

    // Early exit: double join is a no-op.
    if (!_thread->_joinable) {
        return Status::OK();
    }

    int waited_ms = 0;
    bool keep_trying = true;
    while (keep_trying) {
        if (waited_ms >= _warn_after_ms) {
            LOG(WARNING) << strings::Substitute("Waited for $0ms trying to join with $1 (tid $2)",
                                                waited_ms, _thread->_name, _thread->_tid);
        }

        int remaining_before_giveup = std::numeric_limits<int>::max();
        if (_give_up_after_ms != -1) {
            remaining_before_giveup = _give_up_after_ms - waited_ms;
        }

        int remaining_before_next_warn = _warn_every_ms;
        if (waited_ms < _warn_after_ms) {
            remaining_before_next_warn = _warn_after_ms - waited_ms;
        }

        if (remaining_before_giveup < remaining_before_next_warn) {
            keep_trying = false;
        }

        int wait_for = std::min(remaining_before_giveup, remaining_before_next_warn);

        if (_thread->_done.wait_for(std::chrono::milliseconds(wait_for))) {
            // Unconditionally join before returning, to guarantee that any TLS
            // has been destroyed (pthread_key_create() destructors only run
            // after a pthread's user method has returned).
            int ret = pthread_join(_thread->_thread, nullptr);
            CHECK_EQ(ret, 0);
            _thread->_joinable = false;
            return Status::OK();
        }
        waited_ms += wait_for;
    }
    return Status::Aborted("Timed out after {}ms joining on {}", waited_ms, _thread->_name);
}

void register_thread_display_page(WebPageHandler* web_page_handler) {
    web_page_handler->register_template_page(
            "/threadz", "Threads",
            std::bind(&ThreadMgr::display_thread_callback, thread_manager.get(),
                      std::placeholders::_1, std::placeholders::_2),
            true);
}
} // namespace doris
