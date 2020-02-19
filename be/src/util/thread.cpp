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

#include "thread.h"

#include <unistd.h>
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <sys/prctl.h>
#include <sys/types.h>

#include "common/logging.h"
#include "gutil/atomicops.h"
#include "gutil/once.h"
#include "gutil/dynamic_annotations.h"
#include "gutil/strings/substitute.h"
#include "olap/olap_define.h"
#include "util/mutex.h"
#include "util/scoped_cleanup.h"

namespace doris {

class ThreadMgr;

__thread Thread* Thread::_tls = NULL;

// Singleton instance of ThreadMgr. Only visible in this file, used only by Thread.
// // The Thread class adds a reference to thread_manager while it is supervising a thread so
// // that a race between the end of the process's main thread (and therefore the destruction
// // of thread_manager) and the end of a thread that tries to remove itself from the
// // manager after the destruction can be avoided.
static std::shared_ptr<ThreadMgr> thread_manager;
//
// Controls the single (lazy) initialization of thread_manager.
static GoogleOnceType once = GOOGLE_ONCE_INIT;

// A singleton class that tracks all live threads, and groups them together for easy
// auditing. Used only by Thread.
class ThreadMgr {
public:
    ThreadMgr()
        : _threads_started_metric(0),
          _threads_running_metric(0) {}

    ~ThreadMgr() {
        MutexLock lock(&_lock);
        _thread_categories.clear();
    }

    static void set_thread_name(const std::string& name, int64_t tid);

    // not the system TID, since pthread_t is less prone to being recycled.
    void add_thread(const pthread_t& pthread_id, const std::string& name,
                    const std::string& category, int64_t tid);

    // Removes a thread from the supplied category. If the thread has
    // already been removed, this is a no-op.
    void remove_thread(const pthread_t& pthread_id, const std::string& category);

private:

    // Container class for any details we want to capture about a thread
    // TODO: Add start-time.
    // TODO: Track fragment ID.
    class ThreadDescriptor {
    public:
        ThreadDescriptor() { }
        ThreadDescriptor(std::string category, std::string name, int64_t thread_id)
            : _name(std::move(name)),
            _category(std::move(category)),
            _thread_id(thread_id) {}

        const std::string& name() const { return _name; }
        const std::string& category() const { return _category; }
        int64_t thread_id() const { return _thread_id; }

    private:
        std::string _name;
        std::string _category;
        int64_t _thread_id;
    };

    // A ThreadCategory is a set of threads that are logically related.
    // TODO: unordered_map is incompatible with pthread_t, but would be more
    // efficient here.
    typedef std::map<const pthread_t, ThreadDescriptor> ThreadCategory;

    // All thread categorys, keyed on the category name.
    typedef std::map<std::string, ThreadCategory> ThreadCategoryMap;

    // Protects _thread_categories and thread metrics.
    Mutex _lock;

    // All thread categorys that ever contained a thread, even if empty
    ThreadCategoryMap _thread_categories;

    // Counters to track all-time total number of threads, and the
    // current number of running threads.
    uint64_t _threads_started_metric;
    uint64_t _threads_running_metric;

    DISALLOW_COPY_AND_ASSIGN(ThreadMgr);
};

void ThreadMgr::set_thread_name(const std::string& name, int64_t tid) {
    if (tid == getpid()) {
        return ;
    }
    int err = prctl(PR_SET_NAME, name.c_str());
    if (err < 0 && errno != EPERM) {
        LOG(ERROR) << "set_thread_name";
    }
}

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
    ANNOTATE_IGNORE_READS_AND_WRITES_BEGIN();
    {
        MutexLock l(&_lock);
        _thread_categories[category][pthread_id] = ThreadDescriptor(category, name, tid);
        _threads_running_metric++;
        _threads_started_metric++;
    }
    ANNOTATE_IGNORE_SYNC_END();
    ANNOTATE_IGNORE_READS_AND_WRITES_END();
}

void ThreadMgr::remove_thread(const pthread_t& pthread_id, const std::string& category) {
    ANNOTATE_IGNORE_SYNC_BEGIN();
    ANNOTATE_IGNORE_READS_AND_WRITES_BEGIN();
    {
        MutexLock l(&_lock);
        auto category_it = _thread_categories.find(category);
        DCHECK(category_it != _thread_categories.end());
        category_it->second.erase(pthread_id);
        _threads_running_metric--;
    }
    ANNOTATE_IGNORE_SYNC_END();
    ANNOTATE_IGNORE_READS_AND_WRITES_END();
}

Thread::~Thread() {
    if (_joinable) {
        int ret = pthread_detach(_thread);
        CHECK_EQ(ret, 0);
    }
}

void Thread::join() {
    ThreadJoiner(this).join();
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
  return strings::Substitute("Thread $0 (name: \"$1\", category: \"$2\")", tid(), _name, _category);
}

Thread* Thread::current_thread() {
    return _tls;
}

int64_t Thread::unique_thread_id() {
    return static_cast<int64_t>(pthread_self());
} 

int64_t Thread::current_thread_id() {
    return syscall(SYS_gettid);
}

int64_t Thread::wait_for_tid() const {
    int loop_count = 0;
    while (true) {
        int64_t t = Acquire_Load(&_tid);
        if (t != PARENT_WAITING_TID) return t;
        boost::detail::yield(loop_count++);
    }
}

Status Thread::start_thread(const std::string& category, const std::string& name,
                            const ThreadFunctor& functor, uint64_t flags,
                            scoped_refptr<Thread>* holder) {
    GoogleOnceInit(&once, &init_threadmgr);

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

    int ret = pthread_create(&t->_thread, NULL, &Thread::supervise_thread, t.get());
    if (ret) {
        return Status::RuntimeError("Could not create thread", ret, strerror(ret));
    }

    // The thread has been created and is now joinable.
    //
    // Why set this in the parent and not the child? Because only the parent
    // (or someone communicating with the parent) can join, so joinable must
    // be set before the parent returns.
    t->_joinable = true;
    cleanup.cancel();

    VLOG(3) << "Started thread " << t->tid()<< " - " << category << ":" << name;
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

    return NULL;
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

    VLOG(2) << "Ended thread " << t->_tid << " - " << t->category() << ":" << t->name();
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
    if (Thread::current_thread() &&
        Thread::current_thread()->tid() == _thread->tid()) {
        return Status::InvalidArgument("Can't join on own thread", -1, _thread->_name);
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

        if (_thread->_done.wait_for(MonoDelta::FromMilliseconds(wait_for))) {
            // Unconditionally join before returning, to guarantee that any TLS
            // has been destroyed (pthread_key_create() destructors only run
            // after a pthread's user method has returned).
            int ret = pthread_join(_thread->_thread, NULL);
            CHECK_EQ(ret, 0);
            _thread->_joinable = false;
            return Status::OK();
        }
        waited_ms += wait_for;
    }
    return Status::Aborted(strings::Substitute("Timed out after $0ms joining on $1",
                                               waited_ms, _thread->_name));
}

} // namespace doris
