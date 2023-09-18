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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/thread.h
// and modified by Doris

#pragma once
#include <butil/macros.h>
#include <pthread.h>
#include <stdint.h>

#include <functional>
#include <string>
#include <utility>

#include "common/status.h"
#include "gutil/ref_counted.h"
#include "util/countdown_latch.h"

namespace doris {
class WebPageHandler;

class Thread : public RefCountedThreadSafe<Thread> {
public:
    enum CreateFlags { NO_FLAGS = 0, NO_STACK_WATCHDOG = 1 };

    template <class F>
    static Status create_with_flags(const std::string& category, const std::string& name,
                                    const F& f, uint64_t flags, scoped_refptr<Thread>* holder) {
        return start_thread(category, name, f, flags, holder);
    }

    template <class F>
    static Status create(const std::string& category, const std::string& name, const F& f,
                         scoped_refptr<Thread>* holder) {
        return start_thread(category, name, f, NO_FLAGS, holder);
    }

    template <class F, class A1>
    static Status create(const std::string& category, const std::string& name, const F& f,
                         const A1& a1, scoped_refptr<Thread>* holder) {
        return start_thread(category, name, std::bind(f, a1), NO_FLAGS, holder);
    }

    template <class F, class A1, class A2>
    static Status create(const std::string& category, const std::string& name, const F& f,
                         const A1& a1, const A2& a2, scoped_refptr<Thread>* holder) {
        return start_thread(category, name, std::bind(f, a1, a2), NO_FLAGS, holder);
    }

    template <class F, class A1, class A2, class A3>
    static Status create(const std::string& category, const std::string& name, const F& f,
                         const A1& a1, const A2& a2, const A3& a3, scoped_refptr<Thread>* holder) {
        return start_thread(category, name, std::bind(f, a1, a2, a3), NO_FLAGS, holder);
    }

    template <class F, class A1, class A2, class A3, class A4>
    static Status create(const std::string& category, const std::string& name, const F& f,
                         const A1& a1, const A2& a2, const A3& a3, const A4& a4,
                         scoped_refptr<Thread>* holder) {
        return start_thread(category, name, std::bind(f, a1, a2, a3, a4), NO_FLAGS, holder);
    }

    template <class F, class A1, class A2, class A3, class A4, class A5>
    static Status create(const std::string& category, const std::string& name, const F& f,
                         const A1& a1, const A2& a2, const A3& a3, const A4& a4, const A5& a5,
                         scoped_refptr<Thread>* holder) {
        return start_thread(category, name, std::bind(f, a1, a2, a3, a4, a5), NO_FLAGS, holder);
    }

    template <class F, class A1, class A2, class A3, class A4, class A5, class A6>
    static Status create(const std::string& category, const std::string& name, const F& f,
                         const A1& a1, const A2& a2, const A3& a3, const A4& a4, const A5& a5,
                         const A6& a6, scoped_refptr<Thread>* holder) {
        return start_thread(category, name, std::bind(f, a1, a2, a3, a4, a5, a6), NO_FLAGS, holder);
    }

    static void set_self_name(const std::string& name);

#ifndef __APPLE__
    static void set_idle_sched();

    static void set_low_priority();
#endif

    ~Thread();

    // Blocks until this thread finishes execution. Once this method returns, the thread
    // will be unregistered with the ThreadMgr and will not appear in the debug UI.
    void join();

    // The thread ID assigned to this thread by the operating system. If the thread
    // has not yet started running, returns INVALID_TID.
    //
    // NOTE: this may block for a short amount of time if the thread has just been
    // started.
    int64_t tid() const;

    // Returns the thread's pthread ID.
    pthread_t pthread_id() const;

    const std::string& name() const;
    const std::string& category() const;
    std::string to_string() const;

    // The current thread of execution, or nullptr if the current thread isn't a doris::Thread.
    // This call is signal-safe.
    static Thread* current_thread();

    // Returns a unique, stable identifier for this thread. Note that this is a static
    // method and thus can be used on any thread, including the main thread of the
    // process.
    //
    // In general, this should be used when a value is required that is unique to
    // a thread and must work on any thread including the main process thread.
    //
    // NOTE: this is _not_ the TID, but rather a unique value assigned by the
    // thread implementation. So, this value should not be presented to the user
    // in log messages, etc.
    static int64_t unique_thread_id();

    // Returns the system thread ID (tid on Linux) for the current thread. Note
    // that this is a static method and thus can be used from any thread,
    // including the main thread of the process. This is in contrast to
    // Thread::tid(), which only works on doris::Threads.
    //
    // Thread::tid() will return the same value, but the value is cached in the
    // Thread object, so will be faster to call.
    //
    // Thread::unique_thread_id() (or Thread::tid()) should be preferred for
    // performance sensitive code, however it is only guaranteed to return a
    // unique and stable thread ID, not necessarily the system thread ID.
    static int64_t current_thread_id();

private:
    friend class ThreadJoiner;

    enum {
        INVALID_TID = -1,
        PARENT_WAITING_TID = -2,
    };

    // User function to be executed by this thread.
    typedef std::function<void()> ThreadFunctor;
    Thread(const std::string& category, const std::string& name, ThreadFunctor functor)
            : _thread(0),
              _tid(INVALID_TID),
              _functor(std::move(functor)),
              _category(std::move(category)),
              _name(std::move(name)),
              _done(1),
              _joinable(false) {}

    // Library-specific thread ID.
    pthread_t _thread;

    // OS-specific thread ID. Once the constructor finishes start_thread(),
    // guaranteed to be set either to a non-negative integer, or to INVALID_TID.
    //
    // The tid_ member goes through the following states:
    // 1. INVALID_TID: the thread has not been started, or has already exited.
    // 2. PARENT_WAITING_TID: the parent has started the thread, but the
    //    thread has not yet begun running. Therefore the TID is not yet known
    //    but it will be set once the thread starts.
    // 3. <positive value>: the thread is running.
    int64_t _tid;

    const ThreadFunctor _functor;

    const std::string _category;
    const std::string _name;

    // Joiners wait on this latch to be notified if the thread is done.
    //
    // Note that Joiners must additionally pthread_join(), otherwise certain
    // resources that callers expect to be destroyed (like TLS) may still be
    // alive when a Joiner finishes.
    CountDownLatch _done;

    bool _joinable;

    // Thread local pointer to the current thread of execution. Will be nullptr if the current
    // thread is not a Thread.
    static __thread Thread* _tls;

    // Wait for the running thread to publish its tid.
    int64_t wait_for_tid() const;

    // Starts the thread running supervise_thread(), and returns once that thread has
    // initialised and its TID has been read. Waits for notification from the started
    // thread that initialisation is complete before returning. On success, stores a
    // reference to the thread in holder.
    static Status start_thread(const std::string& category, const std::string& name,
                               const ThreadFunctor& functor, uint64_t flags,
                               scoped_refptr<Thread>* holder);

    // Wrapper for the user-supplied function. Invoked from the new thread,
    // with the Thread as its only argument. Executes _functor, but before
    // doing so registers with the global ThreadMgr and reads the thread's
    // system ID. After _functor terminates, unregisters with the ThreadMgr.
    // Always returns nullptr.
    //
    // supervise_thread() notifies start_thread() when thread initialisation is
    // completed via the _tid, which is set to the new thread's system ID.
    // By that point in time supervise_thread() has also taken a reference to
    // the Thread object, allowing it to safely refer to it even after the
    // caller drops its reference.
    //
    // Additionally, start_thread() notifies supervise_thread() when the actual
    // Thread object has been assigned (supervise_thread() is spinning during
    // this time). Without this, the new thread may reference the actual
    // Thread object before it has been assigned by start_thread(). See
    // KUDU-11 for more details.
    static void* supervise_thread(void* arg);

    // Invoked when the user-supplied function finishes or in the case of an
    // abrupt exit (i.e. pthread_exit()). Cleans up after supervise_thread().
    static void finish_thread(void* arg);

    static void init_threadmgr();
};

// Utility to join on a thread, printing warning messages if it
// takes too long. For example:
//
//   ThreadJoiner(&my_thread, "processing thread")
//     .warn_after_ms(1000)
//     .warn_every_ms(5000)
//     .Join();
//
// TODO: would be nice to offer a way to use ptrace() or signals to
// dump the stack trace of the thread we're trying to join on if it
// gets stuck. But, after looking for 20 minutes or so, it seems
// pretty complicated to get right.
class ThreadJoiner {
public:
    explicit ThreadJoiner(Thread* thread);

    // Start emitting warnings after this many milliseconds.
    //
    // Default: 1000 ms.
    ThreadJoiner& warn_after_ms(int ms);

    // After the warnings after started, emit another warning at the
    // given interval.
    //
    // Default: 1000 ms.
    ThreadJoiner& warn_every_ms(int ms);

    // If the thread has not stopped after this number of milliseconds, give up
    // joining on it and return Status::Aborted.
    //
    // -1 (the default) means to wait forever trying to join.
    ThreadJoiner& give_up_after_ms(int ms);

    // Join the thread, subject to the above parameters. If the thread joining
    // fails for any reason, returns RuntimeError. If it times out, returns
    // Aborted.
    Status join();

private:
    enum {
        kDefaultWarnAfterMs = 1000,
        kDefaultWarnEveryMs = 1000,
        kDefaultGiveUpAfterMs = -1 // forever
    };

    Thread* _thread;

    int _warn_after_ms;
    int _warn_every_ms;
    int _give_up_after_ms;

    DISALLOW_COPY_AND_ASSIGN(ThreadJoiner);
};

// Registers /threadz with the debug webserver.
void register_thread_display_page(WebPageHandler* web_page_handler);

} //namespace doris
