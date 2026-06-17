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

#include "util/bthread_shared_mutex.h"

#include <bthread/bthread.h>
#include <gtest/gtest.h>

#include <atomic>
#include <shared_mutex>
#include <thread>
#include <vector>

namespace doris {

// 1) Basic SharedMutex contract. try_lock/try_lock_shared are non-blocking, so
//    this can be checked single-threaded.
TEST(BthreadSharedMutexTest, BasicContract) {
    BthreadSharedMutex m;

    // Multiple shared holders can coexist.
    ASSERT_TRUE(m.try_lock_shared());
    ASSERT_TRUE(m.try_lock_shared());
    // A writer cannot acquire while any reader is held.
    ASSERT_FALSE(m.try_lock());
    m.unlock_shared();
    ASSERT_FALSE(m.try_lock()); // still one reader
    m.unlock_shared();

    // Exclusive ownership.
    ASSERT_TRUE(m.try_lock());
    ASSERT_FALSE(m.try_lock());        // exclusive: a second writer fails
    ASSERT_FALSE(m.try_lock_shared()); // a reader is blocked while the writer holds
    m.unlock();

    // Free again.
    ASSERT_TRUE(m.try_lock_shared());
    m.unlock_shared();
}

// 2) Writer-pending behavior of the two-gate algorithm: once a writer has set
//    the write-entered bit and is waiting for existing readers to drain, new
//    readers must block until the writer acquires and releases the lock.
namespace {
struct WriterPendingCtx {
    BthreadSharedMutex* m;
    std::atomic<bool> acquired {false};
    std::atomic<bool> released {false};
};

void* writer_pending_fn(void* arg) {
    auto* c = static_cast<WriterPendingCtx*>(arg);
    c->m->lock(); // sets write-entered, then blocks until the reader drains
    c->acquired.store(true);
    bthread_usleep(20 * 1000);
    c->m->unlock();
    c->released.store(true);
    return nullptr;
}
} // namespace

TEST(BthreadSharedMutexTest, WriterPendingBlocksNewReaders) {
    bthread_setconcurrency(8);
    BthreadSharedMutex m;
    m.lock_shared(); // reader R1 holds a shared lock

    WriterPendingCtx ctx;
    ctx.m = &m;
    bthread_t w;
    ASSERT_EQ(0, bthread_start_background(&w, nullptr, writer_pending_fn, &ctx));

    // Give the writer time to reach lock() and set the write-entered bit.
    bthread_usleep(100 * 1000);
    ASSERT_FALSE(ctx.acquired.load()); // still waiting on R1 to drain
    // A new reader must NOT barge ahead of the pending writer.
    ASSERT_FALSE(m.try_lock_shared());

    m.unlock_shared(); // drain R1 -> writer can now acquire
    ASSERT_EQ(0, bthread_join(w, nullptr));
    ASSERT_TRUE(ctx.acquired.load());
    ASSERT_TRUE(ctx.released.load());

    // The lock is usable afterwards.
    ASSERT_TRUE(m.try_lock_shared());
    m.unlock_shared();
}

// 3) Cross-thread unlock regression. Ownership is not tied to the OS thread that
//    acquired the lock, so unlocking from a different thread is well defined
//    (with std::shared_mutex / pthread_rwlock_t this is undefined behavior and
//    can permanently wedge the lock).
TEST(BthreadSharedMutexTest, CrossThreadUnlockExclusive) {
    BthreadSharedMutex m;
    m.lock();                           // acquired on this thread
    std::thread t([&] { m.unlock(); }); // released on a different OS thread
    t.join();
    // Not wedged: the lock can be acquired again.
    ASSERT_TRUE(m.try_lock());
    m.unlock();
}

TEST(BthreadSharedMutexTest, CrossThreadUnlockShared) {
    BthreadSharedMutex m;
    m.lock_shared();
    std::thread t([&] { m.unlock_shared(); });
    t.join();
    // The reader was fully released, so a writer can acquire.
    ASSERT_TRUE(m.try_lock());
    m.unlock();
}

// 3b) bthread migration variant: hold the lock across a suspension point so the
//     bthread may migrate to another worker pthread, and unlock from the resumed
//     context (potentially a different OS thread). Many bthreads run concurrently
//     to force migration; the test passing means the lock never wedges.
namespace {
struct MigrateCtx {
    BthreadSharedMutex* m;
    std::atomic<int>* done;
    int iters;
};

void* migrate_fn(void* arg) {
    auto* c = static_cast<MigrateCtx*>(arg);
    for (int i = 0; i < c->iters; ++i) {
        c->m->lock();
        bthread_usleep(100); // suspension point while holding the write lock
        c->m->unlock();      // may run on a different worker than lock()
        c->m->lock_shared();
        bthread_usleep(100);
        c->m->unlock_shared();
    }
    c->done->fetch_add(1);
    return nullptr;
}
} // namespace

TEST(BthreadSharedMutexTest, BthreadMigrationNoWedge) {
    bthread_setconcurrency(8);
    BthreadSharedMutex m;
    std::atomic<int> done {0};
    MigrateCtx ctx {&m, &done, 100};

    constexpr int kBthreads = 16;
    std::vector<bthread_t> tids(kBthreads);
    for (auto& t : tids) {
        ASSERT_EQ(0, bthread_start_background(&t, nullptr, migrate_fn, &ctx));
    }
    for (auto t : tids) {
        ASSERT_EQ(0, bthread_join(t, nullptr));
    }
    ASSERT_EQ(kBthreads, done.load()); // all finished -> no permanent wedge
    ASSERT_TRUE(m.try_lock());
    m.unlock();
}

// 4) Mixed reader/writer stress with invariant counters, also exercising the
//    RAII wrappers std::shared_lock / std::unique_lock (point 5).
namespace {
struct StressCtx {
    BthreadSharedMutex* m;
    std::atomic<int>* active_readers;
    std::atomic<int>* active_writers;
    std::atomic<bool>* failed;
    int iters;
    int id;
};

void* stress_fn(void* arg) {
    auto* c = static_cast<StressCtx*>(arg);
    for (int i = 0; i < c->iters; ++i) {
        if ((c->id + i) % 4 == 0) { // ~25% writers
            std::unique_lock<BthreadSharedMutex> wlock(*c->m);
            int aw = c->active_writers->fetch_add(1) + 1;
            // Invariant: at most one writer, and no readers while writing.
            if (aw != 1 || c->active_readers->load() != 0) {
                c->failed->store(true);
            }
            bthread_usleep(10);
            c->active_writers->fetch_sub(1);
        } else {
            std::shared_lock<BthreadSharedMutex> rlock(*c->m);
            c->active_readers->fetch_add(1);
            // Invariant: no writer while readers are active.
            if (c->active_writers->load() != 0) {
                c->failed->store(true);
            }
            bthread_usleep(10);
            c->active_readers->fetch_sub(1);
        }
    }
    return nullptr;
}
} // namespace

TEST(BthreadSharedMutexTest, MixedReaderWriterInvariants) {
    bthread_setconcurrency(8);
    BthreadSharedMutex m;
    std::atomic<int> active_readers {0};
    std::atomic<int> active_writers {0};
    std::atomic<bool> failed {false};

    constexpr int kBthreads = 16;
    constexpr int kIters = 500;
    std::vector<bthread_t> tids(kBthreads);
    std::vector<StressCtx> ctxs(kBthreads);
    for (int i = 0; i < kBthreads; ++i) {
        ctxs[i] = {&m, &active_readers, &active_writers, &failed, kIters, i};
        ASSERT_EQ(0, bthread_start_background(&tids[i], nullptr, stress_fn, &ctxs[i]));
    }
    for (auto t : tids) {
        ASSERT_EQ(0, bthread_join(t, nullptr));
    }
    ASSERT_FALSE(failed.load());
    ASSERT_EQ(0, active_readers.load());
    ASSERT_EQ(0, active_writers.load());
}

// 5) RAII drop-in coverage: BthreadSharedMutex must work with std::shared_lock
//    and std::unique_lock, which is what the call sites rely on.
TEST(BthreadSharedMutexTest, RaiiWrappers) {
    BthreadSharedMutex m;
    {
        std::shared_lock<BthreadSharedMutex> rlock(m);
        ASSERT_FALSE(m.try_lock()); // writer blocked while shared lock held
    }
    {
        std::unique_lock<BthreadSharedMutex> wlock(m);
        ASSERT_FALSE(m.try_lock_shared()); // reader blocked while exclusive lock held
    }
    // Adopt an already-held lock, mirroring the call-site pattern.
    ASSERT_TRUE(m.try_lock());
    { std::unique_lock<BthreadSharedMutex> wlock(m, std::adopt_lock); }
    ASSERT_TRUE(m.try_lock_shared()); // released by the adopting guard
    m.unlock_shared();
}

} // namespace doris
