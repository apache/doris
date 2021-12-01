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

#ifndef DORIS_BE_RUNTIME_THREAD_RESOURCE_MGR_H
#define DORIS_BE_RUNTIME_THREAD_RESOURCE_MGR_H

#include <stdlib.h>

#include <functional>
#include <list>
#include <mutex>
#include <thread>

#include "common/status.h"

namespace doris {

// Singleton object to manage CPU (aka thread) resources for the process.
// Conceptually, there is a fixed pool of threads that are shared between
// query fragments.  If there is only one fragment running, it can use the
// entire pool, spinning up the maximum number of threads to saturate the
// hardware.  If there are multiple fragments, the CPU pool must be shared
// between them.  Currently, the total system pool is split evenly between
// all consumers.  Each consumer gets ceil(total_system_threads / num_consumers).
//
// Each fragment must register with the ThreadResourceMgr to request threads
// (in the form of tokens).  The fragment has required threads (it can't run
// with fewer threads) and optional threads.  If the fragment is running on its
// own, it will be able to spin up more optional threads.  When the system
// is under load, the ThreadResourceMgr will stop giving out tokens for optional
// threads.
// Pools should not use this for threads that are almost always idle (e.g.
// periodic reporting threads).
// Pools will temporarily go over the quota regularly and this is very
// much by design.  For example, if a pool is running on its own with
// 4 required threads and 28 optional and another pool is added to the
// system, the first pool's quota is then cut by half (16 total) and will
// over time drop the optional threads.
// This class is thread safe.
// TODO: this is an initial simple version to improve the behavior with
// concurrency.  This will need to be expanded post GA.  These include:
//  - More places where threads are optional (e.g. hash table build side,
//    data stream threads, etc).
//  - Admission control
//  - Integration with other nodes/statestore
//  - Priorities for different pools
// If both the mgr and pool locks need to be taken, the mgr lock must
// be taken first.
class ThreadResourceMgr {
public:
    class ResourcePool;

    // This function will be called whenever the pool has more threads it can run on.
    // This can happen on ReleaseThreadToken or if the quota for this pool increases.
    // This is a good place, for example, to wake up anything blocked on available threads.
    // This callback must not block.
    // Note that this is not called once for each available thread or even guaranteed that
    // when it is called, a thread is available (the quota could have changed again in
    // between).  It is simply that something might have happened (similar to condition
    // variable semantics).
    // TODO: this is manageable now since it just needs to call into the io
    // mgr.  What's the best model for something more general.
    typedef std::function<void(ResourcePool*)> thread_available_cb;

    // Pool abstraction for a single resource pool.
    // TODO: this is not quite sufficient going forward.  We need a hierarchy of pools,
    // one for the entire query, and a sub pool for each component that needs threads,
    // all of which share a quota.  Currently, the way state is tracked here, it would
    // be impossible to have two components both want optional threads (e.g. two things
    // that have 1+ thread usage).
    class ResourcePool {
    public:
        virtual ~ResourcePool() {};
        // Acquire a thread for the pool.  This will always succeed; the
        // pool will go over the quota.
        // Pools should use this API to reserve threads they need in order
        // to make progress.
        void acquire_thread_token();

        // Try to acquire a thread for this pool.  If the pool is at
        // the quota, this will return false and the pool should not run.
        // Pools should use this API for resources they can use but don't
        // need (e.g. scanner threads).
        bool try_acquire_thread_token();

        // Set a reserved optional number of threads for this pool.  This can be
        // used to implement that a component needs n+ number of threads.  The
        // first 'num' threads are guaranteed to be acquirable (via try_acquire_thread_token)
        // but anything beyond can fail.
        // This can also be done with:
        //  if (pool->num_optional_threads() < num) acquire_thread_token();
        //  else try_acquire_thread_token();
        // and similar tracking on the Release side but this is common enough to
        // abstract it away.
        void reserve_optional_tokens(int num);

        // Release a thread for the pool.  This must be called once for
        // each call to acquire_thread_token and each successful call to try_acquire_thread_token
        // If the thread token is from acquire_thread_token, required must be true; false
        // if from try_acquire_thread_token.
        // Must not be called from from thread_available_cb.
        void release_thread_token(bool required);

        // Returns the number of threads that are from acquire_thread_token.
        int num_required_threads() const { return _num_threads & 0xFFFFFFFF; }

        // Returns the number of thread resources returned by successful calls
        // to try_acquire_thread_token.
        int num_optional_threads() const { return _num_threads >> 32; }

        // Returns the total number of thread resources for this pool
        // (i.e. num_optional_threads + num_required_threads).
        int64_t num_threads() const { return num_required_threads() + num_optional_threads(); }

        // Returns the number of optional threads that can still be used.
        int num_available_threads() const {
            int value = std::max(quota() - static_cast<int>(num_threads()),
                                 _num_reserved_optional_threads - num_optional_threads());
            return std::max(0, value);
        }

        // Returns the quota for this pool.  Note this changes dynamically
        // based on system load.
        int quota() const { return std::min(_max_quota, _parent->_per_pool_quota); }

        // Sets the max thread quota for this pool.  This is only used for testing since
        // the dynamic values should be used normally.  The actual quota is the min of this
        // value and the dynamic quota.
        void set_max_quota(int quota) { _max_quota = quota; }

    private:
        friend class ThreadResourceMgr;

        ResourcePool(ThreadResourceMgr* parent);

        // Resets internal state.
        void reset();

        ThreadResourceMgr* _parent;

        int _max_quota;
        int _num_reserved_optional_threads;

        // A single 64 bit value to store both the number of optional and
        // required threads.  This is combined to allow using compare and
        // swap operations.  The number of required threads is the lower
        // 32 bits and the number of optional threads is the upper 32 bits.
        int64_t _num_threads;
    };

    // Create a thread mgr object.  If threads_quota is non-zero, it will be
    // the number of threads for the system, otherwise it will be determined
    // based on the hardware.
    ThreadResourceMgr(int threads_quota);
    ThreadResourceMgr();
    ~ThreadResourceMgr();

    int system_threads_quota() const { return _system_threads_quota; }

    // Register a new pool with the thread mgr.  Registering a pool
    // will update the quotas for all existing pools.
    ResourcePool* register_pool();

    // Unregisters the pool.  'pool' is no longer valid after this.
    // This updates the quotas for the remaining pools.
    void unregister_pool(ResourcePool* pool);

private:
    // 'Optimal' number of threads for the entire process.
    int _system_threads_quota;

    // Lock for the entire object.  Protects all fields below.
    std::mutex _lock;

    // Pools currently being managed
    typedef std::set<ResourcePool*> Pools;
    Pools _pools;

    // Each pool currently gets the same share.  This is the ceil of the
    // system quota divided by the number of pools.
    int _per_pool_quota;

    // Recycled list of pool objects
    std::list<ResourcePool*> _free_pool_objs;

    void update_pool_quotas();
};

inline void ThreadResourceMgr::ResourcePool::acquire_thread_token() {
    __sync_fetch_and_add(&_num_threads, 1);
}

inline bool ThreadResourceMgr::ResourcePool::try_acquire_thread_token() {
    while (true) {
        int64_t previous_num_threads = _num_threads;
        int64_t new_optional_threads = (previous_num_threads >> 32) + 1;
        int64_t new_required_threads = previous_num_threads & 0xFFFFFFFF;

        if (new_optional_threads > _num_reserved_optional_threads &&
            new_optional_threads + new_required_threads > quota()) {
            return false;
        }

        int64_t new_value = new_optional_threads << 32 | new_required_threads;

        // Atomically swap the new value if no one updated _num_threads.  We do not
        // not care about the ABA problem here.
        if (__sync_bool_compare_and_swap(&_num_threads, previous_num_threads, new_value)) {
            return true;
        }
    }
}

inline void ThreadResourceMgr::ResourcePool::release_thread_token(bool required) {
    if (required) {
        DCHECK_GT(num_required_threads(), 0);
        __sync_fetch_and_add(&_num_threads, -1);
    } else {
        DCHECK_GT(num_optional_threads(), 0);

        while (true) {
            int64_t previous_num_threads = _num_threads;
            int64_t new_optional_threads = (previous_num_threads >> 32) - 1;
            int64_t new_required_threads = previous_num_threads & 0xFFFFFFFF;
            int64_t new_value = new_optional_threads << 32 | new_required_threads;

            if (__sync_bool_compare_and_swap(&_num_threads, previous_num_threads, new_value)) {
                break;
            }
        }
    }
}

} // namespace doris

#endif
