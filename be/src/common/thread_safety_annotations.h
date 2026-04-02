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

// Thread safety annotation macros and annotated mutex wrappers for
// Clang's -Wthread-safety static analysis.
// Reference: https://clang.llvm.org/docs/ThreadSafetyAnalysis.html

#pragma once

#include <mutex>

// Enable thread safety attributes only with clang.
// The attributes can be safely erased when compiling with other compilers.
#if defined(__clang__) && (!defined(SWIG))
#define THREAD_ANNOTATION_ATTRIBUTE__(x) __attribute__((x))
#else
#define THREAD_ANNOTATION_ATTRIBUTE__(x) // no-op
#endif

#define TSA_CAPABILITY(x) THREAD_ANNOTATION_ATTRIBUTE__(capability(x))

#define TSA_SCOPED_CAPABILITY THREAD_ANNOTATION_ATTRIBUTE__(scoped_lockable)

#define TSA_GUARDED_BY(x) THREAD_ANNOTATION_ATTRIBUTE__(guarded_by(x))

#define TSA_PT_GUARDED_BY(x) THREAD_ANNOTATION_ATTRIBUTE__(pt_guarded_by(x))

#define TSA_ACQUIRED_BEFORE(...) THREAD_ANNOTATION_ATTRIBUTE__(acquired_before(__VA_ARGS__))

#define TSA_ACQUIRED_AFTER(...) THREAD_ANNOTATION_ATTRIBUTE__(acquired_after(__VA_ARGS__))

#define TSA_REQUIRES(...) THREAD_ANNOTATION_ATTRIBUTE__(requires_capability(__VA_ARGS__))

#define TSA_REQUIRES_SHARED(...) \
    THREAD_ANNOTATION_ATTRIBUTE__(requires_shared_capability(__VA_ARGS__))

#define TSA_ACQUIRE(...) THREAD_ANNOTATION_ATTRIBUTE__(acquire_capability(__VA_ARGS__))

#define TSA_ACQUIRE_SHARED(...) \
    THREAD_ANNOTATION_ATTRIBUTE__(acquire_shared_capability(__VA_ARGS__))

#define TSA_RELEASE(...) THREAD_ANNOTATION_ATTRIBUTE__(release_capability(__VA_ARGS__))

#define TSA_RELEASE_SHARED(...) \
    THREAD_ANNOTATION_ATTRIBUTE__(release_shared_capability(__VA_ARGS__))

#define TSA_TRY_ACQUIRE(...) THREAD_ANNOTATION_ATTRIBUTE__(try_acquire_capability(__VA_ARGS__))

#define TSA_TRY_ACQUIRE_SHARED(...) \
    THREAD_ANNOTATION_ATTRIBUTE__(try_acquire_shared_capability(__VA_ARGS__))

#define TSA_EXCLUDES(...) THREAD_ANNOTATION_ATTRIBUTE__(locks_excluded(__VA_ARGS__))

#define TSA_ASSERT_CAPABILITY(x) THREAD_ANNOTATION_ATTRIBUTE__(assert_capability(x))

#define TSA_ASSERT_SHARED_CAPABILITY(x) THREAD_ANNOTATION_ATTRIBUTE__(assert_shared_capability(x))

#define TSA_RETURN_CAPABILITY(x) THREAD_ANNOTATION_ATTRIBUTE__(lock_returned(x))

#define TSA_NO_THREAD_SAFETY_ANALYSIS THREAD_ANNOTATION_ATTRIBUTE__(no_thread_safety_analysis)

// Annotated mutex wrapper for use with Clang thread safety analysis.
// Wraps std::mutex and provides the CAPABILITY annotation so that
// GUARDED_BY / REQUIRES / etc. annotations can reference it.
class TSA_CAPABILITY("mutex") AnnotatedMutex {
public:
    void lock() TSA_ACQUIRE() { _mutex.lock(); }
    void unlock() TSA_RELEASE() { _mutex.unlock(); }
    bool try_lock() TSA_TRY_ACQUIRE(true) { return _mutex.try_lock(); }

    // Access the underlying std::mutex (e.g., for std::condition_variable).
    // Use with care — this bypasses thread safety annotations.
    std::mutex& native_handle() { return _mutex; }

private:
    std::mutex _mutex;
};

// RAII scoped lock guard annotated for thread safety analysis.
template <typename MutexType>
class TSA_SCOPED_CAPABILITY AnnotatedLockGuard {
public:
    explicit AnnotatedLockGuard(MutexType& mu) TSA_ACQUIRE(mu) : _mu(mu) { _mu.lock(); }
    ~AnnotatedLockGuard() TSA_RELEASE() { _mu.unlock(); }

    AnnotatedLockGuard(const AnnotatedLockGuard&) = delete;
    AnnotatedLockGuard& operator=(const AnnotatedLockGuard&) = delete;

private:
    MutexType& _mu;
};
