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

#include "exec/sink/writer/paimon/paimon_jni_memory_manager.h"

#include <algorithm>
#include <cstdio>
#include <mutex>
#include <utility>
#include <vector>

#include "common/check.h"
#include "common/exception.h"
#include "common/logging.h"
#include "core/allocator.h"
#include "exec/sink/writer/paimon/paimon_sink_memory_allocator.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "runtime/workload_management/task_controller.h"
#include "util/defer_op.h"
#include "util/jni-util.h"
#include "util/pretty_printer.h"

namespace doris {

class PaimonJniMemoryManager::Impl {
public:
    Impl(std::shared_ptr<ResourceContext> resource_context,
         std::shared_ptr<PaimonWriterMemoryLease> memory_lease)
            : _resource_context(std::move(resource_context)),
              _memory_lease(std::move(memory_lease)),
              _memory_limit(_memory_lease->memory_limit()) {
        DORIS_CHECK(_resource_context != nullptr);
        DORIS_CHECK(_memory_lease != nullptr);
        DORIS_CHECK(_memory_limit > 0);
    }

    ~Impl() {
        // Java may retain direct buffers until its writer is closed.  Release
        // every outstanding page here as the final native ownership boundary.
        Status release_status;
        try {
            release_all_pages();
        } catch (const std::exception& e) {
            LOG(WARNING) << "Failed to release Paimon JNI native memory: " << e.what();
            release_status = Status::InternalError("Failed to release Paimon JNI native memory: {}",
                                                   e.what());
        } catch (...) {
            LOG(WARNING) << "Failed to release Paimon JNI native memory: unknown exception";
            release_status = Status::InternalError(
                    "Failed to release Paimon JNI native memory: unknown exception");
        }
        if (!release_status.ok()) {
            _memory_lease->poison(release_status);
        }
        _memory_lease->release();
    }

    jobject allocate_page(JNIEnv* env, jint bytes) {
        if (bytes <= 0) {
            throw Exception(Status::InvalidArgument(
                    "Paimon JNI memory page size must be positive, actual={}", bytes));
        }

        // Reserve the writer-local budget before entering the allocator. This
        // prevents concurrent JNI callbacks from transiently allocating past
        // the configured cap and only discovering it after query accounting
        // or the system allocator has already rejected the request.
        Status limit_status;
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if (bytes > _memory_limit - _native_allocated_bytes - _native_reserved_bytes) {
                limit_status = Status::Error<ErrorCode::QUERY_MEMORY_EXCEEDED>(
                        "Paimon JNI write buffer exceeded its {} native memory limit",
                        PrettyPrinter::print_bytes(_memory_limit));
            } else {
                _native_reserved_bytes += bytes;
            }
        }
        if (!limit_status.ok()) {
            _memory_lease->poison(limit_status);
            throw Exception(limit_status);
        }
        bool reservation_committed = false;
        Defer rollback_reservation {[&]() {
            if (!reservation_committed) {
                std::lock_guard<std::mutex> lock(_mutex);
                _native_reserved_bytes -= bytes;
            }
        }};

        // Allocate and account while attached to the query's resource
        // context.  The callback can run on a JVM-created thread, so merely
        // relying on the calling BE thread's context would bypass query
        // memory accounting.
        void* address = with_resource_context([&]() {
            auto reserve_status = _reserve_memory(bytes);
            if (!reserve_status.ok()) {
                throw Exception(reserve_status);
            }

            enable_thread_catch_bad_alloc++;
            Defer restore_bad_alloc_catch {[&]() { enable_thread_catch_bad_alloc--; }};
            void* allocated = nullptr;
            {
                // try_reserve() has already checked and charged this page. The allocator hook
                // consumes that reservation, so checking the same limits again would double
                // count it.
                SCOPED_SKIP_MEMORY_CHECK();
                allocated = _allocator.alloc(static_cast<size_t>(bytes));
            }
            try {
                std::lock_guard<std::mutex> lock(_mutex);
                _allocations.emplace_back(allocated, static_cast<size_t>(bytes));
                _native_reserved_bytes -= bytes;
                _native_allocated_bytes += bytes;
                _native_peak_allocated_bytes =
                        std::max(_native_peak_allocated_bytes, _native_allocated_bytes);
                reservation_committed = true;
            } catch (...) {
                _allocator.free(allocated, static_cast<size_t>(bytes));
                throw;
            }
            return allocated;
        });

        // NewDirectByteBuffer does not copy memory; Paimon will read/write the
        // page directly.  If JNI rejects the address, undo the native
        // allocation and its accounting entry before returning.
        jobject buffer = env->NewDirectByteBuffer(address, bytes);
        if (buffer == nullptr || env->ExceptionCheck()) {
            remove_and_free_page(address, static_cast<size_t>(bytes));
            return nullptr;
        }
        return buffer;
    }

    int64_t memory_limit() const { return _memory_limit; }

    int64_t native_peak_allocated_bytes() const {
        std::lock_guard<std::mutex> lock(_mutex);
        return _native_peak_allocated_bytes;
    }

    void poison(const Status& status) { _memory_lease->poison(status); }

private:
    Status _reserve_memory(int64_t bytes) const {
        auto* task_controller = _resource_context->task_controller();
        if (task_controller->is_cancelled()) {
            auto status = Status::Cancelled(
                    "Paimon JNI native page allocation stopped because the query was cancelled");
            _memory_lease->poison(status);
            return status;
        }
        auto status = thread_context()->thread_mem_tracker_mgr->try_reserve(bytes);
        if (!status.ok()) {
            // This writer already owns the complete operator lease. A tracker rejection therefore
            // cannot be resolved by handing the lease to another LocalState; fail this sink and
            // wake its waiters instead of serially retrying the same impossible allocation.
            _memory_lease->poison(status);
        }
        return status;
    }

    template <typename Function>
    auto with_resource_context(Function&& function)
            -> decltype(std::forward<Function>(function)()) {
        // JNI normally re-enters on the attached blocking pipeline thread. Attach
        // Java-created threads explicitly too, so every allocation/free is
        // charged to the query rather than to an unrelated thread context.
        if (!pthread_context_ptr_init && bthread_self() == 0) {
            SCOPED_ATTACH_TASK(_resource_context);
            return std::forward<Function>(function)();
        }
        if (thread_context()->is_attach_task()) {
            SCOPED_SWITCH_RESOURCE_CONTEXT(_resource_context);
            return std::forward<Function>(function)();
        }
        SCOPED_ATTACH_TASK(_resource_context);
        return std::forward<Function>(function)();
    }

    void release_all_pages() {
        // Detach ownership from the bookkeeping vector under the lock, then
        // free outside the lock.  Allocator/free may invoke code that takes
        // unrelated locks and must not block page accounting readers.
        std::vector<std::pair<void*, size_t>> allocations;
        {
            std::lock_guard<std::mutex> lock(_mutex);
            allocations.swap(_allocations);
            _native_allocated_bytes = 0;
        }
        if (allocations.empty()) {
            return;
        }

        with_resource_context([&]() {
            for (const auto& [address, bytes] : allocations) {
                _allocator.free(address, bytes);
            }
            std::vector<std::pair<void*, size_t>>().swap(allocations);
        });
    }

    void remove_and_free_page(void* address, size_t bytes) {
        // Roll back a page whose Java direct-buffer wrapper could not be
        // created.  The address is removed under the same lock used by the
        // normal accounting path, while the potentially expensive free is
        // performed after releasing it.
        {
            std::lock_guard<std::mutex> lock(_mutex);
            auto it = std::find_if(
                    _allocations.begin(), _allocations.end(),
                    [&](const auto& allocation) { return allocation.first == address; });
            if (it != _allocations.end()) {
                _allocations.erase(it);
                _native_allocated_bytes -= bytes;
            }
        }
        with_resource_context([&]() { _allocator.free(address, bytes); });
    }

    // Query resource context used for all native allocator operations.
    std::shared_ptr<ResourceContext> _resource_context;
    // Keeps the operator admission lease until every Java-backed page has been released.
    std::shared_ptr<PaimonWriterMemoryLease> _memory_lease;
    // Immutable per-writer cap granted by the operator-scoped allocator.
    const int64_t _memory_limit;
    // Doris allocator used instead of JVM/Arrow allocation so native pages are
    // visible to Doris' memory accounting and allocator hooks.
    Allocator<false> _allocator;
    // Protects the allocation list and both usage counters.  JNI callbacks and
    // Java close/finalizer paths may arrive concurrently.
    mutable std::mutex _mutex;
    // Every entry is (native address, size) and remains here until released.
    std::vector<std::pair<void*, size_t>> _allocations;
    // Bytes reserved by callbacks which have passed the local limit check but
    // have not yet completed their allocator call.
    int64_t _native_reserved_bytes = 0;
    // Committed and high-water native page usage, respectively.
    int64_t _native_allocated_bytes = 0;
    int64_t _native_peak_allocated_bytes = 0;
};

namespace {

jobject allocate_paimon_memory_page(JNIEnv* env, jclass, jlong manager_handle, jint bytes) {
    // This is called from PaimonJniWriter's Java memory pool.  The handle is
    // the native manager address passed when the writer is opened; ownership
    // stays with the C++ writer/backend, so this callback must never delete it.
    auto* manager = reinterpret_cast<PaimonJniMemoryManager*>(manager_handle);
    if (manager == nullptr) {
        jclass exception_class = env->FindClass("java/lang/IllegalStateException");
        env->ThrowNew(exception_class, "Paimon JNI memory manager is null");
        env->DeleteLocalRef(exception_class);
        return nullptr;
    }
    try {
        return manager->allocate_page(env, bytes);
    } catch (const Exception& e) {
        const bool cancelled = e.code() == ErrorCode::CANCELLED;
        jclass exception_class =
                env->FindClass(cancelled ? "java/util/concurrent/CancellationException"
                                         : "java/lang/RuntimeException");
        // Avoid dynamic allocation while reporting a failed allocation.
        char message[1024];
        std::snprintf(message, sizeof(message), "Paimon JNI native page allocation failed: %.900s",
                      e.what());
        env->ThrowNew(exception_class, message);
        env->DeleteLocalRef(exception_class);
        return nullptr;
    } catch (const std::exception& e) {
        jclass exception_class = env->FindClass("java/lang/RuntimeException");
        char message[1024];
        std::snprintf(message, sizeof(message), "Paimon JNI native page allocation failed: %.900s",
                      e.what());
        env->ThrowNew(exception_class, message);
        env->DeleteLocalRef(exception_class);
        return nullptr;
    }
}

} // namespace

PaimonJniMemoryManager::PaimonJniMemoryManager(std::unique_ptr<Impl> impl)
        : _impl(std::move(impl)) {}

PaimonJniMemoryManager::~PaimonJniMemoryManager() = default;

Status PaimonJniMemoryManager::create(RuntimeState* state,
                                      std::shared_ptr<PaimonWriterMemoryLease> memory_lease,
                                      std::unique_ptr<PaimonJniMemoryManager>* manager) {
    DORIS_CHECK(state != nullptr);
    DORIS_CHECK(manager != nullptr);
    if (state->get_query_ctx() == nullptr) {
        return Status::InternalError(
                "Paimon JNI writer cannot allocate native memory without QueryContext");
    }

    DORIS_CHECK(memory_lease != nullptr);
    RETURN_IF_ERROR(memory_lease->check_ready());

    // ResourceContext is retained by Impl for the manager's whole lifetime so JNI callbacks stay
    // associated with the query even if Paimon invokes one from a Java-created thread.
    auto impl =
            std::make_unique<Impl>(state->get_query_ctx()->resource_ctx(), std::move(memory_lease));
    *manager = std::unique_ptr<PaimonJniMemoryManager>(new PaimonJniMemoryManager(std::move(impl)));
    return Status::OK();
}

Status PaimonJniMemoryManager::register_natives(JNIEnv* env, jclass writer_class) {
    // Keep the JNI surface minimal: Java asks native code only for a page;
    // all ownership, limits, and cleanup stay in PaimonJniMemoryManager.
    static char allocate_name[] = "allocatePaimonMemoryPage";
    static char allocate_signature[] = "(JI)Ljava/nio/ByteBuffer;";
    static ::JNINativeMethod methods[] = {
            {allocate_name, allocate_signature,
             reinterpret_cast<void*>(&allocate_paimon_memory_page)},
    };
    if (env->RegisterNatives(writer_class, methods,
                             static_cast<jint>(sizeof(methods) / sizeof(methods[0]))) != JNI_OK) {
        RETURN_IF_ERROR(Jni::Env::GetJniExceptionMsg(
                env, true, "JNI exception registering Paimon memory native methods: "));
        return Status::JniError("Failed to register Paimon memory native methods");
    }
    return Status::OK();
}

jobject PaimonJniMemoryManager::allocate_page(JNIEnv* env, jint bytes) {
    return _impl->allocate_page(env, bytes);
}

int64_t PaimonJniMemoryManager::memory_limit() const {
    return _impl->memory_limit();
}

int64_t PaimonJniMemoryManager::native_peak_allocated_bytes() const {
    return _impl->native_peak_allocated_bytes();
}

void PaimonJniMemoryManager::poison(const Status& status) {
    _impl->poison(status);
}

} // namespace doris
