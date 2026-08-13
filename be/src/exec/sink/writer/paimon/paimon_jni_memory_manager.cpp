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
#include <atomic>
#include <limits>
#include <mutex>
#include <utility>
#include <vector>

#include "common/check.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logging.h"
#include "core/allocator.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/defer_op.h"
#include "util/jni-util.h"

namespace doris {

namespace {

class ProcessPaimonJniMemoryLimiter {
public:
    int64_t limit() const {
        const long double limit =
                static_cast<long double>(Jni::Util::get_max_jni_heap_memory_size()) *
                config::paimon_jni_memory_limit_ratio;
        if (limit >= static_cast<long double>(std::numeric_limits<int64_t>::max())) {
            return std::numeric_limits<int64_t>::max();
        }
        return std::max<int64_t>(1, static_cast<int64_t>(limit));
    }

    bool try_reserve(int64_t bytes) {
        if (bytes <= 0) {
            return bytes == 0;
        }
        const int64_t hard_limit = limit();
        int64_t current = _current_bytes.load(std::memory_order_relaxed);
        while (bytes <= hard_limit - current) {
            if (_current_bytes.compare_exchange_weak(current, current + bytes,
                                                     std::memory_order_acq_rel)) {
                update_peak(current + bytes);
                return true;
            }
        }
        _rejected_allocations.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    void release(int64_t bytes) {
        if (bytes == 0) {
            return;
        }
        DORIS_CHECK(bytes > 0);
        const int64_t previous = _current_bytes.fetch_sub(bytes, std::memory_order_acq_rel);
        DORIS_CHECK(bytes <= previous);
    }

    int64_t current_bytes() const { return _current_bytes.load(std::memory_order_relaxed); }
    int64_t peak_bytes() const { return _peak_bytes.load(std::memory_order_relaxed); }
    int64_t rejected_allocations() const {
        return _rejected_allocations.load(std::memory_order_relaxed);
    }

private:
    void update_peak(int64_t value) {
        int64_t peak = _peak_bytes.load(std::memory_order_relaxed);
        while (peak < value &&
               !_peak_bytes.compare_exchange_weak(peak, value, std::memory_order_relaxed)) {
        }
    }

    std::atomic<int64_t> _current_bytes {0};
    std::atomic<int64_t> _peak_bytes {0};
    std::atomic<int64_t> _rejected_allocations {0};
};

ProcessPaimonJniMemoryLimiter& process_memory_limiter() {
    static auto* limiter = new ProcessPaimonJniMemoryLimiter();
    return *limiter;
}

} // namespace

class PaimonJniMemoryManager::Impl {
public:
    explicit Impl(std::shared_ptr<ResourceContext> resource_context)
            : _resource_context(std::move(resource_context)) {
        DORIS_CHECK(_resource_context != nullptr);
    }

    ~Impl() {
        try {
            release_all_pages();
            const int64_t unreleased_java_arrow_bytes =
                    _java_arrow_reserved_bytes.exchange(0, std::memory_order_acq_rel);
            if (unreleased_java_arrow_bytes > 0) {
                LOG(WARNING) << "Releasing unmatched Paimon Java Arrow memory: "
                             << unreleased_java_arrow_bytes << " bytes";
                process_memory_limiter().release(unreleased_java_arrow_bytes);
            }
        } catch (const std::exception& e) {
            LOG(WARNING) << "Failed to release Paimon JNI memory: " << e.what();
        } catch (...) {
            LOG(WARNING) << "Failed to release Paimon JNI memory: unknown exception";
        }
    }

    jobject allocate_page(JNIEnv* env, jint bytes) {
        if (bytes <= 0) {
            throw Exception(Status::InvalidArgument(
                    "Paimon JNI memory page size must be positive, actual={}", bytes));
        }
        if (!process_memory_limiter().try_reserve(bytes)) {
            throw Exception(Status::Error<ErrorCode::QUERY_MEMORY_EXCEEDED>(
                    "Paimon JNI process memory limit exceeded: requested={}, current={}, limit={}",
                    bytes, process_memory_limiter().current_bytes(),
                    process_memory_limiter().limit()));
        }
        bool reservation_committed = false;
        Defer rollback_reservation {[&]() {
            if (!reservation_committed) {
                process_memory_limiter().release(bytes);
            }
        }};

        void* address = with_resource_context([&]() {
            enable_thread_catch_bad_alloc++;
            Defer restore_bad_alloc_catch {[&]() { enable_thread_catch_bad_alloc--; }};
            void* allocated = _allocator.alloc(static_cast<size_t>(bytes));
            try {
                std::lock_guard<std::mutex> lock(_mutex);
                _allocations.emplace_back(allocated, static_cast<size_t>(bytes));
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

        jobject buffer = env->NewDirectByteBuffer(address, bytes);
        if (buffer == nullptr || env->ExceptionCheck()) {
            remove_and_free_page(address, static_cast<size_t>(bytes));
            return nullptr;
        }
        return buffer;
    }

    bool try_reserve_java_arrow(int64_t bytes) {
        if (bytes < 0 || !process_memory_limiter().try_reserve(bytes)) {
            return false;
        }
        _java_arrow_reserved_bytes.fetch_add(bytes, std::memory_order_relaxed);
        return true;
    }

    void release_java_arrow(int64_t bytes) {
        if (bytes < 0) {
            throw Exception(Status::InvalidArgument(
                    "Paimon Java Arrow release size must not be negative, actual={}", bytes));
        }
        int64_t current = _java_arrow_reserved_bytes.load(std::memory_order_relaxed);
        while (bytes <= current) {
            if (_java_arrow_reserved_bytes.compare_exchange_weak(current, current - bytes,
                                                                 std::memory_order_acq_rel)) {
                process_memory_limiter().release(bytes);
                return;
            }
        }
        throw Exception(Status::InternalError(
                "Paimon Java Arrow memory accounting mismatch: released={}, reserved={}", bytes,
                current));
    }

    int64_t memory_limit() const { return process_memory_limiter().limit(); }

    int64_t native_allocated_bytes() const {
        std::lock_guard<std::mutex> lock(_mutex);
        return _native_allocated_bytes;
    }

    int64_t native_peak_allocated_bytes() const {
        std::lock_guard<std::mutex> lock(_mutex);
        return _native_peak_allocated_bytes;
    }

private:
    template <typename Function>
    auto with_resource_context(Function&& function)
            -> decltype(std::forward<Function>(function)()) {
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
                process_memory_limiter().release(bytes);
            }
        });
    }

    void remove_and_free_page(void* address, size_t bytes) {
        {
            std::lock_guard<std::mutex> lock(_mutex);
            auto it = std::find_if(
                    _allocations.begin(), _allocations.end(),
                    [&](const auto& allocation) { return allocation.first == address; });
            DORIS_CHECK(it != _allocations.end());
            _allocations.erase(it);
            _native_allocated_bytes -= bytes;
        }
        with_resource_context([&]() { _allocator.free(address, bytes); });
        process_memory_limiter().release(bytes);
    }

    std::shared_ptr<ResourceContext> _resource_context;
    Allocator<false> _allocator;
    mutable std::mutex _mutex;
    std::vector<std::pair<void*, size_t>> _allocations;
    int64_t _native_allocated_bytes = 0;
    int64_t _native_peak_allocated_bytes = 0;
    std::atomic<int64_t> _java_arrow_reserved_bytes {0};
};

namespace {

jobject allocate_paimon_memory_page(JNIEnv* env, jclass, jlong manager_handle, jint bytes) {
    auto* manager = reinterpret_cast<PaimonJniMemoryManager*>(manager_handle);
    if (manager == nullptr) {
        jclass exception_class = env->FindClass("java/lang/IllegalStateException");
        env->ThrowNew(exception_class, "Paimon JNI memory manager is null");
        env->DeleteLocalRef(exception_class);
        return nullptr;
    }
    try {
        return manager->allocate_page(env, bytes);
    } catch (const std::exception& e) {
        jclass exception_class = env->FindClass("java/lang/OutOfMemoryError");
        env->ThrowNew(exception_class, e.what());
        env->DeleteLocalRef(exception_class);
        return nullptr;
    }
}

jboolean reserve_paimon_java_arrow_memory(JNIEnv* env, jclass, jlong manager_handle, jlong bytes) {
    auto* manager = reinterpret_cast<PaimonJniMemoryManager*>(manager_handle);
    if (manager == nullptr) {
        jclass exception_class = env->FindClass("java/lang/IllegalStateException");
        env->ThrowNew(exception_class, "Paimon JNI memory manager is null");
        env->DeleteLocalRef(exception_class);
        return JNI_FALSE;
    }
    return manager->try_reserve_java_arrow(bytes) ? JNI_TRUE : JNI_FALSE;
}

void release_paimon_java_arrow_memory(JNIEnv* env, jclass, jlong manager_handle, jlong bytes) {
    auto* manager = reinterpret_cast<PaimonJniMemoryManager*>(manager_handle);
    if (manager == nullptr) {
        jclass exception_class = env->FindClass("java/lang/IllegalStateException");
        env->ThrowNew(exception_class, "Paimon JNI memory manager is null");
        env->DeleteLocalRef(exception_class);
        return;
    }
    try {
        manager->release_java_arrow(bytes);
    } catch (const std::exception& e) {
        jclass exception_class = env->FindClass("java/lang/IllegalStateException");
        env->ThrowNew(exception_class, e.what());
        env->DeleteLocalRef(exception_class);
    }
}

} // namespace

PaimonJniMemoryManager::PaimonJniMemoryManager(std::unique_ptr<Impl> impl)
        : _impl(std::move(impl)) {}

PaimonJniMemoryManager::~PaimonJniMemoryManager() = default;

Status PaimonJniMemoryManager::create(RuntimeState* state,
                                      std::unique_ptr<PaimonJniMemoryManager>* manager) {
    DORIS_CHECK(state != nullptr);
    DORIS_CHECK(manager != nullptr);
    if (state->get_query_ctx() == nullptr) {
        return Status::InternalError("Paimon JNI writer requires a QueryContext");
    }
    auto impl = std::make_unique<Impl>(state->get_query_ctx()->resource_ctx());
    *manager = std::unique_ptr<PaimonJniMemoryManager>(new PaimonJniMemoryManager(std::move(impl)));
    return Status::OK();
}

Status PaimonJniMemoryManager::register_natives(JNIEnv* env, jclass writer_class) {
    static char allocate_name[] = "allocatePaimonMemoryPage";
    static char allocate_signature[] = "(JI)Ljava/nio/ByteBuffer;";
    static char reserve_arrow_name[] = "reservePaimonJavaArrowMemory";
    static char reserve_arrow_signature[] = "(JJ)Z";
    static char release_arrow_name[] = "releasePaimonJavaArrowMemory";
    static char release_arrow_signature[] = "(JJ)V";
    static ::JNINativeMethod methods[] = {
            {allocate_name, allocate_signature,
             reinterpret_cast<void*>(&allocate_paimon_memory_page)},
            {reserve_arrow_name, reserve_arrow_signature,
             reinterpret_cast<void*>(&reserve_paimon_java_arrow_memory)},
            {release_arrow_name, release_arrow_signature,
             reinterpret_cast<void*>(&release_paimon_java_arrow_memory)},
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

bool PaimonJniMemoryManager::try_reserve_java_arrow(jlong bytes) {
    return _impl->try_reserve_java_arrow(bytes);
}

void PaimonJniMemoryManager::release_java_arrow(jlong bytes) {
    _impl->release_java_arrow(bytes);
}

int64_t PaimonJniMemoryManager::memory_limit() const {
    return _impl->memory_limit();
}

int64_t PaimonJniMemoryManager::native_allocated_bytes() const {
    return _impl->native_allocated_bytes();
}

int64_t PaimonJniMemoryManager::native_peak_allocated_bytes() const {
    return _impl->native_peak_allocated_bytes();
}

int64_t PaimonJniMemoryManager::global_current_bytes() const {
    return process_memory_limiter().current_bytes();
}

int64_t PaimonJniMemoryManager::global_peak_bytes() const {
    return process_memory_limiter().peak_bytes();
}

int64_t PaimonJniMemoryManager::global_rejected_allocations() const {
    return process_memory_limiter().rejected_allocations();
}

} // namespace doris
