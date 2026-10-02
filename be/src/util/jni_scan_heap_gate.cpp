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

#include "util/jni_scan_heap_gate.h"

#include <chrono>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "util/jni-util.h"
#include "util/time.h"

namespace doris {

namespace {

// How often a waiting reader looks at the heap again although nobody told it to: a GC frees memory
// without a word.
constexpr auto POLL_INTERVAL = std::chrono::milliseconds(100);
constexpr int64_t MB = 1024 * 1024;

// The JVM's heap as java.lang.Runtime reports it, which answers without allocating: a measurement
// taken while the heap is nearly full must not be what fills it.
struct RuntimeHeap {
    Jni::GlobalClass cls;
    Jni::GlobalObject runtime;
    Jni::MethodId total_memory;
    Jni::MethodId free_memory;
    Jni::MethodId max_memory;
};

Status init_runtime_heap(RuntimeHeap* heap) {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    RETURN_IF_ERROR(Jni::Util::find_class(env, "java/lang/Runtime", &heap->cls));
    Jni::MethodId get_runtime;
    RETURN_IF_ERROR(
            heap->cls.get_static_method(env, "getRuntime", "()Ljava/lang/Runtime;", &get_runtime));
    RETURN_IF_ERROR(heap->cls.call_static_object_method(env, get_runtime).call(&heap->runtime));
    RETURN_IF_ERROR(heap->cls.get_method(env, "totalMemory", "()J", &heap->total_memory));
    RETURN_IF_ERROR(heap->cls.get_method(env, "freeMemory", "()J", &heap->free_memory));
    RETURN_IF_ERROR(heap->cls.get_method(env, "maxMemory", "()J", &heap->max_memory));
    return Status::OK();
}

Status measure_jvm_heap(JniScanHeapGate::HeapUsage* usage) {
    // Never destroyed, like the gate below: BE's exit runs static destructors while scanner
    // threads may still be measuring.
    static auto* heap = new RuntimeHeap();
    static std::once_flag init_once;
    static Status init_status;
    std::call_once(init_once, []() { init_status = init_runtime_heap(heap); });
    RETURN_IF_ERROR(init_status);

    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    jlong total = 0;
    jlong free = 0;
    jlong max = 0;
    RETURN_IF_ERROR(heap->runtime.call_long_method(env, heap->total_memory).call(&total));
    RETURN_IF_ERROR(heap->runtime.call_long_method(env, heap->free_memory).call(&free));
    RETURN_IF_ERROR(heap->runtime.call_long_method(env, heap->max_memory).call(&max));
    usage->used = total - free;
    usage->max = max;
    return Status::OK();
}

} // namespace

void JniScanHeapGate::Permit::opened() {
    if (_gate != nullptr && _opening) {
        _opening = false;
        _gate->_opened();
    }
}

void JniScanHeapGate::Permit::release() {
    if (_gate != nullptr) {
        _gate->_release(_opening);
        _gate = nullptr;
        _opening = false;
    }
}

JniScanHeapGate::JniScanHeapGate(HeapProbe probe) : _probe(std::move(probe)) {}

JniScanHeapGate* JniScanHeapGate::instance() {
    // Never destroyed: BE's exit runs static destructors while scanner threads are still alive, and
    // a reader closing then releases its permit into this gate.
    static auto* gate = new JniScanHeapGate(measure_jvm_heap);
    return gate;
}

Status JniScanHeapGate::acquire(const std::function<bool()>& stop_waiting, Permit* permit,
                                int64_t* wait_ns) {
    DORIS_CHECK(permit != nullptr);
    DORIS_CHECK(!permit->held());
    DORIS_CHECK(wait_ns != nullptr);
    const int64_t start = MonotonicNanos();
    while (true) {
        const bool enabled = config::enable_jni_scanner_heap_limiter;
        HeapUsage heap;
        // Measured before taking the lock: it is a call into the JVM, and a reader that waits measures
        // again each time it looks.
        if (enabled) {
            RETURN_IF_ERROR(_probe(&heap));
        }
        const bool stop = stop_waiting();
        std::unique_lock lock(_lock);
        const int64_t waited = MonotonicNanos() - start;
        bool admit = !enabled || stop || _admissible(heap);
        if (!admit && waited >= config::jni_scanner_heap_max_wait_ms * 1000 * 1000) {
            LOG_EVERY_T(WARNING, 10)
                    << "A JNI scanner opens after waiting " << waited / 1000 / 1000
                    << " ms for JVM heap, longer than jni_scanner_heap_max_wait_ms: "
                    << heap.used / MB << " MB used of " << heap.max / MB << " MB, " << _active
                    << " scanners open, " << _opening << " of them not past their first batch";
            admit = true;
        }
        if (admit) {
            ++_active;
            ++_opening;
            permit->_gate = this;
            permit->_opening = true;
            *wait_ns = waited;
            return Status::OK();
        }
        _cv.wait_for(lock, POLL_INTERVAL);
    }
}

bool JniScanHeapGate::_admissible(const HeapUsage& heap) const {
    if (_active == 0) {
        // Nobody would free memory for this reader, so waiting cannot help it.
        return true;
    }
    const auto limit = static_cast<int64_t>(static_cast<double>(heap.max) *
                                            config::jni_scanner_max_heap_usage_ratio);
    const int64_t reserved = config::jni_scanner_heap_reserved_mb_per_open * MB;
    return heap.used + (_opening + 1) * reserved <= limit;
}

void JniScanHeapGate::_opened() {
    std::lock_guard lock(_lock);
    DORIS_CHECK(_opening > 0);
    --_opening;
    _cv.notify_all();
}

void JniScanHeapGate::_release(bool opening) {
    std::lock_guard lock(_lock);
    DORIS_CHECK(_active > 0);
    --_active;
    if (opening) {
        DORIS_CHECK(_opening > 0);
        --_opening;
    }
    _cv.notify_all();
}

int64_t JniScanHeapGate::active() const {
    std::lock_guard lock(_lock);
    return _active;
}

int64_t JniScanHeapGate::opening() const {
    std::lock_guard lock(_lock);
    return _opening;
}

} // namespace doris
