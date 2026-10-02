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

#pragma once

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>

#include "common/status.h"

namespace doris {

// Admits Java scanners into the JVM heap they all share.
//
// Every JNI reader in BE runs its Java scanner in the one JVM BE starts, and nothing else bounds
// what the scanners keep on its heap: a paimon merge read holds a row group of every file it
// merges, a fluss primary-key bucket read replays its change log into a map, a fluss union read
// keeps its whole log tail. Each fits; opened together - the sixteen scanners of one scan, or a few
// queries at once - they run the heap out.
//
// Those readers reach their peak before they produce their first batch. So a reader asks the gate
// before it opens its Java scanner, and the gate admits it when the heap measured now, plus a fixed
// reservation for every admitted reader that has not produced its first batch yet (its footprint is
// not in the measurement yet) and one for this reader, stays within a share of the JVM's maximum
// heap. A reader that has produced a batch is in the measurement and is no longer reserved for.
//
// A reader that is not admitted waits, and looks again whenever another reader produces its first
// batch or closes, and every poll interval in between (a GC frees memory without telling anybody).
// Three things end a wait whatever the heap says: no reader holding a permit (nobody would free
// memory for this one, so one reader at a time is the slowest the gate ever gets), the caller asking
// to stop waiting (a cancelled query), and the wait outlasting jni_scanner_heap_max_wait_ms.
class JniScanHeapGate {
public:
    struct HeapUsage {
        int64_t used = 0;
        int64_t max = 0;
    };
    using HeapProbe = std::function<Status(HeapUsage*)>;

    // Held by a reader from before its Java scanner opens until that scanner is closed.
    class Permit {
    public:
        Permit() = default;
        ~Permit() { release(); }
        Permit(const Permit&) = delete;
        Permit& operator=(const Permit&) = delete;

        // The reader produced its first batch, or reached its end without one. Idempotent.
        void opened();
        // The reader's Java scanner is closed. Idempotent; a permit never held is a no-op.
        void release();
        bool held() const { return _gate != nullptr; }

    private:
        friend class JniScanHeapGate;
        JniScanHeapGate* _gate = nullptr;
        bool _opening = false;
    };

    explicit JniScanHeapGate(HeapProbe probe);

    // The gate every JNI reader of this process shares; it measures the JVM's heap.
    static JniScanHeapGate* instance();

    // Waits until the heap has room for one more reader and hands that reader `permit`.
    // `stop_waiting` is asked whenever the gate looks again; once it answers true the reader is
    // admitted without further waiting, and is expected to notice the stop itself. `wait_ns` gets
    // the time spent here. Fails only when the heap cannot be measured.
    Status acquire(const std::function<bool()>& stop_waiting, Permit* permit, int64_t* wait_ns);

    int64_t active() const;
    int64_t opening() const;

private:
    // Caller holds _lock.
    bool _admissible(const HeapUsage& heap) const;
    void _opened();
    void _release(bool opening);

    const HeapProbe _probe;
    mutable std::mutex _lock;
    std::condition_variable _cv;
    // Permits held, and those among them whose reader has not produced a batch yet.
    int64_t _active = 0;
    int64_t _opening = 0;
};

} // namespace doris
