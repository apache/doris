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
#include <deque>
#include <functional>
#include <mutex>

namespace doris {

// Admits Java scanners by the JVM heap they declare they will hold.
//
// Every JNI reader in BE runs its Java scanner in the one JVM BE starts. A few kinds hold a large part
// of its heap from before their first batch until they close: a paimon merge read keeps a row group of
// every file it merges, a fluss primary-key bucket read replays its change log into a map, a fluss
// union read keeps its whole log tail. Each fits; opened together - the sixteen scanners of one scan,
// or a few queries at once - they can run the heap out.
//
// The connector that plans such a range can say how much heap its reader will hold
// (TFileRangeDesc.jni_heap_bytes), and does so only for a statement that asks with the session variable
// enable_jni_heap_admission. It is off by default: then no reader declares anything, the gate admits
// nothing, and a scan that needs more heap than the JVM has fails with OutOfMemoryError and says how to
// give the JVM more.
//
// A reader that declared its heap asks the gate before it opens its Java scanner. The gate keeps an
// account of what the readers it admitted declared, admits the next one while the account plus this
// reader stays within jni_scanner_heap_budget_ratio of the JVM's maximum heap, and takes a reader's
// share back when its scanner closes. Nothing here looks at the heap itself: what other users of the
// JVM hold, and garbage nobody has collected yet, are not the gate's business.
//
// Readers that do not fit wait in the order they came, so a large one is not passed over for ever by
// small ones arriving behind it. Two things admit a waiting reader whatever the account says: no
// reader holding a share (nobody would give one back for this reader, so one that declares more than
// the whole budget runs alone), and the wait outlasting jni_scanner_heap_max_wait_ms. A reader whose
// scan stops while it waits - a cancelled query, a satisfied limit - leaves the line without a share
// and opens nothing: admitting it would let every waiting reader of a cancelled query open at once,
// above the budget.
class JniScanHeapGate {
public:
    // Held by a reader from before its Java scanner opens until that scanner is closed.
    class Permit {
    public:
        Permit() = default;
        ~Permit() { release(); }
        Permit(const Permit&) = delete;
        Permit& operator=(const Permit&) = delete;

        // The reader's Java scanner is closed. Idempotent; a permit never held is a no-op.
        void release();
        bool held() const { return _gate != nullptr; }

    private:
        friend class JniScanHeapGate;
        JniScanHeapGate* _gate = nullptr;
        int64_t _bytes = 0;
    };

    // `budget` answers how many bytes the admitted readers may declare together. It is asked every
    // time the gate looks, so a change of jni_scanner_heap_budget_ratio applies at once.
    explicit JniScanHeapGate(std::function<int64_t()> budget);

    // The gate every JNI reader of this process shares: its budget is jni_scanner_heap_budget_ratio
    // of the -Xmx the JVM was started with.
    static JniScanHeapGate* instance();

    // Waits until `bytes` fit (see the class comment), hands the reader `permit` for them and returns
    // true. `stop_waiting` is asked whenever the gate looks, the first time before any wait; once it
    // answers true the reader leaves the line without a permit and this returns false: its scan has
    // stopped, and it must not open its Java scanner. `wait_ns` gets the time spent here.
    [[nodiscard]] bool acquire(int64_t bytes, const std::function<bool()>& stop_waiting,
                               Permit* permit, int64_t* wait_ns);

    int64_t admitted_bytes() const;
    int64_t holders() const;
    int64_t waiters() const;

private:
    // Caller holds _lock.
    bool _fits(uint64_t ticket, int64_t bytes) const;
    void _release(int64_t bytes);

    const std::function<int64_t()> _budget;
    mutable std::mutex _lock;
    std::condition_variable _cv;
    // What the admitted readers declared, and how many of them there are.
    int64_t _admitted_bytes = 0;
    int64_t _holders = 0;
    // The readers waiting, in the order they came: only the first may be admitted by the account.
    std::deque<uint64_t> _waiting;
    uint64_t _next_ticket = 0;
};

} // namespace doris
