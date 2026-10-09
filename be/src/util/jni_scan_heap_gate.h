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

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

#include "exec/scan/task_executor/listenable_future.h"

namespace doris {

class Thread;

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
// No thread waits here. A reader that does not fit opens nothing yet; its scanner ends its turn without
// a block and gives its worker back, and the scan scheduler runs it again once the admission's future is
// done. A thread that waited would hold a scan worker that the readers already admitted need for their
// next turns, and with it the place they need among the scheduler's active tasks, so that none of them
// could close and give its share back.
//
// Readers that do not fit wait in the order they came, so a large one is not passed over for ever by
// small ones arriving behind it. Two things admit a waiting reader whatever the account says: no
// reader holding a share (nobody would give one back for this reader, so one that declares more than
// the whole budget runs alone), and the wait outlasting jni_scanner_heap_max_wait_ms. The second is
// for readers whose holders cannot finish until the waiter does - a join whose build side waits for
// the probe side's shares, which the probe side keeps until the build is done. A reader whose scan
// stops while it waits - a cancelled query, a satisfied limit - leaves the line without a share and
// opens nothing: admitting it would let every waiting reader of a cancelled query open at once, above
// the budget.
//
// The gate has a thread of its own. It completes every admission's future, so that the scheduler's
// callbacks never run on a thread that just gave a share back while holding locks of its own; and
// every POLL_INTERVAL it asks the waiting readers whether their scans stopped, admits those that
// waited longer than jni_scanner_heap_max_wait_ms, and applies a changed budget.
class JniScanHeapGate {
public:
    // One reader's request: its place in line, then its share of the budget. Destroying it gives the
    // share back - the reader's Java scanner has closed - or leaves the line.
    class Admission {
    public:
        ~Admission();
        Admission(const Admission&) = delete;
        Admission& operator=(const Admission&) = delete;

        // Still in line: the reader must not open its Java scanner yet.
        bool waiting() const { return _state.load(std::memory_order_acquire) == State::WAITING; }
        // Holds its share. Not waiting and not admitted means the reader's scan stopped first.
        bool admitted() const { return _state.load(std::memory_order_acquire) == State::ADMITTED; }
        // Done once the reader stops waiting, admitted or not. Completed on the gate's thread.
        SharedListenableFuture<Void> future() const { return _future; }
        // How long the reader waited. Read it only once the reader stopped waiting.
        int64_t wait_ns() const { return _wait_ns; }

    private:
        friend class JniScanHeapGate;
        enum class State : int { WAITING, ADMITTED, STOPPED };

        Admission(JniScanHeapGate* gate, int64_t bytes, std::function<bool()> stop_waiting);

        JniScanHeapGate* const _gate;
        const int64_t _bytes;
        const std::function<bool()> _stop_waiting;
        // Set under the gate's lock before the reader can see it.
        uint64_t _ticket = 0;
        int64_t _start_ns = 0;
        // Written under the gate's lock before _state leaves WAITING, read after it has.
        int64_t _wait_ns = 0;
        std::atomic<State> _state {State::WAITING};
        SharedListenableFuture<Void> _future;
    };

    // `budget` answers how many bytes the admitted readers may declare together. It is asked every
    // time the gate looks, so a change of jni_scanner_heap_budget_ratio applies at once.
    explicit JniScanHeapGate(std::function<int64_t()> budget);
    ~JniScanHeapGate();

    // The gate every JNI reader of this process shares: its budget is jni_scanner_heap_budget_ratio
    // of the -Xmx the JVM was started with.
    static JniScanHeapGate* instance();

    // Joins the line for `bytes`. The admission returned is admitted at once when the reader is first
    // in line and fits (see the class comment), stopped at once when `stop_waiting` already says its
    // scan stopped, and waiting otherwise; a waiting one's future is done when it stops waiting.
    // `stop_waiting` is asked now and then every POLL_INTERVAL while the reader waits, on the gate's
    // thread and without its lock: it may only read state the reader shares ownership of, since it
    // can run while the reader is being destroyed, and must read it atomically, since whoever stops
    // the scan writes it on a thread of its own.
    std::unique_ptr<Admission> request(int64_t bytes, std::function<bool()> stop_waiting);

    int64_t admitted_bytes() const;
    int64_t holders() const;
    int64_t waiters() const;
    // Readers admitted because they waited longer than jni_scanner_heap_max_wait_ms, whatever the
    // account said.
    int64_t admitted_after_wait_limit() const;

private:
    using State = Admission::State;

    // The gate's thread.
    void _run();
    // Asks the waiting readers whether their scans stopped, admits those that waited too long, and
    // then whoever fits.
    void _poll(std::unique_lock<std::mutex>& lock);
    // Caller holds _lock. Admits the first in line for as long as it fits.
    void _admit_locked(int64_t now);
    // Caller holds _lock. `admission` has left _waiting.
    void _settle_locked(Admission* admission, State state, int64_t now);
    // ~Admission.
    void _leave(Admission* admission);

    const std::function<int64_t()> _budget;
    mutable std::mutex _lock;
    std::condition_variable _cv;
    // What the admitted readers declared, and how many of them there are.
    int64_t _admitted_bytes = 0;
    int64_t _holders = 0;
    // The readers waiting, in the order they came: only the first may be admitted by the account.
    std::deque<Admission*> _waiting;
    uint64_t _next_ticket = 0;
    // Futures of readers that stopped waiting, for the gate's thread to complete.
    std::vector<SharedListenableFuture<Void>> _settled;
    int64_t _admitted_after_wait_limit = 0;
    bool _stopping = false;
    std::shared_ptr<Thread> _thread;
};

} // namespace doris
