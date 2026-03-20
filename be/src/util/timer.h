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

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <thread>

namespace doris {

// A generic single-threaded timer that dispatches callbacks at scheduled times.
//
// Events are stored in a time-sorted set. The internal thread sleeps via
// condition_variable::wait_until, wakes at the earliest deadline, fires the
// callback (without holding the lock), and reschedules recurring events.
//
// Thread safety: all public methods are thread-safe.
//
// Usage:
//   Timer timer;
//   timer.start();
//   auto id = timer.schedule_recurring(std::chrono::seconds(60), []{ ... });
//   timer.cancel(id);
//   timer.stop();
class Timer {
public:
    using EventId = uint64_t;
    using Callback = std::function<void()>;
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    using Duration = Clock::duration;

    static constexpr EventId kInvalidId = 0;

    Timer() = default;
    ~Timer() { stop(); }

    // Start the internal timer thread. Must be called before scheduling events.
    void start();

    // Stop the timer thread and discard all pending events. Idempotent.
    void stop();

    // Schedule a one-shot callback after `delay`.
    // Returns the event ID (kInvalidId on failure, e.g. timer not started).
    EventId schedule_after(Duration delay, Callback cb);

    // Schedule a recurring callback with the given `interval`.
    // The first firing happens after one interval.
    // Returns the event ID.
    EventId schedule_recurring(Duration interval, Callback cb);

    // Cancel a previously scheduled event. Safe to call from within a callback
    // or concurrently. No-op if the ID is invalid or already fired/cancelled.
    void cancel(EventId id);

private:
    struct Event {
        TimePoint fire_time;
        EventId id = 0;
        Callback cb;
        Duration interval {}; // zero for one-shot

        bool operator<(const Event& o) const {
            if (fire_time != o.fire_time) return fire_time < o.fire_time;
            return id < o.id;
        }
    };

    void _timer_loop();
    EventId _schedule_locked(TimePoint fire_time, Duration interval, Callback cb);

    mutable std::mutex _mutex;
    std::condition_variable _cond;
    bool _stopped = true;

    std::set<Event> _schedule;                // time-ordered event queue
    std::map<EventId, TimePoint> _id_to_time; // id → fire_time, for O(log n) cancel
    std::set<EventId> _cancelled_during_run;  // ids cancelled while callback was running

    EventId _next_id = 1;
    std::thread _thread;
};

} // namespace doris
