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

#include "util/timer.h"

namespace doris {

void Timer::start() {
    std::lock_guard<std::mutex> lk(_mutex);
    if (!_stopped) return;
    _stopped = false;
    _thread = std::thread([this] { _timer_loop(); });
}

void Timer::stop() {
    {
        std::lock_guard<std::mutex> lk(_mutex);
        if (_stopped) return;
        _stopped = true;
        _schedule.clear();
        _id_to_time.clear();
        _cancelled_during_run.clear();
    }
    _cond.notify_all();
    if (_thread.joinable()) _thread.join();
}

Timer::EventId Timer::schedule_after(Duration delay, Callback cb) {
    std::lock_guard<std::mutex> lk(_mutex);
    if (_stopped) return kInvalidId;
    TimePoint fire_time = Clock::now() + delay;
    return _schedule_locked(fire_time, Duration::zero(), std::move(cb));
}

Timer::EventId Timer::schedule_recurring(Duration interval, Callback cb) {
    std::lock_guard<std::mutex> lk(_mutex);
    if (_stopped) return kInvalidId;
    TimePoint fire_time = Clock::now() + interval;
    return _schedule_locked(fire_time, interval, std::move(cb));
}

void Timer::cancel(EventId id) {
    if (id == kInvalidId) return;
    std::lock_guard<std::mutex> lk(_mutex);
    auto it = _id_to_time.find(id);
    if (it != _id_to_time.end()) {
        // Event is still in the queue — remove it directly.
        _schedule.erase({it->second, id, {}, {}});
        _id_to_time.erase(it);
        _cond.notify_all();
    } else {
        // Event may currently be executing (its entry was popped from the queue).
        // Mark it so the loop skips rescheduling after the callback returns.
        _cancelled_during_run.insert(id);
    }
}

// Internal: insert an event and wake the timer thread if needed.
// Caller must hold _mutex.
Timer::EventId Timer::_schedule_locked(TimePoint fire_time, Duration interval, Callback cb) {
    EventId id = _next_id++;
    Event ev {fire_time, id, std::move(cb), interval};
    bool wake = _schedule.empty() || fire_time < _schedule.begin()->fire_time;
    _schedule.insert(std::move(ev));
    _id_to_time[id] = fire_time;
    if (wake) _cond.notify_all();
    return id;
}

void Timer::_timer_loop() {
    std::unique_lock<std::mutex> lk(_mutex);
    while (!_stopped) {
        if (_schedule.empty()) {
            _cond.wait(lk);
            continue;
        }

        auto it = _schedule.begin();
        TimePoint next_fire = it->fire_time;
        if (_cond.wait_until(lk, next_fire) == std::cv_status::timeout ||
            Clock::now() >= next_fire) {
            // Re-check: another thread may have cancelled or added an earlier event.
            it = _schedule.begin();
            if (it == _schedule.end() || Clock::now() < it->fire_time) continue;

            // Pop the event.
            Event ev = *it;
            _schedule.erase(it);
            _id_to_time.erase(ev.id);

            // Run callback without holding the lock.
            lk.unlock();
            ev.cb();
            lk.lock();

            if (_stopped) break;

            // Reschedule if recurring and not cancelled during execution.
            if (ev.interval != Duration::zero()) {
                if (_cancelled_during_run.erase(ev.id) == 0) {
                    TimePoint next = ev.fire_time + ev.interval;
                    // Avoid drift: if we're already behind, fire immediately next tick.
                    if (next < Clock::now()) next = Clock::now() + ev.interval;
                    // Reuse the same id so callers can still cancel via the original id.
                    Event next_ev {next, ev.id, ev.cb, ev.interval};
                    bool wake = _schedule.empty() || next < _schedule.begin()->fire_time;
                    _schedule.insert(std::move(next_ev));
                    _id_to_time[ev.id] = next;
                    if (wake) _cond.notify_all();
                }
            }
        }
    }
}

} // namespace doris
