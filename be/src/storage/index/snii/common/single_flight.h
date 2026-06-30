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

#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

namespace doris::snii {

// Collapses concurrent identical operations (same key) into a single execution.
//
// Motivating case: under a cold cache, Doris parallel scanners _lazy_init the same segment
// concurrently and would each miss the inverted-index caches and redundantly open + decode
// that segment's index. With single-flight the first caller (the leader) executes while the
// concurrent callers (followers) await and reuse its result -- so the duplicated open/decode
// is eliminated.
//
// No blocking work runs under the internal mutex: the leader performs its (potentially IO
// bound) work *after* join_or_lead() returns, holding no lock, and followers block on a
// std::future rather than the mutex. This preserves the SNII "no IO under lock" rule.
//
// Result must be copyable -- each follower receives its own copy. For large payloads use a
// shared_ptr (which is also how Doris query bitmaps are shared across cache consumers), so
// the copy is just a refcount bump and the payload itself is shared read-only.
template <typename Result>
class SingleFlight {
public:
    using ResultFuture = std::shared_future<Result>;

    // If `key` is already in flight, returns the leader's future and the caller is a
    // FOLLOWER: await the future, do not publish. Otherwise registers `key` as in flight,
    // returns std::nullopt, and the caller is the LEADER and MUST call publish(key, ...)
    // exactly once -- use a scope guard so it runs even on an error/early return.
    std::optional<ResultFuture> join_or_lead(const std::string& key) {
        std::lock_guard<std::mutex> guard(_mutex);
        if (auto it = _inflight.find(key); it != _inflight.end()) {
            return it->second->future;
        }
        auto flight = std::make_shared<Flight>();
        flight->future = flight->promise.get_future().share();
        _inflight.emplace(key, std::move(flight));
        return std::nullopt;
    }

    // Publishes the leader's result, wakes all waiting followers, and clears the in-flight
    // entry so the next caller of the same key leads a fresh execution. A no-op if the entry
    // is already gone (defensive; should not happen for a correct leader).
    void publish(const std::string& key, Result result) {
        std::shared_ptr<Flight> flight;
        {
            std::lock_guard<std::mutex> guard(_mutex);
            auto it = _inflight.find(key);
            if (it == _inflight.end()) {
                return;
            }
            flight = std::move(it->second);
            _inflight.erase(it);
        }
        // set_value() outside the lock: it may wake followers, and we never hold the mutex
        // across that hand-off.
        flight->promise.set_value(std::move(result));
    }

    // Number of keys currently in flight. For tests/observability only.
    size_t inflight_size() const {
        std::lock_guard<std::mutex> guard(_mutex);
        return _inflight.size();
    }

private:
    struct Flight {
        std::promise<Result> promise;
        ResultFuture future;
    };

    mutable std::mutex _mutex;
    std::unordered_map<std::string, std::shared_ptr<Flight>> _inflight;
};

} // namespace doris::snii
