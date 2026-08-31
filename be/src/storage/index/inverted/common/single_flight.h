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

#include <array>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

#include "common/check.h"

namespace doris::segment_v2::inverted_index {

// Collapses concurrent operations with the same key into one execution. No work
// or follower wake-up runs while the selected shard mutex is held.
template <typename Result>
class SingleFlight {
public:
    using ResultFuture = std::shared_future<Result>;

    std::optional<ResultFuture> join_or_lead(const std::string& key) {
        auto& shard = _shard_for(key);
        std::lock_guard<std::mutex> guard(shard.mutex);
        if (auto it = shard.inflight.find(key); it != shard.inflight.end()) {
            return it->second->future;
        }
        auto flight = std::make_shared<Flight>();
        flight->future = flight->promise.get_future().share();
        shard.inflight.emplace(key, std::move(flight));
        return std::nullopt;
    }

    void publish(const std::string& key, Result result) {
        auto& shard = _shard_for(key);
        std::shared_ptr<Flight> flight;
        {
            std::lock_guard<std::mutex> guard(shard.mutex);
            auto it = shard.inflight.find(key);
            if (it == shard.inflight.end() || it->second->publishing) {
                return;
            }
            flight = it->second;
            flight->publishing = true;
        }
        flight->promise.set_value(std::move(result));
        {
            std::lock_guard<std::mutex> guard(shard.mutex);
            auto it = shard.inflight.find(key);
            DORIS_CHECK(it != shard.inflight.end());
            DORIS_CHECK(it->second == flight);
            shard.inflight.erase(it);
        }
    }

    size_t inflight_size() const {
        std::array<std::unique_lock<std::mutex>, kShardCount> guards;
        for (size_t i = 0; i < kShardCount; ++i) {
            guards[i] = std::unique_lock(_shards[i].mutex);
        }

        size_t size = 0;
        for (const auto& shard : _shards) {
            size += shard.inflight.size();
        }
        return size;
    }

private:
    struct Flight {
        std::promise<Result> promise;
        ResultFuture future;
        bool publishing = false;
    };

    struct Shard {
        mutable std::mutex mutex;
        std::unordered_map<std::string, std::shared_ptr<Flight>> inflight;
    };

    static constexpr size_t kShardCount = 64;

    Shard& _shard_for(const std::string& key) {
        return _shards[std::hash<std::string> {}(key) % kShardCount];
    }

    std::array<Shard, kShardCount> _shards;
};

} // namespace doris::segment_v2::inverted_index
