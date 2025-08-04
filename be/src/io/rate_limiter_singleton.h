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

#include <butil/macros.h>

#include <memory>

#include "rocksdb/rate_limiter.h"

namespace doris {
namespace io {
using RateLimiterRef = std::shared_ptr<rocksdb::RateLimiter>;
class RateLimiterSingleton {
public:
    static RateLimiterRef getInstance() {
        // !fix: need to check rate_bytes_per_sec
        static RateLimiterRef instance =
                std::shared_ptr<rocksdb::RateLimiter>(rocksdb::NewGenericRateLimiter(
                        1000000 /* rate_bytes_per_sec */, 1000000 /* refill_period_us */,
                        10 /* fairness */, rocksdb::RateLimiter::Mode::kAllIo /* mode */,
                        true /* auto_tuned */
                        ));
        return instance;
    }

private:
    RateLimiterSingleton() = default;
    ~RateLimiterSingleton() = default;

    DISALLOW_COPY_AND_ASSIGN(RateLimiterSingleton);
};

} // namespace io

} // namespace doris
