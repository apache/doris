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

namespace doris::cloud {

class StopWatch {
public:
    StopWatch() {
        running_ = true;
        start_ = std::chrono::steady_clock::now();
    };
    ~StopWatch() = default;

    void start() {
        if (!running_) {
            start_ = std::chrono::steady_clock::now();
            running_ = true;
        }
    }

    void pause() {
        if (running_) {
            elapsed_ = elapsed_ + (std::chrono::steady_clock::now() - start_);
            running_ = false;
        }
    }

    void resume() {
        if (!running_) {
            start_ = std::chrono::steady_clock::now();
            running_ = true;
        }
    }

    void reset() {
        start_ = std::chrono::steady_clock::now();
        elapsed_ = std::chrono::steady_clock::duration {0};
        running_ = true;
    }

    int64_t elapsed_us() const {
        if (!running_) {
            return std::chrono::duration_cast<std::chrono::microseconds>(elapsed_).count();
        }

        auto end = std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::microseconds>(elapsed_ + (end - start_))
                .count();
    }

private:
    std::chrono::steady_clock::time_point start_;
    std::chrono::steady_clock::duration elapsed_ {0};
    bool running_ {false};
};

} // namespace doris::cloud