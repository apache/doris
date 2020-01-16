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
#include <mutex>

namespace {

class Semaphore {
public:
    explicit Semaphore(int count = 0) : _count(count) {}

    void set_count(int count) { _count = count; }

    void signal() {
        std::unique_lock<std::mutex> lock(_mutex);
        ++_count;
        _cv.notify_one();
    }

    void wait() {
        std::unique_lock<std::mutex> lock(_mutex);
        _cv.wait(lock, [=] { return _count > 0; });
        --_count;
    }

private:
    std::mutex _mutex;
    std::condition_variable _cv;
    int _count;
};

} // end namespace
