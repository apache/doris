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

namespace doris {

// used for submitter/worker/waiter pattern
// submitter:
//      one or more submitters submit tasks and call inc_count()
// worker:
//      one or more workers do the task and call dec_count() after finishing the task
// waiter:
//      one or more waiter call xxx_wait() to wait until all or at least one tasks are finished.
// Use pattern:
//      thread1(submitter):
//          CounterCondVariable cond(0);
//          ... submit task ...
//          cond.inc();
//          ... submit task ...
//          cond.inr();
//
//      thread2(worker):
//          ... do work...
//          cond.dec();
//          ... do work...
//          cond.dec();
//
//      thread3(waiter):
//          cond.block_wait();

class CounterCondVariable {
public:
    explicit CounterCondVariable(int init = 0) : _count(init) {}

    // increase the counter
    void inc(int inc = 1) {
        std::unique_lock<std::mutex> lock(_lock);
        _count += inc;
    }

    // decrease the counter, and notify all waiters when counter <= 0
    void dec(int dec = 1) {
        std::unique_lock<std::mutex> lock(_lock);
        _count -= dec;
        if (_count <= 0) {
            _cv.notify_all();
        }
    }

    // wait until count down to zero
    void block_wait() {
        std::unique_lock<std::mutex> lock(_lock);
        if (_count <= 0) {
            return;
        }
        _cv.wait(lock, [this] { return _count <= 0; });
    }

private:
    std::mutex _lock;
    std::condition_variable _cv;
    int _count;
};

} // namespace doris
