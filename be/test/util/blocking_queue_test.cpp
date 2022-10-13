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

#include "util/blocking_queue.hpp"

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <mutex>
#include <thread>

namespace doris {

TEST(BlockingQueueTest, TestBasic) {
    int32_t i;
    BlockingQueue<int32_t> test_queue(5);
    EXPECT_TRUE(test_queue.blocking_put(1));
    EXPECT_TRUE(test_queue.blocking_put(2));
    EXPECT_TRUE(test_queue.blocking_put(3));
    EXPECT_TRUE(test_queue.blocking_get(&i));
    EXPECT_EQ(1, i);
    EXPECT_TRUE(test_queue.blocking_get(&i));
    EXPECT_EQ(2, i);
    EXPECT_TRUE(test_queue.blocking_get(&i));
    EXPECT_EQ(3, i);
}

TEST(BlockingQueueTest, TestGetFromShutdownQueue) {
    int64_t i;
    BlockingQueue<int64_t> test_queue(2);
    EXPECT_TRUE(test_queue.blocking_put(123));
    test_queue.shutdown();
    EXPECT_FALSE(test_queue.blocking_put(456));
    EXPECT_TRUE(test_queue.blocking_get(&i));
    EXPECT_EQ(123, i);
    EXPECT_FALSE(test_queue.blocking_get(&i));
}

class MultiThreadTest {
public:
    MultiThreadTest()
            : _iterations(10000),
              _nthreads(5),
              _queue(_iterations * _nthreads / 10),
              _num_inserters(_nthreads) {}

    void inserter_thread(int arg) {
        for (int i = 0; i < _iterations; ++i) {
            _queue.blocking_put(arg);
        }

        {
            std::lock_guard<std::mutex> guard(_lock);

            if (--_num_inserters == 0) {
                _queue.shutdown();
            }
        }
    }

    void RemoverThread() {
        for (int i = 0; i < _iterations; ++i) {
            int32_t arg;
            bool got = _queue.blocking_get(&arg);

            if (!got) {
                arg = -1;
            }

            {
                std::lock_guard<std::mutex> guard(_lock);
                _gotten[arg] = _gotten[arg] + 1;
            }
        }
    }

    void Run() {
        for (int i = 0; i < _nthreads; ++i) {
            _threads.push_back(std::shared_ptr<std::thread>(
                    new std::thread(std::bind(&MultiThreadTest::inserter_thread, this, i))));
            _threads.push_back(std::shared_ptr<std::thread>(
                    new std::thread(std::bind(&MultiThreadTest::RemoverThread, this))));
        }

        // We add an extra thread to ensure that there aren't enough elements in
        // the queue to go around.  This way, we test removal after shutdown.
        _threads.push_back(std::shared_ptr<std::thread>(
                new std::thread(std::bind(&MultiThreadTest::RemoverThread, this))));

        for (int i = 0; i < _threads.size(); ++i) {
            _threads[i]->join();
        }

        // Let's check to make sure we got what we should have.
        std::lock_guard<std::mutex> guard(_lock);

        for (int i = 0; i < _nthreads; ++i) {
            EXPECT_EQ(_iterations, _gotten[i]);
        }

        // And there were _nthreads * (_iterations + 1)  elements removed, but only
        // _nthreads * _iterations elements added.  So some removers hit the shutdown
        // case.
        EXPECT_EQ(_iterations, _gotten[-1]);
    }

private:
    typedef std::vector<std::shared_ptr<std::thread>> ThreadVector;

    int _iterations;
    int _nthreads;
    BlockingQueue<int32_t> _queue;
    // Lock for _gotten and _num_inserters.
    std::mutex _lock;
    // Map from inserter thread id to number of consumed elements from that id.
    // Ultimately, this should map each thread id to _insertions elements.
    // Additionally, if the blocking_get returns false, this increments id=-1.
    std::map<int32_t, int> _gotten;
    // All inserter and remover threads.
    ThreadVector _threads;
    // Number of inserters which haven't yet finished inserting.
    int _num_inserters;
};

TEST(BlockingQueueTest, TestMultipleThreads) {
    MultiThreadTest test;
    test.Run();
}

} // namespace doris
