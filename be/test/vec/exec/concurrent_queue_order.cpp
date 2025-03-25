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

#include <concurrentqueue.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

namespace doris::vectorized {

class ConcurrentQueueOrder : public testing::Test {
public:
    ConcurrentQueueOrder() = default;
    ~ConcurrentQueueOrder() override = default;
};
// The previously used moodycamel::ConcurrentQueue does not guarantee that the enqueue order matches the dequeue order,
// even when there is only a single producer and a single consumer.
// Refer to this issue: https://github.com/cameron314/concurrentqueue/issues/316
// We can use tokens to ensure the correct order.
TEST_F(ConcurrentQueueOrder, test_not_guarantee_order) {
    {
        moodycamel::ConcurrentQueue<int> data_queue;
        int num = 0;
        std::mutex m;
        std::atomic_bool flag = true;

        auto task = [&](int thread_id) {
            while (flag) {
                std::lock_guard lc {m};
                data_queue.enqueue(num++);
            }
        };
        std::thread input1(task, 0);
        std::thread input2(task, 1);
        std::thread input3(task, 2);

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        flag = false;

        input3.join();
        input1.join();
        input2.join();

        std::cout << "queue size " << data_queue.size_approx() << "\n";
        std::vector<int> outputs;
        int output;
        while (data_queue.try_dequeue(output)) {
            outputs.push_back(output);
        }

        EXPECT_FALSE(std::is_sorted(outputs.begin(), outputs.end()));
        std::cout << "output is sorted : " << std::is_sorted(outputs.begin(), outputs.end())
                  << "\n";
    }
}

TEST_F(ConcurrentQueueOrder, test_guarantee_order) {
    {
        moodycamel::ConcurrentQueue<int> data_queue;
        moodycamel::ProducerToken ptok {data_queue};
        int num = 0;
        std::mutex m;
        std::atomic_bool flag = true;

        auto task = [&](int thread_id) {
            while (flag) {
                std::lock_guard lc {m};
                data_queue.enqueue(ptok, num++);
            }
        };
        std::thread input1(task, 0);
        std::thread input2(task, 1);
        std::thread input3(task, 2);

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        flag = false;

        input3.join();
        input1.join();
        input2.join();

        std::cout << "queue size " << data_queue.size_approx() << "\n";
        std::vector<int> outputs;
        int output;
        while (data_queue.try_dequeue(output)) {
            outputs.push_back(output);
        }

        EXPECT_TRUE(std::is_sorted(outputs.begin(), outputs.end()));
        std::cout << "output is sorted : " << std::is_sorted(outputs.begin(), outputs.end())
                  << "\n";
    }
}
} // namespace doris::vectorized
