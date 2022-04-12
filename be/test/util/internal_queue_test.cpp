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

#include "util/internal_queue.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <mutex>
#include <thread>

#include "common/configbase.h"
#include "testutil/test_util.h"
#include "util/logging.h"
#include "util/thread_group.h"

using std::vector;
using std::thread;

namespace doris {

struct IntNode : public InternalQueue<IntNode>::Node {
    IntNode() : value() {}
    IntNode(int value) : value(value) {}
    int value;
};

// Basic single threaded operation.
TEST(InternalQueue, TestBasic) {
    IntNode one(1);
    IntNode two(2);
    IntNode three(3);
    IntNode four(4);

    InternalQueue<IntNode> list;
    EXPECT_TRUE(list.empty());
    EXPECT_EQ(list.size(), 0);
    EXPECT_TRUE(list.dequeue() == nullptr);
    EXPECT_TRUE(list.validate());

    list.enqueue(&one);
    EXPECT_TRUE(!list.empty());
    EXPECT_EQ(list.size(), 1);
    IntNode* i = list.dequeue();
    EXPECT_TRUE(i != nullptr);
    EXPECT_TRUE(list.empty());
    EXPECT_EQ(list.size(), 0);
    EXPECT_EQ(i->value, 1);
    EXPECT_TRUE(list.validate());

    list.enqueue(&one);
    list.enqueue(&two);
    list.enqueue(&three);
    list.enqueue(&four);
    EXPECT_EQ(list.size(), 4);
    EXPECT_TRUE(list.validate());

    i = list.dequeue();
    EXPECT_TRUE(i != nullptr);
    EXPECT_EQ(i->value, 1);
    EXPECT_TRUE(list.validate());

    i = list.dequeue();
    EXPECT_TRUE(i != nullptr);
    EXPECT_EQ(i->value, 2);
    EXPECT_TRUE(list.validate());

    i = list.dequeue();
    EXPECT_TRUE(i != nullptr);
    EXPECT_EQ(i->value, 3);
    EXPECT_TRUE(list.validate());

    i = list.dequeue();
    EXPECT_TRUE(i != nullptr);
    EXPECT_EQ(i->value, 4);
    EXPECT_TRUE(list.validate());

    list.enqueue(&one);
    list.enqueue(&two);
    list.enqueue(&three);
    list.enqueue(&four);

    IntNode* node = list.head();
    int val = 1;
    while (node != nullptr) {
        EXPECT_EQ(node->value, val);
        node = node->next();
        ++val;
    }

    node = list.tail();
    val = 4;
    while (node != nullptr) {
        EXPECT_EQ(node->value, val);
        node = node->prev();
        --val;
    }

    for (int i = 0; i < 4; ++i) {
        node = list.pop_back();
        EXPECT_TRUE(node != nullptr);
        EXPECT_EQ(node->value, 4 - i);
        EXPECT_TRUE(list.validate());
    }
    EXPECT_TRUE(list.pop_back() == nullptr);
    EXPECT_EQ(list.size(), 0);
    EXPECT_TRUE(list.empty());
}

// Add all the nodes and then remove every other one.
TEST(InternalQueue, TestRemove) {
    std::vector<IntNode> nodes;
    nodes.resize(100);

    InternalQueue<IntNode> queue;

    queue.enqueue(&nodes[0]);
    queue.remove(&nodes[1]);
    EXPECT_TRUE(queue.validate());
    queue.remove(&nodes[0]);
    EXPECT_TRUE(queue.validate());
    queue.remove(&nodes[0]);
    EXPECT_TRUE(queue.validate());

    for (int i = 0; i < nodes.size(); ++i) {
        nodes[i].value = i;
        queue.enqueue(&nodes[i]);
    }

    for (int i = 0; i < nodes.size(); i += 2) {
        queue.remove(&nodes[i]);
        EXPECT_TRUE(queue.validate());
    }

    EXPECT_EQ(queue.size(), nodes.size() / 2);
    for (int i = 0; i < nodes.size() / 2; ++i) {
        IntNode* node = queue.dequeue();
        EXPECT_TRUE(node != nullptr);
        EXPECT_EQ(node->value, i * 2 + 1);
    }
}

const int VALIDATE_INTERVAL = 10000;

// CHECK() is not thread safe so return the result in *failed.
void ProducerThread(InternalQueue<IntNode>* queue, int num_inserts, std::vector<IntNode>* nodes,
                    std::atomic<int32_t>* counter, bool* failed) {
    for (int i = 0; i < num_inserts && !*failed; ++i) {
        // Get the next index to queue.
        int32_t value = (*counter)++;
        nodes->at(value).value = value;
        queue->enqueue(&nodes->at(value));
        if (i % VALIDATE_INTERVAL == 0) {
            if (!queue->validate()) {
                *failed = true;
            }
        }
    }
}

void ConsumerThread(InternalQueue<IntNode>* queue, int num_consumes, int delta,
                    std::vector<int>* results, bool* failed) {
    // Dequeued nodes should be strictly increasing.
    int previous_value = -1;
    for (int i = 0; i < num_consumes && !*failed;) {
        IntNode* node = queue->dequeue();
        if (node == nullptr) {
            continue;
        }
        ++i;
        if (delta > 0) {
            if (node->value != previous_value + delta) {
                *failed = true;
            }
        } else if (delta == 0) {
            if (node->value <= previous_value) {
                *failed = true;
            }
        }
        results->push_back(node->value);
        previous_value = node->value;
        if (i % VALIDATE_INTERVAL == 0) {
            if (!queue->validate()) {
                *failed = true;
            }
        }
    }
}

TEST(InternalQueue, TestClear) {
    std::vector<IntNode> nodes;
    nodes.resize(100);
    InternalQueue<IntNode> queue;
    queue.enqueue(&nodes[0]);
    queue.enqueue(&nodes[1]);
    queue.enqueue(&nodes[2]);

    queue.clear();
    EXPECT_TRUE(queue.validate());
    EXPECT_TRUE(queue.empty());

    queue.enqueue(&nodes[0]);
    queue.enqueue(&nodes[1]);
    queue.enqueue(&nodes[2]);
    EXPECT_TRUE(queue.validate());
    EXPECT_EQ(queue.size(), 3);
}

TEST(InternalQueue, TestSingleProducerSingleConsumer) {
    std::vector<IntNode> nodes;
    std::atomic<int32_t> counter = 0;
    nodes.resize(LOOP_LESS_OR_MORE(100, 1000000));
    std::vector<int> results;

    InternalQueue<IntNode> queue;
    bool failed = false;
    ProducerThread(&queue, nodes.size(), &nodes, &counter, &failed);
    ConsumerThread(&queue, nodes.size(), 1, &results, &failed);
    EXPECT_TRUE(!failed);
    EXPECT_TRUE(queue.empty());
    EXPECT_EQ(results.size(), nodes.size());

    counter = 0;
    results.clear();
    thread producer_thread(ProducerThread, &queue, nodes.size(), &nodes, &counter, &failed);
    thread consumer_thread(ConsumerThread, &queue, nodes.size(), 1, &results, &failed);
    producer_thread.join();
    consumer_thread.join();
    EXPECT_TRUE(!failed);
    EXPECT_TRUE(queue.empty());
    EXPECT_EQ(results.size(), nodes.size());
}

TEST(InternalQueue, TestMultiProducerMultiConsumer) {
    std::vector<IntNode> nodes;
    nodes.resize(LOOP_LESS_OR_MORE(100, 1000000));

    bool failed = false;
    for (int num_producers = 1; num_producers < 5; num_producers += 3) {
        std::atomic<int32_t> counter = 0;
        const int NUM_CONSUMERS = 4;
        EXPECT_EQ(nodes.size() % NUM_CONSUMERS, 0);
        EXPECT_EQ(nodes.size() % num_producers, 0);
        const int num_per_consumer = nodes.size() / NUM_CONSUMERS;
        const int num_per_producer = nodes.size() / num_producers;

        std::vector<vector<int>> results;
        results.resize(NUM_CONSUMERS);

        int expected_delta = -1;
        if (NUM_CONSUMERS == 1 && num_producers == 1) {
            // With one producer and consumer, the queue should have sequential values.
            expected_delta = 1;
        } else if (num_producers == 1) {
            // With one producer, the values added are sequential but can be read off
            // with gaps in each consumer thread.  E.g. thread1 reads: 1, 4, 5, 7, etc.
            // but they should be strictly increasing.
            expected_delta = 0;
        } else {
            // With multiple producers there isn't a guarantee on the order values get
            // enqueued.
            expected_delta = -1;
        }

        InternalQueue<IntNode> queue;
        ThreadGroup consumers;
        ThreadGroup producers;

        for (int i = 0; i < num_producers; ++i) {
            producers.add_thread(new thread(ProducerThread, &queue, num_per_producer, &nodes,
                                            &counter, &failed));
        }

        for (int i = 0; i < NUM_CONSUMERS; ++i) {
            consumers.add_thread(new thread(ConsumerThread, &queue, num_per_consumer,
                                            expected_delta, &results[i], &failed));
        }

        producers.join_all();
        consumers.join_all();
        EXPECT_TRUE(queue.empty());
        EXPECT_TRUE(!failed);

        std::vector<int> all_results;
        for (int i = 0; i < NUM_CONSUMERS; ++i) {
            EXPECT_EQ(results[i].size(), num_per_consumer);
            all_results.insert(all_results.end(), results[i].begin(), results[i].end());
        }
        EXPECT_EQ(all_results.size(), nodes.size());
        sort(all_results.begin(), all_results.end());
        for (int i = 0; i < all_results.size(); ++i) {
            EXPECT_EQ(i, all_results[i]) << all_results[i - 1] << " " << all_results[i + 1];
        }
    }
}

} // end namespace doris
