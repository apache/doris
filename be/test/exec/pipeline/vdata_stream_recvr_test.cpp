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

#include "exec/exchange/vdata_stream_recvr.h"

#include <gtest/gtest.h>

#include <array>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_number.h"
#include "exec/exchange/vdata_stream_mgr.h"
#include "exec/operator/multi_cast_data_streamer.h"
#include "exec/pipeline/dependency.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"
#include "util/unaligned.h"

namespace doris {

struct MockVDataStreamRecvr : public VDataStreamRecvr {
    MockVDataStreamRecvr(RuntimeState* state, RuntimeProfile::HighWaterMarkCounter* counter,
                         RuntimeProfile* profile, int num_senders, bool is_merging)
            : VDataStreamRecvr(nullptr, counter, state, TUniqueId(), 0, num_senders, is_merging,
                               profile, 1) {};

    bool exceeds_limit(size_t block_byte_size) override {
        if (always_exceeds_limit) {
            return true;
        }
        return VDataStreamRecvr::exceeds_limit(block_byte_size);
    }
    bool always_exceeds_limit = false;
};

class DataStreamRecvrTest : public testing::Test {
public:
    DataStreamRecvrTest() = default;
    ~DataStreamRecvrTest() override = default;
    void SetUp() override {}

    void create_recvr(int num_senders, bool is_merging) {
        _mock_counter =
                std::make_unique<RuntimeProfile::HighWaterMarkCounter>(TUnit::UNIT, 0, "test");
        _mock_state = std::make_unique<MockRuntimeState>();
        _mock_profile = std::make_unique<RuntimeProfile>("test");
        recvr = std::make_shared<MockVDataStreamRecvr>(_mock_state.get(), _mock_counter.get(),
                                                       _mock_profile.get(), num_senders,
                                                       is_merging);
    }

    void expect_empty_queue_accounting(VDataStreamRecvr::SenderQueue* sender) {
        EXPECT_TRUE(sender->_block_queue.empty());
        EXPECT_TRUE(sender->_pending_closures.empty());
        EXPECT_EQ(_mock_counter->current_value(), 0);
        EXPECT_EQ(sender->_queue_mem_tracker->consumption(), 0);
        EXPECT_EQ(recvr->_mem_tracker->consumption(), 0);
    }

    std::shared_ptr<MockVDataStreamRecvr> recvr;

    std::unique_ptr<RuntimeProfile::HighWaterMarkCounter> _mock_counter;

    std::unique_ptr<MockRuntimeState> _mock_state;

    std::unique_ptr<RuntimeProfile> _mock_profile;
};

TEST_F(DataStreamRecvrTest, TestCreateSenderQueue) {
    {
        create_recvr(3, false);
        EXPECT_EQ(recvr->sender_queues().size(), 1);
        EXPECT_EQ(recvr->sender_queues().back()->_num_remaining_senders, 3);
    }

    {
        create_recvr(3, true);
        EXPECT_EQ(recvr->sender_queues().size(), 3);
        for (auto& queue : recvr->sender_queues()) {
            EXPECT_EQ(queue->_num_remaining_senders, 1);
        }
    }
}

TEST_F(DataStreamRecvrTest, TestSender) {
    create_recvr(3, false);
    EXPECT_EQ(recvr->sender_queues().size(), 1);
    EXPECT_EQ(recvr->sender_queues().back()->_num_remaining_senders, 3);

    auto* sender = recvr->sender_queues().back();

    auto sink_dep = sender->_local_channel_dependency;
    auto source_dep = std::make_shared<Dependency>(0, 0, "test", false);
    sender->set_dependency(source_dep);

    EXPECT_EQ(sink_dep->ready(), true);
    EXPECT_EQ(source_dep->ready(), false);

    EXPECT_EQ(sender->_num_remaining_senders, 3);
    EXPECT_EQ(sender->_block_queue.size(), 0);

    {
        auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
        sender->add_block(&block, false);
    }

    EXPECT_EQ(sink_dep->ready(), false);

    {
        EXPECT_EQ(sender->_block_queue.size(), 1);
        Block block;
        bool eos = false;
        auto st = sender->get_batch(&block, &eos);
        EXPECT_TRUE(st) << st.msg();
        EXPECT_TRUE(ColumnHelper::block_equal(
                block, ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5})));
        EXPECT_FALSE(eos);
    }

    {
        sender->decrement_senders(1);
        EXPECT_EQ(sender->_num_remaining_senders, 2);
        sender->decrement_senders(2);
        EXPECT_EQ(sender->_num_remaining_senders, 1);
        sender->decrement_senders(3);
        EXPECT_EQ(sender->_num_remaining_senders, 0);

        EXPECT_EQ(sender->_block_queue.size(), 0);
        auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
        sender->add_block(&block, false);
        EXPECT_EQ(sender->_block_queue.size(), 0);
    }

    {
        EXPECT_EQ(sender->_block_queue.size(), 0);
        Block block;
        bool eos = false;
        auto st = sender->get_batch(&block, &eos);
        EXPECT_TRUE(st) << st.msg();
        EXPECT_TRUE(eos);
    }

    sender->close();
}

TEST_F(DataStreamRecvrTest, TestSenderClose) {
    create_recvr(3, false);
    EXPECT_EQ(recvr->sender_queues().size(), 1);
    EXPECT_EQ(recvr->sender_queues().back()->_num_remaining_senders, 3);

    auto* sender = recvr->sender_queues().back();

    auto sink_dep = sender->_local_channel_dependency;
    sender->close();
}

TEST_F(DataStreamRecvrTest, TestRandomSender) {
    create_recvr(3, false);
    EXPECT_EQ(recvr->sender_queues().size(), 1);
    EXPECT_EQ(recvr->sender_queues().back()->_num_remaining_senders, 3);

    auto* sender = recvr->sender_queues().back();

    auto sink_dep = sender->_local_channel_dependency;
    auto source_dep = std::make_shared<Dependency>(0, 0, "test", false);
    sender->set_dependency(source_dep);

    EXPECT_EQ(sink_dep->ready(), true);
    EXPECT_EQ(source_dep->ready(), false);

    auto input_func = [&](int id) {
        mock_random_sleep();
        int input_block = 0;
        while (true) {
            if (sink_dep->ready()) {
                auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
                sender->add_block(&block, false);
                input_block++;
                if (input_block == 100) {
                    sender->decrement_senders(id);
                    break;
                }
            }
        }
    };

    auto outout_func = [&]() {
        mock_random_sleep();
        int output_block = 0;
        while (true) {
            if (source_dep->ready()) {
                Block block;
                bool eos = false;
                auto st = sender->get_batch(&block, &eos);
                EXPECT_TRUE(st) << st.msg();
                if (!block.empty()) {
                    output_block++;
                }
                if (eos) {
                    EXPECT_EQ(output_block, 3 * 100);
                    break;
                }
            }
        }
    };

    std::thread input1(input_func, 1);
    std::thread input2(input_func, 2);
    std::thread input3(input_func, 3);
    std::thread output(outout_func);

    input1.join();
    input2.join();
    input3.join();
    output.join();
}

TEST_F(DataStreamRecvrTest, TestRandomCloseSender) {
    create_recvr(3, false);
    EXPECT_EQ(recvr->sender_queues().size(), 1);
    EXPECT_EQ(recvr->sender_queues().back()->_num_remaining_senders, 3);

    auto* sender = recvr->sender_queues().back();

    auto sink_dep = sender->_local_channel_dependency;
    auto source_dep = std::make_shared<Dependency>(0, 0, "test", false);
    sender->set_dependency(source_dep);

    EXPECT_EQ(sink_dep->ready(), true);
    EXPECT_EQ(source_dep->ready(), false);

    auto input_func = [&](int id) {
        mock_random_sleep();
        int input_block = 0;
        while (true) {
            if (sink_dep->ready()) {
                auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
                sender->add_block(&block, false);
                input_block++;
                if (input_block == 100) {
                    sender->decrement_senders(id);
                    break;
                }
            }
        }
        std::cout << "input func : " << id << " end " << std::endl;
    };

    auto outout_func = [&]() {
        mock_random_sleep();
        try {
            while (true) {
                if (source_dep->ready()) {
                    Block block;
                    bool eos = false;
                    auto st = sender->get_batch(&block, &eos);

                    if (!st.ok()) {
                        std::cout << "get_batch error: " << st.msg() << std::endl;
                        break;
                    }
                    if (eos) {
                        break;
                    }
                }
            }
        } catch (std::exception& e) {
            std::cout << "exception: " << e.what() << std::endl;
        }
        std::cout << "output func end" << std::endl;
    };

    auto close_func = [&]() {
        try {
            mock_random_sleep();
            std::cout << "close_func start" << std::endl;
            recvr->close();
            std::cout << "close_func end" << std::endl;
        } catch (const std::exception& e) {
            std::cout << "close exception: " << e.what() << std::endl;
        }
    };

    std::vector<std::thread> threads;
    threads.emplace_back(input_func, 1);
    threads.emplace_back(input_func, 2);
    threads.emplace_back(input_func, 3);
    threads.emplace_back(outout_func);
    threads.emplace_back(close_func);

    for (auto& t : threads) {
        if (t.joinable()) {
            try {
                t.join();
            } catch (const std::system_error& e) {
                std::cout << "Thread join error: " << e.what() << std::endl;
            }
        }
    }
}

class MockClosure : public google::protobuf::Closure {
    MockClosure() = default;

    ~MockClosure() override = default;

    void Run() override { _cb(); }
    std::function<void()> _cb;
};

void to_pblock(Block& block, PBlock* pblock) {
    size_t uncompressed_bytes = 0;
    size_t compressed_bytes = 0;
    int64_t compressed_time = 0;
    EXPECT_TRUE(block.serialize(BeExecVersionManager::get_newest_version(), pblock,
                                &uncompressed_bytes, &compressed_bytes, &compressed_time,
                                segment_v2::CompressionTypePB::NO_COMPRESSION));
}

std::unique_ptr<PBlock> make_invalid_hll_pblock() {
    constexpr size_t hll_header_size = sizeof(bool) + 2 * sizeof(size_t);
    constexpr uint8_t invalid_hll_type = 0x04;
    auto hll_type =
            DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_HLL, 0, 0);
    auto column = hll_type->create_column();
    column->insert_default();

    auto block = Block({ColumnWithTypeAndName(std::move(column), std::move(hll_type), "hll")});
    auto pblock = std::make_unique<PBlock>();
    to_pblock(block, pblock.get());

    auto* values = pblock->mutable_column_values();
    DCHECK_GE(values->size(), hll_header_size + sizeof(size_t) + 1);
    unaligned_store<size_t>(values->data() + hll_header_size, 1);
    (*values)[hll_header_size + sizeof(size_t)] = static_cast<char>(invalid_hll_type);
    return pblock;
}

TEST_F(DataStreamRecvrTest, TestRemoteSender) {
    create_recvr(3, false);
    EXPECT_EQ(recvr->sender_queues().size(), 1);
    EXPECT_EQ(recvr->sender_queues().back()->_num_remaining_senders, 3);

    auto* sender = recvr->sender_queues().back();

    auto sink_dep = sender->_local_channel_dependency;
    auto source_dep = std::make_shared<Dependency>(0, 0, "test", false);
    sender->set_dependency(source_dep);

    EXPECT_EQ(sink_dep->ready(), true);
    EXPECT_EQ(source_dep->ready(), false);

    EXPECT_EQ(sender->_num_remaining_senders, 3);
    EXPECT_EQ(sender->_block_queue.size(), 0);

    {
        auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
        auto pblock = std::make_unique<PBlock>();
        to_pblock(block, pblock.get());
        MockClosure closure;
        closure._cb = [&]() { std::cout << "cb" << std::endl; };
        google::protobuf::Closure* done = &closure;
        auto st = sender->add_block(std::move(pblock), 1, 1, &done, 0, 0);
        if (done != nullptr) {
            done->Run();
        }
    }
    sender->close();
}

TEST_F(DataStreamRecvrTest, TestRemoteMemLimitSender) {
    create_recvr(3, false);
    EXPECT_EQ(recvr->sender_queues().size(), 1);
    EXPECT_EQ(recvr->sender_queues().back()->_num_remaining_senders, 3);

    auto* sender = recvr->sender_queues().back();

    auto sink_dep = sender->_local_channel_dependency;
    auto source_dep = std::make_shared<Dependency>(0, 0, "test", false);
    sender->set_dependency(source_dep);

    EXPECT_EQ(sink_dep->ready(), true);
    EXPECT_EQ(source_dep->ready(), false);

    EXPECT_EQ(sender->_num_remaining_senders, 3);
    EXPECT_EQ(sender->_block_queue.size(), 0);

    config::exchg_node_buffer_size_bytes = 1;

    Defer set_([&]() { config::exchg_node_buffer_size_bytes = 20485760; });

    {
        auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
        auto pblock = std::make_unique<PBlock>();
        to_pblock(block, pblock.get());
        MockClosure closure;
        bool flag = false;
        closure._cb = [&]() {
            std::cout << "cb" << std::endl;
            EXPECT_TRUE(flag);
        };
        google::protobuf::Closure* done = &closure;
        auto st = sender->add_block(std::move(pblock), 1, 1, &done, 0, 0);
        EXPECT_EQ(done, nullptr);
        flag = true;

        {
            Block block;
            bool eos = false;
            auto st = sender->get_batch(&block, &eos);
        }
    }
    sender->close();
}

TEST_F(DataStreamRecvrTest, RejectedRemoteBlockReleasesCallbackAndAccounting) {
    create_recvr(1, false);
    recvr->always_exceeds_limit = true;
    auto* sender = recvr->sender_queues().back();

    int callback_count = 0;
    MockClosure closure;
    closure._cb = [&]() { ++callback_count; };
    google::protobuf::Closure* done = &closure;

    auto st = sender->add_block(make_invalid_hll_pblock(), 1, 1, &done, 0, 0);
    ASSERT_TRUE(st) << st.msg();
    ASSERT_EQ(done, nullptr);
    ASSERT_GT(_mock_counter->current_value(), 0);

    Block block;
    bool eos = false;
    st = sender->get_batch(&block, &eos);
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(callback_count, 1);
    EXPECT_EQ(_mock_counter->current_value(), 0);

    recvr->close();
    EXPECT_EQ(callback_count, 1);
    EXPECT_EQ(_mock_counter->current_value(), 0);
}

class DataStreamRecvrCallbackTest : public DataStreamRecvrTest,
                                    public ::testing::WithParamInterface<bool> {
public:
    static std::unique_ptr<PBlock> make_valid_pblock() {
        auto block = ColumnHelper::create_block<DataTypeInt32>({1});
        auto pblock = std::make_unique<PBlock>();
        to_pblock(block, pblock.get());
        return pblock;
    }

    void run_concurrent_add_and_cancel(VDataStreamRecvr::SenderQueue* sender,
                                       google::protobuf::Closure* pending,
                                       google::protobuf::Closure* incoming) {
        // The running callback must own neither the lock nor a slot in the pending deque.
        std::unique_lock<std::mutex> lock(sender->_lock, std::try_to_lock);
        ASSERT_TRUE(lock.owns_lock());
        ASSERT_EQ(sender->_pending_closures.size(), 1);
        ASSERT_EQ(sender->_pending_closures.front().first, pending);
        lock.unlock();
        std::thread rpc([&]() {
            google::protobuf::Closure* done = incoming;
            EXPECT_TRUE(sender->add_block(make_valid_pblock(), 1, 3, &done, 0, 0));
            EXPECT_EQ(done, nullptr);
            // Follow the error-EOS RPC path while the consumer's callback is running.
            recvr->remove_sender(0, 1, Status::InternalError("test cancellation"));
        });
        rpc.join();
    }
};

TEST_P(DataStreamRecvrCallbackTest, PendingCallbackDetachedBeforeConcurrentAddAndCancel) {
    bool invalid_block = GetParam();
    create_recvr(1, false);
    recvr->always_exceeds_limit = true;
    auto* sender = recvr->sender_queues().back();
    MockClosure first;
    MockClosure second;
    MockClosure third;
    std::array<int, 3> callback_counts {};
    const std::array<int, 3> expected_counts {1, 1, 1};
    second._cb = [&]() { ++callback_counts[1]; };
    third._cb = [&]() { ++callback_counts[2]; };
    first._cb = [&]() {
        ++callback_counts[0];
        run_concurrent_add_and_cancel(sender, &second, &third);
    };
    google::protobuf::Closure* done = &first;
    ASSERT_TRUE(sender->add_block(invalid_block ? make_invalid_hll_pblock() : make_valid_pblock(),
                                  1, 1, &done, 0, 0));
    ASSERT_EQ(done, nullptr);
    done = &second;
    ASSERT_TRUE(sender->add_block(make_valid_pblock(), 1, 2, &done, 0, 0));
    ASSERT_EQ(done, nullptr);

    Block block;
    bool eos = false;
    auto st = sender->get_batch(&block, &eos);
    EXPECT_EQ(st.ok(), !invalid_block);
    EXPECT_EQ(callback_counts, expected_counts);
    EXPECT_TRUE(sender->_pending_closures.empty());
    EXPECT_TRUE(sender->_is_cancelled);
    recvr->close();
    expect_empty_queue_accounting(sender);
    EXPECT_EQ(callback_counts, expected_counts);
}

INSTANTIATE_TEST_SUITE_P(BlockDecodeResults, DataStreamRecvrCallbackTest, ::testing::Bool());

TEST_F(DataStreamRecvrTest, CloseReleasesQueuedLocalAndRemoteAccounting) {
    create_recvr(1, false);
    recvr->always_exceeds_limit = true;
    auto* sender = recvr->sender_queues().back();
    auto local = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3});
    sender->add_block(&local, true);
    MockClosure closure;
    int callback_count = 0;
    closure._cb = [&]() { ++callback_count; };
    google::protobuf::Closure* done = &closure;
    ASSERT_TRUE(
            sender->add_block(DataStreamRecvrCallbackTest::make_valid_pblock(), 1, 1, &done, 0, 0));
    ASSERT_EQ(done, nullptr);
    ASSERT_EQ(sender->_block_queue.size(), 2);
    ASSERT_GT(_mock_counter->current_value(), 0);
    ASSERT_GT(sender->_queue_mem_tracker->consumption(), 0);
    recvr->close();
    expect_empty_queue_accounting(sender);
    EXPECT_EQ(callback_count, 1);
    sender->close();
    recvr->close();
    expect_empty_queue_accounting(sender);
    EXPECT_EQ(callback_count, 1);
}

TEST_F(DataStreamRecvrTest, TestRemoteMultiSender) {
    create_recvr(3, false);
    EXPECT_EQ(recvr->sender_queues().size(), 1);
    EXPECT_EQ(recvr->sender_queues().back()->_num_remaining_senders, 3);

    auto* sender = recvr->sender_queues().back();

    auto sink_dep = sender->_local_channel_dependency;
    auto source_dep = std::make_shared<Dependency>(0, 0, "test", false);
    sender->set_dependency(source_dep);

    EXPECT_EQ(sink_dep->ready(), true);
    EXPECT_EQ(source_dep->ready(), false);

    std::vector<std::shared_ptr<MockClosure>> closures {10};

    for (auto i = 0; i < 10; i++) {
        closures[i] = std::make_shared<MockClosure>();
    }

    auto input_func = [&](int id) {
        mock_random_sleep();
        int input_block = 0;
        auto closure = closures[id];
        std::atomic_bool cb_flag = true;
        closure->_cb = [&]() { cb_flag = true; };
        while (true) {
            if (sink_dep->ready() && cb_flag) {
                auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
                auto pblock = std::make_unique<PBlock>();
                to_pblock(block, pblock.get());
                google::protobuf::Closure* done = closure.get();
                cb_flag = false;
                auto st = sender->add_block(std::move(pblock), id, input_block, &done, 0, 0);
                EXPECT_TRUE(st) << st.msg();
                input_block++;
                if (done != nullptr) {
                    done->Run();
                }
                if (input_block == 100) {
                    sender->decrement_senders(id);
                    break;
                }
            }
        }
        std::cout << "input func : " << id << " end "
                  << "input_block  : " << input_block << std::endl;
    };

    auto outout_func = [&]() {
        mock_random_sleep();
        int output_block = 0;
        while (true) {
            if (source_dep->ready()) {
                Block block;
                bool eos = false;
                auto st = sender->get_batch(&block, &eos);
                EXPECT_TRUE(st) << st.msg();
                if (!block.empty()) {
                    output_block++;
                }
                if (eos) {
                    EXPECT_EQ(output_block, 3 * 100);
                    break;
                }
            }
        }
    };

    std::thread input1(input_func, 1);
    std::thread input2(input_func, 2);
    std::thread input3(input_func, 3);
    std::thread output(outout_func);

    input1.join();
    input2.join();
    input3.join();
    output.join();
}

TEST_F(DataStreamRecvrTest, TestRemoteLocalMultiSender) {
    create_recvr(3, false);
    EXPECT_EQ(recvr->sender_queues().size(), 1);
    EXPECT_EQ(recvr->sender_queues().back()->_num_remaining_senders, 3);

    auto* sender = recvr->sender_queues().back();

    auto sink_dep = sender->_local_channel_dependency;
    auto source_dep = std::make_shared<Dependency>(0, 0, "test", false);
    sender->set_dependency(source_dep);

    EXPECT_EQ(sink_dep->ready(), true);
    EXPECT_EQ(source_dep->ready(), false);

    std::vector<std::shared_ptr<MockClosure>> closures {10};

    for (auto i = 0; i < 10; i++) {
        closures[i] = std::make_shared<MockClosure>();
    }

    auto input_remote_func = [&](int id) {
        mock_random_sleep();
        int input_block = 0;
        auto closure = closures[id];
        std::atomic_bool cb_flag = true;
        closure->_cb = [&]() { cb_flag = true; };
        while (true) {
            if (sink_dep->ready() && cb_flag) {
                auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
                auto pblock = std::make_unique<PBlock>();
                to_pblock(block, pblock.get());
                google::protobuf::Closure* done = closure.get();
                cb_flag = false;
                auto st = sender->add_block(std::move(pblock), id, input_block, &done, 0, 0);
                EXPECT_TRUE(st) << st.msg();
                input_block++;
                if (done != nullptr) {
                    done->Run();
                }
                if (input_block == 100) {
                    sender->decrement_senders(id);
                    break;
                }
            }
        }
    };

    auto input_local_func = [&](int id) {
        mock_random_sleep();
        int input_block = 0;
        while (true) {
            if (sink_dep->ready()) {
                auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
                sender->add_block(&block, false);
                input_block++;
                if (input_block == 100) {
                    sender->decrement_senders(id);
                    break;
                }
            }
        }
    };

    auto outout_func = [&]() {
        mock_random_sleep();
        int output_block = 0;
        while (true) {
            if (source_dep->ready()) {
                Block block;
                bool eos = false;
                auto st = sender->get_batch(&block, &eos);
                EXPECT_TRUE(st) << st.msg();
                if (!block.empty()) {
                    output_block++;
                }
                if (eos) {
                    EXPECT_EQ(output_block, 3 * 100);
                    break;
                }
            }
        }
    };

    std::thread input1(input_remote_func, 1);
    std::thread input2(input_local_func, 2);
    std::thread input3(input_remote_func, 3);
    std::thread output(outout_func);

    input1.join();
    input2.join();
    input3.join();
    output.join();
}

struct MockVDataStreamMgr : public VDataStreamMgr {
    ~MockVDataStreamMgr() override = default;
    Status find_recvr(const TUniqueId& fragment_instance_id, PlanNodeId node_id,
                      std::shared_ptr<VDataStreamRecvr>* res) override {
        *res = recvr;
        return Status::OK();
    }

    std::shared_ptr<VDataStreamRecvr> recvr;
};

TEST_F(DataStreamRecvrTest, transmit_block) {
    create_recvr(1, true);
    recvr->always_exceeds_limit = true;

    MockVDataStreamMgr mgr;
    mgr.recvr = recvr;

    MockClosure closure;
    closure._cb = [&]() { std::cout << "cb" << std::endl; };
    google::protobuf::Closure* done = &closure;

    PTransmitDataParams request;
    {
        auto* pblock = request.add_blocks();
        auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
        to_pblock(block, pblock);
    }

    {
        auto* pblock = request.add_blocks();
        auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
        to_pblock(block, pblock);
    }

    {
        auto st = mgr.transmit_block(&request, &done, 1000);
        EXPECT_TRUE(st) << st.msg();
    }
    recvr->close();
}

} // namespace doris
