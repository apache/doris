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

#include "vec/runtime/vdata_stream_recvr.h"

#include <gtest/gtest.h>

#include <memory>
#include <thread>
#include <vector>

#include "pipeline/dependency.h"
#include "pipeline/exec/multi_cast_data_streamer.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"
#include "vec/data_types/data_type_number.h"
#include "vec/runtime/vdata_stream_mgr.h"

namespace doris::pipeline {
using namespace vectorized;

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
    EXPECT_TRUE(block.serialize(BeExecVersionManager::get_newest_version(), pblock,
                                &uncompressed_bytes, &compressed_bytes,
                                segment_v2::CompressionTypePB::NO_COMPRESSION));
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
                      std::shared_ptr<VDataStreamRecvr>* res, bool acquire_lock = true) override {
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

// ./run-be-ut.sh --run --filter=DataStreamRecvrTest.*

} // namespace doris::pipeline
