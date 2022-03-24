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

#include "runtime/buffer_control_block.h"

#include <gtest/gtest.h>
#include <pthread.h>

#include "gen_cpp/PaloInternalService_types.h"

namespace doris {

class BufferControlBlockTest : public testing::Test {
public:
    BufferControlBlockTest() {}
    virtual ~BufferControlBlockTest() {}

protected:
    virtual void SetUp() {}

private:
};

TEST_F(BufferControlBlockTest, init_normal) {
    BufferControlBlock control_block(TUniqueId(), 1024);
    ASSERT_TRUE(control_block.init().ok());
}

TEST_F(BufferControlBlockTest, add_one_get_one) {
    BufferControlBlock control_block(TUniqueId(), 1024);
    ASSERT_TRUE(control_block.init().ok());

    TFetchDataResult* add_result = new TFetchDataResult();
    add_result->result_batch.rows.push_back("hello test");
    ASSERT_TRUE(control_block.add_batch(add_result).ok());

    TFetchDataResult get_result;
    ASSERT_TRUE(control_block.get_batch(&get_result).ok());
    ASSERT_FALSE(get_result.eos);
    ASSERT_EQ(1U, get_result.result_batch.rows.size());
    ASSERT_STREQ("hello test", get_result.result_batch.rows[0].c_str());
}

TEST_F(BufferControlBlockTest, get_one_after_close) {
    BufferControlBlock control_block(TUniqueId(), 1024);
    ASSERT_TRUE(control_block.init().ok());

    control_block.close(Status::OK());
    TFetchDataResult get_result;
    ASSERT_TRUE(control_block.get_batch(&get_result).ok());
    ASSERT_TRUE(get_result.eos);
}

TEST_F(BufferControlBlockTest, get_add_after_cancel) {
    BufferControlBlock control_block(TUniqueId(), 1024);
    ASSERT_TRUE(control_block.init().ok());

    ASSERT_TRUE(control_block.cancel().ok());
    TFetchDataResult* add_result = new TFetchDataResult();
    add_result->result_batch.rows.push_back("hello test");
    ASSERT_FALSE(control_block.add_batch(add_result).ok());
    delete add_result;

    TFetchDataResult get_result;
    ASSERT_FALSE(control_block.get_batch(&get_result).ok());
}

void* cancel_thread(void* param) {
    BufferControlBlock* control_block = static_cast<BufferControlBlock*>(param);
    sleep(1);
    control_block->cancel();
    return nullptr;
}

TEST_F(BufferControlBlockTest, add_then_cancel) {
    // only can add one batch
    BufferControlBlock control_block(TUniqueId(), 1);
    ASSERT_TRUE(control_block.init().ok());

    pthread_t id;
    pthread_create(&id, nullptr, cancel_thread, &control_block);

    {
        TFetchDataResult* add_result = new TFetchDataResult();
        add_result->result_batch.rows.push_back("hello test1");
        add_result->result_batch.rows.push_back("hello test2");
        ASSERT_TRUE(control_block.add_batch(add_result).ok());
    }
    {
        TFetchDataResult* add_result = new TFetchDataResult();
        add_result->result_batch.rows.push_back("hello test1");
        add_result->result_batch.rows.push_back("hello test2");
        ASSERT_FALSE(control_block.add_batch(add_result).ok());
        delete add_result;
    }

    TFetchDataResult get_result;
    ASSERT_FALSE(control_block.get_batch(&get_result).ok());

    pthread_join(id, nullptr);
}

TEST_F(BufferControlBlockTest, get_then_cancel) {
    BufferControlBlock control_block(TUniqueId(), 1024);
    ASSERT_TRUE(control_block.init().ok());

    pthread_t id;
    pthread_create(&id, nullptr, cancel_thread, &control_block);

    // get block until cancel
    TFetchDataResult get_result;
    ASSERT_FALSE(control_block.get_batch(&get_result).ok());

    pthread_join(id, nullptr);
}

void* add_thread(void* param) {
    BufferControlBlock* control_block = static_cast<BufferControlBlock*>(param);
    sleep(1);
    {
        TFetchDataResult* add_result = new TFetchDataResult();
        add_result->result_batch.rows.push_back("hello test1");
        add_result->result_batch.rows.push_back("hello test2");
        control_block->add_batch(add_result);
    }
    return nullptr;
}

TEST_F(BufferControlBlockTest, get_then_add) {
    BufferControlBlock control_block(TUniqueId(), 1024);
    ASSERT_TRUE(control_block.init().ok());

    pthread_t id;
    pthread_create(&id, nullptr, add_thread, &control_block);

    // get block until a batch add
    TFetchDataResult get_result;
    ASSERT_TRUE(control_block.get_batch(&get_result).ok());
    ASSERT_FALSE(get_result.eos);
    ASSERT_EQ(2U, get_result.result_batch.rows.size());
    ASSERT_STREQ("hello test1", get_result.result_batch.rows[0].c_str());
    ASSERT_STREQ("hello test2", get_result.result_batch.rows[1].c_str());

    pthread_join(id, nullptr);
}

void* close_thread(void* param) {
    BufferControlBlock* control_block = static_cast<BufferControlBlock*>(param);
    sleep(1);
    control_block->close(Status::OK());
    return nullptr;
}

TEST_F(BufferControlBlockTest, get_then_close) {
    BufferControlBlock control_block(TUniqueId(), 1024);
    ASSERT_TRUE(control_block.init().ok());

    pthread_t id;
    pthread_create(&id, nullptr, close_thread, &control_block);

    // get block until a batch add
    TFetchDataResult get_result;
    ASSERT_TRUE(control_block.get_batch(&get_result).ok());
    ASSERT_TRUE(get_result.eos);
    ASSERT_EQ(0U, get_result.result_batch.rows.size());

    pthread_join(id, nullptr);
}

} // namespace doris
int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

/* vim: set ts=4 sw=4 sts=4 tw=100 noet: */
