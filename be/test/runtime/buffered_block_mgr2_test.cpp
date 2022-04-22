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

#include "runtime/buffered_block_mgr2.h"

#include <gtest/gtest.h>
#include <sys/stat.h>

#include <filesystem>
#include <functional>
#include <thread>

#include "runtime/disk_io_mgr.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/test_env.h"
#include "runtime/tmp_file_mgr.h"
#include "util/cpu_info.h"
#include "util/disk_info.h"
#include "util/filesystem_util.h"
#include "util/logging.h"
#include "util/monotime.h"
#include "util/thread_group.h"

using std::filesystem::directory_iterator;
using std::filesystem::remove;
using std::unique_ptr;
using std::unordered_map;
using std::thread;

using std::string;
using std::stringstream;
using std::vector;

// Note: This is the default scratch dir created by doris.
// config::query_scratch_dirs + TmpFileMgr::_s_tmp_sub_dir_name.
const static string SCRATCH_DIR = "/tmp/doris-scratch";

// This suffix is appended to a tmp dir
const static string SCRATCH_SUFFIX = "/doris-scratch";

// Number of milliseconds to wait to ensure write completes
const static int WRITE_WAIT_MILLIS = 500;

// How often to check for write completion
const static int WRITE_CHECK_INTERVAL_MILLIS = 10;

namespace doris {

class BufferedBlockMgrTest : public ::testing::Test {
protected:
    const static int _block_size = 1024;

    virtual void SetUp() {
        _test_env.reset(new TestEnv());
        _client_tracker.reset(new MemTracker(-1));
    }

    virtual void TearDown() {
        TearDownMgrs();
        _test_env.reset();
        _client_tracker.reset();

        // Tests modify permissions, so make sure we can delete if they didn't clean up.
        for (int i = 0; i < _created_tmp_dirs.size(); ++i) {
            chmod((_created_tmp_dirs[i] + SCRATCH_SUFFIX).c_str(), S_IRWXU);
        }
        FileSystemUtil::remove_paths(_created_tmp_dirs);
        _created_tmp_dirs.clear();
    }

    // Reinitialize _test_env to have multiple temporary directories.
    std::vector<string> InitMultipleTmpDirs(int num_dirs) {
        std::vector<string> tmp_dirs;
        for (int i = 0; i < num_dirs; ++i) {
            std::stringstream dir_str;
            dir_str << "/tmp/buffered-block-mgr-test." << i;
            const string& dir = dir_str.str();
            // Fix permissions in case old directories were left from previous runs of test.
            chmod((dir + SCRATCH_SUFFIX).c_str(), S_IRWXU);
            EXPECT_TRUE(FileSystemUtil::create_directory(dir).ok());
            tmp_dirs.push_back(dir);
            _created_tmp_dirs.push_back(dir);
        }
        _test_env->init_tmp_file_mgr(tmp_dirs, false);
        EXPECT_EQ(num_dirs, _test_env->tmp_file_mgr()->num_active_tmp_devices());
        return tmp_dirs;
    }

    static void ValidateBlock(BufferedBlockMgr2::Block* block, int32_t data) {
        EXPECT_TRUE(block->valid_data_len() == sizeof(int32_t));
        EXPECT_TRUE(*reinterpret_cast<int32_t*>(block->buffer()) == data);
    }

    static int32_t* MakeRandomSizeData(BufferedBlockMgr2::Block* block) {
        // Format is int32_t size, followed by size bytes of data
        int32_t size = (rand() % 252) + 4; // So blocks have 4-256 bytes of data
        uint8_t* data = block->allocate<uint8_t>(size);
        *(reinterpret_cast<int32_t*>(data)) = size;
        int i = 0;
        for (i = 4; i < size - 5; ++i) {
            data[i] = i;
        }
        for (; i < size; ++i) { // End marker of at least 5 0xff's
            data[i] = 0xff;
        }
        return reinterpret_cast<int32_t*>(data); // Really returns a pointer to size
    }

    static void ValidateRandomSizeData(BufferedBlockMgr2::Block* block, int32_t size) {
        int32_t bsize = *(reinterpret_cast<int32_t*>(block->buffer()));
        uint8_t* data = reinterpret_cast<uint8_t*>(block->buffer());
        int i = 0;
        EXPECT_EQ(block->valid_data_len(), size);
        EXPECT_EQ(size, bsize);
        for (i = 4; i < size - 5; ++i) {
            EXPECT_EQ(data[i], i);
        }
        for (; i < size; ++i) {
            EXPECT_EQ(data[i], 0xff);
        }
    }

    /// Helper to create a simple block manager.
    BufferedBlockMgr2* CreateMgr(int64_t query_id, int max_buffers, int block_size,
                                 RuntimeState** query_state = nullptr) {
        RuntimeState* state = nullptr;
        EXPECT_TRUE(_test_env->create_query_state(query_id, max_buffers, block_size, &state).ok());
        if (query_state != nullptr) {
            *query_state = state;
        }
        return state->block_mgr2();
    }

    BufferedBlockMgr2* CreateMgrAndClient(int64_t query_id, int max_buffers, int block_size,
                                          int reserved_blocks,
                                          const std::shared_ptr<MemTracker>& tracker,
                                          BufferedBlockMgr2::Client** client) {
        RuntimeState* state = nullptr;
        BufferedBlockMgr2* mgr = CreateMgr(query_id, max_buffers, block_size, &state);
        EXPECT_TRUE(mgr->register_client(reserved_blocks, tracker, state, client).ok());
        EXPECT_TRUE(client != nullptr);
        return mgr;
    }

    void CreateMgrsAndClients(int64_t start_query_id, int num_mgrs, int buffers_per_mgr,
                              int block_size, int reserved_blocks_per_client,
                              const std::shared_ptr<MemTracker>& tracker,
                              std::vector<BufferedBlockMgr2*>* mgrs,
                              std::vector<BufferedBlockMgr2::Client*>* clients) {
        for (int i = 0; i < num_mgrs; ++i) {
            BufferedBlockMgr2::Client* client;
            BufferedBlockMgr2* mgr =
                    CreateMgrAndClient(start_query_id + i, buffers_per_mgr, _block_size,
                                       reserved_blocks_per_client, tracker, &client);
            mgrs->push_back(mgr);
            clients->push_back(client);
        }
    }

    // Destroy all created query states and associated block managers.
    void TearDownMgrs() {
        // Freeing all block managers should clean up all consumed memory.
        _test_env->tear_down_query_states();
        EXPECT_EQ(_test_env->block_mgr_parent_tracker()->consumption(), 0);
    }

    void AllocateBlocks(BufferedBlockMgr2* block_mgr, BufferedBlockMgr2::Client* client,
                        int num_blocks, std::vector<BufferedBlockMgr2::Block*>* blocks) {
        int32_t* data = nullptr;
        Status status;
        BufferedBlockMgr2::Block* new_block;
        for (int i = 0; i < num_blocks; ++i) {
            status = block_mgr->get_new_block(client, nullptr, &new_block);
            EXPECT_TRUE(status.ok());
            EXPECT_TRUE(new_block != nullptr);
            data = new_block->allocate<int32_t>(sizeof(int32_t));
            *data = blocks->size();
            blocks->push_back(new_block);
        }
    }

    // Pin all blocks, expecting they are pinned successfully.
    void PinBlocks(const std::vector<BufferedBlockMgr2::Block*>& blocks) {
        for (int i = 0; i < blocks.size(); ++i) {
            bool pinned = false;
            EXPECT_TRUE(blocks[i]->pin(&pinned).ok());
            EXPECT_TRUE(pinned);
        }
    }

    // Pin all blocks, expecting no errors from unpin() calls.
    void UnpinBlocks(const std::vector<BufferedBlockMgr2::Block*>& blocks) {
        for (int i = 0; i < blocks.size(); ++i) {
            EXPECT_TRUE(blocks[i]->unpin().ok());
        }
    }

    static void WaitForWrites(BufferedBlockMgr2* block_mgr) {
        std::vector<BufferedBlockMgr2*> block_mgrs;
        block_mgrs.push_back(block_mgr);
        WaitForWrites(block_mgrs);
    }

    // Wait for writes issued through block managers to complete.
    static void WaitForWrites(const std::vector<BufferedBlockMgr2*>& block_mgrs) {
        int max_attempts = WRITE_WAIT_MILLIS / WRITE_CHECK_INTERVAL_MILLIS;
        for (int i = 0; i < max_attempts; ++i) {
            SleepFor(MonoDelta::FromMilliseconds(WRITE_CHECK_INTERVAL_MILLIS));
            if (AllWritesComplete(block_mgrs)) {
                return;
            }
        }
        EXPECT_TRUE(false) << "Writes did not complete after " << WRITE_WAIT_MILLIS << "ms";
    }

    static bool AllWritesComplete(const std::vector<BufferedBlockMgr2*>& block_mgrs) {
        for (int i = 0; i < block_mgrs.size(); ++i) {
            RuntimeProfile::Counter* writes_outstanding =
                    block_mgrs[i]->profile()->get_counter("BlockWritesOutstanding");
            if (writes_outstanding->value() != 0) {
                return false;
            }
        }
        return true;
    }

    // Delete the temporary file backing a block - all subsequent writes to the file
    // should fail. Expects backing file has already been allocated.
    static void DeleteBackingFile(BufferedBlockMgr2::Block* block) {
        const string& path = block->tmp_file_path();
        EXPECT_GT(path.size(), 0);
        EXPECT_TRUE(remove(path));
        LOG(INFO) << "Injected fault by deleting file " << path;
    }

    // Check that the file backing the block has dir as a prefix of its path.
    static bool BlockInDir(BufferedBlockMgr2::Block* block, const string& dir) {
        return block->tmp_file_path().find(dir) == 0;
    }

    // Find a block in the list that is backed by a file with the given directory as prefix
    // of its path.
    static BufferedBlockMgr2::Block* FindBlockForDir(
            const std::vector<BufferedBlockMgr2::Block*>& blocks, const string& dir) {
        for (int i = 0; i < blocks.size(); ++i) {
            if (BlockInDir(blocks[i], dir)) {
                return blocks[i];
            }
        }
        return nullptr;
    }

    void TestGetNewBlockImpl(int block_size) {
        Status status;
        int max_num_blocks = 5;
        BufferedBlockMgr2* block_mgr = nullptr;
        BufferedBlockMgr2::Client* client;
        block_mgr = CreateMgrAndClient(0, max_num_blocks, block_size, 0, _client_tracker, &client);
        EXPECT_EQ(_test_env->block_mgr_parent_tracker()->consumption(), 0);

        // Allocate blocks until max_num_blocks, they should all succeed and memory
        // usage should go up.
        BufferedBlockMgr2::Block* new_block;
        BufferedBlockMgr2::Block* first_block = nullptr;
        for (int i = 0; i < max_num_blocks; ++i) {
            status = block_mgr->get_new_block(client, nullptr, &new_block);
            EXPECT_TRUE(new_block != nullptr);
            EXPECT_EQ(block_mgr->bytes_allocated(), (i + 1) * block_size);
            if (first_block == nullptr) {
                first_block = new_block;
            }
        }

        // Trying to allocate a new one should fail.
        status = block_mgr->get_new_block(client, nullptr, &new_block);
        EXPECT_TRUE(new_block == nullptr);
        EXPECT_EQ(block_mgr->bytes_allocated(), max_num_blocks * block_size);

        // We can allocate a new block by transferring an already allocated one.
        uint8_t* old_buffer = first_block->buffer();
        status = block_mgr->get_new_block(client, first_block, &new_block);
        EXPECT_TRUE(new_block != nullptr);
        EXPECT_TRUE(old_buffer == new_block->buffer());
        EXPECT_EQ(block_mgr->bytes_allocated(), max_num_blocks * block_size);
        EXPECT_TRUE(!first_block->is_pinned());

        // Trying to allocate a new one should still fail.
        status = block_mgr->get_new_block(client, nullptr, &new_block);
        EXPECT_TRUE(new_block == nullptr);
        EXPECT_EQ(block_mgr->bytes_allocated(), max_num_blocks * block_size);

        EXPECT_EQ(block_mgr->writes_issued(), 1);
        TearDownMgrs();
    }

    void TestEvictionImpl(int block_size) {
        Status status;
        DCHECK_GT(block_size, 0);
        int max_num_buffers = 5;
        BufferedBlockMgr2* block_mgr = nullptr;
        BufferedBlockMgr2::Client* client = nullptr;
        block_mgr = CreateMgrAndClient(0, max_num_buffers, block_size, 0, _client_tracker, &client);

        // Check counters.
        RuntimeProfile* profile = block_mgr->profile();
        RuntimeProfile::Counter* buffered_pin = profile->get_counter("BufferedPins");

        std::vector<BufferedBlockMgr2::Block*> blocks;
        AllocateBlocks(block_mgr, client, max_num_buffers, &blocks);

        EXPECT_EQ(block_mgr->bytes_allocated(), max_num_buffers * block_size);
        for (BufferedBlockMgr2::Block* block : blocks) {
            block->unpin();
        }

        // Re-pinning all blocks
        for (int i = 0; i < blocks.size(); ++i) {
            bool pinned = false;
            status = blocks[i]->pin(&pinned);
            EXPECT_TRUE(status.ok());
            EXPECT_TRUE(pinned);
            ValidateBlock(blocks[i], i);
        }
        int buffered_pins_expected = blocks.size();
        EXPECT_EQ(buffered_pin->value(), buffered_pins_expected);

        // Unpin all blocks
        for (BufferedBlockMgr2::Block* block : blocks) {
            block->unpin();
        }
        // Get two new blocks.
        AllocateBlocks(block_mgr, client, 2, &blocks);
        // At least two writes must be issued. The first (num_blocks - 2) must be in memory.
        EXPECT_GE(block_mgr->writes_issued(), 2);
        for (int i = 0; i < (max_num_buffers - 2); ++i) {
            bool pinned = false;
            status = blocks[i]->pin(&pinned);
            EXPECT_TRUE(status.ok());
            EXPECT_TRUE(pinned);
            ValidateBlock(blocks[i], i);
        }
        EXPECT_GE(buffered_pin->value(), buffered_pins_expected);

        // can not pin any more
        for (int i = (max_num_buffers - 2); i < max_num_buffers; ++i) {
            bool pinned = true;
            status = blocks[i]->pin(&pinned);
            EXPECT_TRUE(status.ok());
            EXPECT_FALSE(pinned);
        }

        // the last 2 block has already been pinned
        for (int i = max_num_buffers; i < blocks.size(); ++i) {
            bool pinned = false;
            status = blocks[i]->pin(&pinned);
            EXPECT_TRUE(status.ok());
            EXPECT_TRUE(pinned);
            ValidateBlock(blocks[i], i);
        }

        TearDownMgrs();
    }

    // Test that randomly issues GetFreeBlock(), pin(), unpin(), del() and Close()
    // calls. All calls made are legal - error conditions are not expected until the first
    // call to Close(). This is called 2 times with encryption+integrity on/off.
    // When executed in single-threaded mode 'tid' should be SINGLE_THREADED_TID.
    static const int SINGLE_THREADED_TID = -1;
    void TestRandomInternalImpl(RuntimeState* state, BufferedBlockMgr2* block_mgr, int num_buffers,
                                int tid) {
        DCHECK(block_mgr != nullptr);
        const int num_iterations = 100000;
        const int iters_before_close = num_iterations - 5000;
        bool close_called = false;
        unordered_map<BufferedBlockMgr2::Block*, int> pinned_block_map;
        std::vector<std::pair<BufferedBlockMgr2::Block*, int32_t>> pinned_blocks;
        unordered_map<BufferedBlockMgr2::Block*, int> unpinned_block_map;
        std::vector<std::pair<BufferedBlockMgr2::Block*, int32_t>> unpinned_blocks;

        typedef enum { Pin, New, Unpin, Delete, Close } ApiFunction;
        ApiFunction api_function;

        BufferedBlockMgr2::Client* client;
        Status status = block_mgr->register_client(0, _client_tracker, state, &client);
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(client != nullptr);

        pinned_blocks.reserve(num_buffers);
        BufferedBlockMgr2::Block* new_block;
        for (int i = 0; i < num_iterations; ++i) {
            if ((i % 20000) == 0) {
                LOG(ERROR) << " Iteration " << i << std::endl;
            }
            if (i > iters_before_close && (rand() % 5 == 0)) {
                api_function = Close;
            } else if (pinned_blocks.size() == 0 && unpinned_blocks.size() == 0) {
                api_function = New;
            } else if (pinned_blocks.size() == 0) {
                // Pin or New. Can't unpin or delete.
                api_function = static_cast<ApiFunction>(rand() % 2);
            } else if (pinned_blocks.size() >= num_buffers) {
                // Unpin or delete. Can't pin or get new.
                api_function = static_cast<ApiFunction>(2 + (rand() % 2));
            } else if (unpinned_blocks.size() == 0) {
                // Can't pin. Unpin, new or delete.
                api_function = static_cast<ApiFunction>(1 + (rand() % 3));
            } else {
                // Any api function.
                api_function = static_cast<ApiFunction>(rand() % 4);
            }

            std::pair<BufferedBlockMgr2::Block*, int32_t> block_data;
            int rand_pick = 0;
            int32_t* data = nullptr;
            bool pinned = false;
            switch (api_function) {
            case New:
                status = block_mgr->get_new_block(client, nullptr, &new_block);
                if (close_called || (tid != SINGLE_THREADED_TID && status.is_cancelled())) {
                    EXPECT_TRUE(new_block == nullptr);
                    EXPECT_TRUE(status.is_cancelled());
                    continue;
                }
                EXPECT_TRUE(status.ok());
                EXPECT_TRUE(new_block != nullptr);
                data = MakeRandomSizeData(new_block);
                block_data = std::make_pair(new_block, *data);

                pinned_blocks.push_back(block_data);
                pinned_block_map.insert(std::make_pair(block_data.first, pinned_blocks.size() - 1));
                break;
            case Pin:
                rand_pick = rand() % unpinned_blocks.size();
                block_data = unpinned_blocks[rand_pick];
                status = block_data.first->pin(&pinned);
                if (close_called || (tid != SINGLE_THREADED_TID && status.is_cancelled())) {
                    EXPECT_TRUE(status.is_cancelled());
                    // In single-threaded runs the block should not have been pinned.
                    // In multi-threaded runs pin() may return the block pinned but the status to
                    // be cancelled. In this case we could move the block from unpinned_blocks
                    // to pinned_blocks. We do not do that because after is_cancelled() no actual
                    // block operations should take place.
                    // reason: when block_mgr is cancelled in one thread, the same block_mgr
                    // is waiting for scan-range to be ready.
                    if (tid == SINGLE_THREADED_TID) {
                        EXPECT_FALSE(pinned);
                    }
                    continue;
                }
                EXPECT_TRUE(status.ok());
                EXPECT_TRUE(pinned);
                ValidateRandomSizeData(block_data.first, block_data.second);
                unpinned_blocks[rand_pick] = unpinned_blocks.back();
                unpinned_blocks.pop_back();
                unpinned_block_map[unpinned_blocks[rand_pick].first] = rand_pick;

                pinned_blocks.push_back(block_data);
                pinned_block_map.insert(std::make_pair(block_data.first, pinned_blocks.size() - 1));
                break;
            case Unpin:
                rand_pick = rand() % pinned_blocks.size();
                block_data = pinned_blocks[rand_pick];
                status = block_data.first->unpin();
                if (close_called || (tid != SINGLE_THREADED_TID && status.is_cancelled())) {
                    EXPECT_TRUE(status.is_cancelled());
                    continue;
                }
                EXPECT_TRUE(status.ok());
                pinned_blocks[rand_pick] = pinned_blocks.back();
                pinned_blocks.pop_back();
                pinned_block_map[pinned_blocks[rand_pick].first] = rand_pick;

                unpinned_blocks.push_back(block_data);
                unpinned_block_map.insert(
                        std::make_pair(block_data.first, unpinned_blocks.size() - 1));
                break;
            case Delete:
                rand_pick = rand() % pinned_blocks.size();
                block_data = pinned_blocks[rand_pick];
                block_data.first->del();
                pinned_blocks[rand_pick] = pinned_blocks.back();
                pinned_blocks.pop_back();
                pinned_block_map[pinned_blocks[rand_pick].first] = rand_pick;
                break;
            case Close:
                block_mgr->cancel();
                close_called = true;
                break;
            } // end switch (apiFunction)
        }     // end for ()
    }

    // Single-threaded execution of the TestRandomInternalImpl.
    void TestRandomInternalSingle(int block_size) {
        DCHECK_GT(block_size, 0);
        DCHECK(_test_env.get() != nullptr);
        const int max_num_buffers = 100;
        RuntimeState* state = nullptr;
        BufferedBlockMgr2* block_mgr = CreateMgr(0, max_num_buffers, block_size, &state);
        TestRandomInternalImpl(state, block_mgr, max_num_buffers, SINGLE_THREADED_TID);
        TearDownMgrs();
    }

    // Multi-threaded execution of the TestRandomInternalImpl.
    void TestRandomInternalMulti(int num_threads, int block_size) {
        DCHECK_GT(num_threads, 0);
        DCHECK_GT(block_size, 0);
        DCHECK(_test_env.get() != nullptr);
        const int max_num_buffers = 100;
        RuntimeState* state = nullptr;
        BufferedBlockMgr2* block_mgr =
                CreateMgr(0, num_threads * max_num_buffers, block_size, &state);

        ThreadGroup workers;
        for (int i = 0; i < num_threads; ++i) {
            thread* t = new thread(std::bind(&BufferedBlockMgrTest::TestRandomInternalImpl, this,
                                             state, block_mgr, max_num_buffers, i));
            workers.add_thread(t);
        }
        workers.join_all();
        TearDownMgrs();
    }

    // Repeatedly call BufferedBlockMgr2::Create() and BufferedBlockMgr2::~BufferedBlockMgr2().
    void CreateDestroyThread(int index, RuntimeState* state) {
        const int num_buffers = 10;
        const int iters = 100;
        for (int i = 0; i < iters; ++i) {
            LOG(WARNING) << "CreateDestroyThread thread " << index << " begin " << i << std::endl;
            std::shared_ptr<BufferedBlockMgr2> mgr;
            Status status = BufferedBlockMgr2::create(
                    state, _test_env->block_mgr_parent_tracker(), state->runtime_profile(),
                    _test_env->tmp_file_mgr(), _block_size * num_buffers, _block_size, &mgr);
            LOG(WARNING) << "CreateDestroyThread thread " << index << " end " << i << std::endl;
        }
    }

    // IMPALA-2286: Test for races between BufferedBlockMgr2::Create() and
    // BufferedBlockMgr2::~BufferedBlockMgr2().
    void CreateDestroyMulti() {
        const int num_threads = 4;
        ThreadGroup workers;
        // Create a shared RuntimeState with no BufferedBlockMgr2.
        RuntimeState* shared_state = new RuntimeState(TUniqueId(), TQueryOptions(), TQueryGlobals(),
                                                      _test_env->exec_env());
        for (int i = 0; i < num_threads; ++i) {
            thread* t = new thread(
                    std::bind(&BufferedBlockMgrTest::CreateDestroyThread, this, i, shared_state));
            workers.add_thread(t);
        }
        workers.join_all();
    }

    std::unique_ptr<TestEnv> _test_env;
    std::shared_ptr<MemTracker> _client_tracker;
    std::vector<string> _created_tmp_dirs;
};

TEST_F(BufferedBlockMgrTest, get_new_block) {
    TestGetNewBlockImpl(1024);
    TestGetNewBlockImpl(8 * 1024);
    TestGetNewBlockImpl(8 * 1024 * 1024);
    LOG(WARNING) << "finish test get_new_block." << std::endl;
}

TEST_F(BufferedBlockMgrTest, GetNewBlockSmallBlocks) {
    const int block_size = 1024;
    int max_num_blocks = 3;
    BufferedBlockMgr2* block_mgr;
    BufferedBlockMgr2::Client* client;
    block_mgr = CreateMgrAndClient(0, max_num_blocks, block_size, 0, _client_tracker, &client);
    EXPECT_EQ(0, _test_env->block_mgr_parent_tracker()->consumption());

    std::vector<BufferedBlockMgr2::Block*> blocks;

    // Allocate a small block.
    BufferedBlockMgr2::Block* new_block = nullptr;
    EXPECT_TRUE(block_mgr->get_new_block(client, nullptr, &new_block, 128).ok());
    EXPECT_TRUE(new_block != nullptr);
    EXPECT_EQ(block_mgr->bytes_allocated(), 0);
    EXPECT_EQ(_test_env->block_mgr_parent_tracker()->consumption(), 0);
    EXPECT_EQ(_client_tracker->consumption(), 128);
    EXPECT_TRUE(new_block->is_pinned());
    EXPECT_EQ(new_block->bytes_remaining(), 128);
    EXPECT_TRUE(new_block->buffer() != nullptr);
    blocks.push_back(new_block);

    // Allocate a normal block
    EXPECT_TRUE(block_mgr->get_new_block(client, nullptr, &new_block).ok());
    EXPECT_TRUE(new_block != nullptr);
    EXPECT_EQ(block_mgr->bytes_allocated(), block_mgr->max_block_size());
    EXPECT_EQ(_test_env->block_mgr_parent_tracker()->consumption(), block_mgr->max_block_size());
    EXPECT_EQ(_client_tracker->consumption(), 128 + block_mgr->max_block_size());
    EXPECT_TRUE(new_block->is_pinned());
    EXPECT_EQ(new_block->bytes_remaining(), block_mgr->max_block_size());
    EXPECT_TRUE(new_block->buffer() != nullptr);
    blocks.push_back(new_block);

    // Allocate another small block.
    EXPECT_TRUE(block_mgr->get_new_block(client, nullptr, &new_block, 512).ok());
    EXPECT_TRUE(new_block != nullptr);
    EXPECT_EQ(block_mgr->bytes_allocated(), block_mgr->max_block_size());
    EXPECT_EQ(_test_env->block_mgr_parent_tracker()->consumption(), block_mgr->max_block_size());
    EXPECT_EQ(_client_tracker->consumption(), 128 + 512 + block_mgr->max_block_size());
    EXPECT_TRUE(new_block->is_pinned());
    EXPECT_EQ(new_block->bytes_remaining(), 512);
    EXPECT_TRUE(new_block->buffer() != nullptr);
    blocks.push_back(new_block);

    // Should be able to unpin and pin the middle block
    EXPECT_TRUE(blocks[1]->unpin().ok());

    bool pinned;
    EXPECT_TRUE(blocks[1]->pin(&pinned).ok());
    EXPECT_TRUE(pinned);

    for (int i = 0; i < blocks.size(); ++i) {
        blocks[i]->del();
    }

    TearDownMgrs();
}

// Test that pinning more blocks than the max available buffers.
TEST_F(BufferedBlockMgrTest, Pin) {
    Status status;
    int max_num_blocks = 5;
    const int block_size = 1024;
    BufferedBlockMgr2* block_mgr;
    BufferedBlockMgr2::Client* client;
    block_mgr = CreateMgrAndClient(0, max_num_blocks, block_size, 0, _client_tracker, &client);

    std::vector<BufferedBlockMgr2::Block*> blocks;
    AllocateBlocks(block_mgr, client, max_num_blocks, &blocks);

    // Unpin them all.
    for (int i = 0; i < blocks.size(); ++i) {
        status = blocks[i]->unpin();
        EXPECT_TRUE(status.ok());
    }

    // Allocate more, this should work since we just unpinned some blocks.
    AllocateBlocks(block_mgr, client, max_num_blocks, &blocks);

    // Try to pin a unpinned block, this should not be possible.
    bool pinned;
    status = blocks[0]->pin(&pinned);
    EXPECT_TRUE(status.ok());
    EXPECT_FALSE(pinned);

    // Unpin all blocks.
    for (int i = 0; i < blocks.size(); ++i) {
        status = blocks[i]->unpin();
        EXPECT_TRUE(status.ok());
    }

    // Should be able to pin max_num_blocks blocks.
    for (int i = 0; i < max_num_blocks; ++i) {
        status = blocks[i]->pin(&pinned);
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(pinned);
    }

    // Can't pin any more though.
    status = blocks[max_num_blocks]->pin(&pinned);
    EXPECT_TRUE(status.ok());
    EXPECT_FALSE(pinned);

    TearDownMgrs();
}

// Test the eviction policy of the block mgr. No writes issued until more than
// the max available buffers are allocated. Writes must be issued in LIFO order.
TEST_F(BufferedBlockMgrTest, Eviction) {
    TestEvictionImpl(1024);
    TestEvictionImpl(8 * 1024 * 1024);
}

// Test deletion and reuse of blocks.
TEST_F(BufferedBlockMgrTest, Deletion) {
    int max_num_buffers = 5;
    const int block_size = 1024;
    BufferedBlockMgr2* block_mgr;
    BufferedBlockMgr2::Client* client;
    block_mgr = CreateMgrAndClient(0, max_num_buffers, block_size, 0, _client_tracker, &client);

    // Check counters.
    RuntimeProfile* profile = block_mgr->profile();
    RuntimeProfile::Counter* recycled_cnt = profile->get_counter("BlocksRecycled");
    RuntimeProfile::Counter* created_cnt = profile->get_counter("BlocksCreated");

    std::vector<BufferedBlockMgr2::Block*> blocks;
    AllocateBlocks(block_mgr, client, max_num_buffers, &blocks);
    EXPECT_TRUE(created_cnt->value() == max_num_buffers);

    for (BufferedBlockMgr2::Block* block : blocks) {
        block->del();
    }
    AllocateBlocks(block_mgr, client, max_num_buffers, &blocks);
    EXPECT_TRUE(created_cnt->value() == max_num_buffers);
    EXPECT_TRUE(recycled_cnt->value() == max_num_buffers);

    TearDownMgrs();
}

// Delete blocks of various sizes and statuses to exercise the different code paths.
// This relies on internal validation in block manager to detect many errors.
TEST_F(BufferedBlockMgrTest, DeleteSingleBlocks) {
    int max_num_buffers = 16;
    BufferedBlockMgr2::Client* client;
    BufferedBlockMgr2* block_mgr =
            CreateMgrAndClient(0, max_num_buffers, _block_size, 0, _client_tracker, &client);

    // Pinned I/O block.
    BufferedBlockMgr2::Block* new_block;
    EXPECT_TRUE(block_mgr->get_new_block(client, nullptr, &new_block).ok());
    EXPECT_TRUE(new_block != nullptr);
    EXPECT_TRUE(new_block->is_pinned());
    EXPECT_TRUE(new_block->is_max_size());
    new_block->del();
    EXPECT_TRUE(_client_tracker->consumption() == 0);

    // Pinned non-I/O block.
    int small_block_size = 128;
    EXPECT_TRUE(block_mgr->get_new_block(client, nullptr, &new_block, small_block_size).ok());
    EXPECT_TRUE(new_block != nullptr);
    EXPECT_TRUE(new_block->is_pinned());
    EXPECT_EQ(small_block_size, _client_tracker->consumption());
    new_block->del();
    EXPECT_EQ(0, _client_tracker->consumption());

    // Unpinned I/O block - delete after written to disk.
    EXPECT_TRUE(block_mgr->get_new_block(client, nullptr, &new_block).ok());
    EXPECT_TRUE(new_block != nullptr);
    EXPECT_TRUE(new_block->is_pinned());
    EXPECT_TRUE(new_block->is_max_size());
    new_block->unpin();
    EXPECT_FALSE(new_block->is_pinned());
    WaitForWrites(block_mgr);
    new_block->del();
    EXPECT_TRUE(_client_tracker->consumption() == 0);

    // Unpinned I/O block - delete before written to disk.
    EXPECT_TRUE(block_mgr->get_new_block(client, nullptr, &new_block).ok());
    EXPECT_TRUE(new_block != nullptr);
    EXPECT_TRUE(new_block->is_pinned());
    EXPECT_TRUE(new_block->is_max_size());
    new_block->unpin();
    EXPECT_FALSE(new_block->is_pinned());
    new_block->del();
    WaitForWrites(block_mgr);
    EXPECT_TRUE(_client_tracker->consumption() == 0);

    TearDownMgrs();
}

// Test that all APIs return cancelled after close.
TEST_F(BufferedBlockMgrTest, Close) {
    int max_num_buffers = 5;
    const int block_size = 1024;
    BufferedBlockMgr2* block_mgr;
    BufferedBlockMgr2::Client* client;
    block_mgr = CreateMgrAndClient(0, max_num_buffers, block_size, 0, _client_tracker, &client);

    std::vector<BufferedBlockMgr2::Block*> blocks;
    AllocateBlocks(block_mgr, client, max_num_buffers, &blocks);

    block_mgr->cancel();

    BufferedBlockMgr2::Block* new_block;
    Status status = block_mgr->get_new_block(client, nullptr, &new_block);
    EXPECT_TRUE(status.is_cancelled());
    EXPECT_TRUE(new_block == nullptr);
    status = blocks[0]->unpin();
    EXPECT_TRUE(status.is_cancelled());
    bool pinned;
    status = blocks[0]->pin(&pinned);
    EXPECT_TRUE(status.is_cancelled());
    blocks[1]->del();

    TearDownMgrs();
}

// Clear scratch directory. Return # of files deleted.
static int clear_scratch_dir() {
    int num_files = 0;
    directory_iterator dir_it(SCRATCH_DIR);
    for (; dir_it != directory_iterator(); ++dir_it) {
        ++num_files;
        remove_all(dir_it->path());
    }
    return num_files;
}

// Test that the block manager behaves correctly after a write error.  Delete the scratch
// directory before an operation that would cause a write and test that subsequent API
// calls return 'CANCELLED' correctly.
TEST_F(BufferedBlockMgrTest, WriteError) {
    Status status;
    int max_num_buffers = 2;
    const int block_size = 1024;
    BufferedBlockMgr2* block_mgr;
    BufferedBlockMgr2::Client* client;
    block_mgr = CreateMgrAndClient(0, max_num_buffers, block_size, 0, _client_tracker, &client);

    std::vector<BufferedBlockMgr2::Block*> blocks;
    AllocateBlocks(block_mgr, client, max_num_buffers, &blocks);
    // Unpin two blocks here, to ensure that backing storage is allocated in tmp file.
    for (int i = 0; i < 2; ++i) {
        status = blocks[i]->unpin();
        EXPECT_TRUE(status.ok());
    }
    WaitForWrites(block_mgr);
    // Repin the blocks
    for (int i = 0; i < 2; ++i) {
        bool pinned;
        status = blocks[i]->pin(&pinned);
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(pinned);
    }
    // Remove the backing storage so that future writes will fail
    int num_files = clear_scratch_dir();
    EXPECT_TRUE(num_files > 0);
    for (int i = 0; i < 2; ++i) {
        status = blocks[i]->unpin();
        EXPECT_TRUE(status.ok());
    }
    WaitForWrites(block_mgr);
    // Subsequent calls should fail.
    for (int i = 0; i < 2; ++i) {
        blocks[i]->del();
    }
    BufferedBlockMgr2::Block* new_block;
    status = block_mgr->get_new_block(client, nullptr, &new_block);
    EXPECT_TRUE(status.is_cancelled());
    EXPECT_TRUE(new_block == nullptr);

    TearDownMgrs();
}

// Test block manager error handling when temporary file space cannot be allocated to
// back an unpinned buffer.
TEST_F(BufferedBlockMgrTest, TmpFileAllocateError) {
    Status status;
    int max_num_buffers = 2;
    BufferedBlockMgr2::Client* client;
    BufferedBlockMgr2* block_mgr =
            CreateMgrAndClient(0, max_num_buffers, _block_size, 0, _client_tracker, &client);

    std::vector<BufferedBlockMgr2::Block*> blocks;
    AllocateBlocks(block_mgr, client, max_num_buffers, &blocks);
    // Unpin a block, forcing a write.
    status = blocks[0]->unpin();
    EXPECT_TRUE(status.ok());
    WaitForWrites(block_mgr);
    // Remove temporary files - subsequent operations will fail.
    int num_files = clear_scratch_dir();
    EXPECT_TRUE(num_files > 0);
    // Current implementation will fail here because it tries to expand the tmp file
    // immediately. This behavior is not contractual but we want to know if it changes
    // accidentally.
    status = blocks[1]->unpin();
    EXPECT_FALSE(status.ok());

    TearDownMgrs();
}

// Test that the block manager is able to blacklist a temporary device correctly after a
// write error. We should not allocate more blocks on that device, but existing blocks
// on the device will remain in use.
/// Disabled because blacklisting was disabled as workaround for IMPALA-2305.
TEST_F(BufferedBlockMgrTest, DISABLED_WriteErrorBlacklist) {
    // TEST_F(BufferedBlockMgrTest, WriteErrorBlacklist) {
    // Set up two buffered block managers with two temporary dirs.
    std::vector<string> tmp_dirs = InitMultipleTmpDirs(2);
    // Simulate two concurrent queries.
    const int NUM_BLOCK_MGRS = 2;
    const int MAX_NUM_BLOCKS = 4;
    int blocks_per_mgr = MAX_NUM_BLOCKS / NUM_BLOCK_MGRS;
    std::vector<BufferedBlockMgr2*> block_mgrs;
    std::vector<BufferedBlockMgr2::Client*> clients;
    CreateMgrsAndClients(0, NUM_BLOCK_MGRS, blocks_per_mgr, _block_size, 0, _client_tracker,
                         &block_mgrs, &clients);

    // Allocate files for all 2x2 combinations by unpinning blocks.
    std::vector<vector<BufferedBlockMgr2::Block*>> blocks;
    std::vector<BufferedBlockMgr2::Block*> all_blocks;
    for (int i = 0; i < NUM_BLOCK_MGRS; ++i) {
        std::vector<BufferedBlockMgr2::Block*> mgr_blocks;
        AllocateBlocks(block_mgrs[i], clients[i], blocks_per_mgr, &mgr_blocks);
        UnpinBlocks(mgr_blocks);
        for (int j = 0; j < blocks_per_mgr; ++j) {
            LOG(INFO) << "Manager " << i << " Block " << j << " backed by file "
                      << mgr_blocks[j]->tmp_file_path();
        }
        blocks.push_back(mgr_blocks);
        all_blocks.insert(all_blocks.end(), mgr_blocks.begin(), mgr_blocks.end());
    }
    WaitForWrites(block_mgrs);
    int error_mgr = 0;
    int no_error_mgr = 1;
    const string& error_dir = tmp_dirs[0];
    const string& good_dir = tmp_dirs[1];
    // Delete one file from first scratch dir for first block manager.
    BufferedBlockMgr2::Block* error_block = FindBlockForDir(blocks[error_mgr], error_dir);
    EXPECT_TRUE(error_block != nullptr) << "Expected a tmp file in dir " << error_dir;
    PinBlocks(all_blocks);
    DeleteBackingFile(error_block);
    UnpinBlocks(all_blocks); // Should succeed since tmp file space was already allocated.
    WaitForWrites(block_mgrs);
    EXPECT_TRUE(block_mgrs[error_mgr]->is_cancelled());
    EXPECT_FALSE(block_mgrs[no_error_mgr]->is_cancelled());
    // Temporary device with error should no longer be active.
    std::vector<TmpFileMgr::DeviceId> active_tmp_devices =
            _test_env->tmp_file_mgr()->active_tmp_devices();
    EXPECT_EQ(tmp_dirs.size() - 1, active_tmp_devices.size());
    for (int i = 0; i < active_tmp_devices.size(); ++i) {
        const string& device_path =
                _test_env->tmp_file_mgr()->get_tmp_dir_path(active_tmp_devices[i]);
        EXPECT_EQ(string::npos, error_dir.find(device_path));
    }
    // The second block manager should continue using allocated scratch space, since it
    // didn't encounter a write error itself. In future this could change but for now it is
    // the intended behaviour.
    PinBlocks(blocks[no_error_mgr]);
    UnpinBlocks(blocks[no_error_mgr]);
    EXPECT_TRUE(FindBlockForDir(blocks[no_error_mgr], good_dir) != nullptr);
    EXPECT_TRUE(FindBlockForDir(blocks[no_error_mgr], error_dir) != nullptr);
    // The second block manager should avoid using bad directory for new blocks.
    std::vector<BufferedBlockMgr2::Block*> no_error_new_blocks;
    AllocateBlocks(block_mgrs[no_error_mgr], clients[no_error_mgr], blocks_per_mgr,
                   &no_error_new_blocks);
    UnpinBlocks(no_error_new_blocks);
    for (int i = 0; i < no_error_new_blocks.size(); ++i) {
        LOG(INFO) << "Newly created block backed by file "
                  << no_error_new_blocks[i]->tmp_file_path();
        EXPECT_TRUE(BlockInDir(no_error_new_blocks[i], good_dir));
    }
    // A new block manager should only use the good dir for backing storage.
    BufferedBlockMgr2::Client* new_client;
    BufferedBlockMgr2* new_block_mgr =
            CreateMgrAndClient(9999, blocks_per_mgr, _block_size, 0, _client_tracker, &new_client);
    std::vector<BufferedBlockMgr2::Block*> new_mgr_blocks;
    AllocateBlocks(new_block_mgr, new_client, blocks_per_mgr, &new_mgr_blocks);
    UnpinBlocks(new_mgr_blocks);
    for (int i = 0; i < blocks_per_mgr; ++i) {
        LOG(INFO) << "New manager Block " << i << " backed by file "
                  << new_mgr_blocks[i]->tmp_file_path();
        EXPECT_TRUE(BlockInDir(new_mgr_blocks[i], good_dir));
    }
}

// Check that allocation error resulting from removal of directory results in blocks
/// being allocated in other directories.
TEST_F(BufferedBlockMgrTest, AllocationErrorHandling) {
    // Set up two buffered block managers with two temporary dirs.
    std::vector<string> tmp_dirs = InitMultipleTmpDirs(2);
    // Simulate two concurrent queries.
    int num_block_mgrs = 2;
    int max_num_blocks = 4;
    int blocks_per_mgr = max_num_blocks / num_block_mgrs;
    // std::vector<RuntimeState*> runtime_states;
    std::vector<BufferedBlockMgr2*> block_mgrs;
    std::vector<BufferedBlockMgr2::Client*> clients;
    CreateMgrsAndClients(0, num_block_mgrs, blocks_per_mgr, _block_size, 0, _client_tracker,
                         &block_mgrs, &clients);

    // Allocate files for all 2x2 combinations by unpinning blocks.
    std::vector<vector<BufferedBlockMgr2::Block*>> blocks;
    for (int i = 0; i < num_block_mgrs; ++i) {
        std::vector<BufferedBlockMgr2::Block*> mgr_blocks;
        LOG(INFO) << "Iter " << i;
        AllocateBlocks(block_mgrs[i], clients[i], blocks_per_mgr, &mgr_blocks);
        blocks.push_back(mgr_blocks);
    }
    const string& bad_dir = tmp_dirs[0];
    const string& bad_scratch_subdir = bad_dir + SCRATCH_SUFFIX;
    // const string& good_dir = tmp_dirs[1];
    // const string& good_scratch_subdir = good_dir + SCRATCH_SUFFIX;
    chmod(bad_scratch_subdir.c_str(), 0);
    // The block mgr should attempt to allocate space in bad dir for one block, which will
    // cause an error when it tries to create/expand the file. It should recover and just
    // use the good dir.
    UnpinBlocks(blocks[0]);
    // Directories remain on active list even when they experience errors.
    EXPECT_EQ(2, _test_env->tmp_file_mgr()->num_active_tmp_devices());
    // Blocks should not be written to bad dir even if it remains non-writable.
    UnpinBlocks(blocks[1]);
    // All writes should succeed.
    WaitForWrites(block_mgrs);
    for (int i = 0; i < blocks.size(); ++i) {
        for (int j = 0; j < blocks[i].size(); ++j) {
            blocks[i][j]->del();
        }
    }
}

// Test that block manager fails cleanly when all directories are inaccessible at runtime.
TEST_F(BufferedBlockMgrTest, NoDirsAllocationError) {
    std::vector<string> tmp_dirs = InitMultipleTmpDirs(2);
    int max_num_buffers = 2;
    BufferedBlockMgr2::Client* client;
    BufferedBlockMgr2* block_mgr =
            CreateMgrAndClient(0, max_num_buffers, _block_size, 0, _client_tracker, &client);
    std::vector<BufferedBlockMgr2::Block*> blocks;
    AllocateBlocks(block_mgr, client, max_num_buffers, &blocks);
    for (int i = 0; i < tmp_dirs.size(); ++i) {
        const string& tmp_scratch_subdir = tmp_dirs[i] + SCRATCH_SUFFIX;
        chmod(tmp_scratch_subdir.c_str(), 0);
    }
    for (int i = 0; i < blocks.size(); ++i) {
        EXPECT_FALSE(blocks[i]->unpin().ok());
    }
}

// Create two clients with different number of reserved buffers.
TEST_F(BufferedBlockMgrTest, MultipleClients) {
    Status status;
    int client1_buffers = 3;
    int client2_buffers = 5;
    int max_num_buffers = client1_buffers + client2_buffers;
    const int block_size = 1024;
    RuntimeState* runtime_state;
    BufferedBlockMgr2* block_mgr = CreateMgr(0, max_num_buffers, block_size, &runtime_state);

    BufferedBlockMgr2::Client* client1 = nullptr;
    BufferedBlockMgr2::Client* client2 = nullptr;
    status = block_mgr->register_client(client1_buffers, _client_tracker, runtime_state, &client1);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(client1 != nullptr);
    status = block_mgr->register_client(client2_buffers, _client_tracker, runtime_state, &client2);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(client2 != nullptr);

    // Reserve client 1's and 2's buffers. They should succeed.
    bool reserved = block_mgr->try_acquire_tmp_reservation(client1, 1);
    EXPECT_TRUE(reserved);
    reserved = block_mgr->try_acquire_tmp_reservation(client2, 1);
    EXPECT_TRUE(reserved);

    std::vector<BufferedBlockMgr2::Block*> client1_blocks;
    // Allocate all of client1's reserved blocks, they should all succeed.
    AllocateBlocks(block_mgr, client1, client1_buffers, &client1_blocks);

    // Try allocating one more, that should fail.
    BufferedBlockMgr2::Block* block;
    status = block_mgr->get_new_block(client1, nullptr, &block);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(block == nullptr);

    // Trying to reserve should also fail.
    reserved = block_mgr->try_acquire_tmp_reservation(client1, 1);
    EXPECT_FALSE(reserved);

    // Allocate all of client2's reserved blocks, these should succeed.
    std::vector<BufferedBlockMgr2::Block*> client2_blocks;
    AllocateBlocks(block_mgr, client2, client2_buffers, &client2_blocks);

    // Try allocating one more from client 2, that should fail.
    status = block_mgr->get_new_block(client2, nullptr, &block);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(block == nullptr);

    // Unpin one block from client 1.
    status = client1_blocks[0]->unpin();
    EXPECT_TRUE(status.ok());

    // Client 2 should still not be able to allocate.
    status = block_mgr->get_new_block(client2, nullptr, &block);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(block == nullptr);

    // Client 2 should still not be able to reserve.
    reserved = block_mgr->try_acquire_tmp_reservation(client2, 1);
    EXPECT_FALSE(reserved);

    // Client 1 should be able to though.
    status = block_mgr->get_new_block(client1, nullptr, &block);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(block != nullptr);

    // Unpin two of client 1's blocks (client 1 should have 3 unpinned blocks now).
    status = client1_blocks[1]->unpin();
    EXPECT_TRUE(status.ok());
    status = client1_blocks[2]->unpin();
    EXPECT_TRUE(status.ok());

    // Clear client 1's reservation
    block_mgr->clear_reservations(client1);

    // Client 2 should be able to reserve 1 buffers now (there are 2 left);
    reserved = block_mgr->try_acquire_tmp_reservation(client2, 1);
    EXPECT_TRUE(reserved);

    // Client one can only pin 1.
    bool pinned;
    status = client1_blocks[0]->pin(&pinned);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(pinned);
    // Can't get this one.
    status = client1_blocks[1]->pin(&pinned);
    EXPECT_TRUE(status.ok());
    EXPECT_FALSE(pinned);

    // Client 2 can pick up the one reserved buffer
    status = block_mgr->get_new_block(client2, nullptr, &block);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(block != nullptr);
    // But not a second
    BufferedBlockMgr2::Block* block2;
    status = block_mgr->get_new_block(client2, nullptr, &block2);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(block2 == nullptr);

    // Unpin client 2's block it got from the reservation. Sine this is a tmp
    // reservation, client 1 can pick it up again (it is not longer reserved).
    status = block->unpin();
    EXPECT_TRUE(status.ok());
    status = client1_blocks[1]->pin(&pinned);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(pinned);

    TearDownMgrs();
}

// Create two clients with different number of reserved buffers and some additional.
TEST_F(BufferedBlockMgrTest, MultipleClientsExtraBuffers) {
    Status status;
    int client1_buffers = 1;
    int client2_buffers = 1;
    int max_num_buffers = client1_buffers + client2_buffers + 2;
    const int block_size = 1024;
    RuntimeState* runtime_state;
    BufferedBlockMgr2* block_mgr = CreateMgr(0, max_num_buffers, block_size, &runtime_state);

    BufferedBlockMgr2::Client* client1 = nullptr;
    BufferedBlockMgr2::Client* client2 = nullptr;
    BufferedBlockMgr2::Block* block = nullptr;
    status = block_mgr->register_client(client1_buffers, _client_tracker, runtime_state, &client1);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(client1 != nullptr);
    status = block_mgr->register_client(client2_buffers, _client_tracker, runtime_state, &client2);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(client2 != nullptr);

    std::vector<BufferedBlockMgr2::Block*> client1_blocks;
    // Allocate all of client1's reserved blocks, they should all succeed.
    AllocateBlocks(block_mgr, client1, client1_buffers, &client1_blocks);

    // Allocate all of client2's reserved blocks, these should succeed.
    std::vector<BufferedBlockMgr2::Block*> client2_blocks;
    AllocateBlocks(block_mgr, client2, client2_buffers, &client2_blocks);

    // We have two spare buffers now. Each client should be able to allocate it.
    status = block_mgr->get_new_block(client1, nullptr, &block);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(block != nullptr);
    status = block_mgr->get_new_block(client2, nullptr, &block);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(block != nullptr);

    // Now we are completely full, no one should be able to allocate a new block.
    status = block_mgr->get_new_block(client1, nullptr, &block);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(block == nullptr);
    status = block_mgr->get_new_block(client2, nullptr, &block);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(block == nullptr);

    TearDownMgrs();
}

// Create two clients causing oversubscription.
TEST_F(BufferedBlockMgrTest, ClientOversubscription) {
    Status status;
    int client1_buffers = 1;
    int client2_buffers = 2;
    int max_num_buffers = 2;
    const int block_size = 1024;
    RuntimeState* runtime_state;
    BufferedBlockMgr2* block_mgr = CreateMgr(0, max_num_buffers, block_size, &runtime_state);

    BufferedBlockMgr2::Client* client1 = nullptr;
    BufferedBlockMgr2::Client* client2 = nullptr;
    BufferedBlockMgr2::Block* block = nullptr;
    status = block_mgr->register_client(client1_buffers, _client_tracker, runtime_state, &client1);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(client1 != nullptr);
    status = block_mgr->register_client(client2_buffers, _client_tracker, runtime_state, &client2);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(client2 != nullptr);

    // Client one allocates first block, should work.
    status = block_mgr->get_new_block(client1, nullptr, &block);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(block != nullptr);

    // Client two allocates first block, should work.
    status = block_mgr->get_new_block(client2, nullptr, &block);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(block != nullptr);

    // At this point we've used both buffers. Client one reserved one so subsequent
    // calls should fail with no error (but returns no block).
    status = block_mgr->get_new_block(client1, nullptr, &block);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(block == nullptr);

    // Allocate with client two. Since client two reserved 2 buffers, this should fail
    // with MEM_LIMIT_EXCEEDED.
    status = block_mgr->get_new_block(client2, nullptr, &block);
    EXPECT_TRUE(status.is_mem_limit_exceeded());

    TearDownMgrs();
}

TEST_F(BufferedBlockMgrTest, SingleRandom_plain) {
    TestRandomInternalSingle(1024);
    TestRandomInternalSingle(8 * 1024);
    TestRandomInternalSingle(8 * 1024 * 1024);
}

TEST_F(BufferedBlockMgrTest, Multi2Random_plain) {
    TestRandomInternalMulti(2, 1024);
    TestRandomInternalMulti(2, 8 * 1024);
    TestRandomInternalMulti(2, 8 * 1024 * 1024);
}

TEST_F(BufferedBlockMgrTest, Multi4Random_plain) {
    TestRandomInternalMulti(4, 1024);
    TestRandomInternalMulti(4, 8 * 1024);
    TestRandomInternalMulti(4, 8 * 1024 * 1024);
}

// TODO: Enable when we improve concurrency/scalability of block mgr.
TEST_F(BufferedBlockMgrTest, DISABLED_Multi8Random_plain) {
    TestRandomInternalMulti(8, 1024);
}

TEST_F(BufferedBlockMgrTest, CreateDestroyMulti) {
    CreateDestroyMulti();
}

} // end namespace doris
