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

#include "exec/spill/spill_file.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <memory>
#include <numeric>
#include <vector>

#include "common/config.h"
#include "core/block/block.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exec/spill/spill_file_manager.h"
#include "exec/spill/spill_file_reader.h"
#include "exec/spill/spill_file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_profile.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris::vectorized {

class SpillFileTest : public testing::Test {
protected:
    void SetUp() override {
        _runtime_state = std::make_unique<MockRuntimeState>();

        _profile = std::make_unique<RuntimeProfile>("test");
        _custom_profile = std::make_unique<RuntimeProfile>("CustomCounters");
        _common_profile = std::make_unique<RuntimeProfile>("CommonCounters");

        _common_profile->AddHighWaterMarkCounter("MemoryUsage", TUnit::BYTES, "", 1);
        ADD_TIMER_WITH_LEVEL(_common_profile.get(), "ExecTime", 1);

        ADD_TIMER_WITH_LEVEL(_custom_profile.get(), "SpillTotalTime", 1);
        ADD_TIMER_WITH_LEVEL(_custom_profile.get(), "SpillWriteTime", 1);
        ADD_COUNTER_WITH_LEVEL(_custom_profile.get(), "SpillWriteTaskWaitInQueueCount", TUnit::UNIT,
                               1);
        ADD_COUNTER_WITH_LEVEL(_custom_profile.get(), "SpillWriteTaskCount", TUnit::UNIT, 1);
        ADD_TIMER_WITH_LEVEL(_custom_profile.get(), "SpillWriteTaskWaitInQueueTime", 1);
        ADD_TIMER_WITH_LEVEL(_custom_profile.get(), "SpillWriteFileTime", 1);
        ADD_TIMER_WITH_LEVEL(_custom_profile.get(), "SpillWriteSerializeBlockTime", 1);
        ADD_COUNTER_WITH_LEVEL(_custom_profile.get(), "SpillWriteBlockCount", TUnit::UNIT, 1);
        ADD_COUNTER_WITH_LEVEL(_custom_profile.get(), "SpillWriteBlockBytes", TUnit::BYTES, 1);
        ADD_COUNTER_WITH_LEVEL(_custom_profile.get(), "SpillWriteFileBytes", TUnit::BYTES, 1);
        ADD_COUNTER_WITH_LEVEL(_custom_profile.get(), "SpillWriteRows", TUnit::UNIT, 1);
        ADD_TIMER_WITH_LEVEL(_custom_profile.get(), "SpillReadFileTime", 1);
        ADD_TIMER_WITH_LEVEL(_custom_profile.get(), "SpillReadDerializeBlockTime", 1);
        ADD_COUNTER_WITH_LEVEL(_custom_profile.get(), "SpillReadBlockCount", TUnit::UNIT, 1);
        ADD_COUNTER_WITH_LEVEL(_custom_profile.get(), "SpillReadBlockBytes", TUnit::UNIT, 1);
        ADD_COUNTER_WITH_LEVEL(_custom_profile.get(), "SpillReadFileBytes", TUnit::UNIT, 1);
        ADD_COUNTER_WITH_LEVEL(_custom_profile.get(), "SpillReadRows", TUnit::UNIT, 1);
        ADD_COUNTER_WITH_LEVEL(_custom_profile.get(), "SpillReadFileCount", TUnit::UNIT, 1);
        ADD_COUNTER_WITH_LEVEL(_custom_profile.get(), "SpillWriteFileTotalCount", TUnit::UNIT, 1);
        ADD_COUNTER_WITH_LEVEL(_custom_profile.get(), "SpillWriteFileCurrentCount", TUnit::UNIT, 1);
        ADD_COUNTER_WITH_LEVEL(_custom_profile.get(), "SpillWriteFileCurrentBytes", TUnit::UNIT, 1);

        _profile->add_child(_custom_profile.get(), true);
        _profile->add_child(_common_profile.get(), true);

        _spill_dir = "./ut_dir/spill_file_test";
        auto spill_data_dir = std::make_unique<SpillDataDir>(_spill_dir, 1024L * 1024 * 128);
        auto st = io::global_local_filesystem()->create_directory(spill_data_dir->path(), false);
        ASSERT_TRUE(st.ok()) << "create directory failed: " << st.to_string();

        std::unordered_map<std::string, std::unique_ptr<SpillDataDir>> data_map;
        _data_dir_ptr = spill_data_dir.get();
        data_map.emplace("test", std::move(spill_data_dir));
        auto* spill_file_manager = new SpillFileManager(std::move(data_map));
        ExecEnv::GetInstance()->_spill_file_mgr = spill_file_manager;
        st = spill_file_manager->init();
        ASSERT_TRUE(st.ok()) << "init spill file manager failed: " << st.to_string();
    }

    void TearDown() override {
        ExecEnv::GetInstance()->spill_file_mgr()->stop();
        SAFE_DELETE(ExecEnv::GetInstance()->_spill_file_mgr);
        // Clean up test directory
        auto st = io::global_local_filesystem()->delete_directory(_spill_dir);
        (void)st;
        _runtime_state.reset();
    }

    Block _create_int_block(const std::vector<int32_t>& data) {
        return ColumnHelper::create_block<DataTypeInt32>(data);
    }

    Block _create_two_column_block(const std::vector<int32_t>& col1,
                                   const std::vector<int64_t>& col2) {
        auto block = ColumnHelper::create_block<DataTypeInt32>(col1);
        block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>(col2));
        return block;
    }

    std::unique_ptr<MockRuntimeState> _runtime_state;
    std::unique_ptr<RuntimeProfile> _profile;
    std::unique_ptr<RuntimeProfile> _custom_profile;
    std::unique_ptr<RuntimeProfile> _common_profile;
    std::string _spill_dir;
    SpillDataDir* _data_dir_ptr = nullptr;
};

// ═══════════════════════════════════════════════════════════════════════
// SpillFile basic tests
// ═══════════════════════════════════════════════════════════════════════

TEST_F(SpillFileTest, CreateSpillFile) {
    SpillFileSPtr spill_file;
    auto st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file("test_query/test_file",
                                                                          spill_file);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_TRUE(spill_file != nullptr);
    ASSERT_FALSE(spill_file->ready_for_reading());
}

TEST_F(SpillFileTest, CreateWriterAndReader) {
    SpillFileSPtr spill_file;
    auto st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file("test_query/create_wr",
                                                                          spill_file);
    ASSERT_TRUE(st.ok()) << st.to_string();

    // Create writer
    SpillFileWriterSPtr writer;
    st = spill_file->create_writer(_runtime_state.get(), _profile.get(), writer);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_TRUE(writer != nullptr);

    // Close writer with no data written
    st = writer->close();
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_TRUE(spill_file->ready_for_reading());

    // Create reader on empty file (0 parts)
    auto reader = spill_file->create_reader(_runtime_state.get(), _profile.get());
    ASSERT_TRUE(reader != nullptr);

    st = reader->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    Block block;
    bool eos = false;
    st = reader->read(&block, &eos);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_TRUE(eos);

    st = reader->close();
    ASSERT_TRUE(st.ok()) << st.to_string();
}

// ═══════════════════════════════════════════════════════════════════════
// SpillFileWriter tests
// ═══════════════════════════════════════════════════════════════════════

TEST_F(SpillFileTest, WriteSingleBlock) {
    SpillFileSPtr spill_file;
    auto st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file("test_query/single_block",
                                                                          spill_file);
    ASSERT_TRUE(st.ok());

    SpillFileWriterSPtr writer;
    st = spill_file->create_writer(_runtime_state.get(), _profile.get(), writer);
    ASSERT_TRUE(st.ok());

    auto block = _create_int_block({1, 2, 3, 4, 5});
    st = writer->write_block(_runtime_state.get(), block);
    ASSERT_TRUE(st.ok()) << st.to_string();

    st = writer->close();
    ASSERT_TRUE(st.ok()) << st.to_string();

    ASSERT_TRUE(spill_file->ready_for_reading());

    auto* write_rows_counter = _custom_profile->get_counter("SpillWriteRows");
    ASSERT_TRUE(write_rows_counter != nullptr);
    ASSERT_EQ(write_rows_counter->value(), 5);
}

TEST_F(SpillFileTest, WriteMultipleBlocks) {
    SpillFileSPtr spill_file;
    auto st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file("test_query/multi_blocks",
                                                                          spill_file);
    ASSERT_TRUE(st.ok());

    SpillFileWriterSPtr writer;
    st = spill_file->create_writer(_runtime_state.get(), _profile.get(), writer);
    ASSERT_TRUE(st.ok());

    for (int i = 0; i < 5; ++i) {
        auto block = _create_int_block({i * 10, i * 10 + 1, i * 10 + 2});
        st = writer->write_block(_runtime_state.get(), block);
        ASSERT_TRUE(st.ok()) << "write block " << i << " failed: " << st.to_string();
    }

    st = writer->close();
    ASSERT_TRUE(st.ok()) << st.to_string();

    auto* write_rows_counter = _custom_profile->get_counter("SpillWriteRows");
    ASSERT_EQ(write_rows_counter->value(), 15);

    auto* write_block_counter = _custom_profile->get_counter("SpillWriteBlockCount");
    ASSERT_EQ(write_block_counter->value(), 5);
}

TEST_F(SpillFileTest, WriteTwoColumnBlock) {
    SpillFileSPtr spill_file;
    auto st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file("test_query/two_col",
                                                                          spill_file);
    ASSERT_TRUE(st.ok());

    SpillFileWriterSPtr writer;
    st = spill_file->create_writer(_runtime_state.get(), _profile.get(), writer);
    ASSERT_TRUE(st.ok());

    auto block = _create_two_column_block({1, 2, 3}, {100, 200, 300});
    st = writer->write_block(_runtime_state.get(), block);
    ASSERT_TRUE(st.ok()) << st.to_string();

    st = writer->close();
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_TRUE(spill_file->ready_for_reading());
}

TEST_F(SpillFileTest, WriteEmptyBlock) {
    SpillFileSPtr spill_file;
    auto st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file("test_query/empty_block",
                                                                          spill_file);
    ASSERT_TRUE(st.ok());

    SpillFileWriterSPtr writer;
    st = spill_file->create_writer(_runtime_state.get(), _profile.get(), writer);
    ASSERT_TRUE(st.ok());

    Block empty_block;
    st = writer->write_block(_runtime_state.get(), empty_block);
    ASSERT_TRUE(st.ok()) << st.to_string();

    st = writer->close();
    ASSERT_TRUE(st.ok()) << st.to_string();
}

TEST_F(SpillFileTest, DoubleCloseWriter) {
    SpillFileSPtr spill_file;
    auto st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file("test_query/double_close",
                                                                          spill_file);
    ASSERT_TRUE(st.ok());

    SpillFileWriterSPtr writer;
    st = spill_file->create_writer(_runtime_state.get(), _profile.get(), writer);
    ASSERT_TRUE(st.ok());

    auto block = _create_int_block({1, 2, 3});
    st = writer->write_block(_runtime_state.get(), block);
    ASSERT_TRUE(st.ok());

    st = writer->close();
    ASSERT_TRUE(st.ok());

    // Double close should be a no-op
    st = writer->close();
    ASSERT_TRUE(st.ok());
}

// ═══════════════════════════════════════════════════════════════════════
// SpillFileReader tests
// ═══════════════════════════════════════════════════════════════════════

TEST_F(SpillFileTest, ReadSingleBlock) {
    SpillFileSPtr spill_file;
    auto st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file("test_query/read_single",
                                                                          spill_file);
    ASSERT_TRUE(st.ok());

    // Write
    {
        SpillFileWriterSPtr writer;
        st = spill_file->create_writer(_runtime_state.get(), _profile.get(), writer);
        ASSERT_TRUE(st.ok());

        auto block = _create_int_block({10, 20, 30, 40, 50});
        st = writer->write_block(_runtime_state.get(), block);
        ASSERT_TRUE(st.ok());

        st = writer->close();
        ASSERT_TRUE(st.ok());
    }

    // Read
    auto reader = spill_file->create_reader(_runtime_state.get(), _profile.get());
    st = reader->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    Block block;
    bool eos = false;
    st = reader->read(&block, &eos);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 5);

    // Verify data
    auto col = block.get_by_position(0).column;
    ASSERT_EQ(col->get_int(0), 10);
    ASSERT_EQ(col->get_int(1), 20);
    ASSERT_EQ(col->get_int(2), 30);
    ASSERT_EQ(col->get_int(3), 40);
    ASSERT_EQ(col->get_int(4), 50);

    // Next read should be EOS
    Block block2;
    st = reader->read(&block2, &eos);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(eos);

    st = reader->close();
    ASSERT_TRUE(st.ok());
}

TEST_F(SpillFileTest, OpenCanRetryAfterFailure) {
    SpillFileSPtr spill_file;
    auto st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file("test_query/open_retry",
                                                                          spill_file);
    ASSERT_TRUE(st.ok());

    {
        SpillFileWriterSPtr writer;
        st = spill_file->create_writer(_runtime_state.get(), _profile.get(), writer);
        ASSERT_TRUE(st.ok());

        auto block = _create_int_block({7, 8, 9});
        st = writer->write_block(_runtime_state.get(), block);
        ASSERT_TRUE(st.ok());

        st = writer->close();
        ASSERT_TRUE(st.ok());
    }

    const auto part_path =
            std::filesystem::path(_spill_dir) / "spill" / "test_query" / "open_retry" / "0";
    const auto backup_path =
            std::filesystem::path(_spill_dir) / "spill" / "test_query" / "open_retry" / "0.bak";

    std::filesystem::rename(part_path, backup_path);

    auto reader = spill_file->create_reader(_runtime_state.get(), _profile.get());
    st = reader->open();
    ASSERT_FALSE(st.ok());

    std::filesystem::rename(backup_path, part_path);

    st = reader->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    Block block;
    bool eos = false;
    st = reader->read(&block, &eos);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 3);

    auto col = block.get_by_position(0).column;
    ASSERT_EQ(col->get_int(0), 7);
    ASSERT_EQ(col->get_int(1), 8);
    ASSERT_EQ(col->get_int(2), 9);
}

TEST_F(SpillFileTest, ReadMultipleBlocks) {
    SpillFileSPtr spill_file;
    auto st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file("test_query/read_multi",
                                                                          spill_file);
    ASSERT_TRUE(st.ok());

    const int num_blocks = 10;
    const int rows_per_block = 100;

    // Write
    {
        SpillFileWriterSPtr writer;
        st = spill_file->create_writer(_runtime_state.get(), _profile.get(), writer);
        ASSERT_TRUE(st.ok());

        for (int b = 0; b < num_blocks; ++b) {
            std::vector<int32_t> data(rows_per_block);
            std::iota(data.begin(), data.end(), b * rows_per_block);
            auto block = _create_int_block(data);
            st = writer->write_block(_runtime_state.get(), block);
            ASSERT_TRUE(st.ok());
        }

        st = writer->close();
        ASSERT_TRUE(st.ok());
    }

    // Read all blocks
    auto reader = spill_file->create_reader(_runtime_state.get(), _profile.get());
    st = reader->open();
    ASSERT_TRUE(st.ok());

    size_t total_rows = 0;
    int block_count = 0;
    bool eos = false;
    while (!eos) {
        Block block;
        st = reader->read(&block, &eos);
        ASSERT_TRUE(st.ok()) << st.to_string();
        if (!eos) {
            total_rows += block.rows();
            ++block_count;
        }
    }

    ASSERT_EQ(total_rows, num_blocks * rows_per_block);
    ASSERT_EQ(block_count, num_blocks);

    st = reader->close();
    ASSERT_TRUE(st.ok());
}

TEST_F(SpillFileTest, ReadTwoColumnBlock) {
    SpillFileSPtr spill_file;
    auto st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file("test_query/read_two_col",
                                                                          spill_file);
    ASSERT_TRUE(st.ok());

    // Write
    {
        SpillFileWriterSPtr writer;
        st = spill_file->create_writer(_runtime_state.get(), _profile.get(), writer);
        ASSERT_TRUE(st.ok());

        auto block = _create_two_column_block({1, 2, 3, 4}, {100, 200, 300, 400});
        st = writer->write_block(_runtime_state.get(), block);
        ASSERT_TRUE(st.ok());

        st = writer->close();
        ASSERT_TRUE(st.ok());
    }

    // Read
    auto reader = spill_file->create_reader(_runtime_state.get(), _profile.get());
    st = reader->open();
    ASSERT_TRUE(st.ok());

    Block block;
    bool eos = false;
    st = reader->read(&block, &eos);
    ASSERT_TRUE(st.ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 4);
    ASSERT_EQ(block.columns(), 2);

    // Verify col1
    auto col1 = block.get_by_position(0).column;
    ASSERT_EQ(col1->get_int(0), 1);
    ASSERT_EQ(col1->get_int(3), 4);

    // Verify col2
    auto col2 = block.get_by_position(1).column;
    ASSERT_EQ(col2->get_int(0), 100);
    ASSERT_EQ(col2->get_int(3), 400);

    st = reader->close();
    ASSERT_TRUE(st.ok());
}

// ═══════════════════════════════════════════════════════════════════════
// Roundtrip tests (write -> read -> verify)
// ═══════════════════════════════════════════════════════════════════════

TEST_F(SpillFileTest, RoundtripSingleBlock) {
    SpillFileSPtr spill_file;
    auto st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file(
            "test_query/roundtrip_single", spill_file);
    ASSERT_TRUE(st.ok());

    std::vector<int32_t> original_data = {42, 7, 99, 1, 0, -5, 1000};

    // Write
    {
        SpillFileWriterSPtr writer;
        st = spill_file->create_writer(_runtime_state.get(), _profile.get(), writer);
        ASSERT_TRUE(st.ok());

        auto block = _create_int_block(original_data);
        st = writer->write_block(_runtime_state.get(), block);
        ASSERT_TRUE(st.ok());

        st = writer->close();
        ASSERT_TRUE(st.ok());
    }

    // Read & verify
    auto reader = spill_file->create_reader(_runtime_state.get(), _profile.get());
    st = reader->open();
    ASSERT_TRUE(st.ok());

    Block block;
    bool eos = false;
    st = reader->read(&block, &eos);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(block.rows(), original_data.size());

    auto col = block.get_by_position(0).column;
    for (size_t i = 0; i < original_data.size(); ++i) {
        ASSERT_EQ(col->get_int(i), original_data[i]) << "mismatch at index " << i;
    }

    st = reader->close();
    ASSERT_TRUE(st.ok());
}

TEST_F(SpillFileTest, RoundtripMultipleBlocks) {
    SpillFileSPtr spill_file;
    auto st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file(
            "test_query/roundtrip_multi", spill_file);
    ASSERT_TRUE(st.ok());

    std::vector<std::vector<int32_t>> all_data = {
            {1, 2, 3},
            {10, 20, 30, 40},
            {100, 200},
            {-1, -2, -3, -4, -5},
    };

    // Write
    {
        SpillFileWriterSPtr writer;
        st = spill_file->create_writer(_runtime_state.get(), _profile.get(), writer);
        ASSERT_TRUE(st.ok());

        for (const auto& data : all_data) {
            auto block = _create_int_block(data);
            st = writer->write_block(_runtime_state.get(), block);
            ASSERT_TRUE(st.ok());
        }

        st = writer->close();
        ASSERT_TRUE(st.ok());
    }

    // Read & verify
    auto reader = spill_file->create_reader(_runtime_state.get(), _profile.get());
    st = reader->open();
    ASSERT_TRUE(st.ok());

    size_t block_idx = 0;
    bool eos = false;
    while (!eos && block_idx < all_data.size()) {
        Block block;
        st = reader->read(&block, &eos);
        ASSERT_TRUE(st.ok());
        if (eos) break;

        ASSERT_EQ(block.rows(), all_data[block_idx].size())
                << "block " << block_idx << " row count mismatch";

        auto col = block.get_by_position(0).column;
        for (size_t i = 0; i < all_data[block_idx].size(); ++i) {
            ASSERT_EQ(col->get_int(i), all_data[block_idx][i])
                    << "mismatch at block " << block_idx << " row " << i;
        }
        ++block_idx;
    }

    ASSERT_EQ(block_idx, all_data.size());

    st = reader->close();
    ASSERT_TRUE(st.ok());
}

TEST_F(SpillFileTest, RoundtripLargeData) {
    SpillFileSPtr spill_file;
    auto st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file(
            "test_query/roundtrip_large", spill_file);
    ASSERT_TRUE(st.ok());

    const size_t row_count = 100000;
    std::vector<int32_t> data(row_count);
    std::iota(data.begin(), data.end(), 0);

    // Write
    {
        SpillFileWriterSPtr writer;
        st = spill_file->create_writer(_runtime_state.get(), _profile.get(), writer);
        ASSERT_TRUE(st.ok());

        auto block = _create_int_block(data);
        st = writer->write_block(_runtime_state.get(), block);
        ASSERT_TRUE(st.ok());

        st = writer->close();
        ASSERT_TRUE(st.ok());
    }

    // Read & verify
    auto reader = spill_file->create_reader(_runtime_state.get(), _profile.get());
    st = reader->open();
    ASSERT_TRUE(st.ok());

    Block block;
    bool eos = false;
    st = reader->read(&block, &eos);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(block.rows(), row_count);

    auto col = block.get_by_position(0).column;
    for (size_t i = 0; i < row_count; i += 1000) {
        ASSERT_EQ(col->get_int(i), (int32_t)i) << "mismatch at index " << i;
    }

    st = reader->close();
    ASSERT_TRUE(st.ok());
}

// ═══════════════════════════════════════════════════════════════════════
// Part rotation tests
// ═══════════════════════════════════════════════════════════════════════

TEST_F(SpillFileTest, PartRotation) {
    // Set a very small part size to force rotation
    auto saved_part_size = config::spill_file_part_size_bytes;
    config::spill_file_part_size_bytes = 1024; // 1KB per part

    SpillFileSPtr spill_file;
    auto st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file("test_query/rotation",
                                                                          spill_file);
    ASSERT_TRUE(st.ok());

    const int num_blocks = 20;

    // Write many blocks to trigger multiple part rotations
    {
        SpillFileWriterSPtr writer;
        st = spill_file->create_writer(_runtime_state.get(), _profile.get(), writer);
        ASSERT_TRUE(st.ok());

        for (int i = 0; i < num_blocks; ++i) {
            std::vector<int32_t> data(100);
            std::iota(data.begin(), data.end(), i * 100);
            auto block = _create_int_block(data);
            st = writer->write_block(_runtime_state.get(), block);
            ASSERT_TRUE(st.ok());
        }

        st = writer->close();
        ASSERT_TRUE(st.ok());
    }

    // Read back and verify all data across multiple parts
    auto reader = spill_file->create_reader(_runtime_state.get(), _profile.get());
    st = reader->open();
    ASSERT_TRUE(st.ok());

    size_t total_rows = 0;
    int block_count = 0;
    bool eos = false;
    while (!eos) {
        Block block;
        st = reader->read(&block, &eos);
        ASSERT_TRUE(st.ok());
        if (!eos) {
            total_rows += block.rows();
            ++block_count;
        }
    }

    ASSERT_EQ(total_rows, num_blocks * 100);
    ASSERT_EQ(block_count, num_blocks);

    st = reader->close();
    ASSERT_TRUE(st.ok());

    config::spill_file_part_size_bytes = saved_part_size;
}

TEST_F(SpillFileTest, PartRotationDataIntegrity) {
    // Set a small part size to force rotation
    auto saved_part_size = config::spill_file_part_size_bytes;
    config::spill_file_part_size_bytes = 512;

    SpillFileSPtr spill_file;
    auto st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file(
            "test_query/rotation_integrity", spill_file);
    ASSERT_TRUE(st.ok());

    std::vector<std::vector<int32_t>> all_data;
    const int num_blocks = 30;

    // Write
    {
        SpillFileWriterSPtr writer;
        st = spill_file->create_writer(_runtime_state.get(), _profile.get(), writer);
        ASSERT_TRUE(st.ok());

        for (int i = 0; i < num_blocks; ++i) {
            std::vector<int32_t> data(50);
            std::iota(data.begin(), data.end(), i * 1000);
            all_data.push_back(data);
            auto block = _create_int_block(data);
            st = writer->write_block(_runtime_state.get(), block);
            ASSERT_TRUE(st.ok());
        }

        st = writer->close();
        ASSERT_TRUE(st.ok());
    }

    // Read & verify data integrity across parts
    auto reader = spill_file->create_reader(_runtime_state.get(), _profile.get());
    st = reader->open();
    ASSERT_TRUE(st.ok());

    size_t block_idx = 0;
    bool eos = false;
    while (!eos) {
        Block block;
        st = reader->read(&block, &eos);
        ASSERT_TRUE(st.ok());
        if (eos) break;

        ASSERT_LT(block_idx, all_data.size());
        ASSERT_EQ(block.rows(), all_data[block_idx].size());

        auto col = block.get_by_position(0).column;
        for (size_t i = 0; i < all_data[block_idx].size(); ++i) {
            ASSERT_EQ(col->get_int(i), all_data[block_idx][i])
                    << "data mismatch at block " << block_idx << " row " << i;
        }
        ++block_idx;
    }

    ASSERT_EQ(block_idx, all_data.size());

    st = reader->close();
    ASSERT_TRUE(st.ok());

    config::spill_file_part_size_bytes = saved_part_size;
}

// ═══════════════════════════════════════════════════════════════════════
// Seek tests
// ═══════════════════════════════════════════════════════════════════════

TEST_F(SpillFileTest, SeekToBlock) {
    SpillFileSPtr spill_file;
    auto st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file("test_query/seek",
                                                                          spill_file);
    ASSERT_TRUE(st.ok());

    const int num_blocks = 5;

    // Write
    {
        SpillFileWriterSPtr writer;
        st = spill_file->create_writer(_runtime_state.get(), _profile.get(), writer);
        ASSERT_TRUE(st.ok());

        for (int i = 0; i < num_blocks; ++i) {
            auto block = _create_int_block({i * 10, i * 10 + 1, i * 10 + 2});
            st = writer->write_block(_runtime_state.get(), block);
            ASSERT_TRUE(st.ok());
        }

        st = writer->close();
        ASSERT_TRUE(st.ok());
    }

    // Seek to block 2 (0-based) and read
    auto reader = spill_file->create_reader(_runtime_state.get(), _profile.get());
    st = reader->open();
    ASSERT_TRUE(st.ok());

    st = reader->seek(2);
    ASSERT_TRUE(st.ok()) << st.to_string();

    Block block;
    bool eos = false;
    st = reader->read(&block, &eos);
    ASSERT_TRUE(st.ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 3);

    auto col = block.get_by_position(0).column;
    ASSERT_EQ(col->get_int(0), 20);
    ASSERT_EQ(col->get_int(1), 21);
    ASSERT_EQ(col->get_int(2), 22);

    st = reader->close();
    ASSERT_TRUE(st.ok());
}

TEST_F(SpillFileTest, SeekBeyondEnd) {
    SpillFileSPtr spill_file;
    auto st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file("test_query/seek_beyond",
                                                                          spill_file);
    ASSERT_TRUE(st.ok());

    // Write 3 blocks
    {
        SpillFileWriterSPtr writer;
        st = spill_file->create_writer(_runtime_state.get(), _profile.get(), writer);
        ASSERT_TRUE(st.ok());

        for (int i = 0; i < 3; ++i) {
            auto block = _create_int_block({i});
            st = writer->write_block(_runtime_state.get(), block);
            ASSERT_TRUE(st.ok());
        }

        st = writer->close();
        ASSERT_TRUE(st.ok());
    }

    auto reader = spill_file->create_reader(_runtime_state.get(), _profile.get());
    st = reader->open();
    ASSERT_TRUE(st.ok());

    // Seek beyond the end
    st = reader->seek(100);
    ASSERT_TRUE(st.ok()) << st.to_string();

    Block block;
    bool eos = false;
    st = reader->read(&block, &eos);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(eos);

    st = reader->close();
    ASSERT_TRUE(st.ok());
}

// ═══════════════════════════════════════════════════════════════════════
// SpillFile GC/lifecycle tests
// ═══════════════════════════════════════════════════════════════════════

TEST_F(SpillFileTest, GCCleansUpFiles) {
    std::string spill_file_dir;

    {
        SpillFileSPtr spill_file;
        auto st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file("test_query/gc_test",
                                                                              spill_file);
        ASSERT_TRUE(st.ok());

        SpillFileWriterSPtr writer;
        st = spill_file->create_writer(_runtime_state.get(), _profile.get(), writer);
        ASSERT_TRUE(st.ok());

        auto block = _create_int_block({1, 2, 3});
        st = writer->write_block(_runtime_state.get(), block);
        ASSERT_TRUE(st.ok());

        st = writer->close();
        ASSERT_TRUE(st.ok());

        // Remember the spill directory path
        spill_file_dir = _data_dir_ptr->get_spill_data_path() + "/test_query/gc_test";

        // Verify directory exists
        bool exists = false;
        st = io::global_local_filesystem()->exists(spill_file_dir, &exists);
        ASSERT_TRUE(st.ok());
        ASSERT_TRUE(exists);

        // spill_file goes out of scope here, destructor calls gc()
    }

    // After SpillFile is destroyed, the directory should be cleaned up
    bool exists = false;
    auto st = io::global_local_filesystem()->exists(spill_file_dir, &exists);
    ASSERT_TRUE(st.ok());
    ASSERT_FALSE(exists);
}

TEST_F(SpillFileTest, DeleteSpillFileThroughManager) {
    SpillFileSPtr spill_file;
    auto st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file("test_query/mgr_delete",
                                                                          spill_file);
    ASSERT_TRUE(st.ok());

    SpillFileWriterSPtr writer;
    st = spill_file->create_writer(_runtime_state.get(), _profile.get(), writer);
    ASSERT_TRUE(st.ok());

    auto block = _create_int_block({1, 2, 3});
    st = writer->write_block(_runtime_state.get(), block);
    ASSERT_TRUE(st.ok());

    st = writer->close();
    ASSERT_TRUE(st.ok());

    // Delete through manager (async GC)
    ExecEnv::GetInstance()->spill_file_mgr()->delete_spill_file(spill_file);

    // Run GC to process the deletion
    ExecEnv::GetInstance()->spill_file_mgr()->gc(1000);
}

// ═══════════════════════════════════════════════════════════════════════
// SpillFileManager tests
// ═══════════════════════════════════════════════════════════════════════

TEST_F(SpillFileTest, ManagerNextId) {
    auto id1 = ExecEnv::GetInstance()->spill_file_mgr()->next_id();
    auto id2 = ExecEnv::GetInstance()->spill_file_mgr()->next_id();
    auto id3 = ExecEnv::GetInstance()->spill_file_mgr()->next_id();

    ASSERT_EQ(id2, id1 + 1);
    ASSERT_EQ(id3, id2 + 1);
}

TEST_F(SpillFileTest, ManagerCreateMultipleFiles) {
    const int num_files = 5;
    std::vector<SpillFileSPtr> files;

    for (int i = 0; i < num_files; ++i) {
        SpillFileSPtr spill_file;
        auto st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file(
                fmt::format("test_query/multi_{}", i), spill_file);
        ASSERT_TRUE(st.ok()) << "create file " << i << " failed: " << st.to_string();
        files.push_back(spill_file);
    }

    // Write and close each file
    for (int i = 0; i < num_files; ++i) {
        SpillFileWriterSPtr writer;
        auto st = files[i]->create_writer(_runtime_state.get(), _profile.get(), writer);
        ASSERT_TRUE(st.ok());

        auto block = _create_int_block({i * 100});
        st = writer->write_block(_runtime_state.get(), block);
        ASSERT_TRUE(st.ok());

        st = writer->close();
        ASSERT_TRUE(st.ok());
    }

    // Read each file and verify
    for (int i = 0; i < num_files; ++i) {
        auto reader = files[i]->create_reader(_runtime_state.get(), _profile.get());
        auto st = reader->open();
        ASSERT_TRUE(st.ok());

        Block block;
        bool eos = false;
        st = reader->read(&block, &eos);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(block.rows(), 1);

        auto col = block.get_by_position(0).column;
        ASSERT_EQ(col->get_int(0), i * 100);

        st = reader->close();
        ASSERT_TRUE(st.ok());
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Profile counter tests
// ═══════════════════════════════════════════════════════════════════════

TEST_F(SpillFileTest, WriteCounters) {
    SpillFileSPtr spill_file;
    auto st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file("test_query/counters",
                                                                          spill_file);
    ASSERT_TRUE(st.ok());

    SpillFileWriterSPtr writer;
    st = spill_file->create_writer(_runtime_state.get(), _profile.get(), writer);
    ASSERT_TRUE(st.ok());

    auto block = _create_int_block({1, 2, 3, 4, 5});
    st = writer->write_block(_runtime_state.get(), block);
    ASSERT_TRUE(st.ok());

    auto block2 = _create_int_block({10, 20, 30});
    st = writer->write_block(_runtime_state.get(), block2);
    ASSERT_TRUE(st.ok());

    st = writer->close();
    ASSERT_TRUE(st.ok());

    auto* write_rows = _custom_profile->get_counter("SpillWriteRows");
    ASSERT_TRUE(write_rows != nullptr);
    ASSERT_EQ(write_rows->value(), 8);

    auto* write_blocks = _custom_profile->get_counter("SpillWriteBlockCount");
    ASSERT_TRUE(write_blocks != nullptr);
    ASSERT_EQ(write_blocks->value(), 2);

    auto* write_bytes = _custom_profile->get_counter("SpillWriteFileBytes");
    ASSERT_TRUE(write_bytes != nullptr);
    ASSERT_GT(write_bytes->value(), 0);
}

TEST_F(SpillFileTest, ReadCounters) {
    SpillFileSPtr spill_file;
    auto st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file(
            "test_query/read_counters", spill_file);
    ASSERT_TRUE(st.ok());

    // Write
    {
        SpillFileWriterSPtr writer;
        st = spill_file->create_writer(_runtime_state.get(), _profile.get(), writer);
        ASSERT_TRUE(st.ok());

        auto block = _create_int_block({1, 2, 3, 4, 5});
        st = writer->write_block(_runtime_state.get(), block);
        ASSERT_TRUE(st.ok());

        st = writer->close();
        ASSERT_TRUE(st.ok());
    }

    // Read
    auto reader = spill_file->create_reader(_runtime_state.get(), _profile.get());
    st = reader->open();
    ASSERT_TRUE(st.ok());

    Block block;
    bool eos = false;
    st = reader->read(&block, &eos);
    ASSERT_TRUE(st.ok());

    st = reader->close();
    ASSERT_TRUE(st.ok());

    auto* read_blocks = _custom_profile->get_counter("SpillReadBlockCount");
    ASSERT_TRUE(read_blocks != nullptr);
    ASSERT_EQ(read_blocks->value(), 1);

    auto* read_rows = _custom_profile->get_counter("SpillReadRows");
    ASSERT_TRUE(read_rows != nullptr);
    ASSERT_EQ(read_rows->value(), 5);

    auto* read_file_size = _custom_profile->get_counter("SpillReadFileBytes");
    ASSERT_TRUE(read_file_size != nullptr);
    ASSERT_GT(read_file_size->value(), 0);
}

// ═══════════════════════════════════════════════════════════════════════
// SpillDataDir tests
// ═══════════════════════════════════════════════════════════════════════

TEST_F(SpillFileTest, DataDirCapacityTracking) {
    SpillFileSPtr spill_file;
    auto st = ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file("test_query/capacity",
                                                                          spill_file);
    ASSERT_TRUE(st.ok());

    auto initial_bytes = _data_dir_ptr->get_spill_data_bytes();

    SpillFileWriterSPtr writer;
    st = spill_file->create_writer(_runtime_state.get(), _profile.get(), writer);
    ASSERT_TRUE(st.ok());

    // Write a block to increase usage
    std::vector<int32_t> data(1000);
    std::iota(data.begin(), data.end(), 0);
    auto block = _create_int_block(data);
    st = writer->write_block(_runtime_state.get(), block);
    ASSERT_TRUE(st.ok());

    st = writer->close();
    ASSERT_TRUE(st.ok());

    auto after_write_bytes = _data_dir_ptr->get_spill_data_bytes();
    ASSERT_GT(after_write_bytes, initial_bytes);
}

} // namespace doris::vectorized
