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

#include "exec/spill/spill_repartitioner.h"

#include <gtest/gtest.h>

#include <memory>
#include <numeric>
#include <vector>

#include "core/block/block.h"
#include "core/data_type/data_type_number.h"
#include "exec/partitioner/partitioner.h"
#include "exec/spill/spill_file.h"
#include "exec/spill/spill_file_manager.h"
#include "exec/spill/spill_file_reader.h"
#include "exec/spill/spill_file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_profile.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {

class SpillRepartitionerTest : public testing::Test {
protected:
    void SetUp() override {
        _runtime_state = std::make_unique<MockRuntimeState>();

        // Profile hierarchy required by SpillFileWriter / SpillFileReader:
        //   _profile (operator)
        //     ├── CustomCounters  (write/read timing and byte counters)
        //     └── CommonCounters  (MemoryUsage high-water-mark)
        _profile = std::make_unique<RuntimeProfile>("operator");
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

        _spill_dir = "./ut_dir/spill_repartitioner_test";
        auto spill_data_dir = std::make_unique<SpillDataDir>(_spill_dir, 256L * 1024 * 1024);
        auto st = io::global_local_filesystem()->create_directory(spill_data_dir->path(), false);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::unordered_map<std::string, std::unique_ptr<SpillDataDir>> data_map;
        data_map.emplace("test", std::move(spill_data_dir));
        auto* mgr = new SpillFileManager(std::move(data_map));
        ExecEnv::GetInstance()->_spill_file_mgr = mgr;
        st = mgr->init();
        ASSERT_TRUE(st.ok()) << st.to_string();
    }

    void TearDown() override {
        ExecEnv::GetInstance()->spill_file_mgr()->stop();
        SAFE_DELETE(ExecEnv::GetInstance()->_spill_file_mgr);
        auto st = io::global_local_filesystem()->delete_directory(_spill_dir);
        (void)st;
        _runtime_state.reset();
    }

    // Write int32 values to a new SpillFile and return it (ready for reading).
    SpillFileSPtr _make_input_file(const std::vector<int32_t>& data, const std::string& suffix) {
        SpillFileSPtr f;
        EXPECT_TRUE(ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file(suffix, f).ok());
        SpillFileWriterSPtr w;
        EXPECT_TRUE(f->create_writer(_runtime_state.get(), _profile.get(), w).ok());
        auto block = ColumnHelper::create_block<DataTypeInt32>(data);
        EXPECT_TRUE(w->write_block(_runtime_state.get(), block).ok());
        EXPECT_TRUE(w->close().ok());
        return f;
    }

    // Sum rows across all blocks in a spill file.
    int64_t _count_rows(SpillFileSPtr& f) {
        auto reader = f->create_reader(_runtime_state.get(), _profile.get());
        EXPECT_TRUE(reader->open().ok());
        int64_t total = 0;
        bool eos = false;
        while (!eos) {
            Block b;
            EXPECT_TRUE(reader->read(&b, &eos).ok());
            total += static_cast<int64_t>(b.rows());
        }
        EXPECT_TRUE(reader->close().ok());
        return total;
    }

    // Init repartitioner in column-index mode on a single int32 key at column 0.
    void _init_col_mode(SpillRepartitioner& r, int fanout, int level = 0) {
        r.init_with_key_columns({0}, {std::make_shared<DataTypeInt32>()}, _profile.get(), fanout,
                                level);
    }

    std::unique_ptr<MockRuntimeState> _runtime_state;
    std::unique_ptr<RuntimeProfile> _profile;
    std::unique_ptr<RuntimeProfile> _custom_profile;
    std::unique_ptr<RuntimeProfile> _common_profile;
    std::string _spill_dir;
};

// ── create_output_spill_files ────────────────────────────────────────────────

TEST_F(SpillRepartitionerTest, CreateOutputSpillFilesProducesCorrectFanout) {
    const int fanout = 6;
    std::vector<SpillFileSPtr> output_files;
    auto st = SpillRepartitioner::create_output_spill_files(_runtime_state.get(), /*node_id=*/1,
                                                            "test/create", fanout, output_files);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(static_cast<int>(output_files.size()), fanout);
    for (auto& f : output_files) {
        ASSERT_TRUE(f != nullptr);
    }
}

// ── fanout() accessor ────────────────────────────────────────────────────────

TEST_F(SpillRepartitionerTest, FanoutAccessorReflectsInitValue) {
    SpillRepartitioner r;
    _init_col_mode(r, /*fanout=*/5);
    ASSERT_EQ(r.fanout(), 5);
}

// ── setup_output + finalize ──────────────────────────────────────────────────

TEST_F(SpillRepartitionerTest, SetupOutputAndFinalizeSucceeds) {
    const int fanout = 4;
    SpillRepartitioner r;
    _init_col_mode(r, fanout);

    std::vector<SpillFileSPtr> output_files;
    ASSERT_TRUE(SpillRepartitioner::create_output_spill_files(_runtime_state.get(), 0, "test/setup",
                                                              fanout, output_files)
                        .ok());
    ASSERT_TRUE(r.setup_output(_runtime_state.get(), output_files).ok());
    ASSERT_TRUE(r.finalize().ok());

    // After finalize() all output writers are closed → files are ready to read.
    for (auto& f : output_files) {
        ASSERT_TRUE(f->ready_for_reading());
    }
}

// ── route_block ──────────────────────────────────────────────────────────────

TEST_F(SpillRepartitionerTest, RouteBlockConservesTotalRowCount) {
    const int fanout = 4;
    const int input_rows = 100;

    SpillRepartitioner r;
    _init_col_mode(r, fanout);

    std::vector<SpillFileSPtr> output_files;
    ASSERT_TRUE(SpillRepartitioner::create_output_spill_files(
                        _runtime_state.get(), 0, "test/route_block", fanout, output_files)
                        .ok());
    ASSERT_TRUE(r.setup_output(_runtime_state.get(), output_files).ok());

    // Build a block with values 0..input_rows-1
    std::vector<int32_t> vals(input_rows);
    std::iota(vals.begin(), vals.end(), 0);
    auto block = ColumnHelper::create_block<DataTypeInt32>(vals);

    ASSERT_TRUE(r.route_block(_runtime_state.get(), block).ok());
    ASSERT_TRUE(r.finalize().ok());

    // All rows must appear in exactly one output partition.
    int64_t total = 0;
    for (auto& f : output_files) {
        total += _count_rows(f);
    }
    ASSERT_EQ(total, input_rows);
}

TEST_F(SpillRepartitionerTest, RouteEmptyBlockIsNoop) {
    const int fanout = 4;
    SpillRepartitioner r;
    _init_col_mode(r, fanout);

    std::vector<SpillFileSPtr> output_files;
    ASSERT_TRUE(SpillRepartitioner::create_output_spill_files(
                        _runtime_state.get(), 0, "test/route_empty", fanout, output_files)
                        .ok());
    ASSERT_TRUE(r.setup_output(_runtime_state.get(), output_files).ok());

    Block empty;
    ASSERT_TRUE(r.route_block(_runtime_state.get(), empty).ok());
    ASSERT_TRUE(r.finalize().ok());

    int64_t total = 0;
    for (auto& f : output_files) {
        total += _count_rows(f);
    }
    ASSERT_EQ(total, 0);
}

// ── repartition(SpillFile) ───────────────────────────────────────────────────

TEST_F(SpillRepartitionerTest, RepartitionFromSpillFileConservesRows) {
    const int fanout = 4;
    const int input_rows = 80;

    std::vector<int32_t> vals(input_rows);
    std::iota(vals.begin(), vals.end(), 0);
    auto input_file = _make_input_file(vals, "test/repart_input");

    SpillRepartitioner r;
    _init_col_mode(r, fanout);

    std::vector<SpillFileSPtr> output_files;
    ASSERT_TRUE(SpillRepartitioner::create_output_spill_files(
                        _runtime_state.get(), 0, "test/repart_out", fanout, output_files)
                        .ok());
    ASSERT_TRUE(r.setup_output(_runtime_state.get(), output_files).ok());

    bool done = false;
    while (!done) {
        ASSERT_TRUE(r.repartition(_runtime_state.get(), input_file, &done).ok());
    }
    ASSERT_TRUE(r.finalize().ok());

    int64_t total = 0;
    for (auto& f : output_files) {
        total += _count_rows(f);
    }
    ASSERT_EQ(total, input_rows);
}

// ── repartition(SpillFileReader) ─────────────────────────────────────────────

TEST_F(SpillRepartitionerTest, RepartitionFromReaderConservesRows) {
    const int fanout = 4;
    const int input_rows = 60;

    std::vector<int32_t> vals(input_rows);
    std::iota(vals.begin(), vals.end(), 100);
    auto input_file = _make_input_file(vals, "test/repart_reader_input");

    SpillRepartitioner r;
    _init_col_mode(r, fanout);

    std::vector<SpillFileSPtr> output_files;
    ASSERT_TRUE(SpillRepartitioner::create_output_spill_files(
                        _runtime_state.get(), 0, "test/repart_reader_out", fanout, output_files)
                        .ok());
    ASSERT_TRUE(r.setup_output(_runtime_state.get(), output_files).ok());

    // Open a reader manually and pass it to the reader overload.
    auto reader = input_file->create_reader(_runtime_state.get(), _profile.get());
    ASSERT_TRUE(reader->open().ok());

    bool done = false;
    while (!done) {
        ASSERT_TRUE(r.repartition(_runtime_state.get(), reader, &done).ok());
    }
    // repartition() resets the reader to nullptr on completion.
    ASSERT_EQ(reader, nullptr);
    ASSERT_TRUE(r.finalize().ok());

    int64_t total = 0;
    for (auto& f : output_files) {
        total += _count_rows(f);
    }
    ASSERT_EQ(total, input_rows);
}

// ── level-dependent routing ──────────────────────────────────────────────────

// Route the same block at two different repartition levels and verify that the
// total row count is conserved at each level. Different levels use different
// hash salts, so the per-partition distributions will generally differ.
TEST_F(SpillRepartitionerTest, DifferentLevelsConserveRows) {
    const int fanout = 4;
    const int input_rows = 120;

    std::vector<int32_t> vals(input_rows);
    std::iota(vals.begin(), vals.end(), 0);

    auto run_at_level = [&](int level, const std::string& label) -> std::vector<int64_t> {
        SpillRepartitioner r;
        _init_col_mode(r, fanout, level);

        std::vector<SpillFileSPtr> output_files;
        EXPECT_TRUE(SpillRepartitioner::create_output_spill_files(_runtime_state.get(), 0, label,
                                                                  fanout, output_files)
                            .ok());
        EXPECT_TRUE(r.setup_output(_runtime_state.get(), output_files).ok());

        auto block = ColumnHelper::create_block<DataTypeInt32>(vals);
        EXPECT_TRUE(r.route_block(_runtime_state.get(), block).ok());
        EXPECT_TRUE(r.finalize().ok());

        std::vector<int64_t> counts(fanout);
        for (int i = 0; i < fanout; ++i) {
            counts[i] = _count_rows(output_files[i]);
        }
        return counts;
    };

    auto counts0 = run_at_level(0, "test/level0");
    auto counts1 = run_at_level(1, "test/level1");

    // Both levels must conserve all rows.
    ASSERT_EQ(std::accumulate(counts0.begin(), counts0.end(), 0LL), input_rows);
    ASSERT_EQ(std::accumulate(counts1.begin(), counts1.end(), 0LL), input_rows);

    // The per-partition distributions should differ between levels.
    ASSERT_NE(counts0, counts1) << "Level 0 and level 1 produced identical distributions; "
                                   "the level-dependent salt may not be working.";
}

// ── multiple route_block calls before finalize ───────────────────────────────

TEST_F(SpillRepartitionerTest, MultipleRouteBlockCallsConserveTotalRows) {
    const int fanout = 4;
    SpillRepartitioner r;
    _init_col_mode(r, fanout);

    std::vector<SpillFileSPtr> output_files;
    ASSERT_TRUE(SpillRepartitioner::create_output_spill_files(
                        _runtime_state.get(), 0, "test/multi_route", fanout, output_files)
                        .ok());
    ASSERT_TRUE(r.setup_output(_runtime_state.get(), output_files).ok());

    int64_t expected_total = 0;
    for (int i = 0; i < 5; ++i) {
        std::vector<int32_t> vals(20);
        std::iota(vals.begin(), vals.end(), i * 20);
        auto block = ColumnHelper::create_block<DataTypeInt32>(vals);
        ASSERT_TRUE(r.route_block(_runtime_state.get(), block).ok());
        expected_total += 20;
    }
    ASSERT_TRUE(r.finalize().ok());

    int64_t total = 0;
    for (auto& f : output_files) {
        total += _count_rows(f);
    }
    ASSERT_EQ(total, expected_total);
}

// ── repartition counter is updated ──────────────────────────────────────────

TEST_F(SpillRepartitionerTest, RepartitionRowsCounterUpdated) {
    const int fanout = 4;
    const int input_rows = 50;

    SpillRepartitioner r;
    _init_col_mode(r, fanout);

    std::vector<SpillFileSPtr> output_files;
    ASSERT_TRUE(SpillRepartitioner::create_output_spill_files(_runtime_state.get(), 0,
                                                              "test/counter", fanout, output_files)
                        .ok());
    ASSERT_TRUE(r.setup_output(_runtime_state.get(), output_files).ok());

    std::vector<int32_t> vals(input_rows);
    std::iota(vals.begin(), vals.end(), 0);
    auto block = ColumnHelper::create_block<DataTypeInt32>(vals);
    ASSERT_TRUE(r.route_block(_runtime_state.get(), block).ok());
    ASSERT_TRUE(r.finalize().ok());

    auto* rows_counter = _profile->get_counter("SpillRepartitionRows");
    ASSERT_NE(rows_counter, nullptr);
    ASSERT_EQ(rows_counter->value(), input_rows);
}

} // namespace doris
