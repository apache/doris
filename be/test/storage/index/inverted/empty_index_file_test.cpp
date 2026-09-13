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

#include <brpc/channel.h>
#include <brpc/server.h>

#include "exec/sink/load_stream_stub.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/stream_sink_file_writer.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/index_writer.h"
#include "storage/olap_common.h"

namespace doris {

constexpr int64_t LOAD_ID_LO = 1;
constexpr int64_t LOAD_ID_HI = 2;
constexpr int64_t NUM_STREAM = 3;
constexpr static std::string_view tmp_dir = "./ut_dir/tmp";
class EmptyIndexFileTest : public testing::TestWithParam<InvertedIndexStorageFormatPB> {
    struct WriteState {
        size_t bytes_appended = 0;
        int data_calls = 0;
        int eos_calls = 0;
    };

    class MockStreamStub : public LoadStreamStub {
    public:
        MockStreamStub(PUniqueId load_id, int64_t src_id, std::shared_ptr<WriteState> state)
                : LoadStreamStub(load_id, src_id, std::make_shared<IndexToTabletSchema>(),
                                 std::make_shared<IndexToEnableMoW>()),
                  _state(std::move(state)) {};

        ~MockStreamStub() override = default;

        // APPEND_DATA
        Status append_data(int64_t partition_id, int64_t index_id, int64_t tablet_id,
                           int32_t segment_id, uint64_t offset, std::span<const Slice> data,
                           bool segment_eos = false,
                           FileType file_type = FileType::SEGMENT_FILE) override {
            EXPECT_EQ(offset, _state->bytes_appended);
            if (segment_eos) {
                ++_state->eos_calls;
                EXPECT_TRUE(data.empty());
                return Status::OK();
            }
            ++_state->data_calls;
            for (const auto& slice : data) {
                _state->bytes_appended += slice.size;
            }
            return Status::OK();
        }

    private:
        std::shared_ptr<WriteState> _state;
    };

public:
    EmptyIndexFileTest() = default;
    ~EmptyIndexFileTest() override = default;

protected:
    void SetUp() override {
        _load_id.set_hi(LOAD_ID_HI);
        _load_id.set_lo(LOAD_ID_LO);
        for (int src_id = 0; src_id < NUM_STREAM; src_id++) {
            auto state = std::make_shared<WriteState>();
            _write_states.push_back(state);
            _streams.emplace_back(new MockStreamStub(_load_id, src_id, std::move(state)));
        }
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(tmp_dir).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(std::string(tmp_dir), 1024000000);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        EXPECT_TRUE(tmp_file_dirs->init().ok());
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
    }

    PUniqueId _load_id;
    std::vector<std::shared_ptr<LoadStreamStub>> _streams;
    std::vector<std::shared_ptr<WriteState>> _write_states;
};

TEST_P(EmptyIndexFileTest, PreservesZeroByteFileWhenNoLogicalIndexes) {
    io::FileWriterPtr file_writer = std::make_unique<io::StreamSinkFileWriter>(_streams);
    auto fs = io::global_local_filesystem();
    std::string index_path = "/tmp/empty_index_file_test";
    std::string rowset_id = "1234567890";
    int64_t seg_id = 1234567890;
    auto index_file_writer = std::make_unique<segment_v2::IndexFileWriter>(
            fs, index_path, rowset_id, seg_id, GetParam(), std::move(file_writer), false);
    EXPECT_TRUE(index_file_writer->begin_close().ok());
    EXPECT_TRUE(index_file_writer->finish_close().ok());
    for (const auto& state : _write_states) {
        EXPECT_EQ(state->bytes_appended, 0);
        EXPECT_EQ(state->data_calls, 0);
        EXPECT_EQ(state->eos_calls, 1);
    }
}

INSTANTIATE_TEST_SUITE_P(LegacyCompoundFormats, EmptyIndexFileTest,
                         testing::Values(InvertedIndexStorageFormatPB::V2,
                                         InvertedIndexStorageFormatPB::V3));

} // namespace doris
