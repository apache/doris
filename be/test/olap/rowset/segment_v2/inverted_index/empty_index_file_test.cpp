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

#include "gtest/gtest_pred_impl.h"
#include "io/fs/stream_sink_file_writer.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/index_file_writer.h"
#include "vec/sink/load_stream_stub.h"

namespace doris {

constexpr int64_t LOAD_ID_LO = 1;
constexpr int64_t LOAD_ID_HI = 2;
constexpr int64_t NUM_STREAM = 3;
constexpr static std::string_view tmp_dir = "./ut_dir/tmp";
class EmptyIndexFileTest : public testing::Test {
    class MockStreamStub : public LoadStreamStub {
    public:
        MockStreamStub(PUniqueId load_id, int64_t src_id)
                : LoadStreamStub(load_id, src_id, std::make_shared<IndexToTabletSchema>(),
                                 std::make_shared<IndexToEnableMoW>()) {};

        virtual ~MockStreamStub() = default;

        // APPEND_DATA
        virtual Status append_data(int64_t partition_id, int64_t index_id, int64_t tablet_id,
                                   int32_t segment_id, uint64_t offset, std::span<const Slice> data,
                                   bool segment_eos = false,
                                   FileType file_type = FileType::SEGMENT_FILE) override {
            EXPECT_TRUE(segment_eos);
            return Status::OK();
        }
    };

public:
    EmptyIndexFileTest() = default;
    ~EmptyIndexFileTest() = default;

protected:
    virtual void SetUp() {
        _load_id.set_hi(LOAD_ID_HI);
        _load_id.set_lo(LOAD_ID_LO);
        for (int src_id = 0; src_id < NUM_STREAM; src_id++) {
            _streams.emplace_back(new MockStreamStub(_load_id, src_id));
        }
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(tmp_dir).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(std::string(tmp_dir), 1024000000);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        EXPECT_TRUE(tmp_file_dirs->init().ok());
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));
    }

    virtual void TearDown() {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
    }

    PUniqueId _load_id;
    std::vector<std::shared_ptr<LoadStreamStub>> _streams;
};

TEST_F(EmptyIndexFileTest, test_empty_index_file) {
    io::FileWriterPtr file_writer = std::make_unique<io::StreamSinkFileWriter>(_streams);
    auto fs = io::global_local_filesystem();
    std::string index_path = "/tmp/empty_index_file_test";
    std::string rowset_id = "1234567890";
    int64_t seg_id = 1234567890;
    auto index_file_writer = std::make_unique<segment_v2::IndexFileWriter>(
            fs, index_path, rowset_id, seg_id, InvertedIndexStorageFormatPB::V2,
            std::move(file_writer), false);
    EXPECT_TRUE(index_file_writer->close().ok());
}

} // namespace doris
