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
#include "io/fs/local_file_system.h"
#include "io/fs/stream_sink_file_writer.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/index_writer.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/olap_common.h"

namespace doris {

constexpr int64_t LOAD_ID_LO = 1;
constexpr int64_t LOAD_ID_HI = 2;
constexpr int64_t NUM_STREAM = 3;
constexpr static std::string_view tmp_dir = "./ut_dir/empty_index_file";
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
    // Implements FileWriter and NOTHING else: no concrete writer type can be
    // recognised here, so a close path that dispatches on the implementation
    // instead of the interface leaves this writer open and fails the test.
    class RecordingFileWriter final : public io::FileWriter {
    public:
        Status close(bool non_block = false) override {
            close_calls.push_back(non_block);
            EXPECT_NE(_state, State::CLOSED);
            if (non_block) {
                EXPECT_EQ(_state, State::OPENED);
                RETURN_IF_ERROR(begin_status);
                _state = close_synchronously ? State::CLOSED : State::ASYNC_CLOSING;
                return Status::OK();
            }
            EXPECT_EQ(_state, State::ASYNC_CLOSING);
            _state = State::CLOSED;
            return finish_status;
        }

        Status appendv(const Slice* data, size_t data_cnt) override {
            ++append_calls;
            for (size_t i = 0; i < data_cnt; ++i) {
                _bytes_appended += data[i].size;
            }
            return Status::OK();
        }

        const io::Path& path() const override { return _path; }
        size_t bytes_appended() const override { return _bytes_appended; }
        State state() const override { return _state; }

        std::vector<bool> close_calls;
        int append_calls = 0;
        bool close_synchronously = false;
        Status begin_status = Status::OK();
        Status finish_status = Status::OK();

    private:
        io::Path _path {"recording_0.idx"};
        size_t _bytes_appended = 0;
        State _state = State::OPENED;
    };

    static std::string index_path_prefix() { return std::string(tmp_dir) + "/empty_0"; }

    static std::unique_ptr<segment_v2::IndexFileWriter> make_index_writer(
            io::FileWriterPtr file_writer, InvertedIndexStorageFormatPB format) {
        return std::make_unique<segment_v2::IndexFileWriter>(io::global_local_filesystem(),
                                                             index_path_prefix(), "empty", 0,
                                                             format, std::move(file_writer), false);
    }

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
    auto file_writer = std::make_unique<io::StreamSinkFileWriter>(_streams);
    file_writer->init(_load_id, 1, 2, 3, 0, FileType::INVERTED_INDEX_FILE);
    auto* stream_writer = file_writer.get();
    auto index_file_writer = make_index_writer(std::move(file_writer), GetParam());
    ASSERT_TRUE(index_file_writer->begin_close().ok());
    EXPECT_EQ(stream_writer->state(), io::FileWriter::State::ASYNC_CLOSING);
    ASSERT_TRUE(index_file_writer->finish_close().ok());
    EXPECT_EQ(stream_writer->state(), io::FileWriter::State::CLOSED);
    // Finishing an already closed empty file must not send a second EOS.
    ASSERT_TRUE(index_file_writer->finish_close().ok());
    index_file_writer.reset();
    for (const auto& state : _write_states) {
        EXPECT_EQ(state->bytes_appended, 0);
        EXPECT_EQ(state->data_calls, 0);
        EXPECT_EQ(state->eos_calls, 1);
    }
}

TEST_P(EmptyIndexFileTest, ClosesOpaqueWriterWithoutAppending) {
    auto file_writer = std::make_unique<RecordingFileWriter>();
    auto* recording = file_writer.get();
    auto index_writer = make_index_writer(std::move(file_writer), GetParam());

    ASSERT_TRUE(index_writer->begin_close().ok());
    EXPECT_EQ(recording->close_calls, (std::vector<bool> {true}));
    EXPECT_EQ(recording->state(), io::FileWriter::State::ASYNC_CLOSING);
    ASSERT_TRUE(index_writer->finish_close().ok());
    EXPECT_EQ(recording->close_calls, (std::vector<bool> {true, false}));
    EXPECT_EQ(recording->state(), io::FileWriter::State::CLOSED);
    ASSERT_TRUE(index_writer->finish_close().ok());
    EXPECT_EQ(recording->close_calls, (std::vector<bool> {true, false}));
    // An empty index file is empty: closing it must not write a header.
    EXPECT_EQ(recording->append_calls, 0);
    EXPECT_EQ(recording->bytes_appended(), 0);
}

TEST_P(EmptyIndexFileTest, SkipsAlreadyClosedWriter) {
    auto file_writer = std::make_unique<RecordingFileWriter>();
    auto* recording = file_writer.get();
    ASSERT_TRUE(recording->close(true).ok());
    ASSERT_TRUE(recording->close(false).ok());
    recording->close_calls.clear();
    auto index_writer = make_index_writer(std::move(file_writer), GetParam());
    ASSERT_TRUE(index_writer->begin_close().ok());
    ASSERT_TRUE(index_writer->finish_close().ok());
    EXPECT_TRUE(recording->close_calls.empty());
}

TEST_P(EmptyIndexFileTest, SkipsFinishWhenBeginClosesSynchronously) {
    auto file_writer = std::make_unique<RecordingFileWriter>();
    auto* recording = file_writer.get();
    recording->close_synchronously = true;
    auto index_writer = make_index_writer(std::move(file_writer), GetParam());
    ASSERT_TRUE(index_writer->begin_close().ok());
    EXPECT_EQ(recording->state(), io::FileWriter::State::CLOSED);
    ASSERT_TRUE(index_writer->finish_close().ok());
    EXPECT_EQ(recording->close_calls, (std::vector<bool> {true}));
}

TEST_P(EmptyIndexFileTest, PropagatesBeginCloseError) {
    auto file_writer = std::make_unique<RecordingFileWriter>();
    auto* recording = file_writer.get();
    recording->begin_status = Status::IOError("begin close failed");
    auto index_writer = make_index_writer(std::move(file_writer), GetParam());
    auto st = index_writer->begin_close();
    EXPECT_EQ(st.to_string(), recording->begin_status.to_string());
    EXPECT_EQ(recording->close_calls, (std::vector<bool> {true}));
}

TEST_P(EmptyIndexFileTest, PropagatesFinishCloseError) {
    auto file_writer = std::make_unique<RecordingFileWriter>();
    auto* recording = file_writer.get();
    recording->finish_status = Status::IOError("finish close failed");
    auto index_writer = make_index_writer(std::move(file_writer), GetParam());
    ASSERT_TRUE(index_writer->begin_close().ok());
    auto st = index_writer->finish_close();
    EXPECT_EQ(st.to_string(), recording->finish_status.to_string());
    EXPECT_EQ(recording->close_calls, (std::vector<bool> {true, false}));
}

TEST_P(EmptyIndexFileTest, AllowsNullWriter) {
    // V1 keeps one file per logical index and never owns a container; V2/V3 own
    // one but may be constructed without it (the drop path for a V1 rowset).
    for (auto format : {InvertedIndexStorageFormatPB::V1, GetParam()}) {
        auto index_writer = make_index_writer(nullptr, format);
        ASSERT_TRUE(index_writer->begin_close().ok());
        ASSERT_TRUE(index_writer->finish_close().ok());
    }
}

TEST_P(EmptyIndexFileTest, PreservesLocalEmptyFileAfterDestruction) {
    auto fs = io::global_local_filesystem();
    const auto path =
            segment_v2::InvertedIndexDescriptor::get_index_file_path_v2(index_path_prefix());
    io::FileWriterPtr file_writer;
    ASSERT_TRUE(fs->create_file(path, &file_writer).ok());
    auto* local_writer = file_writer.get();
    bool exists = false;
    ASSERT_TRUE(fs->exists(path, &exists).ok());
    ASSERT_TRUE(exists); // O_CREAT alone does not guarantee persistence.
    auto index_writer = make_index_writer(std::move(file_writer), GetParam());
    ASSERT_TRUE(index_writer->begin_close().ok());
    EXPECT_EQ(local_writer->state(), io::FileWriter::State::ASYNC_CLOSING);
    ASSERT_TRUE(index_writer->finish_close().ok());
    EXPECT_EQ(local_writer->state(), io::FileWriter::State::CLOSED);
    EXPECT_EQ(local_writer->bytes_appended(), 0);
    // ~LocalFileWriter aborts (and DELETES) a writer it was never asked to close.
    index_writer.reset();

    ASSERT_TRUE(fs->exists(path, &exists).ok());
    ASSERT_TRUE(exists);
    int64_t file_size = -1;
    ASSERT_TRUE(fs->file_size(path, &file_size).ok());
    EXPECT_EQ(file_size, 0);

    // The contract every reader of this file relies on: a zero-length container
    // is not corruption, it is "this segment has no index data". Query falls back
    // to a non-indexed evaluation on it and IndexBuilder treats it as "nothing to
    // carry over"; both branch on this exact error code.
    auto reader =
            std::make_unique<segment_v2::IndexFileReader>(fs, index_path_prefix(), GetParam());
    auto st = reader->init();
    EXPECT_TRUE(st.is<ErrorCode::INVERTED_INDEX_BYPASS>()) << st;
    EXPECT_NE(st.to_string().find(" is empty"), std::string::npos) << st;
}

TEST_P(EmptyIndexFileTest, MissingLocalFileReadsAsFileNotFound) {
    // The other half of the same contract: a rowset written before any index
    // existed has no container at all, and that is distinct from an empty one.
    auto reader = std::make_unique<segment_v2::IndexFileReader>(
            io::global_local_filesystem(), std::string(tmp_dir) + "/absent_0", GetParam());
    auto st = reader->init();
    EXPECT_TRUE(st.is<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>()) << st;
}

INSTANTIATE_TEST_SUITE_P(LegacyCompoundFormats, EmptyIndexFileTest,
                         testing::Values(InvertedIndexStorageFormatPB::V2,
                                         InvertedIndexStorageFormatPB::V3));

} // namespace doris
