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

#include "exec/sink/writer/vfile_result_writer.h"

#include <gtest/gtest.h>

#include <filesystem>

#include "exec/operator/result_sink_operator.h"
#include "format/transformer/vorc_transformer.h"
#include "format/transformer/vparquet_transformer.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/local_file_writer.h"
#include "runtime/runtime_state.h"
#include "util/slice.h"

namespace doris {

namespace {

void create_closed_file(const std::filesystem::path& path) {
    io::FileWriterPtr file_writer;
    ASSERT_TRUE(io::global_local_filesystem()->create_file(path, &file_writer).ok());
    ASSERT_TRUE(file_writer->append(Slice("partial", 7)).ok());
    ASSERT_TRUE(file_writer->close().ok());
}

bool file_exists(const std::filesystem::path& path) {
    bool exists = false;
    EXPECT_TRUE(io::global_local_filesystem()->exists(path, &exists).ok());
    return exists;
}

class TrackingFileWriter final : public io::FileWriter {
public:
    Status close(bool) override {
        ++close_count;
        _state = State::CLOSED;
        return Status::OK();
    }

    Status appendv(const Slice*, size_t) override { return Status::OK(); }
    const io::Path& path() const override { return _path; }
    size_t bytes_appended() const override { return 0; }
    State state() const override { return _state; }

    int close_count = 0;

private:
    io::Path _path = "tracking-output";
    State _state = State::OPENED;
};

} // namespace

TEST(VFileResultWriterTest, FailedCloseRemovesClosedOutputFile) {
    const auto directory =
            std::filesystem::temp_directory_path() / "doris_vfile_result_writer_failed_close";
    const auto path = directory / "part.csv";
    std::filesystem::remove_all(directory);
    std::filesystem::create_directories(directory);

    io::FileWriterPtr file_writer;
    ASSERT_TRUE(io::global_local_filesystem()->create_file(path, &file_writer).ok());
    ASSERT_TRUE(file_writer->append(Slice("partial", 7)).ok());
    ASSERT_TRUE(file_writer->close().ok());

    VFileResultWriter writer(TDataSink {}, {}, nullptr, nullptr);
    writer._storage_type = TStorageBackendType::LOCAL;
    writer._file_writer_impl = std::move(file_writer);
    const auto original_error = Status::IOError("injected outfile failure");

    const auto status = writer.close(original_error);

    EXPECT_EQ(status.to_string(), original_error.to_string());
    EXPECT_FALSE(file_exists(path));
    std::filesystem::remove_all(directory);
}

TEST(VFileResultWriterTest, FailedCloseRemovesOnlyOwnedOutputFiles) {
    const auto directory =
            std::filesystem::temp_directory_path() / "doris_vfile_result_writer_owned_files";
    const auto first_path = directory / "part-0.csv";
    const auto second_path = directory / "part-1.csv";
    const auto unrelated_path = directory / "other-query.csv";
    std::filesystem::remove_all(directory);
    std::filesystem::create_directories(directory);
    create_closed_file(first_path);
    create_closed_file(second_path);
    create_closed_file(unrelated_path);

    VFileResultWriter writer(TDataSink {}, {}, nullptr, nullptr);
    writer._storage_type = TStorageBackendType::LOCAL;
    writer._file_system = io::global_local_filesystem();
    writer._created_files = {{writer._file_system, first_path}, {writer._file_system, second_path}};

    ASSERT_FALSE(writer.close(Status::IOError("injected outfile failure")).ok());

    EXPECT_FALSE(file_exists(first_path));
    EXPECT_FALSE(file_exists(second_path));
    EXPECT_TRUE(file_exists(unrelated_path));
    std::filesystem::remove_all(directory);
}

TEST(VFileResultWriterTest, AbortedFormatStreamsDoNotCloseRawWriter) {
    TrackingFileWriter parquet_writer;
    {
        ParquetOutputStream output_stream(&parquet_writer);
        ASSERT_TRUE(output_stream.Abort().ok());
    }
    EXPECT_EQ(parquet_writer.close_count, 0);

    TrackingFileWriter orc_writer;
    {
        VOrcOutputStream output_stream(&orc_writer);
        output_stream.abort();
    }
    EXPECT_EQ(orc_writer.close_count, 0);
}

TEST(VFileResultWriterTest, LocalOutfilePreservesSynchronousClose) {
    const auto directory =
            std::filesystem::temp_directory_path() / "doris_vfile_result_writer_sync_close";
    const auto path = directory / "part.csv";
    std::filesystem::remove_all(directory);
    std::filesystem::create_directories(directory);

    TResultFileSinkOptions thrift_options;
    thrift_options.file_path = directory.string() + "/";
    thrift_options.file_format = TFileFormatType::FORMAT_CSV_PLAIN;
    thrift_options.file_suffix = "csv";
    thrift_options.with_bom = false;
    ResultFileOptions file_options(thrift_options);
    RuntimeState state;
    VExprContextSPtrs output_exprs;
    VFileResultWriter writer(TDataSink {}, output_exprs, nullptr, nullptr);
    writer._state = &state;
    writer._file_opts = &file_options;
    writer._storage_type = TStorageBackendType::LOCAL;

    ASSERT_TRUE(writer._create_file_writer(path.string()).ok());
    const auto* local_writer =
            dynamic_cast<const io::LocalFileWriter*>(writer._file_writer_impl.get());
    ASSERT_NE(local_writer, nullptr);
    EXPECT_TRUE(local_writer->_sync_data);

    ASSERT_FALSE(writer.close(Status::Cancelled("test cleanup")).ok());
    std::filesystem::remove_all(directory);
}

} // namespace doris
