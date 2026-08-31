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

#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
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
    writer._created_file_paths = {first_path, second_path};

    ASSERT_FALSE(writer.close(Status::IOError("injected outfile failure")).ok());

    EXPECT_FALSE(file_exists(first_path));
    EXPECT_FALSE(file_exists(second_path));
    EXPECT_TRUE(file_exists(unrelated_path));
    std::filesystem::remove_all(directory);
}

} // namespace doris
