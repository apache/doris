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

#include "exec/sink/writer/iceberg/viceberg_delete_file_writer.h"

#include <gtest/gtest.h>

#include <string>

#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris {

namespace {

// An ADLS location as the catalog reports it, and the internal form the FE normalizes it to before
// handing it to the backend. Only the first one is resolvable by a reader.
constexpr const char* kOriginalPath =
        "abfss://container@account.dfs.core.windows.net/tbl/data/delete_pos_1.parquet";
constexpr const char* kNormalizedPath = "s3://container/tbl/data/delete_pos_1.parquet";

} // namespace

// The commit data feeds the iceberg manifest, so it must carry the catalog's own URI. Reporting the
// normalized path instead leaves the table permanently unreadable once the snapshot is committed.
TEST(VIcebergDeleteFileWriterTest, CommitDataCarriesOriginalPathNotNormalizedPath) {
    VIcebergDeleteFileWriter writer(TFileContent::POSITION_DELETES, kNormalizedPath, kOriginalPath,
                                    TFileFormatType::FORMAT_PARQUET, TFileCompressType::ZSTD);

    TIcebergCommitData commit_data;
    const Status close_status = writer.close(commit_data);
    ASSERT_TRUE(close_status.ok()) << close_status;

    ASSERT_TRUE(commit_data.__isset.file_path);
    ASSERT_EQ(kOriginalPath, commit_data.file_path);
    ASSERT_EQ(TFileContent::POSITION_DELETES, commit_data.file_content);
}

// A location that was never rewritten passes both forms through unchanged.
TEST(VIcebergDeleteFileWriterTest, CommitDataKeepsPathWhenLocationWasNotRewritten) {
    const std::string path = "s3://bucket/tbl/data/delete_pos_1.parquet";
    VIcebergDeleteFileWriter writer(TFileContent::POSITION_DELETES, path, path,
                                    TFileFormatType::FORMAT_PARQUET, TFileCompressType::ZSTD);

    TIcebergCommitData commit_data;
    const Status close_status = writer.close(commit_data);
    ASSERT_TRUE(close_status.ok()) << close_status;

    ASSERT_EQ(path, commit_data.file_path);
}

TEST(VIcebergDeleteFileWriterTest, FactoryForwardsBothPaths) {
    auto writer = VIcebergDeleteFileWriterFactory::create_writer(
            TFileContent::POSITION_DELETES, kNormalizedPath, kOriginalPath,
            TFileFormatType::FORMAT_PARQUET, TFileCompressType::ZSTD);
    ASSERT_NE(nullptr, writer);

    // The physical path stays normalized so the file is created through the backend's own
    // filesystem handling; only the reported path changes.
    ASSERT_EQ(kNormalizedPath, writer->_output_path);
    ASSERT_EQ(kOriginalPath, writer->_original_output_path);
}

} // namespace doris
