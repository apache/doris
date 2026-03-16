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

#include "format/table/iceberg_delete_file_reader_helper.h"

#include <gtest/gtest.h>

namespace doris {

TEST(IcebergDeleteFileReaderHelperTest, BuildDeleteFileRange) {
    auto range = build_iceberg_delete_file_range("s3://bucket/delete.parquet");
    EXPECT_EQ(range.path, "s3://bucket/delete.parquet");
    EXPECT_EQ(range.start_offset, 0);
    EXPECT_EQ(range.size, -1);
    EXPECT_EQ(range.file_size, -1);
}

TEST(IcebergDeleteFileReaderHelperTest, IsDeletionVector) {
    TIcebergDeleteFileDesc delete_file;
    delete_file.__set_content(3);
    delete_file.__isset.content = true;
    EXPECT_TRUE(is_iceberg_deletion_vector(delete_file));
}

TEST(IcebergDeleteFileReaderHelperTest, IsNotDeletionVectorWhenContentMissing) {
    TIcebergDeleteFileDesc delete_file;
    EXPECT_FALSE(is_iceberg_deletion_vector(delete_file));
}

} // namespace doris
