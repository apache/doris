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
#include <gtest/gtest.h>

#include <filesystem>
#include <memory>

#include "cloud/delete_bitmap_file_reader.h"
#include "cloud/delete_bitmap_file_writer.h"
#include "gmock/gmock.h"
#include "io/fs/local_file_system.h"
#include "testutil/test_util.h"
#include "util/proto_util.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace doris {

class DeleteBitmapFileReaderWriterTest : public testing::Test {};

TEST_F(DeleteBitmapFileReaderWriterTest, TestWriteAndRead) {
    int64_t tablet_id = 43231;
    std::string rowset_id = "432w1abc2";
    std::optional<StorageResource> storage_resource_op;
    DeleteBitmapPB delete_bitmap_pb;
    for (int i = 0; i < 10; ++i) {
        delete_bitmap_pb.add_rowset_ids("rowset_id_" + std::to_string(i));
        delete_bitmap_pb.add_segment_ids(i);
        delete_bitmap_pb.add_versions(i);
        delete_bitmap_pb.add_segment_delete_bitmaps("bitmap.val_" + std::to_string(i));
    }

    DeleteBitmapFileWriter file_writer(tablet_id, rowset_id, storage_resource_op);
    EXPECT_TRUE(file_writer.init().ok());
    EXPECT_TRUE(file_writer.write(delete_bitmap_pb).ok());
    EXPECT_TRUE(file_writer.close().ok());

    DeleteBitmapFileReader reader(tablet_id, rowset_id, storage_resource_op);
    EXPECT_TRUE(reader.init().ok());
    DeleteBitmapPB dbm;
    EXPECT_TRUE(reader.read(dbm).ok());
    EXPECT_TRUE(reader.close().ok());
    EXPECT_EQ(delete_bitmap_pb.rowset_ids_size(), dbm.rowset_ids_size());
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(delete_bitmap_pb.rowset_ids(i), dbm.rowset_ids(i));
        EXPECT_EQ(delete_bitmap_pb.segment_ids(i), dbm.segment_ids(i));
        EXPECT_EQ(delete_bitmap_pb.versions(i), dbm.versions(i));
        EXPECT_EQ(delete_bitmap_pb.segment_delete_bitmaps(i), dbm.segment_delete_bitmaps(i));
    }
}
} // namespace doris