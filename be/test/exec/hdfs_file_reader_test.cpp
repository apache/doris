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

#include "exec/hdfs_file_reader.h"

#include <gtest/gtest.h>

#include "exec/hdfs_reader_writer.h"

namespace doris {

class HdfsFileReaderTest : public testing::Test {};

TEST_F(HdfsFileReaderTest, test_connect_fail) {
    THdfsParams hdfsParams;
    hdfsParams.fs_name = "hdfs://127.0.0.1:8888"; // An invalid address
    HdfsFileReader hdfs_file_reader(hdfsParams, "/user/foo/test.data", 0);
    Status status = hdfs_file_reader.open();
    hdfs_file_reader.close();
    std::string msg = status.get_error_msg();
    EXPECT_TRUE(msg.find("Connection refused") >= 0);
    hdfs_file_reader.close();
}

} // end namespace doris
