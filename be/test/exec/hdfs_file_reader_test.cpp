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

#include "io/hdfs_file_reader.h"

#include <gtest/gtest.h>

#include "io/hdfs_reader_writer.h"

namespace doris {

class HdfsFileReaderTest : public testing::Test {};

TEST_F(HdfsFileReaderTest, test_connect_fail) {
    THdfsParams hdfsParams;
    hdfsParams.__set_fs_name("hdfs://127.0.0.9:8888"); // An invalid address
    hdfsParams.__set_hdfs_kerberos_principal("somebody@TEST.COM");
    hdfsParams.__set_hdfs_kerberos_keytab("/etc/keytab/doris.keytab");
    std::vector<THdfsConf> confs;
    THdfsConf item;
    item.key = "dfs.ha.namenodes.service1";
    item.value = "n1,n2";
    confs.push_back(item);
    hdfsParams.__set_hdfs_conf(confs);
    HdfsFileReader hdfs_file_reader(hdfsParams, "/user/foo/test.data", 0);
    Status status = hdfs_file_reader.open();
    EXPECT_EQ(TStatusCode::INTERNAL_ERROR, status.code());
    hdfs_file_reader.close();
}

} // end namespace doris
