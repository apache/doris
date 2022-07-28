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

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include "io/buffered_reader.h"
#include "io/file_reader.h"
#include <gtest/gtest.h>
#include "util/runtime_profile.h"
#include "io/local_file_reader.h"
#include "vec/exec/format/parquet/parquet_thrift_util.h"
#include "vec/exec/format/parquet/parquet_file_metadata.h"
#include <glog/logging.h>

namespace doris {
namespace vectorized {

class ParquetThriftReaderTest : public testing::Test {
public:
    ParquetThriftReaderTest() { init(); }
    void init();

private:
    std::string path;
    FileReader* reader;
};

void ParquetThriftReaderTest::init() {
    RuntimeProfile profile("test");
    path = "./be/test/exec/test_data/parquet_scanner/localfile.parquet";
    reader = new LocalFileReader(path, 0);
//    reader = new BufferedReader(&profile, file_reader, 1024);
//    reader->open();
}

TEST_F(ParquetThriftReaderTest, normal) {
    std::shared_ptr<FileMetaData> metaData;
    reader->open();
    parse_thrift_footer(reader, metaData);
    tparquet::FileMetaData t_metadata =  metaData->to_thrift_metadata();
    LOG(WARNING) << "num_rows: " << t_metadata.num_rows;
    LOG(WARNING) << "row_groups.size(): " << t_metadata.row_groups.size();
    LOG(WARNING) << "=====================================";
    for (auto value: t_metadata.row_groups) {
    LOG(WARNING) << "row_groups num_rows: " << value.num_rows;
}
    LOG(WARNING) << "=====================================";
    for (auto value: t_metadata.row_groups[0].columns) {
    LOG(WARNING) << "path_in_schema: " << value.meta_data.path_in_schema.data()[0];
}
}

} // namespace vectorized

} // namespace doris
