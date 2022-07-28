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

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <stdio.h>
#include <stdlib.h>

#include <string>

#include "io/buffered_reader.h"
#include "io/file_reader.h"
#include "io/local_file_reader.h"
#include "util/runtime_profile.h"
#include "vec/exec/format/parquet/parquet_thrift_util.h"
#include "vec/exec/format/parquet/vparquet_file_metadata.h"

namespace doris {
namespace vectorized {

class ParquetThriftReaderTest : public testing::Test {
public:
    ParquetThriftReaderTest() {}
};

TEST_F(ParquetThriftReaderTest, normal) {
    LocalFileReader reader("./be/test/exec/test_data/parquet_scanner/localfile.parquet", 0);

    auto st = reader.open();
    EXPECT_TRUE(st.ok());

    std::shared_ptr<FileMetaData> metaData;
    parse_thrift_footer(&reader, metaData);
    tparquet::FileMetaData t_metadata = metaData->to_thrift_metadata();
    LOG(WARNING) << "num row groups: " << metaData->num_row_groups();
    LOG(WARNING) << "num columns: " << metaData->num_columns();
    LOG(WARNING) << "=====================================";
    for (auto value : t_metadata.row_groups) {
        LOG(WARNING) << "row group num_rows: " << value.num_rows;
    }
    LOG(WARNING) << "=====================================";
    for (auto value : t_metadata.schema) {
        LOG(WARNING) << "schema column name: " << value.name;
        LOG(WARNING) << "schema column type: " << value.type;
        LOG(WARNING) << "schema column repetition_type: " << value.repetition_type;
        LOG(WARNING) << "schema column num children: " << value.num_children;
    }
}

} // namespace vectorized

} // namespace doris
