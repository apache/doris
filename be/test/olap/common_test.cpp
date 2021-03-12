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

#include "olap/olap_common.h"

namespace doris {

TEST(DataDirInfo, DataDirInfoLessAvailability) {
    DataDirInfo a;
    a.available = 100;
    DataDirInfo b;
    b.available = 200;
    DataDirInfo c;
    c.available = 300;
    DataDirInfo d;
    d.available = 400;
    std::vector<DataDirInfo> data_dir_infos;
    data_dir_infos.push_back(c);
    data_dir_infos.push_back(d);
    data_dir_infos.push_back(a);
    data_dir_infos.push_back(b);
    std::sort(data_dir_infos.begin(), data_dir_infos.end(), DataDirInfoLessAvailability());
    ASSERT_EQ(data_dir_infos[0].available, 100);
    ASSERT_EQ(data_dir_infos[1].available, 200);
    ASSERT_EQ(data_dir_infos[2].available, 300);
    ASSERT_EQ(data_dir_infos[3].available, 400);
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
