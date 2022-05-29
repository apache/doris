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

#include "olap/tablet_meta.h"

#include <gtest/gtest.h>

#include <string>

namespace doris {

TEST(TabletMetaTest, SaveAndParse) {
    std::string meta_path = "./be/test/olap/test_data/tablet_meta_test.hdr";

    TabletMeta old_tablet_meta(1, 2, 3, 4, 5, TTabletSchema(), 6, {{7, 8}}, UniqueId(9, 10),
                               TTabletType::TABLET_TYPE_DISK, TStorageMedium::HDD,
                               TCompressionType::LZ4F);
    EXPECT_EQ(Status::OK(), old_tablet_meta.save(meta_path));

    {
        // Just to make stack space dirty
        TabletMeta new_tablet_meta;
        new_tablet_meta._preferred_rowset_type = BETA_ROWSET;
    }
    TabletMeta new_tablet_meta;
    new_tablet_meta.create_from_file(meta_path);

    ASSERT_EQ(old_tablet_meta, new_tablet_meta);
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
