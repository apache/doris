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

#include <vector>

#include "olap/memory/wtd_memtable.h"
#include "olap/memory/wtd_reader.h"

namespace doris {
namespace memory {

class WTDTest : public testing::Test {
public:
    WTDTest() {}

protected:
    virtual void SetUp() { system("mkdir -p ./test_run"); }
    virtual void TearDown() { system("rm -rf ./test_run"); }
};

TEST_F(WTDTest, write_file) {
    scoped_refptr<Schema> tsc;
    ASSERT_TRUE(Schema::create("id int,uv int,pv int,city tinyint null", &tsc).ok());

    // Generate one partial row. Use PartialRowWriter for the readability of data.
    PartialRowWriter writer(tsc);
    EXPECT_TRUE(writer.start_batch().ok());
    writer.start_row();
    int id = 233;
    EXPECT_TRUE(writer.set("id", &id).ok());
    EXPECT_TRUE(writer.end_row().ok());
    vector<uint8_t> buffer;
    writer.finish_batch(&buffer);

    std::string wtd_path = "./test_run/0_1.wtd";
    doris::Schema schema(tsc->get_tablet_schema());
    std::unique_ptr<fs::WritableBlock> wblock;
    fs::CreateBlockOptions opts({wtd_path});
    ASSERT_TRUE(fs::fs_util::block_mgr_for_ut()->create_block(opts, &wblock).ok());
    WTDMemTable mt(&schema, wblock.get());
    // the real row data
    mt.insert(buffer.data() + sizeof(uint64_t));
    auto st = mt.flush();
    ASSERT_EQ(OLAPStatus::OLAP_SUCCESS, st);
    ASSERT_TRUE(wblock->close().ok());

    // read to check data
    std::unique_ptr<fs::ReadableBlock> rblock;
    ASSERT_TRUE(fs::fs_util::block_mgr_for_ut()->open_block(wtd_path, &rblock).ok());
    WTDReader reader(rblock.get());
    ASSERT_TRUE(reader.open().ok());
    auto batch = reader.build_batch(&tsc);

    ASSERT_EQ(1, batch->row_size());
    bool has_row = false;
    ASSERT_TRUE(batch->next_row(&has_row).ok());
    ASSERT_TRUE(has_row);

    const ColumnSchema* cs = nullptr;
    const void* data = nullptr;

    ASSERT_TRUE(batch->cur_row_get_cell(0, &cs, &data).ok());
    ASSERT_EQ(cs->cid(), 1);
    ASSERT_EQ(id, *(int32_t*)data);
}

} // namespace memory
} // namespace doris

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
