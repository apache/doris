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

#include "olap/delete_bitmap_index.h"

#include <gtest/gtest.h>

#include "olap/tablet_schema_helper.h"
#include "olap/row_cursor.h"
#include "util/debug_util.h"

namespace doris {

class DeleteBitmapIndexTest : public testing::Test {
public:
    DeleteBitmapIndexTest() { }
    virtual ~DeleteBitmapIndexTest() {
    }
};

TEST_F(DeleteBitmapIndexTest, buider) {
    DeleteBitmapIndexBuilder builder;

    int num_items = 0;
    for (int i = 1000; i < 10000; i += 2) {
        builder.add_delete_item(i);
        num_items++;
    }
    std::vector<Slice> slices;
    segment_v2::PageFooterPB footer;
    auto st = builder.finalize(&slices, &footer);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(segment_v2::DELETE_INDEX_PAGE, footer.type());
    ASSERT_EQ(num_items, footer.delete_index_page_footer().num_items());
    
    std::string buf;
    for (auto& slice : slices) {
        buf.append(slice.data, slice.size);
    }

    DeleteBitmapIndexDecoder decoder;
    st = decoder.parse(buf, footer.delete_index_page_footer());
    ASSERT_TRUE(st.ok());

    auto& bitmap = decoder.get_iterator().delete_bitmap();
    {
        ASSERT_TRUE(bitmap.contains(1002));
    }
    {
        ASSERT_TRUE(!bitmap.contains(1003));
    }
    {
        ASSERT_TRUE(!bitmap.contains(5003));
    }
    {
        ASSERT_TRUE(bitmap.contains(5002));
    }
}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

