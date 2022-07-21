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

    TabletMeta old_tablet_meta(1, 2, 3, 3, 4, 5, TTabletSchema(), 6, {{7, 8}}, UniqueId(9, 10),
                               TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F);
    EXPECT_EQ(Status::OK(), old_tablet_meta.save(meta_path));

    {
        // Just to make stack space dirty
        TabletMeta new_tablet_meta;
        new_tablet_meta._preferred_rowset_type = BETA_ROWSET;
    }
    TabletMeta new_tablet_meta;
    new_tablet_meta.create_from_file(meta_path);

    EXPECT_EQ(old_tablet_meta, new_tablet_meta);
}

TEST(TabletMetaTest, TestDeleteBitmap) {
    std::unique_ptr<DeleteBitmap> dbmp(new DeleteBitmap(10086));
    auto gen1 = [&dbmp](int64_t max_rst_id, uint32_t max_seg_id, uint32_t max_row) {
        for (int64_t i = 0; i < max_rst_id; ++i) {
            for (uint32_t j = 0; j < max_seg_id; ++j) {
                for (uint32_t k = 0; k < max_row; ++k) {
                    dbmp->add({RowsetId {2, 0, 1, i}, j, 0}, k);
                }
            }
        }
    };
    gen1(10, 20, 1000);
    dbmp->add({RowsetId {2, 0, 1, 2}, 2, 0}, 2); // redundant
    {
        roaring::Roaring d;
        dbmp->get({RowsetId {2, 0, 1, 2}, 0, 0}, &d);
        EXPECT_EQ(d.cardinality(), 1000);
        d -= *dbmp->get({RowsetId {2, 0, 1, 2}, 0, 0});
        EXPECT_EQ(d.cardinality(), 0);
    }

    // Add version 1 and 2
    dbmp->add({RowsetId {2, 0, 1, 1}, 1, 1}, 1100);
    dbmp->add({RowsetId {2, 0, 1, 1}, 1, 1}, 1101);
    dbmp->add({RowsetId {2, 0, 1, 1}, 1, 1}, 1102);
    dbmp->add({RowsetId {2, 0, 1, 1}, 1, 1}, 1103);
    dbmp->add({RowsetId {2, 0, 1, 1}, 1, 2}, 1104);

    ASSERT_EQ(dbmp->delete_bitmap.size(), 10 * 20 + 2);

    { // Bitmap of certain verisons only get their own row ids
        auto bm = dbmp->get({RowsetId {2, 0, 1, 1}, 1, 2});
        ASSERT_EQ(bm->cardinality(), 1);
        ASSERT_FALSE(bm->contains(999));
        ASSERT_FALSE(bm->contains(1100));
        ASSERT_TRUE(bm->contains(1104));
    }

    {
        // test remove
        // Nothing removed
        dbmp->remove({RowsetId {2, 0, 1, 1}, 0, 0}, {RowsetId {2, 0, 1, 1}, 0, 0});
        ASSERT_EQ(dbmp->delete_bitmap.size(), 10 * 20 + 2);
        dbmp->remove({RowsetId {2, 0, 1, 100}, 0, 0}, {RowsetId {2, 0, 1, 100}, 50000, 0});
        ASSERT_EQ(dbmp->delete_bitmap.size(), 10 * 20 + 2);

        // Remove all seg of rowset {2,0,1,0}
        dbmp->remove({RowsetId {2, 0, 1, 0}, 0, 0}, {RowsetId {2, 0, 1, 0}, 5000, 0});
        ASSERT_EQ(dbmp->delete_bitmap.size(), 9 * 20 + 2);
        // Remove all rowset {2,0,1,7} to {2,0,1,9}
        dbmp->remove({RowsetId {2, 0, 1, 8}, 0, 0}, {RowsetId {2, 0, 1, 9}, 5000, 0});
        ASSERT_EQ(dbmp->delete_bitmap.size(), 7 * 20 + 2);
    }

    {
        DeleteBitmap db_upper(10086);
        dbmp->subset({RowsetId {2, 0, 1, 1}, 1, 0}, {RowsetId {2, 0, 1, 1}, 1000000, 0}, &db_upper);
        roaring::Roaring d;
        ASSERT_EQ(db_upper.get({RowsetId {2, 0, 1, 1}, 1, 1}, &d), 0);
        ASSERT_EQ(d.cardinality(), 4);
        ASSERT_EQ(db_upper.get({RowsetId {2, 0, 1, 1}, 1, 2}, &d), 0);
        ASSERT_EQ(d.cardinality(), 1);
        ASSERT_EQ(db_upper.delete_bitmap.size(), 20);
    }

    {
        auto old_size = dbmp->delete_bitmap.size();
        // test merge
        DeleteBitmap other(10086);
        other.add({RowsetId {2, 0, 1, 1}, 1, 1}, 1100);
        dbmp->merge(other);
        ASSERT_EQ(dbmp->delete_bitmap.size(), old_size);
        other.add({RowsetId {2, 0, 1, 1}, 1001, 1}, 1100);
        other.add({RowsetId {2, 0, 1, 1}, 1002, 1}, 1100);
        dbmp->merge(other);
        ASSERT_EQ(dbmp->delete_bitmap.size(), old_size + 2);
    }

    ////////////////////////////////////////////////////////////////////////////
    // Cache test
    ////////////////////////////////////////////////////////////////////////////
    // Aggregation bitmap contains all row ids that are in versions smaller or
    {
        // equal to the given version, boundary test
        auto bm = dbmp->get_agg({RowsetId {2, 0, 1, 1}, 1, 2});
        ASSERT_EQ(bm->cardinality(), 1005);
        ASSERT_TRUE(bm->contains(999));
        ASSERT_TRUE(bm->contains(1100));
        ASSERT_TRUE(bm->contains(1101));
        ASSERT_TRUE(bm->contains(1102));
        ASSERT_TRUE(bm->contains(1103));
        ASSERT_TRUE(bm->contains(1104));
        bm = dbmp->get_agg({RowsetId {2, 0, 1, 1}, 1, 2});
        ASSERT_EQ(bm->cardinality(), 1005);
    }

    // Aggregation bitmap contains all row ids that are in versions smaller or
    // equal to the given version, normal test
    {
        auto bm = dbmp->get_agg({RowsetId {2, 0, 1, 1}, 1, 1000});
        ASSERT_EQ(bm->cardinality(), 1005);
        ASSERT_TRUE(bm->contains(999));
        ASSERT_TRUE(bm->contains(1100));
        ASSERT_TRUE(bm->contains(1101));
        ASSERT_TRUE(bm->contains(1102));
        ASSERT_TRUE(bm->contains(1103));
        ASSERT_TRUE(bm->contains(1104));
        bm = dbmp->get_agg({RowsetId {2, 0, 1, 1}, 1, 1000});
        ASSERT_EQ(bm->cardinality(), 1005);
    }

    // Check data is not messed-up
    ASSERT_TRUE(dbmp->contains({RowsetId {2, 0, 1, 1}, 1, 2}, 1104));
    ASSERT_FALSE(dbmp->contains({RowsetId {2, 0, 1, 1}, 1, 2}, 1103));
    ASSERT_TRUE(dbmp->contains_agg({RowsetId {2, 0, 1, 1}, 1, 2}, 1104));
    ASSERT_TRUE(dbmp->contains_agg({RowsetId {2, 0, 1, 1}, 1, 2}, 1103));

    // Test c-tor of agg cache with global LRU
    int cached_cardinality = dbmp->get_agg({RowsetId {2, 0, 1, 1}, 1, 2})->cardinality();
    {
        // New delete bitmap with old agg cache
        std::unique_ptr<DeleteBitmap> dbmp(new DeleteBitmap(10086));
        auto bm = dbmp->get_agg({RowsetId {2, 0, 1, 1}, 1, 2});
        ASSERT_TRUE(bm->contains(1104));
        ASSERT_EQ(bm->cardinality(), cached_cardinality);
    }
}

} // namespace doris
