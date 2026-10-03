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

#include "storage/mow/mow_transform_test_base.h"
#include "storage/segment/segment_loader.h"
#include "util/jsonb/serialize.h"

namespace doris {

class PointQuerySegmentTest : public MowTransformTestBase,
                              public testing::WithParamInterface<bool> {
protected:
    // Exercise the cloud segment-list layout against real local files, as well as legacy IDs.
    void set_segment_ids(const RowsetSharedPtr& rowset) {
        if (!GetParam()) {
            return;
        }
        std::vector<int64_t> ids;
        for (auto seg : rowset->segments()) {
            const int64_t id = 10 + 3 * seg.pos();
            auto old_path = rowset->segment_path(seg.id());
            auto new_path = rowset->segment_path(id);
            ASSERT_TRUE(old_path.has_value()) << old_path.error();
            ASSERT_TRUE(new_path.has_value()) << new_path.error();
            ASSERT_TRUE(io::global_local_filesystem()->rename(*old_path, *new_path).ok());
            ids.push_back(id);
        }
        rowset->rowset_meta()->set_segment_ids(ids);
    }

    size_t cached_segments(const RowsetSegmentCache& cache) {
        return std::count_if(cache._segments.begin(), cache._segments.end(),
                             [](const auto& segment) { return segment != nullptr; });
    }
};

TEST_P(PointQuerySegmentTest, CandidatesAreLoadedLazilyAndReused) {
    auto schema = create_mow_schema(false);
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 901000, 2,
                               {{1, 10}, {3, 30}, {11, 110}, {13, 130}, {21, 210}, {23, 230}},
                               &tablet, 2);
    ASSERT_EQ(rowset->num_segments(), 3);
    SegmentLoader::instance()->erase_segments(*rowset->rowset_meta());
    set_segment_ids(rowset);
    auto path = rowset->segment_path(rowset->segment(0).id());
    ASSERT_TRUE(path.has_value()) << path.error();
    const std::string hidden_path = *path + ".hidden";
    ASSERT_TRUE(io::global_local_filesystem()->rename(*path, hidden_path).ok());

    std::vector<RowsetSharedPtr> rowsets {rowset};
    std::vector<std::unique_ptr<RowsetSegmentCache>> caches(1);
    OlapReaderStatistics stats;
    RowLocation location;
    RowKeyEncoder encoder {*schema, true};
    auto lookup = [&](int32_t key) {
        const auto encoded = encode_key(schema, encoder, key);
        return tablet->lookup_row_key(encoded, schema.get(), false, rowsets, &location, 2, caches,
                                      nullptr, false, nullptr, &stats);
    };

    // An out-of-range miss does not initialize any rowset cache.
    EXPECT_TRUE(lookup(99).is<ErrorCode::KEY_NOT_FOUND>());
    EXPECT_EQ(caches[0], nullptr);
    // Segment 0 is absent: a lookup in segment 1 must not open it.
    ASSERT_TRUE(lookup(11).ok());
    EXPECT_EQ(location.segment_id, rowset->segment(1).id());
    EXPECT_EQ(location.row_id, 0);
    EXPECT_EQ(cached_segments(*caches[0]), 1);
    auto segment = caches[0]->_segments[1];
    ASSERT_TRUE(lookup(13).ok());
    EXPECT_EQ(location.row_id, 1);
    EXPECT_EQ(caches[0]->_segments[1], segment);
    EXPECT_TRUE(lookup(12).is<ErrorCode::KEY_NOT_FOUND>());
    EXPECT_EQ(cached_segments(*caches[0]), 1);
    ASSERT_TRUE(lookup(23).ok());
    EXPECT_EQ(location.segment_id, rowset->segment(2).id());
    EXPECT_EQ(cached_segments(*caches[0]), 2);

    // An actual candidate's open error must propagate and leave its slot retryable.
    auto status = lookup(1);
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(status.is<ErrorCode::KEY_NOT_FOUND>());
    EXPECT_EQ(caches[0]->_segments[0], nullptr);
    ASSERT_TRUE(io::global_local_filesystem()->rename(hidden_path, *path).ok());
    ASSERT_TRUE(lookup(1).ok());
    EXPECT_EQ(location.segment_id, rowset->segment(0).id());
    EXPECT_EQ(cached_segments(*caches[0]), 3);
}

TEST_P(PointQuerySegmentTest, ColdLookupOpensOneOfSevenSegments) {
    auto schema = create_mow_schema(false);
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 901001, 2,
                               {{0, 0}, {10, 1}, {20, 2}, {30, 3}, {40, 4}, {50, 5}, {60, 6}},
                               &tablet, 1);
    ASSERT_EQ(rowset->num_segments(), 7);
    SegmentLoader::instance()->erase_segments(*rowset->rowset_meta());
    set_segment_ids(rowset);
    {
        SegmentCacheHandle eager;
        ASSERT_TRUE(SegmentLoader::instance()
                            ->load_segments(std::static_pointer_cast<BetaRowset>(rowset), &eager,
                                            true, true)
                            .ok());
        ASSERT_EQ(eager.get_segments().size(), 7);
        for (const auto& segment : eager.get_segments()) {
            EXPECT_NE(segment->_pk_index_reader, nullptr);
        }
    }
    SegmentLoader::instance()->erase_segments(*rowset->rowset_meta());
    std::vector<RowsetSharedPtr> rowsets {rowset};
    std::vector<std::unique_ptr<RowsetSegmentCache>> caches(1);
    RowKeyEncoder encoder {*schema, true};
    RowLocation location;
    OlapReaderStatistics stats;
    const auto encoded = encode_key(schema, encoder, 30);
    ASSERT_TRUE(tablet->lookup_row_key(encoded, schema.get(), false, rowsets, &location, 2, caches,
                                       nullptr, false, nullptr, &stats)
                        .ok());
    EXPECT_EQ(location.segment_id, rowset->segment(3).id());
    EXPECT_EQ(cached_segments(*caches[0]), 1);
    EXPECT_NE(caches[0]->_segments[3]->_pk_index_reader, nullptr);
}

TEST_P(PointQuerySegmentTest, RowStoreReadOpensOnlyLocatedSegment) {
    auto schema = create_row_store_schema();
    TabletSharedPtr tablet;
    auto rowset =
            write_rowset(schema, 901002, 2, {{1, 10}, {2, 20}, {11, 110}, {12, 120}}, &tablet, 2);
    ASSERT_EQ(rowset->num_segments(), 2);
    SegmentLoader::instance()->erase_segments(*rowset->rowset_meta());
    set_segment_ids(rowset);
    auto path = rowset->segment_path(rowset->segment(0).id());
    ASSERT_TRUE(path.has_value()) << path.error();
    ASSERT_TRUE(io::global_local_filesystem()->delete_file(*path).ok());
    RowLocation location {rowset->rowset_id(), static_cast<uint32_t>(rowset->segment(1).id()), 1};
    RowKeyEncoder encoder {*schema, true};
    const auto encoded = encode_key(schema, encoder, 12);
    OlapReaderStatistics stats;
    std::string value;
    ASSERT_TRUE(tablet->lookup_row_data(encoded, location, rowset, stats, value,
                                        /*write_to_cache=*/false)
                        .ok());
    Block decoded = schema->create_storage_block({0, 1, 2});
    DataTypeSerDeSPtrs serdes;
    for (size_t i = 0; i < decoded.columns(); ++i) {
        serdes.push_back(decoded.get_by_position(i).type->get_serde());
    }
    ASSERT_TRUE(JsonbSerializeUtil::jsonb_to_block(serdes, value.data(), value.size(),
                                                   {{0, 0}, {1, 1}, {2, 2}}, decoded,
                                                   {"0", "0", "0"}, {})
                        .ok());
    ASSERT_EQ(decoded.rows(), 1);
    EXPECT_EQ(read_int(decoded, 0, 0), 12);
    EXPECT_EQ(read_int(decoded, 1, 0), 120);
}

TEST_P(PointQuerySegmentTest, ColumnStoreReadOpensOnlyLocatedSegment) {
    auto schema = create_mow_schema(false);
    TabletSharedPtr tablet;
    auto rowset =
            write_rowset(schema, 901003, 2, {{1, 10}, {2, 20}, {11, 110}, {12, 120}}, &tablet, 2);
    ASSERT_EQ(rowset->num_segments(), 2);
    SegmentLoader::instance()->erase_segments(*rowset->rowset_meta());
    set_segment_ids(rowset);
    auto path = rowset->segment_path(rowset->segment(0).id());
    ASSERT_TRUE(path.has_value()) << path.error();
    ASSERT_TRUE(io::global_local_filesystem()->delete_file(*path).ok());
    Block block = schema->create_storage_block({0, 1});
    {
        auto guard = block.mutate_columns_scoped();
        ASSERT_TRUE(BaseTablet::fetch_values_by_rowids(rowset, *schema, rowset->segment(1).id(),
                                                       {0, 1}, {0, 1}, guard.mutable_columns())
                            .ok());
    }
    ASSERT_EQ(block.rows(), 2);
    EXPECT_EQ(read_int(block, 0, 0), 11);
    EXPECT_EQ(read_int(block, 1, 0), 110);
    EXPECT_EQ(read_int(block, 0, 1), 12);
    EXPECT_EQ(read_int(block, 1, 1), 120);
}

TEST_P(PointQuerySegmentTest, OverlappingSegmentsKeepNewestAndRespectDeletes) {
    auto schema = create_mow_schema(false);
    TabletSharedPtr tablet;
    // Each segment is sorted; both segments contain the same keys.
    auto rowset =
            write_rowset(schema, 901004, 2, {{1, 10}, {2, 20}, {1, 110}, {2, 120}}, &tablet, 2);
    ASSERT_EQ(rowset->num_segments(), 2);
    SegmentLoader::instance()->erase_segments(*rowset->rowset_meta());
    set_segment_ids(rowset);
    std::vector<RowsetSharedPtr> rowsets {rowset};
    std::vector<std::unique_ptr<RowsetSegmentCache>> caches(1);
    RowKeyEncoder encoder {*schema, true};
    const auto encoded = encode_key(schema, encoder, 1);
    OlapReaderStatistics stats;
    RowLocation location;
    auto lookup = [&] {
        return tablet->lookup_row_key(encoded, schema.get(), false, rowsets, &location, 2, caches,
                                      nullptr, false, nullptr, &stats);
    };
    ASSERT_TRUE(lookup().ok());
    EXPECT_EQ(location.segment_id, rowset->segment(1).id());
    EXPECT_EQ(cached_segments(*caches[0]), 1);
    tablet->tablet_meta()->delete_bitmap().add({rowset->rowset_id(), location.segment_id, 2},
                                               location.row_id);
    // Do not resurrect the older copy in segment 0 when the latest copy was deleted.
    EXPECT_TRUE(lookup().is<ErrorCode::KEY_NOT_FOUND>());
    EXPECT_EQ(cached_segments(*caches[0]), 1);
}

INSTANTIATE_TEST_SUITE_P(SegmentLayouts, PointQuerySegmentTest, testing::Bool());

} // namespace doris
