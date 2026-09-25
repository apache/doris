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

#include <gen_cpp/internal_service.pb.h>
#include <gtest/gtest.h>

#include <array>
#include <memory>
#include <vector>

#include "common/config.h"
#include "runtime/exec_env.h"
#include "service/point_query_executor.h"
#include "storage/key/row_key_encoder.h"
#include "storage/mow/mow_transform_test_base.h"
#include "storage/tablet/tablet_meta.h"

namespace doris {

class PointQuerySnapshotTest : public MowTransformTestBase {
protected:
    void SetUp() override {
        MowTransformTestBase::SetUp();
        _saved_cache = ExecEnv::GetInstance()->get_row_cache();
        _cache = std::make_unique<RowCache>(1024 * 1024, 1);
        ExecEnv::GetInstance()->_row_cache = _cache.get();
        _saved_disable_row_cache = config::disable_storage_row_cache;
        config::disable_storage_row_cache = false;

        _schema = create_mow_schema(/*has_seq=*/false);
        _rowsets.push_back(write_rowset(_schema, 900100, 0, {}, &_tablet));
        _rowsets.push_back(write_rowset(_schema, 900101, 1, {}, &_tablet));
        _rowsets.push_back(write_rowset(_schema, 900102, 2,
                                        {{.k = 1, .v = 11}, {.k = 2, .v = 22}, {.k = 3, .v = 33}},
                                        &_tablet));
        _rowsets.push_back(write_rowset(_schema, 900103, 3,
                                        {{.k = 1, .v = 111}, {.k = 2, .v = 0, .delete_sign = 1}},
                                        &_tablet));
        for (const auto& rowset : _rowsets) {
            ASSERT_TRUE(_tablet->add_rowset(rowset).ok());
        }
        // Version 3 updates key 1 and deletes key 2. These bitmap entries must not hide
        // either old row from a version-2 reader.
        auto& delete_bitmap = _tablet->tablet_meta()->delete_bitmap();
        delete_bitmap.add({_rowsets[2]->rowset_id(), 0, 3}, 0);
        delete_bitmap.add({_rowsets[2]->rowset_id(), 0, 3}, 1);
    }

    void TearDown() override {
        _tablet.reset();
        _rowsets.clear();
        _schema.reset();
        config::disable_storage_row_cache = _saved_disable_row_cache;
        ExecEnv::GetInstance()->_row_cache = _saved_cache;
        _cache.reset();
        MowTransformTestBase::TearDown();
    }

    Status lookup(PointQueryExecutor& executor, int64_t version) {
        executor._tablet = _tablet;
        PTabletKeyLookupRequest request;
        request.set_snapshot_version(version);
        RETURN_IF_ERROR(executor._init_read_version(&request));
        constexpr std::array<int32_t, 4> keys {1, 2, 3, 99};
        RowKeyEncoder encoder {*_schema, /*mow=*/true};
        executor._row_read_ctxs.resize(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            executor._row_read_ctxs[i]._primary_key = encode_key(_schema, encoder, keys[i]);
        }
        return executor._lookup_row_key();
    }

    void expect_row(PointQueryExecutor& executor, size_t index,
                    const RowsetSharedPtr& expected_rowset, uint32_t expected_row_id,
                    int32_t expected_value, int8_t expected_delete_sign) {
        auto& context = executor._row_read_ctxs[index];
        ASSERT_TRUE(context._row_location.has_value());
        ASSERT_NE(context._rowset_ptr, nullptr);
        EXPECT_EQ(*context._rowset_ptr, expected_rowset);
        EXPECT_EQ(context._row_location->rowset_id, expected_rowset->rowset_id());
        EXPECT_EQ(context._row_location->segment_id, 0);
        EXPECT_EQ(context._row_location->row_id, expected_row_id);
        EXPECT_FALSE(context._cached_row_data.valid());

        Block rows;
        ASSERT_TRUE(read_rowset(*context._rowset_ptr, _schema, &rows).ok());
        ASSERT_LT(context._row_location->row_id, rows.rows());
        EXPECT_EQ(read_int(rows, 1, context._row_location->row_id), expected_value);
        EXPECT_EQ(read_tinyint(rows, 2, context._row_location->row_id), expected_delete_sign);
    }

    TabletSchemaSPtr _schema;
    TabletSharedPtr _tablet;
    std::vector<RowsetSharedPtr> _rowsets;
    std::unique_ptr<RowCache> _cache;
    RowCache* _saved_cache = nullptr;
    bool _saved_disable_row_cache = false;
};

TEST_F(PointQuerySnapshotTest, OldSnapshotPreservesRowsBeforeUpdateAndDelete) {
    {
        PointQueryExecutor executor;
        ASSERT_TRUE(lookup(executor, 2).ok());
        EXPECT_EQ(executor._row_hits, 3);
        expect_row(executor, 0, _rowsets[2], 0, 11, 0);
        expect_row(executor, 1, _rowsets[2], 1, 22, 0);
        expect_row(executor, 2, _rowsets[2], 2, 33, 0);
        EXPECT_FALSE(executor._row_read_ctxs[3]._row_location.has_value());
        // Only the per-key references survive release of the captured version path.
        EXPECT_EQ(_rowsets[2]->_refs_by_reader.load(), 3);
        EXPECT_EQ(_rowsets[3]->_refs_by_reader.load(), 0);
    }
    EXPECT_EQ(_rowsets[2]->_refs_by_reader.load(), 0);
}

TEST_F(PointQuerySnapshotTest, NewSnapshotFindsUpdatedRowAndDeleteSign) {
    {
        PointQueryExecutor executor;
        ASSERT_TRUE(lookup(executor, 3).ok());
        EXPECT_EQ(executor._row_hits, 3);
        expect_row(executor, 0, _rowsets[3], 0, 111, 0);
        expect_row(executor, 1, _rowsets[3], 1, 0, 1);
        expect_row(executor, 2, _rowsets[2], 2, 33, 0);
        EXPECT_FALSE(executor._row_read_ctxs[3]._row_location.has_value());
        EXPECT_EQ(_rowsets[2]->_refs_by_reader.load(), 1);
        EXPECT_EQ(_rowsets[3]->_refs_by_reader.load(), 2);
    }
    EXPECT_EQ(_rowsets[2]->_refs_by_reader.load(), 0);
    EXPECT_EQ(_rowsets[3]->_refs_by_reader.load(), 0);
}

TEST_F(PointQuerySnapshotTest, MissingSnapshotVersionFailsWithoutRows) {
    PointQueryExecutor executor;
    EXPECT_FALSE(lookup(executor, 4).ok());
    EXPECT_EQ(executor._row_hits, 0);
    for (auto& context : executor._row_read_ctxs) {
        EXPECT_FALSE(context._row_location.has_value());
        EXPECT_EQ(context._rowset_ptr, nullptr);
        EXPECT_FALSE(context._cached_row_data.valid());
    }
    for (const auto& rowset : _rowsets) {
        EXPECT_EQ(rowset->_refs_by_reader.load(), 0);
    }
}

TEST_F(PointQuerySnapshotTest, OldSnapshotBypassesFutureRowCache) {
    RowKeyEncoder encoder {*_schema, /*mow=*/true};
    auto key = encode_key(_schema, encoder, 1);
    _cache->insert({_tablet->tablet_id(), Slice {key}}, Slice {"future row"});
    RowCache::CacheHandle cached;
    ASSERT_TRUE(_cache->lookup({_tablet->tablet_id(), Slice {key}}, &cached));
    EXPECT_EQ(cached.data().to_string(), "future row");

    PointQueryExecutor executor;
    ASSERT_TRUE(lookup(executor, 2).ok());
    EXPECT_EQ(executor._profile_metrics.row_cache_hits, 0);
    expect_row(executor, 0, _rowsets[2], 0, 11, 0);
    expect_row(executor, 1, _rowsets[2], 1, 22, 0);
    EXPECT_EQ(executor._row_hits, 3);
}

} // namespace doris
