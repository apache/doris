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

#include <functional>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include "core/field.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/index/primary_key_index.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_writer.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet_schema_helper.h"

namespace doris {

TabletSchemaSPtr create_schema(const std::vector<TabletColumnPtr>& columns, KeysType keys_type);
using Generator = std::function<void(size_t rid, int cid, Field& field)>;
void build_segment(SegmentWriterOptions opts, TabletSchemaSPtr build_schema, size_t segment_id,
                   TabletSchemaSPtr query_schema, size_t nrows, Generator generator,
                   std::shared_ptr<Segment>* res, std::string segment_dir);

class SegmentPrimaryKeyLookupTest : public testing::Test {
protected:
    void SetUp() override {
        auto fs = io::global_local_filesystem();
        ASSERT_TRUE(fs->delete_directory(_dir).ok());
        ASSERT_TRUE(fs->create_directory(_dir).ok());
        ExecEnv::GetInstance()->set_storage_engine(
                std::make_unique<StorageEngine>(EngineOptions {}));
        _schema = create_schema(
                {create_varchar_key(0, false),
                 create_int_value(1, FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE,
                                  false)},
                UNIQUE_KEYS);
        SegmentWriterOptions opts;
        opts.enable_unique_key_merge_on_write = true;
        auto generator = [](size_t rid, int cid, Field& field) {
            if (cid == 0) {
                // Each key exceeds the normal PK data-page target, producing a large value index.
                field = Field::create_field<TYPE_VARCHAR>(std::string(50000, 'a' + rid));
            } else {
                field = Field::create_field<TYPE_INT>(static_cast<int32_t>(rid));
            }
        };
        build_segment(opts, _schema, 0, _schema, 8, generator, &_segment, _dir);
        ASSERT_NE(_segment, nullptr);
        ASSERT_NE(_segment->_pk_index_reader, nullptr);
        ASSERT_FALSE(_segment->_load_index_once.has_called());
        ASSERT_FALSE(_segment->_load_pk_bf_once.has_called());
        ASSERT_FALSE(_segment->_pk_index_meta->primary_key_index()
                             .value_index_meta()
                             .is_root_data_page());
        _present_key = _segment->min_key();

        // Find a definite BF miss without initializing the segment's own reader. Do not assume
        // that an arbitrary absent key is rejected, because bloom filters allow false positives.
        PrimaryKeyIndexReader probe;
        ASSERT_TRUE(
                probe.parse_bf(_segment->file_reader(), *_segment->_pk_index_meta, nullptr).ok());
        for (int i = 0; i < 10000; ++i) {
            auto candidate = _present_key + std::to_string(i);
            if (!probe.check_present(Slice(candidate))) {
                _missing_key = std::move(candidate);
                break;
            }
        }
        ASSERT_FALSE(_missing_key.empty());
    }

    void TearDown() override {
        _segment.reset();
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_dir).ok());
    }

    Status lookup(const std::string& key, RowLocation* location) {
        return _segment->lookup_row_key(Slice(key), _schema.get(), false, false, location, nullptr);
    }

    const std::string _dir = "./ut_dir/segment_primary_key_lookup_test";
    TabletSchemaSPtr _schema;
    std::shared_ptr<segment_v2::Segment> _segment;
    std::string _present_key;
    std::string _missing_key;
};

TEST_F(SegmentPrimaryKeyLookupTest, BloomFilterMissDoesNotLoadIndex) {
    RowLocation location;
    for (int i = 0; i < 3; ++i) {
        auto st = lookup(_missing_key, &location);
        EXPECT_TRUE(st.is<ErrorCode::KEY_NOT_FOUND>()) << st;
        EXPECT_TRUE(_segment->_load_pk_bf_once.has_called());
        EXPECT_FALSE(_segment->_load_index_once.has_called());
        EXPECT_EQ(_segment->_pk_index_reader->_index_reader, nullptr);
        EXPECT_TRUE(_segment->healthy_status().ok());
    }
}

TEST_F(SegmentPrimaryKeyLookupTest, HitAfterMissPreservesBloomFilter) {
    RowLocation location;
    ASSERT_TRUE(lookup(_missing_key, &location).is<ErrorCode::KEY_NOT_FOUND>());
    auto* reader = _segment->_pk_index_reader.get();
    auto* bf = reader->_bf.get();
    ASSERT_FALSE(_segment->_load_index_once.has_called());

    ASSERT_TRUE(lookup(_present_key, &location).ok());
    EXPECT_EQ(location.segment_id, _segment->id());
    EXPECT_EQ(location.row_id, 0);
    EXPECT_TRUE(_segment->_load_index_once.has_called());
    EXPECT_EQ(_segment->_pk_index_reader.get(), reader);
    EXPECT_EQ(reader->_bf.get(), bf);
    auto* index = reader->_index_reader.get();
    ASSERT_NE(index, nullptr);

    ASSERT_TRUE(lookup(_present_key, &location).ok());
    EXPECT_EQ(reader->_index_reader.get(), index);
    EXPECT_EQ(reader->_bf.get(), bf);
    EXPECT_TRUE(lookup(_missing_key, &location).is<ErrorCode::KEY_NOT_FOUND>());
}

TEST_F(SegmentPrimaryKeyLookupTest, BloomFilterPositiveStillChecksExactKey) {
    ASSERT_TRUE(_segment->_load_pk_bloom_filter(nullptr).ok());
    // Deliberately make the BF positive for an absent key to exercise the false-positive path.
    _segment->_pk_index_reader->_bf->add_bytes(_missing_key.data(), _missing_key.size());
    ASSERT_TRUE(_segment->_pk_index_reader->check_present(Slice(_missing_key)));
    RowLocation location;
    auto st = lookup(_missing_key, &location);
    EXPECT_TRUE(st.is<ErrorCode::KEY_NOT_FOUND>()) << st;
    EXPECT_TRUE(_segment->_load_index_once.has_called());
}

TEST_F(SegmentPrimaryKeyLookupTest, EagerLoadThenLookupReusesReader) {
    auto* reader = _segment->_pk_index_reader.get();
    ASSERT_TRUE(_segment->load_pk_index_and_bf(nullptr).ok());
    auto* index = reader->_index_reader.get();
    auto* bf = reader->_bf.get();
    RowLocation location;
    ASSERT_TRUE(lookup(_present_key, &location).ok());
    EXPECT_EQ(location.row_id, 0);
    EXPECT_TRUE(lookup(_missing_key, &location).is<ErrorCode::KEY_NOT_FOUND>());
    EXPECT_EQ(_segment->_pk_index_reader.get(), reader);
    EXPECT_EQ(reader->_index_reader.get(), index);
    EXPECT_EQ(reader->_bf.get(), bf);
}

TEST_F(SegmentPrimaryKeyLookupTest, AddedSequenceColumnUsesUnsuffixedBloomKey) {
    TabletSchema latest_schema;
    latest_schema.copy_from(*_schema);
    latest_schema._sequence_col_idx = 1;
    std::string sequence_suffix(latest_schema.column(1).length() + 1, '\0');
    RowLocation location;
    auto missing = _missing_key + sequence_suffix;
    auto st = _segment->lookup_row_key(Slice(missing), &latest_schema, true, false, &location,
                                       nullptr);
    ASSERT_TRUE(st.is<ErrorCode::KEY_NOT_FOUND>()) << st;
    EXPECT_FALSE(_segment->_load_index_once.has_called());

    auto present = _present_key + sequence_suffix;
    std::string encoded_sequence = "not cleared";
    st = _segment->lookup_row_key(Slice(present), &latest_schema, true, false, &location, nullptr,
                                  &encoded_sequence);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(location.row_id, 0);
    EXPECT_TRUE(encoded_sequence.empty()); // The original segment has no sequence column.
}

TEST_F(SegmentPrimaryKeyLookupTest, BloomMissStripsSequenceAndRowIdSuffixes) {
    TabletSchema latest_schema;
    latest_schema.copy_from(*_schema);
    latest_schema._sequence_col_idx = 1;
    latest_schema._cluster_key_uids = {0};
    auto missing = _missing_key + std::string(latest_schema.column(1).length() + 1 +
                                                      PrimaryKeyIndexReader::ROW_ID_LENGTH,
                                              '\0');
    RowLocation location;
    auto st = _segment->lookup_row_key(Slice(missing), &latest_schema, true, true, &location,
                                       nullptr);
    EXPECT_TRUE(st.is<ErrorCode::KEY_NOT_FOUND>()) << st;
    EXPECT_FALSE(_segment->_load_index_once.has_called());
}

TEST_F(SegmentPrimaryKeyLookupTest, ConcurrentIndexAndBloomFilterInitialization) {
    std::promise<void> start;
    auto ready = start.get_future().share();
    std::vector<std::future<Status>> tasks;
    for (int i = 0; i < 12; ++i) {
        tasks.emplace_back(std::async(std::launch::async, [&, ready, i] {
            ready.wait();
            if (i % 3 == 0) {
                // This path need not initialize BF, and can race with BF-only lookup misses.
                return _segment->load_index(nullptr);
            }
            RowLocation location;
            auto st = lookup(i % 3 == 1 ? _present_key : _missing_key, &location);
            if (i % 3 == 2) {
                if (st.is<ErrorCode::KEY_NOT_FOUND>()) {
                    return Status::OK();
                }
                return st.ok() ? Status::InternalError("Expected a bloom-filter miss") : st;
            }
            if (st.ok() && location.row_id != 0) {
                return Status::InternalError("Unexpected primary key row id");
            }
            return st;
        }));
    }
    start.set_value();
    for (auto& task : tasks) {
        auto st = task.get();
        EXPECT_TRUE(st.ok()) << st;
    }
    EXPECT_TRUE(_segment->_load_index_once.has_called());
    EXPECT_TRUE(_segment->_load_pk_bf_once.has_called());
    EXPECT_TRUE(_segment->healthy_status().ok());
}

} // namespace doris
