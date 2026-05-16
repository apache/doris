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

#include "storage/partial_update_info.h"

#include <gen_cpp/olap_file.pb.h>

#include "core/block/block.h"
#include "io/fs/local_file_system.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/segment/vertical_segment_writer.h"
#include "storage/tablet/base_tablet.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

namespace {

TabletSchemaSPtr create_unique_key_schema() {
    auto tablet_schema = std::make_shared<TabletSchema>();
    TabletSchemaPB tablet_schema_pb;
    tablet_schema_pb.set_keys_type(UNIQUE_KEYS);
    tablet_schema_pb.set_num_short_key_columns(1);
    tablet_schema_pb.set_num_rows_per_row_block(1024);
    tablet_schema_pb.set_compress_kind(COMPRESS_NONE);
    tablet_schema_pb.set_next_column_unique_id(4);

    auto* key_column = tablet_schema_pb.add_column();
    key_column->set_unique_id(1);
    key_column->set_name("k");
    key_column->set_type("INT");
    key_column->set_is_key(true);
    key_column->set_length(4);
    key_column->set_index_length(4);
    key_column->set_is_nullable(false);
    key_column->set_is_bf_column(false);

    auto* value_column = tablet_schema_pb.add_column();
    value_column->set_unique_id(2);
    value_column->set_name("v");
    value_column->set_type("INT");
    value_column->set_is_key(false);
    value_column->set_length(4);
    value_column->set_index_length(4);
    value_column->set_is_nullable(false);
    value_column->set_is_bf_column(false);

    auto* delete_sign_column = tablet_schema_pb.add_column();
    delete_sign_column->set_unique_id(3);
    delete_sign_column->set_name(DELETE_SIGN);
    delete_sign_column->set_type("TINYINT");
    delete_sign_column->set_is_key(false);
    delete_sign_column->set_length(1);
    delete_sign_column->set_index_length(1);
    delete_sign_column->set_is_nullable(false);
    delete_sign_column->set_is_bf_column(false);

    tablet_schema->init_from_pb(tablet_schema_pb);
    return tablet_schema;
}

class FakeTablet : public BaseTablet {
public:
    explicit FakeTablet(TabletSchemaSPtr schema)
            : BaseTablet(std::make_shared<TabletMeta>(std::move(schema))) {}

    std::string tablet_path() const override { return ""; }

    bool exceed_version_limit(int32_t /*limit*/) override { return false; }

    Result<std::unique_ptr<RowsetWriter>> create_rowset_writer(RowsetWriterContext& /*context*/,
                                                               bool /*vertical*/) override {
        return ResultError(Status::NotSupported("fake tablet"));
    }

    Result<std::unique_ptr<RowsetWriter>> create_transient_rowset_writer(
            const Rowset& /*rowset*/, std::shared_ptr<PartialUpdateInfo> /*partial_update_info*/,
            int64_t /*txn_expiration*/ = 0) override {
        return ResultError(Status::NotSupported("fake tablet"));
    }

    Status capture_rs_readers(const Version& /*spec_version*/,
                              std::vector<RowSetSplits>* /*rs_splits*/,
                              const CaptureRowsetOps& /*opts*/) override {
        return Status::NotSupported("fake tablet");
    }

    Status save_delete_bitmap(const TabletTxnInfo* /*txn_info*/, int64_t /*txn_id*/,
                              DeleteBitmapPtr /*delete_bitmap*/, RowsetWriter* /*rowset_writer*/,
                              const RowsetIdUnorderedSet& /*cur_rowset_ids*/,
                              int64_t /*lock_id*/ = -1,
                              int64_t /*next_visible_version*/ = -1) override {
        return Status::NotSupported("fake tablet");
    }

    CalcDeleteBitmapExecutor* calc_delete_bitmap_executor() override { return nullptr; }

    void clear_cache() override {}

    Versions calc_missed_versions(int64_t /*spec_version*/,
                                  Versions /*existing_versions*/) const override {
        return {};
    }

    size_t tablet_footprint() override { return 0; }
};

} // namespace

TEST(PartialUpdateInfoTest, PersistsSequenceMapColumnUid) {
    PartialUpdateInfo info;
    info.partial_update_mode = UniqueKeyUpdateModePB::UPDATE_FLEXIBLE_COLUMNS;
    info.sequence_map_col_unqiue_id = 123;

    PartialUpdateInfoPB pb;
    info.to_pb(&pb);

    ASSERT_TRUE(pb.has_sequence_map_col_uid());
    EXPECT_EQ(pb.sequence_map_col_uid(), 123);

    PartialUpdateInfo decoded;
    decoded.from_pb(&pb);
    EXPECT_EQ(decoded.sequence_map_col_uid(), 123);
}

TEST(PartialUpdateInfoTest, DefaultsMissingSequenceMapColumnUid) {
    PartialUpdateInfoPB pb;
    pb.set_partial_update_mode(UniqueKeyUpdateModePB::UPDATE_FLEXIBLE_COLUMNS);

    PartialUpdateInfo decoded;
    decoded.from_pb(&pb);
    EXPECT_EQ(decoded.sequence_map_col_uid(), -1);
}

TEST(PartialUpdateInfoTest, FlexiblePartialUpdateRejectsSlicedRowPos) {
    auto schema = create_unique_key_schema();

    RowsetWriterContext rowset_ctx;
    rowset_ctx.tablet_schema = schema;
    rowset_ctx.partial_update_info = std::make_shared<PartialUpdateInfo>();
    rowset_ctx.partial_update_info->partial_update_mode =
            UniqueKeyUpdateModePB::UPDATE_FLEXIBLE_COLUMNS;

    segment_v2::VerticalSegmentWriterOptions opts;
    opts.rowset_ctx = &rowset_ctx;
    opts.write_type = DataWriteType::TYPE_DIRECT;
    opts.enable_unique_key_merge_on_write = true;

    auto fs = io::global_local_filesystem();
    static_cast<void>(fs->create_directory("./ut_dir"));
    static_cast<void>(
            fs->delete_file("./ut_dir/flexible_partial_update_rejects_sliced_row_pos.dat"));
    io::FileWriterPtr file_writer;
    auto st = fs->create_file("./ut_dir/flexible_partial_update_rejects_sliced_row_pos.dat",
                              &file_writer);
    ASSERT_TRUE(st.ok()) << st;

    auto tablet = std::make_shared<FakeTablet>(schema);
    segment_v2::VerticalSegmentWriter writer(file_writer.get(), 0, schema, tablet, nullptr, opts,
                                             nullptr);

    Block block = schema->create_block();
    ASSERT_TRUE(writer.batch_block(&block, 1, 1).ok());
    st = writer.write_batch();
    EXPECT_TRUE(st.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << st;
    EXPECT_NE(st.to_string().find("whole-block duplicate-key aggregation"), std::string::npos)
            << st;
}

} // namespace doris
