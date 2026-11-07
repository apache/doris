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
#include "storage/rowset/beta_rowset.h"

#include <aws/core/auth/AWSAuthSigner.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/Outcome.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/model/GetObjectResult.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <gen_cpp/olap_common.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <stdint.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "core/block/block.h"
#include "cpp/obj-client/s3_obj_storage_client.h"
#include "cpp/sync_point.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "io/fs/s3_file_system.h"
#include "json2pb/json_to_pb.h"
#include "runtime/exec_env.h"
#include "storage/data_dir.h"
#include "storage/olap_common.h"
#include "storage/options.h"
#include "storage/rowset/beta_rowset_writer.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_reader_context.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/schema.h"
#include "storage/segment/segment.h"
#include "storage/storage_engine.h"
#include "storage/storage_policy.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"
#include "util/s3_util.h"

namespace Aws {
namespace S3 {
namespace Model {
class GetObjectRequest;
class HeadObjectRequest;
} // namespace Model
} // namespace S3
} // namespace Aws
namespace doris {
struct RowsetReaderContext;
} // namespace doris

using std::string;

namespace doris {
using namespace ErrorCode;

static const uint32_t MAX_PATH_LEN = 1024;
static const std::string kTestDir = "./data_test/data/beta_rowset_test";

class BetaRowsetTest : public testing::Test {
public:
    static void SetUpTestSuite() {
        config::tablet_map_shard_size = 1;
        config::txn_map_shard_size = 1;
        config::txn_shard_size = 1;

        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        config::storage_root_path = std::string(buffer) + "/data_test";

        auto st = io::global_local_filesystem()->delete_directory(config::storage_root_path);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(config::storage_root_path);
        ASSERT_TRUE(st.ok()) << st;

        EXPECT_TRUE(io::global_local_filesystem()->create_directory(kTestDir).ok());
    }

protected:
    OlapReaderStatistics _stats;

    // (k1 int, k2 varchar(20), k3 int) duplicated key (k1, k2)
    void create_tablet_schema(TabletSchemaSPtr tablet_schema) {
        TabletSchemaPB tablet_schema_pb;
        tablet_schema_pb.set_keys_type(DUP_KEYS);
        tablet_schema_pb.set_num_short_key_columns(2);
        tablet_schema_pb.set_num_rows_per_row_block(1024);
        tablet_schema_pb.set_compress_kind(COMPRESS_NONE);
        tablet_schema_pb.set_next_column_unique_id(4);

        ColumnPB* column_1 = tablet_schema_pb.add_column();
        column_1->set_unique_id(1);
        column_1->set_name("k1");
        column_1->set_type("INT");
        column_1->set_is_key(true);
        column_1->set_length(4);
        column_1->set_index_length(4);
        column_1->set_is_nullable(true);
        column_1->set_is_bf_column(false);

        ColumnPB* column_2 = tablet_schema_pb.add_column();
        column_2->set_unique_id(2);
        column_2->set_name("k2");
        column_2->set_type(
                "INT"); // TODO change to varchar(20) when dict encoding for string is supported
        column_2->set_length(4);
        column_2->set_index_length(4);
        column_2->set_is_nullable(true);
        column_2->set_is_key(true);
        column_2->set_is_nullable(true);
        column_2->set_is_bf_column(false);

        ColumnPB* column_3 = tablet_schema_pb.add_column();
        column_3->set_unique_id(3);
        column_3->set_name("v1");
        column_3->set_type("INT");
        column_3->set_length(4);
        column_3->set_is_key(false);
        column_3->set_is_nullable(false);
        column_3->set_is_bf_column(false);
        column_3->set_aggregation("SUM");

        tablet_schema->init_from_pb(tablet_schema_pb);
    }

    void create_rowset_writer_context(TabletSchemaSPtr tablet_schema,
                                      RowsetWriterContext* rowset_writer_context) {
        RowsetId rowset_id;
        rowset_id.init(10000);
        rowset_writer_context->rowset_id = rowset_id;
        rowset_writer_context->tablet_id = 12345;
        rowset_writer_context->tablet_schema_hash = 1111;
        rowset_writer_context->partition_id = 10;
        rowset_writer_context->rowset_type = BETA_ROWSET;
        rowset_writer_context->tablet_path = kTestDir;
        rowset_writer_context->rowset_state = VISIBLE;
        rowset_writer_context->tablet_schema = tablet_schema;
        rowset_writer_context->version.first = 10;
        rowset_writer_context->version.second = 10;
    }

    void create_and_init_rowset_reader(Rowset* rowset, RowsetReaderContext& context,
                                       RowsetReaderSharedPtr* result) {
        auto s = rowset->create_reader(result);
        EXPECT_EQ(Status::OK(), s);
        EXPECT_TRUE(*result != nullptr);

        s = (*result)->init(&context);
        EXPECT_EQ(Status::OK(), s);
    }

    // (k1 int, k2 int, v int) unique key (k1, k2)
    void create_unique_tablet_schema(TabletSchemaSPtr tablet_schema) {
        TabletSchemaPB tablet_schema_pb;
        tablet_schema_pb.set_keys_type(UNIQUE_KEYS);
        tablet_schema_pb.set_num_short_key_columns(2);
        tablet_schema_pb.set_num_rows_per_row_block(1024);
        tablet_schema_pb.set_compress_kind(COMPRESS_NONE);
        tablet_schema_pb.set_next_column_unique_id(4);
        const std::array<const char*, 3> names {"k1", "k2", "v"};
        for (int32_t i = 0; i < 3; ++i) {
            ColumnPB* column = tablet_schema_pb.add_column();
            column->set_unique_id(i + 1);
            column->set_name(names[i]);
            column->set_type("INT");
            column->set_is_key(i < 2);
            column->set_length(4);
            column->set_index_length(4);
            column->set_is_nullable(false);
            column->set_is_bf_column(false);
        }
        tablet_schema->init_from_pb(tablet_schema_pb);
    }

    // The rowset one load wrote when it flushed several memtables: segment i holds the rows
    // (k1, k2, v) of `segments[i]`, and the segments overlap on keys.
    void write_overlapping_rowset(StorageEngine& engine, TabletSchemaSPtr tablet_schema,
                                  int64_t rowset_id,
                                  const std::vector<std::vector<std::array<int32_t, 3>>>& segments,
                                  RowsetSharedPtr* rowset) {
        RowsetWriterContext context;
        create_rowset_writer_context(tablet_schema, &context);
        context.rowset_id.init(rowset_id);
        context.segments_overlap = OVERLAPPING;
        auto writer = RowsetFactory::create_rowset_writer(engine, context, false);
        ASSERT_TRUE(writer.has_value()) << writer.error();
        for (const auto& rows : segments) {
            Block block = tablet_schema->create_storage_block();
            {
                auto columns_guard = block.mutate_columns_scoped();
                auto& columns = columns_guard.mutable_columns();
                for (const auto& row : rows) {
                    for (size_t i = 0; i < row.size(); ++i) {
                        columns[i]->insert_data(reinterpret_cast<const char*>(&row[i]),
                                                sizeof(int32_t));
                    }
                }
            }
            auto st = writer.value()->add_block(&block);
            ASSERT_TRUE(st.ok()) << st;
            st = writer.value()->flush();
            ASSERT_TRUE(st.ok()) << st;
        }
        auto st = writer.value()->build(*rowset);
        ASSERT_TRUE(st.ok()) << st;
    }

    // The (k1, k2) of every row a reader of `rowset` initialized with `context` returns.
    std::vector<std::pair<int32_t, int32_t>> read_keys(Rowset* rowset,
                                                       RowsetReaderContext& context) {
        RowsetReaderSharedPtr reader;
        create_and_init_rowset_reader(rowset, context, &reader);
        std::vector<std::pair<int32_t, int32_t>> keys;
        Status st;
        do {
            Block block = context.read_schema->create_read_block();
            st = reader->next_batch(&block);
            for (size_t row = 0; row < block.rows(); ++row) {
                keys.emplace_back(
                        static_cast<int32_t>(block.get_by_position(0).column->get_int(row)),
                        static_cast<int32_t>(block.get_by_position(1).column->get_int(row)));
            }
        } while (st.ok());
        EXPECT_TRUE(st.is<END_OF_FILE>()) << st;
        return keys;
    }

    // The reader context of a read of every column of `tablet_schema` that merges the overlapping
    // segments of a UNIQUE_KEYS rowset, by `orderby_key_columns` when not null and by the whole
    // key otherwise.
    RowsetReaderContext unique_merge_context(const TabletSchema& tablet_schema,
                                             std::vector<uint32_t>* orderby_key_columns) {
        auto read_schema = std::make_shared<ReadSchema>(tablet_schema.columns());
        EXPECT_TRUE(read_schema
                            ->init_from_tablet_schema(tablet_schema,
                                                      /*merge_by_sequence_mapping=*/false,
                                                      /*map_changelog_columns=*/false)
                            .ok());
        RowsetReaderContext context;
        context.read_schema = read_schema;
        context.stats = &_stats;
        context.need_ordered_result = true;
        context.is_unique = true;
        context.read_orderby_key_columns = orderby_key_columns;
        return context;
    }

    void init_rs_meta(RowsetMetaSharedPtr& pb1, int64_t start, int64_t end) {
        std::string json_rowset_meta = R"({
            "rowset_id": 540085,
            "tablet_id": 15674,
            "partition_id": 10000,
            "txn_id": 4045,
            "tablet_schema_hash": 567997588,
            "rowset_type": "BETA_ROWSET",
            "rowset_state": "VISIBLE",
            "start_version": 2,
            "end_version": 2,
            "num_rows": 3929,
            "total_disk_size": 84699,
            "data_disk_size": 84464,
            "index_disk_size": 235,
            "empty": false,
            "load_id": {
                "hi": -5350970832824939812,
                "lo": -6717994719194512122
            },
            "creation_time": 1553765670
        })";

        RowsetMetaPB rowset_meta_pb;
        json2pb::JsonToProtoMessage(json_rowset_meta, &rowset_meta_pb);
        rowset_meta_pb.set_start_version(start);
        rowset_meta_pb.set_end_version(end);
        rowset_meta_pb.set_creation_time(10000);

        pb1->init_from_pb(rowset_meta_pb);
    }

    void construct_column(ColumnPB* column_pb, TabletIndexPB* tablet_index, int64_t index_id,
                          const std::string& index_name, int32_t col_unique_id,
                          const std::string& column_type, const std::string& column_name,
                          const std::map<std::string, std::string>& properties =
                                  std::map<std::string, std::string>(),
                          bool is_key = false) {
        column_pb->set_unique_id(col_unique_id);
        column_pb->set_name(column_name);
        column_pb->set_type(column_type);
        column_pb->set_is_key(is_key);
        column_pb->set_is_nullable(true);
        tablet_index->set_index_id(index_id);
        tablet_index->set_index_name(index_name);
        tablet_index->set_index_type(IndexType::INVERTED);
        tablet_index->add_col_unique_id(col_unique_id);
        if (!properties.empty()) {
            auto* pros = tablet_index->mutable_properties();
            for (const auto& [key, value] : properties) {
                (*pros)[key] = value;
            }
        }
    }

private:
    std::unique_ptr<DataDir> _data_dir;
};

class BetaRowsetWriterForTest : public BetaRowsetWriter {
public:
    explicit BetaRowsetWriterForTest(StorageEngine& engine) : BetaRowsetWriter(engine) {}

    Status build_tmp(RowsetSharedPtr& rowset) { return _build_tmp(rowset); }
};

class S3ClientMock : public Aws::S3::S3Client {
    S3ClientMock() {}
    S3ClientMock(const Aws::Auth::AWSCredentials& credentials,
                 const Aws::Client::ClientConfiguration& clientConfiguration,
                 Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy signPayloads,
                 bool use_virtual_addressing)
            : Aws::S3::S3Client(credentials, clientConfiguration, signPayloads,
                                use_virtual_addressing) {}

    Aws::S3::Model::HeadObjectOutcome HeadObject(
            const Aws::S3::Model::HeadObjectRequest& request) const override {
        Aws::S3::Model::HeadObjectOutcome response;
        response.success = false;
        return response;
    }

    Aws::S3::Model::GetObjectOutcome GetObject(
            const Aws::S3::Model::GetObjectRequest& request) const override {
        Aws::S3::Model::GetObjectOutcome response;
        response.success = false;
        return response;
    }
};

class S3ClientMockGetError : public S3ClientMock {
    Aws::S3::Model::HeadObjectOutcome HeadObject(
            const Aws::S3::Model::HeadObjectRequest& request) const override {
        Aws::S3::Model::HeadObjectOutcome response;
        response.GetResult().SetContentLength(20);
        response.success = true;
        return response;
    }
};

class S3ClientMockGetErrorData : public S3ClientMock {
    Aws::S3::Model::HeadObjectOutcome HeadObject(
            const Aws::S3::Model::HeadObjectRequest& request) const override {
        Aws::S3::Model::HeadObjectOutcome response;
        response.GetResult().SetContentLength(20);
        response.success = true;
        return response;
    }

    Aws::S3::Model::GetObjectOutcome GetObject(
            const Aws::S3::Model::GetObjectRequest& request) const override {
        Aws::S3::Model::GetObjectOutcome response;
        response.GetResult().SetContentLength(4);
        response.success = true;
        return response;
    }
};

class TestBetaRowsetWriter final : public BaseBetaRowsetWriter {
public:
    Status build(RowsetSharedPtr& rowset) override { return Status::OK(); }
};

TEST_F(BetaRowsetTest, ReadTest) {
    RowsetMetaSharedPtr rowset_meta = std::make_shared<RowsetMeta>();
    BetaRowset rowset(nullptr, rowset_meta, "");
    S3Conf s3_conf {.bucket = "bucket",
                    .prefix = "prefix",
                    .client_conf = {
                            .endpoint = "endpoint",
                            .region = "region",
                            .ak = "ak",
                            .sk = "sk",
                            .token = "",
                            .bucket = "",
                            .role_arn = "",
                            .external_id = "",
                    }};
    std::string resource_id = "10000";
    auto res = io::S3FileSystem::create(std::move(s3_conf), io::FileSystem::TMP_FS_ID);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto fs = res.value();
    StorageResource storage_resource(fs);
    auto& client = fs->client_holder()->_client;
    // failed to head object
    {
        Aws::Auth::AWSCredentials aws_cred("ak", "sk");
        Aws::Client::ClientConfiguration aws_config;
        std::shared_ptr<Aws::S3::S3Client> s3_client = std::make_shared<S3ClientMock>(
                aws_cred, aws_config, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                true);

        client = std::make_shared<io::S3ObjStorageClient>(std::move(s3_client));

        rowset.rowset_meta()->set_num_segments(1);
        rowset.rowset_meta()->set_remote_storage_resource(storage_resource);

        std::vector<segment_v2::SegmentSharedPtr> segments;
        Status st = rowset.load_segments(&segments);
        ASSERT_FALSE(st.ok());
    }

    // failed to get object
    {
        Aws::Auth::AWSCredentials aws_cred("ak", "sk");
        Aws::Client::ClientConfiguration aws_config;
        client = std::make_shared<io::S3ObjStorageClient>(
                std::make_shared<Aws::S3::S3Client>(S3ClientMockGetError()));

        rowset.rowset_meta()->set_num_segments(1);
        rowset.rowset_meta()->set_remote_storage_resource(storage_resource);

        std::vector<segment_v2::SegmentSharedPtr> segments;
        Status st = rowset.load_segments(&segments);
        ASSERT_FALSE(st.ok());
    }

    // get error data
    {
        Aws::Auth::AWSCredentials aws_cred("ak", "sk");
        Aws::Client::ClientConfiguration aws_config;
        client = std::make_shared<io::S3ObjStorageClient>(
                std::make_shared<Aws::S3::S3Client>(S3ClientMockGetErrorData()));

        rowset.rowset_meta()->set_num_segments(1);
        rowset.rowset_meta()->set_remote_storage_resource(storage_resource);

        std::vector<segment_v2::SegmentSharedPtr> segments;
        Status st = rowset.load_segments(&segments);
        ASSERT_FALSE(st.ok());
    }
}

TEST_F(BetaRowsetTest, AddToBinlogTest) {
    RowsetMetaSharedPtr rowset_meta = std::make_shared<RowsetMeta>();
    BetaRowset rowset(nullptr, rowset_meta, "");
    std::string resource_id = "10000";
    Status s = rowset.add_to_binlog();
    ASSERT_TRUE(s.ok()) << "first add_to_binlog(): " << s;
    s = rowset.add_to_binlog();
    ASSERT_TRUE(s.ok()) << "second add_to_binlog(): " << s;
}

TEST_F(BetaRowsetTest, PersistInvertedIndexStorageFormatInRowsetMeta) {
    auto tablet_schema = std::make_shared<TabletSchema>();
    create_tablet_schema(tablet_schema);

    RowsetWriterContext context;
    create_rowset_writer_context(tablet_schema, &context);
    context.inverted_index_storage_format = InvertedIndexStorageFormatPB::SNII;

    context.persist_inverted_index_storage_format = false;
    TestBetaRowsetWriter disabled_writer;
    ASSERT_TRUE(disabled_writer.init(context).ok());
    EXPECT_FALSE(disabled_writer.rowset_meta()->has_inverted_index_storage_format());

    context.persist_inverted_index_storage_format = true;
    TestBetaRowsetWriter enabled_writer;
    ASSERT_TRUE(enabled_writer.init(context).ok());
    ASSERT_TRUE(enabled_writer.rowset_meta()->has_inverted_index_storage_format());
    EXPECT_EQ(InvertedIndexStorageFormatPB::SNII,
              enabled_writer.rowset_meta()->inverted_index_storage_format());

    context.inverted_index_storage_format.reset();
    TestBetaRowsetWriter missing_format_writer;
    ASSERT_TRUE(missing_format_writer.init(context).ok());
    EXPECT_FALSE(missing_format_writer.rowset_meta()->has_inverted_index_storage_format());
}

TEST_F(BetaRowsetTest, GetIndexFileNames) {
    // v1
    {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);
        schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);
        construct_column(schema_pb.add_column(), schema_pb.add_index(), 10000, "key_index", 0,
                         "INT", "key");
        construct_column(schema_pb.add_column(), schema_pb.add_index(), 10001, "v1_index", 1,
                         "STRING", "v1");
        schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V1);
        auto tablet_schema = std::make_shared<TabletSchema>();
        tablet_schema->init_from_pb(schema_pb);

        auto rowset_meta = std::make_shared<RowsetMeta>();
        init_rs_meta(rowset_meta, 1, 1);
        rowset_meta->set_num_segments(2);

        BetaRowset rowset(tablet_schema, rowset_meta, "");
        auto file_names = rowset.get_index_file_names();
        ASSERT_EQ(file_names[0], "540085_0_10000.idx");
        ASSERT_EQ(file_names[1], "540085_0_10001.idx");
        ASSERT_EQ(file_names[2], "540085_1_10000.idx");
        ASSERT_EQ(file_names[3], "540085_1_10001.idx");
    }

    // v2
    {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);
        schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);
        construct_column(schema_pb.add_column(), schema_pb.add_index(), 10000, "key_index", 0,
                         "INT", "key");
        construct_column(schema_pb.add_column(), schema_pb.add_index(), 10001, "v1_index", 1,
                         "STRING", "v1");
        schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);
        auto tablet_schema = std::make_shared<TabletSchema>();
        tablet_schema->init_from_pb(schema_pb);

        auto rowset_meta = std::make_shared<RowsetMeta>();
        init_rs_meta(rowset_meta, 1, 1);
        rowset_meta->set_num_segments(2);

        BetaRowset rowset(tablet_schema, rowset_meta, "");
        auto file_names = rowset.get_index_file_names();
        ASSERT_EQ(file_names[0], "540085_0.idx");
        ASSERT_EQ(file_names[1], "540085_1.idx");
    }
}

TEST_F(BetaRowsetTest, SegmentViewUsesRealSegmentId) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V1);
    construct_column(schema_pb.add_column(), schema_pb.add_index(), 10000, "key_index", 0, "INT",
                     "key");
    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(schema_pb);

    auto rowset_meta = std::make_shared<RowsetMeta>();
    init_rs_meta(rowset_meta, 1, 1);
    rowset_meta->set_segment_ids({100, 102, 105});

    BetaRowset rowset(tablet_schema, rowset_meta, kTestDir);
    auto first_seg_path = rowset.segment(0).path();
    ASSERT_TRUE(first_seg_path.has_value()) << first_seg_path.error();
    EXPECT_EQ(first_seg_path.value(), kTestDir + "/540085_100.dat");

    auto seg = rowset.segment(1);
    EXPECT_EQ(seg.pos(), 1);
    EXPECT_EQ(seg.id(), 102);
    EXPECT_EQ(seg.file_name(), "540085_102.dat");

    auto seg_path = seg.path();
    ASSERT_TRUE(seg_path.has_value()) << seg_path.error();
    EXPECT_EQ(seg_path.value(), kTestDir + "/540085_102.dat");
    EXPECT_EQ(seg.file_cache_key(), segment_v2::Segment::file_cache_key("540085", 102));

    auto delete_bitmap_key = seg.delete_bitmap_key(7);
    EXPECT_EQ(std::get<0>(delete_bitmap_key), rowset.rowset_id());
    EXPECT_EQ(std::get<1>(delete_bitmap_key), 102);
    EXPECT_EQ(std::get<2>(delete_bitmap_key), 7);

    auto row_location = seg.row_location(10);
    EXPECT_EQ(row_location.rowset_id, rowset.rowset_id());
    EXPECT_EQ(row_location.segment_id, 102);
    EXPECT_EQ(row_location.row_id, 10);

    auto index_file_names = seg.index_file_names();
    ASSERT_EQ(index_file_names.size(), 1);
    EXPECT_EQ(index_file_names[0], "540085_102_10000.idx");

    auto index_file_cache_key = seg.index_file_cache_key(*tablet_schema->inverted_indexes()[0]);
    ASSERT_TRUE(index_file_cache_key.has_value()) << index_file_cache_key.error();
    EXPECT_EQ(index_file_cache_key.value(), kTestDir + "/540085_102_10000");
}

TEST_F(BetaRowsetTest, RowsetInfoShowsExplicitSegmentIds) {
    auto rowset_meta = std::make_shared<RowsetMeta>();
    init_rs_meta(rowset_meta, 1, 1);
    rowset_meta->set_num_segments(3);
    rowset_meta->set_segments_overlap(NONOVERLAPPING);

    BetaRowset rowset(nullptr, rowset_meta, "");
    std::string legacy_rowset_info = rowset.get_rowset_info_str();
    EXPECT_EQ(legacy_rowset_info.find(" []"), std::string::npos);

    rowset_meta->set_segment_ids({100, 101, 200});
    EXPECT_EQ(rowset.get_rowset_info_str(), legacy_rowset_info + " [100,101,200]");
}

TEST_F(BetaRowsetTest, TmpRowsetUsesCompletedSegmentIds) {
    auto tablet_schema = std::make_shared<TabletSchema>();
    create_tablet_schema(tablet_schema);
    RowsetWriterContext writer_context;
    create_rowset_writer_context(tablet_schema, &writer_context);

    EngineOptions options;
    StorageEngine engine(options);
    BetaRowsetWriterForTest writer(engine);
    ASSERT_TRUE(writer.init(writer_context).ok());

    SegmentStatistics segment_statistics;
    segment_statistics.row_num = 10;
    ASSERT_TRUE(writer.add_segment(6, segment_statistics).ok());
    ASSERT_TRUE(writer.add_segment(2, segment_statistics).ok());

    RowsetSharedPtr tmp_rowset;
    ASSERT_TRUE(writer.build_tmp(tmp_rowset).ok());
    ASSERT_NE(tmp_rowset, nullptr);
    EXPECT_EQ(tmp_rowset->num_segments(), 2);
    EXPECT_EQ(TEST_TRY(tmp_rowset->rowset_meta()->position_of(2)), 0);
    EXPECT_EQ(TEST_TRY(tmp_rowset->rowset_meta()->position_of(6)), 1);
    EXPECT_EQ(tmp_rowset->rowset_meta()->segment_id(0), 2);
    EXPECT_EQ(tmp_rowset->rowset_meta()->segment_id(1), 6);

    auto* beta_rowset = static_cast<BetaRowset*>(tmp_rowset.get());
    segment_v2::SegmentSharedPtr segment;
    for (int64_t seg_id : {3, 7}) {
        auto status = beta_rowset->load_segment(seg_id, nullptr, &segment);
        EXPECT_TRUE(status.is<NOT_FOUND>()) << status;
        EXPECT_EQ(segment, nullptr);
    }
}

TEST_F(BetaRowsetTest, GetSegmentNumRowsFromMeta) {
    // Test getting segment rows from rowset meta (new version data)
    // This test verifies that when segment_rows is present in rowset meta,
    // it uses the cached data directly without loading segments
    auto tablet_schema = std::make_shared<TabletSchema>();
    create_tablet_schema(tablet_schema);

    auto rowset_meta = std::make_shared<RowsetMeta>();
    init_rs_meta(rowset_meta, 1, 1);
    rowset_meta->set_num_segments(3);

    // Set segment rows in rowset meta (simulating new version data)
    std::vector<uint32_t> expected_segment_rows = {100, 200, 300};
    rowset_meta->set_num_segment_rows(expected_segment_rows);

    auto rowset = std::make_shared<BetaRowset>(tablet_schema, rowset_meta, "");

    // Use sync point to verify code path
    auto sp = SyncPoint::get_instance();
    bool used_meta_path = false;
    bool used_footer_path = false;

    sp->set_call_back("BetaRowset::get_segment_num_rows:use_segment_rows_from_meta",
                      [&](auto&& args) { used_meta_path = true; });

    sp->set_call_back("BetaRowset::get_segment_num_rows:load_from_segment_footer",
                      [&](auto&& args) { used_footer_path = true; });

    sp->enable_processing();

    std::vector<uint32_t> segment_rows;
    Status st = rowset->get_segment_num_rows(&segment_rows, false, &_stats);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(segment_rows.size(), 3);
    ASSERT_EQ(segment_rows[0], 100);
    ASSERT_EQ(segment_rows[1], 200);
    ASSERT_EQ(segment_rows[2], 300);

    // Verify that we used the meta path and not the footer path
    ASSERT_TRUE(used_meta_path);
    ASSERT_FALSE(used_footer_path);

    // Test calling get_segment_num_rows twice to verify cache works
    used_meta_path = false;
    used_footer_path = false;
    std::vector<uint32_t> segment_rows_2;
    st = rowset->get_segment_num_rows(&segment_rows_2, false, &_stats);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(segment_rows_2.size(), 3);
    ASSERT_EQ(segment_rows_2[0], 100);
    ASSERT_EQ(segment_rows_2[1], 200);
    ASSERT_EQ(segment_rows_2[2], 300);

    EXPECT_FALSE(used_meta_path);
    EXPECT_FALSE(used_footer_path);

    sp->clear_all_call_backs();
    sp->disable_processing();
    sp->clear_trace();
}

TEST_F(BetaRowsetTest, GetSegmentNumRowsEmptyMeta) {
    // Test when rowset meta has no segment rows (old version data)
    // In this case, it should try to load segments from segment footer
    auto tablet_schema = std::make_shared<TabletSchema>();
    create_tablet_schema(tablet_schema);

    auto rowset_meta = std::make_shared<RowsetMeta>();
    init_rs_meta(rowset_meta, 1, 1);
    rowset_meta->set_num_segments(2);
    // segment_rows is empty (simulating old version data)

    auto rowset = std::make_shared<BetaRowset>(tablet_schema, rowset_meta, "");

    // Use sync point to verify code path
    auto sp = SyncPoint::get_instance();
    bool used_meta_path = false;
    bool used_footer_path = false;

    sp->set_call_back("BetaRowset::get_segment_num_rows:use_segment_rows_from_meta",
                      [&](auto&& args) { used_meta_path = true; });

    sp->set_call_back("BetaRowset::get_segment_num_rows:load_from_segment_footer",
                      [&](auto&& args) { used_footer_path = true; });

    sp->enable_processing();

    std::vector<uint32_t> segment_rows;
    Status st = rowset->get_segment_num_rows(&segment_rows, false, &_stats);

    // Since we don't have actual segment files, it will fail to load segments
    // But the important thing is to verify it tried to load from footer
    ASSERT_TRUE(used_footer_path);
    ASSERT_FALSE(used_meta_path);

    sp->clear_all_call_backs();
    sp->disable_processing();
    sp->clear_trace();
}

TEST_F(BetaRowsetTest, GetSegmentNumRowsCorruptedMeta) {
    // Test when segment_rows size doesn't match segment count
    // This simulates a corrupted rowset meta
    auto tablet_schema = std::make_shared<TabletSchema>();
    create_tablet_schema(tablet_schema);

    auto rowset_meta = std::make_shared<RowsetMeta>();
    init_rs_meta(rowset_meta, 1, 1);
    rowset_meta->set_num_segments(3);

    // Set segment rows with wrong size (should be 3 but only has 2)
    std::vector<uint32_t> wrong_segment_rows = {100, 200};
    rowset_meta->set_num_segment_rows(wrong_segment_rows);

    auto rowset = std::make_shared<BetaRowset>(tablet_schema, rowset_meta, "");

    // Use sync point to verify code path
    auto sp = SyncPoint::get_instance();
    bool used_meta_path = false;
    bool used_footer_path = false;

    sp->set_call_back("BetaRowset::get_segment_num_rows:use_segment_rows_from_meta",
                      [&](auto&& args) { used_meta_path = true; });

    sp->set_call_back("BetaRowset::get_segment_num_rows:load_from_segment_footer",
                      [&](auto&& args) { used_footer_path = true; });

    sp->enable_processing();

    std::vector<uint32_t> segment_rows;
    Status st = rowset->get_segment_num_rows(&segment_rows, false, &_stats);

    // When segment_rows size doesn't match, it should fall back to loading from footer
    ASSERT_FALSE(used_meta_path);
    ASSERT_TRUE(used_footer_path);

    sp->clear_all_call_backs();
    sp->disable_processing();
    sp->clear_trace();
}

TEST_F(BetaRowsetTest, GetSegmentNumRowsRetryAfterFailure) {
    // A failed load must not be cached. The rowset lives as long as its tablet version, so a
    // cached transient error (e.g. S3 SlowDown) would fail every later caller, such as compaction.
    auto tablet_schema = std::make_shared<TabletSchema>();
    create_tablet_schema(tablet_schema);

    auto rowset_meta = std::make_shared<RowsetMeta>();
    init_rs_meta(rowset_meta, 1, 1);
    // Use a dedicated rowset id so that no segment of other tests can be hit in segment cache.
    RowsetId rowset_id;
    rowset_id.init(540099);
    rowset_meta->set_rowset_id(rowset_id);
    rowset_meta->set_num_segments(2);
    // No segment rows in meta and no segment files, so loading from segment footer fails.

    auto rowset = std::make_shared<BetaRowset>(tablet_schema, rowset_meta, "");

    auto sp = SyncPoint::get_instance();
    int meta_path_count = 0;
    int footer_path_count = 0;

    sp->set_call_back("BetaRowset::get_segment_num_rows:use_segment_rows_from_meta",
                      [&](auto&& args) { meta_path_count++; });

    sp->set_call_back("BetaRowset::get_segment_num_rows:load_from_segment_footer",
                      [&](auto&& args) { footer_path_count++; });

    sp->enable_processing();

    std::vector<uint32_t> segment_rows;
    Status st = rowset->get_segment_num_rows(&segment_rows, false, &_stats);
    ASSERT_FALSE(st.ok());
    ASSERT_EQ(footer_path_count, 1);

    // The failure is not cached, so the next call loads again and succeeds.
    rowset_meta->set_num_segment_rows({100, 200});
    st = rowset->get_segment_num_rows(&segment_rows, false, &_stats);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(segment_rows, (std::vector<uint32_t> {100, 200}));
    ASSERT_EQ(meta_path_count, 1);

    // The success is cached, so the following call does not load again.
    std::vector<uint32_t> segment_rows_2;
    st = rowset->get_segment_num_rows(&segment_rows_2, false, &_stats);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(segment_rows_2, (std::vector<uint32_t> {100, 200}));
    ASSERT_EQ(meta_path_count, 1);
    ASSERT_EQ(footer_path_count, 1);

    sp->clear_all_call_backs();
    sp->disable_processing();
    sp->clear_trace();
}

TEST_F(BetaRowsetTest, GetNumSegmentRowsAPI) {
    // Test the simple get_num_segment_rows API (without loading)
    auto tablet_schema = std::make_shared<TabletSchema>();
    create_tablet_schema(tablet_schema);

    auto rowset_meta = std::make_shared<RowsetMeta>();
    init_rs_meta(rowset_meta, 1, 1);
    rowset_meta->set_num_segments(3);

    std::vector<uint32_t> expected_segment_rows = {100, 200, 300};
    rowset_meta->set_num_segment_rows(expected_segment_rows);

    auto rowset = std::make_shared<BetaRowset>(tablet_schema, rowset_meta, "");

    std::vector<uint32_t> segment_rows;
    rowset->get_num_segment_rows(&segment_rows);
    ASSERT_EQ(segment_rows.size(), 3);
    ASSERT_EQ(segment_rows[0], 100);
    ASSERT_EQ(segment_rows[1], 200);
    ASSERT_EQ(segment_rows[2], 300);
}

// A merge-on-write SELECT ... ORDER BY k1 LIMIT n is read by a storage TopN, which merges the
// overlapping segments of a rowset by the ORDER BY prefix (read_orderby_key_columns). Rows equal
// on k1 are different keys, so the merge keeps all of them although the table is UNIQUE_KEYS:
// one load wrote (k1 = i, k2 = i) into the first segment and (k1 = i, k2 = i + 20) into the
// second, and all 40 rows come back in k1 order.
TEST_F(BetaRowsetTest, OrderByKeyPrefixMergeKeepsRowsEqualOnThePrefix) {
    auto tablet_schema = std::make_shared<TabletSchema>();
    create_unique_tablet_schema(tablet_schema);
    std::vector<std::vector<std::array<int32_t, 3>>> segments(2);
    std::vector<std::pair<int32_t, int32_t>> expected_keys;
    for (int32_t i = 0; i < 20; ++i) {
        segments[0].push_back({i, i, i});
        segments[1].push_back({i, i + 20, i + 20});
        expected_keys.emplace_back(i, i);
        expected_keys.emplace_back(i, i + 20);
    }
    EngineOptions options;
    StorageEngine engine(options);
    RowsetSharedPtr rowset;
    ASSERT_NO_FATAL_FAILURE(
            write_overlapping_rowset(engine, tablet_schema, 10100, segments, &rowset));
    ASSERT_TRUE(rowset->is_segments_overlapping());

    std::vector<uint32_t> orderby_key_columns {0};
    auto context = unique_merge_context(*tablet_schema, &orderby_key_columns);
    auto keys = read_keys(rowset.get(), context);
    EXPECT_TRUE(std::ranges::is_sorted(keys, {}, &std::pair<int32_t, int32_t>::first));
    // Rows equal on k1 come out in either order.
    std::ranges::sort(keys);
    EXPECT_EQ(keys, expected_keys);
}

// A merge by the whole key keeps one row of each key that both overlapping segments of a
// UNIQUE_KEYS rowset hold.
TEST_F(BetaRowsetTest, WholeKeyMergeKeepsOneRowPerKey) {
    auto tablet_schema = std::make_shared<TabletSchema>();
    create_unique_tablet_schema(tablet_schema);
    std::vector<std::vector<std::array<int32_t, 3>>> segments(2);
    std::vector<std::pair<int32_t, int32_t>> expected_keys;
    for (int32_t i = 0; i < 20; ++i) {
        segments[0].push_back({i, i, i});
        segments[1].push_back({i, i, i + 20});
        expected_keys.emplace_back(i, i);
    }
    EngineOptions options;
    StorageEngine engine(options);
    RowsetSharedPtr rowset;
    ASSERT_NO_FATAL_FAILURE(
            write_overlapping_rowset(engine, tablet_schema, 10101, segments, &rowset));
    ASSERT_TRUE(rowset->is_segments_overlapping());

    auto context = unique_merge_context(*tablet_schema, nullptr);
    EXPECT_EQ(read_keys(rowset.get(), context), expected_keys);
}

} // namespace doris
