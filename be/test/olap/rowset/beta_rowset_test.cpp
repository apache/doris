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
#include "olap/rowset/beta_rowset.h"

#include <aws/core/auth/AWSAuthSigner.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/Outcome.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/model/GetObjectResult.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <gen_cpp/olap_common.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <stdint.h>
#include <unistd.h>

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "gen_cpp/olap_file.pb.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "io/fs/s3_file_system.h"
#include "io/fs/s3_obj_storage_client.h"
#include "json2pb/json_to_pb.h"
#include "olap/data_dir.h"
#include "olap/olap_common.h"
#include "olap/options.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/storage_engine.h"
#include "olap/storage_policy.h"
#include "olap/tablet_schema.h"
#include "runtime/exec_env.h"
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

        client.reset(new io::S3ObjStorageClient(std::move(s3_client)));

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
        client.reset(new io::S3ObjStorageClient(
                std::make_shared<Aws::S3::S3Client>(S3ClientMockGetError())));

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
        client.reset(new io::S3ObjStorageClient(
                std::make_shared<Aws::S3::S3Client>(S3ClientMockGetErrorData())));

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

} // namespace doris
