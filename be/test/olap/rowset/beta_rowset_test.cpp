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

} // namespace doris
