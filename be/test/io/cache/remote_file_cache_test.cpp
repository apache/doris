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

#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>

#include <memory>
#include <string>
#include <vector>

#include "gen_cpp/olap_file.pb.h"
#include "gtest/gtest.h"
#include "io/fs/s3_common.h"
#include "io/fs/s3_file_system.h"
#include "olap/comparison_predicate.h"
#include "olap/data_dir.h"
#include "olap/options.h"
#include "olap/row_block.h"
#include "olap/row_cursor.h"
#include "olap/rowset/beta_rowset_reader.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_reader_context.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/rowset/segment_v2/segment_writer.h"
#include "olap/storage_engine.h"
#include "olap/tablet_schema.h"
#include "olap/tablet_schema_helper.h"
#include "olap/utils.h"
#include "runtime/exec_env.h"
#include "runtime/mem_pool.h"
#include "runtime/memory/mem_tracker.h"
#include "util/file_utils.h"
#include "util/slice.h"

namespace doris {

using ValueGenerator = std::function<void(size_t rid, int cid, int block_id, RowCursorCell& cell)>;
// 0,  1,  2,  3
// 10, 11, 12, 13
// 20, 21, 22, 23
static void DefaultIntGenerator(size_t rid, int cid, int block_id, RowCursorCell& cell) {
    cell.set_not_null();
    *(int*)cell.mutable_cell_ptr() = rid * 10 + cid;
}

static StorageEngine* k_engine = nullptr;
static std::string kSegmentDir = "./ut_dir/remote_file_cache_test";
static int64_t tablet_id = 0;
static RowsetId rowset_id;
static io::ResourceId resource_id = "test_resourse_id";

class RemoteFileCacheTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        if (FileUtils::check_exist(kSegmentDir)) {
            EXPECT_TRUE(FileUtils::remove_all(kSegmentDir).ok());
        }
        EXPECT_TRUE(FileUtils::create_dir(kSegmentDir).ok());

        doris::ExecEnv::GetInstance()->init_download_cache_required_components();

        doris::EngineOptions options;
        k_engine = new StorageEngine(options);
        StorageEngine::_s_instance = k_engine;
    }

    static void TearDownTestSuite() {
        if (FileUtils::check_exist(kSegmentDir)) {
            EXPECT_TRUE(FileUtils::remove_all(kSegmentDir).ok());
        }
        if (k_engine != nullptr) {
            k_engine->stop();
            delete k_engine;
            k_engine = nullptr;
        }
        config::file_cache_type = "";
    }

    TabletSchemaSPtr create_schema(const std::vector<TabletColumn>& columns,
                                   KeysType keys_type = DUP_KEYS, int num_custom_key_columns = -1) {
        TabletSchemaSPtr res = std::make_shared<TabletSchema>();

        for (auto& col : columns) {
            res->append_column(col);
        }
        res->_num_short_key_columns =
                num_custom_key_columns != -1 ? num_custom_key_columns : res->num_key_columns();
        res->_keys_type = keys_type;
        return res;
    }

    void build_segment(SegmentWriterOptions opts, TabletSchemaSPtr build_schema,
                       TabletSchemaSPtr query_schema, size_t nrows, const ValueGenerator& generator,
                       std::shared_ptr<Segment>* res) {
        std::string filename = fmt::format("{}_0.dat", rowset_id.to_string());
        std::string path = fmt::format("{}/{}", kSegmentDir, filename);
        auto fs = io::global_local_filesystem();

        io::FileWriterPtr file_writer;
        Status st = fs->create_file(path, &file_writer);
        EXPECT_TRUE(st.ok());
        DataDir data_dir(kSegmentDir);
        data_dir.init();
        SegmentWriter writer(file_writer.get(), 0, build_schema, &data_dir, INT32_MAX, opts);
        st = writer.init();
        EXPECT_TRUE(st.ok());

        RowCursor row;
        auto olap_st = row.init(build_schema);
        EXPECT_EQ(Status::OK(), olap_st);

        for (size_t rid = 0; rid < nrows; ++rid) {
            for (int cid = 0; cid < build_schema->num_columns(); ++cid) {
                int row_block_id = rid / opts.num_rows_per_block;
                RowCursorCell cell = row.cell(cid);
                generator(rid, cid, row_block_id, cell);
            }
            EXPECT_TRUE(writer.append_row(row).ok());
        }

        uint64_t file_size, index_size;
        st = writer.finalize(&file_size, &index_size);
        EXPECT_TRUE(st.ok());
        EXPECT_TRUE(file_writer->close().ok());

        EXPECT_EQ("", writer.min_encoded_key().to_string());
        EXPECT_EQ("", writer.max_encoded_key().to_string());

        st = segment_v2::Segment::open(fs, path, "", 0, query_schema, res);
        EXPECT_TRUE(st.ok());
        EXPECT_EQ(nrows, (*res)->num_rows());
    }

    void test_remote_file_cache(std::string file_cache_type, int max_sub_cache_file_size) {
        TabletSchemaSPtr tablet_schema = create_schema(
                {create_int_key(1), create_int_key(2), create_int_value(3), create_int_value(4)});
        SegmentWriterOptions opts;
        opts.num_rows_per_block = 10;

        std::shared_ptr<segment_v2::Segment> segment;
        build_segment(opts, tablet_schema, tablet_schema, 4096, DefaultIntGenerator, &segment);

        config::file_cache_type = file_cache_type;
        config::max_sub_cache_file_size = max_sub_cache_file_size;
        RowsetMetaSharedPtr rowset_meta = std::make_shared<RowsetMeta>();
        BetaRowset rowset(tablet_schema, kSegmentDir, rowset_meta);

        // just use to create s3 filesystem, otherwise won't use cache
        S3Conf s3_conf;
        std::shared_ptr<io::S3FileSystem> fs =
                std::make_shared<io::S3FileSystem>(std::move(s3_conf), resource_id);
        rowset.rowset_meta()->set_resource_id(resource_id);
        rowset.rowset_meta()->set_num_segments(1);
        rowset.rowset_meta()->set_fs(fs);
        rowset.rowset_meta()->set_tablet_id(tablet_id);
        rowset.rowset_meta()->set_rowset_id(rowset_id);

        std::vector<segment_v2::SegmentSharedPtr> segments;
        Status st = rowset.load_segments(&segments);
        ASSERT_TRUE(st.ok());
    }
};

TEST_F(RemoteFileCacheTest, wholefilecache) {
    test_remote_file_cache("whole_file_cache", 0);
}

TEST_F(RemoteFileCacheTest, subfilecache) {
    test_remote_file_cache("sub_file_cache", 1024);
}

} // namespace doris
