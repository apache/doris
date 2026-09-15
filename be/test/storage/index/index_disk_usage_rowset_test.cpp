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

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/index/index_disk_usage.h"
#include "storage/index/index_writer.h"
#include "storage/olap_common.h"
#include "storage/options.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {

class IndexDiskUsageRowsetTest : public ::testing::Test {
protected:
    const std::string kTestDir = "./ut_dir/index_disk_usage_rowset_test";

    void SetUp() override {
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(kTestDir).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(kTestDir, 1024000000);
        auto tmp_file_dirs = std::make_unique<TmpFileDirs>(paths);
        ASSERT_TRUE(tmp_file_dirs->init().ok());
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));
        EngineOptions options;
        options.store_paths = paths;
        auto engine = std::make_unique<StorageEngine>(options);
        _engine = engine.get();
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
    }

    void TearDown() override {
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    // A duplicate-key table whose value column has an inverted index in the V2 format.
    static TabletSchemaSPtr create_schema() {
        auto schema = std::make_shared<TabletSchema>();
        schema->_keys_type = KeysType::DUP_KEYS;
        schema->_inverted_index_storage_format = InvertedIndexStorageFormatPB::V2;

        TabletColumn key;
        key.set_type(FieldType::OLAP_FIELD_TYPE_INT);
        key.set_unique_id(1);
        key.set_name("k1");
        key.set_is_key(true);
        key.set_index_length(4);
        schema->append_column(key);

        TabletColumn value;
        value.set_type(FieldType::OLAP_FIELD_TYPE_INT);
        value.set_unique_id(2);
        value.set_name("v1");
        value.set_is_key(false);
        schema->append_column(value);

        TabletIndexPB index_pb;
        index_pb.set_index_type(IndexType::INVERTED);
        index_pb.set_index_id(10001);
        index_pb.set_index_name("idx_v1");
        index_pb.add_col_unique_id(2);
        TabletIndex index;
        index.init_from_pb(index_pb);
        schema->append_index(std::move(index));
        return schema;
    }

    Status write_rowset(const TabletSchemaSPtr& schema, int32_t num_rows, RowsetSharedPtr* rowset) {
        RowsetWriterContext context;
        context.rowset_id.init(20001);
        context.tablet_id = 1001;
        context.tablet_schema_hash = 1111;
        context.partition_id = 10;
        context.rowset_type = BETA_ROWSET;
        context.tablet_path = kTestDir;
        context.rowset_state = VISIBLE;
        context.tablet_schema = schema;
        context.version = Version(2, 2);
        auto writer = DORIS_TRY(RowsetFactory::create_rowset_writer(*_engine, context, false));

        Block block = schema->create_storage_block();
        auto columns = std::move(block).mutate_columns();
        for (int32_t i = 0; i < num_rows; ++i) {
            columns[0]->insert_data(reinterpret_cast<const char*>(&i), sizeof(i));
            columns[1]->insert_data(reinterpret_cast<const char*>(&i), sizeof(i));
        }
        block = schema->create_storage_block();
        block.set_columns(std::move(columns));
        RETURN_IF_ERROR(writer->add_block(&block));
        RETURN_IF_ERROR(writer->flush());
        return writer->build(*rowset);
    }

    StorageEngine* _engine = nullptr;
};

TEST_F(IndexDiskUsageRowsetTest, RowCountFallsBackToSegmentFooters) {
    RowsetSharedPtr rowset;
    const Status write_status = write_rowset(create_schema(), 8, &rowset);
    ASSERT_TRUE(write_status.ok()) << write_status;
    // Rowsets written before per-segment row counts were persisted have none in their meta.
    rowset->rowset_meta()->set_num_segment_rows({});

    std::vector<IndexDiskUsageRow> rows;
    const Status st = collect_rowset_index_disk_usage(rowset, IndexDiskUsageOptions {},
                                                      /*tablet_id=*/1001, &rows);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_FALSE(rows.empty());
    for (const auto& row : rows) {
        EXPECT_EQ(8, row.row_count) << "index " << row.record.index_id;
    }
}

} // namespace doris::segment_v2
