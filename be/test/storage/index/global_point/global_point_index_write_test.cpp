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

#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/data_dir.h"
#include "storage/index/global_point/global_point_index_reader.h"
#include "storage/rowset/beta_rowset_writer.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

namespace {

constexpr std::string_view kTestDir = "./ut_dir/global_point_index_write_test";
constexpr int32_t kKeyUid = 1;
constexpr int32_t kEvUid = 2;
constexpr int32_t kNameUid = 3;
int64_t g_rowset_id = 30000;

void add_column(TabletSchemaPB* schema_pb, int32_t uid, const std::string& name,
                const std::string& type, bool is_key, int32_t length) {
    ColumnPB* col = schema_pb->add_column();
    col->set_unique_id(uid);
    col->set_name(name);
    col->set_type(type);
    col->set_is_key(is_key);
    col->set_length(length);
    col->set_is_nullable(!is_key);
}

void add_global_point_index(TabletSchemaPB* schema_pb, int64_t index_id, int32_t uid) {
    TabletIndexPB* index_pb = schema_pb->add_index();
    index_pb->set_index_id(index_id);
    index_pb->set_index_name("idx_" + std::to_string(uid));
    index_pb->set_index_type(IndexType::GLOBAL_POINT);
    index_pb->add_col_unique_id(uid);
    (*index_pb->mutable_properties())["fpp"] = "0.01";
}

} // namespace

class GlobalPointIndexWriteTest : public testing::Test {
protected:
    void SetUp() override {
        char buffer[1024];
        ASSERT_NE(getcwd(buffer, sizeof(buffer)), nullptr);
        _absolute_dir = std::string(buffer) + "/" + std::string(kTestDir);
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(_absolute_dir).ok());

        auto engine = std::make_unique<StorageEngine>(EngineOptions {});
        _engine = engine.get();
        _data_dir = std::make_unique<DataDir>(*_engine, _absolute_dir);
        static_cast<void>(_data_dir->update_capacity());
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));

        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);
        schema_pb.set_num_short_key_columns(1);
        add_column(&schema_pb, kKeyUid, "id", "BIGINT", true, 8);
        add_column(&schema_pb, kEvUid, "ev", "INT", false, 4);
        add_column(&schema_pb, kNameUid, "name", "VARCHAR", false, 64);
        add_global_point_index(&schema_pb, 1001, kEvUid);
        add_global_point_index(&schema_pb, 1002, kNameUid);
        _schema = std::make_shared<TabletSchema>();
        _schema->init_from_pb(schema_pb);

        TabletMetaSharedPtr tablet_meta(new TabletMeta(_schema));
        _tablet = std::make_shared<Tablet>(*_engine, tablet_meta, _data_dir.get());
        ASSERT_TRUE(_tablet->init().ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());
    }

    void TearDown() override {
        _tablet.reset();
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        _engine = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    RowsetWriterContext make_context() {
        RowsetWriterContext context;
        RowsetId rowset_id;
        rowset_id.init(g_rowset_id++);
        context.rowset_id = rowset_id;
        context.rowset_type = BETA_ROWSET;
        context.data_dir = _data_dir.get();
        context.rowset_state = VISIBLE;
        context.tablet_schema = _schema;
        context.tablet_path = _tablet->tablet_path();
        context.version = Version(g_rowset_id, g_rowset_id);
        // Several segments per rowset: the bloom must still cover all of them.
        context.max_rows_per_segment = 10;
        return context;
    }

    // Writes rows id = [0, rows), ev = id * 7, name = "event-<id>"; every third ev is NULL.
    RowsetSharedPtr write_rowset(int rows) {
        auto res = RowsetFactory::create_rowset_writer(*_engine, make_context(), false);
        EXPECT_TRUE(res.has_value());
        auto writer = std::move(res).value();
        Block block = _schema->create_storage_block();
        auto columns = std::move(block).mutate_columns();
        for (int i = 0; i < rows; ++i) {
            int64_t id = i;
            columns[0]->insert_data(reinterpret_cast<const char*>(&id), sizeof(id));
            if (i % 3 == 2) {
                // A null pointer inserts NULL into a nullable column.
                columns[1]->insert_data(nullptr, 0);
            } else {
                int32_t ev = i * 7;
                columns[1]->insert_data(reinterpret_cast<const char*>(&ev), sizeof(ev));
            }
            std::string name = "event-" + std::to_string(i);
            columns[2]->insert_data(name.data(), name.size());
        }
        block.set_columns(std::move(columns));
        EXPECT_TRUE(writer->add_block(&block).ok());
        EXPECT_TRUE(writer->flush().ok());
        RowsetSharedPtr rowset;
        EXPECT_TRUE(writer->build(rowset).ok());
        return rowset;
    }

    std::unique_ptr<segment_v2::BloomFilter> load_bloom(const RowsetSharedPtr& rowset,
                                                        const ColumnPointIndexPB& desc) {
        auto path = rowset->global_point_index_path(desc.column_unique_id());
        EXPECT_TRUE(path.has_value());
        std::unique_ptr<segment_v2::BloomFilter> bloom;
        int64_t bytes_read = 0;
        EXPECT_TRUE(segment_v2::try_load_global_point_index(io::global_local_filesystem(),
                                                            path.value(), desc, nullptr, &bloom,
                                                            &bytes_read)
                            .ok());
        return bloom;
    }

    StorageEngine* _engine = nullptr;
    std::unique_ptr<DataDir> _data_dir;
    TabletSchemaSPtr _schema;
    TabletSharedPtr _tablet;
    std::string _absolute_dir;
};

TEST_F(GlobalPointIndexWriteTest, RowsetBloomCoversEverySegment) {
    constexpr int kRows = 35;
    RowsetSharedPtr rowset = write_rowset(kRows);
    ASSERT_NE(rowset, nullptr);
    ASSERT_GT(rowset->num_segments(), 1);

    const auto& descs = rowset->rowset_meta()->point_query_indexes();
    ASSERT_EQ(descs.size(), 2);
    for (const auto& desc : descs) {
        auto bloom = load_bloom(rowset, desc);
        ASSERT_NE(bloom, nullptr) << "column " << desc.column_unique_id();
        if (desc.column_unique_id() == kEvUid) {
            EXPECT_EQ(desc.index_id(), 1001);
            // NULLs are not counted.
            EXPECT_EQ(desc.total_rows(), kRows - kRows / 3);
            EXPECT_TRUE(bloom->has_null());
            for (int i = 0; i < kRows; ++i) {
                if (i % 3 != 2) {
                    int32_t ev = i * 7;
                    EXPECT_TRUE(bloom->test_bytes(reinterpret_cast<const char*>(&ev), sizeof(ev)))
                            << "ev " << ev;
                }
            }
        } else {
            ASSERT_EQ(desc.column_unique_id(), kNameUid);
            EXPECT_EQ(desc.total_rows(), kRows);
            for (int i = 0; i < kRows; ++i) {
                std::string name = "event-" + std::to_string(i);
                EXPECT_TRUE(bloom->test_bytes(name.data(), name.size())) << name;
            }
        }
    }
}

TEST_F(GlobalPointIndexWriteTest, TransientWriterBuildsNoBloom) {
    RowsetWriterContext context = make_context();
    context.is_transient_rowset_writer = true;
    BetaRowsetWriter writer(*_engine);
    ASSERT_TRUE(writer.init(context).ok());
    EXPECT_TRUE(writer.context().global_point_index_builders.empty());

    BetaRowsetWriter normal_writer(*_engine);
    ASSERT_TRUE(normal_writer.init(make_context()).ok());
    EXPECT_EQ(normal_writer.context().global_point_index_builders.size(), 2);
}

// Appending partial-update segments keeps the descriptors of key columns only.
TEST_F(GlobalPointIndexWriteTest, MergeRowsetMetaDropsNonKeyDescriptors) {
    TabletSchemaPB schema_pb;
    _schema->to_schema_pb(&schema_pb);
    add_global_point_index(&schema_pb, 1003, kKeyUid);
    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(schema_pb);

    RowsetMeta host;
    host.set_tablet_schema(schema);
    for (int32_t uid : {kKeyUid, kEvUid, kNameUid}) {
        ColumnPointIndexPB desc;
        desc.set_column_unique_id(uid);
        host.add_point_query_index(desc);
    }
    RowsetMeta appended;
    appended.set_tablet_schema(schema);
    appended.set_num_segments(1);

    host.merge_rowset_meta(appended);
    ASSERT_EQ(host.point_query_indexes().size(), 1);
    EXPECT_EQ(host.point_query_indexes().Get(0).column_unique_id(), kKeyUid);
}

} // namespace doris
