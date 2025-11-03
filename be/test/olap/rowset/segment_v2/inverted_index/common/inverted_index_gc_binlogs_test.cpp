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

#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/storage_engine.h"

namespace doris {

using namespace doris::vectorized;

constexpr static uint32_t MAX_PATH_LEN = 1024;
constexpr static std::string_view dest_dir = "./ut_dir/inverted_index_test";
constexpr static std::string_view tmp_dir = "./ut_dir/tmp";
static int64_t inc_id = 0;

class IndexGcBinglogsTest : public ::testing::Test {
protected:
    void SetUp() override {
        // absolute dir
        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        _curreent_dir = std::string(buffer);
        _absolute_dir = _curreent_dir + std::string(dest_dir);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(_absolute_dir).ok());

        // tmp dir
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(tmp_dir).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(std::string(tmp_dir), 1024000000);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        EXPECT_TRUE(tmp_file_dirs->init().ok());
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));

        // storage engine
        doris::EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        _engine_ref = engine.get();
        _data_dir = std::make_unique<DataDir>(*_engine_ref, _absolute_dir);
        static_cast<void>(_data_dir->update_capacity());
        EXPECT_TRUE(_data_dir->init(true).ok());
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));

        // tablet_schema
        TabletSchemaPB schema_pb;
        _schema_pb.set_keys_type(KeysType::DUP_KEYS);
        _schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);

        construct_column(_schema_pb.add_column(), _schema_pb.add_index(), 10000, "key_index", 0,
                         "INT", "key");
        construct_column(_schema_pb.add_column(), _schema_pb.add_index(), 10001, "v1_index", 1,
                         "STRING", "v1");
    }
    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
        _engine_ref = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    void construct_column(ColumnPB* column_pb, TabletIndexPB* tablet_index, int64_t index_id,
                          const std::string& index_name, int32_t col_unique_id,
                          const std::string& column_type, const std::string& column_name,
                          bool parser = false) {
        column_pb->set_unique_id(col_unique_id);
        column_pb->set_name(column_name);
        column_pb->set_type(column_type);
        column_pb->set_is_key(false);
        column_pb->set_is_nullable(true);
        tablet_index->set_index_id(index_id);
        tablet_index->set_index_name(index_name);
        tablet_index->set_index_type(IndexType::INVERTED);
        tablet_index->add_col_unique_id(col_unique_id);
        if (parser) {
            auto* properties = tablet_index->mutable_properties();
            (*properties)[INVERTED_INDEX_PARSER_KEY] = INVERTED_INDEX_PARSER_UNICODE;
        }
    }

    RowsetWriterContext rowset_writer_context() {
        RowsetWriterContext context;
        RowsetId rowset_id;
        rowset_id.init(inc_id);
        context.rowset_id = rowset_id;
        context.rowset_type = BETA_ROWSET;
        context.data_dir = _data_dir.get();
        context.rowset_state = VISIBLE;
        context.tablet_schema = _tablet_schema;
        context.tablet_path = _tablet->tablet_path();
        context.version = Version(inc_id, inc_id);
        context.max_rows_per_segment = 200;
        inc_id++;
        return context;
    }

    IndexGcBinglogsTest() = default;
    ~IndexGcBinglogsTest() override = default;

private:
    TabletSchemaSPtr _tablet_schema = nullptr;
    StorageEngine* _engine_ref = nullptr;
    std::unique_ptr<DataDir> _data_dir = nullptr;
    TabletSharedPtr _tablet = nullptr;
    std::string _absolute_dir;
    std::string _curreent_dir;
    TabletSchemaPB _schema_pb;
};

TEST_F(IndexGcBinglogsTest, gc_binlogs_test) {
    auto test = [&](InvertedIndexStorageFormatPB format) {
        TabletSchemaPB schema_pb = _schema_pb;
        _schema_pb.set_inverted_index_storage_format(format);
        _tablet_schema.reset(new TabletSchema);
        _tablet_schema->init_from_pb(schema_pb);
        TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));

        tablet_meta->set_tablet_uid(TabletUid(20, 20));
        tablet_meta.get()->_tablet_id = 200;
        _tablet.reset(new Tablet(*_engine_ref, tablet_meta, _data_dir.get()));
        EXPECT_TRUE(_tablet->init().ok());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

        RowsetSharedPtr rowset;
        const auto& res =
                RowsetFactory::create_rowset_writer(*_engine_ref, rowset_writer_context(), false);
        EXPECT_TRUE(res.has_value()) << res.error();
        const auto& rowset_writer = res.value();

        Block block = _tablet_schema->create_block();
        auto columns = block.mutate_columns();

        vectorized::Field key = vectorized::Field::create_field<TYPE_INT>(10);
        vectorized::Field v1 = vectorized::Field::create_field<TYPE_STRING>("v1");
        columns[0]->insert(key);
        columns[1]->insert(v1);

        EXPECT_TRUE(rowset_writer->add_block(&block).ok());
        EXPECT_TRUE(rowset_writer->flush().ok());
        EXPECT_TRUE(rowset_writer->build(rowset).ok());
        EXPECT_TRUE(rowset->add_to_binlog().ok());
        EXPECT_TRUE(_tablet->add_rowset(rowset).ok());
        EXPECT_TRUE(RowsetMetaManager::save(_data_dir->get_meta(), _tablet->tablet_uid(),
                                            rowset->rowset_id(),
                                            rowset->rowset_meta()->get_rowset_pb(), true)
                            .ok());
        _tablet->save_meta();
        _tablet->gc_binlogs(0);
    };
    test(InvertedIndexStorageFormatPB::V1);
    test(InvertedIndexStorageFormatPB::V2);
}

} // namespace doris
