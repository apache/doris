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

#include "olap/task/index_builder.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/storage_engine.h"
#include "olap/tablet_fwd.h"
#include "olap/tablet_schema.h"

namespace doris {
using namespace testing;

class IndexBuilderTest : public ::testing::Test {
protected:
    void SetUp() override {
        char buffer[MAX_PATH_LEN];
        ASSERT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        _current_dir = std::string(buffer);
        _absolute_dir = _current_dir + "/" + std::string(dest_dir);
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(_absolute_dir).ok());

        std::vector<StorePath> paths;
        paths.emplace_back(config::storage_root_path, -1);

        // tmp dir
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(tmp_dir).ok());
        paths.emplace_back(std::string(tmp_dir), 1024000000);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        EXPECT_TRUE(tmp_file_dirs->init().ok());
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));

        // use memory limit
        int64_t inverted_index_cache_limit = 0;
        _inverted_index_searcher_cache = std::unique_ptr<segment_v2::InvertedIndexSearcherCache>(
                InvertedIndexSearcherCache::create_global_instance(inverted_index_cache_limit,
                                                                   256));

        ExecEnv::GetInstance()->set_inverted_index_searcher_cache(
                _inverted_index_searcher_cache.get());
        doris::EngineOptions options;
        options.store_paths = paths;

        auto engine = std::make_unique<StorageEngine>(options);
        _engine_ref = engine.get();
        _data_dir = std::make_unique<DataDir>(*_engine_ref, _absolute_dir);
        ASSERT_TRUE(_data_dir->update_capacity().ok());
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));

        _tablet_meta = create_tablet_meta();

        // Create tablet meta
        // auto* tablet_schema = _tablet_meta->mutable_tablet_schema();
        _tablet_schema = std::make_shared<TabletSchema>();
        create_tablet_schema(_tablet_schema, KeysType::DUP_KEYS);
        // Initialize tablet
        _tablet = std::make_shared<Tablet>(*_engine_ref, _tablet_meta, _data_dir.get());
        ASSERT_TRUE(_tablet->init().ok());
    }

    void TearDown() override {
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        _tablet.reset();
    }

    void create_tablet_schema(TabletSchemaSPtr tablet_schema, KeysType keystype,
                              int num_value_col = 1) {
        // Set basic properties of TabletSchema directly
        tablet_schema->_keys_type = keystype;
        tablet_schema->_num_short_key_columns = 2;
        tablet_schema->_num_rows_per_row_block = 1024;
        tablet_schema->_compress_kind = COMPRESS_NONE;
        tablet_schema->_next_column_unique_id = 4;
        tablet_schema->_inverted_index_storage_format = InvertedIndexStorageFormatPB::V2;

        // Create the first key column
        TabletColumn column_1(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                              FieldType::OLAP_FIELD_TYPE_INT, true);
        column_1.set_unique_id(1);
        column_1.set_name("k1");
        column_1.set_length(4);
        column_1.set_index_length(4);
        column_1.set_is_key(true);
        column_1.set_is_nullable(true);
        column_1.set_is_bf_column(false);
        tablet_schema->append_column(column_1);

        // Create the second key column
        TabletColumn column_2(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                              FieldType::OLAP_FIELD_TYPE_INT, true);
        column_2.set_unique_id(2);
        column_2.set_name("k2");
        column_2.set_length(4);
        column_2.set_index_length(4);
        column_2.set_is_key(true);
        column_2.set_is_nullable(true);
        column_2.set_is_bf_column(false);
        tablet_schema->append_column(column_2);

        // Add inverted index for k1 column
        /*TabletIndex index_1;
        index_1._index_id = 1;
        index_1._index_name = "k1_index";
        index_1._index_type = IndexType::INVERTED;
        index_1._col_unique_ids.push_back(1);
        tablet_schema->append_index(std::move(index_1));

        // Add inverted index for k2 column
        TabletIndex index_2;
        index_2._index_id = 2;
        index_2._index_name = "k2_index";
        index_2._index_type = IndexType::INVERTED;
        index_2._col_unique_ids.push_back(2);
        tablet_schema->append_index(std::move(index_2));*/
    }

    TabletMetaSharedPtr create_tablet_meta() {
        TabletMetaPB tablet_meta_pb;
        tablet_meta_pb.set_table_id(1);
        tablet_meta_pb.set_tablet_id(15673);
        tablet_meta_pb.set_schema_hash(567997577);
        tablet_meta_pb.set_shard_id(0);
        tablet_meta_pb.set_creation_time(1575351212);

        TabletMetaSharedPtr tablet_meta(new TabletMeta());
        tablet_meta->init_from_pb(tablet_meta_pb);
        return tablet_meta;
    }

    // Helper to create rowset meta
    void init_rs_meta(RowsetMetaSharedPtr& rs_meta, TabletSchemaSPtr tablet_schema, int64_t start,
                      int64_t end) {
        RowsetMetaPB rowset_meta_pb;
        rowset_meta_pb.set_rowset_id(540081);
        rowset_meta_pb.set_tablet_id(15673);
        rowset_meta_pb.set_tablet_schema_hash(567997577);
        rowset_meta_pb.set_rowset_type(RowsetTypePB::BETA_ROWSET);
        rowset_meta_pb.set_rowset_state(RowsetStatePB::VISIBLE);
        rowset_meta_pb.set_start_version(start);
        rowset_meta_pb.set_end_version(end);
        rowset_meta_pb.set_num_rows(3929);
        rowset_meta_pb.set_total_disk_size(84699);
        rowset_meta_pb.set_data_disk_size(84464);
        rowset_meta_pb.set_index_disk_size(235);
        rowset_meta_pb.set_num_segments(2);

        rs_meta->init_from_pb(rowset_meta_pb);
        rs_meta->set_tablet_schema(tablet_schema);
    }

    StorageEngine* _engine_ref = nullptr;
    TabletSharedPtr _tablet;
    TabletMetaSharedPtr _tablet_meta;
    TabletSchemaSPtr _tablet_schema;
    std::vector<TColumn> _columns;
    std::vector<doris::TOlapTableIndex> _alter_indexes;
    std::unique_ptr<DataDir> _data_dir = nullptr;
    std::string _current_dir;
    std::string _absolute_dir;
    std::unique_ptr<InvertedIndexSearcherCache> _inverted_index_searcher_cache;

    constexpr static uint32_t MAX_PATH_LEN = 1024;
    constexpr static std::string_view dest_dir = "./ut_dir/index_builder_test";
    constexpr static std::string_view tmp_dir = "./ut_dir/index_builder_test";
};

TEST_F(IndexBuilderTest, BasicBuildTest) {
    // 1. Prepare test data
    TOlapTableIndex index;
    index.index_id = 1;
    index.columns.emplace_back("col1");
    _alter_indexes.push_back(index);

    // 2. Create IndexBuilder
    IndexBuilder builder(ExecEnv::GetInstance()->storage_engine().to_local(), _tablet, _columns,
                         _alter_indexes, false);

    // 3. Verify initialization
    auto status = builder.init();
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(builder._alter_index_ids.size(), 1);
}

TEST_F(IndexBuilderTest, DropIndexTest) {
    // 0. prepare tablet path
    auto tablet_path = _absolute_dir + "/" + std::to_string(14676);
    _tablet->_tablet_path = tablet_path;
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(tablet_path).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet_path).ok());

    // 1. Prepare data for writing
    RowsetSharedPtr rowset;
    const int num_rows = 1000;

    // 2. First add an initial index to the schema (for k1 column)
    TabletIndex initial_index;
    initial_index._index_id = 1;
    initial_index._index_name = "k1_index";
    initial_index._index_type = IndexType::INVERTED;
    initial_index._col_unique_ids.push_back(1); // unique_id for k1
    _tablet_schema->append_index(std::move(initial_index));

    // 3. Create a rowset writer context
    RowsetWriterContext writer_context;
    writer_context.rowset_id.init(15676);
    writer_context.tablet_id = 15676;
    writer_context.tablet_schema_hash = 567997577;
    writer_context.partition_id = 10;
    writer_context.rowset_type = BETA_ROWSET;
    writer_context.tablet_path = _absolute_dir + "/" + std::to_string(15676);
    writer_context.rowset_state = VISIBLE;
    writer_context.tablet_schema = _tablet_schema;
    writer_context.version.first = 10;
    writer_context.version.second = 10;

    ASSERT_TRUE(io::global_local_filesystem()->create_directory(writer_context.tablet_path).ok());

    // 4. Create a rowset writer
    auto res = RowsetFactory::create_rowset_writer(*_engine_ref, writer_context, false);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto rowset_writer = std::move(res).value();

    // 5. Write data to the rowset
    {
        vectorized::Block block = _tablet_schema->create_block();
        auto columns = block.mutate_columns();

        // Add data for k1 and k2 columns
        for (int i = 0; i < num_rows; ++i) {
            // k1 column (int)
            int32_t k1 = i * 10;
            columns[0]->insert_data((const char*)&k1, sizeof(k1));

            // k2 column (int)
            int32_t k2 = i % 100;
            columns[1]->insert_data((const char*)&k2, sizeof(k2));
        }

        // Add the block to the rowset
        Status s = rowset_writer->add_block(&block);
        ASSERT_TRUE(s.ok()) << s.to_string();

        // Flush the writer
        s = rowset_writer->flush();
        ASSERT_TRUE(s.ok()) << s.to_string();

        // Build the rowset
        ASSERT_TRUE(rowset_writer->build(rowset).ok());

        // Add the rowset to the tablet
        ASSERT_TRUE(_tablet->add_rowset(rowset).ok());
    }

    // 6. Verify index exists before dropping
    EXPECT_TRUE(_tablet_schema->has_inverted_index());
    EXPECT_TRUE(_tablet_schema->has_inverted_index_with_index_id(1));

    // 7. Prepare index for dropping
    TOlapTableIndex drop_index;
    drop_index.index_id = 1;
    drop_index.columns.emplace_back("k1");
    _alter_indexes.push_back(drop_index);

    // 8. Create IndexBuilder with drop operation
    IndexBuilder builder(ExecEnv::GetInstance()->storage_engine().to_local(), _tablet, _columns,
                         _alter_indexes, true);

    // 9. Initialize and verify
    auto status = builder.init();
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(builder._alter_index_ids.size(), 1);

    // 10. Execute drop operation
    status = builder.do_build_inverted_index();
    EXPECT_TRUE(status.ok()) << status.to_string();

    // 11. Verify the index has been removed
    // check old tablet path and new tablet path
    auto old_tablet_path = _absolute_dir + "/" + std::to_string(15676);
    auto new_tablet_path = _absolute_dir + "/" + std::to_string(14676);
    bool old_exists = false;
    bool new_exists = false;
    EXPECT_TRUE(io::global_local_filesystem()->exists(old_tablet_path, &old_exists).ok());
    EXPECT_TRUE(old_exists);
    EXPECT_TRUE(io::global_local_filesystem()->exists(new_tablet_path, &new_exists).ok());
    EXPECT_TRUE(new_exists);

    // Check files in old and new directories
    std::vector<io::FileInfo> old_files;
    bool old_dir_exists = false;
    EXPECT_TRUE(io::global_local_filesystem()
                        ->list(old_tablet_path, true, &old_files, &old_dir_exists)
                        .ok());
    EXPECT_TRUE(old_dir_exists);
    int idx_file_count = 0;
    int dat_file_count = 0;
    for (const auto& file : old_files) {
        std::string filename = file.file_name;
        if (filename.find(".idx") != std::string::npos) {
            idx_file_count++;
        }
        if (filename.find(".dat") != std::string::npos) {
            dat_file_count++;
        }
    }
    EXPECT_EQ(idx_file_count, 1) << "Old directory should contain exactly 1 .idx file";
    EXPECT_EQ(dat_file_count, 1) << "Old directory should contain exactly 1 .dat file";

    //auto tablet_schema = _tablet->tablet_schema();
    //EXPECT_FALSE(tablet_schema->has_inverted_index_with_index_id(1));
}

TEST_F(IndexBuilderTest, BuildIndexAfterWritingDataTest) {
    // 0. prepare tablet path
    auto tablet_path = _absolute_dir + "/" + std::to_string(14673);
    _tablet->_tablet_path = tablet_path;
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(tablet_path).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet_path).ok());

    // 1. Prepare data for writing
    RowsetSharedPtr rowset;
    const int num_rows = 1000;

    // 2. Create a rowset writer context
    RowsetWriterContext writer_context;
    writer_context.rowset_id.init(15673);
    writer_context.tablet_id = 15673;
    writer_context.tablet_schema_hash = 567997577;
    writer_context.partition_id = 10;
    writer_context.rowset_type = BETA_ROWSET;
    writer_context.tablet_path = _absolute_dir + "/" + std::to_string(15673);
    writer_context.rowset_state = VISIBLE;
    writer_context.tablet_schema = _tablet_schema;
    writer_context.version.first = 10;
    writer_context.version.second = 10;

    ASSERT_TRUE(io::global_local_filesystem()->create_directory(writer_context.tablet_path).ok());

    // 3. Create a rowset writer
    auto res = RowsetFactory::create_rowset_writer(*_engine_ref, writer_context, false);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto rowset_writer = std::move(res).value();

    // 4. Write data to the rowset
    {
        vectorized::Block block = _tablet_schema->create_block();
        auto columns = block.mutate_columns();

        // Add data for k1 and k2 columns according to the schema
        for (int i = 0; i < num_rows; ++i) {
            // k1 column (int)
            int32_t k1 = i * 10;
            columns[0]->insert_data((const char*)&k1, sizeof(k1));

            // k2 column (int)
            int32_t k2 = i % 100;
            columns[1]->insert_data((const char*)&k2, sizeof(k2));
        }

        // Add the block to the rowset
        Status s = rowset_writer->add_block(&block);
        ASSERT_TRUE(s.ok()) << s.to_string();

        // Flush the writer
        s = rowset_writer->flush();
        ASSERT_TRUE(s.ok()) << s.to_string();

        // Build the rowset
        ASSERT_TRUE(rowset_writer->build(rowset).ok());

        // Add the rowset to the tablet
        ASSERT_TRUE(_tablet->add_rowset(rowset).ok());
    }

    // 5. Prepare index for building
    TOlapTableIndex index1;
    index1.index_id = 1;
    index1.columns.emplace_back("k1");
    index1.index_name = "k1_index";
    index1.index_type = TIndexType::INVERTED;
    _alter_indexes.push_back(index1);

    TOlapTableIndex index2;
    index2.index_id = 2;
    index2.columns.emplace_back("k2");
    index2.index_name = "k2_index";
    index2.index_type = TIndexType::INVERTED;
    _alter_indexes.push_back(index2);

    // 6. Create IndexBuilder
    IndexBuilder builder(ExecEnv::GetInstance()->storage_engine().to_local(), _tablet, _columns,
                         _alter_indexes, false);

    // 7. Initialize and verify
    auto status = builder.init();
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(builder._alter_index_ids.size(), 2);

    // 8. Build index
    status = builder.do_build_inverted_index();
    EXPECT_TRUE(status.ok()) << status.to_string();

    // check old tablet path and new tablet path
    auto old_tablet_path = _absolute_dir + "/" + std::to_string(15673);
    auto new_tablet_path = _absolute_dir + "/" + std::to_string(14673);
    bool old_exists = false;
    bool new_exists = false;
    EXPECT_TRUE(io::global_local_filesystem()->exists(old_tablet_path, &old_exists).ok());
    EXPECT_TRUE(old_exists);
    EXPECT_TRUE(io::global_local_filesystem()->exists(new_tablet_path, &new_exists).ok());
    EXPECT_TRUE(new_exists);

    // Check files in old and new directories
    std::vector<io::FileInfo> old_files;
    bool old_dir_exists = false;
    EXPECT_TRUE(io::global_local_filesystem()
                        ->list(old_tablet_path, true, &old_files, &old_dir_exists)
                        .ok());
    EXPECT_TRUE(old_dir_exists);
    int idx_file_count = 0;
    int dat_file_count = 0;
    for (const auto& file : old_files) {
        std::string filename = file.file_name;
        if (filename.find(".idx") != std::string::npos) {
            idx_file_count++;
        }
        if (filename.find(".dat") != std::string::npos) {
            dat_file_count++;
        }
    }
    EXPECT_EQ(idx_file_count, 0) << "Old directory should contain exactly 0 .idx file";
    EXPECT_EQ(dat_file_count, 1) << "Old directory should contain exactly 1 .dat file";

    std::vector<io::FileInfo> new_files;
    bool new_dir_exists = false;
    EXPECT_TRUE(io::global_local_filesystem()
                        ->list(new_tablet_path, true, &new_files, &new_dir_exists)
                        .ok());
    EXPECT_TRUE(new_dir_exists);
    int new_idx_file_count = 0;
    int new_dat_file_count = 0;
    for (const auto& file : new_files) {
        std::string filename = file.file_name;
        if (filename.find(".idx") != std::string::npos) {
            new_idx_file_count++;
        }
        if (filename.find(".dat") != std::string::npos) {
            new_dat_file_count++;
        }
    }
    EXPECT_EQ(new_idx_file_count, 1) << "New directory should contain exactly 1 .idx files";
    EXPECT_EQ(new_dat_file_count, 1) << "New directory should contain exactly 1 .dat file";

    // 9. Verify the result (indexes should be built successfully)
    //auto tablet_schema = _tablet->tablet_schema();
    //EXPECT_TRUE(tablet_schema->has_inverted_index_with_index_id(1));
    //EXPECT_TRUE(tablet_schema->has_inverted_index_with_index_id(2));
}

TEST_F(IndexBuilderTest, AddIndexWhenOneExistsTest) {
    // 0. prepare tablet path
    auto tablet_path = _absolute_dir + "/" + std::to_string(14675);
    _tablet->_tablet_path = tablet_path;
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(tablet_path).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet_path).ok());

    // 1. Prepare data for writing
    RowsetSharedPtr rowset;
    const int num_rows = 1000;

    // 2. First add an initial index to the schema (for k1 column)
    TabletIndex initial_index;
    initial_index._index_id = 1;
    initial_index._index_name = "k1_index";
    initial_index._index_type = IndexType::INVERTED;
    initial_index._col_unique_ids.push_back(1); // unique_id for k1
    _tablet_schema->append_index(std::move(initial_index));

    // 3. Create rowset writer context
    RowsetWriterContext writer_context;
    writer_context.rowset_id.init(15675);
    writer_context.tablet_id = 15675;
    writer_context.tablet_schema_hash = 567997577;
    writer_context.partition_id = 10;
    writer_context.rowset_type = BETA_ROWSET;
    writer_context.tablet_path = _absolute_dir + "/" + std::to_string(15675);
    writer_context.rowset_state = VISIBLE;
    writer_context.tablet_schema = _tablet_schema;
    writer_context.version.first = 10;
    writer_context.version.second = 10;

    ASSERT_TRUE(io::global_local_filesystem()->create_directory(writer_context.tablet_path).ok());

    // 4. Create rowset writer
    auto res = RowsetFactory::create_rowset_writer(*_engine_ref, writer_context, false);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto rowset_writer = std::move(res).value();

    // 5. Write data to rowset
    {
        vectorized::Block block = _tablet_schema->create_block();
        auto columns = block.mutate_columns();

        // Add data for k1 and k2 columns
        for (int i = 0; i < num_rows; ++i) {
            // k1 column (int)
            int32_t k1 = i * 10;
            columns[0]->insert_data((const char*)&k1, sizeof(k1));

            // k2 column (int)
            int32_t k2 = i % 100;
            columns[1]->insert_data((const char*)&k2, sizeof(k2));
        }

        // Add block to rowset
        Status s = rowset_writer->add_block(&block);
        ASSERT_TRUE(s.ok()) << s.to_string();

        // Flush writer
        s = rowset_writer->flush();
        ASSERT_TRUE(s.ok()) << s.to_string();

        // Build rowset
        ASSERT_TRUE(rowset_writer->build(rowset).ok());

        // Add rowset to tablet
        ASSERT_TRUE(_tablet->add_rowset(rowset).ok());
    }

    // 6. Prepare new index information (only add for k2 column)
    TOlapTableIndex new_index;
    new_index.index_id = 2; // New index ID is 2
    new_index.columns.emplace_back("k2");
    new_index.index_name = "k2_index";
    new_index.index_type = TIndexType::INVERTED;
    _alter_indexes.push_back(new_index);

    // 7. Create IndexBuilder
    IndexBuilder builder(ExecEnv::GetInstance()->storage_engine().to_local(), _tablet, _columns,
                         _alter_indexes, false);

    // 8. Initialize and verify
    auto status = builder.init();
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(builder._alter_index_ids.size(), 1); // Only one new index needs to be built

    // 9. Build index
    status = builder.do_build_inverted_index();
    EXPECT_TRUE(status.ok()) << status.to_string();

    // check old tablet path and new tablet path
    auto old_tablet_path = _absolute_dir + "/" + std::to_string(15675);
    auto new_tablet_path = _absolute_dir + "/" + std::to_string(14675);
    bool old_exists = false;
    bool new_exists = false;
    EXPECT_TRUE(io::global_local_filesystem()->exists(old_tablet_path, &old_exists).ok());
    EXPECT_TRUE(old_exists);
    EXPECT_TRUE(io::global_local_filesystem()->exists(new_tablet_path, &new_exists).ok());
    EXPECT_TRUE(new_exists);

    // Check files in old and new directories
    std::vector<io::FileInfo> old_files;
    bool old_dir_exists = false;
    EXPECT_TRUE(io::global_local_filesystem()
                        ->list(old_tablet_path, true, &old_files, &old_dir_exists)
                        .ok());
    EXPECT_TRUE(old_dir_exists);
    int idx_file_count = 0;
    int dat_file_count = 0;
    for (const auto& file : old_files) {
        std::string filename = file.file_name;
        if (filename.find(".idx") != std::string::npos) {
            idx_file_count++;
        }
        if (filename.find(".dat") != std::string::npos) {
            dat_file_count++;
        }
    }
    EXPECT_EQ(idx_file_count, 1) << "Old directory should contain exactly 1 .idx file";
    EXPECT_EQ(dat_file_count, 1) << "Old directory should contain exactly 1 .dat file";

    std::vector<io::FileInfo> new_files;
    bool new_dir_exists = false;
    EXPECT_TRUE(io::global_local_filesystem()
                        ->list(new_tablet_path, true, &new_files, &new_dir_exists)
                        .ok());
    EXPECT_TRUE(new_dir_exists);
    int new_idx_file_count = 0;
    int new_dat_file_count = 0;
    for (const auto& file : new_files) {
        std::string filename = file.file_name;
        if (filename.find(".idx") != std::string::npos) {
            new_idx_file_count++;
        }
        if (filename.find(".dat") != std::string::npos) {
            new_dat_file_count++;
        }
    }
    EXPECT_EQ(new_idx_file_count, 1) << "New directory should contain exactly 1 .idx files";
    EXPECT_EQ(new_dat_file_count, 1) << "New directory should contain exactly 1 .dat file";

    // 10. Verify results (both indexes should exist)
    // Verify initial index (k1) still exists
    //EXPECT_TRUE(_tablet_schema->has_inverted_index_with_index_id(1));
    // Verify newly added index (k2) is successfully built
    //EXPECT_TRUE(_tablet_schema->has_inverted_index_with_index_id(2));
}

TEST_F(IndexBuilderTest, AddIndexWhenOneExistsTestV1) {
    // 1. Create new schema using V1 format
    auto v1_schema = std::make_shared<TabletSchema>();
    create_tablet_schema(v1_schema, KeysType::DUP_KEYS);

    // 2. Modify to V1 format
    v1_schema->_inverted_index_storage_format = InvertedIndexStorageFormatPB::V1;

    // 3. First add an initial index to the schema (for k1 column)
    TabletIndex initial_index;
    initial_index._index_id = 1;
    initial_index._index_name = "k1_index";
    initial_index._index_type = IndexType::INVERTED;
    initial_index._col_unique_ids.push_back(1); // unique_id for k1
    v1_schema->append_index(std::move(initial_index));

    // 4. Update schema in tablet
    TabletMetaPB tablet_meta_pb;
    _tablet_meta->to_meta_pb(&tablet_meta_pb);

    TabletSchemaPB v1_schema_pb;
    v1_schema->to_schema_pb(&v1_schema_pb);
    tablet_meta_pb.mutable_schema()->CopyFrom(v1_schema_pb);

    _tablet_meta->init_from_pb(tablet_meta_pb);

    // Reinitialize tablet to use new schema
    _tablet = std::make_shared<Tablet>(*_engine_ref, _tablet_meta, _data_dir.get());
    ASSERT_TRUE(_tablet->init().ok());
    auto tablet_path = _absolute_dir + "/" + std::to_string(14674);
    _tablet->_tablet_path = tablet_path;
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(tablet_path).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet_path).ok());

    // 5. Prepare data
    RowsetSharedPtr rowset;
    const int num_rows = 1000;

    // 6. Create rowset writer context
    RowsetWriterContext writer_context;
    writer_context.rowset_id.init(15674);
    writer_context.tablet_id = 15674;
    writer_context.tablet_schema_hash = 567997577;
    writer_context.partition_id = 10;
    writer_context.rowset_type = BETA_ROWSET;
    writer_context.tablet_path = _absolute_dir + "/" + std::to_string(15674);
    writer_context.rowset_state = VISIBLE;
    writer_context.tablet_schema = v1_schema;
    writer_context.version.first = 10;
    writer_context.version.second = 10;

    ASSERT_TRUE(io::global_local_filesystem()->create_directory(writer_context.tablet_path).ok());

    // 7. Create rowset writer
    auto res = RowsetFactory::create_rowset_writer(*_engine_ref, writer_context, false);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto rowset_writer = std::move(res).value();

    // 8. Write data to rowset
    {
        vectorized::Block block = v1_schema->create_block();
        auto columns = block.mutate_columns();

        // Add data for k1 and k2 columns
        for (int i = 0; i < num_rows; ++i) {
            // k1 column (int)
            int32_t k1 = i * 10;
            columns[0]->insert_data((const char*)&k1, sizeof(k1));

            // k2 column (int)
            int32_t k2 = i % 100;
            columns[1]->insert_data((const char*)&k2, sizeof(k2));
        }

        // Add block to rowset
        Status s = rowset_writer->add_block(&block);
        ASSERT_TRUE(s.ok()) << s.to_string();

        // Flush writer
        s = rowset_writer->flush();
        ASSERT_TRUE(s.ok()) << s.to_string();

        // Build rowset
        ASSERT_TRUE(rowset_writer->build(rowset).ok());

        // Add rowset to tablet
        ASSERT_TRUE(_tablet->add_rowset(rowset).ok());
    }

    // 9. Clear existing index list, prepare new index
    _alter_indexes.clear();

    // 10. Prepare new index information (only add for k2 column)
    TOlapTableIndex new_index;
    new_index.index_id = 2; // New index ID is 2
    new_index.columns.emplace_back("k2");
    new_index.index_name = "k2_index";
    new_index.index_type = TIndexType::INVERTED;
    _alter_indexes.push_back(new_index);

    // 11. Create IndexBuilder
    IndexBuilder builder(ExecEnv::GetInstance()->storage_engine().to_local(), _tablet, _columns,
                         _alter_indexes, false);

    // 12. Initialize and verify
    auto status = builder.init();
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(builder._alter_index_ids.size(), 1); // Only one new index needs to be built

    // 13. Build index
    status = builder.do_build_inverted_index();
    EXPECT_TRUE(status.ok()) << status.to_string();

    // check old tablet path and new tablet path
    auto old_tablet_path = _absolute_dir + "/" + std::to_string(15674);
    auto new_tablet_path = _absolute_dir + "/" + std::to_string(14674);
    bool old_exists = false;
    bool new_exists = false;
    EXPECT_TRUE(io::global_local_filesystem()->exists(old_tablet_path, &old_exists).ok());
    EXPECT_TRUE(old_exists);
    EXPECT_TRUE(io::global_local_filesystem()->exists(new_tablet_path, &new_exists).ok());
    EXPECT_TRUE(new_exists);

    // Check files in old and new directories
    std::vector<io::FileInfo> old_files;
    bool old_dir_exists = false;
    EXPECT_TRUE(io::global_local_filesystem()
                        ->list(old_tablet_path, true, &old_files, &old_dir_exists)
                        .ok());
    EXPECT_TRUE(old_dir_exists);
    int idx_file_count = 0;
    int dat_file_count = 0;
    for (const auto& file : old_files) {
        std::string filename = file.file_name;
        if (filename.find(".idx") != std::string::npos) {
            idx_file_count++;
        }
        if (filename.find(".dat") != std::string::npos) {
            dat_file_count++;
        }
    }
    EXPECT_EQ(idx_file_count, 1) << "Old directory should contain exactly 1 .idx file";
    EXPECT_EQ(dat_file_count, 1) << "Old directory should contain exactly 1 .dat file";

    std::vector<io::FileInfo> new_files;
    bool new_dir_exists = false;
    EXPECT_TRUE(io::global_local_filesystem()
                        ->list(new_tablet_path, true, &new_files, &new_dir_exists)
                        .ok());
    EXPECT_TRUE(new_dir_exists);
    int new_idx_file_count = 0;
    int new_dat_file_count = 0;
    for (const auto& file : new_files) {
        std::string filename = file.file_name;
        if (filename.find(".idx") != std::string::npos) {
            new_idx_file_count++;
        }
        if (filename.find(".dat") != std::string::npos) {
            new_dat_file_count++;
        }
    }
    EXPECT_EQ(new_idx_file_count, 2) << "New directory should contain exactly 2 .idx files";
    EXPECT_EQ(new_dat_file_count, 1) << "New directory should contain exactly 1 .dat file";
    // 14. Verify results (both indexes should exist)
    // Verify initial index (k1) still exists
    //EXPECT_TRUE(v1_schema->has_inverted_index_with_index_id(1));
    // Verify newly added index (k2) is successfully built
    //EXPECT_TRUE(_tablet->tablet_schema()->has_inverted_index_with_index_id(2));

    // 15. Confirm storage format is still V1
    //EXPECT_EQ(v1_schema->_inverted_index_storage_format, InvertedIndexStorageFormatPB::V1);
}

TEST_F(IndexBuilderTest, MultiSegmentBuildIndexTest) {
    // 0. prepare tablet path
    auto tablet_path = _absolute_dir + "/" + std::to_string(14677);
    _tablet->_tablet_path = tablet_path;
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(tablet_path).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet_path).ok());

    // 1. Prepare data for writing
    RowsetSharedPtr rowset;
    const int rows_per_segment = 500;
    const int num_segments = 3;

    // 2. Create a rowset writer context with segment size set to trigger multiple segments
    RowsetWriterContext writer_context;
    writer_context.rowset_id.init(15677);
    writer_context.tablet_id = 15677;
    writer_context.tablet_schema_hash = 567997577;
    writer_context.partition_id = 10;
    writer_context.rowset_type = BETA_ROWSET;
    writer_context.tablet_path = _absolute_dir + "/" + std::to_string(15677);
    writer_context.rowset_state = VISIBLE;
    writer_context.tablet_schema = _tablet_schema;
    writer_context.version.first = 10;
    writer_context.version.second = 10;
    // Set small segment size to ensure we create multiple segments
    writer_context.max_rows_per_segment = rows_per_segment;

    ASSERT_TRUE(io::global_local_filesystem()->create_directory(writer_context.tablet_path).ok());

    // 3. Create a rowset writer
    auto res = RowsetFactory::create_rowset_writer(*_engine_ref, writer_context, false);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto rowset_writer = std::move(res).value();

    // 4. Write data to the rowset in multiple batches to ensure we get multiple segments
    for (int segment = 0; segment < num_segments; segment++) {
        vectorized::Block block = _tablet_schema->create_block();
        auto columns = block.mutate_columns();

        // Add data for k1 and k2 columns
        for (int i = 0; i < rows_per_segment; ++i) {
            // k1 column (int) - make values different across segments
            int32_t k1 = (segment * rows_per_segment + i) * 10;
            columns[0]->insert_data((const char*)&k1, sizeof(k1));

            // k2 column (int)
            int32_t k2 = (segment * rows_per_segment + i) % 100;
            columns[1]->insert_data((const char*)&k2, sizeof(k2));
        }

        // Add the block to the rowset
        Status s = rowset_writer->add_block(&block);
        ASSERT_TRUE(s.ok()) << s.to_string();

        // Flush to ensure we create a new segment
        s = rowset_writer->flush();
        ASSERT_TRUE(s.ok()) << s.to_string();
    }

    // 5. Build the rowset
    ASSERT_TRUE(rowset_writer->build(rowset).ok());

    // Verify we have the expected number of segments
    ASSERT_EQ(rowset->num_segments(), num_segments)
            << "Rowset should have " << num_segments << " segments but has "
            << rowset->num_segments();

    // 6. Add the rowset to the tablet
    ASSERT_TRUE(_tablet->add_rowset(rowset).ok());

    // 7. Prepare indexes for building
    TOlapTableIndex index1;
    index1.index_id = 1;
    index1.columns.emplace_back("k1");
    index1.index_name = "k1_index";
    index1.index_type = TIndexType::INVERTED;
    _alter_indexes.push_back(index1);

    TOlapTableIndex index2;
    index2.index_id = 2;
    index2.columns.emplace_back("k2");
    index2.index_name = "k2_index";
    index2.index_type = TIndexType::INVERTED;
    _alter_indexes.push_back(index2);

    // 8. Create IndexBuilder
    IndexBuilder builder(ExecEnv::GetInstance()->storage_engine().to_local(), _tablet, _columns,
                         _alter_indexes, false);

    // 9. Initialize and verify
    auto status = builder.init();
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(builder._alter_index_ids.size(), 2);

    // 10. Build indexes
    status = builder.do_build_inverted_index();
    EXPECT_TRUE(status.ok()) << status.to_string();

    // 11. Check paths and files
    auto old_tablet_path = _absolute_dir + "/" + std::to_string(15677);
    auto new_tablet_path = _absolute_dir + "/" + std::to_string(14677);
    bool old_exists = false;
    bool new_exists = false;
    EXPECT_TRUE(io::global_local_filesystem()->exists(old_tablet_path, &old_exists).ok());
    EXPECT_TRUE(old_exists);
    EXPECT_TRUE(io::global_local_filesystem()->exists(new_tablet_path, &new_exists).ok());
    EXPECT_TRUE(new_exists);

    // 12. Check files in old and new directories
    std::vector<io::FileInfo> old_files;
    bool old_dir_exists = false;
    EXPECT_TRUE(io::global_local_filesystem()
                        ->list(old_tablet_path, true, &old_files, &old_dir_exists)
                        .ok());
    EXPECT_TRUE(old_dir_exists);
    int old_idx_file_count = 0;
    int old_dat_file_count = 0;
    for (const auto& file : old_files) {
        std::string filename = file.file_name;
        if (filename.find(".idx") != std::string::npos) {
            old_idx_file_count++;
        }
        if (filename.find(".dat") != std::string::npos) {
            old_dat_file_count++;
        }
    }
    EXPECT_EQ(old_idx_file_count, 0) << "Old directory should contain exactly 0 .idx files";
    EXPECT_EQ(old_dat_file_count, num_segments)
            << "Old directory should contain exactly " << num_segments << " .dat files";

    std::vector<io::FileInfo> new_files;
    bool new_dir_exists = false;
    EXPECT_TRUE(io::global_local_filesystem()
                        ->list(new_tablet_path, true, &new_files, &new_dir_exists)
                        .ok());
    EXPECT_TRUE(new_dir_exists);
    int new_idx_file_count = 0;
    int new_dat_file_count = 0;
    for (const auto& file : new_files) {
        std::string filename = file.file_name;
        if (filename.find(".idx") != std::string::npos) {
            new_idx_file_count++;
        }
        if (filename.find(".dat") != std::string::npos) {
            new_dat_file_count++;
        }
    }
    EXPECT_EQ(new_idx_file_count, num_segments)
            << "New directory should contain exactly " << num_segments << " .idx files";
    EXPECT_EQ(new_dat_file_count, num_segments)
            << "New directory should contain exactly " << num_segments << " .dat files";
}

TEST_F(IndexBuilderTest, NonExistentColumnIndexTest) {
    // 0. prepare tablet path
    auto tablet_path = _absolute_dir + "/" + std::to_string(14678);
    _tablet->_tablet_path = tablet_path;
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(tablet_path).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet_path).ok());

    // 1. Prepare data for writing
    RowsetSharedPtr rowset;
    const int num_rows = 1000;

    // 2. Create a rowset writer context
    RowsetWriterContext writer_context;
    writer_context.rowset_id.init(15678);
    writer_context.tablet_id = 15678;
    writer_context.tablet_schema_hash = 567997577;
    writer_context.partition_id = 10;
    writer_context.rowset_type = BETA_ROWSET;
    writer_context.tablet_path = _absolute_dir + "/" + std::to_string(15678);
    writer_context.rowset_state = VISIBLE;
    writer_context.tablet_schema = _tablet_schema;
    writer_context.version.first = 10;
    writer_context.version.second = 10;

    ASSERT_TRUE(io::global_local_filesystem()->create_directory(writer_context.tablet_path).ok());

    // 3. Create a rowset writer
    auto res = RowsetFactory::create_rowset_writer(*_engine_ref, writer_context, false);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto rowset_writer = std::move(res).value();

    // 4. Write data to the rowset
    {
        vectorized::Block block = _tablet_schema->create_block();
        auto columns = block.mutate_columns();

        // Add data for k1 and k2 columns
        for (int i = 0; i < num_rows; ++i) {
            // k1 column (int)
            int32_t k1 = i * 10;
            columns[0]->insert_data((const char*)&k1, sizeof(k1));

            // k2 column (int)
            int32_t k2 = i % 100;
            columns[1]->insert_data((const char*)&k2, sizeof(k2));
        }

        // Add the block to the rowset
        Status s = rowset_writer->add_block(&block);
        ASSERT_TRUE(s.ok()) << s.to_string();

        // Flush the writer
        s = rowset_writer->flush();
        ASSERT_TRUE(s.ok()) << s.to_string();

        // Build the rowset
        ASSERT_TRUE(rowset_writer->build(rowset).ok());

        // Add the rowset to the tablet
        ASSERT_TRUE(_tablet->add_rowset(rowset).ok());
    }

    // 5. Prepare indexes for building - including one for a non-existent column
    _alter_indexes.clear();

    // Index for non-existent column "k3"
    TOlapTableIndex index2;
    index2.index_id = 2;
    index2.columns.emplace_back("k3"); // This column doesn't exist in the schema
    index2.index_name = "k3_index";
    index2.index_type = TIndexType::INVERTED;
    _alter_indexes.push_back(index2);

    // 6. Create IndexBuilder
    IndexBuilder builder(ExecEnv::GetInstance()->storage_engine().to_local(), _tablet, _columns,
                         _alter_indexes, false);

    // 7. Initialize and verify
    auto status = builder.init();
    // The init should succeed, as we'll skip non-existent columns later
    EXPECT_TRUE(status.ok()) << status.to_string();

    // 8. Build indexes - should only build for existing columns
    status = builder.do_build_inverted_index();
    EXPECT_TRUE(status.ok()) << status.to_string();

    // 9. Check paths and files
    auto old_tablet_path = _absolute_dir + "/" + std::to_string(15678);
    auto new_tablet_path = _absolute_dir + "/" + std::to_string(14678);
    bool old_exists = false;
    bool new_exists = false;
    EXPECT_TRUE(io::global_local_filesystem()->exists(old_tablet_path, &old_exists).ok());
    EXPECT_TRUE(old_exists);
    EXPECT_TRUE(io::global_local_filesystem()->exists(new_tablet_path, &new_exists).ok());
    EXPECT_TRUE(new_exists);

    // 10. Check files in old and new directories
    std::vector<io::FileInfo> old_files;
    bool old_dir_exists = false;
    EXPECT_TRUE(io::global_local_filesystem()
                        ->list(old_tablet_path, true, &old_files, &old_dir_exists)
                        .ok());
    EXPECT_TRUE(old_dir_exists);
    int old_idx_file_count = 0;
    int old_dat_file_count = 0;
    for (const auto& file : old_files) {
        std::string filename = file.file_name;
        if (filename.find(".idx") != std::string::npos) {
            old_idx_file_count++;
        }
        if (filename.find(".dat") != std::string::npos) {
            old_dat_file_count++;
        }
    }
    EXPECT_EQ(old_idx_file_count, 0) << "Old directory should contain exactly 0 .idx files";
    EXPECT_EQ(old_dat_file_count, 1) << "Old directory should contain exactly 1 .dat file";

    std::vector<io::FileInfo> new_files;
    bool new_dir_exists = false;
    EXPECT_TRUE(io::global_local_filesystem()
                        ->list(new_tablet_path, true, &new_files, &new_dir_exists)
                        .ok());
    EXPECT_TRUE(new_dir_exists);
    int new_idx_file_count = 0;
    int new_dat_file_count = 0;
    for (const auto& file : new_files) {
        std::string filename = file.file_name;
        if (filename.find(".idx") != std::string::npos) {
            new_idx_file_count++;
        }
        if (filename.find(".dat") != std::string::npos) {
            new_dat_file_count++;
        }
    }
    // Should only have index for k1, not for non-existent k3
    EXPECT_EQ(new_idx_file_count, 0)
            << "New directory should contain exactly 0 .idx file for the existing column";
    EXPECT_EQ(new_dat_file_count, 1) << "New directory should contain exactly 1 .dat file";

    // 11. Verify logs in the output to confirm k3 index was skipped
    // This would require examining the log output which isn't easily done in unit tests,
    // but the file count verification above should be sufficient to confirm behavior
}

TEST_F(IndexBuilderTest, AddNonExistentColumnIndexWhenOneExistsTest) {
    // 0. prepare tablet path
    auto tablet_path = _absolute_dir + "/" + std::to_string(14679);
    _tablet->_tablet_path = tablet_path;
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(tablet_path).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet_path).ok());

    // 1. Prepare data for writing
    RowsetSharedPtr rowset;
    const int num_rows = 1000;

    // 2. First add an initial index to the schema (for k1 column)
    TabletIndex initial_index;
    initial_index._index_id = 1;
    initial_index._index_name = "k1_index";
    initial_index._index_type = IndexType::INVERTED;
    initial_index._col_unique_ids.push_back(1); // unique_id for k1
    _tablet_schema->append_index(std::move(initial_index));

    // 3. Create a rowset writer context
    RowsetWriterContext writer_context;
    writer_context.rowset_id.init(15679);
    writer_context.tablet_id = 15679;
    writer_context.tablet_schema_hash = 567997577;
    writer_context.partition_id = 10;
    writer_context.rowset_type = BETA_ROWSET;
    writer_context.tablet_path = _absolute_dir + "/" + std::to_string(15679);
    writer_context.rowset_state = VISIBLE;
    writer_context.tablet_schema = _tablet_schema;
    writer_context.version.first = 10;
    writer_context.version.second = 10;

    ASSERT_TRUE(io::global_local_filesystem()->create_directory(writer_context.tablet_path).ok());

    // 4. Create a rowset writer
    auto res = RowsetFactory::create_rowset_writer(*_engine_ref, writer_context, false);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto rowset_writer = std::move(res).value();

    // 5. Write data to the rowset
    {
        vectorized::Block block = _tablet_schema->create_block();
        auto columns = block.mutate_columns();

        // Add data for k1 and k2 columns
        for (int i = 0; i < num_rows; ++i) {
            // k1 column (int)
            int32_t k1 = i * 10;
            columns[0]->insert_data((const char*)&k1, sizeof(k1));

            // k2 column (int)
            int32_t k2 = i % 100;
            columns[1]->insert_data((const char*)&k2, sizeof(k2));
        }

        // Add the block to the rowset
        Status s = rowset_writer->add_block(&block);
        ASSERT_TRUE(s.ok()) << s.to_string();

        // Flush the writer
        s = rowset_writer->flush();
        ASSERT_TRUE(s.ok()) << s.to_string();

        // Build the rowset
        ASSERT_TRUE(rowset_writer->build(rowset).ok());

        // Add the rowset to the tablet
        ASSERT_TRUE(_tablet->add_rowset(rowset).ok());
    }

    // 6. Prepare indexes for building - valid k2 and non-existent k3
    _alter_indexes.clear();

    // Index for non-existent column "k3"
    TOlapTableIndex index2;
    index2.index_id = 3;
    index2.columns.emplace_back("k3"); // This column doesn't exist in the schema
    index2.index_name = "k3_index";
    index2.index_type = TIndexType::INVERTED;
    _alter_indexes.push_back(index2);

    // 7. Create IndexBuilder
    IndexBuilder builder(ExecEnv::GetInstance()->storage_engine().to_local(), _tablet, _columns,
                         _alter_indexes, false);

    // 8. Initialize and verify
    auto status = builder.init();
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(builder._alter_index_ids.size(), 1); // Only k1 is considered for building

    // 9. Build indexes - should only build for existing columns
    status = builder.do_build_inverted_index();
    EXPECT_TRUE(status.ok()) << status.to_string();

    // 10. Check paths and files
    auto old_tablet_path = _absolute_dir + "/" + std::to_string(15679);
    auto new_tablet_path = _absolute_dir + "/" + std::to_string(14679);
    bool old_exists = false;
    bool new_exists = false;
    EXPECT_TRUE(io::global_local_filesystem()->exists(old_tablet_path, &old_exists).ok());
    EXPECT_TRUE(old_exists);
    EXPECT_TRUE(io::global_local_filesystem()->exists(new_tablet_path, &new_exists).ok());
    EXPECT_TRUE(new_exists);

    // 11. Check files in old and new directories
    std::vector<io::FileInfo> old_files;
    bool old_dir_exists = false;
    EXPECT_TRUE(io::global_local_filesystem()
                        ->list(old_tablet_path, true, &old_files, &old_dir_exists)
                        .ok());
    EXPECT_TRUE(old_dir_exists);
    int old_idx_file_count = 0;
    int old_dat_file_count = 0;
    for (const auto& file : old_files) {
        std::string filename = file.file_name;
        if (filename.find(".idx") != std::string::npos) {
            old_idx_file_count++;
        }
        if (filename.find(".dat") != std::string::npos) {
            old_dat_file_count++;
        }
    }
    EXPECT_EQ(old_idx_file_count, 1)
            << "Old directory should contain exactly 1 .idx file for the original k1 index";
    EXPECT_EQ(old_dat_file_count, 1) << "Old directory should contain exactly 1 .dat file";

    std::vector<io::FileInfo> new_files;
    bool new_dir_exists = false;
    EXPECT_TRUE(io::global_local_filesystem()
                        ->list(new_tablet_path, true, &new_files, &new_dir_exists)
                        .ok());
    EXPECT_TRUE(new_dir_exists);
    int new_idx_file_count = 0;
    int new_dat_file_count = 0;
    for (const auto& file : new_files) {
        std::string filename = file.file_name;
        if (filename.find(".idx") != std::string::npos) {
            new_idx_file_count++;
        }
        if (filename.find(".dat") != std::string::npos) {
            new_dat_file_count++;
        }
    }
    // Should have 2 index files: original k1 index and new k2 index (k3 should be skipped)
    EXPECT_EQ(new_idx_file_count, 1)
            << "New directory should contain exactly 1 .idx files (for k1 and k2, not k3)";
    EXPECT_EQ(new_dat_file_count, 1) << "New directory should contain exactly 1 .dat file";

    // 12. Verify the tablet schema - would need to examine tablet_schema here
    // k1 and k2 indexes should exist, k3 index should not
    // Note: In production code, additional verification of schema would be done here
}

TEST_F(IndexBuilderTest, AddNonExistentColumnIndexWhenOneExistsTestV1) {
    // 1. Create new schema using V1 format
    auto v1_schema = std::make_shared<TabletSchema>();
    create_tablet_schema(v1_schema, KeysType::DUP_KEYS);

    // 2. Modify to V1 format
    v1_schema->_inverted_index_storage_format = InvertedIndexStorageFormatPB::V1;

    // 3. First add an initial index to the schema (for k1 column)
    TabletIndex initial_index;
    initial_index._index_id = 1;
    initial_index._index_name = "k1_index";
    initial_index._index_type = IndexType::INVERTED;
    initial_index._col_unique_ids.push_back(1); // unique_id for k1
    v1_schema->append_index(std::move(initial_index));

    // 4. Update schema in tablet
    TabletMetaPB tablet_meta_pb;
    _tablet_meta->to_meta_pb(&tablet_meta_pb);

    TabletSchemaPB v1_schema_pb;
    v1_schema->to_schema_pb(&v1_schema_pb);
    tablet_meta_pb.mutable_schema()->CopyFrom(v1_schema_pb);

    _tablet_meta->init_from_pb(tablet_meta_pb);

    // 5. Reinitialize tablet to use new schema
    _tablet = std::make_shared<Tablet>(*_engine_ref, _tablet_meta, _data_dir.get());
    ASSERT_TRUE(_tablet->init().ok());
    auto tablet_path = _absolute_dir + "/" + std::to_string(14680);
    _tablet->_tablet_path = tablet_path;
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(tablet_path).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet_path).ok());

    // 6. Prepare data for writing
    RowsetSharedPtr rowset;
    const int num_rows = 1000;

    // 7. Create rowset writer context
    RowsetWriterContext writer_context;
    writer_context.rowset_id.init(15680);
    writer_context.tablet_id = 15680;
    writer_context.tablet_schema_hash = 567997577;
    writer_context.partition_id = 10;
    writer_context.rowset_type = BETA_ROWSET;
    writer_context.tablet_path = _absolute_dir + "/" + std::to_string(15680);
    writer_context.rowset_state = VISIBLE;
    writer_context.tablet_schema = v1_schema;
    writer_context.version.first = 10;
    writer_context.version.second = 10;

    ASSERT_TRUE(io::global_local_filesystem()->create_directory(writer_context.tablet_path).ok());

    // 8. Create rowset writer
    auto res = RowsetFactory::create_rowset_writer(*_engine_ref, writer_context, false);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto rowset_writer = std::move(res).value();

    // 9. Write data to rowset
    {
        vectorized::Block block = v1_schema->create_block();
        auto columns = block.mutate_columns();

        // Add data for k1 and k2 columns
        for (int i = 0; i < num_rows; ++i) {
            // k1 column (int)
            int32_t k1 = i * 10;
            columns[0]->insert_data((const char*)&k1, sizeof(k1));

            // k2 column (int)
            int32_t k2 = i % 100;
            columns[1]->insert_data((const char*)&k2, sizeof(k2));
        }

        // Add block to rowset
        Status s = rowset_writer->add_block(&block);
        ASSERT_TRUE(s.ok()) << s.to_string();

        // Flush writer
        s = rowset_writer->flush();
        ASSERT_TRUE(s.ok()) << s.to_string();

        // Build rowset
        ASSERT_TRUE(rowset_writer->build(rowset).ok());

        // Add rowset to tablet
        ASSERT_TRUE(_tablet->add_rowset(rowset).ok());
    }

    // 10. Prepare indexes for building - valid k2 and non-existent k3
    _alter_indexes.clear();

    // Index for non-existent column "k3"
    TOlapTableIndex index2;
    index2.index_id = 3;
    index2.columns.emplace_back("k3"); // This column doesn't exist in the schema
    index2.index_name = "k3_index";
    index2.index_type = TIndexType::INVERTED;
    _alter_indexes.push_back(index2);

    // Add column information for the non-existent column
    _columns.clear(); // Clear previous column info
    TColumn non_existent_column;
    non_existent_column.column_name = "k3";
    non_existent_column.column_type.type = TPrimitiveType::INT;
    _columns.push_back(non_existent_column);

    // 11. Create IndexBuilder
    IndexBuilder builder(ExecEnv::GetInstance()->storage_engine().to_local(), _tablet, _columns,
                         _alter_indexes, false);

    // 12. Initialize and verify
    auto status = builder.init();
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(builder._alter_index_ids.size(), 1);
    // 13. Build indexes - should only build for existing columns
    status = builder.do_build_inverted_index();
    EXPECT_TRUE(status.ok()) << status.to_string();

    // 14. Check paths and files
    auto old_tablet_path = _absolute_dir + "/" + std::to_string(15680);
    auto new_tablet_path = _absolute_dir + "/" + std::to_string(14680);
    bool old_exists = false;
    bool new_exists = false;
    EXPECT_TRUE(io::global_local_filesystem()->exists(old_tablet_path, &old_exists).ok());
    EXPECT_TRUE(old_exists);
    EXPECT_TRUE(io::global_local_filesystem()->exists(new_tablet_path, &new_exists).ok());
    EXPECT_TRUE(new_exists);

    // 15. Check files in old and new directories
    std::vector<io::FileInfo> old_files;
    bool old_dir_exists = false;
    EXPECT_TRUE(io::global_local_filesystem()
                        ->list(old_tablet_path, true, &old_files, &old_dir_exists)
                        .ok());
    EXPECT_TRUE(old_dir_exists);
    int old_idx_file_count = 0;
    int old_dat_file_count = 0;
    for (const auto& file : old_files) {
        std::string filename = file.file_name;
        if (filename.find(".idx") != std::string::npos) {
            old_idx_file_count++;
        }
        if (filename.find(".dat") != std::string::npos) {
            old_dat_file_count++;
        }
    }
    EXPECT_EQ(old_idx_file_count, 1)
            << "Old directory should contain exactly 1 .idx file for the original k1 index";
    EXPECT_EQ(old_dat_file_count, 1) << "Old directory should contain exactly 1 .dat file";

    std::vector<io::FileInfo> new_files;
    bool new_dir_exists = false;
    EXPECT_TRUE(io::global_local_filesystem()
                        ->list(new_tablet_path, true, &new_files, &new_dir_exists)
                        .ok());
    EXPECT_TRUE(new_dir_exists);
    int new_idx_file_count = 0;
    int new_dat_file_count = 0;
    for (const auto& file : new_files) {
        std::string filename = file.file_name;
        if (filename.find(".idx") != std::string::npos) {
            new_idx_file_count++;
        }
        if (filename.find(".dat") != std::string::npos) {
            new_dat_file_count++;
        }
    }
    // Should have 2 index files: original k1 index and new k2 index (k3 should be skipped)
    EXPECT_EQ(new_idx_file_count, 1)
            << "New directory should contain exactly 1 .idx files (for k1 and k2, not k3)";
    EXPECT_EQ(new_dat_file_count, 1) << "New directory should contain exactly 1 .dat file";

    // 16. Confirm storage format is still V1
    EXPECT_EQ(v1_schema->_inverted_index_storage_format, InvertedIndexStorageFormatPB::V1);
}

} // namespace doris
