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

#include <gmock/gmock.h>

#include "util/index_compaction_utils.cpp"

namespace doris {

using namespace doris::vectorized;

constexpr static uint32_t MAX_PATH_LEN = 1024;
constexpr static std::string_view dest_dir = "./ut_dir/inverted_index_test";
constexpr static std::string_view tmp_dir = "./ut_dir/tmp";

class IndexCompactionTest : public ::testing::Test {
protected:
    void SetUp() override {
        // absolute dir
        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        _current_dir = std::string(buffer);
        _absolute_dir = _current_dir + std::string(dest_dir);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(_absolute_dir).ok());

        // tmp dir
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(tmp_dir).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(std::string(tmp_dir), 1024000000);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        Status st = tmp_file_dirs->init();
        EXPECT_TRUE(st.ok()) << st.to_json();
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));

        // storage engine
        doris::EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        _engine_ref = engine.get();
        _data_dir = std::make_unique<DataDir>(*_engine_ref, _absolute_dir);
        static_cast<void>(_data_dir->update_capacity());
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));

        // tablet_schema
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);
        schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);

        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10000,
                                               "key_index", 0, "INT", "key");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10001,
                                               "v1_index", 1, "STRING", "v1");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10002,
                                               "v2_index", 2, "STRING", "v2", true);
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10003,
                                               "v3_index", 3, "INT", "v3");
        _tablet_schema = std::make_shared<TabletSchema>();
        _tablet_schema->init_from_pb(schema_pb);

        // tablet
        TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));

        _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
        EXPECT_TRUE(_tablet->init().ok());
    }
    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
        _engine_ref = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    IndexCompactionTest() = default;
    ~IndexCompactionTest() override = default;

private:
    TabletSchemaSPtr _tablet_schema = nullptr;
    StorageEngine* _engine_ref = nullptr;
    std::unique_ptr<DataDir> _data_dir = nullptr;
    TabletSharedPtr _tablet = nullptr;
    std::string _absolute_dir;
    std::string _current_dir;
};

TEST_F(IndexCompactionTest, tes_write_index_normally) {
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());
    std::string data_file1 =
            _current_dir + "/be/test/olap/rowset/segment_v2/inverted_index/data/data1.csv";
    std::string data_file2 =
            _current_dir + "/be/test/olap/rowset/segment_v2/inverted_index/data/data2.csv";
    std::vector<std::string> data_files;
    data_files.push_back(data_file1);
    data_files.push_back(data_file2);

    std::vector<RowsetSharedPtr> rowsets(data_files.size());
    auto custom_check_build_rowsets = [](const int32_t& size) { EXPECT_EQ(size, 4); };
    IndexCompactionUtils::build_rowsets(_data_dir, _tablet_schema, _tablet, _engine_ref, rowsets,
                                        data_files, custom_check_build_rowsets);

    auto custom_check_index = [](const BaseCompaction& compaction, const RowsetWriterContext& ctx) {
        EXPECT_EQ(compaction._cur_tablet_schema->inverted_indexes().size(), 4);
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.size() == 2);
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(1));
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(2));
        EXPECT_TRUE(compaction._output_rowset->num_segments() == 1);
    };

    RowsetSharedPtr output_rowset_index;
    auto st = IndexCompactionUtils::do_compaction(rowsets, _engine_ref, _tablet, true,
                                                  output_rowset_index, custom_check_index);
    EXPECT_TRUE(st.ok()) << st.to_string();

    const auto& seg_path = output_rowset_index->segment_path(0);
    EXPECT_TRUE(seg_path.has_value()) << seg_path.error();
    auto inverted_index_file_reader_index = IndexCompactionUtils::init_index_file_reader(
            output_rowset_index, seg_path.value(),
            _tablet_schema->get_inverted_index_storage_format());

    auto custom_check_normal = [](const BaseCompaction& compaction,
                                  const RowsetWriterContext& ctx) {
        EXPECT_EQ(compaction._cur_tablet_schema->inverted_indexes().size(), 4);
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.size() == 0);
        EXPECT_TRUE(compaction._output_rowset->num_segments() == 1);
    };

    RowsetSharedPtr output_rowset_normal;
    st = IndexCompactionUtils::do_compaction(rowsets, _engine_ref, _tablet, false,
                                             output_rowset_normal, custom_check_normal);
    EXPECT_TRUE(st.ok()) << st.to_string();
    const auto& seg_path_normal = output_rowset_normal->segment_path(0);
    EXPECT_TRUE(seg_path_normal.has_value()) << seg_path_normal.error();
    auto inverted_index_file_reader_normal = IndexCompactionUtils::init_index_file_reader(
            output_rowset_normal, seg_path_normal.value(),
            _tablet_schema->get_inverted_index_storage_format());
    // check index file terms
    auto dir_idx_compaction = inverted_index_file_reader_index->_open(10001, "");
    EXPECT_TRUE(dir_idx_compaction.has_value()) << dir_idx_compaction.error();
    auto dir_normal_compaction = inverted_index_file_reader_normal->_open(10001, "");
    EXPECT_TRUE(dir_normal_compaction.has_value()) << dir_normal_compaction.error();
    std::ostringstream oss;
    IndexCompactionUtils::check_terms_stats(dir_idx_compaction->get(), oss);
    std::string output = oss.str();
    EXPECT_EQ(output, expected_output);
    oss.str("");
    oss.clear();
    IndexCompactionUtils::check_terms_stats(dir_normal_compaction->get(), oss);
    output = oss.str();
    EXPECT_EQ(output, expected_output);

    st = IndexCompactionUtils::check_idx_file_correctness(dir_idx_compaction->get(),
                                                          dir_normal_compaction->get());
    EXPECT_TRUE(st.ok()) << st.to_string();

    // check meta and file
    std::map<int, QueryData> query_map = {
            {0, {{"99", "66", "56", "87", "85", "96", "20000"}, {21, 25, 22, 18, 14, 18, 0}}},
            {3, {{"99", "66", "56", "87", "85", "96", "10000"}, {12, 20, 25, 23, 16, 24, 0}}},
            {1, {{"good", "maybe", "great", "null"}, {197, 191, 194, 0}}},
            {2, {{"musicstream.com", "http", "https", "null"}, {191, 799, 1201, 0}}}};
    IndexCompactionUtils::check_meta_and_file(output_rowset_index, _tablet_schema, query_map);
    IndexCompactionUtils::check_meta_and_file(output_rowset_normal, _tablet_schema, query_map);
}

TEST_F(IndexCompactionTest, test_col_unique_ids_empty) {
    // clear column unique id in tablet index 10001 and rebuild tablet_schema
    TabletSchemaPB schema_pb;
    _tablet_schema->to_schema_pb(&schema_pb);
    auto* index_pb = schema_pb.mutable_index(1);
    index_pb->clear_col_unique_id();
    _tablet_schema->init_from_pb(schema_pb);

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());
    std::string data_file1 =
            _current_dir + "/be/test/olap/rowset/segment_v2/inverted_index/data/data1.csv";
    std::string data_file2 =
            _current_dir + "/be/test/olap/rowset/segment_v2/inverted_index/data/data2.csv";
    std::vector<std::string> data_files;
    data_files.push_back(data_file1);
    data_files.push_back(data_file2);

    std::vector<RowsetSharedPtr> rowsets(data_files.size());
    auto custom_check_build_rowsets = [](const int32_t& size) { EXPECT_EQ(size, 3); };
    IndexCompactionUtils::build_rowsets(_data_dir, _tablet_schema, _tablet, _engine_ref, rowsets,
                                        data_files, custom_check_build_rowsets);

    auto custom_check_index = [](const BaseCompaction& compaction, const RowsetWriterContext& ctx) {
        EXPECT_EQ(compaction._cur_tablet_schema->inverted_indexes().size(), 4);
        // only index id 10002 will do index compaction
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.size() == 1);
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(2));
        EXPECT_TRUE(compaction._output_rowset->num_segments() == 1);
    };

    RowsetSharedPtr output_rowset_index;
    auto st = IndexCompactionUtils::do_compaction(rowsets, _engine_ref, _tablet, true,
                                                  output_rowset_index, custom_check_index);
    EXPECT_TRUE(st.ok()) << st.to_string();

    const auto& seg_path = output_rowset_index->segment_path(0);
    EXPECT_TRUE(seg_path.has_value()) << seg_path.error();
    auto inverted_index_file_reader_index = IndexCompactionUtils::init_index_file_reader(
            output_rowset_index, seg_path.value(),
            _tablet_schema->get_inverted_index_storage_format());

    // check index file
    // index 10001 cannot be found in idx file
    auto dir_idx_compaction = inverted_index_file_reader_index->_open(10001, "");
    EXPECT_TRUE(!dir_idx_compaction.has_value()) << dir_idx_compaction.error();
    EXPECT_THAT(dir_idx_compaction.error().to_string(),
                testing::HasSubstr("No index with id 10001 found"));
}

TEST_F(IndexCompactionTest, test_tablet_index_id_not_equal) {
    // replace unique id from 2 to 1 in tablet index 10002 and rebuild tablet_schema
    TabletSchemaPB schema_pb;
    _tablet_schema->to_schema_pb(&schema_pb);
    auto* index_pb = schema_pb.mutable_index(2);
    index_pb->set_col_unique_id(0, 1);
    _tablet_schema->init_from_pb(schema_pb);

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());
    std::string data_file1 =
            _current_dir + "/be/test/olap/rowset/segment_v2/inverted_index/data/data1.csv";
    std::string data_file2 =
            _current_dir + "/be/test/olap/rowset/segment_v2/inverted_index/data/data2.csv";
    std::vector<std::string> data_files;
    data_files.push_back(data_file1);
    data_files.push_back(data_file2);

    std::vector<RowsetSharedPtr> rowsets(data_files.size());
    auto custom_check_build_rowsets = [](const int32_t& size) { EXPECT_EQ(size, 3); };
    IndexCompactionUtils::build_rowsets(_data_dir, _tablet_schema, _tablet, _engine_ref, rowsets,
                                        data_files, custom_check_build_rowsets);

    auto custom_check_index = [](const BaseCompaction& compaction, const RowsetWriterContext& ctx) {
        EXPECT_EQ(compaction._cur_tablet_schema->inverted_indexes().size(), 4);
        // only index id 10001 will do index compaction
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.size() == 1);
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(1));
        EXPECT_TRUE(compaction._output_rowset->num_segments() == 1);
    };

    RowsetSharedPtr output_rowset_index;
    auto st = IndexCompactionUtils::do_compaction(rowsets, _engine_ref, _tablet, true,
                                                  output_rowset_index, custom_check_index);
    EXPECT_TRUE(st.ok()) << st.to_string();

    const auto& seg_path = output_rowset_index->segment_path(0);
    EXPECT_TRUE(seg_path.has_value()) << seg_path.error();
    auto inverted_index_file_reader_index = IndexCompactionUtils::init_index_file_reader(
            output_rowset_index, seg_path.value(),
            _tablet_schema->get_inverted_index_storage_format());

    // check index file
    // index 10002 cannot be found in idx file
    auto dir_idx_compaction = inverted_index_file_reader_index->_open(10002, "");
    EXPECT_TRUE(!dir_idx_compaction.has_value()) << dir_idx_compaction.error();
    EXPECT_THAT(dir_idx_compaction.error().to_string(),
                testing::HasSubstr("No index with id 10002 found"));
}

TEST_F(IndexCompactionTest, test_tablet_schema_tablet_index_is_null) {
    // set index suffix in tablet index 10001 and rebuild tablet_schema
    // simulate the case that index is null, tablet_schema->inverted_index(1) will return nullptr
    TabletSchemaPB schema_pb;
    _tablet_schema->to_schema_pb(&schema_pb);
    auto* index_pb = schema_pb.mutable_index(1);
    index_pb->set_index_suffix_name("mock");
    _tablet_schema->init_from_pb(schema_pb);

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());
    std::string data_file1 =
            _current_dir + "/be/test/olap/rowset/segment_v2/inverted_index/data/data1.csv";
    std::string data_file2 =
            _current_dir + "/be/test/olap/rowset/segment_v2/inverted_index/data/data2.csv";
    std::vector<std::string> data_files;
    data_files.push_back(data_file1);
    data_files.push_back(data_file2);

    std::vector<RowsetSharedPtr> rowsets(data_files.size());
    auto custom_check_build_rowsets = [](const int32_t& size) { EXPECT_EQ(size, 3); };
    IndexCompactionUtils::build_rowsets(_data_dir, _tablet_schema, _tablet, _engine_ref, rowsets,
                                        data_files, custom_check_build_rowsets);

    auto custom_check_index = [](const BaseCompaction& compaction, const RowsetWriterContext& ctx) {
        EXPECT_EQ(compaction._cur_tablet_schema->inverted_indexes().size(), 4);
        // only index id 10002 will do index compaction
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.size() == 1);
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(2));
        EXPECT_TRUE(compaction._output_rowset->num_segments() == 1);
    };

    RowsetSharedPtr output_rowset_index;
    auto st = IndexCompactionUtils::do_compaction(rowsets, _engine_ref, _tablet, true,
                                                  output_rowset_index, custom_check_index);
    EXPECT_TRUE(st.ok()) << st.to_string();

    const auto& seg_path = output_rowset_index->segment_path(0);
    EXPECT_TRUE(seg_path.has_value()) << seg_path.error();
    auto inverted_index_file_reader_index = IndexCompactionUtils::init_index_file_reader(
            output_rowset_index, seg_path.value(),
            _tablet_schema->get_inverted_index_storage_format());

    // check index file
    // index 10001 cannot be found in idx file
    auto dir_idx_compaction = inverted_index_file_reader_index->_open(10001, "");
    EXPECT_TRUE(!dir_idx_compaction.has_value()) << dir_idx_compaction.error();
    EXPECT_THAT(dir_idx_compaction.error().to_string(),
                testing::HasSubstr("No index with id 10001 found"));
}

TEST_F(IndexCompactionTest, test_rowset_schema_tablet_index_is_null) {
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());
    std::string data_file1 =
            _current_dir + "/be/test/olap/rowset/segment_v2/inverted_index/data/data1.csv";
    std::string data_file2 =
            _current_dir + "/be/test/olap/rowset/segment_v2/inverted_index/data/data2.csv";
    std::vector<std::string> data_files;
    data_files.push_back(data_file1);
    data_files.push_back(data_file2);

    std::vector<RowsetSharedPtr> rowsets(data_files.size());
    auto custom_check_build_rowsets = [](const int32_t& size) { EXPECT_EQ(size, 4); };
    IndexCompactionUtils::build_rowsets(_data_dir, _tablet_schema, _tablet, _engine_ref, rowsets,
                                        data_files, custom_check_build_rowsets);

    auto custom_check_index = [](const BaseCompaction& compaction, const RowsetWriterContext& ctx) {
        EXPECT_EQ(compaction._cur_tablet_schema->inverted_indexes().size(), 4);
        // only index id 10002 will do index compaction
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.size() == 1);
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(2));
        EXPECT_TRUE(compaction._output_rowset->num_segments() == 1);
    };

    // set index suffix in tablet index 10001 and rebuild tablet_schema
    // simulate the case that index is null, tablet_schema->inverted_index(1) will return nullptr
    TabletSchemaPB schema_pb;
    TabletSchemaSPtr mock_schema = std::make_shared<TabletSchema>();
    _tablet_schema->to_schema_pb(&schema_pb);
    auto* index_pb = schema_pb.mutable_index(1);
    index_pb->set_index_suffix_name("mock");
    mock_schema->init_from_pb(schema_pb);
    rowsets[0]->_schema = mock_schema;

    RowsetSharedPtr output_rowset_index;
    auto st = IndexCompactionUtils::do_compaction(rowsets, _engine_ref, _tablet, true,
                                                  output_rowset_index, custom_check_index);
    EXPECT_TRUE(st.ok()) << st.to_string();

    const auto& seg_path = output_rowset_index->segment_path(0);
    EXPECT_TRUE(seg_path.has_value()) << seg_path.error();
    auto inverted_index_file_reader_index = IndexCompactionUtils::init_index_file_reader(
            output_rowset_index, seg_path.value(),
            _tablet_schema->get_inverted_index_storage_format());

    // check index file
    // index 10001 cannot be found in idx file
    auto dir_idx_compaction = inverted_index_file_reader_index->_open(10001, "");
    EXPECT_TRUE(dir_idx_compaction.has_value()) << dir_idx_compaction.error();
    // check index 10001 term stats
    std::ostringstream oss;
    IndexCompactionUtils::check_terms_stats(dir_idx_compaction.value().get(), oss);
    std::string output = oss.str();
    EXPECT_EQ(output, expected_output);
}

TEST_F(IndexCompactionTest, test_tablet_index_properties_not_equal) {
    // add mock property in tablet index 10001 and rebuild tablet_schema
    // simulate the case that index properties not equal among input rowsets
    TabletSchemaSPtr mock_schema = std::make_shared<TabletSchema>();
    TabletSchemaPB schema_pb;
    _tablet_schema->to_schema_pb(&schema_pb);
    auto* index_pb = schema_pb.mutable_index(1);
    (*index_pb->mutable_properties())["mock_key"] = "mock_value";
    mock_schema->init_from_pb(schema_pb);

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());
    std::string data_file1 =
            _current_dir + "/be/test/olap/rowset/segment_v2/inverted_index/data/data1.csv";
    std::string data_file2 =
            _current_dir + "/be/test/olap/rowset/segment_v2/inverted_index/data/data2.csv";
    std::vector<std::string> data_files;
    data_files.push_back(data_file1);
    data_files.push_back(data_file2);

    std::vector<RowsetSharedPtr> rowsets(data_files.size());
    auto custom_check_build_rowsets = [](const int32_t& size) { EXPECT_EQ(size, 4); };
    IndexCompactionUtils::build_rowsets(_data_dir, _tablet_schema, _tablet, _engine_ref, rowsets,
                                        data_files, custom_check_build_rowsets);

    auto custom_check_index = [](const BaseCompaction& compaction, const RowsetWriterContext& ctx) {
        EXPECT_EQ(compaction._cur_tablet_schema->inverted_indexes().size(), 4);
        // only index id 10002 will do index compaction
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.size() == 1);
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(2));
        EXPECT_TRUE(compaction._output_rowset->num_segments() == 1);
    };

    // set mock_schema to the first input rowset
    rowsets[0]->_schema = mock_schema;
    RowsetSharedPtr output_rowset_index;
    auto st = IndexCompactionUtils::do_compaction(rowsets, _engine_ref, _tablet, true,
                                                  output_rowset_index, custom_check_index);
    EXPECT_TRUE(st.ok()) << st.to_string();

    const auto& seg_path = output_rowset_index->segment_path(0);
    EXPECT_TRUE(seg_path.has_value()) << seg_path.error();
    auto inverted_index_file_reader_index = IndexCompactionUtils::init_index_file_reader(
            output_rowset_index, seg_path.value(),
            _tablet_schema->get_inverted_index_storage_format());

    // check index file
    auto dir_idx_compaction = inverted_index_file_reader_index->_open(10001, "");
    EXPECT_TRUE(dir_idx_compaction.has_value()) << dir_idx_compaction.error();

    // check index 10001 term stats
    std::ostringstream oss;
    IndexCompactionUtils::check_terms_stats(dir_idx_compaction.value().get(), oss);
    std::string output = oss.str();
    EXPECT_EQ(output, expected_output);
}

TEST_F(IndexCompactionTest, test_is_skip_index_compaction_not_empty) {
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());
    std::string data_file1 =
            _current_dir + "/be/test/olap/rowset/segment_v2/inverted_index/data/data1.csv";
    std::string data_file2 =
            _current_dir + "/be/test/olap/rowset/segment_v2/inverted_index/data/data2.csv";
    std::vector<std::string> data_files;
    data_files.push_back(data_file1);
    data_files.push_back(data_file2);

    std::vector<RowsetSharedPtr> rowsets(data_files.size());
    auto custom_check_build_rowsets = [](const int32_t& size) { EXPECT_EQ(size, 4); };
    IndexCompactionUtils::build_rowsets(_data_dir, _tablet_schema, _tablet, _engine_ref, rowsets,
                                        data_files, custom_check_build_rowsets);

    auto custom_check_index = [](const BaseCompaction& compaction, const RowsetWriterContext& ctx) {
        EXPECT_EQ(compaction._cur_tablet_schema->inverted_indexes().size(), 4);
        // only index id 10002 will do index compaction
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.size() == 1);
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(2));
        EXPECT_TRUE(compaction._output_rowset->num_segments() == 1);
    };

    // set col_unique_id=1(index_id=10001) to skip index compaction
    rowsets[0]->set_skip_index_compaction(1);
    RowsetSharedPtr output_rowset_index;
    auto st = IndexCompactionUtils::do_compaction(rowsets, _engine_ref, _tablet, true,
                                                  output_rowset_index, custom_check_index);
    EXPECT_TRUE(st.ok()) << st.to_string();

    const auto& seg_path = output_rowset_index->segment_path(0);
    EXPECT_TRUE(seg_path.has_value()) << seg_path.error();
    auto inverted_index_file_reader_index = IndexCompactionUtils::init_index_file_reader(
            output_rowset_index, seg_path.value(),
            _tablet_schema->get_inverted_index_storage_format());

    // check index file
    auto dir_idx_compaction = inverted_index_file_reader_index->_open(10001, "");
    EXPECT_TRUE(dir_idx_compaction.has_value()) << dir_idx_compaction.error();

    // check index 10001 term stats
    std::ostringstream oss;
    IndexCompactionUtils::check_terms_stats(dir_idx_compaction.value().get(), oss);
    std::string output = oss.str();
    EXPECT_EQ(output, expected_output);
}

TEST_F(IndexCompactionTest, test_rowset_fs_nullptr) {
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());
    std::string data_file1 =
            _current_dir + "/be/test/olap/rowset/segment_v2/inverted_index/data/data1.csv";
    std::string data_file2 =
            _current_dir + "/be/test/olap/rowset/segment_v2/inverted_index/data/data2.csv";
    std::vector<std::string> data_files;
    data_files.push_back(data_file1);
    data_files.push_back(data_file2);

    std::vector<RowsetSharedPtr> rowsets(data_files.size());
    auto custom_check_build_rowsets = [](const int32_t& size) { EXPECT_EQ(size, 4); };
    IndexCompactionUtils::build_rowsets(_data_dir, _tablet_schema, _tablet, _engine_ref, rowsets,
                                        data_files, custom_check_build_rowsets);

    auto custom_check_index = [](const BaseCompaction& compaction, const RowsetWriterContext& ctx) {
        EXPECT_EQ(compaction._cur_tablet_schema->inverted_indexes().size(), 4);
        // only index id 10002 will do index compaction
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.size() == 1);
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(2));
        EXPECT_TRUE(compaction._output_rowset->num_segments() == 1);
    };

    // set mock_id to resource_id to simulate getting fs nullptr
    RowsetMetaSharedPtr mock_rowset_meta = std::make_shared<RowsetMeta>();
    RowsetMetaPB rs_meta_pb;
    rowsets[0]->to_rowset_pb(&rs_meta_pb);
    rs_meta_pb.set_resource_id("mock_id");
    mock_rowset_meta->init_from_pb(rs_meta_pb);
    rowsets[0]->_rowset_meta = mock_rowset_meta;
    RowsetSharedPtr output_rowset_index;
    auto st = IndexCompactionUtils::do_compaction(rowsets, _engine_ref, _tablet, true,
                                                  output_rowset_index, custom_check_index);
    EXPECT_TRUE(!st.ok());
    EXPECT_THAT(st.to_string(), testing::HasSubstr("[E-206]get fs failed"));
}

TEST_F(IndexCompactionTest, test_input_row_num_zero) {
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());
    std::string data_file1 =
            _current_dir + "/be/test/olap/rowset/segment_v2/inverted_index/data/data1.csv";
    std::string data_file2 =
            _current_dir + "/be/test/olap/rowset/segment_v2/inverted_index/data/data2.csv";
    std::vector<std::string> data_files;
    data_files.push_back(data_file1);
    data_files.push_back(data_file2);

    std::vector<RowsetSharedPtr> rowsets(data_files.size());
    auto custom_check_build_rowsets = [](const int32_t& size) { EXPECT_EQ(size, 4); };
    IndexCompactionUtils::build_rowsets(_data_dir, _tablet_schema, _tablet, _engine_ref, rowsets,
                                        data_files, custom_check_build_rowsets);

    auto custom_check_index = [](const BaseCompaction& compaction, const RowsetWriterContext& ctx) {
        EXPECT_EQ(compaction._cur_tablet_schema->inverted_indexes().size(), 4);
        // only index id 10002 will do index compaction
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.size() == 2);
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(1));
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(2));
        EXPECT_TRUE(compaction._output_rowset->num_segments() == 1);
    };

    // set num_rows to 0 to simulate input_row_num = 0
    for (auto rowset : rowsets) {
        RowsetMetaSharedPtr mock_rowset_meta = std::make_shared<RowsetMeta>();
        RowsetMetaPB rs_meta_pb;
        rowset->to_rowset_pb(&rs_meta_pb);
        rs_meta_pb.set_num_rows(0);
        mock_rowset_meta->init_from_pb(rs_meta_pb);
        rowset->_rowset_meta = mock_rowset_meta;
    }

    RowsetSharedPtr output_rowset_index;
    auto st = IndexCompactionUtils::do_compaction(rowsets, _engine_ref, _tablet, true,
                                                  output_rowset_index, custom_check_index);
    EXPECT_TRUE(st.ok());
    const auto& seg_path = output_rowset_index->segment_path(0);
    EXPECT_TRUE(seg_path.has_value()) << seg_path.error();
    auto inverted_index_file_reader_index = IndexCompactionUtils::init_index_file_reader(
            output_rowset_index, seg_path.value(),
            _tablet_schema->get_inverted_index_storage_format());

    // check index file
    // index 10001 cannot be found in idx file
    auto dir_idx_compaction = inverted_index_file_reader_index->_open(10001, "");
    EXPECT_TRUE(!dir_idx_compaction.has_value()) << dir_idx_compaction.error();
    EXPECT_THAT(dir_idx_compaction.error().to_string(),
                testing::HasSubstr("No index with id 10001 found"));
}

TEST_F(IndexCompactionTest, test_cols_to_do_index_compaction_empty) {
    // add mock property in tablet index 10001, 10002 and rebuild tablet_schema
    // simulate the case that index properties not equal among input rowsets
    // the two cols will skip index compaction and make ctx.columns_to_do_index_compaction empty
    TabletSchemaSPtr mock_schema = std::make_shared<TabletSchema>();
    TabletSchemaPB schema_pb;
    _tablet_schema->to_schema_pb(&schema_pb);
    auto* index_pb_1 = schema_pb.mutable_index(1);
    (*index_pb_1->mutable_properties())["mock_key"] = "mock_value";
    auto* index_pb_2 = schema_pb.mutable_index(2);
    (*index_pb_2->mutable_properties())["mock_key"] = "mock_value";
    mock_schema->init_from_pb(schema_pb);

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());
    std::string data_file1 =
            _current_dir + "/be/test/olap/rowset/segment_v2/inverted_index/data/data1.csv";
    std::string data_file2 =
            _current_dir + "/be/test/olap/rowset/segment_v2/inverted_index/data/data2.csv";
    std::vector<std::string> data_files;
    data_files.push_back(data_file1);
    data_files.push_back(data_file2);

    std::vector<RowsetSharedPtr> rowsets(data_files.size());
    auto custom_check_build_rowsets = [](const int32_t& size) { EXPECT_EQ(size, 4); };
    IndexCompactionUtils::build_rowsets(_data_dir, _tablet_schema, _tablet, _engine_ref, rowsets,
                                        data_files, custom_check_build_rowsets);

    auto custom_check_index = [](const BaseCompaction& compaction, const RowsetWriterContext& ctx) {
        EXPECT_EQ(compaction._cur_tablet_schema->inverted_indexes().size(), 4);
        // none index will do index compaction
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.size() == 0);
        EXPECT_TRUE(compaction._output_rowset->num_segments() == 1);
    };

    // set mock_schema to the first input rowset
    rowsets[0]->_schema = mock_schema;
    RowsetSharedPtr output_rowset_index;
    auto st = IndexCompactionUtils::do_compaction(rowsets, _engine_ref, _tablet, true,
                                                  output_rowset_index, custom_check_index);
    EXPECT_TRUE(st.ok()) << st.to_string();

    const auto& seg_path = output_rowset_index->segment_path(0);
    EXPECT_TRUE(seg_path.has_value()) << seg_path.error();
    auto inverted_index_file_reader_index = IndexCompactionUtils::init_index_file_reader(
            output_rowset_index, seg_path.value(),
            _tablet_schema->get_inverted_index_storage_format());

    // check index file
    auto dir_idx_compaction_1 = inverted_index_file_reader_index->_open(10001, "");
    EXPECT_TRUE(dir_idx_compaction_1.has_value()) << dir_idx_compaction_1.error();

    // check index 10001 term stats
    std::ostringstream oss;
    IndexCompactionUtils::check_terms_stats(dir_idx_compaction_1.value().get(), oss);
    std::string output = oss.str();
    EXPECT_EQ(output, expected_output);

    auto dir_idx_compaction_2 = inverted_index_file_reader_index->_open(10002, "");
    EXPECT_TRUE(dir_idx_compaction_2.has_value()) << dir_idx_compaction_2.error();
}

TEST_F(IndexCompactionTest, test_index_compaction_with_delete) {
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());
    std::string data_file1 =
            _current_dir + "/be/test/olap/rowset/segment_v2/inverted_index/data/data1.csv";
    std::string data_file2 =
            _current_dir + "/be/test/olap/rowset/segment_v2/inverted_index/data/data2.csv";
    std::vector<std::string> data_files;
    data_files.push_back(data_file1);
    data_files.push_back(data_file2);

    std::vector<RowsetSharedPtr> rowsets(data_files.size());
    auto custom_check_build_rowsets = [](const int32_t& size) { EXPECT_EQ(size, 4); };
    IndexCompactionUtils::build_rowsets(_data_dir, _tablet_schema, _tablet, _engine_ref, rowsets,
                                        data_files, custom_check_build_rowsets);

    // create delete predicate rowset and add to tablet
    auto delete_rowset = IndexCompactionUtils::create_delete_predicate_rowset(
            _tablet_schema, "v1='great'", inc_id++);
    EXPECT_TRUE(_tablet->add_rowset(delete_rowset).ok());
    EXPECT_TRUE(_tablet->rowset_map().size() == 3);
    rowsets.push_back(delete_rowset);
    EXPECT_TRUE(rowsets.size() == 3);

    auto custom_check_index = [](const BaseCompaction& compaction, const RowsetWriterContext& ctx) {
        EXPECT_EQ(compaction._cur_tablet_schema->inverted_indexes().size(), 4);
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.size() == 2);
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(1));
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(2));
        EXPECT_TRUE(compaction._output_rowset->num_segments() == 1);
    };

    RowsetSharedPtr output_rowset_index;
    auto st = IndexCompactionUtils::do_compaction(rowsets, _engine_ref, _tablet, true,
                                                  output_rowset_index, custom_check_index);
    EXPECT_TRUE(st.ok()) << st.to_string();

    const auto& seg_path = output_rowset_index->segment_path(0);
    EXPECT_TRUE(seg_path.has_value()) << seg_path.error();
    auto inverted_index_file_reader_index = IndexCompactionUtils::init_index_file_reader(
            output_rowset_index, seg_path.value(),
            _tablet_schema->get_inverted_index_storage_format());

    auto custom_check_normal = [](const BaseCompaction& compaction,
                                  const RowsetWriterContext& ctx) {
        EXPECT_EQ(compaction._cur_tablet_schema->inverted_indexes().size(), 4);
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.size() == 0);
        EXPECT_TRUE(compaction._output_rowset->num_segments() == 1);
    };

    RowsetSharedPtr output_rowset_normal;
    st = IndexCompactionUtils::do_compaction(rowsets, _engine_ref, _tablet, false,
                                             output_rowset_normal, custom_check_normal);
    EXPECT_TRUE(st.ok()) << st.to_string();
    const auto& seg_path_normal = output_rowset_normal->segment_path(0);
    EXPECT_TRUE(seg_path_normal.has_value()) << seg_path_normal.error();
    auto inverted_index_file_reader_normal = IndexCompactionUtils::init_index_file_reader(
            output_rowset_normal, seg_path_normal.value(),
            _tablet_schema->get_inverted_index_storage_format());
    // check index file terms
    auto dir_idx_compaction = inverted_index_file_reader_index->_open(10001, "");
    EXPECT_TRUE(dir_idx_compaction.has_value()) << dir_idx_compaction.error();
    auto dir_normal_compaction = inverted_index_file_reader_normal->_open(10001, "");
    EXPECT_TRUE(dir_normal_compaction.has_value()) << dir_normal_compaction.error();
    std::ostringstream oss;
    IndexCompactionUtils::check_terms_stats(dir_idx_compaction->get(), oss);
    std::string output = oss.str();
    EXPECT_EQ(output, expected_delete_output);
    oss.str("");
    oss.clear();
    IndexCompactionUtils::check_terms_stats(dir_normal_compaction->get(), oss);
    output = oss.str();
    EXPECT_EQ(output, expected_delete_output);

    st = IndexCompactionUtils::check_idx_file_correctness(dir_idx_compaction->get(),
                                                          dir_normal_compaction->get());
    EXPECT_TRUE(st.ok()) << st.to_string();

    // check meta and file
    std::map<int, QueryData> query_map = {
            {0, {{"99", "66", "56", "87", "85", "96", "20000"}, {19, 21, 21, 16, 14, 18, 0}}},
            {3, {{"99", "66", "56", "87", "85", "96", "10000"}, {12, 18, 22, 21, 16, 20, 0}}},
            {1, {{"good", "maybe", "great", "null"}, {197, 191, 0, 0}}},
            {2, {{"musicstream.com", "http", "https", "null"}, {176, 719, 1087, 0}}}};
    IndexCompactionUtils::check_meta_and_file(output_rowset_index, _tablet_schema, query_map);
    IndexCompactionUtils::check_meta_and_file(output_rowset_normal, _tablet_schema, query_map);
}

} // namespace doris
