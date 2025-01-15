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

#include <filesystem>
#include <map>
#include <string>

#include "olap/utils.h"
#include "util/index_compaction_utils.cpp"

namespace doris {

using namespace doris::vectorized;

constexpr static uint32_t MAX_PATH_LEN = 1024;
constexpr static std::string_view dest_dir = "./ut_dir/inverted_index_test";
constexpr static std::string_view tmp_dir = "./ut_dir/tmp";

class DISABLED_IndexCompactionPerformanceTest : public ::testing::Test {
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

        // set config
        config::inverted_index_dict_path =
                _current_dir + "/be/src/clucene/src/contribs-lib/CLucene/analysis/jieba/dict";
        config::enable_segcompaction = false;
        config::enable_ordered_data_compaction = false;
        config::total_permits_for_compaction_score = 200000;
        config::inverted_index_ram_dir_enable = true;
        config::string_type_length_soft_limit_bytes = 10485760;
    }
    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
        _engine_ref = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);

        // restore config
        config::inverted_index_max_buffered_docs = -1;
        config::compaction_batch_size = -1;
        config::inverted_index_compaction_enable = false;
        config::enable_segcompaction = true;
        config::enable_ordered_data_compaction = true;
        config::total_permits_for_compaction_score = 1000000;
        config::inverted_index_ram_dir_enable = true;
        config::string_type_length_soft_limit_bytes = 1048576;
    }

    DISABLED_IndexCompactionPerformanceTest() = default;
    ~DISABLED_IndexCompactionPerformanceTest() override = default;

    void _build_wiki_tablet(const KeysType& keys_type,
                            const InvertedIndexStorageFormatPB& storage_format,
                            const std::map<std::string, std::string>& properties) {
        // tablet_schema
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(keys_type);
        schema_pb.set_inverted_index_storage_format(storage_format);

        IndexCompactionUtils::construct_column(schema_pb.add_column(), 0, "STRING", "title");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10001,
                                               "idx_content", 1, "STRING", "content", properties);
        IndexCompactionUtils::construct_column(schema_pb.add_column(), 2, "STRING", "redirect");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), 3, "STRING", "namespace");
        if (keys_type == KeysType::UNIQUE_KEYS) {
            // unique table must contain the DELETE_SIGN column
            auto* column_pb = schema_pb.add_column();
            IndexCompactionUtils::construct_column(column_pb, 4, "TINYINT", DELETE_SIGN);
            column_pb->set_length(1);
            column_pb->set_index_length(1);
            column_pb->set_is_nullable(false);
        }
        _tablet_schema = std::make_shared<TabletSchema>();
        _tablet_schema->init_from_pb(schema_pb);

        // tablet
        TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
        if (keys_type == KeysType::UNIQUE_KEYS) {
            tablet_meta->_enable_unique_key_merge_on_write = true;
        }

        _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
        EXPECT_TRUE(_tablet->init().ok());
    }

    void _run_normal_wiki_test() {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());
        std::string data_dir =
                _current_dir + "/be/test/olap/rowset/segment_v2/inverted_index/data/performance";
        std::vector<std::string> data_files;
        for (const auto& entry : std::filesystem::directory_iterator(data_dir)) {
            if (entry.is_regular_file()) {
                std::string filename = entry.path().filename().string();
                if (filename.starts_with("wikipedia") && filename.ends_with(".json")) {
                    std::cout << "Found file: " << filename << std::endl;
                    data_files.push_back(entry.path().string());
                }
            }
        }

        std::vector<RowsetSharedPtr> rowsets(data_files.size());
        auto custom_check_build_rowsets = [](const int32_t& size) { EXPECT_EQ(size, 1); };
        IndexCompactionUtils::build_rowsets<IndexCompactionUtils::WikiDataRow>(
                _data_dir, _tablet_schema, _tablet, _engine_ref, rowsets, data_files, _inc_id,
                custom_check_build_rowsets, true, INT32_MAX);

        auto custom_check_index = [](const BaseCompaction& compaction,
                                     const RowsetWriterContext& ctx) {
            EXPECT_EQ(compaction._cur_tablet_schema->inverted_indexes().size(), 1);
            EXPECT_TRUE(ctx.columns_to_do_index_compaction.size() == 1);
            EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(1));
            EXPECT_TRUE(compaction._output_rowset->num_segments() == 1)
                    << compaction._output_rowset->num_segments();
        };

        RowsetSharedPtr output_rowset_index;
        Status st;
        {
            OlapStopWatch watch;
            st = IndexCompactionUtils::do_compaction(rowsets, _engine_ref, _tablet, true,
                                                     output_rowset_index, custom_check_index,
                                                     10000000);
            std::cout << "index compaction time: " << watch.get_elapse_second() << "s" << std::endl;
        }
        EXPECT_TRUE(st.ok()) << st.to_string();

        const auto& seg_path = output_rowset_index->segment_path(0);
        EXPECT_TRUE(seg_path.has_value()) << seg_path.error();
        auto inverted_index_file_reader_index = IndexCompactionUtils::init_index_file_reader(
                output_rowset_index, seg_path.value(),
                _tablet_schema->get_inverted_index_storage_format());

        auto custom_check_normal = [](const BaseCompaction& compaction,
                                      const RowsetWriterContext& ctx) {
            EXPECT_EQ(compaction._cur_tablet_schema->inverted_indexes().size(), 1);
            EXPECT_TRUE(ctx.columns_to_do_index_compaction.size() == 0);
            EXPECT_TRUE(compaction._output_rowset->num_segments() == 1);
        };

        RowsetSharedPtr output_rowset_normal;
        {
            OlapStopWatch watch;
            st = IndexCompactionUtils::do_compaction(rowsets, _engine_ref, _tablet, false,
                                                     output_rowset_normal, custom_check_normal,
                                                     10000000);
            std::cout << "normal compaction time: " << watch.get_elapse_second() << "s"
                      << std::endl;
        }
        EXPECT_TRUE(st.ok()) << st.to_string();
        const auto& seg_path_normal = output_rowset_normal->segment_path(0);
        EXPECT_TRUE(seg_path_normal.has_value()) << seg_path_normal.error();
        auto inverted_index_file_reader_normal = IndexCompactionUtils::init_index_file_reader(
                output_rowset_normal, seg_path_normal.value(),
                _tablet_schema->get_inverted_index_storage_format());

        // check index file terms
        for (int idx = 10001; idx < 10002; idx++) {
            auto dir_idx = inverted_index_file_reader_index->_open(idx, "");
            EXPECT_TRUE(dir_idx.has_value()) << dir_idx.error();
            auto dir_normal = inverted_index_file_reader_normal->_open(idx, "");
            EXPECT_TRUE(dir_normal.has_value()) << dir_normal.error();
            st = IndexCompactionUtils::check_idx_file_correctness(dir_idx->get(),
                                                                  dir_normal->get());
            EXPECT_TRUE(st.ok()) << st.to_string();
        }
    }

private:
    TabletSchemaSPtr _tablet_schema = nullptr;
    StorageEngine* _engine_ref = nullptr;
    std::unique_ptr<DataDir> _data_dir = nullptr;
    TabletSharedPtr _tablet = nullptr;
    std::string _absolute_dir;
    std::string _current_dir;
    int64_t _inc_id = 1000;
};

TEST_F(DISABLED_IndexCompactionPerformanceTest, tes_wikipedia_dup_v2_english) {
    std::map<std::string, std::string> properties;
    properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_ENGLISH);
    properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                       INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES);
    properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE);
    _build_wiki_tablet(KeysType::DUP_KEYS, InvertedIndexStorageFormatPB::V2, properties);
    _run_normal_wiki_test();
}

TEST_F(DISABLED_IndexCompactionPerformanceTest, tes_wikipedia_dup_v2_unicode) {
    std::map<std::string, std::string> properties;
    properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_UNICODE);
    properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                       INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES);
    properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE);
    _build_wiki_tablet(KeysType::DUP_KEYS, InvertedIndexStorageFormatPB::V2, properties);
    _run_normal_wiki_test();
}

TEST_F(DISABLED_IndexCompactionPerformanceTest, tes_wikipedia_dup_v2_chinese) {
    std::map<std::string, std::string> properties;
    properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_CHINESE);
    properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                       INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES);
    properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE);
    _build_wiki_tablet(KeysType::DUP_KEYS, InvertedIndexStorageFormatPB::V2, properties);
    _run_normal_wiki_test();
}

TEST_F(DISABLED_IndexCompactionPerformanceTest, tes_wikipedia_mow_v2_english) {
    std::map<std::string, std::string> properties;
    properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_ENGLISH);
    properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                       INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES);
    properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE);
    _build_wiki_tablet(KeysType::UNIQUE_KEYS, InvertedIndexStorageFormatPB::V2, properties);
    _run_normal_wiki_test();
}

TEST_F(DISABLED_IndexCompactionPerformanceTest, tes_wikipedia_mow_v2_unicode) {
    std::map<std::string, std::string> properties;
    properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_UNICODE);
    properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                       INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES);
    properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE);
    _build_wiki_tablet(KeysType::UNIQUE_KEYS, InvertedIndexStorageFormatPB::V2, properties);
    _run_normal_wiki_test();
}

TEST_F(DISABLED_IndexCompactionPerformanceTest, tes_wikipedia_mow_v2_chinese) {
    std::map<std::string, std::string> properties;
    properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_CHINESE);
    properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                       INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES);
    properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE);
    _build_wiki_tablet(KeysType::UNIQUE_KEYS, InvertedIndexStorageFormatPB::V2, properties);
    _run_normal_wiki_test();
}
} // namespace doris
