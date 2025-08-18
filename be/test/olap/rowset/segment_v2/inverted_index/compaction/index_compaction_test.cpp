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

#include "olap/utils.h"
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

        // set config
        config::inverted_index_dict_path =
                _current_dir + "/contrib/clucene/src/contribs-lib/CLucene/analysis/jieba/dict";
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

    IndexCompactionTest() = default;
    ~IndexCompactionTest() override = default;

    void _build_tablet() {
        // tablet_schema
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);
        schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);

        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10000,
                                               "key_index", 0, "INT", "key");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10001,
                                               "v1_index", 1, "STRING", "v1");
        std::map<std::string, std::string> properties;
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_UNICODE);
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10002,
                                               "v2_index", 2, "STRING", "v2", properties);
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10003,
                                               "v3_index", 3, "INT", "v3");
        _tablet_schema = std::make_shared<TabletSchema>();
        _tablet_schema->init_from_pb(schema_pb);

        // tablet
        TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));

        _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
        EXPECT_TRUE(_tablet->init().ok());
    }

    void _build_wiki_tablet(const KeysType& keys_type,
                            const InvertedIndexStorageFormatPB& storage_format) {
        // tablet_schema
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(keys_type);
        schema_pb.set_inverted_index_storage_format(storage_format);

        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10000,
                                               "idx_title", 0, "STRING", "title",
                                               std::map<std::string, std::string>(), true);
        // parser = english, support_phrase = true, lower_case = true, char_filter = none
        std::map<std::string, std::string> properties;
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_ENGLISH);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, "");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10001,
                                               "idx_content_1", 1, "STRING", "content_1",
                                               properties);
        properties.clear();
        // parser = english, support_phrase = true, lower_case = true, char_filter = char_replace
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_ENGLISH);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "char_replace");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "._");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, " ");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10002,
                                               "idx_content_2", 2, "STRING", "content_2",
                                               properties);
        properties.clear();
        // parser = english, support_phrase = true, lower_case = false, char_filter = none
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_ENGLISH);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_FALSE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, "");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10003,
                                               "idx_content_3", 3, "STRING", "content_3",
                                               properties);
        properties.clear();
        // parser = english, support_phrase = true, lower_case = false, char_filter = char_replace
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_ENGLISH);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_FALSE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "char_replace");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "._");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, " ");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10004,
                                               "idx_content_4", 4, "STRING", "content_4",
                                               properties);
        properties.clear();
        // parser = english, support_phrase = false, lower_case = true, char_filter = none
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_ENGLISH);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_NO);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, "");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10005,
                                               "idx_content_5", 5, "STRING", "content_5",
                                               properties);
        properties.clear();
        // parser = english, support_phrase = false, lower_case = true, char_filter = char_replace
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_ENGLISH);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_NO);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "char_replace");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "._");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, " ");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10006,
                                               "idx_content_6", 6, "STRING", "content_6",
                                               properties);
        properties.clear();
        // parser = english, support_phrase = false, lower_case = false, char_filter = none
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_ENGLISH);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_NO);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_FALSE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, "");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10007,
                                               "idx_content_7", 7, "STRING", "content_7",
                                               properties);
        properties.clear();
        // parser = english, support_phrase = false, lower_case = false, char_filter = char_replace
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_ENGLISH);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_NO);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_FALSE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "char_replace");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "._");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, " ");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10008,
                                               "idx_content_8", 8, "STRING", "content_8",
                                               properties);
        properties.clear();
        // parser = unicode, support_phrase = true, lower_case = true, char_filter = none
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_UNICODE);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, "");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10009,
                                               "idx_content_9", 9, "STRING", "content_9",
                                               properties);
        properties.clear();
        // parser = unicode, support_phrase = true, lower_case = true, char_filter = char_replace
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_UNICODE);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "char_replace");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "._");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, " ");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10010,
                                               "idx_content_10", 10, "STRING", "content_10",
                                               properties);
        properties.clear();
        // parser = unicode, support_phrase = true, lower_case = false, char_filter = none
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_UNICODE);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_FALSE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, "");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10011,
                                               "idx_content_11", 11, "STRING", "content_11",
                                               properties);
        properties.clear();
        // parser = unicode, support_phrase = true, lower_case = false, char_filter = char_replace
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_UNICODE);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_FALSE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "char_replace");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "._");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, " ");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10012,
                                               "idx_content_12", 12, "STRING", "content_12",
                                               properties);
        properties.clear();
        // parser = unicode, support_phrase = false, lower_case = true, char_filter = none
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_UNICODE);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_NO);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, "");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10013,
                                               "idx_content_13", 13, "STRING", "content_13",
                                               properties);
        properties.clear();
        // parser = unicode, support_phrase = false, lower_case = true, char_filter = char_replace
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_UNICODE);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_NO);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "char_replace");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "._");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, " ");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10014,
                                               "idx_content_14", 14, "STRING", "content_14",
                                               properties);
        properties.clear();
        // parser = unicode, support_phrase = false, lower_case = false, char_filter = none
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_UNICODE);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_NO);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_FALSE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, "");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10015,
                                               "idx_content_15", 15, "STRING", "content_15",
                                               properties);
        properties.clear();
        // parser = unicode, support_phrase = false, lower_case = false, char_filter = char_replace
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_UNICODE);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_NO);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_FALSE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "char_replace");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "._");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, " ");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10016,
                                               "idx_content_16", 16, "STRING", "content_16",
                                               properties);
        properties.clear();
        // parser = chinese, parser_mode = fine_grained, support_phrase = true, lower_case = true, char_filter = none
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_CHINESE);
        properties.emplace(INVERTED_INDEX_PARSER_MODE_KEY, INVERTED_INDEX_PARSER_FINE_GRANULARITY);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, "");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10017,
                                               "idx_content_17", 17, "STRING", "content_17",
                                               properties);
        properties.clear();
        // parser = chinese, parser_mode = fine_grained, support_phrase = true, lower_case = true, char_filter = char_replace
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_CHINESE);
        properties.emplace(INVERTED_INDEX_PARSER_MODE_KEY, INVERTED_INDEX_PARSER_FINE_GRANULARITY);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "char_replace");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "._");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, " ");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10018,
                                               "idx_content_18", 18, "STRING", "content_18",
                                               properties);
        properties.clear();
        // parser = chinese, parser_mode = fine_grained, support_phrase = true, lower_case = false, char_filter = none
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_CHINESE);
        properties.emplace(INVERTED_INDEX_PARSER_MODE_KEY, INVERTED_INDEX_PARSER_FINE_GRANULARITY);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_FALSE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, "");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10019,
                                               "idx_content_19", 19, "STRING", "content_19",
                                               properties);
        properties.clear();
        // parser = chinese, parser_mode = fine_grained, support_phrase = true, lower_case = false, char_filter = char_replace
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_CHINESE);
        properties.emplace(INVERTED_INDEX_PARSER_MODE_KEY, INVERTED_INDEX_PARSER_FINE_GRANULARITY);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_FALSE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "char_replace");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "._");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, " ");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10020,
                                               "idx_content_20", 20, "STRING", "content_20",
                                               properties);
        properties.clear();
        // parser = chinese, parser_mode = fine_grained, support_phrase = false, lower_case = true, char_filter = none
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_CHINESE);
        properties.emplace(INVERTED_INDEX_PARSER_MODE_KEY, INVERTED_INDEX_PARSER_FINE_GRANULARITY);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_NO);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, "");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10021,
                                               "idx_content_21", 21, "STRING", "content_21",
                                               properties);
        properties.clear();
        // parser = chinese, parser_mode = fine_grained, support_phrase = false, lower_case = true, char_filter = char_replace
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_CHINESE);
        properties.emplace(INVERTED_INDEX_PARSER_MODE_KEY, INVERTED_INDEX_PARSER_FINE_GRANULARITY);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_NO);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "char_replace");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "._");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, " ");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10022,
                                               "idx_content_22", 22, "STRING", "content_22",
                                               properties);
        properties.clear();
        // parser = chinese, parser_mode = fine_grained, support_phrase = false, lower_case = false, char_filter = none
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_CHINESE);
        properties.emplace(INVERTED_INDEX_PARSER_MODE_KEY, INVERTED_INDEX_PARSER_FINE_GRANULARITY);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_NO);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_FALSE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, "");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10023,
                                               "idx_content_23", 23, "STRING", "content_23",
                                               properties);
        properties.clear();
        // parser = chinese, parser_mode = fine_grained, support_phrase = false, lower_case = false, char_filter = char_replace
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_CHINESE);
        properties.emplace(INVERTED_INDEX_PARSER_MODE_KEY, INVERTED_INDEX_PARSER_FINE_GRANULARITY);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_NO);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_FALSE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "char_replace");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "._");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, " ");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10024,
                                               "idx_content_24", 24, "STRING", "content_24",
                                               properties);
        properties.clear();
        // parser = chinese, parser_mode = coarse_grained, support_phrase = true, lower_case = true, char_filter = none
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_CHINESE);
        properties.emplace(INVERTED_INDEX_PARSER_MODE_KEY,
                           INVERTED_INDEX_PARSER_COARSE_GRANULARITY);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, "");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10025,
                                               "idx_content_25", 25, "STRING", "content_25",
                                               properties);
        properties.clear();
        // parser = chinese, parser_mode = coarse_grained, support_phrase = true, lower_case = true, char_filter = char_replace
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_CHINESE);
        properties.emplace(INVERTED_INDEX_PARSER_MODE_KEY,
                           INVERTED_INDEX_PARSER_COARSE_GRANULARITY);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "char_replace");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "._");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, " ");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10026,
                                               "idx_content_26", 26, "STRING", "content_26",
                                               properties);
        properties.clear();
        // parser = chinese, parser_mode = coarse_grained, support_phrase = true, lower_case = false, char_filter = none
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_CHINESE);
        properties.emplace(INVERTED_INDEX_PARSER_MODE_KEY,
                           INVERTED_INDEX_PARSER_COARSE_GRANULARITY);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_FALSE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, "");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10027,
                                               "idx_content_27", 27, "STRING", "content_27",
                                               properties);
        properties.clear();
        // parser = chinese, parser_mode = coarse_grained, support_phrase = true, lower_case = false, char_filter = char_replace
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_CHINESE);
        properties.emplace(INVERTED_INDEX_PARSER_MODE_KEY,
                           INVERTED_INDEX_PARSER_COARSE_GRANULARITY);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_FALSE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "char_replace");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "._");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, " ");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10028,
                                               "idx_content_28", 28, "STRING", "content_28",
                                               properties);
        properties.clear();
        // parser = chinese, parser_mode = coarse_grained, support_phrase = false, lower_case = true, char_filter = none
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_CHINESE);
        properties.emplace(INVERTED_INDEX_PARSER_MODE_KEY,
                           INVERTED_INDEX_PARSER_COARSE_GRANULARITY);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_NO);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, "");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10029,
                                               "idx_content_29", 29, "STRING", "content_29",
                                               properties);
        properties.clear();
        // parser = chinese, parser_mode = coarse_grained, support_phrase = false, lower_case = true, char_filter = char_replace
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_CHINESE);
        properties.emplace(INVERTED_INDEX_PARSER_MODE_KEY,
                           INVERTED_INDEX_PARSER_COARSE_GRANULARITY);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_NO);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "char_replace");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "._");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, " ");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10030,
                                               "idx_content_30", 30, "STRING", "content_30",
                                               properties);
        properties.clear();
        // parser = chinese, parser_mode = coarse_grained, support_phrase = false, lower_case = false, char_filter = none
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_CHINESE);
        properties.emplace(INVERTED_INDEX_PARSER_MODE_KEY,
                           INVERTED_INDEX_PARSER_COARSE_GRANULARITY);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_NO);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_FALSE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, "");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10031,
                                               "idx_content_31", 31, "STRING", "content_31",
                                               properties);
        properties.clear();
        // parser = chinese, parser_mode = coarse_grained, support_phrase = false, lower_case = false, char_filter = char_replace
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_CHINESE);
        properties.emplace(INVERTED_INDEX_PARSER_MODE_KEY,
                           INVERTED_INDEX_PARSER_COARSE_GRANULARITY);
        properties.emplace(INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                           INVERTED_INDEX_PARSER_PHRASE_SUPPORT_NO);
        properties.emplace(INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_FALSE);
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, "char_replace");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "._");
        properties.emplace(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, " ");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10032,
                                               "idx_content_32", 32, "STRING", "content_32",
                                               properties);
        properties.clear();
        // parser = none, ignore_above = 256
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_NONE);
        properties.emplace(INVERTED_INDEX_PARSER_IGNORE_ABOVE_KEY, "256");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10033,
                                               "idx_content_33", 33, "STRING", "content_33",
                                               properties);
        properties.clear();
        // parser = none, ignore_above = 16383
        properties.emplace(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_NONE);
        properties.emplace(INVERTED_INDEX_PARSER_IGNORE_ABOVE_KEY, "16383");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10034,
                                               "idx_content_34", 34, "STRING", "content_34",
                                               properties);

        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10035,
                                               "idx_redirect", 35, "STRING", "redirect");
        IndexCompactionUtils::construct_column(schema_pb.add_column(), schema_pb.add_index(), 10036,
                                               "idx_namespace", 36, "STRING", "namespace");

        if (keys_type == KeysType::UNIQUE_KEYS) {
            // unique table must contain the DELETE_SIGN column
            auto* column_pb = schema_pb.add_column();
            IndexCompactionUtils::construct_column(column_pb, 37, "TINYINT", DELETE_SIGN);
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

    void _run_normal_wiki_test(bool with_delete = false, const std::string& delete_pred = "",
                               int64_t max_rows_per_segment = 100000,
                               int output_rowset_segment_number = 1) {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());
        std::string data_file1 =
                _current_dir +
                "/be/test/olap/rowset/segment_v2/inverted_index/data/sorted_wikipedia-50-1.json";
        std::string data_file2 =
                _current_dir +
                "/be/test/olap/rowset/segment_v2/inverted_index/data/sorted_wikipedia-50-2.json";
        // for MOW table to delete
        std::string data_file3 =
                _current_dir +
                "/be/test/olap/rowset/segment_v2/inverted_index/data/sorted_wikipedia-50-2.json";
        std::vector<std::string> data_files;
        data_files.push_back(data_file1);
        data_files.push_back(data_file2);
        data_files.push_back(data_file3);

        std::vector<RowsetSharedPtr> rowsets(data_files.size());
        auto custom_check_build_rowsets = [this](const int32_t& size) {
            auto keys_type = _tablet_schema->keys_type();
            if (keys_type == KeysType::UNIQUE_KEYS) {
                EXPECT_EQ(size, _tablet_schema->num_columns() - 1);
            } else {
                EXPECT_EQ(size, _tablet_schema->num_columns());
            }
        };
        IndexCompactionUtils::build_rowsets<IndexCompactionUtils::WikiDataRow>(
                _data_dir, _tablet_schema, _tablet, _engine_ref, rowsets, data_files, _inc_id,
                custom_check_build_rowsets, false, 50);

        if (with_delete) {
            // create delete predicate rowset and add to tablet
            auto delete_rowset = IndexCompactionUtils::create_delete_predicate_rowset(
                    _tablet_schema, delete_pred, _inc_id);
            EXPECT_TRUE(_tablet->add_rowset(delete_rowset).ok());
            EXPECT_TRUE(_tablet->rowset_map().size() == (data_files.size() + 1));
            rowsets.push_back(delete_rowset);
            EXPECT_TRUE(rowsets.size() == (data_files.size() + 1));
        }
        auto custom_check_index = [this, output_rowset_segment_number](
                                          const BaseCompaction& compaction,
                                          const RowsetWriterContext& ctx) {
            auto keys_type = _tablet_schema->keys_type();
            if (keys_type == KeysType::UNIQUE_KEYS) {
                EXPECT_EQ(compaction._cur_tablet_schema->inverted_indexes().size(),
                          _tablet_schema->num_columns() - 1);
                EXPECT_EQ(ctx.columns_to_do_index_compaction.size(),
                          _tablet_schema->num_columns() - 1);
            } else {
                EXPECT_EQ(compaction._cur_tablet_schema->inverted_indexes().size(),
                          _tablet_schema->num_columns());
                EXPECT_EQ(ctx.columns_to_do_index_compaction.size(), _tablet_schema->num_columns());
            }
            EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(0));
            EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(1));
            EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(2));
            EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(3));
            EXPECT_EQ(compaction._output_rowset->num_segments(), output_rowset_segment_number)
                    << compaction._output_rowset->num_segments();
        };

        RowsetSharedPtr output_rowset_index;
        Status st;
        {
            OlapStopWatch watch;
            st = IndexCompactionUtils::do_compaction(rowsets, _engine_ref, _tablet, true,
                                                     output_rowset_index, custom_check_index,
                                                     max_rows_per_segment);
            std::cout << "index compaction time: " << watch.get_elapse_second() << "s" << std::endl;
        }
        EXPECT_TRUE(st.ok()) << st.to_string();

        auto custom_check_normal = [this, output_rowset_segment_number](
                                           const BaseCompaction& compaction,
                                           const RowsetWriterContext& ctx) {
            auto keys_type = _tablet_schema->keys_type();
            if (keys_type == KeysType::UNIQUE_KEYS) {
                EXPECT_EQ(compaction._cur_tablet_schema->inverted_indexes().size(),
                          _tablet_schema->num_columns() - 1);
            } else {
                EXPECT_EQ(compaction._cur_tablet_schema->inverted_indexes().size(),
                          _tablet_schema->num_columns());
            }
            EXPECT_TRUE(ctx.columns_to_do_index_compaction.empty());
            EXPECT_TRUE(compaction._output_rowset->num_segments() == output_rowset_segment_number)
                    << compaction._output_rowset->num_segments();
        };

        RowsetSharedPtr output_rowset_normal;
        {
            OlapStopWatch watch;
            st = IndexCompactionUtils::do_compaction(rowsets, _engine_ref, _tablet, false,
                                                     output_rowset_normal, custom_check_normal,
                                                     max_rows_per_segment);
            std::cout << "normal compaction time: " << watch.get_elapse_second() << "s"
                      << std::endl;
        }
        EXPECT_TRUE(st.ok()) << st.to_string();

        auto num_segments_idx = output_rowset_index->num_segments();
        auto num_segments_normal = output_rowset_normal->num_segments();
        for (int idx = 10000; idx < 10037; idx++) {
            if (num_segments_idx == num_segments_normal == 1) {
                // check index file terms for single segment
                const auto& seg_path = output_rowset_index->segment_path(0);
                EXPECT_TRUE(seg_path.has_value()) << seg_path.error();
                auto inverted_index_file_reader_index =
                        IndexCompactionUtils::init_index_file_reader(
                                output_rowset_index, seg_path.value(),
                                _tablet_schema->get_inverted_index_storage_format());

                const auto& seg_path_normal = output_rowset_normal->segment_path(0);
                EXPECT_TRUE(seg_path_normal.has_value()) << seg_path_normal.error();
                auto inverted_index_file_reader_normal =
                        IndexCompactionUtils::init_index_file_reader(
                                output_rowset_normal, seg_path_normal.value(),
                                _tablet_schema->get_inverted_index_storage_format());

                auto dir_idx = inverted_index_file_reader_index->_open(idx, "");
                EXPECT_TRUE(dir_idx.has_value()) << dir_idx.error();
                auto dir_normal = inverted_index_file_reader_normal->_open(idx, "");
                EXPECT_TRUE(dir_normal.has_value()) << dir_normal.error();
                st = IndexCompactionUtils::check_idx_file_correctness(dir_idx->get(),
                                                                      dir_normal->get());
                EXPECT_TRUE(st.ok()) << st.to_string();
            } else {
                // check index file terms for multiple segments
                std::vector<std::unique_ptr<DorisCompoundReader, DirectoryDeleter>> dirs_idx(
                        num_segments_idx);
                for (int i = 0; i < num_segments_idx; i++) {
                    const auto& seg_path = output_rowset_index->segment_path(i);
                    EXPECT_TRUE(seg_path.has_value()) << seg_path.error();
                    auto inverted_index_file_reader_index =
                            IndexCompactionUtils::init_index_file_reader(
                                    output_rowset_index, seg_path.value(),
                                    _tablet_schema->get_inverted_index_storage_format());
                    auto dir_idx = inverted_index_file_reader_index->_open(idx, "");
                    EXPECT_TRUE(dir_idx.has_value()) << dir_idx.error();
                    dirs_idx[i] = std::move(dir_idx.value());
                }
                std::vector<std::unique_ptr<DorisCompoundReader, DirectoryDeleter>> dirs_normal(
                        num_segments_normal);
                for (int i = 0; i < num_segments_normal; i++) {
                    const auto& seg_path = output_rowset_normal->segment_path(i);
                    EXPECT_TRUE(seg_path.has_value()) << seg_path.error();
                    auto inverted_index_file_reader_normal =
                            IndexCompactionUtils::init_index_file_reader(
                                    output_rowset_normal, seg_path.value(),
                                    _tablet_schema->get_inverted_index_storage_format());
                    auto dir_normal = inverted_index_file_reader_normal->_open(idx, "");
                    EXPECT_TRUE(dir_normal.has_value()) << dir_normal.error();
                    dirs_normal[i] = std::move(dir_normal.value());
                }
                st = IndexCompactionUtils::check_idx_file_correctness(dirs_idx, dirs_normal);
                EXPECT_TRUE(st.ok()) << st.to_string();
            }
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

TEST_F(IndexCompactionTest, tes_write_index_normally) {
    _build_tablet();
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
    IndexCompactionUtils::build_rowsets<IndexCompactionUtils::DataRow>(
            _data_dir, _tablet_schema, _tablet, _engine_ref, rowsets, data_files, _inc_id,
            custom_check_build_rowsets);

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
    _build_tablet();
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
    IndexCompactionUtils::build_rowsets<IndexCompactionUtils::DataRow>(
            _data_dir, _tablet_schema, _tablet, _engine_ref, rowsets, data_files, _inc_id,
            custom_check_build_rowsets);

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
    _build_tablet();
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
    auto custom_check_build_rowsets = [](const int32_t& size) { EXPECT_EQ(size, 4); };
    IndexCompactionUtils::build_rowsets<IndexCompactionUtils::DataRow>(
            _data_dir, _tablet_schema, _tablet, _engine_ref, rowsets, data_files, _inc_id,
            custom_check_build_rowsets);

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
    // index 10002 can be found in idx file
    auto dir_idx_compaction = inverted_index_file_reader_index->_open(10002, "");
    EXPECT_TRUE(dir_idx_compaction.has_value()) << dir_idx_compaction.error();
}

TEST_F(IndexCompactionTest, test_tablet_schema_tablet_index_is_null) {
    _build_tablet();
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
    IndexCompactionUtils::build_rowsets<IndexCompactionUtils::DataRow>(
            _data_dir, _tablet_schema, _tablet, _engine_ref, rowsets, data_files, _inc_id,
            custom_check_build_rowsets);

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
    _build_tablet();
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
    IndexCompactionUtils::build_rowsets<IndexCompactionUtils::DataRow>(
            _data_dir, _tablet_schema, _tablet, _engine_ref, rowsets, data_files, _inc_id,
            custom_check_build_rowsets);

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
    // index 10001 should be found in idx file, it can be produced by normal compaction
    auto dir_idx_compaction = inverted_index_file_reader_index->_open(10001, "");
    EXPECT_TRUE(dir_idx_compaction.has_value()) << dir_idx_compaction.error();
    // check index 10001 term stats
    std::ostringstream oss;
    IndexCompactionUtils::check_terms_stats(dir_idx_compaction.value().get(), oss);
    std::string output = oss.str();
    EXPECT_EQ(output, expected_output);
}

TEST_F(IndexCompactionTest, test_tablet_index_properties_not_equal) {
    _build_tablet();
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
    IndexCompactionUtils::build_rowsets<IndexCompactionUtils::DataRow>(
            _data_dir, _tablet_schema, _tablet, _engine_ref, rowsets, data_files, _inc_id,
            custom_check_build_rowsets);

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
    _build_tablet();
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
    IndexCompactionUtils::build_rowsets<IndexCompactionUtils::DataRow>(
            _data_dir, _tablet_schema, _tablet, _engine_ref, rowsets, data_files, _inc_id,
            custom_check_build_rowsets);

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
    _build_tablet();
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
    IndexCompactionUtils::build_rowsets<IndexCompactionUtils::DataRow>(
            _data_dir, _tablet_schema, _tablet, _engine_ref, rowsets, data_files, _inc_id,
            custom_check_build_rowsets);

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
    _build_tablet();
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
    IndexCompactionUtils::build_rowsets<IndexCompactionUtils::DataRow>(
            _data_dir, _tablet_schema, _tablet, _engine_ref, rowsets, data_files, _inc_id,
            custom_check_build_rowsets);

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
    _build_tablet();
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
    IndexCompactionUtils::build_rowsets<IndexCompactionUtils::DataRow>(
            _data_dir, _tablet_schema, _tablet, _engine_ref, rowsets, data_files, _inc_id,
            custom_check_build_rowsets);

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
    _build_tablet();
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
    IndexCompactionUtils::build_rowsets<IndexCompactionUtils::DataRow>(
            _data_dir, _tablet_schema, _tablet, _engine_ref, rowsets, data_files, _inc_id,
            custom_check_build_rowsets);

    // create delete predicate rowset and add to tablet
    auto delete_rowset = IndexCompactionUtils::create_delete_predicate_rowset(
            _tablet_schema, "v1='great'", _inc_id);
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

TEST_F(IndexCompactionTest, tes_wikipedia_dup_v2) {
    _build_wiki_tablet(KeysType::DUP_KEYS, InvertedIndexStorageFormatPB::V2);
    _run_normal_wiki_test();
}

TEST_F(IndexCompactionTest, tes_wikipedia_mow_v2) {
    _build_wiki_tablet(KeysType::UNIQUE_KEYS, InvertedIndexStorageFormatPB::V2);
    _run_normal_wiki_test();
}

TEST_F(IndexCompactionTest, tes_wikipedia_dup_v2_with_partial_delete) {
    _build_wiki_tablet(KeysType::DUP_KEYS, InvertedIndexStorageFormatPB::V2);
    _run_normal_wiki_test(true, "namespace='Adel, OR'");
}

TEST_F(IndexCompactionTest, tes_wikipedia_mow_v2_with_partial_delete) {
    _build_wiki_tablet(KeysType::UNIQUE_KEYS, InvertedIndexStorageFormatPB::V2);
    _run_normal_wiki_test(true, "namespace='Adel, OR'");
}

TEST_F(IndexCompactionTest, tes_wikipedia_dup_v2_with_total_delete) {
    _build_wiki_tablet(KeysType::DUP_KEYS, InvertedIndexStorageFormatPB::V2);
    std::string delete_pred = "title IS NOT NULL";
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());
    std::string data_file1 =
            _current_dir +
            "/be/test/olap/rowset/segment_v2/inverted_index/data/sorted_wikipedia-50-1.json";
    std::string data_file2 =
            _current_dir +
            "/be/test/olap/rowset/segment_v2/inverted_index/data/sorted_wikipedia-50-2.json";
    // for MOW table to delete
    std::string data_file3 =
            _current_dir +
            "/be/test/olap/rowset/segment_v2/inverted_index/data/sorted_wikipedia-50-2.json";
    std::vector<std::string> data_files;
    data_files.push_back(data_file1);
    data_files.push_back(data_file2);
    data_files.push_back(data_file3);

    std::vector<RowsetSharedPtr> rowsets(data_files.size());
    auto custom_check_build_rowsets = [this](const int32_t& size) {
        EXPECT_EQ(size, _tablet_schema->num_columns());
    };
    IndexCompactionUtils::build_rowsets<IndexCompactionUtils::WikiDataRow>(
            _data_dir, _tablet_schema, _tablet, _engine_ref, rowsets, data_files, _inc_id,
            custom_check_build_rowsets, false, 50);

    // create delete predicate rowset and add to tablet
    auto delete_rowset = IndexCompactionUtils::create_delete_predicate_rowset(_tablet_schema,
                                                                              delete_pred, _inc_id);
    EXPECT_TRUE(_tablet->add_rowset(delete_rowset).ok());
    EXPECT_TRUE(_tablet->rowset_map().size() == (data_files.size() + 1));
    rowsets.push_back(delete_rowset);
    EXPECT_TRUE(rowsets.size() == (data_files.size() + 1));

    auto custom_check_index = [this](const BaseCompaction& compaction,
                                     const RowsetWriterContext& ctx) {
        EXPECT_EQ(compaction._cur_tablet_schema->inverted_indexes().size(),
                  _tablet_schema->num_columns());
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.size() == _tablet_schema->num_columns());
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(0));
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(1));
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(2));
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(3));
        EXPECT_TRUE(compaction._output_rowset->num_segments() == 0);
    };

    RowsetSharedPtr output_rowset_index;
    Status st;
    {
        OlapStopWatch watch;
        st = IndexCompactionUtils::do_compaction(rowsets, _engine_ref, _tablet, true,
                                                 output_rowset_index, custom_check_index);
        std::cout << "index compaction time: " << watch.get_elapse_second() << "s" << std::endl;
    }
    EXPECT_TRUE(st.ok()) << st.to_string();

    auto custom_check_normal = [this](const BaseCompaction& compaction,
                                      const RowsetWriterContext& ctx) {
        EXPECT_EQ(compaction._cur_tablet_schema->inverted_indexes().size(),
                  _tablet_schema->num_columns());
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.size() == 0);
        EXPECT_TRUE(compaction._output_rowset->num_segments() == 0);
    };

    RowsetSharedPtr output_rowset_normal;
    {
        OlapStopWatch watch;
        st = IndexCompactionUtils::do_compaction(rowsets, _engine_ref, _tablet, false,
                                                 output_rowset_normal, custom_check_normal);
        std::cout << "normal compaction time: " << watch.get_elapse_second() << "s" << std::endl;
    }
    EXPECT_TRUE(st.ok()) << st.to_string();
}

TEST_F(IndexCompactionTest, tes_wikipedia_mow_v2_with_total_delete) {
    _build_wiki_tablet(KeysType::UNIQUE_KEYS, InvertedIndexStorageFormatPB::V2);
    std::string delete_pred = "title IS NOT NULL";
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());
    std::string data_file1 =
            _current_dir +
            "/be/test/olap/rowset/segment_v2/inverted_index/data/sorted_wikipedia-50-1.json";
    std::string data_file2 =
            _current_dir +
            "/be/test/olap/rowset/segment_v2/inverted_index/data/sorted_wikipedia-50-2.json";
    // for MOW table to delete
    std::string data_file3 =
            _current_dir +
            "/be/test/olap/rowset/segment_v2/inverted_index/data/sorted_wikipedia-50-2.json";
    std::vector<std::string> data_files;
    data_files.push_back(data_file1);
    data_files.push_back(data_file2);
    data_files.push_back(data_file3);

    std::vector<RowsetSharedPtr> rowsets(data_files.size());
    auto custom_check_build_rowsets = [this](const int32_t& size) {
        EXPECT_EQ(size, _tablet_schema->num_columns() - 1);
    };
    IndexCompactionUtils::build_rowsets<IndexCompactionUtils::WikiDataRow>(
            _data_dir, _tablet_schema, _tablet, _engine_ref, rowsets, data_files, _inc_id,
            custom_check_build_rowsets, false, 50);

    // create delete predicate rowset and add to tablet
    auto delete_rowset = IndexCompactionUtils::create_delete_predicate_rowset(_tablet_schema,
                                                                              delete_pred, _inc_id);
    EXPECT_TRUE(_tablet->add_rowset(delete_rowset).ok());
    EXPECT_TRUE(_tablet->rowset_map().size() == (data_files.size() + 1));
    rowsets.push_back(delete_rowset);
    EXPECT_TRUE(rowsets.size() == (data_files.size() + 1));

    auto custom_check_index = [this](const BaseCompaction& compaction,
                                     const RowsetWriterContext& ctx) {
        EXPECT_EQ(compaction._cur_tablet_schema->inverted_indexes().size(),
                  _tablet_schema->num_columns() - 1);
        EXPECT_EQ(ctx.columns_to_do_index_compaction.size(), _tablet_schema->num_columns() - 1);
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(0));
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(1));
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(2));
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(3));
        EXPECT_TRUE(compaction._output_rowset->num_segments() == 0);
    };

    RowsetSharedPtr output_rowset_index;
    Status st;
    {
        OlapStopWatch watch;
        st = IndexCompactionUtils::do_compaction(rowsets, _engine_ref, _tablet, true,
                                                 output_rowset_index, custom_check_index);
        std::cout << "index compaction time: " << watch.get_elapse_second() << "s" << std::endl;
    }
    EXPECT_TRUE(st.ok()) << st.to_string();

    auto custom_check_normal = [this](const BaseCompaction& compaction,
                                      const RowsetWriterContext& ctx) {
        EXPECT_EQ(compaction._cur_tablet_schema->inverted_indexes().size(),
                  _tablet_schema->num_columns() - 1);
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.size() == 0);
        EXPECT_TRUE(compaction._output_rowset->num_segments() == 0);
    };

    RowsetSharedPtr output_rowset_normal;
    {
        OlapStopWatch watch;
        st = IndexCompactionUtils::do_compaction(rowsets, _engine_ref, _tablet, false,
                                                 output_rowset_normal, custom_check_normal);
        std::cout << "normal compaction time: " << watch.get_elapse_second() << "s" << std::endl;
    }
    EXPECT_TRUE(st.ok()) << st.to_string();
}

TEST_F(IndexCompactionTest, tes_wikipedia_dup_v2_multiple_dest_segments) {
    _build_wiki_tablet(KeysType::DUP_KEYS, InvertedIndexStorageFormatPB::V2);
    _run_normal_wiki_test(false, "", 50, 3);
}

TEST_F(IndexCompactionTest, tes_wikipedia_mow_v2_multiple_dest_segments) {
    _build_wiki_tablet(KeysType::UNIQUE_KEYS, InvertedIndexStorageFormatPB::V2);
    _run_normal_wiki_test(false, "", 50, 2);
}

TEST_F(IndexCompactionTest, tes_wikipedia_dup_v2_multiple_src_lucene_segments) {
    config::inverted_index_max_buffered_docs = 100;
    _build_wiki_tablet(KeysType::DUP_KEYS, InvertedIndexStorageFormatPB::V2);
    _run_normal_wiki_test();
}

TEST_F(IndexCompactionTest, tes_wikipedia_mow_v2_multiple_src_lucene_segments) {
    config::inverted_index_max_buffered_docs = 100;
    _build_wiki_tablet(KeysType::UNIQUE_KEYS, InvertedIndexStorageFormatPB::V2);
    _run_normal_wiki_test();
}

TEST_F(IndexCompactionTest, test_inverted_index_ram_dir_disable) {
    bool original_ram_dir_enable = config::inverted_index_ram_dir_enable;
    config::inverted_index_ram_dir_enable = false;

    _build_tablet();
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
    IndexCompactionUtils::build_rowsets<IndexCompactionUtils::DataRow>(
            _data_dir, _tablet_schema, _tablet, _engine_ref, rowsets, data_files, _inc_id,
            custom_check_build_rowsets);

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
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.empty());
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

    std::map<int, QueryData> query_map = {
            {0, {{"99", "66", "56", "87", "85", "96", "20000"}, {21, 25, 22, 18, 14, 18, 0}}},
            {3, {{"99", "66", "56", "87", "85", "96", "10000"}, {12, 20, 25, 23, 16, 24, 0}}},
            {1, {{"good", "maybe", "great", "null"}, {197, 191, 194, 0}}},
            {2, {{"musicstream.com", "http", "https", "null"}, {191, 799, 1201, 0}}}};
    IndexCompactionUtils::check_meta_and_file(output_rowset_index, _tablet_schema, query_map);
    IndexCompactionUtils::check_meta_and_file(output_rowset_normal, _tablet_schema, query_map);

    config::inverted_index_ram_dir_enable = original_ram_dir_enable;
}

TEST_F(IndexCompactionTest, test_inverted_index_ram_dir_disable_with_debug_point) {
    bool original_enable_debug_points = config::enable_debug_points;
    config::enable_debug_points = true;
    bool original_ram_dir_enable = config::inverted_index_ram_dir_enable;
    config::inverted_index_ram_dir_enable = false;

    _build_tablet();
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
    IndexCompactionUtils::build_rowsets<IndexCompactionUtils::DataRow>(
            _data_dir, _tablet_schema, _tablet, _engine_ref, rowsets, data_files, _inc_id,
            custom_check_build_rowsets);

    DebugPoints::instance()->add("compact_column_delete_tmp_path_error");

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

    EXPECT_FALSE(st.ok());
    EXPECT_THAT(st.to_string(), testing::HasSubstr("compact_column_delete_tmp_path_error"));

    DebugPoints::instance()->remove("compact_column_delete_tmp_path_error");

    config::inverted_index_ram_dir_enable = original_ram_dir_enable;
    config::enable_debug_points = original_enable_debug_points;
}
} // namespace doris
