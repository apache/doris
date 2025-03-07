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

#include <gmock/gmock-more-matchers.h>
#include <gtest/gtest.h>

#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/segment_v2/variant_column_writer_impl.h"
#include "olap/storage_engine.h"
#include "olap/tablet_schema.h"
#include "vec/common/schema_util.h"
#include "vec/json/parse2column.h"

using namespace doris::vectorized;

using namespace doris::segment_v2;

using namespace doris;

constexpr static uint32_t MAX_PATH_LEN = 1024;
constexpr static std::string_view dest_dir = "/ut_dir/schema_util_test";
constexpr static std::string_view tmp_dir = "./ut_dir/tmp";

class SchemaUtilRowsetTest : public testing::Test {
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
    }
    void TearDown() override {
        //EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
        _engine_ref = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

public:
    SchemaUtilRowsetTest() = default;
    virtual ~SchemaUtilRowsetTest() = default;

private:
    StorageEngine* _engine_ref = nullptr;
    std::unique_ptr<DataDir> _data_dir = nullptr;
    TabletSharedPtr _tablet = nullptr;
    std::string _absolute_dir;
    std::string _curreent_dir;
};

static void construct_column(ColumnPB* column_pb, int32_t col_unique_id,
                             const std::string& column_type, const std::string& column_name,
                             bool is_key = false) {
    column_pb->set_unique_id(col_unique_id);
    column_pb->set_name(column_name);
    column_pb->set_type(column_type);
    column_pb->set_is_key(is_key);
    column_pb->set_is_nullable(false);
    if (column_type == "VARIANT") {
        column_pb->set_variant_max_subcolumns_count(3);
    }
}

// static void construct_tablet_index(TabletIndexPB* tablet_index, int64_t index_id, const std::string& index_name, int32_t col_unique_id) {
//     tablet_index->set_index_id(index_id);
//     tablet_index->set_index_name(index_name);
//     tablet_index->set_index_type(IndexType::INVERTED);
//     tablet_index->add_col_unique_id(col_unique_id);
// }

static std::unordered_map<int32_t, schema_util::PathToNoneNullValues> all_path_stats;
static void fill_string_column_with_test_data(auto& column_string, int size, int uid) {
    std::srand(42);
    for (int i = 0; i < size; i++) {
        std::string json_str = "{";
        int num_pairs = std::rand() % 10 + 1;
        for (int j = 0; j < num_pairs; j++) {
            std::string key = "key" + std::to_string(j);
            if (std::rand() % 2 == 0) {
                int value = std::rand() % 100;
                json_str += "\"" + key + "\" : " + std::to_string(value);
            } else {
                std::string value = "str" + std::to_string(std::rand() % 100);
                json_str += "\"" + key + "\" : \"" + value + "\"";
            }
            if (j < num_pairs - 1) {
                json_str += ", ";
            }
            all_path_stats[uid][key] += 1;
        }
        json_str += "}";
        vectorized::Field str(json_str);
        column_string->insert_data(json_str.data(), json_str.size());
    }
}

static void fill_varaint_column(auto& variant_column, int size, int uid) {
    auto type_string = std::make_shared<vectorized::DataTypeString>();
    auto column = type_string->create_column();
    auto column_string = assert_cast<ColumnString*>(column.get());
    fill_string_column_with_test_data(column_string, size, uid);
    vectorized::ParseConfig config;
    config.enable_flatten_nested = false;
    parse_json_to_variant(*variant_column, *column_string, config);
}

static void fill_block_with_test_data(vectorized::Block* block, int size) {
    auto columns = block->mutate_columns();
    // insert key
    for (int i = 0; i < size; i++) {
        vectorized::Field key = i;
        columns[0]->insert(key);
    }

    // insert v1
    fill_varaint_column(columns[1], size, 1);

    // insert v2
    for (int i = 0; i < size; i++) {
        vectorized::Field v2("V2");
        columns[2]->insert(v2);
    }

    // insert v3
    fill_varaint_column(columns[3], size, 3);

    // insert v4
    for (int i = 0; i < size; i++) {
        vectorized::Field v4(i);
        columns[4]->insert(v4);
    }
}
static int64_t inc_id = 1000;
static RowsetWriterContext rowset_writer_context(const std::unique_ptr<DataDir>& data_dir,
                                                 const TabletSchemaSPtr& schema,
                                                 const std::string& tablet_path) {
    RowsetWriterContext context;
    RowsetId rowset_id;
    rowset_id.init(inc_id);
    context.rowset_id = rowset_id;
    context.rowset_type = BETA_ROWSET;
    context.data_dir = data_dir.get();
    context.rowset_state = VISIBLE;
    context.tablet_schema = schema;
    context.tablet_path = tablet_path;
    context.version = Version(inc_id, inc_id);
    context.max_rows_per_segment = 200;
    inc_id++;
    return context;
}

static RowsetSharedPtr create_rowset(auto& rowset_writer, const TabletSchemaSPtr& tablet_schema) {
    vectorized::Block block = tablet_schema->create_block();
    fill_block_with_test_data(&block, 1000);
    auto st = rowset_writer->add_block(&block);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = rowset_writer->flush();
    EXPECT_TRUE(st.ok()) << st.msg();

    RowsetSharedPtr rowset;
    EXPECT_TRUE(rowset_writer->build(rowset).ok());
    EXPECT_TRUE(rowset->num_segments() == 5);
    return rowset;
}

TEST_F(SchemaUtilRowsetTest, collect_path_stats_and_get_compaction_schema) {
    // 1.create tablet schema
    TabletSchemaPB schema_pb;
    construct_column(schema_pb.add_column(), 0, "INT", "key", true);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "v1");
    construct_column(schema_pb.add_column(), 2, "STRING", "v2");
    construct_column(schema_pb.add_column(), 3, "VARIANT", "v3");
    construct_column(schema_pb.add_column(), 4, "INT", "v4");
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(tablet_schema));
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create rowset
    std::vector<RowsetSharedPtr> rowsets;
    for (int i = 0; i < 5; i++) {
        const auto& res = RowsetFactory::create_rowset_writer(
                *_engine_ref,
                rowset_writer_context(_data_dir, tablet_schema, _tablet->tablet_path()), false);
        EXPECT_TRUE(res.has_value()) << res.error();
        const auto& rowset_writer = res.value();
        auto rowset = create_rowset(rowset_writer, tablet_schema);
        EXPECT_TRUE(_tablet->add_rowset(rowset).ok());
        rowsets.push_back(rowset);
    }

    std::unordered_map<int32_t, schema_util::PathToNoneNullValues> path_stats;
    for (const auto& rowset : rowsets) {
        auto st = schema_util::collect_path_stats(rowset, path_stats);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    for (const auto& [uid, path_stats] : path_stats) {
        for (const auto& [path, size] : path_stats) {
            EXPECT_EQ(all_path_stats[uid][path], size);
        }
    }

    // 4. get compaction schema
    TabletSchemaSPtr compaction_schema = tablet_schema;
    auto st = schema_util::get_compaction_schema(rowsets, compaction_schema);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 5. check compaction schema
    std::unordered_map<int32_t, std::vector<std::string>> compaction_schema_map;
    for (const auto& column : compaction_schema->columns()) {
        if (column->parent_unique_id() > 0) {
            compaction_schema_map[column->parent_unique_id()].push_back(column->name());
        }
    }
    for (auto& [uid, paths] : compaction_schema_map) {
        EXPECT_EQ(paths.size(), 4);
        std::sort(paths.begin(), paths.end());
        EXPECT_TRUE(paths[0].ends_with("__DORIS_VARIANT_SPARSE__"));
        EXPECT_TRUE(paths[1].ends_with("key0"));
        EXPECT_TRUE(paths[2].ends_with("key1"));
        EXPECT_TRUE(paths[3].ends_with("key2"));
    }
}
