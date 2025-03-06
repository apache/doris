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

#include "vec/common/schema_util.h"

#include <gmock/gmock-more-matchers.h>
#include <gtest/gtest.h>

#include "olap/rowset/segment_v2/variant_column_writer_impl.h"

#include "vec/json/parse2column.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/tablet_schema.h"
#include "olap/storage_engine.h"

using namespace doris::vectorized;

using namespace doris::segment_v2;

using namespace doris;

constexpr static uint32_t MAX_PATH_LEN = 1024;
constexpr static std::string_view dest_dir = "./ut_dir/schema_util_test";
constexpr static std::string_view tmp_dir = "./ut_dir/tmp";

class SchemaUtilTest : public testing::Test {
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
        std::cout << "absolute_dir: " << _absolute_dir << std::endl;
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
    SchemaUtilTest() = default;
    virtual ~SchemaUtilTest() = default;

private:
    StorageEngine* _engine_ref = nullptr;
    std::unique_ptr<DataDir> _data_dir = nullptr;
    TabletSharedPtr _tablet = nullptr;
    std::string _absolute_dir;
    std::string _curreent_dir;
};

void construct_column(ColumnPB* column_pb, TabletIndexPB* tablet_index, int64_t index_id,
                      const std::string& index_name, int32_t col_unique_id,
                      const std::string& column_type, const std::string& column_name,
                      const IndexType& index_type) {
    column_pb->set_unique_id(col_unique_id);
    column_pb->set_name(column_name);
    column_pb->set_type(column_type);
    column_pb->set_is_nullable(false);
    column_pb->set_is_bf_column(true);
    tablet_index->set_index_id(index_id);
    tablet_index->set_index_name(index_name);
    tablet_index->set_index_type(index_type);
    tablet_index->add_col_unique_id(col_unique_id);
}

void construct_subcolumn(TabletSchemaSPtr schema, const FieldType& type, int32_t col_unique_id,
                         std::string_view path, std::vector<TabletColumn>* subcolumns) {
    TabletColumn subcol;
    subcol.set_type(type);
    subcol.set_is_nullable(true);
    subcol.set_unique_id(-1);
    subcol.set_parent_unique_id(col_unique_id);
    vectorized::PathInData col_path(path);
    subcol.set_path_info(col_path);
    subcol.set_name(col_path.get_path());
    schema->append_column(subcol);
    subcolumns->emplace_back(std::move(subcol));
}

TEST_F(SchemaUtilTest, inherit_column_attributes) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);

    construct_column(schema_pb.add_column(), schema_pb.add_index(), 10000, "key_index", 0, "INT",
                     "key", IndexType::INVERTED);
    construct_column(schema_pb.add_column(), schema_pb.add_index(), 10001, "v1_index", 1, "VARIANT",
                     "v1", IndexType::INVERTED);
    construct_column(schema_pb.add_column(), schema_pb.add_index(), 10003, "v3_index", 3, "VARIANT",
                     "v3", IndexType::INVERTED);

    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(schema_pb);
    std::vector<TabletColumn> subcolumns;

    construct_subcolumn(tablet_schema, FieldType::OLAP_FIELD_TYPE_STRING, 1, "v1.b", &subcolumns);
    construct_subcolumn(tablet_schema, FieldType::OLAP_FIELD_TYPE_INT, 1, "v1.c", &subcolumns);

    construct_subcolumn(tablet_schema, FieldType::OLAP_FIELD_TYPE_ARRAY, 3, "v3.d", &subcolumns);
    construct_subcolumn(tablet_schema, FieldType::OLAP_FIELD_TYPE_FLOAT, 3, "v3.a", &subcolumns);

    schema_util::inherit_column_attributes(tablet_schema);
    for (const auto& col : subcolumns) {
        switch (col._parent_col_unique_id) {
        case 1:
            EXPECT_TRUE(tablet_schema->inverted_index(col) != nullptr);
            break;
        case 3:
            EXPECT_TRUE(tablet_schema->inverted_index(col) == nullptr);
            break;
        default:
            EXPECT_TRUE(false);
        }
    }
    EXPECT_EQ(tablet_schema->inverted_indexes().size(), 7);

    for (const auto& col : tablet_schema->_cols) {
        if (!col->is_extracted_column()) {
            continue;
        }
        switch (col->_parent_col_unique_id) {
        case 1:
            EXPECT_TRUE(col->is_bf_column());
            break;
        case 3:
            EXPECT_TRUE(!col->is_bf_column());
            break;
        default:
            EXPECT_TRUE(false);
        }
    }
}

std::unordered_map<std::string, int> construct_column_map_with_random_values(
        auto& column_map, int key_size, int value_size, const std::string& prefix) {
    std::unordered_map<std::string, int> key_value_counts;
    auto& key = assert_cast<ColumnString&>(column_map->get_keys());
    auto& value = assert_cast<ColumnString&>(column_map->get_values());
    auto& offsets = column_map->get_offsets();

    std::srand(42);
    
    for (int i = 0; i < key_size; ++i) {
        std::string current_key = prefix + std::to_string(i);

        int value_count = std::rand() % value_size + 1;
        key_value_counts[current_key] = value_count;
        
        for (int j = 0; j < value_count; ++j) {
            key.insert_data(current_key.data(), current_key.size());
            auto value_str = prefix + std::to_string(j);
            value.insert_data(value_str.data(), value_str.size());
        }
        offsets.push_back(key.size());
    }
    
    return key_value_counts;
}

TEST_F(SchemaUtilTest, calculate_variant_stats) {
    VariantStatisticsPB stats;
    auto column_map = ColumnMap::create(
            ColumnString::create(), ColumnString::create(), ColumnArray::ColumnOffsets::create());
    
    const auto& key_value_counts = construct_column_map_with_random_values(column_map, 200, 100, "key_");
    
    schema_util::calculate_variant_stats(*column_map, &stats);
    EXPECT_EQ(stats.sparse_column_non_null_size_size(), key_value_counts.size());
    
    for (const auto& kv : key_value_counts) {
        auto it = stats.sparse_column_non_null_size().find(kv.first);
        EXPECT_NE(it, stats.sparse_column_non_null_size().end());
        EXPECT_EQ(it->second, kv.second);
    }

    column_map->clear();
    const auto& key_value_counts2 = construct_column_map_with_random_values(column_map, 3000, 100, "key_");
    schema_util::calculate_variant_stats(*column_map, &stats);
    EXPECT_EQ(stats.sparse_column_non_null_size_size(), 3000);

    for (const auto& [path, size] : stats.sparse_column_non_null_size()) {
        auto first_size = key_value_counts.find(path) == key_value_counts.end() ? 0 : key_value_counts.find(path)->second;
        auto second_size = key_value_counts2.find(path) == key_value_counts2.end() ? 0 : key_value_counts2.find(path)->second;
        EXPECT_EQ(size, first_size + second_size);
    }

    column_map->clear();
    const auto& key_value_counts3 = construct_column_map_with_random_values(column_map, VariantStatistics::MAX_SPARSE_DATA_STATISTICS_SIZE, 5, "key2_");
    schema_util::calculate_variant_stats(*column_map, &stats);
    EXPECT_EQ(VariantStatistics::MAX_SPARSE_DATA_STATISTICS_SIZE, stats.sparse_column_non_null_size_size());

    for (const auto& [path, size] : stats.sparse_column_non_null_size()) {
        auto first_size = key_value_counts.find(path) == key_value_counts.end() ? 0 : key_value_counts.find(path)->second;
        auto second_size = key_value_counts2.find(path) == key_value_counts2.end() ? 0 : key_value_counts2.find(path)->second;
        auto third_size = key_value_counts3.find(path) == key_value_counts3.end() ? 0 : key_value_counts3.find(path)->second;
        EXPECT_EQ(size, first_size + second_size + third_size);
    }
}

TEST_F(SchemaUtilTest, get_subpaths) {
    TabletColumn variant;
    variant.set_unique_id(1);
    variant.set_variant_max_subcolumns_count(3); 

    std::unordered_map<int32_t, schema_util::PathToNoneNullValues> path_stats;
    path_stats[1] = {
        {"path1", 1000},
        {"path2", 800},
        {"path3", 500},
        {"path4", 300},
        {"path5", 200}
    };

    std::unordered_map<int32_t, TabletSchema::PathsSetInfo> uid_to_paths_set_info;
    schema_util::get_subpaths(variant, path_stats, uid_to_paths_set_info);

    EXPECT_EQ(uid_to_paths_set_info[1].sub_path_set.size(), 3);
    EXPECT_EQ(uid_to_paths_set_info[1].sparse_path_set.size(), 2);

    EXPECT_TRUE(uid_to_paths_set_info[1].sub_path_set.find("path1") != uid_to_paths_set_info[1].sub_path_set.end());
    EXPECT_TRUE(uid_to_paths_set_info[1].sub_path_set.find("path2") != uid_to_paths_set_info[1].sub_path_set.end());
    EXPECT_TRUE(uid_to_paths_set_info[1].sub_path_set.find("path3") != uid_to_paths_set_info[1].sub_path_set.end());

    EXPECT_TRUE(uid_to_paths_set_info[1].sparse_path_set.find("path4") != uid_to_paths_set_info[1].sparse_path_set.end());
    EXPECT_TRUE(uid_to_paths_set_info[1].sparse_path_set.find("path5") != uid_to_paths_set_info[1].sparse_path_set.end());
}

TEST_F(SchemaUtilTest, get_subpaths_equal_to_max) {
    TabletColumn variant;
    variant.set_unique_id(1);
    variant.set_variant_max_subcolumns_count(3);

    std::unordered_map<int32_t, schema_util::PathToNoneNullValues> path_stats;
    path_stats[1] = {
        {"path1", 1000},
        {"path2", 800},
        {"path3", 500}
    };

    std::unordered_map<int32_t, TabletSchema::PathsSetInfo> uid_to_paths_set_info;
    schema_util::get_subpaths(variant, path_stats, uid_to_paths_set_info);

    EXPECT_EQ(uid_to_paths_set_info[1].sub_path_set.size(), 3);
    EXPECT_EQ(uid_to_paths_set_info[1].sparse_path_set.size(), 0);

    EXPECT_TRUE(uid_to_paths_set_info[1].sub_path_set.find("path1") != uid_to_paths_set_info[1].sub_path_set.end());
    EXPECT_TRUE(uid_to_paths_set_info[1].sub_path_set.find("path2") != uid_to_paths_set_info[1].sub_path_set.end());
    EXPECT_TRUE(uid_to_paths_set_info[1].sub_path_set.find("path3") != uid_to_paths_set_info[1].sub_path_set.end());
}

TEST_F(SchemaUtilTest, get_subpaths_multiple_variants) {
    TabletColumn variant1;
    variant1.set_unique_id(1);
    variant1.set_variant_max_subcolumns_count(3);

    TabletColumn variant2;
    variant2.set_unique_id(2);
    variant2.set_variant_max_subcolumns_count(2);

    TabletColumn variant3;
    variant3.set_unique_id(3);
    variant3.set_variant_max_subcolumns_count(4);

    std::unordered_map<int32_t, schema_util::PathToNoneNullValues> path_stats;
    path_stats[1] = {
        {"path1", 1000},
        {"path2", 800},
        {"path3", 500},
        {"path4", 300},
        {"path5", 200}
    };
    path_stats[2] = {
        {"path1", 1000},
        {"path2", 800}
    };
    path_stats[3] = {
        {"path1", 1000},
        {"path2", 800},
        {"path3", 500},
        {"path4", 300}
    };
    path_stats[4] = {
        {"path1", 1000},
        {"path2", 800},
        {"path3", 500},
        {"path4", 300},
        {"path5", 200}
    };

    std::unordered_map<int32_t, TabletSchema::PathsSetInfo> uid_to_paths_set_info;
    schema_util::get_subpaths(variant1, path_stats, uid_to_paths_set_info);
    schema_util::get_subpaths(variant2, path_stats, uid_to_paths_set_info);
    schema_util::get_subpaths(variant3, path_stats, uid_to_paths_set_info);

    EXPECT_EQ(uid_to_paths_set_info[1].sub_path_set.size(), 3);
    EXPECT_EQ(uid_to_paths_set_info[1].sparse_path_set.size(), 2);

    EXPECT_EQ(uid_to_paths_set_info[2].sub_path_set.size(), 2);
    EXPECT_EQ(uid_to_paths_set_info[2].sparse_path_set.size(), 0);

    EXPECT_EQ(uid_to_paths_set_info[3].sub_path_set.size(), 4);
    EXPECT_EQ(uid_to_paths_set_info[3].sparse_path_set.size(), 0);

    EXPECT_TRUE(uid_to_paths_set_info[1].sub_path_set.find("path1") != uid_to_paths_set_info[1].sub_path_set.end());
    EXPECT_TRUE(uid_to_paths_set_info[1].sub_path_set.find("path2") != uid_to_paths_set_info[1].sub_path_set.end());
    EXPECT_TRUE(uid_to_paths_set_info[1].sub_path_set.find("path3") != uid_to_paths_set_info[1].sub_path_set.end());

    EXPECT_TRUE(uid_to_paths_set_info[1].sparse_path_set.find("path4") != uid_to_paths_set_info[1].sparse_path_set.end());
    EXPECT_TRUE(uid_to_paths_set_info[1].sparse_path_set.find("path5") != uid_to_paths_set_info[1].sparse_path_set.end());

    EXPECT_TRUE(uid_to_paths_set_info[2].sub_path_set.find("path1") != uid_to_paths_set_info[2].sub_path_set.end());
    EXPECT_TRUE(uid_to_paths_set_info[2].sub_path_set.find("path2") != uid_to_paths_set_info[2].sub_path_set.end());

    EXPECT_TRUE(uid_to_paths_set_info[3].sub_path_set.find("path1") != uid_to_paths_set_info[3].sub_path_set.end());
    EXPECT_TRUE(uid_to_paths_set_info[3].sub_path_set.find("path2") != uid_to_paths_set_info[3].sub_path_set.end());
    EXPECT_TRUE(uid_to_paths_set_info[3].sub_path_set.find("path3") != uid_to_paths_set_info[3].sub_path_set.end());
    EXPECT_TRUE(uid_to_paths_set_info[3].sub_path_set.find("path4") != uid_to_paths_set_info[3].sub_path_set.end());
}

TEST_F(SchemaUtilTest, get_subpaths_no_path_stats) {
    TabletColumn variant;
    variant.set_unique_id(1);
    variant.set_variant_max_subcolumns_count(3);

    std::unordered_map<int32_t, schema_util::PathToNoneNullValues> path_stats;
    path_stats[2] = {
        {"path1", 1000},
        {"path2", 800}
    };

    std::unordered_map<int32_t, TabletSchema::PathsSetInfo> uid_to_paths_set_info;
    schema_util::get_subpaths(variant, path_stats, uid_to_paths_set_info);

    EXPECT_EQ(uid_to_paths_set_info[1].sub_path_set.size(), 0);
    EXPECT_EQ(uid_to_paths_set_info[1].sparse_path_set.size(), 0);
}

void construct_column(ColumnPB* column_pb, int32_t col_unique_id,
                        const std::string& column_type, const std::string& column_name, bool is_key = false) {
    column_pb->set_unique_id(col_unique_id);
    column_pb->set_name(column_name);
    column_pb->set_type(column_type);
    column_pb->set_is_key(is_key);   
    column_pb->set_is_nullable(false);
    if (column_type == "VARIANT") {
        column_pb->set_variant_max_subcolumns_count(3);
    }
}

void construct_tablet_index(TabletIndexPB* tablet_index, int64_t index_id, const std::string& index_name, int32_t col_unique_id) {
    tablet_index->set_index_id(index_id);
    tablet_index->set_index_name(index_name);
    tablet_index->set_index_type(IndexType::INVERTED);
    tablet_index->add_col_unique_id(col_unique_id);
}

void fill_string_column_with_test_data(auto& column_string, int size) {
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
        }
        json_str += "}";
        vectorized::Field str(json_str);
        column_string->insert_data(json_str.data(), json_str.size());
    }
}

void fill_varaint_column(auto& variant_column, int size) {
    auto type_string =std::make_shared<vectorized::DataTypeString>();
    auto column = type_string->create_column();
    auto column_string = assert_cast<ColumnString*>(column.get());
    fill_string_column_with_test_data(column_string, size);
    vectorized::ParseConfig config;
    config.enable_flatten_nested = false;
    //ColumnObject* variant_column_object = assert_cast<ColumnObject*>(variant_column.get());
    parse_json_to_variant(*variant_column, *column_string, config);
}

void fill_block_with_test_data(vectorized::Block* block, int size) {
    auto columns = block->mutate_columns();
    // insert key
    for (int i = 0; i < size; i++) {
        vectorized::Field key = i;
        columns[0]->insert(key);
    }

    // insert v1
    fill_varaint_column(columns[1], size);

    // insert v2
    for (int i = 0; i < size; i++) {
        vectorized::Field v2("V2");
        columns[2]->insert(v2);
    }

    //insert v3
    fill_varaint_column(columns[3], size);

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
        rowset_id.init(inc_id++);
        context.rowset_id = rowset_id;
        context.rowset_type = BETA_ROWSET;
        context.data_dir = data_dir.get();
        context.rowset_state = VISIBLE;
        context.tablet_schema = schema;
        context.tablet_path = tablet_path;
        context.version = Version(inc_id, inc_id);
        context.max_rows_per_segment = 200;
        return context;
    }

TEST_F(SchemaUtilTest, collect_path_stats) {
    TabletSchemaPB schema_pb;

    construct_column(schema_pb.add_column(), 0, "INT", "key", true);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "v1");
    construct_column(schema_pb.add_column(), 2, "STRING", "v2");
    construct_column(schema_pb.add_column(), 3, "VARIANT", "v3");
    construct_column(schema_pb.add_column(), 4, "INT", "v4");

    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(schema_pb);

    TabletMetaSharedPtr tablet_meta(new TabletMeta(tablet_schema));

    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());


    const auto& res = RowsetFactory::create_rowset_writer(
                    *_engine_ref, rowset_writer_context(_data_dir, tablet_schema, _tablet->tablet_path()),
                    false);
    EXPECT_TRUE(res.has_value()) << res.error();
    const auto& rowset_writer = res.value();

    vectorized::Block block = tablet_schema->create_block();
   
    fill_block_with_test_data(&block, 1000);
    auto st = rowset_writer->add_block(&block);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = rowset_writer->flush();
    EXPECT_TRUE(st.ok()) << st.msg();

    RowsetSharedPtr rowset;
    EXPECT_TRUE(rowset_writer->build(rowset).ok());
    EXPECT_TRUE(_tablet->add_rowset(rowset).ok());
    EXPECT_TRUE(rowset->num_segments() == 5);
}


