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

#include "gtest/gtest.h"
#include "olap/cumulative_compaction.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/segment_v2/variant/variant_column_writer_impl.h"
#include "olap/storage_engine.h"
#include "olap/tablet_schema.h"
#include "testutil/schema_utils.h"
#include "vec/common/schema_util.h"
#include "vec/json/parse2column.h"

using namespace doris::vectorized;

namespace doris {

constexpr static uint32_t MAX_PATH_LEN = 1024;
constexpr static std::string_view dest_dir = "/ut_dir/variant_rowset_compaction_test";
constexpr static std::string_view tmp_dir = "./ut_dir/tmp";

class VariantRowsetCompactionTest : public testing::Test {
public:
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
        // _engine_ref->init();
        _data_dir = std::make_unique<DataDir>(*_engine_ref, _absolute_dir);
        static_cast<void>(_data_dir->update_capacity());
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
        _engine_ref = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    VariantRowsetCompactionTest() = default;
    ~VariantRowsetCompactionTest() override = default;

private:
    TabletSchemaSPtr _tablet_schema = nullptr;
    StorageEngine* _engine_ref = nullptr;
    std::unique_ptr<DataDir> _data_dir = nullptr;
    TabletSharedPtr _tablet = nullptr;
    std::string _absolute_dir;
    std::string _current_dir;
};

static std::unordered_map<int32_t, schema_util::PathToNoneNullValues> all_path_stats;
static void fill_string_column_with_test_data(auto& column_string, int size, int uid) {
    std::srand(42);
    for (int i = 0; i < size; i++) {
        std::string json_str = "{";
        int num_pairs = std::rand() % 10 + 1;
        for (int j = 0; j < num_pairs; j++) {
            std::string key = "key" + std::to_string(j);
            if (j % 2 == 0) {
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

TEST_F(VariantRowsetCompactionTest, test_variant_rowset_compaction) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    SchemaUtils::construct_column(schema_pb.add_column(), 1, "INT", "id", 3, true);
    SchemaUtils::construct_column(schema_pb.add_column(), 2, "VARIANT", "V1");
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    tablet_meta->_tablet_id = 10000;
    tablet_meta->set_tablet_uid(TabletUid(10, 10));
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());

    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create rowset
    std::vector<RowsetSharedPtr> rowsets;
    for (int i = 0; i < 5; i++) {
        const auto& res = RowsetFactory::create_rowset_writer(
                *_engine_ref,
                rowset_writer_context(_data_dir, _tablet_schema, _tablet->tablet_path()), false);
        EXPECT_TRUE(res.has_value()) << res.error();
        const auto& rowset_writer = res.value();
        auto rowset = create_rowset(rowset_writer, _tablet_schema);
        EXPECT_TRUE(_tablet->add_rowset(rowset).ok());
        rowsets.push_back(rowset);
    }

    // 4. compaction
    CumulativeCompaction compaction(*_engine_ref, _tablet);
    compaction._input_rowsets = std::move(rowsets);
    compaction.build_basic_info();

    std::vector<RowsetReaderSharedPtr> input_rs_readers;
    input_rs_readers.reserve(compaction._input_rowsets.size());
    for (auto& rowset : compaction._input_rowsets) {
        RowsetReaderSharedPtr rs_reader;
        EXPECT_TRUE(rowset->create_reader(&rs_reader).ok());
        input_rs_readers.push_back(std::move(rs_reader));
    }

    RowsetWriterContext ctx;
    EXPECT_TRUE(compaction.construct_output_rowset_writer(ctx).ok());

    const auto& path_map = compaction._cur_tablet_schema->_path_set_info_map;
    EXPECT_EQ(path_map.size(), 1);

    compaction._stats.rowid_conversion = compaction._rowid_conversion.get();
    EXPECT_TRUE(Merger::vertical_merge_rowsets(_tablet, compaction.compaction_type(),
                                               *(compaction._cur_tablet_schema), input_rs_readers,
                                               compaction._output_rs_writer.get(), 100000, 5,
                                               &compaction._stats)
                        .ok());
    st = compaction._output_rs_writer->build(compaction._output_rowset);
    EXPECT_TRUE(st.ok()) << st.to_string();

    EXPECT_TRUE(compaction._output_rowset->num_segments() == 1);

    const auto& output_rowset = compaction._output_rowset;
    EXPECT_TRUE(output_rowset->num_rows() == 5000);

    const auto& after_compaction_path_map =
            output_rowset->rowset_meta()->tablet_schema()->_path_set_info_map;
    EXPECT_EQ(after_compaction_path_map.size(), 0);

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

} // namespace doris
