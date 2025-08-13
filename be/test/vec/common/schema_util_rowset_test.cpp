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
#include "olap/rowset/segment_v2/variant/variant_column_writer_impl.h"
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
                             bool is_key = false, bool add_children = false) {
    column_pb->set_unique_id(col_unique_id);
    column_pb->set_name(column_name);
    column_pb->set_type(column_type);
    column_pb->set_is_key(is_key);
    column_pb->set_is_nullable(false);
    if (column_type == "VARIANT") {
        column_pb->set_variant_max_subcolumns_count(3);
        if (add_children) {
            ColumnPB* child = column_pb->add_children_columns();
            child->set_name("key0");
            child->set_type("STRING");
        }
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
        // vectorized::Field str(json_str);
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
        auto field = vectorized::Field::create_field<PrimitiveType::TYPE_BIGINT>(i);
        columns[0]->insert(field);
    }

    // insert v1
    fill_varaint_column(columns[1], size, 1);

    // insert v2
    for (int i = 0; i < size; i++) {
        auto v2 = vectorized::Field::create_field<PrimitiveType::TYPE_STRING>("V2");
        columns[2]->insert(v2);
    }

    // insert v3
    fill_varaint_column(columns[3], size, 3);

    // insert v4
    for (int i = 0; i < size; i++) {
        auto v4 = vectorized::Field::create_field<PrimitiveType::TYPE_BIGINT>(i);
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

TEST_F(SchemaUtilRowsetTest, check_path_stats_agg_key) {
    // 1.create tablet schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(AGG_KEYS);
    construct_column(schema_pb.add_column(), 0, "INT", "key", true);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "v1");
    construct_column(schema_pb.add_column(), 2, "STRING", "v2");
    construct_column(schema_pb.add_column(), 3, "VARIANT", "v3");
    construct_column(schema_pb.add_column(), 4, "INT", "v4");
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(tablet_schema));
    std::string absolute_dir = _curreent_dir + std::string("/ut_dir/schema_util_rows");
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(absolute_dir).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(absolute_dir).ok());
    std::unique_ptr<DataDir> _data_dir = std::make_unique<DataDir>(*_engine_ref, absolute_dir);
    static_cast<void>(_data_dir->update_capacity());
    EXPECT_TRUE(_data_dir->init(true).ok());

    TabletSharedPtr _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
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

    // 7. check output rowset
    EXPECT_TRUE(schema_util::VariantCompactionUtil::check_path_stats(rowsets, rowsets[0], _tablet)
                        .ok());
}

TEST_F(SchemaUtilRowsetTest, check_path_stats_agg_delete) {
    // 1.create tablet schema
    TabletSchemaPB schema_pb;
    schema_pb.set_delete_sign_idx(0);
    construct_column(schema_pb.add_column(), 0, "INT", "key", true);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "v1");
    construct_column(schema_pb.add_column(), 2, "STRING", "v2");
    construct_column(schema_pb.add_column(), 3, "VARIANT", "v3");
    construct_column(schema_pb.add_column(), 4, "INT", "v4");
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(tablet_schema));
    std::string absolute_dir = _curreent_dir + std::string("/ut_dir/schema_util_rows1");
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(absolute_dir).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(absolute_dir).ok());
    std::unique_ptr<DataDir> _data_dir = std::make_unique<DataDir>(*_engine_ref, absolute_dir);
    static_cast<void>(_data_dir->update_capacity());
    EXPECT_TRUE(_data_dir->init(true).ok());

    TabletSharedPtr _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
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

    // 7. check output rowset
    Status st = schema_util::VariantCompactionUtil::check_path_stats(rowsets, rowsets[0], _tablet);
    std::cout << st.to_string() << std::endl;
    EXPECT_FALSE(st.ok());
}

TEST_F(SchemaUtilRowsetTest, collect_path_stats_and_get_extended_compaction_schema) {
    all_path_stats.clear();
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

    // 4. get compaction schema
    TabletSchemaSPtr compaction_schema = tablet_schema;

    for (const auto& column : compaction_schema->columns()) {
        if (column->is_extracted_column()) {
            EXPECT_FALSE(column->is_variant_type());
        }
    }

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

    // 6.compaction for output rs
    // create input rowset reader
    std::vector<RowsetReaderSharedPtr> input_rs_readers;
    for (auto& rowset : rowsets) {
        RowsetReaderSharedPtr rs_reader;
        ASSERT_TRUE(rowset->create_reader(&rs_reader).ok());
        input_rs_readers.push_back(std::move(rs_reader));
    }

    // create output rowset writer
    auto create_rowset_writer_context = [this](TabletSchemaSPtr tablet_schema,
                                               const SegmentsOverlapPB& overlap,
                                               uint32_t max_rows_per_segment, Version version) {
        static int64_t inc_id = 1000;
        RowsetWriterContext rowset_writer_context;
        RowsetId rowset_id;
        rowset_id.init(inc_id);
        rowset_writer_context.rowset_id = rowset_id;
        rowset_writer_context.rowset_type = BETA_ROWSET;
        rowset_writer_context.rowset_state = VISIBLE;
        rowset_writer_context.tablet_schema = tablet_schema;
        rowset_writer_context.tablet_path = _absolute_dir + "/../";
        rowset_writer_context.version = version;
        rowset_writer_context.segments_overlap = overlap;
        rowset_writer_context.max_rows_per_segment = max_rows_per_segment;
        inc_id++;
        return rowset_writer_context;
    };
    auto writer_context = create_rowset_writer_context(tablet_schema, NONOVERLAPPING, 3456,
                                                       {0, rowsets.back()->end_version()});
    auto res_ = RowsetFactory::create_rowset_writer(*_engine_ref, writer_context, true);
    ASSERT_TRUE(res_.has_value()) << res_.error();
    auto output_rs_writer = std::move(res_).value();
    Merger::Statistics stats;
    RowIdConversion rowid_conversion;
    stats.rowid_conversion = &rowid_conversion;
    auto s = Merger::vertical_merge_rowsets(_tablet, ReaderType::READER_BASE_COMPACTION,
                                            *tablet_schema, input_rs_readers,
                                            output_rs_writer.get(), 100, 5, &stats);
    ASSERT_TRUE(s.ok()) << s;
    RowsetSharedPtr out_rowset;
    EXPECT_EQ(Status::OK(), output_rs_writer->build(out_rowset));
    ASSERT_TRUE(out_rowset);

    // check no variant subcolumns in output rowset
    for (const auto& column : out_rowset->tablet_schema()->columns()) {
        EXPECT_FALSE(column->is_extracted_column());
    }

    // 7. check output rowset
    EXPECT_TRUE(schema_util::VariantCompactionUtil::check_path_stats(rowsets, out_rowset, _tablet)
                        .ok());
}

TabletSchemaSPtr create_compaction_schema_common(StorageEngine* _engine_ref,
                                                 std::string _absolute_dir) {
    all_path_stats.clear();
    // 1.create tablet schema
    TabletSchemaPB schema_pb;
    construct_column(schema_pb.add_column(), 0, "INT", "key", true);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "v1", false, true);
    construct_column(schema_pb.add_column(), 2, "STRING", "v2");
    construct_column(schema_pb.add_column(), 3, "VARIANT", "v3", false, true);
    construct_column(schema_pb.add_column(), 4, "INT", "v4");
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(tablet_schema));
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_absolute_dir).ok());
    std::unique_ptr<DataDir> _data_dir = std::make_unique<DataDir>(*_engine_ref, _absolute_dir);
    static_cast<void>(_data_dir->update_capacity());
    Status st1 = _data_dir->init(true);
    EXPECT_TRUE(st1.ok()) << st1.msg();
    std::shared_ptr<Tablet> _tablet =
            std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create rowset
    std::vector<RowsetSharedPtr> rowsets;
    for (int i = 0; i < 1; i++) {
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
        auto st = schema_util::VariantCompactionUtil::aggregate_path_to_stats(rowset, &path_stats);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    for (const auto& [uid, path_stats] : path_stats) {
        for (const auto& [path, size] : path_stats) {
            EXPECT_EQ(all_path_stats[uid][path], size);
        }
    }

    // 4. get compaction schema
    TabletSchemaSPtr compaction_schema = tablet_schema;
    auto st = schema_util::VariantCompactionUtil::get_extended_compaction_schema(rowsets,
                                                                                 compaction_schema);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 5. check compaction schema
    std::unordered_map<int32_t, std::vector<std::string>> compaction_schema_map;
    for (const auto& column : compaction_schema->columns()) {
        if (column->parent_unique_id() > 0) {
            compaction_schema_map[column->parent_unique_id()].push_back(column->name());
        }
    }
    for (auto& [uid, paths] : compaction_schema_map) {
        std::sort(paths.begin(), paths.end());
        std::cout << "path[0]: " << paths[0] << std::endl;
        std::cout << "path[1]: " << paths[1] << std::endl;
        std::cout << "path[2]: " << paths[2] << std::endl;
        std::cout << "path[3]: " << paths[3] << std::endl;
        std::cout << "path[4]: " << paths[4] << std::endl;
        EXPECT_EQ(paths.size(), 5);
        EXPECT_TRUE(paths[0].ends_with("__DORIS_VARIANT_SPARSE__"));
        EXPECT_TRUE(paths[2].ends_with("key1"));
        EXPECT_TRUE(paths[3].ends_with("key2"));
        EXPECT_TRUE(paths[4].ends_with("key3"));
    }
    return compaction_schema;
}

TEST_F(SchemaUtilRowsetTest, some_test_for_subcolumn_writer) {
    std::string absolute_dir = _curreent_dir + std::string("/ut_dir/schema_util_rows2");
    TabletSchemaSPtr compaction_schema = create_compaction_schema_common(_engine_ref, absolute_dir);
    // 6. create variantSubColumnWriter
    // 6.1. Create file writer
    io::FileWriterPtr file_writer;
    std::string new_tablet_path = absolute_dir + "/tmp_data/";
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(new_tablet_path).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(new_tablet_path).ok());
    auto file_path = local_segment_path(new_tablet_path, "0", 0);
    auto st1 = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st1.ok()) << st1.msg();
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_COMPACTION;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = compaction_schema;
    // create sub column with pathinfo
    std::cout << compaction_schema->dump_structure() << std::endl;
    // this is v1.key1
    TabletColumn column = compaction_schema->column(2);
    _init_column_meta(opts.meta, 0, column, CompressionTypePB::LZ4);
    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(
            ColumnWriter::create_variant_writer(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantSubcolumnWriter*>(writer.get()) != nullptr);
    auto variant_subcolumn_writer = assert_cast<VariantSubcolumnWriter*>(writer.get());
    // then we can do some thing for sub_writer
    // estimate buffer size
    auto size = variant_subcolumn_writer->estimate_buffer_size();
    std::cout << "size: " << size << std::endl;
    // append data
    auto insert_object = ColumnVariant::create(true);
    fill_varaint_column(insert_object, 1, 1);
    std::cout << insert_object->debug_string() << std::endl;
    std::unique_ptr<VariantColumnData> _variant_column_data = std::make_unique<VariantColumnData>();
    _variant_column_data->column_data = insert_object.get();
    _variant_column_data->row_pos = 0;
    const uint8_t* data = (const uint8_t*)_variant_column_data.get();
    EXPECT_TRUE(variant_subcolumn_writer->append_data(&data, 1));
    // write null data
    EXPECT_TRUE(variant_subcolumn_writer->write_data().ok());
}

TEST_F(SchemaUtilRowsetTest, typed_path_to_sparse_column) {
    all_path_stats.clear();
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

    // 4. get compaction schema
    TabletSchemaSPtr compaction_schema = tablet_schema;
    auto st = schema_util::VariantCompactionUtil::get_extended_compaction_schema(rowsets,
                                                                                 compaction_schema);
    EXPECT_TRUE(st.ok()) << st.msg();
    for (const auto& column : compaction_schema->columns()) {
        if (column->is_extracted_column()) {
            EXPECT_FALSE(column->is_variant_type());
        }
    }

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

    // 6.compaction for output rs
    // create input rowset reader
    std::vector<RowsetReaderSharedPtr> input_rs_readers;
    for (auto& rowset : rowsets) {
        RowsetReaderSharedPtr rs_reader;
        ASSERT_TRUE(rowset->create_reader(&rs_reader).ok());
        input_rs_readers.push_back(std::move(rs_reader));
    }

    // create output rowset writer
    auto create_rowset_writer_context = [this](TabletSchemaSPtr tablet_schema,
                                               const SegmentsOverlapPB& overlap,
                                               uint32_t max_rows_per_segment, Version version) {
        static int64_t inc_id = 1000;
        RowsetWriterContext rowset_writer_context;
        RowsetId rowset_id;
        rowset_id.init(inc_id);
        rowset_writer_context.rowset_id = rowset_id;
        rowset_writer_context.rowset_type = BETA_ROWSET;
        rowset_writer_context.rowset_state = VISIBLE;
        rowset_writer_context.tablet_schema = tablet_schema;
        rowset_writer_context.tablet_path = _absolute_dir + "/../";
        rowset_writer_context.version = version;
        rowset_writer_context.segments_overlap = overlap;
        rowset_writer_context.max_rows_per_segment = max_rows_per_segment;
        inc_id++;
        return rowset_writer_context;
    };
    auto writer_context = create_rowset_writer_context(tablet_schema, NONOVERLAPPING, 3456,
                                                       {0, rowsets.back()->end_version()});
    auto res_ = RowsetFactory::create_rowset_writer(*_engine_ref, writer_context, true);
    ASSERT_TRUE(res_.has_value()) << res_.error();
    auto output_rs_writer = std::move(res_).value();
    Merger::Statistics stats;
    RowIdConversion rowid_conversion;
    stats.rowid_conversion = &rowid_conversion;
    auto s = Merger::vertical_merge_rowsets(_tablet, ReaderType::READER_BASE_COMPACTION,
                                            *tablet_schema, input_rs_readers,
                                            output_rs_writer.get(), 100, 5, &stats);
    ASSERT_TRUE(s.ok()) << s;
    RowsetSharedPtr out_rowset;
    EXPECT_EQ(Status::OK(), output_rs_writer->build(out_rowset));
    ASSERT_TRUE(out_rowset);

    // check no variant subcolumns in output rowset
    for (const auto& column : out_rowset->tablet_schema()->columns()) {
        EXPECT_FALSE(column->is_extracted_column());
    }

    // 7. check output rowset
    EXPECT_TRUE(schema_util::VariantCompactionUtil::check_path_stats(rowsets, out_rowset, _tablet)
                        .ok());
}
