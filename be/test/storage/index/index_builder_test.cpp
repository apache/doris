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

#include "storage/task/index_builder.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <filesystem>

#include "common/config.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_writer.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/olap_common.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet_fwd.h"
#include "storage/tablet/tablet_schema.h"
#include "util/debug_points.h"

namespace doris {
using namespace testing;

class ScopedIndexBuilderDebugPoints {
public:
    ScopedIndexBuilderDebugPoints() : _debug_points_enabled(config::enable_debug_points) {
        config::enable_debug_points = true;
        DebugPoints::instance()->clear();
    }

    ~ScopedIndexBuilderDebugPoints() {
        DebugPoints::instance()->clear();
        config::enable_debug_points = _debug_points_enabled;
    }

    void enable(const std::string& name) { DebugPoints::instance()->add(name); }

private:
    bool _debug_points_enabled;
};

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
        tablet_schema->_inverted_index_storage_format = InvertedIndexStorageFormatPB::V2;

        // Create the first key column
        TabletColumn column_1;
        column_1.set_type(FieldType::OLAP_FIELD_TYPE_INT);
        column_1.set_unique_id(1);
        column_1.set_name("k1");
        column_1.set_is_key(true);
        column_1.set_index_length(4);
        tablet_schema->append_column(column_1);

        // Create the second key column
        TabletColumn column_2;
        column_2.set_type(FieldType::OLAP_FIELD_TYPE_INT);
        column_2.set_unique_id(2);
        column_2.set_name("k2");
        column_2.set_is_key(false);
        tablet_schema->append_column(column_2);
    }

    TabletSchemaSPtr create_ann_tablet_schema() {
        TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
        TabletSchemaPB tablet_schema_pb;
        tablet_schema_pb.set_keys_type(DUP_KEYS);
        tablet_schema->init_from_pb(tablet_schema_pb);
        // Set basic properties of TabletSchema directly
        tablet_schema->_inverted_index_storage_format = InvertedIndexStorageFormatPB::V2;

        TabletColumn array_column;
        array_column.set_name("arr1");
        array_column.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
        array_column.set_unique_id(1);
        array_column.set_length(0);
        array_column.set_index_length(0);
        array_column.set_is_nullable(false);

        TabletColumn child_column;
        child_column.set_name("arr_sub_float");
        child_column.set_type(FieldType::OLAP_FIELD_TYPE_FLOAT);
        child_column.set_length(INT_MAX);
        array_column.add_sub_column(child_column);
        tablet_schema->append_column(array_column);
        return tablet_schema;
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

    void prepare_single_index_build(int64_t rowset_id) {
        auto tablet_path = _absolute_dir + "/" + std::to_string(rowset_id);
        _tablet->_tablet_path = tablet_path;
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(tablet_path).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet_path).ok());

        RowsetWriterContext writer_context;
        writer_context.rowset_id.init(rowset_id);
        writer_context.tablet_id = _tablet->tablet_id();
        writer_context.tablet_schema_hash = _tablet_meta->schema_hash();
        writer_context.partition_id = 10;
        writer_context.rowset_type = BETA_ROWSET;
        writer_context.tablet_path = tablet_path;
        writer_context.rowset_state = VISIBLE;
        writer_context.tablet_schema = _tablet_schema;
        writer_context.version = Version(10, 10);

        auto result = RowsetFactory::create_rowset_writer(*_engine_ref, writer_context, false);
        ASSERT_TRUE(result.has_value()) << result.error();
        auto rowset_writer = std::move(result).value();

        Block block = _tablet_schema->create_block();
        auto columns = std::move(block).mutate_columns();
        for (int i = 0; i < 8; ++i) {
            int32_t k1 = i * 10;
            int32_t k2 = i;
            columns[0]->insert_data(reinterpret_cast<const char*>(&k1), sizeof(k1));
            columns[1]->insert_data(reinterpret_cast<const char*>(&k2), sizeof(k2));
        }
        block.set_columns(std::move(columns));
        ASSERT_TRUE(rowset_writer->add_block(&block).ok());
        ASSERT_TRUE(rowset_writer->flush().ok());

        RowsetSharedPtr rowset;
        ASSERT_TRUE(rowset_writer->build(rowset).ok());
        ASSERT_TRUE(_tablet->add_rowset(rowset).ok());

        TOlapTableIndex index;
        index.index_id = 101;
        index.index_name = "k1_index";
        index.columns.emplace_back("k1");
        index.column_unique_ids.push_back(1);
        index.index_type = TIndexType::INVERTED;
        _alter_indexes.push_back(std::move(index));
    }

    Status build_single_index() {
        IndexBuilder builder(*_engine_ref, _tablet, _columns, _alter_indexes, false);
        RETURN_IF_ERROR(builder.init());
        return builder.do_build_inverted_index();
    }

    // One SNII inverted index for the schema/plan helpers below.
    struct SniiIndexSpec {
        int64_t index_id;
        std::string_view index_name;
        int32_t column_unique_id;
        std::map<std::string, std::string> properties = {
                {"parser", "english"}, {"lower_case", "true"}, {"support_phrase", "true"}};
    };

    // SNII schema with k1(uid 1, key) + body_a(uid 2) + body_b(uid 3) and the
    // given inverted indexes.
    static TabletSchemaSPtr create_snii_schema(const std::vector<SniiIndexSpec>& indexes) {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(DUP_KEYS);
        schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::SNII);
        auto tablet_schema = std::make_shared<TabletSchema>();
        tablet_schema->init_from_pb(schema_pb);

        TabletColumn key_column;
        key_column.set_unique_id(1);
        key_column.set_name("k1");
        key_column.set_type(FieldType::OLAP_FIELD_TYPE_INT);
        key_column.set_length(4);
        key_column.set_index_length(4);
        key_column.set_is_key(true);
        key_column.set_is_nullable(false);
        tablet_schema->append_column(key_column);

        for (const auto& [unique_id, name] : {std::pair<int32_t, std::string_view> {2, "body_a"},
                                              std::pair<int32_t, std::string_view> {3, "body_b"}}) {
            TabletColumn column;
            column.set_unique_id(unique_id);
            column.set_name(std::string(name));
            column.set_type(FieldType::OLAP_FIELD_TYPE_VARCHAR);
            column.set_length(65535);
            column.set_is_nullable(false);
            tablet_schema->append_column(column);
        }

        for (const auto& spec : indexes) {
            TabletIndex index;
            index._index_id = spec.index_id;
            index._index_name = spec.index_name;
            index._index_type = IndexType::INVERTED;
            index._col_unique_ids.push_back(spec.column_unique_id);
            for (const auto& [key, value] : spec.properties) {
                index._properties[key] = value;
            }
            tablet_schema->append_index(std::move(index));
        }
        return tablet_schema;
    }

    static TabletSchemaSPtr create_snii_drop_schema() {
        return create_snii_schema(
                {SniiIndexSpec {.index_id = 1, .index_name = "idx_a", .column_unique_id = 2},
                 SniiIndexSpec {.index_id = 2, .index_name = "idx_b", .column_unique_id = 3}});
    }

    Status create_snii_drop_tablet(const TabletSchemaSPtr& tablet_schema,
                                   const std::string& tablet_path, TabletSharedPtr* tablet) {
        RETURN_IF_ERROR(io::global_local_filesystem()->delete_directory(tablet_path));
        RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(tablet_path));
        auto tablet_meta = create_tablet_meta();
        tablet_meta->_schema = tablet_schema;
        *tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
        (*tablet)->_tablet_path = tablet_path;
        return (*tablet)->init();
    }

    Status create_snii_source_rowset(const TabletSharedPtr& tablet,
                                     const TabletSchemaSPtr& tablet_schema,
                                     const std::string& tablet_path,
                                     RowsetSharedPtr* rowset) const {
        RowsetWriterContext writer_context;
        writer_context.rowset_id.init(15691);
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 10;
        writer_context.rowset_type = BETA_ROWSET;
        writer_context.tablet_path = tablet_path;
        writer_context.rowset_state = VISIBLE;
        writer_context.tablet_schema = tablet_schema;
        writer_context.version = Version(10, 10);

        auto rowset_writer =
                DORIS_TRY(RowsetFactory::create_rowset_writer(*_engine_ref, writer_context, false));
        Block block = tablet_schema->create_block();
        auto columns = std::move(block).mutate_columns();
        const std::vector<std::string> dropped_values = {"drop alpha", "drop beta"};
        const std::vector<std::string> surviving_values = {"keep alpha", "keep beta"};
        for (int32_t i = 0; i < 2; ++i) {
            columns[0]->insert_data(reinterpret_cast<const char*>(&i), sizeof(i));
            columns[1]->insert_data(dropped_values[i].data(), dropped_values[i].size());
            columns[2]->insert_data(surviving_values[i].data(), surviving_values[i].size());
        }
        block = tablet_schema->create_block();
        block.set_columns(std::move(columns));
        RETURN_IF_ERROR(rowset_writer->add_block(&block));
        RETURN_IF_ERROR(rowset_writer->flush());
        RETURN_IF_ERROR(rowset_writer->build(*rowset));
        return tablet->add_rowset(*rowset);
    }

    static TOlapTableIndex create_drop_index(int64_t index_id, std::string index_name,
                                             std::string column_name, int32_t column_unique_id) {
        TOlapTableIndex index;
        index.index_id = index_id;
        index.index_name = std::move(index_name);
        index.index_type = TIndexType::INVERTED;
        index.columns.emplace_back(std::move(column_name));
        index.column_unique_ids.push_back(column_unique_id);
        return index;
    }

    Status drop_snii_index(const TabletSharedPtr& tablet, TOlapTableIndex index,
                           RowsetSharedPtr* output_rowset) const {
        std::vector<TOlapTableIndex> drop_indexes {std::move(index)};
        IndexBuilder builder(*_engine_ref, tablet, _columns, drop_indexes, true);
        RETURN_IF_ERROR(builder.init());
        RETURN_IF_ERROR(builder.do_build_inverted_index());
        DORIS_CHECK_EQ(builder._output_rowsets.size(), 1);
        *output_rowset = builder._output_rowsets.front();
        return Status::OK();
    }

    static TOlapTableIndex create_build_index(int64_t index_id, std::string index_name,
                                              std::string column_name, int32_t column_unique_id,
                                              std::map<std::string, std::string> properties) {
        TOlapTableIndex index = create_drop_index(index_id, std::move(index_name),
                                                  std::move(column_name), column_unique_id);
        index.__set_properties(properties);
        return index;
    }

    // Runs a BUILD INDEX task; rowsets whose schema already carries every
    // requested index are skipped upstream (pick_candidate_rowsets), so the
    // output may legitimately be empty.
    Status build_snii_index(const TabletSharedPtr& tablet, std::vector<TOlapTableIndex> indexes,
                            std::vector<RowsetSharedPtr>* output_rowsets) const {
        IndexBuilder builder(*_engine_ref, tablet, _columns, indexes, false);
        RETURN_IF_ERROR(builder.init());
        RETURN_IF_ERROR(builder.do_build_inverted_index());
        *output_rowsets = builder._output_rowsets;
        return Status::OK();
    }

    static std::string snii_index_path_of(const RowsetSharedPtr& rowset) {
        auto segment_path = rowset->segment_path(0);
        EXPECT_TRUE(segment_path.has_value()) << segment_path.error();
        return segment_v2::InvertedIndexDescriptor::get_index_file_path_v2(
                segment_v2::InvertedIndexDescriptor::get_index_file_path_prefix(
                        segment_path.value()));
    }

    static std::unique_ptr<segment_v2::IndexFileReader> open_snii_reader(
            const RowsetSharedPtr& rowset, int64_t tablet_id) {
        auto segment_path = rowset->segment_path(0);
        EXPECT_TRUE(segment_path.has_value()) << segment_path.error();
        const std::string index_path_prefix {
                segment_v2::InvertedIndexDescriptor::get_index_file_path_prefix(
                        segment_path.value())};
        auto reader = std::make_unique<segment_v2::IndexFileReader>(
                io::global_local_filesystem(), index_path_prefix,
                InvertedIndexStorageFormatPB::SNII, InvertedIndexFileInfo(), tablet_id);
        EXPECT_TRUE(reader->init().ok());
        return reader;
    }

    // Asserts term -> docids through the logical index (index_id) of rowset.
    static void assert_snii_term(const RowsetSharedPtr& rowset, int64_t tablet_id,
                                 int32_t column_unique_id, int64_t index_id,
                                 const std::string& term,
                                 const std::vector<uint32_t>& expected_docids) {
        auto reader = open_snii_reader(rowset, tablet_id);
        const auto index_metas = rowset->tablet_schema()->inverted_indexs(column_unique_id);
        const TabletIndex* index_meta = nullptr;
        for (const auto* candidate : index_metas) {
            if (candidate->index_id() == index_id) {
                index_meta = candidate;
            }
        }
        ASSERT_NE(index_meta, nullptr) << "index " << index_id << " missing from output schema";
        auto logical_index = reader->open_snii_index(index_meta);
        ASSERT_TRUE(logical_index.has_value()) << logical_index.error();
        std::vector<uint32_t> docids;
        ASSERT_TRUE(snii::query::term_query(*logical_index.value(), term, &docids).ok());
        EXPECT_EQ(docids, expected_docids) << "term=" << term << " index_id=" << index_id;
    }

    // Asserts the output container carries the source's valid physical prefix
    // byte for byte (which also pins that the prefix was copied exactly once:
    // a second copy would displace every inherited section reference).
    static void assert_snii_inherited_prefix(
            const RowsetSharedPtr& source_rowset, const RowsetSharedPtr& output_rowset,
            int64_t tablet_id, const std::vector<snii::reader::LogicalIndexKey>& inherit_keys,
            uint32_t doc_count) {
        auto source_reader = open_snii_reader(source_rowset, tablet_id);
        snii::reader::SniiRewriteSnapshot snapshot;
        ASSERT_TRUE(source_reader->prepare_snii_rewrite_snapshot(inherit_keys, doc_count, &snapshot)
                            .ok());
        ASSERT_GT(snapshot.physical_prefix_end(), 0U);

        const auto read_all = [](const std::string& path) {
            io::FileReaderSPtr file_reader;
            EXPECT_TRUE(io::global_local_filesystem()->open_file(path, &file_reader).ok());
            std::string content(file_reader->size(), '\0');
            size_t bytes_read = 0;
            Slice slice(content);
            EXPECT_TRUE(file_reader->read_at(0, slice, &bytes_read).ok());
            EXPECT_EQ(bytes_read, content.size());
            return content;
        };
        const std::string source_bytes = read_all(snii_index_path_of(source_rowset));
        const std::string output_bytes = read_all(snii_index_path_of(output_rowset));
        ASSERT_GE(source_bytes.size(), snapshot.physical_prefix_end());
        ASSERT_GE(output_bytes.size(), snapshot.physical_prefix_end());
        EXPECT_EQ(source_bytes.substr(0, snapshot.physical_prefix_end()),
                  output_bytes.substr(0, snapshot.physical_prefix_end()))
                << "inherited physical prefix must be byte-identical";
    }

    static void assert_snii_surviving_index(const RowsetSharedPtr& source_rowset,
                                            const RowsetSharedPtr& output_rowset) {
        const auto& output_schema = output_rowset->tablet_schema();
        EXPECT_FALSE(output_schema->has_inverted_index_with_index_id(1));
        ASSERT_TRUE(output_schema->has_inverted_index_with_index_id(2));
        EXPECT_EQ(output_rowset->index_disk_size(), source_rowset->index_disk_size());
        EXPECT_EQ(output_rowset->data_disk_size(), source_rowset->data_disk_size());
        EXPECT_EQ(output_rowset->total_disk_size(), source_rowset->total_disk_size());

        auto source_segment_path = source_rowset->segment_path(0);
        ASSERT_TRUE(source_segment_path.has_value()) << source_segment_path.error();
        auto output_segment_path = output_rowset->segment_path(0);
        ASSERT_TRUE(output_segment_path.has_value()) << output_segment_path.error();
        const auto source_index_path = segment_v2::InvertedIndexDescriptor::get_index_file_path_v2(
                segment_v2::InvertedIndexDescriptor::get_index_file_path_prefix(
                        source_segment_path.value()));
        const auto output_index_path = segment_v2::InvertedIndexDescriptor::get_index_file_path_v2(
                segment_v2::InvertedIndexDescriptor::get_index_file_path_prefix(
                        output_segment_path.value()));
        std::error_code equivalent_error;
        EXPECT_TRUE(
                std::filesystem::equivalent(source_index_path, output_index_path, equivalent_error))
                << equivalent_error.message();
    }

    static void assert_snii_term_query(const RowsetSharedPtr& rowset, int64_t tablet_id) {
        auto segment_path = rowset->segment_path(0);
        ASSERT_TRUE(segment_path.has_value()) << segment_path.error();
        const std::string index_path_prefix {
                segment_v2::InvertedIndexDescriptor::get_index_file_path_prefix(
                        segment_path.value())};
        segment_v2::IndexFileReader index_file_reader(
                io::global_local_filesystem(), index_path_prefix,
                InvertedIndexStorageFormatPB::SNII, InvertedIndexFileInfo(), tablet_id);
        ASSERT_TRUE(index_file_reader.init().ok());

        const auto& surviving_indexes = rowset->tablet_schema()->inverted_indexs(3);
        ASSERT_EQ(surviving_indexes.size(), 1);
        auto logical_index = index_file_reader.open_snii_index(surviving_indexes.front());
        ASSERT_TRUE(logical_index.has_value()) << logical_index.error();
        std::vector<uint32_t> docids;
        ASSERT_TRUE(snii::query::term_query(*logical_index.value(), "keep", &docids).ok());
        EXPECT_EQ(docids, (std::vector<uint32_t> {0, 1}));
    }

    static void assert_last_snii_index_dropped(const RowsetSharedPtr& source_rowset,
                                               const RowsetSharedPtr& rowset) {
        EXPECT_FALSE(rowset->tablet_schema()->has_inverted_index());
        EXPECT_EQ(rowset->index_disk_size(), 0);
        EXPECT_EQ(rowset->data_disk_size(), source_rowset->data_disk_size());
        EXPECT_EQ(rowset->total_disk_size(), source_rowset->data_disk_size());
        auto segment_path = rowset->segment_path(0);
        ASSERT_TRUE(segment_path.has_value()) << segment_path.error();
        const auto index_path = segment_v2::InvertedIndexDescriptor::get_index_file_path_v2(
                segment_v2::InvertedIndexDescriptor::get_index_file_path_prefix(
                        segment_path.value()));
        bool index_exists = true;
        ASSERT_TRUE(io::global_local_filesystem()->exists(index_path, &index_exists).ok());
        EXPECT_FALSE(index_exists);
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

TEST_F(IndexBuilderTest, HandleSingleRowsetPreservesOrdinaryAppendFailure) {
    prepare_single_index_build(16604);
    ScopedIndexBuilderDebugPoints debug_points;
    debug_points.enable("IndexBuilder::handle_single_rowset_write_inverted_index_data_error");

    auto status = build_single_index();

    EXPECT_TRUE(status.is<ErrorCode::INTERNAL_ERROR>()) << status;
    EXPECT_EQ(status.msg(), "debug point: handle_single_rowset_write_inverted_index_data_error");
}

TEST_F(IndexBuilderTest, DropInvertedIndexTest) {
    // 0. prepare tablet path
    auto tablet_path = _absolute_dir + "/" + std::to_string(15676);
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
    writer_context.tablet_path = tablet_path;
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
        Block block = _tablet_schema->create_block();
        auto columns = std::move(block).mutate_columns();

        // Add data for k1 and k2 columns
        for (int i = 0; i < num_rows; ++i) {
            // k1 column (int)
            int32_t k1 = i * 10;
            columns[0]->insert_data((const char*)&k1, sizeof(k1));

            // k2 column (int)
            int32_t k2 = i % 100;
            columns[1]->insert_data((const char*)&k2, sizeof(k2));
        }

        block.set_columns(std::move(columns));

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
    drop_index.index_type = TIndexType::INVERTED;
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
    bool exists = false;
    EXPECT_TRUE(io::global_local_filesystem()->exists(tablet_path, &exists).ok());
    EXPECT_TRUE(exists);

    // Check files in old and new directories
    std::vector<io::FileInfo> files;
    bool dir_exists = false;
    EXPECT_TRUE(io::global_local_filesystem()->list(tablet_path, true, &files, &dir_exists).ok());
    EXPECT_TRUE(dir_exists);
    int new_idx_file_count = 0;
    int new_dat_file_count = 0;
    int old_idx_file_count = 0;
    int old_dat_file_count = 0;
    for (const auto& file : files) {
        std::string filename = file.file_name;
        if (filename.find("15676_0.idx") != std::string::npos) {
            old_idx_file_count++;
        }
        if (filename.find("15676_0.dat") != std::string::npos) {
            old_dat_file_count++;
        }
        if (filename.find("020000000000000100000000000000000000000000000000_0.idx") !=
            std::string::npos) {
            new_idx_file_count++;
        }
        if (filename.find("020000000000000100000000000000000000000000000000_0.dat") !=
            std::string::npos) {
            new_dat_file_count++;
        }
    }
    // The index should have been removed
    EXPECT_EQ(old_idx_file_count, 1) << "Tablet path should have 1 .idx file before drop";
    EXPECT_EQ(old_dat_file_count, 1) << "Tablet path should have 1 .dat file before drop";
    EXPECT_EQ(new_idx_file_count, 0) << "Tablet path should have no .idx file after drop";
    EXPECT_EQ(new_dat_file_count, 1) << "Tablet path should have 1 .dat file after drop";

    //auto tablet_schema = _tablet->tablet_schema();
    //EXPECT_FALSE(tablet_schema->has_inverted_index_with_index_id(1));
}

TEST_F(IndexBuilderTest, DropAnnIndexTest) {
    // prepare tablet path
    auto tablet_path = _absolute_dir + "/" + std::to_string(15676);
    _tablet->_tablet_path = tablet_path;
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(tablet_path).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet_path).ok());

    RowsetSharedPtr rowset;

    // Create test ann index properties
    std::map<std::string, std::string> properties;
    properties["index_type"] = "hnsw";
    properties["metric_type"] = "l2_distance";
    properties["dim"] = "4";
    properties["max_degree"] = "16";

    // First add an initial index to the schema (for arr1 column)
    TabletIndex initial_index;
    initial_index._index_id = 1;
    initial_index._index_name = "arr1_index";
    initial_index._index_type = IndexType::ANN;
    initial_index._col_unique_ids.push_back(1); // unique_id for arr1
    initial_index._properties = properties;

    _tablet_schema = create_ann_tablet_schema();
    _tablet_schema->append_index(std::move(initial_index));

    // 3. Create a rowset writer context
    RowsetWriterContext writer_context;
    writer_context.rowset_id.init(15676);
    writer_context.tablet_id = 15676;
    writer_context.tablet_schema_hash = 567997577;
    writer_context.partition_id = 10;
    writer_context.rowset_type = BETA_ROWSET;
    writer_context.tablet_path = tablet_path;
    writer_context.rowset_state = VISIBLE;
    writer_context.tablet_schema = _tablet_schema;
    writer_context.version.first = 10;
    writer_context.version.second = 10;

    ASSERT_TRUE(io::global_local_filesystem()->create_directory(writer_context.tablet_path).ok());

    // Create a rowset writer
    auto res = RowsetFactory::create_rowset_writer(*_engine_ref, writer_context, false);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto rowset_writer = std::move(res).value();

    // Write data to the rowset
    {
        DataTypePtr inner_float = std::make_shared<DataTypeFloat32>();
        DataTypePtr array_type = std::make_shared<DataTypeArray>(inner_float);

        // create a MutableColumnPtr
        MutableColumnPtr col = array_type->create_column();
        // row0
        {
            Array arr;
            arr.push_back(Field::create_field<TYPE_FLOAT>(1.0F));
            arr.push_back(Field::create_field<TYPE_FLOAT>(2.0F));
            arr.push_back(Field::create_field<TYPE_FLOAT>(3.0F));
            arr.push_back(Field::create_field<TYPE_FLOAT>(4.0F));
            col->insert(Field::create_field<TYPE_ARRAY>(arr));
        }
        // row1
        {
            Array arr;
            arr.push_back(Field::create_field<TYPE_FLOAT>(5.0F));
            arr.push_back(Field::create_field<TYPE_FLOAT>(6.0F));
            arr.push_back(Field::create_field<TYPE_FLOAT>(7.0F));
            arr.push_back(Field::create_field<TYPE_FLOAT>(8.0F));
            col->insert(Field::create_field<TYPE_ARRAY>(arr));
        }
        // wrap the constructed column into a ColumnWithTypeAndName
        ColumnPtr column_array = std::move(col);
        ColumnWithTypeAndName type_and_name(column_array, array_type, "arr1");

        // construct Block (containing only this column), with 2 rows
        Block block;
        block.insert(type_and_name);

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

    // Verify index exists before dropping
    EXPECT_TRUE(_tablet_schema->has_ann_index());
    EXPECT_TRUE(_tablet_schema->has_inverted_index_with_index_id(1));

    // Prepare index for dropping
    TOlapTableIndex drop_index;
    drop_index.index_type = TIndexType::type::ANN;
    drop_index.index_id = 1;
    drop_index.index_name = "arr1_index";
    drop_index.columns.emplace_back("arr1");
    _alter_indexes.clear();
    _alter_indexes.push_back(drop_index);

    // Create IndexBuilder with drop operation
    IndexBuilder builder(ExecEnv::GetInstance()->storage_engine().to_local(), _tablet, _columns,
                         _alter_indexes, true);

    // Initialize and verify
    auto status = builder.init();
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(builder._alter_index_ids.size(), 1);

    // Execute drop operation
    status = builder.do_build_inverted_index();
    EXPECT_TRUE(status.ok()) << status.to_string();

    // Verify the index has been removed
    // check old tablet path and new tablet path
    bool exists = false;
    EXPECT_TRUE(io::global_local_filesystem()->exists(tablet_path, &exists).ok());
    EXPECT_TRUE(exists);

    // Check files in old and new directories
    std::vector<io::FileInfo> files;
    bool dir_exists = false;
    EXPECT_TRUE(io::global_local_filesystem()->list(tablet_path, true, &files, &dir_exists).ok());
    EXPECT_TRUE(dir_exists);
    int new_idx_file_count = 0;
    int new_dat_file_count = 0;
    int old_idx_file_count = 0;
    int old_dat_file_count = 0;
    for (const auto& file : files) {
        std::string filename = file.file_name;
        if (filename.find("15676_0.idx") != std::string::npos) {
            old_idx_file_count++;
        }
        if (filename.find("15676_0.dat") != std::string::npos) {
            old_dat_file_count++;
        }
        if (filename.find("020000000000000100000000000000000000000000000000_0.idx") !=
            std::string::npos) {
            new_idx_file_count++;
        }
        if (filename.find("020000000000000100000000000000000000000000000000_0.dat") !=
            std::string::npos) {
            new_dat_file_count++;
        }
    }
    // The index should have been removed
    EXPECT_EQ(old_idx_file_count, 1) << "Tablet path should have 1 .idx file before drop";
    EXPECT_EQ(old_dat_file_count, 1) << "Tablet path should have 1 .dat file before drop";
    EXPECT_EQ(new_idx_file_count, 0) << "Tablet path should have no .idx file after drop";
    EXPECT_EQ(new_dat_file_count, 1) << "Tablet path should have 1 .dat file after drop";
}

TEST_F(IndexBuilderTest, BuildInvertedIndexAfterWritingDataTest) {
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
        Block block = _tablet_schema->create_block();
        auto columns = std::move(block).mutate_columns();

        // Add data for k1 and k2 columns according to the schema
        for (int i = 0; i < num_rows; ++i) {
            // k1 column (int)
            int32_t k1 = i * 10;
            columns[0]->insert_data((const char*)&k1, sizeof(k1));

            // k2 column (int)
            int32_t k2 = i % 100;
            columns[1]->insert_data((const char*)&k2, sizeof(k2));
        }

        block.set_columns(std::move(columns));

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

TEST_F(IndexBuilderTest, BuildAnnIndexAfterWritingDataTest) {
    // 0. prepare tablet path
    auto tablet_path = _absolute_dir + "/" + std::to_string(14686);
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(tablet_path).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet_path).ok());

    // 1. Prepare data for writing
    RowsetSharedPtr rowset;
    const int num_rows = 100;

    // 2. Use ANN schema with array<float> column
    auto ann_schema = create_ann_tablet_schema();

    // 3. Update schema in tablet meta
    TabletMetaPB tablet_meta_pb;
    _tablet_meta->to_meta_pb(&tablet_meta_pb, false);

    TabletSchemaPB ann_schema_pb;
    ann_schema->to_schema_pb(&ann_schema_pb);
    tablet_meta_pb.mutable_schema()->CopyFrom(ann_schema_pb);

    _tablet_meta->init_from_pb(tablet_meta_pb);

    // 4. Reinitialize tablet to use new schema
    _tablet = std::make_shared<Tablet>(*_engine_ref, _tablet_meta, _data_dir.get());
    _tablet->_tablet_path = tablet_path;
    ASSERT_TRUE(_tablet->init().ok());

    _tablet_schema = ann_schema;

    // 3. Create a rowset writer context
    RowsetWriterContext writer_context;
    writer_context.rowset_id.init(15686);
    writer_context.tablet_id = 15686;
    writer_context.tablet_schema_hash = 567997577;
    writer_context.partition_id = 10;
    writer_context.rowset_type = BETA_ROWSET;
    writer_context.tablet_path = _absolute_dir + "/" + std::to_string(15686);
    writer_context.rowset_state = VISIBLE;
    writer_context.tablet_schema = _tablet_schema;
    writer_context.version.first = 10;
    writer_context.version.second = 10;

    ASSERT_TRUE(io::global_local_filesystem()->create_directory(writer_context.tablet_path).ok());

    // 4. Create a rowset writer
    auto res = RowsetFactory::create_rowset_writer(*_engine_ref, writer_context, false);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto rowset_writer = std::move(res).value();

    // 5. Write data to the rowset with float arrays
    {
        DataTypePtr inner_float = std::make_shared<DataTypeFloat32>();
        DataTypePtr array_type = std::make_shared<DataTypeArray>(inner_float);

        // create a MutableColumnPtr
        MutableColumnPtr col = array_type->create_column();

        // Add data for each row - arrays of 4 floats (matching dim=4 in properties)
        for (int i = 0; i < num_rows; ++i) {
            Array arr;
            // Create 4-dimensional float vectors
            arr.push_back(Field::create_field<TYPE_FLOAT>(static_cast<float>(i % 10)));
            arr.push_back(Field::create_field<TYPE_FLOAT>(static_cast<float>((i + 1) % 10)));
            arr.push_back(Field::create_field<TYPE_FLOAT>(static_cast<float>((i + 2) % 10)));
            arr.push_back(Field::create_field<TYPE_FLOAT>(static_cast<float>((i + 3) % 10)));
            col->insert(Field::create_field<TYPE_ARRAY>(arr));
        }

        // wrap the constructed column into a ColumnWithTypeAndName
        ColumnPtr column_array = std::move(col);
        ColumnWithTypeAndName type_and_name(column_array, array_type, "arr1");

        // construct Block (containing only this column), with num_rows rows
        Block block;
        block.insert(type_and_name);

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

    // 6. Prepare ANN index for building
    std::map<std::string, std::string> properties;
    properties["index_type"] = "hnsw";
    properties["metric_type"] = "l2_distance";
    properties["dim"] = "4";
    properties["max_degree"] = "16";

    TOlapTableIndex ann_index;
    ann_index.__set_index_id(1);
    ann_index.__set_columns({"arr1"});
    ann_index.__set_index_name("arr1_ann_index");
    ann_index.__set_index_type(TIndexType::ANN);
    // NOTE: wrong way, it doesn't set __isset.properties flag
    // ann_index.properties = properties;
    ann_index.__set_properties(properties);
    _alter_indexes.push_back(ann_index);

    // 7. Create IndexBuilder
    IndexBuilder builder(ExecEnv::GetInstance()->storage_engine().to_local(), _tablet, _columns,
                         _alter_indexes, false);

    // 8. Initialize and verify
    auto status = builder.init();
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(builder._alter_index_ids.size(), 1);

    // 9. Build ANN index
    status = builder.do_build_inverted_index();
    EXPECT_TRUE(status.ok()) << status.to_string();

    // 10. Check paths and files
    auto old_tablet_path = _absolute_dir + "/" + std::to_string(15686);
    auto new_tablet_path = _absolute_dir + "/" + std::to_string(14686);
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
        Block block = _tablet_schema->create_block();
        auto columns = std::move(block).mutate_columns();

        // Add data for k1 and k2 columns
        for (int i = 0; i < num_rows; ++i) {
            // k1 column (int)
            int32_t k1 = i * 10;
            columns[0]->insert_data((const char*)&k1, sizeof(k1));

            // k2 column (int)
            int32_t k2 = i % 100;
            columns[1]->insert_data((const char*)&k2, sizeof(k2));
        }

        block.set_columns(std::move(columns));

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
    _tablet_meta->to_meta_pb(&tablet_meta_pb, false);

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
        Block block = v1_schema->create_block();
        auto columns = std::move(block).mutate_columns();

        // Add data for k1 and k2 columns
        for (int i = 0; i < num_rows; ++i) {
            // k1 column (int)
            int32_t k1 = i * 10;
            columns[0]->insert_data((const char*)&k1, sizeof(k1));

            // k2 column (int)
            int32_t k2 = i % 100;
            columns[1]->insert_data((const char*)&k2, sizeof(k2));
        }

        block.set_columns(std::move(columns));

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
        Block block = _tablet_schema->create_block();
        auto columns = std::move(block).mutate_columns();

        // Add data for k1 and k2 columns
        for (int i = 0; i < rows_per_segment; ++i) {
            // k1 column (int) - make values different across segments
            int32_t k1 = (segment * rows_per_segment + i) * 10;
            columns[0]->insert_data((const char*)&k1, sizeof(k1));

            // k2 column (int)
            int32_t k2 = (segment * rows_per_segment + i) % 100;
            columns[1]->insert_data((const char*)&k2, sizeof(k2));
        }

        block.set_columns(std::move(columns));

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
        Block block = _tablet_schema->create_block();
        auto columns = std::move(block).mutate_columns();

        // Add data for k1 and k2 columns
        for (int i = 0; i < num_rows; ++i) {
            // k1 column (int)
            int32_t k1 = i * 10;
            columns[0]->insert_data((const char*)&k1, sizeof(k1));

            // k2 column (int)
            int32_t k2 = i % 100;
            columns[1]->insert_data((const char*)&k2, sizeof(k2));
        }

        block.set_columns(std::move(columns));

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

TEST_F(IndexBuilderTest, RenameColumnIndexTest) {
    // 0. prepare tablet path
    auto tablet_path = _absolute_dir + "/" + std::to_string(14679);
    _tablet->_tablet_path = tablet_path;
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(tablet_path).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet_path).ok());
    auto schema = std::make_shared<TabletSchema>();

    schema->_keys_type = KeysType::UNIQUE_KEYS;
    schema->_inverted_index_storage_format = InvertedIndexStorageFormatPB::V2;

    // Create the first key column
    TabletColumn column_1;
    column_1.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    column_1.set_unique_id(1);
    column_1.set_name("k1");
    column_1.set_is_key(true);
    column_1.set_index_length(4);
    schema->append_column(column_1);

    // Create the second key column
    TabletColumn column_2;
    column_2.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    // not sequential unique_id
    column_2.set_unique_id(3);
    column_2.set_name("k2");
    column_2.set_is_key(false);
    schema->append_column(column_2);

    // 1. Prepare data for writing
    RowsetSharedPtr rowset;
    const int num_rows = 1000;

    // 2. First add an initial index to the schema (for k1 column)
    TabletIndex initial_index;
    initial_index._index_id = 1;
    initial_index._index_name = "k1_index";
    initial_index._index_type = IndexType::INVERTED;
    initial_index._col_unique_ids.push_back(1); // unique_id for k1
    schema->append_index(std::move(initial_index));

    // 3. Create a rowset writer context
    RowsetWriterContext writer_context;
    writer_context.rowset_id.init(15679);
    writer_context.tablet_id = 15679;
    writer_context.tablet_schema_hash = 567997577;
    writer_context.partition_id = 10;
    writer_context.rowset_type = BETA_ROWSET;
    writer_context.tablet_path = _absolute_dir + "/" + std::to_string(15679);
    writer_context.rowset_state = VISIBLE;
    writer_context.tablet_schema = schema;
    writer_context.version.first = 10;
    writer_context.version.second = 10;

    ASSERT_TRUE(io::global_local_filesystem()->create_directory(writer_context.tablet_path).ok());

    // 4. Create a rowset writer
    auto res = RowsetFactory::create_rowset_writer(*_engine_ref, writer_context, false);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto rowset_writer = std::move(res).value();

    // 5. Write data to the rowset
    {
        Block block = _tablet_schema->create_block();
        auto columns = std::move(block).mutate_columns();

        // Add data for k1 and k2 columns
        for (int i = 0; i < num_rows; ++i) {
            // k1 column (int)
            int32_t k1 = i * 10;
            columns[0]->insert_data((const char*)&k1, sizeof(k1));

            // k2 column (int)
            int32_t k2 = i % 100;
            columns[1]->insert_data((const char*)&k2, sizeof(k2));
        }

        block.set_columns(std::move(columns));

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

    // Index for rename column "k2" to "k3"
    TOlapTableIndex index2;
    index2.index_id = 3;
    index2.columns.emplace_back("k3"); // This column doesn't exist in the schema
    index2.index_name = "k3_index";
    index2.index_type = TIndexType::INVERTED;
    index2.column_unique_ids.push_back(3);
    index2.__isset.column_unique_ids = true;
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
        Block block = _tablet_schema->create_block();
        auto columns = std::move(block).mutate_columns();

        // Add data for k1 and k2 columns
        for (int i = 0; i < num_rows; ++i) {
            // k1 column (int)
            int32_t k1 = i * 10;
            columns[0]->insert_data((const char*)&k1, sizeof(k1));

            // k2 column (int)
            int32_t k2 = i % 100;
            columns[1]->insert_data((const char*)&k2, sizeof(k2));
        }

        block.set_columns(std::move(columns));

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
    _tablet_meta->to_meta_pb(&tablet_meta_pb, false);

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
        Block block = v1_schema->create_block();
        auto columns = std::move(block).mutate_columns();

        // Add data for k1 and k2 columns
        for (int i = 0; i < num_rows; ++i) {
            // k1 column (int)
            int32_t k1 = i * 10;
            columns[0]->insert_data((const char*)&k1, sizeof(k1));

            // k2 column (int)
            int32_t k2 = i % 100;
            columns[1]->insert_data((const char*)&k2, sizeof(k2));
        }

        block.set_columns(std::move(columns));

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

TEST_F(IndexBuilderTest, NonNullIndexDataTest) {
    // 0. prepare tablet path
    auto tablet_path = _absolute_dir + "/" + std::to_string(14681);
    _tablet->_tablet_path = tablet_path;
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(tablet_path).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet_path).ok());

    // 1. Prepare data for writing
    RowsetSharedPtr rowset;
    const int num_rows = 1000;

    // 2. Create a rowset writer context
    RowsetWriterContext writer_context;
    writer_context.rowset_id.init(15681);
    writer_context.tablet_id = 15681;
    writer_context.tablet_schema_hash = 567997577;
    writer_context.partition_id = 10;
    writer_context.rowset_type = BETA_ROWSET;
    writer_context.tablet_path = _absolute_dir + "/" + std::to_string(15681);
    writer_context.rowset_state = VISIBLE;
    writer_context.tablet_schema = _tablet_schema;
    writer_context.version.first = 10;
    writer_context.version.second = 10;

    ASSERT_TRUE(io::global_local_filesystem()->create_directory(writer_context.tablet_path).ok());

    // 3. Create a rowset writer with non-null values
    auto res = RowsetFactory::create_rowset_writer(*_engine_ref, writer_context, false);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto rowset_writer = std::move(res).value();

    // 4. Write non-null data to the rowset
    {
        Block block = _tablet_schema->create_block();
        auto columns = std::move(block).mutate_columns();

        // Add data for k1 and k2 columns with no null values
        for (int i = 0; i < num_rows; ++i) {
            // k1 column (int)
            int32_t k1 = i * 10;
            columns[0]->insert_data((const char*)&k1, sizeof(k1));

            // k2 column (int)
            int32_t k2 = i % 100;
            columns[1]->insert_data((const char*)&k2, sizeof(k2));
        }

        block.set_columns(std::move(columns));

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

    // 5. Prepare indexes for building - only use non-nullable fields
    TOlapTableIndex index1;
    index1.index_id = 1;
    index1.columns.emplace_back("k1");
    index1.index_name = "k1_index";
    index1.index_type = TIndexType::INVERTED;
    _alter_indexes.push_back(index1);

    // 6. Force columns to be treated as non-null by modifying schema
    TabletSchemaSPtr non_null_schema = std::make_shared<TabletSchema>();
    create_tablet_schema(non_null_schema, KeysType::DUP_KEYS);
    // Set the second column to be non-nullable explicitly
    TabletColumn& k2_column = non_null_schema->mutable_column(1);
    k2_column.set_is_nullable(false);

    // 7. Create IndexBuilder with the modified schema
    IndexBuilder builder(ExecEnv::GetInstance()->storage_engine().to_local(), _tablet, _columns,
                         _alter_indexes, false);

    // 8. Initialize and verify
    auto status = builder.init();
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(builder._alter_index_ids.size(), 1);

    // 9. Build index - should trigger _add_data rather than _add_nullable
    status = builder.do_build_inverted_index();
    EXPECT_TRUE(status.ok()) << status.to_string();

    // 10. Verify results
    auto old_tablet_path = _absolute_dir + "/" + std::to_string(15681);
    auto new_tablet_path = _absolute_dir + "/" + std::to_string(14681);
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

    std::vector<io::FileInfo> new_files;
    bool new_dir_exists = false;
    EXPECT_TRUE(io::global_local_filesystem()
                        ->list(new_tablet_path, true, &new_files, &new_dir_exists)
                        .ok());
    EXPECT_TRUE(new_dir_exists);
    int new_idx_file_count = 0;
    for (const auto& file : new_files) {
        std::string filename = file.file_name;
        if (filename.find(".idx") != std::string::npos) {
            new_idx_file_count++;
        }
    }
    EXPECT_EQ(new_idx_file_count, 1) << "Should have created 1 index file";
}

TEST_F(IndexBuilderTest, NonExistentColumnUniqueIdTest) {
    // 0. prepare tablet path
    auto tablet_path = _absolute_dir + "/" + std::to_string(14682);
    _tablet->_tablet_path = tablet_path;
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(tablet_path).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet_path).ok());

    // 1. Prepare data for writing
    RowsetSharedPtr rowset;
    const int num_rows = 1000;

    // 2. Create a rowset writer context
    RowsetWriterContext writer_context;
    writer_context.rowset_id.init(15682);
    writer_context.tablet_id = 15682;
    writer_context.tablet_schema_hash = 567997577;
    writer_context.partition_id = 10;
    writer_context.rowset_type = BETA_ROWSET;
    writer_context.tablet_path = _absolute_dir + "/" + std::to_string(15682);
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
        Block block = _tablet_schema->create_block();
        auto columns = std::move(block).mutate_columns();

        // Add data for k1 and k2 columns
        for (int i = 0; i < num_rows; ++i) {
            // k1 column (int)
            int32_t k1 = i * 10;
            columns[0]->insert_data((const char*)&k1, sizeof(k1));

            // k2 column (int)
            int32_t k2 = i % 100;
            columns[1]->insert_data((const char*)&k2, sizeof(k2));
        }

        block.set_columns(std::move(columns));

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

    // 5. First add an initial index to the schema (for k1 column)
    TabletIndex initial_index;
    initial_index._index_id = 1;
    initial_index._index_name = "k1_index";
    initial_index._index_type = IndexType::INVERTED;
    initial_index._col_unique_ids.push_back(1); // unique_id for k1
    _tablet_schema->append_index(std::move(initial_index));

    // 6. Prepare indexes for building - specifying column by unique_id that doesn't exist
    _alter_indexes.clear();

    // Use drop operation to test column_unique_ids path
    TOlapTableIndex drop_index;
    drop_index.index_id = 1;
    drop_index.columns.emplace_back("non_existent_column");
    drop_index.column_unique_ids.push_back(999); // This unique ID doesn't exist
    _alter_indexes.push_back(drop_index);

    // 7. Create IndexBuilder with drop operation
    IndexBuilder builder(ExecEnv::GetInstance()->storage_engine().to_local(), _tablet, _columns,
                         _alter_indexes, true);

    // 8. Initialize and verify
    auto status = builder.init();
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(builder._alter_index_ids.size(), 1);

    // 9. Execute drop operation - should handle non-existent column gracefully
    status = builder.do_build_inverted_index();
    EXPECT_TRUE(status.ok()) << status.to_string();

    // 10. Verify paths exists - operations should complete without errors
    auto old_tablet_path = _absolute_dir + "/" + std::to_string(15682);
    auto new_tablet_path = _absolute_dir + "/" + std::to_string(14682);
    bool old_exists = false;
    bool new_exists = false;
    EXPECT_TRUE(io::global_local_filesystem()->exists(old_tablet_path, &old_exists).ok());
    EXPECT_TRUE(old_exists);
    EXPECT_TRUE(io::global_local_filesystem()->exists(new_tablet_path, &new_exists).ok());
    EXPECT_TRUE(new_exists);
}

TEST_F(IndexBuilderTest, DropIndexV1FormatTest) {
    // 1. Create new schema using V1 format
    auto v1_schema = std::make_shared<TabletSchema>();
    create_tablet_schema(v1_schema, KeysType::DUP_KEYS);

    // 2. Modify to V1 format
    v1_schema->_inverted_index_storage_format = InvertedIndexStorageFormatPB::V1;

    // 3. Add an initial index to the schema (for k1 column)
    TabletIndex initial_index;
    initial_index._index_id = 1;
    initial_index._index_name = "k1_index";
    initial_index._index_type = IndexType::INVERTED;
    initial_index._col_unique_ids.push_back(1); // unique_id for k1
    v1_schema->append_index(std::move(initial_index));

    // 4. Update schema in tablet
    TabletMetaPB tablet_meta_pb;
    _tablet_meta->to_meta_pb(&tablet_meta_pb, false);

    TabletSchemaPB v1_schema_pb;
    v1_schema->to_schema_pb(&v1_schema_pb);
    tablet_meta_pb.mutable_schema()->CopyFrom(v1_schema_pb);

    _tablet_meta->init_from_pb(tablet_meta_pb);

    // 5. Reinitialize tablet to use new schema
    _tablet = std::make_shared<Tablet>(*_engine_ref, _tablet_meta, _data_dir.get());
    ASSERT_TRUE(_tablet->init().ok());
    auto tablet_path = _absolute_dir + "/" + std::to_string(15683);
    _tablet->_tablet_path = tablet_path;
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(tablet_path).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet_path).ok());

    // 6. Prepare data for writing
    RowsetSharedPtr rowset;
    const int num_rows = 1000;

    // 7. Create a rowset writer context
    RowsetWriterContext writer_context;
    writer_context.rowset_id.init(15683);
    writer_context.tablet_id = 15683;
    writer_context.tablet_schema_hash = 567997577;
    writer_context.partition_id = 10;
    writer_context.rowset_type = BETA_ROWSET;
    writer_context.tablet_path = tablet_path;
    writer_context.rowset_state = VISIBLE;
    writer_context.tablet_schema = v1_schema;
    writer_context.version.first = 10;
    writer_context.version.second = 10;

    ASSERT_TRUE(io::global_local_filesystem()->create_directory(writer_context.tablet_path).ok());

    // 8. Create a rowset writer
    auto res = RowsetFactory::create_rowset_writer(*_engine_ref, writer_context, false);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto rowset_writer = std::move(res).value();

    // 9. Write data to the rowset
    {
        Block block = v1_schema->create_block();
        auto columns = std::move(block).mutate_columns();

        // Add data for k1 and k2 columns
        for (int i = 0; i < num_rows; ++i) {
            // k1 column (int)
            int32_t k1 = i * 10;
            columns[0]->insert_data((const char*)&k1, sizeof(k1));

            // k2 column (int)
            int32_t k2 = i % 100;
            columns[1]->insert_data((const char*)&k2, sizeof(k2));
        }

        block.set_columns(std::move(columns));

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

    // 10. Prepare to drop the k1 index
    _alter_indexes.clear();
    TOlapTableIndex drop_index;
    drop_index.index_id = 1;
    drop_index.columns.emplace_back("k1");
    drop_index.index_name = "k1_index";
    drop_index.index_type = TIndexType::INVERTED;
    _alter_indexes.push_back(drop_index);

    // 11. Create IndexBuilder with drop operation
    IndexBuilder builder(ExecEnv::GetInstance()->storage_engine().to_local(), _tablet, _columns,
                         _alter_indexes, true);

    // 12. Initialize and verify
    auto status = builder.init();
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(builder._alter_index_ids.size(), 1);

    // 13. Execute drop operation
    status = builder.do_build_inverted_index();
    EXPECT_TRUE(status.ok()) << status.to_string();

    // 14. Verify paths exists
    bool exists = false;
    EXPECT_TRUE(io::global_local_filesystem()->exists(tablet_path, &exists).ok());
    EXPECT_TRUE(exists);

    // 15. Verify the index has been removed
    std::vector<io::FileInfo> files;
    bool dir_exists = false;
    EXPECT_TRUE(io::global_local_filesystem()->list(tablet_path, true, &files, &dir_exists).ok());
    EXPECT_TRUE(dir_exists);

    // Verify no index files in the new directory
    int new_idx_file_count = 0;
    int new_dat_file_count = 0;
    int old_idx_file_count = 0;
    int old_dat_file_count = 0;
    for (const auto& file : files) {
        std::string filename = file.file_name;
        if (filename.find("15683_0_1.idx") != std::string::npos) {
            old_idx_file_count++;
        }
        if (filename.find("15683_0.dat") != std::string::npos) {
            old_dat_file_count++;
        }
        if (filename.find("020000000000000100000000000000000000000000000000_0_1.idx") !=
            std::string::npos) {
            new_idx_file_count++;
        }
        if (filename.find("020000000000000100000000000000000000000000000000_0.dat") !=
            std::string::npos) {
            new_dat_file_count++;
        }
    }
    // The index should have been removed
    EXPECT_EQ(old_idx_file_count, 1) << "Tablet path should have 1 .idx file before drop";
    EXPECT_EQ(old_dat_file_count, 1) << "Tablet path should have 1 .dat file before drop";
    EXPECT_EQ(new_idx_file_count, 0) << "Tablet path should have no .idx file after drop";
    EXPECT_EQ(new_dat_file_count, 1) << "Tablet path should have 1 .dat file after drop";
}

TEST_F(IndexBuilderTest, ResourceCleanupTest) {
    // 0. prepare tablet path
    auto tablet_path = _absolute_dir + "/" + std::to_string(15684);
    _tablet->_tablet_path = tablet_path;
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(tablet_path).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet_path).ok());

    // 1. Prepare data for writing
    RowsetSharedPtr rowset;
    const int num_rows = 1000;

    // 2. Create a rowset writer context
    RowsetWriterContext writer_context;
    writer_context.rowset_id.init(15684);
    writer_context.tablet_id = 15684;
    writer_context.tablet_schema_hash = 567997577;
    writer_context.partition_id = 10;
    writer_context.rowset_type = BETA_ROWSET;
    writer_context.tablet_path = tablet_path;
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
        Block block = _tablet_schema->create_block();
        auto columns = std::move(block).mutate_columns();

        // Add data for k1 and k2 columns
        for (int i = 0; i < num_rows; ++i) {
            // k1 column (int)
            int32_t k1 = i * 10;
            columns[0]->insert_data((const char*)&k1, sizeof(k1));

            // k2 column (int)
            int32_t k2 = i % 100;
            columns[1]->insert_data((const char*)&k2, sizeof(k2));
        }

        block.set_columns(std::move(columns));

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

    // 5. Prepare indexes for building
    TOlapTableIndex index1;
    index1.index_id = 1;
    index1.columns.emplace_back("k1");
    index1.index_name = "k1_index";
    index1.index_type = TIndexType::INVERTED;
    _alter_indexes.push_back(index1);

    // Create a custom IndexBuilder with a spy function to test resource cleanup
    class TestIndexBuilder : public IndexBuilder {
    public:
        TestIndexBuilder(StorageEngine& engine, TabletSharedPtr tablet,
                         const std::vector<TColumn>& columns,
                         const std::vector<doris::TOlapTableIndex>& alter_inverted_indexes,
                         bool is_drop_op)
                : IndexBuilder(engine, tablet, columns, alter_inverted_indexes, is_drop_op) {}

        ~TestIndexBuilder() override = default;
        // Override update_inverted_index_info to inject failure
        Status update_inverted_index_info() override {
            RETURN_IF_ERROR(IndexBuilder::update_inverted_index_info());
            // Create a fake error to trigger cleanup
            return Status::Error<ErrorCode::INTERNAL_ERROR>("Simulated error for testing cleanup");
        }
    };

    // 6. Create our test builder
    TestIndexBuilder builder(ExecEnv::GetInstance()->storage_engine().to_local(), _tablet, _columns,
                             _alter_indexes, false);

    // 7. Initialize and verify
    auto status = builder.init();
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(builder._alter_index_ids.size(), 1);

    // 8. Build index - should fail with our simulated error
    status = builder.do_build_inverted_index();
    EXPECT_FALSE(status.ok()) << "Expected failure, but got success";
    EXPECT_TRUE(status.is<ErrorCode::INTERNAL_ERROR>()) << "Expected internal error";
    EXPECT_EQ(status.to_string(), "[INTERNAL_ERROR]Simulated error for testing cleanup")
            << "Error message doesn't match expected";

    // Verify the paths haven't been modified since the operation failed
    bool exists = false;
    EXPECT_TRUE(io::global_local_filesystem()->exists(tablet_path, &exists).ok());
    EXPECT_TRUE(exists);

    auto rowset_id = extract_rowset_id("020000000000000100000000000000000000000000000000_0.dat");
    EXPECT_TRUE(_engine_ref->check_rowset_id_in_unused_rowsets(rowset_id))
            << "Rowset id should be in unused rowsets";
}

TEST_F(IndexBuilderTest, ArrayTypeIndexTest) {
    // 1. Prepare tablet path
    auto tablet_path = _absolute_dir + "/" + std::to_string(14685);
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(tablet_path).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet_path).ok());

    // 2. Create tablet schema with array type
    auto tablet_schema = std::make_shared<TabletSchema>();
    TabletColumn column_1(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                          FieldType::OLAP_FIELD_TYPE_INT, true);
    column_1.set_unique_id(1);
    column_1.set_is_key(true);
    column_1.set_name("k1");
    column_1.set_index_length(4);
    tablet_schema->append_column(column_1);

    // Array type column
    TabletColumn column_2;
    column_2.set_unique_id(2);
    column_2.set_is_key(false);
    column_2.set_name("array_col");
    column_2.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
    column_2.set_is_nullable(false);
    // Add a primitive type for array items
    TabletColumn array_item_column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                                   FieldType::OLAP_FIELD_TYPE_VARCHAR, true);
    array_item_column.set_is_key(false);
    array_item_column.set_length(64);
    column_2.add_sub_column(array_item_column);
    tablet_schema->append_column(column_2);

    // 3. Create tablet
    auto tablet_meta = create_tablet_meta();
    auto tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    tablet->_tablet_path = tablet_path;
    ASSERT_TRUE(tablet->init().ok());

    // 4. Add inverted index for array column
    _columns.clear();
    TColumn tc1;
    tc1.column_name = "array_col";
    _columns.push_back(tc1);

    _alter_indexes.clear();
    TOlapTableIndex tt_index;
    tt_index.index_id = 1;
    tt_index.index_name = "array_index";
    tt_index.columns.emplace_back("array_col");
    tt_index.column_unique_ids.push_back(2);
    tt_index.index_type = TIndexType::type::INVERTED;
    _alter_indexes.push_back(tt_index);

    // 5. Create a rowset writer
    RowsetWriterContext writer_context;
    writer_context.rowset_id.init(14685);
    writer_context.tablet_id = 14685;
    writer_context.tablet_schema_hash = 567997577;
    writer_context.partition_id = 10;
    writer_context.rowset_type = BETA_ROWSET;
    writer_context.tablet_path = tablet_path;
    writer_context.rowset_state = VISIBLE;
    writer_context.tablet_schema = tablet_schema;
    writer_context.version.first = 10;
    writer_context.version.second = 10;

    ASSERT_TRUE(io::global_local_filesystem()->create_directory(writer_context.tablet_path).ok());

    // 6. Create rowset writer
    auto res = RowsetFactory::create_rowset_writer(*_engine_ref, writer_context, false);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto rowset_writer = std::move(res).value();

    // 7. Create data block and write data
    {
        Block block = tablet_schema->create_block();
        auto columns = std::move(block).mutate_columns();

        // Prepare columns for k1 and array_col
        for (int i = 0; i < 1000; i++) {
            // k1 column (int)
            int32_t k1 = i;
            columns[0]->insert_data((const char*)&k1, sizeof(k1));

            // array_col column
            // Create array data with 1-5 elements
            int array_size = i % 5 + 1;

            // For array type, we need to create a complex nested column structure
            auto& array_col = static_cast<ColumnArray&>(*columns[1]);
            Array arr;
            // Add string elements to the array
            for (int j = 0; j < array_size; j++) {
                std::string val = "item_" + std::to_string(i) + "_" + std::to_string(j);
                arr.push_back(Field::create_field<TYPE_STRING>(val));
            }
            array_col.insert(Field::create_field<TYPE_ARRAY>(arr));
        }

        block.set_columns(std::move(columns));

        // Add block to rowset
        Status s = rowset_writer->add_block(&block);
        ASSERT_TRUE(s.ok()) << s.to_string();

        // Flush writer
        s = rowset_writer->flush();
        ASSERT_TRUE(s.ok()) << s.to_string();
    }

    // 8. Build rowset
    RowsetSharedPtr rowset;
    ASSERT_TRUE(rowset_writer->build(rowset).ok());
    ASSERT_TRUE(rowset != nullptr);
    ASSERT_TRUE(tablet->add_rowset(rowset).ok());

    // 9. Initialize and build inverted index
    IndexBuilder builder(*_engine_ref, tablet, _columns, _alter_indexes, false);
    auto status = builder.init();
    EXPECT_TRUE(status.ok()) << status.to_string();

    status = builder.do_build_inverted_index();
    EXPECT_TRUE(status.ok()) << status.to_string();

    // 10. Verify that the index has been created
    std::string segment_path = local_segment_path(
            tablet->tablet_path(),
            extract_rowset_id("020000000000000100000000000000000000000000000000_0.dat").to_string(),
            0);

    if (tablet_schema->get_inverted_index_storage_format() == InvertedIndexStorageFormatPB::V1) {
        // V1 format
        auto index_path = InvertedIndexDescriptor::get_index_file_path_v1(
                InvertedIndexDescriptor::get_index_file_path_prefix(segment_path), 1, "");
        bool exists = false;
        EXPECT_TRUE(io::global_local_filesystem()->exists(index_path, &exists).ok());
        EXPECT_TRUE(exists) << "Index file not found: " << index_path;
    } else {
        // V2+ format
        auto index_path = InvertedIndexDescriptor::get_index_file_path_v2(
                InvertedIndexDescriptor::get_index_file_path_prefix(segment_path));
        bool exists = false;
        EXPECT_TRUE(io::global_local_filesystem()->exists(index_path, &exists).ok());
        EXPECT_TRUE(exists) << "Index file not found: " << index_path;
    }
}

TEST_F(IndexBuilderTest, UniqueKeysTableIndexTest) {
    // 0. prepare tablet path
    auto tablet_path = _absolute_dir + "/" + std::to_string(14688);
    _tablet->_tablet_path = tablet_path;
    _tablet->_tablet_meta->_schema = _tablet_schema;
    _tablet->_tablet_meta->_schema->_keys_type = KeysType::UNIQUE_KEYS;
    _tablet->_tablet_meta->_enable_unique_key_merge_on_write = true;
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(tablet_path).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet_path).ok());

    // 1. Prepare data for writing
    RowsetSharedPtr rowset;
    const int rows_per_segment = 500;

    // 2. Create a rowset writer context with segment size set to trigger multiple segments
    RowsetWriterContext writer_context;
    writer_context.rowset_id.init(15677);
    writer_context.tablet_id = 15677;
    writer_context.tablet_schema_hash = 567997577;
    writer_context.partition_id = 10;
    writer_context.rowset_type = BETA_ROWSET;
    writer_context.tablet_path = tablet_path;
    writer_context.rowset_state = VISIBLE;
    writer_context.tablet_schema = _tablet_schema;
    writer_context.version.first = 10;
    writer_context.version.second = 10;
    // Set small segment size to ensure we create multiple segments
    writer_context.max_rows_per_segment = rows_per_segment;

    ASSERT_TRUE(io::global_local_filesystem()->create_directory(writer_context.tablet_path).ok());

    auto res = RowsetFactory::create_rowset_writer(*_engine_ref, writer_context, false);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto rowset_writer = std::move(res).value();

    {
        Block block = _tablet_schema->create_block();
        auto columns = std::move(block).mutate_columns();

        // Add data for k1 and k2 columns
        for (int i = 0; i < 1000; ++i) {
            // k1 column (int)
            int32_t k1 = i * 10;
            columns[0]->insert_data((const char*)&k1, sizeof(k1));

            // k2 column (int)
            int32_t k2 = i % 100;
            columns[1]->insert_data((const char*)&k2, sizeof(k2));
        }

        block.set_columns(std::move(columns));

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

    // 6. Create test class that overrides methods to simulate unique key table behavior
    class TestIndexBuilder : public IndexBuilder {
    public:
        TestIndexBuilder(StorageEngine& engine, TabletSharedPtr tablet,
                         const std::vector<TColumn>& columns,
                         const std::vector<doris::TOlapTableIndex>& alter_inverted_indexes,
                         bool is_drop_op)
                : IndexBuilder(engine, tablet, columns, alter_inverted_indexes, is_drop_op) {}

        ~TestIndexBuilder() override = default;

        // Override to make sure modify_rowsets with UNIQUE_KEYS path is called
        Status modify_rowsets(const Merger::Statistics* stats = nullptr) override {
            // Call parent method which should use the UNIQUE_KEYS path
            return IndexBuilder::modify_rowsets(stats);
        }
    };

    _alter_indexes.clear();
    TOlapTableIndex tt_index;
    tt_index.index_id = 1;
    tt_index.index_name = "k1_index";
    tt_index.columns.emplace_back("k1");
    tt_index.column_unique_ids.push_back(1);
    tt_index.index_type = TIndexType::type::INVERTED;
    _alter_indexes.push_back(tt_index);
    // 7. Initialize and build inverted index
    TestIndexBuilder builder(*_engine_ref, _tablet, _columns, _alter_indexes, false);
    auto status = builder.init();
    EXPECT_TRUE(status.ok()) << status.to_string();

    // 8. Execute build index, which should go through UNIQUE_KEYS path in modify_rowsets
    status = builder.do_build_inverted_index();
    EXPECT_TRUE(status.ok()) << status.to_string();

    // 9. Verify that the index was created successfully
    std::string segment_path = local_segment_path(
            _tablet->tablet_path(),
            extract_rowset_id("020000000000000100000000000000000000000000000000_0.dat").to_string(),
            0);

    if (_tablet_schema->get_inverted_index_storage_format() == InvertedIndexStorageFormatPB::V1) {
        auto index_path = InvertedIndexDescriptor::get_index_file_path_v1(
                InvertedIndexDescriptor::get_index_file_path_prefix(segment_path), 1, "");
        bool exists = false;
        EXPECT_TRUE(io::global_local_filesystem()->exists(index_path, &exists).ok());
        EXPECT_TRUE(exists) << "Index file not found: " << index_path;
    } else {
        auto index_path = InvertedIndexDescriptor::get_index_file_path_v2(
                InvertedIndexDescriptor::get_index_file_path_prefix(segment_path));
        bool exists = false;
        EXPECT_TRUE(io::global_local_filesystem()->exists(index_path, &exists).ok());
        EXPECT_TRUE(exists) << "Index file not found: " << index_path;
    }
}

TEST_F(IndexBuilderTest, HandleSingleRowsetErrorTest) {
    // 1. Create a test class that overrides handle_single_rowset to simulate error scenarios
    class TestIndexBuilder : public IndexBuilder {
    public:
        TestIndexBuilder(StorageEngine& engine, TabletSharedPtr tablet,
                         const std::vector<TColumn>& columns,
                         const std::vector<doris::TOlapTableIndex>& alter_inverted_indexes,
                         bool is_drop_op, bool simulate_non_local_rowset_error = false)
                : IndexBuilder(engine, tablet, columns, alter_inverted_indexes, is_drop_op),
                  _simulate_non_local_rowset_error(simulate_non_local_rowset_error) {}

        ~TestIndexBuilder() override = default;

        // Override to simulate error conditions
        Status handle_single_rowset(RowsetMetaSharedPtr output_rowset_meta,
                                    std::vector<segment_v2::SegmentSharedPtr>& segments) override {
            if (_simulate_non_local_rowset_error) {
                // Simulate the condition where is_local_rowset is false
                return Status::InternalError("should be local rowset. tablet_id={} rowset_id={}",
                                             123, "test_rowset_id");
            }

            // Call parent method for normal processing
            return IndexBuilder::handle_single_rowset(output_rowset_meta, segments);
        }

    private:
        bool _simulate_non_local_rowset_error;
    };

    // 2. Prepare tablet path
    std::string tablet_path = _absolute_dir + "/" + std::to_string(14687);
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tablet_path).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(tablet_path).ok());

    // 3. Set up tablet schema and tablet
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    create_tablet_schema(tablet_schema, KeysType::DUP_KEYS, 2);

    auto tablet_meta = create_tablet_meta();
    tablet_meta->_schema = tablet_schema;
    auto tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    tablet->_tablet_path = tablet_path;
    ASSERT_TRUE(tablet->init().ok());

    // 4. Create inverted index definition
    _alter_indexes.clear();
    TOlapTableIndex tt_index;
    tt_index.index_id = 1;
    tt_index.index_name = "k1_index";
    tt_index.columns.emplace_back("k1");
    tt_index.column_unique_ids.push_back(1);
    tt_index.index_type = TIndexType::type::INVERTED;
    _alter_indexes.push_back(tt_index);

    // 5. Create a rowset
    RowsetWriterContext writer_context;
    writer_context.rowset_id = _engine_ref->next_rowset_id();
    writer_context.tablet_id = 14687;
    writer_context.tablet_path = tablet_path;
    writer_context.tablet_schema_hash = 1111;
    writer_context.partition_id = 10;
    writer_context.rowset_type = BETA_ROWSET;
    writer_context.segments_overlap = NONOVERLAPPING;
    writer_context.tablet_schema = tablet_schema;
    writer_context.version.first = 10;
    writer_context.version.second = 10;

    auto result = tablet->create_rowset_writer(writer_context, false);
    EXPECT_TRUE(result.has_value()) << result.error();
    auto rowset_writer = std::move(result).value();

    {
        Block block = _tablet_schema->create_block();
        auto columns = std::move(block).mutate_columns();

        // Add data for k1 and k2 columns
        for (int i = 0; i < 1000; ++i) {
            // k1 column (int)
            int32_t k1 = i * 10;
            columns[0]->insert_data((const char*)&k1, sizeof(k1));

            // k2 column (int)
            int32_t k2 = i % 100;
            columns[1]->insert_data((const char*)&k2, sizeof(k2));
        }

        block.set_columns(std::move(columns));

        // Add the block to the rowset
        Status s = rowset_writer->add_block(&block);
        ASSERT_TRUE(s.ok()) << s.to_string();

        // Flush the writer
        s = rowset_writer->flush();
        ASSERT_TRUE(s.ok()) << s.to_string();

        // Build the rowset
        RowsetSharedPtr rowset;
        ASSERT_TRUE(rowset_writer->build(rowset).ok());

        // Add the rowset to the tablet
        ASSERT_TRUE(tablet->add_rowset(rowset).ok());
    }

    // 6. Test error scenario with non-local rowset
    TestIndexBuilder builder(*_engine_ref, tablet, _columns, _alter_indexes, false, true);
    auto status = builder.init();
    EXPECT_TRUE(status.ok()) << status.to_string();

    // Execute build_index, which should fail due to simulated error
    status = builder.do_build_inverted_index();
    EXPECT_FALSE(status.ok()) << "Expected failure but got success";
    EXPECT_TRUE(status.is<ErrorCode::INTERNAL_ERROR>())
            << "Expected internal error but got: " << status.to_string();
    EXPECT_TRUE(status.to_string().find("should be local rowset") != std::string::npos)
            << "Error message doesn't match expected: " << status.to_string();
}

TEST_F(IndexBuilderTest, UpdateInvertedIndexInfoErrorTest) {
    // 1. Create a test class that overrides update_inverted_index_info to simulate error scenarios
    class TestIndexBuilder : public IndexBuilder {
    public:
        TestIndexBuilder(StorageEngine& engine, TabletSharedPtr tablet,
                         const std::vector<TColumn>& columns,
                         const std::vector<doris::TOlapTableIndex>& alter_inverted_indexes,
                         bool is_drop_op, int error_type = 0)
                : IndexBuilder(engine, tablet, columns, alter_inverted_indexes, is_drop_op),
                  _error_type(error_type) {}

        ~TestIndexBuilder() override = default;

        // Override update_inverted_index_info to inject errors
        Status update_inverted_index_info() override {
            if (_error_type == 1) {
                // Simulate non-local rowset error in update_inverted_index_info
                return Status::InternalError("should be local rowset. tablet_id={} rowset_id={}",
                                             123, "test_rowset_id");
            } else if (_error_type == 2) {
                // Simulate size retrieval error
                return Status::Error<ErrorCode::INIT_FAILED>("debug point: get fs failed");
            }

            // Call parent method for normal processing
            return IndexBuilder::update_inverted_index_info();
        }

    private:
        int _error_type; // 0: no error, 1: non-local rowset error, 2: size retrieval error
    };

    // 2. Prepare tablet path
    std::string tablet_path = _absolute_dir + "/" + std::to_string(14688);
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tablet_path).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(tablet_path).ok());

    // 3. Set up tablet schema and tablet
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    create_tablet_schema(tablet_schema, KeysType::DUP_KEYS, 2);

    auto tablet_meta = create_tablet_meta();
    tablet_meta->_schema = tablet_schema;
    auto tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    tablet->_tablet_path = tablet_path;
    ASSERT_TRUE(tablet->init().ok());

    // 4. Create inverted index definition
    _alter_indexes.clear();
    TOlapTableIndex tt_index;
    tt_index.index_id = 1;
    tt_index.index_name = "k1_index";
    tt_index.columns.emplace_back("k1");
    tt_index.column_unique_ids.push_back(1);
    tt_index.index_type = TIndexType::type::INVERTED;
    _alter_indexes.push_back(tt_index);

    // 5. Create a rowset
    RowsetWriterContext writer_context;
    writer_context.rowset_id = _engine_ref->next_rowset_id();
    writer_context.tablet_id = 14688;
    writer_context.tablet_path = tablet_path;
    writer_context.tablet_schema_hash = 1111;
    writer_context.partition_id = 10;
    writer_context.rowset_type = BETA_ROWSET;
    writer_context.segments_overlap = NONOVERLAPPING;
    writer_context.tablet_schema = tablet_schema;
    writer_context.version.first = 10;
    writer_context.version.second = 10;

    auto result = tablet->create_rowset_writer(writer_context, false);
    EXPECT_TRUE(result.has_value()) << result.error();
    auto rowset_writer = std::move(result).value();

    // Write data
    {
        Block block = tablet_schema->create_block();
        auto columns = std::move(block).mutate_columns();

        // Add data for k1 and k2 columns
        for (int i = 0; i < 1000; ++i) {
            // k1 column (int)
            int32_t k1 = i * 10;
            columns[0]->insert_data((const char*)&k1, sizeof(k1));

            // k2 column (int)
            int32_t k2 = i % 100;
            columns[1]->insert_data((const char*)&k2, sizeof(k2));
        }

        block.set_columns(std::move(columns));

        // Add the block to the rowset
        Status s = rowset_writer->add_block(&block);
        ASSERT_TRUE(s.ok()) << s.to_string();

        // Flush the writer
        s = rowset_writer->flush();
        ASSERT_TRUE(s.ok()) << s.to_string();

        // Build the rowset
        RowsetSharedPtr rowset;
        ASSERT_TRUE(rowset_writer->build(rowset).ok());

        // Add the rowset to the tablet
        ASSERT_TRUE(tablet->add_rowset(rowset).ok());
    }
    // 6. Test error scenarios

    // 6.1 Test non-local rowset error
    {
        TestIndexBuilder builder(*_engine_ref, tablet, _columns, _alter_indexes, false, 1);
        auto status = builder.init();
        EXPECT_TRUE(status.ok()) << status.to_string();

        // Execute build_index, which should fail due to simulated error
        status = builder.do_build_inverted_index();
        EXPECT_FALSE(status.ok()) << "Expected failure but got success";
        EXPECT_TRUE(status.is<ErrorCode::INTERNAL_ERROR>())
                << "Expected internal error but got: " << status.to_string();
        EXPECT_TRUE(status.to_string().find("should be local rowset") != std::string::npos)
                << "Error message doesn't match expected: " << status.to_string();
    }

    // 6.2 Test size retrieval error
    {
        TestIndexBuilder builder(*_engine_ref, tablet, _columns, _alter_indexes, false, 2);
        auto status = builder.init();
        EXPECT_TRUE(status.ok()) << status.to_string();

        // Execute build_index, which should fail due to simulated error
        status = builder.do_build_inverted_index();
        EXPECT_FALSE(status.ok()) << "Expected failure but got success";
        EXPECT_TRUE(status.is<ErrorCode::INIT_FAILED>())
                << "Expected INIT_FAILED but got: " << status.to_string();
        EXPECT_TRUE(status.to_string().find("debug point: get fs failed") != std::string::npos)
                << "Error message doesn't match expected: " << status.to_string();
    }
}

// Test case: Drop one index should not affect other indexes on the same column
// This test verifies the bug fix: when dropping one inverted index,
// other indexes on the same column should NOT be deleted
TEST_F(IndexBuilderTest, DropOneIndexNotAffectOtherIndexesOnSameColumnTest) {
    // 0. prepare tablet path
    auto tablet_path = _absolute_dir + "/" + std::to_string(15690);
    _tablet->_tablet_path = tablet_path;
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(tablet_path).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet_path).ok());

    // 1. Prepare data for writing
    RowsetSharedPtr rowset;
    const int num_rows = 1000;

    // 2. Add two different inverted indexes on the same column (k1)
    // First index with index_id = 1
    TabletIndex index1;
    index1._index_id = 1;
    index1._index_name = "k1_index_1";
    index1._index_type = IndexType::INVERTED;
    index1._col_unique_ids.push_back(1); // unique_id for k1
    _tablet_schema->append_index(TabletIndex(index1));

    // Second index with index_id = 2 (different analyzer/properties)
    TabletIndex index2;
    index2._index_id = 2;
    index2._index_name = "k1_index_2";
    index2._index_type = IndexType::INVERTED;
    index2._col_unique_ids.push_back(1); // same column: k1
    _tablet_schema->append_index(TabletIndex(index2));

    // 3. Create a rowset writer context
    RowsetWriterContext writer_context;
    writer_context.rowset_id.init(15690);
    writer_context.tablet_id = 15690;
    writer_context.tablet_schema_hash = 567997577;
    writer_context.partition_id = 10;
    writer_context.rowset_type = BETA_ROWSET;
    writer_context.tablet_path = tablet_path;
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
        Block block = _tablet_schema->create_block();
        auto columns = std::move(block).mutate_columns();

        // Add data for k1 and k2 columns
        for (int i = 0; i < num_rows; ++i) {
            // k1 column (int)
            int32_t k1 = i * 10;
            columns[0]->insert_data((const char*)&k1, sizeof(k1));

            // k2 column (int)
            int32_t k2 = i % 100;
            columns[1]->insert_data((const char*)&k2, sizeof(k2));
        }

        block.set_columns(std::move(columns));

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

    // 6. Verify both indexes exist before dropping
    EXPECT_TRUE(_tablet_schema->has_inverted_index());
    EXPECT_TRUE(_tablet_schema->has_inverted_index_with_index_id(1));
    EXPECT_TRUE(_tablet_schema->has_inverted_index_with_index_id(2));

    // 7. Prepare to drop ONLY index_id=1 (not index_id=2)
    TOlapTableIndex drop_index;
    drop_index.index_type = TIndexType::INVERTED;
    drop_index.index_id = 1; // Only drop index with id=1
    drop_index.columns.emplace_back("k1");
    _alter_indexes.push_back(drop_index);

    // 8. Create IndexBuilder with drop operation
    IndexBuilder builder(ExecEnv::GetInstance()->storage_engine().to_local(), _tablet, _columns,
                         _alter_indexes, true);

    // 9. Initialize and verify
    auto status = builder.init();
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(builder._alter_index_ids.size(), 1);
    EXPECT_TRUE(builder._alter_index_ids.contains(1) > 0);

    // 10. Execute drop operation
    status = builder.do_build_inverted_index();
    EXPECT_TRUE(status.ok()) << status.to_string();

    // 11. Verify the results:
    // - index_id=1 should be dropped
    // - index_id=2 should still exist (this is the key verification for the bug fix)

    // Get the output rowset's tablet schema to verify
    ASSERT_FALSE(builder._output_rowsets.empty());
    auto output_rowset = builder._output_rowsets[0];
    auto output_schema = output_rowset->tablet_schema();

    // Verify index_id=1 has been removed
    EXPECT_FALSE(output_schema->has_inverted_index_with_index_id(1))
            << "index_id=1 should have been dropped";

    // Verify index_id=2 still exists (this is the bug fix verification)
    EXPECT_TRUE(output_schema->has_inverted_index_with_index_id(2))
            << "index_id=2 should still exist after dropping index_id=1";

    // 12. Additional verification: check the number of inverted indexes
    // There should be exactly 1 inverted index remaining
    const auto& inverted_indexes = output_schema->inverted_indexes();
    int inverted_index_count = 0;
    for (const auto& idx : inverted_indexes) {
        if (idx->index_type() == IndexType::INVERTED) {
            inverted_index_count++;
        }
    }
    EXPECT_EQ(inverted_index_count, 1)
            << "Should have exactly 1 inverted index remaining after drop";
}

TEST_F(IndexBuilderTest, DropOneSniiIndexPreservesSurvivingPhysicalIndex) {
    const auto tablet_path = _absolute_dir + "/15691";
    auto tablet_schema = create_snii_drop_schema();
    TabletSharedPtr tablet;
    ASSERT_TRUE(create_snii_drop_tablet(tablet_schema, tablet_path, &tablet).ok());
    RowsetSharedPtr source_rowset;
    ASSERT_TRUE(create_snii_source_rowset(tablet, tablet_schema, tablet_path, &source_rowset).ok());
    ASSERT_GT(source_rowset->index_disk_size(), 0);

    RowsetSharedPtr output_rowset;
    ASSERT_TRUE(drop_snii_index(tablet, create_drop_index(1, "idx_a", "body_a", 2), &output_rowset)
                        .ok());
    assert_snii_surviving_index(source_rowset, output_rowset);
    assert_snii_term_query(output_rowset, tablet->tablet_id());

    ScopedIndexBuilderDebugPoints debug_points;
    debug_points.enable("IndexBuilder::update_inverted_index_info_index_file_reader_init_not_ok");
    RowsetSharedPtr final_rowset;
    ASSERT_TRUE(drop_snii_index(tablet, create_drop_index(2, "idx_b", "body_b", 3), &final_rowset)
                        .ok());
    assert_last_snii_index_dropped(source_rowset, final_rowset);
}

// Classification only: unchanged-and-present -> inherit; requested-and-absent ->
// build; same key with a changed definition -> rebuild, never inherit; a container
// key the target schema no longer holds is not inherited; two build indexes on one
// column share one column group (the "scan the column once" pin).
TEST_F(IndexBuilderTest, SniiBuildPlanClassifiesInheritBuildReplaceAndDrop) {
    const auto input_schema = create_snii_schema(
            {SniiIndexSpec {.index_id = 1, .index_name = "idx_a", .column_unique_id = 2}});
    auto output_schema = create_snii_schema(
            {SniiIndexSpec {.index_id = 1, .index_name = "idx_a", .column_unique_id = 2},
             SniiIndexSpec {.index_id = 2, .index_name = "idx_b", .column_unique_id = 3},
             SniiIndexSpec {.index_id = 3,
                            .index_name = "idx_c",
                            .column_unique_id = 3,
                            .properties = {{"parser", "none"}}}});
    // The container carries idx_a plus a stale key (9) the target schema dropped.
    const auto container_has = [](const TabletIndex& index, bool* exists) {
        *exists = index.index_id() == 1;
        return Status::OK();
    };

    IndexBuilder::SniiIndexRewritePlan plan;
    ASSERT_TRUE(IndexBuilder::plan_snii_index_rewrite(*input_schema, *output_schema, {2, 3},
                                                      container_has, &plan)
                        .ok());
    ASSERT_EQ(plan.inherit_keys.size(), 1U);
    EXPECT_EQ(plan.inherit_keys.front().index_id, 1U);
    // idx_b and idx_c both target column 3: exactly ONE column group with both.
    ASSERT_EQ(plan.build_columns.size(), 1U);
    EXPECT_EQ(plan.build_columns.front().first, 3);
    ASSERT_EQ(plan.build_columns.front().second.size(), 2U);
}

TEST_F(IndexBuilderTest, SniiBuildPlanClassifiesReplaceAndRetry) {
    const auto input_schema = create_snii_schema(
            {SniiIndexSpec {.index_id = 1, .index_name = "idx_a", .column_unique_id = 2}});
    const auto container_has = [](const TabletIndex& index, bool* exists) {
        *exists = index.index_id() == 1;
        return Status::OK();
    };

    // Same key, changed definition: the request replaces idx_a's parser, so the
    // old metadata must NOT be inherited -- the index is rebuilt.
    auto replaced_schema = create_snii_schema({SniiIndexSpec {.index_id = 1,
                                                              .index_name = "idx_a",
                                                              .column_unique_id = 2,
                                                              .properties = {{"parser", "none"}}}});
    IndexBuilder::SniiIndexRewritePlan replace_plan;
    ASSERT_TRUE(IndexBuilder::plan_snii_index_rewrite(*input_schema, *replaced_schema, {1},
                                                      container_has, &replace_plan)
                        .ok());
    EXPECT_TRUE(replace_plan.inherit_keys.empty());
    ASSERT_EQ(replace_plan.build_columns.size(), 1U);
    EXPECT_EQ(replace_plan.build_columns.front().first, 2);

    // Retry: the requested index already exists in schema and container with the
    // same definition -> inherit, no build work at all.
    IndexBuilder::SniiIndexRewritePlan retry_plan;
    ASSERT_TRUE(IndexBuilder::plan_snii_index_rewrite(*input_schema, *input_schema, {1},
                                                      container_has, &retry_plan)
                        .ok());
    ASSERT_EQ(retry_plan.inherit_keys.size(), 1U);
    EXPECT_TRUE(retry_plan.build_columns.empty());
}

TEST_F(IndexBuilderTest, SniiBuildAddsSecondIndexAndInheritsFirst) {
    const auto tablet_path = _absolute_dir + "/15691";
    auto tablet_schema = create_snii_schema(
            {SniiIndexSpec {.index_id = 1, .index_name = "idx_a", .column_unique_id = 2}});
    TabletSharedPtr tablet;
    ASSERT_TRUE(create_snii_drop_tablet(tablet_schema, tablet_path, &tablet).ok());
    RowsetSharedPtr source_rowset;
    ASSERT_TRUE(create_snii_source_rowset(tablet, tablet_schema, tablet_path, &source_rowset).ok());

    std::vector<RowsetSharedPtr> output_rowsets;
    ASSERT_TRUE(build_snii_index(tablet,
                                 {create_build_index(2, "idx_b", "body_b", 3,
                                                     {{"parser", "english"},
                                                      {"lower_case", "true"},
                                                      {"support_phrase", "true"}})},
                                 &output_rowsets)
                        .ok());
    ASSERT_EQ(output_rowsets.size(), 1U);
    const RowsetSharedPtr& output_rowset = output_rowsets.front();

    ASSERT_TRUE(output_rowset->tablet_schema()->has_inverted_index_with_index_id(1));
    ASSERT_TRUE(output_rowset->tablet_schema()->has_inverted_index_with_index_id(2));
    // The inherited index answers as before; the built index answers over the
    // historical rows.
    assert_snii_term(output_rowset, tablet->tablet_id(), 2, 1, "drop", {0, 1});
    assert_snii_term(output_rowset, tablet->tablet_id(), 3, 2, "keep", {0, 1});
    assert_snii_inherited_prefix(source_rowset, output_rowset, tablet->tablet_id(),
                                 {{.index_id = 1, .index_suffix = ""}}, /*doc_count=*/2);
}

TEST_F(IndexBuilderTest, SniiBuildAllSharesOneColumnScanAndOnePrefixCopy) {
    const auto tablet_path = _absolute_dir + "/15691";
    auto tablet_schema = create_snii_schema(
            {SniiIndexSpec {.index_id = 1, .index_name = "idx_a", .column_unique_id = 2}});
    TabletSharedPtr tablet;
    ASSERT_TRUE(create_snii_drop_tablet(tablet_schema, tablet_path, &tablet).ok());
    RowsetSharedPtr source_rowset;
    ASSERT_TRUE(create_snii_source_rowset(tablet, tablet_schema, tablet_path, &source_rowset).ok());

    // BUILD ALL: two new indexes on the SAME column plus the untouched idx_a.
    std::vector<RowsetSharedPtr> output_rowsets;
    ASSERT_TRUE(
            build_snii_index(tablet,
                             {create_build_index(2, "idx_b", "body_b", 3,
                                                 {{"parser", "english"},
                                                  {"lower_case", "true"},
                                                  {"support_phrase", "true"}}),
                              create_build_index(3, "idx_c", "body_b", 3, {{"parser", "none"}})},
                             &output_rowsets)
                    .ok());
    ASSERT_EQ(output_rowsets.size(), 1U);
    const RowsetSharedPtr& output_rowset = output_rowsets.front();

    assert_snii_term(output_rowset, tablet->tablet_id(), 2, 1, "drop", {0, 1});
    assert_snii_term(output_rowset, tablet->tablet_id(), 3, 2, "keep", {0, 1});
    // idx_c is untokenized: the whole cell value is one term.
    assert_snii_term(output_rowset, tablet->tablet_id(), 3, 3, "keep alpha", {0});
    assert_snii_inherited_prefix(source_rowset, output_rowset, tablet->tablet_id(),
                                 {{.index_id = 1, .index_suffix = ""}}, /*doc_count=*/2);
}

// A retried build names an index the rowset schema already carries: the rowset
// is skipped upstream (pick_candidate_rowsets_to_build_inverted_index), so no
// analyzer, decode or encode runs and nothing is rewritten. The same-key-with-
// changed-definition case is unreachable here for the same reason; its
// classification is pinned by SniiBuildPlanClassifiesReplaceAndRetry.
TEST_F(IndexBuilderTest, SniiBuildRetrySkipsRowsetsAlreadyCoveringTheIndex) {
    const auto tablet_path = _absolute_dir + "/15691";
    auto tablet_schema = create_snii_drop_schema(); // idx_a and idx_b both present
    TabletSharedPtr tablet;
    ASSERT_TRUE(create_snii_drop_tablet(tablet_schema, tablet_path, &tablet).ok());
    RowsetSharedPtr source_rowset;
    ASSERT_TRUE(create_snii_source_rowset(tablet, tablet_schema, tablet_path, &source_rowset).ok());

    std::vector<RowsetSharedPtr> output_rowsets;
    ASSERT_TRUE(build_snii_index(tablet,
                                 {create_build_index(2, "idx_b", "body_b", 3,
                                                     {{"parser", "english"},
                                                      {"lower_case", "true"},
                                                      {"support_phrase", "true"}})},
                                 &output_rowsets)
                        .ok());
    // Skipped, not rewritten: no output rowset, the tablet still serves the
    // original one, and the container remains fully queryable.
    EXPECT_TRUE(output_rowsets.empty());
    auto rowset = tablet->get_rowset_by_version(Version(10, 10));
    ASSERT_NE(rowset, nullptr);
    EXPECT_EQ(rowset->rowset_id(), source_rowset->rowset_id());
    assert_snii_term(source_rowset, tablet->tablet_id(), 2, 1, "drop", {0, 1});
    assert_snii_term(source_rowset, tablet->tablet_id(), 3, 2, "keep", {0, 1});
}

TEST_F(IndexBuilderTest, SniiBuildFailureCommitsNoRowset) {
    const auto tablet_path = _absolute_dir + "/15691";
    auto tablet_schema = create_snii_schema(
            {SniiIndexSpec {.index_id = 1, .index_name = "idx_a", .column_unique_id = 2}});
    TabletSharedPtr tablet;
    ASSERT_TRUE(create_snii_drop_tablet(tablet_schema, tablet_path, &tablet).ok());
    RowsetSharedPtr source_rowset;
    ASSERT_TRUE(create_snii_source_rowset(tablet, tablet_schema, tablet_path, &source_rowset).ok());

    ScopedIndexBuilderDebugPoints debug_points;
    debug_points.enable("IndexBuilder::handle_single_rowset_snii_index_build_finish_error");
    std::vector<RowsetSharedPtr> output_rowsets;
    const Status status = build_snii_index(
            tablet,
            {create_build_index(
                    2, "idx_b", "body_b", 3,
                    {{"parser", "english"}, {"lower_case", "true"}, {"support_phrase", "true"}})},
            &output_rowsets);
    // Specifically the INJECTED failure: the SNII build path must run far enough
    // to hit the debug point and then fail the whole task.
    ASSERT_TRUE(status.is<ErrorCode::INTERNAL_ERROR>())
            << "expected the injected index build failure, got: " << status;

    // The source rowset is untouched and still fully queryable ...
    assert_snii_term(source_rowset, tablet->tablet_id(), 2, 1, "drop", {0, 1});
    // ... and the tablet still serves the ORIGINAL rowset for that version: the
    // failed build committed nothing.
    auto rowset = tablet->get_rowset_by_version(Version(10, 10));
    ASSERT_NE(rowset, nullptr);
    EXPECT_EQ(rowset->rowset_id(), source_rowset->rowset_id());
}

} // namespace doris
