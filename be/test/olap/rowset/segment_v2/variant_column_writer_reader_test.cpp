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

#include <atomic>
#include <thread>

#include "common/config.h"
#include "gtest/gtest.h"
#include "olap/rowset/segment_v2/column_meta_accessor.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/column_reader_cache.h"
#include "olap/rowset/segment_v2/variant/binary_column_extract_iterator.h"
#include "olap/rowset/segment_v2/variant/hierarchical_data_iterator.h"
#include "olap/rowset/segment_v2/variant/sparse_column_merge_iterator.h"
#include "olap/rowset/segment_v2/variant/variant_column_reader.h"
#include "olap/rowset/segment_v2/variant/variant_column_writer_impl.h"
#include "olap/rowset/segment_v2/variant/variant_doc_snpashot_compact_iterator.h"
#include "olap/storage_engine.h"
#include "testutil/variant_util.h"
#include "vec/data_types/serde/data_type_serde.h"

using namespace doris::vectorized;

namespace doris {

constexpr static uint32_t MAX_PATH_LEN = 1024;
constexpr static std::string_view dest_dir = "/ut_dir/variant_column_writer_test";
constexpr static std::string_view tmp_dir = "./ut_dir/tmp";

static void construct_column(ColumnPB* column_pb, int32_t col_unique_id,
                             const std::string& column_type, const std::string& column_name,
                             int variant_max_subcolumns_count = 3, bool is_key = false,
                             bool is_nullable = false, int variant_sparse_hash_shard_count = 0,
                             bool variant_enable_doc_mode = false,
                             int64_t variant_doc_materialization_min_rows = 0,
                             int variant_doc_hash_shard_count = 0) {
    column_pb->set_unique_id(col_unique_id);
    column_pb->set_name(column_name);
    column_pb->set_type(column_type);
    column_pb->set_is_key(is_key);
    column_pb->set_is_nullable(is_nullable);
    if (column_type == "VARIANT") {
        column_pb->set_variant_max_subcolumns_count(variant_max_subcolumns_count);
        column_pb->set_variant_max_sparse_column_statistics_size(10000);
        // 5 sparse hash shard
        column_pb->set_variant_sparse_hash_shard_count(variant_sparse_hash_shard_count);
        column_pb->set_variant_enable_doc_mode(variant_enable_doc_mode);
        column_pb->set_variant_doc_materialization_min_rows(variant_doc_materialization_min_rows);
        if (variant_doc_hash_shard_count > 0) {
            column_pb->set_variant_doc_hash_shard_count(variant_doc_hash_shard_count);
        }
    }
}

// static void construct_tablet_index(TabletIndexPB* tablet_index, int64_t index_id,
//                                    const std::string& index_name, int32_t col_unique_id) {
//     tablet_index->set_index_id(index_id);
//     tablet_index->set_index_name(index_name);
//     tablet_index->set_index_type(IndexType::INVERTED);
//     tablet_index->add_col_unique_id(col_unique_id);
// }

// MockColumnReaderCache class for testing
class MockColumnReaderCache : public segment_v2::ColumnReaderCache {
public:
    MockColumnReaderCache(const SegmentFooterPB& footer, const io::FileReaderSPtr& file_reader,
                          const std::shared_ptr<TabletSchema>& tablet_schema)
            : ColumnReaderCache(nullptr, nullptr, nullptr, 0,
                                [](std::shared_ptr<SegmentFooterPB>&, OlapReaderStatistics*) {
                                    return Status::OK();
                                }),
              _footer(footer),
              _file_reader(file_reader),
              _tablet_schema(tablet_schema) {}

    Status get_path_column_reader(
            int32_t col_uid, vectorized::PathInData relative_path,
            std::shared_ptr<segment_v2::ColumnReader>* column_reader, OlapReaderStatistics* stats,
            const SubcolumnColumnMetaInfo::Node* node_hint = nullptr) override {
        DCHECK(node_hint != nullptr);
        // Use node_hint's footer_ordinal to locate the specific ColumnMeta
        int32_t footer_ordinal = node_hint->data.footer_ordinal;
        if (footer_ordinal < 0 || footer_ordinal >= _footer.columns_size()) {
            *column_reader = nullptr;
            return Status::OK();
        }

        // Create ColumnReaderOptions
        ColumnReaderOptions opts;
        opts.kept_in_memory = false;
        opts.be_exec_version = BeExecVersionManager::get_newest_version();
        opts.tablet_schema = _tablet_schema;

        // Use ColumnReader::create to generate the corresponding ColumnReader
        return segment_v2::ColumnReader::create(opts, _footer.columns(footer_ordinal),
                                                _footer.num_rows(), _file_reader, column_reader);
    }

private:
    const SegmentFooterPB& _footer;
    const io::FileReaderSPtr& _file_reader;
    const std::shared_ptr<TabletSchema>& _tablet_schema;
};

// Helper to create a root VariantColumnReader using ColumnMetaAccessor, which
// hides inline vs external column meta layout (V2 vs V3).
static Status create_variant_root_reader(const SegmentFooterPB& footer,
                                         const io::FileReaderSPtr& file_reader,
                                         const TabletSchemaSPtr& tablet_schema,
                                         std::shared_ptr<segment_v2::ColumnReader>* out) {
    segment_v2::ColumnMetaAccessor accessor;
    RETURN_IF_ERROR(accessor.init(footer, file_reader));

    segment_v2::ColumnReaderOptions opts;
    opts.kept_in_memory = false;
    opts.be_exec_version = BeExecVersionManager::get_newest_version();
    opts.tablet_schema = tablet_schema;

    auto variant_reader = std::make_shared<segment_v2::VariantColumnReader>();
    int32_t root_uid = tablet_schema->column(0).unique_id();
    auto footer_sp = std::make_shared<SegmentFooterPB>();
    footer_sp->CopyFrom(footer);
    RETURN_IF_ERROR(variant_reader->init(opts, &accessor, footer_sp, root_uid, footer.num_rows(),
                                         file_reader));
    *out = std::move(variant_reader);
    return Status::OK();
}

class VariantColumnWriterReaderTest : public testing::Test {
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

    VariantColumnWriterReaderTest() = default;
    ~VariantColumnWriterReaderTest() override = default;

protected:
    TabletSchemaSPtr _tablet_schema = nullptr;
    StorageEngine* _engine_ref = nullptr;
    std::unique_ptr<DataDir> _data_dir = nullptr;
    TabletSharedPtr _tablet = nullptr;
    std::string _absolute_dir;
    std::string _current_dir;
};

void check_column_meta(const ColumnMetaPB& column_meta, auto& path_with_size) {
    EXPECT_TRUE(column_meta.has_column_path_info());
    auto path = std::make_shared<vectorized::PathInData>();
    path->from_protobuf(column_meta.column_path_info());
    EXPECT_EQ(column_meta.column_path_info().parrent_column_unique_id(), 1);
    EXPECT_EQ(column_meta.none_null_size(), path_with_size[path->copy_pop_front().get_path()]);
}

void check_sparse_column_meta(const ColumnMetaPB& column_meta, auto& path_with_size) {
    EXPECT_TRUE(column_meta.has_column_path_info());
    auto path = std::make_shared<vectorized::PathInData>();
    path->from_protobuf(column_meta.column_path_info());
    EXPECT_EQ(column_meta.column_path_info().parrent_column_unique_id(), 1);
    for (const auto& [pat, size] : column_meta.variant_statistics().sparse_column_non_null_size()) {
        EXPECT_EQ(size, path_with_size[pat]);
    }
    auto base_path = path->copy_pop_front().get_path();
    EXPECT_TRUE(base_path == "__DORIS_VARIANT_SPARSE__" ||
                base_path.rfind("__DORIS_VARIANT_SPARSE__.b", 0) == 0);
}

static void fill_variant_column_with_doc_value_only(
        vectorized::MutableColumnPtr& column_object, int num_rows,
        std::unordered_map<int, std::string>* inserted) {
    auto type_string = std::make_shared<vectorized::DataTypeString>();
    auto json_column = type_string->create_column();
    auto* column_string = assert_cast<vectorized::ColumnString*>(json_column.get());
    VariantUtil::fill_string_column_with_test_data(column_string, num_rows, inserted);

    vectorized::ParseConfig config;
    config.enable_flatten_nested = false;
    config.parse_to = vectorized::ParseConfig::ParseTo::OnlyDocValueColumn;
    variant_util::parse_json_to_variant(*column_object, *column_string, config);
}

// DOC_COMPACT reads only one doc bucket column (e.g. "__DORIS_VARIANT_DOC_VALUE__.b0"), so it
// naturally returns only the subset of keys mapped into that bucket.
// This helper derives the expected JSON string for a given bucket from the full JSON produced by
// VariantUtil::fill_string_column_with_test_data, without parsing JSON.
static std::string expected_doc_bucket_json_from_full(const std::string& full_json, int bucket_num,
                                                      int bucket_index) {
    auto bucket_of = [&](const std::string& key) -> uint32_t {
        StringRef ref {key.data(), key.size()};
        return vectorized::variant_util::variant_binary_shard_of(ref, bucket_num);
    };

    std::string out;
    out.reserve(full_json.size());
    out.push_back('{');

    bool first = true;
    // fill_string_column_with_test_data generates keys "key0".."key9" at most.
    for (int j = 0; j < 10; ++j) {
        const std::string key = "key" + std::to_string(j);
        const std::string needle = "\"" + key + "\":";
        if (full_json.find(needle) == std::string::npos) {
            continue;
        }
        if (bucket_of(key) != static_cast<uint32_t>(bucket_index)) {
            continue;
        }
        if (!first) {
            out.push_back(',');
        }
        first = false;
        out.append("\"");
        out.append(key);
        out.append("\":");
        if (j % 2 == 0) {
            out.append("88");
        } else {
            out.append("\"str99\"");
        }
    }

    out.push_back('}');
    return out;
}

TEST_F(VariantColumnWriterReaderTest, test_statics) {
    // VariantStatisticsPB stats_pb;
    // auto* subcolumns_stats = stats_pb.mutable_sparse_column_non_null_size();
    // (*subcolumns_stats)["key0"] = 500;  // 50% of rows have key0
    // (*subcolumns_stats)["key1"] = 500;  // 50% of rows have key1
    // (*subcolumns_stats)["key2"] = 333;  // 33.3% of rows have key2
    // (*subcolumns_stats)["key3"] = 200;  // 20% of rows have key3
    // (*subcolumns_stats)["key4"] = 1000; // 100% of rows have key4

    // auto* sparse_stats = stats_pb.mutable_sparse_column_non_null_size();
    // (*sparse_stats)["key5"] = 100;
    // (*sparse_stats)["key6"] = 200;
    // (*sparse_stats)["key7"] = 300;

    // // 6.2 Test from_pb
    // segment_v2::VariantStatistics stats;
    // stats.from_pb(stats_pb);

    // // 6.3 Verify statistics
    // EXPECT_EQ(stats.sparse_column_non_null_size["key0"], 500);
    // EXPECT_EQ(stats.sparse_column_non_null_size["key1"], 500);
    // EXPECT_EQ(stats.sparse_column_non_null_size["key2"], 333);
    // EXPECT_EQ(stats.sparse_column_non_null_size["key3"], 200);
    // EXPECT_EQ(stats.sparse_column_non_null_size["key4"], 1000);

    // EXPECT_EQ(stats.sparse_column_non_null_size["key5"], 100);
    // EXPECT_EQ(stats.sparse_column_non_null_size["key6"], 200);
    // EXPECT_EQ(stats.sparse_column_non_null_size["key7"], 300);
}

TEST_F(VariantColumnWriterReaderTest, test_write_data_normal) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    int variant_sparse_hash_shard_count = rand() % 10 + 1;
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 3, false, false,
                     variant_sparse_hash_shard_count);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_external_segment_meta_used_default(external_segment_meta_used_default);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());

    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn column = _tablet_schema->column(0);
    _init_column_meta(opts.meta, 0, column, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto column_object = (*std::move(block.get_by_position(0).column)).mutate();
    std::unordered_map<int, std::string> inserted_jsonstr;
    auto path_with_size =
            VariantUtil::fill_object_column_with_test_data(column_object, 1000, &inserted_jsonstr);
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), 1000).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    int expected_sparse_cols =
            variant_sparse_hash_shard_count > 1 ? variant_sparse_hash_shard_count : 1;
    EXPECT_EQ(footer.columns_size(), 1 + 3 + expected_sparse_cols);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);

    for (int i = 1; i < footer.columns_size() - 1; ++i) {
        auto column_met = footer.columns(i);
        check_column_meta(column_met, path_with_size);
    }
    check_sparse_column_meta(footer.columns(footer.columns_size() - 1), path_with_size);

    // 7. check variant reader
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    // create root variant reader using ColumnMetaAccessor (supports inline/external meta)
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    auto variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    auto subcolumn_meta = variant_column_reader->get_subcolumn_meta_by_path(PathInData("key0"));
    EXPECT_TRUE(subcolumn_meta != nullptr);
    subcolumn_meta = variant_column_reader->get_subcolumn_meta_by_path(PathInData("key1"));
    EXPECT_TRUE(subcolumn_meta != nullptr);
    subcolumn_meta = variant_column_reader->get_subcolumn_meta_by_path(PathInData("key2"));
    EXPECT_TRUE(subcolumn_meta != nullptr);
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("key3")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("key4")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("key5")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("key6")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("key7")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("key8")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("key9")));
    auto size = variant_column_reader->get_metadata_size();
    EXPECT_GT(size, 0);

    // 8. check statistics
    auto statistics = variant_column_reader->get_stats();
    for (const auto& [path, siz] : statistics->subcolumns_non_null_size) {
        EXPECT_EQ(path_with_size[path], siz);
    }
    for (const auto& [path, siz] : statistics->sparse_column_non_null_size) {
        EXPECT_EQ(path_with_size[path], siz);
    }

    // 9. check hier reader
    ColumnIteratorUPtr it;
    TabletColumn parent_column = _tablet_schema->column(0);
    StorageReadOptions storage_read_opts;
    OlapReaderStatistics stats;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    storage_read_opts.stats = &stats;
    st = variant_column_reader->new_iterator(&it, &parent_column, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<HierarchicalDataIterator*>(it.get()) != nullptr);
    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = it->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    MutableColumnPtr new_column_object = ColumnVariant::create(3, false);
    size_t nrows = 1000;
    st = it->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = it->next_batch(&nrows, new_column_object);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(stats.bytes_read > 0);

    // seek_to_first for HierarchicalDataIterator no need to implement
    {
        auto iter = assert_cast<HierarchicalDataIterator*>(it.get());
        std::shared_ptr<ColumnReader> column_reader1;
        st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader1);
        EXPECT_TRUE(st.ok()) << st.msg();
        std::cout << "hier:" << iter->get_current_ordinal() << std::endl;
        //  now we can find exist
        auto exist_node = std::make_unique<SubcolumnColumnMetaInfo::Node>(
                SubcolumnColumnMetaInfo::Node::Kind::SCALAR);
        exist_node->path = PathInData("key0");
        OlapReaderStatistics stats;
        Status sts = iter->add_stream(0, exist_node.get(), &column_reader_cache, &stats);
        EXPECT_TRUE(sts.ok());
        auto jsonb_type = std::make_shared<DataTypeJsonb>();
        // if node path is emtpy we will meet error
        auto variant_column_reader1 = assert_cast<VariantColumnReader*>(column_reader1.get());
        EXPECT_TRUE(variant_column_reader1 != nullptr);
        auto r = variant_column_reader1->get_subcolumns_meta_info()->get_leaves()[1];
        r->path = PathInData("");
        // if we clear the parts manually, we will meet error, but it can be handled, and should not happen
        r->path.parts.clear();
        sts = iter->add_stream(0, r.get(), &column_reader_cache, &stats);
        EXPECT_FALSE(sts.ok());
    }

    vectorized::DataTypeSerDe::FormatOptions options;
    auto tz = cctz::utc_time_zone();
    options.timezone = &tz;
    for (int i = 0; i < 1000; ++i) {
        std::string value;
        assert_cast<ColumnVariant*>(new_column_object.get())
                ->serialize_one_row_to_string(i, &value, options);

        EXPECT_EQ(value, inserted_jsonstr[i]);
    }

    std::vector<rowid_t> row_ids;
    for (int i = 0; i < 1000; ++i) {
        if (i % 7 == 0) {
            row_ids.push_back(i);
        }
    }
    new_column_object = ColumnVariant::create(3, false);
    st = it->read_by_rowids(row_ids.data(), row_ids.size(), new_column_object);
    EXPECT_TRUE(st.ok()) << st.msg();
    for (int i = 0; i < row_ids.size(); ++i) {
        std::string value;
        assert_cast<ColumnVariant*>(new_column_object.get())
                ->serialize_one_row_to_string(i, &value, options);
        EXPECT_EQ(value, inserted_jsonstr[row_ids[i]]);
    }

    auto read_to_column_object = [&](ColumnIteratorUPtr& it) {
        new_column_object = ColumnVariant::create(3);
        nrows = 1000;
        st = it->seek_to_ordinal(0);
        EXPECT_TRUE(st.ok()) << st.msg();
        st = it->next_batch(&nrows, new_column_object);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(stats.bytes_read > 0);
        EXPECT_EQ(nrows, 1000);
    };

    // 10. check sparse extract reader
    PathToBinaryColumnCacheUPtr sparse_column_cache =
            std::make_unique<std::unordered_map<std::string, BinaryColumnCacheSPtr>>();
    stats.bytes_read = 0;
    for (int i = 3; i < 10; ++i) {
        std::string key = ".key" + std::to_string(i);
        TabletColumn subcolumn_in_sparse;
        subcolumn_in_sparse.set_name(parent_column.name_lower_case() + key);
        subcolumn_in_sparse.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
        subcolumn_in_sparse.set_parent_unique_id(parent_column.unique_id());
        subcolumn_in_sparse.set_path_info(PathInData(parent_column.name_lower_case() + key));
        subcolumn_in_sparse.set_variant_max_subcolumns_count(
                parent_column.variant_max_subcolumns_count());
        subcolumn_in_sparse.set_is_nullable(true);

        ColumnIteratorUPtr it;
        st = variant_column_reader->new_iterator(&it, &subcolumn_in_sparse, &storage_read_opts,
                                                 &column_reader_cache, sparse_column_cache.get());
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(assert_cast<BinaryColumnExtractIterator*>(it.get()) != nullptr);
        st = it->init(column_iter_opts);
        EXPECT_TRUE(st.ok()) << st.msg();

        int64_t before_bytes_read = stats.bytes_read;
        read_to_column_object(it);
        // In bucketized mode, different keys may map to different buckets and trigger extra IO.
        if (variant_sparse_hash_shard_count <= 1 && before_bytes_read != 0) {
            EXPECT_EQ(stats.bytes_read, before_bytes_read);
        }

        for (int row = 0; row < 1000; ++row) {
            std::string value;
            assert_cast<ColumnVariant*>(new_column_object.get())
                    ->serialize_one_row_to_string(row, &value, options);
            if (inserted_jsonstr[row].find(key) != std::string::npos) {
                if (i % 2 == 0) {
                    EXPECT_EQ(value, "88");
                } else {
                    EXPECT_EQ(value, "str99");
                }
            }
        }
    }

    // 11. check leaf reader
    auto check_leaf_reader = [&]() {
        for (int i = 0; i < 3; ++i) {
            std::string key = ".key" + std::to_string(i);
            TabletColumn subcolumn;
            subcolumn.set_name(parent_column.name_lower_case() + key);
            subcolumn.set_type((FieldType)(int)footer.columns(i + 1).type());
            subcolumn.set_parent_unique_id(parent_column.unique_id());
            subcolumn.set_path_info(PathInData(parent_column.name_lower_case() + key));
            subcolumn.set_variant_max_subcolumns_count(
                    parent_column.variant_max_subcolumns_count());
            subcolumn.set_is_nullable(true);

            ColumnIteratorUPtr it;
            st = variant_column_reader->new_iterator(&it, &subcolumn, &storage_read_opts,
                                                     &column_reader_cache);
            EXPECT_TRUE(st.ok()) << st.msg();
            std::cout << "key " << key << std::endl;
            EXPECT_TRUE(dynamic_cast<FileColumnIterator*>(it.get()) != nullptr);
            st = it->init(column_iter_opts);
            EXPECT_TRUE(st.ok()) << st.msg();

            auto column_type = DataTypeFactory::instance().create_data_type(subcolumn, false);
            auto read_column = column_type->create_column();
            nrows = 1000;
            st = it->seek_to_ordinal(0);
            EXPECT_TRUE(st.ok()) << st.msg();
            st = it->next_batch(&nrows, read_column);
            EXPECT_TRUE(st.ok()) << st.msg();
            EXPECT_TRUE(stats.bytes_read > 0);

            for (int row = 0; row < 1000; ++row) {
                const std::string& value = column_type->to_string(*read_column, row);
                if (inserted_jsonstr[row].find(key) != std::string::npos) {
                    if (i % 2 == 0) {
                        EXPECT_EQ(value, "88");
                    } else {
                        EXPECT_EQ(value, "str99");
                    }
                }
            }
        }
    };
    check_leaf_reader();

    // 12. check empty
    TabletColumn subcolumn;
    subcolumn.set_name(parent_column.name_lower_case() + ".key10");
    subcolumn.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    subcolumn.set_parent_unique_id(parent_column.unique_id());
    subcolumn.set_path_info(PathInData(parent_column.name_lower_case() + ".key10"));
    subcolumn.set_is_nullable(true);
    ColumnIteratorUPtr it1;
    st = variant_column_reader->new_iterator(&it1, &subcolumn, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<DefaultValueColumnIterator*>(it1.get()) != nullptr);

    // 13. check statistics size == limit
    auto& variant_stats = variant_column_reader->_statistics;
    EXPECT_TRUE(variant_stats->sparse_column_non_null_size.size() <
                variant_column_reader->_variant_sparse_column_statistics_size);
    auto limit = variant_column_reader->_variant_sparse_column_statistics_size -
                 variant_stats->sparse_column_non_null_size.size();
    for (int i = 0; i < limit; ++i) {
        std::string key = parent_column.name_lower_case() + ".key10" + std::to_string(i);
        variant_stats->sparse_column_non_null_size[key] = 10000;
    }
    EXPECT_TRUE(variant_stats->sparse_column_non_null_size.size() ==
                variant_column_reader->_variant_sparse_column_statistics_size);
    EXPECT_TRUE(variant_column_reader->is_exceeded_sparse_column_limit());

    ColumnIteratorUPtr it2;
    st = variant_column_reader->new_iterator(&it2, &subcolumn, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<HierarchicalDataIterator*>(it2.get()) != nullptr);
    st = it2->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto check_empty_column = [&]() {
        for (int row = 0; row < 1000; ++row) {
            std::string value;
            assert_cast<ColumnVariant*>(new_column_object.get())
                    ->serialize_one_row_to_string(row, &value, options);

            EXPECT_EQ(value, "{}");
        }
    };

    read_to_column_object(it2);
    check_empty_column();

    // construct tablet schema for compaction
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_BASE_COMPACTION;
    storage_read_opts.tablet_schema = _tablet_schema;
    std::unordered_map<int32_t, TabletSchema::PathsSetInfo> uid_to_paths_set_info;
    TabletSchema::PathsSetInfo paths_set_info;
    paths_set_info.sub_path_set.insert("key0");
    paths_set_info.sub_path_set.insert("key3");
    paths_set_info.sub_path_set.insert("key4");
    paths_set_info.sparse_path_set.insert("key1");
    paths_set_info.sparse_path_set.insert("key2");
    paths_set_info.sparse_path_set.insert("key5");
    paths_set_info.sparse_path_set.insert("key6");
    paths_set_info.sparse_path_set.insert("key7");
    paths_set_info.sparse_path_set.insert("key8");
    paths_set_info.sparse_path_set.insert("key9");
    uid_to_paths_set_info[parent_column.unique_id()] = paths_set_info;
    _tablet_schema->set_path_set_info(std::move(uid_to_paths_set_info));

    // mock a subcolumn in compaction
    TabletColumn subcolumn_in_compaction;
    subcolumn_in_compaction.set_name(parent_column.name_lower_case() + ".key10");
    subcolumn_in_compaction.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    subcolumn_in_compaction.set_parent_unique_id(parent_column.unique_id());
    subcolumn_in_compaction.set_path_info(PathInData(parent_column.name_lower_case() + ".key10"));
    subcolumn_in_compaction.set_is_nullable(true);
    _tablet_schema->append_column(subcolumn_in_compaction);

    // 14. check compaction subcolumn reader
    check_leaf_reader();
    // 15. check compaction root reader
    ColumnIteratorUPtr it3;
    st = variant_column_reader->new_iterator(&it3, &parent_column, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<VariantRootColumnIterator*>(it3.get()) != nullptr);
    st = it3->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();
    // test VariantRootColumnIterator for next_batch and read_by_rowids
    {
        auto iter = assert_cast<VariantRootColumnIterator*>(it3.get());
        auto nullable_dt = std::make_shared<vectorized::DataTypeNullable>(
                std::make_shared<vectorized::DataTypeVariant>(3));
        MutableColumnPtr root_column_object = nullable_dt->create_column();
        nrows = 1000;
        st = iter->seek_to_ordinal(0);
        EXPECT_TRUE(st.ok()) << st.msg();
        st = iter->next_batch(&nrows, root_column_object);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(stats.bytes_read > 0);

        std::vector<rowid_t> row_ids1 = {0, 10, 100};
        root_column_object->clear();
        st = iter->read_by_rowids(row_ids1.data(), row_ids1.size(), root_column_object);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(root_column_object->size() == row_ids1.size());
        auto row_id = iter->get_current_ordinal();
        std::cout << "current row id: " << row_id << std::endl;
    }

    // 16. check compacton sparse column
    TabletColumn sparse_column =
            variant_sparse_hash_shard_count > 1
                    ? variant_util::create_sparse_shard_column(parent_column, 0)
                    : variant_util::create_sparse_column(parent_column);
    ColumnIteratorUPtr it4;
    st = variant_column_reader->new_iterator(&it4, &sparse_column, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<SparseColumnMergeIterator*>(it4.get()) != nullptr);
    st = it4->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();
    auto column_type = DataTypeFactory::instance().create_data_type(sparse_column, false);
    auto read_column = column_type->create_column();
    nrows = 1000;
    st = it4->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = it4->next_batch(&nrows, read_column);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(stats.bytes_read > 0);

    {
        // test SparseColumnMergeIterator seek_to_first
        auto iter = assert_cast<SparseColumnMergeIterator*>(it4.get());
        EXPECT_ANY_THROW(iter->get_current_ordinal());
        // and test read_by_rowids for 0 -> 1000
        std::vector<rowid_t> row_ids1;
        for (int i = 0; i < 1000; ++i) {
            row_ids1.push_back(i);
        }
        auto column_type1 = DataTypeFactory::instance().create_data_type(sparse_column, false);
        auto read_column1 = column_type1->create_column();
        st = iter->read_by_rowids(row_ids1.data(), row_ids1.size(), read_column1);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(read_column1->size() == row_ids1.size());
        // test _process_data_without_sparse_column
        std::cout << "_iter._src_subcolumn_map size : " << iter->_src_subcolumns_for_sparse.size()
                  << std::endl;
        std::cout << "_iter.root  " << iter->_src_subcolumns_for_sparse.empty() << std::endl;
        // fill with dst SparseMap
        MutableColumnPtr sparse_dst =
                ColumnMap::create(ColumnString::create(), ColumnString::create(),
                                  ColumnArray::ColumnOffsets::create());
        iter->_process_data_without_sparse_column(sparse_dst, 1);
        EXPECT_TRUE(sparse_dst->size() == 1);
    }
    //
    //    {
    //        // read with opt
    //        auto iter = assert_cast<SparseColumnMergeIterator*>(it4);
    //        StorageReadOptions storage_read_opts1;
    //        storage_read_opts1.io_ctx.reader_type = ReaderType::READER_QUERY;
    //        iter->_read_opts = &storage_read_opts1;
    //        auto read_column1 = column_type->create_column();
    //        st = iter->next_batch(&nrows, read_column1, nullptr);
    //        EXPECT_TRUE(st.ok()) << st.msg();
    //        EXPECT_TRUE(stats.bytes_read > 0);
    //        iter->_read_opts->io_ctx.reader_type = ReaderType::READER_BASE_COMPACTION;
    //        st = iter->next_batch(&nrows, read_column1, nullptr);
    //        EXPECT_TRUE(st.ok()) << st.msg();
    //    }

    for (int row = 0; row < 1000; ++row) {
        const std::string& value = column_type->to_string(*read_column, row);
        EXPECT_TRUE(value.find("key0") == std::string::npos)
                << "row: " << row << ", value: " << value;
        EXPECT_TRUE(value.find("key3") == std::string::npos)
                << "row: " << row << ", value: " << value;
        EXPECT_TRUE(value.find("key4") == std::string::npos)
                << "row: " << row << ", value: " << value;
    }

    // 17. check limit = 10000
    subcolumn.set_name(parent_column.name_lower_case() + ".key10");
    subcolumn.set_path_info(PathInData(parent_column.name_lower_case() + ".key10"));
    ColumnIteratorUPtr it5;
    st = variant_column_reader->new_iterator(&it5, &subcolumn, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<BinaryColumnExtractIterator*>(it5.get()) != nullptr);
    EXPECT_TRUE(it5->init(column_iter_opts).ok());

    {
        // test BinaryColumnExtractIterator seek_to_first
        auto iter = assert_cast<BinaryColumnExtractIterator*>(it5.get());
        EXPECT_TRUE(st.ok()) << st.msg();
        // and test read_by_rowids
        std::vector<rowid_t> row_ids1;
        for (int i = 0; i < 1000; ++i) {
            row_ids1.push_back(i);
        }
        MutableColumnPtr sparse_dst1 = ColumnVariant::create(3);
        st = iter->read_by_rowids(row_ids1.data(), row_ids1.size(), sparse_dst1);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(sparse_dst1->size() == row_ids1.size());
        // test to nullable column object
        std::cout << "test 2 " << std::endl;
        MutableColumnPtr sparse_dst2 =
                ColumnNullable::create(ColumnVariant::create(3), ColumnUInt8::create());
        st = iter->read_by_rowids(row_ids1.data(), row_ids1.size(), sparse_dst2);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(sparse_dst2->size() == row_ids1.size());
        std::cout << "test 3" << std::endl;
        MutableColumnPtr sparse_dst3 = ColumnVariant::create(3);
        size_t rs = 1000;
        bool has_null = false;
        st = iter->seek_to_ordinal(0);
        EXPECT_TRUE(st.ok()) << st.msg();
        st = iter->next_batch(&rs, sparse_dst3, &has_null);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(sparse_dst3->size() == row_ids1.size());
        // test _process_data_without_sparse_column
        // fill with dst SparseMap
        MutableColumnPtr sparse_dst =
                ColumnMap::create(ColumnString::create(), ColumnString::create(),
                                  ColumnArray::ColumnOffsets::create());
        iter->_process_data_without_sparse_column(sparse_dst, 1);
        EXPECT_TRUE(sparse_dst->size() == 1);
    }

    for (int i = 0; i < limit; ++i) {
        std::string key = parent_column.name_lower_case() + ".key10" + std::to_string(i);
        variant_stats->sparse_column_non_null_size.erase(key);
    }

    // 18. check compacton sparse extract column
    ColumnIteratorUPtr it6;
    subcolumn.set_name(parent_column.name_lower_case() + ".key3");
    subcolumn.set_path_info(PathInData(parent_column.name_lower_case() + ".key3"));
    st = variant_column_reader->new_iterator(&it6, &subcolumn, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<BinaryColumnExtractIterator*>(it6.get()) != nullptr);

    // 19. check compaction default column
    subcolumn.set_name(parent_column.name_lower_case() + ".key10");
    subcolumn.set_path_info(PathInData(parent_column.name_lower_case() + ".key10"));
    ColumnIteratorUPtr it7;
    st = variant_column_reader->new_iterator(&it7, &subcolumn, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<DefaultValueColumnIterator*>(it7.get()) != nullptr);
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_write_doc_and_read_hierarchical_doc) {
    constexpr int kRows = 200;
    constexpr int kDocBuckets = 2;

    // 1. create tablet_schema (enable doc mode, small shard count to keep footer small)
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 3, false, false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/true,
                     /*variant_doc_materialization_min_rows=*/100000,
                     /*variant_doc_hash_shard_count=*/kDocBuckets);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = false;
    _tablet_schema->set_external_segment_meta_used_default(external_segment_meta_used_default);
    tablet_meta->_tablet_id = 31000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create variant writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn parent_column = _tablet_schema->column(0);
    _init_column_meta(opts.meta, 0, parent_column, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &parent_column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write doc-value-only data into variant
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto column_object = (*std::move(block.get_by_position(0).column)).mutate();
    std::unordered_map<int, std::string> inserted_jsonstr;
    fill_variant_column_with_doc_value_only(column_object, kRows, &inserted_jsonstr);
    olap_data_convertor->add_column_data_convertor(parent_column);
    olap_data_convertor->set_source_content(&block, 0, kRows);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), kRows).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(kRows);

    // 6. validate footer contains doc snapshot bucket columns and per-bucket stats
    EXPECT_EQ(footer.columns_size(), 1 + kDocBuckets);
    for (int i = 1; i < footer.columns_size(); ++i) {
        const auto& col = footer.columns(i);
        EXPECT_TRUE(col.has_column_path_info());
        vectorized::PathInData path;
        path.from_protobuf(col.column_path_info());
        auto rel = path.copy_pop_front().get_path();
        EXPECT_TRUE(rel.find(DOC_VALUE_COLUMN_PATH) != std::string::npos) << rel;
        EXPECT_TRUE(col.has_variant_statistics());
        EXPECT_GT(col.variant_statistics().doc_value_column_non_null_size_size(), 0);
    }

    // 7. open a VariantColumnReader on this segment
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);
    EXPECT_TRUE(variant_column_reader->get_stats()->has_doc_column_non_null_size());
    EXPECT_TRUE(variant_column_reader->get_subcolumn_meta_by_path(PathInData("key0")) == nullptr);

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    // 8. Read root with QUERY reader type: should choose ReadKind::HIERARCHICAL_DOC
    StorageReadOptions storage_read_opts;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    OlapReaderStatistics stats;
    storage_read_opts.stats = &stats;
    ColumnIteratorUPtr it;
    st = variant_column_reader->new_iterator(&it, &parent_column, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(dynamic_cast<HierarchicalDataIterator*>(it.get()) != nullptr);
    EXPECT_EQ(stats.variant_doc_value_column_iter_count, 1);

    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = it->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    MutableColumnPtr dst =
            ColumnVariant::create(parent_column.variant_max_subcolumns_count(), false);
    size_t nrows = kRows;
    st = it->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = it->next_batch(&nrows, dst);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_EQ(nrows, kRows);

    vectorized::DataTypeSerDe::FormatOptions options;
    auto tz = cctz::utc_time_zone();
    options.timezone = &tz;
    for (int i = 0; i < kRows; ++i) {
        std::string value;
        assert_cast<ColumnVariant*>(dst.get())->serialize_one_row_to_string(i, &value, options);
        EXPECT_EQ(value, inserted_jsonstr[i]);
    }
}

TEST_F(VariantColumnWriterReaderTest,
       test_write_doc_materialized_by_min_rows_and_read_metadata_and_data) {
    constexpr int kRows = 200;
    constexpr int kDocBuckets = 2;

    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 3, false, false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/true,
                     /*variant_doc_materialization_min_rows=*/0,
                     /*variant_doc_hash_shard_count=*/kDocBuckets);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    _tablet_schema->set_external_segment_meta_used_default(false);
    tablet_meta->_tablet_id = 31002;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn parent_column = _tablet_schema->column(0);
    _init_column_meta(opts.meta, 0, parent_column, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &parent_column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());

    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto column_object = (*std::move(block.get_by_position(0).column)).mutate();
    std::unordered_map<int, std::string> inserted_jsonstr;
    fill_variant_column_with_doc_value_only(column_object, kRows, &inserted_jsonstr);
    olap_data_convertor->add_column_data_convertor(parent_column);
    olap_data_convertor->set_source_content(&block, 0, kRows);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), kRows).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(kRows);

    EXPECT_GT(footer.columns_size(), 1 + kDocBuckets);

    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);
    EXPECT_TRUE(variant_column_reader->get_subcolumn_meta_by_path(PathInData("key0")) != nullptr);

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    StorageReadOptions query_read_opts;
    query_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    OlapReaderStatistics query_stats;
    query_read_opts.stats = &query_stats;
    ColumnIteratorUPtr root_it;
    st = variant_column_reader->new_iterator(&root_it, &parent_column, &query_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(dynamic_cast<HierarchicalDataIterator*>(root_it.get()) != nullptr);

    ColumnIteratorOptions root_iter_opts;
    root_iter_opts.stats = &query_stats;
    root_iter_opts.file_reader = file_reader.get();
    st = root_it->init(root_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    vectorized::DataTypeSerDe::FormatOptions options;
    auto tz = cctz::utc_time_zone();
    options.timezone = &tz;
    MutableColumnPtr dst =
            ColumnVariant::create(parent_column.variant_max_subcolumns_count(), false);
    size_t nrows = kRows;
    st = root_it->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = root_it->next_batch(&nrows, dst);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_EQ(nrows, kRows);
    for (int i = 0; i < kRows; ++i) {
        std::string value;
        assert_cast<ColumnVariant*>(dst.get())->serialize_one_row_to_string(i, &value, options);
        EXPECT_EQ(value, inserted_jsonstr[i]);
    }

    StorageReadOptions compact_read_opts;
    compact_read_opts.io_ctx.reader_type = ReaderType::READER_BASE_COMPACTION;
    compact_read_opts.tablet_schema = _tablet_schema;
    OlapReaderStatistics compact_stats;
    compact_read_opts.stats = &compact_stats;
    TabletColumn doc_bucket_col =
            vectorized::variant_util::create_doc_value_column(parent_column, 0);
    ColumnIteratorUPtr bucket_it;
    st = variant_column_reader->new_iterator(&bucket_it, &doc_bucket_col, &compact_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(bucket_it != nullptr);

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_read_doc_compact_from_doc_value_bucket) {
    constexpr int kRows = 200;
    constexpr int kDocBuckets = 4;

    // 1. create tablet_schema (enable doc mode)
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 3, false, false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/true,
                     /*variant_doc_materialization_min_rows=*/0,
                     /*variant_doc_hash_shard_count=*/kDocBuckets);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    _tablet_schema->set_external_segment_meta_used_default(false);
    tablet_meta->_tablet_id = 32000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. write doc-value-only segment
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn parent_column = _tablet_schema->column(0);
    _init_column_meta(opts.meta, 0, parent_column, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &parent_column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());

    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto column_object = (*std::move(block.get_by_position(0).column)).mutate();
    std::unordered_map<int, std::string> inserted_jsonstr;
    fill_variant_column_with_doc_value_only(column_object, kRows, &inserted_jsonstr);
    olap_data_convertor->add_column_data_convertor(parent_column);
    olap_data_convertor->set_source_content(&block, 0, kRows);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), kRows).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(kRows);

    // 4. open reader
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    // 5. trigger flat-leaf planning by using compaction reader type + schema with extracted columns
    auto compaction_schema = std::make_shared<TabletSchema>();
    compaction_schema->init_from_pb(schema_pb);
    TabletColumn extracted;
    extracted.set_name(parent_column.name_lower_case() + ".dummy");
    extracted.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    extracted.set_parent_unique_id(parent_column.unique_id());
    extracted.set_path_info(PathInData(parent_column.name_lower_case() + ".dummy"));
    extracted.set_is_nullable(true);
    compaction_schema->append_column(extracted);

    StorageReadOptions storage_read_opts;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_BASE_COMPACTION;
    storage_read_opts.tablet_schema = compaction_schema;
    OlapReaderStatistics stats;
    storage_read_opts.stats = &stats;

    vectorized::DataTypeSerDe::FormatOptions options;
    auto tz = cctz::utc_time_zone();
    options.timezone = &tz;

    // 6. Read and validate each doc value bucket column: should choose ReadKind::DOC_COMPACT.
    for (int bucket = 0; bucket < kDocBuckets; ++bucket) {
        TabletColumn doc_bucket_col =
                vectorized::variant_util::create_doc_value_column(parent_column, bucket);
        ColumnIteratorUPtr it;
        st = variant_column_reader->new_iterator(&it, &doc_bucket_col, &storage_read_opts,
                                                 &column_reader_cache);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(dynamic_cast<segment_v2::VariantDocValueCompactIterator*>(it.get()) != nullptr);

        ColumnIteratorOptions column_iter_opts;
        column_iter_opts.stats = &stats;
        column_iter_opts.file_reader = file_reader.get();
        st = it->init(column_iter_opts);
        EXPECT_TRUE(st.ok()) << st.msg();

        MutableColumnPtr dst =
                ColumnVariant::create(parent_column.variant_max_subcolumns_count(), false);
        size_t nrows = kRows;
        st = it->seek_to_ordinal(0);
        EXPECT_TRUE(st.ok()) << st.msg();
        st = it->next_batch(&nrows, dst);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_EQ(nrows, kRows);

        for (int i = 0; i < kRows; ++i) {
            std::string value;
            assert_cast<ColumnVariant*>(dst.get())->serialize_one_row_to_string(i, &value, options);
            const std::string expected =
                    expected_doc_bucket_json_from_full(inserted_jsonstr[i], kDocBuckets, bucket);
            EXPECT_EQ(value, expected);
        }
    }
}

TEST_F(VariantColumnWriterReaderTest, test_write_doc_compact_writer_and_read_doc_compact) {
    constexpr int kRows = 200;
    constexpr int kDocBuckets = 4;
    constexpr int kBucket = 0;

    // 1. create tablet_schema: root variant is in doc mode; plus one extracted doc bucket column
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 3, false, false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/true,
                     /*variant_doc_materialization_min_rows=*/0,
                     /*variant_doc_hash_shard_count=*/kDocBuckets);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    TabletColumn parent_column = _tablet_schema->column(0);
    TabletColumn extracted_doc_bucket =
            vectorized::variant_util::create_doc_value_column(parent_column, kBucket);
    // This matches VariantCompactionUtil::get_extended_compaction_schema behavior:
    // extracted doc bucket columns are represented as VARIANT to trigger VariantDocCompactWriter.
    extracted_doc_bucket.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    extracted_doc_bucket.set_is_nullable(false);
    _tablet_schema->append_column(extracted_doc_bucket);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    _tablet_schema->set_external_segment_meta_used_default(false);
    tablet_meta->_tablet_id = 33000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column writers: root VariantColumnWriter + extracted VariantDocCompactWriter
    SegmentFooterPB footer;

    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    rowset_ctx.tablet_schema = _tablet_schema;

    ColumnWriterOptions root_opts;
    root_opts.meta = footer.add_columns();
    root_opts.compression_type = CompressionTypePB::LZ4;
    root_opts.file_writer = file_writer.get();
    root_opts.footer = &footer;
    root_opts.rowset_ctx = &rowset_ctx;
    _init_column_meta(root_opts.meta, 0, parent_column, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> root_writer;
    EXPECT_TRUE(
            ColumnWriter::create(root_opts, &parent_column, file_writer.get(), &root_writer).ok());
    EXPECT_TRUE(root_writer->init().ok());

    TabletColumn extracted_doc_bucket_col = _tablet_schema->column(1);
    ColumnWriterOptions doc_compact_opts = root_opts;
    doc_compact_opts.meta = footer.add_columns();
    _init_column_meta(doc_compact_opts.meta, 0, extracted_doc_bucket_col, CompressionTypePB::LZ4);
    std::unique_ptr<ColumnWriter> doc_compact_writer;
    EXPECT_TRUE(ColumnWriter::create(doc_compact_opts, &extracted_doc_bucket_col, file_writer.get(),
                                     &doc_compact_writer)
                        .ok());
    EXPECT_TRUE(doc_compact_writer->init().ok());

    // 5. build doc-value-only data:
    // - root column uses the full JSON (doc values only is enough for this test)
    // - extracted doc bucket column uses bucket-filtered JSON so that doc bucket data matches
    //   the bucket index expected by VariantDocCompactWriter.
    std::unordered_map<int, std::string> inserted_full_json;
    auto type_string = std::make_shared<vectorized::DataTypeString>();
    auto full_json_column = type_string->create_column();
    auto* full_strings = assert_cast<vectorized::ColumnString*>(full_json_column.get());
    VariantUtil::fill_string_column_with_test_data(full_strings, kRows, &inserted_full_json);

    std::unordered_map<int, std::string> expected_bucket_json;
    auto bucket_json_column = type_string->create_column();
    auto* bucket_strings = assert_cast<vectorized::ColumnString*>(bucket_json_column.get());
    for (int i = 0; i < kRows; ++i) {
        const std::string& full = inserted_full_json[i];
        std::string bucket_json = expected_doc_bucket_json_from_full(full, kDocBuckets, kBucket);
        expected_bucket_json.emplace(i, bucket_json);
        bucket_strings->insert_data(bucket_json.data(), bucket_json.size());
    }

    vectorized::ParseConfig config;
    config.enable_flatten_nested = false;
    config.parse_to = vectorized::ParseConfig::ParseTo::OnlyDocValueColumn;

    vectorized::MutableColumnPtr root_variant =
            vectorized::ColumnVariant::create(parent_column.variant_max_subcolumns_count(), false);
    variant_util::parse_json_to_variant(*root_variant, *full_strings, config);

    vectorized::MutableColumnPtr bucket_variant =
            vectorized::ColumnVariant::create(parent_column.variant_max_subcolumns_count(), false);
    variant_util::parse_json_to_variant(*bucket_variant, *bucket_strings, config);

    // 6. append and write
    {
        auto root_data = std::make_unique<vectorized::VariantColumnData>();
        root_data->column_data = root_variant.get();
        root_data->row_pos = 0;
        const auto* data = reinterpret_cast<const uint8_t*>(root_data.get());
        EXPECT_TRUE(root_writer->append_data(&data, kRows).ok());
    }
    {
        auto bucket_data = std::make_unique<vectorized::VariantColumnData>();
        bucket_data->column_data = bucket_variant.get();
        bucket_data->row_pos = 0;
        const auto* data = reinterpret_cast<const uint8_t*>(bucket_data.get());
        EXPECT_TRUE(doc_compact_writer->append_data(&data, kRows).ok());
    }

    EXPECT_TRUE(root_writer->finish().ok());
    EXPECT_TRUE(doc_compact_writer->finish().ok());
    EXPECT_TRUE(root_writer->write_data().ok());
    EXPECT_TRUE(doc_compact_writer->write_data().ok());
    EXPECT_TRUE(root_writer->write_ordinal_index().ok());
    EXPECT_TRUE(doc_compact_writer->write_ordinal_index().ok());
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(kRows);

    // 7. open reader and validate:
    // - doc bucket can be read via DOC_COMPACT iterator in flat-leaf compaction mode
    // - materialized leaf meta exists for at least one key in this bucket
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    bool checked_one_key = false;
    for (int j = 0; j < 10; ++j) {
        const std::string key = "key" + std::to_string(j);
        StringRef ref {key.data(), key.size()};
        if (vectorized::variant_util::variant_binary_shard_of(ref, kDocBuckets) ==
            static_cast<uint32_t>(kBucket)) {
            EXPECT_TRUE(variant_column_reader->get_subcolumn_meta_by_path(PathInData(key)) !=
                        nullptr);
            checked_one_key = true;
            break;
        }
    }
    EXPECT_TRUE(checked_one_key);

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);
    StorageReadOptions storage_read_opts;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_BASE_COMPACTION;
    storage_read_opts.tablet_schema = _tablet_schema;
    OlapReaderStatistics stats;
    storage_read_opts.stats = &stats;

    TabletColumn doc_bucket_map =
            vectorized::variant_util::create_doc_value_column(parent_column, kBucket);
    ColumnIteratorUPtr it;
    st = variant_column_reader->new_iterator(&it, &doc_bucket_map, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(dynamic_cast<segment_v2::VariantDocValueCompactIterator*>(it.get()) != nullptr);

    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = it->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    vectorized::DataTypeSerDe::FormatOptions options;
    auto tz = cctz::utc_time_zone();
    options.timezone = &tz;

    vectorized::MutableColumnPtr dst =
            ColumnVariant::create(parent_column.variant_max_subcolumns_count(), false);
    size_t nrows = kRows;
    st = it->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = it->next_batch(&nrows, dst);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_EQ(nrows, kRows);

    for (int i = 0; i < kRows; ++i) {
        std::string value;
        assert_cast<ColumnVariant*>(dst.get())->serialize_one_row_to_string(i, &value, options);
        EXPECT_EQ(value, expected_bucket_json[i]);
    }

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_doc_compact_sparse_write_array_gap) {
    constexpr int kRows = 2;
    constexpr int kDocBuckets = 1;
    constexpr int kBucket = 0;

    struct ConfigGuard {
        bool old_value;
        ~ConfigGuard() { config::enable_variant_doc_sparse_write_subcolumns = old_value; }
    };
    ConfigGuard guard {config::enable_variant_doc_sparse_write_subcolumns};
    config::enable_variant_doc_sparse_write_subcolumns = true;

    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 3, false, false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/true,
                     /*variant_doc_materialization_min_rows=*/0,
                     /*variant_doc_hash_shard_count=*/kDocBuckets);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    TabletColumn parent_column = _tablet_schema->column(0);
    TabletColumn extracted_doc_bucket =
            vectorized::variant_util::create_doc_value_column(parent_column, kBucket);
    extracted_doc_bucket.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    extracted_doc_bucket.set_is_nullable(false);
    _tablet_schema->append_column(extracted_doc_bucket);

    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    _tablet_schema->set_external_segment_meta_used_default(false);
    tablet_meta->_tablet_id = 33001;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    SegmentFooterPB footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    rowset_ctx.tablet_schema = _tablet_schema;

    TabletColumn extracted_doc_bucket_col = _tablet_schema->column(1);
    ColumnWriterOptions doc_compact_opts;
    doc_compact_opts.meta = footer.add_columns();
    doc_compact_opts.compression_type = CompressionTypePB::LZ4;
    doc_compact_opts.file_writer = file_writer.get();
    doc_compact_opts.footer = &footer;
    doc_compact_opts.rowset_ctx = &rowset_ctx;
    _init_column_meta(doc_compact_opts.meta, 0, extracted_doc_bucket_col, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> doc_compact_writer;
    EXPECT_TRUE(ColumnWriter::create(doc_compact_opts, &extracted_doc_bucket_col, file_writer.get(),
                                     &doc_compact_writer)
                        .ok());
    EXPECT_TRUE(doc_compact_writer->init().ok());

    auto type_string = std::make_shared<vectorized::DataTypeString>();
    auto json_column = type_string->create_column();
    auto* strings = assert_cast<vectorized::ColumnString*>(json_column.get());
    const std::string row0 = R"({"arr":[1,2]})";
    const std::string row1 = R"({})";
    strings->insert_data(row0.data(), row0.size());
    strings->insert_data(row1.data(), row1.size());

    vectorized::ParseConfig parse_cfg;
    parse_cfg.enable_flatten_nested = false;
    parse_cfg.parse_to = vectorized::ParseConfig::ParseTo::OnlyDocValueColumn;

    vectorized::MutableColumnPtr bucket_variant =
            vectorized::ColumnVariant::create(parent_column.variant_max_subcolumns_count(), false);
    variant_util::parse_json_to_variant(*bucket_variant, *strings, parse_cfg);

    auto bucket_data = std::make_unique<vectorized::VariantColumnData>();
    bucket_data->column_data = bucket_variant.get();
    bucket_data->row_pos = 0;
    const auto* data = reinterpret_cast<const uint8_t*>(bucket_data.get());
    EXPECT_TRUE(doc_compact_writer->append_data(&data, kRows).ok());

    EXPECT_TRUE(doc_compact_writer->finish().ok());
    EXPECT_TRUE(doc_compact_writer->write_data().ok());
    EXPECT_TRUE(doc_compact_writer->write_ordinal_index().ok());
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(kRows);

    bool found_arr = false;
    for (int i = 0; i < footer.columns_size(); ++i) {
        const auto& col = footer.columns(i);
        if (!col.has_column_path_info()) {
            continue;
        }
        vectorized::PathInData path;
        path.from_protobuf(col.column_path_info());
        if (path.copy_pop_front().get_path() == "arr") {
            EXPECT_EQ(col.type(), (int)FieldType::OLAP_FIELD_TYPE_ARRAY);
            EXPECT_TRUE(col.is_nullable());
            found_arr = true;
            break;
        }
    }
    EXPECT_TRUE(found_arr);

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_write_doc_sparse_write_array_gap_and_read) {
    constexpr int kRows = 2;
    constexpr int kDocBuckets = 1;

    struct ConfigGuard {
        bool old_value;
        ~ConfigGuard() { config::enable_variant_doc_sparse_write_subcolumns = old_value; }
    };
    ConfigGuard guard {config::enable_variant_doc_sparse_write_subcolumns};
    config::enable_variant_doc_sparse_write_subcolumns = true;

    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 3, false, false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/true,
                     /*variant_doc_materialization_min_rows=*/0,
                     /*variant_doc_hash_shard_count=*/kDocBuckets);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    _tablet_schema->set_external_segment_meta_used_default(false);
    tablet_meta->_tablet_id = 33002;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    SegmentFooterPB footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    rowset_ctx.tablet_schema = _tablet_schema;

    TabletColumn parent_column = _tablet_schema->column(0);
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    opts.rowset_ctx = &rowset_ctx;
    _init_column_meta(opts.meta, 0, parent_column, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &parent_column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());

    auto type_string = std::make_shared<vectorized::DataTypeString>();
    auto json_column = type_string->create_column();
    auto* strings = assert_cast<vectorized::ColumnString*>(json_column.get());
    std::unordered_map<int, std::string> inserted_json;
    inserted_json.emplace(0, R"({"arr":[1,2]})");
    inserted_json.emplace(1, R"({})");
    strings->insert_data(inserted_json[0].data(), inserted_json[0].size());
    strings->insert_data(inserted_json[1].data(), inserted_json[1].size());

    vectorized::ParseConfig parse_cfg;
    parse_cfg.enable_flatten_nested = false;
    parse_cfg.parse_to = vectorized::ParseConfig::ParseTo::OnlyDocValueColumn;

    vectorized::MutableColumnPtr variant_column =
            vectorized::ColumnVariant::create(parent_column.variant_max_subcolumns_count(), false);
    variant_util::parse_json_to_variant(*variant_column, *strings, parse_cfg);

    auto variant_data = std::make_unique<vectorized::VariantColumnData>();
    variant_data->column_data = variant_column.get();
    variant_data->row_pos = 0;
    const auto* data = reinterpret_cast<const uint8_t*>(variant_data.get());
    EXPECT_TRUE(writer->append_data(&data, kRows).ok());

    EXPECT_TRUE(writer->finish().ok());
    EXPECT_TRUE(writer->write_data().ok());
    EXPECT_TRUE(writer->write_ordinal_index().ok());
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(kRows);

    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    const auto* arr_node =
            variant_column_reader->get_subcolumn_meta_by_path(vectorized::PathInData("arr"));
    EXPECT_TRUE(arr_node != nullptr);

    bool found_arr_meta = false;
    for (int i = 0; i < footer.columns_size(); ++i) {
        const auto& col = footer.columns(i);
        if (!col.has_column_path_info()) {
            continue;
        }
        vectorized::PathInData path;
        path.from_protobuf(col.column_path_info());
        if (path.copy_pop_front().get_path() == "arr") {
            EXPECT_EQ(col.type(), (int)FieldType::OLAP_FIELD_TYPE_ARRAY);
            EXPECT_TRUE(col.is_nullable());
            found_arr_meta = true;
            break;
        }
    }
    EXPECT_TRUE(found_arr_meta);

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);
    StorageReadOptions storage_read_opts;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    OlapReaderStatistics stats;
    storage_read_opts.stats = &stats;
    ColumnIteratorUPtr it;
    st = variant_column_reader->new_iterator(&it, &parent_column, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();

    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = it->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    vectorized::MutableColumnPtr dst =
            ColumnVariant::create(parent_column.variant_max_subcolumns_count(), false);
    size_t nrows = kRows;
    st = it->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = it->next_batch(&nrows, dst);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_EQ(nrows, kRows);

    vectorized::DataTypeSerDe::FormatOptions options;
    auto tz = cctz::utc_time_zone();
    options.timezone = &tz;
    for (int i = 0; i < kRows; ++i) {
        std::string value;
        assert_cast<ColumnVariant*>(dst.get())->serialize_one_row_to_string(i, &value, options);
        if (i == 0) {
            EXPECT_EQ(value, "{\"arr\":[1, 2]}");
        } else {
            EXPECT_EQ(value, "{}");
        }
    }

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_write_data_advanced) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    int variant_sparse_hash_shard_count = rand() % 10 + 1;
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, false, false,
                     variant_sparse_hash_shard_count);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_external_segment_meta_used_default(external_segment_meta_used_default);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn column = _tablet_schema->column(0);
    _init_column_meta(opts.meta, 0, column, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto column_object = (*std::move(block.get_by_position(0).column)).mutate();
    std::unordered_map<int, std::string> inserted_jsonstr;
    auto path_with_size = VariantUtil::fill_object_column_with_nested_test_data(column_object, 1000,
                                                                                &inserted_jsonstr);
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), 1000).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    int expected_sparse_cols =
            variant_sparse_hash_shard_count > 1 ? variant_sparse_hash_shard_count : 1;
    EXPECT_EQ(footer.columns_size(), 1 + 10 + expected_sparse_cols);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);

    for (int i = 1; i < footer.columns_size() - 1; ++i) {
        auto column_met = footer.columns(i);
        check_column_meta(column_met, path_with_size);
    }
    check_sparse_column_meta(footer.columns(footer.columns_size() - 1), path_with_size);

    // 7. check variant reader
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    // 8. check statistics
    auto statistics = variant_column_reader->get_stats();
    for (const auto& [path, size] : statistics->subcolumns_non_null_size) {
        EXPECT_EQ(path_with_size[path], size);
    }
    for (const auto& [path, size] : statistics->sparse_column_non_null_size) {
        EXPECT_EQ(path_with_size[path], size);
    }

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    // 9. check root
    ColumnIteratorUPtr it;
    TabletColumn parent_column = _tablet_schema->column(0);
    StorageReadOptions storage_read_opts;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    OlapReaderStatistics stats;
    storage_read_opts.stats = &stats;
    st = variant_column_reader->new_iterator(&it, &parent_column, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<HierarchicalDataIterator*>(it.get()) != nullptr);
    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = it->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    MutableColumnPtr new_column_object = ColumnVariant::create(3);
    size_t nrows = 1000;
    st = it->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = it->next_batch(&nrows, new_column_object);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(stats.bytes_read > 0);

    vectorized::DataTypeSerDe::FormatOptions options;
    auto tz = cctz::utc_time_zone();
    options.timezone = &tz;
    for (int i = 0; i < 1000; ++i) {
        std::string value;
        assert_cast<ColumnVariant*>(new_column_object.get())
                ->serialize_one_row_to_string(i, &value, options);
        EXPECT_EQ(value, inserted_jsonstr[i]);
    }

    auto read_to_column_object = [&](ColumnIteratorUPtr& it) {
        new_column_object = ColumnVariant::create(10);
        nrows = 1000;
        st = it->seek_to_ordinal(0);
        EXPECT_TRUE(st.ok()) << st.msg();
        st = it->next_batch(&nrows, new_column_object);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(stats.bytes_read > 0);
        EXPECT_EQ(nrows, 1000);
    };

    auto check_key_stats = [&](const std::string& key_num) {
        std::string key = ".key" + key_num;
        TabletColumn subcolumn_in_nested;
        subcolumn_in_nested.set_name(parent_column.name_lower_case() + key);
        subcolumn_in_nested.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
        subcolumn_in_nested.set_parent_unique_id(parent_column.unique_id());
        subcolumn_in_nested.set_path_info(PathInData(parent_column.name_lower_case() + key));
        subcolumn_in_nested.set_variant_max_subcolumns_count(
                parent_column.variant_max_subcolumns_count());
        subcolumn_in_nested.set_is_nullable(true);

        ColumnIteratorUPtr it1;
        st = variant_column_reader->new_iterator(&it1, &subcolumn_in_nested, &storage_read_opts,
                                                 &column_reader_cache);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(assert_cast<HierarchicalDataIterator*>(it1.get()) != nullptr);
        st = it1->init(column_iter_opts);
        EXPECT_TRUE(st.ok()) << st.msg();
        read_to_column_object(it1);

        size_t key_count = 0;
        size_t key_nested_count = 0;
        for (int row = 0; row < 1000; ++row) {
            std::string value;
            assert_cast<ColumnVariant*>(new_column_object.get())
                    ->serialize_one_row_to_string(row, &value, options);
            if (value.find("nested" + key_num) != std::string::npos) {
                key_nested_count++;
            } else if (value.find("88") != std::string::npos) {
                key_count++;
            }
        }
        EXPECT_EQ(key_count, path_with_size["key" + key_num]);
        EXPECT_EQ(key_nested_count, path_with_size["key" + key_num + ".nested" + key_num]);
    };

    for (int i = 3; i < 10; ++i) {
        check_key_stats(std::to_string(i));
    }

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_write_sub_index) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "v", 2, false);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);
    TabletColumn& variant = _tablet_schema->mutable_column_by_uid(1);
    // add subcolumn
    TabletColumn subcolumn2;
    subcolumn2.set_name("v.b");
    subcolumn2.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    variant.add_sub_column(subcolumn2);
    variant.set_is_bf_column(true);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_external_segment_meta_used_default(external_segment_meta_used_default);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn column = _tablet_schema->column(0);
    _init_column_meta(opts.meta, 0, column, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto column_object = VariantUtil::construct_basic_varint_column();
    auto vw = assert_cast<VariantColumnWriter*>(writer.get());

    std::unique_ptr<VariantColumnData> _variant_column_data = std::make_unique<VariantColumnData>();
    // pass the real ColumnVariant pointer instead of address of shared_ptr
    _variant_column_data->column_data = column_object.get();
    _variant_column_data->row_pos = 0;
    const uint8_t* data = (const uint8_t*)_variant_column_data.get();
    st = vw->append_data(&data, 10);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_bloom_filter_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(10);

    // 6. check footer
    std::cout << footer.columns_size() << std::endl;
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);
}

TEST_F(VariantColumnWriterReaderTest, test_write_data_nullable) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    // make nullable tablet_column
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, true, true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_external_segment_meta_used_default(external_segment_meta_used_default);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    // nullable variant column
    TabletColumn column = _tablet_schema->column(0);
    _init_column_meta(opts.meta, 0, column, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    // here is nullable variant
    auto block = _tablet_schema->create_block();
    auto nullable_object = assert_cast<ColumnNullable*>(
            (*std::move(block.get_by_position(0).column)).mutate().get());
    std::unordered_map<int, std::string> inserted_jsonstr;
    auto column_object = nullable_object->get_nested_column_ptr();
    variant_util::PathToNoneNullValues path_with_size;
    for (int idx = 0; idx < 10; idx++) {
        nullable_object->insert_default(); // insert null
        auto res = VariantUtil::fill_object_column_with_test_data(column_object, 80,
                                                                  &inserted_jsonstr);
        path_with_size.insert(res.begin(), res.end());
        for (int j = 0; j < 80; ++j) {
            vectorized::Field f = vectorized::Field::create_field<TYPE_BOOLEAN>(UInt8(0));
            nullable_object->get_null_map_column_ptr()->insert(f);
        }
        nullable_object->insert_many_defaults(17);
        res = VariantUtil::fill_object_column_with_test_data(column_object, 2, &inserted_jsonstr);
        path_with_size.insert(res.begin(), res.end());
        for (int j = 0; j < 2; ++j) {
            vectorized::Field f = vectorized::Field::create_field<TYPE_BOOLEAN>(UInt8(0));
            nullable_object->get_null_map_column_ptr()->insert(f);
        }
    }
    // sort path_with_size with value
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    // variant do not implement append_nulls
    auto* vw = assert_cast<VariantColumnWriter*>(writer.get());
    const auto* ptr = (const uint8_t*)accessor->get_data();
    st = vw->append_nullable(accessor->get_nullmap(), &ptr, 1000);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    auto size = vw->estimate_buffer_size();
    std::cout << "size: " << size << std::endl;
    st = vw->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 12);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);

    for (int i = 1; i < footer.columns_size() - 1; ++i) {
        auto column_meta = footer.columns(i);
        EXPECT_TRUE(column_meta.has_column_path_info());
        auto path = std::make_shared<vectorized::PathInData>();
        EXPECT_EQ(column_meta.column_path_info().parrent_column_unique_id(), 1);
        EXPECT_GT(column_meta.none_null_size(), path_with_size[path->copy_pop_front().get_path()]);
    }
    check_sparse_column_meta(footer.columns(footer.columns_size() - 1), path_with_size);

    // 7. check variant reader
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    // 8. check statistics
    auto statistics = variant_column_reader->get_stats();
    for (const auto& [path, size] : statistics->subcolumns_non_null_size) {
        EXPECT_GT(size, path_with_size[path]);
    }
    for (const auto& [path, size] : statistics->sparse_column_non_null_size) {
        EXPECT_EQ(path_with_size[path], size);
    }

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    // 9. check root
    ColumnIteratorUPtr it;
    TabletColumn parent_column = _tablet_schema->column(0);
    StorageReadOptions storage_read_opts;
    OlapReaderStatistics stats;
    storage_read_opts.stats = &stats;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    st = variant_column_reader->new_iterator(&it, &parent_column, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<HierarchicalDataIterator*>(it.get()) != nullptr);
    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = it->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_write_data_nullable_without_finalize) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    // make nullable tablet_column
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, true, true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_external_segment_meta_used_default(external_segment_meta_used_default);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    // nullable variant column
    TabletColumn column = _tablet_schema->column(0);
    _init_column_meta(opts.meta, 0, column, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    // here is nullable variant
    auto block = _tablet_schema->create_block();
    auto nullable_object = assert_cast<ColumnNullable*>(
            (*std::move(block.get_by_position(0).column)).mutate().get());
    std::unordered_map<int, std::string> inserted_jsonstr;
    auto column_object = nullable_object->get_nested_column_ptr();
    variant_util::PathToNoneNullValues path_with_size;
    for (int idx = 0; idx < 10; idx++) {
        nullable_object->insert_default(); // insert null
        auto res = VariantUtil::fill_object_column_with_test_data(column_object, 80,
                                                                  &inserted_jsonstr);
        path_with_size.insert(res.begin(), res.end());
        for (int j = 0; j < 80; ++j) {
            vectorized::Field f = vectorized::Field::create_field<TYPE_BOOLEAN>(UInt8(0));
            nullable_object->get_null_map_column_ptr()->insert(f);
        }
        nullable_object->insert_many_defaults(17);
        res = VariantUtil::fill_object_column_with_test_data(column_object, 2, &inserted_jsonstr);
        path_with_size.insert(res.begin(), res.end());
        for (int j = 0; j < 2; ++j) {
            vectorized::Field f = vectorized::Field::create_field<TYPE_BOOLEAN>(UInt8(0));
            nullable_object->get_null_map_column_ptr()->insert(f);
        }
    }
    // sort path_with_size with value
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    // variant do not implement append_nulls
    auto* vw = assert_cast<VariantColumnWriter*>(writer.get());
    const auto* ptr = (const uint8_t*)accessor->get_data();
    st = vw->append_nullable(accessor->get_nullmap(), &ptr, 1000);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 12);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);
}

TEST_F(VariantColumnWriterReaderTest, test_write_bm_with_finalize) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    // make nullable tablet_column
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, true, true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_external_segment_meta_used_default(external_segment_meta_used_default);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    // nullable variant column
    TabletColumn column = _tablet_schema->column(0);
    _init_column_meta(opts.meta, 0, column, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    // here is nullable variant
    auto block = _tablet_schema->create_block();
    auto nullable_object = assert_cast<ColumnNullable*>(
            (*std::move(block.get_by_position(0).column)).mutate().get());
    std::unordered_map<int, std::string> inserted_jsonstr;
    auto column_object = nullable_object->get_nested_column_ptr();
    variant_util::PathToNoneNullValues path_with_size;
    for (int idx = 0; idx < 10; idx++) {
        nullable_object->insert_default(); // insert null
        auto res = VariantUtil::fill_object_column_with_test_data(column_object, 80,
                                                                  &inserted_jsonstr);
        path_with_size.insert(res.begin(), res.end());
        for (int j = 0; j < 80; ++j) {
            vectorized::Field f = vectorized::Field::create_field<TYPE_BOOLEAN>(UInt8(0));
            nullable_object->get_null_map_column_ptr()->insert(f);
        }
        nullable_object->insert_many_defaults(17);
        res = VariantUtil::fill_object_column_with_test_data(column_object, 2, &inserted_jsonstr);
        path_with_size.insert(res.begin(), res.end());
        for (int j = 0; j < 2; ++j) {
            vectorized::Field f = vectorized::Field::create_field<TYPE_BOOLEAN>(UInt8(0));
            nullable_object->get_null_map_column_ptr()->insert(f);
        }
    }
    // sort path_with_size with value
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    // variant do not implement append_nulls
    auto* vw = assert_cast<VariantColumnWriter*>(writer.get());
    const auto* ptr = (const uint8_t*)accessor->get_data();
    st = vw->append_nullable(accessor->get_nullmap(), &ptr, 1000);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->_impl->finalize();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 12);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);
}

TEST_F(VariantColumnWriterReaderTest, test_write_bf_with_finalize) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    // make nullable tablet_column
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, true, true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_external_segment_meta_used_default(external_segment_meta_used_default);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    // nullable variant column
    TabletColumn column = _tablet_schema->column(0);
    _init_column_meta(opts.meta, 0, column, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    // here is nullable variant
    auto block = _tablet_schema->create_block();
    auto nullable_object = assert_cast<ColumnNullable*>(
            (*std::move(block.get_by_position(0).column)).mutate().get());
    std::unordered_map<int, std::string> inserted_jsonstr;
    auto column_object = nullable_object->get_nested_column_ptr();
    variant_util::PathToNoneNullValues path_with_size;
    for (int idx = 0; idx < 10; idx++) {
        nullable_object->insert_default(); // insert null
        auto res = VariantUtil::fill_object_column_with_test_data(column_object, 80,
                                                                  &inserted_jsonstr);
        path_with_size.insert(res.begin(), res.end());
        for (int j = 0; j < 80; ++j) {
            vectorized::Field f = vectorized::Field::create_field<TYPE_BOOLEAN>(UInt8(0));
            nullable_object->get_null_map_column_ptr()->insert(f);
        }
        nullable_object->insert_many_defaults(17);
        res = VariantUtil::fill_object_column_with_test_data(column_object, 2, &inserted_jsonstr);
        path_with_size.insert(res.begin(), res.end());
        for (int j = 0; j < 2; ++j) {
            vectorized::Field f = vectorized::Field::create_field<TYPE_BOOLEAN>(UInt8(0));
            nullable_object->get_null_map_column_ptr()->insert(f);
        }
    }
    // sort path_with_size with value
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    // variant do not implement append_nulls
    auto* vw = assert_cast<VariantColumnWriter*>(writer.get());
    const auto* ptr = (const uint8_t*)accessor->get_data();
    st = vw->append_nullable(accessor->get_nullmap(), &ptr, 1000);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->_impl->finalize();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_bloom_filter_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 12);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);
}

TEST_F(VariantColumnWriterReaderTest, test_write_zm_with_finalize) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    // make nullable tablet_column
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, true, true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_external_segment_meta_used_default(external_segment_meta_used_default);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    // nullable variant column
    TabletColumn column = _tablet_schema->column(0);
    _init_column_meta(opts.meta, 0, column, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    // here is nullable variant
    auto block = _tablet_schema->create_block();
    auto nullable_object = assert_cast<ColumnNullable*>(
            (*std::move(block.get_by_position(0).column)).mutate().get());
    std::unordered_map<int, std::string> inserted_jsonstr;
    auto column_object = nullable_object->get_nested_column_ptr();
    variant_util::PathToNoneNullValues path_with_size;
    for (int idx = 0; idx < 10; idx++) {
        nullable_object->insert_default(); // insert null
        auto res = VariantUtil::fill_object_column_with_test_data(column_object, 80,
                                                                  &inserted_jsonstr);
        path_with_size.insert(res.begin(), res.end());
        for (int j = 0; j < 80; ++j) {
            vectorized::Field f = vectorized::Field::create_field<TYPE_BOOLEAN>(UInt8(0));
            nullable_object->get_null_map_column_ptr()->insert(f);
        }
        nullable_object->insert_many_defaults(17);
        res = VariantUtil::fill_object_column_with_test_data(column_object, 2, &inserted_jsonstr);
        path_with_size.insert(res.begin(), res.end());
        for (int j = 0; j < 2; ++j) {
            vectorized::Field f = vectorized::Field::create_field<TYPE_BOOLEAN>(UInt8(0));
            nullable_object->get_null_map_column_ptr()->insert(f);
        }
    }
    // sort path_with_size with value
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    // variant do not implement append_nulls
    auto* vw = assert_cast<VariantColumnWriter*>(writer.get());
    const auto* ptr = (const uint8_t*)accessor->get_data();
    st = vw->append_nullable(accessor->get_nullmap(), &ptr, 1000);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->_impl->finalize();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 12);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);
}

TEST_F(VariantColumnWriterReaderTest, test_write_inverted_with_finalize) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    // make nullable tablet_column
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, true, true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_external_segment_meta_used_default(external_segment_meta_used_default);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    // nullable variant column
    TabletColumn column = _tablet_schema->column(0);
    _init_column_meta(opts.meta, 0, column, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    // here is nullable variant
    auto block = _tablet_schema->create_block();
    auto nullable_object = assert_cast<ColumnNullable*>(
            (*std::move(block.get_by_position(0).column)).mutate().get());
    std::unordered_map<int, std::string> inserted_jsonstr;
    auto column_object = nullable_object->get_nested_column_ptr();
    variant_util::PathToNoneNullValues path_with_size;
    for (int idx = 0; idx < 10; idx++) {
        nullable_object->insert_default(); // insert null
        auto res = VariantUtil::fill_object_column_with_test_data(column_object, 80,
                                                                  &inserted_jsonstr);
        path_with_size.insert(res.begin(), res.end());
        for (int j = 0; j < 80; ++j) {
            vectorized::Field f = vectorized::Field::create_field<TYPE_BOOLEAN>(UInt8(0));
            nullable_object->get_null_map_column_ptr()->insert(f);
        }
        nullable_object->insert_many_defaults(17);
        res = VariantUtil::fill_object_column_with_test_data(column_object, 2, &inserted_jsonstr);
        path_with_size.insert(res.begin(), res.end());
        for (int j = 0; j < 2; ++j) {
            vectorized::Field f = vectorized::Field::create_field<TYPE_BOOLEAN>(UInt8(0));
            nullable_object->get_null_map_column_ptr()->insert(f);
        }
    }
    // sort path_with_size with value
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    // variant do not implement append_nulls
    auto* vw = assert_cast<VariantColumnWriter*>(writer.get());
    const auto* ptr = (const uint8_t*)accessor->get_data();
    st = vw->append_nullable(accessor->get_nullmap(), &ptr, 1000);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->_impl->finalize();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_inverted_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 12);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);
}

TEST_F(VariantColumnWriterReaderTest, test_no_sub_in_sparse_column) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1");
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_external_segment_meta_used_default(external_segment_meta_used_default);
    tablet_meta->_tablet_id = 10001;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn column = _tablet_schema->column(0);
    _init_column_meta(opts.meta, 0, column, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto column_object = (*std::move(block.get_by_position(0).column)).mutate();
    auto type_string = std::make_shared<vectorized::DataTypeString>();
    auto json_column = type_string->create_column();
    auto column_string = assert_cast<ColumnString*>(json_column.get());
    // for some test data in json string to insert variant column
    // make list for json string
    for (int i = 0; i < 1000; ++i) {
        std::string inserted_jsonstr =
                (R"({"a": {"b": )" + std::to_string(i) + R"(, "c": )" + std::to_string(i) +
                 R"(}, "d": )" + std::to_string(i) + R"(})");
        // insert json string to variant column
        column_string->insert_data(inserted_jsonstr.data(), inserted_jsonstr.size());
    }

    vectorized::ParseConfig config;
    config.enable_flatten_nested = false;
    variant_util::parse_json_to_variant(*column_object, *column_string, config);
    std::cout << "column_object size: "
              << assert_cast<ColumnVariant*>(column_object.get())->debug_string() << std::endl;

    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), 1000).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // check footer
    EXPECT_EQ(footer.columns_size(), 5);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);

    // 6. create reader
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    std::shared_ptr<ColumnReader> reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    auto variant_column_reader = assert_cast<VariantColumnReader*>(reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    // 7. test exist_in_sparse_column
    auto* variant_reader = assert_cast<VariantColumnReader*>(reader.get());
    vectorized::PathInData non_existent_path("non.existent.path");
    EXPECT_FALSE(variant_reader->exist_in_sparse_column(non_existent_path));

    // 8. test prefix_exist_in_sparse_column = true which means we have prefix in sparse column
    for (auto& path : variant_reader->get_stats()->sparse_column_non_null_size) {
        std::cout << "sparse_column_non_null_size path: " << path.first << ", size: " << path.second
                  << std::endl;
    }
    for (auto& path : variant_reader->get_stats()->subcolumns_non_null_size) {
        std::cout << "subcolumns_non_null_size path: " << path.first << ", size: " << path.second
                  << std::endl;
    }
    vectorized::PathInData prefix_path("a");
    EXPECT_FALSE(variant_reader->exist_in_sparse_column(prefix_path));

    // 9. test get_metadata_size with null statistics
    EXPECT_GT(variant_reader->get_metadata_size(), 0);

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    // 10. test hierarchical reader with empty statistics
    ColumnIteratorUPtr iterator;
    StorageReadOptions read_opts;
    OlapReaderStatistics stats;
    read_opts.stats = &stats;
    st = variant_reader->new_iterator(&iterator, &column, &read_opts, &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(iterator != nullptr);
}

TEST_F(VariantColumnWriterReaderTest, test_prefix_in_sub_and_sparse) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1");
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_external_segment_meta_used_default(external_segment_meta_used_default);
    tablet_meta->_tablet_id = 10001;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn column = _tablet_schema->column(0);
    _init_column_meta(opts.meta, 0, column, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto column_object = (*std::move(block.get_by_position(0).column)).mutate();
    auto type_string = std::make_shared<vectorized::DataTypeString>();
    auto json_column = type_string->create_column();
    auto column_string = assert_cast<ColumnString*>(json_column.get());
    // for some test data in json string to insert variant column
    // insert some test data to json string
    for (int i = 0; i < 1000; ++i) {
        std::string inserted_jsonstr =
                (R"({"a": {"b": )" + std::to_string(i) + R"(, "c": )" + std::to_string(i) +
                 R"(}, "d": )" + std::to_string(i) + R"(})");
        // add some rand key for sparse column with 'a.b' prefix : {"a": {"b": 1, "c": 1, "e": 1}, "d": 1}
        if (i % 17 == 0) {
            inserted_jsonstr =
                    (R"({"a": {"b": )" + std::to_string(i) + R"(, "c": )" + std::to_string(i) +
                     R"(, "e": )" + std::to_string(i) + R"(}, "d": )" + std::to_string(i) + R"(})");
        }
        // add some rand key for spare column without prefix: {"a": {"b": 1, "c": 1}, "d": 1, "e": 1}
        if (i % 177 == 0) {
            inserted_jsonstr =
                    (R"({"a": {"b": )" + std::to_string(i) + R"(, "c": )" + std::to_string(i) +
                     R"(}, "d": )" + std::to_string(i) + R"(, "e": )" + std::to_string(i) + R"(})");
        }
        // insert json string to variant column
        column_string->insert_data(inserted_jsonstr.data(), inserted_jsonstr.size());
    }

    vectorized::ParseConfig config;
    config.enable_flatten_nested = false;
    variant_util::parse_json_to_variant(*column_object, *column_string, config);
    std::cout << "column_object size: "
              << assert_cast<ColumnVariant*>(column_object.get())->debug_string() << std::endl;

    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), 1000).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // check footer
    EXPECT_EQ(footer.columns_size(), 5);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);

    // 6. create reader
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    std::shared_ptr<ColumnReader> reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    auto variant_column_reader = assert_cast<VariantColumnReader*>(reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    // 7. test exist_in_sparse_column
    auto* variant_reader = assert_cast<VariantColumnReader*>(reader.get());
    vectorized::PathInData non_existent_path("non.existent.path");
    EXPECT_FALSE(variant_reader->exist_in_sparse_column(non_existent_path));

    // 8. test prefix_exist_in_sparse_column = true which means we have prefix in sparse column
    for (auto& path : variant_reader->get_stats()->sparse_column_non_null_size) {
        std::cout << "sparse_column_non_null_size path: " << path.first << ", size: " << path.second
                  << std::endl;
    }
    for (auto& path : variant_reader->get_stats()->subcolumns_non_null_size) {
        std::cout << "subcolumns_non_null_size path: " << path.first << ", size: " << path.second
                  << std::endl;
    }
    vectorized::PathInData prefix_path("a");
    EXPECT_TRUE(variant_reader->exist_in_sparse_column(prefix_path));

    // 9. test get_metadata_size with null statistics
    EXPECT_GT(variant_reader->get_metadata_size(), 0);

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    // 10. test hierarchical reader with empty statistics
    ColumnIteratorUPtr iterator;
    StorageReadOptions read_opts;
    OlapReaderStatistics stats;
    read_opts.stats = &stats;
    st = variant_reader->new_iterator(&iterator, &column, &read_opts, &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(iterator != nullptr);
}

void test_write_variant_column(StorageEngine* _engine_ref, std::string _absolute_dir,
                               std::string& file_path, SegmentFooterPB& footer,
                               std::shared_ptr<TabletSchema> _tablet_schema,
                               bool nullable = false) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "v", 3, false, nullable);
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_external_segment_meta_used_default(external_segment_meta_used_default);
    tablet_meta->_tablet_id = 10000;
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_absolute_dir).ok());
    std::unique_ptr<DataDir> _data_dir = std::make_unique<DataDir>(*_engine_ref, _absolute_dir);
    static_cast<void>(_data_dir->update_capacity());
    Status st1 = _data_dir->init(true);
    EXPECT_TRUE(st1.ok()) << st1.msg();
    std::shared_ptr<Tablet> _tablet =
            std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn tablet_column = _tablet_schema->column(0);
    _init_column_meta(opts.meta, 0, tablet_column, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &tablet_column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. make test data for column_object
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto column_object = (*std::move(block.get_by_position(0).column)).mutate();
    VariantUtil::VariantStringCreator simple_column_object = [](ColumnString* column_string,
                                                                size_t size) {
        // for some test data in json string to insert variant column
        // insert some test data to json string: {"a" : {"b" : [{"c" : {"d" : i, "e": "a@b"}}]}, "x": "y"}}
        for (int i = 0; i < size; ++i) {
            std::string inserted_jsonstr = (R"({"a" : {"b" : [{"c" : {"d" : )" + std::to_string(i) +
                                            R"(, "e": "a@b"}}]}, "x": "y"})");
            // insert json string to variant column
            column_string->insert_data(inserted_jsonstr.data(), inserted_jsonstr.size());
        }
    };
    if (nullable) {
        auto null_object = assert_cast<ColumnNullable*>(column_object.get());
        auto _object = null_object->get_nested_column_ptr();
        null_object->get_null_map_column_ptr()->insert_many_defaults(1000);
        VariantUtil::fill_variant_column(_object, 1000, 1, true, &simple_column_object);
    } else {
        VariantUtil::fill_variant_column(column_object, 1000, 1, true, &simple_column_object);
    }
    EXPECT_TRUE(column_object->size() == 1000);
    olap_data_convertor->add_column_data_convertor(tablet_column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), 1000).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 5);
    auto column_m = footer.columns(0);
    EXPECT_EQ(column_m.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);

    for (int i = 1; i < footer.columns_size() - 1; ++i) {
        auto column_meta = footer.columns(i);
        EXPECT_TRUE(column_meta.has_column_path_info());
        auto path = std::make_shared<vectorized::PathInData>();
        path->from_protobuf(column_meta.column_path_info());
        EXPECT_EQ(column_meta.column_path_info().parrent_column_unique_id(), 1);
    }
}

TEST_F(VariantColumnWriterReaderTest, test_nested_subcolumn) {
    // write data
    std::string absolute_dir = _current_dir + std::string("/ut_dir/variant_test_nested_subcolumn");
    // declare file_path and footer
    std::string file_path;
    SegmentFooterPB footer;
    std::shared_ptr<TabletSchema> _tablet_schema = std::make_shared<TabletSchema>();
    test_write_variant_column(_engine_ref, absolute_dir, file_path, footer, _tablet_schema);
    // reader data
    // check variant reader
    io::FileReaderSPtr file_reader;
    Status st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);
    // test read situation for compaction with should flat all sub column
    EXPECT_FALSE(variant_column_reader->get_subcolumns_meta_info()->empty());

    // create a nested column array<struct> which not exists in subcolumn
    TabletColumn struct_column;
    struct_column.set_name("b");
    struct_column.set_type(FieldType::OLAP_FIELD_TYPE_STRUCT);
    TabletColumn int_column;
    int_column.set_name("i");
    int_column.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    TabletColumn string_column;
    string_column.set_name("s");
    string_column.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    struct_column.add_sub_column(int_column);
    struct_column.add_sub_column(string_column);

    TabletColumn target_column;
    target_column.set_name("a");
    target_column.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
    target_column.add_sub_column(struct_column);

    // {"a" : {"b" : [{"i" : 1, "s": "abs"}]}}
    // DefaultNestedColumnIterator with sibling_iter
    PathInDataBuilder builder;
    builder.append("v", false); // First part is variant
    builder.append("a", false); //  Second part is struct
    builder.append("b", false); // Third part is struct
    builder.append("i", true); // Fourth part is int as array for b.i array<int> , b.s array<string>
    // this will be a.b.i and a.b.s
    PathInData path = builder.build();
    EXPECT_TRUE(path.has_nested_part());
    target_column.set_path_info(path);
    EXPECT_TRUE(target_column.is_nested_subcolumn())
            << target_column._column_path->has_nested_part();

    StorageReadOptions storageReadOptions;
    storageReadOptions.io_ctx.reader_type = ReaderType::READER_CUMULATIVE_COMPACTION;

    // DefaultNestedColumnIterator with nullptr parameter
    PathInDataBuilder builder1;
    builder1.append("v", false); // First part is variant
    builder1.append("v", false); // First part is variant
    builder1.append("a", false); //  Second part is struct
    builder1.append("b", false); // Third part is struct
    builder1.append("i",
                    true); // Fourth part is int as array for b.i array<int> , b.s array<string>
    // this will be a.b.i and a.b.s
    PathInData path1 = builder1.build();
    EXPECT_TRUE(path1.has_nested_part());
    target_column.set_path_info(path1);
    EXPECT_TRUE(target_column.is_nested_subcolumn())
            << target_column._column_path->has_nested_part();
}

TEST_F(VariantColumnWriterReaderTest, test_nested_iter) {
    // write data
    std::string absolute_dir = _current_dir + std::string("/ut_dir/variant_test_nested_iter");
    // declare file_path and footer
    std::string file_path;
    SegmentFooterPB footer;
    std::shared_ptr<TabletSchema> _tablet_schema = std::make_shared<TabletSchema>();
    test_write_variant_column(_engine_ref, absolute_dir, file_path, footer, _tablet_schema);
    // reader data
    // check variant reader
    io::FileReaderSPtr file_reader;
    Status st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);
    // test read situation for compaction with should flat all sub column
    EXPECT_FALSE(variant_column_reader->get_subcolumns_meta_info()->empty());

    StorageReadOptions storageReadOptions;
    storageReadOptions.io_ctx.reader_type = ReaderType::READER_QUERY;
    OlapReaderStatistics stats;
    storageReadOptions.stats = &stats;

    ColumnIteratorUPtr nested_column_iter;
    st = variant_column_reader->new_iterator(&nested_column_iter, &_tablet_schema->column(0),
                                             &storageReadOptions, &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    // this is nested column root
    auto* nested_iter = assert_cast<HierarchicalDataIterator*>(nested_column_iter.get());
    EXPECT_TRUE(nested_iter != nullptr);
    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = nested_iter->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();
    // fill with nullable ColumnVariant target
    MutableColumnPtr new_column_object1 = ColumnVariant::create(3);
    MutableColumnPtr null_object =
            ColumnNullable::create(new_column_object1->assume_mutable(), ColumnUInt8::create());
    size_t n = 1000;
    st = nested_iter->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    bool has_null = false;
    st = nested_iter->next_batch(&n, null_object, &has_null);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(stats.bytes_read > 0);
    {
        // fill with nullable ColumnVariant target
        MutableColumnPtr new_column_object12 = ColumnVariant::create(3);
        MutableColumnPtr null_object12 = ColumnNullable::create(
                new_column_object12->assume_mutable(), ColumnUInt8::create());
        st = nested_iter->seek_to_ordinal(0);
        EXPECT_TRUE(st.ok()) << st.msg();
        st = nested_iter->next_batch(&n, null_object12, &has_null);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(stats.bytes_read > 0);
    }
    // read dst is nullable column object
    {
        ColumnIteratorUPtr nested_column_iter1;
        TabletColumn target_column;
        target_column.set_name("a");
        target_column.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
        // {"a" : {"b" : [{"i" : 1, "s": "abs"}]}}
        PathInDataBuilder builder;
        builder.append("v", false); // First part is variant
        builder.append("a", false); //  Second part is struct
        builder.append("b", false); // Third part is struct
        PathInData path = builder.build();
        target_column.set_path_info(path);

        st = variant_column_reader->new_iterator(&nested_column_iter1, &target_column,
                                                 &storageReadOptions, &column_reader_cache);
        EXPECT_TRUE(st.ok()) << st.msg();
        // this is nested column root
        auto* nested_iter2 = assert_cast<HierarchicalDataIterator*>(nested_column_iter1.get());
        EXPECT_TRUE(nested_iter2 != nullptr);
        st = nested_iter2->init(column_iter_opts);
        EXPECT_TRUE(st.ok()) << st.msg();
        // fill with nullable ColumnVariant target
        MutableColumnPtr new_column_object2 = ColumnVariant::create(3);
        MutableColumnPtr null_object2 =
                ColumnNullable::create(new_column_object2->assume_mutable(), ColumnUInt8::create());
        size_t nrows = 1000;
        st = nested_iter2->seek_to_ordinal(0);
        EXPECT_TRUE(st.ok()) << st.msg();
        st = nested_iter2->next_batch(&nrows, null_object2, &has_null);
        EXPECT_TRUE(st.ok()) << st.msg();
    }
    // test _process_with_nested_column for offsets not equals
    {
        ColumnIteratorUPtr nested_column_iter1;
        TabletColumn target_column;
        target_column.set_name("a");
        target_column.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
        // {"a" : {"b" : [{"i" : 1, "s": "abs"}]}}
        PathInDataBuilder builder;
        builder.append("v", false); // First part is variant
        builder.append("a", false); //  Second part is struct
        builder.append("b", false); // Third part is struct
        PathInData path = builder.build();
        target_column.set_path_info(path);

        st = variant_column_reader->new_iterator(&nested_column_iter1, &target_column,
                                                 &storageReadOptions, &column_reader_cache);
        EXPECT_TRUE(st.ok()) << st.msg();
        // this is nested column root
        auto* nested_iter2 = assert_cast<HierarchicalDataIterator*>(nested_column_iter1.get());
        EXPECT_TRUE(nested_iter2 != nullptr);
        st = nested_iter2->init(column_iter_opts);
        EXPECT_TRUE(st.ok()) << st.msg();
        st = nested_iter2->seek_to_ordinal(0);
        EXPECT_TRUE(st.ok()) << st.msg();
        std::map<vectorized::PathInData, PathsWithColumnAndType> nested_subcolumns;
        // fill nested_subcolumns with different offset of array
        vectorized::PathInData parent_path("a");
        vectorized::PathInData relative_path("b");
        DataTypePtr base_data_type = std::make_shared<vectorized::DataTypeArray>(
                std::make_shared<vectorized::DataTypeString>());
        auto base_column = base_data_type->create_column();
        PathWithColumnAndType base = {relative_path, base_column->get_ptr(), base_data_type};
        nested_subcolumns[parent_path].emplace_back(base);
        DataTypePtr second_data_type = std::make_shared<vectorized::DataTypeString>();
        auto second_column = second_data_type->create_column();
        PathWithColumnAndType second = {relative_path, second_column->get_ptr(), second_data_type};
        nested_subcolumns[parent_path].emplace_back(second);
        // test _process_with_nested_column with different type
        // init container which is ColumnVariant
        MutableColumnPtr nested_column_object = ColumnVariant::create(3);
        auto& container_variant = assert_cast<ColumnVariant&>(*nested_column_object);
        st = nested_iter2->_process_nested_columns(container_variant, nested_subcolumns, n);
        std::cout << st.msg() << std::endl;
        EXPECT_FALSE(st.ok()) << st.msg();

        // then delete second
        nested_subcolumns[parent_path].pop_back();
        // add new nested column with array but not same offset
        auto column_a = base_data_type->create_column();
        column_a->insert_default();
        PathWithColumnAndType new_column = {relative_path, column_a->get_ptr(), base_data_type};
        nested_subcolumns[parent_path].emplace_back(new_column);
        // test _process_with_nested_column with different offset
        st = nested_iter2->_process_nested_columns(container_variant, nested_subcolumns, n);
        std::cout << st.msg() << std::endl;
        EXPECT_FALSE(st.ok()) << st.msg();
    }
}

TEST_F(VariantColumnWriterReaderTest, test_nested_iter_nullable) {
    // write data
    std::string absolute_dir = _current_dir + std::string("/ut_dir/variant_test_nested_iter");
    // declare file_path and footer
    std::string file_path;
    SegmentFooterPB footer;
    std::shared_ptr<TabletSchema> _tablet_schema = std::make_shared<TabletSchema>();
    test_write_variant_column(_engine_ref, absolute_dir, file_path, footer, _tablet_schema, true);
    // reader data
    // check variant reader
    io::FileReaderSPtr file_reader;
    Status st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);
    // test read situation for compaction with should flat all sub column
    EXPECT_FALSE(variant_column_reader->get_subcolumns_meta_info()->empty());

    StorageReadOptions storageReadOptions;
    storageReadOptions.io_ctx.reader_type = ReaderType::READER_QUERY;
    OlapReaderStatistics stats;
    storageReadOptions.stats = &stats;

    ColumnIteratorUPtr nested_column_iter;
    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    st = variant_column_reader->new_iterator(&nested_column_iter, &_tablet_schema->column(0),
                                             &storageReadOptions, &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    // this is nested column root
    auto* nested_iter = assert_cast<HierarchicalDataIterator*>(nested_column_iter.get());
    EXPECT_TRUE(nested_iter != nullptr);
    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = nested_iter->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();
    // fill with nullable ColumnVariant target
    MutableColumnPtr new_column_object1 = ColumnVariant::create(3);
    MutableColumnPtr null_object =
            ColumnNullable::create(new_column_object1->assume_mutable(), ColumnUInt8::create());
    size_t nrows = 1000;
    st = nested_iter->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    bool has_null = false;
    st = nested_iter->next_batch(&nrows, null_object, &has_null);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(stats.bytes_read > 0);
}

TEST_F(VariantColumnWriterReaderTest, test_read_with_checksum) {
    auto fill_string_column_with_test_data =
            [&](auto& column_string, int size,
                std::unordered_map<int, std::string>* inserted_jsonstr,
                variant_util::PathToNoneNullValues* path_with_size) {
                for (int i = 0; i < size; ++i) {
                    std::string jsonstr;
                    if (i % 2 == 0) {
                        jsonstr = R"({"b" : 3})";
                        (*path_with_size)["b"] += 1;
                    } else {
                        jsonstr = R"({"b" : {"c" : 5}})";
                        (*path_with_size)["b.c"] += 1;
                    }
                    inserted_jsonstr->emplace(i, jsonstr);
                    column_string->insert_data(jsonstr.c_str(), jsonstr.size());
                }
            };

    auto fill_object_column_with_test_data =
            [&](auto& column_object, int size,
                std::unordered_map<int, std::string>* inserted_jsonstr,
                variant_util::PathToNoneNullValues* path_with_size) {
                auto type_string = std::make_shared<vectorized::DataTypeString>();
                auto column = type_string->create_column();
                auto* column_string = assert_cast<ColumnString*>(column.get());
                fill_string_column_with_test_data(column_string, size, inserted_jsonstr,
                                                  path_with_size);
                vectorized::ParseConfig config;
                config.enable_flatten_nested = false;
                variant_util::parse_json_to_variant(*column_object, *column_string, config);
            };

    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1");
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_external_segment_meta_used_default(external_segment_meta_used_default);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());

    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn column = _tablet_schema->column(0);
    _init_column_meta(opts.meta, 0, column, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto column_object = (*std::move(block.get_by_position(0).column)).mutate();
    variant_util::PathToNoneNullValues path_with_size;
    std::unordered_map<int, std::string> inserted_jsonstr;
    fill_object_column_with_test_data(column_object, 1000, &inserted_jsonstr, &path_with_size);

    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), 1000).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 4);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);

    for (int i = 1; i < footer.columns_size() - 1; ++i) {
        auto column_met = footer.columns(i);
        check_column_meta(column_met, path_with_size);
    }
    check_sparse_column_meta(footer.columns(footer.columns_size() - 1), path_with_size);

    // 7. check variant reader
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    const auto* subcolumn_meta = variant_column_reader->get_subcolumn_meta_by_path(PathInData("b"));
    EXPECT_TRUE(subcolumn_meta != nullptr);
    subcolumn_meta = variant_column_reader->get_subcolumn_meta_by_path(PathInData("b.c"));
    EXPECT_TRUE(subcolumn_meta != nullptr);

    TabletColumn parent_column = _tablet_schema->column(0);
    StorageReadOptions storage_read_opts;

    storage_read_opts.tablet_schema = _tablet_schema;

    TabletColumn subcolumn;
    subcolumn.set_name(parent_column.name_lower_case() + ".b");
    subcolumn.set_type((FieldType)(int)footer.columns(1).type());
    subcolumn.set_parent_unique_id(parent_column.unique_id());
    subcolumn.set_path_info(PathInData(parent_column.name_lower_case() + ".b"));
    subcolumn.set_variant_max_subcolumns_count(parent_column.variant_max_subcolumns_count());
    subcolumn.set_is_nullable(true);
    _tablet_schema->append_column(subcolumn);
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    OlapReaderStatistics stats;
    storage_read_opts.stats = &stats;
    ColumnIteratorUPtr hierarchical_it;
    st = variant_column_reader->new_iterator(&hierarchical_it, &subcolumn, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(dynamic_cast<HierarchicalDataIterator*>(hierarchical_it.get()) != nullptr);

    storage_read_opts.io_ctx.reader_type = ReaderType::READER_CHECKSUM;
    ColumnIteratorUPtr it;
    st = variant_column_reader->new_iterator(&it, &subcolumn, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(dynamic_cast<FileColumnIterator*>(it.get()) != nullptr);
    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = it->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto column_type = DataTypeFactory::instance().create_data_type(subcolumn, true);
    auto read_column = column_type->create_column();
    size_t nrows = 1000;
    st = it->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = it->next_batch(&nrows, read_column);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(stats.bytes_read > 0);

    for (int row = 0; row < 1000; ++row) {
        const std::string& value = column_type->to_string(*read_column, row);
        if (row % 2 == 0) {
            EXPECT_EQ(value, "3");
        }
    }
}

// Concurrently trigger external meta loading and subcolumn meta access to guard against
// data races between `load_external_meta_once` writer and readers like
// `get_subcolumn_meta_by_path` / `get_metadata_size`. This roughly simulates the
// production crash stack where one thread was loading external meta while another
// thread was reading from `_subcolumns_meta_info`.
TEST_F(VariantColumnWriterReaderTest, test_concurrent_load_external_meta_and_get_subcolumn_meta) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1");
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet with external segment meta explicitly enabled so that
    // VariantColumnReader builds a VariantExternalMetaReader.
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = true;
    _tablet_schema->set_external_segment_meta_used_default(external_segment_meta_used_default);
    tablet_meta->_tablet_id = 20000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());

    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn column = _tablet_schema->column(0);
    _init_column_meta(opts.meta, 0, column, CompressionTypePB::LZ4);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write a small amount of data to build some subcolumns
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto column_object = (*std::move(block.get_by_position(0).column)).mutate();
    std::unordered_map<int, std::string> inserted_jsonstr;
    auto path_with_size =
            VariantUtil::fill_object_column_with_test_data(column_object, 200, &inserted_jsonstr);
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 200);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), 200).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(200);

    // 6. open a VariantColumnReader on this segment
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    // 7. run load_external_meta_once and subcolumn meta access concurrently.
    const int rounds = 200;
    std::atomic<bool> failed {false};
    Status writer_status = Status::OK();

    std::thread writer_thread([&] {
        for (int i = 0; i < rounds && !failed.load(); ++i) {
            Status s = variant_column_reader->load_external_meta_once();
            if (!s.ok()) {
                writer_status = s;
                failed.store(true);
                break;
            }
        }
    });

    std::thread reader_thread([&] {
        for (int i = 0; i < rounds && !failed.load(); ++i) {
            // Access subcolumn meta and metadata size repeatedly.
            auto* node = variant_column_reader->get_subcolumn_meta_by_path(PathInData("key0"));
            (void)node;
            auto meta_size = variant_column_reader->get_metadata_size();
            (void)meta_size;
        }
    });

    writer_thread.join();
    reader_thread.join();

    EXPECT_TRUE(writer_status.ok());
}

} // namespace doris
