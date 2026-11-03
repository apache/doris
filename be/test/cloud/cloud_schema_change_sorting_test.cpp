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

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "cloud/cloud_rowset_writer.h"
#include "cloud/cloud_schema_change_job.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "common/config.h"
#include "common/object_pool.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/fs/local_file_system.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/delete/delete_handler.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/schema.h"
#include "storage/tablet/tablet_meta.h"
#include "util/debug_points.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace doris {

namespace {

constexpr std::string_view kTestDir = "./ut_dir/cloud_schema_change_sorting_test";
constexpr int kNumRows = 200;

void add_int_column(TabletSchemaPB* schema, int32_t unique_id, const std::string& name,
                    bool is_key) {
    auto* column = schema->add_column();
    column->set_unique_id(unique_id);
    column->set_name(name);
    column->set_type("INT");
    column->set_is_key(is_key);
    column->set_length(4);
    column->set_index_length(4);
    column->set_is_nullable(false);
    column->set_is_bf_column(false);
    column->set_aggregation("NONE");
}

// Base schema is (k1, k2, v), the new schema reorders the keys to (k2, k1, v), so the schema
// change has to sort the data.
TabletSchemaSPtr create_schema(bool reorder_keys) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_num_short_key_columns(2);
    schema_pb.set_num_rows_per_row_block(1024);
    schema_pb.set_compress_kind(COMPRESS_NONE);
    schema_pb.set_next_column_unique_id(4);
    if (reorder_keys) {
        add_int_column(&schema_pb, 2, "k2", true);
        add_int_column(&schema_pb, 1, "k1", true);
    } else {
        add_int_column(&schema_pb, 1, "k1", true);
        add_int_column(&schema_pb, 2, "k2", true);
    }
    add_int_column(&schema_pb, 3, "v", false);
    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(schema_pb);
    return schema;
}

// Rowset files in `dir`, grouped by rowset id.
std::set<std::string> rowset_ids_of_files(const std::string& dir) {
    std::set<std::string> rowset_ids;
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        auto name = entry.path().filename().native();
        if (entry.is_regular_file() && name != "version") {
            rowset_ids.insert(name.substr(0, 48));
        }
    }
    return rowset_ids;
}

} // namespace

class CloudSchemaChangeSortingTest : public testing::Test {
protected:
    void SetUp() override {
        _origin_enable_debug_points = config::enable_debug_points;
        config::enable_debug_points = true;
        // The TTL manager of file cache sleeps for these intervals, shorten them so that the
        // file cache can be destroyed quickly in TearDown.
        _origin_ttl_gc_interval_ms = config::file_cache_background_ttl_gc_interval_ms;
        _origin_ttl_info_update_interval_ms =
                config::file_cache_background_ttl_info_update_interval_ms;
        config::file_cache_background_ttl_gc_interval_ms = 100;
        config::file_cache_background_ttl_info_update_interval_ms = 100;

        auto fs = io::global_local_filesystem();
        ASSERT_TRUE(fs->delete_directory(std::string(kTestDir)).ok());
        ASSERT_TRUE(fs->create_directory(std::string(kTestDir)).ok());
        _cache_dir = std::filesystem::absolute(std::string(kTestDir) + "/file_cache").string();

        _origin_file_cache_factory = ExecEnv::GetInstance()->file_cache_factory();
        _file_cache_factory = std::make_unique<io::FileCacheFactory>();
        ExecEnv::GetInstance()->set_file_cache_factory(_file_cache_factory.get());
        io::FileCacheSettings settings;
        settings.query_queue_size = 16 * 1024 * 1024;
        settings.query_queue_elements = 16;
        settings.index_queue_size = 16 * 1024 * 1024;
        settings.index_queue_elements = 16;
        settings.disposable_queue_size = 16 * 1024 * 1024;
        settings.disposable_queue_elements = 16;
        settings.capacity = 64 * 1024 * 1024;
        settings.max_file_block_size = 1024 * 1024;
        settings.max_query_cache_size = 0;
        ASSERT_TRUE(io::FileCacheFactory::instance()->create_file_cache(_cache_dir, settings).ok());

        EngineOptions options;
        options.backend_uid = UniqueId::gen_uid();
        auto engine = std::make_unique<CloudStorageEngine>(options);
        _engine = engine.get();
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));

        _base_schema = create_schema(false);
        _new_schema = create_schema(true);
        _base_tablet =
                std::make_shared<CloudTablet>(*_engine, std::make_shared<TabletMeta>(_base_schema));
        _new_tablet =
                std::make_shared<CloudTablet>(*_engine, std::make_shared<TabletMeta>(_new_schema));

        TDescriptorTable t_desc_tbl;
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = 0;
        t_tuple_desc.byteSize = 0;
        t_tuple_desc.numNullBytes = 0;
        t_desc_tbl.tupleDescriptors.push_back(t_tuple_desc);
        ASSERT_TRUE(DescriptorTbl::create(&_pool, t_desc_tbl, &_desc_tbl).ok());
    }

    void TearDown() override {
        DebugPoints::instance()->clear();
        config::enable_debug_points = _origin_enable_debug_points;

        _base_tablet.reset();
        _new_tablet.reset();
        _engine = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        _file_cache_factory.reset();
        ExecEnv::GetInstance()->set_file_cache_factory(_origin_file_cache_factory);
        static_cast<void>(io::global_local_filesystem()->delete_directory(std::string(kTestDir)));
        config::file_cache_background_ttl_gc_interval_ms = _origin_ttl_gc_interval_ms;
        config::file_cache_background_ttl_info_update_interval_ms =
                _origin_ttl_info_update_interval_ms;
    }

    // Writes a base rowset sorted by (k1, k2). As a local rowset it is written to the root of
    // the file cache path, like the intermediate rowsets.
    RowsetSharedPtr write_base_rowset() {
        RowsetWriterContext context;
        context.version = Version(2, 2);
        context.rowset_state = VISIBLE;
        context.segments_overlap = NONOVERLAPPING;
        context.tablet_schema = _base_schema;
        context.newest_write_timestamp = UnixSeconds();
        context.allow_packed_file = false;
        auto writer_result = _base_tablet->create_rowset_writer(context, false);
        if (!writer_result.has_value()) {
            ADD_FAILURE() << "failed to create rowset writer: " << writer_result.error();
            return nullptr;
        }
        auto writer = std::move(writer_result).value();
        auto block = _base_schema->create_storage_block();
        auto columns = std::move(block).mutate_columns();
        for (int32_t i = 0; i < kNumRows; ++i) {
            int32_t k1 = i / 10;
            int32_t k2 = i % 10;
            columns[0]->insert_data(reinterpret_cast<const char*>(&k1), sizeof(k1));
            columns[1]->insert_data(reinterpret_cast<const char*>(&k2), sizeof(k2));
            columns[2]->insert_data(reinterpret_cast<const char*>(&i), sizeof(i));
        }
        block.set_columns(std::move(columns));
        RowsetSharedPtr rowset;
        if (!writer->add_block(&block).ok() || !writer->flush().ok() ||
            !writer->build(rowset).ok()) {
            ADD_FAILURE() << "failed to write base rowset";
            return nullptr;
        }
        return rowset;
    }

    // Runs schema change with sorting on `base_rowset`. Every block read is turned into an
    // intermediate rowset, so that there are many intermediate rowsets.
    Status run_sorting_schema_change(SchemaChange* sc_procedure, const RowsetSharedPtr& base_rowset,
                                     RowsetSharedPtr* output_rowset) {
        auto read_schema = std::make_shared<ReadSchema>(_base_schema->columns());
        RETURN_IF_ERROR(read_schema->init_from_tablet_schema(*_base_schema, false, false));
        DeleteHandler delete_handler;
        std::vector<TabletColumn> dropped_columns;
        RETURN_IF_ERROR(delete_handler.init({}, 2, read_schema, &dropped_columns));

        RowsetReaderContext reader_context;
        reader_context.reader_type = ReaderType::READER_ALTER_TABLE;
        reader_context.need_ordered_result = true;
        reader_context.delete_handler = &delete_handler;
        reader_context.read_schema = read_schema;
        reader_context.batch_size = 16;
        reader_context.version = Version(0, 2);
        RowsetReaderSharedPtr reader;
        RETURN_IF_ERROR(base_rowset->create_reader(&reader));
        RETURN_IF_ERROR(reader->init(&reader_context));

        RowsetWriterContext context;
        context.version = Version(2, 2);
        context.rowset_state = VISIBLE;
        context.segments_overlap = OVERLAPPING;
        context.tablet_schema = _new_schema;
        context.newest_write_timestamp = UnixSeconds();
        context.write_type = DataWriteType::TYPE_SCHEMA_CHANGE;
        context.allow_packed_file = false;
        auto writer = DORIS_TRY(_new_tablet->create_rowset_writer(context, false));

        DebugPoints::instance()->add("VBaseSchemaChangeWithSorting._inner_process.create_rowset");
        RETURN_IF_ERROR(sc_procedure->process(reader, writer.get(), _new_tablet, _base_tablet,
                                              _base_schema, _new_schema));
        return writer->build(*output_rowset);
    }

    std::unique_ptr<BlockChanger> create_changer() {
        auto changer = std::make_unique<BlockChanger>(_new_schema, *_desc_tbl,
                                                      std::make_shared<RuntimeState>());
        changer->set_type(AlterTabletType::SCHEMA_CHANGE);
        changer->set_compatible_version(BeExecVersionManager::get_newest_version());
        // new (k2, k1, v) <- base (k1, k2, v)
        const int32_t ref_column_idx[] = {1, 0, 2};
        for (size_t i = 0; i < 3; ++i) {
            auto* mapping = changer->get_mutable_column_mapping(i);
            mapping->ref_column_idx = ref_column_idx[i];
            mapping->new_column = &_new_schema->column(i);
        }
        return changer;
    }

    bool _origin_enable_debug_points = false;
    int64_t _origin_ttl_gc_interval_ms = 0;
    int64_t _origin_ttl_info_update_interval_ms = 0;
    std::string _cache_dir;
    io::FileCacheFactory* _origin_file_cache_factory = nullptr;
    std::unique_ptr<io::FileCacheFactory> _file_cache_factory;
    CloudStorageEngine* _engine = nullptr;
    TabletSchemaSPtr _base_schema;
    TabletSchemaSPtr _new_schema;
    CloudTabletSPtr _base_tablet;
    CloudTabletSPtr _new_tablet;
    ObjectPool _pool;
    DescriptorTbl* _desc_tbl = nullptr;
};

TEST_F(CloudSchemaChangeSortingTest, SortingGeneratesIntermediateRowsets) {
    // Make sure the scenario does generate intermediate rowsets in the file cache path, which
    // `VBaseSchemaChangeWithSorting` itself never removes.
    auto base_rowset = write_base_rowset();
    ASSERT_NE(base_rowset, nullptr);
    auto changer = create_changer();
    VBaseSchemaChangeWithSorting sc_procedure(*changer, 1024 * 1024 * 1024);
    RowsetSharedPtr output_rowset;
    auto st = run_sorting_schema_change(&sc_procedure, base_rowset, &output_rowset);
    ASSERT_TRUE(st.ok()) << st;

    auto rowset_ids = rowset_ids_of_files(_cache_dir);
    rowset_ids.erase(base_rowset->rowset_id().to_string());
    rowset_ids.erase(output_rowset->rowset_id().to_string());
    ASSERT_GT(rowset_ids.size(), 1);
}

TEST_F(CloudSchemaChangeSortingTest, RemoveIntermediateRowsetsOnSuccess) {
    auto base_rowset = write_base_rowset();
    ASSERT_NE(base_rowset, nullptr);
    auto changer = create_changer();
    VCloudSchemaChangeWithSorting sc_procedure(*changer, 1024 * 1024 * 1024);
    RowsetSharedPtr output_rowset;
    auto st = run_sorting_schema_change(&sc_procedure, base_rowset, &output_rowset);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(output_rowset->num_rows(), kNumRows);

    std::set<std::string> expected {base_rowset->rowset_id().to_string(),
                                    output_rowset->rowset_id().to_string()};
    ASSERT_EQ(rowset_ids_of_files(_cache_dir), expected);
}

TEST_F(CloudSchemaChangeSortingTest, RemoveIntermediateRowsetsOnFailure) {
    auto base_rowset = write_base_rowset();
    ASSERT_NE(base_rowset, nullptr);
    auto changer = create_changer();
    VCloudSchemaChangeWithSorting sc_procedure(*changer, 1024 * 1024 * 1024);
    DebugPoints::instance()->add(
            "VBaseSchemaChangeWithSorting._inner_process.before_external_sorting");
    RowsetSharedPtr output_rowset;
    auto st = run_sorting_schema_change(&sc_procedure, base_rowset, &output_rowset);
    ASSERT_FALSE(st.ok());
    ASSERT_NE(st.to_string().find("fail before external sorting"), std::string::npos) << st;

    std::set<std::string> expected {base_rowset->rowset_id().to_string()};
    ASSERT_EQ(rowset_ids_of_files(_cache_dir), expected);
}

TEST_F(CloudSchemaChangeSortingTest, RemoveStaleLocalRowsetFiles) {
    auto dir = std::string(kTestDir) + "/stale";
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(dir).ok());

    auto current_uid = UniqueId::gen_uid();
    auto previous_uid = UniqueId::gen_uid();
    auto rowset_id = [](int64_t inc_id, const UniqueId& uid) {
        RowsetId id;
        id.init(2, inc_id, uid.hi, uid.lo);
        return id.to_string();
    };
    auto touch = [&](const std::string& name) {
        io::FileWriterPtr writer;
        ASSERT_TRUE(io::global_local_filesystem()->create_file(dir + "/" + name, &writer).ok());
        ASSERT_TRUE(writer->append("data").ok());
        ASSERT_TRUE(writer->close().ok());
    };

    auto stale = rowset_id(10, previous_uid);
    auto current = rowset_id(10, current_uid);
    std::vector<std::string> removed {stale + "_0.dat", stale + "_1.dat", stale + "_0.idx",
                                      stale + "_0_10000@suffix.idx"};
    std::vector<std::string> kept {current + "_0.dat", current + "_0.idx", "version",
                                   "not_a_rowset_0.dat", stale + "_0.tmp"};
    for (const auto& name : removed) {
        touch(name);
    }
    for (const auto& name : kept) {
        touch(name);
    }
    // directories are never removed, even if the name looks like a rowset file
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(dir + "/" + stale + "_2.dat").ok());
    kept.push_back(stale + "_2.dat");

    ASSERT_EQ(CloudRowsetWriter::remove_stale_local_rowset_files(dir, current_uid), removed.size());
    for (const auto& name : removed) {
        EXPECT_FALSE(std::filesystem::exists(dir + "/" + name)) << name;
    }
    for (const auto& name : kept) {
        EXPECT_TRUE(std::filesystem::exists(dir + "/" + name)) << name;
    }
}

} // namespace doris
