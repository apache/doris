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

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Descriptors_constants.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <string>
#include <utility>
#include <vector>

#include "cloud/config.h"
#include "common/config.h"
#include "common/object_pool.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/field.h"
#include "cpp/sync_point.h"
#include "exprs/vexpr.h"
#include "io/fs/file_reader.h"
#include "io/fs/local_file_system.h"
#include "io/fs/read_ahead_metrics.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "service/point_query_executor.h"
#include "storage/data_dir.h"
#include "storage/iterators.h"
#include "storage/options.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/beta_rowset_writer.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/schema.h"
#include "storage/segment/column_read_ahead_resources.h"
#include "storage/segment/mock/mock_segment.h"
#include "storage/segment/page_io.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_loader.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"
#include "util/defer_op.h"
#include "util/jsonb/serialize.h"
#include "util/threadpool.h"
#include "util/thrift_util.h"

namespace doris {

namespace segment_v2 {
namespace {

class TestFileReader final : public io::FileReader {
public:
    explicit TestFileReader(std::string data) : _data(std::move(data)) {}

    Status close() override {
        _closed = true;
        return Status::OK();
    }
    const io::Path& path() const override { return _path; }
    size_t size() const override { return _data.size(); }
    bool closed() const override { return _closed; }
    int64_t mtime() const override { return 0; }

    size_t read_calls() const { return _read_calls; }

protected:
    Status read_at_impl(size_t, Slice, size_t*, const io::IOContext*) override {
        ++_read_calls;
        return Status::IOError("unexpected file read for a constant column");
    }

private:
    const io::Path _path {"test-segment.dat"};
    const std::string _data;
    size_t _read_calls {0};
    bool _closed {false};
};

TEST(PointQueryRowIdReadTest, ReadsSlotDefaultBeforeNewColumnReachesTabletSchema) {
    auto source = std::make_shared<TestFileReader>(std::string(256, 'x'));
    auto schema = std::make_shared<TabletSchema>();
    testing::NiceMock<MockSegment> segment(schema);
    segment.set_file_reader_for_test(source);
    ON_CALL(segment, file_reader()).WillByDefault(testing::Return(source));
    EXPECT_CALL(segment, _get_segment_footer(testing::_, testing::_, testing::_))
            .WillOnce([](std::shared_ptr<SegmentFooterPB>& footer, OlapReaderStatistics*,
                         const io::IOContext*) {
                footer = std::make_shared<SegmentFooterPB>();
                footer->set_num_rows(3);
                return Status::OK();
            });
    auto slot_data = TSlotDescriptorBuilder()
                             .type(TYPE_INT)
                             .column_name("new_column")
                             .set_nullIndicatorBit(-1)
                             .build();
    slot_data.__set_col_unique_id(123);
    slot_data.__set_col_default_value("42");
    SlotDescriptor slot(slot_data);
    MutableColumnPtr result = ColumnInt32::create();
    OlapReaderStatistics stats;
    StorageReadOptions options;
    options.stats = &stats;
    RowIdColumnRead column {.slot = &slot, .result = &result};
    const std::vector<uint32_t> row_ids {0, 2};
    ASSERT_TRUE(segment.read_columns_by_rowids(*schema, row_ids,
                                               std::span<const RowIdColumnRead>(&column, 1),
                                               options)
                        .ok());
    const auto& values = assert_cast<const ColumnInt32&>(*result).get_data();
    ASSERT_EQ(values.size(), 2);
    EXPECT_EQ(values[0], 42);
    EXPECT_EQ(values[1], 42);
    EXPECT_EQ(source->read_calls(), 0);
}

} // namespace
} // namespace segment_v2

class PointQueryReadAheadTest : public testing::Test {
protected:
    void SetUp() override {
        char buffer[1024];
        ASSERT_NE(getcwd(buffer, sizeof(buffer)), nullptr);
        _root = std::string(buffer) + "/point_query_read_ahead_ut";
        _old_storage_root = config::storage_root_path;
        config::storage_root_path = _root;
        const auto fs = io::global_local_filesystem();
        ASSERT_TRUE(fs->delete_directory(_root).ok());
        ASSERT_TRUE(fs->create_directory(_root).ok());
        EngineOptions options;
        options.store_paths.emplace_back(_root, -1);
        auto engine = std::make_unique<StorageEngine>(options);
        ASSERT_TRUE(engine->open().ok());
        _engine = engine.get();
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
        _data_dir = std::make_unique<DataDir>(*_engine, _root);
        static_cast<void>(_data_dir->update_capacity());
    }

    void TearDown() override {
        _data_dir.reset();
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        static_cast<void>(io::global_local_filesystem()->delete_directory(_root));
        config::storage_root_path = _old_storage_root;
    }

    TabletSchemaSPtr create_row_store_schema() {
        TabletSchemaPB pb;
        pb.set_keys_type(UNIQUE_KEYS);
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(1024);
        pb.set_compress_kind(COMPRESS_LZ4);
        pb.set_next_column_unique_id(4);
        pb.set_store_row_column(true);
        const auto add = [&](int uid, const std::string& name, const std::string& type, bool is_key,
                             int length) {
            auto* column = pb.add_column();
            column->set_unique_id(uid);
            column->set_name(name);
            column->set_type(type);
            column->set_is_key(is_key);
            column->set_length(length);
            column->set_index_length(type == "STRING" ? 4 : length);
            column->set_is_nullable(!is_key && name == "v");
            column->set_is_bf_column(false);
            column->set_aggregation("NONE");
            if (name == "v" || name == DELETE_SIGN) {
                column->set_default_value("0");
            }
        };
        add(0, "k", "INT", true, 4);
        add(1, "v", "INT", false, 4);
        add(2, DELETE_SIGN, "TINYINT", false, 1);
        add(3, BeConsts::ROW_STORE_COL, "STRING", false, 2147483643);
        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(pb);
        return schema;
    }

    RowsetSharedPtr write_rowset(const TabletSchemaSPtr& schema, int64_t rowset_id, int64_t version,
                                 const std::vector<std::pair<int32_t, int32_t>>& rows,
                                 TabletSharedPtr* tablet) {
        TabletMetaPB tablet_pb;
        tablet_pb.set_tablet_id(90001);
        tablet_pb.set_partition_id(10);
        tablet_pb.set_enable_unique_key_merge_on_write(true);
        schema->to_schema_pb(tablet_pb.mutable_schema());
        auto tablet_meta = std::make_shared<TabletMeta>();
        tablet_meta->init_from_pb(tablet_pb);
        *tablet = std::make_shared<Tablet>(*_engine, tablet_meta, _data_dir.get(), "point_read_ut");

        RowsetWriterContext context;
        context.rowset_id.init(rowset_id);
        context.tablet_id = 90001;
        context.tablet_schema_hash = 1;
        context.partition_id = 10;
        context.rowset_type = BETA_ROWSET;
        context.tablet_path = _root;
        context.rowset_state = VISIBLE;
        context.tablet_schema = schema;
        context.version = {version, version};
        context.enable_unique_key_merge_on_write = true;
        context.write_type = DataWriteType::TYPE_DIRECT;
        context.tablet = *tablet;
        auto result = RowsetFactory::create_rowset_writer(*_engine, context, false);
        EXPECT_TRUE(result.has_value()) << result.error();
        if (!result.has_value()) {
            return nullptr;
        }
        auto writer = std::move(result).value();
        Block block = schema->create_block();
        {
            auto guard = block.mutate_columns_scoped();
            auto& columns = guard.mutable_columns();
            for (const auto& [key, value] : rows) {
                columns[0]->insert_data(reinterpret_cast<const char*>(&key), sizeof(key));
                columns[1]->insert_data(reinterpret_cast<const char*>(&value), sizeof(value));
                columns[2]->insert_default();
                columns[3]->insert_default();
            }
        }
        EXPECT_TRUE(writer->add_block(&block).ok());
        EXPECT_TRUE(writer->flush().ok());
        RowsetSharedPtr rowset;
        EXPECT_TRUE(writer->build(rowset).ok());
        return rowset;
    }

    // 4.2 resolves hidden rowset versions after scanning rather than with constant
    // column readers. Exercise two physical columns and a slot-derived default here.
    enum class ReadMode { NORMAL, READ_AHEAD, RESOURCE_ERROR, DATA_ERROR };

    void check_column_read(ReadMode mode, bool stale_tablet_schema = false) {
        auto schema = create_row_store_schema();
        TabletSharedPtr tablet;
        auto rowset = write_rowset(schema, 5201, 2, {{1, 11}, {2, 22}}, &tablet);
        ASSERT_NE(rowset, nullptr);
        SegmentCacheHandle segments;
        ASSERT_TRUE(SegmentLoader::instance()
                            ->load_segments(std::static_pointer_cast<BetaRowset>(rowset), &segments)
                            .ok());
        ASSERT_EQ(segments.get_segments().size(), 1);
        const auto& segment = segments.get_segments().front();
        std::unique_ptr<ThreadPool> executor;
        ASSERT_TRUE(
                ThreadPoolBuilder("PointReadBatchTest").set_max_threads(2).build(&executor).ok());
        std::unique_ptr<io::FileRangeReadScheduler> scheduler;
        ASSERT_TRUE(io::FileRangeReadScheduler::create({.max_bytes_per_query = 1024 * 1024,
                                                        .max_bytes_per_be = 2 * 1024 * 1024},
                                                       executor.get(), &scheduler)
                            .ok());
        auto* env = ExecEnv::GetInstance();
        auto old_scheduler = std::move(env->_file_range_read_scheduler);
        env->_file_range_read_scheduler = std::move(scheduler);
        const auto old_cloud_id = config::cloud_unique_id;
        const bool old_read_ahead = config::enable_query_read_ahead;
        const bool old_async_write = config::enable_async_file_cache_write;
        const bool old_page_cache = config::disable_storage_page_cache;
        const auto old_window = config::read_ahead_eager_window_bytes;
        auto* sync = SyncPoint::get_instance();
        Defer restore {[&] {
            sync->disable_processing();
            executor->wait();
            env->_file_range_read_scheduler = std::move(old_scheduler);
            config::cloud_unique_id = old_cloud_id;
            config::enable_query_read_ahead = old_read_ahead;
            config::enable_async_file_cache_write = old_async_write;
            config::disable_storage_page_cache = old_page_cache;
            config::read_ahead_eager_window_bytes = old_window;
        }};
        config::cloud_unique_id = "PointReadBatchTest";
        config::enable_query_read_ahead = true;
        config::enable_async_file_cache_write = true;
        config::disable_storage_page_cache = true;
        if (mode == ReadMode::RESOURCE_ERROR) {
            config::read_ahead_eager_window_bytes = 0;
        }
        auto key_data = TSlotDescriptorBuilder()
                                .type(TYPE_INT)
                                .column_name("k")
                                .set_nullIndicatorBit(-1)
                                .build();
        key_data.__set_col_unique_id(0);
        SlotDescriptor key_slot(key_data);
        auto value_data = TSlotDescriptorBuilder()
                                  .type(TYPE_INT)
                                  .column_name("v")
                                  .set_nullIndicatorBit(0)
                                  .build();
        value_data.__set_col_unique_id(1);
        SlotDescriptor value_slot(value_data);
        auto default_data = TSlotDescriptorBuilder()
                                    .type(TYPE_BIGINT)
                                    .column_name("new_default")
                                    .set_nullIndicatorBit(-1)
                                    .build();
        default_data.__set_col_unique_id(1001);
        default_data.__set_col_default_value("42");
        SlotDescriptor default_slot(default_data);
        MutableColumnPtr keys = key_slot.type()->create_column();
        MutableColumnPtr values = value_slot.type()->create_column();
        MutableColumnPtr defaults = default_slot.type()->create_column();
        const std::array<segment_v2::RowIdColumnRead, 3> columns {
                {{.slot = &key_slot, .result = &keys},
                 {.slot = &value_slot, .result = &values},
                 {.slot = &default_slot, .result = &defaults}}};
        size_t submissions = 0;
        SyncPoint::CallbackGuard range_guard;
        SyncPoint::CallbackGuard crc_guard;
        sync->set_call_back(
                "FileRangeRead::create:inject_failure",
                [&](auto&&) {
                    // Every initial submission, including the second column, must precede the
                    // first decoded value. A per-column prepare/read loop breaks this contract.
                    ++submissions;
                    EXPECT_TRUE(keys->empty());
                    EXPECT_TRUE(values->empty());
                    EXPECT_TRUE(defaults->empty());
                },
                &range_guard);
        if (mode == ReadMode::DATA_ERROR) {
            sync->set_call_back(
                    "PageIO::read_and_decompress_page:crc_failure_inj",
                    [&](auto&& args) {
                        auto* ctx = try_any_cast<segment_v2::InjectionContext*>(args[0]);
                        // Keep preparation/index reads intact; inject only once both columns'
                        // initial ranges exist, so this exercises an actual decode-time error.
                        if (submissions >= 2 && ctx->opts->type == segment_v2::DATA_PAGE) {
                            *ctx->crc ^= 1;
                        }
                    },
                    &crc_guard);
        }
        sync->enable_processing();
        OlapReaderStatistics stats;
        StorageReadOptions options;
        options.stats = &stats;
        options.use_page_cache = false;
        options.io_ctx.reader_type = ReaderType::READER_QUERY;
        options.io_ctx.file_cache_stats = &stats.file_cache_stats;
        options.version = rowset->version();
        auto context = mode == ReadMode::NORMAL
                               ? nullptr
                               : env->file_range_read_scheduler()->create_context();
        TabletSchema reader_schema;
        if (stale_tablet_schema) {
            // The reader BE knows only k; the rowset written elsewhere also stores v.
            reader_schema.append_column(schema->column(0));
        }
        const auto status = segment->read_columns_by_rowids(
                stale_tablet_schema ? reader_schema : *schema, {0, 1}, columns, options, context);
        if (mode == ReadMode::DATA_ERROR) {
            EXPECT_FALSE(status.ok());
            EXPECT_TRUE(keys->empty());
            EXPECT_TRUE(values->empty());
            EXPECT_TRUE(defaults->empty());
        } else {
            ASSERT_TRUE(status.ok()) << status;
            ASSERT_EQ(keys->size(), 2);
            ASSERT_EQ(values->size(), 2);
            ASSERT_EQ(defaults->size(), 2);
            EXPECT_EQ(keys->get_int(0), 1);
            EXPECT_EQ(keys->get_int(1), 2);
            const auto& nested = assert_cast<const ColumnNullable&>(*values).get_nested_column();
            EXPECT_EQ(nested.get_int(0), 11);
            EXPECT_EQ(nested.get_int(1), 22);
            EXPECT_EQ(defaults->get_int(0), 42);
            EXPECT_EQ(defaults->get_int(1), 42);
        }
        if (mode == ReadMode::NORMAL || mode == ReadMode::RESOURCE_ERROR) {
            EXPECT_EQ(submissions, 0);
        } else {
            EXPECT_GE(submissions, 2);
        }
        if (mode == ReadMode::RESOURCE_ERROR) {
            std::string row;
            const RowLocation location {rowset->rowset_id(), segment->id(), 0};
            EXPECT_FALSE(tablet->lookup_row_data({}, location, segment, rowset, stats, row, false,
                                                 nullptr, context)
                                 .ok());
            EXPECT_TRUE(row.empty());
        }
        executor->wait();
        if (context != nullptr) {
            EXPECT_EQ(context->resident_bytes(), 0);
        }
    }

    StorageEngine* _engine = nullptr;
    std::unique_ptr<DataDir> _data_dir;
    std::string _root;
    std::string _old_storage_root;
};

TEST_F(PointQueryReadAheadTest, UnifiedColumnReadsWithoutReadAheadContext) {
    check_column_read(ReadMode::NORMAL);
}

TEST_F(PointQueryReadAheadTest, UnifiedColumnReadsSubmitAllColumnsBeforeDecoding) {
    check_column_read(ReadMode::READ_AHEAD);
}

TEST_F(PointQueryReadAheadTest, ReadsStoredValuesWhenTabletSchemaIsStale) {
    check_column_read(ReadMode::NORMAL, true);
}

TEST_F(PointQueryReadAheadTest, ReadAheadReadsStoredValuesWhenTabletSchemaIsStale) {
    check_column_read(ReadMode::READ_AHEAD, true);
}

TEST_F(PointQueryReadAheadTest, ColumnResourceErrorsFallBackAndRowStoreResourceErrorsPropagate) {
    check_column_read(ReadMode::RESOURCE_ERROR);
}

TEST_F(PointQueryReadAheadTest, UnifiedColumnReadsPropagateDataErrors) {
    check_column_read(ReadMode::DATA_ERROR);
}

TEST_F(PointQueryReadAheadTest, RowStoreFallsBackWhenReadAheadSwitchChanges) {
    auto schema = create_row_store_schema();
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 5101, 2, {{1, 11}, {2, 22}}, &tablet);
    ASSERT_NE(rowset, nullptr);
    const RowLocation location {rowset->rowset_id(), 0, 1};
    const std::string key = "key";
    OlapReaderStatistics expected_stats;
    std::string expected;
    ASSERT_TRUE(
            tablet->lookup_row_data(Slice(key), location, rowset, expected_stats, expected).ok());
    SegmentCacheHandle segment_cache;
    ASSERT_TRUE(
            SegmentLoader::instance()
                    ->load_segments(std::static_pointer_cast<BetaRowset>(rowset), &segment_cache)
                    .ok());
    ASSERT_EQ(segment_cache.get_segments().size(), 1);
    const auto& segment = segment_cache.get_segments().front();

    const auto old_cloud_unique_id = config::cloud_unique_id;
    const bool old_read_ahead = config::enable_query_read_ahead;
    const bool old_async_write = config::enable_async_file_cache_write;
    auto* sync_point = SyncPoint::get_instance();
    Defer restore {[&] {
        sync_point->disable_processing();
        config::cloud_unique_id = old_cloud_unique_id;
        config::enable_query_read_ahead = old_read_ahead;
        config::enable_async_file_cache_write = old_async_write;
    }};
    config::cloud_unique_id = "PointQueryReadAheadTest";

    for (int switch_index = 0; switch_index < 2; ++switch_index) {
        SCOPED_TRACE(switch_index);
        config::enable_query_read_ahead = true;
        config::enable_async_file_cache_write = true;
        ASSERT_TRUE(segment_v2::ColumnReadAheadResources::enabled());
        auto context = std::make_shared<io::FileRangeReadContext>(1024 * 1024);

        // Model a switch closing after the request created its context and resolved the segment.
        // Both the read-ahead and fallback paths must reuse that segment without loading it again.
        if (switch_index == 0) {
            config::enable_query_read_ahead = false;
        } else {
            config::enable_async_file_cache_write = false;
        }
        size_t segment_loads = 0;
        SyncPoint::CallbackGuard guard;
        sync_point->set_call_back(
                "SegmentLoader::load_segments", [&](auto&&) { ++segment_loads; }, &guard);
        sync_point->enable_processing();
        OlapReaderStatistics stats;
        std::string actual;
        const auto status = tablet->lookup_row_data(Slice(key), location, segment, rowset, stats,
                                                    actual, false, nullptr, context);
        sync_point->disable_processing();
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_EQ(segment_loads, 0);
        EXPECT_EQ(actual, expected);
        EXPECT_EQ(context->resident_bytes(), 0);
    }
}

// Helper class for setting up Reusable objects to test LookupConnectionCache
class ReusableTestHelper {
public:
    static TDescriptorTable create_descriptor_tablet() {
        TDescriptorTableBuilder dtb;
        TTupleDescriptorBuilder tuple_builder;

        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_TINYINT)
                                       .column_name("k1")
                                       .column_pos(0)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_SMALLINT)
                                       .column_name("k2")
                                       .column_pos(1)
                                       .build());
        tuple_builder.add_slot(
                TSlotDescriptorBuilder().type(TYPE_INT).column_name("k3").column_pos(2).build());
        tuple_builder.add_slot(
                TSlotDescriptorBuilder().type(TYPE_BIGINT).column_name("k4").column_pos(3).build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_LARGEINT)
                                       .column_name("k5")
                                       .column_pos(4)
                                       .build());
        tuple_builder.add_slot(
                TSlotDescriptorBuilder().type(TYPE_DATE).column_name("k6").column_pos(5).build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_DATETIME)
                                       .column_name("k7")
                                       .column_pos(6)
                                       .build());
        tuple_builder.add_slot(
                TSlotDescriptorBuilder().string_type(4).column_name("k8").column_pos(7).build());
        tuple_builder.add_slot(
                TSlotDescriptorBuilder().string_type(65).column_name("k9").column_pos(8).build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .decimal_type(6, 3)
                                       .column_name("k10")
                                       .column_pos(9)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_DATEV2)
                                       .column_name("k11")
                                       .column_pos(10)
                                       .build());

        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_TINYINT)
                                       .column_name("v1")
                                       .column_pos(11)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_SMALLINT)
                                       .column_name("v2")
                                       .column_pos(12)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_INT)
                                       .column_name("v3")
                                       .column_pos(13)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_BIGINT)
                                       .column_name("v4")
                                       .column_pos(14)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_LARGEINT)
                                       .column_name("v5")
                                       .column_pos(15)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_DATE)
                                       .column_name("v6")
                                       .column_pos(16)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_DATETIME)
                                       .column_name("v7")
                                       .column_pos(17)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(4)
                                       .column_name("v8")
                                       .column_pos(18)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(65)
                                       .column_name("v9")
                                       .column_pos(19)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .decimal_type(6, 3)
                                       .column_name("v10")
                                       .column_pos(20)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_DATEV2)
                                       .column_name("v11")
                                       .column_pos(21)
                                       .nullable(false)
                                       .build());
        tuple_builder.build(&dtb);

        return dtb.desc_tbl();
    }

    static std::shared_ptr<Reusable> create_reusable() {
        auto obj_pool = std::make_unique<ObjectPool>();
        auto runtime_state = RuntimeState::create_unique();
        auto reusable = std::make_shared<Reusable>();

        // Create a simple TupleDescriptor for testing
        TDescriptorTable t_desc_tbl = create_descriptor_tablet();

        // Initialize Reusable
        Status st = reusable->init(t_desc_tbl, output_exprs, query_options, *tablet_schema, 2);
        if (!st.ok()) {
            return nullptr;
        }

        return reusable;
    }

    static std::vector<TExpr> output_exprs;
    static TQueryOptions query_options;
    static std::shared_ptr<TabletSchema> tablet_schema;
};

std::vector<TExpr> ReusableTestHelper::output_exprs = []() {
    std::vector<TExpr> list;
    TExpr expr;
    expr.nodes.emplace_back(TExprNode());
    expr.nodes[0].node_type = TExprNodeType::SLOT_REF;
    auto type = TTypeDesc();
    type.types.emplace_back(TTypeNode());
    type.types[0].type = TTypeNodeType::SCALAR;
    type.types[0].__isset.scalar_type = true;
    type.types[0].scalar_type.type = TPrimitiveType::BIGINT;
    expr.nodes[0].type = type;
    expr.nodes[0].num_children = 0;
    list.push_back(expr);
    return list;
}();

TQueryOptions ReusableTestHelper::query_options = TQueryOptions();
std::shared_ptr<TabletSchema> ReusableTestHelper::tablet_schema = []() {
    auto schema = std::make_shared<TabletSchema>();
    for (int i = 0; i < 11; ++i) {
        schema->append_column(TabletColumn(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                                           FieldType::OLAP_FIELD_TYPE_BIGINT));
    }
    TabletColumn delete_sign;
    delete_sign.set_name(DELETE_SIGN);
    delete_sign.set_unique_id(1000);
    delete_sign.set_type(FieldType::OLAP_FIELD_TYPE_TINYINT);
    schema->append_column(delete_sign);
    return schema;
}();

TEST(PointQueryReusableTest, AcceptDescriptorsWithoutAccessPaths) {
    auto descriptor = ReusableTestHelper::create_descriptor_tablet();
    for (auto& slot : descriptor.slotDescriptors) {
        slot.__set_all_access_paths({});
        slot.__set_predicate_access_paths({});
    }
    Reusable reusable;
    auto status =
            reusable.init(descriptor, ReusableTestHelper::output_exprs,
                          ReusableTestHelper::query_options, *ReusableTestHelper::tablet_schema, 2);
    ASSERT_TRUE(status.ok()) << status.to_string();
}

TEST(PointQueryReusableTest, RejectAllColumnAccessPaths) {
    std::vector<TColumnAccessPath> paths;
    for (const auto& components : std::vector<std::vector<std::string>> {
                 {"k1"}, {"k1", "leaf"}, {"k1", "OFFSET"}, {"k1", "NULL"}}) {
        TColumnAccessPath data_path;
        data_path.__set_type(TAccessPathType::DATA);
        TDataAccessPath data;
        data.__set_path(components);
        data_path.__set_data_access_path(data);
        paths.push_back(data_path); // Legacy DATA paths have no version field.
        data_path.__set_version(g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_TYPED);
        paths.push_back(data_path);

        TColumnAccessPath meta_path;
        meta_path.__set_type(TAccessPathType::META);
        meta_path.__set_version(g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_TYPED);
        TMetaAccessPath meta;
        meta.__set_path(components);
        meta_path.__set_meta_access_path(meta);
        paths.push_back(meta_path);
    }
    paths.emplace_back(); // Even a malformed path must not bypass the guard.
    for (size_t path_idx = 0; path_idx < paths.size(); ++path_idx) {
        SCOPED_TRACE(path_idx);
        for (const bool predicate : {false, true}) {
            SCOPED_TRACE(predicate);
            auto descriptor = ReusableTestHelper::create_descriptor_tablet();
            auto& slot = descriptor.slotDescriptors[0];
            slot.__set_all_access_paths(predicate ? TColumnAccessPaths {}
                                                  : TColumnAccessPaths {paths[path_idx]});
            slot.__set_predicate_access_paths(predicate ? TColumnAccessPaths {paths[path_idx]}
                                                        : TColumnAccessPaths {});
            Reusable reusable;
            auto status = reusable.init(descriptor, ReusableTestHelper::output_exprs,
                                        ReusableTestHelper::query_options,
                                        *ReusableTestHelper::tablet_schema, 2);
            ASSERT_FALSE(status.ok());
            EXPECT_NE(status.to_string().find("does not support column access paths"),
                      std::string::npos);
            EXPECT_NE(status.to_string().find("k1"), std::string::npos);
        }
    }
}

TEST(PointQueryReusableTest, RejectFullStructRootAccessPaths) {
    TabletSchema schema;
    schema.copy_from(*ReusableTestHelper::tablet_schema);
    TabletColumn column;
    column.set_name("typed");
    column.set_unique_id(100);
    column.set_type(FieldType::OLAP_FIELD_TYPE_STRUCT);
    TabletColumn child;
    child.set_name("leaf");
    child.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    column.add_sub_column(child);
    schema.append_column(column);

    auto descriptor = ReusableTestHelper::create_descriptor_tablet();
    descriptor.slotDescriptors.resize(1);
    auto& slot = descriptor.slotDescriptors[0];
    slot.__set_colName("typed");
    slot.__set_col_unique_id(100);
    const DataTypePtr full_type = std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeString>()}, Strings {"leaf"});
    slot.__set_slotType(full_type->to_thrift());
    TColumnAccessPath root;
    root.__set_type(TAccessPathType::DATA);
    TDataAccessPath data;
    data.__set_path({"typed"});
    root.__set_data_access_path(data);
    for (const bool predicate : {false, true}) {
        slot.__set_all_access_paths(predicate ? TColumnAccessPaths {} : TColumnAccessPaths {root});
        slot.__set_predicate_access_paths(predicate ? TColumnAccessPaths {root}
                                                    : TColumnAccessPaths {});
        Reusable reusable;
        auto status = reusable.init(descriptor, ReusableTestHelper::output_exprs,
                                    ReusableTestHelper::query_options, schema, 2);
        ASSERT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find("does not support column access paths"),
                  std::string::npos);
    }
}

TEST(PointQueryReusableTest, MissingScalarWithoutPathsUsesPayloadOrFeDefault) {
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    const std::string stored_value = "payload";
    for (const bool payload_has_uid : {false, true}) {
        TabletColumn source_column;
        source_column.set_name("source");
        source_column.set_unique_id(payload_has_uid ? 100 : 99);
        source_column.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
        TabletSchema source_schema;
        source_schema.append_column(source_column);
        auto values = string_type->create_column();
        values->insert_data(stored_value.data(), stored_value.size());
        Block source;
        source.insert({std::move(values), string_type, "source"});
        auto jsonb_column = ColumnString::create();
        JsonbSerializeUtil::block_to_jsonb(source_schema, source, *jsonb_column, 1,
                                           DataTypeSerDeSPtrs {string_type->get_serde()}, {});
        auto descriptor = ReusableTestHelper::create_descriptor_tablet();
        descriptor.slotDescriptors.resize(1);
        auto& slot = descriptor.slotDescriptors[0];
        slot.__set_colName("new_column");
        slot.__set_col_unique_id(100);
        slot.__set_slotType(string_type->to_thrift());
        slot.__set_nullIndicatorBit(-1);
        slot.__set_col_default_value("abcd");

        Reusable reusable;
        auto status = reusable.init(descriptor, ReusableTestHelper::output_exprs,
                                    ReusableTestHelper::query_options,
                                    *ReusableTestHelper::tablet_schema, 1);
        ASSERT_TRUE(status.ok()) << status.to_string();
        Block decoded(reusable.tuple_desc()->slots(), 1);
        status = JsonbSerializeUtil::jsonb_to_block(reusable.get_data_type_serdes(), *jsonb_column,
                                                    reusable.get_col_uid_to_idx(), decoded,
                                                    reusable.get_col_default_values(), {});
        ASSERT_TRUE(status.ok()) << status.to_string();
        ASSERT_EQ(decoded.rows(), 1);
        EXPECT_EQ(decoded.get_by_position(0).column->get_data_at(0).to_string(),
                  payload_has_uid ? stored_value : "abcd");
    }
}

// RowCache test class
class RowCacheTest : public testing::Test {
protected:
    void SetUp() override {
        // Create RowCache instance
        _row_cache = new RowCache(1024, 4); // 1KB cache, 4 shards
    }

    void TearDown() override { delete _row_cache; }

    RowCache* _row_cache = nullptr;
};

// Test basic RowCache functionality
TEST_F(RowCacheTest, PQTestBasicOperations) {
    // Create test data
    const int64_t tablet_id = 12345;
    const std::string key_data = "test_key";
    Slice key_slice(key_data.c_str(), key_data.size());

    const std::string value_data = "test_value";
    Slice value_slice(value_data.c_str(), value_data.size());

    RowCache::RowCacheKey cache_key(tablet_id, key_slice);

    // Test insertion
    _row_cache->insert(cache_key, value_slice);

    // Test lookup
    RowCache::CacheHandle handle;
    bool found = _row_cache->lookup(cache_key, &handle);
    ASSERT_TRUE(found) << "Cache entry should be found";
    ASSERT_TRUE(handle.valid()) << "Cache handle should be valid";

    // Verify data correctness
    Slice cached_data = handle.data();
    ASSERT_EQ(cached_data.size, value_slice.size) << "Cache data size should match";
    ASSERT_EQ(memcmp(cached_data.data, value_slice.data, value_slice.size), 0)
            << "Cache data content should match";

    // Test deletion
    _row_cache->erase(cache_key);
    RowCache::CacheHandle handle2;
    found = _row_cache->lookup(cache_key, &handle2);
    ASSERT_FALSE(found) << "Cache entry should be deleted";
    ASSERT_FALSE(handle2.valid()) << "Cache handle should be invalid";
}

// Test RowCache LRU mechanism
TEST_F(RowCacheTest, PQTestLRUEviction) {
    // Use small capacity cache to test LRU eviction
    RowCache small_cache(100, 1); // 100 bytes capacity, 1 shard

    // Create enough entries to trigger LRU eviction
    for (int i = 0; i < 10; i++) {
        const int64_t tablet_id = 12345;
        std::string key_data = "key_" + std::to_string(i);
        Slice key_slice(key_data.c_str(), key_data.size());

        // Create values large enough, about 20 bytes each
        std::string value_data = "value_" + std::to_string(i) + std::string(10, 'x');
        Slice value_slice(value_data.c_str(), value_data.size());

        RowCache::RowCacheKey cache_key(tablet_id, key_slice);
        small_cache.insert(cache_key, value_slice);
    }

    // Check if some earlier entries have been evicted
    bool at_least_one_evicted = false;
    for (int i = 0; i < 5; i++) {
        const int64_t tablet_id = 12345;
        std::string key_data = "key_" + std::to_string(i);
        Slice key_slice(key_data.c_str(), key_data.size());

        RowCache::RowCacheKey cache_key(tablet_id, key_slice);
        RowCache::CacheHandle handle;
        if (!small_cache.lookup(cache_key, &handle)) {
            at_least_one_evicted = true;
            break;
        }
    }

    ASSERT_TRUE(at_least_one_evicted) << "At least one cache entry should be evicted";

    // Ensure the most recently accessed entry is still in cache
    const int64_t tablet_id = 12345;
    std::string key_data = "key_9"; // Last inserted key
    Slice key_slice(key_data.c_str(), key_data.size());

    RowCache::RowCacheKey cache_key(tablet_id, key_slice);
    RowCache::CacheHandle handle;
    bool found = small_cache.lookup(cache_key, &handle);
    //evicted
    ASSERT_TRUE(!found);
}

// LookupConnectionCache test class
class LookupConnectionCacheTest : public testing::Test {
protected:
    void SetUp() override {
        // Create LookupConnectionCache instance
        _lookup_cache = new LookupConnectionCache(1024); // 1KB cache
    }

    void TearDown() override { delete _lookup_cache; }

    LookupConnectionCache* _lookup_cache = nullptr;
};

TEST_F(LookupConnectionCacheTest, PQTestLRUEvictionPolicy) {
    LookupConnectionCache cache(40);

    for (int i = 1; i <= 45; ++i) {
        auto reusable = std::make_shared<Reusable>();
        reusable->_block_pool.resize(10);
        cache.add(i, reusable);
    }
    EXPECT_LT(cache.get_element_count(), 45) << "capacity " << cache.get_capacity();
    auto entry2 = cache.get(10);

    auto reusable5 = std::make_shared<Reusable>();
    reusable5->_block_pool.resize(10);
    cache.add(41, reusable5);
    cache.add(42, reusable5);
    cache.add(43, reusable5);
    EXPECT_LT(cache.get_element_count(), 45) << "capacity " << cache.get_capacity();
}

// Test cache capacity boundary and LRU eviction policy
TEST_F(LookupConnectionCacheTest, PQTestCapacityBoundary) {
    LookupConnectionCache cache(40);
    const int num_entries = 45;

    // Insert 15 entries to exceed capacity
    for (int i = 0; i < num_entries; ++i) {
        auto reusable = ReusableTestHelper::create_reusable();
        cache.add(i, reusable);
    }

    // Verify at least 5 entries are evicted
    int found_count = 0;
    for (int i = 0; i < num_entries; ++i) {
        if (cache.get(i) != nullptr) found_count++;
    }
    EXPECT_LT(found_count, 45) << "LRU eviction should maintain capacity limit";
}

// Test thread safety with concurrent operations
TEST_F(LookupConnectionCacheTest, PQTestConcurrentAccess) {
    const int num_threads = 4;
    const int num_ops_per_thread = 1000;
    LookupConnectionCache cache(1024 * 1024 * 100); // 100MB buffer

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            // Mixed operations: 50% insert, 50% query
            for (int j = 0; j < num_ops_per_thread; ++j) {
                int64_t key = i * 1000 + j;
                auto reusable = ReusableTestHelper::create_reusable();

                if (j % 2 == 0) {
                    cache.add(key, reusable); // Insert
                } else {
                    auto entry = cache.get(key); // Query
                    EXPECT_TRUE(entry == nullptr || entry.use_count() == 2);
                }
            }
        });
    }

    for (auto& t : threads) t.join();
}

// Test exceptional input handling
TEST_F(LookupConnectionCacheTest, PQTestInvalidKeys) {
    LookupConnectionCache cache(1024 * 1024);

    // Null value test
    cache.add(0, nullptr);

    // Oversized entry test (1MB limit)
    auto large_reusable = std::make_shared<Reusable>();
    large_reusable->_block_pool.resize(1024 * 1024); // 1MB block
    cache.add(1, large_reusable);
}

// Test key collision handling
TEST_F(LookupConnectionCacheTest, PQTestDuplicateAdd) {
    LookupConnectionCache cache(1024 * 1024);
    auto reusable1 = ReusableTestHelper::create_reusable();
    auto reusable2 = ReusableTestHelper::create_reusable();

    // Overwrite existing key
    cache.add(123, reusable1);
    cache.add(123, reusable2);

    auto entry = cache.get(123);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry.get(), reusable2.get()) << "Last write should win in key collision";
}

// Test reference counting mechanism
TEST_F(LookupConnectionCacheTest, PQTestEntryLifetime) {
    LookupConnectionCache cache(1024 * 1024);
    {
        auto reusable = ReusableTestHelper::create_reusable();
        cache.add(123, reusable);
        auto entry = cache.get(123);
        ASSERT_NE(entry, nullptr);
        EXPECT_EQ(entry.use_count(), 3); // Cache + local reference
    }                                    // Local reference released

    // Verify cache maintains ownership
    auto entry = cache.get(123);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry.use_count(), 2) << "Cache should maintain sole ownership after scope exit";
}

} // namespace doris
