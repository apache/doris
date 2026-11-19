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
#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/internal_service.pb.h>
#include <google/protobuf/service.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <fstream>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <mutex>
#include <roaring/roaring.hh>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cloud/cloud_engine_calc_delete_bitmap_task.h"
#include "cloud/cloud_ms_rpc_rate_limiters.h"
#include "cloud/cloud_rowset_builder.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablets_channel.h"
#include "cloud/cloud_txn_delete_bitmap_cache.h"
#include "cloud/config.h"
#include "cloud/pb_convert.h"
#include "common/check.h"
#include "common/config.h"
#include "common/status.h"
#include "core/field.h"
#include "cpp/obj-storage/client/s3_obj_storage_client.h"
#include "cpp/obj-storage/parser/obj_storage_config.h"
#include "cpp/sync_point.h"
#include "exec/sink/vtablet_finder.h"
#include "exec/sink/writer/vtablet_writer.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/obj_storage_file_system.h"
#include "io/object_storage/obj_storage_client_holder.h"
#include "load/delta_writer/delta_writer.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "storage/olap_common.h"
#include "storage/row_cursor.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/segment/test_segment_writer.h"
#include "storage/storage_policy.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/tablet_info.h"
#include "storage/txn/txn_manager.h"
#include "storage/utils.h"
#include "testutil/scoped_temp_dir.h"
#include "util/defer_op.h"
#include "util/uid_util.h"

namespace doris {
using namespace cloud;

namespace {

class TabletWriterOpenRpc : public google::protobuf::RpcChannel {
public:
    void CallMethod(const google::protobuf::MethodDescriptor* method,
                    google::protobuf::RpcController*, const google::protobuf::Message* request,
                    google::protobuf::Message*, google::protobuf::Closure* done) override {
        EXPECT_EQ(method, PBackendService::descriptor()->FindMethodByName("tablet_writer_open"));
        const auto* open_request = dynamic_cast<const PTabletWriterOpenRequest*>(request);
        EXPECT_NE(open_request, nullptr);
        if (open_request != nullptr) {
            requests.push_back(*open_request);
        }
        done->Run();
    }

    std::vector<PTabletWriterOpenRequest> requests;
};

class PendingSegmentClient final : public S3ObjStorageClient {
public:
    PendingSegmentClient() : S3ObjStorageClient(nullptr) {}

    ObjStorageHeadResult head_object(const ObjStoragePath& path) override {
        return {.resp = ObjStorageResponse::OK(),
                .file_size = static_cast<long long>(segments.at(path.key).size())};
    }

    ObjStorageResponse get_object(const ObjStoragePath& path, void* buffer, std::size_t offset,
                                  std::size_t size, std::size_t* bytes_read) override {
        const auto& data = segments.at(path.key);
        DORIS_CHECK(offset <= data.size() && size <= data.size() - offset);
        std::memcpy(buffer, data.data() + offset, size);
        *bytes_read = size;
        return ObjStorageResponse::OK();
    }

    std::unordered_map<std::string, std::string> segments;
};

} // namespace

TEST(CloudMow2PCTest, SinkFlagReachesInitialAndIncrementalDeltaWriters) {
    constexpr int64_t index_id = 100;
    constexpr int64_t first_tablet_id = 101;
    constexpr int64_t second_tablet_id = 102;
    CloudStorageEngine engine(EngineOptions {});
    RuntimeState state;
    for (bool fenced : {false, true}) {
        SCOPED_TRACE(fenced);
        RuntimeProfile profile("CloudMow2PC");
        TDataSink sink;
        TOlapTableSink olap_sink;
        if (fenced) {
            olap_sink.__set_is_cloud_mow_2pc(true);
        }
        sink.__set_olap_table_sink(olap_sink);
        VTabletWriter writer(sink, {}, nullptr, nullptr);
        writer._txn_id = 103;
        writer._sender_id = 0;
        writer._num_senders = 1;
        writer._txn_expiration = 1000;
        writer._schema = std::make_shared<OlapTableSchemaParam>();
        POlapTableSchemaParam schema;
        auto* index = schema.add_indexes();
        index->set_id(index_id);
        index->set_schema_hash(0);
        schema.mutable_tuple_desc()->set_id(0);
        ASSERT_TRUE(writer._schema->init(schema).ok());
        OlapTableLocationParam location(writer._t_sink.olap_table_sink.location);
        writer._location = &location;
        writer._tablet_finder = std::make_unique<OlapTabletFinder>(
                nullptr, OlapTabletFinder::FindTabletMode::FIND_TABLET_EVERY_ROW);

        TabletWriterOpenRpc rpc;
        IndexChannel index_channel(&writer, index_id, nullptr);
        VNodeChannel node(&writer, &index_channel, 1, false);
        node._state = &state;
        node._stub = std::make_shared<PBackendService_Stub>(&rpc);
        TabletsChannelKey key(writer._load_id, index_id);
        CloudTabletsChannel tablets(engine, key, key.id, false, &profile);

        TTabletWithPartition tablet;
        tablet.__set_partition_id(1);
        tablet.__set_tablet_id(first_tablet_id);
        node.add_tablet(tablet);
        node.open();
        ASSERT_EQ(rpc.requests.size(), 1U);
        EXPECT_FALSE(rpc.requests[0].is_incremental());
        EXPECT_EQ(rpc.requests[0].has_is_cloud_mow_2pc(), fenced);
        auto status = tablets.open(rpc.requests[0]);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_TRUE(tablets._tablet_writers.contains(first_tablet_id));
        EXPECT_EQ(tablets._tablet_writers.at(first_tablet_id)->_req.is_cloud_mow_2pc, fenced);

        tablet.__set_tablet_id(second_tablet_id);
        node.add_tablet(tablet);
        node.incremental_open();
        ASSERT_EQ(rpc.requests.size(), 2U);
        EXPECT_TRUE(rpc.requests[1].is_incremental());
        EXPECT_EQ(rpc.requests[1].has_is_cloud_mow_2pc(), fenced);
        status = tablets.incremental_open(rpc.requests[1]);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_TRUE(tablets._tablet_writers.contains(second_tablet_id));
        EXPECT_EQ(tablets._tablet_writers.at(second_tablet_id)->_req.is_cloud_mow_2pc, fenced);
    }
}

namespace {

enum class WriterSnapshot {
    BEFORE_COMPACTION,
    AFTER_COMPACTION,
    BEFORE_MATCHING_ROWSET,
    BEFORE_MATCHING_COMPACTION,
    BEFORE_HIGHER_SEQUENCE_COMPACTION,
    NON_SEQUENCE_MULTI_SEGMENT,
    NON_SEQUENCE_MULTI_SEGMENT_DUPLICATE,
    SEQUENCE_MULTI_SEGMENT,
};

void check_fenced_publish(WriterSnapshot snapshot) {
    constexpr int64_t txn_id = 12345;
    constexpr int64_t one_phase_txn_id = txn_id + 1;
    constexpr int64_t attempt = -42;
    constexpr int64_t writer_version = 2;
    const bool intra_load_duplicate =
            snapshot == WriterSnapshot::NON_SEQUENCE_MULTI_SEGMENT_DUPLICATE;
    const bool has_sequence = snapshot != WriterSnapshot::BEFORE_MATCHING_ROWSET &&
                              snapshot != WriterSnapshot::NON_SEQUENCE_MULTI_SEGMENT &&
                              !intra_load_duplicate;
    const bool multi_segment = snapshot == WriterSnapshot::NON_SEQUENCE_MULTI_SEGMENT ||
                               snapshot == WriterSnapshot::SEQUENCE_MULTI_SEGMENT ||
                               intra_load_duplicate;
    const bool higher_sequence = snapshot == WriterSnapshot::BEFORE_HIGHER_SEQUENCE_COMPACTION;
    const bool matching_compaction = snapshot == WriterSnapshot::BEFORE_MATCHING_COMPACTION;
    const bool has_sequence_mark = snapshot == WriterSnapshot::BEFORE_COMPACTION ||
                                   snapshot == WriterSnapshot::AFTER_COMPACTION || higher_sequence;
    const bool replaces_baseline = has_sequence_mark || matching_compaction;
    const int64_t version = writer_version + (replaces_baseline ? 1 : 2);
    EngineOptions engine_options;
    engine_options.backend_uid = UniqueId::gen_uid();
    CloudStorageEngine engine(engine_options);
    if (multi_segment) {
        // Multi-segment publish uses a bitmap executor that the engine constructor leaves unset.
        engine.init_calc_delete_bitmap_executor_for_UT();
    }
    engine._txn_delete_bitmap_cache = std::make_unique<CloudTxnDeleteBitmapCache>(1024U * 1024U);
    auto& cache = engine.txn_delete_bitmap_cache();
    ASSERT_TRUE(cache.init().ok());
    TColumn key;
    key.__set_column_name("k");
    key.__set_is_key(true);
    key.column_type.type = TPrimitiveType::INT;
    TColumn sequence;
    sequence.__set_column_name(SEQUENCE_COL);
    sequence.__set_is_key(false);
    sequence.__set_aggregation_type(TAggregationType::REPLACE);
    sequence.column_type.type = TPrimitiveType::INT;
    TTabletSchema schema;
    schema.__set_short_key_column_count(1);
    schema.__set_keys_type(TKeysType::UNIQUE_KEYS);
    if (has_sequence) {
        schema.__set_columns({key, sequence});
        schema.__set_sequence_col_idx(1);
    } else {
        schema.__set_columns({key});
    }
    auto tablet_meta = std::make_shared<TabletMeta>(
            1001, 2, 15673, 15674, 4, 5, schema, 2U,
            std::unordered_map<uint32_t, uint32_t> {{0, 0}, {1, 1}}, UniqueId(9, 10),
            TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F, 0, true);
    auto tablet = std::make_shared<CloudTablet>(engine, tablet_meta);
    ASSERT_TRUE(tablet->set_tablet_state(TABLET_RUNNING).ok());
    ASSERT_EQ(tablet->tablet_schema()->has_sequence_col(), has_sequence);

    auto make_visible_rowset = [&](Version visible_version, RowsetSharedPtr* result) {
        auto visible_meta = std::make_shared<RowsetMeta>();
        visible_meta->set_rowset_type(BETA_ROWSET);
        visible_meta->set_rowset_state(VISIBLE);
        visible_meta->set_rowset_id(engine.next_rowset_id());
        visible_meta->set_version(visible_version);
        visible_meta->set_tablet_schema(tablet->tablet_schema());
        return RowsetFactory::create_rowset(tablet->tablet_schema(), "", visible_meta, result);
    };
    RowsetSharedPtr initial;
    RowsetSharedPtr baseline_rowset;
    RowsetSharedPtr compacted;
    ASSERT_TRUE(make_visible_rowset(Version(0, 1), &initial).ok());
    ASSERT_TRUE(
            make_visible_rowset(Version(writer_version, writer_version), &baseline_rowset).ok());
    {
        std::unique_lock lock(tablet->get_header_lock());
        tablet->add_rowsets({initial, baseline_rowset}, false, lock, false);
    }
    if (has_sequence_mark && !higher_sequence) {
        ASSERT_TRUE(make_visible_rowset(Version(0, writer_version), &compacted).ok());
    }

    test::ScopedTempDirectory temp("cloud_mow_2pc");
    auto segment_client = std::make_shared<PendingSegmentClient>();
    constexpr int64_t resource_id = 15674012345;
    ObjStorageResourceConfig storage_config {};
    storage_config.bucket = "test";
    auto fs = std::shared_ptr<io::ObjStorageFileSystem>(
            new io::ObjStorageFileSystem(std::move(storage_config), std::to_string(resource_id)));
    fs->client_holder()->_client = segment_client;
    const StorageResource resource(fs);
    put_storage_resource(resource_id, resource, 0);
    Defer remove_resource([&] { delete_storage_resource(resource_id); });

    auto init_row = [&](RowCursor* row, int32_t key_value, int32_t sequence_value) {
        std::vector<Field> fields {Field::create_field<TYPE_INT>(key_value)};
        if (has_sequence) {
            fields.push_back(Field::create_field<TYPE_INT>(sequence_value));
        }
        return row->init_scan_key(tablet->tablet_schema(), std::move(fields));
    };
    auto write_segment = [&](const RowsetId& id, uint32_t segment_id, const RowCursor& row) {
        const auto object_key =
                resource.remote_segment_path(tablet->tablet_id(), id.to_string(), segment_id);
        const auto segment_path =
                temp.path() / (id.to_string() + "_" + std::to_string(segment_id) + ".dat");
        io::FileWriterPtr file;
        ASSERT_TRUE(io::global_local_filesystem()->create_file(segment_path, &file).ok());
        segment_v2::VerticalSegmentWriterOptions options;
        options.enable_unique_key_merge_on_write = true;
        segment_v2::TestVerticalSegmentWriter writer(file.get(), segment_id,
                                                     tablet->tablet_schema(), nullptr, nullptr,
                                                     options, nullptr);
        ASSERT_TRUE(writer.init().ok());
        ASSERT_TRUE(writer.append_row(row).ok());
        uint64_t index_size = 0;
        uint64_t file_size = 0;
        ASSERT_TRUE(writer.finalize_columns(&index_size).ok());
        ASSERT_TRUE(writer.finalize_footer(&file_size).ok());
        ASSERT_TRUE(file->close().ok());
        std::ifstream segment_input(segment_path, std::ios::binary);
        auto& data = segment_client->segments[object_key];
        data.assign(std::istreambuf_iterator<char>(segment_input),
                    std::istreambuf_iterator<char>());
        ASSERT_EQ(data.size(), file_size);
        ASSERT_GT(file_size, 0U);
    };

    const auto rowset_id = engine.next_rowset_id();
    RowCursor pending_row;
    ASSERT_TRUE(init_row(&pending_row, 1, 5).ok());
    ASSERT_NO_FATAL_FAILURE(write_segment(rowset_id, 0U, pending_row));
    if (multi_segment) {
        RowCursor second_row;
        ASSERT_TRUE(init_row(&second_row, intra_load_duplicate ? 1 : 2, 5).ok());
        ASSERT_NO_FATAL_FAILURE(write_segment(rowset_id, 1U, second_row));
    }
    auto meta = std::make_shared<RowsetMeta>();
    meta->set_rowset_type(BETA_ROWSET);
    meta->set_rowset_state(COMMITTED);
    meta->set_rowset_id(rowset_id);
    meta->set_tablet_id(tablet->tablet_id());
    meta->set_tablet_schema(tablet->tablet_schema());
    meta->set_num_segments(multi_segment ? 2 : 1);
    meta->set_num_rows(multi_segment ? 2 : 1);
    meta->set_resource_id(std::to_string(resource_id));
    RowsetSharedPtr pending;
    auto status = RowsetFactory::create_rowset(tablet->tablet_schema(), "", meta, &pending);
    ASSERT_TRUE(status.ok()) << status;

    RowsetSharedPtr matching_rowset;
    if (!has_sequence_mark || higher_sequence) {
        auto matching_meta = std::make_shared<RowsetMeta>();
        ASSERT_TRUE(matching_meta->init(meta.get()));
        matching_meta->set_rowset_id(engine.next_rowset_id());
        matching_meta->set_rowset_state(VISIBLE);
        matching_meta->set_version(Version(replaces_baseline ? 0 : version - 1, version - 1));
        matching_meta->set_num_segments(1);
        matching_meta->set_num_rows(1);
        RowCursor matching_row;
        ASSERT_TRUE(init_row(&matching_row, 1, higher_sequence ? 6 : 5).ok());
        ASSERT_NO_FATAL_FAILURE(write_segment(matching_meta->rowset_id(), 0U, matching_row));
        std::string encoded_key;
        matching_row.encode_key<true>(&encoded_key, tablet->tablet_schema()->num_key_columns());
        KeyBoundsPB bounds;
        bounds.set_min_key(encoded_key);
        bounds.set_max_key(encoded_key);
        matching_meta->add_segment_key_bounds(std::move(bounds));
        status = RowsetFactory::create_rowset(tablet->tablet_schema(), "", matching_meta,
                                              &matching_rowset);
        ASSERT_TRUE(status.ok()) << status;
        if (replaces_baseline) {
            compacted = matching_rowset;
        }
    }

    const auto old_write_version = config::delete_bitmap_store_write_version;
    const auto old_retention = config::tablet_txn_info_min_expired_seconds;
    const auto old_file_cache = config::enable_file_cache;
    const auto old_correctness_check = config::enable_merge_on_write_correctness_check;
    config::delete_bitmap_store_write_version = 1;
    config::tablet_txn_info_min_expired_seconds = 1800;
    config::enable_file_cache = false;
    config::enable_merge_on_write_correctness_check = false;
    auto* sync = SyncPoint::get_instance();
    Defer restore {[&] {
        sync->disable_processing();
        config::delete_bitmap_store_write_version = old_write_version;
        config::tablet_txn_info_min_expired_seconds = old_retention;
        config::enable_file_cache = old_file_cache;
        config::enable_merge_on_write_correctness_check = old_correctness_check;
    }};

    WriteRequest req {};
    req.tablet_id = tablet->tablet_id();
    req.txn_id = txn_id;
    const int64_t before = std::time(nullptr);
    req.txn_expiration = before + 3 * 24 * 60 * 60;
    req.is_cloud_mow_2pc = true;
    CloudRowsetBuilder builder(engine, req, nullptr);
    builder._tablet = tablet;
    builder._rowset = pending;
    builder._delete_bitmap = std::make_shared<DeleteBitmap>(req.tablet_id);
    const DeleteBitmap::BitmapKey pending_key {rowset_id, 0U, DeleteBitmap::TEMP_VERSION_COMMON};
    if (multi_segment) {
        tablet->add_sentinel_mark_to_delete_bitmap(builder._delete_bitmap.get(), {rowset_id});
    }
    auto install_compaction = [&] {
        std::unique_lock lock(tablet->get_header_lock());
        tablet->add_rowsets({compacted}, true, lock, false);
    };
    const DeleteBitmap::BitmapKey baseline_key {baseline_rowset->rowset_id(), 0U,
                                                DeleteBitmap::TEMP_VERSION_COMMON};
    if (has_sequence_mark) {
        // The writer saw a higher-sequence row and marked the incoming row deleted.
        builder._delete_bitmap->add(pending_key, 0);
    } else if (intra_load_duplicate) {
        // The duplicate in segment 1 supersedes the row in segment 0.
        builder._delete_bitmap->add(pending_key, 0);
    } else if (matching_compaction) {
        builder._delete_bitmap->add(baseline_key, 0);
    }
    if (snapshot == WriterSnapshot::AFTER_COMPACTION) {
        // The writer's commit-phase pass refreshed its rowset IDs and kept the pending mark.
        install_compaction();
    }
    ASSERT_TRUE(tablet->get_all_rs_id(writer_version, builder._rowset_ids.get()).ok());
    status = builder.set_txn_related_info();
    ASSERT_TRUE(status.ok()) << status;

    RowsetSharedPtr cached_rowset;
    DeleteBitmapPtr cached_bitmap;
    RowsetIdUnorderedSet cached_ids;
    std::shared_ptr<PartialUpdateInfo> partial_update;
    std::shared_ptr<PublishStatus> publish_status;
    TxnPublishInfo previous_publish;
    int64_t expiry = 0;
    status = cache.get_tablet_txn_info(txn_id, req.tablet_id, &cached_rowset, &cached_bitmap,
                                       &cached_ids, &expiry, &partial_update, &publish_status,
                                       &previous_publish);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(cached_rowset, pending);
    EXPECT_EQ(cached_ids, *builder._rowset_ids);
    EXPECT_EQ(cached_ids.contains(baseline_rowset->rowset_id()),
              snapshot != WriterSnapshot::AFTER_COMPACTION);
    EXPECT_EQ(cached_bitmap->contains(pending_key, 0), has_sequence_mark || intra_load_duplicate);
    EXPECT_EQ(cached_bitmap->contains(baseline_key, 0), matching_compaction);
    EXPECT_EQ(cached_bitmap->has_calculated_for_multi_segments(rowset_id), multi_segment);
    EXPECT_GE(expiry, before + 1800);
    EXPECT_LE(expiry, std::time(nullptr) + 1800);
    EXPECT_LT(expiry, req.txn_expiration);

    if (replaces_baseline && snapshot != WriterSnapshot::AFTER_COMPACTION) {
        install_compaction();
    } else if (!replaces_baseline) {
        std::unique_lock lock(tablet->get_header_lock());
        tablet->add_rowsets({matching_rowset}, false, lock, false);
    }
    RowsetIdUnorderedSet visible_ids;
    ASSERT_TRUE(tablet->get_all_rs_id(version - 1, &visible_ids).ok());
    EXPECT_EQ(visible_ids.contains(baseline_rowset->rowset_id()), !replaces_baseline);
    if (replaces_baseline) {
        EXPECT_TRUE(visible_ids.contains(compacted->rowset_id()));
        EXPECT_EQ(cached_ids.contains(compacted->rowset_id()),
                  snapshot == WriterSnapshot::AFTER_COMPACTION);
    } else {
        EXPECT_TRUE(visible_ids.contains(matching_rowset->rowset_id()));
        EXPECT_FALSE(cached_ids.contains(matching_rowset->rowset_id()));
    }

    WriteRequest one_phase_req = req;
    one_phase_req.txn_id = one_phase_txn_id;
    one_phase_req.is_cloud_mow_2pc = false;
    CloudRowsetBuilder one_phase_builder(engine, one_phase_req, nullptr);
    one_phase_builder._tablet = tablet;
    one_phase_builder._rowset = pending;
    one_phase_builder._delete_bitmap = std::make_shared<DeleteBitmap>(req.tablet_id);
    ASSERT_TRUE(one_phase_builder.set_txn_related_info().ok());

    int txn_reads = 0;
    int pending_rowset_reads = 0;
    SyncPoint::CallbackGuard get_guard;
    sync->set_call_back(
            "CloudMetaMgr::retry_rpc",
            [&](auto&& args) {
                auto rpc = *try_any_cast<MetaServiceRPC*>(args[0]);
                if (rpc == MetaServiceRPC::GET_TXN) {
                    ++txn_reads;
                    EXPECT_EQ(try_any_cast<const GetTxnRequest*>(args[1])->txn_id(), txn_id);
                    auto* response = try_any_cast<GetTxnResponse*>(args[2]);
                    auto* info = response->mutable_txn_info();
                    info->set_status(TxnStatusPB::TXN_STATUS_PRECOMMITTED);
                    info->set_is_mow_load(true);
                    auto* mapping = info->add_tablet_commit_infos();
                    mapping->set_tablet_id(req.tablet_id);
                    mapping->set_pending_rowset_id(rowset_id.to_string());
                } else {
                    ASSERT_EQ(rpc, MetaServiceRPC::GET_ROWSET);
                    ++pending_rowset_reads;
                    const auto* request = try_any_cast<const GetRowsetRequest*>(args[1]);
                    EXPECT_EQ(request->pending_txn_id(), txn_id);
                    EXPECT_EQ(request->idx().tablet_id(), req.tablet_id);
                    auto* response = try_any_cast<GetRowsetResponse*>(args[2]);
                    response->set_read_pending_rowset(true);
                    *response->add_rowset_meta() =
                            doris_rowset_meta_to_cloud(meta->get_rowset_pb());
                }
                *try_any_cast_ret<Status>(args) = {Status::OK(), true};
            },
            &get_guard);
    UpdateDeleteBitmapRequest persisted;
    SyncPoint::CallbackGuard update_guard;
    sync->set_call_back(
            "CloudMetaMgr::update_delete_bitmap.before_rpc",
            [&](auto&& args) {
                persisted.CopyFrom(*try_any_cast<UpdateDeleteBitmapRequest*>(args[0]));
                *try_any_cast_ret<Status>(args) = {Status::OK(), true};
            },
            &update_guard);
    sync->enable_processing();

    auto check_persisted_mark = [&](int index, const RowsetId& deleted_rowset_id) {
        EXPECT_EQ(persisted.rowset_ids(index), deleted_rowset_id.to_string());
        EXPECT_EQ(persisted.segment_ids(index), 0U);
        EXPECT_EQ(persisted.versions(index), version);
        const auto& encoded = persisted.segment_delete_bitmaps(index);
        const auto bitmap = roaring::Roaring::readSafe(encoded.data(), encoded.size());
        EXPECT_EQ(bitmap.cardinality(), 1U);
        EXPECT_TRUE(bitmap.contains(0U));
    };
    auto check_persisted_bitmap = [&] {
        EXPECT_EQ(persisted.initiator(), attempt);
        EXPECT_EQ(persisted.next_visible_version(), version);
        if (has_sequence_mark && !higher_sequence) {
            EXPECT_EQ(persisted.rowset_ids_size(), 0);
        } else {
            const int expected_marks = intra_load_duplicate ? 2 : 1;
            ASSERT_EQ(persisted.rowset_ids_size(), expected_marks);
            ASSERT_EQ(persisted.segment_ids_size(), expected_marks);
            ASSERT_EQ(persisted.versions_size(), expected_marks);
            ASSERT_EQ(persisted.segment_delete_bitmaps_size(), expected_marks);
            if (intra_load_duplicate) {
                const int pending_index = persisted.rowset_ids(0) == rowset_id.to_string() ? 0 : 1;
                check_persisted_mark(pending_index, rowset_id);
                check_persisted_mark(1 - pending_index, matching_rowset->rowset_id());
            } else {
                check_persisted_mark(0, higher_sequence ? rowset_id : matching_rowset->rowset_id());
            }
        }
    };

    TCalcDeleteBitmapRequest calc_request;
    calc_request.__set_transaction_id(txn_id);
    calc_request.__set_lock_initiator(attempt);
    std::vector<TTabletId> errors;
    CloudEngineCalcDeleteBitmapTask engine_task(engine, calc_request, &errors);
    CloudTabletCalcDeleteBitmapTask calc(engine, req.tablet_id, txn_id, version, {}, attempt,
                                         &engine_task);
    status = calc._handle_rowset(tablet, version);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(txn_reads, 1);
    EXPECT_EQ(pending_rowset_reads, has_sequence_mark ? 1 : 0);
    check_persisted_bitmap();
    EXPECT_EQ(cached_bitmap->contains(pending_key, 0), has_sequence_mark || intra_load_duplicate);
    EXPECT_EQ(cached_bitmap->contains(baseline_key, 0), matching_compaction);
    EXPECT_FALSE(meta->has_version());
    ASSERT_NE(publish_status, nullptr);
    EXPECT_EQ(*publish_status, PublishStatus::INIT);
    EXPECT_FALSE(cache.get_rowset_and_delete_bitmap(txn_id, req.tablet_id).has_value());

    // An intervening rowset can change the publish bitmap cardinality. The check compares
    // only a published cache entry, so the writer's baseline is not compared with that result.
    DeleteBitmap calculated(*cached_bitmap);
    calculated.add({baseline_rowset->rowset_id(), 0U, DeleteBitmap::TEMP_VERSION_COMMON}, 8);
    EXPECT_TRUE(tablet->check_delete_bitmap_cache(txn_id, &calculated).ok());

    tablet->discard_fenced_writer_baselines({txn_id, one_phase_txn_id});
    status = cache.get_tablet_txn_info(txn_id, req.tablet_id, &cached_rowset, &cached_bitmap,
                                       &cached_ids, &expiry, &partial_update, &publish_status,
                                       &previous_publish);
    EXPECT_TRUE(status.is<ErrorCode::NOT_FOUND>()) << status;
    status = cache.get_tablet_txn_info(one_phase_txn_id, req.tablet_id, &cached_rowset,
                                       &cached_bitmap, &cached_ids, &expiry, &partial_update,
                                       &publish_status, &previous_publish);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(cached_rowset, pending);
    EXPECT_EQ(expiry, one_phase_req.txn_expiration);
    persisted.Clear();
    CloudEngineCalcDeleteBitmapTask cold_engine_task(engine, calc_request, &errors);
    CloudTabletCalcDeleteBitmapTask cold_calc(engine, req.tablet_id, txn_id, version, {}, attempt,
                                              &cold_engine_task);
    status = cold_calc._handle_rowset(tablet, version);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(txn_reads, 2);
    EXPECT_EQ(pending_rowset_reads, has_sequence_mark ? 2 : 1);
    check_persisted_bitmap();
}

} // namespace

TEST(CloudMow2PCTest, CalcRecomputesSequenceBitmapWhenCompactionPrecedesCacheInsert) {
    check_fenced_publish(WriterSnapshot::AFTER_COMPACTION);
}

TEST(CloudMow2PCTest, CalcRecomputesSequenceBitmapWhenCompactionFollowsCacheInsert) {
    check_fenced_publish(WriterSnapshot::BEFORE_COMPACTION);
}

TEST(CloudMow2PCTest, CalcUsesNonSequenceBaselineAndDeletesNewlyVisibleMatch) {
    check_fenced_publish(WriterSnapshot::BEFORE_MATCHING_ROWSET);
}

TEST(CloudMow2PCTest, CalcUsesSequenceBaselineAndDeletesCompactedMatch) {
    check_fenced_publish(WriterSnapshot::BEFORE_MATCHING_COMPACTION);
}

TEST(CloudMow2PCTest, CalcRecomputesPendingDeleteAgainstHigherSequenceCompactedRow) {
    check_fenced_publish(WriterSnapshot::BEFORE_HIGHER_SEQUENCE_COMPACTION);
}

TEST(CloudMow2PCTest, CalcKeepsNonSequenceMultiSegmentBaselineWarm) {
    check_fenced_publish(WriterSnapshot::NON_SEQUENCE_MULTI_SEGMENT);
}

TEST(CloudMow2PCTest, CalcKeepsNonSequenceIntraLoadDeleteBaselineWarm) {
    check_fenced_publish(WriterSnapshot::NON_SEQUENCE_MULTI_SEGMENT_DUPLICATE);
}

TEST(CloudMow2PCTest, CalcKeepsSequenceSentinelOnlyBaselineWarm) {
    check_fenced_publish(WriterSnapshot::SEQUENCE_MULTI_SEGMENT);
}

} // namespace doris
