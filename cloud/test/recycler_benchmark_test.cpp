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

#include <butil/guid.h>
#include <fmt/core.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest-spi.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "common/bvars.h"
#include "common/config.h"
#include "common/defer.h"
#include "common/simple_thread_pool.h"
#include "cpp/sync_point.h"
#include "meta-service/meta_service_schema.h"
#include "meta-service/txn_lazy_committer.h"
#include "meta-store/blob_message.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "recycler/recycler.h"
#include "recycler/s3_accessor.h"
#include "recycler/util.h"

namespace doris::cloud {
namespace {

const std::string kBenchmarkInstanceId = "recycler_benchmark_instance";
const std::string kBenchmarkResourceId = "recycler_benchmark_resource";
double recycler_benchmark_duration_tolerance_ratio = 0.5;

std::string get_env(const char* name) {
    const char* value = std::getenv(name);
    return value == nullptr ? "" : value;
}

std::string get_env_with_fallback(const char* primary, const char* fallback) {
    const char* value = std::getenv(primary);
    return value == nullptr ? get_env(fallback) : std::string(value);
}

struct BenchmarkS3Config {
    bool enabled = false;
    std::string access_key;
    std::string secret_key;
    std::string role_arn;
    std::string external_id;
    std::string endpoint;
    std::string provider;
    std::string bucket;
    std::string region;
    std::string prefix;
};

BenchmarkS3Config load_benchmark_s3_config() {
    BenchmarkS3Config config;
    config.enabled = get_env("ENABLE_S3_CLIENT") == "1";
    if (!config.enabled) {
        return config;
    }

    config.access_key = get_env("S3_AK");
    config.secret_key = get_env("S3_SK");
    config.role_arn = get_env("AWS_ROLE_ARN");
    config.external_id = get_env("AWS_EXTERNAL_ID");
    config.endpoint = get_env_with_fallback("S3_ENDPOINT", "AWS_ENDPOINT");
    config.provider = get_env("S3_PROVIDER");
    config.bucket = get_env_with_fallback("S3_BUCKET", "AWS_BUCKET");
    config.region = get_env_with_fallback("S3_REGION", "AWS_REGION");
    config.prefix = get_env_with_fallback("S3_PREFIX", "AWS_PREFIX");
    return config;
}

void set_obj_store_provider(const std::string& provider, ObjectStoreInfoPB* obj_info) {
    if (provider == "AZURE") {
        obj_info->set_provider(ObjectStoreInfoPB_Provider_AZURE);
    } else if (provider == "GCS") {
        obj_info->set_provider(ObjectStoreInfoPB_Provider_GCP);
    } else {
        obj_info->set_provider(ObjectStoreInfoPB_Provider_S3);
    }
}

// Number of recyclable rowsets seeded per branch.
constexpr int64_t kRowsetsPerBranch = 30000;
// Commit the seeded recycle rowset KVs in batches to keep each txn small.
constexpr int64_t kSeedCommitBatch = 2000;
constexpr int64_t kRowsetsPerPackedFile = 10;
constexpr int64_t kPackedFileCount = kRowsetsPerBranch / kRowsetsPerPackedFile;
constexpr int kMaxPackedRecyclePasses = 10;
static_assert(kRowsetsPerBranch % kRowsetsPerPackedFile == 0);
static_assert(kSeedCommitBatch % kRowsetsPerPackedFile == 0);
constexpr int64_t kBenchmarkIndexId = 20000;
constexpr int32_t kBenchmarkSchemaVersion = 1;
constexpr int64_t kBenchmarkDbId = 10000;

struct RecycleRowsetConfigGuard {
    RecycleRowsetConfigGuard()
            : old_enable_mark(config::enable_mark_delete_rowset_before_recycle),
              old_enable_abort(config::enable_abort_txn_and_job_for_delete_rowset_before_recycle) {
        config::enable_mark_delete_rowset_before_recycle = true;
        config::enable_abort_txn_and_job_for_delete_rowset_before_recycle = true;
    }

    ~RecycleRowsetConfigGuard() {
        config::enable_mark_delete_rowset_before_recycle = old_enable_mark;
        config::enable_abort_txn_and_job_for_delete_rowset_before_recycle = old_enable_abort;
    }

    bool old_enable_mark;
    bool old_enable_abort;
};

doris::TabletSchemaCloudPB make_benchmark_schema() {
    doris::TabletSchemaCloudPB schema;
    schema.set_schema_version(kBenchmarkSchemaVersion);
    schema.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V1);
    auto* index = schema.add_index();
    index->set_index_id(1);
    index->set_index_type(IndexType::INVERTED);
    return schema;
}

int put_benchmark_schema(TxnKv* txn_kv, const std::string& instance_id) {
    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        return -1;
    }
    std::string schema_key;
    meta_schema_key({instance_id, kBenchmarkIndexId, kBenchmarkSchemaVersion}, &schema_key);
    auto schema = make_benchmark_schema();
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg;
    put_schema_kv(code, msg, txn.get(), schema_key, schema);
    if (code != MetaServiceCode::OK) {
        return -1;
    }
    return txn->commit() == TxnErrorCode::TXN_OK ? 0 : -1;
}

int remove_benchmark_schema(TxnKv* txn_kv, const std::string& instance_id) {
    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        return -1;
    }
    std::string schema_key;
    meta_schema_key({instance_id, kBenchmarkIndexId, kBenchmarkSchemaVersion}, &schema_key);
    ValueBuf schema_value;
    if (blob_get(txn.get(), schema_key, &schema_value) != TxnErrorCode::TXN_OK) {
        return -1;
    }
    schema_value.remove(txn.get());
    return txn->commit() == TxnErrorCode::TXN_OK ? 0 : -1;
}

// Each value maps to one recyclable branch inside
// InstanceRecycler::recycle_rowsets()::handle_rowset_kv. Both mark and abort
// flags are enabled by RecycleRowsetConfigGuard for every scenario.
enum class RecycleRowsetBranch {
    // Old-version RecycleRowsetPB (no `type`) whose resource_id is empty: the
    // recycler removes the KV only, without touching object storage.
    kLegacyEmptyResource,
    // Old-version RecycleRowsetPB with a real resource_id: recycled through the
    // single-rowset delete_rowset_data_by_prefix path.
    kLegacyWithResource,
    // Already-marked PREPARE rowset without a related txn/job: recycled directly.
    kPrepareDirect,
    // New PREPARE rowset queued to be marked as recycled first
    // (enable_mark_delete_rowset_before_recycle = true).
    kPrepareMark,
    // New PREPARE rowset carrying a load_id that triggers the abort-txn/job path
    // (enable_abort_txn_and_job_for_delete_rowset_before_recycle = true).
    kPrepareAbort,
    // New COMPACT/DROP rowset with segments: recycled via the batched
    // delete_rowset_data path.
    kCompactedWithData,
    // New COMPACT/DROP rowset without segments: treated as an empty rowset and
    // removed by KV delete only.
    kCompactedEmpty,
};

// Build one RowsetMetaCloudPB for the given branch. Kept intentionally minimal
// and self-contained so this benchmark does not depend on recycler_test.cpp.
doris::RowsetMetaCloudPB make_rowset_meta(RecycleRowsetBranch branch, int64_t tablet_id,
                                          const std::string& rowset_id) {
    doris::RowsetMetaCloudPB meta;
    meta.set_rowset_id(0); // deprecated but required
    meta.set_rowset_id_v2(rowset_id);
    meta.set_tablet_id(tablet_id);
    meta.set_index_id(kBenchmarkIndexId);
    meta.set_schema_version(kBenchmarkSchemaVersion);
    meta.mutable_tablet_schema()->CopyFrom(make_benchmark_schema());
    meta.set_start_version(2);
    meta.set_end_version(2);
    meta.set_data_disk_size(1024);
    meta.set_index_disk_size(512);
    meta.set_total_disk_size(1536);
    if (branch == RecycleRowsetBranch::kPrepareDirect) {
        meta.set_is_recycled(true);
    }
    switch (branch) {
    case RecycleRowsetBranch::kCompactedEmpty:
        meta.set_num_segments(0);
        break;
    default:
        meta.set_num_segments(1);
        break;
    }
    // Only the branches that reach delete_rowset_data need a resolvable resource.
    if (branch != RecycleRowsetBranch::kLegacyEmptyResource) {
        meta.set_resource_id(kBenchmarkResourceId);
    }
    if (branch == RecycleRowsetBranch::kPrepareAbort) {
        // A load_id makes make_related_txn_or_job_abort_task emit a TXN abort
        // task; end_version != 1 is required to enter the abort branch.
        meta.mutable_load_id()->set_hi(tablet_id);
        meta.mutable_load_id()->set_lo(1);
        meta.set_txn_id(tablet_id);
    }
    return meta;
}

// Wrap a RowsetMetaCloudPB into a RecycleRowsetPB shaped for the branch.
RecycleRowsetPB make_recycle_rowset(RecycleRowsetBranch branch,
                                    const doris::RowsetMetaCloudPB& meta) {
    RecycleRowsetPB pb;
    pb.set_creation_time(1); // long expired once retention is 0 / immediate recycle
    pb.set_expiration(1);
    switch (branch) {
    case RecycleRowsetBranch::kLegacyEmptyResource:
        // Old-version layout: no `type`, resource_id left empty on purpose.
        pb.set_tablet_id(meta.tablet_id());
        pb.set_resource_id("");
        break;
    case RecycleRowsetBranch::kLegacyWithResource:
        // Old-version layout: no `type`, resource_id populated.
        pb.set_tablet_id(meta.tablet_id());
        pb.set_resource_id(meta.resource_id());
        break;
    case RecycleRowsetBranch::kPrepareDirect:
    case RecycleRowsetBranch::kPrepareMark:
    case RecycleRowsetBranch::kPrepareAbort:
        pb.set_type(RecycleRowsetPB::PREPARE);
        pb.mutable_rowset_meta()->CopyFrom(meta);
        break;
    case RecycleRowsetBranch::kCompactedWithData:
    case RecycleRowsetBranch::kCompactedEmpty:
        pb.set_type(RecycleRowsetPB::COMPACT);
        pb.mutable_rowset_meta()->CopyFrom(meta);
        if (branch == RecycleRowsetBranch::kCompactedWithData) {
            // Match production rowsets whose schema is stored separately in the schema KV.
            pb.mutable_rowset_meta()->clear_tablet_schema();
        }
        break;
    }
    return pb;
}

enum class DeleteBitmapVersion { kNone, kV1, kV2 };

void put_benchmark_delete_bitmap(Transaction* txn, const std::string& instance_id,
                                 const doris::RowsetMetaCloudPB& meta,
                                 DeleteBitmapVersion version) {
    switch (version) {
    case DeleteBitmapVersion::kNone:
        break;
    case DeleteBitmapVersion::kV1:
        txn->put(meta_delete_bitmap_key({instance_id, meta.tablet_id(), meta.rowset_id_v2(), 0, 0}),
                 "delete_bitmap_data");
        break;
    case DeleteBitmapVersion::kV2: {
        // As with segment/index files, metadata is enough to exercise deletion of
        // the standalone .dbm file. Keep bitmap storage independent of data packing.
        DeleteBitmapStoragePB storage;
        storage.set_store_in_fdb(false);
        blob_put(txn,
                 versioned::meta_delete_bitmap_key(
                         {instance_id, meta.tablet_id(), meta.rowset_id_v2()}),
                 storage, 0);
        break;
    }
    }
}

// Seed `count` recycle rowset KVs for one branch. Every rowset gets a distinct
// tablet_id so the per-tablet recycle batch limit never truncates the workload.
int seed_recycle_rowsets(TxnKv* txn_kv, const std::string& instance_id, RecycleRowsetBranch branch,
                         int64_t count, int64_t tablet_id_base, bool write_schema_kv = true,
                         DeleteBitmapVersion bitmap_version = DeleteBitmapVersion::kNone) {
    if (write_schema_kv && put_benchmark_schema(txn_kv, instance_id) != 0) {
        return -1;
    }
    std::unique_ptr<Transaction> txn;
    for (int64_t i = 0; i < count; ++i) {
        if (i % kSeedCommitBatch == 0) {
            if (txn) {
                if (txn->commit() != TxnErrorCode::TXN_OK) {
                    return -1;
                }
            }
            if (txn_kv->create_txn(&txn) != TxnErrorCode::TXN_OK) {
                return -1;
            }
        }
        int64_t tablet_id = tablet_id_base + i;
        std::string rowset_id = fmt::format("{:018d}", i);
        auto meta = make_rowset_meta(branch, tablet_id, rowset_id);
        auto pb = make_recycle_rowset(branch, meta);

        std::string key;
        recycle_rowset_key({instance_id, tablet_id, rowset_id}, &key);
        std::string val;
        pb.SerializeToString(&val);
        txn->put(key, val);
        put_benchmark_delete_bitmap(txn.get(), instance_id, meta, bitmap_version);

        if (branch == RecycleRowsetBranch::kPrepareAbort) {
            const auto txn_id = meta.txn_id();
            const auto label = fmt::format("recycler_benchmark_txn_{}", txn_id);
            TxnIndexPB index;
            index.mutable_tablet_index()->set_db_id(kBenchmarkDbId);
            index.mutable_tablet_index()->set_tablet_id(tablet_id);
            TxnInfoPB info;
            info.set_txn_id(txn_id);
            info.set_db_id(kBenchmarkDbId);
            info.set_label(label);
            info.set_status(TxnStatusPB::TXN_STATUS_PREPARED);
            info.set_prepare_time(1);
            info.set_timeout_ms(60000);
            TxnRunningPB running;
            running.set_timeout_time(info.prepare_time() + info.timeout_ms());
            TxnLabelPB txn_label;
            txn_label.add_txn_ids(txn_id);
            txn->put(txn_index_key({instance_id, txn_id}), index.SerializeAsString());
            txn->put(txn_info_key({instance_id, kBenchmarkDbId, txn_id}), info.SerializeAsString());
            txn->put(txn_running_key({instance_id, kBenchmarkDbId, txn_id}),
                     running.SerializeAsString());
            txn->atomic_set_ver_value(txn_label_key({instance_id, kBenchmarkDbId, label}),
                                      txn_label.SerializeAsString());
        }
    }
    if (txn && txn->commit() != TxnErrorCode::TXN_OK) {
        return -1;
    }
    return 0;
}

std::string benchmark_packed_file_path(int64_t file_id) {
    return fmt::format("data/packed_file/recycler_benchmark_{}.pack", file_id);
}

// Seed a complete packed file and its rowsets in the same transaction.
void put_packed_recycle_rowsets(Transaction* txn, const std::string& instance_id,
                                int64_t tablet_id_base, int64_t file_id,
                                DeleteBitmapVersion bitmap_version) {
    const auto path = benchmark_packed_file_path(file_id);
    PackedFileInfoPB packed_info;
    packed_info.set_resource_id(kBenchmarkResourceId);
    packed_info.set_state(PackedFileInfoPB::NORMAL);
    int64_t offset = 0;
    for (int64_t j = 0; j < kRowsetsPerPackedFile; ++j) {
        // Spread references across scan batches so different recycler workers can
        // contend on the same packed file KV, instead of handling them serially.
        const auto rowset_number = file_id + j * kPackedFileCount;
        const auto tablet_id = tablet_id_base + rowset_number;
        const auto rowset_id = fmt::format("{:018d}", rowset_number);
        auto meta = make_rowset_meta(RecycleRowsetBranch::kCompactedWithData, tablet_id, rowset_id);
        const std::pair<std::string, int64_t> files[] = {
                {segment_path(tablet_id, rowset_id, 0), meta.data_disk_size()},
                {inverted_index_path_v1(tablet_id, rowset_id, 0, 1, ""), meta.index_disk_size()}};
        for (const auto& [small_path, size] : files) {
            auto& location = (*meta.mutable_packed_slice_locations())[small_path];
            location.set_packed_file_path(path);
            location.set_offset(offset);
            location.set_size(size);
            auto* slice = packed_info.add_slices();
            slice->set_path(small_path);
            slice->set_offset(offset);
            slice->set_size(size);
            slice->set_deleted(false);
            slice->set_tablet_id(tablet_id);
            slice->set_rowset_id(rowset_id);
            offset += size;
        }
        auto rowset = make_recycle_rowset(RecycleRowsetBranch::kCompactedWithData, meta);
        txn->put(recycle_rowset_key({instance_id, tablet_id, rowset_id}),
                 rowset.SerializeAsString());
        put_benchmark_delete_bitmap(txn, instance_id, meta, bitmap_version);
    }
    packed_info.set_ref_cnt(packed_info.slices_size());
    packed_info.set_total_slice_num(packed_info.slices_size());
    packed_info.set_total_slice_bytes(offset);
    packed_info.set_remaining_slice_bytes(offset);
    txn->put(packed_file_key({instance_id, path}), packed_info.SerializeAsString());
}

class RecyclerBenchmarkTest : public ::testing::Test {
protected:
    struct RecycleMetrics {
        int64_t num = 0;
        int64_t bytes = 0;
    };

    struct BenchmarkResult {
        RecycleMetrics metrics;
        double elapsed_ms = 0;
    };

    using RecycleFunction = int (InstanceRecycler::*)();

    static const std::map<std::string, RecycleFunction>& recycle_functions() {
        static const std::map<std::string, RecycleFunction> functions = {
                {"recycle_cluster_snapshots", &InstanceRecycler::recycle_cluster_snapshots},
                {"recycle_operation_logs", &InstanceRecycler::recycle_operation_logs},
                {"recycle_indexes", &InstanceRecycler::recycle_indexes},
                {"recycle_partitions", &InstanceRecycler::recycle_partitions},
                {"recycle_tmp_rowsets", &InstanceRecycler::recycle_tmp_rowsets},
                {"recycle_rowsets", &InstanceRecycler::recycle_rowsets},
                {"recycle_packed_files", &InstanceRecycler::recycle_packed_files},
                {"abort_timeout_txn", &InstanceRecycler::abort_timeout_txn},
                {"recycle_expired_txn_label", &InstanceRecycler::recycle_expired_txn_label},
                {"recycle_copy_jobs", &InstanceRecycler::recycle_copy_jobs},
                {"recycle_stage", &InstanceRecycler::recycle_stage},
                {"recycle_expired_stage_objects", &InstanceRecycler::recycle_expired_stage_objects},
                {"recycle_versions", &InstanceRecycler::recycle_versions},
                {"recycle_restore_jobs", &InstanceRecycler::recycle_restore_jobs},
        };
        return functions;
    }

    void SetUp() override {
        old_force_immediate_recycle_ = config::force_immediate_recycle;
        old_retention_seconds_ = config::retention_seconds;

        const auto tolerance_env = get_env("recycler_benchmark_duration_tolerance_ratio");
        size_t parsed_size = 0;
        recycler_benchmark_duration_tolerance_ratio =
                tolerance_env.empty() ? 0.5 : std::stod(tolerance_env, &parsed_size);
        ASSERT_EQ(parsed_size, tolerance_env.size()) << "invalid recycler benchmark tolerance";
        ASSERT_TRUE(std::isfinite(recycler_benchmark_duration_tolerance_ratio));
        ASSERT_GE(recycler_benchmark_duration_tolerance_ratio, 0);

        config::force_immediate_recycle = true;
        config::retention_seconds = 0;

        instance_.set_instance_id(std::string(kBenchmarkInstanceId));
        auto* obj_info = instance_.add_obj_info();
        obj_info->set_id(kBenchmarkResourceId);
        const auto s3_config = load_benchmark_s3_config();
        ASSERT_NO_FATAL_FAILURE(configure_obj_info(s3_config, obj_info));

        s3_producer_pool_ = std::make_shared<SimpleThreadPool>(
                config::recycle_pool_parallelism, "recycler_benchmark_s3_producer_pool");
        recycle_tablet_pool_ = std::make_shared<SimpleThreadPool>(
                config::recycle_pool_parallelism, "recycler_benchmark_recycle_tablet_pool");
        group_recycle_function_pool_ = std::make_shared<SimpleThreadPool>(
                config::recycle_pool_parallelism, "recycler_benchmark_group_recycle_function_pool");
        ASSERT_EQ(s3_producer_pool_->start(), 0);
        ASSERT_EQ(recycle_tablet_pool_->start(), 0);
        ASSERT_EQ(group_recycle_function_pool_->start(), 0);

        thread_pool_group_ = RecyclerThreadPoolGroup(s3_producer_pool_, recycle_tablet_pool_,
                                                     group_recycle_function_pool_);
        ASSERT_NO_FATAL_FAILURE(init_recycler());
    }

    void init_recycler() {
        recycler_.reset();
        txn_lazy_committer_.reset();
        txn_kv_ = std::make_shared<MemTxnKv>();
        ASSERT_EQ(txn_kv_->init(), 0);
        txn_lazy_committer_ = std::make_shared<TxnLazyCommitter>(txn_kv_);
        recycler_ = std::make_unique<InstanceRecycler>(txn_kv_, instance_, thread_pool_group_,
                                                       txn_lazy_committer_);

        if (load_benchmark_s3_config().enabled) {
            auto s3_conf = S3Conf::from_obj_store_info(instance_.obj_info(0));
            ASSERT_TRUE(s3_conf.has_value());

            std::shared_ptr<S3Accessor> accessor;
            ASSERT_EQ(S3Accessor::create(std::move(*s3_conf), &accessor), 0);
            s3_accessor_ = std::move(accessor);
            recycler_->TEST_add_accessor(kBenchmarkResourceId, s3_accessor_);
        }
        ASSERT_EQ(recycler_->init(), 0);
    }

    template <typename Benchmark>
    void run_benchmark(const std::string& branch, Benchmark benchmark) {
        ::testing::TestPartResultArray failures;
        {
            ::testing::ScopedFakeTestPartResultReporter reporter(
                    ::testing::ScopedFakeTestPartResultReporter::INTERCEPT_ALL_THREADS, &failures);
            SCOPED_TRACE(branch);
            try {
                // Fatal assertions return from this lambda or the scenario, not the whole test.
                [&] {
                    if (reset_before_next_benchmark_) {
                        ASSERT_NO_FATAL_FAILURE(init_recycler());
                        reset_before_next_benchmark_ = false;
                    }
                    benchmark();
                }();
            } catch (const std::exception& e) {
                ADD_FAILURE() << "benchmark threw: " << e.what();
            } catch (...) {
                ADD_FAILURE() << "benchmark threw an unknown exception";
            }
        }
        for (int i = 0; i < failures.size(); ++i) {
            const auto& failure = failures.GetTestPartResult(i);
            if (failure.failed()) {
                benchmark_failures_ +=
                        fmt::format("branch={}\n{}\n", branch, ::testing::PrintToString(failure));
                // Failed seeding/recycling can leave KVs and caches behind. Start the next
                // scenario with fresh state so its results are independent of this failure.
                reset_before_next_benchmark_ = true;
            }
        }
    }

    static void configure_obj_info(const BenchmarkS3Config& s3_config,
                                   ObjectStoreInfoPB* obj_info) {
        if (!s3_config.enabled) {
            obj_info->set_prefix(kBenchmarkResourceId);
            return;
        }
        ASSERT_FALSE(s3_config.endpoint.empty());
        ASSERT_FALSE(s3_config.region.empty());
        ASSERT_FALSE(s3_config.bucket.empty());
        ASSERT_FALSE(s3_config.prefix.empty());
        ASSERT_TRUE((s3_config.access_key.empty() && s3_config.secret_key.empty()) ||
                    (!s3_config.access_key.empty() && !s3_config.secret_key.empty()));

        obj_info->set_ak(s3_config.access_key);
        obj_info->set_sk(s3_config.secret_key);
        obj_info->set_endpoint(s3_config.endpoint);
        obj_info->set_region(s3_config.region);
        obj_info->set_bucket(s3_config.bucket);
        obj_info->set_prefix(fmt::format("{}{}recycler_benchmark/{}", s3_config.prefix,
                                         s3_config.prefix.ends_with('/') ? "" : "/",
                                         butil::GenerateGUID()));
        set_obj_store_provider(s3_config.provider, obj_info);
        if (s3_config.access_key.empty()) {
            obj_info->set_role_arn(s3_config.role_arn);
            obj_info->set_external_id(s3_config.external_id);
            obj_info->set_cred_provider_type(CredProviderTypePB::INSTANCE_PROFILE);
        }
    }

    void TearDown() override {
        recycler_.reset();
        txn_lazy_committer_.reset();
        if (s3_accessor_) {
            // The accessor is rooted at this run's GUID directory, never the external prefix.
            const int ret = s3_accessor_->delete_all();
            if (ret != 0) {
                benchmark_failures_ += fmt::format("branch=s3_cleanup ret={} prefix={}\n", ret,
                                                   instance_.obj_info(0).prefix());
            }
            s3_accessor_.reset();
        }

        std::string report;
        for (const auto& [operation_type, branches] : benchmark_results_) {
            size_t branch_width = 24;
            for (const auto& entry : branches) {
                branch_width = std::max(branch_width, entry.first.size());
            }
            report += fmt::format("recycler benchmark: operation={}\n", operation_type);
            report += fmt::format("  {:<{}}  {:>12}  {:>13}  {:>16}\n", "branch", branch_width,
                                  "total_ms", "recycled_num", "recycled_bytes");
            double total_elapsed_ms = 0;
            for (const auto& [branch, result] : branches) {
                report += fmt::format("  {:<{}}  {:>12.2f}  {:>13}  {:>16}\n", branch, branch_width,
                                      result.elapsed_ms, result.metrics.num, result.metrics.bytes);
                total_elapsed_ms += result.elapsed_ms;
            }
            report += fmt::format("  {:<{}}  {:>12.2f}\n", "total_elapsed_ms", branch_width,
                                  total_elapsed_ms);
        }
        std::cout << report << std::flush;
        if (!benchmark_failures_.empty()) {
            ADD_FAILURE() << "recycler benchmark failures:\n" << benchmark_failures_;
        }

        thread_pool_group_ = {};
        if (group_recycle_function_pool_) {
            ASSERT_EQ(group_recycle_function_pool_->stop(), 0);
        }
        if (recycle_tablet_pool_) {
            ASSERT_EQ(recycle_tablet_pool_->stop(), 0);
        }
        if (s3_producer_pool_) {
            ASSERT_EQ(s3_producer_pool_->stop(), 0);
        }
        group_recycle_function_pool_.reset();
        recycle_tablet_pool_.reset();
        s3_producer_pool_.reset();
        txn_kv_.reset();

        config::force_immediate_recycle = old_force_immediate_recycle_;
        config::retention_seconds = old_retention_seconds_;
    }

    RecycleMetrics read_recycle_metrics(const std::string& operation_type) const {
        return {.num = g_bvar_recycler_instance_recycle_total_num_since_started.get(
                        {kBenchmarkInstanceId, operation_type}),
                .bytes = g_bvar_recycler_instance_recycle_total_bytes_since_started.get(
                        {kBenchmarkInstanceId, operation_type})};
    }

    void check_elapsed_ms(const std::string& branch, double actual_ms, double baseline_ms) {
        const double limit_ms = baseline_ms * (1 + recycler_benchmark_duration_tolerance_ratio);
        if (actual_ms > limit_ms) {
            benchmark_failures_ +=
                    fmt::format("branch={} actual_ms={:.2f} baseline_ms={:.2f} limit_ms={:.2f}\n",
                                branch, actual_ms, baseline_ms, limit_ms);
        }
    }

    // Time only the recycler call; seeding, assertions and metrics reads are excluded.
    void measure(const std::string& operation_type, const std::string& branch,
                 const std::string& phase) {
        SCOPED_TRACE(phase);
        const auto function_it = recycle_functions().find(operation_type);
        ASSERT_NE(function_it, recycle_functions().end())
                << "unknown recycler operation: " << operation_type;

        const auto before = read_recycle_metrics(operation_type);
        const auto start = std::chrono::steady_clock::now();
        const int ret = (recycler_.get()->*(function_it->second))();
        const auto elapsed =
                std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start);
        const auto after = read_recycle_metrics(operation_type);
        const RecycleMetrics metrics {.num = after.num - before.num,
                                      .bytes = after.bytes - before.bytes};
        auto& result = benchmark_results_[operation_type][branch];
        result.elapsed_ms += elapsed.count();
        result.metrics.num += metrics.num;
        result.metrics.bytes += metrics.bytes;
        ASSERT_EQ(ret, 0) << "recycler operation failed: " << operation_type;
    }

    void seed_packed_recycle_rowsets(int64_t tablet_id_base, DeleteBitmapVersion bitmap_version) {
        ASSERT_EQ(put_benchmark_schema(txn_kv_.get(), kBenchmarkInstanceId), 0);
        std::unique_ptr<Transaction> txn;
        for (int64_t file_id = 0; file_id < kPackedFileCount; ++file_id) {
            if (file_id % (kSeedCommitBatch / kRowsetsPerPackedFile) == 0) {
                if (txn) {
                    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
                }
                ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
            }
            put_packed_recycle_rowsets(txn.get(), kBenchmarkInstanceId, tablet_id_base, file_id,
                                       bitmap_version);
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    int64_t count_recycle_rowsets() {
        std::string begin = recycle_rowset_key({kBenchmarkInstanceId, 0, ""});
        const auto end =
                recycle_rowset_key({kBenchmarkInstanceId, std::numeric_limits<int64_t>::max(), ""});
        std::unique_ptr<Transaction> txn;
        if (txn_kv_->create_txn(&txn) != TxnErrorCode::TXN_OK) {
            return -1;
        }
        int64_t count = 0;
        std::unique_ptr<RangeGetIterator> it;
        do {
            if (txn->get(begin, end, &it) != TxnErrorCode::TXN_OK) {
                return -1;
            }
            count += it->size();
            begin = it->next_begin_key();
        } while (it->more());
        return count;
    }

    void check_recycled_bitmap_range(int64_t tablet_id_base) {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::unique_ptr<RangeGetIterator> it;
        ASSERT_EQ(txn->get(versioned::meta_delete_bitmap_key(
                                   {kBenchmarkInstanceId, tablet_id_base, ""}),
                           versioned::meta_delete_bitmap_key(
                                   {kBenchmarkInstanceId, tablet_id_base + kRowsetsPerBranch, ""}),
                           &it),
                  TxnErrorCode::TXN_OK);
        ASSERT_FALSE(it->has_next());
    }

    void check_marked_rowsets(int64_t tablet_id_base, int64_t count = kRowsetsPerBranch) {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        for (int64_t i = 0; i < count; ++i) {
            std::string value;
            ASSERT_EQ(txn->get(recycle_rowset_key({kBenchmarkInstanceId, tablet_id_base + i,
                                                   fmt::format("{:018d}", i)}),
                               &value),
                      TxnErrorCode::TXN_OK);
            RecycleRowsetPB rowset;
            ASSERT_TRUE(rowset.ParseFromString(value));
            ASSERT_TRUE(rowset.rowset_meta().is_recycled());
        }
    }

    void check_txn_status(int64_t txn_id_base, int64_t count, TxnStatusPB status) {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        for (int64_t i = 0; i < count; ++i) {
            const auto txn_id = txn_id_base + i;
            std::string value;
            ASSERT_EQ(
                    txn->get(txn_info_key({kBenchmarkInstanceId, kBenchmarkDbId, txn_id}), &value),
                    TxnErrorCode::TXN_OK);
            TxnInfoPB info;
            ASSERT_TRUE(info.ParseFromString(value));
            ASSERT_EQ(info.status(), status) << "txn_id=" << txn_id;
            const bool aborted = status == TxnStatusPB::TXN_STATUS_ABORTED;
            ASSERT_EQ(txn->get(txn_running_key({kBenchmarkInstanceId, kBenchmarkDbId, txn_id}),
                               &value),
                      aborted ? TxnErrorCode::TXN_KEY_NOT_FOUND : TxnErrorCode::TXN_OK);
            ASSERT_EQ(txn->get(recycle_txn_key({kBenchmarkInstanceId, kBenchmarkDbId, txn_id}),
                               &value),
                      aborted ? TxnErrorCode::TXN_OK : TxnErrorCode::TXN_KEY_NOT_FOUND);
        }
    }

    std::shared_ptr<MemTxnKv> txn_kv_;
    InstanceInfoPB instance_;
    RecyclerThreadPoolGroup thread_pool_group_;
    std::shared_ptr<SimpleThreadPool> s3_producer_pool_;
    std::shared_ptr<SimpleThreadPool> recycle_tablet_pool_;
    std::shared_ptr<SimpleThreadPool> group_recycle_function_pool_;
    std::shared_ptr<TxnLazyCommitter> txn_lazy_committer_;
    std::unique_ptr<InstanceRecycler> recycler_;
    std::shared_ptr<S3Accessor> s3_accessor_;
    std::map<std::string, std::map<std::string, BenchmarkResult>> benchmark_results_;
    std::string benchmark_failures_;
    bool reset_before_next_benchmark_ = false;
    double test_elapsed_ms_ = 0;

    bool old_force_immediate_recycle_ = false;
    int64_t old_retention_seconds_ = 0;
};

// Non-overlapping tablet id ranges per branch so a single recycle_rowsets() run
// can cover several branches at once without id collisions.
constexpr int64_t kTabletBase = 1'000'000;
constexpr int64_t kTabletStride = 1'000'000'000LL;

int64_t tablet_base_for(RecycleRowsetBranch branch) {
    return kTabletBase + static_cast<int64_t>(branch) * kTabletStride;
}

TEST_F(RecyclerBenchmarkTest, RecycleRowsets) {
    RecycleRowsetConfigGuard config_guard;
    const auto test_start = std::chrono::steady_clock::now();
    DORIS_CLOUD_DEFER {
        const auto elapsed = std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - test_start);
        // Includes seeding and validation, unlike the sum of timed recycler calls.
        test_elapsed_ms_ = elapsed.count();
    };

    run_benchmark("legacy_empty_resource", [&] {
        ASSERT_EQ(seed_recycle_rowsets(txn_kv_.get(), kBenchmarkInstanceId,
                                       RecycleRowsetBranch::kLegacyEmptyResource, kRowsetsPerBranch,
                                       tablet_base_for(RecycleRowsetBranch::kLegacyEmptyResource)),
                  0);
        ASSERT_NO_FATAL_FAILURE(measure("recycle_rowsets", "legacy_empty_resource", "delete"));
        ASSERT_EQ(count_recycle_rowsets(), 0);
    });

    run_benchmark("legacy_with_resource", [&] {
        ASSERT_EQ(seed_recycle_rowsets(txn_kv_.get(), kBenchmarkInstanceId,
                                       RecycleRowsetBranch::kLegacyWithResource, kRowsetsPerBranch,
                                       tablet_base_for(RecycleRowsetBranch::kLegacyWithResource)),
                  0);
        ASSERT_NO_FATAL_FAILURE(measure("recycle_rowsets", "legacy_with_resource", "delete"));
        ASSERT_EQ(count_recycle_rowsets(), 0);
    });

    run_benchmark("prepare_direct", [&] {
        ASSERT_EQ(seed_recycle_rowsets(txn_kv_.get(), kBenchmarkInstanceId,
                                       RecycleRowsetBranch::kPrepareDirect, kRowsetsPerBranch,
                                       tablet_base_for(RecycleRowsetBranch::kPrepareDirect)),
                  0);
        ASSERT_NO_FATAL_FAILURE(measure("recycle_rowsets", "prepare_direct", "delete"));
        ASSERT_EQ(count_recycle_rowsets(), 0);
    });

    run_benchmark("prepare_mark", [&] {
        const auto tablet_id_base = tablet_base_for(RecycleRowsetBranch::kPrepareMark);
        ASSERT_EQ(seed_recycle_rowsets(txn_kv_.get(), kBenchmarkInstanceId,
                                       RecycleRowsetBranch::kPrepareMark, kRowsetsPerBranch,
                                       tablet_id_base),
                  0);
        ASSERT_NO_FATAL_FAILURE(measure("recycle_rowsets", "prepare_mark", "mark"));
        ASSERT_EQ(count_recycle_rowsets(), kRowsetsPerBranch);
        ASSERT_NO_FATAL_FAILURE(check_marked_rowsets(tablet_id_base));
        ASSERT_NO_FATAL_FAILURE(measure("recycle_rowsets", "prepare_mark", "delete"));
        ASSERT_EQ(count_recycle_rowsets(), 0);
    });

    run_benchmark("prepare_abort", [&] {
        const auto txn_id_base = tablet_base_for(RecycleRowsetBranch::kPrepareAbort);
        ASSERT_EQ(seed_recycle_rowsets(txn_kv_.get(), kBenchmarkInstanceId,
                                       RecycleRowsetBranch::kPrepareAbort, kRowsetsPerBranch,
                                       txn_id_base),
                  0);
        ASSERT_NO_FATAL_FAILURE(
                check_txn_status(txn_id_base, kRowsetsPerBranch, TxnStatusPB::TXN_STATUS_PREPARED));
        ASSERT_NO_FATAL_FAILURE(measure("recycle_rowsets", "prepare_abort", "mark"));
        ASSERT_EQ(count_recycle_rowsets(), kRowsetsPerBranch);
        ASSERT_NO_FATAL_FAILURE(check_marked_rowsets(txn_id_base));
        ASSERT_NO_FATAL_FAILURE(
                check_txn_status(txn_id_base, kRowsetsPerBranch, TxnStatusPB::TXN_STATUS_PREPARED));
        ASSERT_NO_FATAL_FAILURE(measure("recycle_rowsets", "prepare_abort", "abort_and_delete"));
        ASSERT_EQ(count_recycle_rowsets(), 0);
        ASSERT_NO_FATAL_FAILURE(
                check_txn_status(txn_id_base, kRowsetsPerBranch, TxnStatusPB::TXN_STATUS_ABORTED));
    });

    // Keep the missing-schema case separate from the bitmap ablation below.
    run_benchmark("compacted_without_schema", [&] {
        const auto tablet_id_base = tablet_base_for(RecycleRowsetBranch::kCompactedWithData);
        ASSERT_EQ(seed_recycle_rowsets(txn_kv_.get(), kBenchmarkInstanceId,
                                       RecycleRowsetBranch::kCompactedWithData, kRowsetsPerBranch,
                                       tablet_id_base),
                  0);
        ASSERT_EQ(remove_benchmark_schema(txn_kv_.get(), kBenchmarkInstanceId), 0);
        ASSERT_NO_FATAL_FAILURE(measure("recycle_rowsets", "compacted_without_schema", "delete"));
        ASSERT_EQ(count_recycle_rowsets(), 0);
    });

    // Hold rowset count, schema and file sizes fixed; vary only data packing and
    // bitmap version. V1 bitmap KVs survive rowset recycling, so isolate each case.
    for (const auto& [bitmap_version, bitmap_name] :
         {std::pair {DeleteBitmapVersion::kNone, "data_only"},
          std::pair {DeleteBitmapVersion::kV1, "delete_bitmap_v1"},
          std::pair {DeleteBitmapVersion::kV2, "delete_bitmap_v2"}}) {
        SCOPED_TRACE(bitmap_name);
        const auto tablet_id_base =
                tablet_base_for(RecycleRowsetBranch::kCompactedWithData) +
                (1 + 2 * static_cast<int64_t>(bitmap_version)) * kRowsetsPerBranch;
        run_benchmark(fmt::format("compacted_with_data/{}", bitmap_name), [&] {
            ASSERT_EQ(seed_recycle_rowsets(txn_kv_.get(), kBenchmarkInstanceId,
                                           RecycleRowsetBranch::kCompactedWithData,
                                           kRowsetsPerBranch, tablet_id_base, true, bitmap_version),
                      0);
            ASSERT_EQ(count_recycle_rowsets(), kRowsetsPerBranch);
            ASSERT_NO_FATAL_FAILURE(measure("recycle_rowsets",
                                            fmt::format("compacted_with_data/{}", bitmap_name),
                                            "delete"));
            ASSERT_EQ(count_recycle_rowsets(), 0);
            ASSERT_NO_FATAL_FAILURE(check_recycled_bitmap_range(tablet_id_base));
        });

        run_benchmark(fmt::format("compacted_with_packed_data/{}", bitmap_name), [&] {
            ASSERT_NO_FATAL_FAILURE(seed_packed_recycle_rowsets(tablet_id_base + kRowsetsPerBranch,
                                                                bitmap_version));

            int64_t remaining = count_recycle_rowsets();
            ASSERT_EQ(remaining, kRowsetsPerBranch);
            for (int pass = 1; pass <= kMaxPackedRecyclePasses && remaining > 0; ++pass) {
                const int64_t previous_remaining = remaining;
                // A worker may exhaust packed-file transaction retries while recycle_rowsets()
                // still returns success. Include every pass in the reported total elapsed time.
                ASSERT_NO_FATAL_FAILURE(
                        measure("recycle_rowsets",
                                fmt::format("compacted_with_packed_data/{}", bitmap_name),
                                fmt::format("delete_pass_{}", pass)));
                remaining = count_recycle_rowsets();
                ASSERT_GE(remaining, 0);
                ASSERT_LT(remaining, previous_remaining)
                        << "packed rowset recycling made no progress, pass=" << pass;
            }
            ASSERT_EQ(remaining, 0)
                    << "packed rowset recycling exceeded " << kMaxPackedRecyclePasses << " passes";

            // Check outside the timed calls that every packed file reached zero references.
            std::unique_ptr<Transaction> txn;
            ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
            for (int64_t file_id = 0; file_id < kPackedFileCount; ++file_id) {
                std::string value;
                ASSERT_EQ(txn->get(packed_file_key({kBenchmarkInstanceId,
                                                    benchmark_packed_file_path(file_id)}),
                                   &value),
                          TxnErrorCode::TXN_KEY_NOT_FOUND);
            }
            ASSERT_NO_FATAL_FAILURE(
                    check_recycled_bitmap_range(tablet_id_base + kRowsetsPerBranch));
        });
    }

    run_benchmark("compacted_empty", [&] {
        ASSERT_EQ(seed_recycle_rowsets(txn_kv_.get(), kBenchmarkInstanceId,
                                       RecycleRowsetBranch::kCompactedEmpty, kRowsetsPerBranch,
                                       tablet_base_for(RecycleRowsetBranch::kCompactedEmpty)),
                  0);
        ASSERT_NO_FATAL_FAILURE(measure("recycle_rowsets", "compacted_empty", "delete"));
        ASSERT_EQ(count_recycle_rowsets(), 0);
    });

    // Validate ordering outside the timed calls so callback KV reads do not skew timings.
    run_benchmark("prepare_abort_before_delete", [&] {
        const auto txn_id = tablet_base_for(RecycleRowsetBranch::kPrepareAbort) + kRowsetsPerBranch;
        ASSERT_EQ(seed_recycle_rowsets(txn_kv_.get(), kBenchmarkInstanceId,
                                       RecycleRowsetBranch::kPrepareAbort, 1, txn_id),
                  0);
        ASSERT_NO_FATAL_FAILURE(check_txn_status(txn_id, 1, TxnStatusPB::TXN_STATUS_PREPARED));
        ASSERT_EQ(recycler_->recycle_rowsets(), 0);
        ASSERT_EQ(count_recycle_rowsets(), 1);
        ASSERT_NO_FATAL_FAILURE(check_marked_rowsets(txn_id, 1));
        ASSERT_NO_FATAL_FAILURE(check_txn_status(txn_id, 1, TxnStatusPB::TXN_STATUS_PREPARED));

        auto* sp = SyncPoint::get_instance();
        std::atomic<int> rechecks {0};
        DORIS_CLOUD_DEFER {
            sp->clear_all_call_backs();
            sp->disable_processing();
        };
        sp->set_call_back("InstanceRecycler::batch_recheck_rowsets_after_abort", [&](auto&&) {
            ASSERT_NO_FATAL_FAILURE(check_txn_status(txn_id, 1, TxnStatusPB::TXN_STATUS_ABORTED));
            ASSERT_EQ(count_recycle_rowsets(), 1);
            ++rechecks;
        });
        sp->enable_processing();

        ASSERT_EQ(recycler_->recycle_rowsets(), 0);
        ASSERT_EQ(rechecks.load(), 1);
        ASSERT_EQ(count_recycle_rowsets(), 0);
    });

    // Keep mixed-workload IDs separate from the transactions aborted above.
    // Already-marked direct rowsets are deleted in the first pass.
    run_benchmark("mixed", [&] {
        constexpr int64_t mixed_tablet_offset = 7 * kTabletStride;
        ASSERT_EQ(seed_recycle_rowsets(
                          txn_kv_.get(), kBenchmarkInstanceId,
                          RecycleRowsetBranch::kLegacyEmptyResource, kRowsetsPerBranch,
                          mixed_tablet_offset +
                                  tablet_base_for(RecycleRowsetBranch::kLegacyEmptyResource)),
                  0);
        ASSERT_EQ(seed_recycle_rowsets(
                          txn_kv_.get(), kBenchmarkInstanceId,
                          RecycleRowsetBranch::kLegacyWithResource, kRowsetsPerBranch,
                          mixed_tablet_offset +
                                  tablet_base_for(RecycleRowsetBranch::kLegacyWithResource)),
                  0);
        ASSERT_EQ(
                seed_recycle_rowsets(
                        txn_kv_.get(), kBenchmarkInstanceId, RecycleRowsetBranch::kPrepareDirect,
                        kRowsetsPerBranch,
                        mixed_tablet_offset + tablet_base_for(RecycleRowsetBranch::kPrepareDirect)),
                0);
        const auto mark_tablet_id_base =
                mixed_tablet_offset + tablet_base_for(RecycleRowsetBranch::kPrepareMark);
        ASSERT_EQ(seed_recycle_rowsets(txn_kv_.get(), kBenchmarkInstanceId,
                                       RecycleRowsetBranch::kPrepareMark, kRowsetsPerBranch,
                                       mark_tablet_id_base),
                  0);
        const auto txn_id_base =
                mixed_tablet_offset + tablet_base_for(RecycleRowsetBranch::kPrepareAbort);
        ASSERT_EQ(seed_recycle_rowsets(txn_kv_.get(), kBenchmarkInstanceId,
                                       RecycleRowsetBranch::kPrepareAbort, kRowsetsPerBranch,
                                       txn_id_base),
                  0);
        ASSERT_EQ(seed_recycle_rowsets(
                          txn_kv_.get(), kBenchmarkInstanceId,
                          RecycleRowsetBranch::kCompactedWithData, kRowsetsPerBranch,
                          mixed_tablet_offset +
                                  tablet_base_for(RecycleRowsetBranch::kCompactedWithData)),
                  0);
        ASSERT_EQ(
                seed_recycle_rowsets(txn_kv_.get(), kBenchmarkInstanceId,
                                     RecycleRowsetBranch::kCompactedEmpty, kRowsetsPerBranch,
                                     mixed_tablet_offset +
                                             tablet_base_for(RecycleRowsetBranch::kCompactedEmpty)),
                0);
        ASSERT_NO_FATAL_FAILURE(measure("recycle_rowsets", "mixed", "mark_and_delete_ready"));
        ASSERT_EQ(count_recycle_rowsets(), 2 * kRowsetsPerBranch);
        ASSERT_NO_FATAL_FAILURE(check_marked_rowsets(mark_tablet_id_base));
        ASSERT_NO_FATAL_FAILURE(check_marked_rowsets(txn_id_base));
        ASSERT_NO_FATAL_FAILURE(
                check_txn_status(txn_id_base, kRowsetsPerBranch, TxnStatusPB::TXN_STATUS_PREPARED));
        ASSERT_NO_FATAL_FAILURE(measure("recycle_rowsets", "mixed", "abort_and_delete_prepare"));
        ASSERT_NO_FATAL_FAILURE(
                check_txn_status(txn_id_base, kRowsetsPerBranch, TxnStatusPB::TXN_STATUS_ABORTED));
        ASSERT_EQ(count_recycle_rowsets(), 0);
    });

    // Recorded with 30,000 rowsets per branch. Recalibrate if the workload changes.
    static_assert(kRowsetsPerBranch == 30000);
    const std::map<std::string, double> baseline_elapsed_ms = {
            {"compacted_empty", 3310.68},
            {"compacted_with_data/data_only", 6904.55},
            {"compacted_with_data/delete_bitmap_v1", 7065.21},
            {"compacted_with_data/delete_bitmap_v2", 9579.79},
            {"compacted_with_packed_data/data_only", 15503.91},
            {"compacted_with_packed_data/delete_bitmap_v1", 18272.14},
            {"compacted_with_packed_data/delete_bitmap_v2", 21364.90},
            {"compacted_without_schema", 9709.03},
            {"legacy_empty_resource", 3632.59},
            {"legacy_with_resource", 5942.18},
            {"mixed", 55093.53},
            {"prepare_abort", 23580.78},
            {"prepare_direct", 5880.97},
            {"prepare_mark", 9693.66},
    };
    const auto& results = benchmark_results_["recycle_rowsets"];
    double total_elapsed_ms = 0;
    for (const auto& [branch, baseline_ms] : baseline_elapsed_ms) {
        const auto result = results.find(branch);
        if (result == results.end()) {
            benchmark_failures_ +=
                    fmt::format("branch={} did not produce a timing result\n", branch);
            continue;
        }
        check_elapsed_ms(branch, result->second.elapsed_ms, baseline_ms);
        total_elapsed_ms += result->second.elapsed_ms;
    }
    check_elapsed_ms("total_elapsed_ms", total_elapsed_ms, 194452.59);
}

} // namespace
} // namespace doris::cloud
