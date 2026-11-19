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

#include <brpc/controller.h>
#include <butil/guid.h>
#include <gen_cpp/cloud.pb.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>

#include "common/config.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-service/meta_service.h"
#include "meta-service/meta_service_helper.h"
#include "meta-store/blob_message.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/txn_kv.h"
#include "mock_resource_manager.h"
#include "rate-limiter/rate_limiter.h"
#include "snapshot/snapshot_manager.h"

namespace doris::cloud {
namespace {

// MemTxnKv normally accepts arbitrarily large transactions. Enforce the real size
// limits so one logical blob exceeding 10 MB cannot pass without being batched.
struct BitmapTxnLimits {
    size_t largest_commit = 0;
    size_t blob_commits = 0;
};

class BitmapSizeLimitedTransaction : public memkv::Transaction {
public:
    BitmapSizeLimitedTransaction(std::shared_ptr<MemTxnKv> kv, BitmapTxnLimits& limits)
            : memkv::Transaction(std::move(kv)), limits_(limits) {}

    void put(std::string_view key, std::string_view value) override {
        EXPECT_LE(value.size(), 100'000);
        memkv::Transaction::put(key, value);
    }

    TxnErrorCode commit() override {
        auto size = approximate_bytes();
        limits_.largest_commit = std::max(limits_.largest_commit, size);
        if (size > 10'000'000) {
            return TxnErrorCode::TXN_BYTES_TOO_LARGE;
        }
        auto err = memkv::Transaction::commit();
        if (err == TxnErrorCode::TXN_OK && put_bytes() > 100'000) {
            ++limits_.blob_commits;
        }
        return err;
    }

private:
    BitmapTxnLimits& limits_;
};

class BitmapSizeLimitedKv : public MemTxnKv {
public:
    TxnErrorCode create_txn(std::unique_ptr<Transaction>* txn) override {
        auto result = std::make_unique<BitmapSizeLimitedTransaction>(shared_from_this(), limits);
        EXPECT_EQ(result->init(), 0);
        *txn = std::move(result);
        return TxnErrorCode::TXN_OK;
    }

    BitmapTxnLimits limits;
};

class LargeDeleteBitmapTest : public testing::TestWithParam<bool> {
protected:
    void SetUp() override {
        old_limit_ = config::max_txn_commit_byte;
        old_cluster_file_ = config::fdb_cluster_file_path;
        config::max_txn_commit_byte = 7 * 1024 * 1024;
        if (GetParam()) {
            config::fdb_cluster_file_path = "fdb.cluster";
            static auto fdb_kv = std::make_shared<FdbTxnKv>();
            static const int init_result = fdb_kv->init();
            ASSERT_EQ(init_result, 0);
            kv_ = fdb_kv;
            instance_id_ = "large_bitmap_" + butil::GenerateGUID();
            auto* sp = SyncPoint::get_instance();
            sp->set_call_back("get_instance_id", [&](auto&& args) {
                auto* ret = try_any_cast_ret<std::string>(args);
                ret->first = instance_id_;
                ret->second = true;
            });
            sp->enable_processing();
            RecordProperty("storage_backend", "FoundationDB");
        } else {
            limited_kv_ = std::make_shared<BitmapSizeLimitedKv>();
            kv_ = limited_kv_;
            ASSERT_EQ(kv_->init(), 0);
            RecordProperty("storage_backend", "MemTxnKv");
        }
        auto resource = std::make_shared<MockResourceManager>(kv_);
        instance_id_ = get_instance_id(resource, "test_cloud_unique_id");
        auto service =
                std::make_unique<MetaServiceImpl>(kv_, resource, std::make_shared<RateLimiter>(),
                                                  std::make_shared<SnapshotManager>(kv_));
        service_ = std::make_unique<MetaServiceProxy>(std::move(service));
    }

    void TearDown() override {
        config::max_txn_commit_byte = old_limit_;
        config::fdb_cluster_file_path = old_cluster_file_;
        SyncPoint::get_instance()->clear_all_call_backs();
        SyncPoint::get_instance()->disable_processing();
        if (kv_ != nullptr && limited_kv_ == nullptr) {
            // Each FDB case owns a unique instance; never clear unrelated keys.
            std::unique_ptr<Transaction> txn;
            auto err = kv_->create_txn(&txn);
            EXPECT_EQ(err, TxnErrorCode::TXN_OK);
            if (err == TxnErrorCode::TXN_OK) {
                for (const auto& prefix :
                     {meta_key_prefix(instance_id_), versioned::meta_key_prefix(instance_id_)}) {
                    txn->remove(prefix, prefix + '\xff');
                }
                EXPECT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
            }
        }
    }

    void lock(int64_t lock_id = 1001) {
        GetDeleteBitmapUpdateLockRequest req;
        GetDeleteBitmapUpdateLockResponse res;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_table_id(100);
        req.add_partition_ids(101);
        req.set_lock_id(lock_id);
        req.set_initiator(103);
        req.set_expiration(60);
        brpc::Controller cntl;
        service_->get_delete_bitmap_update_lock(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
    }

    UpdateDeleteBitmapRequest request(const std::string& value, int64_t lock_id = 1001,
                                      bool v2 = false) {
        UpdateDeleteBitmapRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_table_id(100);
        req.set_partition_id(101);
        req.set_tablet_id(102);
        req.set_lock_id(lock_id);
        req.set_initiator(103);
        req.set_store_version(v2 ? 2 : 1);
        if (v2) {
            req.add_delta_rowset_ids("new_rowset");
            auto* storage = req.add_delete_bitmap_storages();
            storage->set_store_in_fdb(true);
            auto* bitmap = storage->mutable_delete_bitmap();
            bitmap->add_rowset_ids("old_rowset");
            bitmap->add_segment_ids(0);
            bitmap->add_versions(2);
            bitmap->add_segment_delete_bitmaps(value);
        } else {
            req.add_rowset_ids("old_rowset");
            req.add_segment_ids(0);
            req.add_versions(2);
            req.add_segment_delete_bitmaps(value);
        }
        return req;
    }

    UpdateDeleteBitmapResponse update(const UpdateDeleteBitmapRequest& req) {
        UpdateDeleteBitmapResponse res;
        brpc::Controller cntl;
        service_->update_delete_bitmap(&cntl, &req, &res, nullptr);
        return res;
    }

    void check_value(const std::string& expected, bool v2 = false) {
        GetDeleteBitmapRequest req;
        GetDeleteBitmapResponse res;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_tablet_id(102);
        req.set_store_version(v2 ? 2 : 1);
        req.add_rowset_ids(v2 ? "new_rowset" : "old_rowset");
        req.add_begin_versions(v2 ? 0 : 2);
        req.add_end_versions(2);
        brpc::Controller cntl;
        service_->get_delete_bitmap(&cntl, &req, &res, nullptr);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        if (v2) {
            ASSERT_EQ(res.delete_bitmap_storages_size(), 1);
            ASSERT_EQ(res.delete_bitmap_storages(0).delete_bitmap().segment_delete_bitmaps_size(),
                      1);
            EXPECT_EQ(res.delete_bitmap_storages(0).delete_bitmap().segment_delete_bitmaps(0),
                      expected);
        } else {
            ASSERT_EQ(res.segment_delete_bitmaps_size(), 1);
            EXPECT_EQ(res.segment_delete_bitmaps(0), expected);
        }
    }

    static std::string large_value(size_t size = 10'578'008) {
        std::string value(size, '\0');
        for (size_t offset = 0; offset < value.size(); ++offset) {
            value[offset] = static_cast<char>((offset / DEFAULT_BLOB_SPLIT_SIZE) % 251);
        }
        return value;
    }

    void write_and_check_large_value(size_t size, bool v2) {
        RecordProperty("value_bytes", std::to_string(size));
        lock();
        auto value = large_value(size);
        auto res = update(request(value, 1001, v2));
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        if (limited_kv_ != nullptr) {
            auto expected_commits =
                    (size + config::max_txn_commit_byte - 1) / config::max_txn_commit_byte;
            EXPECT_GE(limited_kv_->limits.blob_commits, expected_commits);
            EXPECT_LE(limited_kv_->limits.largest_commit, config::max_txn_commit_byte);
        }
        check_value(value, v2);
    }

    void write_and_check_two_values(bool v2) {
        constexpr size_t value_size = 6 * 1024 * 1024;
        RecordProperty("value_bytes", std::to_string(value_size));
        RecordProperty("logical_values", "2");
        lock();
        auto req = request(std::string(value_size, 'a'), 1001, v2);
        const std::string second(value_size, 'b');
        if (v2) {
            req.add_delta_rowset_ids("new_rowset_2");
            *req.add_delete_bitmap_storages() =
                    request(second, 1001, true).delete_bitmap_storages(0);
        } else {
            req.add_rowset_ids("old_rowset");
            req.add_segment_ids(1);
            req.add_versions(2);
            req.add_segment_delete_bitmaps(second);
        }

        auto res = update(req);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
        if (limited_kv_ != nullptr) {
            EXPECT_EQ(limited_kv_->limits.blob_commits, 2);
            EXPECT_LE(limited_kv_->limits.largest_commit, config::max_txn_commit_byte);
        }
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        for (int i = 0; i < 2; ++i) {
            auto key = v2 ? versioned::meta_delete_bitmap_key(
                                    {instance_id_, 102, req.delta_rowset_ids(i)})
                          : meta_delete_bitmap_key({instance_id_, 102, req.rowset_ids(i),
                                                    req.versions(i), req.segment_ids(i)});
            ValueBuf stored;
            ASSERT_EQ(blob_get(txn.get(), key, &stored), TxnErrorCode::TXN_OK);
            auto expected = v2 ? req.delete_bitmap_storages(i).SerializeAsString()
                               : req.segment_delete_bitmaps(i);
            EXPECT_EQ(stored.value(), expected);
        }
    }

    void check_overlapping_retry(bool after_validation, bool different_value = false) {
        const bool old_retry = config::enable_txn_store_retry;
        config::enable_txn_store_retry = false;
        DORIS_CLOUD_DEFER {
            config::enable_txn_store_retry = old_retry;
        };
        auto* sp = SyncPoint::get_instance();
        for (size_t size : {8UL * 1024 * 1024, 32UL * 1024 * 1024}) {
            for (bool v2 : {false, true}) {
                SCOPED_TRACE(testing::Message() << "size=" << size << " v2=" << v2);
                lock();
                const auto value = large_value(size);
                auto req = request(value, 1001, v2);
                bool delayed_handler = false;
                bool injected = false;
                size_t active_commits = 0;
                size_t delayed_commits = 0;
                auto run_delayed_handler = [&] {
                    injected = true;
                    delayed_handler = true;
                    auto delayed_req =
                            different_value ? request(std::string(size, 'r'), 1001, v2) : req;
                    // Reenter the RPC while the active handler is paused. Let the
                    // delayed handler commit pending cleanup, optionally rewrite a
                    // prefix, then stop it before it can finish the bitmap.
                    auto delayed_res = update(delayed_req);
                    EXPECT_EQ(delayed_res.status().code(), MetaServiceCode::KV_TXN_CONFLICT)
                            << delayed_res.status().msg();
                    delayed_handler = false;
                };
                sp->set_call_back("update_delete_bitmap:commit:err", [&](auto&& args) {
                    if (delayed_handler) {
                        if (++delayed_commits == (different_value ? 2 : 1)) {
                            *try_any_cast<TxnErrorCode*>(args[2]) = TxnErrorCode::TXN_CONFLICT;
                        }
                    } else if (++active_commits == 2 && !after_validation) {
                        run_delayed_handler();
                    }
                });
                sp->set_call_back("update_delete_bitmap:check_prefix:after", [&](auto&&) {
                    if (after_validation && !injected) {
                        run_delayed_handler();
                    }
                });
                sp->enable_processing();
                auto res = update(req);
                EXPECT_TRUE(injected);
                EXPECT_EQ(res.status().code(), MetaServiceCode::KV_TXN_CONFLICT)
                        << res.status().msg();
                sp->clear_call_back("update_delete_bitmap:commit:err");
                sp->clear_call_back("update_delete_bitmap:check_prefix:after");

                res = update(req);
                ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
                check_value(value, v2);
            }
        }
    }

    int64_t old_limit_ = 0;
    std::string old_cluster_file_;
    std::shared_ptr<TxnKv> kv_;
    std::shared_ptr<BitmapSizeLimitedKv> limited_kv_;
    std::unique_ptr<MetaServiceProxy> service_;
    std::string instance_id_;
};

TEST_P(LargeDeleteBitmapTest, SingleV1BitmapExceedsTransactionLimit) {
    write_and_check_large_value(10'578'008, false);
}

TEST_P(LargeDeleteBitmapTest, SingleV2StorageExceedsTransactionLimit) {
    write_and_check_large_value(10'578'008, true);
}

TEST_P(LargeDeleteBitmapTest, SingleV1BitmapSpansSeveralTransactions) {
    write_and_check_large_value(32 * 1024 * 1024, false);
}

TEST_P(LargeDeleteBitmapTest, SingleV2StorageSpansSeveralTransactions) {
    write_and_check_large_value(32 * 1024 * 1024, true);
}

TEST_P(LargeDeleteBitmapTest, TwoV1BitmapsExceedCombinedTransactionLimit) {
    write_and_check_two_values(false);
}

TEST_P(LargeDeleteBitmapTest, TwoV2StoragesExceedCombinedTransactionLimit) {
    write_and_check_two_values(true);
}

TEST_P(LargeDeleteBitmapTest, DualWriteExceedsTransactionLimit) {
    lock();
    auto value = large_value();
    auto req = request(value);
    auto v2 = request(value, 1001, true);
    req.set_store_version(3);
    req.add_delta_rowset_ids(v2.delta_rowset_ids(0));
    *req.add_delete_bitmap_storages() = v2.delete_bitmap_storages(0);
    auto res = update(req);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
    check_value(value);
    check_value(value, true);
}

TEST_P(LargeDeleteBitmapTest, CompactionBitmapExceedsTransactionLimit) {
    lock(-1);
    auto value = large_value();
    auto res = update(request(value, -1));
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
    check_value(value);
}

TEST_P(LargeDeleteBitmapTest, SchemaChangeBitmapExceedsTransactionLimit) {
    lock(-2);
    auto value = large_value();
    auto res = update(request(value, -2));
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
    check_value(value);
}

TEST_P(LargeDeleteBitmapTest, PendingCleanupBeforePrefixValidation) {
    check_overlapping_retry(false);
}

TEST_P(LargeDeleteBitmapTest, PendingCleanupAfterPrefixValidation) {
    if (!GetParam()) {
        GTEST_SKIP() << "MemTxnKv does not detect read conflicts with covering range clears";
    }
    check_overlapping_retry(true);
}

TEST_P(LargeDeleteBitmapTest, ChangedPrefixBeforePrefixValidation) {
    check_overlapping_retry(false, true);
}

TEST_P(LargeDeleteBitmapTest, MaybeCommittedFragmentRetriesIdempotently) {
    lock();
    bool injected = false;
    size_t commits = 0;
    auto* sp = SyncPoint::get_instance();
    sp->set_call_back("update_delete_bitmap:commit:err", [&](auto&& args) {
        // The first commit persists pending intent; the second persists a prefix
        // of the large bitmap. Report an ambiguous result after the real commit.
        if (++commits == 2) {
            *try_any_cast<TxnErrorCode*>(args[2]) = TxnErrorCode::TXN_MAYBE_COMMITTED;
            injected = true;
        }
    });
    sp->enable_processing();
    auto value = large_value();
    auto res = update(request(value));
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
    EXPECT_TRUE(injected);
    check_value(value);
}

TEST_P(LargeDeleteBitmapTest, LockLossStopsFragmentsAndShorterRetryCleansTail) {
    lock();
    bool removed_lock = false;
    size_t commits = 0;
    auto* sp = SyncPoint::get_instance();
    sp->set_call_back("update_delete_bitmap:commit:err", [&](auto&&) {
        if (++commits != 2) {
            return;
        }
        removed_lock = true;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->remove(meta_delete_bitmap_update_lock_key({instance_id_, 100, -1}));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    });
    sp->enable_processing();
    auto res = update(request(large_value()));
    ASSERT_EQ(res.status().code(), MetaServiceCode::LOCK_EXPIRED) << res.status().msg();
    ASSERT_TRUE(removed_lock);
    sp->clear_call_back("update_delete_bitmap:commit:err");

    lock();
    const std::string shorter(DEFAULT_BLOB_SPLIT_SIZE + 1, 's');
    res = update(request(shorter));
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
    check_value(shorter);
}

TEST_P(LargeDeleteBitmapTest, ExplicitSubTransactionPreservesEarlierBitmaps) {
    lock();
    auto value = large_value();
    auto req = request(value);
    req.set_is_explicit_txn(true);
    req.set_txn_id(1001);
    auto res = update(req);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();

    const auto pending_key = meta_pending_delete_bitmap_key({instance_id_, 102});
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string data;
    ASSERT_EQ(txn->get(pending_key, &data), TxnErrorCode::TXN_OK);
    PendingDeleteBitmapPB previous;
    ASSERT_TRUE(previous.ParseFromString(data));
    ASSERT_EQ(previous.delete_bitmap_keys_size(), 1);

    req.set_txn_id(1002);
    req.set_versions(0, 3);
    res = update(req);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << res.status().msg();
    check_value(value);
    ASSERT_EQ(kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(pending_key, &data), TxnErrorCode::TXN_OK);
    PendingDeleteBitmapPB current;
    ASSERT_TRUE(current.ParseFromString(data));
    EXPECT_EQ(current.delete_bitmap_keys_size(), 2);
    EXPECT_EQ(current.delete_bitmap_keys(0), previous.delete_bitmap_keys(0));
    ValueBuf next;
    ASSERT_EQ(blob_get(txn.get(), meta_delete_bitmap_key({instance_id_, 102, "old_rowset", 3, 0}),
                       &next),
              TxnErrorCode::TXN_OK);
    EXPECT_EQ(next.value(), value);
}

TEST_P(LargeDeleteBitmapTest, UnlockedAggregationKeepsAtomicBitmapOverwrite) {
    auto old_key = meta_delete_bitmap_key({instance_id_, 102, "old_rowset", 1, 0});
    auto current_key = meta_delete_bitmap_key({instance_id_, 102, "old_rowset", 2, 0});
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
    RowsetMetaCloudPB old_rowset;
    old_rowset.set_rowset_id(0);
    old_rowset.set_rowset_id_v2("old_rowset");
    old_rowset.set_start_version(0);
    old_rowset.set_end_version(0);
    txn->put(meta_rowset_key({instance_id_, 102, 0}), old_rowset.SerializeAsString());
    RowsetMetaCloudPB output_rowset;
    output_rowset.set_rowset_id(0);
    output_rowset.set_rowset_id_v2("output_rowset");
    output_rowset.set_start_version(1);
    output_rowset.set_end_version(2);
    txn->put(meta_rowset_key({instance_id_, 102, 2}), output_rowset.SerializeAsString());
    blob_put(txn.get(), old_key, "older_visible_bitmap", 0);
    blob_put(txn.get(), current_key, "visible_bitmap", 0);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    auto req = request(large_value(), -3);
    req.set_without_lock(true);
    req.set_pre_rowset_agg_start_version(1);
    req.set_pre_rowset_agg_end_version(2);
    req.add_pre_rowset_versions(0);
    auto res = update(req);
    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT) << res.status().msg();
    if (limited_kv_ != nullptr) {
        EXPECT_GT(limited_kv_->limits.largest_commit, 10'000'000);
    }
    ASSERT_EQ(kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
    ValueBuf current;
    ASSERT_EQ(blob_get(txn.get(), current_key, &current), TxnErrorCode::TXN_OK);
    EXPECT_EQ(current.value(), "visible_bitmap");
    ValueBuf old;
    ASSERT_EQ(blob_get(txn.get(), old_key, &old), TxnErrorCode::TXN_OK);
    EXPECT_EQ(old.value(), "older_visible_bitmap");
}

INSTANTIATE_TEST_SUITE_P(Mem, LargeDeleteBitmapTest, testing::Values(false));
INSTANTIATE_TEST_SUITE_P(Fdb, LargeDeleteBitmapTest, testing::Values(true));

} // namespace
} // namespace doris::cloud
