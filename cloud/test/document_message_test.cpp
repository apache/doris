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

#include "meta-store/document_message.h"

#include <bthread/bthread.h>
#include <fmt/format.h>
#include <foundationdb/fdb_c_options.g.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_common.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <memory>
#include <mutex>
#include <string>
#include <string_view>

#include "common/config.h"
#include "common/util.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"

using namespace doris::cloud;

static std::mutex config_mutex;

int main(int argc, char** argv) {
    config::init(nullptr, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

static size_t count_range(TxnKv* txn_kv, std::string_view begin = "",
                          std::string_view end = "\xFF") {
    std::unique_ptr<Transaction> txn;
    EXPECT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    if (!txn) {
        return 0; // Failed to create transaction
    }

    FullRangeGetOptions opts;
    opts.txn = txn.get();
    auto iter = txn_kv->full_range_get(std::string(begin), std::string(end), std::move(opts));
    size_t total = 0;
    for (auto&& kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
        total += 1;
    }

    EXPECT_TRUE(iter->is_valid()); // The iterator should still be valid after the next call.
    return total;
}

static bool is_empty_range(TxnKv* txn_kv, std::string_view begin = "",
                           std::string_view end = "\xFF") {
    return count_range(txn_kv, begin, end) == 0;
}

static std::string dump_range(TxnKv* txn_kv, std::string_view begin = "",
                              std::string_view end = "\xFF") {
    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        return "Failed to create dump range transaction";
    }
    FullRangeGetOptions opts;
    opts.txn = txn.get();
    auto iter = txn_kv->full_range_get(std::string(begin), std::string(end), std::move(opts));
    std::string buffer;
    for (auto&& kv = iter->next(); kv.has_value(); kv = iter->next()) {
        buffer += fmt::format("Key: {}, Value: {}\n", hex(kv->first), hex(kv->second));
    }
    EXPECT_TRUE(iter->is_valid()); // The iterator should still be valid after the next call.
    return buffer;
}

TEST(DocumentMessageTest, SplitSingleMessage) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    {
        // Write a message that does not need to be split.
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        SplitSingleMessagePB msg;
        msg.set_other_fields(1);
        ASSERT_TRUE(document_put(txn.get(), "split_single_message_key", std::move(msg)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        // Read the message back.
        std::unique_ptr<Transaction> read_txn;
        ASSERT_EQ(txn_kv->create_txn(&read_txn), TxnErrorCode::TXN_OK);
        SplitSingleMessagePB read_msg;
        ASSERT_EQ(document_get(read_txn.get(), "split_single_message_key", &read_msg),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(read_msg.other_fields(), 1);

        ASSERT_EQ(count_range(txn_kv.get()), 1) << dump_range(txn_kv.get());

        // Remove the message.
        document_remove<SplitSingleMessagePB>(read_txn.get(), "split_single_message_key");
        ASSERT_EQ(read_txn->commit(), TxnErrorCode::TXN_OK);
    }

    ASSERT_TRUE(is_empty_range(txn_kv.get())) << dump_range(txn_kv.get());

    {
        // Write a message that needs to be split.
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        SplitSingleMessagePB msg;
        msg.set_other_fields(2);
        auto* key_bounds = msg.mutable_segment_key_bounds();
        key_bounds->set_min_key("min_key");
        key_bounds->set_max_key("max_key");

        ASSERT_TRUE(document_put(txn.get(), "split_single_message_key_split", std::move(msg)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        // Read the message back.
        std::unique_ptr<Transaction> read_txn;
        ASSERT_EQ(txn_kv->create_txn(&read_txn), TxnErrorCode::TXN_OK);
        SplitSingleMessagePB read_msg;
        ASSERT_EQ(document_get(read_txn.get(), "split_single_message_key_split", &read_msg),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(read_msg.other_fields(), 2);
        ASSERT_EQ(read_msg.segment_key_bounds().min_key(), "min_key");
        ASSERT_EQ(read_msg.segment_key_bounds().max_key(), "max_key");
        ASSERT_EQ(count_range(txn_kv.get()), 2) << dump_range(txn_kv.get());

        // Remove the message.
        document_remove<SplitSingleMessagePB>(read_txn.get(), "split_single_message_key_split");
        ASSERT_EQ(read_txn->commit(), TxnErrorCode::TXN_OK);
    }

    ASSERT_TRUE(is_empty_range(txn_kv.get())) << dump_range(txn_kv.get());
}

TEST(DocumentMessageTest, DocumentPutRowsetMeta) {
    std::unique_lock lock(config_mutex);
    config::enable_split_rowset_meta_pb = true;
    config::split_rowset_meta_pb_size = 0; // Always split the rowset meta pb.

    auto txn_kv = std::make_shared<MemTxnKv>();
    doris::RowsetMetaCloudPB meta;
    meta.set_rowset_id(123);
    meta.set_rowset_id_v2("document_put_rowset_meta");
    meta.set_start_version(10000);
    meta.set_end_version(10005);
    meta.set_num_rows(100);
    meta.set_num_segments(1);
    doris::KeyBoundsPB* key_bounds = meta.mutable_segments_key_bounds()->Add();
    key_bounds->set_min_key("min_key");
    key_bounds->set_max_key("max_key");
    key_bounds = meta.mutable_segments_key_bounds()->Add();
    key_bounds->set_min_key("min_key2");
    key_bounds->set_max_key("max_key2");

    std::string rowset_meta_key =
            meta_rowset_key({meta.rowset_id_v2(), meta.end_version(), meta.num_segments()});
    {
        // create a txn , and put rowset meta
        doris::RowsetMetaCloudPB meta_copy(meta);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_TRUE(document_put(txn.get(), rowset_meta_key, std::move(meta_copy)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // create a txn , and get rowset meta
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        doris::RowsetMetaCloudPB saved_meta;
        ASSERT_EQ(document_get(txn.get(), rowset_meta_key, &saved_meta), TxnErrorCode::TXN_OK);
        ASSERT_EQ(saved_meta.rowset_id_v2(), meta.rowset_id_v2());
        ASSERT_EQ(saved_meta.end_version(), meta.end_version());
        ASSERT_EQ(saved_meta.start_version(), meta.start_version());
        ASSERT_EQ(saved_meta.num_rows(), meta.num_rows());
        ASSERT_EQ(saved_meta.num_segments(), meta.num_segments());
        ASSERT_EQ(saved_meta.segments_key_bounds_size(), 2);
        ASSERT_EQ(saved_meta.segments_key_bounds(0).min_key(), "min_key");
        ASSERT_EQ(saved_meta.segments_key_bounds(0).max_key(), "max_key");
        ASSERT_EQ(saved_meta.segments_key_bounds(1).min_key(), "min_key2");
        ASSERT_EQ(saved_meta.segments_key_bounds(1).max_key(), "max_key2");
    }

    ASSERT_GE(txn_kv->total_kvs(), 2) << dump_range(txn_kv.get());

    {
        // create a txn, and remove rowset meta
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        document_remove<doris::RowsetMetaCloudPB>(txn.get(), rowset_meta_key);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    ASSERT_TRUE(is_empty_range(txn_kv.get())) << dump_range(txn_kv.get());
}

TEST(DocumentMessageTest, DocumentPutRowsetMetaWithoutSplit) {
    std::unique_lock lock(config_mutex);
    config::enable_split_rowset_meta_pb = false;

    auto txn_kv = std::make_shared<MemTxnKv>();
    doris::RowsetMetaCloudPB meta;
    meta.set_rowset_id(123);
    meta.set_rowset_id_v2("document_put_rowset_meta");
    meta.set_start_version(10000);
    meta.set_end_version(10005);
    meta.set_num_rows(100);
    meta.set_num_segments(1);
    doris::KeyBoundsPB* key_bounds = meta.mutable_segments_key_bounds()->Add();
    key_bounds->set_min_key("min_key");
    key_bounds->set_max_key("max_key");

    std::string rowset_meta_key =
            meta_rowset_key({meta.rowset_id_v2(), meta.end_version(), meta.num_segments()});
    {
        // create a txn , and put rowset meta
        doris::RowsetMetaCloudPB meta_copy(meta);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_TRUE(document_put(txn.get(), rowset_meta_key, std::move(meta_copy)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // create a txn , and get rowset meta
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        doris::RowsetMetaCloudPB saved_meta;
        ASSERT_EQ(document_get(txn.get(), rowset_meta_key, &saved_meta), TxnErrorCode::TXN_OK);
        ASSERT_EQ(saved_meta.rowset_id_v2(), meta.rowset_id_v2());
        ASSERT_EQ(saved_meta.end_version(), meta.end_version());
        ASSERT_EQ(saved_meta.start_version(), meta.start_version());
        ASSERT_EQ(saved_meta.num_rows(), meta.num_rows());
        ASSERT_EQ(saved_meta.num_segments(), meta.num_segments());
        ASSERT_EQ(saved_meta.segments_key_bounds_size(), 1);
        ASSERT_EQ(saved_meta.segments_key_bounds(0).min_key(), "min_key");
        ASSERT_EQ(saved_meta.segments_key_bounds(0).max_key(), "max_key");
    }

    ASSERT_EQ(txn_kv->total_kvs(), 1) << dump_range(txn_kv.get());

    {
        // create a txn, and remove rowset meta
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        document_remove<doris::RowsetMetaCloudPB>(txn.get(), rowset_meta_key);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    ASSERT_TRUE(is_empty_range(txn_kv.get())) << dump_range(txn_kv.get());

    {
        // Allow split but no thing to split, because the rowset meta pb is small enough.
        config::enable_split_rowset_meta_pb = true;
        config::split_rowset_meta_pb_size = 100000000; // Big enough to not split.
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        doris::RowsetMetaCloudPB meta_copy(meta);
        ASSERT_TRUE(document_put(txn.get(), rowset_meta_key, std::move(meta_copy)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        // Read the message back.
        std::unique_ptr<Transaction> read_txn;
        ASSERT_EQ(txn_kv->create_txn(&read_txn), TxnErrorCode::TXN_OK);
        doris::RowsetMetaCloudPB read_meta;
        ASSERT_EQ(document_get(read_txn.get(), rowset_meta_key, &read_meta), TxnErrorCode::TXN_OK);
        ASSERT_EQ(read_meta.rowset_id_v2(), meta.rowset_id_v2());
        ASSERT_EQ(read_meta.end_version(), meta.end_version());
        ASSERT_EQ(read_meta.start_version(), meta.start_version());
        ASSERT_EQ(read_meta.num_rows(), meta.num_rows());
        ASSERT_EQ(read_meta.num_segments(), meta.num_segments());
        ASSERT_EQ(read_meta.segments_key_bounds_size(), 1);
        ASSERT_EQ(read_meta.segments_key_bounds(0).min_key(), "min_key");
        ASSERT_EQ(read_meta.segments_key_bounds(0).max_key(), "max_key");

        // Remove the message.
        document_remove<doris::RowsetMetaCloudPB>(read_txn.get(), rowset_meta_key);
        ASSERT_EQ(read_txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // Allow split but no thing to split, because the split fields is empty.
        config::enable_split_rowset_meta_pb = true;
        config::split_rowset_meta_pb_size = 0; // Always split the rowset meta pb.
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        doris::RowsetMetaCloudPB meta_copy(meta);
        meta_copy.clear_segments_key_bounds(); // Clear the segments key bounds to make it empty.
        ASSERT_TRUE(document_put(txn.get(), rowset_meta_key, std::move(meta_copy)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        // If you meat a failed here, it means the rowset meta pb is split into multiple keys,
        // you should clear the split fields you added.
        ASSERT_EQ(txn_kv->total_kvs(), 1) << dump_range(txn_kv.get());

        // Read the message back.
        std::unique_ptr<Transaction> read_txn;
        ASSERT_EQ(txn_kv->create_txn(&read_txn), TxnErrorCode::TXN_OK);
        doris::RowsetMetaCloudPB read_meta;
        ASSERT_EQ(document_get(read_txn.get(), rowset_meta_key, &read_meta), TxnErrorCode::TXN_OK);
        ASSERT_EQ(read_meta.rowset_id_v2(), meta.rowset_id_v2());
        ASSERT_EQ(read_meta.end_version(), meta.end_version());
        ASSERT_EQ(read_meta.start_version(), meta.start_version());
        ASSERT_EQ(read_meta.num_rows(), meta.num_rows());
        ASSERT_EQ(read_meta.num_segments(), meta.num_segments());
        ASSERT_EQ(read_meta.segments_key_bounds_size(), 0); // No segments key bounds
        // Remove the message.
        document_remove<doris::RowsetMetaCloudPB>(read_txn.get(), rowset_meta_key);
        ASSERT_EQ(read_txn->commit(), TxnErrorCode::TXN_OK);
    }

    ASSERT_TRUE(is_empty_range(txn_kv.get())) << dump_range(txn_kv.get());
}

TEST(DocumentMessageTest, DocumentPutVersionPB) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    VersionPB version_pb;
    version_pb.set_version(123);

    {
        // create a txn , and put version pb
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_TRUE(document_put(txn.get(), "version_key", std::move(version_pb)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // create a txn , and get version pb
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        VersionPB saved_version_pb;
        ASSERT_EQ(document_get(txn.get(), "version_key", &saved_version_pb), TxnErrorCode::TXN_OK);
        ASSERT_EQ(saved_version_pb.version(), 123);
    }

    ASSERT_EQ(txn_kv->total_kvs(), 1) << dump_range(txn_kv.get());

    {
        // create a txn, and remove version pb
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        document_remove<VersionPB>(txn.get(), "version_key");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    ASSERT_TRUE(is_empty_range(txn_kv.get())) << dump_range(txn_kv.get());
}

TEST(DocumentMessageTest, DocumentGetAllRowsetMeta) {
    std::unique_lock lock(config_mutex);
    config::enable_split_rowset_meta_pb = true;
    config::split_rowset_meta_pb_size = 0; // Always split the rowset meta pb.

    auto txn_kv = std::make_shared<MemTxnKv>();
    doris::RowsetMetaCloudPB meta;
    meta.set_rowset_id(123);
    meta.set_rowset_id_v2("document_put_rowset_meta");
    meta.set_start_version(10000);
    meta.set_end_version(10005);
    meta.set_num_rows(100);
    meta.set_num_segments(1);
    doris::KeyBoundsPB* key_bounds = meta.mutable_segments_key_bounds()->Add();
    key_bounds->set_min_key("min_key");
    key_bounds->set_max_key("max_key");

    std::string rowset_meta_key_prefix = "rowset_meta_key_";
    for (int i = 0; i < 10; ++i) {
        // create a txn , and put rowset meta
        std::string rowset_meta_key = rowset_meta_key_prefix + std::to_string(i);
        doris::RowsetMetaCloudPB meta_copy(meta);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_TRUE(document_put(txn.get(), rowset_meta_key, std::move(meta_copy)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // scan all rowset meta keys
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        FullRangeGetOptions opts;
        opts.txn = txn.get();
        opts.batch_limit = 2;
        auto iter = txn_kv->full_range_get(rowset_meta_key_prefix, rowset_meta_key_prefix + '\xFF',
                                           std::move(opts));
        int count = 0;
        while (true) {
            doris::RowsetMetaCloudPB saved_meta;
            TxnErrorCode err = document_get(iter.get(), &saved_meta);
            if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
                break; // No more keys to read
            }
            ASSERT_EQ(err, TxnErrorCode::TXN_OK);
            ASSERT_EQ(saved_meta.rowset_id_v2(), meta.rowset_id_v2());
            ASSERT_EQ(saved_meta.end_version(), meta.end_version());
            ASSERT_EQ(saved_meta.start_version(), meta.start_version());
            ASSERT_EQ(saved_meta.num_rows(), meta.num_rows());
            ASSERT_EQ(saved_meta.num_segments(), meta.num_segments());
            ASSERT_EQ(saved_meta.segments_key_bounds_size(), 1);
            ASSERT_EQ(saved_meta.segments_key_bounds(0).min_key(), "min_key");
            ASSERT_EQ(saved_meta.segments_key_bounds(0).max_key(), "max_key");
            count += 1;
        }
        ASSERT_EQ(count, 10) << dump_range(txn_kv.get());
    }
}
