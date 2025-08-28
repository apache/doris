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

#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>

#include "common/config.h"
#include "common/util.h"
#include "meta-store/document_message_get_range.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versionstamp.h"

using namespace doris::cloud;

static std::mutex config_mutex;

int main(int argc, char** argv) {
    config::init(nullptr, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

// Convert a string to a hex-escaped string.
// A non-displayed character is represented as \xHH where HH is the hexadecimal value of the character.
// A displayed character is represented as itself.
static std::string escape_hex(std::string_view data) {
    std::string result;
    for (char c : data) {
        if (isprint(c)) {
            result += c;
        } else {
            result += fmt::format("\\x{:02x}", static_cast<unsigned char>(c));
        }
    }
    return result;
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
        buffer +=
                fmt::format("Key: {}, Value: {}\n", escape_hex(kv->first), escape_hex(kv->second));
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

TEST(DocumentMessageTest, VersionedDocumentPut) {
    std::unique_lock lock(config_mutex);
    config::enable_split_rowset_meta_pb = true;
    config::split_rowset_meta_pb_size = 0; // Always split the rowset meta pb.

    auto txn_kv = std::make_shared<MemTxnKv>();
    doris::RowsetMetaCloudPB meta;
    meta.set_rowset_id(123);
    meta.set_rowset_id_v2("versioned_document_test");
    meta.set_start_version(10000);
    meta.set_end_version(10005);
    meta.set_num_rows(100);
    meta.set_num_segments(1);
    doris::KeyBoundsPB* key_bounds = meta.mutable_segments_key_bounds()->Add();
    key_bounds->set_min_key("min_key");
    key_bounds->set_max_key("max_key");

    std::string key_prefix = "versioned_rowset_meta_key_";
    {
        // Create a txn and put versioned rowset meta with initial version
        doris::RowsetMetaCloudPB meta_copy(meta);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_TRUE(versioned::document_put(txn.get(), key_prefix, std::move(meta_copy)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    Versionstamp version;
    {
        // Get the rowset meta with version
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        doris::RowsetMetaCloudPB read_meta;
        ASSERT_EQ(versioned::document_get(txn.get(), key_prefix, &read_meta, &version),
                  TxnErrorCode::TXN_OK);
    }

    // Remove the range
    {
        // Create a txn and remove the versioned rowset meta
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        versioned::document_remove<doris::RowsetMetaCloudPB>(txn.get(), key_prefix, version);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    ASSERT_TRUE(is_empty_range(txn_kv.get())) << dump_range(txn_kv.get());

    {
        // Create a txn and try to get the removed versioned rowset meta
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        doris::RowsetMetaCloudPB read_meta;
        ASSERT_EQ(versioned::document_get(txn.get(), key_prefix, &read_meta, &version),
                  TxnErrorCode::TXN_KEY_NOT_FOUND);
    }
}

TEST(DocumentMessageTest, VersionedDocumentPutAndGet) {
    std::unique_lock lock(config_mutex);
    config::enable_split_rowset_meta_pb = true;
    config::split_rowset_meta_pb_size = 0; // Always split the rowset meta pb.

    auto txn_kv = std::make_shared<MemTxnKv>();
    doris::RowsetMetaCloudPB meta;
    meta.set_rowset_id(123);
    meta.set_rowset_id_v2("versioned_document_test");
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

    std::string key_prefix = "versioned_rowset_meta_key_";
    {
        // Create a txn and put versioned rowset meta with initial version
        doris::RowsetMetaCloudPB meta_copy(meta);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_TRUE(versioned::document_put(txn.get(), key_prefix, std::move(meta_copy)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // Get the rowset meta with version
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        doris::RowsetMetaCloudPB read_meta;
        Versionstamp version;
        ASSERT_EQ(versioned::document_get(txn.get(), key_prefix, &read_meta, &version),
                  TxnErrorCode::TXN_OK);

        // Verify the metadata
        ASSERT_EQ(read_meta.rowset_id_v2(), meta.rowset_id_v2());
        ASSERT_EQ(read_meta.end_version(), meta.end_version());
        ASSERT_EQ(read_meta.start_version(), meta.start_version());
        ASSERT_EQ(read_meta.num_rows(), meta.num_rows());
        ASSERT_EQ(read_meta.num_segments(), meta.num_segments());
        ASSERT_EQ(read_meta.segments_key_bounds_size(), 2);
        ASSERT_EQ(read_meta.segments_key_bounds(0).min_key(), "min_key");
        ASSERT_EQ(read_meta.segments_key_bounds(0).max_key(), "max_key");
        ASSERT_EQ(read_meta.segments_key_bounds(1).min_key(), "min_key2");
        ASSERT_EQ(read_meta.segments_key_bounds(1).max_key(), "max_key2");
    }

    {
        // Update the rowset meta with a new version
        doris::RowsetMetaCloudPB updated_meta(meta);
        updated_meta.set_num_rows(200); // Change num_rows
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_TRUE(versioned::document_put(txn.get(), key_prefix, std::move(updated_meta)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // Get the latest version of rowset meta
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        doris::RowsetMetaCloudPB read_meta;
        Versionstamp version;
        ASSERT_EQ(versioned::document_get(txn.get(), key_prefix, &read_meta, &version),
                  TxnErrorCode::TXN_OK);

        // Verify the updated metadata
        ASSERT_EQ(read_meta.rowset_id_v2(), meta.rowset_id_v2());
        ASSERT_EQ(read_meta.end_version(), meta.end_version());
        ASSERT_EQ(read_meta.start_version(), meta.start_version());
        ASSERT_EQ(read_meta.num_rows(), 200); // This should now be 200
        ASSERT_EQ(read_meta.num_segments(), meta.num_segments());
        ASSERT_EQ(read_meta.segments_key_bounds_size(), 2);
        ASSERT_EQ(read_meta.segments_key_bounds(0).min_key(), "min_key");
        ASSERT_EQ(read_meta.segments_key_bounds(0).max_key(), "max_key");
        ASSERT_EQ(read_meta.segments_key_bounds(1).min_key(), "min_key2");
        ASSERT_EQ(read_meta.segments_key_bounds(1).max_key(), "max_key2");
    }

    // Testing with a non-splittable message
    {
        std::string simple_key_prefix = "versioned_version_pb_key_";
        VersionPB version_pb;
        version_pb.set_version(123);

        // Put the simple message
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_TRUE(versioned::document_put(txn.get(), simple_key_prefix, std::move(version_pb)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        // Get the simple message
        std::unique_ptr<Transaction> read_txn;
        ASSERT_EQ(txn_kv->create_txn(&read_txn), TxnErrorCode::TXN_OK);
        VersionPB read_version_pb;
        Versionstamp version;
        ASSERT_EQ(versioned::document_get(read_txn.get(), simple_key_prefix, &read_version_pb,
                                          &version),
                  TxnErrorCode::TXN_OK);

        // Verify the simple message
        ASSERT_EQ(read_version_pb.version(), 123);
    }
}

TEST(DocumentMessageTest, VersionedDocumentPutAndGetWithVersionstamp) {
    std::unique_lock lock(config_mutex);
    config::enable_split_rowset_meta_pb = true;
    config::split_rowset_meta_pb_size = 0; // Always split the rowset meta pb.

    auto txn_kv = std::make_shared<MemTxnKv>();
    doris::RowsetMetaCloudPB meta;
    meta.set_rowset_id(123);
    meta.set_rowset_id_v2("versioned_document_test");
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

    std::string key_prefix = "versioned_rowset_meta_key_";
    {
        // Create a txn and put versioned rowset meta with initial version
        doris::RowsetMetaCloudPB meta_copy(meta);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_TRUE(versioned::document_put(txn.get(), key_prefix, std::move(meta_copy)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    Versionstamp first_versionstamp;
    {
        // Get the rowset meta with version
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        doris::RowsetMetaCloudPB read_meta;
        ASSERT_EQ(versioned::document_get(txn.get(), key_prefix, &read_meta, &first_versionstamp),
                  TxnErrorCode::TXN_OK);

        // Verify the metadata
        ASSERT_EQ(read_meta.rowset_id_v2(), meta.rowset_id_v2());
        ASSERT_EQ(read_meta.end_version(), meta.end_version());
        ASSERT_EQ(read_meta.start_version(), meta.start_version());
        ASSERT_EQ(read_meta.num_rows(), meta.num_rows());
        ASSERT_EQ(read_meta.num_segments(), meta.num_segments());
        ASSERT_EQ(read_meta.segments_key_bounds_size(), 2);
        ASSERT_EQ(read_meta.segments_key_bounds(0).min_key(), "min_key");
        ASSERT_EQ(read_meta.segments_key_bounds(0).max_key(), "max_key");
        ASSERT_EQ(read_meta.segments_key_bounds(1).min_key(), "min_key2");
        ASSERT_EQ(read_meta.segments_key_bounds(1).max_key(), "max_key2");
    }

    {
        // Update the rowset meta with a new version
        doris::RowsetMetaCloudPB updated_meta(meta);
        updated_meta.set_num_rows(200); // Change num_rows
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_TRUE(versioned::document_put(txn.get(), key_prefix, std::move(updated_meta)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // Get the latest version of rowset meta
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        doris::RowsetMetaCloudPB read_meta;
        Versionstamp version;
        ASSERT_EQ(versioned::document_get(txn.get(), key_prefix, &read_meta, &version),
                  TxnErrorCode::TXN_OK);

        // Verify the updated metadata
        ASSERT_EQ(read_meta.rowset_id_v2(), meta.rowset_id_v2());
        ASSERT_EQ(read_meta.end_version(), meta.end_version());
        ASSERT_EQ(read_meta.start_version(), meta.start_version());
        ASSERT_EQ(read_meta.num_rows(), 200); // This should now be 200
        ASSERT_EQ(read_meta.num_segments(), meta.num_segments());
        ASSERT_EQ(read_meta.segments_key_bounds_size(), 2);
        ASSERT_EQ(read_meta.segments_key_bounds(0).min_key(), "min_key");
        ASSERT_EQ(read_meta.segments_key_bounds(0).max_key(), "max_key");
        ASSERT_EQ(read_meta.segments_key_bounds(1).min_key(), "min_key2");
        ASSERT_EQ(read_meta.segments_key_bounds(1).max_key(), "max_key2");
    }

    {
        // Get the previous version of rowset meta
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        doris::RowsetMetaCloudPB read_meta;
        Versionstamp snapshot_version(first_versionstamp.version() + 1, 0);
        Versionstamp version;
        ASSERT_EQ(versioned::document_get(txn.get(), key_prefix, snapshot_version, &read_meta,
                                          &version),
                  TxnErrorCode::TXN_OK);

        // Verify the updated metadata
        ASSERT_EQ(read_meta.rowset_id_v2(), meta.rowset_id_v2());
        ASSERT_EQ(read_meta.end_version(), meta.end_version());
        ASSERT_EQ(read_meta.start_version(), meta.start_version());
        ASSERT_EQ(read_meta.num_rows(), meta.num_rows());
        ASSERT_EQ(read_meta.num_segments(), meta.num_segments());
        ASSERT_EQ(read_meta.segments_key_bounds_size(), 2);
        ASSERT_EQ(read_meta.segments_key_bounds(0).min_key(), "min_key");
        ASSERT_EQ(read_meta.segments_key_bounds(0).max_key(), "max_key");
        ASSERT_EQ(read_meta.segments_key_bounds(1).min_key(), "min_key2");
        ASSERT_EQ(read_meta.segments_key_bounds(1).max_key(), "max_key2");
    }

    {
        // Not found
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        doris::RowsetMetaCloudPB read_meta;
        Versionstamp snapshot_version(Versionstamp::min());
        Versionstamp version;
        ASSERT_EQ(versioned::document_get(txn.get(), key_prefix, snapshot_version, &read_meta,
                                          &version),
                  TxnErrorCode::TXN_KEY_NOT_FOUND)
                << dump_range(txn_kv.get());
    }
}

TEST(DocumentMessageTest, VersionedDocumentPutVersionPB) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    VersionPB version_pb;
    version_pb.set_version(123);

    {
        // create a txn , and put version pb
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_TRUE(versioned::document_put(txn.get(), "version_key", std::move(version_pb)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    Versionstamp versionstamp;
    {
        // create a txn , and get version pb
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        VersionPB saved_version_pb;
        ASSERT_EQ(
                versioned::document_get(txn.get(), "version_key", &saved_version_pb, &versionstamp),
                TxnErrorCode::TXN_OK);
        ASSERT_EQ(saved_version_pb.version(), 123);
    }

    ASSERT_EQ(txn_kv->total_kvs(), 1) << dump_range(txn_kv.get());

    {
        // create a txn, and remove version pb
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        versioned::document_remove<VersionPB>(txn.get(), "version_key", versionstamp);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    ASSERT_TRUE(is_empty_range(txn_kv.get())) << dump_range(txn_kv.get());
}

TEST(DocumentMessageTest, VersionedSplitSingleMessage) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    {
        // Write a message that does not need to be split.
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        SplitSingleMessagePB msg;
        msg.set_other_fields(1);
        ASSERT_TRUE(versioned::document_put(txn.get(), "split_single_message_key", std::move(msg)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        // Read the message back.
        std::unique_ptr<Transaction> read_txn;
        ASSERT_EQ(txn_kv->create_txn(&read_txn), TxnErrorCode::TXN_OK);
        SplitSingleMessagePB read_msg;
        Versionstamp version;
        ASSERT_EQ(versioned::document_get(read_txn.get(), "split_single_message_key", &read_msg,
                                          &version),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(read_msg.other_fields(), 1);

        ASSERT_EQ(count_range(txn_kv.get()), 1) << dump_range(txn_kv.get());

        // Remove the message.
        versioned::document_remove<SplitSingleMessagePB>(read_txn.get(), "split_single_message_key",
                                                         version);
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

        ASSERT_TRUE(versioned::document_put(txn.get(), "split_single_message_key_split",
                                            std::move(msg)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        // Read the message back.
        std::unique_ptr<Transaction> read_txn;
        ASSERT_EQ(txn_kv->create_txn(&read_txn), TxnErrorCode::TXN_OK);
        SplitSingleMessagePB read_msg;
        Versionstamp version;
        ASSERT_EQ(versioned::document_get(read_txn.get(), "split_single_message_key_split",
                                          &read_msg, &version),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(read_msg.other_fields(), 2);
        ASSERT_EQ(read_msg.segment_key_bounds().min_key(), "min_key");
        ASSERT_EQ(read_msg.segment_key_bounds().max_key(), "max_key");
        ASSERT_EQ(count_range(txn_kv.get()), 2) << dump_range(txn_kv.get());

        // Remove the message.
        versioned::document_remove<SplitSingleMessagePB>(read_txn.get(),
                                                         "split_single_message_key_split", version);
        ASSERT_EQ(read_txn->commit(), TxnErrorCode::TXN_OK);
    }

    ASSERT_TRUE(is_empty_range(txn_kv.get())) << dump_range(txn_kv.get());
}

TEST(DocumentMessageTest, VersionedDocumentGetAllRowsetMeta) {
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
    key_bounds->set_min_key("min_key");
    key_bounds->set_max_key("max_key");

    std::string rowset_meta_key_prefix = "rowset_meta_key_";
    for (int i = 0; i < 10; ++i) {
        // create a txn , and put rowset meta
        std::string rowset_meta_key = rowset_meta_key_prefix + std::to_string(i);
        doris::RowsetMetaCloudPB meta_copy(meta);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_TRUE(versioned::document_put(txn.get(), rowset_meta_key, std::move(meta_copy)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // scan all rowset meta keys
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        // [begin_key, end_key]
        std::string begin_key = rowset_meta_key_prefix + std::to_string(0);
        std::string end_key = rowset_meta_key_prefix + std::to_string(9);
        versioned::ReadDocumentMessagesOptions read_opts;
        read_opts.batch_limit = 2;
        read_opts.exclude_begin_key = false;
        read_opts.exclude_end_key = false;
        auto iter = versioned::document_get_range<doris::RowsetMetaCloudPB>(txn.get(), begin_key,
                                                                            end_key, read_opts);
        int count = 0;
        while (true) {
            auto&& kvp = iter->next();
            if (!kvp.has_value()) {
                break; // No more keys to read
            }
            auto [key, version, saved_meta] = kvp.value();
            ASSERT_EQ(saved_meta.rowset_id_v2(), meta.rowset_id_v2());
            ASSERT_EQ(saved_meta.end_version(), meta.end_version());
            ASSERT_EQ(saved_meta.start_version(), meta.start_version());
            ASSERT_EQ(saved_meta.num_rows(), meta.num_rows());
            ASSERT_EQ(saved_meta.num_segments(), meta.num_segments());
            ASSERT_EQ(saved_meta.segments_key_bounds_size(), 2);
            ASSERT_EQ(saved_meta.segments_key_bounds(0).min_key(), "min_key");
            ASSERT_EQ(saved_meta.segments_key_bounds(0).max_key(), "max_key");
            ASSERT_EQ(saved_meta.segments_key_bounds(1).min_key(), "min_key");
            ASSERT_EQ(saved_meta.segments_key_bounds(1).max_key(), "max_key");
            count += 1;
        }
        ASSERT_TRUE(iter->is_valid()); // The iterator should still be valid after the next call.
        ASSERT_EQ(count, 10) << dump_range(txn_kv.get());
    }
}

TEST(DocumentMessageTest, VersionedGetRangeWithVersionstamp) {
    std::unique_lock lock(config_mutex);
    config::enable_split_rowset_meta_pb = true;
    config::split_rowset_meta_pb_size = 0; // Always split the rowset meta pb.

    auto txn_kv = std::make_shared<MemTxnKv>();
    std::string key_prefix = "get_range_key_";

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
    key_bounds->set_min_key("min_key");
    key_bounds->set_max_key("max_key");
    {
        // Put a series of versioned documents
        std::vector<std::pair<int, Versionstamp>> versions = {
                {10, Versionstamp(10, 0)}, {20, Versionstamp(20, 0)}, {30, Versionstamp(30, 0)},
                {40, Versionstamp(40, 0)}, {10, Versionstamp(40, 0)}, {50, Versionstamp(50, 0)},
                {30, Versionstamp(60, 0)}, {20, Versionstamp(70, 0)}, {10, Versionstamp(80, 0)},
                {60, Versionstamp(90, 0)}, {70, Versionstamp(90, 0)},
        };
        for (const auto& [suffix, versionstamp] : versions) {
            std::unique_ptr<Transaction> txn;
            ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
            std::string key = fmt::format("{}{:02}", key_prefix, suffix);

            doris::RowsetMetaCloudPB meta_copy(meta);
            ASSERT_TRUE(
                    versioned::document_put(txn.get(), key, versionstamp, std::move(meta_copy)));
            ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        }
    }

    struct TestCase {
        Versionstamp snapshot_version;
        int begin, end;
        std::vector<std::pair<int, Versionstamp>> expected_results;
    };

    std::vector<TestCase> test_cases = {
            // Test case 1: Get all versions with max snapshot version
            // Results should be in reverse order by key (70, 60, 50, 40, 30, 20, 10)
            {
                    Versionstamp::max(),
                    0,
                    99,
                    {{70, Versionstamp(90, 0)},
                     {60, Versionstamp(90, 0)},
                     {50, Versionstamp(50, 0)},
                     {40, Versionstamp(40, 0)},
                     {30, Versionstamp(60, 0)},
                     {20, Versionstamp(70, 0)},
                     {10, Versionstamp(80, 0)}},
            },

            // Test case 2: Get versions with a specific snapshot version (50, 0)
            // Only keys with version < (50, 0) should be returned: 10->40, 20->20, 30->30, 40->40
            // Results should be in reverse order by key (40, 30, 20, 10)
            {
                    Versionstamp(50, 0),
                    10,
                    70,
                    {{40, Versionstamp(40, 0)},
                     {30, Versionstamp(30, 0)},
                     {20, Versionstamp(20, 0)},
                     {10, Versionstamp(40, 0)}},
            },

            // Test case 3: Get versions with a non-existing snapshot version
            {Versionstamp(5, 0), 10, 70, {}},

            // Test case 4: Get versions with a specific range [20, 60)
            // Results should be in reverse order by key
            {
                    Versionstamp::max(),
                    20,
                    60,
                    {{50, Versionstamp(50, 0)},
                     {40, Versionstamp(40, 0)},
                     {30, Versionstamp(60, 0)},
                     {20, Versionstamp(70, 0)}},
            },

            // Test case 5: Empty range - begin_key >= end_key
            {
                    Versionstamp::max(),
                    50,
                    30,
                    {},
            },

            // Test case 6: Single key range [30, 31)
            {
                    Versionstamp::max(),
                    30,
                    31,
                    {{30, Versionstamp(60, 0)}},
            },

            // Test case 7: Range with no matching keys [25, 30)
            {
                    Versionstamp::max(),
                    25,
                    30,
                    {},
            },

            // Test case 8: Exact boundary - key 20 included, key 60 excluded
            {
                    Versionstamp::max(),
                    20,
                    60,
                    {{50, Versionstamp(50, 0)},
                     {40, Versionstamp(40, 0)},
                     {30, Versionstamp(60, 0)},
                     {20, Versionstamp(70, 0)}},
            },

            // Test case 9: Version boundary test - snapshot at (45, 0)
            // Should include versions < 45: 10->40, 20->20, 30->30, 40->40
            {
                    Versionstamp(45, 0),
                    0,
                    99,
                    {{40, Versionstamp(40, 0)},
                     {30, Versionstamp(30, 0)},
                     {20, Versionstamp(20, 0)},
                     {10, Versionstamp(40, 0)}},
            },

            // Test case 10: Test with exact version boundary (40, 0)
            // Should include versions < 40: 10->10, 20->20, 30->30
            {
                    Versionstamp(40, 0),
                    0,
                    99,
                    {{30, Versionstamp(30, 0)},
                     {20, Versionstamp(20, 0)},
                     {10, Versionstamp(10, 0)}},
            },
    };

    size_t case_index = 0;
    for (auto&& tc : test_cases) {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        // Get range with range [begin, end)
        versioned::ReadDocumentMessagesOptions opts;
        opts.snapshot_version = tc.snapshot_version;
        opts.exclude_begin_key = false;
        opts.exclude_end_key = true;
        std::string begin_key = fmt::format("{}{:02}", key_prefix, tc.begin);
        std::string end_key = fmt::format("{}{:02}", key_prefix, tc.end);
        auto iter = versioned::document_get_range<doris::RowsetMetaCloudPB>(txn.get(), begin_key,
                                                                            end_key, opts);
        size_t count = 0;
        for (auto&& kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
            auto [key, version, _value] = kvp.value();
            key.remove_prefix(key_prefix.size());
            int suffix = std::stoi(std::string(key));
            ASSERT_EQ(suffix, tc.expected_results[count].first) << " count=" << count;
            ASSERT_EQ(version, tc.expected_results[count].second)
                    << " count=" << count << ", key=" << escape_hex(key)
                    << ", version=" << version.version()
                    << ", expected=" << tc.expected_results[count].second.version();
            count += 1;
        }
        ASSERT_TRUE(iter->is_valid()); // The iterator should still be valid after the next call.
        ASSERT_EQ(count, tc.expected_results.size());
        case_index += 1;
        std::cout << "Test case " << case_index << " passed." << std::endl;
    }

    // Iterate cases via peek method
    case_index = 0;
    for (auto&& tc : test_cases) {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        // Get range with range [begin, end)
        versioned::ReadDocumentMessagesOptions opts;
        opts.snapshot_version = tc.snapshot_version;
        opts.exclude_begin_key = false;
        opts.exclude_end_key = true;
        std::string begin_key = fmt::format("{}{:02}", key_prefix, tc.begin);
        std::string end_key = fmt::format("{}{:02}", key_prefix, tc.end);
        auto iter = versioned::document_get_range<doris::RowsetMetaCloudPB>(txn.get(), begin_key,
                                                                            end_key, opts);
        size_t count = 0;
        for (auto&& kvp = iter->peek(); kvp.has_value(); iter->next(), kvp = iter->peek()) {
            auto [key, version, _value] = kvp.value();
            key.remove_prefix(key_prefix.size());
            int suffix = std::stoi(std::string(key));
            ASSERT_EQ(suffix, tc.expected_results[count].first) << " count=" << count;
            ASSERT_EQ(version, tc.expected_results[count].second)
                    << " count=" << count << ", key=" << escape_hex(key)
                    << ", version=" << version.version()
                    << ", expected=" << tc.expected_results[count].second.version();
            count += 1;
        }
        ASSERT_TRUE(iter->is_valid()); // The iterator should still be valid after the next call.
        ASSERT_EQ(count, tc.expected_results.size());
        case_index += 1;
        std::cout << "Peek test case " << case_index << " passed." << std::endl;
    }
}

TEST(DocumentMessageTest, VersionedGetRangeWithVersionstamp2) {
    std::unique_lock lock(config_mutex);
    config::enable_split_rowset_meta_pb = true;
    config::split_rowset_meta_pb_size = 0; // Always split the rowset meta pb.

    auto txn_kv = std::make_shared<MemTxnKv>();
    std::string_view key_prefix = "get_range2_key_";

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
    key_bounds->set_min_key("min_key");
    key_bounds->set_max_key("max_key");

    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        // Insert 1000 versioned documents with version 100
        for (int i = 0; i < 1000; i++) {
            std::string key = fmt::format("{}{:03}", key_prefix, i);
            Versionstamp version(100, 0);
            doris::RowsetMetaCloudPB meta_copy(meta);
            ASSERT_TRUE(versioned::document_put(txn.get(), key, version, std::move(meta_copy)));
            ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        }
        // Insert some versioned documents, with version 200
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        for (int i = 0; i < 1000; i++) {
            std::string key = fmt::format("{}{:03}", key_prefix, i);
            Versionstamp version(200, 0);
            doris::RowsetMetaCloudPB meta_copy(meta);
            ASSERT_TRUE(versioned::document_put(txn.get(), key, version, std::move(meta_copy)));
            ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        }
    }

    // Iterate [0, 999] with version 200
    {
        std::string begin_key = fmt::format("{}{:03}", key_prefix, 0);
        std::string end_key = fmt::format("{}{:03}", key_prefix, 999);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        versioned::ReadDocumentMessagesOptions opts;
        opts.snapshot_version = Versionstamp::max();
        opts.exclude_begin_key = false;
        opts.exclude_end_key = false;
        opts.batch_limit = 11;
        auto iter = versioned::document_get_range<doris::RowsetMetaCloudPB>(txn.get(), begin_key,
                                                                            end_key, opts);
        size_t count = 0;
        for (auto&& kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
            auto [key, version, _value] = kvp.value();
            key.remove_prefix(key_prefix.size());
            int suffix = std::stoi(std::string(key));
            ASSERT_EQ(suffix, 999 - count) << " count=" << count << ", key=" << key;
            ASSERT_EQ(version.version(), 200)
                    << " count=" << count << ", key=" << key << ", version=" << version.version();
            count += 1;
        }
        ASSERT_TRUE(iter->is_valid()); // The iterator should still be valid after the next call.
        ASSERT_EQ(count, 1000) << dump_range(txn_kv.get());
    }

    // Iterate [0, 999] with 100
    {
        std::string begin_key = fmt::format("{}{:03}", key_prefix, 0);
        std::string end_key = fmt::format("{}{:03}", key_prefix, 999);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        versioned::ReadDocumentMessagesOptions opts;
        opts.snapshot_version = Versionstamp(150, 0);
        opts.exclude_begin_key = false;
        opts.exclude_end_key = false;
        opts.batch_limit = 11; // Limit to 100 results per batch
        auto iter = versioned::document_get_range<doris::RowsetMetaCloudPB>(txn.get(), begin_key,
                                                                            end_key, opts);
        size_t count = 0;
        for (auto&& kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
            auto [key, version, _value] = kvp.value();
            key.remove_prefix(key_prefix.size());
            int suffix = std::stoi(std::string(key));
            ASSERT_EQ(suffix, 999 - count) << " count=" << count << ", key=" << key;
            ASSERT_EQ(version.version(), 100)
                    << " count=" << count << ", key=" << key << ", version=" << version.version();
            count += 1;
        }
        ASSERT_TRUE(iter->is_valid()); // The iterator should still be valid after the next call.
        ASSERT_EQ(count, 1000) << dump_range(txn_kv.get());
    }
}

TEST(DocumentMessageTest, VersionedGetRangeWithRangeKeySelector) {
    std::unique_lock lock(config_mutex);
    config::enable_split_rowset_meta_pb = true;
    config::split_rowset_meta_pb_size = 0; // Always split the rowset meta pb.

    auto txn_kv = std::make_shared<MemTxnKv>();
    std::string_view key_prefix = "get_range_key_selector_";

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
    key_bounds->set_min_key("min_key");
    key_bounds->set_max_key("max_key");

    // Insert 9 versioned documents with version 100
    for (int i = 0; i < 10; i++) {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string key = fmt::format("{}{:03}", key_prefix, i);
        Versionstamp version(100, 0);
        doris::RowsetMetaCloudPB meta_copy(meta);
        ASSERT_TRUE(versioned::document_put(txn.get(), key, version, std::move(meta_copy)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    struct TestCase {
        std::string begin_key, end_key;
        bool exclude_begin_key, exclude_end_key;
        size_t expected_count;
    };

    std::vector<TestCase> test_cases = {
            // Test case 1: Range [0, 9]
            {fmt::format("{}{:03}", key_prefix, 0), fmt::format("{}{:03}", key_prefix, 9), false,
             false, 10},

            // Test case 2: Range [0, 9)
            {fmt::format("{}{:03}", key_prefix, 0), fmt::format("{}{:03}", key_prefix, 9), false,
             true, 9},

            // Test case 3: Range (0, 9]
            {fmt::format("{}{:03}", key_prefix, 0), fmt::format("{}{:03}", key_prefix, 9), true,
             false, 9},

            // Test case 4: Range (0, 9)
            {fmt::format("{}{:03}", key_prefix, 0), fmt::format("{}{:03}", key_prefix, 9), true,
             true, 8},
    };

    size_t case_index = 0;
    for (const auto& tc : test_cases) {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        versioned::ReadDocumentMessagesOptions opts;
        opts.snapshot_version = Versionstamp::max();
        opts.exclude_begin_key = tc.exclude_begin_key;
        opts.exclude_end_key = tc.exclude_end_key;
        opts.batch_limit = 11; // Limit to 100 results per batch
        auto iter = versioned::document_get_range<doris::RowsetMetaCloudPB>(txn.get(), tc.begin_key,
                                                                            tc.end_key, opts);
        size_t count = 0;
        std::string keys;
        for (auto&& kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
            auto [key, version, _value] = kvp.value();
            keys += fmt::format("{} -> {}\n", key, version.version());
            count += 1;
        }
        // The iterator should still be valid after the next call.
        ASSERT_TRUE(iter->is_valid())
                << "Iterator should be valid after next call"
                << ", count=" << count << ", begin_key=" << escape_hex(tc.begin_key)
                << ", end_key=" << escape_hex(tc.end_key) << ", case_index=" << case_index
                << ", exclude_begin_key=" << tc.exclude_begin_key
                << ", exclude_end_key=" << tc.exclude_end_key
                << ", error_code=" << iter->error_code();
        ASSERT_EQ(count, tc.expected_count) << keys << ", case_index=" << case_index
                                            << ", exclude_begin_key=" << tc.exclude_begin_key
                                            << ", exclude_end_key=" << tc.exclude_end_key
                                            << ", begin_key=" << escape_hex(tc.begin_key)
                                            << ", end_key=" << escape_hex(tc.end_key);
        case_index += 1;
    }
}
