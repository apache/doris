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

#include "meta-store/blob_message.h"

#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <limits>
#include <memory>
#include <string>

#include "meta-store/codec.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"

using namespace doris::cloud;

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
        buffer += fmt::format("Key: {}\n", escape_hex(kv->first));
    }
    EXPECT_TRUE(iter->is_valid()); // The iterator should still be valid after the next call.
    return buffer;
}

TEST(BlobMessageTest, BlobKeyCodec) {
    const std::vector<std::tuple<std::string, uint8_t, uint16_t>> test_cases {
            {"test_blob_key", 0, 0},
            {std::string("\0binary\xff", 8), 127, std::numeric_limits<uint16_t>::max()},
    };
    for (const auto& [expected_origin_key, expected_version, expected_sequence] : test_cases) {
        std::string origin_key;
        uint8_t version = 0;
        uint16_t sequence = 0;
        auto raw_key = encode_blob_key(expected_origin_key, expected_version, expected_sequence);
        ASSERT_TRUE(decode_blob_key(raw_key, &origin_key, &version, &sequence));
        EXPECT_EQ(origin_key, expected_origin_key);
        EXPECT_EQ(version, expected_version);
        EXPECT_EQ(sequence, expected_sequence);
        EXPECT_TRUE(decode_blob_key(raw_key, nullptr, &version, &sequence));
    }

    EXPECT_EQ(encode_blob_key("key", 0x56, 0x1234),
              std::string("key\x12\x56\0\0\0\0\0\x12\x34", 12));
}

TEST(BlobMessageTest, BlobKeySequenceDoesNotWrap) {
    const size_t sequence = static_cast<size_t>(std::numeric_limits<uint16_t>::max()) + 1;
    EXPECT_NE(encode_blob_key("key", 0, 0), encode_blob_key("key", 0, sequence));
}

TEST(BlobMessageTest, ValueBufVersionAboveInt8Max) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

    std::string key = "version_above_int8_max";
    std::string raw_key = key;
    encode_int64(-(int64_t {1} << 56), &raw_key); // version 255, sequence 0
    txn->put(raw_key, "value");
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    ValueBuf value;
    ASSERT_EQ(blob_get(txn.get(), key, &value), TxnErrorCode::TXN_OK);
    EXPECT_EQ(value.ver, std::numeric_limits<uint8_t>::max());
    EXPECT_EQ(value.value(), "value");
}

TEST(BlobMessageTest, ClampSmallSplitSize) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

    std::string value(MIN_BLOB_SPLIT_SIZE + 1, 'a');
    blob_put(txn.get(), "key", value, 0, 1);
    EXPECT_EQ(txn->num_put_keys(), 2);
    versioned::blob_put(txn.get(), "versioned_key", value, 0, 1);
    EXPECT_EQ(txn->num_put_keys(), 4);
}

TEST(BlobMessageTest, DecodeBlobKeyFields) {
    auto encoded_origin_key = meta_delete_bitmap_key({"instance_id", 1, "rowset_id", 2, 3});
    std::string origin_key;
    uint8_t version = 0;
    uint16_t sequence = 0;
    std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> fields;
    ASSERT_TRUE(decode_blob_key(encode_blob_key(encoded_origin_key, 2, 123), &origin_key, &version,
                                &sequence, &fields));
    EXPECT_EQ(origin_key, encoded_origin_key);
    EXPECT_EQ(version, 2);
    EXPECT_EQ(sequence, 123);
    ASSERT_EQ(fields.size(), 7);
    EXPECT_EQ(std::get<std::string>(std::get<0>(fields[0])), "meta");
    EXPECT_EQ(std::get<std::string>(std::get<0>(fields[1])), "instance_id");
    EXPECT_EQ(std::get<std::string>(std::get<0>(fields[2])), "delete_bitmap");
    EXPECT_EQ(std::get<int64_t>(std::get<0>(fields[3])), 1);
    EXPECT_EQ(std::get<std::string>(std::get<0>(fields[4])), "rowset_id");
    EXPECT_EQ(std::get<int64_t>(std::get<0>(fields[5])), 2);
    EXPECT_EQ(std::get<int64_t>(std::get<0>(fields[6])), 3);
}

TEST(BlobMessageTest, RejectInvalidBlobKey) {
    std::string origin_key;
    uint8_t version = 0;
    uint16_t sequence = 0;
    EXPECT_FALSE(decode_blob_key("invalid", &origin_key, &version, &sequence));

    std::string invalid_suffix = "origin";
    invalid_suffix.append(9, '\0');
    EXPECT_FALSE(decode_blob_key(invalid_suffix, &origin_key, &version, &sequence));

    std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> fields;
    EXPECT_FALSE(
            decode_blob_key(encode_blob_key("", 2, 123), nullptr, &version, &sequence, &fields));
    EXPECT_FALSE(decode_blob_key(encode_blob_key("invalid", 2, 123), nullptr, &version, &sequence,
                                 &fields));
}

// Test blob_put and blob_get with small message (single KV)
TEST(BlobMessageTest, PutGetSmallMessage) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

    // Create a small protobuf message
    doris::RowsetMetaCloudPB rowset_meta;
    rowset_meta.set_rowset_id(12345);
    rowset_meta.set_tablet_id(678);
    rowset_meta.set_num_rows(100);
    rowset_meta.set_data_disk_size(1024);

    std::string key = "test_blob_small_message";
    uint8_t version = 1;

    // Put the message
    blob_put(txn.get(), key, rowset_meta, version);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    // Get the message
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    ValueBuf val;
    ASSERT_EQ(blob_get(txn.get(), key, &val), TxnErrorCode::TXN_OK);
    ASSERT_EQ(val.ver, version);

    // Parse and verify
    doris::RowsetMetaCloudPB retrieved_meta;
    ASSERT_TRUE(val.to_pb(&retrieved_meta));
    ASSERT_EQ(retrieved_meta.rowset_id(), rowset_meta.rowset_id());
    ASSERT_EQ(retrieved_meta.tablet_id(), rowset_meta.tablet_id());
    ASSERT_EQ(retrieved_meta.num_rows(), rowset_meta.num_rows());
    ASSERT_EQ(retrieved_meta.data_disk_size(), rowset_meta.data_disk_size());
}

static OperationLogPB create_operation_log(int num_tablets, int num_partitions) {
    OperationLogPB operation_log;
    CommitTxnLogPB* commit_txn_log = operation_log.mutable_commit_txn();
    for (int i = 0; i < num_tablets; i++) {
        int64_t tablet_id = static_cast<int64_t>(std::numeric_limits<uint32_t>::max()) + i;
        commit_txn_log->mutable_tablet_to_partition_map()->insert({tablet_id, i + 1000});
    }
    for (int i = 0; i < num_partitions; i++) {
        int64_t partition_id = static_cast<int64_t>(std::numeric_limits<uint32_t>::max()) + i;
        commit_txn_log->mutable_partition_version_map()->insert({partition_id, i});
    }
    return operation_log;
}

// Test blob_put and blob_get with large message (split into multiple KVs)
TEST(BlobMessageTest, PutGetLargeMessage) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

    // Create a large protobuf message that will be split, 100k tablet and 10k partitions
    OperationLogPB operation_log = create_operation_log(100000, 10000);

    std::string key = "test_blob_large_message";
    uint8_t version = 2;
    size_t split_size = 50000; // 50KB split size

    // Put the large message
    versioned::blob_put(txn.get(), key, operation_log, version, split_size);
    txn->enable_get_versionstamp();
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    Versionstamp vs;
    ASSERT_EQ(txn->get_versionstamp(&vs), TxnErrorCode::TXN_OK);

    // Get the message
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    ValueBuf val;
    std::string versioned_key = encode_versioned_key(key, vs);
    ASSERT_EQ(blob_get(txn.get(), versioned_key, &val), TxnErrorCode::TXN_OK)
            << dump_range(txn_kv.get()) << ", versioned_key=" << escape_hex(versioned_key);
    ASSERT_EQ(val.ver, version);

    // Verify it was split into multiple KVs
    std::vector<std::string> keys = val.keys();
    ASSERT_GT(keys.size(), 1) << "Large message should be split into multiple KVs";

    // Parse and verify
    OperationLogPB retrieved_log;
    ASSERT_TRUE(val.to_pb(&retrieved_log));
    ASSERT_EQ(retrieved_log.commit_txn().tablet_to_partition_map_size(),
              operation_log.commit_txn().tablet_to_partition_map_size());
    ASSERT_EQ(retrieved_log.commit_txn().partition_version_map_size(),
              operation_log.commit_txn().partition_version_map_size());
}

// Test blob_put with string value
TEST(BlobMessageTest, PutGetStringValue) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

    std::string key = "test_blob_string_value";
    std::string value = "This is a test string value";
    uint8_t version = 3;

    // Put the string value
    blob_put(txn.get(), key, value, version);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    // Get the value
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    ValueBuf val;
    ASSERT_EQ(blob_get(txn.get(), key, &val), TxnErrorCode::TXN_OK);
    ASSERT_EQ(val.ver, version);

    // Verify the value
    std::string retrieved_value = val.value();
    ASSERT_EQ(retrieved_value, value);
}

// Test blob_get with non-existent key
TEST(BlobMessageTest, GetNonExistentKey) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

    std::string key = "test_blob_non_existent_key";
    ValueBuf val;
    ASSERT_EQ(blob_get(txn.get(), key, &val), TxnErrorCode::TXN_KEY_NOT_FOUND);
}

// Test blob_get_range with TxnKv
TEST(BlobMessageTest, GetRangeWithTxnKv) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::vector<std::string> expected_keys;
    for (int i = 0; i < 5; i++) {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        std::string key = "test_blob_range_test_" + std::to_string(i);
        expected_keys.push_back(key);
        OperationLogPB operation_log = create_operation_log(100000, 10000);
        operation_log.set_min_timestamp(i);
        versioned::blob_put(txn.get(), key, operation_log, 2, 50000);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Get range
    std::string begin_key = "test_blob_range_test_";
    std::string end_key = begin_key + "\xff";

    auto iter = blob_get_range(txn_kv, begin_key, end_key);
    ASSERT_NE(iter, nullptr);

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

    // Iterate and verify
    int count = 0;
    std::vector<std::string> retrieved_keys;
    while (iter->valid()) {
        ASSERT_EQ(iter->version(), 2);

        Versionstamp vs;
        std::string_view key(iter->key());
        ASSERT_TRUE(decode_versioned_key(&key, &vs));
        retrieved_keys.push_back(std::string(key));

        OperationLogPB retrieved_log;
        ASSERT_TRUE(iter->parse_value(&retrieved_log));
        ASSERT_EQ(retrieved_log.min_timestamp(), count);

        for (auto&& raw_key : iter->raw_keys()) {
            txn->remove(raw_key);
        }

        count++;
        iter->next();
    }
    ASSERT_EQ(count, 5);
    ASSERT_EQ(retrieved_keys, expected_keys);
    ASSERT_EQ(iter->error_code(), TxnErrorCode::TXN_OK);

    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    {
        // The range should be empty now
        auto iter2 = blob_get_range(txn_kv, begin_key, end_key);
        ASSERT_NE(iter2, nullptr);
        ASSERT_FALSE(iter2->valid());
        ASSERT_EQ(iter2->error_code(), TxnErrorCode::TXN_OK);
    }
}

TEST(BlobMessageTest, BlobIteratorRejectsMalformedChunks) {
    auto expect_invalid = [](std::string_view origin_key,
                             const std::vector<std::string>& raw_keys) {
        auto txn_kv = std::make_shared<MemTxnKv>();
        ASSERT_EQ(txn_kv->init(), 0);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        for (const auto& raw_key : raw_keys) {
            txn->put(raw_key, "value");
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        auto iter = blob_get_range(txn_kv, origin_key, std::string(origin_key) + "\xff");
        EXPECT_FALSE(iter->valid());
        EXPECT_EQ(iter->error_code(), TxnErrorCode::TXN_INVALID_DATA);
    };

    expect_invalid("sequence_gap",
                   {encode_blob_key("sequence_gap", 0, 0), encode_blob_key("sequence_gap", 0, 2)});
    expect_invalid("version_mismatch", {encode_blob_key("version_mismatch", 1, 0),
                                        encode_blob_key("version_mismatch", 2, 1)});
    std::string invalid_suffix = "invalid_suffix";
    invalid_suffix.append(9, '\0');
    expect_invalid("invalid_suffix", {invalid_suffix});
}

// Test blob_get_range with empty range
TEST(BlobMessageTest, GetRangeEmpty) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string begin_key = "test_blob_empty_range_";
    std::string end_key = begin_key + "\xff";

    auto iter = blob_get_range(txn_kv, begin_key, end_key);
    ASSERT_NE(iter, nullptr);
    ASSERT_FALSE(iter->valid());
    ASSERT_EQ(iter->error_code(), TxnErrorCode::TXN_OK);
}
