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

#include "meta-store/versioned_value.h"

#include <bthread/bthread.h>
#include <fmt/format.h>
#include <foundationdb/fdb_c_options.g.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_common.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <iostream>
#include <memory>
#include <string>
#include <string_view>

#include "common/config.h"
#include "common/util.h"
#include "meta-store/codec.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versionstamp.h"

using namespace doris::cloud;

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

TEST(VersionedValueTest, Remove) {
    auto txn_kv = std::make_shared<MemTxnKv>();

    std::string key_prefix = "remove_key_";

    // Put 10 versions
    for (int i = 0; i < 10; ++i) {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        VersionPB version_pb;
        version_pb.set_version(i * 10);
        Versionstamp versionstamp(i * 10, 0);
        versioned_put(txn.get(), key_prefix, versionstamp, version_pb.SerializeAsString());
        std::string key(key_prefix);
        encode_versionstamp(versionstamp, &key);
        std::cout << "Put key: " << hex(key) << ", version: " << versionstamp.version()
                  << std::endl;
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    ASSERT_EQ(count_range(txn_kv.get()), 10) << dump_range(txn_kv.get());

    {
        // Remove the latest version
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        Versionstamp last_version(90, 0);
        versioned_remove(txn.get(), key_prefix, last_version);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    ASSERT_EQ(count_range(txn_kv.get()), 9) << dump_range(txn_kv.get());

    {
        // Remove a specific version
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        versioned_remove(txn.get(), key_prefix, Versionstamp(50, 0));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    ASSERT_EQ(count_range(txn_kv.get()), 8) << dump_range(txn_kv.get());

    {
        // Verify the removed key is not found
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        Versionstamp snapshot(51, 0);
        Versionstamp versionstamp;
        std::string value;
        ASSERT_EQ(versioned_get(txn.get(), key_prefix, snapshot, &versionstamp, &value),
                  TxnErrorCode::TXN_OK)
                << dump_range(txn_kv.get());
        VersionPB version_pb;
        ASSERT_TRUE(version_pb.ParseFromString(value));
        ASSERT_EQ(versionstamp.version(), 40)
                << dump_range(txn_kv.get()) << versionstamp.to_string();
    }
}

TEST(VersionedValueTest, Get) {
    auto txn_kv = std::make_shared<MemTxnKv>();

    std::string key_prefix = "get_key_";

    // Put 9 versions
    for (int i = 1; i < 10; ++i) {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        VersionPB version_pb;
        version_pb.set_version(i * 10);
        Versionstamp versionstamp(i * 10, 0);
        versioned_put(txn.get(), key_prefix, versionstamp, version_pb.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // Get with the latest version
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        Versionstamp versionstamp;
        std::string value;
        ASSERT_EQ(versioned_get(txn.get(), key_prefix, Versionstamp::max(), &versionstamp, &value),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(versionstamp.version(), 90);
        VersionPB version_pb;
        ASSERT_TRUE(version_pb.ParseFromString(value));
        ASSERT_EQ(version_pb.version(), 90);
    }

    {
        // Get with a specific version
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        std::string value;
        Versionstamp versionstamp;
        ASSERT_EQ(versioned_get(txn.get(), key_prefix, Versionstamp(51, 0), &versionstamp, &value),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(versionstamp.version(), 50);
        VersionPB version_pb;
        ASSERT_TRUE(version_pb.ParseFromString(value));
        ASSERT_EQ(version_pb.version(), 50);
    }

    {
        // Get with a non-existing version
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string value;
        Versionstamp versionstamp;
        ASSERT_EQ(versioned_get(txn.get(), key_prefix, Versionstamp(5, 0), &versionstamp, &value),
                  TxnErrorCode::TXN_KEY_NOT_FOUND);
    }
}

TEST(VersionedValueTest, GetRange) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    std::string key_prefix = "get_range_key_";

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
            VersionPB version_pb;
            version_pb.set_version(suffix);
            std::string key = fmt::format("{}{:02}", key_prefix, suffix);
            versioned_put(txn.get(), key, versionstamp, version_pb.SerializeAsString());
            ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        }
    }

    struct TestCase {
        Versionstamp snapshot_version;
        std::string begin_key;
        std::string end_key;
        std::vector<std::pair<int, Versionstamp>> expected_results;
    };

    std::vector<TestCase> test_cases = {
            // Test case 1: Get all versions with max snapshot version
            // Results should be in reverse order by key (70, 60, 50, 40, 30, 20, 10)
            {
                    Versionstamp::max(),
                    key_prefix,
                    key_prefix + '\xFF' + '\xFF',
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
                    key_prefix + "10",
                    key_prefix + "70",
                    {{40, Versionstamp(40, 0)},
                     {30, Versionstamp(30, 0)},
                     {20, Versionstamp(20, 0)},
                     {10, Versionstamp(40, 0)}},
            },

            // Test case 3: Get versions with a non-existing snapshot version
            {Versionstamp(5, 0), key_prefix + "10", key_prefix + "70", {}},

            // Test case 4: Get versions with a specific range [20, 60)
            // Results should be in reverse order by key
            {
                    Versionstamp::max(),
                    key_prefix + "20",
                    key_prefix + "60",
                    {{50, Versionstamp(50, 0)},
                     {40, Versionstamp(40, 0)},
                     {30, Versionstamp(60, 0)},
                     {20, Versionstamp(70, 0)}},
            },

            // Test case 5: Empty range - begin_key >= end_key
            {
                    Versionstamp::max(),
                    key_prefix + "50",
                    key_prefix + "30",
                    {},
            },

            // Test case 6: Single key range [30, 31)
            {
                    Versionstamp::max(),
                    key_prefix + "30",
                    key_prefix + "31",
                    {{30, Versionstamp(60, 0)}},
            },

            // Test case 7: Range with no matching keys [25, 30)
            {
                    Versionstamp::max(),
                    key_prefix + "25",
                    key_prefix + "30",
                    {},
            },

            // Test case 8: Exact boundary - key 20 included, key 60 excluded
            {
                    Versionstamp::max(),
                    key_prefix + "20",
                    key_prefix + "60",
                    {{50, Versionstamp(50, 0)},
                     {40, Versionstamp(40, 0)},
                     {30, Versionstamp(60, 0)},
                     {20, Versionstamp(70, 0)}},
            },

            // Test case 9: Version boundary test - snapshot at (45, 0)
            // Should include versions < 45: 10->40, 20->20, 30->30, 40->40
            {
                    Versionstamp(45, 0),
                    key_prefix,
                    key_prefix + '\xFF' + '\xFF',
                    {{40, Versionstamp(40, 0)},
                     {30, Versionstamp(30, 0)},
                     {20, Versionstamp(20, 0)},
                     {10, Versionstamp(40, 0)}},
            },

            // Test case 10: Test with exact version boundary (40, 0)
            // Should include versions < 40: 10->10, 20->20, 30->30
            {
                    Versionstamp(40, 0),
                    key_prefix,
                    key_prefix + '\xFF' + '\xFF',
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
        VersionedRangeGetOptions opts;
        opts.snapshot_version = tc.snapshot_version;
        opts.begin_key_selector = RangeKeySelector::FIRST_GREATER_OR_EQUAL;
        opts.end_key_selector = RangeKeySelector::FIRST_GREATER_OR_EQUAL;
        auto iter = versioned_get_range(txn.get(), tc.begin_key, tc.end_key, opts);
        size_t count = 0;
        for (auto&& kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
            auto [key, version, _value] = kvp.value();
            key.remove_prefix(key_prefix.size());
            int suffix = std::stoi(std::string(key));
            ASSERT_EQ(suffix, tc.expected_results[count].first) << " count=" << count;
            ASSERT_EQ(version, tc.expected_results[count].second)
                    << " count=" << count << ", key=" << hex(key)
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
        VersionedRangeGetOptions opts;
        opts.snapshot_version = tc.snapshot_version;
        opts.begin_key_selector = RangeKeySelector::FIRST_GREATER_OR_EQUAL;
        opts.end_key_selector = RangeKeySelector::FIRST_GREATER_OR_EQUAL;
        auto iter = versioned_get_range(txn.get(), tc.begin_key, tc.end_key, opts);
        size_t count = 0;
        for (auto&& kvp = iter->peek(); kvp.has_value(); iter->next(), kvp = iter->peek()) {
            auto [key, version, _value] = kvp.value();
            key.remove_prefix(key_prefix.size());
            int suffix = std::stoi(std::string(key));
            ASSERT_EQ(suffix, tc.expected_results[count].first) << " count=" << count;
            ASSERT_EQ(version, tc.expected_results[count].second)
                    << " count=" << count << ", key=" << hex(key)
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

TEST(VersionedMessageTest, GetRange2) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    std::string_view key_prefix = "get_range2_key_";
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        // Insert 1000 versioned documents with version 100
        for (int i = 0; i < 1000; i++) {
            std::string key = fmt::format("{}{:03}", key_prefix, i);
            Versionstamp version(100, 0);
            VersionPB value;
            value.set_version(100);
            versioned_put(txn.get(), key, version, value.SerializeAsString());
            ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        }
        // Insert some versioned documents, with version 200
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        for (int i = 0; i < 1000; i++) {
            std::string key = fmt::format("{}{:03}", key_prefix, i);
            Versionstamp version(200, 0);
            VersionPB value;
            value.set_version(200);
            versioned_put(txn.get(), key, version, value.SerializeAsString());
            ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        }
    }

    // Iterate [0, 999] with version 200
    {
        std::string begin_key = fmt::format("{}{:03}", key_prefix, 0);
        std::string end_key = fmt::format("{}{:03}", key_prefix, 999);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        VersionedRangeGetOptions opts;
        opts.snapshot_version = Versionstamp::max();
        opts.begin_key_selector = RangeKeySelector::FIRST_GREATER_OR_EQUAL;
        opts.end_key_selector = RangeKeySelector::FIRST_GREATER_THAN;
        opts.batch_limit = 11;
        auto iter = versioned_get_range(txn.get(), begin_key, end_key, opts);
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
        VersionedRangeGetOptions opts;
        opts.snapshot_version = Versionstamp(150, 0);
        opts.begin_key_selector = RangeKeySelector::FIRST_GREATER_OR_EQUAL;
        opts.end_key_selector = RangeKeySelector::FIRST_GREATER_THAN;
        opts.batch_limit = 11; // Limit to 100 results per batch
        auto iter = versioned_get_range(txn.get(), begin_key, end_key, opts);
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

TEST(VersionedMessageTest, GetRangeWithRangeKeySelector) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    std::string_view key_prefix = "get_range_key_selector_";

    // Insert 9 versioned documents with version 100
    for (int i = 0; i < 10; i++) {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string key = fmt::format("{}{:03}", key_prefix, i);
        Versionstamp version(100, 0);
        VersionPB value;
        value.set_version(100);
        versioned_put(txn.get(), key, version, value.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    struct TestCase {
        std::string begin_key;
        std::string end_key;
        RangeKeySelector begin_selector;
        RangeKeySelector end_selector;
        size_t expected_count;
    };

    std::vector<TestCase> test_cases = {
            // Test case 1: Range [0, 9] with FIRST_GREATER_OR_EQUAL and FIRST_GREATER_THAN
            {fmt::format("{}{:03}", key_prefix, 0), fmt::format("{}{:03}", key_prefix, 9),
             RangeKeySelector::FIRST_GREATER_OR_EQUAL, RangeKeySelector::FIRST_GREATER_THAN, 10},

            // Test case 2: Range [0, 9) with FIRST_GREATER_OR_EQUAL and FIRST_GREATER_OR_EQUAL
            {fmt::format("{}{:03}", key_prefix, 0), fmt::format("{}{:03}", key_prefix, 9),
             RangeKeySelector::FIRST_GREATER_OR_EQUAL, RangeKeySelector::FIRST_GREATER_OR_EQUAL, 9},

            // Test case 3: Range (0, 9] with FIRST_GREATER_THAN and FIRST_GREATER_THAN
            {fmt::format("{}{:03}", key_prefix, 0), fmt::format("{}{:03}", key_prefix, 9),
             RangeKeySelector::FIRST_GREATER_THAN, RangeKeySelector::FIRST_GREATER_THAN, 9},

            // Test case 4: Range (0, 9) with FIRST_GREATER_THAN and FIRST_GREATER_OR_EQUAL
            {fmt::format("{}{:03}", key_prefix, 0), fmt::format("{}{:03}", key_prefix, 9),
             RangeKeySelector::FIRST_GREATER_THAN, RangeKeySelector::FIRST_GREATER_OR_EQUAL, 8},

            // Test case 5: Range [0, 8) with LAST_LESS_OR_EQUAL and LAST_LESS_THAN
            {fmt::format("{}{:03}", key_prefix, 0), fmt::format("{}{:03}", key_prefix, 9),
             RangeKeySelector::LAST_LESS_OR_EQUAL, RangeKeySelector::LAST_LESS_THAN, 8},

            // Test case 6: Range [0, 9) with LAST_LESS_OR_EQUAL and LAST_LESS_OR_EQUAL
            {fmt::format("{}{:03}", key_prefix, 0), fmt::format("{}{:03}", key_prefix, 9),
             RangeKeySelector::LAST_LESS_OR_EQUAL, RangeKeySelector::LAST_LESS_OR_EQUAL, 9},

            // Test case 7: Range (0, 8) with LAST_LESS_THAN and LAST_LESS_THAN
            {fmt::format("{}{:03}", key_prefix, 1), fmt::format("{}{:03}", key_prefix, 9),
             RangeKeySelector::LAST_LESS_THAN, RangeKeySelector::LAST_LESS_THAN, 8},

            // Test case 8: Range (0, 9) with LAST_LESS_THAN and LAST_LESS_OR_EQUAL
            {fmt::format("{}{:03}", key_prefix, 1), fmt::format("{}{:03}", key_prefix, 9),
             RangeKeySelector::LAST_LESS_THAN, RangeKeySelector::LAST_LESS_OR_EQUAL, 9},
    };

    size_t case_index = 0;
    for (const auto& tc : test_cases) {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        VersionedRangeGetOptions opts;
        opts.snapshot_version = Versionstamp::max();
        opts.begin_key_selector = tc.begin_selector;
        opts.end_key_selector = tc.end_selector;
        opts.batch_limit = 11; // Limit to 100 results per batch
        auto iter = versioned_get_range(txn.get(), tc.begin_key, tc.end_key, opts);
        size_t count = 0;
        std::string keys;
        for (auto&& kvp = iter->next(); kvp.has_value(); kvp = iter->next()) {
            auto [key, version, _value] = kvp.value();
            keys += fmt::format("{} -> {}\n", key, version.version());
            count += 1;
        }
        ASSERT_TRUE(iter->is_valid()); // The iterator should still be valid after the next call.
        ASSERT_EQ(count, tc.expected_count)
                << keys << ", case_index=" << case_index << ", begin_key=" << tc.begin_key
                << ", end_key=" << tc.end_key
                << ", begin_selector=" << static_cast<int>(tc.begin_selector)
                << ", end_selector=" << static_cast<int>(tc.end_selector);
        case_index += 1;
    }
}

TEST(VersionedValueTest, RemoveAll) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    std::string key_prefix = "remove_all_key_";

    // Insert 10 versioned values
    for (int i = 0; i < 10; ++i) {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        VersionPB version_pb;
        version_pb.set_version(i * 10);
        Versionstamp versionstamp(i * 10, 0);
        versioned_put(txn.get(), key_prefix, versionstamp, version_pb.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }
    ASSERT_EQ(count_range(txn_kv.get()), 10) << dump_range(txn_kv.get());

    // Remove all versioned values
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        versioned_remove_all(txn.get(), key_prefix);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }
    ASSERT_EQ(count_range(txn_kv.get()), 0) << dump_range(txn_kv.get());

    // Insert 5 versioned values and verify
    for (int i = 0; i < 5; ++i) {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        VersionPB version_pb;
        version_pb.set_version(i * 100);
        Versionstamp versionstamp(i * 100, 0);
        versioned_put(txn.get(), key_prefix, versionstamp, version_pb.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }
    ASSERT_EQ(count_range(txn_kv.get()), 5) << dump_range(txn_kv.get());

    // Remove all versioned values again
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        versioned_remove_all(txn.get(), key_prefix);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }
    ASSERT_EQ(count_range(txn_kv.get()), 0) << dump_range(txn_kv.get());
}

TEST(VersionedValueTest, BatchGet) {
    std::shared_ptr<TxnKv> txn_kv = std::make_shared<MemTxnKv>();
    std::string key_prefix = "batch_get_key_";
    std::vector<std::string> keys;
    for (int i = 0; i < 5; ++i) {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        VersionPB version_pb;
        version_pb.set_version(i * 10);
        Versionstamp versionstamp(i * 10, 0);
        std::string key = fmt::format("{}{:02}", key_prefix, i);
        versioned_put(txn.get(), key, versionstamp, version_pb.SerializeAsString());
        keys.push_back(key);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    struct TestCase {
        Versionstamp snapshot_version;
        std::vector<std::string> keys;
        std::vector<std::optional<Versionstamp>> expected_results;
    };
    std::vector<TestCase> test_cases = {
            // Test case 1: Get all keys with max snapshot version
            {Versionstamp::max(),
             keys,
             {
                     {Versionstamp(0, 0)},
                     {Versionstamp(10, 0)},
                     {Versionstamp(20, 0)},
                     {Versionstamp(30, 0)},
                     {Versionstamp(40, 0)},
             }},
            // Test case 2: Get keys with a specific snapshot version (30, 0)
            {Versionstamp(30, 0),
             keys,
             {
                     {Versionstamp(0, 0)},
                     {Versionstamp(10, 0)},
                     {Versionstamp(20, 0)},
                     {std::nullopt},
                     {std::nullopt},
             }},
            // Test case 3: Get a non-existing keys
            {Versionstamp::max(),
             {"non_existing_key_1", "non_existing_key_2"},
             {
                     std::nullopt,
                     std::nullopt,
             }},
    };
    for (const auto& tc : test_cases) {
        std::cout << "Running test case with snapshot version: " << tc.snapshot_version.to_string()
                  << std::endl;
        std::vector<std::optional<std::pair<std::string, Versionstamp>>> results;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        auto err = versioned_batch_get(txn.get(), tc.keys, tc.snapshot_version, &results);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK)
                << "Failed to batch get keys with snapshot " << tc.snapshot_version.to_string();
        ASSERT_EQ(results.size(), tc.expected_results.size());
        for (size_t i = 0; i < results.size(); ++i) {
            if (tc.expected_results[i].has_value()) {
                ASSERT_TRUE(results[i].has_value());
                ASSERT_EQ(results[i]->second.version(), tc.expected_results[i]->version())
                        << "Mismatch for key: " << tc.keys[i];
            } else {
                ASSERT_FALSE(results[i].has_value())
                        << "Expected no value for key: " << tc.keys[i] << ", value: "
                        << (results[i].has_value() ? results[i]->second.to_string() : "null");
            }
        }
    }
}
