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

#include "meta-store/txn_kv.h"

#include <bthread/bthread.h>
#include <fmt/format.h>
#include <foundationdb/fdb_c_options.g.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include "common/config.h"
#include "common/defer.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-service/doris_txn.h"
#include "meta-store/blob_message.h"
#include "meta-store/codec.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"

using namespace doris::cloud;

std::shared_ptr<TxnKv> txn_kv;

void init_txn_kv() {
    config::fdb_cluster_file_path = "fdb.cluster";
    txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<FdbTxnKv>());
    ASSERT_NE(txn_kv.get(), nullptr);
    int ret = txn_kv->init();
    ASSERT_EQ(ret, 0);
}

int main(int argc, char** argv) {
    config::init(nullptr, true);
    ::testing::InitGoogleTest(&argc, argv);
    init_txn_kv();
    return RUN_ALL_TESTS();
}

TEST(TxnKvTest, Network) {
    fdb::Network network(FDBNetworkOption {});
    network.init();
    network.stop();
}

TEST(TxnKvTest, GetVersionTest) {
    std::unique_ptr<Transaction> txn;
    std::string key;
    std::string val;
    {
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        key.push_back('\xfe');
        key.append(" unit_test_prefix ");
        key.append(" GetVersionTest ");
        txn->atomic_set_ver_value(key, "");
        TxnErrorCode err = txn->commit();
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        int64_t ver0 = 0;
        ASSERT_EQ(txn->get_committed_version(&ver0), TxnErrorCode::TXN_OK);
        ASSERT_GT(ver0, 0);

        err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        err = txn->get(key, &val);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        int64_t ver1 = 0;
        ASSERT_EQ(txn->get_read_version(&ver1), TxnErrorCode::TXN_OK);
        ASSERT_GE(ver1, ver0);

        int64_t ver2;
        int64_t txn_id;
        int ret = get_txn_id_from_fdb_ts(val, &txn_id);
        ASSERT_EQ(ret, 0);
        ver2 = txn_id >> 10;

        std::cout << "ver0=" << ver0 << " ver1=" << ver1 << " ver2=" << ver2 << std::endl;
    }
}

TEST(TxnKvTest, ConflictTest) {
    std::unique_ptr<Transaction> txn, txn1, txn2;
    std::string key = "unit_test";
    std::string val, val1, val2;

    // Historical data
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put("unit_test", "xxxxxxxxxxxxx");
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    // txn1 begin
    ASSERT_EQ(txn_kv->create_txn(&txn1), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn1->get(key, &val1), TxnErrorCode::TXN_OK);
    std::cout << "val1=" << val1 << std::endl;

    // txn2 begin
    ASSERT_EQ(txn_kv->create_txn(&txn2), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn2->get(key, &val2), TxnErrorCode::TXN_OK);
    std::cout << "val2=" << val2 << std::endl;

    // txn2 commit
    val2 = "zzzzzzzzzzzzzzz";
    txn2->put(key, val2);
    ASSERT_EQ(txn2->commit(), TxnErrorCode::TXN_OK);

    // txn1 commit, intend to fail
    val1 = "yyyyyyyyyyyyyyy";
    txn1->put(key, val1);
    ASSERT_NE(txn1->commit(), TxnErrorCode::TXN_OK);

    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
    ASSERT_EQ(val, val2); // First wins
    std::cout << "final val=" << val << std::endl;
}

TEST(TxnKvTest, AtomicSetVerKeyTest) {
    std::string key_prefix = "atomic_set_ver_key_1";

    std::string versionstamp_1;
    {
        // write key_1
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->atomic_set_ver_key(key_prefix, "1");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        // read key_1
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string end_key = key_prefix + "\xFF";
        std::unique_ptr<RangeGetIterator> it;
        ASSERT_EQ(txn->get(key_prefix, end_key, &it), TxnErrorCode::TXN_OK);
        ASSERT_TRUE(it->has_next());
        auto&& [key_1, _1] = it->next();
        ASSERT_EQ(key_1.length(), key_prefix.size() + 10)
                << key_1 << key_prefix; // versionstamp = 10bytes
        key_1.remove_prefix(key_prefix.size());
        versionstamp_1 = key_1;
    }

    std::string versionstamp_2;
    {
        // write key_2
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        key_prefix = "atomic_set_ver_key_2";
        txn->atomic_set_ver_key(key_prefix, "2");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        // read key_2
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string end_key = key_prefix + "\xFF";
        std::unique_ptr<RangeGetIterator> it;
        ASSERT_EQ(txn->get(key_prefix, end_key, &it), TxnErrorCode::TXN_OK);
        ASSERT_TRUE(it->has_next());
        auto&& [key_2, _2] = it->next();
        ASSERT_EQ(key_2.length(), key_prefix.size() + 10); // versionstamp = 10bytes
        key_2.remove_prefix(key_prefix.size());
        versionstamp_2 = key_2;
    }

    ASSERT_LT(versionstamp_1, versionstamp_2);

    std::string versionstamp_3;
    {
        // write key_3, with offset
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string suffix = "_suffix";
        std::string prefix = "atomic_set_ver_key_3_";
        std::string k(prefix);
        uint32_t offset = k.size();
        k.append(10, '\0'); // reserve 10 bytes for versionstamp
        k += suffix;
        ASSERT_TRUE(txn->atomic_set_ver_key(k, offset, "3"));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        // read key_3
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string end_key = prefix + "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF";
        std::unique_ptr<RangeGetIterator> it;
        ASSERT_EQ(txn->get(prefix, end_key, &it), TxnErrorCode::TXN_OK);
        ASSERT_TRUE(it->has_next());
        auto&& [key_3, _3] = it->next();
        ASSERT_EQ(key_3.length(), k.size());
        key_3.remove_suffix(suffix.size());
        key_3.remove_prefix(prefix.size());
        versionstamp_3 = key_3;
    }

    ASSERT_LT(versionstamp_2, versionstamp_3);

    {
        // write key, but offset is invalid
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string prefix = "atomic_set_ver_key_4_";
        std::string k(prefix);
        k.append(10, '\0');             // reserve 10 bytes for versionstamp
        uint32_t offset = k.size() + 1; // invalid offset
        ASSERT_FALSE(txn->atomic_set_ver_key(k, offset, "4"));

        k = "fake";
        ASSERT_FALSE(txn->atomic_set_ver_key(k, 0, "4"));
    }
}

TEST(TxnKvTest, AtomicAddTest) {
    std::unique_ptr<Transaction> txn, txn1, txn2;
    std::string key = "counter";
    // clear counter
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->remove(key);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    // txn1 atomic add
    ASSERT_EQ(txn_kv->create_txn(&txn1), TxnErrorCode::TXN_OK);
    txn1->atomic_add(key, 10);
    // txn2 atomic add
    ASSERT_EQ(txn_kv->create_txn(&txn2), TxnErrorCode::TXN_OK);
    txn2->atomic_add(key, 20);
    // txn1 commit success
    ASSERT_EQ(txn1->commit(), TxnErrorCode::TXN_OK);
    // txn2 commit success
    ASSERT_EQ(txn2->commit(), TxnErrorCode::TXN_OK);
    // Check counter val
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string val;
    ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
    ASSERT_EQ(val.size(), 8);
    ASSERT_EQ(*(int64_t*)val.data(), 30);

    // txn1 atomic add
    ASSERT_EQ(txn_kv->create_txn(&txn1), TxnErrorCode::TXN_OK);
    txn1->atomic_add(key, 30);
    // txn2 get and put
    ASSERT_EQ(txn_kv->create_txn(&txn2), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn2->get(key, &val), TxnErrorCode::TXN_OK);
    ASSERT_EQ(val.size(), 8);
    ASSERT_EQ(*(int64_t*)val.data(), 30);
    *(int64_t*)val.data() = 100;
    txn2->put(key, val);
    // txn1 commit success
    ASSERT_EQ(txn1->commit(), TxnErrorCode::TXN_OK);
    // txn2 commit, intend to fail
    ASSERT_NE(txn2->commit(), TxnErrorCode::TXN_OK);
    // Check counter val
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
    ASSERT_EQ(val.size(), 8);
    ASSERT_EQ(*(int64_t*)val.data(), 60);
}

TEST(TxnKvTest, CompatibleGetTest) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    doris::TabletSchemaCloudPB schema;
    schema.set_schema_version(1);
    for (int i = 0; i < 1000; ++i) {
        auto column = schema.add_column();
        column->set_unique_id(i);
        column->set_name("col" + std::to_string(i));
        column->set_type("VARCHAR");
        column->set_aggregation("NONE");
        column->set_length(100);
        column->set_index_length(80);
    }
    std::string instance_id = "compatible_get_test_" + std::to_string(::time(nullptr));
    auto key = meta_schema_key({instance_id, 10005, 1});
    auto val = schema.SerializeAsString();

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(doris::cloud::key_exists(txn.get(), key), TxnErrorCode::TXN_KEY_NOT_FOUND);
    ValueBuf val_buf;
    ASSERT_EQ(doris::cloud::blob_get(txn.get(), key, &val_buf), TxnErrorCode::TXN_KEY_NOT_FOUND);
    txn->put(key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    // Check get
    TxnErrorCode err = txn_kv->create_txn(&txn);
    ASSERT_EQ(err, TxnErrorCode::TXN_OK);
    err = doris::cloud::key_exists(txn.get(), key);
    ASSERT_EQ(err, TxnErrorCode::TXN_OK);
    err = doris::cloud::blob_get(txn.get(), key, &val_buf);
    ASSERT_EQ(err, TxnErrorCode::TXN_OK);
    EXPECT_EQ(val_buf.ver, 0);
    doris::TabletSchemaCloudPB saved_schema;
    ASSERT_TRUE(val_buf.to_pb(&saved_schema));
    ASSERT_EQ(saved_schema.column_size(), schema.column_size());
    for (size_t i = 0; i < saved_schema.column_size(); ++i) {
        auto& saved_col = saved_schema.column(i);
        auto& col = schema.column(i);
        EXPECT_EQ(saved_col.name(), col.name());
    }

    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    val_buf.remove(txn.get());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    // Check remove
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(doris::cloud::key_exists(txn.get(), key), TxnErrorCode::TXN_KEY_NOT_FOUND);
    ASSERT_EQ(doris::cloud::blob_get(txn.get(), key, &val_buf), TxnErrorCode::TXN_KEY_NOT_FOUND);
}

TEST(TxnKvTest, PutLargeValueTest) {
    auto txn_kv = std::make_shared<MemTxnKv>();

    auto sp = doris::SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        doris::SyncPoint::get_instance()->clear_all_call_backs();
    };
    sp->enable_processing();

    doris::TabletSchemaCloudPB schema;
    schema.set_schema_version(1);
    for (int i = 0; i < 10000; ++i) {
        auto column = schema.add_column();
        column->set_unique_id(i);
        column->set_name("col" + std::to_string(i));
        column->set_type("VARCHAR");
        column->set_aggregation("NONE");
        column->set_length(100);
        column->set_index_length(80);
    }
    std::cout << "value size=" << schema.SerializeAsString().size() << std::endl;

    std::string instance_id = "put_large_value_" + std::to_string(::time(nullptr));
    auto key = meta_schema_key({instance_id, 10005, 1});
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    doris::cloud::blob_put(txn.get(), key, schema, 1, 100);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    // Check get
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    ValueBuf val_buf;
    doris::TabletSchemaCloudPB saved_schema;
    ASSERT_EQ(doris::cloud::key_exists(txn.get(), key), TxnErrorCode::TXN_OK);
    TxnErrorCode err = doris::cloud::blob_get(txn.get(), key, &val_buf);
    ASSERT_EQ(err, TxnErrorCode::TXN_OK);
    std::cout << "num iterators=" << val_buf.iters.size() << std::endl;
    EXPECT_EQ(val_buf.ver, 1);
    ASSERT_TRUE(val_buf.to_pb(&saved_schema));
    ASSERT_EQ(saved_schema.column_size(), schema.column_size());
    for (size_t i = 0; i < saved_schema.column_size(); ++i) {
        auto& saved_col = saved_schema.column(i);
        auto& col = schema.column(i);
        EXPECT_EQ(saved_col.name(), col.name());
    }
    // Check multi range get
    sp->set_call_back("memkv::Transaction::get", [](auto&& args) {
        auto* limit = doris::try_any_cast<int*>(args[0]);
        *limit = 100;
    });
    err = doris::cloud::blob_get(txn.get(), key, &val_buf);
    ASSERT_EQ(err, TxnErrorCode::TXN_OK);
    std::cout << "num iterators=" << val_buf.iters.size() << std::endl;
    EXPECT_EQ(val_buf.ver, 1);
    ASSERT_TRUE(val_buf.to_pb(&saved_schema));
    ASSERT_EQ(saved_schema.column_size(), schema.column_size());
    for (size_t i = 0; i < saved_schema.column_size(); ++i) {
        auto& saved_col = saved_schema.column(i);
        auto& col = schema.column(i);
        EXPECT_EQ(saved_col.name(), col.name());
    }
    // Check keys
    auto& iters = val_buf.iters;
    size_t i = 0;
    std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> fields;
    for (auto&& it : iters) {
        it->reset();
        while (it->has_next()) {
            auto [k, _] = it->next();
            k.remove_prefix(1);
            fields.clear();
            int ret = decode_key(&k, &fields);
            ASSERT_EQ(ret, 0);
            int64_t* suffix = std::get_if<int64_t>(&std::get<0>(fields.back()));
            ASSERT_TRUE(suffix);
            EXPECT_EQ(*suffix, (1L << 56) + i++);
        }
    }

    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    val_buf.remove(txn.get());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    // Check remove
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(doris::cloud::key_exists(txn.get(), key), TxnErrorCode::TXN_KEY_NOT_FOUND);
    ASSERT_EQ(doris::cloud::blob_get(txn.get(), key, &val_buf), TxnErrorCode::TXN_KEY_NOT_FOUND);
}

TEST(TxnKvTest, RangeGetIteratorContinue) {
    // insert data
    std::string prefix("range_get_iterator_continue");
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        for (size_t i = 0; i < 1000; ++i) {
            txn->put(fmt::format("{}-{:05}", prefix, i), std::to_string(i));
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

    std::unique_ptr<RangeGetIterator> it;
    std::string end = prefix + "\xFF";
    ASSERT_EQ(txn->get(prefix, end, &it, true, 500), TxnErrorCode::TXN_OK);

    size_t i = 0;
    while (true) {
        while (it->has_next()) {
            auto [k, v] = it->next();
            ASSERT_EQ(k, fmt::format("{}-{:05}", prefix, i));
            ASSERT_EQ(v, std::to_string(i));
            i += 1;
        }
        if (!it->more()) {
            break;
        }
        std::string begin = it->next_begin_key();
        ASSERT_EQ(txn->get(begin, end, &it, true, 500), TxnErrorCode::TXN_OK);
    }
    ASSERT_EQ(i, 1000);
}

TEST(TxnKvTest, RangeGetIteratorSeek) {
    // insert data
    std::string prefix("range_get_iterator_seek");
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        for (size_t i = 0; i < 10; ++i) {
            txn->put(fmt::format("{}-{:05}", prefix, i), std::to_string(i));
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Seek to MID
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        std::unique_ptr<RangeGetIterator> it;
        std::string end = prefix + "\xFF";
        ASSERT_EQ(txn->get(prefix, end, &it, true, 500), TxnErrorCode::TXN_OK);

        it->seek(5);
        std::vector<std::string_view> values;
        while (it->has_next()) {
            auto [_, v] = it->next();
            values.push_back(v);
        }

        std::vector<std::string_view> expected_values {"5", "6", "7", "8", "9"};
        ASSERT_EQ(values, expected_values);

        // reset
        it->reset();
        values.clear();
        while (it->has_next()) {
            auto [_, v] = it->next();
            values.push_back(v);
        }
        expected_values = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"};
        ASSERT_EQ(values, expected_values);
    }

    // Seek out of range?
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        std::unique_ptr<RangeGetIterator> it;
        std::string end = prefix + "\xFF";
        ASSERT_EQ(txn->get(prefix, end, &it, true, 500), TxnErrorCode::TXN_OK);

        it->seek(10);
        ASSERT_FALSE(it->has_next());
    }
}

TEST(TxnKvTest, AbortTxn) {
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

    txn->atomic_set_ver_key("prefix", "value");
    ASSERT_EQ(txn->abort(), TxnErrorCode::TXN_OK);
}

TEST(TxnKvTest, RunInBthread) {
    bthread_t tid;
    auto thread = +[](void*) -> void* {
        std::unique_ptr<Transaction> txn;
        EXPECT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        std::string value;
        EXPECT_EQ(txn->get("not_exists_key", &value), TxnErrorCode::TXN_KEY_NOT_FOUND);
        txn->remove("not_exists_key");
        EXPECT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        return nullptr;
    };
    bthread_start_background(&tid, nullptr, thread, nullptr);
    bthread_join(tid, nullptr);
}

TEST(TxnKvTest, KvErrorCodeFormat) {
    std::vector<std::tuple<TxnErrorCode, std::string_view>> codes {
            {TxnErrorCode::TXN_OK, "Ok"},
            {TxnErrorCode::TXN_KEY_NOT_FOUND, "KeyNotFound"},
            {TxnErrorCode::TXN_CONFLICT, "Conflict"},
            {TxnErrorCode::TXN_TOO_OLD, "TxnTooOld"},
            {TxnErrorCode::TXN_MAYBE_COMMITTED, "MaybeCommitted"},
            {TxnErrorCode::TXN_RETRYABLE_NOT_COMMITTED, "RetryableNotCommitted"},
            {TxnErrorCode::TXN_TIMEOUT, "Timeout"},
            {TxnErrorCode::TXN_INVALID_ARGUMENT, "InvalidArgument"},
            {TxnErrorCode::TXN_UNIDENTIFIED_ERROR, "Unknown"},
    };
    for (auto&& [code, expect] : codes) {
        std::string msg = fmt::format("{}", code);
        ASSERT_EQ(msg, expect);
        std::stringstream out;
        out << code;
        msg = out.str();
        ASSERT_EQ(msg, expect);
    }
}

TEST(TxnKvTest, BatchGet) {
    std::vector<std::string> keys;
    std::vector<std::string> values;
    constexpr int nums = 100;
    for (int i = 0; i < nums; ++i) {
        keys.push_back("BatchGet_" + std::to_string(i));
    }
    std::unique_ptr<Transaction> txn;
    // put kv
    {
        auto ret = txn_kv->create_txn(&txn);
        ASSERT_EQ(ret, TxnErrorCode::TXN_OK);
        for (const auto& k : keys) {
            txn->put(k, k);
            values.push_back(k);
        }
        ret = txn->commit();
        ASSERT_EQ(ret, TxnErrorCode::TXN_OK);
    }
    // batch get
    {
        auto ret = txn_kv->create_txn(&txn);
        ASSERT_EQ(ret, TxnErrorCode::TXN_OK);
        std::vector<std::optional<std::string>> res;
        ret = txn->batch_get(&res, keys);
        ASSERT_EQ(ret, TxnErrorCode::TXN_OK);
        ASSERT_EQ(res.size(), values.size());
        for (auto i = 0; i < res.size(); ++i) {
            ASSERT_EQ(res[i].has_value(), true);
            ASSERT_EQ(res[i].value(), values[i]);
        }
    }

    // batch get with no-exists keys
    {
        auto ret = txn_kv->create_txn(&txn);
        ASSERT_EQ(ret, TxnErrorCode::TXN_OK);
        std::vector<std::optional<std::string>> res;
        std::vector<std::string> keys {"BatchGet_empty1", "BatchGet_empty2", "BatchGet_empty3"};
        ret = txn->batch_get(&res, keys);
        ASSERT_EQ(ret, TxnErrorCode::TXN_OK);
        ASSERT_EQ(res.size(), keys.size());
        for (const auto& r : res) {
            ASSERT_EQ(r.has_value(), false);
        }
    }
}

TEST(TxnKvTest, FullRangeGetIterator) {
    using namespace std::chrono_literals;

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    ASSERT_EQ(err, TxnErrorCode::TXN_OK);
    constexpr std::string_view prefix = "FullRangeGetIterator";

    std::string last_key {prefix};
    encode_int64(INT64_MAX, &last_key);
    txn->remove(prefix, last_key);
    for (int i = 0; i < 100; ++i) {
        std::string key {prefix};
        encode_int64(i, &key);
        txn->put(key, std::to_string(i));
    }
    err = txn->commit();
    ASSERT_EQ(err, TxnErrorCode::TXN_OK);

    std::string begin {prefix};
    std::string end {prefix};
    encode_int64(INT64_MAX, &end);

    auto* sp = doris::SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        doris::SyncPoint::get_instance()->clear_all_call_backs();
    };
    sp->enable_processing();

    {
        // Without txn
        FullRangeGetOptions opts(txn_kv);
        opts.batch_limit = 11;

        auto it = txn_kv->full_range_get(begin, end, opts);
        ASSERT_TRUE(it->is_valid());

        int cnt = 0;
        for (auto kvp = it->next(); kvp.has_value(); kvp = it->next()) {
            auto [k, v] = *kvp;
            EXPECT_EQ(v, std::to_string(cnt));
            ++cnt;
            // Total cost: 100ms * 100 = 10s > fdb txn timeout 5s, however we create a new transaction
            // in each inner range get
            std::this_thread::sleep_for(100ms);
        }
        ASSERT_TRUE(it->is_valid());
        EXPECT_EQ(cnt, 100);
    }

    {
        // Peek
        FullRangeGetOptions opts(txn_kv);
        opts.batch_limit = 11;
        auto it = txn_kv->full_range_get(begin, end, opts);
        ASSERT_TRUE(it->is_valid());

        // 1. Peek the first element without next
        auto&& kvp = it->peek();
        ASSERT_TRUE(kvp.has_value());
        auto [k, v] = *kvp;
        std::string expected(prefix);
        encode_int64(0, &expected);
        EXPECT_EQ(k, expected);
        EXPECT_EQ(v, "0");

        int cnt = 0;
        for (auto kvp = it->peek(); kvp.has_value(); kvp = it->peek()) {
            auto [k, v] = *kvp;
            EXPECT_EQ(v, std::to_string(cnt));

            // 2. Peek the next element is equal to the peeked element
            auto&& kvp2 = it->next();
            ASSERT_TRUE(kvp2.has_value());
            auto [k2, v2] = *kvp2;
            EXPECT_EQ(k2, k);
            EXPECT_EQ(v2, v);

            ++cnt;
            // Total cost: 100ms * 100 = 10s > fdb txn timeout 5s, however we create a new transaction
            // in each inner range get
            std::this_thread::sleep_for(100ms);
        }
        ASSERT_TRUE(it->is_valid());
        EXPECT_EQ(cnt, 100);
    }

    {
        // With limitation
        FullRangeGetOptions opts(txn_kv);
        opts.batch_limit = 110;
        opts.exact_limit = 11;

        auto it = txn_kv->full_range_get(begin, end, opts);
        ASSERT_TRUE(it->is_valid());

        int cnt = 0;
        for (auto kvp = it->next(); kvp.has_value(); kvp = it->next()) {
            auto [k, v] = *kvp;
            EXPECT_EQ(v, std::to_string(cnt));
            ++cnt;
        }
        ASSERT_TRUE(it->is_valid());
        EXPECT_EQ(cnt, 11);
    }

    {
        // With limitation, prefetch
        FullRangeGetOptions opts(txn_kv);
        opts.batch_limit = 5;
        opts.prefetch = true;
        opts.exact_limit = 11;

        auto it = txn_kv->full_range_get(begin, end, opts);
        ASSERT_TRUE(it->is_valid());

        int cnt = 0;
        for (auto kvp = it->next(); kvp.has_value(); kvp = it->next()) {
            auto [k, v] = *kvp;
            EXPECT_EQ(v, std::to_string(cnt));
            ++cnt;
        }
        ASSERT_TRUE(it->is_valid());
        EXPECT_EQ(cnt, 11);
    }

    {
        // With txn
        err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        FullRangeGetOptions opts(txn_kv);
        opts.batch_limit = 11;
        opts.txn = txn.get();

        auto it = txn_kv->full_range_get(begin, end, opts);
        ASSERT_TRUE(it->is_valid());

        int cnt = 0;
        for (auto kvp = it->next(); kvp.has_value(); kvp = it->next()) {
            auto [k, v] = *kvp;
            EXPECT_EQ(v, std::to_string(cnt));
            ++cnt;
        }
        ASSERT_TRUE(it->is_valid());
        EXPECT_EQ(cnt, 100);
    }

    {
        // With prefetch
        err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        FullRangeGetOptions opts(txn_kv);
        opts.batch_limit = 11;
        opts.txn = txn.get();
        opts.prefetch = true;

        int prefetch_cnt = 0;
        doris::SyncPoint::CallbackGuard guard;
        sp->set_call_back(
                "fdb.FullRangeGetIterator.has_next_prefetch",
                [&](auto&&) {
                    ++prefetch_cnt;
                    std::cout << "With prefetch prefetch_cnt=" << prefetch_cnt << std::endl;
                },
                &guard);

        auto it = txn_kv->full_range_get(begin, end, opts);
        ASSERT_TRUE(it->is_valid());

        int cnt = 0;
        for (auto kvp = it->next(); kvp.has_value(); kvp = it->next()) {
            auto [k, v] = *kvp;
            EXPECT_EQ(v, std::to_string(cnt));
            ++cnt;
            // Sleep to wait for prefetch to be ready
            std::this_thread::sleep_for(1ms);
        }
        ASSERT_TRUE(it->is_valid());
        EXPECT_EQ(cnt, 100);
    }

    {
        // With object pool
        std::vector<std::unique_ptr<RangeGetIterator>> obj_pool;
        FullRangeGetOptions opts(txn_kv);
        opts.batch_limit = 11;
        opts.obj_pool = &obj_pool;

        auto it = txn_kv->full_range_get(begin, end, opts);
        ASSERT_TRUE(it->is_valid());

        int cnt = 0;
        std::vector<std::string_view> values;
        for (auto kvp = it->next(); kvp.has_value(); kvp = it->next()) {
            auto [k, v] = *kvp;
            EXPECT_EQ(v, std::to_string(cnt));
            values.push_back(v);
            if (cnt % 25 == 24) {
                // values should be alive
                int base = cnt / 25 * 25;
                for (int i = 0; i < values.size(); ++i) {
                    EXPECT_EQ(values[i], std::to_string(base + i));
                }
                values.clear();
                obj_pool.clear();
            }
            ++cnt;
        }
        ASSERT_TRUE(it->is_valid());
        EXPECT_EQ(cnt, 100);
    }

    {
        // Abnormal
        FullRangeGetOptions opts(txn_kv);
        opts.batch_limit = 11;
        err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        opts.txn = txn.get();
        auto it = txn_kv->full_range_get(begin, end, opts);
        auto* fdb_it = static_cast<fdb::FullRangeGetIterator*>(it.get());
        fdb_it->code_ = TxnErrorCode::TXN_INVALID_ARGUMENT;
        ASSERT_FALSE(it->is_valid());
        ASSERT_FALSE(it->has_next());
        ASSERT_FALSE(it->next().has_value());

        fdb_it->code_ = TxnErrorCode::TXN_OK;
        ASSERT_TRUE(it->is_valid());
        int cnt = 0;
        for (auto kvp = it->next(); kvp.has_value(); kvp = it->next()) {
            auto [k, v] = *kvp;
            EXPECT_EQ(v, std::to_string(cnt));
            ++cnt;
            // Total cost: 100ms * 100 = 10s > fdb txn timeout 5s
            std::this_thread::sleep_for(100ms);
        }
        // Txn timeout
        ASSERT_FALSE(it->is_valid());
        ASSERT_FALSE(it->has_next());
        ASSERT_FALSE(it->next().has_value());
    }

    {
        // Abnormal dtor
        int prefetch_cnt = 0;
        doris::SyncPoint::CallbackGuard guard;
        sp->set_call_back(
                "fdb.FullRangeGetIterator.has_next_prefetch",
                [&](auto&&) {
                    ++prefetch_cnt;
                    std::cout << "Abnormal dtor prefetch_cnt=" << prefetch_cnt << std::endl;
                },
                &guard);

        FullRangeGetOptions opts(txn_kv);
        opts.batch_limit = 11;
        opts.prefetch = true;
        auto it = txn_kv->full_range_get(begin, end, opts);
        auto kvp = it->next();
        ASSERT_TRUE(kvp.has_value());
        kvp = it->next(); // Trigger prefetch
        ASSERT_TRUE(kvp.has_value());
        auto* fdb_it = static_cast<fdb::FullRangeGetIterator*>(it.get());
        ASSERT_TRUE(fdb_it->fut_ != nullptr); // There is an inflight range get
        // Since there is an inflight range get, should not trigger another prefetch
        ASSERT_FALSE(fdb_it->prefetch());

        // `~FullRangeGetIterator` without consuming inflight range get result
    }

    {
        // Benchmark prefetch
        // No prefetch
        FullRangeGetOptions opts(txn_kv);
        opts.batch_limit = 11;
        err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        opts.txn = txn.get();

        auto it = txn_kv->full_range_get(begin, end, opts);
        int cnt = 0;
        auto start = std::chrono::steady_clock::now();
        for (auto kvp = it->next(); kvp.has_value(); kvp = it->next()) {
            ++cnt;
            std::this_thread::sleep_for(1ms);
        }
        auto finish = std::chrono::steady_clock::now();
        ASSERT_TRUE(it->is_valid());
        EXPECT_EQ(cnt, 100);
        std::cout << "no prefetch cost="
                  << std::chrono::duration_cast<std::chrono::milliseconds>(finish - start).count()
                  << "ms" << std::endl;

        // Prefetch
        err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        opts.txn = txn.get();
        opts.prefetch = true;
        it = txn_kv->full_range_get(begin, end, opts);
        cnt = 0;
        start = std::chrono::steady_clock::now();
        for (auto kvp = it->next(); kvp.has_value(); kvp = it->next()) {
            ++cnt;
            std::this_thread::sleep_for(1ms);
        }
        finish = std::chrono::steady_clock::now();
        ASSERT_TRUE(it->is_valid());
        EXPECT_EQ(cnt, 100);
        std::cout << "prefetch cost="
                  << std::chrono::duration_cast<std::chrono::milliseconds>(finish - start).count()
                  << "ms" << std::endl;

        // Use RangeGetIterator
        err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        std::unique_ptr<RangeGetIterator> inner_it;
        auto inner_begin = begin;
        cnt = 0;
        start = std::chrono::steady_clock::now();
        do {
            err = txn->get(inner_begin, end, &inner_it, false, 11);
            ASSERT_EQ(err, TxnErrorCode::TXN_OK);
            if (!inner_it->has_next()) {
                break;
            }
            while (inner_it->has_next()) {
                // recycle corresponding resources
                auto [k, v] = inner_it->next();
                std::this_thread::sleep_for(1ms);
                ++cnt;
                if (!inner_it->has_next()) {
                    inner_begin = k;
                }
            }
            inner_begin.push_back('\x00'); // Update to next smallest key for iteration
        } while (inner_it->more());
        finish = std::chrono::steady_clock::now();
        EXPECT_EQ(cnt, 100);
        std::cout << "RangeGetIterator cost="
                  << std::chrono::duration_cast<std::chrono::milliseconds>(finish - start).count()
                  << "ms" << std::endl;
    }
}

TEST(TxnKvTest, RangeGetKeySelector) {
    constexpr std::string_view prefix = "range_get_key_selector_";

    {
        // Remove the existing keys and insert some new keys.
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        std::string last_key = fmt::format("{}{}", prefix, 9);
        encode_int64(INT64_MAX, &last_key);
        txn->remove(prefix, last_key);
        for (int i = 0; i < 5; ++i) {
            std::string key = fmt::format("{}{}", prefix, i);
            txn->put(key, std::to_string(i));
        }
        err = txn->commit();
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
    }

    struct TestCase {
        RangeKeySelector begin_key_selector, end_key_selector;
        std::vector<std::string> expected_keys;
    };

    std::string range_begin = fmt::format("{}{}", prefix, 1);
    std::string range_end = fmt::format("{}{}", prefix, 3);
    std::vector<TestCase> test_case {
            {
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    {fmt::format("{}{}", prefix, 1), fmt::format("{}{}", prefix, 2)},
            },
            {
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    RangeKeySelector::FIRST_GREATER_THAN,
                    {
                            fmt::format("{}{}", prefix, 1),
                            fmt::format("{}{}", prefix, 2),
                            fmt::format("{}{}", prefix, 3),
                    },
            },
            {
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    RangeKeySelector::LAST_LESS_OR_EQUAL,
                    {fmt::format("{}{}", prefix, 1), fmt::format("{}{}", prefix, 2)},
            },
            {
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    RangeKeySelector::LAST_LESS_THAN,
                    {fmt::format("{}{}", prefix, 1)},
            },
            {
                    RangeKeySelector::FIRST_GREATER_THAN,
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    {fmt::format("{}{}", prefix, 2)},
            },
            {
                    RangeKeySelector::FIRST_GREATER_THAN,
                    RangeKeySelector::FIRST_GREATER_THAN,
                    {fmt::format("{}{}", prefix, 2), fmt::format("{}{}", prefix, 3)},
            },
            {
                    RangeKeySelector::FIRST_GREATER_THAN,
                    RangeKeySelector::LAST_LESS_OR_EQUAL,
                    {fmt::format("{}{}", prefix, 2)},
            },
            {
                    RangeKeySelector::FIRST_GREATER_THAN,
                    RangeKeySelector::LAST_LESS_THAN,
                    {},
            },
            {
                    RangeKeySelector::LAST_LESS_OR_EQUAL,
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    {fmt::format("{}{}", prefix, 1), fmt::format("{}{}", prefix, 2)},
            },
            {
                    RangeKeySelector::LAST_LESS_OR_EQUAL,
                    RangeKeySelector::FIRST_GREATER_THAN,
                    {
                            fmt::format("{}{}", prefix, 1),
                            fmt::format("{}{}", prefix, 2),
                            fmt::format("{}{}", prefix, 3),
                    },
            },
            {
                    RangeKeySelector::LAST_LESS_OR_EQUAL,
                    RangeKeySelector::LAST_LESS_OR_EQUAL,
                    {fmt::format("{}{}", prefix, 1), fmt::format("{}{}", prefix, 2)},
            },
            {
                    RangeKeySelector::LAST_LESS_OR_EQUAL,
                    RangeKeySelector::LAST_LESS_THAN,
                    {fmt::format("{}{}", prefix, 1)},
            },
            {
                    RangeKeySelector::LAST_LESS_THAN,
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    {
                            fmt::format("{}{}", prefix, 0),
                            fmt::format("{}{}", prefix, 1),
                            fmt::format("{}{}", prefix, 2),
                    },
            },
            {
                    RangeKeySelector::LAST_LESS_THAN,
                    RangeKeySelector::FIRST_GREATER_THAN,
                    {
                            fmt::format("{}{}", prefix, 0),
                            fmt::format("{}{}", prefix, 1),
                            fmt::format("{}{}", prefix, 2),
                            fmt::format("{}{}", prefix, 3),
                    },
            },
            {
                    RangeKeySelector::LAST_LESS_THAN,
                    RangeKeySelector::LAST_LESS_OR_EQUAL,
                    {
                            fmt::format("{}{}", prefix, 0),
                            fmt::format("{}{}", prefix, 1),
                            fmt::format("{}{}", prefix, 2),
                    },
            },
            {
                    RangeKeySelector::LAST_LESS_THAN,
                    RangeKeySelector::LAST_LESS_THAN,
                    {fmt::format("{}{}", prefix, 0), fmt::format("{}{}", prefix, 1)},
            },
    };

    // Scan range with different key selectors
    for (const auto& tc : test_case) {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        RangeGetOptions options;
        options.batch_limit = 1000;
        options.begin_key_selector = tc.begin_key_selector;
        options.end_key_selector = tc.end_key_selector;
        std::unique_ptr<RangeGetIterator> it;
        err = txn->get(range_begin, range_end, &it, options);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        std::vector<std::string> actual_keys;
        while (it->has_next()) {
            auto [k, v] = it->next();
            actual_keys.emplace_back(k);
        }
        EXPECT_EQ(actual_keys, tc.expected_keys)
                << "Failed for begin_key_selector=" << static_cast<int>(tc.begin_key_selector)
                << ", end_key_selector=" << static_cast<int>(tc.end_key_selector);
    }
}

TEST(TxnKvTest, ReverseRangeGet) {
    constexpr std::string_view prefix = "reverse_range_get_";

    {
        // Remove the existing keys and insert some new keys.
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        std::string last_key = fmt::format("{}{}", prefix, 9);
        encode_int64(INT64_MAX, &last_key);
        txn->remove(prefix, last_key);
        for (int i = 0; i < 5; ++i) {
            std::string key = fmt::format("{}{}", prefix, i);
            txn->put(key, std::to_string(i));
        }
        err = txn->commit();
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
    }

    std::string range_begin = fmt::format("{}{}", prefix, 1);
    std::string range_end = fmt::format("{}{}", prefix, 3);

    struct TestCase {
        RangeKeySelector begin_key_selector, end_key_selector;
        std::vector<std::string> expected_keys;
    };

    std::vector<TestCase> test_case {
            // 1. [begin, end)
            {
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    {fmt::format("{}{}", prefix, 2), fmt::format("{}{}", prefix, 1)},
            },
            // 2. [begin, end]
            {
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    RangeKeySelector::FIRST_GREATER_THAN,
                    {
                            fmt::format("{}{}", prefix, 3),
                            fmt::format("{}{}", prefix, 2),
                            fmt::format("{}{}", prefix, 1),
                    },
            },
            // 3. (begin, end)
            {
                    RangeKeySelector::FIRST_GREATER_THAN,
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    {fmt::format("{}{}", prefix, 2)},
            },
            // 4. (begin, end]
            {
                    RangeKeySelector::FIRST_GREATER_THAN,
                    RangeKeySelector::FIRST_GREATER_THAN,
                    {fmt::format("{}{}", prefix, 3), fmt::format("{}{}", prefix, 2)},
            },
    };
    for (const auto& tc : test_case) {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        RangeGetOptions options;
        options.batch_limit = 1000;
        options.begin_key_selector = tc.begin_key_selector;
        options.end_key_selector = tc.end_key_selector;
        options.reverse = true; // Reserve range get
        std::unique_ptr<RangeGetIterator> it;
        err = txn->get(range_begin, range_end, &it, options);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        std::vector<std::string> actual_keys;
        while (it->has_next()) {
            auto [k, v] = it->next();
            actual_keys.emplace_back(k);
        }
        EXPECT_EQ(actual_keys, tc.expected_keys)
                << "Failed for begin_key_selector=" << static_cast<int>(tc.begin_key_selector)
                << ", end_key_selector=" << static_cast<int>(tc.end_key_selector);
    }
}

TEST(TxnKvTest, ReverseFullRangeGet) {
    constexpr std::string_view prefix = "reverse_full_range_get_";

    {
        // Remove the existing keys and insert some new keys.
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        std::string last_key = fmt::format("{}{:03}", prefix, 99);
        encode_int64(INT64_MAX, &last_key);
        txn->remove(prefix, last_key);
        for (int i = 0; i < 100; ++i) {
            std::string key = fmt::format("{}{:03}", prefix, i);
            txn->put(key, std::to_string(i));
        }
        err = txn->commit();
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
    }

    std::string range_begin = fmt::format("{}{:03}", prefix, 1);
    std::string range_end = fmt::format("{}{:03}", prefix, 98);

    struct TestCase {
        RangeKeySelector begin_key_selector, end_key_selector;
    };

    std::vector<TestCase> test_case {
            // 1. [begin, end)
            {
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
            },
            // 2. [begin, end]
            {
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    RangeKeySelector::FIRST_GREATER_THAN,
            },
            // 3. (begin, end)
            {
                    RangeKeySelector::FIRST_GREATER_THAN,
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
            },
            // 4. (begin, end]
            {
                    RangeKeySelector::FIRST_GREATER_THAN,
                    RangeKeySelector::FIRST_GREATER_THAN,
            },
    };

    for (const auto& tc : test_case) {
        std::vector<std::string> expected_keys;
        {
            // Read the expected keys via range_get
            std::unique_ptr<Transaction> txn;
            TxnErrorCode err = txn_kv->create_txn(&txn);
            ASSERT_EQ(err, TxnErrorCode::TXN_OK);

            RangeGetOptions options;
            options.batch_limit = 11;
            options.begin_key_selector = tc.begin_key_selector;
            options.end_key_selector = tc.end_key_selector;
            options.reverse = true; // Reserve range get
            std::string begin = range_begin, end = range_end;

            std::unique_ptr<RangeGetIterator> it;
            do {
                err = txn->get(begin, end, &it, options);
                ASSERT_EQ(err, TxnErrorCode::TXN_OK);

                while (it->has_next()) {
                    auto [k, v] = it->next();
                    expected_keys.emplace_back(k);
                }
                // Get next begin key for reverse range get
                end = it->last_key();
                options.end_key_selector = RangeKeySelector::FIRST_GREATER_OR_EQUAL;
            } while (it->more());
        }

        std::vector<std::string> actual_keys;
        {
            // Read the actual keys via full_range_get
            FullRangeGetOptions opts(txn_kv);
            opts.batch_limit = 11;
            opts.begin_key_selector = tc.begin_key_selector;
            opts.end_key_selector = tc.end_key_selector;
            opts.reverse = true; // Reserve full range get

            auto it = txn_kv->full_range_get(range_begin, range_end, opts);
            ASSERT_TRUE(it->is_valid());

            while (it->has_next()) {
                auto kvp = it->next();
                ASSERT_TRUE(kvp.has_value());
                auto [k, v] = *kvp;
                actual_keys.emplace_back(k);
            }
        }

        EXPECT_EQ(actual_keys, expected_keys)
                << "Failed for begin_key_selector=" << static_cast<int>(tc.begin_key_selector)
                << ", end_key_selector=" << static_cast<int>(tc.end_key_selector);
    }
}

TEST(TxnKvTest, ReverseFullRangeGet2) {
    constexpr std::string_view prefix = "reverse_full_range_get_2_";

    {
        // Remove the existing keys and insert some new keys.
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        std::string last_key = fmt::format("{}b", prefix);
        encode_int64(INT64_MAX, &last_key);
        txn->remove(prefix, last_key);
        std::string key(prefix);
        for (int i = 0; i < 100; ++i) {
            key += 'a';
            txn->put(key, std::to_string(i));
        }
        err = txn->commit();
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
    }

    std::string range_begin(prefix);
    std::string range_end = fmt::format("{}b", prefix);

    struct TestCase {
        RangeKeySelector begin_key_selector, end_key_selector;
    };

    std::vector<TestCase> test_case {
            // 1. [begin, end)
            {
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
            },
            // 2. [begin, end]
            {
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    RangeKeySelector::FIRST_GREATER_THAN,
            },
            // 3. (begin, end)
            {
                    RangeKeySelector::FIRST_GREATER_THAN,
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
            },
            // 4. (begin, end]
            {
                    RangeKeySelector::FIRST_GREATER_THAN,
                    RangeKeySelector::FIRST_GREATER_THAN,
            },
    };

    for (const auto& tc : test_case) {
        std::vector<std::string> expected_keys;
        {
            // Read the expected keys via range_get
            std::unique_ptr<Transaction> txn;
            TxnErrorCode err = txn_kv->create_txn(&txn);
            ASSERT_EQ(err, TxnErrorCode::TXN_OK);

            RangeGetOptions options;
            options.batch_limit = 11;
            options.begin_key_selector = tc.begin_key_selector;
            options.end_key_selector = tc.end_key_selector;
            options.reverse = true; // Reserve range get
            std::string begin = range_begin, end = range_end;

            std::unique_ptr<RangeGetIterator> it;
            do {
                err = txn->get(begin, end, &it, options);
                ASSERT_EQ(err, TxnErrorCode::TXN_OK);

                while (it->has_next()) {
                    auto [k, v] = it->next();
                    expected_keys.emplace_back(k);
                }
                // Get next begin key for reverse range get
                end = it->last_key();
                options.end_key_selector = RangeKeySelector::FIRST_GREATER_OR_EQUAL;
            } while (it->more());
        }

        std::vector<std::string> actual_keys;
        {
            // Read the actual keys via full_range_get
            FullRangeGetOptions opts(txn_kv);
            opts.batch_limit = 11;
            opts.begin_key_selector = tc.begin_key_selector;
            opts.end_key_selector = tc.end_key_selector;
            opts.reverse = true; // Reserve full range get

            auto it = txn_kv->full_range_get(range_begin, range_end, opts);
            ASSERT_TRUE(it->is_valid());

            while (it->has_next()) {
                auto kvp = it->next();
                ASSERT_TRUE(kvp.has_value());
                auto [k, v] = *kvp;
                actual_keys.emplace_back(k);
            }
        }

        EXPECT_EQ(actual_keys, expected_keys)
                << "Failed for begin_key_selector=" << static_cast<int>(tc.begin_key_selector)
                << ", end_key_selector=" << static_cast<int>(tc.end_key_selector);
    }
}

TEST(TxnKvTest, BatchScan) {
    std::vector<std::pair<std::string, std::string>> test_data = {
            {"BatchScan_different_key", "different_value"},
            {"BatchScan_prefix1", "value1"},
            {"BatchScan_prefix1_sub1", "sub_value1"},
            {"BatchScan_prefix1_sub2", "sub_value2"},
            {"BatchScan_prefix2", "value2"},
            {"BatchScan_prefix2_sub1", "sub_value3"},
            {"BatchScan_prefix3", "value3"}};

    std::unique_ptr<Transaction> txn;

    {
        auto ret = txn_kv->create_txn(&txn);
        ASSERT_EQ(ret, TxnErrorCode::TXN_OK);
        for (const auto& [key, val] : test_data) {
            txn->put(key, val);
        }
        ret = txn->commit();
        ASSERT_EQ(ret, TxnErrorCode::TXN_OK);
    }

    struct TestCase {
        bool reverse;
        std::vector<std::string> scan_keys;
        std::vector<std::optional<std::string>> expected_keys;
    };

    std::vector<TestCase> test_cases = {
            {
                    false,
                    {"BatchScan_prefix1", "BatchScan_prefix2", "BatchScan_prefix3",
                     "BatchScan_different_key"},
                    {"BatchScan_prefix1", "BatchScan_prefix2", "BatchScan_prefix3",
                     "BatchScan_different_key"},
            },
            {
                    false,
                    {"BatchScan_prefix1_", "BatchScan_prefix2_"},
                    {"BatchScan_prefix1_sub1", "BatchScan_prefix2_sub1"},
            },
            {
                    true,
                    {"BatchScan_prefix1", "BatchScan_prefix2", "BatchScan_prefix3",
                     "BatchScan_different_key"},
                    {"BatchScan_prefix1_sub2", "BatchScan_prefix2_sub1", "BatchScan_prefix3",
                     "BatchScan_different_key"},
            },
            {
                    true,
                    {"BatchScan_prefix1_", "BatchScan_prefix2_"},
                    {"BatchScan_prefix1_sub2", "BatchScan_prefix2_sub1"},
            },
            {
                    true,
                    {"BatchScan_prefix4"},
                    {std::nullopt},
            }};

    size_t count = 0;
    for (auto& tc : test_cases) {
        auto ret = txn_kv->create_txn(&txn);
        ASSERT_EQ(ret, TxnErrorCode::TXN_OK);
        std::vector<std::optional<std::pair<std::string, std::string>>> results;
        Transaction::BatchGetOptions opts;
        opts.reverse = tc.reverse; // Reverse order
        opts.snapshot = false;

        ret = txn->batch_scan(&results, tc.scan_keys, opts);
        ASSERT_EQ(ret, TxnErrorCode::TXN_OK);
        ASSERT_EQ(results.size(), tc.scan_keys.size());

        count += 1;
        for (size_t i = 0; i < results.size(); ++i) {
            if (tc.expected_keys[i].has_value()) {
                ASSERT_TRUE(results[i].has_value());
                std::string& key = results[i].value().first;
                ASSERT_EQ(key, tc.expected_keys[i]) << tc.scan_keys[i] << ", tc: " << count;
            } else {
                ASSERT_FALSE(results[i].has_value()) << tc.scan_keys[i] << ", tc: " << count;
            }
        }
    }
}

TEST(TxnKvTest, ReportConflictingRange) {
    config::enable_logging_conflict_keys = true;

    constexpr std::string_view key_prefix = "txn_kv_test__report_conflicting_range";
    std::string key = std::string(key_prefix) + std::to_string(time(nullptr));

    {
        // 1. write a common key
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(key, "value0");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // 2. two txns, conflicting writes
    std::unique_ptr<Transaction> txn1, txn2;
    ASSERT_EQ(txn_kv->create_txn(&txn1), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn_kv->create_txn(&txn2), TxnErrorCode::TXN_OK);

    std::string val1, val2;
    ASSERT_EQ(txn1->get(key, &val1), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn2->get(key, &val2), TxnErrorCode::TXN_OK);

    txn1->put(key, "value1");
    txn2->put(key, "value2");

    ASSERT_EQ(txn1->commit(), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn2->commit(), TxnErrorCode::TXN_CONFLICT);

    // 3. get the conflicting ranges.
    std::vector<std::pair<std::string, std::string>> values;
    ASSERT_EQ(reinterpret_cast<fdb::Transaction*>(txn2.get())->get_conflicting_range(&values),
              TxnErrorCode::TXN_OK);
    ASSERT_EQ(values.size(), 2);
    ASSERT_EQ(values[0].first, key);
    ASSERT_EQ(values[1].second, "0");
    ASSERT_TRUE(values[1].first.starts_with(key));
}
