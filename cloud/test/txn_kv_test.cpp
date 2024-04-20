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

#include "meta-service/txn_kv.h"

#include <bthread/bthread.h>
#include <fmt/format.h>
#include <foundationdb/fdb_c_options.g.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <cstddef>

#include "common/config.h"
#include "common/stopwatch.h"
#include "common/sync_point.h"
#include "common/util.h"
#include "meta-service/doris_txn.h"
#include "meta-service/keys.h"
#include "meta-service/mem_txn_kv.h"
#include "meta-service/txn_kv.h"
#include "meta-service/txn_kv_error.h"

using namespace doris::cloud;

std::shared_ptr<TxnKv> txn_kv;

void init_txn_kv() {
    config::fdb_cluster_file_path = "fdb.cluster";
    // UT may be run without fdb, it will take unnecessary time with the default value
    config::fdb_txn_timeout_ms = 500;
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
    std::string key_prefix = "key_1";

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
        ASSERT_EQ(key_1.length(), key_prefix.size() + 10); // versionstamp = 10bytes
        key_1.remove_prefix(key_prefix.size());
        versionstamp_1 = key_1;
    }

    std::string versionstamp_2;
    {
        // write key_2
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        key_prefix = "key_2";
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
    ASSERT_EQ(doris::cloud::get(txn.get(), key, &val_buf), TxnErrorCode::TXN_KEY_NOT_FOUND);
    txn->put(key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    // Check get
    TxnErrorCode err = txn_kv->create_txn(&txn);
    ASSERT_EQ(err, TxnErrorCode::TXN_OK);
    err = doris::cloud::key_exists(txn.get(), key);
    ASSERT_EQ(err, TxnErrorCode::TXN_OK);
    err = doris::cloud::get(txn.get(), key, &val_buf);
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
    ASSERT_EQ(doris::cloud::get(txn.get(), key, &val_buf), TxnErrorCode::TXN_KEY_NOT_FOUND);
}

TEST(TxnKvTest, PutLargeValueTest) {
    auto txn_kv = std::make_shared<MemTxnKv>();

    auto sp = SyncPoint::get_instance();
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
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
    doris::cloud::put(txn.get(), key, schema, 1, 100);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    // Check get
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    ValueBuf val_buf;
    doris::TabletSchemaCloudPB saved_schema;
    ASSERT_EQ(doris::cloud::key_exists(txn.get(), key), TxnErrorCode::TXN_OK);
    TxnErrorCode err = doris::cloud::get(txn.get(), key, &val_buf);
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
    sp->set_call_back("memkv::Transaction::get", [](void* limit) { *((int*)limit) = 100; });
    err = doris::cloud::get(txn.get(), key, &val_buf);
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
    ASSERT_EQ(doris::cloud::get(txn.get(), key, &val_buf), TxnErrorCode::TXN_KEY_NOT_FOUND);
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
