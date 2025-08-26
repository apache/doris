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

#include "txn_kv.h"

#include <bthread/countdown_event.h>
#include <byteswap.h>
#include <foundationdb/fdb_c_types.h>

#include <algorithm>
#include <atomic>
#include <bit>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "common/bvars.h"
#include "common/config.h"
#include "common/defer.h"
#include "common/logging.h"
#include "common/stopwatch.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-store/keys.h"
#include "meta-store/txn_kv_error.h"

// =============================================================================
//  FoundationDB implementation of TxnKv
// =============================================================================

#define RETURN_IF_ERROR(op)                            \
    do {                                               \
        TxnErrorCode code = op;                        \
        if (code != TxnErrorCode::TXN_OK) return code; \
    } while (false)

namespace doris::cloud {

static void may_logging_single_version_reading(std::string_view key) {
    if (!config::enable_logging_for_single_version_reading) {
        return;
    }

    if (!key.starts_with(CLOUD_USER_KEY_SPACE01)) {
        return;
    }

    std::vector<std::string> single_version_meta_key_prefixs =
            get_single_version_meta_key_prefixs();
    for (std::string& prefix : single_version_meta_key_prefixs) {
        if (key.starts_with(prefix)) {
            LOG(WARNING) << "Read single version meta key: " << hex(key);
            return;
        }
    }
}

static std::tuple<fdb_bool_t, int> apply_key_selector(RangeKeySelector selector) {
    // Keep consistent with FDB_KEYSEL_* constants.
    switch (selector) {
    case RangeKeySelector::FIRST_GREATER_OR_EQUAL:
        return {0, 1};
    case RangeKeySelector::FIRST_GREATER_THAN:
        return {1, 1};
    case RangeKeySelector::LAST_LESS_OR_EQUAL:
        return {1, 0};
    case RangeKeySelector::LAST_LESS_THAN:
        return {0, 0};
    }
    LOG(FATAL) << "Unknown RangeKeySelector: " << static_cast<int>(selector);
    return {0, 0};
}

int FdbTxnKv::init() {
    network_ = std::make_shared<fdb::Network>(FDBNetworkOption {});
    int ret = network_->init();
    if (ret != 0) {
        LOG(WARNING) << "failed to init network";
        return ret;
    }
    database_ = std::make_shared<fdb::Database>(network_, config::fdb_cluster_file_path,
                                                FDBDatabaseOption {});
    ret = database_->init();
    if (ret != 0) {
        LOG(WARNING) << "failed to init database";
        return ret;
    }
    return 0;
}

TxnErrorCode FdbTxnKv::create_txn(std::unique_ptr<Transaction>* txn) {
    auto* t = new fdb::Transaction(database_);
    txn->reset(t);
    auto ret = t->init();
    if (ret != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to init txn, ret=" << ret;
    }
    return ret;
}

TxnErrorCode FdbTxnKv::create_txn_with_system_access(std::unique_ptr<Transaction>* txn) {
    auto t = std::make_unique<fdb::Transaction>(database_);
    TxnErrorCode code = t->init();
    if (code == TxnErrorCode::TXN_OK) {
        code = t->enable_access_system_keys();
    }
    if (code != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to init txn, ret=" << code;
        return code;
    }

    *txn = std::move(t);
    return TxnErrorCode::TXN_OK;
}

std::unique_ptr<FullRangeGetIterator> FdbTxnKv::full_range_get(std::string begin, std::string end,
                                                               FullRangeGetOptions opts) {
    return std::make_unique<fdb::FullRangeGetIterator>(std::move(begin), std::move(end),
                                                       std::move(opts));
}

TxnErrorCode FdbTxnKv::get_partition_boundaries(std::vector<std::string>* boundaries) {
    boundaries->clear();

    std::unique_ptr<Transaction> txn;
    TxnErrorCode code = create_txn_with_system_access(&txn);
    if (code != TxnErrorCode::TXN_OK) {
        return code;
    }

    std::string begin_key(fdb_partition_key_prefix());
    std::string end_key(fdb_partition_key_end());

    RangeGetOptions opts;
    opts.snapshot = true;
    std::unique_ptr<RangeGetIterator> iter;
    do {
        code = txn->get(begin_key, end_key, &iter, opts);
        if (code != TxnErrorCode::TXN_OK) {
            if (code == TxnErrorCode::TXN_TOO_OLD) {
                code = create_txn_with_system_access(&txn);
                if (code == TxnErrorCode::TXN_OK) {
                    continue;
                }
            }
            LOG_WARNING("failed to get fdb boundaries")
                    .tag("code", code)
                    .tag("begin_key", hex(begin_key))
                    .tag("end_key", hex(end_key));
            return code;
        }

        while (iter->has_next()) {
            auto&& [key, value] = iter->next();
            boundaries->emplace_back(key);
        }

        begin_key = iter->next_begin_key();
    } while (iter->more());

    return TxnErrorCode::TXN_OK;
}

double FdbTxnKv::get_client_thread_busyness() const {
    return fdb_database_get_main_thread_busyness(database_->db());
}

} // namespace doris::cloud

namespace doris::cloud::fdb {

// Ref https://apple.github.io/foundationdb/api-error-codes.html#developer-guide-error-codes.
constexpr fdb_error_t FDB_ERROR_CODE_TIMED_OUT = 1004;
constexpr fdb_error_t FDB_ERROR_CODE_TXN_TOO_OLD = 1007;
constexpr fdb_error_t FDB_ERROR_CODE_TXN_CONFLICT = 1020;
constexpr fdb_error_t FDB_ERROR_CODE_TXN_TIMED_OUT = 1031;
constexpr fdb_error_t FDB_ERROR_CODE_INVALID_OPTION_VALUE = 2006;
constexpr fdb_error_t FDB_ERROR_CODE_INVALID_OPTION = 2007;
constexpr fdb_error_t FDB_ERROR_CODE_VERSION_INVALID = 2011;
constexpr fdb_error_t FDB_ERROR_CODE_RANGE_LIMITS_INVALID = 2012;
constexpr fdb_error_t FDB_ERROR_CODE_TXN_TOO_LARGE = 2101;
constexpr fdb_error_t FDB_ERROR_CODE_KEY_TOO_LARGE = 2102;
constexpr fdb_error_t FDB_ERROR_CODE_VALUE_TOO_LARGE = 2103;
constexpr fdb_error_t FDB_ERROR_CODE_CONNECTION_STRING_INVALID = 2104;

static bool fdb_error_is_txn_conflict(fdb_error_t err) {
    return err == FDB_ERROR_CODE_TXN_CONFLICT;
}

static TxnErrorCode cast_as_txn_code(fdb_error_t err) {
    switch (err) {
    case 0:
        return TxnErrorCode::TXN_OK;
    case FDB_ERROR_CODE_INVALID_OPTION:
    case FDB_ERROR_CODE_INVALID_OPTION_VALUE:
    case FDB_ERROR_CODE_VERSION_INVALID:
    case FDB_ERROR_CODE_RANGE_LIMITS_INVALID:
    case FDB_ERROR_CODE_CONNECTION_STRING_INVALID:
        return TxnErrorCode::TXN_INVALID_ARGUMENT;
    case FDB_ERROR_CODE_TXN_TOO_LARGE:
        return TxnErrorCode::TXN_BYTES_TOO_LARGE;
    case FDB_ERROR_CODE_KEY_TOO_LARGE:
        return TxnErrorCode::TXN_KEY_TOO_LARGE;
    case FDB_ERROR_CODE_VALUE_TOO_LARGE:
        return TxnErrorCode::TXN_VALUE_TOO_LARGE;
    case FDB_ERROR_CODE_TIMED_OUT:
    case FDB_ERROR_CODE_TXN_TIMED_OUT:
        return TxnErrorCode::TXN_TIMEOUT;
    case FDB_ERROR_CODE_TXN_TOO_OLD:
        return TxnErrorCode::TXN_TOO_OLD;
    case FDB_ERROR_CODE_TXN_CONFLICT:
        return TxnErrorCode::TXN_CONFLICT;
    }

    if (fdb_error_predicate(FDB_ERROR_PREDICATE_MAYBE_COMMITTED, err)) {
        return TxnErrorCode::TXN_MAYBE_COMMITTED;
    }
    if (fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE_NOT_COMMITTED, err)) {
        return TxnErrorCode::TXN_RETRYABLE_NOT_COMMITTED;
    }
    return TxnErrorCode::TXN_UNIDENTIFIED_ERROR;
}

// =============================================================================
// Impl of Network
// =============================================================================
decltype(Network::working) Network::working {false};

int Network::init() {
    // Globally once
    bool expected = false;
    if (!Network::working.compare_exchange_strong(expected, true)) return 1;

    fdb_error_t err = fdb_select_api_version(fdb_get_max_api_version());
    if (err) {
        LOG(WARNING) << "failed to select api version, max api version: "
                     << fdb_get_max_api_version() << ", err: " << fdb_get_error(err);
        return 1;
    }

    // Setup network thread
    // Optional setting
    // FDBNetworkOption opt;
    // fdb_network_set_option()
    (void)opt_;
    // ATTN: Network can be configured only once,
    //       even if fdb_stop_network() is called successfully
    err = fdb_setup_network(); // Must be called only once before any
                               // other functions of C-API
    if (err) {
        LOG(WARNING) << "failed to setup fdb network, err: " << fdb_get_error(err);
        return 1;
    }

    // Network complete callback is optional, and useful for some cases
    //   std::function<void()> network_complete_callback =
    //                         []() { std::cout << __PRETTY_FUNCTION__ << std::endl; };
    //   err = fdb_add_network_thread_completion_hook(callback1,
    //                                                &network_complete_callback);
    //   std::cout << "fdb_add_network_thread_completion_hook error: "
    //     << fdb_get_error(err) << std::endl;
    //   if (err) std::exit(-1);

    // Run network in a separate thread
    network_thread_ = std::shared_ptr<std::thread>(
            new std::thread([] {
                // Will not return until fdb_stop_network() called
                auto err = fdb_run_network();
                LOG(WARNING) << "exit fdb_run_network"
                             << (err ? std::string(", error: ") + fdb_get_error(err) : "");
            }),
            [](auto* p) {
                auto err = fdb_stop_network();
                LOG(WARNING) << "fdb_stop_network"
                             << (err ? std::string(", error: ") + fdb_get_error(err) : "");
                p->join();
                delete p;

                // Another network will work only after this thread exits
                bool expected = true;
                Network::working.compare_exchange_strong(expected, false);
            });
    pthread_setname_np(network_thread_->native_handle(), "fdb_network_thread");

    return 0;
}

void Network::stop() {
    network_thread_.reset();
}

// =============================================================================
// Impl of Database
// =============================================================================

int Database::init() {
    // TODO: process opt
    fdb_error_t err = fdb_create_database(cluster_file_path_.c_str(), &db_);
    if (err) {
        LOG(WARNING) << __PRETTY_FUNCTION__ << " fdb_create_database error: " << fdb_get_error(err)
                     << " conf: " << cluster_file_path_;
        return 1;
    }

    return 0;
}

// =============================================================================
// Impl of Transaction
// =============================================================================

TxnErrorCode Transaction::init() {
    // TODO: process opt
    fdb_error_t err = fdb_database_create_transaction(db_->db(), &txn_);
    TEST_SYNC_POINT_CALLBACK("transaction:init:create_transaction_err", &err);
    if (err) {
        LOG(WARNING) << __PRETTY_FUNCTION__
                     << " fdb_database_create_transaction error:" << fdb_get_error(err);
        return cast_as_txn_code(err);
    }

    // FDB txn callback only guaranteed *at most once*, because the future might be set to `Never`
    // by unexpected. In order to achieve *exactly once* semantic, a timeout must be set to force
    // fdb cancel future and invoke callback eventually.
    //
    // See:
    // - https://apple.github.io/foundationdb/api-c.html#fdb_future_set_callback.
    // - https://forums.foundationdb.org/t/does-fdb-future-set-callback-guarantee-exactly-once-execution/1498/2
    static_assert(sizeof(config::fdb_txn_timeout_ms) == sizeof(int64_t));
    err = fdb_transaction_set_option(txn_, FDBTransactionOption::FDB_TR_OPTION_TIMEOUT,
                                     (const uint8_t*)&config::fdb_txn_timeout_ms,
                                     sizeof(config::fdb_txn_timeout_ms));
    if (err) {
        LOG_WARNING("fdb_transaction_set_option error: ")
                .tag("code", err)
                .tag("msg", fdb_get_error(err));
        return cast_as_txn_code(err);
    }

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode Transaction::enable_access_system_keys() {
    fdb_error_t err = fdb_transaction_set_option(
            txn_, FDBTransactionOption::FDB_TR_OPTION_ACCESS_SYSTEM_KEYS, nullptr, 0);
    if (err) {
        LOG_WARNING("fdb_transaction_set_option error: ")
                .tag("option", "FDB_TR_OPTION_ACCESS_SYSTEM_KEYS")
                .tag("code", err)
                .tag("msg", fdb_get_error(err));
        return cast_as_txn_code(err);
    }
    return TxnErrorCode::TXN_OK;
}

void Transaction::put(std::string_view key, std::string_view val) {
    StopWatch sw;
    fdb_transaction_set(txn_, (uint8_t*)key.data(), key.size(), (uint8_t*)val.data(), val.size());
    g_bvar_txn_kv_put << sw.elapsed_us();

    ++num_put_keys_;
    put_bytes_ += key.size() + val.size();
    approximate_bytes_ += key.size() * 3 + val.size(); // See fdbclient/ReadYourWrites.actor.cpp
}

// return 0 for success otherwise error
static TxnErrorCode bthread_fdb_future_block_until_ready(FDBFuture* fut) {
    bthread::CountdownEvent event;
    static auto callback = [](FDBFuture*, void* event) {
        ((bthread::CountdownEvent*)event)->signal();
    };
    auto err = fdb_future_set_callback(fut, callback, &event);
    if (err) [[unlikely]] {
        LOG(WARNING) << "fdb_future_set_callback failed, err=" << fdb_get_error(err);
        return cast_as_txn_code(err);
    }
    if (int ec = event.wait(); ec != 0) [[unlikely]] {
        LOG(WARNING) << "CountdownEvent wait failed, err=" << std::strerror(ec);
        return TxnErrorCode::TXN_UNIDENTIFIED_ERROR;
    }
    return TxnErrorCode::TXN_OK;
}

// return TXN_OK for success otherwise error
static TxnErrorCode await_future(FDBFuture* fut) {
    if (bthread_self() != 0) {
        return bthread_fdb_future_block_until_ready(fut);
    }
    auto err = fdb_future_block_until_ready(fut);
    if (err) [[unlikely]] {
        LOG(WARNING) << "fdb_future_block_until_ready failed: " << fdb_get_error(err);
        return cast_as_txn_code(err);
    }
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode Transaction::get(std::string_view key, std::string* val, bool snapshot) {
    may_logging_single_version_reading(key);

    StopWatch sw;
    approximate_bytes_ += key.size() * 2; // See fdbclient/ReadYourWrites.actor.cpp for details
    auto* fut = fdb_transaction_get(txn_, (uint8_t*)key.data(), key.size(), snapshot);

    g_bvar_txn_kv_get_count_normalized << 1;
    auto release_fut = [fut, &sw](int*) {
        fdb_future_destroy(fut);
        g_bvar_txn_kv_get << sw.elapsed_us();
    };
    std::unique_ptr<int, decltype(release_fut)> defer((int*)0x01, std::move(release_fut));
    RETURN_IF_ERROR(await_future(fut));
    auto err = fdb_future_get_error(fut);
    TEST_SYNC_POINT_CALLBACK("transaction:get:get_err", &err);
    if (err) {
        LOG(WARNING) << __PRETTY_FUNCTION__
                     << " failed to fdb_future_get_error err=" << fdb_get_error(err)
                     << " key=" << hex(key);
        return cast_as_txn_code(err);
    }

    fdb_bool_t found;
    const uint8_t* ret;
    int len;
    err = fdb_future_get_value(fut, &found, &ret, &len);
    num_get_keys_++;

    if (err) {
        LOG(WARNING) << __PRETTY_FUNCTION__
                     << " failed to fdb_future_get_value err=" << fdb_get_error(err)
                     << " key=" << hex(key);
        return cast_as_txn_code(err);
    }
    get_bytes_ += len + key.size();

    if (!found) return TxnErrorCode::TXN_KEY_NOT_FOUND;
    *val = std::string((char*)ret, len);
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode Transaction::get(std::string_view begin, std::string_view end,
                              std::unique_ptr<cloud::RangeGetIterator>* iter,
                              const RangeGetOptions& opts) {
    may_logging_single_version_reading(begin);

    StopWatch sw;
    approximate_bytes_ += begin.size() + end.size();
    DORIS_CLOUD_DEFER {
        g_bvar_txn_kv_range_get << sw.elapsed_us();
    };

    int limit = opts.batch_limit;
    fdb_bool_t snapshot = opts.snapshot ? 1 : 0;
    fdb_bool_t reverse = opts.reverse ? 1 : 0;
    auto [begin_or_equal, begin_offset] = apply_key_selector(opts.begin_key_selector);
    auto [end_or_equal, end_offset] = apply_key_selector(opts.end_key_selector);
    FDBFuture* fut = fdb_transaction_get_range(
            txn_, (uint8_t*)begin.data(), begin.size(), begin_or_equal, begin_offset,
            (uint8_t*)end.data(), end.size(), end_or_equal, end_offset, limit,
            0 /*target_bytes, unlimited*/, FDBStreamingMode::FDB_STREAMING_MODE_WANT_ALL,
            0 /*iteration*/, snapshot, reverse);

    RETURN_IF_ERROR(await_future(fut));
    auto err = fdb_future_get_error(fut);
    TEST_SYNC_POINT_CALLBACK("transaction:get_range:get_err", &err);
    if (err) {
        LOG(WARNING) << fdb_get_error(err);
        return cast_as_txn_code(err);
    }

    std::unique_ptr<RangeGetIterator> ret(new RangeGetIterator(fut));
    RETURN_IF_ERROR(ret->init());
    num_get_keys_ += ret->size();
    get_bytes_ += ret->get_kv_bytes();
    g_bvar_txn_kv_get_count_normalized << ret->size();

    *(iter) = std::move(ret);

    return TxnErrorCode::TXN_OK;
}

std::unique_ptr<cloud::FullRangeGetIterator> Transaction::full_range_get(std::string_view begin,
                                                                         std::string_view end,
                                                                         FullRangeGetOptions opts) {
    // We don't need to hold a reference to the TxnKv here, since there is a txn full range iterator.
    opts.txn = this;
    opts.txn_kv.reset();
    return std::make_unique<FullRangeGetIterator>(std::string(begin), std::string(end), opts);
}

void Transaction::atomic_set_ver_key(std::string_view key_prefix, std::string_view val) {
    StopWatch sw;
    std::string key(key_prefix);
    int prefix_size = key.size();
    // ATTN:
    // 10 bytes for versiontimestamp must be 0, trailing 4 bytes is for prefix len
    key.append(14, '\0');
    std::memcpy(key.data() + (key.size() - 4), &prefix_size, 4);

    fdb_transaction_atomic_op(txn_, (uint8_t*)key.data(), key.size(), (uint8_t*)val.data(),
                              val.size(),
                              FDBMutationType::FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_KEY);

    g_bvar_txn_kv_atomic_set_ver_key << sw.elapsed_us();
    ++num_put_keys_;
    put_bytes_ += key.size() + val.size();
    approximate_bytes_ += key.size() * 3 + val.size();
}

bool Transaction::atomic_set_ver_key(std::string_view key, uint32_t offset, std::string_view val) {
    if (key.size() < 10 || offset + 10 > key.size()) {
        LOG(WARNING) << "atomic_set_ver_key: invalid key or offset, key=" << hex(key)
                     << " offset=" << offset << ", key_size=" << key.size();
        return false;
    }

    StopWatch sw;
    std::string key_buf(key);
    // 4 bytes for prefix len, assume in letter-endian
    key_buf.append((const char*)&offset, 4);

    fdb_transaction_atomic_op(txn_, (uint8_t*)key_buf.data(), key_buf.size(), (uint8_t*)val.data(),
                              val.size(),
                              FDBMutationType::FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_KEY);

    g_bvar_txn_kv_atomic_set_ver_key << sw.elapsed_us();
    ++num_put_keys_;
    put_bytes_ += key_buf.size() + val.size();
    approximate_bytes_ += key_buf.size() * 3 + val.size();
    return true;
}

void Transaction::atomic_set_ver_value(std::string_view key, std::string_view value) {
    StopWatch sw;
    std::string val(value);
    int prefix_size = val.size();
    // ATTN:
    // 10 bytes for versiontimestamp must be 0, trailing 4 bytes is for prefix len
    val.append(14, '\0');
    std::memcpy(val.data() + (val.size() - 4), &prefix_size, 4);

    fdb_transaction_atomic_op(txn_, (uint8_t*)key.data(), key.size(), (uint8_t*)val.data(),
                              val.size(),
                              FDBMutationType::FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_VALUE);

    g_bvar_txn_kv_atomic_set_ver_value << sw.elapsed_us();
    ++num_put_keys_;
    put_bytes_ += key.size() + val.size();
    approximate_bytes_ += key.size() * 3 + val.size();
}

void Transaction::atomic_add(std::string_view key, int64_t to_add) {
    StopWatch sw;
    auto val = std::make_unique<std::string>(sizeof(to_add), '\0');
    std::memcpy(val->data(), &to_add, sizeof(to_add));
    fdb_transaction_atomic_op(txn_, (uint8_t*)key.data(), key.size(), (uint8_t*)val->data(),
                              sizeof(to_add), FDBMutationType::FDB_MUTATION_TYPE_ADD);

    g_bvar_txn_kv_atomic_add << sw.elapsed_us();
    ++num_put_keys_;
    put_bytes_ += key.size() + 8;
    approximate_bytes_ += key.size() * 3 + 8;
}

bool Transaction::decode_atomic_int(std::string_view data, int64_t* val) {
    if (data.size() != sizeof(*val)) {
        return false;
    }

    // ATTN: The FDB_MUTATION_TYPE_ADD stores integers in a little-endian representation.
    std::memcpy(val, data.data(), sizeof(*val));
    if constexpr (std::endian::native == std::endian::big) {
        *val = bswap_64(*val);
    }
    return true;
}

void Transaction::remove(std::string_view key) {
    StopWatch sw;
    fdb_transaction_clear(txn_, (uint8_t*)key.data(), key.size());
    g_bvar_txn_kv_remove << sw.elapsed_us();
    ++num_del_keys_;
    delete_bytes_ += key.size();
    approximate_bytes_ += key.size() * 4; // See fdbclient/ReadYourWrites.actor.cpp for details.
}

void Transaction::remove(std::string_view begin, std::string_view end) {
    StopWatch sw;
    fdb_transaction_clear_range(txn_, (uint8_t*)begin.data(), begin.size(), (uint8_t*)end.data(),
                                end.size());
    g_bvar_txn_kv_range_remove << sw.elapsed_us();
    num_del_keys_ += 2;
    delete_bytes_ += begin.size() + end.size();
    approximate_bytes_ +=
            (begin.size() + end.size()) * 2; // See fdbclient/ReadYourWrites.actor.cpp for details.
}

TxnErrorCode Transaction::commit() {
    fdb_error_t err = 0;
    TEST_INJECTION_POINT_CALLBACK("Transaction::commit.inject_random_fault", &err);
    TEST_SYNC_POINT_CALLBACK("transaction:commit:get_err", &err);
    if (err == 0) [[likely]] {
        StopWatch sw;
        auto* fut = fdb_transaction_commit(txn_);
        auto release_fut = [fut, &sw](int*) {
            fdb_future_destroy(fut);
            g_bvar_txn_kv_commit << sw.elapsed_us();
        };
        std::unique_ptr<int, decltype(release_fut)> defer((int*)0x01, std::move(release_fut));
        RETURN_IF_ERROR(await_future(fut));
        err = fdb_future_get_error(fut);
    }

    if (err) {
        LOG(WARNING) << "fdb commit error, code=" << err << " msg=" << fdb_get_error(err);
        fdb_error_is_txn_conflict(err) ? g_bvar_txn_kv_commit_conflict_counter << 1
                                       : g_bvar_txn_kv_commit_error_counter << 1;
        return cast_as_txn_code(err);
    }
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode Transaction::get_read_version(int64_t* version) {
    StopWatch sw;
    auto* fut = fdb_transaction_get_read_version(txn_);
    DORIS_CLOUD_DEFER {
        fdb_future_destroy(fut);
        g_bvar_txn_kv_get_read_version << sw.elapsed_us();
    };
    RETURN_IF_ERROR(await_future(fut));
    auto err = fdb_future_get_error(fut);
    TEST_SYNC_POINT_CALLBACK("transaction:get_read_version:get_err", &err);
    if (err) {
        LOG(WARNING) << "get read version: " << fdb_get_error(err);
        return cast_as_txn_code(err);
    }
    err = fdb_future_get_int64(fut, version);
    if (err) {
        LOG(WARNING) << "get read version: " << fdb_get_error(err);
        return cast_as_txn_code(err);
    }
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode Transaction::get_committed_version(int64_t* version) {
    StopWatch sw;
    auto err = fdb_transaction_get_committed_version(txn_, version);
    if (err) {
        LOG(WARNING) << "get committed version " << fdb_get_error(err);
        g_bvar_txn_kv_get_committed_version << sw.elapsed_us();
        return cast_as_txn_code(err);
    }
    g_bvar_txn_kv_get_committed_version << sw.elapsed_us();
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode Transaction::abort() {
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode RangeGetIterator::init() {
    if (fut_ == nullptr) return TxnErrorCode::TXN_UNIDENTIFIED_ERROR;
    idx_ = 0;
    kvs_size_ = 0;
    more_ = false;
    kvs_ = nullptr;
    auto err = fdb_future_get_keyvalue_array(fut_, &kvs_, &kvs_size_, &more_);
    TEST_SYNC_POINT_CALLBACK("range_get_iterator:init:get_keyvalue_array_err", &err);
    if (err) {
        LOG(WARNING) << "fdb_future_get_keyvalue_array failed, err=" << fdb_get_error(err);
        return cast_as_txn_code(err);
    }
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode Transaction::batch_get(std::vector<std::optional<std::string>>* res,
                                    const std::vector<std::string>& keys,
                                    const BatchGetOptions& opts) {
    struct FDBFutureDelete {
        void operator()(FDBFuture* future) { fdb_future_destroy(future); }
    };

    res->clear();
    if (keys.empty()) {
        return TxnErrorCode::TXN_OK;
    }

    StopWatch sw;
    auto stop_watcher = [&sw](int*) { g_bvar_txn_kv_batch_get << sw.elapsed_us(); };
    std::unique_ptr<int, decltype(stop_watcher)> defer((int*)0x01, std::move(stop_watcher));

    size_t num_keys = keys.size();
    res->reserve(keys.size());
    g_bvar_txn_kv_get_count_normalized << keys.size();
    std::vector<std::unique_ptr<FDBFuture, FDBFutureDelete>> futures;
    futures.reserve(opts.concurrency);
    for (size_t i = 0; i < num_keys; i += opts.concurrency) {
        size_t size = std::min(i + opts.concurrency, num_keys);
        for (size_t j = i; j < size; j++) {
            const auto& k = keys[j];
            may_logging_single_version_reading(k);
            futures.emplace_back(
                    fdb_transaction_get(txn_, (uint8_t*)k.data(), k.size(), opts.snapshot));
            approximate_bytes_ += k.size() * 2;
        }

        size_t num_futures = futures.size();
        for (auto j = 0; j < num_futures; j++) {
            FDBFuture* future = futures[j].get();
            std::string_view key = keys[i + j];
            RETURN_IF_ERROR(await_future(future));
            fdb_error_t err = fdb_future_get_error(future);
            if (err) {
                LOG(WARNING) << __PRETTY_FUNCTION__
                             << " failed to fdb_future_get_error err=" << fdb_get_error(err)
                             << " key=" << hex(key);
                return cast_as_txn_code(err);
            }
            fdb_bool_t found;
            const uint8_t* ret;
            int len;
            err = fdb_future_get_value(future, &found, &ret, &len);
            num_get_keys_++;
            if (err) {
                LOG(WARNING) << __PRETTY_FUNCTION__
                             << " failed to fdb_future_get_value err=" << fdb_get_error(err)
                             << " key=" << hex(key);
                return cast_as_txn_code(err);
            }
            if (!found) {
                res->push_back(std::nullopt);
                continue;
            }
            get_bytes_ += len + key.size();
            res->push_back(std::string((char*)ret, len));
        }
        futures.clear();
    }
    DCHECK_EQ(res->size(), num_keys);
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode Transaction::batch_scan(
        std::vector<std::optional<std::pair<std::string, std::string>>>* res,
        const std::vector<std::string>& keys, const BatchGetOptions& opts) {
    struct FDBFutureDelete {
        void operator()(FDBFuture* future) { fdb_future_destroy(future); }
    };

    res->clear();
    if (keys.empty()) {
        return TxnErrorCode::TXN_OK;
    }

    StopWatch sw;
    auto stop_watcher = [&sw](int*) { g_bvar_txn_kv_range_get << sw.elapsed_us(); };
    std::unique_ptr<int, decltype(stop_watcher)> defer((int*)0x01, std::move(stop_watcher));

    size_t num_keys = keys.size();
    res->reserve(keys.size());
    g_bvar_txn_kv_get_count_normalized << keys.size();
    std::vector<std::unique_ptr<FDBFuture, FDBFutureDelete>> futures;
    futures.reserve(opts.concurrency);

    fdb_bool_t snapshot = opts.snapshot ? 1 : 0;
    fdb_bool_t reverse = opts.reverse ? 1 : 0;
    for (size_t i = 0; i < num_keys; i += opts.concurrency) {
        size_t batch_size = std::min(i + opts.concurrency, num_keys);
        for (size_t j = i; j < batch_size; j++) {
            const auto& key = keys[j];
            FDBFuture* fut;
            if (reverse) {
                fut = fdb_transaction_get_range(
                        txn_, FDB_KEYSEL_FIRST_GREATER_THAN((uint8_t*)"", 0),
                        FDB_KEYSEL_FIRST_GREATER_THAN((uint8_t*)key.data(), key.size()),
                        1, // limit: take the first one
                        0, // target_bytes, unlimited
                        FDBStreamingMode::FDB_STREAMING_MODE_WANT_ALL,
                        0,        // iteration
                        snapshot, // snapshot
                        reverse   // reverse
                );
            } else {
                fut = fdb_transaction_get_range(
                        txn_, FDB_KEYSEL_FIRST_GREATER_OR_EQUAL((uint8_t*)key.data(), key.size()),
                        FDB_KEYSEL_FIRST_GREATER_THAN((uint8_t*)"\xFF", 1),
                        1, // limit: take the first one
                        0, // target_bytes, unlimited
                        FDBStreamingMode::FDB_STREAMING_MODE_WANT_ALL,
                        0,        // iteration
                        snapshot, // snapshot
                        reverse   // reverse
                );
            }

            futures.emplace_back(fut);
            approximate_bytes_ += key.size() * 2;
        }

        size_t num_futures = futures.size();
        for (size_t j = 0; j < num_futures; j++) {
            FDBFuture* future = futures[j].get();
            std::string_view key = keys[i + j];

            RETURN_IF_ERROR(await_future(future));
            fdb_error_t err = fdb_future_get_error(future);
            if (err) {
                LOG(WARNING) << __PRETTY_FUNCTION__
                             << " failed to fdb_future_get_error err=" << fdb_get_error(err)
                             << " key=" << hex(key);
                return cast_as_txn_code(err);
            }

            const FDBKeyValue* kvs;
            int kvs_size;
            fdb_bool_t more;
            err = fdb_future_get_keyvalue_array(future, &kvs, &kvs_size, &more);
            num_get_keys_++;

            if (err) {
                LOG(WARNING) << __PRETTY_FUNCTION__
                             << " failed to fdb_future_get_keyvalue_array err="
                             << fdb_get_error(err) << " key=" << hex(key);
                return cast_as_txn_code(err);
            }

            if (kvs_size == 0) {
                res->push_back(std::nullopt);
            } else {
                const FDBKeyValue& kv = kvs[0];
                get_bytes_ += kv.value_length + key.size();
                std::string output_key((char*)kv.key, kv.key_length);
                std::string output_value((char*)kv.value, kv.value_length);
                res->emplace_back(std::make_pair(std::move(output_key), std::move(output_value)));
            }
        }
        futures.clear();
    }

    DCHECK_EQ(res->size(), num_keys);
    return TxnErrorCode::TXN_OK;
}

FullRangeGetIterator::FullRangeGetIterator(std::string begin, std::string end,
                                           FullRangeGetOptions opts)
        : opts_(std::move(opts)), begin_(std::move(begin)), end_(std::move(end)) {
    DCHECK(dynamic_cast<FdbTxnKv*>(opts_.txn_kv.get()));
    DCHECK(!opts_.txn || dynamic_cast<fdb::Transaction*>(opts_.txn)) << opts_.txn;
}

FullRangeGetIterator::~FullRangeGetIterator() {
    if (fut_) {
        static_cast<void>(fdb::await_future(fut_));
        fdb_future_destroy(fut_);
    }
}

bool FullRangeGetIterator::has_next() {
    if (!is_valid()) {
        return false;
    }

    if (opts_.exact_limit > 0 && num_consumed_ >= opts_.exact_limit) {
        return false;
    }

    if (!inner_iter_) {
        // The first call
        init();
        if (!is_valid()) {
            return false;
        }

        return inner_iter_->has_next();
    }

    if (inner_iter_->has_next()) {
        if (prefetch()) {
            TEST_SYNC_POINT("fdb.FullRangeGetIterator.has_next_prefetch");
            async_get_next_batch();
        }
        return true;
    }

    if (!inner_iter_->more()) {
        return false;
    }

    if (!fut_) {
        async_get_next_batch();
        if (!is_valid()) {
            return false;
        }
    }

    await_future();
    return is_valid() ? inner_iter_->has_next() : false;
}

std::optional<std::pair<std::string_view, std::string_view>> FullRangeGetIterator::next() {
    if (!has_next()) {
        return std::nullopt;
    }

    num_consumed_++;
    return inner_iter_->next();
}

std::optional<std::pair<std::string_view, std::string_view>> FullRangeGetIterator::peek() {
    if (!has_next()) {
        return std::nullopt;
    }

    return inner_iter_->peek();
}

void FullRangeGetIterator::await_future() {
    auto ret = fdb::await_future(fut_);
    if (ret != TxnErrorCode::TXN_OK) {
        code_ = ret;
        return;
    }

    auto err = fdb_future_get_error(fut_);
    if (err) {
        code_ = cast_as_txn_code(err);
        LOG(WARNING) << fdb_get_error(err);
        return;
    }

    if (opts_.obj_pool && inner_iter_) {
        opts_.obj_pool->push_back(std::move(inner_iter_));
    }
    inner_iter_ = std::make_unique<RangeGetIterator>(fut_);
    fut_ = nullptr;
    code_ = inner_iter_->init();
}

void FullRangeGetIterator::init() {
    async_inner_get(begin_, end_);
    if (!is_valid()) {
        return;
    }

    await_future();
}

bool FullRangeGetIterator::prefetch() {
    return opts_.prefetch && is_valid() && !fut_ && inner_iter_->more() &&
           (opts_.exact_limit <= 0 || num_consumed_ + inner_iter_->remaining() < opts_.exact_limit);
}

void FullRangeGetIterator::async_inner_get(std::string_view begin, std::string_view end) {
    DCHECK(!fut_);

    auto* txn = static_cast<Transaction*>(opts_.txn);
    if (!txn) {
        // Create a new txn for each inner range get
        std::unique_ptr<cloud::Transaction> txn1;
        // TODO(plat1ko): Async create txn
        TxnErrorCode err = opts_.txn_kv->create_txn(&txn1);
        if (err != TxnErrorCode::TXN_OK) {
            code_ = err;
            return;
        }

        txn_.reset(static_cast<Transaction*>(txn1.release()));
        txn = txn_.get();
    }

    // TODO(plat1ko): Support `Transaction::async_get` api
    int limit = std::max(opts_.batch_limit, 0);
    if (opts_.exact_limit > 0) {
        // If we have consumed some keys, we need to adjust the remaining limit.
        int consumed = num_consumed_ + (inner_iter_ ? inner_iter_->remaining() : 0);
        limit = std::min(limit, std::max(0, opts_.exact_limit - consumed));
    }
    fdb_bool_t snapshot = opts_.snapshot ? 1 : 0;
    fdb_bool_t reverse = opts_.reverse ? 1 : 0;
    auto [begin_or_equal, begin_offset] = apply_key_selector(opts_.begin_key_selector);
    auto [end_or_equal, end_offset] = apply_key_selector(opts_.end_key_selector);
    fut_ = fdb_transaction_get_range(txn->txn_, (uint8_t*)begin.data(), begin.size(),
                                     begin_or_equal, begin_offset, (uint8_t*)end.data(), end.size(),
                                     end_or_equal, end_offset, limit, 0 /*target_bytes, unlimited*/,
                                     FDBStreamingMode::FDB_STREAMING_MODE_WANT_ALL, 0 /*iteration*/,
                                     snapshot, reverse);
}

void FullRangeGetIterator::async_get_next_batch() {
    if (opts_.reverse) {
        // Change the end key to the previous last key. The key selector will be
        // FIRST_GREATER_OR_EQUAL, so we need to use the last key of the inner iterator as the
        // end key, since the end key is exclusive.
        opts_.end_key_selector = RangeKeySelector::FIRST_GREATER_OR_EQUAL;
        std::string_view end_key = inner_iter_->last_key();
        async_inner_get(begin_, end_key);
    } else {
        opts_.begin_key_selector = RangeKeySelector::FIRST_GREATER_OR_EQUAL;
        std::string begin_key = inner_iter_->next_begin_key();
        async_inner_get(begin_key, end_);
    }
}

} // namespace doris::cloud::fdb
