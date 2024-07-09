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
#include <string_view>
#include <thread>
#include <vector>

#include "common/bvars.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/stopwatch.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-service/txn_kv_error.h"

// =============================================================================
//  FoundationDB implementation of TxnKv
// =============================================================================

#define RETURN_IF_ERROR(op)                            \
    do {                                               \
        TxnErrorCode code = op;                        \
        if (code != TxnErrorCode::TXN_OK) return code; \
    } while (false)

namespace doris::cloud {

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

std::unique_ptr<FullRangeGetIterator> FdbTxnKv::full_range_get(std::string begin, std::string end,
                                                               FullRangeGetIteratorOptions opts) {
    return std::make_unique<fdb::FullRangeGetIterator>(std::move(begin), std::move(end),
                                                       std::move(opts));
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
    StopWatch sw;
    approximate_bytes_ += key.size() * 2; // See fdbclient/ReadYourWrites.actor.cpp for details
    auto* fut = fdb_transaction_get(txn_, (uint8_t*)key.data(), key.size(), snapshot);

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

    if (err) {
        LOG(WARNING) << __PRETTY_FUNCTION__
                     << " failed to fdb_future_get_value err=" << fdb_get_error(err)
                     << " key=" << hex(key);
        return cast_as_txn_code(err);
    }

    if (!found) return TxnErrorCode::TXN_KEY_NOT_FOUND;
    *val = std::string((char*)ret, len);
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode Transaction::get(std::string_view begin, std::string_view end,
                              std::unique_ptr<cloud::RangeGetIterator>* iter, bool snapshot,
                              int limit) {
    StopWatch sw;
    approximate_bytes_ += begin.size() + end.size();
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [&sw](int*) { g_bvar_txn_kv_range_get << sw.elapsed_us(); });

    FDBFuture* fut = fdb_transaction_get_range(
            txn_, FDB_KEYSEL_FIRST_GREATER_OR_EQUAL((uint8_t*)begin.data(), begin.size()),
            FDB_KEYSEL_FIRST_GREATER_OR_EQUAL((uint8_t*)end.data(), end.size()), limit,
            0 /*target_bytes, unlimited*/, FDBStreamingMode::FDB_STREAMING_MODE_WANT_ALL,
            //       FDBStreamingMode::FDB_STREAMING_MODE_ITERATOR,
            0 /*iteration*/, snapshot, false /*reverse*/);

    RETURN_IF_ERROR(await_future(fut));
    auto err = fdb_future_get_error(fut);
    TEST_SYNC_POINT_CALLBACK("transaction:get_range:get_err", &err);
    if (err) {
        LOG(WARNING) << fdb_get_error(err);
        return cast_as_txn_code(err);
    }

    std::unique_ptr<RangeGetIterator> ret(new RangeGetIterator(fut));
    RETURN_IF_ERROR(ret->init());

    *(iter) = std::move(ret);

    return TxnErrorCode::TXN_OK;
}

void Transaction::atomic_set_ver_key(std::string_view key_prefix, std::string_view val) {
    StopWatch sw;
    std::unique_ptr<std::string> key(new std::string(key_prefix));
    int prefix_size = key->size();
    // ATTN:
    // 10 bytes for versiontimestamp must be 0, trailing 4 bytes is for prefix len
    key->resize(key->size() + 14, '\0');
    std::memcpy(key->data() + (key->size() - 4), &prefix_size, 4);

    fdb_transaction_atomic_op(txn_, (uint8_t*)key->data(), key->size(), (uint8_t*)val.data(),
                              val.size(),
                              FDBMutationType::FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_KEY);

    g_bvar_txn_kv_atomic_set_ver_key << sw.elapsed_us();
    ++num_put_keys_;
    put_bytes_ += key_prefix.size() + val.size();
    approximate_bytes_ += key_prefix.size() * 3 + val.size();
}

void Transaction::atomic_set_ver_value(std::string_view key, std::string_view value) {
    StopWatch sw;
    std::unique_ptr<std::string> val(new std::string(value));
    int prefix_size = val->size();
    // ATTN:
    // 10 bytes for versiontimestamp must be 0, trailing 4 bytes is for prefix len
    val->resize(val->size() + 14, '\0');
    std::memcpy(val->data() + (val->size() - 4), &prefix_size, 4);

    fdb_transaction_atomic_op(txn_, (uint8_t*)key.data(), key.size(), (uint8_t*)val->data(),
                              val->size(),
                              FDBMutationType::FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_VALUE);

    g_bvar_txn_kv_atomic_set_ver_value << sw.elapsed_us();
    ++num_put_keys_;
    put_bytes_ += key.size() + value.size();
    approximate_bytes_ += key.size() * 3 + value.size();
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
    std::unique_ptr<int, std::function<void(int*)>> defer((int*)0x01, [fut, &sw](...) {
        fdb_future_destroy(fut);
        g_bvar_txn_kv_get_read_version << sw.elapsed_us();
    });
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
    std::vector<std::unique_ptr<FDBFuture, FDBFutureDelete>> futures;
    futures.reserve(opts.concurrency);
    for (size_t i = 0; i < num_keys; i += opts.concurrency) {
        size_t size = std::min(i + opts.concurrency, num_keys);
        for (size_t j = i; j < size; j++) {
            const auto& k = keys[j];
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
            res->push_back(std::string((char*)ret, len));
        }
        futures.clear();
    }
    DCHECK_EQ(res->size(), num_keys);
    return TxnErrorCode::TXN_OK;
}

FullRangeGetIterator::FullRangeGetIterator(std::string begin, std::string end,
                                           FullRangeGetIteratorOptions opts)
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
    if (!is_valid_) {
        return false;
    }

    if (!inner_iter_) {
        // The first call
        init();
        if (!is_valid_) {
            return false;
        }

        return inner_iter_->has_next();
    }

    if (inner_iter_->has_next()) {
        if (prefetch()) {
            TEST_SYNC_POINT("fdb.FullRangeGetIterator.has_next_prefetch");
            async_inner_get(inner_iter_->next_begin_key());
        }
        return true;
    }

    if (!inner_iter_->more()) {
        return false;
    }

    if (!fut_) {
        async_inner_get(inner_iter_->next_begin_key());
        if (!is_valid_) {
            return false;
        }
    }

    await_future();
    return is_valid_ ? inner_iter_->has_next() : false;
}

std::optional<std::pair<std::string_view, std::string_view>> FullRangeGetIterator::next() {
    if (!has_next()) {
        return std::nullopt;
    }

    return inner_iter_->next();
}

void FullRangeGetIterator::await_future() {
    auto ret = fdb::await_future(fut_);
    if (ret != TxnErrorCode::TXN_OK) {
        is_valid_ = false;
        return;
    }

    auto err = fdb_future_get_error(fut_);
    if (err) {
        is_valid_ = false;
        LOG(WARNING) << fdb_get_error(err);
        return;
    }

    if (opts_.obj_pool && inner_iter_) {
        opts_.obj_pool->push_back(std::move(inner_iter_));
    }
    inner_iter_ = std::make_unique<RangeGetIterator>(fut_);
    fut_ = nullptr;

    if (inner_iter_->init() != TxnErrorCode::TXN_OK) {
        is_valid_ = false;
    }
}

void FullRangeGetIterator::init() {
    async_inner_get(begin_);
    if (!is_valid_) {
        return;
    }

    await_future();
}

bool FullRangeGetIterator::prefetch() {
    return opts_.prefetch && is_valid_ && !fut_ && inner_iter_->more();
}

void FullRangeGetIterator::async_inner_get(std::string_view begin) {
    DCHECK(!fut_);

    auto* txn = static_cast<Transaction*>(opts_.txn);
    if (!txn) {
        // Create a new txn for each inner range get
        std::unique_ptr<cloud::Transaction> txn1;
        // TODO(plat1ko): Async create txn
        TxnErrorCode err = opts_.txn_kv->create_txn(&txn1);
        if (err != TxnErrorCode::TXN_OK) {
            is_valid_ = false;
            return;
        }

        txn_.reset(static_cast<Transaction*>(txn1.release()));
        txn = txn_.get();
    }

    // TODO(plat1ko): Support `Transaction::async_get` api
    fut_ = fdb_transaction_get_range(
            txn->txn_, FDB_KEYSEL_FIRST_GREATER_OR_EQUAL((uint8_t*)begin.data(), begin.size()),
            FDB_KEYSEL_FIRST_GREATER_OR_EQUAL((uint8_t*)end_.data(), end_.size()), opts_.limit,
            0 /*target_bytes, unlimited*/, FDBStreamingMode::FDB_STREAMING_MODE_WANT_ALL,
            //       FDBStreamingMode::FDB_STREAMING_MODE_ITERATOR,
            0 /*iteration*/, opts_.snapshot, false /*reverse*/);
}

} // namespace doris::cloud::fdb
