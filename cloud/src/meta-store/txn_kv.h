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

#pragma once

#include <foundationdb/fdb_c.h>
#include <foundationdb/fdb_c_options.g.h>
#include <gtest/gtest_prod.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "txn_kv_error.h"

// =============================================================================
//
// =============================================================================

namespace doris::cloud {

class Transaction;
class RangeGetIterator;
class TxnKv;

// A key selector is used to specify the position of a key in a range query.
enum class RangeKeySelector {
    FIRST_GREATER_OR_EQUAL,
    FIRST_GREATER_THAN,
    LAST_LESS_OR_EQUAL,
    LAST_LESS_THAN
};

struct RangeGetOptions {
    // if true, key range will not be included in txn conflict detection this time.
    bool snapshot = false;
    // if non-zero, indicates the maximum number of key-value pairs to return.
    int batch_limit = 10000;
    // if true, the iterator will return keys in reverse order.
    bool reverse = false;
    // The key selector for the beginning of the range.
    RangeKeySelector begin_key_selector = RangeKeySelector::FIRST_GREATER_OR_EQUAL;
    // The key selector for the end of the range.
    //
    // REMEMBER: The end key is exclusive by default.
    RangeKeySelector end_key_selector = RangeKeySelector::FIRST_GREATER_OR_EQUAL;
};

/**
 * Unlike `RangeGetIterator`, which can only iterate within a page of range, this iterator is
 * capable of iterating over the entire specified range.
 *
 * Usage:
 * for (auto kvp = it.next(); kvp.has_value(); kvp = it.next()) {
 *     auto [k, v] = *kvp;
 * }
 * if (!it.is_valid()) {
 *     return err;
 * }
 */
struct FullRangeGetOptions : public RangeGetOptions {
    std::shared_ptr<TxnKv> txn_kv;
    // Trigger prefetch getting next batch kvs before access them
    bool prefetch = false;
    // If non-zero, indicates the exact number of key-value pairs to return.
    int exact_limit = 0;
    // Reference. If not null, each inner range get is performed through this transaction; otherwise
    // perform each inner range get through a new transaction.
    Transaction* txn = nullptr;
    // If users want to extend the lifespan of the kv pair returned by `next()`, they can pass an
    // object pool to collect the inner iterators that have completed iterated.
    std::vector<std::unique_ptr<RangeGetIterator>>* obj_pool = nullptr;

    FullRangeGetOptions(std::shared_ptr<TxnKv> _txn_kv) : txn_kv(std::move(_txn_kv)) {}
    FullRangeGetOptions() = default;
};

class FullRangeGetIterator {
public:
    FullRangeGetIterator() = default;

    virtual ~FullRangeGetIterator() = default;

    virtual bool is_valid() const = 0;

    virtual bool has_next() = 0;

    virtual TxnErrorCode error_code() const = 0;

    virtual std::optional<std::pair<std::string_view, std::string_view>> next() = 0;

    virtual std::optional<std::pair<std::string_view, std::string_view>> peek() = 0;
};

class TxnKv {
public:
    TxnKv() = default;
    virtual ~TxnKv() = default;

    /**
     * Creates a transaction
     * TODO: add options to create the txn
     *
     * @param txn output param
     * @return TXN_OK for success
     */
    virtual TxnErrorCode create_txn(std::unique_ptr<Transaction>* txn) = 0;

    virtual int init() = 0;

    virtual std::unique_ptr<FullRangeGetIterator> full_range_get(std::string begin, std::string end,
                                                                 FullRangeGetOptions opts) = 0;
};

class Transaction {
public:
    Transaction() = default;
    virtual ~Transaction() = default;

    virtual void put(std::string_view key, std::string_view val) = 0;

    /**
     * @param snapshot if true, `key` will not be included in txn conflict detection this time
     * @return TXN_OK for success get a key, TXN_KEY_NOT_FOUND for key not found, otherwise for error
     */
    virtual TxnErrorCode get(std::string_view key, std::string* val, bool snapshot = false) = 0;

    /**
     * Closed-open range
     * @param begin the begin key, inclusive
     * @param end the end key, exclusive
     * @param iter output param for the iterator to iterate over the key-value pairs in the specified range.
     *             If the range is empty, the iterator will be valid but `has_next()` will return false.
     *             If an error occurs, the iterator will be invalid and `error_code()` will return the error code.
     * @param opts options for range get
     *             - `snapshot`: if true, the range will not be included in txn conflict detection this time.
     *             - `limit`: the maximum number of key-value pairs to return.
     *             - `reverse`: if true, the iterator will return keys in reverse order.
     * @return TXN_OK for success, otherwise for error
     */
    virtual TxnErrorCode get(std::string_view begin, std::string_view end,
                             std::unique_ptr<RangeGetIterator>* iter,
                             const RangeGetOptions& opts) = 0;

    // A convenience method for `get` with default options, to keep backward compatibility.
    TxnErrorCode get(std::string_view begin, std::string_view end,
                     std::unique_ptr<RangeGetIterator>* iter, bool snapshot = false,
                     int limit = 10000) {
        RangeGetOptions opts;
        opts.snapshot = snapshot;
        opts.batch_limit = limit;
        return get(begin, end, iter, opts);
    }

    /**
     * Get a full range of key-value pairs.
     * @param begin the begin key, inclusive
     * @param end the end key, exclusive
     * @param opts options for full range get
     * @return a FullRangeGetIterator for iterating over the key-value pairs in the specified range.
     *         If the range is empty, the iterator will be valid but `has_next()` will return false.
     *         If an error occurs, the iterator will be invalid and `error_code()` will return the error code.
     */
    virtual std::unique_ptr<FullRangeGetIterator> full_range_get(
            std::string_view begin, std::string_view end,
            FullRangeGetOptions opts = FullRangeGetOptions()) = 0;

    /**
     * Put a key-value pair in which key will in the form of
     * `key_prefix + versiontimestamp`. `versiontimestamp` is autogenerated by the
     * system and it's 10-byte long and encoded in big-endian
     *
     * @param key_prefix prefix for key convertion, can be zero-length
     * @param val value
     */
    virtual void atomic_set_ver_key(std::string_view key_prefix, std::string_view val) = 0;

    /**
     * Put a key-value pair in which key will in the form of `key_prefix + versiontimestamp + key_suffix`.
     * `versiontimestamp` is autogenerated by the system and it's 10-byte long and encoded in big-endian.
     *
     * @param key key for conversion, it should at least 10-byte long.
     * @param offset the offset of the versionstamp to place. `offset` + 10 must be less than or equal to
     *               the length of `key`.
     * @param val value
     * @return true for success, otherwise the offset is invalid or the key is too short.
     */
    virtual bool atomic_set_ver_key(std::string_view key, uint32_t offset,
                                    std::string_view val) = 0;

    /**
     * Put a key-value pair in which key will in the form of
     * `value + versiontimestamp`. `versiontimestamp` is autogenerated by the
     * system and it's 10-byte long and encoded in big-endian
     *
     * @param key prefix for key convertion, can be zero-length
     * @param val value
     */
    virtual void atomic_set_ver_value(std::string_view key, std::string_view val) = 0;

    /**
     * Adds a value to database.
     *
     * The default value is zero if no such key exists before.
     *
     * @param to_add positive for addition, negative for substraction
     * @return 0 for success otherwise error
     */
    virtual void atomic_add(std::string_view key, int64_t to_add) = 0;
    // TODO: min max or and xor cmp_and_clear set_ver_value

    /**
     * Decode the atomic value written by `atomic_add`.
     *
     * @param data the data to decode
     * @return true for success, otherwise the data format is invalid.
     */
    virtual bool decode_atomic_int(std::string_view data, int64_t* val) = 0;

    virtual void remove(std::string_view key) = 0;

    /**
     * Remove a closed-open range
     */
    virtual void remove(std::string_view begin, std::string_view end) = 0;

    /**
     *
     *@return TXN_OK for success otherwise error
     */
    virtual TxnErrorCode commit() = 0;

    /**
     * Gets the read version used by the txn.
     * Note that it does not make any sense we call this function before
     * any `Transaction::get()` is called.
     *
     *@return TXN_OK for success otherwise error
     */
    virtual TxnErrorCode get_read_version(int64_t* version) = 0;

    /**
     * Gets the commited version used by the txn.
     * Note that it does not make any sense we call this function before
     * a successful call to `Transaction::commit()`.
     *
     *@return TXN_OK for success, TXN_CONFLICT for conflict, otherwise error
     */
    virtual TxnErrorCode get_committed_version(int64_t* version) = 0;

    /**
     * Aborts this transaction
     *
     * @return TXN_OK for success otherwise error
     */
    virtual TxnErrorCode abort() = 0;

    struct BatchGetOptions {
        BatchGetOptions() : BatchGetOptions(false) {};
        BatchGetOptions(bool s) : snapshot(s), concurrency(1000) {};

        // if true, `key` will not be included in txn conflict detection this time.
        //
        // Default: false
        bool snapshot;

        // the maximum number of concurrent requests submitted to fdb at one time.
        //
        // Default: 1000
        int concurrency;

        // Used for `batch_scan`, if true, the underlying iterator will return keys in reverse order.
        //
        // Default: false
        bool reverse = false;
    };

    /**
     * @brief batch get keys
     *
     * @param res
     * @param keys
     * @param opts
     * @return If all keys are successfully retrieved, return TXN_OK. Otherwise, return the code of the first occurring error
     */
    virtual TxnErrorCode batch_get(std::vector<std::optional<std::string>>* res,
                                   const std::vector<std::string>& keys,
                                   const BatchGetOptions& opts = BatchGetOptions()) = 0;

    /**
     * @brief Batch scan for the first key-value pair with each given key prefix.
     *
     * For each key prefix in the input key_prefixs, this function scans for keys that start with
     * that prefix in the direction specified by `opts.reverse` (forward by default) and returns
     * the first key-value pair encountered. If no key with the given prefix is found, the
     * corresponding result is an empty optional.
     *
     * The function scans keys in batches and is more efficient than scanning each prefix individually.
     *
     * @param[out] res The output vector of optionals. Each element corresponds to the same index as in the input key_prefixs.
     *                  If a key-value pair with the prefix is found, the element will contain the pair; otherwise, it will be std::nullopt.
     * @param[in] key_prefixs The list of key prefixes to scan for.
     * @param[in] opts Options such as `reverse` and `snapshot`. If `reverse` is true, the scan is in the backward direction.
     *
     * @return TXN_OK if all scans completed successfully. If any error occurs during the scanning,
     *         the function stops immediately and returns the error code of the first error encountered.
     *         Note: The output vector `res` may be partially filled when an error occurs.
     */
    virtual TxnErrorCode batch_scan(
            std::vector<std::optional<std::pair<std::string, std::string>>>* res,
            const std::vector<std::string>& key_prefixs,
            const BatchGetOptions& opts = BatchGetOptions());

    /**
     * @brief Batch scan for the first key-value pairs within each given range.
     *
     * For each range in the input ranges, this function performs a range scan from the begin key
     * (inclusive) to the end key (exclusive) in the direction specified by `opts.reverse` (forward
     * by default) and returns all key-value pairs found within that range. If no keys are found
     * within a range, the corresponding result is an empty optional.
     *
     * The function scans ranges in batches and is more efficient than scanning each range individually.
     *
     * @param[out] res The output vector of optionals. Each element corresponds to the same index as in the input ranges.
     *                  If key-value pairs are found within the range, the element will contain a vector of pairs;
     *                  otherwise, it will be std::nullopt.
     * @param[in] ranges The list of ranges to scan. Each range is a pair of (begin_key, end_key) where
     *                   begin_key is inclusive and end_key is exclusive.
     * @param[in] opts Options such as `reverse` and `snapshot`. If `reverse` is true, the scan is in the backward direction.
     *
     * @return TXN_OK if all scans completed successfully. If any error occurs during the scanning,
     *         the function stops immediately and returns the error code of the first error encountered.
     *         Note: The output vector `res` may be partially filled when an error occurs.
     */
    virtual TxnErrorCode batch_scan(
            std::vector<std::optional<std::pair<std::string, std::string>>>* res,
            const std::vector<std::pair<std::string, std::string>>& ranges,
            const BatchGetOptions& opts = BatchGetOptions()) = 0;

    /**
     * @brief return the approximate bytes consumed by the underlying transaction buffer.
     **/
    virtual size_t approximate_bytes() const = 0;

    /**
     * @brief return the num get keys submitted to this txn.
     **/
    virtual size_t num_get_keys() const = 0;

    /**
     * @brief return the num delete keys submitted to this txn.
     **/
    virtual size_t num_del_keys() const = 0;

    /**
     * @brief return the num put keys submitted to this txn.
     **/
    virtual size_t num_put_keys() const = 0;

    /**
     * @brief return the bytes of the delete keys consumed.
     **/
    virtual size_t delete_bytes() const = 0;

    /**
     * @brief return the bytes of the get values consumed.
     **/
    virtual size_t get_bytes() const = 0;

    /**
     * @brief return the bytes of the put key and values consumed.
     **/
    virtual size_t put_bytes() const = 0;
};

class RangeGetIterator {
public:
    RangeGetIterator() = default;
    virtual ~RangeGetIterator() = default;

    /**
     * Checks if we can call `next()` on this iterator.
     */
    virtual bool has_next() const = 0;

    /**
     * Gets next element, this is usually called after a check of `has_next()` succeeds,
     * If `has_next()` is not checked, the return value may be undefined.
     *
     * @return a kv pair
     */
    virtual std::pair<std::string_view, std::string_view> next() = 0;

    /**
     * Gets next element but not advance the cursor, this is usually called after a check of `has_next()` succeeds,
     * If `has_next()` is not checked, the return value may be undefined.
     *
     * @return a kv pair
     */
    virtual std::pair<std::string_view, std::string_view> peek() const = 0;

    /**
     * Repositions the offset to `pos`
     */
    virtual void seek(size_t pos) = 0;

    /**
     * Checks if there are more KVs to be get from the range, caller usually wants
     * to issue another `get` with the last key of this iteration.
     *
     * @return if there are more kvs that this iterator cannot cover
     */
    virtual bool more() const = 0;

    /**
     *
     * Gets size of the range, some kinds of iterators may not support this function.
     *
     * @return size
     */
    virtual int size() const = 0;

    /**
     * Get all FDBKeyValue's bytes include key's bytes
     * RangeGetIterator created by get range, when get range the keys in the range too.
     */
    virtual int64_t get_kv_bytes() const = 0;

    /**
     * Get the remaining size of the range, some kinds of iterators may not support this function.
     *
     * @return size
     */
    virtual int remaining() const = 0;

    /**
     * Resets to initial state, some kinds of iterators may not support this function.
     */
    virtual void reset() = 0;

    /**
     * Get the begin key of the next iterator if `more()` is true, otherwise returns empty string.
     */
    virtual std::string next_begin_key() const = 0;

    /**
     * Get the last key of the iterator, it can be used as the end key of the next iterator when
     * the key selector is FIRST_GREATER_OR_EQUAL.
     *
     * ATTN: This is ONLY used for reverse range get.
     */
    virtual std::string_view last_key() const = 0;

    RangeGetIterator(const RangeGetIterator&) = delete;
    RangeGetIterator& operator=(const RangeGetIterator&) = delete;
};

// =============================================================================
//  FoundationDB implementation of TxnKv
// =============================================================================

namespace fdb {
class Database;
class Transaction;
class Network;
} // namespace fdb

class FdbTxnKv : public TxnKv {
public:
    FdbTxnKv() = default;
    ~FdbTxnKv() override = default;

    TxnErrorCode create_txn(std::unique_ptr<Transaction>* txn) override;
    TxnErrorCode create_txn_with_system_access(std::unique_ptr<Transaction>* txn);

    int init() override;

    std::unique_ptr<FullRangeGetIterator> full_range_get(std::string begin, std::string end,
                                                         FullRangeGetOptions opts) override;

    // Return the partition boundaries of the database.
    TxnErrorCode get_partition_boundaries(std::vector<std::string>* boundaries);

    // Returns a value where 0 indicates that the client is idle and 1 (or larger) indicates that
    // the client is saturated. This value is updated every second.
    double get_client_thread_busyness() const;

    static std::string_view fdb_partition_key_prefix() { return "\xff/keyServers/"; }
    static std::string_view fdb_partition_key_end() {
        // '0' is the next byte after '/' in the ASCII table
        return "\xff/keyServers0";
    }

private:
    std::shared_ptr<fdb::Network> network_;
    std::shared_ptr<fdb::Database> database_;
};

namespace fdb {

class Network {
public:
    Network(FDBNetworkOption opt) : opt_(opt) {}

    /**
     * @return 0 for success otherwise non-zero
     */
    int init();

    /**
     * Notify the newwork thread to stop, this is an async. call, check
     * Network::working to ensure the network exited finally.
     *
     * FIXME: may be we can implement it as a sync. function.
     */
    void stop();

    ~Network() = default;

private:
    std::shared_ptr<std::thread> network_thread_;
    FDBNetworkOption opt_;

    // Global state, only one instance of Network is allowed
    static std::atomic<bool> working;
};

class Database {
public:
    Database(std::shared_ptr<Network> net, std::string cluster_file, FDBDatabaseOption opt)
            : network_(std::move(net)), cluster_file_path_(std::move(cluster_file)), opt_(opt) {}

    /**
     *
     * @return 0 for success otherwise false
     */
    int init();

    ~Database() {
        if (db_ != nullptr) fdb_database_destroy(db_);
    }

    FDBDatabase* db() { return db_; };

    std::shared_ptr<Transaction> create_txn(FDBTransactionOption opt);

private:
    std::shared_ptr<Network> network_;
    std::string cluster_file_path_;
    FDBDatabase* db_ = nullptr;
    FDBDatabaseOption opt_;
};

class RangeGetIterator : public cloud::RangeGetIterator {
public:
    /**
     * Iterator takes the ownership of input future
     */
    RangeGetIterator(FDBFuture* fut, bool owns = true)
            : fut_(fut), owns_fut_(owns), kvs_(nullptr), kvs_size_(-1), more_(false), idx_(-1) {}

    RangeGetIterator(RangeGetIterator&& o) {
        if (fut_ && owns_fut_) fdb_future_destroy(fut_);
        fut_ = o.fut_;
        owns_fut_ = o.owns_fut_;
        kvs_ = o.kvs_;
        kvs_size_ = o.kvs_size_;
        more_ = o.more_;
        idx_ = o.idx_;

        o.fut_ = nullptr;
        o.kvs_ = nullptr;
        o.idx_ = 0;
        o.kvs_size_ = 0;
        o.more_ = false;
    }

    ~RangeGetIterator() override {
        // Release all memory
        if (fut_ && owns_fut_) fdb_future_destroy(fut_);
    }

    TxnErrorCode init();

    std::pair<std::string_view, std::string_view> next() override {
        if (idx_ < 0 || idx_ >= kvs_size_) return {};
        const auto& kv = kvs_[idx_++];
        return {{(char*)kv.key, (size_t)kv.key_length}, {(char*)kv.value, (size_t)kv.value_length}};
    }

    std::pair<std::string_view, std::string_view> peek() const override {
        if (idx_ < 0 || idx_ >= kvs_size_) return {};
        const auto& kv = kvs_[idx_];
        return {{(char*)kv.key, (size_t)kv.key_length}, {(char*)kv.value, (size_t)kv.value_length}};
    }

    void seek(size_t pos) override { idx_ = pos; }

    bool has_next() const override { return (idx_ < kvs_size_); }

    /**
     * Check if there are more KVs to be get from the range, caller usually wants
     * to issue a nother `get` with the last key of this iteration.
     */
    bool more() const override { return more_; }

    int size() const override { return kvs_size_; }

    int64_t get_kv_bytes() const override {
        int64_t total_bytes {};
        for (int i = 0; i < kvs_size_; i++) {
            total_bytes += kvs_[i].key_length + kvs_[i].value_length;
        }
        return total_bytes;
    }

    int remaining() const override {
        if (idx_ < 0 || idx_ >= kvs_size_) return 0;
        return kvs_size_ - idx_;
    }

    void reset() override { idx_ = 0; }

    std::string next_begin_key() const override {
        std::string k;
        if (!more()) return k;
        const auto& kv = kvs_[kvs_size_ - 1];
        k.reserve((size_t)kv.key_length + 1);
        k.append((char*)kv.key, (size_t)kv.key_length);
        k.push_back('\x00');
        return k;
    }

    std::string_view last_key() const override {
        if (kvs_size_ <= 0) return {};
        const auto& kv = kvs_[kvs_size_ - 1];
        return {(char*)kv.key, (size_t)kv.key_length};
    }

    RangeGetIterator(const RangeGetIterator&) = delete;
    RangeGetIterator& operator=(const RangeGetIterator&) = delete;

private:
    FDBFuture* fut_;
    bool owns_fut_;
    const FDBKeyValue* kvs_;
    int kvs_size_;
    fdb_bool_t more_;
    int idx_;
};

class Transaction : public cloud::Transaction {
    FRIEND_TEST(TxnKvTest, ReportConflictingRange);
    FRIEND_TEST(TxnKvTest, VersionedGetConflictRange);

public:
    friend class Database;
    friend class FullRangeGetIterator;

    Transaction(std::shared_ptr<Database> db) : db_(std::move(db)) {}

    ~Transaction() override {
        if (txn_) fdb_transaction_destroy(txn_);
    }

    /**
     *
     * @return TxnErrorCode for success otherwise false
     */
    TxnErrorCode init();
    TxnErrorCode enable_access_system_keys();

    void put(std::string_view key, std::string_view val) override;

    using cloud::Transaction::get;
    /**
     * @param snapshot if true, `key` will not be included in txn conflict detection this time
     * @return TXN_OK for success get a key, TXN_KEY_NOT_FOUND for key not found, otherwise for error
     */
    TxnErrorCode get(std::string_view key, std::string* val, bool snapshot = false) override;
    /**
     * Closed-open range
     * @param begin the begin key, inclusive
     * @param end the end key, exclusive
     * @param iter output param for the iterator to iterate over the key-value pairs in the specified range.
     *             If the range is empty, the iterator will be valid but `has_next()` will return false.
     *             If an error occurs, the iterator will be invalid and `error_code()` will return the error code.
     * @param opts options for range get
     *             - `snapshot`: if true, the range will not be included in txn conflict detection this time.
     *             - `limit`: the maximum number of key-value pairs to return.
     *             - `reverse`: if true, the iterator will return keys in reverse order.
     * @return TXN_OK for success, otherwise for error
     */
    TxnErrorCode get(std::string_view begin, std::string_view end,
                     std::unique_ptr<cloud::RangeGetIterator>* iter,
                     const RangeGetOptions& opts) override;

    std::unique_ptr<cloud::FullRangeGetIterator> full_range_get(
            std::string_view begin, std::string_view end,
            FullRangeGetOptions opts = FullRangeGetOptions()) override;

    /**
     * Put a key-value pair in which key will in the form of
     * `key_prefix + versiontimestamp`. `versiontimestamp` is autogenerated by the
     * system and it's 10-byte long and encoded in big-endian
     *
     * @param key_prefix prefix for key convertion, can be zero-length
     * @param val value
     */
    void atomic_set_ver_key(std::string_view key_prefix, std::string_view val) override;

    /**
     * Put a key-value pair in which key will in the form of `key_prefix + versiontimestamp + key_suffix`.
     *
     * @param key key for conversion, it should at least 10-byte long.
     * @param offset the offset of the versionstamp to place. `offset` + 10 must be less than or equal to
     *               the length of `key`.
     * @param val value
     * @return true for success, otherwise the offset is invalid or the key is too short.
     */
    bool atomic_set_ver_key(std::string_view key, uint32_t offset, std::string_view val) override;

    /**
     * Put a key-value pair in which key will in the form of
     * `value + versiontimestamp`. `versiontimestamp` is autogenerated by the
     * system and it's 10-byte long and encoded in big-endian
     *
     * @param key prefix for key convertion, can be zero-length
     * @param val value
     */
    void atomic_set_ver_value(std::string_view key, std::string_view val) override;

    /**
     * Adds a value to database
     * @param to_add positive for addition, negative for substraction
     */
    void atomic_add(std::string_view key, int64_t to_add) override;
    // TODO: min max or and xor cmp_and_clear set_ver_value

    bool decode_atomic_int(std::string_view data, int64_t* val) override;

    void remove(std::string_view key) override;

    /**
     * Remove a closed-open range
     */
    void remove(std::string_view begin, std::string_view end) override;

    /**
     *
     *@return TXN_OK for success, TXN_CONFLICT for conflict, otherwise for error
     */
    TxnErrorCode commit() override;

    TxnErrorCode get_read_version(int64_t* version) override;
    TxnErrorCode get_committed_version(int64_t* version) override;

    TxnErrorCode abort() override;

    TxnErrorCode batch_get(std::vector<std::optional<std::string>>* res,
                           const std::vector<std::string>& keys,
                           const BatchGetOptions& opts = BatchGetOptions()) override;

    TxnErrorCode batch_scan(std::vector<std::optional<std::pair<std::string, std::string>>>* res,
                            const std::vector<std::pair<std::string, std::string>>& ranges,
                            const BatchGetOptions& opts = BatchGetOptions()) override;

    size_t approximate_bytes() const override { return approximate_bytes_; }

    size_t num_get_keys() const override { return num_get_keys_; }

    size_t num_del_keys() const override { return num_del_keys_; }

    size_t num_put_keys() const override { return num_put_keys_; }

    size_t delete_bytes() const override { return delete_bytes_; }

    size_t put_bytes() const override { return put_bytes_; }

    size_t get_bytes() const override { return get_bytes_; }

private:
    // Return the conflicting range when the transaction commit returns TXN_CONFLICT.
    //
    // It only works when the report_conflicting_ranges option is enabled.
    TxnErrorCode get_conflicting_range(
            std::vector<std::pair<std::string, std::string>>* key_values);
    TxnErrorCode report_conflicting_range();

    std::shared_ptr<Database> db_ {nullptr};
    bool commited_ = false;
    bool aborted_ = false;
    FDBTransaction* txn_ = nullptr;

    size_t num_get_keys_ {0};
    size_t num_del_keys_ {0};
    size_t num_put_keys_ {0};
    size_t delete_bytes_ {0};
    size_t get_bytes_ {0};
    size_t put_bytes_ {0};
    size_t approximate_bytes_ {0};
};

class FullRangeGetIterator final : public cloud::FullRangeGetIterator {
public:
    FullRangeGetIterator(std::string begin, std::string end, FullRangeGetOptions opts);

    ~FullRangeGetIterator() override;

    bool is_valid() const override { return code_ == TxnErrorCode::TXN_OK; }

    bool has_next() override;

    TxnErrorCode error_code() const override { return code_; }

    std::optional<std::pair<std::string_view, std::string_view>> next() override;

    std::optional<std::pair<std::string_view, std::string_view>> peek() override;

private:
    // Set `is_valid_` to false if meet any error
    void init();

    // Await `fut_` and create new inner iter.
    // Set `is_valid_` to false if meet any error
    void await_future();

    // Perform a paginate range get asynchronously and set `fut_`.
    // Set `is_valid_` to false if meet any error
    void async_inner_get(std::string_view begin, std::string_view end);
    void async_get_next_batch();

    bool prefetch();

    FullRangeGetOptions opts_;

    TxnErrorCode code_ = TxnErrorCode::TXN_OK;
    int num_consumed_ = 0;
    std::string begin_;
    std::string end_;
    std::unique_ptr<Transaction> txn_;
    std::unique_ptr<RangeGetIterator> inner_iter_;
    FDBFuture* fut_ = nullptr;
};

} // namespace fdb

} // namespace doris::cloud
