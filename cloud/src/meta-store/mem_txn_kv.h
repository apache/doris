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

#include <cstddef>
#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "txn_kv.h"
#include "txn_kv_error.h"

namespace doris::cloud {

namespace memkv {
class Transaction;
enum class ModifyOpType;
} // namespace memkv

class MemTxnKv : public TxnKv, public std::enable_shared_from_this<MemTxnKv> {
    friend class memkv::Transaction;

public:
    MemTxnKv() = default;
    ~MemTxnKv() override = default;

    TxnErrorCode create_txn(std::unique_ptr<Transaction>* txn) override;

    int init() override;

    std::unique_ptr<FullRangeGetIterator> full_range_get(std::string begin, std::string end,
                                                         FullRangeGetOptions opts) override;

    TxnErrorCode get_kv(const std::string& key, std::string* val, int64_t version);
    TxnErrorCode get_kv(const std::string& begin, const std::string& end, int64_t version,
                        const RangeGetOptions& opts, bool* more,
                        std::vector<std::pair<std::string, std::string>>* kv_list);

    size_t total_kvs() const {
        std::lock_guard<std::mutex> l(lock_);
        return mem_kv_.size();
    }

    void update_commit_version(int64_t version) {
        std::lock_guard<std::mutex> l(lock_);
        committed_version_ = std::max(committed_version_, version);
        read_version_ = std::max(committed_version_, read_version_);
    }

    int64_t get_bytes_ {};
    int64_t put_bytes_ {};
    int64_t del_bytes_ {};
    int64_t get_count_ {};
    int64_t put_count_ {};
    int64_t del_count_ {};

private:
    using OpTuple = std::tuple<memkv::ModifyOpType, std::string, std::string>;
    TxnErrorCode update(const std::set<std::string>& read_set, const std::vector<OpTuple>& op_list,
                        int64_t read_version, int64_t* committed_version);

    int get_kv(std::map<std::string, std::string>* kv, int64_t* version);

    int64_t get_last_commited_version();
    int64_t get_last_read_version();

    static int gen_version_timestamp(int64_t ver, int16_t seq, std::string* str);

    struct LogItem {
        memkv::ModifyOpType op_;
        int64_t commit_version_;

        // for get's op: key=key, value=""
        // for range get's op: key=begin, value=end
        // for atomic_set_ver_key/atomic_set_ver_value's op: key=key, value=value
        // for atomic_add's op: key=key, value=to_add
        // for remove's op: key=key, value=""
        // for range remove's op: key=begin, value=end
        std::string key;
        std::string value;
    };

    struct Version {
        int64_t commit_version;
        std::optional<std::string> value;
    };

    std::map<std::string, std::list<Version>> mem_kv_;
    std::unordered_map<std::string, std::list<LogItem>> log_kv_;
    mutable std::mutex lock_;
    int64_t committed_version_ = 0;
    int64_t read_version_ = 0;
};

namespace memkv {

enum class ModifyOpType {
    PUT,
    ATOMIC_SET_VER_KEY,
    ATOMIC_SET_VER_VAL,
    ATOMIC_ADD,
    REMOVE,
    REMOVE_RANGE
};

class Transaction : public cloud::Transaction {
public:
    Transaction(std::shared_ptr<MemTxnKv> kv);

    ~Transaction() override = default;

    /**
     *
     * @return 0 for success otherwise false
     */
    int init();

    void put(std::string_view key, std::string_view val) override;

    using cloud::Transaction::get;
    /**
     * @param snapshot if true, `key` will not be included in txn conflict detection this time
     * @return TXN_OK for success get a key, TXN_KEY_NOT_FOUND for key not found, otherwise for error
     */
    TxnErrorCode get(std::string_view key, std::string* val, bool snapshot = false) override;
    /**
     * Closed-open range
     * @param begin inclusive
     * @param end exclusive
     * @param iter output param for the iterator to iterate over the key-value pairs in the specified range.
     * @param opts options for range get
     * @return TXN_OK for success, otherwise for error
     */
    TxnErrorCode get(std::string_view begin, std::string_view end,
                     std::unique_ptr<cloud::RangeGetIterator>* iter,
                     const RangeGetOptions& opts) override;

    std::unique_ptr<cloud::FullRangeGetIterator> full_range_get(
            std::string_view begin, std::string_view end,
            cloud::FullRangeGetOptions opts = cloud::FullRangeGetOptions()) override;

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
     * `versiontimestamp` is autogenerated by the system and it's 10-byte long and encoded
     * in big-endian.
     *
     * @param key key for conversion, it should at least 10-byte long.
     * @param offset the offset of the versionstamp to place. `offset` +
     *               10 must be less than or equal to the length of `key`.
     * @param val value
     * @return true for success, otherwise the offset is invalid or the key is too short
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
     *@return TXN_OK for success otherwise error
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

    void enable_get_versionstamp() override;

    TxnErrorCode get_versionstamp(std::string* versionstamp) override;

private:
    TxnErrorCode inner_get(const std::string& key, std::string* val, bool snapshot);

    TxnErrorCode inner_get(const std::string& begin, const std::string& end,
                           std::unique_ptr<cloud::RangeGetIterator>* iter,
                           const RangeGetOptions& opts);

    std::shared_ptr<MemTxnKv> kv_ {nullptr};
    bool commited_ = false;
    bool aborted_ = false;
    std::mutex lock_;
    std::set<std::string> unreadable_keys_;
    std::set<std::string> read_set_;
    std::map<std::string, std::string> writes_;
    std::vector<std::pair<std::string, std::string>> remove_ranges_;
    std::vector<std::tuple<ModifyOpType, std::string, std::string>> op_list_;

    int64_t committed_version_ = -1;
    int64_t read_version_ = -1;

    size_t approximate_bytes_ {0};
    size_t num_get_keys_ {0};
    size_t num_del_keys_ {0};
    size_t num_put_keys_ {0};
    size_t delete_bytes_ {0};
    size_t put_bytes_ {0};
    size_t get_bytes_ {0};

    bool versionstamp_enabled_ {false};
    std::string versionstamp_result_;
};

class RangeGetIterator : public cloud::RangeGetIterator {
public:
    RangeGetIterator(std::vector<std::pair<std::string, std::string>> kvs, bool more)
            : kvs_(std::move(kvs)), kvs_size_(kvs_.size()), idx_(0), more_(more) {}

    ~RangeGetIterator() override = default;

    bool has_next() const override { return idx_ < kvs_size_; }

    std::pair<std::string_view, std::string_view> next() override {
        if (idx_ < 0 || idx_ >= kvs_size_) return {};
        auto& kv = kvs_[idx_++];
        return {kv.first, kv.second};
    }

    std::pair<std::string_view, std::string_view> peek() const override {
        if (idx_ < 0 || idx_ >= kvs_size_) return {};
        const auto& kv = kvs_[idx_];
        return {kv.first, kv.second};
    }

    void seek(size_t pos) override { idx_ = pos; }

    bool more() const override { return more_; }

    int remaining() const override {
        if (idx_ < 0 || idx_ >= kvs_size_) return 0;
        return kvs_size_ - idx_;
    }

    int64_t get_kv_bytes() const override {
        int64_t kv_bytes {};
        for (auto& [k, v] : kvs_) kv_bytes += k.size() + v.size();
        return kv_bytes;
    }

    int size() const override { return kvs_size_; }
    void reset() override { idx_ = 0; }

    std::string next_begin_key() const override {
        std::string k;
        if (!more()) return k;
        const auto& key = kvs_[kvs_size_ - 1].first;
        k.reserve(key.size() + 1);
        k.append(key);
        k.push_back('\x00');
        return k;
    }

    std::string_view last_key() const override {
        if (!more()) return {};
        return kvs_[kvs_size_ - 1].first;
    }

private:
    std::vector<std::pair<std::string, std::string>> kvs_;
    int kvs_size_;
    int idx_;
    bool more_;
};

class FullRangeGetIterator final : public cloud::FullRangeGetIterator {
public:
    FullRangeGetIterator(std::string begin, std::string end, FullRangeGetOptions opts);

    ~FullRangeGetIterator() override;

    bool is_valid() const override { return is_valid_; }

    TxnErrorCode error_code() const override { return code_; }

    bool has_next() override;

    std::optional<std::pair<std::string_view, std::string_view>> next() override;

    std::optional<std::pair<std::string_view, std::string_view>> peek() override;

private:
    FullRangeGetOptions opts_;
    bool is_valid_ {true};
    TxnErrorCode code_ {TxnErrorCode::TXN_OK};
    std::unique_ptr<cloud::RangeGetIterator> inner_iter_;
    std::string begin_;
    std::string end_;
    std::unique_ptr<cloud::Transaction> txn_;
};

} // namespace memkv
} // namespace doris::cloud