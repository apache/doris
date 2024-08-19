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

#include "mem_txn_kv.h"

#include <glog/logging.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <string>

#include "cpp/sync_point.h"
#include "meta-service/txn_kv_error.h"
#include "txn_kv.h"

namespace doris::cloud {

int MemTxnKv::init() {
    return 0;
}

TxnErrorCode MemTxnKv::create_txn(std::unique_ptr<Transaction>* txn) {
    auto* t = new memkv::Transaction(this->shared_from_this());
    txn->reset(t);
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode MemTxnKv::get_kv(const std::string& key, std::string* val, int64_t version) {
    std::lock_guard<std::mutex> l(lock_);
    auto it = mem_kv_.find(key);
    if (it == mem_kv_.end() || it->second.empty()) {
        return TxnErrorCode::TXN_KEY_NOT_FOUND;
    }

    for (auto&& entry : it->second) {
        if (entry.commit_version <= version) {
            if (!entry.value.has_value()) {
                return TxnErrorCode::TXN_KEY_NOT_FOUND;
            }
            *val = *entry.value;
            return TxnErrorCode::TXN_OK;
        }
    }
    return TxnErrorCode::TXN_KEY_NOT_FOUND;
}

TxnErrorCode MemTxnKv::get_kv(const std::string& begin, const std::string& end, int64_t version,
                              int limit, bool* more, std::map<std::string, std::string>* kv_list) {
    if (begin >= end) {
        return TxnErrorCode::TXN_OK;
    }

    bool use_limit = true;

    if (limit < 0) {
        return TxnErrorCode::TXN_UNIDENTIFIED_ERROR;
    }
    if (limit == 0) {
        use_limit = false;
    }

    std::unique_lock<std::mutex> l(lock_);

    *more = false;
    auto begin_iter = mem_kv_.lower_bound(begin);
    auto end_iter = mem_kv_.lower_bound(end);
    for (; begin_iter != mem_kv_.end() && begin_iter != end_iter; begin_iter++) {
        for (auto&& entry : begin_iter->second) {
            if (entry.commit_version > version) {
                continue;
            }

            if (!entry.value.has_value()) {
                break;
            }

            kv_list->insert_or_assign(begin_iter->first, *entry.value);
            limit--;
            break;
        }
        if (use_limit && limit == 0) {
            break;
        }
    }
    if (use_limit && limit == 0 && ++begin_iter != end_iter) {
        *more = true;
    }
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode MemTxnKv::update(const std::set<std::string>& read_set,
                              const std::vector<OpTuple>& op_list, int64_t read_version,
                              int64_t* committed_version) {
    std::lock_guard<std::mutex> l(lock_);

    // check_conflict
    for (const auto& k : read_set) {
        auto iter = log_kv_.find(k);
        if (iter != log_kv_.end()) {
            auto log_item = iter->second;
            if (log_item.front().commit_version_ > read_version) {
                LOG(WARNING) << "commit conflict";
                //keep the same behaviour with fdb.
                return TxnErrorCode::TXN_CONFLICT;
            }
        }
    }

    ++committed_version_;

    int16_t seq = 0;
    for (const auto& vec : op_list) {
        const auto& [op_type, k, v] = vec;
        LogItem log_item {op_type, committed_version_, k, v};
        log_kv_[k].push_front(log_item);
        switch (op_type) {
        case memkv::ModifyOpType::PUT: {
            mem_kv_[k].push_front(Version {committed_version_, v});
            break;
        }
        case memkv::ModifyOpType::ATOMIC_SET_VER_KEY: {
            std::string ver_key(k);
            gen_version_timestamp(committed_version_, seq, &ver_key);
            mem_kv_[ver_key].push_front(Version {committed_version_, v});
            break;
        }
        case memkv::ModifyOpType::ATOMIC_SET_VER_VAL: {
            std::string ver_val(v);
            gen_version_timestamp(committed_version_, seq, &ver_val);
            mem_kv_[k].push_front(Version {committed_version_, ver_val});
            break;
        }
        case memkv::ModifyOpType::ATOMIC_ADD: {
            std::string org_val;
            if (!mem_kv_[k].empty()) {
                org_val = mem_kv_[k].front().value.value_or("");
            }
            if (org_val.size() != 8) {
                org_val.resize(8, '\0');
            }
            int64_t res = *(int64_t*)org_val.data() + *(int64_t*)v.data();
            std::memcpy(org_val.data(), &res, 8);
            mem_kv_[k].push_front(Version {committed_version_, org_val});
            break;
        }
        case memkv::ModifyOpType::REMOVE: {
            mem_kv_[k].push_front(Version {committed_version_, std::nullopt});
            break;
        }
        case memkv::ModifyOpType::REMOVE_RANGE: {
            auto begin_iter = mem_kv_.lower_bound(k);
            auto end_iter = mem_kv_.lower_bound(v);
            while (begin_iter != end_iter) {
                mem_kv_[begin_iter->first].push_front(Version {committed_version_, std::nullopt});
                begin_iter++;
            }
            break;
        }
        default:
            break;
        }
    }

    *committed_version = committed_version_;
    return TxnErrorCode::TXN_OK;
}

int MemTxnKv::gen_version_timestamp(int64_t ver, int16_t seq, std::string* str) {
    // Convert litter endian to big endian
    static auto to_big_int64 = [](int64_t v) {
        v = ((v & 0xffffffff00000000) >> 32) | ((v & 0x00000000ffffffff) << 32);
        v = ((v & 0xffff0000ffff0000) >> 16) | ((v & 0x0000ffff0000ffff) << 16);
        v = ((v & 0xff00ff00ff00ff00) >> 8) | ((v & 0x00ff00ff00ff00ff) << 8);
        return v;
    };

    static auto to_big_int16 = [](int16_t v) {
        v = ((v & 0xff00) >> 8) | ((v & 0x00ff) << 8);
        return v;
    };

    ver = to_big_int64(ver);
    seq = to_big_int16(seq);

    int size = str->size();
    str->resize(size + 10, '\0');
    std::memcpy(str->data() + size, &ver, sizeof(ver));
    std::memcpy(str->data() + size + 8, &seq, sizeof(seq));
    return 0;
}

int64_t MemTxnKv::get_last_commited_version() {
    std::lock_guard<std::mutex> l(lock_);
    return committed_version_;
}

int64_t MemTxnKv::get_last_read_version() {
    std::lock_guard<std::mutex> l(lock_);
    read_version_ = committed_version_;
    return read_version_;
}

std::unique_ptr<FullRangeGetIterator> MemTxnKv::full_range_get(std::string begin, std::string end,
                                                               FullRangeGetIteratorOptions opts) {
    return std::make_unique<memkv::FullRangeGetIterator>(std::move(begin), std::move(end),
                                                         std::move(opts));
}

} // namespace doris::cloud

namespace doris::cloud::memkv {

// =============================================================================
// Impl of Transaction
// =============================================================================

Transaction::Transaction(std::shared_ptr<MemTxnKv> kv) : kv_(std::move(kv)) {
    std::lock_guard<std::mutex> l(lock_);
    read_version_ = kv_->committed_version_;
}

int Transaction::init() {
    return 0;
}

void Transaction::put(std::string_view key, std::string_view val) {
    std::lock_guard<std::mutex> l(lock_);
    std::string k(key.data(), key.size());
    std::string v(val.data(), val.size());
    writes_.insert_or_assign(k, v);
    op_list_.emplace_back(ModifyOpType::PUT, k, v);
    ++num_put_keys_;
    put_bytes_ += key.size() + val.size();
    approximate_bytes_ += key.size() + val.size();
}

TxnErrorCode Transaction::get(std::string_view key, std::string* val, bool snapshot) {
    std::lock_guard<std::mutex> l(lock_);
    std::string k(key.data(), key.size());
    // the key set by atomic_xxx can't not be read before the txn is committed.
    // if it is read, the txn will not be able to commit.
    if (unreadable_keys_.count(k) != 0) {
        aborted_ = true;
        LOG(WARNING) << "read unreadable key, abort";
        return TxnErrorCode::TXN_UNIDENTIFIED_ERROR;
    }
    return inner_get(k, val, snapshot);
}

TxnErrorCode Transaction::get(std::string_view begin, std::string_view end,
                              std::unique_ptr<cloud::RangeGetIterator>* iter, bool snapshot,
                              int limit) {
    TEST_SYNC_POINT_CALLBACK("memkv::Transaction::get", &limit);
    std::lock_guard<std::mutex> l(lock_);
    std::string begin_k(begin.data(), begin.size());
    std::string end_k(end.data(), end.size());
    // TODO: figure out what happen if range_get has part of unreadable_keys
    if (unreadable_keys_.count(begin_k) != 0) {
        aborted_ = true;
        LOG(WARNING) << "read unreadable key, abort";
        return TxnErrorCode::TXN_UNIDENTIFIED_ERROR;
    }
    return inner_get(begin_k, end_k, iter, snapshot, limit);
}

TxnErrorCode Transaction::inner_get(const std::string& key, std::string* val, bool snapshot) {
    // Read your writes.
    auto it = writes_.find(key);
    if (it != writes_.end()) {
        *val = it->second;
        return TxnErrorCode::TXN_OK;
    }

    if (!snapshot) read_set_.emplace(key);
    TxnErrorCode err = kv_->get_kv(key, val, read_version_);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }

    for (auto&& [start, end] : remove_ranges_) {
        if (start <= key && key < end) {
            return TxnErrorCode::TXN_KEY_NOT_FOUND;
        }
    }
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode Transaction::inner_get(const std::string& begin, const std::string& end,
                                    std::unique_ptr<cloud::RangeGetIterator>* iter, bool snapshot,
                                    int limit) {
    bool more = false;
    std::map<std::string, std::string> kv_map;
    TxnErrorCode err = kv_->get_kv(begin, end, read_version_, limit, &more, &kv_map);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }

    // Overwrite by your writes.
    auto pred = [&](const std::pair<std::string, std::string>& val) {
        for (auto&& [start, end] : remove_ranges_) {
            if (start <= val.first && val.first < end) {
                return true;
            }
        }
        return false;
    };
    for (auto it = kv_map.begin(), last = kv_map.end(); it != last;) {
        if (pred(*it)) {
            it = kv_map.erase(it);
        } else {
            ++it;
        }
    }

    if (!snapshot) {
        for (auto&& [key, _] : kv_map) {
            read_set_.insert(key);
        }
    }

    auto begin_iter = writes_.lower_bound(begin);
    auto end_iter = writes_.lower_bound(end);
    while (begin_iter != end_iter) {
        kv_map.insert_or_assign(begin_iter->first, begin_iter->second);
        begin_iter++;
    }

    std::vector<std::pair<std::string, std::string>> kv_list(kv_map.begin(), kv_map.end());
    *iter = std::make_unique<memkv::RangeGetIterator>(std::move(kv_list), more);
    return TxnErrorCode::TXN_OK;
}

void Transaction::atomic_set_ver_key(std::string_view key_prefix, std::string_view val) {
    std::lock_guard<std::mutex> l(lock_);
    std::string k(key_prefix.data(), key_prefix.size());
    std::string v(val.data(), val.size());
    unreadable_keys_.insert(k);
    op_list_.emplace_back(ModifyOpType::ATOMIC_SET_VER_KEY, k, v);

    ++num_put_keys_;
    put_bytes_ += key_prefix.size() + val.size();
    approximate_bytes_ += key_prefix.size() + val.size();
}

void Transaction::atomic_set_ver_value(std::string_view key, std::string_view value) {
    std::lock_guard<std::mutex> l(lock_);
    std::string k(key.data(), key.size());
    std::string v(value.data(), value.size());
    unreadable_keys_.insert(k);
    op_list_.emplace_back(ModifyOpType::ATOMIC_SET_VER_VAL, k, v);

    ++num_put_keys_;
    put_bytes_ += key.size() + value.size();
    approximate_bytes_ += key.size() + value.size();
}

void Transaction::atomic_add(std::string_view key, int64_t to_add) {
    std::string k(key.data(), key.size());
    std::string v(sizeof(to_add), '\0');
    memcpy(v.data(), &to_add, sizeof(to_add));
    std::lock_guard<std::mutex> l(lock_);
    op_list_.emplace_back(ModifyOpType::ATOMIC_ADD, std::move(k), std::move(v));

    ++num_put_keys_;
    put_bytes_ += key.size() + 8;
    approximate_bytes_ += key.size() + 8;
}

bool Transaction::decode_atomic_int(std::string_view data, int64_t* val) {
    if (data.size() != sizeof(int64_t)) {
        return false;
    }

    memcpy(val, data.data(), sizeof(*val));
    return true;
}

void Transaction::remove(std::string_view key) {
    std::lock_guard<std::mutex> l(lock_);
    std::string k(key.data(), key.size());
    writes_.erase(k);
    std::string end_key = k;
    end_key.push_back(0x0);
    remove_ranges_.emplace_back(k, end_key);
    op_list_.emplace_back(ModifyOpType::REMOVE, k, "");

    ++num_del_keys_;
    delete_bytes_ += key.size();
    approximate_bytes_ += key.size();
}

void Transaction::remove(std::string_view begin, std::string_view end) {
    std::lock_guard<std::mutex> l(lock_);
    std::string begin_k(begin.data(), begin.size());
    std::string end_k(end.data(), end.size());
    if (begin_k >= end_k) {
        aborted_ = true;
    } else {
        // ATTN: we do not support read your writes about delete range.
        auto begin_iter = writes_.lower_bound(begin_k);
        auto end_iter = writes_.lower_bound(end_k);
        writes_.erase(begin_iter, end_iter);
        remove_ranges_.emplace_back(begin_k, end_k);
        op_list_.emplace_back(ModifyOpType::REMOVE_RANGE, begin_k, end_k);
    }
    ++num_del_keys_;
    delete_bytes_ += begin.size() + end.size();
    approximate_bytes_ += begin.size() + end.size();
}

TxnErrorCode Transaction::commit() {
    std::lock_guard<std::mutex> l(lock_);
    if (aborted_) {
        return TxnErrorCode::TXN_UNIDENTIFIED_ERROR;
    }
    auto code = kv_->update(read_set_, op_list_, read_version_, &committed_version_);
    if (code != TxnErrorCode::TXN_OK) {
        return code;
    }
    commited_ = true;
    op_list_.clear();
    read_set_.clear();
    writes_.clear();
    remove_ranges_.clear();
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode Transaction::get_read_version(int64_t* version) {
    std::lock_guard<std::mutex> l(lock_);
    *version = read_version_;
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode Transaction::get_committed_version(int64_t* version) {
    std::lock_guard<std::mutex> l(lock_);
    if (!commited_) {
        return TxnErrorCode::TXN_UNIDENTIFIED_ERROR;
    }
    *version = committed_version_;
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode Transaction::abort() {
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode Transaction::batch_get(std::vector<std::optional<std::string>>* res,
                                    const std::vector<std::string>& keys,
                                    const BatchGetOptions& opts) {
    if (keys.empty()) {
        return TxnErrorCode::TXN_OK;
    }
    std::lock_guard<std::mutex> l(lock_);
    res->reserve(keys.size());
    for (const auto& k : keys) {
        if (unreadable_keys_.count(k) != 0) {
            aborted_ = true;
            LOG(WARNING) << "read unreadable key, abort";
            return TxnErrorCode::TXN_UNIDENTIFIED_ERROR;
        }
        std::string val;
        auto ret = inner_get(k, &val, opts.snapshot);
        ret == TxnErrorCode::TXN_OK ? res->push_back(val) : res->push_back(std::nullopt);
    }
    return TxnErrorCode::TXN_OK;
}

FullRangeGetIterator::FullRangeGetIterator(std::string begin, std::string end,
                                           FullRangeGetIteratorOptions opts)
        : opts_(std::move(opts)), begin_(std::move(begin)), end_(std::move(end)) {}

FullRangeGetIterator::~FullRangeGetIterator() = default;

bool FullRangeGetIterator::has_next() {
    if (!is_valid_) {
        return false;
    }

    if (!inner_iter_) {
        auto* txn = opts_.txn;
        if (!txn) {
            // Create a new txn for each inner range get
            std::unique_ptr<cloud::Transaction> txn1;
            TxnErrorCode err = opts_.txn_kv->create_txn(&txn_);
            if (err != TxnErrorCode::TXN_OK) {
                is_valid_ = false;
                return false;
            }

            txn = txn_.get();
        }

        TxnErrorCode err = txn->get(begin_, end_, &inner_iter_, opts_.snapshot, 0);
        if (err != TxnErrorCode::TXN_OK) {
            is_valid_ = false;
            return false;
        }
    }

    return inner_iter_->has_next();
}

std::optional<std::pair<std::string_view, std::string_view>> FullRangeGetIterator::next() {
    if (!has_next()) {
        return std::nullopt;
    }

    return inner_iter_->next();
}

} // namespace doris::cloud::memkv
