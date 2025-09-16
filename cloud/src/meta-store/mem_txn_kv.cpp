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
#include <ranges>
#include <string>

#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-store/txn_kv_error.h"
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
                              const RangeGetOptions& opts, bool* more,
                              std::vector<std::pair<std::string, std::string>>* kv_list) {
    if (begin >= end) {
        return TxnErrorCode::TXN_OK;
    }

    bool use_limit = true;
    int limit = opts.batch_limit;

    if (limit < 0) {
        return TxnErrorCode::TXN_UNIDENTIFIED_ERROR;
    }
    if (limit == 0) {
        use_limit = false;
    }

    std::unique_lock<std::mutex> l(lock_);

    auto apply_key_selector = [&](RangeKeySelector selector,
                                  const std::string& key) -> decltype(mem_kv_.lower_bound(key)) {
        auto iter = mem_kv_.lower_bound(key);
        switch (selector) {
        case RangeKeySelector::FIRST_GREATER_OR_EQUAL:
            break;
        case RangeKeySelector::FIRST_GREATER_THAN:
            if (iter != mem_kv_.end() && iter->first == key) {
                ++iter;
            }
            break;
        case RangeKeySelector::LAST_LESS_OR_EQUAL:
            if (iter != mem_kv_.begin() && iter->first != key) {
                --iter;
            }
            break;
        case RangeKeySelector::LAST_LESS_THAN:
            if (iter != mem_kv_.begin()) {
                --iter;
            }
            break;
        }
        return iter;
    };

    *more = false;

    bool reverse = opts.reverse;
    std::vector<std::pair<std::string, std::string>> temp_results;

    if (!reverse) {
        // Forward iteration
        auto begin_iter = apply_key_selector(opts.begin_key_selector, begin);
        auto end_iter = apply_key_selector(opts.end_key_selector, end);
        if (begin_iter == mem_kv_.end() ||
            (end_iter != mem_kv_.end() && end_iter->first < begin_iter->first)) {
            // If the begin iterator is at the end or the end iterator is before begin, return empty
            kv_list->clear();
            *more = false;
            return TxnErrorCode::TXN_OK;
        }

        for (; begin_iter != end_iter; begin_iter++) {
            // Find the appropriate version
            for (auto&& entry : begin_iter->second) {
                if (entry.commit_version > version) {
                    continue;
                }

                if (!entry.value.has_value()) {
                    break;
                }

                temp_results.emplace_back(begin_iter->first, *entry.value);
                break;
            }

            if (use_limit && temp_results.size() >= static_cast<size_t>(limit)) {
                *more = true;
                break;
            }
        }
    } else {
        // Reverse iteration
        auto end_iter = apply_key_selector(opts.end_key_selector, end);
        auto begin_iter = apply_key_selector(opts.begin_key_selector, begin);
        if (begin_iter == mem_kv_.end() ||
            (end_iter != mem_kv_.end() && end_iter->first <= begin_iter->first)) {
            kv_list->clear();
            *more = false;
            return TxnErrorCode::TXN_OK;
        }

        do {
            --end_iter; // end always excludes the last key

            for (auto&& entry : end_iter->second) {
                if (entry.commit_version > version) {
                    continue;
                }

                if (!entry.value.has_value()) {
                    break;
                }

                temp_results.emplace_back(end_iter->first, *entry.value);
                break;
            }

            if (use_limit && temp_results.size() >= static_cast<size_t>(limit)) {
                *more = true;
                break;
            }
        } while (end_iter != begin_iter);
    }

    kv_list->swap(temp_results);

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

    size_t size = str->size();
    if (size < 14) {
        LOG(WARNING) << "gen_version_timestamp: str size is too small, size: " << size
                     << ", required: 14";
        return -1;
    }
    uint32_t offset = 0;
    std::memcpy(&offset, str->data() + size - 4, sizeof(offset));
    str->resize(size - 4, '\0');
    if (offset + 10 > str->size()) {
        LOG(WARNING) << "gen_version_timestamp: offset + 10 > str size, offset: " << offset
                     << ", str size: " << size;
        return -1;
    }
    std::memcpy(str->data() + offset, &ver, sizeof(ver));
    std::memcpy(str->data() + offset + 8, &seq, sizeof(seq));
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
                                                               FullRangeGetOptions opts) {
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
    kv_->put_count_++;
    kv_->put_bytes_ += key.size() + val.size();
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
                              std::unique_ptr<cloud::RangeGetIterator>* iter,
                              const RangeGetOptions& opts) {
    RangeGetOptions options = opts;
    TEST_SYNC_POINT_CALLBACK("memkv::Transaction::get", &options.batch_limit);
    std::lock_guard<std::mutex> l(lock_);
    std::string begin_k(begin.data(), begin.size());
    std::string end_k(end.data(), end.size());
    // TODO: figure out what happen if range_get has part of unreadable_keys
    if (unreadable_keys_.count(begin_k) != 0) {
        aborted_ = true;
        LOG(WARNING) << "read unreadable key, abort";
        return TxnErrorCode::TXN_UNIDENTIFIED_ERROR;
    }
    return inner_get(begin_k, end_k, iter, options);
}

std::unique_ptr<cloud::FullRangeGetIterator> Transaction::full_range_get(
        std::string_view begin, std::string_view end, cloud::FullRangeGetOptions opts) {
    opts.txn = this;
    opts.txn_kv.reset();
    return kv_->full_range_get(std::string(begin), std::string(end), std::move(opts));
}

TxnErrorCode Transaction::inner_get(const std::string& key, std::string* val, bool snapshot) {
    num_get_keys_++;
    kv_->get_count_++;
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
    get_bytes_ += val->size() + key.size();
    kv_->get_bytes_ += val->size() + key.size();
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode Transaction::inner_get(const std::string& begin, const std::string& end,
                                    std::unique_ptr<cloud::RangeGetIterator>* iter,
                                    const RangeGetOptions& opts) {
    bool more = false;
    bool snapshot = opts.snapshot;
    std::vector<std::pair<std::string, std::string>> kv_list;
    TxnErrorCode err = kv_->get_kv(begin, end, read_version_, opts, &more, &kv_list);
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
    for (auto it = kv_list.begin(), last = kv_list.end(); it != last;) {
        if (pred(*it)) {
            it = kv_list.erase(it);
        } else {
            ++it;
        }
    }

    if (!snapshot) {
        for (auto&& [key, _] : kv_list) {
            read_set_.insert(key);
        }
    }

    std::map<std::string, std::string> kv_map;
    for (const auto& [key, value] : kv_list) {
        kv_map[key] = value;
    }

    // Get writes in the range and apply key selectors
    auto apply_key_selector = [&](RangeKeySelector selector,
                                  const std::string& key) -> decltype(writes_.lower_bound(key)) {
        auto iter = writes_.lower_bound(key);
        switch (selector) {
        case RangeKeySelector::FIRST_GREATER_OR_EQUAL:
            break;
        case RangeKeySelector::FIRST_GREATER_THAN:
            if (iter != writes_.end() && iter->first == key) {
                ++iter;
            }
            break;
        case RangeKeySelector::LAST_LESS_OR_EQUAL:
            if (iter != writes_.begin() && iter->first != key) {
                --iter;
            }
            break;
        case RangeKeySelector::LAST_LESS_THAN:
            if (iter != writes_.begin()) {
                --iter;
            }
            break;
        }
        return iter;
    };

    auto begin_iter = apply_key_selector(opts.begin_key_selector, begin);
    auto end_iter = apply_key_selector(opts.end_key_selector, end);

    // The end_iter is exclusive, so we need to check if it is valid:
    // 1. end_iter is in the end
    // 2. or the begin_iter is less than the end_iter
    for (; begin_iter != end_iter &&
           (end_iter == writes_.end() || begin_iter->first < end_iter->first);
         ++begin_iter) {
        const auto& key = begin_iter->first;
        const auto& value = begin_iter->second;
        kv_map[key] = value;
    }

    kv_list.clear();
    if (!opts.reverse) {
        for (const auto& [key, value] : kv_map) {
            kv_list.emplace_back(key, value);
        }
    } else {
        for (auto& it : std::ranges::reverse_view(kv_map)) {
            kv_list.emplace_back(it.first, it.second);
        }
    }

    if (opts.batch_limit > 0 && kv_list.size() > static_cast<size_t>(opts.batch_limit)) {
        more = true;
        kv_list.resize(opts.batch_limit);
    }

    num_get_keys_ += kv_list.size();
    kv_->get_count_ += kv_list.size();
    for (auto& [k, v] : kv_list) {
        get_bytes_ += k.size() + v.size();
        kv_->get_bytes_ += k.size() + v.size();
    }
    *iter = std::make_unique<memkv::RangeGetIterator>(std::move(kv_list), more);
    return TxnErrorCode::TXN_OK;
}

void Transaction::atomic_set_ver_key(std::string_view key_prefix, std::string_view val) {
    std::lock_guard<std::mutex> l(lock_);
    kv_->put_count_++;
    std::string k(key_prefix.data(), key_prefix.size());
    std::string v(val.data(), val.size());
    uint32_t prefix_size = k.size();
    // ATTN:
    // 10 bytes for versiontimestamp must be 0, trailing 4 bytes is for
    // prefix len
    k.resize(k.size() + 14, '\0');
    std::memcpy(k.data() + (k.size() - 4), &prefix_size, 4);
    unreadable_keys_.insert(k);
    op_list_.emplace_back(ModifyOpType::ATOMIC_SET_VER_KEY, k, v);

    ++num_put_keys_;

    kv_->put_bytes_ += k.size() + val.size();
    put_bytes_ += k.size() + val.size();
    approximate_bytes_ += k.size() + val.size();
}

bool Transaction::atomic_set_ver_key(std::string_view key, uint32_t offset, std::string_view val) {
    if (key.size() < 10 || offset + 10 > key.size()) {
        LOG(WARNING) << "atomic_set_ver_key: invalid key or offset, key=" << key
                     << " offset=" << offset << ", key_size=" << key.size();
        return false;
    }
    std::lock_guard<std::mutex> l(lock_);
    kv_->put_count_++;
    std::string k(key.data(), key.size());
    std::string v(val.data(), val.size());
    k.append((char*)&offset, sizeof(offset)); // ATTN: assume little-endian
    unreadable_keys_.insert(k);
    op_list_.emplace_back(ModifyOpType::ATOMIC_SET_VER_KEY, k, v);

    ++num_put_keys_;
    put_bytes_ += k.size() + v.size();
    approximate_bytes_ += k.size() + v.size();
    return true;
}

void Transaction::atomic_set_ver_value(std::string_view key, std::string_view value) {
    std::lock_guard<std::mutex> l(lock_);
    kv_->put_count_++;
    std::string k(key.data(), key.size());
    std::string v(value.data(), value.size());
    size_t prefix_size = v.size();
    // ATTN:
    // 10 bytes for versiontimestamp must be 0, trailing 4 bytes is for
    // prefix len
    v.resize(v.size() + 14, '\0');
    std::memcpy(v.data() + (v.size() - 4), &prefix_size, 4);
    unreadable_keys_.insert(k);
    op_list_.emplace_back(ModifyOpType::ATOMIC_SET_VER_VAL, k, v);

    ++num_put_keys_;
    kv_->put_bytes_ += key.size() + value.size();
    put_bytes_ += key.size() + value.size();
    approximate_bytes_ += key.size() + value.size();
}

void Transaction::atomic_add(std::string_view key, int64_t to_add) {
    std::string k(key.data(), key.size());
    std::string v(sizeof(to_add), '\0');
    memcpy(v.data(), &to_add, sizeof(to_add));
    std::lock_guard<std::mutex> l(lock_);
    kv_->put_count_++;
    op_list_.emplace_back(ModifyOpType::ATOMIC_ADD, std::move(k), std::move(v));

    ++num_put_keys_;
    put_bytes_ += key.size() + 8;
    kv_->put_bytes_ += key.size() + 8;
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
    kv_->del_count_++;
    std::string k(key.data(), key.size());
    writes_.erase(k);
    std::string end_key = k;
    end_key.push_back(0x0);
    remove_ranges_.emplace_back(k, end_key);
    op_list_.emplace_back(ModifyOpType::REMOVE, k, "");

    ++num_del_keys_;
    kv_->del_bytes_ += key.size();
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
    kv_->del_count_ += 2;
    // same as normal txn
    num_del_keys_ += 2;
    kv_->del_bytes_ += begin.size() + end.size();
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

    // Generate versionstamp if enabled
    if (versionstamp_enabled_) {
        // For MemTxnKv, generate a fake versionstamp based on committed_version_
        // In real FDB, this would be the actual 10-byte versionstamp
        versionstamp_result_.resize(10);
        uint64_t version_be = __builtin_bswap64(static_cast<uint64_t>(committed_version_));
        std::memcpy(versionstamp_result_.data(), &version_be, 8);
        // Last 2 bytes set to 0 (batch order in FDB)
        versionstamp_result_[8] = 0;
        versionstamp_result_[9] = 0;
    }

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

void Transaction::enable_get_versionstamp() {
    versionstamp_enabled_ = true;
}

TxnErrorCode Transaction::get_versionstamp(std::string* versionstamp) {
    if (!versionstamp_enabled_) {
        LOG(WARNING) << "get_versionstamp called but versionstamp not enabled";
        return TxnErrorCode::TXN_INVALID_ARGUMENT;
    }

    if (versionstamp_result_.empty()) {
        LOG(WARNING) << "versionstamp not available, commit may not have been called or failed";
        return TxnErrorCode::TXN_KEY_NOT_FOUND;
    }

    *versionstamp = versionstamp_result_;
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
    kv_->get_count_ += keys.size();
    num_get_keys_ += keys.size();
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode Transaction::batch_scan(
        std::vector<std::optional<std::pair<std::string, std::string>>>* res,
        const std::vector<std::pair<std::string, std::string>>& ranges,
        const BatchGetOptions& opts) {
    if (ranges.empty()) {
        return TxnErrorCode::TXN_OK;
    }
    std::lock_guard<std::mutex> l(lock_);
    res->reserve(ranges.size());

    for (const auto& [start_key, end_key] : ranges) {
        if (unreadable_keys_.count(start_key) != 0) {
            aborted_ = true;
            LOG(WARNING) << "read unreadable key, abort";
            return TxnErrorCode::TXN_UNIDENTIFIED_ERROR;
        }

        RangeGetOptions range_opts;
        range_opts.snapshot = opts.snapshot;
        range_opts.batch_limit = 1;
        range_opts.reverse = opts.reverse;
        range_opts.begin_key_selector = RangeKeySelector::FIRST_GREATER_OR_EQUAL;
        range_opts.end_key_selector = RangeKeySelector::FIRST_GREATER_OR_EQUAL;

        std::unique_ptr<cloud::RangeGetIterator> iter;
        auto ret = inner_get(start_key, end_key, &iter, range_opts);
        if (ret != TxnErrorCode::TXN_OK) {
            return ret;
        }

        if (iter->has_next()) {
            auto [found_key, found_value] = iter->next();
            res->push_back(std::make_pair(std::string(found_key), std::string(found_value)));
        } else {
            res->push_back(std::nullopt);
        }
    }

    kv_->get_count_ += ranges.size();
    num_get_keys_ += ranges.size();
    return TxnErrorCode::TXN_OK;
}

FullRangeGetIterator::FullRangeGetIterator(std::string begin, std::string end,
                                           FullRangeGetOptions opts)
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
                code_ = err;
                return false;
            }

            txn = txn_.get();
        }

        // For simplicity, we always get the entire range without batch limit.
        RangeGetOptions opts;
        opts.snapshot = opts_.snapshot;
        opts.batch_limit = 0;
        opts.reverse = opts_.reverse;
        opts.begin_key_selector = opts_.begin_key_selector;
        opts.end_key_selector = opts_.end_key_selector;
        TxnErrorCode err = txn->get(begin_, end_, &inner_iter_, opts);
        if (err != TxnErrorCode::TXN_OK) {
            is_valid_ = false;
            code_ = err;
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

std::optional<std::pair<std::string_view, std::string_view>> FullRangeGetIterator::peek() {
    if (!has_next()) {
        return std::nullopt;
    }

    return inner_iter_->peek();
}

} // namespace doris::cloud::memkv
