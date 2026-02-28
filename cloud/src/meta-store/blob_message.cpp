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

#include "blob_message.h"

#include <butil/iobuf.h>
#include <butil/iobuf_inl.h>
#include <google/protobuf/message.h>

#include <cstdint>

#include "common/logging.h"
#include "common/util.h"
#include "meta-store/codec.h"
#include "meta-store/txn_kv.h"

namespace doris::cloud {

static std::vector<std::string_view> split_string(const std::string_view& str, int n) {
    std::vector<std::string_view> substrings;

    for (size_t i = 0; i < str.size(); i += n) {
        substrings.push_back(str.substr(i, n));
    }

    return substrings;
}

bool ValueBuf::to_pb(google::protobuf::Message* pb) const {
    butil::IOBuf merge;
    for (auto&& it : iters) {
        it->reset();
        while (it->has_next()) {
            auto [k, v] = it->next();
            merge.append_user_data((void*)v.data(), v.size(), +[](void*) {});
        }
    }
    butil::IOBufAsZeroCopyInputStream merge_stream(merge);
    return pb->ParseFromZeroCopyStream(&merge_stream);
}

std::string ValueBuf::value() const {
    butil::IOBuf merge;
    for (auto&& it : iters) {
        it->reset();
        while (it->has_next()) {
            auto [k, v] = it->next();
            merge.append_user_data((void*)v.data(), v.size(), +[](void*) {});
        }
    }
    return merge.to_string();
}

std::vector<std::string> ValueBuf::keys() const {
    std::vector<std::string> ret;
    for (auto&& it : iters) {
        it->reset();
        while (it->has_next()) {
            auto [k, _] = it->next();
            ret.emplace_back(k.data(), k.size());
        }
    }
    return ret;
}

void ValueBuf::remove(Transaction* txn) const {
    for (auto&& it : iters) {
        it->reset();
        while (it->has_next()) {
            txn->remove(it->next().first);
        }
    }
}

TxnErrorCode ValueBuf::get(Transaction* txn, std::string_view key, bool snapshot) {
    iters.clear();
    ver = -1;

    std::string begin_key {key};
    std::string end_key {key};
    encode_int64(INT64_MAX, &end_key);
    std::unique_ptr<RangeGetIterator> it;
    TxnErrorCode err = txn->get(begin_key, end_key, &it, snapshot);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    if (!it->has_next()) {
        return TxnErrorCode::TXN_KEY_NOT_FOUND;
    }
    // Extract version
    auto [k, _] = it->next();
    if (k.size() == key.size()) { // Old version KV
        DCHECK(k == key) << hex(k) << ' ' << hex(key);
        DCHECK_EQ(it->size(), 1) << hex(k) << ' ' << hex(key);
        ver = 0;
    } else {
        k.remove_prefix(key.size());
        int64_t suffix;
        if (decode_int64(&k, &suffix) != 0) [[unlikely]] {
            LOG_WARNING("failed to decode key").tag("key", hex(k));
            return TxnErrorCode::TXN_INVALID_DATA;
        }
        ver = suffix >> 56 & 0xff;
    }
    bool more = it->more();
    if (!more) {
        iters.push_back(std::move(it));
        return TxnErrorCode::TXN_OK;
    }
    begin_key = it->next_begin_key();
    iters.push_back(std::move(it));
    do {
        err = txn->get(begin_key, end_key, &it, snapshot);
        if (err != TxnErrorCode::TXN_OK) {
            return err;
        }
        more = it->more();
        if (more) {
            begin_key = it->next_begin_key();
        }
        iters.push_back(std::move(it));
    } while (more);
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode blob_get(Transaction* txn, std::string_view key, ValueBuf* val, bool snapshot) {
    return val->get(txn, key, snapshot);
}

void blob_put(Transaction* txn, std::string_view key, const google::protobuf::Message& pb,
              uint8_t ver, size_t split_size) {
    std::string value;
    bool ret = pb.SerializeToString(&value); // Always success
    DCHECK(ret) << hex(key) << ' ' << pb.ShortDebugString();
    blob_put(txn, key, value, ver, split_size);
}

void blob_put(Transaction* txn, std::string_view key, std::string_view value, uint8_t ver,
              size_t split_size) {
    auto split_vec = split_string(value, split_size);
    int64_t suffix_base = ver;
    suffix_base <<= 56;
    for (size_t i = 0; i < split_vec.size(); ++i) {
        std::string k(key);
        encode_int64(suffix_base + i, &k);
        txn->put(k, split_vec[i]);
    }
}

BlobIterator::BlobIterator(std::unique_ptr<FullRangeGetIterator> iter)
        : error_code_(TxnErrorCode::TXN_OK), iter_(std::move(iter)) {
    // Load the first blob
    next();
}

bool BlobIterator::valid() const {
    return error_code_ == TxnErrorCode::TXN_OK && is_underlying_iter_valid() &&
           // ... or it has reached the end of blobs
           current_blob_key_.size() > 0;
}

TxnErrorCode BlobIterator::error_code() const {
    if (error_code_ != TxnErrorCode::TXN_OK) {
        return error_code_;
    }
    if (!iter_) {
        return TxnErrorCode::TXN_INVALID_DATA;
    }
    return iter_->error_code();
}

void BlobIterator::next() {
    // Switch to next blob, so clear previous state
    version_ = -1;
    current_blob_key_.clear();
    current_blob_values_.clear();
    current_blob_raw_keys_.clear();

    if (error_code_ == TxnErrorCode::TXN_OK && is_underlying_iter_valid() && iter_->has_next()) {
        load_current_blob();
    }
}

bool BlobIterator::parse_value(google::protobuf::Message* pb) const {
    butil::IOBuf merge;
    for (auto&& value : current_blob_values_) {
        merge.append_user_data((void*)value.data(), value.size(), +[](void*) {});
    }
    butil::IOBufAsZeroCopyInputStream merge_stream(merge);
    return pb->ParseFromZeroCopyStream(&merge_stream);
}

void BlobIterator::load_current_blob() {
    uint16_t next_sequence = 0;
    if (auto&& kvp = iter_->peek(); !kvp.has_value()) {
        // All keys have been processed.
        return;
    } else if (!extract_origin_key(kvp->first, &current_blob_key_, &version_, &next_sequence)) {
        return;
    }

    while (iter_->is_valid() && iter_->has_next()) {
        auto [k, v] = *(iter_->peek());

        // Is this key part of the current blob?
        std::string origin_key;
        uint8_t version = -1;
        uint16_t sequence = -1;
        if (!extract_origin_key(k, &origin_key, &version, &sequence) ||
            origin_key != current_blob_key_) {
            return;
        } else if (version != version_ || sequence != next_sequence) {
            LOG_WARNING("blob key version or sequence mismatch")
                    .tag("expected_version", version_)
                    .tag("actual_version", version)
                    .tag("expected_sequence", next_sequence)
                    .tag("actual_sequence", sequence)
                    .tag("key", hex(k));
            error_code_ = TxnErrorCode::TXN_INVALID_DATA;
            return;
        }

        current_blob_raw_keys_.push_back(std::string(k));
        current_blob_values_.push_back(std::string(v));
        iter_->next();
        next_sequence++;
    }
}

bool BlobIterator::extract_origin_key(std::string_view raw_key, std::string* output,
                                      uint8_t* version, uint16_t* sequence) {
    // The suffix is 8 bytes: |version(1)|dummy(5)|sequence(2)|
    if (raw_key.size() < 9) {
        LOG_WARNING("failed to extract origin key").tag("key", hex(raw_key));
        error_code_ = TxnErrorCode::TXN_INVALID_DATA;
        return false;
    }

    const size_t origin_key_size = raw_key.size() - 9;
    std::string_view origin_key = raw_key.substr(0, origin_key_size);
    raw_key.remove_prefix(origin_key_size);
    int64_t suffix = 0;
    if (decode_int64(&raw_key, &suffix) != 0) {
        LOG_WARNING("failed to decode int64")
                .tag("key", hex(raw_key))
                .tag("origin_key", hex(origin_key));
        error_code_ = TxnErrorCode::TXN_INVALID_DATA;
        return false;
    }

    *version = (suffix >> 56) & 0xff;
    *sequence = suffix & 0xffff;
    *output = std::string(origin_key);

    return true;
}

std::unique_ptr<BlobIterator> blob_get_range(const std::shared_ptr<TxnKv>& txn_kv,
                                             std::string_view begin_key, std::string_view end_key,
                                             bool snapshot) {
    FullRangeGetOptions options;
    options.txn_kv = txn_kv;
    options.prefetch = true;
    options.snapshot = snapshot;
    std::unique_ptr<FullRangeGetIterator> iter =
            txn_kv->full_range_get(std::string(begin_key), std::string(end_key), options);

    return std::make_unique<BlobIterator>(std::move(iter));
}

std::unique_ptr<BlobIterator> blob_get_range(Transaction* txn, std::string_view begin_key,
                                             std::string_view end_key, bool snapshot) {
    FullRangeGetOptions options;
    options.prefetch = true;
    options.snapshot = snapshot;
    std::unique_ptr<FullRangeGetIterator> iter =
            txn->full_range_get(std::string(begin_key), std::string(end_key), options);

    return std::make_unique<BlobIterator>(std::move(iter));
}

namespace versioned {

void blob_put(Transaction* txn, std::string_view key, std::string_view value, uint8_t ver,
              size_t split_size) {
    std::string encoded_key(key);
    uint32_t offset = encode_versionstamp(Versionstamp::min(), &encoded_key);
    encode_versionstamp_end(&encoded_key);

    auto split_vec = split_string(value, split_size);
    int64_t suffix_base = ver;
    suffix_base <<= 56;
    for (size_t i = 0; i < split_vec.size(); ++i) {
        std::string k(encoded_key);
        encode_int64(suffix_base + i, &k);
        txn->atomic_set_ver_key(k, offset, split_vec[i]);
    }
}

void blob_put(Transaction* txn, std::string_view key, const google::protobuf::Message& pb,
              uint8_t ver, size_t split_size) {
    std::string value;
    bool ret = pb.SerializeToString(&value); // Always success
    DCHECK(ret) << hex(key) << ' ' << pb.ShortDebugString();
    versioned::blob_put(txn, key, value, ver, split_size);
}

void blob_put(Transaction* txn, std::string_view key, Versionstamp vs, std::string_view value,
              uint8_t ver, size_t split_size) {
    std::string encoded_key(key);
    encode_versionstamp(vs, &encoded_key);
    encode_versionstamp_end(&encoded_key);
    // Avoid set versionstamp again
    doris::cloud::blob_put(txn, encoded_key, value, ver, split_size);
}

void blob_put(Transaction* txn, std::string_view key, Versionstamp vs,
              const google::protobuf::Message& pb, uint8_t ver, size_t split_size) {
    std::string value;
    bool ret = pb.SerializeToString(&value); // Always success
    DCHECK(ret) << hex(key) << ' ' << pb.ShortDebugString();
    versioned::blob_put(txn, key, vs, value, ver, split_size);
}

} // namespace versioned
} // namespace doris::cloud
