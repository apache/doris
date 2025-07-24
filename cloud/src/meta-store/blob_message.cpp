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

} // namespace doris::cloud
