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

#include <string>
#include <string_view>

#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"

namespace google::protobuf {
class Message;
}

namespace doris::cloud {

/**
 * Supports splitting large values (>100KB) into multiple KVs, with a logical value size of up to the fdb transaction limit (<10MB).
 * Supports multi version format parsing of values (which can be any byte sequence format), and can recognize the version of values
 * that are forward compatible with older versions of values.
 * Key format:
 *  {origin_key}{suffix: i64}
 *  suffix (big-endian):
 *    |Bytes 0      |Bytes 1-5    |Bytes 6-7    |
 *    |-------------|-------------|-------------|
 *    |version      |dummy        |sequence     |
 */
struct ValueBuf {
    // TODO(plat1ko): Support decompression
    [[nodiscard]] bool to_pb(google::protobuf::Message* pb) const;
    // TODO: More bool to_xxx(Xxx* xxx) const;

    // Remove all splitted KV in `iters_` via `txn`
    void remove(Transaction* txn) const;

    // Get a key, save raw splitted values of the key to `this`, value length may be bigger than 100k
    // Return TXN_OK for success get a key, TXN_KEY_NOT_FOUND for key not found, otherwise for error.
    TxnErrorCode get(Transaction* txn, std::string_view key, bool snapshot = false);

    // return the merged value in ValueBuf
    std::string value() const;

    // return all keys in ValueBuf, if the value is not splitted, size of keys is 1
    std::vector<std::string> keys() const;

    std::vector<std::unique_ptr<RangeGetIterator>> iters;
    int8_t ver {-1};
};

/**
 * Get a key, return key's value, value length may be bigger than 100k
 * @param txn fdb txn handler
 * @param key encode key
 * @param val return wrapped raw splitted values of the key
 * @param snapshot if true, `key` will not be included in txn conflict detection this time
 * @return return TXN_OK for success get a key, TXN_KEY_NOT_FOUND for key not found, otherwise for error.
 */
TxnErrorCode blob_get(Transaction* txn, std::string_view key, ValueBuf* val, bool snapshot = false);

/**
 * Put a KV, it's value may be bigger than 100k
 * TODO(plat1ko): Support compression
 * @param txn fdb txn handler
 * @param key encode key
 * @param pb value to save
 * @param ver value version
 * @param split_size how many byte sized fragments are the value split into
 */
void blob_put(Transaction* txn, std::string_view key, const google::protobuf::Message& pb,
              uint8_t ver, size_t split_size = 90 * 1000);

/**
 * Put a KV, it's value may be bigger than 100k
 * @param txn fdb txn handler
 * @param key encode key
 * @param value value to save
 * @param ver value version
 * @param split_size how many byte sized fragments are the value split into
 */
void blob_put(Transaction* txn, std::string_view key, std::string_view value, uint8_t ver,
              size_t split_size = 90 * 1000);

// Iterator for blob key-value pairs.
//
// The keys with the same origin key are considered as a blob. The origin key is obtained by
// removing the sequence suffix from the raw key.
//
// ATTN: This iterator is not compatible with the old style blob keys (without sequence suffix).
//
// Usage:
//   auto iter = blob_get_range(txn_kv, begin_key, end_key);
//   for (; iter->valid(); iter->next()) {
//      // Process the current blob
//      auto origin_key = iter->key();
//      auto version = iter->version();
//      auto raw_keys = iter->raw_keys();
//      auto values = iter->values();
//      // Or parse the blob value into a protobuf message
//      YourProtoMessage pb;
//      if (iter->parse_value(&pb)) {
//          // Successfully parsed
//      }
//   }
//   if (iter->error_code() != TxnErrorCode::TXN_OK) {
//      // Handle error
//   }
class BlobIterator {
public:
    BlobIterator(std::unique_ptr<FullRangeGetIterator> iter);
    ~BlobIterator() = default;

    BlobIterator(const BlobIterator&) = delete;
    BlobIterator& operator=(const BlobIterator&) = delete;

    // There are two states of validity:
    // 1. The iterator is valid and points to a blob (a set of KVs with the same origin key).
    //    In this state, `valid()` returns true.
    // 2. The iterator is invalid (either due to an error or because it has reached the end).
    //    In this state, `valid()` returns false.
    bool valid() const;

    // Move to the next blob.
    //
    // If there is no next blob, the iterator becomes invalid.
    void next();

    // Get the error code of the iterator. If the iterator has reached the end, returns TXN_OK.
    TxnErrorCode error_code() const;

    // Return the version of the current blob. `valid()` must be true before calling this method.
    uint8_t version() const { return version_; }

    // Return the origin key of the current blob. `valid()` must be true before calling this method.
    std::string_view key() const { return current_blob_key_; }

    // Return the raw keys of the current blob. `valid()` must be true before calling this method.
    const std::vector<std::string>& raw_keys() const { return current_blob_raw_keys_; }

    // Return the values of the current blob. `valid()` must be true before calling this method.
    const std::vector<std::string>& values() const { return current_blob_values_; }

    // Parse the value of the current blob into the given protobuf message. `valid()` must be true before calling this method.
    bool parse_value(google::protobuf::Message* pb) const;

private:
    bool is_underlying_iter_valid() const { return iter_ && iter_->is_valid(); }

    void load_current_blob();
    bool extract_origin_key(std::string_view raw_key, std::string* origin_key, uint8_t* version,
                            uint16_t* sequence);

    TxnErrorCode error_code_ = TxnErrorCode::TXN_OK;
    std::unique_ptr<FullRangeGetIterator> iter_;
    uint8_t version_ = 0;
    std::string current_blob_key_;
    std::vector<std::string> current_blob_raw_keys_;
    std::vector<std::string> current_blob_values_;
};

// Get a range of blob key-value pairs.
//
// It requires that all keys in the range are in the new blob format (with sequence suffix).
std::unique_ptr<BlobIterator> blob_get_range(const std::shared_ptr<TxnKv>& txn_kv,
                                             std::string_view begin_key, std::string_view end_key,
                                             bool snapshot = false);

// Get a range of blob key-value pairs.
//
// It requires that all keys in the range are in the new blob format (with sequence suffix).
std::unique_ptr<BlobIterator> blob_get_range(Transaction* txn, std::string_view begin_key,
                                             std::string_view end_key, bool snapshot = false);

namespace versioned {

// Put a blob message with a auto generated versionstamp.
void blob_put(Transaction* txn, std::string_view key, const google::protobuf::Message& pb,
              uint8_t ver = 0, size_t split_size = 90 * 1000);

// Put a blob message with a auto generated versionstamp.
void blob_put(Transaction* txn, std::string_view key, std::string_view value, uint8_t ver = 0,
              size_t split_size = 90 * 1000);

// Put a blob message with a specified versionstamp.
void blob_put(Transaction* txn, std::string_view key, Versionstamp v,
              const google::protobuf::Message& pb, uint8_t ver = 0, size_t split_size = 90 * 1000);

// Put a blob message with a specified versionstamp.
void blob_put(Transaction* txn, std::string_view key, Versionstamp v, std::string_view value,
              uint8_t ver = 0, size_t split_size = 90 * 1000);

} // namespace versioned

} // namespace doris::cloud
