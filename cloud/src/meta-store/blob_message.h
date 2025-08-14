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

} // namespace doris::cloud
