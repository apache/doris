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

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "meta-store/txn_kv_error.h"

namespace google::protobuf {
class Message;
}

namespace doris::cloud {
class TxnKv;
class Transaction;
class RangeGetIterator;

std::string hex(std::string_view str);

std::string unhex(std::string_view str);

/**
 * Prettifies the given key, the first byte must be key space tag, say 0x01, and
 * the remaining part must be the output of codec function family.
 *
 * The result is like following:
 *
 * /------------------------------------------------------------------------------------------------------- 0. key space: 1
 * | /----------------------------------------------------------------------------------------------------- 1. txn
 * | |           /----------------------------------------------------------------------------------------- 2. instance_id_deadbeef
 * | |           |                                             /------------------------------------------- 3. txn_label
 * | |           |                                             |                       /------------------- 4. 10003
 * | |           |                                             |                       |                 /- 5. insert_3fd5ad12d0054a9b-8c776d3218c6adb7
 * | |           |                                             |                       |                 |
 * v v           v                                             v                       v                 v
 * 011074786e000110696e7374616e63655f69645f646561646265656600011074786e5f696e646578000112000000000000271310696e736572745f336664356164313264303035346139622d386337373664333231386336616462370001
 *
 * @param key_hex encoded key hex string
 * @param unicode whether to use unicode (UTF8) to draw line, default false.
 * @return the pretty format, empty result if error occurs
 */
std::string prettify_key(std::string_view key_hex, bool unicode = false);

/**
 * Converts proto message to json string
 *
 * @return empty string if conversion failed
 */
std::string proto_to_json(const ::google::protobuf::Message& msg, bool add_whitespace = false);

/**
 * Test whether key exists
 * @param txn fdb txn handler
 * @param key encode key
 * @param snapshot if true, `key` will not be included in txn conflict detection this time
 * @return TXN_OK for key existed, TXN_KEY_NOT_FOUND for key not found, otherwise for kv error
 */
TxnErrorCode key_exists(Transaction* txn, std::string_view key, bool snapshot = false);

} // namespace doris::cloud
