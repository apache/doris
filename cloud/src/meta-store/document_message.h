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

#include <gen_cpp/olap_file.pb.h>
#include <google/protobuf/message.h>

#include "gen_cpp/cloud.pb.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"

namespace doris::cloud {

namespace details {
void document_delete_single(Transaction* txn, std::string_view key);
void document_delete_range(Transaction* txn, std::string_view prefix);
} // namespace details

template <typename Message>
concept IsProtobufMessage = std::is_base_of_v<google::protobuf::Message, Message> &&
                            std::is_same_v<std::remove_cv_t<Message>, Message>;

// Put a document message into the transaction, returns true on success, otherwise false.
//
// The document put operation will split some messages into multiple keys, such as RowsetMetaCloudPB.
// The split fields will be stored in the `split_schema` field of the message.
// If the message does not have a split schema, it will be stored as a single key.
//
// Only the repeated fields or message fields that are splitable.
bool document_put(Transaction* txn, std::string_view key, google::protobuf::Message&& msg);

// Get a message from the transaction by key.
//
// If the message is split into multiple keys, it will be reconstructed using the `split_schema
// field of the message.
TxnErrorCode document_get(Transaction* txn, std::string_view key, google::protobuf::Message* msg,
                          bool snapshot = false);

// Get a message from the iterator, which is used for range queries.
//
// The iterator should be initialized with a range that matches the key prefix of the message.
// If the message is split into multiple keys, it will be reconstructed using the `split_schema`
// field of the message.
TxnErrorCode document_get(FullRangeGetIterator* iter, google::protobuf::Message* msg);

// Remove a document message from the transaction by key.
// If the message is split into multiple keys, it will remove all keys that match the prefix
// of the message key.
template <typename Message>
    requires IsProtobufMessage<Message>
void document_remove(Transaction* txn, std::string_view key) {
    if constexpr (std::is_same_v<Message, RowsetMetaCloudPB> ||
                  std::is_same_v<Message, SplitSingleMessagePB>) {
        details::document_delete_range(txn, key);
    } else {
        details::document_delete_single(txn, key);
    }
}

} // namespace doris::cloud
