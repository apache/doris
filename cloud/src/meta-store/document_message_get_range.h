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

#include <glog/logging.h>

#include "common/util.h"
#include "meta-store/codec.h"
#include "meta-store/document_message.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versionstamp.h"

namespace doris::cloud::versioned {

// Options for reading document messages from a transaction.
//
// The default range is (begin_key, end_key].
struct ReadDocumentMessagesOptions {
    // Whether to execute snapshot read?
    bool snapshot = false;
    // The maximum number of keys to return in a single batch.
    int batch_limit = 0;
    // The versionstamp to use for the snapshot read.
    // If this is set to Versionstamp::max(), it will read the latest version.
    // Otherwise, it will read the version before this versionstamp.
    Versionstamp snapshot_version = Versionstamp::max();
    // Whether to exclude the begin key from the result range.
    bool exclude_begin_key = true;
    // Whether to exclude the end key from the result range.
    bool exclude_end_key = false;
};

template <typename Message>
    requires IsProtobufMessage<Message>
class DocumentMessageIterator {
public:
    using Element = std::tuple<std::string_view, Versionstamp, Message&>;
    DocumentMessageIterator(std::unique_ptr<FullRangeGetIterator> iter,
                            Versionstamp snapshot_version)
            : snapshot_version_(snapshot_version), iter_(std::move(iter)) {}
    DocumentMessageIterator(const DocumentMessageIterator&) = delete;
    DocumentMessageIterator& operator=(const DocumentMessageIterator&) = delete;
    DocumentMessageIterator(DocumentMessageIterator&& other)
            : snapshot_version_(other.snapshot_version_),
              error_code_(other.error_code_),
              has_find_(other.has_find_),
              current_key_and_version_(std::move(other.current_key_and_version_)),
              current_key_(other.current_key_),
              current_version_(other.current_version_),
              current_value_(std::move(other.current_value_)),
              iter_(std::move(other.iter_)) {}
    DocumentMessageIterator& operator=(DocumentMessageIterator&& other) {
        if (this != &other) {
            snapshot_version_ = other.snapshot_version_;
            has_find_ = other.has_find_;
            current_key_ = other.current_key_;
            error_code_ = other.error_code_;
            current_key_and_version_ = std::move(other.current_key_and_version_);
            current_version_ = other.current_version_;
            current_value_ = std::move(other.current_value_);
            iter_ = std::move(other.iter_);
        }
        return *this;
    }

    TxnErrorCode error_code() const {
        return error_code_ == TxnErrorCode::TXN_OK ? iter_->error_code() : error_code_;
    }

    bool is_valid() const { return iter_->is_valid() && error_code_ == TxnErrorCode::TXN_OK; }
    bool has_next();

    std::optional<Element> next();
    std::optional<Element> peek();

private:
    Versionstamp snapshot_version_;

    TxnErrorCode error_code_ = TxnErrorCode::TXN_OK;
    bool has_find_ = false;
    std::string current_key_and_version_;
    std::string_view current_key_;
    Versionstamp current_version_;
    Message current_value_;
    std::unique_ptr<FullRangeGetIterator> iter_;
};

template <typename Message>
    requires IsProtobufMessage<Message>
bool DocumentMessageIterator<Message>::has_next() {
    while (is_valid() && !has_find_ && iter_->has_next()) {
        auto [key, value] = iter_->peek().value();
        if (!current_key_and_version_.empty() && key.starts_with(current_key_and_version_)) {
            // Skip keys that are part of the current document
            iter_->next();
            continue;
        }

        // Parse the key to extract the document key prefix and versionstamp
        std::string_view document_key = key;
        Versionstamp version;

        // Decode the versionstamp from the end of the key
        if (decode_tailing_versionstamp_end(&document_key) ||
            decode_tailing_versionstamp(&document_key, &version)) {
            LOG(ERROR) << "Failed to decode versionstamp from key: " << hex(key);
            error_code_ = TxnErrorCode::TXN_INVALID_DATA;
            return false; // Stop iteration on error
        }

        // Save the current key and version for filtering the split keys
        current_key_and_version_ = document_key;
        encode_versionstamp(version, &current_key_and_version_);

        // Filter out documents newer than snapshot version or duplicate keys
        if (snapshot_version_ <= version || document_key == current_key_) {
            iter_->next(); // Skip this version
            continue;
        }

        // Found a valid document, reconstruct it
        Message msg;
        TxnErrorCode code = document_get_versioned_children(iter_.get(), &msg);
        if (code != TxnErrorCode::TXN_OK) {
            LOG(ERROR) << "Failed to get versioned document for key: " << hex(document_key)
                       << ", error code: " << code;
            error_code_ = code;
            return false;
        }

        // Successfully found and reconstructed a document
        has_find_ = true;
        current_key_ = document_key;
        current_version_ = version;
        current_value_ = std::move(msg);
        return true;
    }

    return is_valid() && has_find_;
}

template <typename Message>
    requires IsProtobufMessage<Message>
std::optional<typename DocumentMessageIterator<Message>::Element>
DocumentMessageIterator<Message>::next() {
    if (!has_next()) {
        return std::nullopt;
    }

    has_find_ = false; // Reset for next call
    return std::make_tuple(current_key_, current_version_, std::ref(current_value_));
}

template <typename Message>
    requires IsProtobufMessage<Message>
std::optional<typename DocumentMessageIterator<Message>::Element>
DocumentMessageIterator<Message>::peek() {
    if (!has_next()) {
        return std::nullopt;
    }

    return std::make_tuple(current_key_, current_version_, std::ref(current_value_));
}

// Read the specified document messages from the transaction by key range.
//
// The iterator will return the messages in the specified range in reversed order, and it will reconstruct
// the messages using the `split_schema` field of the message if it is split into multiple keys.
//
// The range is specified by the `begin` and `end` keys, which are expected to be the document keys.
// The `begin` key is inclusive, and the `end` key is exclusive by default, caller can change this
// behaviour by setting the range key selector in the options.
//
// ATTN: document keys are expected, rather than document key prefixes!
template <typename Message>
    requires IsProtobufMessage<Message>
std::unique_ptr<DocumentMessageIterator<Message>> document_get_range(
        Transaction* txn, std::string_view begin, std::string_view end,
        const ReadDocumentMessagesOptions& options) {
    std::string begin_key(begin);
    std::string end_key(end);
    if (options.exclude_begin_key) {
        encode_versionstamp(Versionstamp::max(), &begin_key);
    } else {
        encode_versionstamp(Versionstamp::min(), &begin_key);
    }
    if (options.exclude_end_key) { // the end key is exclusive by default in fdb range get.
        encode_versionstamp(Versionstamp::min(), &end_key);
    } else {
        encode_versionstamp(Versionstamp::max(), &end_key);
    }

    // Set up FullRangeGetOptions
    FullRangeGetOptions range_options;
    range_options.reverse = true; // Get latest versions first
    range_options.prefetch = true;
    range_options.begin_key_selector = RangeKeySelector::FIRST_GREATER_THAN;
    range_options.end_key_selector = RangeKeySelector::FIRST_GREATER_THAN;
    range_options.snapshot = options.snapshot;
    range_options.batch_limit = options.batch_limit;

    // Create the underlying range iterator
    std::unique_ptr<FullRangeGetIterator> iter =
            txn->full_range_get(begin_key, end_key, std::move(range_options));

    // Return the document message iterator
    return std::make_unique<DocumentMessageIterator<Message>>(std::move(iter),
                                                              options.snapshot_version);
}

} // namespace doris::cloud::versioned
