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
#include "meta-store/versionstamp.h"

namespace doris::cloud {

// The options for getting a range of versioned value.
struct VersionedRangeGetOptions {
    // Whether to read the snapshot.
    bool snapshot = false;
    // The number of keys returned in the underlying range request.
    int batch_limit = 0; // 0 means no limit
    // The versionstamp to get the document keys.
    Versionstamp snapshot_version = Versionstamp::max();
    // The begin key selector for the range get operation.
    //
    // Since versioned keys are stored as "key_prefix + versionstamp", we need to determine
    // the exact range boundaries. The selector controls which versionstamp is appended
    // to the key prefix to form the complete range boundary key:
    // - LAST_LESS_THAN/FIRST_GREATER_OR_EQUAL: appends Versionstamp::min() (earliest version)
    // - LAST_LESS_OR_EQUAL/FIRST_GREATER_THAN: appends Versionstamp::max() (latest version)
    RangeKeySelector begin_key_selector = RangeKeySelector::FIRST_GREATER_THAN;
    // The end key selector for the range get operation.
    //
    // Works the same way as begin_key_selector, determining the end boundary of the range
    // by appending the appropriate versionstamp to the end key prefix.
    RangeKeySelector end_key_selector = RangeKeySelector::FIRST_GREATER_OR_EQUAL;
};

// Get a range of versioned value from the transaction.
//
// The key values are returned in reverse order, meaning the bigger key is returned first.
class VersionedRangeGetIterator {
public:
    using Element = std::tuple<std::string_view, Versionstamp, std::string_view>;

    VersionedRangeGetIterator(std::unique_ptr<FullRangeGetIterator> iter,
                              Versionstamp snapshot_version)
            : snapshot_version_(snapshot_version), iter_(std::move(iter)) {}

    VersionedRangeGetIterator(const VersionedRangeGetIterator&) = delete;
    VersionedRangeGetIterator& operator=(const VersionedRangeGetIterator&) = delete;
    VersionedRangeGetIterator(VersionedRangeGetIterator&& other) noexcept
            : snapshot_version_(other.snapshot_version_),
              error_code_(other.error_code_),
              has_find_(other.has_find_),
              current_version_(other.current_version_),
              current_key_(other.current_key_),
              current_value_(other.current_value_),
              iter_(std::move(other.iter_)) {}
    VersionedRangeGetIterator& operator=(VersionedRangeGetIterator&& other) noexcept {
        if (this != &other) {
            iter_ = std::move(other.iter_);
            error_code_ = other.error_code_;
            has_find_ = other.has_find_;
            current_version_ = other.current_version_;
            current_key_ = other.current_key_;
            current_value_ = other.current_value_;
            snapshot_version_ = other.snapshot_version_;
        }
        return *this;
    }

    bool has_next();

    bool is_valid() const { return error_code_ == TxnErrorCode::TXN_OK && iter_->is_valid(); }

    TxnErrorCode error_code() const {
        return error_code_ == TxnErrorCode::TXN_OK ? iter_->error_code() : error_code_;
    }

    std::optional<Element> next();

    std::optional<Element> peek();

private:
    std::tuple<std::string_view, Versionstamp> parse_key(std::string_view key);

    Versionstamp snapshot_version_;

    TxnErrorCode error_code_ = TxnErrorCode::TXN_OK;
    bool has_find_ = false; // Whether we have found a valid key in the current iteration.
    Versionstamp current_version_;
    std::string_view current_key_;
    std::string_view current_value_;
    std::unique_ptr<FullRangeGetIterator> iter_;
};

// Get a range of versioned value from the transaction.
//
// This function performs a range query on versioned documents stored with key format
// "key_prefix + versionstamp". It returns an iterator that can traverse through
// all matching documents within the specified key range (in reverse order).
//
// It returns a iterator. The iterator returns documents in reverse order (latest first)
// and automatically filters out documents newer than the snapshot_version.
std::unique_ptr<VersionedRangeGetIterator> versioned_get_range(
        Transaction* txn, std::string_view begin, std::string_view end,
        const VersionedRangeGetOptions& opts);

// Remove a versioned document from the transaction by key prefix and versionstamp.
void versioned_remove(Transaction* txn, std::string_view key_prefix, Versionstamp v);

// Remove a versioned document from the transaction by key.
void versioned_remove(Transaction* txn, std::string_view key);

// Put a versioned value into the transaction with a specific versionstamp.
bool versioned_put(Transaction* txn, std::string_view key_prefix, Versionstamp v,
                   std::string_view value);

// Put a versioned value, the versionstamp will be generated automatically.
bool versioned_put(Transaction* txn, std::string_view key_prefix, std::string_view);

// Get a versioned value from the transaction by key prefix and versionstamp.
//
// The versionstamp of the document will be returned in `v`.
TxnErrorCode versioned_get(Transaction* txn, std::string_view key_prefix,
                           Versionstamp snapshot_version, Versionstamp* v, std::string* value,
                           bool snapshot = true);

// Get a versioned value from the transaction by key prefix and the latest versionstamp.
static inline TxnErrorCode versioned_get(Transaction* txn, std::string_view key_prefix,
                                         Versionstamp* v, std::string* value,
                                         bool snapshot = true) {
    return versioned_get(txn, key_prefix, Versionstamp::max(), v, value, snapshot);
}

} // namespace doris::cloud
