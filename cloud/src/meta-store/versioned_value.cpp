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

#include "meta-store/versioned_value.h"

#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "common/util.h"
#include "meta-store/codec.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"

namespace doris::cloud {

bool VersionedRangeGetIterator::has_next() {
    while (is_valid() && !has_find_ && iter_->has_next()) {
        auto [key, value] = iter_->peek().value();
        auto [parsed_key, version] = parse_key(key);
        if (error_code_ != TxnErrorCode::TXN_OK) {
            return false; // Error occurred while parsing the key
        } else if (snapshot_version_ <= version || parsed_key == current_key_) {
            // Filter out keys that are older than the snapshot version
            // or the same as the current key (to avoid duplicates)
            iter_->next(); // Move to the next key
            continue;
        }

        // Find the next valid key that is not older than the snapshot version
        has_find_ = true;
        current_key_ = parsed_key;
        current_version_ = version;
        current_value_ = value;
        return true;
    }
    return is_valid() && has_find_;
}

std::optional<VersionedRangeGetIterator::Element> VersionedRangeGetIterator::next() {
    if (!has_next()) {
        return std::nullopt;
    }

    has_find_ = false; // Reset for the next call
    // The current_key_ and current_version_ are already set by has_next()
    return std::make_tuple(current_key_, current_version_, current_value_);
}

std::optional<VersionedRangeGetIterator::Element> VersionedRangeGetIterator::peek() {
    if (!has_next()) {
        return std::nullopt;
    }

    // The current_key_ and current_version_ are already set by has_next()
    return std::make_tuple(current_key_, current_version_, current_value_);
}

std::tuple<std::string_view, Versionstamp> VersionedRangeGetIterator::parse_key(
        std::string_view key) {
    Versionstamp version;
    if (decode_tailing_versionstamp_end(&key) || decode_tailing_versionstamp(&key, &version)) {
        LOG(ERROR) << "Failed to decode tailing versionstamp from key: " << hex(key);
        error_code_ = TxnErrorCode::TXN_INVALID_DATA;
        return {key, Versionstamp::min()};
    }
    return {key, version};
}

bool versioned_put(Transaction* txn, std::string_view key, std::string_view value) {
    std::string key_with_versionstamp(key);
    uint32_t offset = encode_versionstamp(Versionstamp::min(), &key_with_versionstamp);
    encode_versionstamp_end(&key_with_versionstamp);
    txn->atomic_set_ver_key(key_with_versionstamp, offset, value);
    return true;
}

bool versioned_put(Transaction* txn, std::string_view key, Versionstamp v, std::string_view value) {
    std::string key_with_versionstamp(key);
    encode_versionstamp(v, &key_with_versionstamp);
    encode_versionstamp_end(&key_with_versionstamp);
    txn->put(key_with_versionstamp, value);
    return true;
}

TxnErrorCode versioned_get(Transaction* txn, std::string_view key, Versionstamp snapshot_version,
                           Versionstamp* value_version, std::string* value, bool snapshot) {
    std::string end_key(key);
    encode_versionstamp(snapshot_version, &end_key);

    // Range [0, v)
    FullRangeGetOptions options;
    options.exact_limit = 1; // We expect only one key for the given versionstamp
    options.reverse = true;  // Get the latest version first
    options.snapshot = snapshot;
    options.begin_key_selector = RangeKeySelector::FIRST_GREATER_THAN;
    options.end_key_selector = RangeKeySelector::FIRST_GREATER_OR_EQUAL;

    std::unique_ptr<FullRangeGetIterator> iter =
            txn->full_range_get(key, end_key, std::move(options));
    auto&& item = iter->next();
    if (!item.has_value() && !iter->is_valid()) {
        LOG(ERROR) << "versioned document get failed, key: " << hex(key)
                   << ", version: " << snapshot_version.to_string()
                   << ", error code: " << iter->error_code();
        return iter->error_code();
    } else if (!item.has_value()) {
        return TxnErrorCode::TXN_KEY_NOT_FOUND;
    }

    std::string_view actual_key = item->first;
    Versionstamp key_version;
    if (decode_tailing_versionstamp_end(&actual_key) ||
        decode_tailing_versionstamp(&actual_key, &key_version)) {
        LOG(ERROR) << "Failed to decode tailing versionstamp from key: " << hex(actual_key);
        return TxnErrorCode::TXN_INVALID_DATA;
    }

    if (value) {
        *value = item->second;
    }
    if (value_version) {
        *value_version = key_version;
    }
    return TxnErrorCode::TXN_OK;
}

std::unique_ptr<VersionedRangeGetIterator> versioned_get_range(
        Transaction* txn, std::string_view begin, std::string_view end,
        const VersionedRangeGetOptions& opts) {
    auto apply_key_selector = [](std::string& key, const RangeKeySelector& selector) {
        switch (selector) {
        case RangeKeySelector::LAST_LESS_THAN:
        case RangeKeySelector::FIRST_GREATER_OR_EQUAL:
            encode_versionstamp(Versionstamp::min(), &key);
            break;
        case RangeKeySelector::LAST_LESS_OR_EQUAL:
        case RangeKeySelector::FIRST_GREATER_THAN:
            encode_versionstamp(Versionstamp::max(), &key);
            break;
        }
    };

    std::string begin_key(begin);
    std::string end_key(end);
    apply_key_selector(begin_key, opts.begin_key_selector);
    apply_key_selector(end_key, opts.end_key_selector);

    FullRangeGetOptions options;
    options.reverse = true; // Get the latest version first
    options.begin_key_selector = opts.begin_key_selector;
    options.end_key_selector = opts.end_key_selector;
    options.snapshot = opts.snapshot;
    options.batch_limit = opts.batch_limit;

    std::unique_ptr<FullRangeGetIterator> iter =
            txn->full_range_get(begin_key, end_key, std::move(options));
    return std::make_unique<VersionedRangeGetIterator>(std::move(iter), opts.snapshot_version);
}

void versioned_remove(Transaction* txn, std::string_view key, Versionstamp v) {
    std::string key_with_versionstamp(key);
    encode_versionstamp(v, &key_with_versionstamp);
    encode_versionstamp_end(&key_with_versionstamp);
    txn->remove(key_with_versionstamp);
}

} // namespace doris::cloud
