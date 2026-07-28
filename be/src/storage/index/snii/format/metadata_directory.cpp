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

#include "storage/index/snii/format/metadata_directory.h"

#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "gen_cpp/snii.pb.h"

namespace doris::snii::format {
namespace {

Status corrupted(std::string_view message) {
    return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(message);
}

Status unsupported(std::string_view message) {
    return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>(message);
}

Status decode_blob_ref(const doris::snii::SniiBlobRefPB& input, MetadataBlobRef* out) {
    if (!input.has_offset() || !input.has_length()) {
        return corrupted("metadata directory: missing blob reference field");
    }
    if (input.length() == 0) {
        return corrupted("metadata directory: empty mandatory blob reference");
    }
    *out = {.offset = input.offset(), .length = input.length()};
    return Status::OK();
}

Status decode_directory_pb(const doris::snii::SniiMetadataDirectoryPB& input,
                           std::vector<LogicalIndexMetadataRef>* out) {
    if (input.required_features_size() != 0) {
        return unsupported("metadata directory: required feature is not supported");
    }

    std::vector<LogicalIndexMetadataRef> entries;
    entries.reserve(input.indexes_size());
    for (const auto& index : input.indexes()) {
        if (!index.has_index_id() || !index.has_index_suffix() || !index.has_core_metadata() ||
            !index.has_sampled_term_index() || !index.has_dict_block_directory()) {
            return corrupted("metadata directory: missing required logical field");
        }

        LogicalIndexMetadataRef entry;
        entry.index_id = index.index_id();
        entry.index_suffix = index.index_suffix();
        RETURN_IF_ERROR(decode_blob_ref(index.core_metadata(), &entry.core_metadata));
        RETURN_IF_ERROR(decode_blob_ref(index.sampled_term_index(), &entry.sampled_term_index));
        RETURN_IF_ERROR(decode_blob_ref(index.dict_block_directory(), &entry.dict_block_directory));

        for (const auto& existing : entries) {
            if (existing.index_id == entry.index_id &&
                existing.index_suffix == entry.index_suffix) {
                return corrupted("metadata directory: duplicate logical index key");
            }
        }
        entries.push_back(std::move(entry));
    }
    *out = std::move(entries);
    return Status::OK();
}

void encode_blob_ref(const MetadataBlobRef& input, doris::snii::SniiBlobRefPB* out) {
    out->set_offset(input.offset);
    out->set_length(input.length);
}

} // namespace

Status MetadataDirectory::decode(Slice bytes, MetadataDirectory* out) {
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("metadata directory: null output");
    }
    out->entries_.clear();
    if (bytes.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
        return corrupted("metadata directory: protobuf payload exceeds INT_MAX");
    }

    doris::snii::SniiMetadataDirectoryPB directory;
    if (!directory.ParseFromArray(bytes.data(), static_cast<int>(bytes.size()))) {
        return corrupted("metadata directory: protobuf parsing failed");
    }

    std::vector<LogicalIndexMetadataRef> entries;
    RETURN_IF_ERROR(decode_directory_pb(directory, &entries));
    out->entries_ = std::move(entries);
    return Status::OK();
}

const LogicalIndexMetadataRef* MetadataDirectory::find(uint64_t index_id,
                                                       std::string_view suffix) const {
    for (const auto& entry : entries_) {
        if (entry.index_id == index_id && entry.index_suffix == suffix) {
            return &entry;
        }
    }
    return nullptr;
}

Status encode_metadata_directory(const std::vector<LogicalIndexMetadataRef>& entries,
                                 ByteSink* out) {
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("metadata directory: null output");
    }

    doris::snii::SniiMetadataDirectoryPB directory;
    for (const auto& entry : entries) {
        auto* index = directory.add_indexes();
        index->set_index_id(entry.index_id);
        index->set_index_suffix(entry.index_suffix);
        encode_blob_ref(entry.core_metadata, index->mutable_core_metadata());
        encode_blob_ref(entry.sampled_term_index, index->mutable_sampled_term_index());
        encode_blob_ref(entry.dict_block_directory, index->mutable_dict_block_directory());
    }

    std::vector<LogicalIndexMetadataRef> validated;
    RETURN_IF_ERROR(decode_directory_pb(directory, &validated));
    const size_t size = directory.ByteSizeLong();
    if (size > static_cast<size_t>(std::numeric_limits<int>::max())) {
        return corrupted("metadata directory: protobuf payload exceeds INT_MAX");
    }
    std::string payload(size, '\0');
    if (!directory.SerializeToArray(payload.data(), static_cast<int>(size))) {
        return corrupted("metadata directory: protobuf serialization failed");
    }
    out->put_bytes(Slice(payload));
    return Status::OK();
}

} // namespace doris::snii::format
