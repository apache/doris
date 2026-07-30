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

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "gen_cpp/snii.pb.h"
#include "storage/index/snii/format/format_constants.h"

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

// Decodes one opaque blob sub-file. Unlike decode_blob_ref, length == 0 is
// LEGAL here: an empty BKD segment stores 0-byte `bkd` / `bkd_index` files.
Status decode_named_blob(const doris::snii::SniiNamedBlobPB& input, NamedBlobFileRef* out) {
    if (!input.has_name() || !input.has_offset() || !input.has_length() || !input.has_crc32c()) {
        return corrupted("metadata directory: missing named blob field");
    }
    if (input.name().empty()) {
        return corrupted("metadata directory: empty blob file name");
    }
    if (input.length() > std::numeric_limits<uint64_t>::max() - input.offset()) {
        return corrupted("metadata directory: blob file range overflows");
    }
    out->name = input.name();
    out->offset = input.offset();
    out->length = input.length();
    out->crc32c = input.crc32c();
    return Status::OK();
}

// Rows 1/2 of the decode matrix: the original three-blob contract, unchanged.
Status decode_inverted_entry(const doris::snii::SniiLogicalIndexMetadataPB& index,
                             LogicalIndexMetadataRef* entry) {
    if (!index.has_core_metadata() || !index.has_sampled_term_index() ||
        !index.has_dict_block_directory()) {
        return corrupted("metadata directory: missing required logical field");
    }
    if (index.files_size() != 0) {
        return corrupted("metadata directory: inverted entry carries blob files");
    }
    entry->kind = LogicalIndexKind::kInverted;
    RETURN_IF_ERROR(decode_blob_ref(index.core_metadata(), &entry->core_metadata));
    RETURN_IF_ERROR(decode_blob_ref(index.sampled_term_index(), &entry->sampled_term_index));
    RETURN_IF_ERROR(decode_blob_ref(index.dict_block_directory(), &entry->dict_block_directory));
    return Status::OK();
}

// Rows 3/4 of the decode matrix: an opaque named-file table and nothing else.
Status decode_blob_entry(const doris::snii::SniiLogicalIndexMetadataPB& index, uint32_t kind_value,
                         LogicalIndexMetadataRef* entry) {
    if (index.has_core_metadata() || index.has_sampled_term_index() ||
        index.has_dict_block_directory()) {
        return corrupted("metadata directory: blob entry carries inverted metadata");
    }
    if (index.files_size() == 0) {
        return corrupted("metadata directory: blob entry has no files");
    }
    entry->kind = static_cast<LogicalIndexKind>(kind_value);
    entry->files.reserve(index.files_size());
    for (const auto& file : index.files()) {
        NamedBlobFileRef decoded;
        RETURN_IF_ERROR(decode_named_blob(file, &decoded));
        for (const auto& existing : entry->files) {
            if (existing.name == decoded.name) {
                return corrupted("metadata directory: duplicate blob file name");
            }
        }
        entry->files.push_back(std::move(decoded));
    }
    return Status::OK();
}

// The decode validation matrix (design 2026-07-28 §3.1). Shared by the reader
// and by the encoder's self-check, so it constrains both sides at once.
Status decode_directory_pb(const doris::snii::SniiMetadataDirectoryPB& input,
                           std::vector<LogicalIndexMetadataRef>* out) {
    // Row 0a/0b: whitelist of known required features; anything else is a
    // format this binary does not understand.
    bool feature_blob = false;
    for (const uint32_t feature : input.required_features()) {
        if (feature == kFeatureBlobLogicalIndex) {
            feature_blob = true;
            continue;
        }
        return unsupported("metadata directory: required feature is not supported");
    }

    bool has_blob_entry = false;
    std::vector<LogicalIndexMetadataRef> entries;
    entries.reserve(input.indexes_size());
    for (const auto& index : input.indexes()) {
        if (!index.has_index_id() || !index.has_index_suffix()) {
            return corrupted("metadata directory: missing required logical field");
        }

        LogicalIndexMetadataRef entry;
        entry.index_id = index.index_id();
        entry.index_suffix = index.index_suffix();

        const uint32_t kind_value = index.has_kind()
                                            ? index.kind()
                                            : static_cast<uint32_t>(LogicalIndexKind::kInverted);
        switch (kind_value) {
        case static_cast<uint32_t>(LogicalIndexKind::kInverted):
            RETURN_IF_ERROR(decode_inverted_entry(index, &entry));
            break;
        case static_cast<uint32_t>(LogicalIndexKind::kBkd):
        case static_cast<uint32_t>(LogicalIndexKind::kAnn):
            has_blob_entry = true;
            RETURN_IF_ERROR(decode_blob_entry(index, kind_value, &entry));
            break;
        default:
            // Row 5: a kind this binary does not know how to open.
            return unsupported("metadata directory: unknown logical index kind");
        }

        // Row 6: keys are unique across kinds.
        for (const auto& existing : entries) {
            if (existing.index_id == entry.index_id &&
                existing.index_suffix == entry.index_suffix) {
                return corrupted("metadata directory: duplicate logical index key");
            }
        }
        entries.push_back(std::move(entry));
    }

    // The feature flag and the entries must agree in BOTH directions: a blob
    // entry without the flag would make old binaries misreport Corruption
    // instead of Unsupported; a flag without blob entries would make every
    // pre-blob binary reject a file it could actually read.
    if (has_blob_entry != feature_blob) {
        return corrupted("metadata directory: blob feature flag disagrees with entries");
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
    bool any_blob = false;
    for (const auto& entry : entries) {
        auto* index = directory.add_indexes();
        index->set_index_id(entry.index_id);
        index->set_index_suffix(entry.index_suffix);
        if (entry.kind == LogicalIndexKind::kInverted) {
            // BYTE GATE: never touch field 6/7 here -- proto2 presence
            // semantics would serialize even set_kind(0) and change the bytes
            // of every pure-text directory (golden digests included).
            if (!entry.files.empty()) {
                return corrupted("metadata directory: inverted entry carries blob files");
            }
            encode_blob_ref(entry.core_metadata, index->mutable_core_metadata());
            encode_blob_ref(entry.sampled_term_index, index->mutable_sampled_term_index());
            encode_blob_ref(entry.dict_block_directory, index->mutable_dict_block_directory());
        } else {
            // The three inverted refs do not serialize for blob entries, so
            // the shared decode self-check below could not catch a caller that
            // filled them; reject that in-memory shape here.
            if (entry.core_metadata.offset != 0 || entry.core_metadata.length != 0 ||
                entry.sampled_term_index.offset != 0 || entry.sampled_term_index.length != 0 ||
                entry.dict_block_directory.offset != 0 || entry.dict_block_directory.length != 0) {
                return corrupted("metadata directory: blob entry carries inverted metadata");
            }
            any_blob = true;
            index->set_kind(static_cast<uint32_t>(entry.kind));
            for (const auto& file : entry.files) {
                auto* named = index->add_files();
                named->set_name(file.name);
                named->set_offset(file.offset);
                named->set_length(file.length);
                named->set_crc32c(file.crc32c);
            }
        }
    }
    if (any_blob) {
        directory.add_required_features(kFeatureBlobLogicalIndex);
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
