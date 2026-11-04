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

#include "format_v2/blob/paimon_blob_column_reader.h"

#include <algorithm>
#include <ranges>
#include <utility>

#include "core/data_type/data_type_file.h"
#include "core/pod_array.h"
#include "format_v2/blob/blob_file_reader.h"
#include "io/file_factory.h"
#include "io/fs/tracing_file_reader.h"
#include "io/io_common.h"

namespace doris::format::paimon {

Status PaimonBlobColumnReader::validate_blob_files(std::vector<const TPaimonBlobFileDesc*>* files,
                                                   int64_t anchor_first_row_id,
                                                   int64_t anchor_row_count) {
    if (files->empty()) {
        return Status::InvalidArgument("Cannot validate an empty managed BLOB file group");
    }
    for (const auto* file : *files) {
        if (!file->__isset.path || !file->__isset.file_size || !file->__isset.first_row_id ||
            !file->__isset.row_count || file->path.empty() || file->file_size < 5 ||
            file->row_count <= 0 || file->first_row_id < anchor_first_row_id ||
            file->first_row_id > anchor_first_row_id + anchor_row_count ||
            file->row_count > anchor_first_row_id + anchor_row_count - file->first_row_id) {
            return Status::InvalidArgument("Invalid managed BLOB file descriptor");
        }
    }
    std::ranges::sort(*files, {}, [](const auto* file) { return file->first_row_id; });
    int64_t next = anchor_first_row_id;
    for (const auto* file : *files) {
        if (file->first_row_id != next) {
            return Status::Corruption("BLOB column {} has overlapping or missing row ranges",
                                      file->field_id);
        }
        next += file->row_count;
    }
    if (next != anchor_first_row_id + anchor_row_count) {
        return Status::Corruption("BLOB column {} does not cover its Parquet anchor",
                                  files->front()->field_id);
    }
    return Status::OK();
}

PaimonBlobColumnReader::PaimonBlobColumnReader(const ColumnDefinition& column,
                                               const std::vector<const TPaimonBlobFileDesc*>& files,
                                               bool blob_as_descriptor)
        : _blob_column_name(column.name),
          _blob_column_type(column.type),
          _blob_as_descriptor(blob_as_descriptor) {
    _blob_files.reserve(files.size());
    for (const auto* file : files) {
        _blob_files.push_back(BlobFileState {.blob_metadata = *file, .blob_reader = nullptr});
    }
}

PaimonBlobColumnReader::~PaimonBlobColumnReader() = default;

Status PaimonBlobColumnReader::_open_blob_file(BlobFileState* blob_file,
                                               const ReadContext& context) {
    if (blob_file->blob_reader) {
        return Status::OK();
    }
    io::FileDescription description;
    description.path = blob_file->blob_metadata.path;
    description.file_size = blob_file->blob_metadata.file_size;
    description.is_immutable = true;
    description.fs_name = context.fs_name;
    description.file_cache_admission = context.file_cache_admission;
    auto reader = FileFactory::create_file_reader(
            context.system_properties, description,
            FileFactory::get_reader_options(context.runtime_state, description), context.profile);
    if (!reader.has_value()) {
        return std::move(reader).error();
    }
    if (reader.value()->size() != uint64_t(description.file_size)) {
        return Status::Corruption("BLOB file size differs from its manifest: {}", description.path);
    }
    auto file_reader = std::move(reader).value();
    // Match ordinary file reads so footer, index and payload I/O all reach scanner accounting.
    if (context.io_ctx && context.io_ctx->file_reader_stats) {
        file_reader = std::make_shared<io::TracingFileReader>(std::move(file_reader),
                                                              context.io_ctx->file_reader_stats);
    }
    auto blob = std::make_unique<format::BlobFileReader>();
    RETURN_IF_ERROR(
            blob->open(std::move(file_reader), blob_file->blob_metadata.row_count, context.io_ctx));
    blob_file->blob_reader = std::move(blob);
    return Status::OK();
}

Status PaimonBlobColumnReader::read_blob(const ColumnInt64& anchor_row_positions,
                                         int64_t anchor_first_row_id, const ReadContext& context,
                                         MutableColumnPtr* blob_column_result) {
    auto blob_column = _blob_column_type->create_column();
    blob_column->reserve(anchor_row_positions.size());
    if (_blob_files.empty()) {
        // Schema evolution can leave this field absent for the whole anchor. A partial
        // range is different: validate_blob_files() rejects holes in an existing file group.
        if (!_blob_column_type->is_nullable()) {
            return Status::Corruption("Missing required BLOB column {}", _blob_column_name);
        }
        blob_column->insert_many_defaults(anchor_row_positions.size());
        *blob_column_result = std::move(blob_column);
        return Status::OK();
    }
    // Reuse a tracked buffer within this batch; release its capacity when the batch finishes.
    PODArray<char> payload;
    // The output mode is fixed for this reader, so unused FILE children remain NULL.
    auto blob_file_value = Field::create_field<TYPE_FILE>(File(DataTypeFile::FIELD_COUNT));
    auto& blob_file_fields = blob_file_value.get<TYPE_FILE>();
    BlobFileState* blob_file = nullptr;
    for (const auto anchor_row_position : anchor_row_positions.get_data()) {
        if (context.io_ctx && context.io_ctx->should_stop) {
            return Status::Cancelled("Paimon BLOB read cancelled");
        }
        // Convert the original Parquet file position to a Paimon logical row ID.
        // Example: anchor origin 1000 + file position 4 = logical row 1004.
        const int64_t paimon_row_id = anchor_first_row_id + anchor_row_position;
        // Consecutive selected rows usually share a source. Only search and open when the
        // row leaves its range; retain correctness for non-monotonic row positions too.
        if (blob_file == nullptr || paimon_row_id < blob_file->blob_metadata.first_row_id ||
            paimon_row_id - blob_file->blob_metadata.first_row_id >=
                    blob_file->blob_metadata.row_count) {
            // Validated ranges are contiguous. upper_bound finds the next file's start;
            // its predecessor covers paimon_row_id, including an exact file-start match.
            auto it = std::ranges::upper_bound(_blob_files, paimon_row_id, {},
                                               [](const BlobFileState& blob_file) {
                                                   return blob_file.blob_metadata.first_row_id;
                                               });
            if (it == _blob_files.begin()) {
                return Status::Corruption("BLOB column has no file for row {}", paimon_row_id);
            }
            --it;
            blob_file = &*it;
            RETURN_IF_ERROR(_open_blob_file(blob_file, context));
        }
        std::optional<BlobFileReader::ByteRange> payload_range;
        // Rebase against this .blob file's origin, which can differ from the anchor's.
        // For a .blob starting at 1003, logical row 1004 is file-local record 1.
        const int64_t blob_row_position = paimon_row_id - blob_file->blob_metadata.first_row_id;
        RETURN_IF_ERROR(blob_file->blob_reader->get_range(blob_row_position, &payload_range));
        if (!payload_range) {
            if (!_blob_column_type->is_nullable()) {
                return Status::Corruption("NULL in required BLOB column {}", _blob_column_name);
            }
            blob_column->insert_default();
            continue;
        }
        if (_blob_as_descriptor) {
            blob_file_fields[DataTypeFile::URI_FIELD_INDEX] =
                    Field::create_field<TYPE_STRING>(blob_file->blob_metadata.path);
            blob_file_fields[DataTypeFile::OFFSET_FIELD_INDEX] =
                    Field::create_field<TYPE_BIGINT>(payload_range->offset);
            blob_file_fields[DataTypeFile::SIZE_FIELD_INDEX] =
                    Field::create_field<TYPE_BIGINT>(payload_range->size);
        } else {
            // The index exposes the payload length before any payload buffer allocation or I/O.
            if (payload_range->size > context.blob_max_size) {
                return Status::InvalidArgument(
                        "Paimon BLOB column '{}' inline size {} bytes exceeds "
                        "paimon_blob_max_size={} bytes. Increase paimon_blob_max_size or use "
                        "blob-as-descriptor=true",
                        _blob_column_name, payload_range->size, context.blob_max_size);
            }
            // Inline output contains only the payload; metadata children remain NULL.
            RETURN_IF_ERROR(
                    blob_file->blob_reader->read_payload(*payload_range, &payload, context.io_ctx));
            blob_file_fields[DataTypeFile::INLINE_FIELD_INDEX] =
                    Field::create_field<TYPE_VARBINARY>(
                            StringView(payload.data(), static_cast<uint32_t>(payload.size())));
        }
        // Large StringView values borrow payload's buffer; insert copies the bytes into
        // the FILE column before the next read can resize or overwrite that buffer.
        blob_column->insert(blob_file_value);
    }
    *blob_column_result = std::move(blob_column);
    return Status::OK();
}

} // namespace doris::format::paimon
