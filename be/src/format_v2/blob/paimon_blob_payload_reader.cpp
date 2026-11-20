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

#include "format_v2/blob/paimon_blob_payload_reader.h"

#include "format_v2/blob/paimon_blob_reference.h"
#include "io/file_factory.h"
#include "io/fs/tracing_file_reader.h"
#include "io/io_common.h"

namespace doris::format::paimon {

Result<io::FileReaderSPtr> PaimonBlobPayloadReader::open_file(
        const std::string& path, int64_t file_size, const PaimonBlobReadContext& read_context) {
    io::FileDescription file_description;
    file_description.path = path;
    file_description.file_size = file_size;
    file_description.is_immutable = true;
    file_description.fs_name = read_context.fs_name;
    file_description.file_cache_admission = read_context.file_cache_admission;
    auto open_result = FileFactory::create_file_reader(
            read_context.system_properties, file_description,
            FileFactory::get_reader_options(read_context.runtime_state, file_description),
            read_context.profile);
    if (!open_result.has_value()) {
        return unexpected(std::move(open_result).error());
    }
    auto file_reader = std::move(open_result).value();
    if (read_context.io_ctx != nullptr && read_context.io_ctx->file_reader_stats != nullptr) {
        file_reader = std::make_shared<io::TracingFileReader>(
                std::move(file_reader), read_context.io_ctx->file_reader_stats);
    }
    if (file_size >= 0 && file_reader->size() != static_cast<uint64_t>(file_size)) {
        file_reader->collect_profile_before_close();
        return unexpected(Status::Corruption("BLOB file size differs from its manifest: {}", path));
    }
    return file_reader;
}

void PaimonBlobPayloadReader::reset() {
    for (const auto& [uri, file_reader] : _file_readers) {
        file_reader->collect_profile_before_close();
    }
    _file_readers.clear();
}

Status PaimonBlobPayloadReader::read_range(const PaimonBlobReference& reference,
                                           const PaimonBlobReadContext& read_context,
                                           PODArray<char>* payload_buffer) {
    if (read_context.io_ctx != nullptr && read_context.io_ctx->should_stop) {
        return Status::Cancelled("Paimon BLOB read cancelled");
    }
    auto cached_reader_it = _file_readers.find(reference.uri);
    if (cached_reader_it == _file_readers.end()) {
        if (_file_readers.size() >= MAX_OPEN_FILES) {
            _file_readers.begin()->second->collect_profile_before_close();
            _file_readers.erase(_file_readers.begin());
        }
        // A descriptor contains payload length, not the size of its containing file.
        auto open_result = open_file(std::string(reference.uri), -1, read_context);
        if (!open_result.has_value()) {
            return std::move(open_result).error();
        }
        cached_reader_it =
                _file_readers.emplace(std::string(reference.uri), std::move(open_result).value())
                        .first;
    }
    const auto& file_reader = cached_reader_it->second;
    if (uint64_t(reference.offset) > file_reader->size() ||
        uint64_t(reference.length) > file_reader->size() - uint64_t(reference.offset)) {
        return Status::Corruption("Paimon BLOB descriptor range exceeds managed pack size: {}",
                                  reference.uri);
    }
    payload_buffer->resize(static_cast<size_t>(reference.length));
    if (payload_buffer->empty()) {
        return Status::OK();
    }
    size_t bytes_read = 0;
    RETURN_IF_ERROR(file_reader->read_at(static_cast<size_t>(reference.offset),
                                         Slice(payload_buffer->data(), payload_buffer->size()),
                                         &bytes_read, read_context.io_ctx));
    if (bytes_read != payload_buffer->size()) {
        return Status::Corruption("Short read of Paimon BLOB payload: {}", reference.uri);
    }
    return Status::OK();
}

} // namespace doris::format::paimon
