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

#include "index_storage_format_v2.h"

#include "cloud/config.h"
#include "common/cast_set.h"
#include "olap/rowset/segment_v2/index_file_writer.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "util/debug_points.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

FileMetadata::FileMetadata(int64_t id, std::string suffix, std::string file, int64_t off,
                           int64_t len, lucene::store::Directory* dir)
        : index_id(id),
          index_suffix(std::move(suffix)),
          filename(std::move(file)),
          offset(off),
          length(len),
          directory(dir) {}

IndexStorageFormatV2::IndexStorageFormatV2(IndexFileWriter* index_file_writer)
        : IndexStorageFormat(index_file_writer) {}

Status IndexStorageFormatV2::write() {
    std::unique_ptr<lucene::store::Directory, DirectoryDeleter> out_dir = nullptr;
    std::unique_ptr<lucene::store::IndexOutput> compound_file_output = nullptr;
    ErrorContext error_context;
    try {
        // Calculate header length and initialize offset
        int64_t current_offset = header_length();

        // Prepare file metadata
        std::vector<FileMetadata> file_metadata;
        MetaFileRange meta_range;
        prepare_file_metadata(current_offset, file_metadata, meta_range);

        // Create output stream
        auto result = create_output_stream();
        out_dir = std::move(result.first);
        compound_file_output = std::move(result.second);
        VLOG_DEBUG << fmt::format("Output compound index file to streams: {}", out_dir->toString());

        // Write version and number of indices
        write_version_and_indices_count(compound_file_output.get());

        // Write index headers and file metadata
        write_index_headers_and_metadata(compound_file_output.get(), file_metadata);

        // Copy file data
        copy_files_data(compound_file_output.get(), file_metadata);

        _index_file_writer->_total_file_size = compound_file_output->getFilePointer();
        _index_file_writer->_file_info.set_index_size(_index_file_writer->_total_file_size);
    } catch (CLuceneError& err) {
        error_context.eptr = std::current_exception();
        auto index_path = InvertedIndexDescriptor::get_index_file_path_v2(
                _index_file_writer->_index_path_prefix);
        error_context.err_msg.append("CLuceneError occur when close idx file: ");
        error_context.err_msg.append(index_path);
        error_context.err_msg.append(", error msg: ");
        error_context.err_msg.append(err.what());
        LOG(ERROR) << error_context.err_msg;
    }
    FINALLY({
        FINALLY_CLOSE(compound_file_output);
        FINALLY_CLOSE(out_dir);
    })

    return Status::OK();
}

int64_t IndexStorageFormatV2::header_length() {
    int64_t header_size = 0;
    header_size +=
            sizeof(int32_t) * 2; // Account for the size of the version number and number of indices

    for (const auto& entry : _index_file_writer->_indices_dirs) {
        const auto& suffix = entry.first.second;
        header_size += sizeof(int64_t); // index id
        header_size += sizeof(int32_t); // index suffix name size
        header_size += suffix.length(); // index suffix name
        header_size += sizeof(int32_t); // index file count
        const auto& dir = entry.second;
        std::vector<std::string> files;
        dir->list(&files);

        for (const auto& file : files) {
            header_size += sizeof(int32_t); // file name size
            header_size += file.length();   // file name
            header_size += sizeof(int64_t); // file offset
            header_size += sizeof(int64_t); // file size
        }
    }
    return header_size;
}

void IndexStorageFormatV2::prepare_file_metadata(int64_t& current_offset,
                                                 std::vector<FileMetadata>& file_metadata,
                                                 MetaFileRange& meta_range) {
    std::vector<FileMetadata> meta_files;
    std::vector<FileMetadata> normal_files;
    for (const auto& entry : _index_file_writer->_indices_dirs) {
        const int64_t index_id = entry.first.first;
        const auto& index_suffix = entry.first.second;
        auto* dir = entry.second.get();

        auto sorted_files = prepare_sorted_files(dir);

        for (const auto& file : sorted_files) {
            bool is_meta = false;

            for (const auto& file_info : InvertedIndexDescriptor::index_file_info_map) {
                if (file.filename.find(file_info.first) != std::string::npos) {
                    meta_files.emplace_back(index_id, index_suffix, file.filename, 0, file.filesize,
                                            dir);
                    is_meta = true;
                    break;
                }
            }

            if (!is_meta) {
                normal_files.emplace_back(index_id, index_suffix, file.filename, 0, file.filesize,
                                          dir);
            }
        }
    }

    file_metadata.reserve(meta_files.size() + normal_files.size());

    // meta file
    meta_range.start_offset = current_offset;
    for (auto& entry : meta_files) {
        entry.offset = current_offset;
        file_metadata.emplace_back(std::move(entry));
        current_offset += entry.length;
    }
    meta_range.end_offset = current_offset;

    // normal file
    for (auto& entry : normal_files) {
        entry.offset = current_offset;
        file_metadata.emplace_back(std::move(entry));
        current_offset += entry.length;
    }

    DBUG_EXECUTE_IF("CSIndexInput.readInternal", {
        bool is_meta_file = true;
        for (const auto& entry : file_metadata) {
            bool is_meta = false;
            for (const auto& map_entry : InvertedIndexDescriptor::index_file_info_map) {
                if (entry.filename.find(map_entry.first) != std::string::npos) {
                    is_meta = true;
                    break;
                }
            }

            if (is_meta_file && !is_meta) {
                is_meta_file = false;
            } else if (!is_meta_file && is_meta) {
                throw Exception(ErrorCode::INTERNAL_ERROR,
                                "Invalid file order: meta files must be at the beginning of the "
                                "file_metadata structure.");
            }
        }
    });
}

std::pair<std::unique_ptr<lucene::store::Directory, DirectoryDeleter>,
          std::unique_ptr<lucene::store::IndexOutput>>
IndexStorageFormatV2::create_output_stream() {
    io::Path index_path {InvertedIndexDescriptor::get_index_file_path_v2(
            _index_file_writer->_index_path_prefix)};

    auto* out_dir = DorisFSDirectoryFactory::getDirectory(_index_file_writer->_fs,
                                                          index_path.parent_path().c_str());
    out_dir->set_file_writer_opts(_index_file_writer->_opts);
    std::unique_ptr<lucene::store::Directory, DirectoryDeleter> out_dir_ptr(out_dir);

    DCHECK(_index_file_writer->_idx_v2_writer != nullptr)
            << "inverted index file writer v2 is nullptr";
    auto compound_file_output = out_dir->createOutputV2(_index_file_writer->_idx_v2_writer.get());
    return {std::move(out_dir_ptr), std::move(compound_file_output)};
}

void IndexStorageFormatV2::write_version_and_indices_count(lucene::store::IndexOutput* output) {
    // Write the version number
    output->writeInt(_index_file_writer->_storage_format);

    // Write the number of indices
    const auto num_indices = static_cast<uint32_t>(_index_file_writer->_indices_dirs.size());
    output->writeInt(num_indices);
}

void IndexStorageFormatV2::write_index_headers_and_metadata(
        lucene::store::IndexOutput* output, const std::vector<FileMetadata>& file_metadata) {
    // Group files by index_id and index_suffix
    std::map<std::pair<int64_t, std::string>, std::vector<FileMetadata>> indices;

    for (const auto& meta : file_metadata) {
        indices[{meta.index_id, meta.index_suffix}].push_back(meta);
    }

    for (const auto& index_entry : indices) {
        int64_t index_id = index_entry.first.first;
        const std::string& index_suffix = index_entry.first.second;
        const auto& files = index_entry.second;

        // Write the index ID and the number of files
        output->writeLong(index_id);
        output->writeInt(cast_set<int32_t>(index_suffix.length()));
        output->writeBytes(reinterpret_cast<const uint8_t*>(index_suffix.data()),
                           cast_set<int32_t>(index_suffix.length()));
        output->writeInt(cast_set<int32_t>(files.size()));

        // Write file metadata
        for (const auto& file : files) {
            output->writeInt(cast_set<int32_t>(file.filename.length()));
            output->writeBytes(reinterpret_cast<const uint8_t*>(file.filename.data()),
                               cast_set<int32_t>(file.filename.length()));
            output->writeLong(file.offset);
            output->writeLong(file.length);
        }
    }
}

void IndexStorageFormatV2::copy_files_data(lucene::store::IndexOutput* output,
                                           const std::vector<FileMetadata>& file_metadata) {
    const int64_t buffer_length = 16384;
    uint8_t buffer[buffer_length];

    for (const auto& meta : file_metadata) {
        copy_file(meta.filename.c_str(), meta.directory, output, buffer, buffer_length);
    }
}

void IndexStorageFormatV2::add_meta_files_to_index_cache(const MetaFileRange& meta_range) {
#ifndef BE_TEST
    if (!config::is_cloud_mode()) {
        return;
    }
#endif

    if (auto* cache_builder = _index_file_writer->_idx_v2_writer->cache_builder();
        cache_builder != nullptr && cache_builder->_expiration_time == 0) {
        int64_t meta_file_size = meta_range.end_offset - meta_range.start_offset;
        if (meta_file_size > 0) {
            auto holder =
                    cache_builder->allocate_cache_holder(meta_range.start_offset, meta_file_size);
            for (auto& segment : holder->file_blocks) {
                static_cast<void>(segment->change_cache_type_between_normal_and_index(
                        io::FileCacheType::INDEX));
            }
        }
    }
}

} // namespace doris::segment_v2
#include "common/compile_check_end.h"