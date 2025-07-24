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

#include "index_storage_format_v1.h"

#include "common/cast_set.h"
#include "olap/rowset/segment_v2/index_file_writer.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "util/debug_points.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

IndexStorageFormatV1::IndexStorageFormatV1(IndexFileWriter* index_file_writer)
        : IndexStorageFormat(index_file_writer) {}

Status IndexStorageFormatV1::write() {
    int64_t total_size = 0;
    std::unique_ptr<lucene::store::Directory, DirectoryDeleter> out_dir = nullptr;
    std::unique_ptr<lucene::store::IndexOutput> output = nullptr;
    ErrorContext error_context;
    for (const auto& entry : _index_file_writer->_indices_dirs) {
        const int64_t index_id = entry.first.first;
        const auto& index_suffix = entry.first.second;
        try {
            const auto& directory = entry.second.get();

            // Prepare sorted file list
            auto sorted_files = prepare_sorted_files(directory);

            // Calculate header length
            auto [header_length, header_file_count] =
                    calculate_header_length(sorted_files, directory);

            // Create output stream
            auto result = create_output_stream(index_id, index_suffix);
            out_dir = std::move(result.first);
            output = std::move(result.second);

            size_t start = output->getFilePointer();
            // Write header and data
            write_header_and_data(output.get(), sorted_files, directory, header_length,
                                  header_file_count);

            // Collect file information
            auto compound_file_size = output->getFilePointer() - start;
            total_size += compound_file_size;
            add_index_info(index_id, index_suffix, compound_file_size);
        } catch (CLuceneError& err) {
            error_context.eptr = std::current_exception();
            auto index_path = InvertedIndexDescriptor::get_index_file_path_v1(
                    _index_file_writer->_index_path_prefix, index_id, index_suffix);
            error_context.err_msg.append("CLuceneError occur when write_v1 idx file: ");
            error_context.err_msg.append(index_path);
            error_context.err_msg.append(", error msg: ");
            error_context.err_msg.append(err.what());
            LOG(ERROR) << error_context.err_msg;
        }
        FINALLY({
            FINALLY_CLOSE(output);
            FINALLY_CLOSE(out_dir);
            output = nullptr;
            out_dir = nullptr;
        })
    }

    _index_file_writer->_total_file_size = total_size;
    return Status::OK();
}

std::pair<int64_t, int32_t> IndexStorageFormatV1::calculate_header_length(
        const std::vector<FileInfo>& sorted_files, lucene::store::Directory* directory) {
    // Use RAMDirectory to calculate header length
    lucene::store::RAMDirectory ram_dir;
    auto* out_idx = ram_dir.createOutput("temp_idx");
    DBUG_EXECUTE_IF("IndexFileWriter::calculate_header_length_ram_output_is_nullptr",
                    { out_idx = nullptr; })
    if (out_idx == nullptr) {
        LOG(WARNING) << "IndexFileWriter::calculate_header_length error: RAMDirectory "
                        "output is nullptr.";
        _CLTHROWA(CL_ERR_IO, "Create RAMDirectory output error");
    }
    std::unique_ptr<lucene::store::IndexOutput> ram_output(out_idx);
    auto file_count = cast_set<int32_t>(sorted_files.size());
    ram_output->writeVInt(file_count);

    int64_t header_file_length = 0;
    const int64_t buffer_length = 16384;
    uint8_t ram_buffer[buffer_length];
    int32_t header_file_count = 0;
    for (const auto& file : sorted_files) {
        ram_output->writeString(file.filename);
        ram_output->writeLong(0);
        ram_output->writeLong(file.filesize);
        header_file_length += file.filesize;

        if (header_file_length <= DorisFSDirectory::MAX_HEADER_DATA_SIZE) {
            copy_file(file.filename.c_str(), directory, ram_output.get(), ram_buffer,
                      buffer_length);
            header_file_count++;
        }
    }

    int64_t header_length = ram_output->getFilePointer();
    ram_output->close();
    ram_dir.close();
    return {header_length, header_file_count};
}

std::pair<std::unique_ptr<lucene::store::Directory, DirectoryDeleter>,
          std::unique_ptr<lucene::store::IndexOutput>>
IndexStorageFormatV1::create_output_stream(int64_t index_id, const std::string& index_suffix) {
    io::Path cfs_path(InvertedIndexDescriptor::get_index_file_path_v1(
            _index_file_writer->_index_path_prefix, index_id, index_suffix));
    auto idx_path = cfs_path.parent_path();
    std::string idx_name = cfs_path.filename();

    std::unique_ptr<DorisFSDirectory, DirectoryDeleter> out_dir_ptr(
            DorisFSDirectoryFactory::getDirectory(_index_file_writer->_fs, idx_path.c_str()));
    out_dir_ptr->set_file_writer_opts(_index_file_writer->_opts);

    std::unique_ptr<lucene::store::IndexOutput> output(out_dir_ptr->createOutput(idx_name.c_str()));
    DBUG_EXECUTE_IF("IndexFileWriter::write_v1_out_dir_createOutput_nullptr",
                    { output = nullptr; });
    if (output == nullptr) {
        LOG(WARNING) << "IndexFileWriter::create_output_stream_v1 error: CompoundDirectory "
                        "output is nullptr.";
        _CLTHROWA(CL_ERR_IO, "Create CompoundDirectory output error");
    }

    return {std::move(out_dir_ptr), std::move(output)};
}

void IndexStorageFormatV1::write_header_and_data(lucene::store::IndexOutput* output,
                                                 const std::vector<FileInfo>& sorted_files,
                                                 lucene::store::Directory* directory,
                                                 int64_t header_length, int32_t header_file_count) {
    output->writeVInt(cast_set<int32_t>(sorted_files.size()));
    int64_t data_offset = header_length;
    const int64_t buffer_length = 16384;
    uint8_t buffer[buffer_length];

    for (int i = 0; i < sorted_files.size(); ++i) {
        auto file = sorted_files[i];
        output->writeString(file.filename);

        // DataOffset
        if (i < header_file_count) {
            // file data write in header, so we set its offset to -1.
            output->writeLong(-1);
        } else {
            output->writeLong(data_offset);
        }
        output->writeLong(file.filesize); // FileLength
        if (i < header_file_count) {
            // append data
            copy_file(file.filename.c_str(), directory, output, buffer, buffer_length);
        } else {
            data_offset += file.filesize;
        }
    }

    for (size_t i = header_file_count; i < sorted_files.size(); ++i) {
        copy_file(sorted_files[i].filename.c_str(), directory, output, buffer, buffer_length);
    }
}

void IndexStorageFormatV1::add_index_info(int64_t index_id, const std::string& index_suffix,
                                          int64_t compound_file_size) {
    InvertedIndexFileInfo_IndexInfo index_info;
    index_info.set_index_id(index_id);
    index_info.set_index_suffix(index_suffix);
    index_info.set_index_file_size(compound_file_size);
    auto* new_index_info = _index_file_writer->_file_info.add_index_info();
    *new_index_info = index_info;
}

} // namespace doris::segment_v2
#include "common/compile_check_end.h"