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

#include "olap/rowset/segment_v2/inverted_index_file_writer.h"

#include <glog/logging.h>

#include <algorithm>
#include <filesystem>

#include "common/status.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "olap/tablet_schema.h"

namespace doris::segment_v2 {

Status InvertedIndexFileWriter::initialize(InvertedIndexDirectoryMap& indices_dirs) {
    _indices_dirs = std::move(indices_dirs);
    return Status::OK();
}

Status InvertedIndexFileWriter::_insert_directory_into_map(int64_t index_id,
                                                           const std::string& index_suffix,
                                                           std::shared_ptr<DorisFSDirectory> dir) {
    auto key = std::make_pair(index_id, index_suffix);
    auto [it, inserted] = _indices_dirs.emplace(key, std::move(dir));
    if (!inserted) {
        LOG(ERROR) << "InvertedIndexFileWriter::open attempted to insert a duplicate key: ("
                   << key.first << ", " << key.second << ")";
        LOG(ERROR) << "Directories already in map: ";
        for (const auto& entry : _indices_dirs) {
            LOG(ERROR) << "Key: (" << entry.first.first << ", " << entry.first.second << ")";
        }
        return Status::InternalError(
                "InvertedIndexFileWriter::open attempted to insert a duplicate dir");
    }
    return Status::OK();
}

Result<std::shared_ptr<DorisFSDirectory>> InvertedIndexFileWriter::open(
        const TabletIndex* index_meta) {
    auto local_fs_index_path = InvertedIndexDescriptor::get_temporary_index_path(
            _tmp_dir, _rowset_id, _seg_id, index_meta->index_id(), index_meta->get_index_suffix());
    bool can_use_ram_dir = true;
    auto dir = std::shared_ptr<DorisFSDirectory>(DorisFSDirectoryFactory::getDirectory(
            _local_fs, local_fs_index_path.c_str(), can_use_ram_dir));
    auto st =
            _insert_directory_into_map(index_meta->index_id(), index_meta->get_index_suffix(), dir);
    if (!st.ok()) {
        return ResultError(st);
    }

    return dir;
}

Status InvertedIndexFileWriter::delete_index(const TabletIndex* index_meta) {
    DBUG_EXECUTE_IF("InvertedIndexFileWriter::delete_index_index_meta_nullptr",
                    { index_meta = nullptr; });
    if (!index_meta) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT>("Index metadata is null.");
    }

    auto index_id = index_meta->index_id();
    const auto& index_suffix = index_meta->get_index_suffix();

    // Check if the specified index exists
    auto index_it = _indices_dirs.find(std::make_pair(index_id, index_suffix));
    DBUG_EXECUTE_IF("InvertedIndexFileWriter::delete_index_indices_dirs_reach_end",
                    { index_it = _indices_dirs.end(); })
    if (index_it == _indices_dirs.end()) {
        std::ostringstream errMsg;
        errMsg << "No inverted index with id " << index_id << " and suffix " << index_suffix
               << " found.";
        LOG(WARNING) << errMsg.str();
        return Status::OK();
    }

    _indices_dirs.erase(index_it);
    return Status::OK();
}

int64_t InvertedIndexFileWriter::headerLength() {
    int64_t header_size = 0;
    header_size +=
            sizeof(int32_t) * 2; // Account for the size of the version number and number of indices

    for (const auto& entry : _indices_dirs) {
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

Status InvertedIndexFileWriter::close() {
    DCHECK(!_closed) << debug_string();
    _closed = true;
    if (_indices_dirs.empty()) {
        return Status::OK();
    }
    DBUG_EXECUTE_IF("inverted_index_storage_format_must_be_v2", {
        if (_storage_format != InvertedIndexStorageFormatPB::V2) {
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "InvertedIndexFileWriter::close fault injection:inverted index storage format "
                    "must be v2");
        }
    })
    if (_storage_format == InvertedIndexStorageFormatPB::V1) {
        try {
            RETURN_IF_ERROR(write_v1());
            for (const auto& entry : _indices_dirs) {
                const auto& dir = entry.second;
                // delete index path, which contains separated inverted index files
                if (std::strcmp(dir->getObjectName(), "DorisFSDirectory") == 0) {
                    auto* compound_dir = static_cast<DorisFSDirectory*>(dir.get());
                    compound_dir->deleteDirectory();
                }
            }
        } catch (CLuceneError& err) {
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "CLuceneError occur when close, error msg: {}", err.what());
        }
    } else {
        try {
            RETURN_IF_ERROR(write_v2());
            for (const auto& entry : _indices_dirs) {
                const auto& dir = entry.second;
                // delete index path, which contains separated inverted index files
                if (std::strcmp(dir->getObjectName(), "DorisFSDirectory") == 0) {
                    auto* compound_dir = static_cast<DorisFSDirectory*>(dir.get());
                    compound_dir->deleteDirectory();
                }
            }
        } catch (CLuceneError& err) {
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "CLuceneError occur when close idx file {}, error msg: {}",
                    InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix),
                    err.what());
        }
    }
    return Status::OK();
}

void InvertedIndexFileWriter::sort_files(std::vector<FileInfo>& file_infos) {
    auto file_priority = [](const std::string& filename) {
        if (filename.find("segments") != std::string::npos) {
            return 1;
        }
        if (filename.find("fnm") != std::string::npos) {
            return 2;
        }
        if (filename.find("tii") != std::string::npos) {
            return 3;
        }
        return 4; // Other files
    };
    std::sort(file_infos.begin(), file_infos.end(), [&](const FileInfo& a, const FileInfo& b) {
        int32_t priority_a = file_priority(a.filename);
        int32_t priority_b = file_priority(b.filename);
        if (priority_a != priority_b) {
            return priority_a < priority_b;
        }
        return a.filesize < b.filesize;
    });
}

void InvertedIndexFileWriter::copyFile(const char* fileName, lucene::store::Directory* dir,
                                       lucene::store::IndexOutput* output, uint8_t* buffer,
                                       int64_t bufferLength) {
    lucene::store::IndexInput* tmp = nullptr;
    CLuceneError err;
    auto open = dir->openInput(fileName, tmp, err);
    DBUG_EXECUTE_IF("InvertedIndexFileWriter::copyFile_openInput_error", {
        open = false;
        err.set(CL_ERR_IO, "debug point: copyFile_openInput_error");
    });
    if (!open) {
        throw err;
    }

    std::unique_ptr<lucene::store::IndexInput> input(tmp);
    int64_t start_ptr = output->getFilePointer();
    int64_t length = input->length();
    int64_t remainder = length;
    int64_t chunk = bufferLength;

    while (remainder > 0) {
        int64_t len = std::min({chunk, length, remainder});
        input->readBytes(buffer, len);
        output->writeBytes(buffer, len);
        remainder -= len;
    }
    DBUG_EXECUTE_IF("InvertedIndexFileWriter::copyFile_remainder_is_not_zero", { remainder = 10; });
    if (remainder != 0) {
        std::ostringstream errMsg;
        errMsg << "Non-zero remainder length after copying: " << remainder << " (id: " << fileName
               << ", length: " << length << ", buffer size: " << chunk << ")";
        err.set(CL_ERR_IO, errMsg.str().c_str());
        throw err;
    }

    int64_t end_ptr = output->getFilePointer();
    int64_t diff = end_ptr - start_ptr;
    DBUG_EXECUTE_IF("InvertedIndexFileWriter::copyFile_diff_not_equals_length",
                    { diff = length - 10; });
    if (diff != length) {
        std::ostringstream errMsg;
        errMsg << "Difference in the output file offsets " << diff
               << " does not match the original file length " << length;
        err.set(CL_ERR_IO, errMsg.str().c_str());
        throw err;
    }
    input->close();
}

Status InvertedIndexFileWriter::write_v1() {
    int64_t total_size = 0;
    std::string err_msg;
    lucene::store::Directory* out_dir = nullptr;
    std::exception_ptr eptr;
    std::unique_ptr<lucene::store::IndexOutput> output = nullptr;
    for (const auto& entry : _indices_dirs) {
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
            auto result = create_output_stream_v1(index_id, index_suffix);
            out_dir = result.first;
            output = std::move(result.second);

            size_t start = output->getFilePointer();
            // Write header and data
            write_header_and_data_v1(output.get(), sorted_files, directory, header_length,
                                     header_file_count);

            // Collect file information
            auto compound_file_size = output->getFilePointer() - start;
            total_size += compound_file_size;
            add_index_info(index_id, index_suffix, compound_file_size);
        } catch (CLuceneError& err) {
            eptr = std::current_exception();
            auto index_path = InvertedIndexDescriptor::get_index_file_path_v1(
                    _index_path_prefix, index_id, index_suffix);
            err_msg = "CLuceneError occur when write_v1 idx file " + index_path +
                      " error msg: " + err.what();
        }

        // Close and clean up
        finalize_output_dir(out_dir);
        if (output) {
            output->close();
        }

        if (eptr) {
            LOG(ERROR) << err_msg;
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(err_msg);
        }
    }

    _total_file_size = total_size;
    return Status::OK();
}

Status InvertedIndexFileWriter::write_v2() {
    std::string err_msg;
    lucene::store::Directory* out_dir = nullptr;
    std::unique_ptr<lucene::store::IndexOutput> compound_file_output = nullptr;
    std::exception_ptr eptr;
    try {
        // Calculate header length and initialize offset
        int64_t current_offset = headerLength();
        // Prepare file metadata
        auto file_metadata = prepare_file_metadata_v2(current_offset);

        // Create output stream
        auto result = create_output_stream_v2();
        out_dir = result.first;
        compound_file_output = std::move(result.second);

        // Write version and number of indices
        write_version_and_indices_count(compound_file_output.get());

        // Write index headers and file metadata
        write_index_headers_and_metadata(compound_file_output.get(), file_metadata);

        // Copy file data
        copy_files_data_v2(compound_file_output.get(), file_metadata);

        _total_file_size = compound_file_output->getFilePointer();
        _file_info.set_index_size(_total_file_size);
    } catch (CLuceneError& err) {
        eptr = std::current_exception();
        auto index_path = InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix);
        err_msg = "CLuceneError occur when close idx file " + index_path +
                  " error msg: " + err.what();
    }

    // Close and clean up
    finalize_output_dir(out_dir);
    if (compound_file_output) {
        compound_file_output->close();
    }

    if (eptr) {
        LOG(ERROR) << err_msg;
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(err_msg);
    }

    return Status::OK();
}

// Helper function implementations
std::vector<FileInfo> InvertedIndexFileWriter::prepare_sorted_files(
        lucene::store::Directory* directory) {
    std::vector<std::string> files;
    directory->list(&files);

    // Remove write.lock file
    files.erase(std::remove(files.begin(), files.end(), DorisFSDirectory::WRITE_LOCK_FILE),
                files.end());

    std::vector<FileInfo> sorted_files;
    for (const auto& file : files) {
        FileInfo file_info;
        file_info.filename = file;
        file_info.filesize = directory->fileLength(file.c_str());
        sorted_files.push_back(std::move(file_info));
    }

    // Sort the files
    sort_files(sorted_files);
    return sorted_files;
}

void InvertedIndexFileWriter::finalize_output_dir(lucene::store::Directory* out_dir) {
    if (out_dir != nullptr) {
        out_dir->close();
        _CLDECDELETE(out_dir)
    }
}

void InvertedIndexFileWriter::add_index_info(int64_t index_id, const std::string& index_suffix,
                                             int64_t compound_file_size) {
    InvertedIndexFileInfo_IndexInfo index_info;
    index_info.set_index_id(index_id);
    index_info.set_index_suffix(index_suffix);
    index_info.set_index_file_size(compound_file_size);
    auto* new_index_info = _file_info.add_index_info();
    *new_index_info = index_info;
}

std::pair<int64_t, int32_t> InvertedIndexFileWriter::calculate_header_length(
        const std::vector<FileInfo>& sorted_files, lucene::store::Directory* directory) {
    // Use RAMDirectory to calculate header length
    lucene::store::RAMDirectory ram_dir;
    auto* out_idx = ram_dir.createOutput("temp_idx");
    DBUG_EXECUTE_IF("InvertedIndexFileWriter::calculate_header_length_ram_output_is_nullptr",
                    { out_idx = nullptr; })
    if (out_idx == nullptr) {
        LOG(WARNING) << "InvertedIndexFileWriter::calculate_header_length error: RAMDirectory "
                        "output is nullptr.";
        _CLTHROWA(CL_ERR_IO, "Create RAMDirectory output error");
    }
    std::unique_ptr<lucene::store::IndexOutput> ram_output(out_idx);
    int32_t file_count = sorted_files.size();
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
            copyFile(file.filename.c_str(), directory, ram_output.get(), ram_buffer, buffer_length);
            header_file_count++;
        }
    }

    int64_t header_length = ram_output->getFilePointer();
    ram_output->close();
    ram_dir.close();
    return {header_length, header_file_count};
}

std::pair<lucene::store::Directory*, std::unique_ptr<lucene::store::IndexOutput>>
InvertedIndexFileWriter::create_output_stream_v1(int64_t index_id,
                                                 const std::string& index_suffix) {
    io::Path cfs_path(InvertedIndexDescriptor::get_index_file_path_v1(_index_path_prefix, index_id,
                                                                      index_suffix));
    auto idx_path = cfs_path.parent_path();
    std::string idx_name = cfs_path.filename();

    auto* out_dir = DorisFSDirectoryFactory::getDirectory(_fs, idx_path.c_str());
    out_dir->set_file_writer_opts(_opts);

    auto* out = out_dir->createOutput(idx_name.c_str());
    DBUG_EXECUTE_IF("InvertedIndexFileWriter::write_v1_out_dir_createOutput_nullptr",
                    { out = nullptr; });
    if (out == nullptr) {
        LOG(WARNING) << "InvertedIndexFileWriter::create_output_stream_v1 error: CompoundDirectory "
                        "output is nullptr.";
        _CLTHROWA(CL_ERR_IO, "Create CompoundDirectory output error");
    }

    std::unique_ptr<lucene::store::IndexOutput> output(out);
    return {out_dir, std::move(output)};
}

void InvertedIndexFileWriter::write_header_and_data_v1(lucene::store::IndexOutput* output,
                                                       const std::vector<FileInfo>& sorted_files,
                                                       lucene::store::Directory* directory,
                                                       int64_t header_length,
                                                       int32_t header_file_count) {
    output->writeVInt(sorted_files.size());
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
            copyFile(file.filename.c_str(), directory, output, buffer, buffer_length);
        } else {
            data_offset += file.filesize;
        }
    }

    for (size_t i = header_file_count; i < sorted_files.size(); ++i) {
        copyFile(sorted_files[i].filename.c_str(), directory, output, buffer, buffer_length);
    }
}

std::pair<lucene::store::Directory*, std::unique_ptr<lucene::store::IndexOutput>>
InvertedIndexFileWriter::create_output_stream_v2() {
    io::Path index_path {InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix)};
    auto* out_dir = DorisFSDirectoryFactory::getDirectory(_fs, index_path.parent_path().c_str());
    out_dir->set_file_writer_opts(_opts);
    DCHECK(_idx_v2_writer != nullptr) << "inverted index file writer v2 is nullptr";
    auto compound_file_output = std::unique_ptr<lucene::store::IndexOutput>(
            out_dir->createOutputV2(_idx_v2_writer.get()));
    return std::make_pair(out_dir, std::move(compound_file_output));
}

void InvertedIndexFileWriter::write_version_and_indices_count(lucene::store::IndexOutput* output) {
    // Write the version number
    output->writeInt(InvertedIndexStorageFormatPB::V2);

    // Write the number of indices
    const auto num_indices = static_cast<uint32_t>(_indices_dirs.size());
    output->writeInt(num_indices);
}

std::vector<InvertedIndexFileWriter::FileMetadata>
InvertedIndexFileWriter::prepare_file_metadata_v2(int64_t& current_offset) {
    std::vector<FileMetadata> file_metadata;

    for (const auto& entry : _indices_dirs) {
        const int64_t index_id = entry.first.first;
        const auto& index_suffix = entry.first.second;
        auto* dir = entry.second.get();

        // Get sorted files
        auto sorted_files = prepare_sorted_files(dir);

        for (const auto& file : sorted_files) {
            file_metadata.emplace_back(index_id, index_suffix, file.filename, current_offset,
                                       file.filesize, dir);
            current_offset += file.filesize; // Update the data offset
        }
    }
    return file_metadata;
}

void InvertedIndexFileWriter::write_index_headers_and_metadata(
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
        output->writeInt(static_cast<int32_t>(index_suffix.length()));
        output->writeBytes(reinterpret_cast<const uint8_t*>(index_suffix.data()),
                           index_suffix.length());
        output->writeInt(static_cast<int32_t>(files.size()));

        // Write file metadata
        for (const auto& file : files) {
            output->writeInt(static_cast<int32_t>(file.filename.length()));
            output->writeBytes(reinterpret_cast<const uint8_t*>(file.filename.data()),
                               file.filename.length());
            output->writeLong(file.offset);
            output->writeLong(file.length);
        }
    }
}

void InvertedIndexFileWriter::copy_files_data_v2(lucene::store::IndexOutput* output,
                                                 const std::vector<FileMetadata>& file_metadata) {
    const int64_t buffer_length = 16384;
    uint8_t buffer[buffer_length];

    for (const auto& meta : file_metadata) {
        copyFile(meta.filename.c_str(), meta.directory, output, buffer, buffer_length);
    }
}
} // namespace doris::segment_v2
