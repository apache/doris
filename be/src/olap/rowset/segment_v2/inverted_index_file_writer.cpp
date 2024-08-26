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

#include <filesystem>

#include "common/status.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "olap/tablet_schema.h"
#include "runtime/exec_env.h"

namespace doris::segment_v2 {

Status InvertedIndexFileWriter::initialize(InvertedIndexDirectoryMap& indices_dirs) {
    _indices_dirs = std::move(indices_dirs);
    return Status::OK();
}

Result<DorisFSDirectory*> InvertedIndexFileWriter::open(const TabletIndex* index_meta) {
    auto tmp_file_dir = ExecEnv::GetInstance()->get_tmp_file_dirs()->get_tmp_file_dir();
    const auto& local_fs = io::global_local_filesystem();
    auto local_fs_index_path = InvertedIndexDescriptor::get_temporary_index_path(
            tmp_file_dir.native(), _rowset_id, _seg_id, index_meta->index_id(),
            index_meta->get_index_suffix());
    bool exists = false;
    auto st = local_fs->exists(local_fs_index_path, &exists);
    if (!st.ok()) {
        LOG(ERROR) << "index_path:" << local_fs_index_path << " exists error:" << st;
        return ResultError(st);
    }

    if (exists) {
        LOG(ERROR) << "try to init a directory:" << local_fs_index_path << " already exists";
        return ResultError(Status::InternalError("init_fulltext_index directory already exists"));
    }

    bool can_use_ram_dir = true;
    auto* dir = DorisFSDirectoryFactory::getDirectory(local_fs, local_fs_index_path.c_str(),
                                                      can_use_ram_dir);
    _indices_dirs.emplace(std::make_pair(index_meta->index_id(), index_meta->get_index_suffix()),
                          std::unique_ptr<DorisFSDirectory>(dir));
    return dir;
}

Status InvertedIndexFileWriter::delete_index(const TabletIndex* index_meta) {
    if (!index_meta) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT>("Index metadata is null.");
    }

    auto index_id = index_meta->index_id();
    const auto& index_suffix = index_meta->get_index_suffix();

    // Check if the specified index exists
    auto index_it = _indices_dirs.find(std::make_pair(index_id, index_suffix));
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
            _total_file_size = write_v1();
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
            _total_file_size = write_v2();
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
    if (!dir->openInput(fileName, tmp, err)) {
        throw err;
    }

    std::unique_ptr<lucene::store::IndexInput> input(tmp);
    int64_t start_ptr = output->getFilePointer();
    int64_t length = input->length();
    int64_t remainder = length;
    int64_t chunk = bufferLength;

    while (remainder > 0) {
        int64_t len = std::min(std::min(chunk, length), remainder);
        input->readBytes(buffer, len);
        output->writeBytes(buffer, len);
        remainder -= len;
    }
    if (remainder != 0) {
        std::ostringstream errMsg;
        errMsg << "Non-zero remainder length after copying: " << remainder << " (id: " << fileName
               << ", length: " << length << ", buffer size: " << chunk << ")";
        err.set(CL_ERR_IO, errMsg.str().c_str());
        throw err;
    }

    int64_t end_ptr = output->getFilePointer();
    int64_t diff = end_ptr - start_ptr;
    if (diff != length) {
        std::ostringstream errMsg;
        errMsg << "Difference in the output file offsets " << diff
               << " does not match the original file length " << length;
        err.set(CL_ERR_IO, errMsg.str().c_str());
        throw err;
    }
    input->close();
}

int64_t InvertedIndexFileWriter::write_v1() {
    int64_t total_size = 0;
    for (const auto& entry : _indices_dirs) {
        const int64_t index_id = entry.first.first;
        const auto& index_suffix = entry.first.second;
        try {
            const auto& directory = entry.second;
            std::vector<std::string> files;
            directory->list(&files);
            // remove write.lock file
            auto it = std::find(files.begin(), files.end(), DorisFSDirectory::WRITE_LOCK_FILE);
            if (it != files.end()) {
                files.erase(it);
            }

            std::vector<FileInfo> sorted_files;
            for (auto file : files) {
                FileInfo file_info;
                file_info.filename = file;
                file_info.filesize = directory->fileLength(file.c_str());
                sorted_files.emplace_back(std::move(file_info));
            }
            sort_files(sorted_files);

            int32_t file_count = sorted_files.size();

            io::Path cfs_path(InvertedIndexDescriptor::get_index_file_path_v1(
                    _index_path_prefix, index_id, index_suffix));
            auto idx_path = cfs_path.parent_path();
            std::string idx_name = cfs_path.filename();
            // write file entries to ram directory to get header length
            lucene::store::RAMDirectory ram_dir;
            auto* out_idx = ram_dir.createOutput(idx_name.c_str());
            if (out_idx == nullptr) {
                LOG(WARNING) << "Write compound file error: RAMDirectory output is nullptr.";
                _CLTHROWA(CL_ERR_IO, "Create RAMDirectory output error");
            }

            std::unique_ptr<lucene::store::IndexOutput> ram_output(out_idx);
            ram_output->writeVInt(file_count);
            // write file entries in ram directory
            // number of files, which data are in header
            int header_file_count = 0;
            int64_t header_file_length = 0;
            const int64_t buffer_length = 16384;
            uint8_t ram_buffer[buffer_length];
            for (auto file : sorted_files) {
                ram_output->writeString(file.filename); // file name
                ram_output->writeLong(0);               // data offset
                ram_output->writeLong(file.filesize);   // file length
                header_file_length += file.filesize;
                if (header_file_length <= DorisFSDirectory::MAX_HEADER_DATA_SIZE) {
                    copyFile(file.filename.c_str(), directory.get(), ram_output.get(), ram_buffer,
                             buffer_length);
                    header_file_count++;
                }
            }
            auto header_len = ram_output->getFilePointer();
            ram_output->close();
            ram_dir.deleteFile(idx_name.c_str());
            ram_dir.close();

            auto* out_dir = DorisFSDirectoryFactory::getDirectory(_fs, idx_path.c_str());
            out_dir->set_file_writer_opts(_opts);

            auto* out = out_dir->createOutput(idx_name.c_str());
            if (out == nullptr) {
                LOG(WARNING) << "Write compound file error: CompoundDirectory output is nullptr.";
                _CLTHROWA(CL_ERR_IO, "Create CompoundDirectory output error");
            }
            std::unique_ptr<lucene::store::IndexOutput> output(out);
            size_t start = output->getFilePointer();
            output->writeVInt(file_count);
            // write file entries
            int64_t data_offset = header_len;
            uint8_t header_buffer[buffer_length];
            for (int i = 0; i < sorted_files.size(); ++i) {
                auto file = sorted_files[i];
                output->writeString(file.filename); // FileName
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
                    copyFile(file.filename.c_str(), directory.get(), output.get(), header_buffer,
                             buffer_length);
                } else {
                    data_offset += file.filesize;
                }
            }
            // write rest files' data
            uint8_t data_buffer[buffer_length];
            for (int i = header_file_count; i < sorted_files.size(); ++i) {
                auto file = sorted_files[i];
                copyFile(file.filename.c_str(), directory.get(), output.get(), data_buffer,
                         buffer_length);
            }
            out_dir->close();
            // NOTE: need to decrease ref count, but not to delete here,
            // because index cache may get the same directory from DIRECTORIES
            _CLDECDELETE(out_dir)
            auto compound_file_size = output->getFilePointer() - start;
            output->close();
            //LOG(INFO) << (idx_path / idx_name).c_str() << " size:" << compound_file_size;
            total_size += compound_file_size;
            InvertedIndexFileInfo_IndexInfo index_info;
            index_info.set_index_id(index_id);
            index_info.set_index_suffix(index_suffix);
            index_info.set_index_file_size(compound_file_size);
            auto* new_index_info = _file_info.add_index_info();
            *new_index_info = index_info;
        } catch (CLuceneError& err) {
            LOG(ERROR) << "CLuceneError occur when close idx file "
                       << InvertedIndexDescriptor::get_index_file_path_v1(_index_path_prefix,
                                                                          index_id, index_suffix)
                       << " error msg: " << err.what();

            throw err;
        }
    }
    return total_size;
}

int64_t InvertedIndexFileWriter::write_v2() {
    // Create the output stream to write the compound file
    int64_t current_offset = headerLength();

    io::Path index_path {InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix)};

    auto* out_dir = DorisFSDirectoryFactory::getDirectory(_fs, index_path.parent_path().c_str());
    out_dir->set_file_writer_opts(_opts);

    std::unique_ptr<lucene::store::IndexOutput> compound_file_output;
    // idx v2 writer != nullptr means memtable on sink node now
    if (_idx_v2_writer != nullptr) {
        compound_file_output = std::unique_ptr<lucene::store::IndexOutput>(
                out_dir->createOutputV2(_idx_v2_writer.get()));
    } else {
        compound_file_output = std::unique_ptr<lucene::store::IndexOutput>(
                out_dir->createOutput(index_path.filename().c_str()));
    }

    // Write the version number
    compound_file_output->writeInt(InvertedIndexStorageFormatPB::V2);

    // Write the number of indices
    const auto numIndices = static_cast<uint32_t>(_indices_dirs.size());
    compound_file_output->writeInt(numIndices);

    std::vector<std::tuple<std::string, int64_t, int64_t, CL_NS(store)::Directory*>>
            file_metadata; // Store file name, offset, file length, and corresponding directory

    // First, write all index information and file metadata
    for (const auto& entry : _indices_dirs) {
        const int64_t index_id = entry.first.first;
        const auto& index_suffix = entry.first.second;
        const auto& dir = entry.second;
        std::vector<std::string> files;
        dir->list(&files);

        auto it = std::find(files.begin(), files.end(), DorisFSDirectory::WRITE_LOCK_FILE);
        if (it != files.end()) {
            files.erase(it);
        }
        // sort file list by file length
        std::vector<std::pair<std::string, int64_t>> sorted_files;
        for (const auto& file : files) {
            sorted_files.emplace_back(file, dir->fileLength(file.c_str()));
        }

        std::sort(sorted_files.begin(), sorted_files.end(),
                  [](const std::pair<std::string, int64_t>& a,
                     const std::pair<std::string, int64_t>& b) { return (a.second < b.second); });

        int32_t file_count = sorted_files.size();

        // Write the index ID and the number of files
        compound_file_output->writeLong(index_id);
        compound_file_output->writeInt(static_cast<int32_t>(index_suffix.length()));
        compound_file_output->writeBytes(reinterpret_cast<const uint8_t*>(index_suffix.data()),
                                         index_suffix.length());
        compound_file_output->writeInt(file_count);

        // Calculate the offset for each file and write the file metadata
        for (const auto& file : sorted_files) {
            int64_t file_length = dir->fileLength(file.first.c_str());
            compound_file_output->writeInt(static_cast<int32_t>(file.first.length()));
            compound_file_output->writeBytes(reinterpret_cast<const uint8_t*>(file.first.data()),
                                             file.first.length());
            compound_file_output->writeLong(current_offset);
            compound_file_output->writeLong(file_length);

            file_metadata.emplace_back(file.first, current_offset, file_length, dir.get());
            current_offset += file_length; // Update the data offset
        }
    }

    const int64_t buffer_length = 16384;
    uint8_t header_buffer[buffer_length];

    // Next, write the file data
    for (const auto& info : file_metadata) {
        const std::string& file = std::get<0>(info);
        auto* dir = std::get<3>(info);

        // Write the actual file data
        copyFile(file.c_str(), dir, compound_file_output.get(), header_buffer, buffer_length);
    }

    out_dir->close();
    // NOTE: need to decrease ref count, but not to delete here,
    // because index cache may get the same directory from DIRECTORIES
    _CLDECDELETE(out_dir)
    auto compound_file_size = compound_file_output->getFilePointer();
    compound_file_output->close();
    _file_info.set_index_size(compound_file_size);
    return compound_file_size;
}
} // namespace doris::segment_v2