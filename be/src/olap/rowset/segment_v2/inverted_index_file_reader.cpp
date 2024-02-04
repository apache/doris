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

#include "olap/rowset/segment_v2/inverted_index_file_reader.h"

#include <memory>
#include <utility>

#include "olap/rowset/segment_v2/inverted_index_compound_directory.h"
#include "olap/rowset/segment_v2/inverted_index_compound_reader.h"
#include "olap/tablet_schema.h"

namespace doris::segment_v2 {

Status InvertedIndexFileReader::init(int32_t read_buffer_size, bool open_idx_file_cache) {
    _read_buffer_size = read_buffer_size;
    _open_idx_file_cache = open_idx_file_cache;
    if (_storage_format == InvertedIndexStorageFormatPB::V2) {
        return _init_from_v2(read_buffer_size);
    } else {
        return Status::OK();
    }
}

Status InvertedIndexFileReader::_init_from_v2(int32_t read_buffer_size) {
    auto index_file_full_path = _index_file_dir / _index_file_name;
    try {
        int64_t file_size = 0;
        RETURN_IF_ERROR(_fs->file_size(index_file_full_path, &file_size));
        if (file_size == 0) {
            LOG(WARNING) << "inverted index file " << index_file_full_path << " is empty.";
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "inverted index file {} is empty", index_file_full_path.native());
        }

        CLuceneError err;
        CL_NS(store)::IndexInput* index_input = nullptr;
        auto ok = DorisCompoundDirectory::FSIndexInput::open(_fs, index_file_full_path.c_str(),
                                                             index_input, err, read_buffer_size);
        if (!ok) {
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "CLuceneError occur when open idx file {}, error msg: {}",
                    index_file_full_path.native(), err.what());
        }
        index_input->setIdxFileCache(_open_idx_file_cache);
        _stream = std::unique_ptr<CL_NS(store)::IndexInput>(index_input);
        int32_t version = _stream->readInt(); // Read version number
        if (version == InvertedIndexStorageFormatPB::V2) {
            DCHECK(version == _storage_format);
            int32_t numIndices = _stream->readInt(); // Read number of indices
            ReaderFileEntry* entry = nullptr;

            for (int32_t i = 0; i < numIndices; ++i) {
                int64_t indexId = _stream->readInt();       // Read index ID
                int32_t suffix_length = _stream->readInt(); // Read suffix length
                std::vector<uint8_t> suffix_data(suffix_length);
                _stream->readBytes(suffix_data.data(), suffix_length);
                std::string suffix_str(suffix_data.begin(), suffix_data.end());

                int32_t numFiles = _stream->readInt(); // Read number of files in the index

                // true, true means it will deconstruct key and value
                auto fileEntries = std::make_unique<EntriesType>(true, true);

                for (int32_t j = 0; j < numFiles; ++j) {
                    entry = _CLNEW ReaderFileEntry();

                    int32_t file_name_length = _stream->readInt();
                    // aid will destruct in EntriesType map.
                    char* aid = (char*)malloc(file_name_length + 1);
                    _stream->readBytes(reinterpret_cast<uint8_t*>(aid), file_name_length);
                    aid[file_name_length] = '\0';
                    //stream->readString(tid, CL_MAX_PATH);
                    entry->file_name = std::string(aid, file_name_length);
                    entry->offset = _stream->readLong();
                    entry->length = _stream->readLong();

                    fileEntries->put(aid, entry);
                }

                _indices_entries.emplace(std::make_pair(indexId, std::move(suffix_str)),
                                         std::move(fileEntries));
            }
        } else {
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "unknown inverted index format {}", version);
        }
    } catch (CLuceneError& err) {
        if (_stream != nullptr) {
            try {
                _stream->close();
            } catch (CLuceneError& err) {
                return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                        "CLuceneError occur when close idx file {}, error msg: {}",
                        index_file_full_path.native(), err.what());
            }
        }
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "CLuceneError occur when init idx file {}, error msg: {}",
                index_file_full_path.native(), err.what());
    }
    return Status::OK();
}

Result<std::unique_ptr<DorisCompoundReader>> InvertedIndexFileReader::open(
        const TabletIndex* index_meta) const {
    auto index_id = index_meta->index_id();
    auto index_suffix = index_meta->get_index_suffix();
    std::unique_ptr<DorisCompoundReader> compound_reader;

    if (_storage_format == InvertedIndexStorageFormatPB::V1) {
        DorisCompoundDirectory* dir = nullptr;
        auto file_name = InvertedIndexDescriptor::get_index_file_name(_index_file_name, index_id,
                                                                      index_suffix);
        try {
            dir = DorisCompoundDirectoryFactory::getDirectory(_fs, _index_file_dir.c_str());

            compound_reader = std::make_unique<DorisCompoundReader>(
                    dir, file_name.c_str(), _read_buffer_size, _open_idx_file_cache);
        } catch (CLuceneError& err) {
            if (dir != nullptr) {
                dir->close();
                _CLDELETE(dir)
            }
            return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "CLuceneError occur when open idx file {}, error msg: {}",
                    (_index_file_dir / file_name).native(), err.what()));
        }
    } else {
        if (_stream == nullptr) {
            return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "CLuceneError occur when open idx file {}, stream is nullptr",
                    (_index_file_dir / _index_file_name).native()));
        }

        // Check if the specified index exists
        auto index_it = _indices_entries.find(std::make_pair(index_id, index_suffix));
        if (index_it == _indices_entries.end()) {
            std::ostringstream errMsg;
            errMsg << "No index with id " << index_id << " found";
            return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "CLuceneError occur when open idx file {}, error msg: {}",
                    (_index_file_dir / _index_file_name).native(), errMsg.str()));
        }
        bool own_index_input = false;
        compound_reader = std::make_unique<DorisCompoundReader>(
                _stream.get(), index_it->second.get(), own_index_input, _read_buffer_size);
    }
    return compound_reader;
}

std::string InvertedIndexFileReader::get_index_file_path(const TabletIndex* index_meta) const {
    return _index_file_dir /
           InvertedIndexDescriptor::get_index_file_name(_index_file_name, index_meta->index_id(),
                                                        index_meta->get_index_suffix());
}

Status InvertedIndexFileReader::index_file_exist(const TabletIndex* index_meta, bool* res) const {
    if (_storage_format == InvertedIndexStorageFormatPB::V1) {
        auto index_file_path = _index_file_dir / InvertedIndexDescriptor::get_index_file_name(
                                                         _index_file_name, index_meta->index_id(),
                                                         index_meta->get_index_suffix());
        return _fs->exists(index_file_path, res);
    } else {
        if (_stream == nullptr) {
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "idx file {} is not opened", (_index_file_dir / _index_file_name).native());
        }
        //TODO: support index suffix
        // Check if the specified index exists
        auto index_it = _indices_entries.find(
                std::make_pair(index_meta->index_id(), index_meta->get_index_suffix()));
        if (index_it == _indices_entries.end()) {
            *res = false;
        } else {
            *res = true;
        }
    }
    return Status::OK();
}

void InvertedIndexFileReader::debug_file_entries() {
    for (auto& index : _indices_entries) {
        LOG(INFO) << "index_id:" << index.first.first;
        auto* index_entries = index.second.get();
        for (auto& entry : (*index_entries)) {
            ReaderFileEntry* file_entry = entry.second;
            LOG(INFO) << "file entry name:" << file_entry->file_name
                      << " length:" << file_entry->length << " offset:" << file_entry->offset;
        }
    }
}

} // namespace doris::segment_v2
