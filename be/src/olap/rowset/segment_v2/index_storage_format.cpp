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

#include "index_storage_format.h"

#include "common/cast_set.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "util/debug_points.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"
IndexStorageFormat::IndexStorageFormat(IndexFileWriter* index_file_writer)
        : _index_file_writer(index_file_writer) {}

void IndexStorageFormat::sort_files(std::vector<FileInfo>& file_infos) {
    auto file_priority = [](const std::string& filename) {
        for (const auto& entry : InvertedIndexDescriptor::index_file_info_map) {
            if (filename.find(entry.first) != std::string::npos) {
                return entry.second;
            }
        }
        return std::numeric_limits<int32_t>::max(); // Other files
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

std::vector<FileInfo> IndexStorageFormat::prepare_sorted_files(
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

void IndexStorageFormat::copy_file(const char* fileName, lucene::store::Directory* dir,
                                   lucene::store::IndexOutput* output, uint8_t* buffer,
                                   int64_t bufferLength) {
    try {
        CLuceneError err;
        std::unique_ptr<lucene::store::IndexInput> input(dir->openInput(fileName));
        DBUG_EXECUTE_IF("IndexFileWriter::copyFile_openInput_error", {
            err.set(CL_ERR_IO, "debug point: copyFile_openInput_error");
            throw err;
        });
        int64_t start_ptr = output->getFilePointer();
        int64_t length = input->length();
        int64_t remainder = length;
        int64_t chunk = bufferLength;

        while (remainder > 0) {
            auto len = cast_set<int32_t>(std::min({chunk, length, remainder}));
            input->readBytes(buffer, len);
            output->writeBytes(buffer, len);
            remainder -= len;
        }
        DBUG_EXECUTE_IF("IndexFileWriter::copyFile_remainder_is_not_zero", { remainder = 10; });
        if (remainder != 0) {
            std::ostringstream errMsg;
            errMsg << "Non-zero remainder length after copying: " << remainder
                   << " (id: " << fileName << ", length: " << length << ", buffer size: " << chunk
                   << ")";
            err.set(CL_ERR_IO, errMsg.str().c_str());
            throw err;
        }

        int64_t end_ptr = output->getFilePointer();
        int64_t diff = end_ptr - start_ptr;
        DBUG_EXECUTE_IF("IndexFileWriter::copyFile_diff_not_equals_length",
                        { diff = length - 10; });
        if (diff != length) {
            std::ostringstream errMsg;
            errMsg << "Difference in the output file offsets " << diff
                   << " does not match the original file length " << length;
            err.set(CL_ERR_IO, errMsg.str().c_str());
            throw err;
        }
        input->close();
    } catch (const CLuceneError& e) {
        if (e.number() == CL_ERR_EmptyIndexSegment) {
            LOG(WARNING) << "InvertedIndexFileWriter::copyFile: " << fileName << " is empty";
            return;
        } else {
            throw e;
        }
    }
}

} // namespace doris::segment_v2
#include "common/compile_check_end.h"