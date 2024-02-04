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

#pragma once

#include <CLucene.h> // IWYU pragma: keep
#include <CLucene/store/IndexInput.h>
#include <gen_cpp/olap_file.pb.h>

#include <string>
#include <utility>
#include <vector>

#include "io/fs/file_system.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"

namespace doris {
class TabletIndex;

namespace segment_v2 {
class DorisCompoundDirectory;

class DorisCompoundFileWriter : LUCENE_BASE {
public:
    DorisCompoundFileWriter() = default;
    DorisCompoundFileWriter(CL_NS(store)::Directory* dir);
    ~DorisCompoundFileWriter() override = default;
    /** Returns the directory of the compound file. */
    CL_NS(store)::Directory* getDirectory();
    virtual size_t writeCompoundFile();
    static void copyFile(const char* fileName, lucene::store::Directory* dir,
                         lucene::store::IndexOutput* output, uint8_t* buffer, int64_t bufferLength);

private:
    class FileInfo {
    public:
        std::string filename;
        int32_t filesize;
    };

    void sort_files(std::vector<FileInfo>& file_infos);

    CL_NS(store)::Directory* directory = nullptr;
};

class InvertedIndexFileWriter {
public:
    InvertedIndexFileWriter(io::FileSystemSPtr fs, io::Path segment_file_dir,
                            std::string segment_file_name,
                            InvertedIndexStorageFormatPB storage_format)
            : _fs(std::move(fs)),
              _index_file_dir(std::move(segment_file_dir)),
              _segment_file_name(std::move(segment_file_name)),
              _storage_format(storage_format) {}

    Result<DorisCompoundDirectory*> open(const TabletIndex* index_meta);
    ~InvertedIndexFileWriter() = default;
    size_t write();
    Status close();
    size_t headerLength();
    const io::Path& get_index_file_dir() const { return _index_file_dir; };
    //const std::string& get_index_file_name() const { return _segment_file_name; };
    std::string get_index_file_path(const TabletIndex* index_meta) const;

private:
    std::map<std::pair<int64_t, std::string>, std::unique_ptr<DorisCompoundDirectory>>
            _indices_dirs;
    const io::FileSystemSPtr _fs;
    io::FileSystemSPtr _lfs;
    io::Path _index_file_dir;
    std::string _segment_file_name;
    InvertedIndexStorageFormatPB _storage_format;
    size_t _file_size = 0;
};
} // namespace segment_v2
} // namespace doris