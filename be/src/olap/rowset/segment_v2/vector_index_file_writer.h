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

#include "io/fs/file_system.h"
#include "vector_index_desc.h"

namespace doris {
class TabletIndex;
class VectorIndexDescriptor;

namespace segment_v2 {

class VectorIndexFileWriter {
public:
    VectorIndexFileWriter(io::FileSystemSPtr fs,
                          io::Path segment_file_dir,
                          std::string segment_file_name)
            : _fs(std::move(fs)),
              _index_file_dir(std::move(segment_file_dir)),
              _segment_file_name(std::move(segment_file_name)) {}

    ~VectorIndexFileWriter() = default;

    io::Path& get_index_file_dir() { return _index_file_dir; }

    std::string get_segment_file_name() { return _segment_file_name; }

    io::FileSystemSPtr& get_fs() { return _fs; }

    std::vector<std::string> get_index_paths() {
        std::vector<std::string> index_paths;
        for (auto index: _indexes) {
            std::string segment_path = _index_file_dir/_segment_file_name;
            std::string index_path = VectorIndexDescriptor::get_index_file_name(segment_path,index.first,index.second);
            index_paths.emplace_back(index_path);
        }
        return index_paths;
    };

    void add_index(int64_t index_id, std::string index_suffix);

private:
    io::FileSystemSPtr _fs;
    io::Path _index_file_dir;
    std::string _segment_file_name;
    std::vector<std::pair<int64_t,std::string>> _indexes;
};

} // namespace segment_v2
} // namespace doris