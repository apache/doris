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

#include "olap/rowset/segment_v2/vector_index_desc.h"

namespace doris {
namespace segment_v2 {

class VectorIndexFileReader {
public:
    VectorIndexFileReader(const io::FileSystemSPtr& fs,
                          io::Path segment_file_dir,
                          std::string segment_file_name):
                          _fs(fs),
                          _index_file_dir(std::move(segment_file_dir)),
                          _segment_file_name(std::move(segment_file_name)){}

    io::FileSystemSPtr get_fs() { return _fs; }

    io::Path& get_index_file_dir() { return _index_file_dir; }

    std::string get_segment_file_name() { return _segment_file_name; }

private:
    io::FileSystemSPtr _fs;
    io::Path _index_file_dir;
    std::string _segment_file_name;
};

}
}
