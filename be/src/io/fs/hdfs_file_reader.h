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

#include "io/fs/file_reader.h"
#include "io/fs/hdfs_file_system.h"
namespace doris {
namespace io {

class HdfsFileReader : public FileReader {
public:
    HdfsFileReader(Path path, size_t file_size, const std::string& name_node, hdfsFile hdfs_file,
                   HdfsFileSystem* fs);

    ~HdfsFileReader() override;

    Status close() override;

    Status read_at(size_t offset, Slice result, const IOContext& io_ctx,
                   size_t* bytes_read) override;

    const Path& path() const override { return _path; }

    size_t size() const override { return _file_size; }

    bool closed() const override { return _closed.load(std::memory_order_acquire); }

private:
    Path _path;
    size_t _file_size;
    const std::string& _name_node;
    hdfsFile _hdfs_file;
    HdfsFileSystem* _fs;
    std::atomic<bool> _closed = false;
};
} // namespace io
} // namespace doris
