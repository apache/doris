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

#include "common/status.h"
#include "io/fs/file_writer.h"
#include "io/fs/hdfs.h"
#include "io/fs/path.h"
#include "util/slice.h"

namespace doris::io {

class HdfsHandler;

class HdfsFileWriter final : public FileWriter {
public:
    // Accepted path format:
    // - fs_name/path_to_file
    // - /path_to_file
    // TODO(plat1ko): Support related path for cloud mode
    static Result<FileWriterPtr> create(Path path, HdfsHandler* handler,
                                        const std::string& fs_name);

    HdfsFileWriter(Path path, HdfsHandler* handler, hdfsFile hdfs_file, std::string fs_name);
    ~HdfsFileWriter() override;

    Status close() override;
    Status appendv(const Slice* data, size_t data_cnt) override;
    Status finalize() override;
    const Path& path() const override { return _path; }
    size_t bytes_appended() const override { return _bytes_appended; }
    bool closed() const override { return _closed; }

private:
    Path _path;
    HdfsHandler* _hdfs_handler = nullptr;
    hdfsFile _hdfs_file = nullptr;
    std::string _fs_name;
    size_t _bytes_appended = 0;
    bool _closed = false;
};

} // namespace doris::io
