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

#include <atomic>
#include <memory>

#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/path.h"
#include "util/slice.h"

namespace doris {
struct StorePath;
struct DataDirInfo;
struct CachePath;
} // namespace doris

namespace doris::io {

struct BeConfDataDirReader {
    static std::vector<doris::DataDirInfo> be_config_data_dir_list;

    static void get_data_dir_by_file_path(Path* file_path, std::string* data_dir_arg);

    static void init_be_conf_data_dir(const std::vector<doris::StorePath>& store_paths,
                                      const std::vector<doris::StorePath>& spill_store_paths,
                                      const std::vector<doris::CachePath>& cache_paths);
};

struct IOContext;

class LocalFileReader final : public FileReader {
public:
    LocalFileReader(Path path, size_t file_size, int fd);

    ~LocalFileReader() override;

    Status close() override;

    const Path& path() const override { return _path; }

    size_t size() const override { return _file_size; }

    bool closed() const override { return _closed.load(std::memory_order_acquire); }

    const std::string& get_data_dir_path() override { return _data_dir_path; }

private:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_ctx) override;

private:
    int _fd = -1; // owned
    Path _path;
    size_t _file_size;
    std::atomic<bool> _closed = false;
    std::string _data_dir_path; // be conf's data dir path
};

} // namespace doris::io
