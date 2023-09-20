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

#include <stddef.h>

#include <atomic>
#include <memory>
#include <string>

#include "common/status.h"
#include "io/fs/file_handle_cache.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/hdfs.h"
#include "io/fs/hdfs_file_system.h"
#include "io/fs/path.h"
#include "util/slice.h"

namespace doris {
namespace io {
struct IOContext;

class HdfsFileReader : public FileReader {
public:
    HdfsFileReader(Path path, const std::string& name_node, FileHandleCache::Accessor accessor,
                   RuntimeProfile* profile);

    ~HdfsFileReader() override;

    Status close() override;

    const Path& path() const override { return _path; }

    size_t size() const override { return _handle->file_size(); }

    bool closed() const override { return _closed.load(std::memory_order_acquire); }

    FileSystemSPtr fs() const override { return _accessor.fs(); }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_ctx) override;

private:
#ifdef USE_HADOOP_HDFS
    struct HDFSProfile {
        RuntimeProfile::Counter* total_bytes_read;
        RuntimeProfile::Counter* total_local_bytes_read;
        RuntimeProfile::Counter* total_short_circuit_bytes_read;
        RuntimeProfile::Counter* total_total_zero_copy_bytes_read;

        RuntimeProfile::Counter* total_hedged_read;
        RuntimeProfile::Counter* hedged_read_in_cur_thread;
        RuntimeProfile::Counter* hedged_read_wins;
    };
#endif

    Path _path;
    const std::string& _name_node;
    FileHandleCache::Accessor _accessor;
    CachedHdfsFileHandle* _handle = nullptr; // owned by _cached_file_handle
    std::atomic<bool> _closed = false;
    RuntimeProfile* _profile;
#ifdef USE_HADOOP_HDFS
    HDFSProfile _hdfs_profile;
#endif
};
} // namespace io
} // namespace doris
