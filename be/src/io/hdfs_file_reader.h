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

#include <chrono>

#include "gen_cpp/PlanNodes_types.h"
#include "gutil/hash/hash.h"
#include "io/file_reader.h"
#include "io/hdfs_builder.h"

namespace doris {

class HdfsFsHandle {
private:
    // the number of referenced client
    std::atomic<int> _ref_cnt;
    // HdfsFsCache try to remove the oldest handler when the cache is full
    std::atomic<uint64_t> _last_access_time;
    // Client will set invalid if error thrown, and HdfsFsCache will not reuse this handler
    std::atomic<bool> _invalid;

    uint64_t _now() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                .count();
    }

public:
    hdfsFS hdfs_fs;
    // When cache is full, and all handlers are in use, HdfsFsCache will return an uncached handler.
    // Client should delete the handler in such case.
    const bool from_cache;

    HdfsFsHandle(hdfsFS fs, bool cached)
            : _ref_cnt(0), _last_access_time(0), _invalid(false), hdfs_fs(fs), from_cache(cached) {}

    ~HdfsFsHandle() {
        DCHECK(_ref_cnt == 0);
        if (hdfs_fs != nullptr) {
            // Even if there is an error, the resources associated with the hdfsFS will be freed.
            hdfsDisconnect(hdfs_fs);
        }
        hdfs_fs = nullptr;
    }

    int64_t last_access_time() { return _last_access_time; }

    void inc_ref() {
        _ref_cnt++;
        _last_access_time = _now();
    }

    void dec_ref() {
        _ref_cnt--;
        _last_access_time = _now();
    }

    int ref_cnt() { return _ref_cnt; }

    bool invalid() { return _invalid; }

    void set_invalid() { _invalid = true; }
};

// Cache for HDFS file system
class HdfsFsCache {
public:
    static int MAX_CACHE_HANDLE;

    static HdfsFsCache* instance() {
        static HdfsFsCache s_instance;
        return &s_instance;
    }

    HdfsFsCache(const HdfsFsCache&) = delete;
    const HdfsFsCache& operator=(const HdfsFsCache&) = delete;

    // This function is thread-safe
    Status get_connection(THdfsParams& hdfs_params, HdfsFsHandle** fs_handle);

private:
    HdfsFsCache() = default;
    uint64 _hdfs_hash_code(THdfsParams& hdfs_params);
    Status _create_fs(THdfsParams& hdfs_params, hdfsFS* fs);
    void _clean_invalid();
    void _clean_oldest();

    std::mutex _lock;
    std::unordered_map<uint64, std::unique_ptr<HdfsFsHandle>> _cache;
};

class HdfsFileReader : public FileReader {
public:
    HdfsFileReader(const THdfsParams& hdfs_params, const std::string& path, int64_t start_offset);
    HdfsFileReader(const std::map<std::string, std::string>& properties, const std::string& path,
                   int64_t start_offset);
    ~HdfsFileReader() override;

    Status open() override;

    // Read content to 'buf', 'buf_len' is the max size of this buffer.
    // Return ok when read success, and 'buf_len' is set to size of read content
    // If reach to end of file, the eof is set to true. meanwhile 'buf_len'
    // is set to zero.
    Status read(uint8_t* buf, int64_t buf_len, int64_t* bytes_read, bool* eof) override;
    Status readat(int64_t position, int64_t nbytes, int64_t* bytes_read, void* out) override;
    Status read_one_message(std::unique_ptr<uint8_t[]>* buf, int64_t* length) override;
    int64_t size() override;
    Status seek(int64_t position) override;
    Status tell(int64_t* position) override;
    void close() override;
    bool closed() override;

private:
    void _parse_properties(const std::map<std::string, std::string>& prop);

    THdfsParams _hdfs_params;
    std::string _namenode;
    std::string _path;
    int64_t _current_offset;
    int64_t _file_size;
    hdfsFS _hdfs_fs;
    hdfsFile _hdfs_file;
    HdfsFsHandle* _fs_handle = nullptr;
    bool _closed = false;
    bool _opened = false;
};

} // namespace doris
