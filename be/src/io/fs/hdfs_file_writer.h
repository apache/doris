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

namespace doris {
struct Slice;
namespace io {

class HdfsHandler;
class BlockFileCache;
struct FileCacheAllocatorBuilder;
struct AsyncCloseStatusPack;

class HdfsFileWriter final : public FileWriter {
public:
    // Accepted path format:
    // - fs_name/path_to_file
    // - /path_to_file
    // TODO(plat1ko): Support related path for cloud mode
    static Result<FileWriterPtr> create(Path path, std::shared_ptr<HdfsHandler> handler,
                                        const std::string& fs_name,
                                        const FileWriterOptions* opts = nullptr);

    HdfsFileWriter(Path path, std::shared_ptr<HdfsHandler> handler, hdfsFile hdfs_file,
                   std::string fs_name, const FileWriterOptions* opts = nullptr);
    ~HdfsFileWriter() override;

    Status appendv(const Slice* data, size_t data_cnt) override;
    const Path& path() const override { return _path; }
    size_t bytes_appended() const override { return _bytes_appended; }
    State state() const override { return _state; }

    Status close(bool non_block = false) override;

    FileCacheAllocatorBuilder* cache_builder() const override {
        return _cache_builder == nullptr ? nullptr : _cache_builder.get();
    }

private:
    Status _close_impl();
    // Flush buffered data into HDFS client and write local file cache if enabled
    // **Notice**: this would clear the underlying buffer
    Status _flush_buffer();
    Status append_hdfs_file(std::string_view content);
    void _write_into_local_file_cache();
    Status _append(std::string_view content);
    void _flush_and_reset_approximate_jni_buffer_size();
    Status _acquire_jni_memory(size_t size);

    Path _path;
    std::shared_ptr<HdfsHandler> _hdfs_handler = nullptr;
    hdfsFile _hdfs_file = nullptr;
    std::string _fs_name;
    size_t _bytes_appended = 0;
    bool _sync_file_data;
    std::unique_ptr<FileCacheAllocatorBuilder>
            _cache_builder; // nullptr if disable write file cache
    class BatchBuffer {
    public:
        BatchBuffer(size_t capacity);
        size_t append(std::string_view content);
        bool full() const;
        const char* data() const;
        size_t capacity() const;
        size_t size() const;
        void clear();
        std::string_view content() const;

    private:
        std::string _batch_buffer;
    };
    BatchBuffer _batch_buffer;
    size_t _approximate_jni_buffer_size = 0;
    std::unique_ptr<AsyncCloseStatusPack> _async_close_pack;
    // We should make sure that close_impl's return value is consistent
    // So we need add one field to restore the value first time return by calling close_impl
    Status _st;
    State _state {State::OPENED};
};

} // namespace io
} // namespace doris
