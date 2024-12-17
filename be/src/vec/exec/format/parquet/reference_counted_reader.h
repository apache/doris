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

#include <cstdint>
#include <memory>
#include <string>

#include "io/fs/buffered_reader.h"
#include "io/fs/file_reader.h"

namespace doris {
namespace vectorized {

class ChunkReader {
public:
    struct Statistics {
        int64_t merged_io {0};
        int64_t merged_bytes {0};

        void merge(Statistics& statistics) {
            merged_io += statistics.merged_io;
            merged_bytes += statistics.merged_bytes;
        }
    };
    virtual int64_t getDiskOffset() = 0;
    virtual Status read(const io::IOContext* io_ctx, std::shared_ptr<Slice>* result) = 0;
    //    Status read(size_t offset, Slice result, size_t* bytes_read,
    //                const io::IOContext* io_ctx);
    virtual void free() = 0;
    virtual size_t size() = 0;
    virtual size_t file_size() = 0;
    virtual const io::Path& path() const = 0;
    virtual ~ChunkReader() = default;
};

class ReferenceCountedReader : public ChunkReader {
public:
    // 对应Java中的MAX_ARRAY_SIZE常量
    static const int32_t MAX_ARRAY_SIZE = INT32_MAX - 8;

    ReferenceCountedReader(const io::PrefetchRange& range, io::FileReaderSPtr file_reader,
                           ChunkReader::Statistics& statistics);

    ~ReferenceCountedReader() override = default;

    void addReference();

    // ChunkReader interface implementation
    int64_t getDiskOffset() override;
    Status read(const io::IOContext* io_ctx, std::shared_ptr<Slice>* result) override;
    //    Status read(size_t offset, Slice result, size_t* bytes_read,
    //                                        const io::IOContext* io_ctx);
    void free() override;

    size_t size() override;

    size_t file_size() override;

    const io::Path& path() const override { return _file_reader->path(); }

    std::string toString() const;

    int32_t reference_count() const { return _reference_count; }

private:
    //    Status readFully(size_t offset, Slice result, size_t* bytes_read, const doris::io::IOContext* io_ctx);

    io::PrefetchRange _range;
    io::FileReaderSPtr _file_reader;
    ChunkReader::Statistics& _statistics;
    std::unique_ptr<char[]> _data;
    int32_t _reference_count;
};

} // namespace vectorized
} // namespace doris
