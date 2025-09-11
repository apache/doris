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

#include "io/fs/buffered_reader.h"
#include "io/fs/file_reader.h"
#include "vec/common/custom_allocator.h"

namespace doris {
namespace vectorized {

class OrcMergeRangeFileReader : public io::FileReader {
public:
    struct Statistics {
        int64_t copy_time = 0;
        int64_t read_time = 0;
        int64_t request_io = 0;
        int64_t merged_io = 0;
        int64_t request_bytes = 0;
        int64_t merged_bytes = 0;
        int64_t apply_bytes = 0;
    };

    OrcMergeRangeFileReader(RuntimeProfile* profile, io::FileReaderSPtr inner_reader,
                            io::PrefetchRange range);

    ~OrcMergeRangeFileReader() override = default;

    Status close() override {
        if (!_closed) {
            _closed = true;
        }
        return Status::OK();
    }

    const io::Path& path() const override { return _inner_reader->path(); }

    size_t size() const override { return _size; }

    bool closed() const override { return _closed; }

    // for test only
    const Statistics& statistics() const { return _statistics; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext* io_ctx) override;

    void _collect_profile_before_close() override;

private:
    RuntimeProfile::Counter* _copy_time = nullptr;
    RuntimeProfile::Counter* _read_time = nullptr;
    RuntimeProfile::Counter* _request_io = nullptr;
    RuntimeProfile::Counter* _merged_io = nullptr;
    RuntimeProfile::Counter* _request_bytes = nullptr;
    RuntimeProfile::Counter* _merged_bytes = nullptr;
    RuntimeProfile::Counter* _apply_bytes = nullptr;

    RuntimeProfile* _profile;
    io::FileReaderSPtr _inner_reader;
    io::PrefetchRange _range;

    DorisUniqueBufferPtr<char> _cache;
    int64_t _current_start_offset = -1;

    size_t _size;
    bool _closed = false;

    Statistics _statistics;
};

} // namespace vectorized
} // namespace doris