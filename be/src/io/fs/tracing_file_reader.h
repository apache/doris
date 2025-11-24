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
#include "io/fs/file_reader.h"
#include "util/runtime_profile.h"

namespace doris {

namespace io {

class TracingFileReader : public FileReader {
public:
    TracingFileReader(doris::io::FileReaderSPtr inner, FileReaderStats* stats)
            : _inner(std::move(inner)), _stats(stats) {}

    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_ctx) override {
        SCOPED_RAW_TIMER(&_stats->read_time_ns);
        Status st = _inner->read_at(offset, result, bytes_read, io_ctx);
        _stats->read_calls++;
        _stats->read_bytes += *bytes_read;
        return st;
    }

    Status close() override { return _inner->close(); }
    const doris::io::Path& path() const override { return _inner->path(); }
    size_t size() const override { return _inner->size(); }
    bool closed() const override { return _inner->closed(); }
    const std::string& get_data_dir_path() override { return _inner->get_data_dir_path(); }

    void _collect_profile_at_runtime() override { return _inner->collect_profile_at_runtime(); }
    void _collect_profile_before_close() override { return _inner->collect_profile_before_close(); }

    FileReaderStats* stats() const { return _stats; }

private:
    doris::io::FileReaderSPtr _inner;
    FileReaderStats* _stats;
};

} // namespace io
} // namespace doris
