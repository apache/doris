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

#include <memory>

#include "butil/macros.h"
#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/path.h"

namespace doris {
struct IOContext;
}

namespace doris::io {

// FileReader wrapper that reads data from merge file using offset and size
// It wraps an inner reader that points to the merge file,
// and adjusts read offsets based on the small file's position in the merge file
class MergeFileReader final : public FileReader {
public:
    MergeFileReader(FileReaderSPtr inner_reader, Path path, int64_t offset, int64_t size);
    ~MergeFileReader() override;

    MergeFileReader(const MergeFileReader&) = delete;
    const MergeFileReader& operator=(const MergeFileReader&) = delete;

    Status close() override;

    const Path& path() const override { return _path; }

    size_t size() const override { return _file_size; }

    bool closed() const override { return _closed; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_ctx) override;

private:
    FileReaderSPtr _inner_reader;
    Path _path;
    int64_t _merge_file_offset; // Offset in merge file where this small file starts
    int64_t _file_size;         // Size of the small file
    bool _closed = false;
};

} // namespace doris::io
