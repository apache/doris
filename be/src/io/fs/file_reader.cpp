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

#include "io/fs/file_reader.h"

#include <bthread/bthread.h>
#include <butil/iobuf.h>
#include <glog/logging.h>

#include "io/cache/cached_remote_file_reader.h"
#include "io/fs/file_system.h"
#include "util/async_io.h"

namespace doris::io {

const std::string FileReader::VIRTUAL_REMOTE_DATA_DIR = "virtual_remote_data_dir";

Status FileReader::read_at(size_t offset, Slice result, size_t* bytes_read,
                           const IOContext* io_ctx) {
    DCHECK(bthread_self() == 0);
    Status st = read_at_impl(offset, result, bytes_read, io_ctx);
    if (!st) {
        LOG(WARNING) << st;
    }
    return st;
}

Status FileReader::read_at_iobuf(size_t offset, size_t bytes_req, butil::IOBuf* out,
                                 size_t* bytes_read, const IOContext* io_ctx) {
    DCHECK(bthread_self() == 0);
    Status st = read_at_iobuf_impl(offset, bytes_req, out, bytes_read, io_ctx);
    if (!st) {
        LOG(WARNING) << st;
    }
    return st;
}

Status FileReader::read_at_iobuf_impl(size_t offset, size_t bytes_req, butil::IOBuf* out,
                                      size_t* bytes_read, const IOContext* io_ctx) {
    if (out == nullptr || bytes_read == nullptr) {
        return Status::InvalidArgument("read_at_iobuf requires non-null out and bytes_read");
    }
    *bytes_read = 0;
    (void)offset;
    (void)bytes_req;
    (void)io_ctx;
    return Status::NotSupported("read_at_iobuf is not supported by current FileReader");
}

Result<FileReaderSPtr> create_cached_file_reader(FileReaderSPtr raw_reader,
                                                 const FileReaderOptions& opts) {
    switch (opts.cache_type) {
    case io::FileCachePolicy::NO_CACHE:
        return raw_reader;
    case FileCachePolicy::FILE_BLOCK_CACHE:
        return std::make_shared<CachedRemoteFileReader>(std::move(raw_reader), opts);
    default:
        return ResultError(Status::InternalError("Unknown cache type: {}", opts.cache_type));
    }
}

} // namespace doris::io
