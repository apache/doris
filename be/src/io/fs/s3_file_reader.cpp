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

#include "io/fs/s3_file_reader.h"

#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>

#include "io/fs/s3_common.h"
#include "util/doris_metrics.h"

namespace doris {
namespace io {

S3FileReader::S3FileReader(Path path, size_t file_size, std::string key, std::string bucket,
                           S3FileSystem* fs)
        : _path(std::move(path)),
          _file_size(file_size),
          _fs(fs),
          _bucket(std::move(bucket)),
          _key(std::move(key)) {
    DorisMetrics::instance()->s3_file_open_reading->increment(1);
    DorisMetrics::instance()->s3_file_reader_total->increment(1);
}

S3FileReader::~S3FileReader() {
    close();
}

Status S3FileReader::close() {
    bool expected = false;
    if (_closed.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        DorisMetrics::instance()->s3_file_open_reading->increment(-1);
    }
    return Status::OK();
}

Status S3FileReader::read_at(size_t offset, Slice result, const IOContext& /*io_ctx*/,
                             size_t* bytes_read) {
    DCHECK(!closed());
    if (offset > _file_size) {
        return Status::IOError("offset exceeds file size(offset: {}, file size: {}, path: {})",
                               offset, _file_size, _path.native());
    }
    size_t bytes_req = result.size;
    char* to = result.data;
    bytes_req = std::min(bytes_req, _file_size - offset);
    if (UNLIKELY(bytes_req == 0)) {
        *bytes_read = 0;
        return Status::OK();
    }

    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(_bucket).WithKey(_key);
    request.SetRange(fmt::format("bytes={}-{}", offset, offset + bytes_req - 1));
    request.SetResponseStreamFactory(AwsWriteableStreamFactory(to, bytes_req));

    auto client = _fs->get_client();
    if (!client) {
        return Status::InternalError("init s3 client error");
    }
    auto outcome = client->GetObject(request);
    if (!outcome.IsSuccess()) {
        return Status::IOError("failed to read from {}: {}", _path.native(),
                               outcome.GetError().GetMessage());
    }
    *bytes_read = outcome.GetResult().GetContentLength();
    if (*bytes_read != bytes_req) {
        return Status::IOError("failed to read from {}(bytes read: {}, bytes req: {})",
                               _path.native(), *bytes_read, bytes_req);
    }
    DorisMetrics::instance()->s3_bytes_read_total->increment(*bytes_read);
    return Status::OK();
}

} // namespace io
} // namespace doris
