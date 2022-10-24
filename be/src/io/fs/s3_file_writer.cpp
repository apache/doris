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

#include "io/fs/s3_file_writer.h"

#include <fmt/core.h>
#include <sys/uio.h>

#include <cerrno>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

#include "common/compiler_util.h"
#include "common/status.h"
#include "gutil/macros.h"
#include "io/fs/file_writer.h"
#include "io/fs/path.h"
#include "io/fs/s3_file_system.h"
#include "util/doris_metrics.h"

namespace doris {
namespace io {

S3FileWriter::S3FileWriter(FileSystemPtr fs, Path path)
        : FileWriter(std::move(path)), _fs(fs) {
    DorisMetrics::instance()->s3_file_open_writing->increment(1);
    DorisMetrics::instance()->s3_file_writer_total->increment(1);
}

S3FileWriter::~S3FileWriter() {
    if (!_closed) {
        WARN_IF_ERROR(abort(), fmt::format("Cannot abort {}", _path.native()));
    }
}

Status S3FileWriter::close() {
    return _close(true);
}

Status S3FileWriter::abort() {
    RETURN_IF_ERROR(_close(false));
    return fs->delete_file(_path);
}

Status S3FileWriter::_open() {
    std::string key = _fs->get_key(_path.native());
    Aws::S3::Model::CreateMultipartUploadRequest create_request;
    create_request.SetBucket(_fs->s3_conf().bucket.c_str());
    create_request.SetKey(key.c_str());
    create_request.SetContentType("text/plain");

    auto outcome = s3_client.CreateMultipartUpload(create_request);

    if (outcome.IsSuccess()) {
        _upload_id = createMultipartUploadOutcome.GetResult().GetUploadId();
        LOG(INFO) << "create multi part upload successfully (endpoint=" << _fs->s3_conf().endpoint
                  << ", bucket=" << _fs->s3_conf().bucket << ", key=" << key << ") upload_id: "
                  << _upload_id;
        return Status::OK();
    }
    return Status::IOError("failed to create multi part upload (endpoint={}, bucket={}, key={}): {}",
                           _fs->s3_conf().endpoint, _fs->s3_conf().bucket, key,
                           outcome.GetError().GetMessage());
}

Status S3FileWriter::append(const Slice& data) {
    Status st = appendv(&data, 1);
    if (st.ok()) {
        DorisMetrics::instance()->s3_bytes_written_total->increment(data.size);
    }
    return st;
}

Status S3FileWriter::appendv(const Slice* data, size_t data_cnt) {
    DCHECK(!_closed);
    if (!_is_open) {
        RETURN_IF_ERROR(_open());
        _is_open = true;
    }

    size_t bytes_req = 0;
    struct iovec iov[data_cnt];
    for (size_t i = 0; i < data_cnt; i++) {
        const Slice& result = data[i];
        bytes_req += result.size;
        ++_cur_part_num;

        // start upload
        std::string key = _fs->get_key(_path.native());
        Aws::S3::Model::UploadPartRequest upload_request;
        upload_request.WithBucket(_fs->s3_conf().bucket.c_str()).WithKey(key.c_str())
                .WithPartNumber(_cur_part_num).WithUploadId(upload_id.c_str());

        std::shared_ptr<Aws::StringStream> stream_ptr =
                Aws::MakeShared<Aws::StringStream>("WriteStream::Upload" /* log id */, data.to_string());

        upload_request.SetBody(stream_ptr);

        Aws::Utils::ByteBuffer part_md5(
                Aws::Utils::HashingUtils::CalculateMD5(*stream_ptr));
        upload_request.SetContentMD5(Aws::Utils::HashingUtils::Base64Encode(part_md5));

        auto start_pos = stream_ptr->tellg();
        stream_ptr->seekg(0LL, stream_ptr->end);
        upload_request.SetContentLength(static_cast<long>(stream_ptr->tellg()));
        stream_ptr->seekg(start_pos);

        auto upload_part_callable = _fs->client().UploadPartCallable(upload_request);

        UploadPartOutcome upload_part_outcome = upload_part_callable.get();
        CompletedPart completed_part;
        completed_part.SetPartNumber(_cur_part_num);
        auto etag = upload_part_outcome.GetResult().GetETag();
        DCHECK(etag.empty());
        completed_part.SetETag(etag);
        _completed_parts.emplace_back(completed_part);


        iov[i] = {result.data, result.size};
    }

    _bytes_appended += bytes_req;
    return Status::OK();
}

Status S3FileWriter::finalize() {
    DCHECK(!_closed);
    if (_is_open) {
        _close(true);
    }
    return Status::OK();
}

Status S3FileWriter::_close(bool sync) {
    if (_closed) {
        return Status::OK();
    }
    if (sync && _is_open) {
        std::string key = _fs->get_key(_path.native());
        Aws::S3::Model::CompleteMultipartUploadRequest complete_request;
        complete_request.WithBucket(_fs->s3_conf().bucket.c_str()).WithKey(key.c_str())
                .WithUploadId(_upload_id.c_str());

        CompletedMultipartUpload completed_upload;
        for (CompletedPart part : _completed_parts) {
            completed_upload.AddParts(part);
        }

        complete_request.WithMultipartUpload(completed_upload);

        auto compute_outcome = _fs->client().CompleteMultipartUpload(complete_request);

        if (!compute_outcome.IsSuccess()) {
            return Status::IOError("failed to create multi part upload (endpoint={}, bucket={}, key={}): {}",
                                   _fs->s3_conf().endpoint, _fs->s3_conf().bucket, key,
                                   compute_outcome.GetError().GetMessage());
        }
        _is_open = false;
    }
    _closed = true;

    DorisMetrics::instance()->s3_file_open_writing->increment(-1);
    DorisMetrics::instance()->s3_file_created_total->increment(1);
    DorisMetrics::instance()->s3_bytes_written_total->increment(_bytes_appended);

    LOG(INFO) << "complete multi part upload successfully (endpoint=" << _fs->s3_conf().endpoint
              << ", bucket=" << _fs->s3_conf().bucket << ", key=" << key << ") upload_id: "
              << _upload_id;
    return Status::OK();
}

Status S3FileWriter::write_at(size_t offset, const Slice& data) {
    return Status::NotSupported("not support");
}

} // namespace io
} // namespace doris
