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

#include "io/fs/oss_file_writer.h"

#include <alibabacloud/oss/OssClient.h>
#include <bvar/reducer.h>
#include <fmt/format.h>
#include <glog/logging.h>

#include <algorithm>
#include <sstream>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/file_writer.h"
#include "io/fs/path.h"
#include "util/doris_metrics.h"

namespace doris::io {

bvar::Adder<uint64_t> oss_file_writer_total("oss_file_writer_total_num");
bvar::Adder<uint64_t> oss_bytes_written_total("oss_file_writer_bytes_written");
bvar::Adder<uint64_t> oss_file_created_total("oss_file_writer_file_created");
bvar::Adder<uint64_t> oss_file_being_written("oss_file_writer_file_being_written");

OSSFileWriter::OSSFileWriter(std::shared_ptr<OSSClientHolder> client, std::string bucket,
                             std::string key, const FileWriterOptions* opts)
        : _path(fmt::format("oss://{}/{}", bucket, key)),
          _bucket(std::move(bucket)),
          _key(std::move(key)),
          _client(std::move(client)),
          _buffer_size(config::s3_write_buffer_size) {
    oss_file_writer_total << 1;
    oss_file_being_written << 1;
    _pending_buf.reserve(_buffer_size);
    init_cache_builder(opts, _path);
}

OSSFileWriter::~OSSFileWriter() {
    // Abort multipart upload if not completed to prevent orphaned parts
    if (state() != State::CLOSED && !_upload_id.empty()) {
        LOG(WARNING) << "OSSFileWriter destroyed without close(), aborting multipart upload: "
                     << _path.native() << " upload_id: " << _upload_id;
        static_cast<void>(_abort_multipart_upload());
    }

    if (state() == State::OPENED && !_failed) {
        oss_bytes_written_total << _bytes_appended;
    }
    oss_file_being_written << -1;
}

Status OSSFileWriter::appendv(const Slice* data, size_t data_cnt) {
    if (state() != State::OPENED) {
        return Status::InternalError("append to closed file: {}", _path.native());
    }

    for (size_t i = 0; i < data_cnt; i++) {
        const char* src = data[i].data;
        size_t src_size = data[i].size;

        for (size_t pos = 0; pos < src_size;) {
            if (_failed) {
                return _st;
            }

            size_t available = _buffer_size - _pending_buf.size();
            size_t to_copy = std::min(available, src_size - pos);

            _pending_buf.insert(_pending_buf.end(), src + pos, src + pos + to_copy);
            pos += to_copy;
            _bytes_appended += to_copy;

            // Flush when buffer is full
            if (_pending_buf.size() >= _buffer_size) {
                RETURN_IF_ERROR(_flush_buffer());
            }
        }
    }

    return Status::OK();
}

Status OSSFileWriter::_flush_buffer() {
    if (_pending_buf.empty()) {
        return Status::OK();
    }

    // First part or small file - need to decide strategy
    if (_cur_part_num == 1 && _pending_buf.size() < _buffer_size) {
        // Don't flush yet, wait for close to decide
        return Status::OK();
    }

    // Create multipart upload on first flush of full buffer
    if (_cur_part_num == 1 && _upload_id.empty()) {
        RETURN_IF_ERROR(_create_multipart_upload());
    }

    // Upload as part
    RETURN_IF_ERROR(_upload_part(_cur_part_num, _pending_buf.data(), _pending_buf.size()));
    _cur_part_num++;
    _pending_buf.clear();

    return Status::OK();
}

Status OSSFileWriter::_create_multipart_upload() {
    auto client = _client->get();
    if (!client) {
        return Status::InternalError("OSS client not initialized");
    }

    AlibabaCloud::OSS::InitiateMultipartUploadRequest request(_bucket, _key);
    auto outcome = client->InitiateMultipartUpload(request);

    if (!outcome.isSuccess()) {
        std::string err = fmt::format("OSS InitiateMultipartUpload failed: {} - {}",
                                      outcome.error().Code(), outcome.error().Message());
        LOG(WARNING) << err << ", path: " << _path.native();
        return Status::IOError(err);
    }

    _upload_id = outcome.result().UploadId();
    LOG(INFO) << "OSS multipart upload initiated: " << _path.native()
              << " upload_id: " << _upload_id;

    return Status::OK();
}

Status OSSFileWriter::_upload_part(int part_num, const char* data, size_t size) {
    auto client = _client->get();
    if (!client) {
        return Status::InternalError("OSS client not initialized");
    }

    // Avoid double copy: write directly to stringstream instead of creating intermediate string
    auto content = std::make_shared<std::stringstream>();
    content->write(data, size);

    AlibabaCloud::OSS::UploadPartRequest request(_bucket, _key, content);
    request.setPartNumber(part_num);
    request.setUploadId(_upload_id);
    request.setContentLength(size);

    auto outcome = client->UploadPart(request);
    if (!outcome.isSuccess()) {
        _failed = true;
        std::string err = fmt::format("OSS UploadPart {} failed: {} - {}", part_num,
                                      outcome.error().Code(), outcome.error().Message());
        LOG(WARNING) << err << ", path: " << _path.native();
        return Status::IOError(err);
    }

    oss_bytes_written_total << size;

    AlibabaCloud::OSS::Part part(part_num, outcome.result().ETag());

    std::lock_guard<std::mutex> lock(_completed_lock);
    _completed_parts.push_back(part);

    VLOG_DEBUG << "OSS UploadPart " << part_num << " completed: " << _path.native() << " size: "
               << size;

    return Status::OK();
}

Status OSSFileWriter::_complete_multipart_upload() {
    auto client = _client->get();
    if (!client) {
        return Status::InternalError("OSS client not initialized");
    }

    // Sort parts by part number
    std::sort(_completed_parts.begin(), _completed_parts.end(),
              [](const AlibabaCloud::OSS::Part& a, const AlibabaCloud::OSS::Part& b) {
                  return a.PartNumber() < b.PartNumber();
              });

    AlibabaCloud::OSS::CompleteMultipartUploadRequest request(_bucket, _key, _completed_parts,
                                                               _upload_id);
    auto outcome = client->CompleteMultipartUpload(request);

    if (!outcome.isSuccess()) {
        _failed = true;
        std::string err = fmt::format("OSS CompleteMultipartUpload failed: {} - {}",
                                      outcome.error().Code(), outcome.error().Message());
        LOG(WARNING) << err << ", path: " << _path.native();
        // Abort to clean up parts
        static_cast<void>(_abort_multipart_upload());
        return Status::IOError(err);
    }

    LOG(INFO) << "OSS multipart upload completed: " << _path.native()
              << " parts: " << _completed_parts.size() << " bytes: " << _bytes_appended;

    oss_file_created_total << 1;
    return Status::OK();
}

Status OSSFileWriter::_abort_multipart_upload() {
    if (_upload_id.empty()) {
        return Status::OK();
    }

    auto client = _client->get();
    if (!client) {
        return Status::InternalError("OSS client not initialized");
    }

    AlibabaCloud::OSS::AbortMultipartUploadRequest request(_bucket, _key, _upload_id);
    auto outcome = client->AbortMultipartUpload(request);

    if (!outcome.isSuccess()) {
        std::string err = fmt::format("OSS AbortMultipartUpload failed: {} - {}",
                                      outcome.error().Code(), outcome.error().Message());
        LOG(WARNING) << err << ", path: " << _path.native() << " upload_id: " << _upload_id;
        return Status::IOError(err);
    }

    LOG(INFO) << "OSS multipart upload aborted: " << _path.native()
              << " upload_id: " << _upload_id;
    _upload_id.clear();

    return Status::OK();
}

Status OSSFileWriter::_put_object(const char* data, size_t size) {
    auto client = _client->get();
    if (!client) {
        return Status::InternalError("OSS client not initialized");
    }

    // Avoid double copy: write directly to stringstream
    auto content = std::make_shared<std::stringstream>();
    content->write(data, size);

    AlibabaCloud::OSS::PutObjectRequest request(_bucket, _key, content);
    auto outcome = client->PutObject(request);

    if (!outcome.isSuccess()) {
        _failed = true;
        std::string err = fmt::format("OSS PutObject failed: {} - {}", outcome.error().Code(),
                                      outcome.error().Message());
        LOG(WARNING) << err << ", path: " << _path.native();
        return Status::IOError(err);
    }

    LOG(INFO) << "OSS PutObject completed: " << _path.native() << " size: " << size;

    oss_file_created_total << 1;
    oss_bytes_written_total << size;

    return Status::OK();
}

Status OSSFileWriter::close(bool non_block) {
    if (state() == State::CLOSED) {
        return _st;
    }

    if (non_block) {
        LOG(WARNING) << "OSS file writer non-blocking close not yet implemented, using sync close";
    }

    _st = _close_impl();
    _state = State::CLOSED;
    return _st;
}

Status OSSFileWriter::_close_impl() {
    if (_failed) {
        // Abort multipart upload on failure to clean up parts
        if (!_upload_id.empty()) {
            static_cast<void>(_abort_multipart_upload());
        }
        return _st;
    }

    // Case 1: No data written - create empty file
    if (_bytes_appended == 0) {
        return _put_object("", 0);
    }

    // Case 2: Small file (single part) - use PutObject
    if (_cur_part_num == 1 && _upload_id.empty()) {
        return _put_object(_pending_buf.data(), _pending_buf.size());
    }

    // Case 3: Multipart upload - flush remaining data and complete
    if (!_pending_buf.empty()) {
        Status st = _upload_part(_cur_part_num, _pending_buf.data(), _pending_buf.size());
        if (!st.ok()) {
            // Abort on failure
            static_cast<void>(_abort_multipart_upload());
            return st;
        }
        _pending_buf.clear();
    }

    return _complete_multipart_upload();
}

} // namespace doris::io
