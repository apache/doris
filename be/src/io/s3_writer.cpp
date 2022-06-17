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

#include "io/s3_writer.h"

#include <aws/core/utils/FileSystemUtils.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>

#include "runtime/exec_env.h"
#include "runtime/tmp_file_mgr.h"
#include "service/backend_options.h"
#include "util/s3_uri.h"
#include "util/s3_util.h"

namespace doris {

#ifndef CHECK_S3_CLIENT
#define CHECK_S3_CLIENT(client)                                    \
    if (!client) {                                                 \
        return Status::InternalError("init aws s3 client error."); \
    }
#endif

S3Writer::S3Writer(const std::map<std::string, std::string>& properties, const std::string& path,
                   int64_t start_offset)
        : _properties(properties),
          _path(path),
          _uri(path),
          _client(ClientFactory::instance().create(_properties)) {
    std::string tmp_path = ExecEnv::GetInstance()->tmp_file_mgr()->get_tmp_dir_path();
    LOG(INFO) << "init aws s3 client with tmp path " << tmp_path;
    if (tmp_path.at(tmp_path.size() - 1) != '/') {
        tmp_path.append("/");
    }
    _temp_file = std::make_shared<Aws::Utils::TempFile>(
            tmp_path.c_str(), ".doris_tmp",
            std::ios_base::binary | std::ios_base::trunc | std::ios_base::in | std::ios_base::out);
    DCHECK(_client) << "init aws s3 client error.";
}

S3Writer::~S3Writer() {
    close();
}

Status S3Writer::open() {
    CHECK_S3_CLIENT(_client);
    if (!_uri.parse()) {
        return Status::InvalidArgument("s3 uri is invalid: " + _path);
    }
    Aws::S3::Model::HeadObjectRequest request;
    request.WithBucket(_uri.get_bucket()).WithKey(_uri.get_key());
    Aws::S3::Model::HeadObjectOutcome response = _client->HeadObject(request);
    if (response.IsSuccess()) {
        return Status::AlreadyExist(_path + " already exists.");
    } else if (response.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        return Status::OK();
    } else {
        std::stringstream out;
        out << "Error: [" << response.GetError().GetExceptionName() << ":"
            << response.GetError().GetMessage() << "] at " << BackendOptions::get_localhost();
        return Status::InternalError(out.str());
    }
}

Status S3Writer::write(const uint8_t* buf, size_t buf_len, size_t* written_len) {
    if (buf_len == 0) {
        *written_len = 0;
        return Status::OK();
    }
    if (!_temp_file) {
        RETURN_NOT_OK_STATUS_WITH_WARN(
                Status::BufferAllocFailed(
                        fmt::format("The internal temporary file is not writable for {}. at {}",
                                    strerror(errno), BackendOptions::get_localhost())),
                "write temp file error");
    }
    _temp_file->write(reinterpret_cast<const char*>(buf), buf_len);
    if (!_temp_file->good()) {
        RETURN_NOT_OK_STATUS_WITH_WARN(
                Status::BufferAllocFailed(
                        fmt::format("Could not append to the internal temporary file for {}. at {}",
                                    strerror(errno), BackendOptions::get_localhost())),
                "write temp file error");
    }
    *written_len = buf_len;
    return Status::OK();
}

Status S3Writer::close() {
    if (_temp_file) {
        RETURN_IF_ERROR(_sync());
        _temp_file.reset();
    }
    return Status::OK();
}

Status S3Writer::_sync() {
    if (!_temp_file) {
        RETURN_NOT_OK_STATUS_WITH_WARN(
                Status::BufferAllocFailed(
                        fmt::format("The internal temporary file is not writable for {}. at {}",
                                    strerror(errno), BackendOptions::get_localhost())),
                "write temp file error");
    }
    CHECK_S3_CLIENT(_client);
    Aws::S3::Model::PutObjectRequest request;
    request.WithBucket(_uri.get_bucket()).WithKey(_uri.get_key());
    long offset = _temp_file->tellp();
    _temp_file->seekg(0);
    request.SetBody(_temp_file);
    request.SetContentLength(offset);
    auto response = _client->PutObject(request);
    _temp_file->clear();
    _temp_file->seekp(offset);
    if (response.IsSuccess()) {
        return Status::OK();
    } else {
        std::stringstream out;
        out << "Error: [" << response.GetError().GetExceptionName() << ":"
            << response.GetError().GetMessage() << "] at " << BackendOptions::get_localhost();
        return Status::InternalError(out.str());
    }
}
} // end namespace doris
