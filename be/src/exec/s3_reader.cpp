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

#include "exec/s3_reader.h"

#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>

#include "common/logging.h"
#include "gutil/strings/strcat.h"
#include "service/backend_options.h"
#include "util/s3_util.h"

namespace doris {

#ifndef CHECK_S3_CLIENT
#define CHECK_S3_CLIENT(client)                                    \
    if (!client) {                                                 \
        return Status::InternalError("init aws s3 client error."); \
    }
#endif

S3Reader::S3Reader(const std::map<std::string, std::string>& properties, const std::string& path,
                   int64_t start_offset)
        : _properties(properties),
          _path(path),
          _uri(path),
          _cur_offset(start_offset),
          _file_size(0),
          _closed(false),
          _client(ClientFactory::instance().create(_properties)) {
    DCHECK(_client) << "init aws s3 client error.";
}

S3Reader::~S3Reader() {}

Status S3Reader::open() {
    CHECK_S3_CLIENT(_client);
    if (!_uri.parse()) {
        return Status::InvalidArgument("s3 uri is invalid: " + _path);
    }
    Aws::S3::Model::HeadObjectRequest request;
    request.WithBucket(_uri.get_bucket()).WithKey(_uri.get_key());
    Aws::S3::Model::HeadObjectOutcome response = _client->HeadObject(request);
    if (response.IsSuccess()) {
        _file_size = response.GetResult().GetContentLength();
        return Status::OK();
    } else if (response.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        return Status::NotFound(_path + " not exists!");
    } else {
        std::stringstream out;
        out << "Error: [" << response.GetError().GetExceptionName() << ":"
            << response.GetError().GetMessage() << "] at " << BackendOptions::get_localhost();
        return Status::InternalError(out.str());
    }
}

Status S3Reader::read(uint8_t* buf, int64_t buf_len, int64_t* bytes_read, bool* eof) {
    DCHECK_NE(buf_len, 0);
    RETURN_IF_ERROR(readat(_cur_offset, buf_len, bytes_read, buf));
    if (*bytes_read == 0) {
        *eof = true;
    } else {
        *eof = false;
    }
    return Status::OK();
}

Status S3Reader::readat(int64_t position, int64_t nbytes, int64_t* bytes_read, void* out) {
    CHECK_S3_CLIENT(_client);
    if (position >= _file_size) {
        *bytes_read = 0;
        VLOG_FILE << "Read end of file: " + _path;
        return Status::OK();
    }
    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(_uri.get_bucket()).WithKey(_uri.get_key());
    string bytes = StrCat("bytes=", position, "-");
    if (position + nbytes < _file_size) {
        bytes = StrCat(bytes.c_str(), position + nbytes - 1);
    }
    request.SetRange(bytes.c_str());
    auto response = _client->GetObject(request);
    if (!response.IsSuccess()) {
        *bytes_read = 0;
        std::stringstream out;
        out << "Error: [" << response.GetError().GetExceptionName() << ":"
            << response.GetError().GetMessage() << "] at " << BackendOptions::get_localhost();
        LOG(INFO) << out.str();
        return Status::InternalError(out.str());
    }
    *bytes_read = response.GetResult().GetContentLength();
    *bytes_read = nbytes < *bytes_read ? nbytes : *bytes_read;
    _cur_offset = position + *bytes_read;
    response.GetResult().GetBody().read((char*)out, *bytes_read);
    return Status::OK();
}

Status S3Reader::read_one_message(std::unique_ptr<uint8_t[]>* buf, int64_t* length) {
    bool eof;
    int64_t file_size = size() - _cur_offset;
    if (file_size <= 0) {
        buf->reset();
        *length = 0;
        return Status::OK();
    }
    buf->reset(new uint8_t[file_size]);
    read(buf->get(), file_size, length, &eof);
    return Status::OK();
}

int64_t S3Reader::size() {
    return _file_size;
}

Status S3Reader::seek(int64_t position) {
    _cur_offset = position;
    return Status::OK();
}

Status S3Reader::tell(int64_t* position) {
    *position = _cur_offset;
    return Status::OK();
}

void S3Reader::close() {
    _closed = true;
}

bool S3Reader::closed() {
    return _closed;
}

} // end namespace doris
