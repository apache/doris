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

#include "util/s3_storage_backend.h"

#include <aws/s3/S3Client.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <boost/algorithm/string.hpp>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>

#include "common/logging.h"
#include "gutil/strings/strip.h"
#include "util/s3_uri.h"
#include "util/s3_util.h"

namespace doris {

#ifndef CHECK_S3_CLIENT
#define CHECK_S3_CLIENT(client)                                    \
    if (!client) {                                                 \
        return Status::InternalError("init aws s3 client error."); \
    }
#endif

#ifndef CHECK_S3_PATH
#define CHECK_S3_PATH(uri, path)                                      \
    S3URI uri(path);                                                  \
    if (!uri.parse()) {                                               \
        return Status::InvalidArgument("s3 uri is invalid: " + path); \
    }
#endif

#ifndef RETRUN_S3_STATUS
#define RETRUN_S3_STATUS(response)                         \
    if (response.IsSuccess()) {                            \
        return Status::OK();                               \
    } else {                                               \
        return Status::InternalError(error_msg(response)); \
    }
#endif

S3StorageBackend::S3StorageBackend(const std::map<std::string, std::string>& prop)
        : _properties(prop) {
    _client = ClientFactory::instance().create(_properties);
    DCHECK(_client) << "init aws s3 client error.";
}

S3StorageBackend::~S3StorageBackend() {}

Status S3StorageBackend::download(const std::string& remote, const std::string& local) {
    CHECK_S3_CLIENT(_client);
    CHECK_S3_PATH(uri, remote);
    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(uri.get_bucket()).WithKey(uri.get_key());
    Aws::S3::Model::GetObjectOutcome response = _client->GetObject(request);
    if (response.IsSuccess()) {
        Aws::OFStream local_file;
        local_file.open(local, std::ios::out | std::ios::binary);
        if (local_file.good()) {
            local_file << response.GetResult().GetBody().rdbuf();
        }
        if (!local_file.good()) {
            return Status::InternalError("failed to write file: " + local);
        }
    } else {
        return Status::IOError("s3 download error: " + error_msg(response));
    }
    return Status::OK();
}

Status S3StorageBackend::direct_download(const std::string& remote, std::string* content) {
    CHECK_S3_CLIENT(_client);
    CHECK_S3_PATH(uri, remote);
    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(uri.get_bucket()).WithKey(uri.get_key());
    Aws::S3::Model::GetObjectOutcome response = _client->GetObject(request);
    if (response.IsSuccess()) {
        std::stringstream ss;
        ss << response.GetResult().GetBody().rdbuf();
        *content = ss.str();
    } else {
        return Status::IOError("s3 direct_download error: " + error_msg(response));
    }
    return Status::OK();
}

Status S3StorageBackend::upload(const std::string& local, const std::string& remote) {
    CHECK_S3_CLIENT(_client);
    CHECK_S3_PATH(uri, remote);
    Aws::S3::Model::PutObjectRequest request;
    request.WithBucket(uri.get_bucket()).WithKey(uri.get_key());

    const std::shared_ptr<Aws::IOStream> input_data = Aws::MakeShared<Aws::FStream>(
            local.c_str(), local.c_str(), std::ios_base::in | std::ios_base::binary);
    if (input_data->good()) {
        request.SetBody(input_data);
    }
    if (!input_data->good()) {
        return Status::InternalError("failed to read file: " + local);
    }
    Aws::S3::Model::PutObjectOutcome response = _client->PutObject(request);

    RETRUN_S3_STATUS(response);
}

Status S3StorageBackend::list(const std::string& remote_path, bool contain_md5,
                              bool recursion, std::map<std::string, FileStat>* files) {
    std::string normal_str(remote_path);
    if (!normal_str.empty() && normal_str.at(normal_str.size() - 1) != '/') {
        normal_str += '/';
    }
    CHECK_S3_CLIENT(_client);
    CHECK_S3_PATH(uri, normal_str);

    Aws::S3::Model::ListObjectsRequest request;
    request.WithBucket(uri.get_bucket()).WithPrefix(uri.get_key());
    if (!recursion) {
        request.WithDelimiter("/");
    }
    Aws::S3::Model::ListObjectsOutcome response = _client->ListObjects(request);
    if (response.IsSuccess()) {
        Aws::Vector<Aws::S3::Model::Object> objects = response.GetResult().GetContents();

        for (Aws::S3::Model::Object& object : objects) {
            std::string key = object.GetKey();
            if (key.at(key.size() - 1) == '/') {
                continue;
            }
            key = boost::algorithm::replace_first_copy(key, uri.get_key(), "");
            if (contain_md5) {
                size_t pos = key.find_last_of(".");
                if (pos == std::string::npos || pos == key.size() - 1) {
                    // Not found checksum separator, ignore this file
                    continue;
                }
                FileStat stat = {std::string(key, 0, pos), std::string(key, pos + 1), object.GetSize()};
                files->emplace(std::string(key, 0, pos), stat);
            } else {
                FileStat stat = {key, "", object.GetSize()};
                files->emplace(key, stat);
            }
        }
        return Status::OK();
    } else {
        return Status::InternalError("list form s3 error: " + error_msg(response));
    }
}

Status S3StorageBackend::rename(const std::string& orig_name, const std::string& new_name) {
    RETURN_IF_ERROR(copy(orig_name, new_name));
    return rm(orig_name);
}

Status S3StorageBackend::rename_dir(const std::string& orig_name, const std::string& new_name) {
    RETURN_IF_ERROR(copy_dir(orig_name, new_name));
    return rmdir(orig_name);
}

Status S3StorageBackend::direct_upload(const std::string& remote, const std::string& content) {
    CHECK_S3_CLIENT(_client);
    CHECK_S3_PATH(uri, remote);
    Aws::S3::Model::PutObjectRequest request;
    request.WithBucket(uri.get_bucket()).WithKey(uri.get_key());
    const std::shared_ptr<Aws::IOStream> input_data =
            Aws::MakeShared<Aws::StringStream>("upload_directly");
    *input_data << content.c_str();
    if (input_data->good()) {
        request.SetBody(input_data);
    }
    if (!input_data->good()) {
        return Status::InternalError("failed to read from string");
    }
    Aws::S3::Model::PutObjectOutcome response = _client->PutObject(request);

    RETRUN_S3_STATUS(response);
}

Status S3StorageBackend::rm(const std::string& remote) {
    CHECK_S3_CLIENT(_client);
    CHECK_S3_PATH(uri, remote);
    Aws::S3::Model::DeleteObjectRequest request;

    request.WithKey(uri.get_key()).WithBucket(uri.get_bucket());

    Aws::S3::Model::DeleteObjectOutcome response = _client->DeleteObject(request);

    RETRUN_S3_STATUS(response);
}

Status S3StorageBackend::rmdir(const std::string& remote) {
    CHECK_S3_CLIENT(_client);
    std::map<std::string, FileStat> files;
    std::string normal_path(remote);
    if (!normal_path.empty() && normal_path.at(normal_path.size() - 1) != '/') {
        normal_path += '/';
    }
    LOG(INFO) << "Remove S3 dir: " << remote;
    RETURN_IF_ERROR(list(normal_path, false, true, &files));

    for (auto &file : files) {
        std::string file_path = normal_path + file.second.name;
        RETURN_IF_ERROR(rm(file_path));
    }
    return Status::OK();
}

Status S3StorageBackend::copy(const std::string& src, const std::string& dst) {
    CHECK_S3_CLIENT(_client);
    CHECK_S3_PATH(src_uri, src);
    CHECK_S3_PATH(dst_uri, dst);
    Aws::S3::Model::CopyObjectRequest request;
    request.WithCopySource(src_uri.get_bucket() + "/" + src_uri.get_key())
            .WithKey(dst_uri.get_key())
            .WithBucket(dst_uri.get_bucket());

    Aws::S3::Model::CopyObjectOutcome response = _client->CopyObject(request);

    RETRUN_S3_STATUS(response);
}

Status S3StorageBackend::copy_dir(const std::string& src, const std::string& dst) {
    std::map<std::string, FileStat> files;
    LOG(INFO) << "Copy S3 dir: " << src << " -> " << dst;
    RETURN_IF_ERROR(list(src, false, true, &files));
    if (files.size() == 0) {
        LOG(WARNING) << "Nothing need to copy: " << src << " -> " << dst;
        return Status::OK();
    }
    for (auto &kv : files) {
        RETURN_IF_ERROR(copy(src + "/" + kv.first, dst + "/" + kv.first));
    }
    return Status::OK();
}

// dir is not supported by s3, it will cause something confusion.
Status S3StorageBackend::mkdir(const std::string& path) {
    return Status::OK();
}

// dir is not supported by s3, it will cause something confusion.
Status S3StorageBackend::mkdirs(const std::string& path) {
    return Status::OK();
}

Status S3StorageBackend::exist(const std::string& path) {
    CHECK_S3_CLIENT(_client);
    CHECK_S3_PATH(uri, path);
    Aws::S3::Model::HeadObjectRequest request;
    request.WithBucket(uri.get_bucket()).WithKey(uri.get_key());
    Aws::S3::Model::HeadObjectOutcome response = _client->HeadObject(request);
    if (response.IsSuccess()) {
        return Status::OK();
    } else if (response.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        return Status::NotFound(path + " not exists!");
    } else {
        return Status::InternalError(error_msg(response));
    }
}

Status S3StorageBackend::exist_dir(const std::string& path) {
    std::map<std::string, FileStat> files;
    RETURN_IF_ERROR(list(path, false, true, &files));
    if (files.size() > 0) {
        return Status::OK();
    }
    return Status::NotFound(path + " not exists!");
}

Status S3StorageBackend::upload_with_checksum(const std::string& local, const std::string& remote,
                                              const std::string& checksum) {
    return upload(local, remote + "." + checksum);
}

template <typename AwsOutcome>
std::string S3StorageBackend::error_msg(const AwsOutcome& outcome) {
    std::stringstream out;
    out << "Error: [" << outcome.GetError().GetExceptionName() << ":"
        << outcome.GetError().GetMessage();
    return out.str();
}

} // end namespace doris
