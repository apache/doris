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

#include "io/fs/gcs_file_system.h"

#include <utility>

#include "io/fs/gcs_file_writer.h"

namespace doris::io {

Result<std::shared_ptr<GcsFileSystem>> GcsFileSystem::create(
        std::map<std::string, std::string> properties, std::string id) {
    return std::shared_ptr<GcsFileSystem>(new GcsFileSystem(std::move(properties), std::move(id)));
}

GcsFileSystem::GcsFileSystem(std::map<std::string, std::string> properties, std::string id)
        : FileSystem(std::move(id), FileSystemType::S3), _properties(std::move(properties)) {}

Status GcsFileSystem::create_file_impl(const Path& file, FileWriterPtr* writer,
                                       const FileWriterOptions* /*opts*/) {
    *writer = DORIS_TRY(GcsFileWriter::create(file, _properties));
    return Status::OK();
}

Status GcsFileSystem::open_file_impl(const Path& /*file*/, FileReaderSPtr* /*reader*/,
                                     const FileReaderOptions* /*opts*/) {
    return Status::NotSupported("GCS OAuth filesystem is write-only; reads use HTTPS ranges");
}

Status GcsFileSystem::create_directory_impl(const Path& /*dir*/, bool /*failed_if_exists*/) {
    return Status::OK();
}

Status GcsFileSystem::delete_file_impl(const Path& file) {
    return GcsFileWriter::delete_object(file, _properties);
}

Status GcsFileSystem::batch_delete_impl(const std::vector<Path>& files) {
    for (const Path& file : files) {
        RETURN_IF_ERROR(delete_file_impl(file));
    }
    return Status::OK();
}

Status GcsFileSystem::delete_directory_impl(const Path& /*dir*/) {
    return Status::NotSupported("GCS OAuth recursive delete is not supported");
}

Status GcsFileSystem::exists_impl(const Path& /*path*/, bool* /*res*/) const {
    return Status::NotSupported("GCS OAuth metadata lookup is not supported");
}

Status GcsFileSystem::file_size_impl(const Path& /*file*/, int64_t* /*file_size*/) const {
    return Status::NotSupported("GCS OAuth metadata lookup is not supported");
}

Status GcsFileSystem::list_impl(const Path& /*dir*/, bool /*only_file*/,
                                std::vector<FileInfo>* /*files*/, bool* /*exists*/) {
    return Status::NotSupported("GCS OAuth listing is not supported");
}

Status GcsFileSystem::rename_impl(const Path& /*orig_name*/, const Path& /*new_name*/) {
    return Status::NotSupported("GCS OAuth rename is not supported");
}

Status GcsFileSystem::absolute_path(const Path& path, Path& abs_path) const {
    if (path.string().find("://") == std::string::npos) {
        return Status::InvalidArgument("GCS OAuth filesystem requires an absolute gs:// path");
    }
    abs_path = path;
    return Status::OK();
}

} // namespace doris::io
