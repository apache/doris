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

#include "io/fs/http_file_system.h"


#include <fstream>

#include "common/status.h"
#include "http/http_status.h"
#include "io/fs/err_utils.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/http_file_reader.h"

namespace doris::io {
HttpFileSystem::HttpFileSystem(Path&& root_path, std::string id)
        : RemoteFileSystem(std::move(root_path), std::move(id), FileSystemType::HTTP) {}

Status HttpFileSystem::init(const std::string& url) {
    _url = url;
    return Status::OK();
}

Result<std::shared_ptr<HttpFileSystem>> HttpFileSystem::create(std::string id,
                                                               const std::string& url) {
    Path root_path = "";
    std::shared_ptr<HttpFileSystem> fs(new HttpFileSystem(std::move(root_path), std::move(id)));

    RETURN_IF_ERROR_RESULT(fs->init(url));

    return fs;
}

Status HttpFileSystem::open_file_internal(const Path& path, FileReaderSPtr* reader,
                                          const FileReaderOptions& opts) {
    OpenFileInfo file_info;
    file_info.path = path;
    auto http_reader = std::make_shared<HttpFileReader>(file_info, path.native());
    RETURN_IF_ERROR(http_reader->open(opts));
    *reader = http_reader;
    return Status::OK();
}

Status HttpFileSystem::download_impl(const Path& remote_file, const Path& local_file) {
    FileReaderSPtr reader;
    RETURN_IF_ERROR(open_file(remote_file, &reader));

    auto *http_reader = dynamic_cast<HttpFileReader*>(reader.get());
    if(http_reader == nullptr) {
        return Status::InternalError("Expected HttpFileReader");
    }

    int64_t total_size = http_reader->size();
    std::vector<char> buffer(total_size);

    RETURN_IF_ERROR(http_reader->read_range(0, total_size, buffer.data()));

    std::ofstream ofs(local_file.native(), std::ios::binary);

    if (!ofs) {
        return Status::IOError("Failed to open local file: {}", local_file.native());
    }

    ofs.write(buffer.data(), total_size);
    return Status::OK();
}

Status HttpFileSystem::file_size_impl(const Path& file, int64_t* file_size) const {
    FileReaderOptions opts;
    FileReaderSPtr reader;
    RETURN_IF_ERROR(const_cast<HttpFileSystem*>(this)->open_file(file, &reader, &opts));
    *file_size = reader->size();
    RETURN_IF_ERROR(reader->close());
    return Status::OK();
}

Status HttpFileSystem::exists_impl(const Path& path, bool* res) const {
    FileReaderSPtr reader;
    auto st = const_cast<HttpFileSystem*>(this)->open_file(path, &reader);
    if (st.ok()) {
        *res = true;
        return Status::OK();
    } else if (st.code() == HttpStatus::NOT_FOUND) {
        *res = false;
        return Status::OK();
    } else {
        return st;
    }
}

} // namespace doris::io