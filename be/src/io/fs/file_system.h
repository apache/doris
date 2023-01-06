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

#include "common/status.h"
#include "gutil/macros.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_reader_options.h"
#include "io/fs/file_writer.h"
#include "io/fs/path.h"

namespace doris {
namespace io {

class FileWriter;
class FileReader;

// Currently, FE use resource name to identify a Resource.
using ResourceId = std::string;

enum class FileSystemType : uint8_t {
    LOCAL,
    S3,
    HDFS,
    BROKER,
};

class FileSystem {
public:
    FileSystem(Path&& root_path, ResourceId&& resource_id, FileSystemType type)
            : _root_path(std::move(root_path)), _resource_id(std::move(resource_id)), _type(type) {}

    virtual ~FileSystem() = default;

    DISALLOW_COPY_AND_ASSIGN(FileSystem);

    virtual Status create_file(const Path& path, FileWriterPtr* writer) = 0;

    virtual Status open_file(const Path& path, const FileReaderOptions& reader_options,
                             FileReaderSPtr* reader) = 0;

    virtual Status open_file(const Path& path, FileReaderSPtr* reader) = 0;

    virtual Status delete_file(const Path& path) = 0;

    // create directory recursively
    virtual Status create_directory(const Path& path) = 0;

    // remove all under directory recursively
    virtual Status delete_directory(const Path& path) = 0;

    // hard link `src` to `dest`
    // FIXME(cyx): Should we move this method to LocalFileSystem?
    virtual Status link_file(const Path& src, const Path& dest) = 0;

    virtual Status exists(const Path& path, bool* res) const = 0;

    virtual Status file_size(const Path& path, size_t* file_size) const = 0;

    virtual Status list(const Path& path, std::vector<Path>* files) = 0;

    const Path& root_path() const { return _root_path; }
    const ResourceId& resource_id() const { return _resource_id; }
    const FileSystemType type() const { return _type; }

protected:
    Path _root_path;
    ResourceId _resource_id;
    FileSystemType _type;
};

using FileSystemSPtr = std::shared_ptr<FileSystem>;

} // namespace io
} // namespace doris
