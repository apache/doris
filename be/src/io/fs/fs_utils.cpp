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

#include "io/fs/fs_utils.h"

#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"

namespace doris {
namespace io {

Status read_file_to_string(FileSystemSPtr fs, const Path& file, std::string* content) {
    FileReaderSPtr file_reader;
    RETURN_IF_ERROR(fs->open_file(file, &file_reader));
    size_t file_size = file_reader->size();
    content->resize(file_size);
    size_t bytes_read = 0;
    RETURN_IF_ERROR(file_reader->read_at(0, {*content}, &bytes_read));
    if (bytes_read != file_size) {
        return Status::IOError("failed to read file {} to string. bytes read: {}, file size: {}",
                               file.native(), bytes_read, file_size);
    }
    return file_reader->close();
}

} // namespace io
} // namespace doris
