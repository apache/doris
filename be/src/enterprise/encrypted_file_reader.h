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

#include "io/fs/file_reader.h"
#include "io/fs/file_reader_writer_fwd.h"

namespace doris::io {

struct EncryptionInfo;

class EncryptedFileReader : public FileReader {
public:
    EncryptedFileReader(FileReaderSPtr inner, std::unique_ptr<EncryptionInfo> encryption_info,
                        size_t file_size)
            : _reader_inner(std::move(inner)),
              _encryption_info(std::move(encryption_info)),
              _file_size(file_size) {}

    Status close() override;

    const Path& path() const override;

    size_t size() const override;

    bool closed() const override;

private:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_ctx) override;

    FileReaderSPtr _reader_inner;
    std::unique_ptr<const EncryptionInfo> _encryption_info;
    size_t _file_size;
};

} // namespace doris::io
