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

#include <atomic>

#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_writer.h"
#include "util/once.h"

namespace doris::io {

struct EncryptionInfo;

class EncryptedFileWriter : public FileWriter {
public:
    EncryptedFileWriter(FileWriterPtr inner,
                        const std::shared_ptr<const EncryptionInfo>& encryption_info)
            : _writer_inner(std::move(inner)), _encryption_info(encryption_info) {}
    ~EncryptedFileWriter() override;

    Status close(bool non_block = false) override;

    Status appendv(const Slice* data, size_t data_cnt) override;

    const Path& path() const override;

    size_t bytes_appended() const override;

    State state() const override;

    //FileCacheAllocatorBuilder* cache_builder() const override;

private:
    std::atomic_bool _is_footer_written {false};
    DorisCallOnce<Status> _write_footer_once;
    FileWriterPtr _writer_inner;
    std::shared_ptr<const EncryptionInfo> _encryption_info;
    size_t _bytes_appended = 0;
};

} // namespace doris::io
