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

#include "cloud/cloud_storage_engine.h"
#include "common/status.h"
#include "io/fs/file_reader_writer_fwd.h"

namespace doris {

class DeleteBitmapPB;

class DeleteBitmapFileWriter {
public:
    explicit DeleteBitmapFileWriter(int64_t tablet_id, const std::string& rowset_id,
                                    std::optional<StorageResource>& storage_resource);
    ~DeleteBitmapFileWriter();

    Status init();
    Status write(const DeleteBitmapPB& delete_bitmap);
    Status close();

public:
    static constexpr const char* DELETE_BITMAP_MAGIC = "DBM1";
    static const uint32_t MAGIC_SIZE = 4;
    static const int64_t LENGTH_SIZE = sizeof(uint64_t);
    static const int64_t CHECKSUM_SIZE = sizeof(uint32_t);

private:
    int64_t _tablet_id;
    std::string _rowset_id;
    std::optional<StorageResource> _storage_resource;
    std::string _path;
    io::FileWriterPtr _file_writer;
};

} // namespace doris