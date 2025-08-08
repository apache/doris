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

class DeleteBitmapFileReader {
public:
    explicit DeleteBitmapFileReader(int64_t tablet_id, const std::string& rowset_id,
                                    std::optional<StorageResource>& storage_resource);
    ~DeleteBitmapFileReader();

    Status init();
    Status close();
    Status read(DeleteBitmapPB& delete_bitmap);

private:
    int64_t _tablet_id;
    std::string _rowset_id;
    std::optional<StorageResource> _storage_resource;
    std::string _path;
    io::FileReaderSPtr _file_reader;
};

} // namespace doris