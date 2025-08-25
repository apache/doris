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

#include "cloud/delete_bitmap_file_writer.h"

#include "io/fs/file_writer.h"
#include "util/crc32c.h"

namespace doris {
#include "common/compile_check_begin.h"

DeleteBitmapFileWriter::DeleteBitmapFileWriter(int64_t tablet_id, const std::string& rowset_id,
                                               std::optional<StorageResource>& storage_resource)
        : _tablet_id(tablet_id), _rowset_id(rowset_id), _storage_resource(storage_resource) {}

DeleteBitmapFileWriter::~DeleteBitmapFileWriter() {}

Status DeleteBitmapFileWriter::init() {
#ifdef BE_TEST
    _path = "./log/" + _rowset_id + "_delete_bitmap.dat";
    io::Path path = _path;
    auto parent_path = path.parent_path();
    bool exists = false;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(parent_path, &exists));
    if (!exists) {
        RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(parent_path));
    }
    RETURN_IF_ERROR(io::global_local_filesystem()->create_file(_path, &_file_writer));
    return Status::OK();
#endif
    if (!_storage_resource) {
        return Status::InternalError("invalid storage resource for tablet_id={}", _tablet_id);
    }
    _path = _storage_resource->remote_delete_bitmap_path(_tablet_id, _rowset_id);
    io::FileWriterOptions opts;
    // opts.write_file_cache = true;
    return _storage_resource->fs->create_file(_path, &_file_writer, &opts);
}

Status DeleteBitmapFileWriter::close() {
    if (!_file_writer) {
        return Status::InternalError("fail to close delete bitmap file={} because writer is null",
                                     _path);
    }
    auto st = _file_writer->close();
    if (!st.ok()) {
        LOG(WARNING) << "failed to close delete bitmap file=" << _path << ", st=" << st.to_string();
    }
    return st;
}

Status DeleteBitmapFileWriter::write(const DeleteBitmapPB& delete_bitmap) {
    if (!_file_writer) {
        return Status::InternalError("fail to write delete bitmap file={} because writer is null",
                                     _path);
    }
    // 0. write magic
    RETURN_IF_ERROR(_file_writer->append({DELETE_BITMAP_MAGIC, MAGIC_SIZE}));

    // 1. write delete bitmap length
    uint64_t delete_bitmap_len = delete_bitmap.ByteSizeLong();
    uint8_t len_buf[LENGTH_SIZE];
    encode_fixed64_le(len_buf, delete_bitmap_len);
    RETURN_IF_ERROR(_file_writer->append({len_buf, LENGTH_SIZE}));

    // 2. write delete bitmap
    std::string content = delete_bitmap.SerializeAsString();
    RETURN_IF_ERROR(_file_writer->append(content));

    // 3. write checksum
    uint8_t checksum_buf[CHECKSUM_SIZE];
    uint32_t checksum = crc32c::Value(content.data(), delete_bitmap_len);
    encode_fixed32_le(checksum_buf, checksum);
    RETURN_IF_ERROR(_file_writer->append({checksum_buf, CHECKSUM_SIZE}));
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris
