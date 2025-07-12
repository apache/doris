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

#include "cloud/cloud_storage_engine.h"
#include "io/fs/file_writer.h"
#include "util/crc32c.h"

namespace doris {

DeleteBitmapFileWriter::DeleteBitmapFileWriter(int64_t tablet_id, const std::string& rowset_id)
        : _tablet_id(tablet_id), _rowset_id(rowset_id) {}

DeleteBitmapFileWriter::~DeleteBitmapFileWriter() {}

Status DeleteBitmapFileWriter::init() {
    CloudStorageEngine& engine = ExecEnv::GetInstance()->storage_engine().to_cloud();
    std::string vault_id = "1"; // TODO: make this configurable or pass as a parameter
    auto storage_resource = engine.get_storage_resource(vault_id);
    if (!storage_resource) {
        return Status::InternalError("vault id not found, maybe not sync, vault id {}", vault_id);
    }
    _path = storage_resource->remote_delete_bitmap_path(_tablet_id, _rowset_id);
    LOG(INFO) << "sout: delete bitmap file path=" << _path;
    io::FileWriterOptions opts;
    // opts.write_file_cache = true;
    return storage_resource->fs->create_file(_path, &_file_writer, &opts);
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

} // namespace doris
