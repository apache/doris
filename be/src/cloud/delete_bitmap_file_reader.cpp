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

#include "cloud/delete_bitmap_file_reader.h"

#include "cloud/delete_bitmap_file_writer.h"
#include "common/status.h"
#include "io/fs/file_reader.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace doris {
#include "common/compile_check_begin.h"

DeleteBitmapFileReader::DeleteBitmapFileReader(int64_t tablet_id, const std::string& rowset_id,
                                               std::optional<StorageResource>& storage_resource)
        : _tablet_id(tablet_id), _rowset_id(rowset_id), _storage_resource(storage_resource) {}

DeleteBitmapFileReader::~DeleteBitmapFileReader() = default;

Status DeleteBitmapFileReader::init() {
#ifdef BE_TEST
    _path = "./log/" + _rowset_id + "_delete_bitmap.db";
    bool exists = false;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(_path, &exists));
    if (!exists) {
        return Status::NotFound("{} doesn't exist", _path);
    }
    RETURN_IF_ERROR(io::global_local_filesystem()->open_file(_path, &_file_reader));
    return Status::OK();
#endif
    if (!_storage_resource) {
        return Status::InternalError("invalid storage resource for tablet_id={}", _tablet_id);
    }
    _path = _storage_resource->remote_delete_bitmap_path(_tablet_id, _rowset_id);
    io::FileReaderOptions opts;
    return _storage_resource->fs->open_file(_path, &_file_reader, &opts);
}

Status DeleteBitmapFileReader::close() {
    if (_file_reader) {
        return _file_reader->close();
    }
    return Status::OK();
}

Status DeleteBitmapFileReader::read(DeleteBitmapPB& delete_bitmap) {
    size_t bytes_read = 0;
    size_t offset = 0;
    // 0. read magic number
    {
        uint8_t magic_buf[DeleteBitmapFileWriter::MAGIC_SIZE];
        RETURN_IF_ERROR(_file_reader->read_at(
                offset, {magic_buf, DeleteBitmapFileWriter::MAGIC_SIZE}, &bytes_read));
        offset += DeleteBitmapFileWriter::MAGIC_SIZE;
        if (bytes_read != DeleteBitmapFileWriter::MAGIC_SIZE ||
            memcmp(magic_buf, DeleteBitmapFileWriter::DELETE_BITMAP_MAGIC,
                   DeleteBitmapFileWriter::MAGIC_SIZE) != 0) {
            return Status::InternalError(
                    "read delete bitmap failed from {} because magic is not "
                    "matched",
                    _path);
        }
    }
    // 1. read delete bitmap proto length
    size_t delete_bitmap_len = 0;
    {
        uint8_t len_buf[DeleteBitmapFileWriter::LENGTH_SIZE];
        RETURN_IF_ERROR(_file_reader->read_at(
                offset, {len_buf, DeleteBitmapFileWriter::LENGTH_SIZE}, &bytes_read));
        offset += DeleteBitmapFileWriter::LENGTH_SIZE;
        delete_bitmap_len = decode_fixed64_le(len_buf);
        if (delete_bitmap_len == 0) {
            return Status::InternalError("read delete bitmap failed from {} because length is 0",
                                         _path);
        }
        if (offset == _file_reader->size()) {
            LOG(WARNING) << "read delete bitmap failed because reach end of file=" << _path
                         << ", file size=" << _file_reader->size() << ", offset=" << offset
                         << ", delete_bitmap_len=" << delete_bitmap_len;
            return Status::InternalError(
                    "read delete bitmap failed from {} because reach end of file", _path);
        }
    }
    // 2. read delete bitmap
    std::string delete_bitmap_buf;
    {
        delete_bitmap_buf.resize(delete_bitmap_len);
        RETURN_IF_ERROR(_file_reader->read_at(
                offset, {delete_bitmap_buf.c_str(), delete_bitmap_len}, &bytes_read));
        offset += delete_bitmap_len;
        if (!delete_bitmap.ParseFromString(delete_bitmap_buf)) {
            LOG(WARNING) << "deserialize delete bitmap failed from file=" << _path
                         << ", file size=" << _file_reader->size()
                         << ", delete_bitmap_len=" << delete_bitmap_len
                         << ", bytes_read=" << bytes_read;
            return Status::InternalError("deserialize delete bitmap failed from {}", _path);
        }
    }
    // 3. checksum
    uint8_t checksum_len_buf[DeleteBitmapFileWriter::CHECKSUM_SIZE];
    RETURN_IF_ERROR(_file_reader->read_at(
            offset, {checksum_len_buf, DeleteBitmapFileWriter::CHECKSUM_SIZE}, &bytes_read));
    offset += DeleteBitmapFileWriter::CHECKSUM_SIZE;
    uint32_t checksum = decode_fixed32_le(checksum_len_buf);
    uint32_t computed_checksum = crc32c::Value(delete_bitmap_buf.data(), delete_bitmap_len);
    if (computed_checksum != checksum) {
        return Status::InternalError("delete bitmap checksum failed from file=" + _path +
                                     ", computed checksum=" + std::to_string(computed_checksum) +
                                     ", expected=" + std::to_string(checksum));
    }
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris
