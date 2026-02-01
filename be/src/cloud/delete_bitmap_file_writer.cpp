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

#include <crc32c/crc32c.h>

#include "cloud/config.h"
#include "io/fs/file_writer.h"
#include "io/fs/packed_file_writer.h"

namespace doris {
#include "common/compile_check_begin.h"

DeleteBitmapFileWriter::DeleteBitmapFileWriter(int64_t tablet_id, const std::string& rowset_id,
                                               std::optional<StorageResource>& storage_resource)
        : _tablet_id(tablet_id), _rowset_id(rowset_id), _storage_resource(storage_resource) {}

DeleteBitmapFileWriter::DeleteBitmapFileWriter(int64_t tablet_id, const std::string& rowset_id,
                                               std::optional<StorageResource>& storage_resource,
                                               bool enable_packed_file, int64_t txn_id)
        : _tablet_id(tablet_id),
          _rowset_id(rowset_id),
          _storage_resource(storage_resource),
          _enable_packed_file(enable_packed_file),
          _txn_id(txn_id) {}

DeleteBitmapFileWriter::~DeleteBitmapFileWriter() {}

Status DeleteBitmapFileWriter::init() {
#ifdef BE_TEST
    _path = "./log/" + _rowset_id + "_delete_bitmap.db";
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

    if (_enable_packed_file) {
        // Create underlying file writer
        io::FileWriterPtr inner_writer;
        // Disable write_file_cache for inner writer when using PackedFileWriter.
        // Small files will be cached separately by PackedFileManager using the
        // small file path as cache key.
        opts.write_file_cache = false;
        RETURN_IF_ERROR(_storage_resource->fs->create_file(_path, &inner_writer, &opts));

        // Wrap with PackedFileWriter
        io::PackedAppendContext append_info;
        append_info.resource_id = _storage_resource->fs->id();
        append_info.tablet_id = _tablet_id;
        append_info.rowset_id = _rowset_id;
        append_info.txn_id = _txn_id;
        append_info.write_file_cache = false;

        _file_writer = std::make_unique<io::PackedFileWriter>(std::move(inner_writer),
                                                              io::Path(_path), append_info);
    } else {
        RETURN_IF_ERROR(_storage_resource->fs->create_file(_path, &_file_writer, &opts));
    }
    return Status::OK();
}

Status DeleteBitmapFileWriter::close() {
    if (!_file_writer) {
        return Status::InternalError("fail to close delete bitmap file={} because writer is null",
                                     _path);
    }
    auto st = _file_writer->close();
    if (!st.ok()) {
        LOG(WARNING) << "failed to close delete bitmap file=" << _path << ", st=" << st.to_string();
        return st;
    }

    // Check if file was written to packed file
    if (_enable_packed_file) {
        auto* packed_writer = static_cast<io::PackedFileWriter*>(_file_writer.get());
        io::PackedSliceLocation loc;
        st = packed_writer->get_packed_slice_location(&loc);
        if (!st.ok()) {
            LOG(WARNING) << "failed to get packed slice location for delete bitmap file=" << _path
                         << ", st=" << st.to_string();
            return st;
        }
        if (!loc.packed_file_path.empty()) {
            _is_packed = true;
            _packed_location = loc;
        }
    }
    return Status::OK();
}

Status DeleteBitmapFileWriter::get_packed_slice_location(io::PackedSliceLocation* location) const {
    if (!_is_packed) {
        return Status::InternalError("delete bitmap file is not packed");
    }
    *location = _packed_location;
    return Status::OK();
}

Status DeleteBitmapFileWriter::write(const DeleteBitmapPB& delete_bitmap) {
    if (delete_bitmap.rowset_ids_size() == 0) {
        return Status::InternalError("empty delete bitmap for file={}", _path);
    }
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
    uint32_t checksum = crc32c::Crc32c(content.data(), delete_bitmap_len);
    encode_fixed32_le(checksum_buf, checksum);
    RETURN_IF_ERROR(_file_writer->append({checksum_buf, CHECKSUM_SIZE}));
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris
