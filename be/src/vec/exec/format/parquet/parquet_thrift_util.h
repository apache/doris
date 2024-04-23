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

#include <common/status.h>
#include <gen_cpp/parquet_types.h>

#include <cstdint>

#include "common/logging.h"
#include "io/fs/file_reader.h"
#include "io/io_common.h"
#include "olap/iterators.h"
#include "util/coding.h"
#include "util/thrift_util.h"
#include "vparquet_file_metadata.h"

namespace doris::vectorized {

constexpr uint8_t PARQUET_VERSION_NUMBER[4] = {'P', 'A', 'R', '1'};
constexpr uint32_t PARQUET_FOOTER_SIZE = 8;
constexpr size_t INIT_META_SIZE = 128 * 1024; // 128k

static Status parse_thrift_footer(io::FileReaderSPtr file, FileMetaData** file_metadata,
                                  size_t* meta_size, io::IOContext* io_ctx) {
    size_t file_size = file->size();
    size_t bytes_read = std::min(file_size, INIT_META_SIZE);
    std::vector<uint8_t> footer(bytes_read);
    RETURN_IF_ERROR(file->read_at(file_size - bytes_read, Slice(footer.data(), bytes_read),
                                  &bytes_read, io_ctx));

    // validate magic
    uint8_t* magic_ptr = footer.data() + bytes_read - 4;
    if (bytes_read < PARQUET_FOOTER_SIZE ||
        memcmp(magic_ptr, PARQUET_VERSION_NUMBER, sizeof(PARQUET_VERSION_NUMBER)) != 0) {
        return Status::Corruption(
                "Invalid magic number in parquet file, bytes read: {}, file size: {}, path: {}, "
                "read magic: {}",
                bytes_read, file_size, file->path().native(),
                std::string((char*)magic_ptr, sizeof(PARQUET_VERSION_NUMBER)));
    }

    // get metadata_size
    uint32_t metadata_size = decode_fixed32_le(footer.data() + bytes_read - PARQUET_FOOTER_SIZE);
    if (metadata_size > file_size - PARQUET_FOOTER_SIZE) {
        return Status::Corruption("Parquet footer size({}) is large than file size({})",
                                  metadata_size, file_size);
    }
    std::unique_ptr<uint8_t[]> new_buff;
    uint8_t* meta_ptr;
    if (metadata_size > bytes_read - PARQUET_FOOTER_SIZE) {
        new_buff.reset(new uint8_t[metadata_size]);
        RETURN_IF_ERROR(file->read_at(file_size - PARQUET_FOOTER_SIZE - metadata_size,
                                      Slice(new_buff.get(), metadata_size), &bytes_read, io_ctx));
        meta_ptr = new_buff.get();
    } else {
        meta_ptr = footer.data() + bytes_read - PARQUET_FOOTER_SIZE - metadata_size;
    }

    tparquet::FileMetaData t_metadata;
    // deserialize footer
    RETURN_IF_ERROR(deserialize_thrift_msg(meta_ptr, &metadata_size, true, &t_metadata));
    *file_metadata = new FileMetaData(t_metadata);
    RETURN_IF_ERROR((*file_metadata)->init_schema());
    *meta_size = PARQUET_FOOTER_SIZE + metadata_size;
    return Status::OK();
}
} // namespace doris::vectorized
