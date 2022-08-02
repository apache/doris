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

#include <cstdint>

#include "common/logging.h"
#include "gen_cpp/parquet_types.h"
#include "io/file_reader.h"
#include "util/coding.h"
#include "util/thrift_util.h"
#include "vparquet_file_metadata.h"

namespace doris::vectorized {

constexpr uint8_t PARQUET_VERSION_NUMBER[4] = {'P', 'A', 'R', '1'};
constexpr int64_t PARQUET_FOOTER_READ_SIZE = 64 * 1024;
constexpr uint32_t PARQUET_FOOTER_SIZE = 8;

Status parse_thrift_footer(FileReader* file, std::shared_ptr<FileMetaData>& file_metadata) {
    // try with buffer on stack
    uint8_t buff[PARQUET_FOOTER_READ_SIZE];
    int64_t file_size = file->size();
    // read footer bytes
    uint64_t footer_read_size = std::min(file_size, PARQUET_FOOTER_READ_SIZE);

    int64_t bytes_read = 0;
    RETURN_IF_ERROR(
            file->readat(file_size - footer_read_size, footer_read_size, &bytes_read, buff));

    // validate magic
    uint8_t* magic_ptr = buff + footer_read_size - sizeof(PARQUET_VERSION_NUMBER);
    if (memcmp(magic_ptr, PARQUET_VERSION_NUMBER, sizeof(PARQUET_VERSION_NUMBER)) != 0) {
        return Status::Corruption("Invalid magic number in parquet file");
    }

    // get metadata_size
    uint8_t* footer_buff = buff + footer_read_size - PARQUET_FOOTER_SIZE;
    uint32_t metadata_size = decode_fixed32_le(footer_buff);
    if (metadata_size > file_size - PARQUET_FOOTER_SIZE) {
        Status::Corruption("Parquet file size is ", file_size,
                           " bytes, smaller than the size reported by footer's (", metadata_size,
                           "bytes)");
    }
    tparquet::FileMetaData t_metadata;
    // deserialize footer
    RETURN_IF_ERROR(
            deserialize_thrift_msg(footer_buff - metadata_size, &metadata_size, true, &t_metadata));
    file_metadata.reset(new FileMetaData(t_metadata));
    RETURN_IF_ERROR(file_metadata->init_schema());
    return Status::OK();
}

//    Status parse_page_header() {
//        uint8_t* page_buf;
//
//    }
} // namespace doris::vectorized
