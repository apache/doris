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

#include "vec/runtime/vnative_transformer.h"

#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/data.pb.h>
#include <glog/logging.h>

#include "agent/be_exec_version_manager.h"
#include "io/fs/file_writer.h"
#include "runtime/runtime_state.h"
#include "util/slice.h"
#include "vec/core/block.h"
#include "vec/exec/format/native/native_format.h"

namespace doris::vectorized {

#include "common/compile_check_begin.h"

namespace {

// Map high-level TFileCompressType to low-level segment_v2::CompressionTypePB.
segment_v2::CompressionTypePB to_local_compression_type(TFileCompressType::type type) {
    using CT = segment_v2::CompressionTypePB;
    switch (type) {
    case TFileCompressType::GZ:
    case TFileCompressType::ZLIB:
    case TFileCompressType::DEFLATE:
        return CT::ZLIB;
    case TFileCompressType::LZ4FRAME:
    case TFileCompressType::LZ4BLOCK:
        return CT::LZ4;
    case TFileCompressType::SNAPPYBLOCK:
        return CT::SNAPPY;
    case TFileCompressType::ZSTD:
        return CT::ZSTD;
    default:
        return CT::ZSTD;
    }
}

} // namespace

VNativeTransformer::VNativeTransformer(RuntimeState* state, doris::io::FileWriter* file_writer,
                                       const VExprContextSPtrs& output_vexpr_ctxs,
                                       bool output_object_data,
                                       TFileCompressType::type compress_type)
        : VFileFormatTransformer(state, output_vexpr_ctxs, output_object_data),
          _file_writer(file_writer),
          _compression_type(to_local_compression_type(compress_type)) {}

Status VNativeTransformer::open() {
    // Write Doris Native file header:
    // [magic bytes "DORISN1\0"][uint32_t format_version]
    DCHECK(_file_writer != nullptr);
    uint32_t version = DORIS_NATIVE_FORMAT_VERSION;

    Slice magic_slice(DORIS_NATIVE_MAGIC, sizeof(DORIS_NATIVE_MAGIC));
    Slice version_slice(reinterpret_cast<char*>(&version), sizeof(uint32_t));

    RETURN_IF_ERROR(_file_writer->append(magic_slice));
    RETURN_IF_ERROR(_file_writer->append(version_slice));

    _written_len += sizeof(DORIS_NATIVE_MAGIC) + sizeof(uint32_t);
    return Status::OK();
}

Status VNativeTransformer::write(const Block& block) {
    if (block.rows() == 0) {
        return Status::OK();
    }

    // Serialize Block into PBlock using existing vec serialization logic.
    PBlock pblock;
    size_t uncompressed_bytes = 0;
    size_t compressed_bytes = 0;
    int64_t compressed_time = 0;

    RETURN_IF_ERROR(block.serialize(BeExecVersionManager::get_newest_version(), &pblock,
                                    &uncompressed_bytes, &compressed_bytes, &compressed_time,
                                    _compression_type));

    std::string buff;
    if (!pblock.SerializeToString(&buff)) {
        auto err = Status::Error<ErrorCode::SERIALIZE_PROTOBUF_ERROR>(
                "serialize native block error. block rows: {}", block.rows());
        return err;
    }

    // Layout of Doris Native file:
    // [uint64_t block_size][PBlock bytes]...
    uint64_t len = buff.size();
    Slice len_slice(reinterpret_cast<char*>(&len), sizeof(len));
    RETURN_IF_ERROR(_file_writer->append(len_slice));
    RETURN_IF_ERROR(_file_writer->append(buff));

    _written_len += sizeof(len) + buff.size();
    _cur_written_rows += block.rows();

    return Status::OK();
}

Status VNativeTransformer::close() {
    // Close underlying FileWriter to ensure data is flushed to disk.
    if (_file_writer != nullptr && _file_writer->state() != doris::io::FileWriter::State::CLOSED) {
        RETURN_IF_ERROR(_file_writer->close());
    }

    return Status::OK();
}

int64_t VNativeTransformer::written_len() {
    return _written_len;
}

#include "common/compile_check_end.h"

} // namespace doris::vectorized
