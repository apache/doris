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

#include "io/compress/compression_factory.h"

#include "io/compress/bzip2_compressor.h"
#include "io/compress/stream_compression_file_writer.h"
#include "io/compress/zlib_compressor.h"

namespace doris::io {
Status CompressionFactory::create_compressor(CompressType type,
                                             std::unique_ptr<Compressor>* compressor) {
    // TODO: read conf to init Compressor
    switch (type) {
    case CompressType::GZIP:
        *compressor = std::make_unique<ZlibCompressor>();
        break;
    case CompressType::BZIP2:
        *compressor = std::make_unique<Bzip2Compressor>();
        break;
    // case CompressType::LZ4:
    //     *compressor = std::make_unique<LZ4Compressor>();
    //     break;
    default:
        return Status::InternalError("unknown compress type");
    }
    RETURN_IF_ERROR((*compressor)->init());
    return Status::OK();
}

Status CompressionFactory::create_compression_file_wrtier(
        CompressType type, std::unique_ptr<FileWriter> writer,
        std::unique_ptr<FileWriter>* result_writer) {
    std::unique_ptr<Compressor> compressor;
    RETURN_IF_ERROR(create_compressor(type, &compressor));

    // TODO: conf buffer size
    auto stream_writer = std::make_unique<StreamCompressionFileWriter>(std::move(writer),
                                                                       std::move(compressor), 4096);
    RETURN_IF_ERROR(stream_writer->init());
    *result_writer = std::move(stream_writer);
    return Status::OK();
}

} // namespace doris::io
