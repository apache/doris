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

#include "util/codec.h"
#include <boost/assign/list_of.hpp>

#include "util/compress.h"
#include "util/decompress.h"

#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/Descriptors_constants.h"

namespace doris {

const char* const Codec::DEFAULT_COMPRESSION =
    "org.apache.hadoop.io.compress.DefaultCodec";

const char* const Codec::GZIP_COMPRESSION =
    "org.apache.hadoop.io.compress.GzipCodec";

const char* const Codec::BZIP2_COMPRESSION =
    "org.apache.hadoop.io.compress.BZip2Codec";

const char* const Codec::SNAPPY_COMPRESSION =
    "org.apache.hadoop.io.compress.SnappyCodec";

const char* const UNKNOWN_CODEC_ERROR =
    "This compression codec is currently unsupported: ";

const Codec::CodecMap Codec::CODEC_MAP = boost::assign::map_list_of
        ("", THdfsCompression::NONE)
        (Codec::DEFAULT_COMPRESSION, THdfsCompression::DEFAULT)
        (Codec::GZIP_COMPRESSION, THdfsCompression::GZIP)
        (Codec::BZIP2_COMPRESSION, THdfsCompression::BZIP2)
        (Codec::SNAPPY_COMPRESSION, THdfsCompression::SNAPPY_BLOCKED);

std::string Codec::get_codec_name(THdfsCompression::type type) {
    std::map<const std::string, THdfsCompression::type>::const_iterator im;

    for (im = g_Descriptors_constants.COMPRESSION_MAP.begin();
            im != g_Descriptors_constants.COMPRESSION_MAP.end(); ++im) {
        if (im->second == type) {
            return im->first;
        }
    }

    DCHECK(im != g_Descriptors_constants.COMPRESSION_MAP.end());
    return "INVALID";
}

Status Codec::create_compressor(RuntimeState* runtime_state, MemPool* mem_pool,
                               bool reuse, const std::string& codec,
                               boost::scoped_ptr<Codec>* compressor) {
    std::map<const std::string, const THdfsCompression::type>::const_iterator
    type = CODEC_MAP.find(codec);

    if (type == CODEC_MAP.end()) {
        std::stringstream ss;
        ss << UNKNOWN_CODEC_ERROR << codec;
        return Status::InternalError(ss.str());
    }

    Codec* comp = NULL;
    RETURN_IF_ERROR(
            create_compressor(runtime_state, mem_pool, reuse, type->second, &comp));
    compressor->reset(comp);
    return Status::OK();
}

Status Codec::create_compressor(RuntimeState* runtime_state, MemPool* mem_pool,
                               bool reuse, THdfsCompression::type format,
                               boost::scoped_ptr<Codec>* compressor) {
    Codec* comp = NULL;
    RETURN_IF_ERROR(
            create_compressor(runtime_state, mem_pool, reuse, format, &comp));
    compressor->reset(comp);
    return Status::OK();
}

Status Codec::create_compressor(RuntimeState* runtime_state, MemPool* mem_pool,
                               bool reuse, THdfsCompression::type format,
                               Codec** compressor) {
    switch (format) {
    case THdfsCompression::NONE:
        *compressor = NULL;
        return Status::OK();

    case THdfsCompression::GZIP:
        *compressor = new GzipCompressor(GzipCompressor::GZIP, mem_pool, reuse);
        break;

    case THdfsCompression::DEFAULT:
        *compressor = new GzipCompressor(GzipCompressor::ZLIB, mem_pool, reuse);
        break;

    case THdfsCompression::DEFLATE:
        *compressor = new GzipCompressor(GzipCompressor::DEFLATE, mem_pool, reuse);
        break;

    case THdfsCompression::BZIP2:
        *compressor = new BzipCompressor(mem_pool, reuse);
        break;

    case THdfsCompression::SNAPPY_BLOCKED:
        *compressor = new SnappyBlockCompressor(mem_pool, reuse);
        break;

    case THdfsCompression::SNAPPY:
        *compressor = new SnappyCompressor(mem_pool, reuse);
        break;
    }

    return (*compressor)->init();
}

Status Codec::create_decompressor(RuntimeState* runtime_state, MemPool* mem_pool,
                                 bool reuse, const std::string& codec,
                                 boost::scoped_ptr<Codec>* decompressor) {
    std::map<const std::string, const THdfsCompression::type>::const_iterator
    type = CODEC_MAP.find(codec);

    if (type == CODEC_MAP.end()) {
        std::stringstream ss;
        ss << UNKNOWN_CODEC_ERROR << codec;
        return Status::InternalError(ss.str());
    }

    Codec* decom = NULL;
    RETURN_IF_ERROR(
        create_decompressor(runtime_state, mem_pool, reuse, type->second, &decom));
    decompressor->reset(decom);
    return Status::OK();
}

Status Codec::create_decompressor(RuntimeState* runtime_state, MemPool* mem_pool,
                                 bool reuse, THdfsCompression::type format,
                                 boost::scoped_ptr<Codec>* decompressor) {
    Codec* decom = NULL;
    RETURN_IF_ERROR(
        create_decompressor(runtime_state, mem_pool, reuse, format, &decom));
    decompressor->reset(decom);
    return Status::OK();
}

Status Codec::create_decompressor(RuntimeState* runtime_state, MemPool* mem_pool,
                                 bool reuse, THdfsCompression::type format,
                                 Codec** decompressor) {
    switch (format) {
    case THdfsCompression::NONE:
        *decompressor = NULL;
        return Status::OK();

    case THdfsCompression::DEFAULT:
    case THdfsCompression::GZIP:
        *decompressor = new GzipDecompressor(mem_pool, reuse, false);
        break;

    case THdfsCompression::DEFLATE:
        *decompressor = new GzipDecompressor(mem_pool, reuse, true);
        break;

    case THdfsCompression::BZIP2:
        *decompressor = new BzipDecompressor(mem_pool, reuse);
        break;

    case THdfsCompression::SNAPPY_BLOCKED:
        *decompressor = new SnappyBlockDecompressor(mem_pool, reuse);
        break;

    case THdfsCompression::SNAPPY:
        *decompressor = new SnappyDecompressor(mem_pool, reuse);
        break;
    }

    return (*decompressor)->init();
}

Codec::Codec(MemPool* mem_pool, bool reuse_buffer) :
        _memory_pool(mem_pool),
        _temp_memory_pool(_memory_pool->mem_tracker()),
        _reuse_buffer(reuse_buffer),
        _out_buffer(NULL),
        _buffer_length(0) {
}

}
