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

#include "util/block_compression.h"

#include <bzlib.h>
#include <gen_cpp/parquet_types.h>
#include <gen_cpp/segment_v2.pb.h>
#include <glog/logging.h>

#include <exception>
// Only used on x86 or x86_64
#if defined(__x86_64__) || defined(_M_X64) || defined(i386) || defined(__i386__) || \
        defined(__i386) || defined(_M_IX86)
#include <libdeflate.h>
#endif
#include <glog/log_severity.h>
#include <glog/logging.h>
#include <limits.h>
#include <lz4/lz4.h>
#include <lz4/lz4frame.h>
#include <lz4/lz4hc.h>
#include <snappy/snappy-sinksource.h>
#include <snappy/snappy.h>
#include <stdint.h>
#include <zconf.h>
#include <zlib.h>
#include <zstd.h>
#include <zstd_errors.h>

#include <algorithm>
#include <limits>
#include <mutex>
#include <new>
#include <ostream>

#include "common/config.h"
#include "common/factory_creator.h"
#include "exec/decompressor.h"
#include "gutil/endian.h"
#include "gutil/strings/substitute.h"
#include "orc/OrcFile.hh"
#include "runtime/thread_context.h"
#include "util/bit_util.h"
#include "util/defer_op.h"
#include "util/faststring.h"

namespace orc {
/**
 * Decompress the bytes in to the output buffer.
 * @param inputAddress the start of the input
 * @param inputLimit one past the last byte of the input
 * @param outputAddress the start of the output buffer
 * @param outputLimit one past the last byte of the output buffer
 * @result the number of bytes decompressed
 */
uint64_t lzoDecompress(const char* inputAddress, const char* inputLimit, char* outputAddress,
                       char* outputLimit);
} // namespace orc

namespace doris {

using strings::Substitute;

// exception safe
Status BlockCompressionCodec::compress(const std::vector<Slice>& inputs, size_t uncompressed_size,
                                       faststring* output) {
    faststring buf;
    // we compute total size to avoid more memory copy
    buf.reserve(uncompressed_size);
    for (auto& input : inputs) {
        buf.append(input.data, input.size);
    }
    return compress(buf, output);
}

bool BlockCompressionCodec::exceed_max_compress_len(size_t uncompressed_size) {
    return uncompressed_size > std::numeric_limits<int32_t>::max();
}

class Lz4BlockCompression : public BlockCompressionCodec {
private:
    class Context {
        ENABLE_FACTORY_CREATOR(Context);

    public:
        Context() : ctx(nullptr) {
            SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
                    ExecEnv::GetInstance()->block_compression_mem_tracker());
            buffer = std::make_unique<faststring>();
        }
        LZ4_stream_t* ctx;
        std::unique_ptr<faststring> buffer;
        ~Context() {
            SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
                    ExecEnv::GetInstance()->block_compression_mem_tracker());
            if (ctx) {
                LZ4_freeStream(ctx);
            }
            buffer.reset();
        }
    };

public:
    static Lz4BlockCompression* instance() {
        static Lz4BlockCompression s_instance;
        return &s_instance;
    }
    ~Lz4BlockCompression() override {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
                ExecEnv::GetInstance()->block_compression_mem_tracker());
        _ctx_pool.clear();
    }

    Status compress(const Slice& input, faststring* output) override {
        if (input.size > INT_MAX) {
            return Status::InvalidArgument(
                    "LZ4 not support those case(input.size>INT_MAX), maybe you should change "
                    "fragment_transmission_compression_codec to snappy, size={}",
                    input.size);
        }

        std::unique_ptr<Context> context;
        RETURN_IF_ERROR(_acquire_compression_ctx(context));
        bool compress_failed = false;
        Defer defer {[&] {
            if (!compress_failed) {
                _release_compression_ctx(std::move(context));
            }
        }};

        try {
            Slice compressed_buf;
            size_t max_len = max_compressed_len(input.size);
            if (max_len > MAX_COMPRESSION_BUFFER_SIZE_FOR_REUSE) {
                // use output directly
                output->resize(max_len);
                compressed_buf.data = reinterpret_cast<char*>(output->data());
                compressed_buf.size = max_len;
            } else {
                // reuse context buffer if max_len <= MAX_COMPRESSION_BUFFER_FOR_REUSE
                {
                    // context->buffer is resuable between queries, should accouting to
                    // global tracker.
                    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
                            ExecEnv::GetInstance()->block_compression_mem_tracker());
                    context->buffer->resize(max_len);
                }
                compressed_buf.data = reinterpret_cast<char*>(context->buffer->data());
                compressed_buf.size = max_len;
            }

            size_t compressed_len =
                    LZ4_compress_fast_continue(context->ctx, input.data, compressed_buf.data,
                                               input.size, compressed_buf.size, ACCELARATION);
            if (compressed_len == 0) {
                compress_failed = true;
                return Status::InvalidArgument("Output buffer's capacity is not enough, size={}",
                                               compressed_buf.size);
            }
            output->resize(compressed_len);
            if (max_len <= MAX_COMPRESSION_BUFFER_SIZE_FOR_REUSE) {
                output->assign_copy(reinterpret_cast<uint8_t*>(compressed_buf.data),
                                    compressed_len);
            }
        } catch (...) {
            // Do not set compress_failed to release context
            DCHECK(!compress_failed);
            return Status::InternalError("Fail to do LZ4Block compress due to exception");
        }
        return Status::OK();
    }

    Status decompress(const Slice& input, Slice* output) override {
        auto decompressed_len =
                LZ4_decompress_safe(input.data, output->data, input.size, output->size);
        if (decompressed_len < 0) {
            return Status::InvalidArgument("fail to do LZ4 decompress, error={}", decompressed_len);
        }
        output->size = decompressed_len;
        return Status::OK();
    }

    size_t max_compressed_len(size_t len) override { return LZ4_compressBound(len); }

private:
    // reuse LZ4 compress stream
    Status _acquire_compression_ctx(std::unique_ptr<Context>& out) {
        std::lock_guard<std::mutex> l(_ctx_mutex);
        if (_ctx_pool.empty()) {
            std::unique_ptr<Context> localCtx = Context::create_unique();
            if (localCtx.get() == nullptr) {
                return Status::InvalidArgument("new LZ4 context error");
            }
            localCtx->ctx = LZ4_createStream();
            if (localCtx->ctx == nullptr) {
                return Status::InvalidArgument("LZ4_createStream error");
            }
            out = std::move(localCtx);
            return Status::OK();
        }
        out = std::move(_ctx_pool.back());
        _ctx_pool.pop_back();
        return Status::OK();
    }
    void _release_compression_ctx(std::unique_ptr<Context> context) {
        DCHECK(context);
        LZ4_resetStream(context->ctx);
        std::lock_guard<std::mutex> l(_ctx_mutex);
        _ctx_pool.push_back(std::move(context));
    }

private:
    mutable std::mutex _ctx_mutex;
    mutable std::vector<std::unique_ptr<Context>> _ctx_pool;
    static const int32_t ACCELARATION = 1;
};

class HadoopLz4BlockCompression : public Lz4BlockCompression {
public:
    HadoopLz4BlockCompression() {
        Status st = Decompressor::create_decompressor(CompressType::LZ4BLOCK, &_decompressor);
        if (!st.ok()) {
            LOG(FATAL) << "HadoopLz4BlockCompression construction failed. status = " << st << "\n";
        }
    }

    ~HadoopLz4BlockCompression() override = default;

    static HadoopLz4BlockCompression* instance() {
        static HadoopLz4BlockCompression s_instance;
        return &s_instance;
    }

    // hadoop use block compression for lz4
    // https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-nativetask/src/main/native/src/codec/Lz4Codec.cc
    Status compress(const Slice& input, faststring* output) override {
        // be same with hadop https://github.com/apache/hadoop/blob/trunk/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/compress/Lz4Codec.java
        size_t lz4_block_size = config::lz4_compression_block_size;
        size_t overhead = lz4_block_size / 255 + 16;
        size_t max_input_size = lz4_block_size - overhead;

        size_t data_len = input.size;
        char* data = input.data;
        std::vector<OwnedSlice> buffers;
        size_t out_len = 0;

        while (data_len > 0) {
            size_t input_size = std::min(data_len, max_input_size);
            Slice input_slice(data, input_size);
            faststring output_data;
            RETURN_IF_ERROR(Lz4BlockCompression::compress(input_slice, &output_data));
            out_len += output_data.size();
            buffers.push_back(output_data.build());
            data += input_size;
            data_len -= input_size;
        }

        // hadoop block compression: umcompressed_length | compressed_length1 | compressed_data1 | compressed_length2 | compressed_data2 | ...
        size_t total_output_len = 4 + 4 * buffers.size() + out_len;
        output->resize(total_output_len);
        char* output_buffer = (char*)output->data();
        BigEndian::Store32(output_buffer, input.get_size());
        output_buffer += 4;
        for (const auto& buffer : buffers) {
            auto slice = buffer.slice();
            BigEndian::Store32(output_buffer, slice.get_size());
            output_buffer += 4;
            memcpy(output_buffer, slice.get_data(), slice.get_size());
            output_buffer += slice.get_size();
        }

        DCHECK_EQ(output_buffer - (char*)output->data(), total_output_len);

        return Status::OK();
    }

    Status decompress(const Slice& input, Slice* output) override {
        size_t input_bytes_read = 0;
        size_t decompressed_len = 0;
        size_t more_input_bytes = 0;
        size_t more_output_bytes = 0;
        bool stream_end = false;
        auto st = _decompressor->decompress((uint8_t*)input.data, input.size, &input_bytes_read,
                                            (uint8_t*)output->data, output->size, &decompressed_len,
                                            &stream_end, &more_input_bytes, &more_output_bytes);
        //try decompress use hadoopLz4 ,if failed fall back lz4.
        return (st != Status::OK() || stream_end != true)
                       ? Lz4BlockCompression::decompress(input, output)
                       : Status::OK();
    }

private:
    std::unique_ptr<Decompressor> _decompressor;
};
// Used for LZ4 frame format, decompress speed is two times faster than LZ4.
class Lz4fBlockCompression : public BlockCompressionCodec {
private:
    class CContext {
        ENABLE_FACTORY_CREATOR(CContext);

    public:
        CContext() : ctx(nullptr) {
            SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
                    ExecEnv::GetInstance()->block_compression_mem_tracker());
            buffer = std::make_unique<faststring>();
        }
        LZ4F_compressionContext_t ctx;
        std::unique_ptr<faststring> buffer;
        ~CContext() {
            SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
                    ExecEnv::GetInstance()->block_compression_mem_tracker());
            if (ctx) {
                LZ4F_freeCompressionContext(ctx);
            }
            buffer.reset();
        }
    };
    class DContext {
        ENABLE_FACTORY_CREATOR(DContext);

    public:
        DContext() : ctx(nullptr) {}
        LZ4F_decompressionContext_t ctx;
        ~DContext() {
            if (ctx) {
                LZ4F_freeDecompressionContext(ctx);
            }
        }
    };

public:
    static Lz4fBlockCompression* instance() {
        static Lz4fBlockCompression s_instance;
        return &s_instance;
    }
    ~Lz4fBlockCompression() {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
                ExecEnv::GetInstance()->block_compression_mem_tracker());
        _ctx_c_pool.clear();
        _ctx_d_pool.clear();
    }

    Status compress(const Slice& input, faststring* output) override {
        std::vector<Slice> inputs {input};
        return compress(inputs, input.size, output);
    }

    Status compress(const std::vector<Slice>& inputs, size_t uncompressed_size,
                    faststring* output) override {
        return _compress(inputs, uncompressed_size, output);
    }

    Status decompress(const Slice& input, Slice* output) override {
        return _decompress(input, output);
    }

    size_t max_compressed_len(size_t len) override {
        return std::max(LZ4F_compressBound(len, &_s_preferences),
                        LZ4F_compressFrameBound(len, &_s_preferences));
    }

private:
    Status _compress(const std::vector<Slice>& inputs, size_t uncompressed_size,
                     faststring* output) {
        std::unique_ptr<CContext> context;
        RETURN_IF_ERROR(_acquire_compression_ctx(context));
        bool compress_failed = false;
        Defer defer {[&] {
            if (!compress_failed) {
                _release_compression_ctx(std::move(context));
            }
        }};

        try {
            Slice compressed_buf;
            size_t max_len = max_compressed_len(uncompressed_size);
            if (max_len > MAX_COMPRESSION_BUFFER_SIZE_FOR_REUSE) {
                // use output directly
                output->resize(max_len);
                compressed_buf.data = reinterpret_cast<char*>(output->data());
                compressed_buf.size = max_len;
            } else {
                {
                    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
                            ExecEnv::GetInstance()->block_compression_mem_tracker());
                    // reuse context buffer if max_len <= MAX_COMPRESSION_BUFFER_FOR_REUSE
                    context->buffer->resize(max_len);
                }
                compressed_buf.data = reinterpret_cast<char*>(context->buffer->data());
                compressed_buf.size = max_len;
            }

            auto wbytes = LZ4F_compressBegin(context->ctx, compressed_buf.data, compressed_buf.size,
                                             &_s_preferences);
            if (LZ4F_isError(wbytes)) {
                compress_failed = true;
                return Status::InvalidArgument("Fail to do LZ4F compress begin, res={}",
                                               LZ4F_getErrorName(wbytes));
            }
            size_t offset = wbytes;
            for (auto input : inputs) {
                wbytes = LZ4F_compressUpdate(context->ctx, compressed_buf.data + offset,
                                             compressed_buf.size - offset, input.data, input.size,
                                             nullptr);
                if (LZ4F_isError(wbytes)) {
                    compress_failed = true;
                    return Status::InvalidArgument("Fail to do LZ4F compress update, res={}",
                                                   LZ4F_getErrorName(wbytes));
                }
                offset += wbytes;
            }
            wbytes = LZ4F_compressEnd(context->ctx, compressed_buf.data + offset,
                                      compressed_buf.size - offset, nullptr);
            if (LZ4F_isError(wbytes)) {
                compress_failed = true;
                return Status::InvalidArgument("Fail to do LZ4F compress end, res={}",
                                               LZ4F_getErrorName(wbytes));
            }
            offset += wbytes;
            output->resize(offset);
            if (max_len <= MAX_COMPRESSION_BUFFER_SIZE_FOR_REUSE) {
                output->assign_copy(reinterpret_cast<uint8_t*>(compressed_buf.data), offset);
            }
        } catch (...) {
            // Do not set compress_failed to release context
            DCHECK(!compress_failed);
            return Status::InternalError("Fail to do LZ4F compress due to exception");
        }

        return Status::OK();
    }

    Status _decompress(const Slice& input, Slice* output) {
        bool decompress_failed = false;
        std::unique_ptr<DContext> context;
        RETURN_IF_ERROR(_acquire_decompression_ctx(context));
        Defer defer {[&] {
            if (!decompress_failed) {
                _release_decompression_ctx(std::move(context));
            }
        }};
        size_t input_size = input.size;
        auto lres = LZ4F_decompress(context->ctx, output->data, &output->size, input.data,
                                    &input_size, nullptr);
        if (LZ4F_isError(lres)) {
            decompress_failed = true;
            return Status::InvalidArgument("Fail to do LZ4F decompress, res={}",
                                           LZ4F_getErrorName(lres));
        } else if (input_size != input.size) {
            decompress_failed = true;
            return Status::InvalidArgument(
                    strings::Substitute("Fail to do LZ4F decompress: trailing data left in "
                                        "compressed data, read=$0 vs given=$1",
                                        input_size, input.size));
        } else if (lres != 0) {
            decompress_failed = true;
            return Status::InvalidArgument(
                    "Fail to do LZ4F decompress: expect more compressed data, expect={}", lres);
        }
        return Status::OK();
    }

private:
    // acquire a compression ctx from pool, release while finish compress,
    // delete if compression failed
    Status _acquire_compression_ctx(std::unique_ptr<CContext>& out) {
        std::lock_guard<std::mutex> l(_ctx_c_mutex);
        if (_ctx_c_pool.empty()) {
            std::unique_ptr<CContext> localCtx = CContext::create_unique();
            if (localCtx.get() == nullptr) {
                return Status::InvalidArgument("failed to new LZ4F CContext");
            }
            auto res = LZ4F_createCompressionContext(&localCtx->ctx, LZ4F_VERSION);
            if (LZ4F_isError(res) != 0) {
                return Status::InvalidArgument(strings::Substitute(
                        "LZ4F_createCompressionContext error, res=$0", LZ4F_getErrorName(res)));
            }
            out = std::move(localCtx);
            return Status::OK();
        }
        out = std::move(_ctx_c_pool.back());
        _ctx_c_pool.pop_back();
        return Status::OK();
    }
    void _release_compression_ctx(std::unique_ptr<CContext> context) {
        DCHECK(context);
        std::lock_guard<std::mutex> l(_ctx_c_mutex);
        _ctx_c_pool.push_back(std::move(context));
    }

    Status _acquire_decompression_ctx(std::unique_ptr<DContext>& out) {
        std::lock_guard<std::mutex> l(_ctx_d_mutex);
        if (_ctx_d_pool.empty()) {
            std::unique_ptr<DContext> localCtx = DContext::create_unique();
            if (localCtx.get() == nullptr) {
                return Status::InvalidArgument("failed to new LZ4F DContext");
            }
            auto res = LZ4F_createDecompressionContext(&localCtx->ctx, LZ4F_VERSION);
            if (LZ4F_isError(res) != 0) {
                return Status::InvalidArgument(strings::Substitute(
                        "LZ4F_createDeompressionContext error, res=$0", LZ4F_getErrorName(res)));
            }
            out = std::move(localCtx);
            return Status::OK();
        }
        out = std::move(_ctx_d_pool.back());
        _ctx_d_pool.pop_back();
        return Status::OK();
    }
    void _release_decompression_ctx(std::unique_ptr<DContext> context) {
        DCHECK(context);
        // reset decompression context to avoid ERROR_maxBlockSize_invalid
        LZ4F_resetDecompressionContext(context->ctx);
        std::lock_guard<std::mutex> l(_ctx_d_mutex);
        _ctx_d_pool.push_back(std::move(context));
    }

private:
    static LZ4F_preferences_t _s_preferences;

    std::mutex _ctx_c_mutex;
    // LZ4F_compressionContext_t is a pointer so no copy here
    std::vector<std::unique_ptr<CContext>> _ctx_c_pool;

    std::mutex _ctx_d_mutex;
    std::vector<std::unique_ptr<DContext>> _ctx_d_pool;
};

LZ4F_preferences_t Lz4fBlockCompression::_s_preferences = {
        {LZ4F_max256KB, LZ4F_blockLinked, LZ4F_noContentChecksum, LZ4F_frame, 0ULL, 0U,
         LZ4F_noBlockChecksum},
        0,
        0u,
        0u,
        {0u, 0u, 0u}};

class Lz4HCBlockCompression : public BlockCompressionCodec {
private:
    class Context {
        ENABLE_FACTORY_CREATOR(Context);

    public:
        Context() : ctx(nullptr) {
            SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
                    ExecEnv::GetInstance()->block_compression_mem_tracker());
            buffer = std::make_unique<faststring>();
        }
        LZ4_streamHC_t* ctx;
        std::unique_ptr<faststring> buffer;
        ~Context() {
            SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
                    ExecEnv::GetInstance()->block_compression_mem_tracker());
            if (ctx) {
                LZ4_freeStreamHC(ctx);
            }
            buffer.reset();
        }
    };

public:
    static Lz4HCBlockCompression* instance() {
        static Lz4HCBlockCompression s_instance;
        return &s_instance;
    }
    ~Lz4HCBlockCompression() {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
                ExecEnv::GetInstance()->block_compression_mem_tracker());
        _ctx_pool.clear();
    }

    Status compress(const Slice& input, faststring* output) override {
        std::unique_ptr<Context> context;
        RETURN_IF_ERROR(_acquire_compression_ctx(context));
        bool compress_failed = false;
        Defer defer {[&] {
            if (!compress_failed) {
                _release_compression_ctx(std::move(context));
            }
        }};

        try {
            Slice compressed_buf;
            size_t max_len = max_compressed_len(input.size);
            if (max_len > MAX_COMPRESSION_BUFFER_SIZE_FOR_REUSE) {
                // use output directly
                output->resize(max_len);
                compressed_buf.data = reinterpret_cast<char*>(output->data());
                compressed_buf.size = max_len;
            } else {
                {
                    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
                            ExecEnv::GetInstance()->block_compression_mem_tracker());
                    // reuse context buffer if max_len <= MAX_COMPRESSION_BUFFER_FOR_REUSE
                    context->buffer->resize(max_len);
                }
                compressed_buf.data = reinterpret_cast<char*>(context->buffer->data());
                compressed_buf.size = max_len;
            }

            size_t compressed_len = LZ4_compress_HC_continue(
                    context->ctx, input.data, compressed_buf.data, input.size, compressed_buf.size);
            if (compressed_len == 0) {
                compress_failed = true;
                return Status::InvalidArgument("Output buffer's capacity is not enough, size={}",
                                               compressed_buf.size);
            }
            output->resize(compressed_len);
            if (max_len <= MAX_COMPRESSION_BUFFER_SIZE_FOR_REUSE) {
                output->assign_copy(reinterpret_cast<uint8_t*>(compressed_buf.data),
                                    compressed_len);
            }
        } catch (...) {
            // Do not set compress_failed to release context
            DCHECK(!compress_failed);
            return Status::InternalError("Fail to do LZ4HC compress due to exception");
        }
        return Status::OK();
    }

    Status decompress(const Slice& input, Slice* output) override {
        auto decompressed_len =
                LZ4_decompress_safe(input.data, output->data, input.size, output->size);
        if (decompressed_len < 0) {
            return Status::InvalidArgument("fail to do LZ4 decompress, error={}", decompressed_len);
        }
        output->size = decompressed_len;
        return Status::OK();
    }

    size_t max_compressed_len(size_t len) override { return LZ4_compressBound(len); }

private:
    Status _acquire_compression_ctx(std::unique_ptr<Context>& out) {
        std::lock_guard<std::mutex> l(_ctx_mutex);
        if (_ctx_pool.empty()) {
            std::unique_ptr<Context> localCtx = Context::create_unique();
            if (localCtx.get() == nullptr) {
                return Status::InvalidArgument("new LZ4HC context error");
            }
            localCtx->ctx = LZ4_createStreamHC();
            if (localCtx->ctx == nullptr) {
                return Status::InvalidArgument("LZ4_createStreamHC error");
            }
            out = std::move(localCtx);
            return Status::OK();
        }
        out = std::move(_ctx_pool.back());
        _ctx_pool.pop_back();
        return Status::OK();
    }
    void _release_compression_ctx(std::unique_ptr<Context> context) {
        DCHECK(context);
        LZ4_resetStreamHC_fast(context->ctx, _compression_level);
        std::lock_guard<std::mutex> l(_ctx_mutex);
        _ctx_pool.push_back(std::move(context));
    }

private:
    int64_t _compression_level = config::LZ4_HC_compression_level;
    mutable std::mutex _ctx_mutex;
    mutable std::vector<std::unique_ptr<Context>> _ctx_pool;
};

class SnappySlicesSource : public snappy::Source {
public:
    SnappySlicesSource(const std::vector<Slice>& slices)
            : _available(0), _cur_slice(0), _slice_off(0) {
        for (auto& slice : slices) {
            // We filter empty slice here to avoid complicated process
            if (slice.size == 0) {
                continue;
            }
            _available += slice.size;
            _slices.push_back(slice);
        }
    }
    ~SnappySlicesSource() override {}

    // Return the number of bytes left to read from the source
    size_t Available() const override { return _available; }

    // Peek at the next flat region of the source.  Does not reposition
    // the source.  The returned region is empty iff Available()==0.
    //
    // Returns a pointer to the beginning of the region and store its
    // length in *len.
    //
    // The returned region is valid until the next call to Skip() or
    // until this object is destroyed, whichever occurs first.
    //
    // The returned region may be larger than Available() (for example
    // if this ByteSource is a view on a substring of a larger source).
    // The caller is responsible for ensuring that it only reads the
    // Available() bytes.
    const char* Peek(size_t* len) override {
        if (_available == 0) {
            *len = 0;
            return nullptr;
        }
        // we should assure that *len is not 0
        *len = _slices[_cur_slice].size - _slice_off;
        DCHECK(*len != 0);
        return _slices[_cur_slice].data + _slice_off;
    }

    // Skip the next n bytes.  Invalidates any buffer returned by
    // a previous call to Peek().
    // REQUIRES: Available() >= n
    void Skip(size_t n) override {
        _available -= n;
        while (n > 0) {
            auto left = _slices[_cur_slice].size - _slice_off;
            if (left > n) {
                // n can be digest in current slice
                _slice_off += n;
                return;
            }
            _slice_off = 0;
            _cur_slice++;
            n -= left;
        }
    }

private:
    std::vector<Slice> _slices;
    size_t _available;
    size_t _cur_slice;
    size_t _slice_off;
};

class SnappyBlockCompression : public BlockCompressionCodec {
public:
    static SnappyBlockCompression* instance() {
        static SnappyBlockCompression s_instance;
        return &s_instance;
    }
    ~SnappyBlockCompression() override = default;

    Status compress(const Slice& input, faststring* output) override {
        size_t max_len = max_compressed_len(input.size);
        output->resize(max_len);
        Slice s(*output);

        snappy::RawCompress(input.data, input.size, s.data, &s.size);
        output->resize(s.size);
        return Status::OK();
    }

    Status decompress(const Slice& input, Slice* output) override {
        if (!snappy::RawUncompress(input.data, input.size, output->data)) {
            return Status::InvalidArgument("Fail to do Snappy decompress");
        }
        // NOTE: GetUncompressedLength only takes O(1) time
        snappy::GetUncompressedLength(input.data, input.size, &output->size);
        return Status::OK();
    }

    Status compress(const std::vector<Slice>& inputs, size_t uncompressed_size,
                    faststring* output) override {
        auto max_len = max_compressed_len(uncompressed_size);
        output->resize(max_len);

        SnappySlicesSource source(inputs);
        snappy::UncheckedByteArraySink sink(reinterpret_cast<char*>(output->data()));
        output->resize(snappy::Compress(&source, &sink));
        return Status::OK();
    }

    size_t max_compressed_len(size_t len) override { return snappy::MaxCompressedLength(len); }
};

class HadoopSnappyBlockCompression : public SnappyBlockCompression {
public:
    static HadoopSnappyBlockCompression* instance() {
        static HadoopSnappyBlockCompression s_instance;
        return &s_instance;
    }
    ~HadoopSnappyBlockCompression() override = default;

    // hadoop use block compression for snappy
    // https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-nativetask/src/main/native/src/codec/SnappyCodec.cc
    Status compress(const Slice& input, faststring* output) override {
        // be same with hadop https://github.com/apache/hadoop/blob/trunk/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/compress/SnappyCodec.java
        size_t snappy_block_size = config::snappy_compression_block_size;
        size_t overhead = snappy_block_size / 6 + 32;
        size_t max_input_size = snappy_block_size - overhead;

        size_t data_len = input.size;
        char* data = input.data;
        std::vector<OwnedSlice> buffers;
        size_t out_len = 0;

        while (data_len > 0) {
            size_t input_size = std::min(data_len, max_input_size);
            Slice input_slice(data, input_size);
            faststring output_data;
            RETURN_IF_ERROR(SnappyBlockCompression::compress(input_slice, &output_data));
            out_len += output_data.size();
            // the OwnedSlice will be moved here
            buffers.push_back(output_data.build());
            data += input_size;
            data_len -= input_size;
        }

        // hadoop block compression: umcompressed_length | compressed_length1 | compressed_data1 | compressed_length2 | compressed_data2 | ...
        size_t total_output_len = 4 + 4 * buffers.size() + out_len;
        output->resize(total_output_len);
        char* output_buffer = (char*)output->data();
        BigEndian::Store32(output_buffer, input.get_size());
        output_buffer += 4;
        for (const auto& buffer : buffers) {
            auto slice = buffer.slice();
            BigEndian::Store32(output_buffer, slice.get_size());
            output_buffer += 4;
            memcpy(output_buffer, slice.get_data(), slice.get_size());
            output_buffer += slice.get_size();
        }

        DCHECK_EQ(output_buffer - (char*)output->data(), total_output_len);

        return Status::OK();
    }

    Status decompress(const Slice& input, Slice* output) override {
        return Status::InternalError("unimplement: SnappyHadoopBlockCompression::decompress");
    }
};

class ZlibBlockCompression : public BlockCompressionCodec {
public:
    static ZlibBlockCompression* instance() {
        static ZlibBlockCompression s_instance;
        return &s_instance;
    }
    ~ZlibBlockCompression() override = default;

    Status compress(const Slice& input, faststring* output) override {
        size_t max_len = max_compressed_len(input.size);
        output->resize(max_len);
        Slice s(*output);

        auto zres = ::compress((Bytef*)s.data, &s.size, (Bytef*)input.data, input.size);
        if (zres != Z_OK) {
            return Status::InvalidArgument("Fail to do ZLib compress, error={}", zError(zres));
        }
        output->resize(s.size);
        return Status::OK();
    }

    Status compress(const std::vector<Slice>& inputs, size_t uncompressed_size,
                    faststring* output) override {
        size_t max_len = max_compressed_len(uncompressed_size);
        output->resize(max_len);

        z_stream zstrm;
        zstrm.zalloc = Z_NULL;
        zstrm.zfree = Z_NULL;
        zstrm.opaque = Z_NULL;
        auto zres = deflateInit(&zstrm, Z_DEFAULT_COMPRESSION);
        if (zres != Z_OK) {
            return Status::InvalidArgument("Fail to do ZLib stream compress, error={}, res={}",
                                           zError(zres), zres);
        }
        // we assume that output is e
        zstrm.next_out = (Bytef*)output->data();
        zstrm.avail_out = output->size();
        for (int i = 0; i < inputs.size(); ++i) {
            if (inputs[i].size == 0) {
                continue;
            }
            zstrm.next_in = (Bytef*)inputs[i].data;
            zstrm.avail_in = inputs[i].size;
            int flush = (i == (inputs.size() - 1)) ? Z_FINISH : Z_NO_FLUSH;

            zres = deflate(&zstrm, flush);
            if (zres != Z_OK && zres != Z_STREAM_END) {
                return Status::InvalidArgument("Fail to do ZLib stream compress, error={}, res={}",
                                               zError(zres), zres);
            }
        }

        output->resize(zstrm.total_out);
        zres = deflateEnd(&zstrm);
        if (zres != Z_OK) {
            return Status::InvalidArgument("Fail to do deflateEnd on ZLib stream, error={}, res={}",
                                           zError(zres), zres);
        }
        return Status::OK();
    }

    Status decompress(const Slice& input, Slice* output) override {
        size_t input_size = input.size;
        auto zres =
                ::uncompress2((Bytef*)output->data, &output->size, (Bytef*)input.data, &input_size);
        if (zres != Z_OK) {
            return Status::InvalidArgument("Fail to do ZLib decompress, error={}", zError(zres));
        }
        return Status::OK();
    }

    size_t max_compressed_len(size_t len) override {
        // one-time overhead of six bytes for the entire stream plus five bytes per 16 KB block
        return len + 6 + 5 * ((len >> 14) + 1);
    }
};

class Bzip2BlockCompression : public BlockCompressionCodec {
public:
    static Bzip2BlockCompression* instance() {
        static Bzip2BlockCompression s_instance;
        return &s_instance;
    }
    ~Bzip2BlockCompression() override = default;

    Status compress(const Slice& input, faststring* output) override {
        size_t max_len = max_compressed_len(input.size);
        output->resize(max_len);
        uint32_t size = output->size();
        auto bzres = BZ2_bzBuffToBuffCompress((char*)output->data(), &size, (char*)input.data,
                                              input.size, 9, 0, 0);
        if (bzres != BZ_OK) {
            return Status::InternalError("Fail to do Bzip2 compress, ret={}", bzres);
        }
        output->resize(size);
        return Status::OK();
    }

    Status compress(const std::vector<Slice>& inputs, size_t uncompressed_size,
                    faststring* output) override {
        size_t max_len = max_compressed_len(uncompressed_size);
        output->resize(max_len);

        bz_stream bzstrm;
        bzero(&bzstrm, sizeof(bzstrm));
        int bzres = BZ2_bzCompressInit(&bzstrm, 9, 0, 0);
        if (bzres != BZ_OK) {
            return Status::InternalError("Failed to init bz2. status code: {}", bzres);
        }
        // we assume that output is e
        bzstrm.next_out = (char*)output->data();
        bzstrm.avail_out = output->size();
        for (int i = 0; i < inputs.size(); ++i) {
            if (inputs[i].size == 0) {
                continue;
            }
            bzstrm.next_in = (char*)inputs[i].data;
            bzstrm.avail_in = inputs[i].size;
            int flush = (i == (inputs.size() - 1)) ? BZ_FINISH : BZ_RUN;

            bzres = BZ2_bzCompress(&bzstrm, flush);
            if (bzres != BZ_OK && bzres != BZ_STREAM_END) {
                return Status::InternalError("Fail to do bzip2 stream compress, res={}", bzres);
            }
        }

        size_t total_out = (size_t)bzstrm.total_out_hi32 << 32 | (size_t)bzstrm.total_out_lo32;
        output->resize(total_out);
        bzres = BZ2_bzCompressEnd(&bzstrm);
        if (bzres != BZ_OK) {
            return Status::InternalError("Fail to do deflateEnd on bzip2 stream, res={}", bzres);
        }
        return Status::OK();
    }

    Status decompress(const Slice& input, Slice* output) override {
        return Status::InternalError("unimplement: Bzip2BlockCompression::decompress");
    }

    size_t max_compressed_len(size_t len) override {
        // TODO: make sure the max_compressed_len for bzip2
        return len * 2;
    }
};

// for ZSTD compression and decompression, with BOTH fast and high compression ratio
class ZstdBlockCompression : public BlockCompressionCodec {
private:
    class CContext {
        ENABLE_FACTORY_CREATOR(CContext);

    public:
        CContext() : ctx(nullptr) {
            SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
                    ExecEnv::GetInstance()->block_compression_mem_tracker());
            buffer = std::make_unique<faststring>();
        }
        ZSTD_CCtx* ctx;
        std::unique_ptr<faststring> buffer;
        ~CContext() {
            SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
                    ExecEnv::GetInstance()->block_compression_mem_tracker());
            if (ctx) {
                ZSTD_freeCCtx(ctx);
            }
            buffer.reset();
        }
    };
    class DContext {
        ENABLE_FACTORY_CREATOR(DContext);

    public:
        DContext() : ctx(nullptr) {}
        ZSTD_DCtx* ctx;
        ~DContext() {
            if (ctx) {
                ZSTD_freeDCtx(ctx);
            }
        }
    };

public:
    static ZstdBlockCompression* instance() {
        static ZstdBlockCompression s_instance;
        return &s_instance;
    }
    ~ZstdBlockCompression() {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
                ExecEnv::GetInstance()->block_compression_mem_tracker());
        _ctx_c_pool.clear();
        _ctx_d_pool.clear();
    }

    size_t max_compressed_len(size_t len) override { return ZSTD_compressBound(len); }

    Status compress(const Slice& input, faststring* output) override {
        std::vector<Slice> inputs {input};
        return compress(inputs, input.size, output);
    }

    // follow ZSTD official example
    //  https://github.com/facebook/zstd/blob/dev/examples/streaming_compression.c
    Status compress(const std::vector<Slice>& inputs, size_t uncompressed_size,
                    faststring* output) override {
        std::unique_ptr<CContext> context;
        RETURN_IF_ERROR(_acquire_compression_ctx(context));
        bool compress_failed = false;
        Defer defer {[&] {
            if (!compress_failed) {
                _release_compression_ctx(std::move(context));
            }
        }};

        try {
            size_t max_len = max_compressed_len(uncompressed_size);
            Slice compressed_buf;
            if (max_len > MAX_COMPRESSION_BUFFER_SIZE_FOR_REUSE) {
                // use output directly
                output->resize(max_len);
                compressed_buf.data = reinterpret_cast<char*>(output->data());
                compressed_buf.size = max_len;
            } else {
                {
                    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
                            ExecEnv::GetInstance()->block_compression_mem_tracker());
                    // reuse context buffer if max_len <= MAX_COMPRESSION_BUFFER_FOR_REUSE
                    context->buffer->resize(max_len);
                }
                compressed_buf.data = reinterpret_cast<char*>(context->buffer->data());
                compressed_buf.size = max_len;
            }

            // set compression level to default 3
            auto ret = ZSTD_CCtx_setParameter(context->ctx, ZSTD_c_compressionLevel,
                                              ZSTD_CLEVEL_DEFAULT);
            if (ZSTD_isError(ret)) {
                return Status::InvalidArgument("ZSTD_CCtx_setParameter compression level error: {}",
                                               ZSTD_getErrorString(ZSTD_getErrorCode(ret)));
            }
            // set checksum flag to 1
            ret = ZSTD_CCtx_setParameter(context->ctx, ZSTD_c_checksumFlag, 1);
            if (ZSTD_isError(ret)) {
                return Status::InvalidArgument("ZSTD_CCtx_setParameter checksumFlag error: {}",
                                               ZSTD_getErrorString(ZSTD_getErrorCode(ret)));
            }

            ZSTD_outBuffer out_buf = {compressed_buf.data, compressed_buf.size, 0};

            for (size_t i = 0; i < inputs.size(); i++) {
                ZSTD_inBuffer in_buf = {inputs[i].data, inputs[i].size, 0};

                bool last_input = (i == inputs.size() - 1);
                auto mode = last_input ? ZSTD_e_end : ZSTD_e_continue;

                bool finished = false;
                do {
                    // do compress
                    auto ret = ZSTD_compressStream2(context->ctx, &out_buf, &in_buf, mode);

                    if (ZSTD_isError(ret)) {
                        compress_failed = true;
                        return Status::InvalidArgument("ZSTD_compressStream2 error: {}",
                                                       ZSTD_getErrorString(ZSTD_getErrorCode(ret)));
                    }

                    // ret is ZSTD hint for needed output buffer size
                    if (ret > 0 && out_buf.pos == out_buf.size) {
                        compress_failed = true;
                        return Status::InvalidArgument("ZSTD_compressStream2 output buffer full");
                    }

                    finished = last_input ? (ret == 0) : (in_buf.pos == inputs[i].size);
                } while (!finished);
            }

            // set compressed size for caller
            output->resize(out_buf.pos);
            if (max_len <= MAX_COMPRESSION_BUFFER_SIZE_FOR_REUSE) {
                output->assign_copy(reinterpret_cast<uint8_t*>(compressed_buf.data), out_buf.pos);
            }
        } catch (std::exception& e) {
            return Status::InternalError("Fail to do ZSTD compress due to exception {}", e.what());
        } catch (...) {
            // Do not set compress_failed to release context
            DCHECK(!compress_failed);
            return Status::InternalError("Fail to do ZSTD compress due to exception");
        }

        return Status::OK();
    }

    Status decompress(const Slice& input, Slice* output) override {
        std::unique_ptr<DContext> context;
        bool decompress_failed = false;
        RETURN_IF_ERROR(_acquire_decompression_ctx(context));
        Defer defer {[&] {
            if (!decompress_failed) {
                _release_decompression_ctx(std::move(context));
            }
        }};

        size_t ret = ZSTD_decompressDCtx(context->ctx, output->data, output->size, input.data,
                                         input.size);
        if (ZSTD_isError(ret)) {
            decompress_failed = true;
            return Status::InvalidArgument("ZSTD_decompressDCtx error: {}",
                                           ZSTD_getErrorString(ZSTD_getErrorCode(ret)));
        }

        // set decompressed size for caller
        output->size = ret;

        return Status::OK();
    }

private:
    Status _acquire_compression_ctx(std::unique_ptr<CContext>& out) {
        std::lock_guard<std::mutex> l(_ctx_c_mutex);
        if (_ctx_c_pool.empty()) {
            std::unique_ptr<CContext> localCtx = CContext::create_unique();
            if (localCtx.get() == nullptr) {
                return Status::InvalidArgument("failed to new ZSTD CContext");
            }
            //typedef LZ4F_cctx* LZ4F_compressionContext_t;
            localCtx->ctx = ZSTD_createCCtx();
            if (localCtx->ctx == nullptr) {
                return Status::InvalidArgument("Failed to create ZSTD compress ctx");
            }
            out = std::move(localCtx);
            return Status::OK();
        }
        out = std::move(_ctx_c_pool.back());
        _ctx_c_pool.pop_back();
        return Status::OK();
    }
    void _release_compression_ctx(std::unique_ptr<CContext> context) {
        DCHECK(context);
        auto ret = ZSTD_CCtx_reset(context->ctx, ZSTD_reset_session_only);
        DCHECK(!ZSTD_isError(ret));
        std::lock_guard<std::mutex> l(_ctx_c_mutex);
        _ctx_c_pool.push_back(std::move(context));
    }

    Status _acquire_decompression_ctx(std::unique_ptr<DContext>& out) {
        std::lock_guard<std::mutex> l(_ctx_d_mutex);
        if (_ctx_d_pool.empty()) {
            std::unique_ptr<DContext> localCtx = DContext::create_unique();
            if (localCtx.get() == nullptr) {
                return Status::InvalidArgument("failed to new ZSTD DContext");
            }
            localCtx->ctx = ZSTD_createDCtx();
            if (localCtx->ctx == nullptr) {
                return Status::InvalidArgument("Fail to init ZSTD decompress context");
            }
            out = std::move(localCtx);
            return Status::OK();
        }
        out = std::move(_ctx_d_pool.back());
        _ctx_d_pool.pop_back();
        return Status::OK();
    }
    void _release_decompression_ctx(std::unique_ptr<DContext> context) {
        DCHECK(context);
        // reset ctx to start a new decompress session
        auto ret = ZSTD_DCtx_reset(context->ctx, ZSTD_reset_session_only);
        DCHECK(!ZSTD_isError(ret));
        std::lock_guard<std::mutex> l(_ctx_d_mutex);
        _ctx_d_pool.push_back(std::move(context));
    }

private:
    mutable std::mutex _ctx_c_mutex;
    mutable std::vector<std::unique_ptr<CContext>> _ctx_c_pool;

    mutable std::mutex _ctx_d_mutex;
    mutable std::vector<std::unique_ptr<DContext>> _ctx_d_pool;
};

class GzipBlockCompression : public ZlibBlockCompression {
public:
    static GzipBlockCompression* instance() {
        static GzipBlockCompression s_instance;
        return &s_instance;
    }
    ~GzipBlockCompression() override = default;

    Status compress(const Slice& input, faststring* output) override {
        size_t max_len = max_compressed_len(input.size);
        output->resize(max_len);

        z_stream z_strm = {};
        z_strm.zalloc = Z_NULL;
        z_strm.zfree = Z_NULL;
        z_strm.opaque = Z_NULL;

        int zres = deflateInit2(&z_strm, Z_DEFAULT_COMPRESSION, Z_DEFLATED, MAX_WBITS + GZIP_CODEC,
                                8, Z_DEFAULT_STRATEGY);

        if (zres != Z_OK) {
            return Status::InvalidArgument("Fail to init zlib compress");
        }

        z_strm.next_in = (Bytef*)input.get_data();
        z_strm.avail_in = input.get_size();
        z_strm.next_out = (Bytef*)output->data();
        z_strm.avail_out = output->size();

        zres = deflate(&z_strm, Z_FINISH);
        if (zres != Z_OK && zres != Z_STREAM_END) {
            return Status::InvalidArgument("Fail to do ZLib stream compress, error={}, res={}",
                                           zError(zres), zres);
        }

        output->resize(z_strm.total_out);
        zres = deflateEnd(&z_strm);
        if (zres != Z_OK) {
            return Status::InvalidArgument("Fail to end zlib compress");
        }
        return Status::OK();
    }

    Status compress(const std::vector<Slice>& inputs, size_t uncompressed_size,
                    faststring* output) override {
        size_t max_len = max_compressed_len(uncompressed_size);
        output->resize(max_len);

        z_stream zstrm;
        zstrm.zalloc = Z_NULL;
        zstrm.zfree = Z_NULL;
        zstrm.opaque = Z_NULL;
        auto zres = deflateInit2(&zstrm, Z_DEFAULT_COMPRESSION, Z_DEFLATED, MAX_WBITS + GZIP_CODEC,
                                 8, Z_DEFAULT_STRATEGY);
        if (zres != Z_OK) {
            return Status::InvalidArgument("Fail to do ZLib stream compress, error={}, res={}",
                                           zError(zres), zres);
        }
        // we assume that output is e
        zstrm.next_out = (Bytef*)output->data();
        zstrm.avail_out = output->size();
        for (int i = 0; i < inputs.size(); ++i) {
            if (inputs[i].size == 0) {
                continue;
            }
            zstrm.next_in = (Bytef*)inputs[i].data;
            zstrm.avail_in = inputs[i].size;
            int flush = (i == (inputs.size() - 1)) ? Z_FINISH : Z_NO_FLUSH;

            zres = deflate(&zstrm, flush);
            if (zres != Z_OK && zres != Z_STREAM_END) {
                return Status::InvalidArgument("Fail to do ZLib stream compress, error={}, res={}",
                                               zError(zres), zres);
            }
        }

        output->resize(zstrm.total_out);
        zres = deflateEnd(&zstrm);
        if (zres != Z_OK) {
            return Status::InvalidArgument("Fail to do deflateEnd on ZLib stream, error={}, res={}",
                                           zError(zres), zres);
        }
        return Status::OK();
    }

    Status decompress(const Slice& input, Slice* output) override {
        z_stream z_strm = {};
        z_strm.zalloc = Z_NULL;
        z_strm.zfree = Z_NULL;
        z_strm.opaque = Z_NULL;

        int ret = inflateInit2(&z_strm, MAX_WBITS + GZIP_CODEC);
        if (ret != Z_OK) {
            return Status::InternalError("Fail to do ZLib stream compress, error={}, res={}",
                                         zError(ret), ret);
        }

        // 1. set input and output
        z_strm.next_in = reinterpret_cast<Bytef*>(input.data);
        z_strm.avail_in = input.size;
        z_strm.next_out = reinterpret_cast<Bytef*>(output->data);
        z_strm.avail_out = output->size;

        if (z_strm.avail_out > 0) {
            // We only support non-streaming use case  for block decompressor
            ret = inflate(&z_strm, Z_FINISH);
            if (ret != Z_OK && ret != Z_STREAM_END) {
                (void)inflateEnd(&z_strm);
                return Status::InternalError("Fail to do ZLib stream compress, error={}, res={}",
                                             zError(ret), ret);
            }
        }
        (void)inflateEnd(&z_strm);

        return Status::OK();
    }

    size_t max_compressed_len(size_t len) override {
        z_stream zstrm;
        zstrm.zalloc = Z_NULL;
        zstrm.zfree = Z_NULL;
        zstrm.opaque = Z_NULL;
        auto zres = deflateInit2(&zstrm, Z_DEFAULT_COMPRESSION, Z_DEFLATED, MAX_WBITS + GZIP_CODEC,
                                 MEM_LEVEL, Z_DEFAULT_STRATEGY);
        if (zres != Z_OK) {
            // Fall back to zlib estimate logic for deflate, notice this may
            // cause decompress error
            LOG(WARNING) << "Fail to do ZLib stream compress, error=" << zError(zres)
                         << ", res=" << zres;
            return ZlibBlockCompression::max_compressed_len(len);
        } else {
            zres = deflateEnd(&zstrm);
            if (zres != Z_OK) {
                LOG(WARNING) << "Fail to do deflateEnd on ZLib stream, error=" << zError(zres)
                             << ", res=" << zres;
            }
            // Mark, maintainer of zlib, has stated that 12 needs to be added to
            // result for gzip
            // http://compgroups.net/comp.unix.programmer/gzip-compressing-an-in-memory-string-usi/54854
            // To have a safe upper bound for "wrapper variations", we add 32 to
            // estimate
            int upper_bound = deflateBound(&zstrm, len) + 32;
            return upper_bound;
        }
    }

private:
    // Magic number for zlib, see https://zlib.net/manual.html for more details.
    const static int GZIP_CODEC = 16; // gzip
    // The memLevel parameter specifies how much memory should be allocated for
    // the internal compression state.
    const static int MEM_LEVEL = 8;
};

// Only used on x86 or x86_64
#if defined(__x86_64__) || defined(_M_X64) || defined(i386) || defined(__i386__) || \
        defined(__i386) || defined(_M_IX86)
class GzipBlockCompressionByLibdeflate final : public GzipBlockCompression {
public:
    GzipBlockCompressionByLibdeflate() : GzipBlockCompression() {}
    static GzipBlockCompressionByLibdeflate* instance() {
        static GzipBlockCompressionByLibdeflate s_instance;
        return &s_instance;
    }
    ~GzipBlockCompressionByLibdeflate() override = default;

    Status decompress(const Slice& input, Slice* output) override {
        if (input.empty()) {
            output->size = 0;
            return Status::OK();
        }
        thread_local std::unique_ptr<libdeflate_decompressor, void (*)(libdeflate_decompressor*)>
                decompressor {libdeflate_alloc_decompressor(), libdeflate_free_decompressor};
        if (!decompressor) {
            return Status::InternalError("libdeflate_alloc_decompressor error.");
        }
        std::size_t out_len;
        auto result = libdeflate_gzip_decompress(decompressor.get(), input.data, input.size,
                                                 output->data, output->size, &out_len);
        if (result != LIBDEFLATE_SUCCESS) {
            return Status::InternalError("libdeflate_gzip_decompress error, res={}", result);
        }
        return Status::OK();
    }
};
#endif

class LzoBlockCompression final : public BlockCompressionCodec {
public:
    static LzoBlockCompression* instance() {
        static LzoBlockCompression s_instance;
        return &s_instance;
    }

    Status compress(const Slice& input, faststring* output) override {
        return Status::InvalidArgument("not impl lzo compress.");
    }
    size_t max_compressed_len(size_t len) override { return 0; };
    Status decompress(const Slice& input, Slice* output) override {
        auto* input_ptr = input.data;
        auto remain_input_size = input.size;
        auto* output_ptr = output->data;
        auto remain_output_size = output->size;
        auto* output_limit = output->data + output->size;

        // Example:
        // OriginData(The original data will be divided into several large data block.) :
        //      large data block1 | large data block2 | large data block3 | ....
        // The large data block will be divided into several small data block.
        // Suppose a large data block is divided into three small blocks:
        // large data block1:            | small block1 | small block2 | small block3 |
        // CompressData:   <A [B1 compress(small block1) ] [B2 compress(small block1) ] [B3 compress(small block1)]>
        //
        // A : original length of the current block of large data block.
        // sizeof(A) = 4 bytes.
        // A = length(small block1) + length(small block2) + length(small block3)
        // Bx : length of  small data block bx.
        // sizeof(Bx) = 4 bytes.
        // Bx = length(compress(small blockx))
        try {
            while (remain_input_size > 0) {
                if (remain_input_size < 4) {
                    return Status::InvalidArgument(
                            "Need more input buffer to get large_block_uncompressed_len.");
                }

                uint32_t large_block_uncompressed_len = BigEndian::Load32(input_ptr);
                input_ptr += 4;
                remain_input_size -= 4;

                if (remain_output_size < large_block_uncompressed_len) {
                    return Status::InvalidArgument(
                            "Need more output buffer to get uncompressed data.");
                }

                while (large_block_uncompressed_len > 0) {
                    if (remain_input_size < 4) {
                        return Status::InvalidArgument(
                                "Need more input buffer to get small_block_compressed_len.");
                    }

                    uint32_t small_block_compressed_len = BigEndian::Load32(input_ptr);
                    input_ptr += 4;
                    remain_input_size -= 4;

                    if (remain_input_size < small_block_compressed_len) {
                        return Status::InvalidArgument(
                                "Need more input buffer to decompress small block.");
                    }

                    auto small_block_uncompressed_len =
                            orc::lzoDecompress(input_ptr, input_ptr + small_block_compressed_len,
                                               output_ptr, output_limit);

                    input_ptr += small_block_compressed_len;
                    remain_input_size -= small_block_compressed_len;

                    output_ptr += small_block_uncompressed_len;
                    large_block_uncompressed_len -= small_block_uncompressed_len;
                    remain_output_size -= small_block_uncompressed_len;
                }
            }
        } catch (const orc::ParseError& e) {
            //Prevent be from hanging due to orc::lzoDecompress throw exception
            return Status::InternalError("Fail to do LZO decompress, error={}", e.what());
        }
        return Status::OK();
    }
};

Status get_block_compression_codec(segment_v2::CompressionTypePB type,
                                   BlockCompressionCodec** codec) {
    switch (type) {
    case segment_v2::CompressionTypePB::NO_COMPRESSION:
        *codec = nullptr;
        break;
    case segment_v2::CompressionTypePB::SNAPPY:
        *codec = SnappyBlockCompression::instance();
        break;
    case segment_v2::CompressionTypePB::LZ4:
        *codec = Lz4BlockCompression::instance();
        break;
    case segment_v2::CompressionTypePB::LZ4F:
        *codec = Lz4fBlockCompression::instance();
        break;
    case segment_v2::CompressionTypePB::LZ4HC:
        *codec = Lz4HCBlockCompression::instance();
        break;
    case segment_v2::CompressionTypePB::ZLIB:
        *codec = ZlibBlockCompression::instance();
        break;
    case segment_v2::CompressionTypePB::ZSTD:
        *codec = ZstdBlockCompression::instance();
        break;
    default:
        return Status::InternalError("unknown compression type({})", type);
    }

    return Status::OK();
}

// this can only be used in hive text write
Status get_block_compression_codec(TFileCompressType::type type, BlockCompressionCodec** codec) {
    switch (type) {
    case TFileCompressType::PLAIN:
        *codec = nullptr;
        break;
    case TFileCompressType::ZLIB:
        *codec = ZlibBlockCompression::instance();
        break;
    case TFileCompressType::GZ:
        *codec = GzipBlockCompression::instance();
        break;
    case TFileCompressType::BZ2:
        *codec = Bzip2BlockCompression::instance();
        break;
    case TFileCompressType::LZ4BLOCK:
        *codec = HadoopLz4BlockCompression::instance();
        break;
    case TFileCompressType::SNAPPYBLOCK:
        *codec = HadoopSnappyBlockCompression::instance();
        break;
    case TFileCompressType::ZSTD:
        *codec = ZstdBlockCompression::instance();
        break;
    default:
        return Status::InternalError("unsupport compression type({}) int hive text", type);
    }

    return Status::OK();
}

Status get_block_compression_codec(tparquet::CompressionCodec::type parquet_codec,
                                   BlockCompressionCodec** codec) {
    switch (parquet_codec) {
    case tparquet::CompressionCodec::UNCOMPRESSED:
        *codec = nullptr;
        break;
    case tparquet::CompressionCodec::SNAPPY:
        *codec = SnappyBlockCompression::instance();
        break;
    case tparquet::CompressionCodec::LZ4_RAW: // we can use LZ4 compression algorithm parse LZ4_RAW
    case tparquet::CompressionCodec::LZ4:
        *codec = HadoopLz4BlockCompression::instance();
        break;
    case tparquet::CompressionCodec::ZSTD:
        *codec = ZstdBlockCompression::instance();
        break;
    case tparquet::CompressionCodec::GZIP:
// Only used on x86 or x86_64
#if defined(__x86_64__) || defined(_M_X64) || defined(i386) || defined(__i386__) || \
        defined(__i386) || defined(_M_IX86)
        *codec = GzipBlockCompressionByLibdeflate::instance();
#else
        *codec = GzipBlockCompression::instance();
#endif
        break;
    case tparquet::CompressionCodec::LZO:
        *codec = LzoBlockCompression::instance();
        break;
    default:
        return Status::InternalError("unknown compression type({})", parquet_codec);
    }

    return Status::OK();
}

} // namespace doris
