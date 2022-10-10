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

#include <lz4/lz4.h>
#include <lz4/lz4frame.h>
#include <snappy/snappy-sinksource.h>
#include <snappy/snappy.h>
#include <zlib.h>
#include <zstd.h>
#include <zstd_errors.h>

#include <limits>

#include "gutil/strings/substitute.h"
#include "util/defer_op.h"
#include "util/faststring.h"

namespace doris {

using strings::Substitute;

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
    if (uncompressed_size > std::numeric_limits<int32_t>::max()) {
        return true;
    }
    return false;
}

class Lz4BlockCompression : public BlockCompressionCodec {
private:
    struct Context {
        Context() : ctx(nullptr) {}
        LZ4_stream_t* ctx;
        faststring buffer;
    };

public:
    static Lz4BlockCompression* instance() {
        static Lz4BlockCompression s_instance;
        return &s_instance;
    }
    ~Lz4BlockCompression() {
        for (auto ctx : _ctx_pool) {
            _delete_compression_ctx(ctx);
        }
    }

    Status compress(const Slice& input, faststring* output) override {
        if (input.size > INT_MAX) {
            return Status::InvalidArgument(
                    "LZ4 not support those case(input.size>INT_MAX), maybe you should change "
                    "fragment_transmission_compression_codec to snappy, size={}",
                    input.size);
        }

        Context* context;
        RETURN_IF_ERROR(_acquire_compression_ctx(&context));
        bool compress_failed = false;
        Defer defer {[&] {
            if (compress_failed) {
                _delete_compression_ctx(context);
            } else {
                _release_compression_ctx(context);
            }
        }};
        Slice compressed_buf;
        size_t max_len = max_compressed_len(input.size);
        if (max_len > MAX_COMPRESSION_BUFFER_SIZE_FOR_REUSE) {
            // use output directly
            output->resize(max_len);
            compressed_buf.data = reinterpret_cast<char*>(output->data());
            compressed_buf.size = max_len;
        } else {
            // reuse context buffer if max_len < MAX_COMPRESSION_BUFFER_FOR_REUSE
            context->buffer.resize(max_len);
            compressed_buf.data = reinterpret_cast<char*>(context->buffer.data());
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
        if (max_len < MAX_COMPRESSION_BUFFER_SIZE_FOR_REUSE) {
            output->assign_copy(reinterpret_cast<uint8_t*>(compressed_buf.data), compressed_len);
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
    Status _acquire_compression_ctx(Context** out) {
        std::lock_guard<std::mutex> l(_ctx_mutex);
        if (_ctx_pool.empty()) {
            Context* context = new (std::nothrow) Context();
            if (context == nullptr) {
                return Status::InvalidArgument("new LZ4 context error");
            }
            context->ctx = LZ4_createStream();
            if (context->ctx == nullptr) {
                delete context;
                return Status::InvalidArgument("LZ4_createStream error");
            }
            *out = context;
            return Status::OK();
        }
        *out = _ctx_pool.back();
        _ctx_pool.pop_back();
        return Status::OK();
    }
    void _release_compression_ctx(Context* context) {
        DCHECK(context);
        LZ4_resetStream(context->ctx);
        std::lock_guard<std::mutex> l(_ctx_mutex);
        _ctx_pool.push_back(context);
    }
    void _delete_compression_ctx(Context* context) {
        DCHECK(context);
        LZ4_freeStream(context->ctx);
        delete context;
    }

private:
    mutable std::mutex _ctx_mutex;
    mutable std::vector<Context*> _ctx_pool;
    static const int32_t ACCELARATION = 1;
};

// Used for LZ4 frame format, decompress speed is two times faster than LZ4.
class Lz4fBlockCompression : public BlockCompressionCodec {
private:
    struct CContext {
        CContext() : ctx(nullptr) {}
        LZ4F_compressionContext_t ctx;
        faststring buffer;
    };
    struct DContext {
        DContext() : ctx(nullptr) {}
        LZ4F_decompressionContext_t ctx;
    };

public:
    static Lz4fBlockCompression* instance() {
        static Lz4fBlockCompression s_instance;
        return &s_instance;
    }
    ~Lz4fBlockCompression() {
        for (auto ctx : _ctx_c_pool) {
            _delete_compression_ctx(ctx);
        }
        for (auto ctx : _ctx_d_pool) {
            _delete_decompression_ctx(ctx);
        }
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
        CContext* context;
        RETURN_IF_ERROR(_acquire_compression_ctx(&context));
        bool compress_failed = false;
        Defer defer {[&] {
            if (compress_failed) {
                _delete_compression_ctx(context);
            } else {
                _release_compression_ctx(context);
            }
        }};
        Slice compressed_buf;
        size_t max_len = max_compressed_len(uncompressed_size);
        if (max_len > MAX_COMPRESSION_BUFFER_SIZE_FOR_REUSE) {
            // use output directly
            output->resize(max_len);
            compressed_buf.data = reinterpret_cast<char*>(output->data());
            compressed_buf.size = max_len;
        } else {
            // reuse context buffer if max_len < MAX_COMPRESSION_BUFFER_FOR_REUSE
            context->buffer.resize(max_len);
            compressed_buf.data = reinterpret_cast<char*>(context->buffer.data());
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
        if (max_len < MAX_COMPRESSION_BUFFER_SIZE_FOR_REUSE) {
            output->assign_copy(reinterpret_cast<uint8_t*>(compressed_buf.data), offset);
        }

        return Status::OK();
    }

    Status _decompress(const Slice& input, Slice* output) {
        bool decompress_failed = false;
        DContext* context = nullptr;
        RETURN_IF_ERROR(_acquire_decompression_ctx(&context));
        Defer defer {[&] {
            if (decompress_failed) {
                _delete_decompression_ctx(context);
            } else {
                _release_decompression_ctx(context);
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
    Status _acquire_compression_ctx(CContext** out) {
        std::lock_guard<std::mutex> l(_ctx_c_mutex);
        if (_ctx_c_pool.empty()) {
            CContext* context = new (std::nothrow) CContext();
            if (context == nullptr) {
                return Status::InvalidArgument("failed to new LZ4F CContext");
            }
            auto res = LZ4F_createCompressionContext(&context->ctx, LZ4F_VERSION);
            if (LZ4F_isError(res) != 0) {
                return Status::InvalidArgument(strings::Substitute(
                        "LZ4F_createCompressionContext error, res=$0", LZ4F_getErrorName(res)));
            }
            *out = context;
            return Status::OK();
        }
        *out = _ctx_c_pool.back();
        _ctx_c_pool.pop_back();
        return Status::OK();
    }
    void _release_compression_ctx(CContext* context) {
        DCHECK(context);
        std::lock_guard<std::mutex> l(_ctx_c_mutex);
        _ctx_c_pool.push_back(context);
    }
    void _delete_compression_ctx(CContext* context) {
        DCHECK(context);
        LZ4F_freeCompressionContext(context->ctx);
        delete context;
    }

    Status _acquire_decompression_ctx(DContext** out) {
        std::lock_guard<std::mutex> l(_ctx_d_mutex);
        if (_ctx_d_pool.empty()) {
            DContext* context = new (std::nothrow) DContext();
            if (context == nullptr) {
                return Status::InvalidArgument("failed to new LZ4F DContext");
            }
            auto res = LZ4F_createDecompressionContext(&context->ctx, LZ4F_VERSION);
            if (LZ4F_isError(res) != 0) {
                return Status::InvalidArgument(strings::Substitute(
                        "LZ4F_createDeompressionContext error, res=$0", LZ4F_getErrorName(res)));
            }
            *out = context;
            return Status::OK();
        }
        *out = _ctx_d_pool.back();
        _ctx_d_pool.pop_back();
        return Status::OK();
    }
    void _release_decompression_ctx(DContext* context) {
        DCHECK(context);
        // reset decompression context to avoid ERROR_maxBlockSize_invalid
        LZ4F_resetDecompressionContext(context->ctx);
        std::lock_guard<std::mutex> l(_ctx_d_mutex);
        _ctx_d_pool.push_back(context);
    }
    void _delete_decompression_ctx(DContext* context) {
        DCHECK(context);
        LZ4F_freeDecompressionContext(context->ctx);
        delete context;
    }

private:
    static LZ4F_preferences_t _s_preferences;

    std::mutex _ctx_c_mutex;
    // LZ4F_compressionContext_t is a pointer so no copy here
    std::vector<CContext*> _ctx_c_pool;

    std::mutex _ctx_d_mutex;
    std::vector<DContext*> _ctx_d_pool;
};

LZ4F_preferences_t Lz4fBlockCompression::_s_preferences = {
        {LZ4F_max256KB, LZ4F_blockLinked, LZ4F_noContentChecksum, LZ4F_frame, 0ULL, 0U,
         LZ4F_noBlockChecksum},
        0,
        0u,
        0u,
        {0u, 0u, 0u}};

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
        return _slices[_cur_slice].data;
    }

    // Skip the next n bytes.  Invalidates any buffer returned by
    // a previous call to Peek().
    // REQUIRES: Available() >= n
    void Skip(size_t n) override {
        _available -= n;
        do {
            auto left = _slices[_cur_slice].size - _slice_off;
            if (left > n) {
                // n can be digest in current slice
                _slice_off += n;
                return;
            }
            _slice_off = 0;
            _cur_slice++;
            n -= left;
        } while (n > 0);
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
    ~SnappyBlockCompression() override {}

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

class ZlibBlockCompression : public BlockCompressionCodec {
public:
    static ZlibBlockCompression* instance() {
        static ZlibBlockCompression s_instance;
        return &s_instance;
    }
    ~ZlibBlockCompression() {}

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

// for ZSTD compression and decompression, with BOTH fast and high compression ratio
class ZstdBlockCompression : public BlockCompressionCodec {
private:
    struct CContext {
        CContext() : ctx(nullptr) {}
        ZSTD_CCtx* ctx;
        faststring buffer;
    };
    struct DContext {
        DContext() : ctx(nullptr) {}
        ZSTD_DCtx* ctx;
    };

public:
    static ZstdBlockCompression* instance() {
        static ZstdBlockCompression s_instance;
        return &s_instance;
    }
    ~ZstdBlockCompression() {
        for (auto ctx : _ctx_c_pool) {
            _delete_compression_ctx(ctx);
        }
        for (auto ctx : _ctx_d_pool) {
            _delete_decompression_ctx(ctx);
        }
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
        CContext* context;
        RETURN_IF_ERROR(_acquire_compression_ctx(&context));
        bool compress_failed = false;
        Defer defer {[&] {
            if (compress_failed) {
                _delete_compression_ctx(context);
            } else {
                _release_compression_ctx(context);
            }
        }};

        size_t max_len = max_compressed_len(uncompressed_size);
        Slice compressed_buf;
        if (max_len > MAX_COMPRESSION_BUFFER_SIZE_FOR_REUSE) {
            // use output directly
            output->resize(max_len);
            compressed_buf.data = reinterpret_cast<char*>(output->data());
            compressed_buf.size = max_len;
        } else {
            // reuse context buffer if max_len < MAX_COMPRESSION_BUFFER_FOR_REUSE
            context->buffer.resize(max_len);
            compressed_buf.data = reinterpret_cast<char*>(context->buffer.data());
            compressed_buf.size = max_len;
        }

        // set compression level to default 3
        auto ret =
                ZSTD_CCtx_setParameter(context->ctx, ZSTD_c_compressionLevel, ZSTD_CLEVEL_DEFAULT);
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
        if (max_len < MAX_COMPRESSION_BUFFER_SIZE_FOR_REUSE) {
            output->assign_copy(reinterpret_cast<uint8_t*>(compressed_buf.data), out_buf.pos);
        }

        return Status::OK();
    }

    // follow ZSTD official example
    //  https://github.com/facebook/zstd/blob/dev/examples/streaming_decompression.c
    Status decompress(const Slice& input, Slice* output) override {
        DContext* context;
        bool compress_failed = false;
        RETURN_IF_ERROR(_acquire_decompression_ctx(&context));
        Defer defer {[&] {
            if (compress_failed) {
                _delete_decompression_ctx(context);
            } else {
                _release_decompression_ctx(context);
            }
        }};

        ZSTD_inBuffer in_buf = {input.data, input.size, 0};
        ZSTD_outBuffer out_buf = {output->data, output->size, 0};

        while (in_buf.pos < in_buf.size) {
            // do decompress
            auto ret = ZSTD_decompressStream(context->ctx, &out_buf, &in_buf);

            if (ZSTD_isError(ret)) {
                compress_failed = true;
                return Status::InvalidArgument("ZSTD_decompressStream error: {}",
                                               ZSTD_getErrorString(ZSTD_getErrorCode(ret)));
            }

            // ret is ZSTD hint for needed output buffer size
            if (ret > 0 && out_buf.pos == out_buf.size) {
                compress_failed = true;
                return Status::InvalidArgument("ZSTD_decompressStream output buffer full");
            }
        }

        // set decompressed size for caller
        output->size = out_buf.pos;

        return Status::OK();
    }

private:
    Status _acquire_compression_ctx(CContext** out) {
        std::lock_guard<std::mutex> l(_ctx_c_mutex);
        if (_ctx_c_pool.empty()) {
            CContext* context = new (std::nothrow) CContext();
            if (context == nullptr) {
                return Status::InvalidArgument("failed to new ZSTD CContext");
            }
            //typedef LZ4F_cctx* LZ4F_compressionContext_t;
            context->ctx = ZSTD_createCCtx();
            if (context->ctx == nullptr) {
                return Status::InvalidArgument("Failed to create ZSTD compress ctx");
            }
            *out = context;
            return Status::OK();
        }
        *out = _ctx_c_pool.back();
        _ctx_c_pool.pop_back();
        return Status::OK();
    }
    void _release_compression_ctx(CContext* context) {
        DCHECK(context);
        auto ret = ZSTD_CCtx_reset(context->ctx, ZSTD_reset_session_only);
        DCHECK(!ZSTD_isError(ret));
        std::lock_guard<std::mutex> l(_ctx_c_mutex);
        _ctx_c_pool.push_back(context);
    }
    void _delete_compression_ctx(CContext* context) {
        DCHECK(context);
        ZSTD_freeCCtx(context->ctx);
        delete context;
    }

    Status _acquire_decompression_ctx(DContext** out) {
        std::lock_guard<std::mutex> l(_ctx_d_mutex);
        if (_ctx_d_pool.empty()) {
            DContext* context = new (std::nothrow) DContext();
            if (context == nullptr) {
                return Status::InvalidArgument("failed to new ZSTD DContext");
            }
            context->ctx = ZSTD_createDCtx();
            if (context->ctx == nullptr) {
                return Status::InvalidArgument("Fail to init ZSTD decompress context");
            }
            *out = context;
            return Status::OK();
        }
        *out = _ctx_d_pool.back();
        _ctx_d_pool.pop_back();
        return Status::OK();
    }
    void _release_decompression_ctx(DContext* context) {
        DCHECK(context);
        // reset ctx to start a new decompress session
        auto ret = ZSTD_DCtx_reset(context->ctx, ZSTD_reset_session_only);
        DCHECK(!ZSTD_isError(ret));
        std::lock_guard<std::mutex> l(_ctx_d_mutex);
        _ctx_d_pool.push_back(context);
    }
    void _delete_decompression_ctx(DContext* context) {
        DCHECK(context);
        ZSTD_freeDCtx(context->ctx);
        delete context;
    }

private:
    mutable std::mutex _ctx_c_mutex;
    mutable std::vector<CContext*> _ctx_c_pool;

    mutable std::mutex _ctx_d_mutex;
    mutable std::vector<DContext*> _ctx_d_pool;
};

class GzipBlockCompression final : public ZlibBlockCompression {
public:
    static GzipBlockCompression* instance() {
        static GzipBlockCompression s_instance;
        return &s_instance;
    }
    ~GzipBlockCompression() override = default;

    Status decompress(const Slice& input, Slice* output) override {
        z_stream z_strm = {nullptr};
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
    case segment_v2::CompressionTypePB::ZLIB:
        *codec = ZlibBlockCompression::instance();
        break;
    case segment_v2::CompressionTypePB::ZSTD:
        *codec = ZstdBlockCompression::instance();
        break;
    default:
        return Status::NotFound("unknown compression type({})", type);
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
    case tparquet::CompressionCodec::LZ4:
        *codec = Lz4BlockCompression::instance();
        break;
    case tparquet::CompressionCodec::ZSTD:
        *codec = ZstdBlockCompression::instance();
        break;
    case tparquet::CompressionCodec::GZIP:
        *codec = GzipBlockCompression::instance();
        break;
    default:
        return Status::NotFound("unknown compression type({})", parquet_codec);
    }

    return Status::OK();
}

} // namespace doris
