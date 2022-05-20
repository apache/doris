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

#include <limits>

#include "gutil/strings/substitute.h"
#include "util/faststring.h"
#include "runtime/threadlocal.h"

namespace doris {

using strings::Substitute;

Status BlockCompressionCodec::compress(const std::vector<Slice>& inputs, Slice* output) const {
    faststring buf;
    // we compute total size to avoid more memory copy
    size_t total_size = Slice::compute_total_size(inputs);
    buf.reserve(total_size);
    for (auto& input : inputs) {
        buf.append(input.data, input.size);
    }
    return compress(buf, output);
}

class Lz4BlockCompression : public BlockCompressionCodec {
public:
    static const Lz4BlockCompression* instance() {
        static Lz4BlockCompression s_instance;
        return &s_instance;
    }
    ~Lz4BlockCompression() override {}

    Status compress(const Slice& input, Slice* output) const override {
        if (input.size > std::numeric_limits<int32_t>::max() ||
            output->size > std::numeric_limits<int32_t>::max()) {
            return Status::InvalidArgument("LZ4 cannot handle data large than 2G");
        }
        auto compressed_len =
                LZ4_compress_default(input.data, output->data, input.size, output->size);
        if (compressed_len == 0) {
            return Status::InvalidArgument(strings::Substitute(
                    "Output buffer's capacity is not enough, size=$0", output->size));
        }
        output->size = compressed_len;
        return Status::OK();
    }

    Status decompress(const Slice& input, Slice* output) const override {
        auto decompressed_len =
                LZ4_decompress_safe(input.data, output->data, input.size, output->size);
        if (decompressed_len < 0) {
            return Status::InvalidArgument(
                    strings::Substitute("fail to do LZ4 decompress, error=$0", decompressed_len));
        }
        output->size = decompressed_len;
        return Status::OK();
    }

    size_t max_compressed_len(size_t len) const override {
        if (len > std::numeric_limits<int32_t>::max()) {
            return 0;
        }
        return LZ4_compressBound(len);
    }
};

class Lz4fCompressionContext {
public:
    Status init(bool is_compress) {
        if (is_compress && !ctx_c_inited) {
            auto ret1 = LZ4F_createCompressionContext(&ctx_c, LZ4F_VERSION);
            if (LZ4F_isError(ret1)) {
                return Status::InvalidArgument(strings::Substitute(
                        "Fail to LZ4F_createCompressionContext, msg=$0", LZ4F_getErrorName(ret1)));
            }
            ctx_c_inited = true;
        }

        if (!is_compress && !ctx_d_inited) {
            auto ret2 = LZ4F_createDecompressionContext(&ctx_d, LZ4F_VERSION);
            if (LZ4F_isError(ret2)) {
                return Status::InvalidArgument(strings::Substitute(
                        "Fail to LZ4F_createDecompressionContext, msg=$0", LZ4F_getErrorName(ret2)));
            }
            ctx_d_inited = true;
        }
        // reset decompression context to avoid ERROR_maxBlockSize_invalid
        if (!is_compress) LZ4F_resetDecompressionContext(ctx_d);

        return Status::OK();
    }

    ~Lz4fCompressionContext() {
        if (ctx_c_inited) LZ4F_freeCompressionContext(ctx_c);
        if (ctx_d_inited) LZ4F_freeDecompressionContext(ctx_d);
    }

    LZ4F_compressionContext_t get_compress_ctx() { return ctx_c; }
    LZ4F_decompressionContext_t get_decompress_ctx() { return ctx_d; }

public:
    LZ4F_compressionContext_t ctx_c;
    bool ctx_c_inited = false;
    LZ4F_decompressionContext_t ctx_d;
    bool ctx_d_inited = false;
};

// a workaroud for no-trival thread_local requiring GLIBC_2.18
//   refer to https://github.com/apache/incubator-doris/pull/7911
class Lz4fCompressionContextPtr {
public:
    Lz4fCompressionContextPtr();
    Lz4fCompressionContext* get();
private:
    DECLARE_STATIC_THREAD_LOCAL(Lz4fCompressionContext, thread_local_lz4f_ctx);
};

DEFINE_STATIC_THREAD_LOCAL(Lz4fCompressionContext, Lz4fCompressionContextPtr, thread_local_lz4f_ctx);

Lz4fCompressionContextPtr::Lz4fCompressionContextPtr() {
    INIT_STATIC_THREAD_LOCAL(Lz4fCompressionContext, thread_local_lz4f_ctx);
}

Lz4fCompressionContext* Lz4fCompressionContextPtr::get() {
    return thread_local_lz4f_ctx;
}

// thread_local context for lz4f compress/decompress
// one lz4f_ctx per each thread for performance and thread safe
inline thread_local Lz4fCompressionContextPtr lz4f_ctx;

// Used for LZ4 frame format, decompress speed is two times faster than LZ4.
class Lz4fBlockCompression : public BlockCompressionCodec {
public:
    static const Lz4fBlockCompression* instance() {
        static Lz4fBlockCompression s_instance;
        return &s_instance;
    }

    ~Lz4fBlockCompression() override {}

    Status compress(const Slice& input, Slice* output) const override {
        std::vector<Slice> inputs {input};
        return compress(inputs, output);
    }

    Status compress(const std::vector<Slice>& inputs, Slice* output) const override {
        auto pctx = lz4f_ctx.get();
        if (pctx && pctx->init(true).ok())
            return _compress(pctx->ctx_c, inputs, output);
        else
            return Status::InvalidArgument("Fail to get thread local lz4f_ctx");
    }

    Status decompress(const Slice& input, Slice* output) const override {
        auto pctx = lz4f_ctx.get();
        if (pctx && pctx->init(false).ok())
            return _decompress(pctx->ctx_d, input, output);
        else
            return Status::InvalidArgument("Fail to get thread local lz4f_ctx");
    }

    size_t max_compressed_len(size_t len) const override {
        if (len > std::numeric_limits<int32_t>::max()) {
            return 0;
        }
        return std::max(LZ4F_compressBound(len, &_s_preferences),
                        LZ4F_compressFrameBound(len, &_s_preferences));
    }

private:
    Status _compress(LZ4F_compressionContext_t ctx, const std::vector<Slice>& inputs,
                     Slice* output) const {
        auto wbytes = LZ4F_compressBegin(ctx, output->data, output->size, &_s_preferences);
        if (LZ4F_isError(wbytes)) {
            return Status::InvalidArgument(strings::Substitute(
                    "Fail to do LZ4F compress begin, res=$0", LZ4F_getErrorName(wbytes)));
        }
        size_t offset = wbytes;
        for (auto input : inputs) {
            wbytes = LZ4F_compressUpdate(ctx, output->data + offset, output->size - offset,
                                         input.data, input.size, nullptr);
            if (LZ4F_isError(wbytes)) {
                return Status::InvalidArgument(strings::Substitute(
                        "Fail to do LZ4F compress update, res=$0", LZ4F_getErrorName(wbytes)));
            }
            offset += wbytes;
        }
        wbytes = LZ4F_compressEnd(ctx, output->data + offset, output->size - offset, nullptr);
        if (LZ4F_isError(wbytes)) {
            return Status::InvalidArgument(strings::Substitute(
                    "Fail to do LZ4F compress end, res=$0", LZ4F_getErrorName(wbytes)));
        }
        offset += wbytes;
        output->size = offset;
        return Status::OK();
    }

    Status _decompress(LZ4F_decompressionContext_t ctx, const Slice& input, Slice* output) const {
        size_t input_size = input.size;
        auto lres =
                LZ4F_decompress(ctx, output->data, &output->size, input.data, &input_size, nullptr);
        if (LZ4F_isError(lres)) {
            return Status::InvalidArgument(strings::Substitute("Fail to do LZ4F decompress, res=$0",
                                                               LZ4F_getErrorName(lres)));
        } else if (input_size != input.size) {
            return Status::InvalidArgument(
                    strings::Substitute("Fail to do LZ4F decompress: trailing data left in "
                                        "compressed data, read=$0 vs given=$1",
                                        input_size, input.size));
        } else if (lres != 0) {
            return Status::InvalidArgument(strings::Substitute(
                    "Fail to do LZ4F decompress: expect more compressed data, expect=$0", lres));
        }
        return Status::OK();
    }

private:
    static LZ4F_preferences_t _s_preferences;
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
    static const SnappyBlockCompression* instance() {
        static SnappyBlockCompression s_instance;
        return &s_instance;
    }
    ~SnappyBlockCompression() override {}

    Status compress(const Slice& input, Slice* output) const override {
        snappy::RawCompress(input.data, input.size, output->data, &output->size);
        return Status::OK();
    }

    Status decompress(const Slice& input, Slice* output) const override {
        if (!snappy::RawUncompress(input.data, input.size, output->data)) {
            return Status::InvalidArgument("Fail to do Snappy decompress");
        }
        // NOTE: GetUncompressedLength only takes O(1) time
        snappy::GetUncompressedLength(input.data, input.size, &output->size);
        return Status::OK();
    }

    Status compress(const std::vector<Slice>& inputs, Slice* output) const override {
        SnappySlicesSource source(inputs);
        snappy::UncheckedByteArraySink sink(output->data);
        output->size = snappy::Compress(&source, &sink);
        return Status::OK();
    }

    size_t max_compressed_len(size_t len) const override {
        return snappy::MaxCompressedLength(len);
    }
};

class ZlibBlockCompression : public BlockCompressionCodec {
public:
    static const ZlibBlockCompression* instance() {
        static ZlibBlockCompression s_instance;
        return &s_instance;
    }
    ~ZlibBlockCompression() {}

    Status compress(const Slice& input, Slice* output) const override {
        auto zres = ::compress((Bytef*)output->data, &output->size, (Bytef*)input.data, input.size);
        if (zres != Z_OK) {
            return Status::InvalidArgument(
                    strings::Substitute("Fail to do ZLib compress, error=$0", zError(zres)));
        }
        return Status::OK();
    }

    Status compress(const std::vector<Slice>& inputs, Slice* output) const override {
        z_stream zstrm;
        zstrm.zalloc = Z_NULL;
        zstrm.zfree = Z_NULL;
        zstrm.opaque = Z_NULL;
        auto zres = deflateInit(&zstrm, Z_DEFAULT_COMPRESSION);
        if (zres != Z_OK) {
            return Status::InvalidArgument(strings::Substitute(
                    "Fail to do ZLib stream compress, error=$0, res=$1", zError(zres), zres));
        }
        // we assume that output is e
        zstrm.next_out = (Bytef*)output->data;
        zstrm.avail_out = output->size;
        for (int i = 0; i < inputs.size(); ++i) {
            if (inputs[i].size == 0) {
                continue;
            }
            zstrm.next_in = (Bytef*)inputs[i].data;
            zstrm.avail_in = inputs[i].size;
            int flush = (i == (inputs.size() - 1)) ? Z_FINISH : Z_NO_FLUSH;

            zres = deflate(&zstrm, flush);
            if (zres != Z_OK && zres != Z_STREAM_END) {
                return Status::InvalidArgument(strings::Substitute(
                        "Fail to do ZLib stream compress, error=$0, res=$1", zError(zres), zres));
            }
        }

        output->size = zstrm.total_out;
        zres = deflateEnd(&zstrm);
        if (zres != Z_OK) {
            return Status::InvalidArgument(strings::Substitute(
                    "Fail to do deflateEnd on ZLib stream, error=$0, res=$1", zError(zres), zres));
        }
        return Status::OK();
    }

    Status decompress(const Slice& input, Slice* output) const override {
        size_t input_size = input.size;
        auto zres =
                ::uncompress2((Bytef*)output->data, &output->size, (Bytef*)input.data, &input_size);
        if (zres != Z_OK) {
            return Status::InvalidArgument(
                    strings::Substitute("Fail to do ZLib decompress, error=$0", zError(zres)));
        }
        return Status::OK();
    }

    size_t max_compressed_len(size_t len) const override {
        // one-time overhead of six bytes for the entire stream plus five bytes per 16 KB block
        return len + 6 + 5 * ((len >> 14) + 1);
    }
};

Status get_block_compression_codec(segment_v2::CompressionTypePB type,
                                   const BlockCompressionCodec** codec) {
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
    default:
        return Status::NotFound(strings::Substitute("unknown compression type($0)", type));
    }
    return Status::OK();
}

} // namespace doris
