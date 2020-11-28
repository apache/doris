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

#include "gutil/strings/substitute.h"
#include "util/faststring.h"

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

    size_t max_compressed_len(size_t len) const override { return LZ4_compressBound(len); }
};

// Used for LZ4 frame format, decompress speed is two times faster than LZ4.
class Lz4fBlockCompression : public BlockCompressionCodec {
public:
    static const Lz4fBlockCompression* instance() {
        static Lz4fBlockCompression s_instance;
        return &s_instance;
    }

    ~Lz4fBlockCompression() override {}

    Status compress(const Slice& input, Slice* output) const override {
        auto compressed_len = LZ4F_compressFrame(output->data, output->size, input.data, input.size,
                                                 &_s_preferences);
        if (LZ4F_isError(compressed_len)) {
            return Status::InvalidArgument(strings::Substitute(
                    "Fail to do LZ4F compress frame, msg=$0", LZ4F_getErrorName(compressed_len)));
        }
        output->size = compressed_len;
        return Status::OK();
    }

    Status compress(const std::vector<Slice>& inputs, Slice* output) const override {
        LZ4F_compressionContext_t ctx = nullptr;
        auto lres = LZ4F_createCompressionContext(&ctx, LZ4F_VERSION);
        if (lres != 0) {
            return Status::InvalidArgument(strings::Substitute("Fail to do LZ4F compress, res=$0",
                                                               LZ4F_getErrorName(lres)));
        }
        auto st = _compress(ctx, inputs, output);
        LZ4F_freeCompressionContext(ctx);
        return st;
    }

    Status decompress(const Slice& input, Slice* output) const override {
        LZ4F_decompressionContext_t ctx;
        auto lres = LZ4F_createDecompressionContext(&ctx, LZ4F_VERSION);
        if (LZ4F_isError(lres)) {
            return Status::InvalidArgument(strings::Substitute("Fail to do LZ4F decompress, res=$0",
                                                               LZ4F_getErrorName(lres)));
        }
        auto st = _decompress(ctx, input, output);
        LZ4F_freeDecompressionContext(ctx);
        return st;
    }

    size_t max_compressed_len(size_t len) const override {
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
        {
                LZ4F_max256KB,
                LZ4F_blockLinked,
                LZ4F_noContentChecksum,
                LZ4F_frame,
                0,     // unknown content size,
                {0, 0} // reserved, must be set to 0
        },
        0,            // compression level; 0 == default
        0,            // autoflush
        {0, 0, 0, 0}, // reserved, must be set to 0
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
