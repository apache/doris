#pragma once

#include <zlib.h>

#include <memory>

#include "io/compress/compressor.h"
#include "util/byte_buffer.h"
namespace doris::io {

class ZlibCompressor : public Compressor {
public:
    /**
   * The compression level for zlib library.
   */
    enum CompressionLevel {
        /**
     * Compression level for no compression.
     */
        NO_COMPRESSION = 0,

        /**
     * Compression level for fastest compression.
     */
        BEST_SPEED = 1,

        /**
     * Compression level 2.
     */
        TWO = 2,

        /**
     * Compression level 3.
     */
        THREE = 3,

        /**
     * Compression level 4.
     */
        FOUR = 4,

        /**
     * Compression level 5.
     */
        FIVE = 5,

        /**
     * Compression level 6.
     */
        SIX = 6,

        /**
     * Compression level 7.
     */
        SEVEN = 7,

        /**
     * Compression level 8.
     */
        EIGHT = 8,

        /**
     * Compression level for best compression.
     */
        BEST_COMPRESSION = 9,

        /**
     * Default compression level.
     */
        DEFAULT_COMPRESSION = -1,

    };

    /**
   * The compression level for zlib library.
   */
    enum CompressionStrategy {
        /**
     * Compression strategy best used for data consisting mostly of small
     * values with a somewhat random distribution. Forces more Huffman coding
     * and less string matching.
     */
        FILTERED = 2,

        /**
     * Compression strategy for Huffman coding only.
     */
        HUFFMAN_ONLY = 2,

        /**
     * Compression strategy to limit match distances to one
     * (run-length encoding).
     */
        RLE = 3,

        /**
     * Compression strategy to prevent the use of dynamic Huffman codes, 
     * allowing for a simpler decoder for special applications.
     */
        FIXED = 4,

        /**
     * Default compression strategy.
     */
        DEFAULT_STRATEGY = 0,
    };

    /**
   * The type of header for compressed data.
   */
    enum CompressionHeader {
        /**
     * No headers/trailers/checksums.
     */
        NO_HEADER = -15,

        /**
     * Default headers/trailers/checksums.
     */
        DEFAULT_HEADER = 15,

        /**
     * Simple gzip headers/trailers.
     */
        GZIP_FORMAT = 31,

    };

    ZlibCompressor(CompressionLevel level, CompressionStrategy strategy, CompressionHeader header,
                   size_t direct_buffer_size);
    ZlibCompressor();
    ~ZlibCompressor() override;
    Status set_input(const char* data, size_t offset, size_t length) override;
    bool need_input() override;
    size_t get_bytes_read() override;
    Status compress(char* buffer, size_t offset, size_t length, size_t& compressed_length) override;

private:
    z_stream _stream;
    const CompressionLevel _level;
    const CompressionStrategy _strategy;
    const CompressionHeader _window_bits;
    const size_t _direct_bufffer_size;
    char* _user_buffer = nullptr;
    size_t _user_buffer_offset = 0;
    size_t _user_buffer_length = 0;
    ByteBufferPtr _uncompressed_buffer = nullptr;
    size_t _uncompressed_buffer_offset = 0;
    size_t _uncompressed_buffer_length = 0;
    bool _keep_unpressed_buffer = false;
    ByteBufferPtr _compressed_buffer = nullptr;
    bool _finished = false;
};
} // namespace doris::io
