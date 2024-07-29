#pragma once

#include "io/compress/compressor.h"
#include "util/byte_buffer.h"
#include "zlib.h"
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
                   size_t direct_buffer_size)
            : _level(level),
              _strategy(strategy),
              _window_bits(header),
              _direct_bufffer_size(direct_buffer_size) {}
    ~ZlibCompressor() override { deflateEnd(&_stream); }
    Status init() override;
    Status set_input(const char* data, size_t length) override;
    bool need_input() override;
    Status compress(char* buffer, size_t length, size_t& compressed_length) override;
    size_t get_bytes_read() override;
    size_t get_bytes_written() override;
    void finish() override;
    bool finished() override;
    void reset() override;

private:
    void _set_input_from_saved_data();
    Status _deflate_direct_buffer(size_t& no_compressed_bytes);
    z_stream _stream;
    const CompressionLevel _level;
    const CompressionStrategy _strategy;
    const CompressionHeader _window_bits;
    const size_t _direct_bufffer_size;
    const char* _user_buffer = nullptr;
    size_t _user_buffer_length = 0;
    ByteBufferPtr _uncompressed_buffer = nullptr;
    bool _keep_uncompressed_buffer = false;
    ByteBufferPtr _compressed_buffer = nullptr;
    bool _finish = false;
    bool _finished = false;
};
} // namespace doris::io
