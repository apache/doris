#include "zlib_compressor.h"
#include "util/byte_buffer.h"

namespace doris::io {
ZlibCompressor::ZlibCompressor(CompressionLevel level, CompressionStrategy strategy,
                               CompressionHeader header, size_t direct_buffer_size)
        : _level(level),
          _strategy(strategy),
          _window_bits(header),
          _direct_bufffer_size(direct_buffer_size) {
            _uncompressed_buffer = ByteBuffer::allocate(_direct_bufffer_size);
            _compressed_buffer = ByteBuffer::allocate(_direct_bufffer_size);
            
          }

}; // namespace doris::io