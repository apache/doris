#include "zlib_compressor.h"

#include <zlib.h>

#include <algorithm>

#include "common/status.h"
#include "util/byte_buffer.h"

namespace doris::io {
Status ZlibCompressor::init() {
    _stream = {};
    _stream.zalloc = Z_NULL;
    _stream.zfree = Z_NULL;
    _stream.opaque = Z_NULL;

    int ret = deflateInit2(&_stream, _level, Z_DEFLATED, _window_bits, 8, _strategy);
    if (ret != Z_OK) {
        return Status::InternalError("deflateInit2 failed, error: {}", ret);
    }

    _uncompressed_buffer = ByteBuffer::allocate(_direct_bufffer_size);
    _compressed_buffer = ByteBuffer::allocate(_direct_bufffer_size);
    _compressed_buffer->pos = _direct_bufffer_size;
    return Status::OK();
}

Status ZlibCompressor::set_input(const char* data, size_t length) {
    _user_buffer = data;
    _user_buffer_length = length;
    _set_input_from_saved_data();

    // Reinitialize zlib's output direct buffer
    _compressed_buffer->limit = _direct_bufffer_size;
    _compressed_buffer->pos = _direct_bufffer_size;
    return Status::OK();
}

void ZlibCompressor::_set_input_from_saved_data() {
    size_t len = std::min(_user_buffer_length, _uncompressed_buffer->remaining());
    _uncompressed_buffer->put_bytes(_user_buffer, len);
    _user_buffer += len;
    _user_buffer_length -= len;
}
bool ZlibCompressor::need_input() {
    // Consume remaining compressed data?
    if (_compressed_buffer->has_remaining()) {
        return false;
    }

    // Check if zlib has consumed all input
    // compress should be invoked if keepUncompressedBuf true
    if (_keep_uncompressed_buffer && _uncompressed_buffer->pos > 0) {
        return false;
    }

    if (_uncompressed_buffer->remaining() > 0) {
        // Check if we have consumed all user-input
        if (_user_buffer_length <= 0) {
            return true;
        } else {
            // copy enough data from userBuf to uncompressedDirectBuf
            _set_input_from_saved_data();
            return _uncompressed_buffer->has_remaining();
        }
    }
    return false;
}
Status ZlibCompressor::compress(char* buffer, size_t length, size_t& compressed_length) {
    compressed_length = _compressed_buffer->remaining();
    if (compressed_length > 0) {
        compressed_length = std::min(length, compressed_length);
        _compressed_buffer->get_bytes(buffer, compressed_length);
        return Status::OK();
    }

    // Re-initialize the zlib's output direct buffer
    _compressed_buffer->pos = 0;
    _compressed_buffer->limit = _direct_bufffer_size;

    RETURN_IF_ERROR(_deflate_direct_buffer(compressed_length));
    _compressed_buffer->limit = compressed_length;

    // Check if zlib consumed all input buffer
    // set keepUncompressedBuf properly
    if (_uncompressed_buffer->pos <= 0) { // zlib consumed all input buffer
        _keep_uncompressed_buffer = false;
        _uncompressed_buffer->pos = 0;
        _uncompressed_buffer->limit = _direct_bufffer_size;
    } else {
        _keep_uncompressed_buffer = true;
    }

    compressed_length = std::min(length, compressed_length);
    _compressed_buffer->get_bytes(buffer, compressed_length);
    return Status::OK();
}

Status ZlibCompressor::_deflate_direct_buffer(size_t& no_compressed_bytes) {
    _stream.next_in = (Bytef*)(_uncompressed_buffer->ptr + _uncompressed_buffer->pos);
    _stream.avail_in = _uncompressed_buffer->pos;
    _stream.next_out = (Bytef*)_compressed_buffer->ptr;
    _stream.avail_out = _compressed_buffer->remaining();

    int ret = deflate(&_stream, _finish ? Z_FINISH : Z_NO_FLUSH);
    switch (ret) {
    case Z_STREAM_END:
        _finished = true;
        break;
    case Z_OK:
        // TODO:
        break;
    case Z_BUF_ERROR:
        break;
    default:
        return Status::InternalError("deflate failed, error: {}", _stream.msg);
    }
    return Status::OK();
}

size_t ZlibCompressor::get_bytes_read() {
    return _stream.total_in;
}
size_t ZlibCompressor::get_bytes_written() {
    return _stream.total_out;
}
void ZlibCompressor::finish() {
    _finished = true;
}
bool ZlibCompressor::finished() {
    return _finished && !_compressed_buffer->has_remaining();
}
void ZlibCompressor::reset() {
    int ret = deflateReset(&_stream);
    
}

}; // namespace doris::io