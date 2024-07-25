#include "io/compress/compression_file_writer.h"
#include "io/compress/stream_compression_file_writer.h"
namespace doris::io {

Status StreamCompressionFileWriter::appendv(const Slice* data, size_t data_cnt) {
    for (size_t i = 0; i < data_cnt; ++i) {
        
    }
    while (!_compressor->finished()) {
        RETURN_IF_ERROR(compress());
    }
    return Status::OK();
}

Status StreamCompressionFileWriter::init() {
    RETURN_IF_ERROR(CompressionFileWriter::init());
    if (_buffer_size <= 0) {
        return Status::InternalError("buffer size is invalid");
    }
    _buffer = new char[_buffer_size];
}

Status StreamCompressionFileWriter::finish() {
    if (!_compressor->finished()) {
        _compressor->finish();
        while (!_compressor->finished()) {
            RETURN_IF_ERROR(compress());
        }
    }
}

Status StreamCompressionFileWriter::compress() {
    size_t len;
    RETURN_IF_ERROR(_compressor->compress(_buffer, _buffer_size, &len));
    if (len > 0) {
        RETURN_IF_ERROR(_file_writer->append(_buffer, len));
    }
    return Status::OK();
}

} // namespace doris::io