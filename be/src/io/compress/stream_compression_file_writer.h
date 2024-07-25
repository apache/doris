#include <memory>

#include "common/status.h"
#include "io/compress/compression_file_writer.h"

namespace doris::io {
class StreamCompressionFileWriter : public CompressionFileWriter {
public:
    StreamCompressionFileWriter(std::unique_ptr<FileWriter> file_writer,
                                std::unique_ptr<Compressor> compressor, size_t buffer_size)
            : CompressionFileWriter(file_writer, compressor), _buffer_size(buffer_size) {}
    ~StreamCompressionFileWriter() override {
        if (_buffer != nullptr) {
            delete[] _buffer;
            _buffer = nullptr;
        }
    };

    Status appendv(const Slice* data, size_t data_cnt) override;
    Status init() override;
    Status finish() override;

protected:
    virtual Status compress();
    char* _buffer = nullptr;
    size_t _buffer_size = 0;
};

} // namespace doris::io