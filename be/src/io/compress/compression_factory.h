#include <memory>

#include "common/status.h"
#include "io/compress/compressor.h"
#include "io/fs/file_writer.h"
namespace doris::io {
class CompressionFactory {
public:
    enum CompressType { UNCOMPRESSED, GZIP, DEFLATE, BZIP2, LZ4FRAME, LZOP, LZ4BLOCK, SNAPPYBLOCK };

    static Status create_compressor(CompressType type, std::unique_ptr<Compressor>* compressor);
    static Status create_compression_file_wrtier(CompressType type,
                                                 std::unique_ptr<FileWriter> writer,
                                                 std::unique_ptr<FileWriter>* result_writer);
};
} // namespace doris::io