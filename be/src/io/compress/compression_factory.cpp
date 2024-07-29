#include "io/compress/compression_factory.h"
namespace doris::io {
Status CompressionFactory::create_compressor(CompressType type,
                                             std::unique_ptr<Compressor>* compressor) {
    switch (type) {
    case CompressType::BZIP2:
        *compressor = std::make_unique<BZip2Compressor>();
        break;
    case CompressType::GZIP:
        *compressor = std::make_unique<GzipCompressor>();
        break;
    default:
        return Status::InternalError("unknown compress type");
    }
    return Status::OK();
}

Status CompressionFactory::create_compression_file_wrtier(
        CompressType type, std::unique_ptr<FileWriter> writer,
        std::unique_ptr<FileWriter>* result_writer) {
            
        }
} // namespace doris::io