#include "snii/encoding/zstd_codec.h"

#include <zstd.h>

#include <string>

namespace snii {

Status zstd_compress(Slice input, int level, std::vector<uint8_t>* out) {
    size_t bound = ZSTD_compressBound(input.size());
    out->resize(bound);
    size_t n = ZSTD_compress(out->data(), bound, input.data(), input.size(), level);
    if (ZSTD_isError(n)) {
        return Status::Internal(std::string("zstd compress: ") + ZSTD_getErrorName(n));
    }
    out->resize(n);
    return Status::OK();
}

Status zstd_decompress(Slice input, size_t expected_uncomp_len, std::vector<uint8_t>* out) {
    out->resize(expected_uncomp_len);
    size_t n = ZSTD_decompress(out->data(), expected_uncomp_len, input.data(), input.size());
    if (ZSTD_isError(n)) {
        return Status::Corruption(std::string("zstd decompress: ") + ZSTD_getErrorName(n));
    }
    if (n != expected_uncomp_len) {
        return Status::Corruption("zstd decompressed length mismatch");
    }
    return Status::OK();
}

} // namespace snii
