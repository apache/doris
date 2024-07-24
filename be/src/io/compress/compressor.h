#pragma once

#include <cstddef>
#include <cstring>

#include "common/status.h"
namespace doris::io {

class Compressor {
public:
    virtual ~Compressor() = default;
    virtual Status set_input(const char* data, size_t offset, size_t length) = 0;
    virtual bool need_input() = 0;
    virtual size_t get_bytes_read() = 0;
    virtual Status compress(char* buffer, size_t offset, size_t length,
                            size_t& compressed_length) = 0;
    virtual size_t get_bytes_written() = 0;
    virtual void finish() = 0;
    virtual void reset() = 0;
    virtual std::string debug_string() = 0;
};

} // namespace doris::io