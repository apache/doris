// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <cstdint>
#include <lzo/lzo1x.h>
#include <lzo/lzoconf.h>

// LZO file format constants
const uint8_t LZOP_MAGIC[9] = {0x89, 0x4c, 0x5a, 0x4f, 0x00, 0x0d, 0x0a, 0x1a, 0x0a};
const uint16_t LZOP_VERSION = 0x1040;
const uint16_t MY_LZO_VERSION = 0x2080;  // LZO library version
const uint16_t LZOP_VERSION_NEEDED = 0x0940;
const uint8_t COMPRESSION_METHOD = 1;  // LZO1X
const uint8_t COMPRESSION_LEVEL = 5;
const uint32_t LZOP_FLAGS = 0x00003;  // Use ADLER32 for all checksums
const uint32_t HEADER_SIZE = 34;  // Minimum header size without filename
const uint32_t ADLER32_INIT_VALUE = 1;  // Initial value for Adler32

// Compute Adler-32 checksum (same implementation as in Doris)
uint32_t olap_adler32(uint32_t adler, const char* buf, size_t len) {
    uint32_t s1 = adler & 0xffff;
    uint32_t s2 = (adler >> 16) & 0xffff;
    
    for (size_t i = 0; i < len; i++) {
        s1 = (s1 + (unsigned char)buf[i]) % 65521;
        s2 = (s2 + s1) % 65521;
    }
    
    return (s2 << 16) + s1;
}

class LzoWriter {
public:
    LzoWriter(const std::string& filename) : _filename(filename) {}

    bool init() {
        // Initialize LZO library
        if (lzo_init() != LZO_E_OK) {
            std::cerr << "Failed to initialize LZO library" << std::endl;
            return false;
        }

        _out_file.open(_filename, std::ios::binary);
        if (!_out_file.is_open()) {
            std::cerr << "Failed to open output file: " << _filename << std::endl;
            return false;
        }

        // Allocate work memory for compression
        _wrkmem.resize(LZO1X_1_MEM_COMPRESS);
        
        // Write file header
        write_header();
        return true;
    }

    void write_header() {
        // Prepare header data first
        std::vector<uint8_t> header_data;
        
        // Write magic number (not included in checksum)
        _out_file.write(reinterpret_cast<const char*>(LZOP_MAGIC), sizeof(LZOP_MAGIC));

        // Add version info to header data
        uint16_t version = __builtin_bswap16(LZOP_VERSION);
        header_data.insert(header_data.end(), reinterpret_cast<uint8_t*>(&version), 
                         reinterpret_cast<uint8_t*>(&version) + sizeof(version));
        
        uint16_t lib_version = __builtin_bswap16(MY_LZO_VERSION);
        header_data.insert(header_data.end(), reinterpret_cast<uint8_t*>(&lib_version),
                         reinterpret_cast<uint8_t*>(&lib_version) + sizeof(lib_version));
        
        uint16_t version_needed = __builtin_bswap16(LZOP_VERSION_NEEDED);
        header_data.insert(header_data.end(), reinterpret_cast<uint8_t*>(&version_needed),
                         reinterpret_cast<uint8_t*>(&version_needed) + sizeof(version_needed));
        
        // Add method and level
        header_data.push_back(COMPRESSION_METHOD);
        header_data.push_back(COMPRESSION_LEVEL);

        // Add flags
        uint32_t flags = __builtin_bswap32(LZOP_FLAGS);
        header_data.insert(header_data.end(), reinterpret_cast<uint8_t*>(&flags),
                         reinterpret_cast<uint8_t*>(&flags) + sizeof(flags));

        // Add mode
        uint32_t mode = 0;
        header_data.insert(header_data.end(), reinterpret_cast<uint8_t*>(&mode),
                         reinterpret_cast<uint8_t*>(&mode) + sizeof(mode));

        // Add mtime
        header_data.insert(header_data.end(), reinterpret_cast<uint8_t*>(&mode),
                         reinterpret_cast<uint8_t*>(&mode) + sizeof(mode));
        header_data.insert(header_data.end(), reinterpret_cast<uint8_t*>(&mode),
                         reinterpret_cast<uint8_t*>(&mode) + sizeof(mode));

        // Add filename length
        header_data.push_back(0);

        // Write all header data
        _out_file.write(reinterpret_cast<const char*>(header_data.data()), header_data.size());

        // Calculate and write header checksum
        uint32_t header_checksum = compute_adler32(header_data.data(), header_data.size());
        write_uint32(header_checksum);
    }

    void write_normal_block(const std::string& data) {
        std::vector<uint8_t> compressed_data(data.size() + data.size() / 16 + 64 + 3);
        lzo_uint compressed_len = 0;

        // Compress the data
        int r = lzo1x_1_compress(
            reinterpret_cast<const uint8_t*>(data.data()),
            data.size(),
            compressed_data.data(),
            &compressed_len,
            _wrkmem.data());

        if (r != LZO_E_OK) {
            std::cerr << "Compression failed" << std::endl;
            return;
        }

        std::cout << "Block info:" << std::endl;
        std::cout << "  Original data size: " << data.size() << std::endl;
        std::cout << "  Compressed size: " << compressed_len << std::endl;
        std::cout << "  Original data: '" << data << "'" << std::endl;

        // Write uncompressed size
        write_uint32(data.size());
        
        // If compressed size is not smaller than original size,
        // we will store the original data without compression
        bool is_compressed = compressed_len < data.size();
        
        // Write compressed size (or original size if not compressed)
        write_uint32(is_compressed ? compressed_len : data.size());

        // Write uncompressed checksum
        uint32_t uncompressed_checksum = compute_adler32(
            reinterpret_cast<const uint8_t*>(data.data()), data.size());
        write_uint32(uncompressed_checksum);

        std::cout << "  Uncompressed checksum calculation:" << std::endl;
        std::cout << "    Data length: " << data.size() << std::endl;
        std::cout << "    First few bytes:";
        for (size_t i = 0; i < std::min(data.size(), size_t(16)); ++i) {
            printf(" %02x", (unsigned char)data[i]);
        }
        std::cout << std::endl;
        std::cout << "    Computed checksum: " << std::hex << uncompressed_checksum << std::dec << std::endl;

        if (is_compressed) {
            // Detailed logging of compressed data
            std::cout << "  Complete compressed data:" << std::endl;
            std::cout << "    All bytes:";
            for (size_t i = 0; i < compressed_len; ++i) {
                if (i % 16 == 0) std::cout << std::endl << "    ";
                printf(" %02x", compressed_data[i]);
            }
            std::cout << std::endl;

            // Write compressed checksum
            uint32_t compressed_checksum = compute_adler32(compressed_data.data(), compressed_len);
            write_uint32(compressed_checksum);

            std::cout << "  Compressed checksum calculation:" << std::endl;
            std::cout << "    Data length: " << compressed_len << std::endl;
            std::cout << "    Bytes used for checksum:";
            for (size_t i = 0; i < compressed_len; ++i) {
                if (i % 16 == 0) std::cout << std::endl << "    ";
                printf(" %02x", compressed_data[i]);
            }
            std::cout << std::endl;
            std::cout << "    Computed checksum: " << std::hex << compressed_checksum << std::dec << std::endl;

            // Write compressed data
            _out_file.write(reinterpret_cast<const char*>(compressed_data.data()), compressed_len);
        } else {
            std::cout << "  Data not compressed (compressed size >= original size)" << std::endl;
            // Write original data directly
            _out_file.write(data.data(), data.size());
        }
        std::cout << "----------------------------------------" << std::endl;
    }

    void write_zero_block() {
        // Write a block with uncompressed size = 0 to mark end of file
        write_uint32(0);
    }

    void close() {
        if (_out_file.is_open()) {
            _out_file.close();
        }
    }

private:
    void write_uint8(uint8_t value) {
        _out_file.write(reinterpret_cast<const char*>(&value), sizeof(value));
    }

    void write_uint16(uint16_t value) {
        value = __builtin_bswap16(value);  // Convert to big-endian
        _out_file.write(reinterpret_cast<const char*>(&value), sizeof(value));
    }

    void write_uint32(uint32_t value) {
        value = __builtin_bswap32(value);  // Convert to big-endian
        _out_file.write(reinterpret_cast<const char*>(&value), sizeof(value));
    }

    // Compute Adler-32 checksum using the same implementation as Doris
    uint32_t compute_adler32(const uint8_t* data, size_t len) {
        uint32_t checksum = olap_adler32(ADLER32_INIT_VALUE, reinterpret_cast<const char*>(data), len);
        std::cout << "  Adler32 details:" << std::endl;
        std::cout << "    Input length: " << len << std::endl;
        std::cout << "    Initial value: " << ADLER32_INIT_VALUE << std::endl;
        std::cout << "    Final checksum: " << std::hex << checksum << std::dec << std::endl;
        return checksum;
    }

    std::string _filename;
    std::ofstream _out_file;
    std::vector<uint8_t> _wrkmem;
};

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <output_file>" << std::endl;
        return 1;
    }

    LzoWriter writer(argv[1]);
    if (!writer.init()) {
        return 1;
    }

    // Write a zero-sized block at the begin
    writer.write_zero_block();

    // Write first normal block with test data
    std::string test_data1 = "This is the first block of test data for LZO compression!\n";
    writer.write_normal_block(test_data1);

    // Write a zero-sized block in the middle
    writer.write_zero_block();

    // Write third normal block with different test data
    std::string test_data2 = "This is the third block with more test data for LZO compression!";
    writer.write_normal_block(test_data2);

    // Write a zero-sized block in the end
    writer.write_zero_block();

    // Write a zero-sized block in the end
    writer.write_zero_block();

    writer.close();
    std::cout << "Successfully created LZO file with three blocks (middle block size = 0)" << std::endl;

    return 0;
} 
