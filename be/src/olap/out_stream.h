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

#ifndef DORIS_BE_SRC_OLAP_COLUMN_FILE_OUT_STREAM_H
#define DORIS_BE_SRC_OLAP_COLUMN_FILE_OUT_STREAM_H

#include "olap/byte_buffer.h"
#include "olap/compress.h"
#include "olap/olap_define.h"
#include "olap/stream_index_writer.h"
#include "olap/stream_name.h"

namespace doris {
class FileHandler;

// Unlike OrcFile, we cannot guarantee the reliability of stored data without HDFS at the bottom, so we must write
// Check value, check this check value when reading data
// Adopt TLV type header, which has sufficient scalability
struct StreamHead {
    enum StreamType { UNCOMPRESSED = 0, COMPRESSED = 1 };
    uint8_t type;         // 256 types, should be enough for future expansion
    uint32_t length : 24; // 24-bit length
    uint32_t checksum;    // 32-bit check value
    StreamHead() : type(COMPRESSED), length(0), checksum(0) {}
} __attribute__((packed));

// Output stream, use a set of ByteBuffer to buffer all data
class OutStream {
public:
    // The output stream supports two modes: compressed or uncompressed. If compression is enabled, the compression function is given
    explicit OutStream(uint32_t buffer_size, Compressor compressor);

    ~OutStream();

    // Output a byte to the stream
    inline OLAPStatus write(char byte) {
        OLAPStatus res = OLAP_SUCCESS;
        if (_current == nullptr) {
            res = _create_new_input_buffer();
            if (res != OLAP_SUCCESS) {
                return res;
            }
        }
        if (_current->remaining() < 1) {
            res = _spill();
            if (res != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to spill current buffer.");
                return res;
            }
            if (_current == nullptr) {
                res = _create_new_input_buffer();
                if (res != OLAP_SUCCESS) {
                    return res;
                }
            }
        }
        return _current->put(byte);
    }

    // Output a piece of data to the stream
    OLAPStatus write(const char* buffer, uint64_t length);

    // Record the current position of the stream in the index entry
    void get_position(PositionEntryWriter* index_entry) const;

    // Returns the size of all data in the stream
    uint64_t get_stream_length() const;

    // Returns the size of the buffer that has been allocated
    uint64_t get_total_buffer_size() const;

    // Output the cached data stream to a file
    OLAPStatus write_to_file(FileHandler* file_handle, uint32_t write_mbytes_per_sec) const;

    bool is_suppressed() const { return _is_suppressed; }
    void suppress() { _is_suppressed = true; }
    // Output data to output_buffers
    OLAPStatus flush();
    // Calculate the crc32 value of the output data
    uint32_t crc32(uint32_t checksum) const;
    const std::vector<StorageByteBuffer*>& output_buffers() { return _output_buffers; }

    void print_position_debug_info() {
        VLOG_TRACE << "compress: " << _spilled_bytes;

        if (_current != nullptr) {
            VLOG_TRACE << "uncompress=" << (_current->position() - sizeof(StreamHead));
        } else {
            VLOG_TRACE << "uncompress 0";
        }
    }

private:
    OLAPStatus _create_new_input_buffer();
    OLAPStatus _write_head(StorageByteBuffer* buf, uint64_t position, StreamHead::StreamType type,
                           uint32_t length);
    OLAPStatus _spill();
    OLAPStatus _compress(StorageByteBuffer* input, StorageByteBuffer* output,
                         StorageByteBuffer* overflow, bool* smaller);
    void _output_uncompress();
    void _output_compressed();
    OLAPStatus _make_sure_output_buffer();

    uint32_t _buffer_size;                           // Compressed block size
    Compressor _compressor;                          // Compression function, if NULL means no compression
    std::vector<StorageByteBuffer*> _output_buffers; // Buffer all output
    bool _is_suppressed;                             // Whether the stream is terminated
    StorageByteBuffer* _current;                     // Cache uncompressed data
    StorageByteBuffer* _compressed;                  // Bytes to be output to output_buffers
    StorageByteBuffer* _overflow;                    // Bytes that can't fit in _output
    uint64_t _spilled_bytes;                         // The number of bytes that have been output to output

    DISALLOW_COPY_AND_ASSIGN(OutStream);
};

// Define the factory method of the output stream
// Host all output streams, and encapsulate information such as compression algorithm, whether to enable Index, block size, etc.
class OutStreamFactory {
public:
    explicit OutStreamFactory(CompressKind compress_kind, uint32_t stream_buffer_size);

    ~OutStreamFactory();

    //The lifetime of the stream after creation is still managed by OutStreamFactory
    OutStream* create_stream(uint32_t column_unique_id, StreamInfoMessage::Kind kind);

    const std::map<StreamName, OutStream*>& streams() const { return _streams; }

private:
    std::map<StreamName, OutStream*> _streams; // All created streams
    CompressKind _compress_kind;
    Compressor _compressor;
    uint32_t _stream_buffer_size;

    DISALLOW_COPY_AND_ASSIGN(OutStreamFactory);
};

/*
class OutStreamBufferWrapper : public std::streambuf {
public:
    OutStreamBufferWrapper(OutStream* output)
        : std::streambuf(),
        _stream(output),
        _skip_size(0) {

    }
    virtual ~OutStreamBufferWrapper() {}
    virtual int_type overflow(typename traits::int_type c = traits::eof()) {
        return c;
    }
protected:
    OutStream* _stream;
    size_t _skip_size;
};
*/

} // namespace doris
#endif // DORIS_BE_SRC_OLAP_COLUMN_FILE_OUT_STREAM_H
