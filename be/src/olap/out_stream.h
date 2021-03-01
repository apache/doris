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

// 与OrcFile不同,我们底层没有HDFS无法保证存储数据的可靠性,所以必须写入
// 校验值,在读取数据的时候检验这一校验值
// 采用TLV类型的头部,有足够的扩展性
struct StreamHead {
    enum StreamType { UNCOMPRESSED = 0, COMPRESSED = 1 };
    uint8_t type;         // 256种类型, 应该足够以后的扩展了
    uint32_t length : 24; // 24位长度
    uint32_t checksum;    // 32位校验值
    StreamHead() : type(COMPRESSED), length(0), checksum(0) {}
} __attribute__((packed));

// 输出流,使用一组ByteBuffer缓存所有的数据
class OutStream {
public:
    // 输出流支持压缩或者不压缩两种模式,如果启用压缩,给出压缩函数
    explicit OutStream(uint32_t buffer_size, Compressor compressor);

    ~OutStream();

    // 向流输出一个字节
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

    // 向流输出一段数据
    OLAPStatus write(const char* buffer, uint64_t length);

    // 将流的当前位置记录在索引项中
    void get_position(PositionEntryWriter* index_entry) const;

    // 返回流中所有数据的大小
    uint64_t get_stream_length() const;

    // 返回已经分配的缓冲区大小
    uint64_t get_total_buffer_size() const;

    // 将缓存的数据流输出到文件
    OLAPStatus write_to_file(FileHandler* file_handle, uint32_t write_mbytes_per_sec) const;

    bool is_suppressed() const { return _is_suppressed; }
    void suppress() { _is_suppressed = true; }
    // 将数据输出到output_buffers
    OLAPStatus flush();
    // 计算输出数据的crc32值
    uint32_t crc32(uint32_t checksum) const;
    const std::vector<StorageByteBuffer*>& output_buffers() { return _output_buffers; }

    void print_position_debug_info() {
        VLOG_TRACE << "compress: " << _spilled_bytes;

        if (_current != NULL) {
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

    uint32_t _buffer_size;                           // 压缩块大小
    Compressor _compressor;                          // 压缩函数,如果为NULL表示不压缩
    std::vector<StorageByteBuffer*> _output_buffers; // 缓冲所有的输出
    bool _is_suppressed;                             // 流是否被终止
    StorageByteBuffer* _current;                     // 缓存未压缩的数据
    StorageByteBuffer* _compressed;                  // 即将输出到output_buffers中的字节
    StorageByteBuffer* _overflow;                    // _output中放不下的字节
    uint64_t _spilled_bytes;                         // 已经输出到output的字节数

    DISALLOW_COPY_AND_ASSIGN(OutStream);
};

// 定义输出流的工厂方法
// 将所有的输出流托管,同时封装了诸如压缩算法,是否启用Index,block大小等信息
class OutStreamFactory {
public:
    explicit OutStreamFactory(CompressKind compress_kind, uint32_t stream_buffer_size);

    ~OutStreamFactory();

    // 创建后的stream的生命期依旧由OutStreamFactory管理
    OutStream* create_stream(uint32_t column_unique_id, StreamInfoMessage::Kind kind);

    const std::map<StreamName, OutStream*>& streams() const { return _streams; }

private:
    std::map<StreamName, OutStream*> _streams; // 所有创建过的流
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
