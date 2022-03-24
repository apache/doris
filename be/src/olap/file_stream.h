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

#ifndef DORIS_BE_SRC_OLAP_COLUMN_FILE_FILE_STREAM_H
#define DORIS_BE_SRC_OLAP_COLUMN_FILE_FILE_STREAM_H

#include <gen_cpp/column_data_file.pb.h>

#include <iostream>
#include <istream>
#include <streambuf>
#include <vector>

#include "olap/byte_buffer.h"
#include "olap/compress.h"
#include "olap/file_helper.h"
#include "olap/olap_common.h"
#include "olap/stream_index_reader.h"
#include "util/runtime_profile.h"

namespace doris {

// 定义输入数据流接口
class ReadOnlyFileStream {
public:
    // 构造方法, 使用一组ByteBuffer创建一个InStream
    // 输入的ByteBuffer在流中的位置可以不连续,例如通过Index确定某些数据不需要
    // 读取后,则不读入这部分的数据. 但InStream封装了ByteBuffer不连续这一事实,
    // 从上层使用者来看,依旧是在访问一段连续的流.上层使用者应该保证不读取StorageByteBuffer
    // 之间没有数据的空洞位置.
    //
    // 当使用mmap的时候,这里会退化为只有一个ByteBuffer, 是否使用mmap取决于在性能
    // 调优阶段的测试结果
    //
    // Input:
    //     inputs - 一组ByteBuffer保存具体的流中的数据
    //     offsets - input中每个ByteBuffer的数据在流中的偏移位置
    //     length - 流的总字节长度
    //     Decompressor - 如果流被压缩过,则提供一个解压缩函数,否则为NULL
    //     compress_buffer_size - 如果使用压缩,给出压缩的块大小
    ReadOnlyFileStream(FileHandler* handler, StorageByteBuffer** shared_buffer,
                       Decompressor decompressor, uint32_t compress_buffer_size,
                       OlapReaderStatistics* stats);

    ReadOnlyFileStream(FileHandler* handler, StorageByteBuffer** shared_buffer, uint64_t offset,
                       uint64_t length, Decompressor decompressor, uint32_t compress_buffer_size,
                       OlapReaderStatistics* stats);

    ~ReadOnlyFileStream() { SAFE_DELETE(_compressed_helper); }

    inline OLAPStatus init() {
        _compressed_helper = StorageByteBuffer::create(_compress_buffer_size);
        if (nullptr == _compressed_helper) {
            OLAP_LOG_WARNING("fail to create compressed buffer");
            return OLAP_ERR_MALLOC_ERROR;
        }

        _uncompressed = nullptr;
        return OLAP_SUCCESS;
    }

    inline void reset(uint64_t offset, uint64_t length) { _file_cursor.reset(offset, length); }

    // 从数据流中读取一个字节,内部指针后移
    // 如果数据流结束, 返回OLAP_ERR_COLUMN_STREAM_EOF
    inline OLAPStatus read(char* byte);

    // 从数据流读入一段数据
    // Input:
    //     buffer - 存储读入的数据
    //     buf_size - 输入时给出buffer的大小,返回时给出实际读取的字节数
    // 如果数据流结束, 返回OLAP_ERR_COLUMN_STREAM_EOF
    inline OLAPStatus read(char* buffer, uint64_t* buf_size);

    inline OLAPStatus read_all(char* buffer, uint64_t* buf_size);
    // 设置读取的位置
    OLAPStatus seek(PositionProvider* position);

    // 跳过指定size的流
    OLAPStatus skip(uint64_t skip_length);

    // 返回流的總長度
    uint64_t stream_length() { return _file_cursor.length(); }

    bool eof() {
        if (_uncompressed == nullptr) {
            return _file_cursor.eof();
        } else {
            return _file_cursor.eof() && _uncompressed->remaining() == 0;
        }
    }

    // 返回当前块剩余可读字节数
    uint64_t available();

    size_t get_buffer_size() { return _compress_buffer_size; }

    inline void get_buf(char** buf, uint32_t* remaining_bytes) {
        if (UNLIKELY(_uncompressed == nullptr)) {
            *buf = nullptr;
            *remaining_bytes = 0;
        } else {
            *buf = _uncompressed->array();
            *remaining_bytes = _uncompressed->remaining();
        }
    }

    inline void get_position(uint32_t* position) { *position = _uncompressed->position(); }

    inline void set_position(uint32_t pos) { _uncompressed->set_position(pos); }

    inline int remaining() {
        if (_uncompressed == nullptr) {
            return 0;
        }
        return _uncompressed->remaining();
    }

private:
    // Use to read a specified range in file
    class FileCursor {
    public:
        FileCursor(FileHandler* file_handler, size_t offset, size_t length)
                : _file_handler(file_handler), _offset(offset), _length(length), _used(0) {}

        ~FileCursor() {}

        void reset(size_t offset, size_t length) {
            _offset = offset;
            _length = length;
            _used = 0;
        }

        OLAPStatus read(char* out_buffer, size_t length) {
            if (_used + length <= _length) {
                OLAPStatus res = _file_handler->pread(out_buffer, length, _used + _offset);
                if (OLAP_SUCCESS != res) {
                    OLAP_LOG_WARNING("fail to read from file. [res=%d]", res);
                    return res;
                }

                _used += length;
            } else {
                return OLAP_ERR_COLUMN_STREAM_EOF;
            }

            return OLAP_SUCCESS;
        }

        size_t position() { return _used; }

        size_t remain() { return _length - _used; }

        size_t length() { return _length; }

        inline bool eof() { return _used == _length; }

        OLAPStatus seek(size_t offset) {
            if (offset > _length) {
                return OLAP_ERR_OUT_OF_BOUND;
            }

            _used = offset;
            return OLAP_SUCCESS;
        }

        const std::string& file_name() const { return _file_handler->file_name(); }

        size_t offset() const { return _offset; }

    private:
        FileHandler* _file_handler;
        size_t _offset; // start from where
        size_t _length; // length limit
        size_t _used;
    };

    OLAPStatus _assure_data();
    OLAPStatus _fill_compressed(size_t length);

    FileCursor _file_cursor;
    StorageByteBuffer* _compressed_helper;
    StorageByteBuffer* _uncompressed;
    StorageByteBuffer** _shared_buffer;

    Decompressor _decompressor;
    size_t _compress_buffer_size;
    size_t _current_compress_position;

    OlapReaderStatistics* _stats;

    DISALLOW_COPY_AND_ASSIGN(ReadOnlyFileStream);
};

inline OLAPStatus ReadOnlyFileStream::read(char* byte) {
    OLAPStatus res = _assure_data();

    if (OLAP_SUCCESS != res) {
        return res;
    }

    res = _uncompressed->get(byte);
    return res;
}

inline OLAPStatus ReadOnlyFileStream::read(char* buffer, uint64_t* buf_size) {
    OLAPStatus res;
    uint64_t read_length = *buf_size;
    *buf_size = 0;

    do {
        res = _assure_data();
        if (OLAP_SUCCESS != res) {
            break;
        }

        uint64_t actual_length = std::min(read_length - *buf_size, _uncompressed->remaining());

        res = _uncompressed->get(buffer, actual_length);
        if (OLAP_SUCCESS != res) {
            break;
        }

        *buf_size += actual_length;
        buffer += actual_length;
    } while (*buf_size < read_length);

    return res;
}

inline OLAPStatus ReadOnlyFileStream::read_all(char* buffer, uint64_t* buffer_size) {
    OLAPStatus res;
    uint64_t read_length = 0;
    uint64_t buffer_remain = *buffer_size;

    while (OLAP_SUCCESS == _assure_data()) {
        read_length = _uncompressed->remaining();

        if (buffer_remain < read_length) {
            res = OLAP_ERR_BUFFER_OVERFLOW;
            break;
        }

        res = _uncompressed->get(buffer, read_length);
        if (OLAP_SUCCESS != res) {
            break;
        }

        buffer_remain -= read_length;
        buffer += read_length;
    }

    if (eof()) {
        *buffer_size -= buffer_remain;
        return OLAP_SUCCESS;
    }

    return res;
}

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_COLUMN_FILE_FILE_STREAM_H
