// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#ifndef BDG_PALO_BE_SRC_OLAP_COLUMN_FILE_IN_STREAM_H
#define BDG_PALO_BE_SRC_OLAP_COLUMN_FILE_IN_STREAM_H

#include <gen_cpp/column_data_file.pb.h>

#include <iostream>
#include <istream>
#include <streambuf>
#include <vector>

#include "olap/column_file/byte_buffer.h"
#include "olap/column_file/compress.h"
#include "olap/column_file/stream_index_reader.h"
#include "olap/olap_common.h"

namespace palo {
namespace column_file {

// 提供Column Reader的Seek位置, 由于ColumnReader的seek需要多个position地址
// PositionProvider 提供了next方法, 将对position的操作封装为stack的形式

// 定义输入数据流接口
class InStream {
public:
    // 构造方法, 使用一组ByteBuffer创建一个InStream
    // 输入的ByteBuffer在流中的位置可以不连续,例如
    // 通过Index确定某些数据不需要
    // 读取后,则不读入这部分的数据. 但InStream封装
    // 了ByteBuffer不连续这一事实,
    // 从上层使用者来看,依旧是在访问一段连续的流.
    // 上层使用者应该保证不读取ByteBuffer
    // 之间没有数据的空洞位置.
    //
    // 当使用mmap的时候,这里会退化为只有一个ByteBuffer, 是�
    // ��使用mmap取决于在性能
    // 调优阶段的测试结果
    //
    // Input:
    //     inputs - 一组ByteBuffer保存具体的流中的数据
    //     offsets - input中每个ByteBuffer的数据在流中的偏移位置
    //     length - 流的总字节长度
    //     Decompressor - 如果流被压缩过,则提供一个解压缩函数,否则为NULL
    //     compress_buffer_size - 如果使用压缩,给出压缩的块大小
    explicit InStream(std::vector<ByteBuffer*>* inputs,
            const std::vector<uint64_t>& offsets,
            uint64_t length,
            Decompressor decompressor,
            uint32_t compress_buffer_size);

    ~InStream();

    // 从数据流中读取一个字节,内部指针后移
    // 如果数据流结束, 返回OLAP_ERR_COLUMN_STREAM_EOF
    inline OLAPStatus read(char* byte);

    // 从数据流读入一段数据
    // Input:
    //     buffer - 存储读入的数据
    //     buf_size - 输入时给出buffer的大小,返回时给出实际读取的字节数
    // 如果数据流结束, 返回OLAP_ERR_COLUMN_STREAM_EOF
    inline OLAPStatus read(char* buffer, uint64_t* buf_size);

    // 设置读取的位置
    OLAPStatus seek(PositionProvider* position);

    // 跳过指定size的流
    OLAPStatus skip(uint64_t skip_length);

    // 返回流的總長度
    uint64_t stream_length() {
        uint64_t length = 0;

        for (size_t buffer_index = 0; buffer_index < _inputs.size(); ++buffer_index) {
            length += _inputs[buffer_index]->limit();
        }

        return length;
    }

    uint64_t estimate_uncompressed_length() {
        return _inputs.size() * _compress_buffer_size;
    }

    bool eof() {
        return _current_offset == _length;
    }

    // 返回当前块剩余可读字节数
    uint64_t available();

    // 返回当前块剩余的内存
    const char* available_buffer() {
        if (OLAP_SUCCESS == _assure_data()) {
            size_t offset = _uncompressed->position();
            return _uncompressed->array(offset);
        }

        return NULL;
    }
private:
    OLAPStatus _assure_data();
    OLAPStatus _slice(uint64_t chunk_size, ByteBuffer** out_slice);
    OLAPStatus _seek(uint64_t position);

    std::vector<ByteBuffer*> _inputs;
    std::vector<uint64_t> _offsets;
    uint64_t _length;
    Decompressor _decompressor;
    uint32_t _compress_buffer_size;
    uint64_t _current_offset;
    uint64_t _current_range;
    ByteBuffer* _compressed;
    ByteBuffer* _uncompressed;

    DISALLOW_COPY_AND_ASSIGN(InStream);
};

// byte buffer的封装， 用于流式读取（暂时用于支持pb的流式反序列化）
// 其实也可以直接和instream合在一起，先这么写着
class InStreamBufferWrapper : public std::streambuf {
public:
    InStreamBufferWrapper(InStream* input) : 
            std::streambuf(),
            _stream(input),
            _skip_size(0) {

    }
    virtual ~InStreamBufferWrapper() {}
    virtual int_type underflow() {
        if (NULL != _stream) {
            if (OLAP_SUCCESS == _stream->skip(_skip_size)) {
                char* buf = const_cast<char*>(_stream->available_buffer());

                if (NULL != buf) {
                    size_t read_length = _stream->available();
                    setg(buf, buf, buf + read_length);
                    _skip_size = read_length;
                    return traits_type::to_int_type(*gptr());
                }
            }
        }

        return traits_type::eof();
    }
protected:
    InStream* _stream;
    size_t _skip_size;
};

inline OLAPStatus InStream::read(char* byte) {
    OLAPStatus res;

    if (OLAP_SUCCESS != (res = _assure_data())) {
        return res;
    }

    return _uncompressed->get(byte);
}

inline OLAPStatus InStream::read(char* buffer, uint64_t* buf_size) {
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

}  // namespace column_file
}  // namespace palo

#endif // BDG_PALO_BE_SRC_OLAP_COLUMN_FILE_IN_STREAM_H
