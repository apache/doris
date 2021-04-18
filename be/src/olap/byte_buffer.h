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

#ifndef DORIS_BE_SRC_OLAP_COLUMN_FILE_BYTE_BUFFER_H
#define DORIS_BE_SRC_OLAP_COLUMN_FILE_BYTE_BUFFER_H

#include <boost/shared_ptr.hpp>

#include "olap/file_helper.h"
#include "olap/olap_define.h"
#include "util/mem_util.hpp"

namespace doris {

// ByteBuffer是用于数据缓存的一个类
// ByteBuffer内部维护一个char数组用于缓存数据;
// 同时ByteBuffer维护内部指针用于数据读写;
//
// ByteBuffer有如下几个重要的使用概念:
//     capacity - 缓存区的容量, 在初始化时设立, 是内部char数组的大小
//     position - 当前内部指针的位置
//     limit - 最大使用限制, 这个值小于等于capacity, position始终小于limit
//
// ByteBuffer支持直接利用拷贝构造函数或者=操作符安全的进行数据的浅拷贝
class StorageByteBuffer {
public:
    // 通过new方法创建一个容量为capacity的StorageByteBuffer.
    // 新buffer的position为0, limit为capacity
    // 调用者获得新建的ByteBuffer的所有权,并需使用delete删除获得的StorageByteBuffer
    //
    // TODO. 我认为这里create用法应该是直接返回ByteBuffer本身而不是智能指
    // 针，否则智能指针就无法发挥作用
    //  目前内存的管理还是手动的。而且需要认为delete。
    static StorageByteBuffer* create(uint64_t capacity);

    // 通过引用另一个ByteBuffer的内存创建一个新的StorageByteBuffer
    // 新buffer的position为0, limit为length
    // 调用者获得新建的ByteBuffer的所有权,并需使用delete删除获得的StorageByteBuffer
    // Inputs:
    //   - reference 引用的内存
    //   - offset 引用的Buffer在原ByteBuffer中的位置, 即&reference->array()[offset]
    //   - length 引用的Buffer的长度
    // Notes:
    //   offset + length < reference->capacity
    //
    // TODO. 同create
    static StorageByteBuffer* reference_buffer(StorageByteBuffer* reference, uint64_t offset,
                                               uint64_t length);

    // 通过mmap创建一个ByteBuffer, mmap成功后的内存由ByteBuffer托管
    // start, length, prot, flags, fd, offset都是mmap函数的参数
    // 调用者获得新建的ByteBuffer的所有权,并需使用delete删除获得的StorageByteBuffer
    static StorageByteBuffer* mmap(void* start, uint64_t length, int prot, int flags, int fd,
                                   uint64_t offset);

    // 由于olap的文件都是用FileHandler封装的，因此稍微修?
    // ??下接口，省略掉的参数可以在handler中取到
    // 旧接口仍然保留，或许会用到？
    static StorageByteBuffer* mmap(FileHandler* handler, uint64_t offset, int prot, int flags);

    inline uint64_t capacity() const { return _capacity; }

    inline uint64_t position() const { return _position; }
    // 设置内部指针的位置
    // 如果新位置大于等于limit, 则返回OLAP_ERR_INPUT_PARAMETER_ERROR
    OLAPStatus set_position(uint64_t new_position) {
        if (new_position <= _limit) {
            _position = new_position;
            return OLAP_SUCCESS;
        } else {
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }
    }

    inline uint64_t limit() const { return _limit; }
    //设置新的limit
    //如果limit超过capacity, 返回OLAP_ERR_INPUT_PARAMETER_ERROR
    //如果position大于新的limit, 设置position等于limit
    OLAPStatus set_limit(uint64_t new_limit) {
        if (new_limit > _capacity) {
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }

        _limit = new_limit;

        if (_position > _limit) {
            _position = _limit;
        }

        return OLAP_SUCCESS;
    }

    inline uint64_t remaining() const { return _limit - _position; }

    // 将limit设置为当前position
    // 将position设置为0
    // 这个函数可以用于将ByteBuffer从写状态转为读状态, 即在进行一些写之后
    // 调用本函数,之后可以对ByteBuffer做读操作.
    void flip() {
        _limit = _position;
        _position = 0;
    }

    // 以下三个读取函数进行inline优化

    // 读取一个字节的数据, 完成后增加position
    inline OLAPStatus get(char* result) {
        if (OLAP_LIKELY(_position < _limit)) {
            *result = _array[_position++];
            return OLAP_SUCCESS;
        } else {
            return OLAP_ERR_OUT_OF_BOUND;
        }
    }

    // 读取指定位置的一个字节的数据
    inline OLAPStatus get(uint64_t index, char* result) {
        if (OLAP_LIKELY(index < _limit)) {
            *result = _array[index];
            return OLAP_SUCCESS;
        } else {
            return OLAP_ERR_OUT_OF_BOUND;
        }
    }

    // 读取length长度的一段数据到dst, 完成后增加position
    inline OLAPStatus get(char* dst, uint64_t dst_size, uint64_t length) {
        //没有足够的数据可读
        if (OLAP_UNLIKELY(length > remaining())) {
            return OLAP_ERR_OUT_OF_BOUND;
        }

        //dst不够大
        if (OLAP_UNLIKELY(length > dst_size)) {
            return OLAP_ERR_BUFFER_OVERFLOW;
        }

        memory_copy(dst, &_array[_position], length);
        _position += length;
        return OLAP_SUCCESS;
    }

    // 读取dst_size长的数据到dst
    inline OLAPStatus get(char* dst, uint64_t dst_size) { return get(dst, dst_size, dst_size); }

    // 写入一个字节, 完成后增加position
    // 如果写入前position >= limit, 则返回OLAP_ERR_BUFFER_OVERFLOW
    OLAPStatus put(char src);

    // 在index位置写入数据, 不会改变position
    // Returns:
    //   OLAP_ERR_BUFFER_OVERFLOW : index >= limit
    OLAPStatus put(uint64_t index, char src);

    // 从&src[offset]读取length字节数据, 并写入buffer, 完成后增加position
    // Returns:
    //   OLAP_ERR_BUFFER_OVERFLOW: remaining() < length
    //   OLAP_ERR_OUT_OF_BOUND: offset + length > src_size
    OLAPStatus put(const char* src, uint64_t src_size, uint64_t offset, uint64_t length);

    // 写入一组数据
    OLAPStatus put(const char* src, uint64_t src_size) { return put(src, src_size, 0, src_size); }

    // 返回ByteBuffer内部的char数组
    const char* array() const { return _array; }
    const char* array(size_t position) const {
        return position >= _limit ? NULL : &_array[position];
    }
    char* array() { return _array; }

private:
    // 自定义析构类,支持对new[]和mmap的内存进行析构
    // 默认使用delete进行释放
    class BufDeleter {
    public:
        BufDeleter();
        // 设置使用mmap方式
        void set_mmap(size_t mmap_length);
        void operator()(char* p);

    private:
        bool _is_mmap;       // 是否使用mmap
        size_t _mmap_length; // 如果使用mmap,记录mmap的长度
    };

private:
    // 不支持直接创建ByteBuffer, 而是通过create方法创建
    StorageByteBuffer();

private:
    boost::shared_ptr<char> _buf; // 托管的内存
    char* _array;
    uint64_t _capacity;
    uint64_t _limit;
    uint64_t _position;
    bool _is_mmap;
};

} // namespace doris
#endif // DORIS_BE_SRC_OLAP_COLUMN_FILE_BYTE_BUFFER_H
