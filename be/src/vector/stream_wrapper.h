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

#pragma once

#include <CLucene.h>
#include <CLucene/store/IndexInput.h>
#include <CLucene/store/IndexOutput.h>

#include <mutex>

class IReaderWrapper {
public:
    virtual ~IReaderWrapper() = default;
    virtual void seek(uint64_t pos) = 0;
    virtual void read(char* s, uint64_t n, uint64_t offset) = 0;
    virtual void read(char* s, uint64_t n) = 0;
};

class ShareStringStreamReaderWrapper : public IReaderWrapper {
private:
    std::stringstream* ss;           // 共享的底层stringstream
    std::mutex* mtx;                 // 保护共享资源的互斥锁
    uint64_t _offset;                // 当前读取器的偏移量
    std::streamsize last_read_count; // 最后一次读取的字节数

public:
    // 构造函数，接收共享的stringstream和互斥锁
    ShareStringStreamReaderWrapper(std::stringstream& stream, std::mutex& mutex)
            : ss(&stream), mtx(&mutex), _offset(0), last_read_count(0) {}

    // 与std::stringstream兼容的read方法
    void read(char* s, uint64_t n) {
        std::lock_guard<std::mutex> lock(*mtx);

        // 移动到当前偏移位置
        ss->seekg(_offset, ss->beg);

        // 读取指定数量的字节
        ss->read(s, n);

        // 获取实际读取的字节数
        last_read_count = ss->gcount();

        // 更新偏移量
        _offset += static_cast<size_t>(last_read_count);
    }

    void read(char* s, uint64_t n, uint64_t offset) {
        std::lock_guard<std::mutex> lock(*mtx);
        // 移动到当前偏移位置
        ss->seekg(offset, ss->beg);
        // 读取指定数量的字节
        ss->read(s, n);
    }

    // 与std::stringstream兼容的seekp方法，设置偏移量到指定位置
    void seek(uint64_t pos) { _offset = pos; }

    ~ShareStringStreamReaderWrapper() {}
};

//简单把std::stringstream封装下，为了diskann::load_bin有个统一的接口
class SampleStringStreamReaderWrapper : public IReaderWrapper {
private:
    std::stringstream* ss;
    size_t _offset;
    std::streamsize last_read_count;

public:
    // 构造函数，接收共享的stringstream和互斥锁
    SampleStringStreamReaderWrapper(std::stringstream& stream)
            : ss(&stream), _offset(0), last_read_count(0) {}

    // 与std::stringstream兼容的read方法
    void read(char* s, uint64_t n) { ss->read(s, n); }

    void read(char* s, uint64_t n, uint64_t offset) {
        // 移动到当前偏移位置
        ss->seekg(offset, ss->beg);
        // 读取指定数量的字节
        ss->read(s, n);
    }

    void seek(uint64_t pos) { ss->seekg(pos, ss->beg); }
    ~SampleStringStreamReaderWrapper() {}
};

class IndexInputReaderWrapper : public IReaderWrapper {
private:
    lucene::store::IndexInput* _input;
    std::mutex mtx;

public:
    IndexInputReaderWrapper(lucene::store::IndexInput* input) { _input = input; }
    void seek(uint64_t offset) { _input->seek(offset); }
    //Note that the offset here and the offset in the inputindex do not have the same meaning
    void read(char* s, uint64_t n, uint64_t offset) {
        std::lock_guard<std::mutex> lock(mtx);
        _input->seek(offset);
        _input->readBytes(reinterpret_cast<uint8_t*>(s), static_cast<int32_t>(n));
    }

    void read(char* s, uint64_t n) {
        _input->readBytes(reinterpret_cast<uint8_t*>(s), static_cast<int32_t>(n));
    }
    ~IndexInputReaderWrapper() {
        if (_input != nullptr) {
            _input->close();
            delete _input;
            _input = nullptr;
        }
    }

    //used for debug
    std::stringstream readAll() {
        std::stringstream buffer;
        uint64_t len = _input->length();
        _input->seek(0); // 确保从头开始读取

        std::vector<char> data(len); // 创建缓冲区
        _input->readBytes(reinterpret_cast<uint8_t*>(data.data()), static_cast<int32_t>(len));

        buffer.write(data.data(), len);
        return buffer;
    }
};

using IReaderWrapperSPtr = std::shared_ptr<IReaderWrapper>;
using ShareStringStreamReaderWrapperSPtr = std::shared_ptr<ShareStringStreamReaderWrapper>;
using IndexInputReaderWrapperSPtr = std::shared_ptr<IndexInputReaderWrapper>;
using SampleStringStreamReaderWrapperSPtr = std::shared_ptr<SampleStringStreamReaderWrapper>;
