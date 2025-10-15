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

#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace doris::pyudf {

/**
 * {@link PythonObjectBufferPool} manages a thread-safe pool of 64MB pre-allocated buffers for Python UDF/UDTF results.
 * It implements the singleton pattern with lazy initialization and supports buffer reuse and automatic cleanup using
 * condition variables and mutex synchronization.
 */
class PythonObjectBufferPool {
public:
    static PythonObjectBufferPool& instance() {
        static PythonObjectBufferPool pool;
        return pool;
    }

    std::pair<size_t, std::unique_ptr<std::string>> acquire_buffer(size_t min_size) {
        if (min_size > _buffer_size) {
            return std::make_pair(0, nullptr);
        }
        std::unique_lock<std::mutex> lock(_mutex);
        while (true) {
            for (auto& buffer : _buffers) {
                if (!buffer->in_use) {
                    size_t id = buffer->id;
                    buffer->in_use = true;
                    buffer->data.resize(min_size);
                    return std::make_pair(id,
                                          std::make_unique<std::string>(std::move(buffer->data)));
                }
            }
            _condition.wait(lock);
        }
    }

    void release_buffer(size_t id, std::unique_ptr<std::string> buffer) {
        if (!buffer) {
            return;
        }
        std::lock_guard<std::mutex> lock(_mutex);
        for (auto& b : _buffers) {
            if (b->id == id) {
                b->data.clear();
                b->in_use = false;
                break;
            }
        }
        _condition.notify_one();
    }

private:
    struct Buffer {
        std::string data;
        bool in_use;
        size_t id;

        Buffer(size_t capacity, bool in_use_, size_t id_) : in_use(in_use_), id(id_) {
            // The buffer will be pre-allocated.
            data.reserve(capacity);
        }
        Buffer(const std::string& str, bool in_use_, size_t id_)
                : data(str), in_use(in_use_), id(id_) {}
    };

    std::condition_variable _condition;
    size_t _buffer_size = 64 * 1024 * 1024;

    PythonObjectBufferPool() {
        // Allocate 5 * 64 MB for python udf when doris be is started.
        for (int i = 0; i < 5; ++i) {
            _buffers.push_back(std::make_unique<Buffer>(_buffer_size, false, next_id++));
        }
    }

    std::mutex _mutex;
    std::vector<std::unique_ptr<Buffer>> _buffers;
    size_t next_id = 1;
};

/**
 * {@link PythonObjectBufferGuard} is a RAII-style buffer manager for Python object serialization. It automatically
 * acquires/releases buffers from the pool during construction/destruction. Ensures memory safety and simplifies
 * resource management in UDF/UDTF operations.
 */
class PythonObjectBufferGuard {
public:
    PythonObjectBufferGuard(size_t length)
            : buffer_id(0), buffer(PythonObjectBufferPool::instance().acquire_buffer(length)) {
        if (buffer.second) {
            buffer_id = buffer.first;
        }
    }

    ~PythonObjectBufferGuard() {
        if (buffer.second) {
            PythonObjectBufferPool::instance().release_buffer(buffer_id, std::move(buffer.second));
        }
    }

    std::string* get() { return buffer.second.get(); }

private:
    size_t buffer_id;
    std::pair<size_t, std::unique_ptr<std::string>> buffer;
};

} // namespace doris::pyudf