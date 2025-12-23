/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "orc/MemoryPool.hh"
#include "orc/Int128.hh"

#include "Adaptor.hh"

#include <string.h>
#include <cstdlib>
#include <iostream>

namespace orc {

  MemoryPool::~MemoryPool() {
    // PASS
  }

  class MemoryPoolImpl : public MemoryPool {
   public:
    virtual ~MemoryPoolImpl() override;

    char* malloc(uint64_t size) override;
    void free(char* p) override;
  };

  char* MemoryPoolImpl::malloc(uint64_t size) {
    return static_cast<char*>(std::malloc(size));
  }

  void MemoryPoolImpl::free(char* p) {
    std::free(p);
  }

  MemoryPoolImpl::~MemoryPoolImpl() {
    // PASS
  }

  template <class T>
  DataBuffer<T>::DataBuffer(MemoryPool& pool, uint64_t newSize)
      : memoryPool(pool), buf(nullptr), currentSize(0), currentCapacity(0) {
    resize(newSize);
  }

  template <class T>
  DataBuffer<T>::DataBuffer(DataBuffer<T>&& buffer) noexcept
      : memoryPool(buffer.memoryPool),
        buf(buffer.buf),
        currentSize(buffer.currentSize),
        currentCapacity(buffer.currentCapacity) {
    buffer.buf = nullptr;
    buffer.currentSize = 0;
    buffer.currentCapacity = 0;
  }

  template <class T>
  DataBuffer<T>::~DataBuffer() {
    for (uint64_t i = currentSize; i > 0; --i) {
      (buf + i - 1)->~T();
    }
    if (buf) {
      memoryPool.free(reinterpret_cast<char*>(buf));
    }
  }

  template <class T>
  void DataBuffer<T>::resize(uint64_t newSize) {
    reserve(newSize);
    if (currentSize > newSize) {
      for (uint64_t i = currentSize; i > newSize; --i) {
        (buf + i - 1)->~T();
      }
    } else if (newSize > currentSize) {
      for (uint64_t i = currentSize; i < newSize; ++i) {
        new (buf + i) T();
      }
    }
    currentSize = newSize;
  }

  template <class T>
  void DataBuffer<T>::reserve(uint64_t newCapacity) {
    if (newCapacity > currentCapacity || !buf) {
      if (buf) {
        T* buf_old = buf;
        buf = reinterpret_cast<T*>(memoryPool.malloc(sizeof(T) * newCapacity));
        memcpy(buf, buf_old, sizeof(T) * currentSize);
        memoryPool.free(reinterpret_cast<char*>(buf_old));
      } else {
        buf = reinterpret_cast<T*>(memoryPool.malloc(sizeof(T) * newCapacity));
      }
      currentCapacity = newCapacity;
    }
  }

  // Specializations for char

  template <>
  DataBuffer<char>::~DataBuffer() {
    if (buf) {
      memoryPool.free(reinterpret_cast<char*>(buf));
    }
  }

  template <>
  void DataBuffer<char>::resize(uint64_t newSize) {
    reserve(newSize);
    if (newSize > currentSize) {
      memset(buf + currentSize, 0, newSize - currentSize);
    }
    currentSize = newSize;
  }

  // Specializations for char*

  template <>
  DataBuffer<char*>::~DataBuffer() {
    if (buf) {
      memoryPool.free(reinterpret_cast<char*>(buf));
    }
  }

  template <>
  void DataBuffer<char*>::resize(uint64_t newSize) {
    reserve(newSize);
    if (newSize > currentSize) {
      memset(buf + currentSize, 0, (newSize - currentSize) * sizeof(char*));
    }
    currentSize = newSize;
  }

  // Specializations for double

  template <>
  DataBuffer<double>::~DataBuffer() {
    if (buf) {
      memoryPool.free(reinterpret_cast<char*>(buf));
    }
  }

  template <>
  void DataBuffer<double>::resize(uint64_t newSize) {
    reserve(newSize);
    if (newSize > currentSize) {
      memset(buf + currentSize, 0, (newSize - currentSize) * sizeof(double));
    }
    currentSize = newSize;
  }

  // Specializations for float

  template <>
  DataBuffer<float>::~DataBuffer() {
    if (buf) {
      memoryPool.free(reinterpret_cast<char*>(buf));
    }
  }

  template <>
  void DataBuffer<float>::resize(uint64_t newSize) {
    reserve(newSize);
    if (newSize > currentSize) {
      memset(buf + currentSize, 0, (newSize - currentSize) * sizeof(float));
    }
    currentSize = newSize;
  }

  // Specializations for int64_t

  template <>
  DataBuffer<int64_t>::~DataBuffer() {
    if (buf) {
      memoryPool.free(reinterpret_cast<char*>(buf));
    }
  }

  template <>
  void DataBuffer<int64_t>::resize(uint64_t newSize) {
    reserve(newSize);
    if (newSize > currentSize) {
      memset(buf + currentSize, 0, (newSize - currentSize) * sizeof(int64_t));
    }
    currentSize = newSize;
  }

  // Specializations for int32_t

  template <>
  DataBuffer<int32_t>::~DataBuffer() {
    if (buf) {
      memoryPool.free(reinterpret_cast<char*>(buf));
    }
  }

  template <>
  void DataBuffer<int32_t>::resize(uint64_t newSize) {
    reserve(newSize);
    if (newSize > currentSize) {
      memset(buf + currentSize, 0, (newSize - currentSize) * sizeof(int32_t));
    }
    currentSize = newSize;
  }

  // Specializations for int16_t

  template <>
  DataBuffer<int16_t>::~DataBuffer() {
    if (buf) {
      memoryPool.free(reinterpret_cast<char*>(buf));
    }
  }

  template <>
  void DataBuffer<int16_t>::resize(uint64_t newSize) {
    reserve(newSize);
    if (newSize > currentSize) {
      memset(buf + currentSize, 0, (newSize - currentSize) * sizeof(int16_t));
    }
    currentSize = newSize;
  }

  // Specializations for int8_t

  template <>
  DataBuffer<int8_t>::~DataBuffer() {
    if (buf) {
      memoryPool.free(reinterpret_cast<char*>(buf));
    }
  }

  template <>
  void DataBuffer<int8_t>::resize(uint64_t newSize) {
    reserve(newSize);
    if (newSize > currentSize) {
      memset(buf + currentSize, 0, (newSize - currentSize) * sizeof(int8_t));
    }
    currentSize = newSize;
  }

  // Specializations for uint64_t

  template <>
  DataBuffer<uint64_t>::~DataBuffer() {
    if (buf) {
      memoryPool.free(reinterpret_cast<char*>(buf));
    }
  }

  template <>
  void DataBuffer<uint64_t>::resize(uint64_t newSize) {
    reserve(newSize);
    if (newSize > currentSize) {
      memset(buf + currentSize, 0, (newSize - currentSize) * sizeof(uint64_t));
    }
    currentSize = newSize;
  }

  // Specializations for unsigned char

  template <>
  DataBuffer<unsigned char>::~DataBuffer() {
    if (buf) {
      memoryPool.free(reinterpret_cast<char*>(buf));
    }
  }

  template <>
  void DataBuffer<unsigned char>::resize(uint64_t newSize) {
    reserve(newSize);
    if (newSize > currentSize) {
      memset(buf + currentSize, 0, newSize - currentSize);
    }
    currentSize = newSize;
  }

#ifdef __clang__
#pragma clang diagnostic ignored "-Wweak-template-vtables"
#endif

  template class DataBuffer<char>;
  template class DataBuffer<char*>;
  template class DataBuffer<double>;
  template class DataBuffer<float>;
  template class DataBuffer<Int128>;
  template class DataBuffer<int64_t>;
  template class DataBuffer<int32_t>;
  template class DataBuffer<int16_t>;
  template class DataBuffer<int8_t>;
  template class DataBuffer<uint64_t>;
  template class DataBuffer<unsigned char>;

#ifdef __clang__
#pragma clang diagnostic ignored "-Wexit-time-destructors"
#endif

  MemoryPool* getDefaultPool() {
    static MemoryPoolImpl internal;
    return &internal;
  }
}  // namespace orc
