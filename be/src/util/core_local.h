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

#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "common/compiler_util.h"
#include "gutil/macros.h"

namespace doris {

class CoreDataAllocator {
public:
    virtual ~CoreDataAllocator() {}
    virtual void* get_or_create(size_t id) = 0;
};

class CoreDataAllocatorFactory {
public:
    CoreDataAllocatorFactory() {}
    ~CoreDataAllocatorFactory();
    CoreDataAllocator* get_allocator(size_t cpu_id, size_t data_bytes);
    static CoreDataAllocatorFactory* instance();

private:
    DISALLOW_COPY_AND_ASSIGN(CoreDataAllocatorFactory);

private:
    std::mutex _lock;
    std::map<std::pair<size_t, size_t>, CoreDataAllocator*> _allocators;
};

template <typename T>
class CoreLocalValueController {
public:
    CoreLocalValueController() {
        int num_cpus = static_cast<int>(std::thread::hardware_concurrency());
        _size = 8;
        while (_size < num_cpus) {
            _size <<= 1;
        }
        _allocators.resize(_size, nullptr);
        for (int i = 0; i < _size; ++i) {
            _allocators[i] = CoreDataAllocatorFactory::instance()->get_allocator(i, sizeof(T));
        }
    }

    ~CoreLocalValueController() {}

    int get_id() {
        std::lock_guard<std::mutex> l(_lock);
        int id = 0;
        if (_free_ids.empty()) {
            id = _next_id++;
        } else {
            id = _free_ids.back();
            _free_ids.pop_back();
        }
        return id;
    }
    void reclaim_id(int id) {
        std::lock_guard<std::mutex> l(_lock);
        _free_ids.push_back(id);
    }
    size_t size() const { return _size; }
    CoreDataAllocator* allocator(int i) const { return _allocators[i]; }

    static CoreLocalValueController<T>* instance() {
        static CoreLocalValueController<T> _s_instance;
        return &_s_instance;
    }

private:
    DISALLOW_COPY_AND_ASSIGN(CoreLocalValueController);

private:
    std::mutex _lock;
    int _next_id = 0;
    std::deque<int> _free_ids;
    std::vector<CoreDataAllocator*> _allocators;
    size_t _size;
};

template <typename T>
class CoreLocalValue {
public:
    CoreLocalValue(const T init_value = T()) {
        CoreLocalValueController<T>* controller = CoreLocalValueController<T>::instance();
        _id = controller->get_id();
        _size = controller->size();
        _values.resize(_size, nullptr);
        for (int i = 0; i < _size; ++i) {
            void* ptr = controller->allocator(i)->get_or_create(_id);
            _values[i] = new (ptr) T(init_value);
        }
    }

    ~CoreLocalValue() {
        for (int i = 0; i < _size; ++i) {
            _values[i]->~T();
        }
        CoreLocalValueController<T>::instance()->reclaim_id(_id);
    }

    size_t size() const { return _size; }
    T* access() const {
#ifdef __APPLE__
        size_t cpu_id = 0;
#else
        size_t cpu_id = sched_getcpu();
#endif
        if (cpu_id >= _size) {
            cpu_id &= _size - 1;
        }
        return access_at_core(cpu_id);
    }
    T* access_at_core(size_t core_idx) const { return _values[core_idx]; }

    inline void reset() {
        for (int i = 0; i < _size; ++i) {
            _values[i]->~T();
        }
        _values.clear();
        _values.resize(_size, nullptr);
        CoreLocalValueController<T>* controller = CoreLocalValueController<T>::instance();
        for (int i = 0; i < _size; ++i) {
            void* ptr = controller->allocator(i)->get_or_create(_id);
            _values[i] = new (ptr) T();
        }
    }

private:
    int _id = -1;
    size_t _size = 0;
    std::vector<T*> _values;
};

} // namespace doris
