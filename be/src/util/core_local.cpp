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

#include "util/core_local.h"

#include <cstdlib>
#include <vector>

#include "common/logging.h"
#include "util/spinlock.h"

namespace doris {

constexpr int BLOCK_SIZE = 4096;
struct alignas(CACHE_LINE_SIZE) CoreDataBlock {
    void* at(size_t offset) { return data + offset; }
    char data[BLOCK_SIZE];

    static void* operator new(size_t nbytes) {
        void* p = nullptr;
        if (posix_memalign(&p, alignof(CoreDataBlock), nbytes) == 0) {
            return p;
        }
        throw std::bad_alloc();
    }

    static void operator delete(void* p) { free(p); }
};

template <size_t ELEMENT_BYTES>
class CoreDataAllocatorImpl : public CoreDataAllocator {
public:
    virtual ~CoreDataAllocatorImpl();
    void* get_or_create(size_t id) override {
        size_t block_id = id / ELEMENTS_PER_BLOCK;
        {
            std::lock_guard<SpinLock> l(_lock);
            if (block_id >= _blocks.size()) {
                _blocks.resize(block_id + 1);
            }
        }
        CoreDataBlock* block = _blocks[block_id];
        if (block == nullptr) {
            block = new CoreDataBlock();
            _blocks[block_id] = block;
        }
        size_t offset = (id % ELEMENTS_PER_BLOCK) * ELEMENT_BYTES;
        return block->at(offset);
    }

private:
    static constexpr int ELEMENTS_PER_BLOCK = BLOCK_SIZE / ELEMENT_BYTES;
    SpinLock _lock; // lock to protect the modification of _blocks
    std::vector<CoreDataBlock*> _blocks;
};

template <size_t ELEMENT_BYTES>
CoreDataAllocatorImpl<ELEMENT_BYTES>::~CoreDataAllocatorImpl() {
    for (auto block : _blocks) {
        delete block;
    }
}

CoreDataAllocatorFactory* CoreDataAllocatorFactory::instance() {
    static CoreDataAllocatorFactory _s_instance;
    return &_s_instance;
}

CoreDataAllocator* CoreDataAllocatorFactory::get_allocator(size_t cpu_idx, size_t data_bytes) {
    std::lock_guard<std::mutex> l(_lock);
    auto pair = std::make_pair(cpu_idx, data_bytes);
    auto it = _allocators.find(pair);
    if (it != std::end(_allocators)) {
        return it->second;
    }
    CoreDataAllocator* allocator = nullptr;
    switch (data_bytes) {
    case 1:
        allocator = new CoreDataAllocatorImpl<1>();
        break;
    case 2:
        allocator = new CoreDataAllocatorImpl<2>();
        break;
    case 3:
    case 4:
        allocator = new CoreDataAllocatorImpl<4>();
        break;
    case 5:
    case 6:
    case 7:
    case 8:
        allocator = new CoreDataAllocatorImpl<8>();
        break;
    default:
        DCHECK(false) << "don't support core local value for this size, size=" << data_bytes;
    }
    _allocators.emplace(pair, allocator);
    return allocator;
}

CoreDataAllocatorFactory::~CoreDataAllocatorFactory() {
    for (auto& it : _allocators) {
        delete it.second;
    }
}

} // namespace doris
