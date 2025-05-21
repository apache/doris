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
#include <stdexcept>
#include <vector>

#include "CLucene/_ApiHeader.h"

namespace doris::segment_v2 {

template <typename T>
class IKMemoryPool {
public:
    // IKMemoryPool: A layered memory pool for QuickSortSet::Cell objects, dynamically expanding by adding new layers.
    IKMemoryPool(size_t layerSize) : layerSize_(layerSize), allocatedCount_(0) {
        if (layerSize_ == 0) {
            throw std::invalid_argument("Layer size must be greater than 0");
        }
        addLayer();
    }

    T* allocate() {
        if (freeList_ == nullptr) {
            addLayer();
        }

        FreeNode* node = freeList_;
        freeList_ = freeList_->next;
        ++allocatedCount_;
        return reinterpret_cast<T*>(node);
    }

    void deallocate(T* ptr) {
        if (!ptr) {
            return;
        }

        FreeNode* node = reinterpret_cast<FreeNode*>(ptr);
        node->next = freeList_;
        freeList_ = node;
        --allocatedCount_;
    }

    void mergeFreeList(T* externalHead, T* externalTail, size_t count) {
        if (!externalHead || !externalTail) {
            return;
        } else if (count == 1) {
            deallocate(externalHead);
            return;
        }

        FreeNode* tailNode = reinterpret_cast<FreeNode*>(externalTail);
        tailNode->next = freeList_;
        freeList_ = reinterpret_cast<FreeNode*>(externalHead);
        allocatedCount_ -= count;
    }

    size_t available() const { return (layers_.size() * layerSize_) - allocatedCount_; }

    size_t poolSize() const { return layers_.size() * layerSize_; }

private:
    struct FreeNode {
        FreeNode* next = nullptr;
    };

    size_t layerSize_;
    size_t allocatedCount_;
    std::vector<std::unique_ptr<char[]>> layers_;
    FreeNode* freeList_ = nullptr;
    void addLayer() {
        auto newLayer = std::make_unique<char[]>(sizeof(T) * layerSize_);
        layers_.push_back(std::move(newLayer));

        char* layerStart = layers_.back().get();
        for (size_t i = 0; i < layerSize_; ++i) {
            char* block = layerStart + (i * sizeof(T));
            FreeNode* node = reinterpret_cast<FreeNode*>(block);
            node->next = freeList_;
            freeList_ = node;
        }
    }
};
} // namespace doris::segment_v2
