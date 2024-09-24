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
#include <roaring/roaring.hh>

namespace doris::segment_v2::inverted_index {
class InvertedIndexResultBitmap {
private:
    std::shared_ptr<roaring::Roaring> _data_bitmap = nullptr;
    std::shared_ptr<roaring::Roaring> _null_bitmap = nullptr;

public:
    // Default constructor
    InvertedIndexResultBitmap() = default;

    ~InvertedIndexResultBitmap() = default;

    // Constructor with arguments
    InvertedIndexResultBitmap(std::shared_ptr<roaring::Roaring> data_bitmap,
                              std::shared_ptr<roaring::Roaring> null_bitmap)
            : _data_bitmap(std::move(data_bitmap)), _null_bitmap(std::move(null_bitmap)) {}

    // Copy constructor
    InvertedIndexResultBitmap(const InvertedIndexResultBitmap& other)
            : _data_bitmap(std::make_shared<roaring::Roaring>(*other._data_bitmap)),
              _null_bitmap(std::make_shared<roaring::Roaring>(*other._null_bitmap)) {}

    // Move constructor
    InvertedIndexResultBitmap(InvertedIndexResultBitmap&& other) noexcept
            : _data_bitmap(std::move(other._data_bitmap)),
              _null_bitmap(std::move(other._null_bitmap)) {}

    // Copy assignment operator
    InvertedIndexResultBitmap& operator=(const InvertedIndexResultBitmap& other) {
        if (this != &other) { // Prevent self-assignment
            _data_bitmap = std::make_shared<roaring::Roaring>(*other._data_bitmap);
            _null_bitmap = std::make_shared<roaring::Roaring>(*other._null_bitmap);
        }
        return *this;
    }

    // Move assignment operator
    InvertedIndexResultBitmap& operator=(InvertedIndexResultBitmap&& other) noexcept {
        if (this != &other) { // Prevent self-assignment
            _data_bitmap = std::move(other._data_bitmap);
            _null_bitmap = std::move(other._null_bitmap);
        }
        return *this;
    }

    // Operator &=
    InvertedIndexResultBitmap& operator&=(const InvertedIndexResultBitmap& other) {
        if (_data_bitmap && _null_bitmap && other._data_bitmap && other._null_bitmap) {
            auto new_null_bitmap = (*_data_bitmap & *other._null_bitmap) |
                                   (*_null_bitmap & *other._data_bitmap) |
                                   (*_null_bitmap & *other._null_bitmap);
            *_data_bitmap &= *other._data_bitmap;
            *_null_bitmap = std::move(new_null_bitmap);
        }
        return *this;
    }

    // Operator |=
    InvertedIndexResultBitmap& operator|=(const InvertedIndexResultBitmap& other) {
        if (_data_bitmap && _null_bitmap && other._data_bitmap && other._null_bitmap) {
            auto new_null_bitmap = (*_null_bitmap | *other._null_bitmap) - *_data_bitmap;
            *_data_bitmap |= *other._data_bitmap;
            *_null_bitmap = std::move(new_null_bitmap);
        }
        return *this;
    }

    // NOT operation
    const InvertedIndexResultBitmap& op_not(const roaring::Roaring* universe) const {
        if (_data_bitmap && _null_bitmap) {
            *_data_bitmap = *universe - *_data_bitmap - *_null_bitmap;
            // The _null_bitmap remains unchanged.
        }
        return *this;
    }

    // Operator -=
    InvertedIndexResultBitmap& operator-=(const InvertedIndexResultBitmap& other) {
        if (_data_bitmap && _null_bitmap && other._data_bitmap && other._null_bitmap) {
            *_data_bitmap -= *other._data_bitmap;
            *_data_bitmap -= *other._null_bitmap;
            *_null_bitmap -= *other._null_bitmap;
        }
        return *this;
    }

    void mask_out_null() {
        if (_data_bitmap && _null_bitmap) {
            *_data_bitmap -= *_null_bitmap;
        }
    }

    const std::shared_ptr<roaring::Roaring>& get_data_bitmap() const { return _data_bitmap; }

    const std::shared_ptr<roaring::Roaring>& get_null_bitmap() const { return _null_bitmap; }

    // Check if both bitmaps are empty
    bool is_empty() const { return (_data_bitmap == nullptr && _null_bitmap == nullptr); }
};
} // namespace doris::segment_v2::inverted_index