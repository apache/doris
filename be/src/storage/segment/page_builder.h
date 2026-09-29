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

#include <gen_cpp/segment_v2.pb.h>
#include <stdint.h>

#include <memory>
#include <vector>

#include "common/status.h"
#include "storage/segment/common.h"
#include "storage/storage_layout.h"
#include "util/slice.h"

namespace doris {
namespace segment_v2 {

// PageBuilder is used to build page
// Page is a data management unit, including:
// 1. Data Page: store encoded and compressed data
// 2. BloomFilter Page: store bloom filter of data
// 3. Ordinal Index Page: store ordinal index of data
// 4. Short Key Index Page: store short key index of data
// 5. Bitmap Index Page: store bitmap index of data
class PageBuilder {
public:
    PageBuilder() = default;
    PageBuilder(const PageBuilder&) = delete;
    PageBuilder& operator=(const PageBuilder&) = delete;

    virtual ~PageBuilder() = default;

    // Init the internal state of the page builder.
    virtual Status init() = 0;

    // Used by column writer to determine whether the current page is full.
    // Column writer depends on the result to decide whether to flush current page.
    virtual bool is_page_full() = 0;

    // Appends rows [row_pos, row_pos + n) of `column`. `*added` rows were taken,
    // fewer than n only when the page filled up.
    virtual Status add(const IColumn& column, size_t row_pos, size_t n, size_t* added) = 0;

    // Finish building the current page, return the encoded data.
    // This api should be followed by reset() before reusing the builder
    // It will return error status when memory allocated failed during finish
    virtual Status finish(OwnedSlice* owned_slice) = 0;

    // Get the dictionary page for dictionary encoding mode column.
    virtual Status get_dictionary_page(OwnedSlice* dictionary_page) {
        return Status::NotSupported("get_dictionary_page not implemented");
    }

    virtual Status get_dictionary_page_encoding(EncodingTypePB* encoding) const {
        return Status::NotSupported("get_dictionary_page_encoding not implemented");
    }

    // Reset the internal state of the page builder.
    //
    // Any data previously returned by finish may be invalidated by this call.
    virtual Status reset() = 0;

    // Return the number of entries that have been added to the page.
    virtual size_t count() const = 0;

    // Return the total bytes of pageBuilder that have been added to the page.
    virtual uint64_t size() const = 0;

    // Return the uncompressed data size in bytes (raw data added via add() method).
    // This is used to track the original data size before compression.
    virtual uint64_t get_raw_data_size() const = 0;
};

// A builder whose cells are fixed width: its FieldType's StorageValues.
template <FieldType Type>
class FixedWidthPageBuilder : public PageBuilder {
public:
    using StorageValue = typename StorageLayout<Type>::StorageValue;
    // Appends up to *count cells, already StorageValues; *count
    // becomes how many went in, fewer than offered only when the page is full.
    virtual Status add_cells(const StorageValue* cells, size_t* count) = 0;
};

// A builder whose cells are Slices.
class StringPageBuilder : public PageBuilder {
public:
    // Appends up to *count values; *count becomes how many went in, fewer
    // than offered only when the page is full.
    virtual Status add_slices(const Slice* values, size_t* count) = 0;
};

template <typename Derived, typename Base>
class PageBuilderHelper : public Base {
public:
    template <typename... Args>
    static Status create(PageBuilder** builder, Args&&... args) {
        std::unique_ptr<PageBuilder> builder_uniq_ptr(new Derived(std::forward<Args>(args)...));
        RETURN_IF_ERROR(builder_uniq_ptr->init());
        *builder = builder_uniq_ptr.release();
        return Status::OK();
    }
};

// The entries of a string page builder: `add_one` takes one value and the loop
// stops at the first full page, leaving in *count / *added how many values went
// in.
template <class AddOne>
Status add_each_slice(PageBuilder& builder, const Slice* values, size_t* count, AddOne add_one) {
    size_t i = 0;
    for (; !builder.is_page_full() && i < *count; ++i) {
        RETURN_IF_ERROR(add_one(values[i]));
    }
    *count = i;
    return Status::OK();
}

template <FieldType Type, class AddOne>
Status add_string_cells(PageBuilder& builder, const IColumn& column, size_t row_pos, size_t n,
                        PaddedPODArray<char>& tmp_buffer, size_t* added, AddOne add_one) {
    size_t i = 0;
    for (; !builder.is_page_full() && i < n; ++i) {
        const StringRef value = StorageLayout<Type>::storage_at(column, row_pos + i, tmp_buffer);
        RETURN_IF_ERROR(add_one(Slice(value.data, value.size)));
    }
    *added = i;
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
