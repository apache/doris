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

#include <cstdint>
#include <iterator>
#include <string>
#include <vector>
#include <vector>
#include <roaring/roaring.hh>

#include "common/status.h"
#include "gen_cpp/segment_v2.pb.h"
#include "util/faststring.h"
#include "util/slice.h"

#include "util/debug_util.h"

namespace doris {

class DeleteBitmapIndexIterator;
class DeleteBitmapIndexDecoder;

/// This class is a builder which can build delete bitmap index. SegmentWriter can use it to generate
/// delete bitmap index page and save it in segment.
class DeleteBitmapIndexBuilder {
public:
    /// Construction function of DeleteBitmapIndexBuilder
    DeleteBitmapIndexBuilder() : _num_items(0) {
    }

    /// Add delete item to delete bitmap index
    Status add_delete_item(const uint32_t& _row_count);

    /// How many bytes are required to serialize this bitmap
    uint64_t size() {
        return _delete_bitmap.getSizeInBytes(false);
    }

    /// When the segment flush, use finalize function to flush index data to slice to generate index page 
    /// and fill the page footer record meta.
    Status finalize(std::vector<Slice>* body, segment_v2::PageFooterPB* footer);

private:
    /// the number of delete items in delete bitmap index
    uint32_t _num_items;

    /// roaring bitmap to record rowids of delete items 
    Roaring _delete_bitmap;

    faststring _buf;
};

/// An Iterator to iterate one delete bitmap index.
/// Client can use this class to access the bitmap.
class DeleteBitmapIndexIterator {
public:
    /// Construction function of DeleteBitmapIndexBuilder
    DeleteBitmapIndexIterator(const DeleteBitmapIndexDecoder* decoder)
            : _decoder(decoder) {}

    /// get const delete bitmap to access delete bitmap record
    const Roaring& delete_bitmap() const;

private:
    const DeleteBitmapIndexDecoder* _decoder;
};

/// Used to decode bitmap ordinal to footer and encoded index data.
/// Usage:
///      DeleteBitmapIndexDecoder decoder;
///      decoder.parse(body, footer);
class DeleteBitmapIndexDecoder {
public:
    DeleteBitmapIndexDecoder(bool parsed = false) : _parsed(parsed), _delete_bitmap() {}

    /// client should assure that body is available when this class is used
    Status parse(const Slice& body, const segment_v2::DeleteIndexFooterPB& footer);

    /// The number of delete items in delete bitmap index
    uint32_t num_items() const {
        DCHECK(_parsed);
        return _footer.num_items();
    }

    /// Get the iterator of DeleteBitmapIndex
    DeleteBitmapIndexIterator get_iterator() const { 
        DCHECK(_parsed);
        return {this};
    }

    /// get const delete bitmap to access delete bitmap record
    const Roaring& delete_bitmap() const { return _delete_bitmap; }

private:
    bool _parsed;

    // All following fields are only valid after parse has been executed successfully
    segment_v2::DeleteIndexFooterPB _footer;
    Roaring _delete_bitmap;
};

}
