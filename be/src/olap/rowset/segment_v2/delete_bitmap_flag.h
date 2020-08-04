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

#include "common/status.h"
#include "gen_cpp/segment_v2.pb.h"
#include "util/faststring.h"
#include "util/slice.h"
#include "util/rle_encoding.h" 
#include "util/debug_util.h"
#include "util/bitmap.h"
#include "olap/rowset/segment_v2/common.h"

namespace doris {

namespace segment_v2 {

class DeleteBitmapFlagDecoder;
class DeleteBitmapFlagIterator;

/// This class is a builder which can build delete bitmap flag. SegmentWriter can use it to generate
/// delete bitmap flag page and save it in segment.
class DeleteBitmapFlagBuilder {
public:
    /// Construction function of DeleteBitmapFlagBuilder
    DeleteBitmapFlagBuilder() : _num_items(0), _buf(512), _delete_bitmap(&_buf, 1) {
    }

    /// Add delete item to delete bitmap flag
    Status add_delete_item(bool value, const uint32_t& _row_count);

    /// How many bytes are required to serialize this bitmap
    uint64_t size() { return _buf.size(); }

    /// When the segment flush, use finalize function to flush flag data to slice to generate flag page 
    /// and fill the page footer record meta.
    Status finalize(std::vector<Slice>* body, segment_v2::PageFooterPB* footer);

private:
    /// the number of delete items in delete bitmap flag
    uint32_t _num_items;

    faststring _buf;
    /// roaring bitmap to record rowids of delete items 
    RleEncoder<bool> _delete_bitmap;
};

/// Used to decode bitmap ordinal to footer and encoded flag data.
/// Usage:
///      DeleteBitmapFlagDecoder decoder;
///      decoder.parse(body, footer);
class DeleteBitmapFlagDecoder {
public:
    DeleteBitmapFlagDecoder(bool parsed = false) : _parsed(parsed) {}

    /// client should assure that body is available when this class is used
    Status parse(const Slice& body, const segment_v2::DeleteFlagFooterPB& footer);

    /// The number of delete items in delete bitmap flag
    uint32_t num_items() {
        DCHECK(_parsed);
        return _footer.num_items();
    }

    /// get const delete bitmap to access delete bitmap record
    RleDecoder<bool>& get_delete_bitmap() { return _delete_bitmap; }

private:
    bool _parsed;

    // All following fields are only valid after parse has been executed successfully
    segment_v2::DeleteFlagFooterPB _footer;
    RleDecoder<bool> _delete_bitmap;
};

/// An Iterator to iterate one delete bitmap flag.
/// Client can use this class to access the bitmap.
class DeleteBitmapFlagIterator {
public:
    /// Construction function of DeleteBitmapFlagBuilder
    DeleteBitmapFlagIterator(DeleteBitmapFlagDecoder* decoder)
            : _decoder(decoder), _current_ordinal(0) {}

    /// seek to ordinal
    Status seek_to_ordinal(ordinal_t ord) {

        ordinal_t skips = ord - _current_ordinal;
        if (skips != 0 && _decoder->num_items() != 0) {
            _decoder->get_delete_bitmap().Skip(skips);
        }
        _current_ordinal = ord;
        return Status::OK();
    }

    /// get next_batch row is_delete
    Status next_batch(uint8_t* bitmap, size_t row_offset, size_t range_size) {

        if (_decoder->num_items() == 0) {
            BitmapChangeBits(bitmap, row_offset, range_size, false);
        } else {
            size_t offset = 0;
            while (offset < range_size) {
                bool is_delete = false;
                size_t this_run = _decoder->get_delete_bitmap().GetNextRun(&is_delete, range_size - offset);
                BitmapChangeBits(bitmap, row_offset + offset, this_run, is_delete);

                _current_ordinal += this_run;
                offset += this_run;
            }
        }
        return Status::OK();
    }

private:
    DeleteBitmapFlagDecoder* _decoder;
    // current value ordinal
    ordinal_t _current_ordinal;
};

}
}
