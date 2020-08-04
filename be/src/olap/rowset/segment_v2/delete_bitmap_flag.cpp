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

#include "olap/rowset/segment_v2/delete_bitmap_flag.h"

#include <string>
#include "gutil/strings/substitute.h"

using strings::Substitute;

namespace doris {

namespace segment_v2 {

Status DeleteBitmapFlagBuilder::add_delete_item(bool value, const uint32_t& row_count) {
    _delete_bitmap.Put(value, row_count);
    if (value) {
        _num_items += row_count;
    }
    return Status::OK();
}

Status DeleteBitmapFlagBuilder::finalize(std::vector<Slice>* body,
                                          segment_v2::PageFooterPB* page_footer) {
    // get the size after serialize
    uint32_t size = 0;
    if (_num_items > 0) {
        _delete_bitmap.Flush();
        size = _buf.size();
    }
    // fill in bitmap flag page
    page_footer->set_type(segment_v2::DELETE_FLAG_PAGE);
    page_footer->set_uncompressed_size(size);

    segment_v2::DeleteFlagFooterPB* footer = page_footer->mutable_delete_flag_page_footer();
    footer->set_num_items(_num_items);
    footer->set_content_bytes(size);

    if (_num_items > 0) {
        body->emplace_back(_buf);
    }
    return Status::OK();
}

Status DeleteBitmapFlagDecoder::parse(const Slice& body, const segment_v2::DeleteFlagFooterPB& footer) {
    _footer = footer;
    // check if body size match footer's information
    if (body.size != (_footer.content_bytes())) {
        return Status::Corruption(Substitute("Flag size not match, need=$0, real=$1",
                                             _footer.content_bytes(), body.size));
    }

    if (footer.num_items() > 0) {
        // set flag buffer
        Slice flag_data(body.data, _footer.content_bytes());
        // load delete bitmap
        _delete_bitmap = RleDecoder<bool>((const uint8_t*)flag_data.data, flag_data.size, 1);
    }

    _parsed = true;
    return Status::OK();
}
}
}
