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

#include "olap/delete_bitmap_index.h"

#include <string>
#include "gutil/strings/substitute.h"

using strings::Substitute;

namespace doris {

Status DeleteBitmapIndexBuilder::add_delete_item(const uint32_t& _row_count) {
    _delete_bitmap.add(_row_count);
    _num_items++;
    return Status::OK();
}

Status DeleteBitmapIndexBuilder::finalize(std::vector<Slice>* body,
                                          segment_v2::PageFooterPB* page_footer) {
    // get the size after serialize
    _delete_bitmap.runOptimize();
    uint32_t size = _delete_bitmap.getSizeInBytes(false);
    // fill in bitmap index page
    page_footer->set_type(segment_v2::DELETE_INDEX_PAGE);
    page_footer->set_uncompressed_size(size);

    segment_v2::DeleteIndexFooterPB* footer = page_footer->mutable_delete_index_page_footer();
    footer->set_num_items(_num_items);
    footer->set_content_bytes(size);

    // write bitmap to slice as return
    _buf.resize(size);
    _delete_bitmap.write(reinterpret_cast<char*>(_buf.data()), false);
    body->emplace_back(_buf);
    return Status::OK();
}

Status DeleteBitmapIndexDecoder::parse(const Slice& body, const segment_v2::DeleteIndexFooterPB& footer) {
    _footer = footer;
    // check if body size match footer's information
    if (body.size != (_footer.content_bytes())) {
        return Status::Corruption(Substitute("Index size not match, need=$0, real=$1",
                                             _footer.content_bytes(), body.size));
    }
    // set index buffer
    Slice index_data(body.data, _footer.content_bytes());
    // load delete bitmap
    _delete_bitmap = Roaring::read(index_data.data, false);

    _parsed = true;
    return Status::OK();
}

const Roaring& DeleteBitmapIndexIterator:: delete_bitmap() const{
    return _decoder->delete_bitmap();
}
}
