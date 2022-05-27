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

#include "olap/rowset/segment_v2/bitmap_index_reader.h"

#include "olap/types.h"

namespace doris {
namespace segment_v2 {

Status BitmapIndexReader::load(bool use_page_cache, bool kept_in_memory) {
    const IndexedColumnMetaPB& dict_meta = _bitmap_index_meta->dict_column();
    const IndexedColumnMetaPB& bitmap_meta = _bitmap_index_meta->bitmap_column();
    _has_null = _bitmap_index_meta->has_null();

    _dict_column_reader.reset(new IndexedColumnReader(_path_desc, dict_meta));
    _bitmap_column_reader.reset(new IndexedColumnReader(_path_desc, bitmap_meta));
    RETURN_IF_ERROR(_dict_column_reader->load(use_page_cache, kept_in_memory));
    RETURN_IF_ERROR(_bitmap_column_reader->load(use_page_cache, kept_in_memory));
    return Status::OK();
}

Status BitmapIndexReader::new_iterator(BitmapIndexIterator** iterator) {
    *iterator = new BitmapIndexIterator(this);
    return Status::OK();
}

Status BitmapIndexIterator::seek_dictionary(const void* value, bool* exact_match) {
    RETURN_IF_ERROR(_dict_column_iter.seek_at_or_after(value, exact_match));
    _current_rowid = _dict_column_iter.get_current_ordinal();
    return Status::OK();
}

Status BitmapIndexIterator::read_bitmap(rowid_t ordinal, roaring::Roaring* result) {
    DCHECK(0 <= ordinal && ordinal < _reader->bitmap_nums());

    size_t num_to_read = 1;
    std::unique_ptr<ColumnVectorBatch> cvb;
    RETURN_IF_ERROR(
            ColumnVectorBatch::create(num_to_read, false, _reader->type_info(), nullptr, &cvb));
    ColumnBlock block(cvb.get(), _pool.get());
    ColumnBlockView column_block_view(&block);

    RETURN_IF_ERROR(_bitmap_column_iter.seek_to_ordinal(ordinal));
    size_t num_read = num_to_read;
    RETURN_IF_ERROR(_bitmap_column_iter.next_batch(&num_read, &column_block_view));
    DCHECK(num_to_read == num_read);

    *result = roaring::Roaring::read(reinterpret_cast<const Slice*>(block.data())->data, false);
    _pool->clear();
    return Status::OK();
}

Status BitmapIndexIterator::read_union_bitmap(rowid_t from, rowid_t to, roaring::Roaring* result) {
    DCHECK(0 <= from && from <= to && to <= _reader->bitmap_nums());

    for (rowid_t pos = from; pos < to; pos++) {
        roaring::Roaring bitmap;
        RETURN_IF_ERROR(read_bitmap(pos, &bitmap));
        *result |= bitmap;
    }
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
