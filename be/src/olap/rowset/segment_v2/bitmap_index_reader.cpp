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

#include <gen_cpp/segment_v2.pb.h>
#include <glog/logging.h>
#include <stddef.h>

#include <roaring/roaring.hh>

#include "olap/types.h"
#include "util/debug_points.h"
#include "vec/columns/column.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {
namespace segment_v2 {

Status BitmapIndexReader::load(bool use_page_cache, bool kept_in_memory) {
    // TODO yyq: implement a new once flag to avoid status construct.
    return _load_once.call([this, use_page_cache, kept_in_memory] {
        return _load(use_page_cache, kept_in_memory, std::move(_index_meta));
    });
}

Status BitmapIndexReader::_load(bool use_page_cache, bool kept_in_memory,
                                std::unique_ptr<BitmapIndexPB> index_meta) {
    const IndexedColumnMetaPB& dict_meta = index_meta->dict_column();
    const IndexedColumnMetaPB& bitmap_meta = index_meta->bitmap_column();
    _has_null = index_meta->has_null();

    _dict_column_reader.reset(new IndexedColumnReader(_file_reader, dict_meta));
    _bitmap_column_reader.reset(new IndexedColumnReader(_file_reader, bitmap_meta));
    RETURN_IF_ERROR(_dict_column_reader->load(use_page_cache, kept_in_memory));
    RETURN_IF_ERROR(_bitmap_column_reader->load(use_page_cache, kept_in_memory));
    return Status::OK();
}

Status BitmapIndexReader::new_iterator(BitmapIndexIterator** iterator) {
    DBUG_EXECUTE_IF("BitmapIndexReader::new_iterator.fail",
                    { return Status::InternalError("new_iterator for bitmap index failed"); });
    *iterator = new BitmapIndexIterator(this);
    return Status::OK();
}

Status BitmapIndexIterator::seek_dictionary(const void* value, bool* exact_match) {
    RETURN_IF_ERROR(_dict_column_iter.seek_at_or_after(value, exact_match));
    _current_rowid = _dict_column_iter.get_current_ordinal();
    return Status::OK();
}

Status BitmapIndexIterator::read_bitmap(rowid_t ordinal, roaring::Roaring* result) {
    DCHECK(ordinal < _reader->bitmap_nums());

    size_t num_to_read = 1;
    auto data_type = vectorized::DataTypeFactory::instance().create_data_type(
            _reader->type_info()->type(), 1, 0);
    auto column = data_type->create_column();

    RETURN_IF_ERROR(_bitmap_column_iter.seek_to_ordinal(ordinal));
    size_t num_read = num_to_read;
    RETURN_IF_ERROR(_bitmap_column_iter.next_batch(&num_read, column));
    DCHECK(num_to_read == num_read);

    *result = roaring::Roaring::read(column->get_data_at(0).data, false);
    return Status::OK();
}

Status BitmapIndexIterator::read_union_bitmap(rowid_t from, rowid_t to, roaring::Roaring* result) {
    DCHECK(from <= to && to <= _reader->bitmap_nums());

    for (rowid_t pos = from; pos < to; pos++) {
        roaring::Roaring bitmap;
        RETURN_IF_ERROR(read_bitmap(pos, &bitmap));
        *result |= bitmap;
    }
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
