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

#include <cstdint>
#include <vector>

#include "common/status.h"
#include "gen_cpp/column_data_file.pb.h"
#include "gen_cpp/olap_file.pb.h"
#include "olap/field_info.h"
#include "olap/file_helper.h"
#include "olap/row_cursor.h"
#include "olap/olap_index.h"
#include "util/faststring.h"
#include "util/slice.h"

namespace doris {

// Used to encode a segment short key indices to binary format. This builder
// is now compatible with old version's format. For V1 segment, indices are
// saved in a single file. And _header contains information about indeces,
// which is stored at the begin of the index file.
// NOTE: actuallly some information saved in header is not a good choice,
// for example protobuf message, which we can't known its length until all
// keys has been written. So we can reserve a certain space for it. which
// make us must write index file after all indices has been added.
// index file
// Usage:
//      ShortKeyIndexBuilder builder(schema, segment_id, num_rows, delete_flag);
//      builder.init();
//      builder.add_item(key1, block1);
//      ...
//      builder.add_item(keyN, blockN);
//      builder.finalize(segment_size, num_rows, &slices);
class ShortKeyIndexBuilder {
public:
    ShortKeyIndexBuilder(const std::vector<FieldInfo>& key_fields,
                         uint32_t segment_id,
                         uint32_t num_rows_per_block,
                         bool delete_flag)
        : _key_fields(key_fields),
        _segment_id(segment_id),
        _num_rows_per_block(num_rows_per_block),
        _delete_flag(delete_flag) { }

    Status init();
    // Actually, block_id is useless now, may be removed in some day
    Status add_item(const RowCursor& key, uint32_t block_id);
    Status finalize(uint32_t segment_size, uint32_t num_rows, std::vector<Slice>* slices);

private:
    std::vector<FieldInfo> _key_fields;
    uint32_t _segment_id = 0;
    uint32_t _num_rows_per_block;
    bool _delete_flag;

    // short key length
    size_t _key_length;

    // used to save serialized key temporarily
    faststring _tmp_key_buf;

    faststring _header_buf;
    faststring _index_buf;

    // NOTE: most of these two fields are useless.
    // put here just for compatible. I will remove this one day
    // only used to compatible with old version
    IndexFileHeaderV1 _header;
};

// Used to decode short key to header and encoded index data.
// Actually, this class should decode index data to memory format.
// But for compatible with old code, we reuse MemIndex class.
// Usage:
//      MemIndex index;
//      ShortKeyIndexDecoder decoder(slice)
//      decoder.parse();
//      index.load_segment(decoder.header(), decoder.index_data());
class ShortKeyIndexDecoder {
public:
    ShortKeyIndexDecoder(const Slice& data) : _data(data) { }
    Status parse();
    const IndexFileHeaderV1& header() const {
        return _header;
    }
    const Slice& index_data() const { return _index_data; }
private:
    Slice _data;

    // valid after parse() is called, Header has been removed.
    // Only contains index data 
    Slice _index_data;

    // only used to compatible with old version
    IndexFileHeaderV1 _header;
};

class ShortKeyIndexIterator {
public:
    ShortKeyIndexIterator(const MemIndex* index) : _index(index), _segment(0), _block(0) {
        _helper.init(_index->short_key_fields());
    }

    // Position at the first key in the index that at or past target
    inline void seek_to(const RowCursor& key) {
        auto offset = _index->find(key, &_helper, false);
        if (offset.offset > 0) {
            offset.offset -= 1;
            offset = _index->next(offset);
        }
        _segment = offset.segment;
        _block = offset.offset;
    }

    // Position at the first key in the index that post target
    inline void seek_after(const RowCursor& key) {
        auto offset = _index->find(key, &_helper, true);
        if (offset.offset > 0) {
            offset.offset -= 1;
            offset = _index->next(offset);
        }
        _segment = offset.segment;
        _block = offset.offset;
    }

    // Position at the first key in the index that before target
    inline void seek_before(const RowCursor& key) {
        seek_to(key);
        if (valid()) {
            prev();
        }
    }

    inline bool valid() const {
        return _segment < _index->segment_count();
    }

    // Move iterator to prev index item.
    // When iterator at the first item, it will keep it position
    // and still be at the first item.
    inline void prev() {
        DCHECK(valid());
        OLAPIndexOffset offset(_segment, _block);
        auto new_offset = _index->prev(offset);
        _segment = new_offset.segment;
        _block = new_offset.offset;
    }

    // Move iterator to next index item.
    // When iterator at the last item, it will turn to a invalid
    // position with valid() return false.
    inline void next() {
        DCHECK(valid());
        OLAPIndexOffset offset(_segment, _block);
        auto new_offset = _index->next(offset);
        _segment = new_offset.segment;
        _block = new_offset.offset;
    }

    uint32_t segment() const { return _segment; }
    uint32_t block() const { return _block; }

    // used to debug, print current key, segment, block
    std::string to_string() {
        Slice slice;
        OLAPIndexOffset offset(_segment, _block);
        _index->get_entry(offset, &slice);

        _helper.attach(slice.data);
        std::stringstream ss;
        ss << "key=" << _helper.to_string() << ", segment=" << _segment << ", block=" << _block;
        return ss.str();
    }
private:
    const MemIndex* _index;
    RowCursor _helper;
    uint32_t _segment;
    uint32_t _block;
};


}
