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

#include "olap/rowset/segment_v2/indexed_column_writer.h"

#include <string>

#include "env/env.h"
#include "olap/rowset/segment_v2/encoding_info.h"
#include "olap/rowset/segment_v2/index_page.h"
#include "olap/rowset/segment_v2/options.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_compression.h"
#include "olap/rowset/segment_v2/page_pointer.h"
#include "olap/key_coder.h"
#include "olap/types.h"
#include "util/block_compression.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace doris {
namespace segment_v2 {

IndexedColumnWriter::IndexedColumnWriter(const IndexedColumnWriterOptions& options,
                                         const TypeInfo* typeinfo,
                                         WritableFile* output_file)
        : _options(options),
          _typeinfo(typeinfo),
          _file(output_file),
          _mem_tracker(-1),
          _mem_pool(&_mem_tracker),
          _num_values(0),
          _num_data_pages(0),
          _validx_key_coder(nullptr),
          _compress_codec(nullptr) {
    _first_value.resize(_typeinfo->size());
}

IndexedColumnWriter::~IndexedColumnWriter() = default;

Status IndexedColumnWriter::init() {
    const EncodingInfo* encoding_info;
    RETURN_IF_ERROR(EncodingInfo::get(_typeinfo, _options.encoding, &encoding_info));

    PageBuilder* data_page_builder;
    RETURN_IF_ERROR(encoding_info->create_page_builder(PageBuilderOptions(), &data_page_builder));
    _data_page_builder.reset(data_page_builder);

    if (_options.write_ordinal_index) {
        _ordinal_index_builder.reset(new IndexPageBuilder(_options.index_page_size, true));
    }
    if (_options.write_value_index) {
        _value_index_builder.reset(new IndexPageBuilder(_options.index_page_size, true));
        _validx_key_coder = get_key_coder(_typeinfo->type());
    }

    if (_options.compression != NO_COMPRESSION) {
        RETURN_IF_ERROR(get_block_compression_codec(_options.compression, &_compress_codec));
    }
    return Status::OK();
}

Status IndexedColumnWriter::add(const void* value) {
    if (_options.write_value_index && _data_page_builder->count() == 0) {
        // remember page's first value because it's used to build value index
        _typeinfo->deep_copy(_first_value.data(), value, &_mem_pool);
    }
    size_t num_to_write = 1;
    RETURN_IF_ERROR(_data_page_builder->add(reinterpret_cast<const uint8_t*>(value), &num_to_write));
    _num_values++;
    if (_data_page_builder->is_page_full()) {
        RETURN_IF_ERROR(_finish_current_data_page());
    }
    return Status::OK();
}

Status IndexedColumnWriter::_finish_current_data_page() {
    const uint32_t page_row_count = _data_page_builder->count();

    if (page_row_count == 0) {
        return Status::OK();
    }

    uint32_t first_rowid = _num_values - page_row_count;
    faststring page_header;
    put_varint32(&page_header, first_rowid);
    put_varint32(&page_header, page_row_count);

    OwnedSlice page_data = _data_page_builder->finish();
    _data_page_builder->reset();

    return _append_data_page({Slice(page_header), page_data.slice()}, first_rowid);
}

Status IndexedColumnWriter::_append_data_page(const std::vector<Slice>& data_page, rowid_t first_rowid) {
    RETURN_IF_ERROR(_append_page(data_page, &_last_data_page));
    _num_data_pages++;

    if (_options.write_ordinal_index) {
        std::string key;
        KeyCoderTraits<OLAP_FIELD_TYPE_UNSIGNED_INT>::full_encode_ascending(
            &first_rowid, &key);
        _ordinal_index_builder->add(key, _last_data_page);
    }

    if (_options.write_value_index) {
        std::string key;
        _validx_key_coder->full_encode_ascending(_first_value.data(), &key);
        // TODO short separate key optimize
        _value_index_builder->add(key, _last_data_page);
        // TODO record last key in short separate key optimize
    }
    return Status::OK();
}

Status IndexedColumnWriter::_append_page(const std::vector<Slice>& page, PagePointer* pp) {
    std::vector<Slice> output_page;

    // Put compressor out of if block, because we will use compressor's
    // content until this function finished.
    PageCompressor compressor(_compress_codec);
    if (_compress_codec != nullptr) {
        RETURN_IF_ERROR(compressor.compress(page, &output_page));
    } else {
        output_page = page;
    }

    // checksum
    uint8_t checksum_buf[sizeof(uint32_t)];
    uint32_t checksum = crc32c::Value(output_page);
    encode_fixed32_le(checksum_buf, checksum);
    output_page.emplace_back(checksum_buf, sizeof(uint32_t));

    // append to file
    pp->offset = _file->size();
    RETURN_IF_ERROR(_file->appendv(&output_page[0], output_page.size()));
    pp->size = _file->size() - pp->offset;
    return Status::OK();
}

Status IndexedColumnWriter::finish(IndexedColumnMetaPB* meta) {
    RETURN_IF_ERROR(_finish_current_data_page());
    if (_options.write_ordinal_index) {
        RETURN_IF_ERROR(_flush_index(_ordinal_index_builder.get(),
                                     meta->mutable_ordinal_index_meta()));
    }
    if (_options.write_value_index) {
        RETURN_IF_ERROR(_flush_index(_value_index_builder.get(),
                                     meta->mutable_value_index_meta()));
    }
    meta->set_data_type(_typeinfo->type());
    meta->set_encoding(_options.encoding);
    meta->set_num_values(_num_values);
    meta->set_compression(_options.compression);
    return Status::OK();
}

Status IndexedColumnWriter::_flush_index(IndexPageBuilder* index_builder, BTreeMetaPB* meta) {
    if (_num_data_pages <= 1) {
        meta->set_is_root_data_page(true);
        _last_data_page.to_proto(meta->mutable_root_page());
    } else {
        Slice root_page = index_builder->finish();
        PagePointer pp;
        RETURN_IF_ERROR(_append_page({root_page}, &pp));

        meta->set_is_root_data_page(false);
        pp.to_proto(meta->mutable_root_page());
    }
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
