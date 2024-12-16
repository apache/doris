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

#include <gen_cpp/segment_v2.pb.h>

#include <ostream>
#include <string>

#include "common/logging.h"
#include "io/fs/file_writer.h"
#include "olap/key_coder.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/encoding_info.h"
#include "olap/rowset/segment_v2/index_page.h"
#include "olap/rowset/segment_v2/options.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_io.h"
#include "olap/rowset/segment_v2/page_pointer.h"
#include "olap/types.h"
#include "util/block_compression.h"
#include "util/slice.h"

namespace doris {
namespace segment_v2 {

IndexedColumnWriter::IndexedColumnWriter(const IndexedColumnWriterOptions& options,
                                         const TypeInfo* type_info, io::FileWriter* file_writer)
        : _options(options),
          _type_info(type_info),
          _file_writer(file_writer),
          _num_values(0),
          _num_data_pages(0),
          _disk_size(0),
          _value_key_coder(nullptr),
          _compress_codec(nullptr) {
    _first_value.resize(_type_info->size());
}

IndexedColumnWriter::~IndexedColumnWriter() = default;

Status IndexedColumnWriter::init() {
    const EncodingInfo* encoding_info;
    RETURN_IF_ERROR(EncodingInfo::get(_type_info, _options.encoding, &encoding_info));
    _options.encoding = encoding_info->encoding();
    // should store more concrete encoding type instead of DEFAULT_ENCODING
    // because the default encoding of a data type can be changed in the future
    DCHECK_NE(_options.encoding, DEFAULT_ENCODING);

    PageBuilder* data_page_builder = nullptr;
    PageBuilderOptions builder_option;
    builder_option.need_check_bitmap = false;
    builder_option.data_page_size = _options.data_page_size;
    RETURN_IF_ERROR(encoding_info->create_page_builder(builder_option, &data_page_builder));
    _data_page_builder.reset(data_page_builder);

    if (_options.write_ordinal_index) {
        _ordinal_index_builder.reset(new IndexPageBuilder(_options.index_page_size, true));
    }
    if (_options.write_value_index) {
        _value_index_builder.reset(new IndexPageBuilder(_options.index_page_size, true));
        _value_key_coder = get_key_coder(_type_info->type());
    }

    if (_options.compression != NO_COMPRESSION) {
        RETURN_IF_ERROR(get_block_compression_codec(_options.compression, &_compress_codec));
    }
    return Status::OK();
}

Status IndexedColumnWriter::add(const void* value) {
    if (_options.write_value_index && _data_page_builder->count() == 0) {
        // remember page's first value because it's used to build value index
        _type_info->deep_copy(_first_value.data(), value, &_arena);
    }
    size_t num_to_write = 1;
    RETURN_IF_ERROR(
            _data_page_builder->add(reinterpret_cast<const uint8_t*>(value), &num_to_write));
    CHECK(num_to_write == 1 || num_to_write == 0);
    if (num_to_write == 0) {
        CHECK(_data_page_builder->is_page_full());
        // current page is already full, we need to first flush the current page,
        // and then add the value to the new page
        size_t num_val;
        RETURN_IF_ERROR(_finish_current_data_page(num_val));
        return add(value);
    }
    _num_values++;
    size_t num_val;
    if (_data_page_builder->is_page_full()) {
        RETURN_IF_ERROR(_finish_current_data_page(num_val));
    }
    return Status::OK();
}

Status IndexedColumnWriter::_finish_current_data_page(size_t& num_val) {
    auto num_values_in_page = _data_page_builder->count();
    num_val = num_values_in_page;
    if (num_values_in_page == 0) {
        return Status::OK();
    }
    ordinal_t first_ordinal = _num_values - num_values_in_page;

    // IndexedColumn doesn't have NULLs, thus data page body only contains encoded values
    OwnedSlice page_body;
    RETURN_IF_ERROR(_data_page_builder->finish(&page_body));
    RETURN_IF_ERROR(_data_page_builder->reset());

    PageFooterPB footer;
    footer.set_type(DATA_PAGE);
    footer.set_uncompressed_size(page_body.slice().get_size());
    footer.mutable_data_page_footer()->set_first_ordinal(first_ordinal);
    footer.mutable_data_page_footer()->set_num_values(num_values_in_page);
    footer.mutable_data_page_footer()->set_nullmap_size(0);

    uint64_t start_size = _file_writer->bytes_appended();
    RETURN_IF_ERROR(PageIO::compress_and_write_page(
            _compress_codec, _options.compression_min_space_saving, _file_writer,
            {page_body.slice()}, footer, &_last_data_page));
    _num_data_pages++;
    _disk_size += (_file_writer->bytes_appended() - start_size);

    if (_options.write_ordinal_index) {
        std::string key;
        KeyCoderTraits<FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT>::full_encode_ascending(
                &first_ordinal, &key);
        _ordinal_index_builder->add(key, _last_data_page);
    }

    if (_options.write_value_index) {
        std::string key;
        _value_key_coder->full_encode_ascending(_first_value.data(), &key);
        // TODO short separate key optimize
        _value_index_builder->add(key, _last_data_page);
        // TODO record last key in short separate key optimize
    }
    return Status::OK();
}

Status IndexedColumnWriter::finish(IndexedColumnMetaPB* meta) {
    size_t num_val_in_page;
    RETURN_IF_ERROR(_finish_current_data_page(num_val_in_page));
    if (_options.write_ordinal_index) {
        RETURN_IF_ERROR(
                _flush_index(_ordinal_index_builder.get(), meta->mutable_ordinal_index_meta()));
    }
    if (_options.write_value_index) {
        RETURN_IF_ERROR(_flush_index(_value_index_builder.get(), meta->mutable_value_index_meta()));
    }
    meta->set_data_type(int(_type_info->type()));
    meta->set_encoding(_options.encoding);
    meta->set_num_values(_num_values);
    meta->set_compression(_options.compression);
    // `_finish_current_data_page` will be called in `add` function when page is full,
    // so num_val_in_page will be zero in this case.
    if (_num_data_pages <= 1 && num_val_in_page != 0) {
        DCHECK(num_val_in_page == _num_values)
                << "num_val_in_page: " << num_val_in_page << ", _num_values: " << _num_values;
    }
    return Status::OK();
}

Status IndexedColumnWriter::_flush_index(IndexPageBuilder* index_builder, BTreeMetaPB* meta) {
    if (_num_data_pages <= 1) {
        meta->set_is_root_data_page(true);
        _last_data_page.to_proto(meta->mutable_root_page());
    } else {
        OwnedSlice page_body;
        PageFooterPB page_footer;
        index_builder->finish(&page_body, &page_footer);

        PagePointer pp;
        uint64_t start_size = _file_writer->bytes_appended();
        RETURN_IF_ERROR(PageIO::compress_and_write_page(
                _compress_codec, _options.compression_min_space_saving, _file_writer,
                {page_body.slice()}, page_footer, &pp));
        _disk_size += (_file_writer->bytes_appended() - start_size);

        meta->set_is_root_data_page(false);
        pp.to_proto(meta->mutable_root_page());
    }
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
