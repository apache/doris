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

#include "olap/rowset/segment_v2/binary_dict_page.h"

#include "common/logging.h"
#include "util/slice.h" // for Slice
#include "gutil/strings/substitute.h" // for Substitute
#include "olap/rowset/segment_v2/bitshuffle_page.h"

namespace doris {
namespace segment_v2 {

using strings::Substitute;

BinaryDictPageBuilder::BinaryDictPageBuilder(const PageBuilderOptions& options) :
    _options(options),
    _finished(false),
    _data_page_builder(nullptr),
    _dict_builder(nullptr),
    _encoding_type(DICT_ENCODING),
    _pool(&_tracker) {
    // initially use DICT_ENCODING
    // TODO: the data page builder type can be created by Factory according to user config
    _data_page_builder.reset(new BitshufflePageBuilder<OLAP_FIELD_TYPE_INT>(options));
    PageBuilderOptions dict_builder_options;
    dict_builder_options.data_page_size = _options.dict_page_size;
    _dict_builder.reset(new BinaryPlainPageBuilder(dict_builder_options));
    reset();
}

bool BinaryDictPageBuilder::is_page_full() {
    if (_data_page_builder->is_page_full()) {
        return true;
    }
    if (_encoding_type == DICT_ENCODING && _dict_builder->is_page_full()) {
        return true;
    }
    return false;
}

Status BinaryDictPageBuilder::add(const uint8_t* vals, size_t* count) {
    if (_encoding_type == DICT_ENCODING) {
        DCHECK(!_finished);
        DCHECK_GT(*count, 0);
        const Slice* src = reinterpret_cast<const Slice*>(vals);
        size_t num_added = 0;
        uint32_t value_code = -1;

        if (_data_page_builder->count() == 0) {
            _first_value.assign_copy(reinterpret_cast<const uint8_t*>(src->get_data()), src->get_size());
        }

        for (int i = 0; i < *count; ++i, ++src) {
            auto iter = _dictionary.find(*src);
            if (iter != _dictionary.end()) {
                value_code = iter->second;
            } else {
                if (_dict_builder->is_page_full()) {
                    break;
                }
                Slice dict_item(src->data, src->size);
                if (src->size > 0) {
                    char* item_mem = (char*)_pool.allocate(src->size);
                    if (item_mem == nullptr) {
                        return Status::MemoryAllocFailed(Substitute("memory allocate failed, size:$0", src->size));
                    }
                    dict_item.relocate(item_mem);
                }
                value_code = _dictionary.size();
                _dictionary.emplace(dict_item, value_code);
                _dict_items.push_back(dict_item);
                _dict_builder->update_prepared_size(dict_item.size);
            }
            size_t add_count = 1;
            RETURN_IF_ERROR(_data_page_builder->add(reinterpret_cast<const uint8_t*>(&value_code), &add_count));
            if (add_count == 0) {
                // current data page is full, stop processing remaining inputs
                break;
            }
            num_added += 1;
        }
        *count = num_added;
        return Status::OK();
    } else {
        DCHECK_EQ(_encoding_type, PLAIN_ENCODING);
        return _data_page_builder->add(vals, count);
    }
}

OwnedSlice BinaryDictPageBuilder::finish() {
    DCHECK(!_finished);
    _finished = true;

    OwnedSlice data_slice = _data_page_builder->finish();
    // TODO(gaodayue) separate page header and content to avoid this copy
    _buffer.append(data_slice.slice().data, data_slice.slice().size);
    encode_fixed32_le(&_buffer[0], _encoding_type);
    return _buffer.build();
}

void BinaryDictPageBuilder::reset() {
    _finished = false;
    _buffer.reserve(_options.data_page_size + BINARY_DICT_PAGE_HEADER_SIZE);
    _buffer.resize(BINARY_DICT_PAGE_HEADER_SIZE);

    if (_encoding_type == DICT_ENCODING
            && _dict_builder->is_page_full()) {
        _data_page_builder.reset(new BinaryPlainPageBuilder(_options));
        _encoding_type = PLAIN_ENCODING;
    } else {
        _data_page_builder->reset();
    }
    _finished = false;
}

size_t BinaryDictPageBuilder::count() const {
    return _data_page_builder->count();
}

uint64_t BinaryDictPageBuilder::size() const {
    return _pool.total_allocated_bytes() + _data_page_builder->size();
}

Status BinaryDictPageBuilder::get_dictionary_page(OwnedSlice* dictionary_page) {
    _dictionary.clear();
    _dict_builder->reset();
    size_t add_count = 1;
    // here do not check is_page_full of dict_builder
    // because it is checked in add
    for (auto& dict_item: _dict_items) {
        RETURN_IF_ERROR(_dict_builder->add(reinterpret_cast<const uint8_t*>(&dict_item), &add_count));
    }
    *dictionary_page = _dict_builder->finish();
    _dict_items.clear();
    return Status::OK();
}

Status BinaryDictPageBuilder::get_first_value(void* value) const {
    DCHECK(_finished);
    if (_data_page_builder->count() == 0) {
        return Status::NotFound("page is empty");
    }
    if (_encoding_type != DICT_ENCODING) {
        return _data_page_builder->get_first_value(value);
    }
    *reinterpret_cast<Slice*>(value) = Slice(_first_value);
    return Status::OK();
}

Status BinaryDictPageBuilder::get_last_value(void* value) const {
    DCHECK(_finished);
    if (_data_page_builder->count() == 0) {
        return Status::NotFound("page is empty");
    }
    if (_encoding_type != DICT_ENCODING) {
        return _data_page_builder->get_last_value(value);
    }
    uint32_t value_code;
    RETURN_IF_ERROR(_data_page_builder->get_last_value(&value_code));
    // TODO _dict_items is cleared in get_dictionary_page, which could cause
    // get_last_value to fail when it's called after get_dictionary_page.
    // the solution is to read last value from _dict_builder instead of _dict_items
    *reinterpret_cast<Slice*>(value) = _dict_items[value_code];
    return Status::OK();
}

BinaryDictPageDecoder::BinaryDictPageDecoder(Slice data, const PageDecoderOptions& options) :
    _data(data),
    _options(options),
    _data_page_decoder(nullptr),
    _parsed(false),
    _encoding_type(UNKNOWN_ENCODING) { }

Status BinaryDictPageDecoder::init() {
    CHECK(!_parsed);
    if (_data.size < BINARY_DICT_PAGE_HEADER_SIZE) {
        return Status::Corruption(Substitute("invalid data size:$0, header size:$1",
                _data.size, BINARY_DICT_PAGE_HEADER_SIZE));
    }
    size_t type = decode_fixed32_le((const uint8_t*)&_data.data[0]);
    _encoding_type = static_cast<EncodingTypePB>(type);
    _data.remove_prefix(BINARY_DICT_PAGE_HEADER_SIZE);
    if (_encoding_type == DICT_ENCODING) {
        _data_page_decoder.reset(new BitShufflePageDecoder<OLAP_FIELD_TYPE_INT>(_data, _options));
    } else if (_encoding_type == PLAIN_ENCODING) {
        DCHECK_EQ(_encoding_type, PLAIN_ENCODING);
        _data_page_decoder.reset(new BinaryPlainPageDecoder(_data, _options));
    } else {
        LOG(WARNING) << "invalide encoding type:" << _encoding_type;
        return Status::Corruption(Substitute("invalid encoding type:$0", _encoding_type));
    }

    RETURN_IF_ERROR(_data_page_decoder->init());
    _parsed = true;
    return Status::OK();
}

Status BinaryDictPageDecoder::seek_to_position_in_page(size_t pos) {
    return _data_page_decoder->seek_to_position_in_page(pos);
}

bool BinaryDictPageDecoder::is_dict_encoding() const {
    return _encoding_type == DICT_ENCODING;
}

void BinaryDictPageDecoder::set_dict_decoder(PageDecoder* dict_decoder){
    _dict_decoder = (BinaryPlainPageDecoder*)dict_decoder;
};

Status BinaryDictPageDecoder::next_batch(size_t* n, ColumnBlockView* dst) {
    if (_encoding_type == PLAIN_ENCODING) {
        return _data_page_decoder->next_batch(n, dst);
    }
    // dictionary encoding
    DCHECK(_parsed);
    DCHECK(_dict_decoder != nullptr) << "dict decoder pointer is nullptr";
    if (PREDICT_FALSE(*n == 0)) {
        *n = 0;
        return Status::OK();
    }
    Slice* out = reinterpret_cast<Slice*>(dst->data());
    _code_buf.resize((*n) * sizeof(int32_t));

    // copy the codewords into a temporary buffer first
    // And then copy the strings corresponding to the codewords to the destination buffer
    TypeInfo *type_info = get_type_info(OLAP_FIELD_TYPE_INT);
    // the data in page is not null
    ColumnBlock column_block(type_info, _code_buf.data(), nullptr, *n, dst->column_block()->pool());
    ColumnBlockView tmp_block_view(&column_block);
    RETURN_IF_ERROR(_data_page_decoder->next_batch(n, &tmp_block_view));
    for (int i = 0; i < *n; ++i) {
        int32_t codeword = *reinterpret_cast<int32_t*>(&_code_buf[i * sizeof(int32_t)]);
        // get the string from the dict decoder
        Slice element = _dict_decoder->string_at_index(codeword);
        if (element.size > 0) {
            char* destination = (char*)dst->column_block()->pool()->allocate(element.size);
            if (destination == nullptr) {
                return Status::MemoryAllocFailed(Substitute(
                    "memory allocate failed, size:$0", element.size));
            }
            element.relocate(destination);
        }
        *out = element;
        ++out;
    }
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
