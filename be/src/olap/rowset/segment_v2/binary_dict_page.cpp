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

#include <gen_cpp/segment_v2.pb.h>

#include <algorithm>
#include <ostream>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/logging.h"
#include "common/status.h"
#include "gutil/port.h"
#include "gutil/strings/substitute.h" // for Substitute
#include "olap/rowset/segment_v2/bitshuffle_page.h"
#include "util/coding.h"
#include "util/slice.h" // for Slice
#include "vec/columns/column.h"

namespace doris {
struct StringRef;

namespace segment_v2 {

using strings::Substitute;

BinaryDictPageBuilder::BinaryDictPageBuilder(const PageBuilderOptions& options)
        : _options(options),
          _finished(false),
          _data_page_builder(nullptr),
          _dict_builder(nullptr),
          _encoding_type(DICT_ENCODING) {}

Status BinaryDictPageBuilder::init() {
    // initially use DICT_ENCODING
    // TODO: the data page builder type can be created by Factory according to user config
    PageBuilder* data_page_builder_ptr = nullptr;
    RETURN_IF_ERROR(BitshufflePageBuilder<FieldType::OLAP_FIELD_TYPE_INT>::create(
            &data_page_builder_ptr, _options));
    _data_page_builder.reset(data_page_builder_ptr);
    PageBuilderOptions dict_builder_options;
    dict_builder_options.data_page_size =
            std::min(_options.data_page_size, _options.dict_page_size);
    dict_builder_options.is_dict_page = true;

    PageBuilder* dict_builder_ptr = nullptr;
    RETURN_IF_ERROR(BinaryPlainPageBuilder<FieldType::OLAP_FIELD_TYPE_VARCHAR>::create(
            &dict_builder_ptr, dict_builder_options));
    _dict_builder.reset(static_cast<BinaryPlainPageBuilder<FieldType::OLAP_FIELD_TYPE_VARCHAR>*>(
            dict_builder_ptr));
    return reset();
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
        auto* actual_builder = dynamic_cast<BitshufflePageBuilder<FieldType::OLAP_FIELD_TYPE_INT>*>(
                _data_page_builder.get());

        if (_data_page_builder->count() == 0) {
            _first_value.assign_copy(reinterpret_cast<const uint8_t*>(src->get_data()),
                                     src->get_size());
        }

        for (int i = 0; i < *count; ++i, ++src) {
            if (is_page_full()) {
                break;
            }

            if (src->empty() && _has_empty) {
                value_code = _empty_code;
            } else if (auto iter = _dictionary.find(*src); iter != _dictionary.end()) {
                value_code = iter->second;
            } else {
                Slice dict_item(src->data, src->size);
                if (src->size > 0) {
                    char* item_mem = _arena.alloc(src->size);
                    if (item_mem == nullptr) {
                        return Status::MemoryAllocFailed("memory allocate failed, size:{}",
                                                         src->size);
                    }
                    dict_item.relocate(item_mem);
                }
                value_code = _dictionary.size();
                size_t add_count = 1;
                RETURN_IF_ERROR(_dict_builder->add(reinterpret_cast<const uint8_t*>(&dict_item),
                                                   &add_count));
                if (add_count == 0) {
                    // current dict page is full, stop processing remaining inputs
                    break;
                }
                _dictionary.emplace(dict_item, value_code);
                if (src->empty()) {
                    _has_empty = true;
                    _empty_code = value_code;
                }
            }
            size_t add_count = 1;
            RETURN_IF_ERROR(actual_builder->single_add(
                    reinterpret_cast<const uint8_t*>(&value_code), &add_count));
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
    if (VLOG_DEBUG_IS_ON && _encoding_type == DICT_ENCODING) {
        VLOG_DEBUG << "dict page size:" << _dict_builder->size();
    }

    DCHECK(!_finished);
    _finished = true;

    OwnedSlice data_slice = _data_page_builder->finish();
    // TODO(gaodayue) separate page header and content to avoid this copy
    _buffer.append(data_slice.slice().data, data_slice.slice().size);
    encode_fixed32_le(&_buffer[0], _encoding_type);
    return _buffer.build();
}

Status BinaryDictPageBuilder::reset() {
    RETURN_IF_CATCH_EXCEPTION({
        _finished = false;
        _buffer.reserve(_options.data_page_size + BINARY_DICT_PAGE_HEADER_SIZE);
        _buffer.resize(BINARY_DICT_PAGE_HEADER_SIZE);

        if (_encoding_type == DICT_ENCODING && _dict_builder->is_page_full()) {
            PageBuilder* data_page_builder_ptr = nullptr;
            RETURN_IF_ERROR(BinaryPlainPageBuilder<FieldType::OLAP_FIELD_TYPE_VARCHAR>::create(
                    &data_page_builder_ptr, _options));
            _data_page_builder.reset(data_page_builder_ptr);
            _encoding_type = PLAIN_ENCODING;
        } else {
            RETURN_IF_ERROR(_data_page_builder->reset());
        }
    });
    return Status::OK();
}

size_t BinaryDictPageBuilder::count() const {
    return _data_page_builder->count();
}

uint64_t BinaryDictPageBuilder::size() const {
    return _arena.used_size() + _data_page_builder->size();
}

Status BinaryDictPageBuilder::get_dictionary_page(OwnedSlice* dictionary_page) {
    *dictionary_page = _dict_builder->finish();
    return Status::OK();
}

Status BinaryDictPageBuilder::get_first_value(void* value) const {
    DCHECK(_finished);
    if (_data_page_builder->count() == 0) {
        return Status::Error<ErrorCode::ENTRY_NOT_FOUND>("page is empty");
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
        return Status::Error<ErrorCode::ENTRY_NOT_FOUND>("page is empty");
    }
    if (_encoding_type != DICT_ENCODING) {
        return _data_page_builder->get_last_value(value);
    }
    uint32_t value_code;
    RETURN_IF_ERROR(_data_page_builder->get_last_value(&value_code));
    *reinterpret_cast<Slice*>(value) = _dict_builder->get(value_code);
    return Status::OK();
}

BinaryDictPageDecoder::BinaryDictPageDecoder(Slice data, const PageDecoderOptions& options)
        : _data(data),
          _options(options),
          _data_page_decoder(nullptr),
          _parsed(false),
          _encoding_type(UNKNOWN_ENCODING) {}

Status BinaryDictPageDecoder::init() {
    CHECK(!_parsed);
    if (_data.size < BINARY_DICT_PAGE_HEADER_SIZE) {
        return Status::Corruption("invalid data size:{}, header size:{}", _data.size,
                                  BINARY_DICT_PAGE_HEADER_SIZE);
    }
    size_t type = decode_fixed32_le((const uint8_t*)&_data.data[0]);
    _encoding_type = static_cast<EncodingTypePB>(type);
    _data.remove_prefix(BINARY_DICT_PAGE_HEADER_SIZE);
    if (_encoding_type == DICT_ENCODING) {
        _data_page_decoder.reset(
                _bit_shuffle_ptr =
                        new BitShufflePageDecoder<FieldType::OLAP_FIELD_TYPE_INT>(_data, _options));
    } else if (_encoding_type == PLAIN_ENCODING) {
        DCHECK_EQ(_encoding_type, PLAIN_ENCODING);
        _data_page_decoder.reset(
                new BinaryPlainPageDecoder<FieldType::OLAP_FIELD_TYPE_INT>(_data, _options));
    } else {
        LOG(WARNING) << "invalid encoding type:" << _encoding_type;
        return Status::Corruption("invalid encoding type:{}", _encoding_type);
    }

    RETURN_IF_ERROR(_data_page_decoder->init());
    _parsed = true;
    return Status::OK();
}

BinaryDictPageDecoder::~BinaryDictPageDecoder() {}

Status BinaryDictPageDecoder::seek_to_position_in_page(size_t pos) {
    return _data_page_decoder->seek_to_position_in_page(pos);
}

bool BinaryDictPageDecoder::is_dict_encoding() const {
    return _encoding_type == DICT_ENCODING;
}

void BinaryDictPageDecoder::set_dict_decoder(PageDecoder* dict_decoder, StringRef* dict_word_info) {
    _dict_decoder = (BinaryPlainPageDecoder<FieldType::OLAP_FIELD_TYPE_VARCHAR>*)dict_decoder;
    _dict_word_info = dict_word_info;
};

Status BinaryDictPageDecoder::next_batch(size_t* n, vectorized::MutableColumnPtr& dst) {
    if (_encoding_type == PLAIN_ENCODING) {
        dst = dst->convert_to_predicate_column_if_dictionary();
        return _data_page_decoder->next_batch(n, dst);
    }
    // dictionary encoding
    DCHECK(_parsed);
    DCHECK(_dict_decoder != nullptr) << "dict decoder pointer is nullptr";

    if (PREDICT_FALSE(*n == 0 || _bit_shuffle_ptr->_cur_index >= _bit_shuffle_ptr->_num_elements)) {
        *n = 0;
        return Status::OK();
    }

    size_t max_fetch = std::min(*n, static_cast<size_t>(_bit_shuffle_ptr->_num_elements -
                                                        _bit_shuffle_ptr->_cur_index));
    *n = max_fetch;

    const auto* data_array = reinterpret_cast<const int32_t*>(_bit_shuffle_ptr->get_data(0));
    size_t start_index = _bit_shuffle_ptr->_cur_index;

    dst->insert_many_dict_data(data_array, start_index, _dict_word_info, max_fetch,
                               _dict_decoder->_num_elems);

    _bit_shuffle_ptr->_cur_index += max_fetch;

    return Status::OK();
}

Status BinaryDictPageDecoder::read_by_rowids(const rowid_t* rowids, ordinal_t page_first_ordinal,
                                             size_t* n, vectorized::MutableColumnPtr& dst) {
    if (_encoding_type == PLAIN_ENCODING) {
        dst = dst->convert_to_predicate_column_if_dictionary();
        return _data_page_decoder->read_by_rowids(rowids, page_first_ordinal, n, dst);
    }
    DCHECK(_parsed);
    DCHECK(_dict_decoder != nullptr) << "dict decoder pointer is nullptr";

    if (PREDICT_FALSE(*n == 0)) {
        *n = 0;
        return Status::OK();
    }

    const auto* data_array = reinterpret_cast<const int32_t*>(_bit_shuffle_ptr->get_data(0));
    auto total = *n;
    size_t read_count = 0;
    _buffer.resize(total);
    for (size_t i = 0; i < total; ++i) {
        ordinal_t ord = rowids[i] - page_first_ordinal;
        if (PREDICT_FALSE(ord >= _bit_shuffle_ptr->_num_elements)) {
            break;
        }

        _buffer[read_count++] = data_array[ord];
    }

    if (LIKELY(read_count > 0)) {
        dst->insert_many_dict_data(_buffer.data(), 0, _dict_word_info, read_count,
                                   _dict_decoder->_num_elems);
    }
    *n = read_count;
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
