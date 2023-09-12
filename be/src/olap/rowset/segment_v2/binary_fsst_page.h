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

#include <gen_cpp/PaloBrokerService_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>

#include <cstddef>
#include <memory>
#include <vector>

#include "common/status.h"
#include "fsst.h"
#include "fsst12.h"
#include "gutil/integral_types.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/binary_plain_page.h"
#include "olap/rowset/segment_v2/options.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "vec/common/string_ref.h"
namespace doris {
namespace segment_v2 {
class BinaryFsstPageBuilder : public PageBuilder {
public:
    BinaryFsstPageBuilder(const PageBuilderOptions& options) {
        _data_page_builder.reset(
                new BinaryPlainPageBuilder<FieldType::OLAP_FIELD_TYPE_VARCHAR>(options));
        PageBuilderOptions dict_builder_options;
        dict_builder_options.data_page_size =
                std::min(options.data_page_size, options.dict_page_size);
        dict_builder_options.is_dict_page = true;
        _dict_builder.reset(
                new BinaryPlainPageBuilder<FieldType::OLAP_FIELD_TYPE_NONE>(dict_builder_options));
    }

    ~BinaryFsstPageBuilder() override {
        if (_fsst_encoder != nullptr) {
            fsst_destroy(_fsst_encoder);
        }
    }

    bool is_page_full() override { return _data_page_builder->is_page_full(); }

    Status analyze(const uint8_t* vals, size_t count) override {
        const Slice* src = reinterpret_cast<const Slice*>(vals);
        std::vector<size_t> fsst_string_sizes;
        fsst_string_sizes.reserve(count);
        std::vector<unsigned char*> fsst_string_ptrs;
        fsst_string_ptrs.reserve(count);
        size_t total_size = 0;
        for (int i = 0; i < count; ++i) {
            auto s = &src[i];
            if (s->empty()) {
                continue;
            }
            fsst_string_sizes.push_back(s->size);
            fsst_string_ptrs.push_back(reinterpret_cast<unsigned char*>(s->data));
            total_size += s->size;
        }
        if (fsst_string_ptrs.empty()) {
            return Status::OK();
        }
        _fsst_encoder = fsst_create(count, &fsst_string_sizes[0], &fsst_string_ptrs[0], 0);

        size_t compress_buffer_size = total_size * 2 + 7;
        _strings_out.resize(fsst_string_ptrs.size(), nullptr);
        _sizes_out.resize(fsst_string_ptrs.size(), 0);
        _compress_buffer.resize(compress_buffer_size, 0);

        size_t res =
                fsst_compress(this->_fsst_encoder, fsst_string_ptrs.size(), &fsst_string_sizes[0],
                              &fsst_string_ptrs[0], compress_buffer_size, &_compress_buffer[0],
                              &_sizes_out[0], &_strings_out[0]);
        if (res != fsst_string_ptrs.size()) {
            return Status::InternalError("fsst_compress failed");
        };
        return Status::OK();
    }

    Status add(const uint8_t* vals, size_t* count) override {
        DCHECK(!_finished);
        DCHECK_GT(*count, 0);
        const Slice* src = reinterpret_cast<const Slice*>(vals);
        size_t num_added = 0;
        for (size_t i = 0; i < *count; ++i, ++src) {
            size_t add_count = 1;
            if (src->empty()) {
                _data_page_builder->add(reinterpret_cast<const uint8*>(src), &add_count);
            } else {
                _data_page_builder->add_one(
                        reinterpret_cast<char*>(_strings_out[_compressed_index]),
                        _sizes_out[_compressed_index], &add_count);
                _compressed_index++;
            }
            num_added++;
            if (add_count == 0) {
                break;
            }
        }
        *count = num_added;
        return Status::OK();
    }

    OwnedSlice finish() override {
        DCHECK(!_finished);
        _finished = true;
        return _data_page_builder->finish();
    }

    void reset() override {
        _finished = false;
        _data_page_builder->reset();
        _dict_builder->reset();
    }

    size_t count() const override { return _data_page_builder->count(); }

    uint64_t size() const override { return _data_page_builder->size(); }

    Status get_dictionary_page(OwnedSlice* dictionary_page) override {
        *dictionary_page = _dict_builder->finish();
        return Status::OK();
    }

    Status get_first_value(void* value) const override {
        DCHECK(_finished);
        uint32_t value_code;
        RETURN_IF_ERROR(_data_page_builder->get_first_value(&value_code));
        *reinterpret_cast<Slice*>(value) = _dict_builder->get(value_code);
        return Status::OK();
    }

    Status get_last_value(void* value) const override {
        DCHECK(_finished);
        uint32_t value_code;
        RETURN_IF_ERROR(_data_page_builder->get_last_value(&value_code));
        *reinterpret_cast<Slice*>(value) = _dict_builder->get(value_code);
        return Status::OK();
    }

private:
    std::unique_ptr<BinaryPlainPageBuilder<FieldType::OLAP_FIELD_TYPE_VARCHAR>> _data_page_builder;
    std::unique_ptr<BinaryPlainPageBuilder<FieldType::OLAP_FIELD_TYPE_NONE>> _dict_builder;
    bool _finished = false;
    fsst_encoder_t* _fsst_encoder;
    std::vector<unsigned char> _compress_buffer;
    size_t _compressed_index = 0;
    std::vector<unsigned char*> _strings_out;
    std::vector<size_t> _sizes_out;
};

class BinaryFsstPageDecoder : public PageDecoder {
public:
    BinaryFsstPageDecoder(Slice data, const PageDecoderOptions& options) {
        _data_page_decoder.reset(
                new BinaryPlainPageDecoder<FieldType::OLAP_FIELD_TYPE_VARCHAR>(data, options));
    }

    ~BinaryFsstPageDecoder() override {}

    Status init() override { return _data_page_decoder->init(); }

    Status seek_to_position_in_page(size_t pos) override {
        return _data_page_decoder->seek_to_position_in_page(pos);
    }

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst) override {
        auto codes = dst->clone_empty();
        RETURN_IF_ERROR(_data_page_decoder->next_batch(n, codes));
        return decode(dst, *n, std::move(codes));
    }

    Status read_by_rowids(const rowid_t* rowids, ordinal_t page_first_ordinal, size_t* n,
                          vectorized::MutableColumnPtr& dst) override {
        auto codes = dst->clone_empty();
        RETURN_IF_ERROR(_data_page_decoder->read_by_rowids(rowids, page_first_ordinal, n, codes));
        return decode(dst, *n, std::move(codes));
    }

    size_t count() const override { return _data_page_decoder->count(); }

    void set_dict_decoder(PageDecoder* dict_decoder, StringRef* dict_word_info) {
        _dict_word_info = dict_word_info;
    };

    size_t current_index() const override { return _data_page_decoder->current_index(); }

private:
    Status decode(vectorized::MutableColumnPtr& dst, size_t n, const vectorized::ColumnPtr& codes) {
        constexpr size_t BufferSize = 255;
        unsigned char decompress_buffer[BufferSize]; //todo(sky): choose a proper size
        auto fsst_decoder = reinterpret_cast<const fsst_decoder_t*>(_dict_word_info->data);
        auto fsst_decoder_mut = const_cast<fsst_decoder_t*>(fsst_decoder);

        for (size_t i = 0; i < n; i++) {
            StringRef code = codes->get_data_at(i);
            if (code.empty()) {
                dst->insert_data(code.data, code.size);
            } else {
                auto code_mut = const_cast<unsigned char*>(
                        reinterpret_cast<const unsigned char*>(code.data));
                size_t data_size = fsst_decompress(fsst_decoder_mut, code.size, code_mut,
                                                   BufferSize, &decompress_buffer[0]);
                dst->insert_data(reinterpret_cast<const char*>(decompress_buffer), data_size);
            }
        }
        return Status::OK();
    }

private:
    StringRef* _dict_word_info = nullptr;
    std::unique_ptr<BinaryPlainPageDecoder<FieldType::OLAP_FIELD_TYPE_VARCHAR>> _data_page_decoder;
};
} // namespace segment_v2
} // namespace doris
