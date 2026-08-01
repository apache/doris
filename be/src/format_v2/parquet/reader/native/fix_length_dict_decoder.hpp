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

#include <limits>

#include "format_v2/parquet/reader/native/decoder.h"

namespace doris::format::parquet::native {

// Dictionary decoders retain only encoded physical values and the index-stream cursor. Logical
// interpretation is deliberately delegated to DataTypeSerDe through decode_dictionary().
template <tparquet::Type::type PhysicalType>
class FixLengthDictDecoder final : public BaseDictDecoder {
public:
    FixLengthDictDecoder() = default;
    ~FixLengthDictDecoder() override = default;

    size_t dictionary_size() const override { return _num_dictionary_values; }

    Status decode_dictionary(ParquetFixedValueConsumer& fixed_consumer,
                             ParquetBinaryValueConsumer& binary_consumer) override {
        return fixed_consumer.consume(_dict.get(), _num_dictionary_values,
                                      static_cast<size_t>(_type_length));
    }

    Status decode_selected_fixed_values(const ParquetSelection& selection,
                                        ParquetFixedValueConsumer& consumer) override {
        DORIS_CHECK_GT(_type_length, 0);
        // Raw predicates on non-string dictionaries must observe decoded physical values, not
        // dictionary IDs, so expand each validated index before invoking the predicate consumer.
        class ExpandedValueConsumer final : public ParquetDictionaryValueConsumer {
        public:
            ExpandedValueConsumer(const uint8_t* dictionary, size_t dictionary_size,
                                  size_t value_width, ParquetFixedValueConsumer& consumer,
                                  std::vector<uint8_t>& scratch)
                    : _dictionary(dictionary),
                      _dictionary_size(dictionary_size),
                      _value_width(value_width),
                      _consumer(consumer),
                      _scratch(scratch) {}

            Status consume_indices(const uint32_t* indices, size_t num_values) override {
                DORIS_CHECK(indices != nullptr || num_values == 0);
                if (UNLIKELY(num_values > std::numeric_limits<size_t>::max() / _value_width)) {
                    return Status::IOError("Parquet dictionary expansion size overflows");
                }
                _scratch.resize(num_values * _value_width);
                for (size_t row = 0; row < num_values; ++row) {
                    DORIS_CHECK_LT(indices[row], _dictionary_size);
                    memcpy(_scratch.data() + row * _value_width,
                           _dictionary + static_cast<size_t>(indices[row]) * _value_width,
                           _value_width);
                }
                return _consumer.consume(_scratch.data(), num_values, _value_width);
            }

            Status consume_repeated(uint32_t index, size_t num_values) override {
                DORIS_CHECK_LT(index, _dictionary_size);
                constexpr size_t MAX_BATCH_VALUES = 1024;
                const uint8_t* value = _dictionary + static_cast<size_t>(index) * _value_width;
                while (num_values > 0) {
                    const size_t batch = std::min(num_values, MAX_BATCH_VALUES);
                    _scratch.resize(batch * _value_width);
                    for (size_t row = 0; row < batch; ++row) {
                        memcpy(_scratch.data() + row * _value_width, value, _value_width);
                    }
                    RETURN_IF_ERROR(_consumer.consume(_scratch.data(), batch, _value_width));
                    num_values -= batch;
                }
                return Status::OK();
            }

        private:
            const uint8_t* const _dictionary;
            const size_t _dictionary_size;
            const size_t _value_width;
            ParquetFixedValueConsumer& _consumer;
            std::vector<uint8_t>& _scratch;
        } expanded_consumer(_dict.get(), _num_dictionary_values, static_cast<size_t>(_type_length),
                            consumer, _expanded_values);
        return decode_selected_dictionary_values(selection, expanded_consumer);
    }

    void release_scratch(size_t max_retained_bytes) override {
        BaseDictDecoder::release_scratch(max_retained_bytes);
        release_vector_if_oversized(&_expanded_values, max_retained_bytes);
    }

    size_t retained_scratch_bytes() const override {
        return BaseDictDecoder::retained_scratch_bytes() + _expanded_values.capacity();
    }

    size_t active_scratch_bytes() const override {
        return BaseDictDecoder::active_scratch_bytes() + _expanded_values.size();
    }

    Status set_dict(DorisUniqueBufferPtr<uint8_t>& dict, int32_t length,
                    size_t num_values) override {
        if (UNLIKELY(_type_length <= 0 || length < 0 ||
                     num_values > std::numeric_limits<size_t>::max() /
                                          static_cast<size_t>(_type_length) ||
                     num_values * static_cast<size_t>(_type_length) !=
                             static_cast<size_t>(length))) {
            return Status::Corruption("Wrong dictionary data for fixed length type");
        }
        if (UNLIKELY(dict == nullptr)) {
            return Status::Corruption("Fixed-length Parquet dictionary is null");
        }
        _dict = std::move(dict);
        _num_dictionary_values = num_values;
        ++_dictionary_generation;
        return Status::OK();
    }

private:
    size_t _num_dictionary_values = 0;
    std::vector<uint8_t> _expanded_values;
};

} // namespace doris::format::parquet::native
