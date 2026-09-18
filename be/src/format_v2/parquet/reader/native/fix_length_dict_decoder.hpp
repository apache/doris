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
};

} // namespace doris::format::parquet::native
