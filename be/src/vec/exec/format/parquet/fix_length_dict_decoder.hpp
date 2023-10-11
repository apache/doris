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

#include "util/bit_util.h"
#include "vec/columns/column_dictionary.h"
#include "vec/columns/column_nullable.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exec/format/parquet/decoder.h"

namespace doris::vectorized {

template <tparquet::Type::type PhysicalType>
class FixLengthDictDecoder final : public BaseDictDecoder {
public:
    FixLengthDictDecoder() : BaseDictDecoder() {};
    ~FixLengthDictDecoder() override = default;

    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector, bool is_dict_filter) override {
        if (select_vector.has_filter()) {
            return _decode_values<true>(doris_column, data_type, select_vector, is_dict_filter);
        } else {
            return _decode_values<false>(doris_column, data_type, select_vector, is_dict_filter);
        }
    }

    template <bool has_filter>
    Status _decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                          ColumnSelectVector& select_vector, bool is_dict_filter) {
        size_t non_null_size = select_vector.num_values() - select_vector.num_nulls();
        if (doris_column->is_column_dictionary() &&
            assert_cast<ColumnDictI32&>(*doris_column).dict_size() == 0) {
            std::vector<StringRef> dict_items;
            dict_items.reserve(_dict_items.size());
            for (int i = 0; i < _dict_items.size(); ++i) {
                dict_items.emplace_back((char*)(&_dict_items[i]), _type_length);
            }
            assert_cast<ColumnDictI32&>(*doris_column)
                    .insert_many_dict_data(&dict_items[0], dict_items.size());
        }
        if (doris_column->is_column_dictionary()) {
            ColumnDictI32& dict_column = assert_cast<ColumnDictI32&>(*doris_column);
            if (dict_column.dict_size() == 0) {
                std::vector<StringRef> dict_items;
                dict_items.reserve(_dict_items.size());
                for (int i = 0; i < _dict_items.size(); ++i) {
                    dict_items.emplace_back((char*)(&_dict_items[i]), _type_length);
                }
                dict_column.insert_many_dict_data(&dict_items[0], dict_items.size());
            }
        }
        _indexes.resize(non_null_size);
        _index_batch_decoder->GetBatch(&_indexes[0], non_null_size);

        if (doris_column->is_column_dictionary() || is_dict_filter) {
            return _decode_dict_values<has_filter>(doris_column, select_vector, is_dict_filter);
        }

        _decode_numeric<has_filter>(doris_column, select_vector);

        return Status::OK();
    }

    Status set_dict(std::unique_ptr<uint8_t[]>& dict, int32_t length, size_t num_values) override {
        if (num_values * _type_length != length) {
            return Status::Corruption("Wrong dictionary data for fixed length type");
        }
        _dict = std::move(dict);
        char* dict_item_address = reinterpret_cast<char*>(_dict.get());
        _dict_items.resize(num_values);
        for (size_t i = 0; i < num_values; ++i) {
            _dict_items[i] = *(DataType*)dict_item_address;
            dict_item_address += _type_length;
        }
        return Status::OK();
    }

protected:
    template <bool has_filter>
    Status _decode_numeric(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector) {
        auto& column_data = static_cast<ColumnType&>(*doris_column).get_data();
        size_t data_index = column_data.size();
        column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
        size_t dict_index = 0;
        ColumnSelectVector::DataReadType read_type;
        while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
            switch (read_type) {
            case ColumnSelectVector::CONTENT: {
                for (size_t i = 0; i < run_length; ++i) {
                    column_data[data_index++] =
                            static_cast<DataType>(_dict_items[_indexes[dict_index++]]);
                }
                break;
            }
            case ColumnSelectVector::NULL_DATA: {
                data_index += run_length;
                break;
            }
            case ColumnSelectVector::FILTERED_CONTENT: {
                dict_index += run_length;
                break;
            }
            case ColumnSelectVector::FILTERED_NULL: {
                // do nothing
                break;
            }
            }
        }
        return Status::OK();
    }
    using ColumnType = ParquetConvert::PhysicalTypeTraits<PhysicalType>::ColumnType;
    using DataType = ParquetConvert::PhysicalTypeTraits<PhysicalType>::DataType;

    // For dictionary encoding
    std::vector<DataType> _dict_items;
};

template <>
class FixLengthDictDecoder<tparquet::Type::FIXED_LEN_BYTE_ARRAY> final : public BaseDictDecoder {
public:
    FixLengthDictDecoder() : BaseDictDecoder() {};
    ~FixLengthDictDecoder() override = default;

    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector, bool is_dict_filter) override {
        if (select_vector.has_filter()) {
            return _decode_values<true>(doris_column, data_type, select_vector, is_dict_filter);
        } else {
            return _decode_values<false>(doris_column, data_type, select_vector, is_dict_filter);
        }
    }

    template <bool has_filter>
    Status _decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                          ColumnSelectVector& select_vector, bool is_dict_filter) {
        size_t non_null_size = select_vector.num_values() - select_vector.num_nulls();
        if (doris_column->is_column_dictionary() &&
            assert_cast<ColumnDictI32&>(*doris_column).dict_size() == 0) {
            std::vector<StringRef> dict_items;
            dict_items.reserve(_dict_items.size());
            for (int i = 0; i < _dict_items.size(); ++i) {
                dict_items.emplace_back(_dict_items[i], _type_length);
            }
            assert_cast<ColumnDictI32&>(*doris_column)
                    .insert_many_dict_data(&dict_items[0], dict_items.size());
        }
        _indexes.resize(non_null_size);
        _index_batch_decoder->GetBatch(&_indexes[0], non_null_size);

        if (doris_column->is_column_dictionary() || is_dict_filter) {
            return _decode_dict_values<has_filter>(doris_column, select_vector, is_dict_filter);
        }

        return _decode_string<has_filter>(doris_column, select_vector);
    }

protected:
    template <bool has_filter>
    Status _decode_string(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector) {
        size_t dict_index = 0;
        ColumnSelectVector::DataReadType read_type;
        while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
            switch (read_type) {
            case ColumnSelectVector::CONTENT: {
                std::vector<StringRef> string_values;
                string_values.reserve(run_length);
                for (size_t i = 0; i < run_length; ++i) {
                    string_values.emplace_back(_dict_items[_indexes[dict_index++]], _type_length);
                }
                doris_column->insert_many_strings(&string_values[0], run_length);
                break;
            }
            case ColumnSelectVector::NULL_DATA: {
                doris_column->insert_many_defaults(run_length);
                break;
            }
            case ColumnSelectVector::FILTERED_CONTENT: {
                dict_index += run_length;
                break;
            }
            case ColumnSelectVector::FILTERED_NULL: {
                // do nothing
                break;
            }
            }
        }
        return Status::OK();
    }

    // For dictionary encoding
    std::vector<char*> _dict_items;
};

} // namespace doris::vectorized
