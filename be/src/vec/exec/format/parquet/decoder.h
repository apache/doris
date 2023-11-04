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

#include <gen_cpp/parquet_types.h>
#include <glog/logging.h>
#include <stddef.h>

#include <cstdint>
#include <memory>
#include <ostream>
#include <vector>

#include "common/status.h"
#include "schema_desc.h"
#include "util/rle_encoding.h"
#include "util/slice.h"
#include "vec/columns/column.h"
#include "vec/columns/column_dictionary.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_decimal.h" // IWYU pragma: keep
#include "vec/data_types/data_type_nullable.h"
#include "vec/exec/format/format_common.h"
#include "vec/exec/format/parquet/parquet_common.h"

namespace cctz {
class time_zone;
} // namespace cctz
namespace doris {
namespace vectorized {
class ColumnString;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

class Decoder {
public:
    Decoder() = default;
    virtual ~Decoder() = default;

    static Status get_decoder(tparquet::Type::type type, tparquet::Encoding::type encoding,
                              std::unique_ptr<Decoder>& decoder);

    // The type with fix length
    void set_type_length(int32_t type_length) { _type_length = type_length; }

    // Set the data to be decoded
    virtual void set_data(Slice* data) {
        _data = data;
        _offset = 0;
    }

    // Write the decoded values batch to doris's column
    virtual Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                 ColumnSelectVector& select_vector, bool is_dict_filter) = 0;

    virtual Status skip_values(size_t num_values) = 0;

    virtual Status set_dict(std::unique_ptr<uint8_t[]>& dict, int32_t length, size_t num_values) {
        return Status::NotSupported("set_dict is not supported");
    }

    virtual Status read_dict_values_to_column(MutableColumnPtr& doris_column) {
        return Status::NotSupported("read_dict_values_to_column is not supported");
    }

    virtual Status get_dict_codes(const ColumnString* column_string,
                                  std::vector<int32_t>* dict_codes) {
        return Status::NotSupported("get_dict_codes is not supported");
    }

    virtual MutableColumnPtr convert_dict_column_to_string_column(const ColumnInt32* dict_column) {
        LOG(FATAL) << "Method convert_dict_column_to_string_column is not supported";
    }

protected:
    int32_t _type_length;
    Slice* _data = nullptr;
    uint32_t _offset = 0;
};

class BaseDictDecoder : public Decoder {
public:
    BaseDictDecoder() = default;
    virtual ~BaseDictDecoder() override = default;

    // Set the data to be decoded
    virtual void set_data(Slice* data) override {
        _data = data;
        _offset = 0;
        uint8_t bit_width = *data->data;
        _index_batch_decoder.reset(
                new RleBatchDecoder<uint32_t>(reinterpret_cast<uint8_t*>(data->data) + 1,
                                              static_cast<int>(data->size) - 1, bit_width));
    }

protected:
    /**
     * Decode dictionary-coded values into doris_column, ensure that doris_column is ColumnDictI32 type,
     * and the coded values must be read into _indexes previously.
     */
    template <bool has_filter>
    Status _decode_dict_values(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector,
                               bool is_dict_filter) {
        DCHECK(doris_column->is_column_dictionary() || is_dict_filter);
        size_t dict_index = 0;
        ColumnSelectVector::DataReadType read_type;
        PaddedPODArray<Int32>& column_data =
                doris_column->is_column_dictionary()
                        ? assert_cast<ColumnDictI32&>(*doris_column).get_data()
                        : assert_cast<ColumnInt32&>(*doris_column).get_data();
        while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
            switch (read_type) {
            case ColumnSelectVector::CONTENT: {
                uint32_t* start_index = &_indexes[0];
                column_data.insert(start_index + dict_index, start_index + dict_index + run_length);
                dict_index += run_length;
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
                break;
            }
            }
        }
        return Status::OK();
    }

    Status skip_values(size_t num_values) override {
        _indexes.resize(num_values);
        _index_batch_decoder->GetBatch(&_indexes[0], num_values);
        return Status::OK();
    }

    // For dictionary encoding
    std::unique_ptr<uint8_t[]> _dict = nullptr;
    std::unique_ptr<RleBatchDecoder<uint32_t>> _index_batch_decoder = nullptr;
    std::vector<uint32_t> _indexes;
};

} // namespace doris::vectorized
