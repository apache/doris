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

#include <orc/OrcFile.hh>

#include "common/config.h"
#include "exec/olap_common.h"
#include "io/file_reader.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/exec/format/format_common.h"
#include "vec/exec/format/generic_reader.h"

namespace doris::vectorized {

class ORCFileInputStream : public orc::InputStream {
public:
    struct Statistics {
        int64_t read_time = 0;
        int64_t read_calls = 0;
        int64_t read_bytes = 0;
    };

    ORCFileInputStream(const std::string& file_name, FileReader* file_reader)
            : _file_name(file_name), _file_reader(file_reader) {};

    ~ORCFileInputStream() override {
        if (_file_reader != nullptr) {
            _file_reader->close();
            delete _file_reader;
        }
    }

    uint64_t getLength() const override { return _file_reader->size(); }

    uint64_t getNaturalReadSize() const override { return config::orc_natural_read_size_mb << 20; }

    void read(void* buf, uint64_t length, uint64_t offset) override;

    const std::string& getName() const override { return _file_name; }

    Statistics& statistics() { return _statistics; }

private:
    Statistics _statistics;
    const std::string& _file_name;
    FileReader* _file_reader;
};

class OrcReader : public GenericReader {
public:
    struct Statistics {
        int64_t column_read_time = 0;
        int64_t get_batch_time = 0;
        int64_t parse_meta_time = 0;
        int64_t decode_value_time = 0;
        int64_t decode_null_map_time = 0;
    };

    OrcReader(RuntimeProfile* profile, const TFileScanRangeParams& params,
              const TFileRangeDesc& range, const std::vector<std::string>& column_names,
              size_t batch_size, const std::string& ctz);

    ~OrcReader() override;
    // for test
    void set_file_reader(const std::string& file_name, FileReader* file_reader) {
        _file_reader = new ORCFileInputStream(file_name, file_reader);
    }

    Status init_reader(
            std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range);

    Status get_next_block(Block* block, size_t* read_rows, bool* eof) override;

    void close();

    int64_t size() const { return _file_reader->getLength(); }

    std::unordered_map<std::string, TypeDescriptor> get_name_to_type() override;
    Status get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                       std::unordered_set<std::string>* missing_cols) override;

private:
    struct OrcProfile {
        RuntimeProfile::Counter* read_time;
        RuntimeProfile::Counter* read_calls;
        RuntimeProfile::Counter* read_bytes;
        RuntimeProfile::Counter* column_read_time;
        RuntimeProfile::Counter* get_batch_time;
        RuntimeProfile::Counter* parse_meta_time;
        RuntimeProfile::Counter* decode_value_time;
        RuntimeProfile::Counter* decode_null_map_time;
    };
    void _init_profile();
    Status _init_read_columns();
    TypeDescriptor _convert_to_doris_type(const orc::Type* orc_type);
    void _init_search_argument(
            std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range);
    void _init_bloom_filter(
            std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range);
    Status _orc_column_to_doris_column(const std::string& col_name, const ColumnPtr& doris_column,
                                       const DataTypePtr& data_type,
                                       const orc::Type* orc_column_type,
                                       orc::ColumnVectorBatch* cvb, size_t num_values);

    template <typename CppType, typename OrcColumnType>
    Status _decode_flat_column(const std::string& col_name, const MutableColumnPtr& data_column,
                               orc::ColumnVectorBatch* cvb, size_t num_values) {
        SCOPED_RAW_TIMER(&_statistics.decode_value_time);
        OrcColumnType* data = dynamic_cast<OrcColumnType*>(cvb);
        if (data == nullptr) {
            return Status::InternalError("Wrong data type for colum '{}'", col_name);
        }
        auto* cvb_data = data->data.data();
        auto& column_data = static_cast<ColumnVector<CppType>&>(*data_column).get_data();
        auto origin_size = column_data.size();
        column_data.resize(origin_size + num_values);
        for (int i = 0; i < num_values; ++i) {
            column_data[origin_size + i] = (CppType)cvb_data[i];
        }
        return Status::OK();
    }

    template <typename DecimalPrimitiveType>
    void _init_decimal_converter(const DataTypePtr& data_type, DecimalScaleParams& scale_params,
                                 const int32_t orc_decimal_scale) {
        if (scale_params.scale_type != DecimalScaleParams::NOT_INIT) {
            return;
        }
        auto* decimal_type = reinterpret_cast<DataTypeDecimal<Decimal<DecimalPrimitiveType>>*>(
                const_cast<IDataType*>(remove_nullable(data_type).get()));
        auto dest_scale = decimal_type->get_scale();
        if (dest_scale > orc_decimal_scale) {
            scale_params.scale_type = DecimalScaleParams::SCALE_UP;
            scale_params.scale_factor = DecimalScaleParams::get_scale_factor<DecimalPrimitiveType>(
                    dest_scale - orc_decimal_scale);
        } else if (dest_scale < orc_decimal_scale) {
            scale_params.scale_type = DecimalScaleParams::SCALE_DOWN;
            scale_params.scale_factor = DecimalScaleParams::get_scale_factor<DecimalPrimitiveType>(
                    orc_decimal_scale - dest_scale);
        } else {
            scale_params.scale_type = DecimalScaleParams::NO_SCALE;
            scale_params.scale_factor = 1;
        }
    }

    template <typename DecimalPrimitiveType, typename OrcColumnType>
    Status _decode_explicit_decimal_column(const std::string& col_name,
                                           const MutableColumnPtr& data_column,
                                           const DataTypePtr& data_type,
                                           DecimalScaleParams& scale_params,
                                           orc::ColumnVectorBatch* cvb, size_t num_values) {
        OrcColumnType* data = dynamic_cast<OrcColumnType*>(cvb);
        if (data == nullptr) {
            return Status::InternalError("Wrong data type for colum '{}'", col_name);
        }
        _init_decimal_converter<DecimalPrimitiveType>(data_type, scale_params, data->scale);

        auto* cvb_data = data->values.data();
        auto& column_data =
                static_cast<ColumnVector<DecimalPrimitiveType>&>(*data_column).get_data();
        auto origin_size = column_data.size();
        column_data.resize(origin_size + num_values);

        for (int i = 0; i < num_values; ++i) {
            int128_t value;
            if constexpr (std::is_same_v<OrcColumnType, orc::Decimal64VectorBatch>) {
                value = static_cast<int128_t>(cvb_data[i]);
            } else {
                uint64_t hi = data->values[i].getHighBits();
                uint64_t lo = data->values[i].getLowBits();
                value = (((int128_t)hi) << 64) | (int128_t)lo;
            }
            if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {
                value *= scale_params.scale_factor;
            } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {
                value /= scale_params.scale_factor;
            }
            auto& v = reinterpret_cast<DecimalPrimitiveType&>(column_data[origin_size + i]);
            v = (DecimalPrimitiveType)value;
        }
        return Status::OK();
    }

    template <typename DecimalPrimitiveType>
    Status _decode_decimal_column(const std::string& col_name, const MutableColumnPtr& data_column,
                                  const DataTypePtr& data_type, DecimalScaleParams& scale_params,
                                  orc::ColumnVectorBatch* cvb, size_t num_values) {
        SCOPED_RAW_TIMER(&_statistics.decode_value_time);
        if (dynamic_cast<orc::Decimal64VectorBatch*>(cvb) != nullptr) {
            return _decode_explicit_decimal_column<DecimalPrimitiveType, orc::Decimal64VectorBatch>(
                    col_name, data_column, data_type, scale_params, cvb, num_values);
        } else {
            return _decode_explicit_decimal_column<DecimalPrimitiveType,
                                                   orc::Decimal128VectorBatch>(
                    col_name, data_column, data_type, scale_params, cvb, num_values);
        }
    }

    template <typename CppType, typename DorisColumnType, typename OrcColumnType>
    Status _decode_time_column(const std::string& col_name, const MutableColumnPtr& data_column,
                               orc::ColumnVectorBatch* cvb, size_t num_values) {
        SCOPED_RAW_TIMER(&_statistics.decode_value_time);
        auto* data = dynamic_cast<OrcColumnType*>(cvb);
        if (data == nullptr) {
            return Status::InternalError("Wrong data type for colum '{}'", col_name);
        }
        auto& column_data = static_cast<ColumnVector<DorisColumnType>&>(*data_column).get_data();
        auto origin_size = column_data.size();
        column_data.resize(origin_size + num_values);
        for (int i = 0; i < num_values; ++i) {
            auto& v = reinterpret_cast<CppType&>(column_data[origin_size + i]);
            if constexpr (std::is_same_v<OrcColumnType, orc::LongVectorBatch>) { // date
                int64_t& date_value = data->data[i];
                v.from_unixtime(date_value * 24 * 60 * 60, _time_zone); // day to seconds
                if constexpr (std::is_same_v<CppType, VecDateTimeValue>) {
                    // we should cast to date if using date v1.
                    v.cast_to_date();
                }
            } else { // timestamp
                v.from_unixtime(data->data[i], _time_zone);
                if constexpr (std::is_same_v<CppType, DateV2Value<DateTimeV2ValueType>>) {
                    // nanoseconds will lose precision. only keep microseconds.
                    v.set_microsecond(data->nanoseconds[i] / 1000);
                }
            }
        }
        return Status::OK();
    }

    Status _decode_string_column(const std::string& col_name, const MutableColumnPtr& data_column,
                                 const orc::TypeKind& type_kind, orc::ColumnVectorBatch* cvb,
                                 size_t num_values);

    Status _fill_doris_array_offsets(const std::string& col_name,
                                     const MutableColumnPtr& data_column, orc::ListVectorBatch* lvb,
                                     size_t num_values, size_t* element_size);

    RuntimeProfile* _profile;
    const TFileScanRangeParams& _scan_params;
    const TFileRangeDesc& _scan_range;
    size_t _batch_size;
    int64_t _range_start_offset;
    int64_t _range_size;
    const std::string& _ctz;
    const std::vector<std::string>& _column_names;
    cctz::time_zone _time_zone;

    std::list<std::string> _read_cols;
    std::list<std::string> _missing_cols;
    std::unordered_map<std::string, int> _colname_to_idx;
    std::vector<const orc::Type*> _col_orc_type;
    ORCFileInputStream* _file_reader = nullptr;
    Statistics _statistics;
    OrcProfile _orc_profile;
    bool _closed = false;

    std::unique_ptr<orc::ColumnVectorBatch> _batch;
    std::unique_ptr<orc::Reader> _reader;
    std::unique_ptr<orc::RowReader> _row_reader;
    orc::ReaderOptions _reader_options;
    orc::RowReaderOptions _row_reader_options;

    // only for decimal
    DecimalScaleParams _decimal_scale_params;
};

} // namespace doris::vectorized
