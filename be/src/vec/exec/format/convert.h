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

#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/parquet_types.h>
#include <glog/logging.h>

#include <algorithm>
#include <functional>
#include <ostream>
#include <utility>

#include "common/status.h"
#include "io/file_factory.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exec/format/parquet/parquet_common.h"
#include "gen_cpp/descriptors.pb.h"
#include "olap/olap_common.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

namespace convert {

class DocTime {

public:
    std::unique_ptr<DecodeParams> _decode_params;
    const FieldSchema * _field_schema;
    void init_time(const FieldSchema *field_schema, cctz::time_zone* ctz) {
        if (_decode_params == nullptr) {
            _decode_params.reset(new DecodeParams());
        }
        if (ctz != nullptr) {
            _decode_params->ctz = ctz;
        }

        _field_schema =  field_schema;
        const auto& schema = field_schema->parquet_schema;
        if (schema.__isset.logicalType && schema.logicalType.__isset.TIMESTAMP) {
            const auto& timestamp_info = schema.logicalType.TIMESTAMP;
            if (!timestamp_info.isAdjustedToUTC) {
                // should set timezone to utc+0
                _decode_params->ctz = const_cast<cctz::time_zone*>(&_decode_params->utc0);
            }
            const auto& time_unit = timestamp_info.unit;
            if (time_unit.__isset.MILLIS) {
                _decode_params->second_mask = 1000;
                _decode_params->scale_to_nano_factor = 1000000;
            } else if (time_unit.__isset.MICROS) {
                _decode_params->second_mask = 1000000;
                _decode_params->scale_to_nano_factor = 1000;
            } else if (time_unit.__isset.NANOS) {
                _decode_params->second_mask = 1000000000;
                _decode_params->scale_to_nano_factor = 1;
            }
        } else if (schema.__isset.converted_type) {
            const auto& converted_type = schema.converted_type;
            if (converted_type == tparquet::ConvertedType::TIMESTAMP_MILLIS) {
                _decode_params->second_mask = 1000;
                _decode_params->scale_to_nano_factor = 1000000;
            } else if (converted_type == tparquet::ConvertedType::TIMESTAMP_MICROS) {
                _decode_params->second_mask = 1000000;
                _decode_params->scale_to_nano_factor = 1000;
            }
        }

        if (_decode_params->ctz) {
            VecDateTimeValue t;
            t.from_unixtime(0, *_decode_params->ctz);
            _decode_params->offset_days = t.day() == 31 ? 0 : 1;
        }
    }
    template<typename DecimalType>
    void init_decimal_converter(DataTypePtr& data_type) {
        if (_decode_params == nullptr || _field_schema == nullptr ||
            _decode_params->decimal_scale.scale_type != DecimalScaleParams::NOT_INIT) {
            return;
        }
        auto scale = _field_schema->parquet_schema.scale;
        auto* decimal_type = reinterpret_cast<DataTypeDecimal<DecimalType>*>(
                const_cast<IDataType*>(remove_nullable(data_type).get()));
        auto dest_scale = decimal_type->get_scale();
        if (dest_scale > scale) {
            _decode_params->decimal_scale.scale_type = DecimalScaleParams::SCALE_UP;
            _decode_params->decimal_scale.scale_factor =
                    DecimalScaleParams::get_scale_factor<DecimalType>(dest_scale - scale);
        } else if (dest_scale < scale) {
            _decode_params->decimal_scale.scale_type = DecimalScaleParams::SCALE_DOWN;
            _decode_params->decimal_scale.scale_factor =
                    DecimalScaleParams::get_scale_factor<DecimalType>(scale - dest_scale);
        } else {
            _decode_params->decimal_scale.scale_type = DecimalScaleParams::NO_SCALE;
            _decode_params->decimal_scale.scale_factor = 1;
        }
    }

};

static Status
convert_data_type_from_parquet(tparquet::Type::type parquet_type ,
                           vectorized::DataTypePtr& ans_data_type ,
                           DataTypePtr& src_type,bool * need_convert) {
    std::cout << getTypeName(src_type->get_type_id()) <<"\n";
    if (is_complex_type(src_type)){
        *need_convert = false;
        return Status::OK();
    }
    switch (parquet_type) {
        case tparquet::Type::type::BOOLEAN:
            ans_data_type = std::make_shared<DataTypeUInt8>();
            break;
        case tparquet::Type::type::INT32:
            ans_data_type = std::make_shared<DataTypeInt32>();
            break;
        case tparquet::Type::type::INT64:
            ans_data_type = std::make_shared<DataTypeInt64>();
            break;
        case tparquet::Type::type::FLOAT:
            ans_data_type = std::make_shared<DataTypeFloat32>();
            break;
        case tparquet::Type::type::DOUBLE:
            ans_data_type = std::make_shared<DataTypeFloat64>();
            break;
        case tparquet::Type::type::BYTE_ARRAY:
        case tparquet::Type::type::FIXED_LEN_BYTE_ARRAY:
            ans_data_type = std::make_shared<DataTypeString>();
            break;
        case tparquet::Type::type::INT96:
            ans_data_type = std::make_shared<DataTypeInt128>();
            break;
        default:
            std::cout <<"--->"<<parquet_type <<"\n";
            break;
    }
    if (ans_data_type->get_type_id() == src_type->get_type_id()){
        *need_convert = false;
        return Status::OK();
    }
    if (src_type->is_nullable()){
        auto& nested_src_type= reinterpret_cast<const DataTypeNullable*>(src_type.get())->get_nested_type();
        std::cout << getTypeName(nested_src_type->get_type_id()) <<"\n";
        auto sub = ans_data_type;
        ans_data_type = std::make_shared<DataTypeNullable>(ans_data_type);

        if (nested_src_type->get_type_id() == sub ->get_type_id()){
            *need_convert = false;
            return Status::OK();
        }
    }

    *need_convert = true;
    return Status::OK();
}




struct ColumnConvert {
    Status virtual convert(const IColumn* src_col , IColumn* dst_col ){
        return Status::OK();
    }
    virtual ~ColumnConvert() = default;
};

template<typename src_type,typename dst_type,bool is_nullable>
struct NumberColumnConvert : public ColumnConvert {
    virtual Status  convert(const IColumn* src_col , IColumn* dst_col ) override;

};
void
convert_null(const IColumn **src_col, IColumn **dst_col){
    size_t rows = (*src_col)->size();
    if ((*src_col)->is_nullable()) {
        auto src_nullable_column = reinterpret_cast<const ColumnNullable *>(*src_col);
        auto dst_nullable_column = reinterpret_cast<ColumnNullable *>(*dst_col);
        auto& dst_null_col = dst_nullable_column->get_null_map_column();

        for(auto j =0;j<rows;j++) {
            dst_null_col.insert(src_nullable_column->get_null_map_column()[j]);
        }

        *src_col = &src_nullable_column->get_nested_column();
        *dst_col = &dst_nullable_column->get_nested_column();
    }
}



template<typename src_type,typename dst_type,bool is_nullable>
Status NumberColumnConvert<src_type,dst_type,is_nullable>::convert(const IColumn *src_col, IColumn *dst_col) {
    size_t rows = src_col->size();
    if constexpr (is_nullable){
        convert_null(&src_col,&dst_col);
    }


    for(int i =0;i<rows;i++) {
//        src_type src_value =  reinterpret_cast<const ColumnVector<src_type>*>(src_col)->get_data()[i];

        dst_type value = static_cast<dst_type>(
                reinterpret_cast<const ColumnVector<src_type>*>(src_col)->get_data()[i]);

        reinterpret_cast<ColumnVector<dst_type>*>(dst_col)->insert(value);

    }


    return Status::OK();
}
template<typename src_type,bool is_nullable>
struct NumberColumnToStringConvert : public ColumnConvert {
    virtual Status  convert(const IColumn* src_col , IColumn* dst_col ) override;
};

template<typename src_type,bool is_nullable>
Status NumberColumnToStringConvert<src_type,is_nullable>::convert(const IColumn *src_col, IColumn *dst_col) {
    size_t rows = src_col->size();
    if constexpr (is_nullable){
        convert_null(&src_col,&dst_col);
    }

    for(int i =0;i<rows;i++) {
        std::string value = std::to_string(reinterpret_cast<const ColumnVector<src_type>*>(src_col)->get_data()[i]);
        reinterpret_cast<ColumnString*>(dst_col)->insert_data(value.data(),value.size());

    }
    return Status::OK();
}

template<bool is_nullable>
struct int128totimestamp: public ColumnConvert {
    int128totimestamp(DocTime *pTime) {
        doc = pTime;
    }

    inline uint64_t to_timestamp_micros(uint32_t hi , uint64_t lo) const {
        return (hi - ParquetInt96::JULIAN_EPOCH_OFFSET_DAYS) * ParquetInt96::MICROS_IN_DAY + lo / ParquetInt96::NANOS_PER_MICROSECOND;
    }
     Status  convert(const IColumn* src_col , IColumn* dst_col ) {
        size_t rows = src_col->size();
        if constexpr (is_nullable){
            convert_null(&src_col,&dst_col);
        }

        for(int i =0;i<rows;i++) {
            __int128 x =  reinterpret_cast<const ColumnVector<Int128>*>(src_col)->get_data()[i];
            uint32_t hi = x>>64; uint64_t lo = (x<<64)>>64 ;
            dst_col = static_cast<ColumnVector<UInt64>*>(dst_col);
            reinterpret_cast<ColumnVector<UInt64>*>(dst_col)->insert(0);
            auto& num = static_cast<ColumnVector<UInt64>*>(dst_col)->get_data()[i];
            auto &value = reinterpret_cast<DateV2Value<DateTimeV2ValueType>&>(num);
            int64_t micros = to_timestamp_micros(hi,lo);
            value.from_unixtime(micros / 1000000, *doc->_decode_params->ctz);
            value.set_microsecond(micros % 1000000);
            std::cout << "value = " << value <<"\n";
        }
        return Status::OK();
    }
    DocTime *doc;
};

template<bool is_nullable>
struct int64totimestamp: public ColumnConvert {

public:
    int64totimestamp(DocTime *pTime) {
        doc = pTime;
    }

    Status convert(const IColumn *src_col, IColumn *dst_col) {
        size_t rows = src_col->size();
        if constexpr (is_nullable) {
            convert_null(&src_col, &dst_col);
        }
        dst_col->resize(rows);
        for (int i = 0; i < rows; i++) {
            int64 x = reinterpret_cast<const ColumnVector<Int64> *>(src_col)->get_data()[i];
            dst_col = static_cast<ColumnVector<UInt64> *>(dst_col);
//            reinterpret_cast<ColumnVector<UInt64>*>(dst_col)->insert(0);
            auto &num = static_cast<ColumnVector<UInt64> *>(dst_col)->get_data()[i];
            auto &value = reinterpret_cast<DateV2Value<DateTimeV2ValueType> &>(num);
            value.from_unixtime(x / doc->_decode_params->second_mask, *doc->_decode_params->ctz);
            value.set_microsecond((x % doc->_decode_params->second_mask) *
                                  doc->_decode_params->scale_to_nano_factor / 1000);
            std::cout << "value = " << value << "\n";
        }
        return Status::OK();
    }

    DocTime *doc;
};


template<bool is_nullable>
class int32todate : public ColumnConvert {
public:
    DocTime *doc;
    int32todate(DocTime *pTime) {
        doc = pTime;
    }
    Status  convert(const IColumn* src_col , IColumn* dst_col ) {
        size_t rows = src_col->size();
        if constexpr (is_nullable){
            convert_null(&src_col,&dst_col);
        }
        dst_col->resize(rows);
        for(int i = 0;i< rows;i++) {

//            auto& value = reinterpret_cast<const ColumnVector<uint32>*>(src_col)->get_data()[i];
//            reinterpret_cast<DateV2Value<DateV2ValueType>>();
            auto& value = reinterpret_cast<DateV2Value <DateV2ValueType> &>(reinterpret_cast<ColumnDateV2 *>(dst_col)->get_data()[i]);
//            value = reinterpret_cast<const ColumnVector<uint32>*>(src_col)->get_data()[i];
            int64_t date_value = reinterpret_cast<const ColumnVector<uint32>*>(src_col)->get_data()[i] + doc ->_decode_params->offset_days;
            date_day_offset_dict& date_dict = date_day_offset_dict::get();
            value = date_dict[date_value];
        }

        return Status::OK();
    }
};

template< typename DecimalType , bool is_nullable>
class stringtodecimal : public ColumnConvert {
public:
    DocTime *doc;
    stringtodecimal(DocTime *pTime) {
        doc = pTime;
    }
    Status  convert(const IColumn* src_col , IColumn* dst_col ) {
        size_t rows = src_col->size();
        if constexpr (is_nullable){
            convert_null(&src_col,&dst_col);
        }
        DecimalScaleParams& scale_params = doc->_decode_params->decimal_scale;
        auto buf = static_cast<const ColumnString*>(src_col)->get_chars().data();
        auto& offset = static_cast<const ColumnString*>(src_col)->get_offsets();
        dst_col->resize(rows);
        auto& data = static_cast<ColumnDecimal<DecimalType>*>(dst_col)->get_data();
        for(int i = 0;i< rows;i++) {
            int len = offset[i] - offset[i-1];
            Int128 value = buf[offset[i-1]] & 0x80 ? -1 : 0;
            memcpy(reinterpret_cast<char*>(&value) + sizeof(Int128) - len, buf+offset[i-1],
                   len);
            value = BigEndian::ToHost128(value);
            if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {
                value *= scale_params.scale_factor;
            } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {
                value /= scale_params.scale_factor;
            }
            auto& v = reinterpret_cast<DecimalType&>(data[i]);
            v = (DecimalType)value;
        }

        return Status::OK();
    }
};
template< typename NumberType, typename DecimalPhysicalType, bool is_nullable>
class numbertodecimal: public ColumnConvert {

    DocTime *doc;
public:
    Status  convert(const IColumn* src_col , IColumn* dst_col ) {
        size_t rows = src_col->size();
        if constexpr (is_nullable){
            convert_null(&src_col,&dst_col);
        }
        auto* src_data = static_cast<const ColumnVector<NumberType>*>(src_col)->get_data().data();
        dst_col->resize(rows);
        DecimalScaleParams& scale_params = doc->_decode_params->decimal_scale;
        auto* data = static_cast< ColumnDecimal<Decimal<Int64>>*>(dst_col)->get_data().data();

        for(int i = 0;i < rows;i++) {
            Int128 value = src_data[i];
            if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {
                value *= scale_params.scale_factor;
            } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {
                value /= scale_params.scale_factor;
            }
            data[i] = (DecimalPhysicalType)value;
        }
        return Status::OK();
    }

public:
    numbertodecimal(DocTime *pTime) {
        doc = pTime;
    }
};
/*
 *              Int128 value = *reinterpret_cast<DecimalPhysicalType*>(buf_start);
                if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {
                    value *= scale_params.scale_factor;
                } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {
                    value /= scale_params.scale_factor;
                }
                auto& v = reinterpret_cast<DecimalPrimitiveType&>(column_data[data_index++]);
                v = (DecimalPrimitiveType)value;
 *
 *
 */

template<bool is_nullable = true>
static
Status get_converter_impl(std::shared_ptr<const IDataType> src_data_type , std::shared_ptr<const IDataType> dst_data_type,
                          std::unique_ptr<ColumnConvert> * converter,
                          DocTime&  doc[[maybe_unused]]  ){
    auto src_type = src_data_type -> get_type_id();
    auto dst_type = dst_data_type -> get_type_id();

    switch (dst_type) {
#define DISPATCH(NUMERIC_TYPE, CPP_NUMERIC_TYPE, PHYSICAL_TYPE)                                                 \
    case NUMERIC_TYPE:                                                                                          \
        switch(src_type){                                                                                       \
            case TypeIndex::UInt8:                                                                              \
                *converter = std::make_unique<NumberColumnConvert<UInt8 ,CPP_NUMERIC_TYPE,is_nullable>>();      \
                break;                                                                                          \
            case TypeIndex::Int32:                                                                              \
                *converter = std::make_unique<NumberColumnConvert<Int32 ,CPP_NUMERIC_TYPE,is_nullable>>();      \
                break;                                                                                          \
            case TypeIndex::Int64:                                                                              \
                *converter = std::make_unique<NumberColumnConvert<Int64 ,CPP_NUMERIC_TYPE,is_nullable>>();      \
                break;                                                                                          \
            case TypeIndex::Float32:                                                                            \
                *converter = std::make_unique<NumberColumnConvert<Float32 ,CPP_NUMERIC_TYPE,is_nullable>>();    \
                break;                                                                                          \
            case TypeIndex::Float64:                                                                            \
                *converter = std::make_unique<NumberColumnConvert<Float64 ,CPP_NUMERIC_TYPE,is_nullable>>();    \
                break;                                                                                          \
            case TypeIndex::Int128:                                                                             \
                *converter = std::make_unique<NumberColumnConvert<Int128 ,CPP_NUMERIC_TYPE,is_nullable>>();     \
                break;                                                                                          \
            default:                                                                                            \
                break;                                                                                          \
                }                                                                                               \
            break;
    FOR_LOGICAL_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    case TypeIndex::String:
        switch (src_type) {
#define DISPATCH1(NUMERIC_TYPE, CPP_NUMERIC_TYPE, PHYSICAL_TYPE)                                                \
        case NUMERIC_TYPE:                                                                                      \
            *converter = std::make_unique<NumberColumnToStringConvert<CPP_NUMERIC_TYPE,is_nullable>>();         \
            break;
            FOR_LOGICAL_NUMERIC_TYPES(DISPATCH1)
#undef DISPATCH1
        default:
            break;
        }
        break;
    case TypeIndex::DateV2:
        if (src_type == TypeIndex::Int32) {
            *converter = std::make_unique<int32todate<is_nullable>>(&doc);
        }
        break;
    case TypeIndex::DateTimeV2:
        if (src_type == TypeIndex::Int128) {
            *converter = std::make_unique<int128totimestamp<is_nullable>>(&doc);
        }else if (src_type == TypeIndex::Int64) {
            *converter = std::make_unique<int64totimestamp<is_nullable>>(&doc);
        }
        break;
    case TypeIndex::Decimal64:
        if (src_type == TypeIndex::Int128) {
            *converter = std::make_unique<numbertodecimal< Int128 , Int64 ,is_nullable>>(&doc);
        } else if (src_type == TypeIndex::String) {
            doc.init_decimal_converter<Decimal64>(dst_data_type);
            *converter = std::make_unique<stringtodecimal<Decimal64,is_nullable>>(&doc);
        } else if (src_type == TypeIndex::Int32) {
            *converter = std::make_unique<numbertodecimal< Int32 , Int64 ,is_nullable>>(&doc);
        } else if (src_type == TypeIndex::Int64) {
            *converter = std::make_unique<numbertodecimal< Int64 , Int64 ,is_nullable>>(&doc);
        }
        break;
    default:
        break;
    }

    if (converter->get() == nullptr){
        return Status::NotSupported("Can't cast type {} to type {}",
        getTypeName(src_type ), getTypeName(dst_type));
    }

    return Status::OK();
}

static
Status get_converter(std::shared_ptr<const IDataType> src_type , std::shared_ptr<const IDataType> dst_type ,
                     std::unique_ptr<ColumnConvert> * converter ,DocTime&doc){

    if (src_type->is_nullable()){

        auto src = reinterpret_cast<const DataTypeNullable*> (src_type.get())->get_nested_type();
        auto dst = reinterpret_cast<const DataTypeNullable*> (dst_type.get())->get_nested_type();

        return get_converter_impl<true>(src,dst,converter,doc);
    }else {
        return get_converter_impl<false>(src_type, dst_type,converter,doc);
    }
    return Status::OK();
}
};


}