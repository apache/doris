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

#include "vorc_reader.h"

#include <tuple>

#include "cctz/civil_time.h"
#include "cctz/time_zone.h"
#include "gutil/strings/substitute.h"
#include "io/fs/file_reader.h"
#include "olap/iterators.h"
#include "util/slice.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_struct.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_struct.h"

namespace doris::vectorized {

#define FOR_FLAT_ORC_COLUMNS(M)                            \
    M(TypeIndex::Int8, Int8, orc::LongVectorBatch)         \
    M(TypeIndex::UInt8, UInt8, orc::LongVectorBatch)       \
    M(TypeIndex::Int16, Int16, orc::LongVectorBatch)       \
    M(TypeIndex::UInt16, UInt16, orc::LongVectorBatch)     \
    M(TypeIndex::Int32, Int32, orc::LongVectorBatch)       \
    M(TypeIndex::UInt32, UInt32, orc::LongVectorBatch)     \
    M(TypeIndex::Int64, Int64, orc::LongVectorBatch)       \
    M(TypeIndex::UInt64, UInt64, orc::LongVectorBatch)     \
    M(TypeIndex::Float32, Float32, orc::DoubleVectorBatch) \
    M(TypeIndex::Float64, Float64, orc::DoubleVectorBatch)

void ORCFileInputStream::read(void* buf, uint64_t length, uint64_t offset) {
    _statistics->fs_read_calls++;
    _statistics->fs_read_bytes += length;
    SCOPED_RAW_TIMER(&_statistics->fs_read_time);
    uint64_t has_read = 0;
    char* out = reinterpret_cast<char*>(buf);
    while (has_read < length) {
        size_t loop_read;
        Slice result(out + has_read, length - has_read);
        Status st = _file_reader->read_at(offset + has_read, result, &loop_read, _io_ctx);
        if (!st.ok()) {
            throw orc::ParseError(
                    strings::Substitute("Failed to read $0: $1", _file_name, st.to_string()));
        }
        if (loop_read == 0) {
            break;
        }
        has_read += loop_read;
    }
    if (has_read != length) {
        throw orc::ParseError(strings::Substitute("Try to read $0 bytes from $1, actually read $2",
                                                  length, has_read, _file_name));
    }
}

OrcReader::OrcReader(RuntimeProfile* profile, RuntimeState* state,
                     const TFileScanRangeParams& params, const TFileRangeDesc& range,
                     const std::vector<std::string>& column_names, size_t batch_size,
                     const std::string& ctz, io::IOContext* io_ctx)
        : _profile(profile),
          _state(state),
          _scan_params(params),
          _scan_range(range),
          _batch_size(std::max(batch_size, _MIN_BATCH_SIZE)),
          _range_start_offset(range.start_offset),
          _range_size(range.size),
          _ctz(ctz),
          _column_names(column_names),
          _is_hive(params.__isset.slot_name_to_schema_pos),
          _io_ctx(io_ctx) {
    TimezoneUtils::find_cctz_time_zone(ctz, _time_zone);
    _init_profile();
    _init_system_properties();
    _init_file_description();
}

OrcReader::OrcReader(const TFileScanRangeParams& params, const TFileRangeDesc& range,
                     const std::vector<std::string>& column_names, const std::string& ctz,
                     io::IOContext* io_ctx)
        : _profile(nullptr),
          _scan_params(params),
          _scan_range(range),
          _ctz(ctz),
          _column_names(column_names),
          _is_hive(params.__isset.slot_name_to_schema_pos),
          _file_system(nullptr),
          _io_ctx(io_ctx) {
    _init_system_properties();
    _init_file_description();
}

OrcReader::~OrcReader() {
    close();
}

void OrcReader::close() {
    if (!_closed) {
        _collect_profile_on_close();
        _closed = true;
    }
}

void OrcReader::_collect_profile_on_close() {
    if (_profile != nullptr) {
        COUNTER_UPDATE(_orc_profile.read_time, _statistics.fs_read_time);
        COUNTER_UPDATE(_orc_profile.read_calls, _statistics.fs_read_calls);
        COUNTER_UPDATE(_orc_profile.read_bytes, _statistics.fs_read_bytes);
        COUNTER_UPDATE(_orc_profile.column_read_time, _statistics.column_read_time);
        COUNTER_UPDATE(_orc_profile.get_batch_time, _statistics.get_batch_time);
        COUNTER_UPDATE(_orc_profile.parse_meta_time, _statistics.parse_meta_time);
        COUNTER_UPDATE(_orc_profile.decode_value_time, _statistics.decode_value_time);
        COUNTER_UPDATE(_orc_profile.decode_null_map_time, _statistics.decode_null_map_time);
    }
}

int64_t OrcReader::size() const {
    return _file_input_stream->getLength();
}

void OrcReader::_init_profile() {
    if (_profile != nullptr) {
        static const char* orc_profile = "OrcReader";
        ADD_TIMER(_profile, orc_profile);
        _orc_profile.read_time = ADD_TIMER(_profile, "FileReadTime");
        _orc_profile.read_calls = ADD_COUNTER(_profile, "FileReadCalls", TUnit::UNIT);
        _orc_profile.read_bytes = ADD_COUNTER(_profile, "FileReadBytes", TUnit::BYTES);
        _orc_profile.column_read_time = ADD_CHILD_TIMER(_profile, "ColumnReadTime", orc_profile);
        _orc_profile.get_batch_time = ADD_CHILD_TIMER(_profile, "GetBatchTime", orc_profile);
        _orc_profile.parse_meta_time = ADD_CHILD_TIMER(_profile, "ParseMetaTime", orc_profile);
        _orc_profile.decode_value_time = ADD_CHILD_TIMER(_profile, "DecodeValueTime", orc_profile);
        _orc_profile.decode_null_map_time =
                ADD_CHILD_TIMER(_profile, "DecodeNullMapTime", orc_profile);
    }
}

Status OrcReader::_create_file_reader() {
    if (_file_input_stream == nullptr) {
        io::FileReaderSPtr inner_reader;
        io::FileCachePolicy cache_policy = FileFactory::get_cache_policy(_state);
        RETURN_IF_ERROR(FileFactory::create_file_reader(_profile, _system_properties,
                                                        _file_description, &_file_system,
                                                        &inner_reader, cache_policy));
        _file_input_stream.reset(
                new ORCFileInputStream(_scan_range.path, inner_reader, &_statistics, _io_ctx));
    }
    if (_file_input_stream->getLength() == 0) {
        return Status::EndOfFile("empty orc file: " + _scan_range.path);
    }
    // create orc reader
    try {
        orc::ReaderOptions options;
        _reader = orc::createReader(
                std::unique_ptr<ORCFileInputStream>(_file_input_stream.release()), options);
    } catch (std::exception& e) {
        return Status::InternalError("Init OrcReader failed. reason = {}", e.what());
    }
    return Status::OK();
}

Status OrcReader::init_reader(
        std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    SCOPED_RAW_TIMER(&_statistics.parse_meta_time);
    RETURN_IF_ERROR(_create_file_reader());
    // _init_bloom_filter(colname_to_value_range);

    // create orc row reader
    _row_reader_options.range(_range_start_offset, _range_size);
    _row_reader_options.setTimezoneName(_ctz);
    RETURN_IF_ERROR(_init_read_columns());
    _init_search_argument(colname_to_value_range);
    _row_reader_options.include(_read_cols);
    try {
        _row_reader = _reader->createRowReader(_row_reader_options);
        _batch = _row_reader->createRowBatch(_batch_size);
    } catch (std::exception& e) {
        return Status::InternalError("Failed to create orc row reader. reason = {}", e.what());
    }
    auto& selected_type = _row_reader->getSelectedType();
    _col_orc_type.resize(selected_type.getSubtypeCount());
    for (int i = 0; i < selected_type.getSubtypeCount(); ++i) {
        std::string name;
        // For hive engine, translate the column name in orc file to schema column name.
        // This is for Hive 1.x which use internal column name _col0, _col1...
        if (_is_hive) {
            name = _file_col_to_schema_col[selected_type.getFieldName(i)];
        } else {
            name = _get_field_name_lower_case(&selected_type, i);
        }
        _colname_to_idx[name] = i;
        _col_orc_type[i] = selected_type.getSubtype(i);
    }
    return Status::OK();
}

Status OrcReader::get_parsed_schema(std::vector<std::string>* col_names,
                                    std::vector<TypeDescriptor>* col_types) {
    RETURN_IF_ERROR(_create_file_reader());
    auto& root_type = _reader->getType();
    for (int i = 0; i < root_type.getSubtypeCount(); ++i) {
        col_names->emplace_back(_get_field_name_lower_case(&root_type, i));
        col_types->emplace_back(_convert_to_doris_type(root_type.getSubtype(i)));
    }
    return Status::OK();
}

Status OrcReader::_init_read_columns() {
    auto& root_type = _reader->getType();
    std::vector<std::string> orc_cols;
    std::vector<std::string> orc_cols_lower_case;
    for (int i = 0; i < root_type.getSubtypeCount(); ++i) {
        orc_cols.emplace_back(root_type.getFieldName(i));
        orc_cols_lower_case.emplace_back(_get_field_name_lower_case(&root_type, i));
    }
    for (auto& col_name : _column_names) {
        if (_is_hive) {
            auto iter = _scan_params.slot_name_to_schema_pos.find(col_name);
            DCHECK(iter != _scan_params.slot_name_to_schema_pos.end());
            int pos = iter->second;
            orc_cols_lower_case[pos] = iter->first;
        }
        auto iter = std::find(orc_cols_lower_case.begin(), orc_cols_lower_case.end(), col_name);
        if (iter == orc_cols_lower_case.end()) {
            _missing_cols.emplace_back(col_name);
        } else {
            int pos = std::distance(orc_cols_lower_case.begin(), iter);
            _read_cols.emplace_back(orc_cols[pos]);
            _read_cols_lower_case.emplace_back(col_name);
            // For hive engine, store the orc column name to schema column name map.
            // This is for Hive 1.x orc file with internal column name _col0, _col1...
            if (_is_hive) {
                _file_col_to_schema_col[orc_cols[pos]] = col_name;
            }
        }
    }
    return Status::OK();
}

struct OrcPredicate {
    std::string col_name;
    orc::PredicateDataType data_type;
    std::vector<orc::Literal> literals;
    SQLFilterOp op;
};

//  orc only support LONG, FLOAT, STRING, DATE, DECIMAL, TIMESTAMP, BOOLEAN to push down predicates
static std::unordered_map<orc::TypeKind, orc::PredicateDataType> TYPEKIND_TO_PREDICATE_TYPE = {
        {orc::TypeKind::BYTE, orc::PredicateDataType::LONG},
        {orc::TypeKind::SHORT, orc::PredicateDataType::LONG},
        {orc::TypeKind::INT, orc::PredicateDataType::LONG},
        {orc::TypeKind::LONG, orc::PredicateDataType::LONG},
        {orc::TypeKind::FLOAT, orc::PredicateDataType::FLOAT},
        {orc::TypeKind::DOUBLE, orc::PredicateDataType::FLOAT},
        {orc::TypeKind::STRING, orc::PredicateDataType::STRING},
        {orc::TypeKind::BINARY, orc::PredicateDataType::STRING},
        {orc::TypeKind::CHAR, orc::PredicateDataType::STRING},
        {orc::TypeKind::VARCHAR, orc::PredicateDataType::STRING},
        {orc::TypeKind::DATE, orc::PredicateDataType::DATE},
        {orc::TypeKind::DECIMAL, orc::PredicateDataType::DECIMAL},
        {orc::TypeKind::TIMESTAMP, orc::PredicateDataType::TIMESTAMP},
        {orc::TypeKind::BOOLEAN, orc::PredicateDataType::BOOLEAN}};

template <typename CppType>
std::tuple<bool, orc::Literal> convert_to_orc_literal(const orc::Type* type, const void* value,
                                                      int precision, int scale) {
    try {
        switch (type->getKind()) {
        case orc::TypeKind::BOOLEAN:
            return std::make_tuple(true, orc::Literal(bool(*((uint8_t*)value))));
        case orc::TypeKind::BYTE:
            return std::make_tuple(true, orc::Literal(int64_t(*((int8_t*)value))));
        case orc::TypeKind::SHORT:
            return std::make_tuple(true, orc::Literal(int64_t(*((int16_t*)value))));
        case orc::TypeKind::INT:
            return std::make_tuple(true, orc::Literal(int64_t(*((int32_t*)value))));
        case orc::TypeKind::LONG:
            return std::make_tuple(true, orc::Literal(*((int64_t*)value)));
        case orc::TypeKind::FLOAT:
            return std::make_tuple(true, orc::Literal(double(*((float*)value))));
        case orc::TypeKind::DOUBLE:
            return std::make_tuple(true, orc::Literal(*((double*)value)));
        case orc::TypeKind::STRING:
            [[fallthrough]];
        case orc::TypeKind::BINARY:
            [[fallthrough]];
        case orc::TypeKind::CHAR:
            [[fallthrough]];
        case orc::TypeKind::VARCHAR: {
            StringRef* string_value = (StringRef*)value;
            return std::make_tuple(true, orc::Literal(string_value->data, string_value->size));
        }
        case orc::TypeKind::DECIMAL: {
            int128_t decimal_value;
            if constexpr (std::is_same_v<CppType, DecimalV2Value>) {
                decimal_value = *reinterpret_cast<const int128_t*>(value);
                precision = DecimalV2Value::PRECISION;
                scale = DecimalV2Value::SCALE;
            } else if constexpr (std::is_same_v<CppType, int32_t>) {
                decimal_value = *((int32_t*)value);
            } else if constexpr (std::is_same_v<CppType, int64_t>) {
                decimal_value = *((int64_t*)value);
            } else {
                decimal_value = *((int128_t*)value);
            }
            return std::make_tuple(true, orc::Literal(orc::Int128(uint64_t(decimal_value >> 64),
                                                                  uint64_t(decimal_value)),
                                                      precision, scale));
        }
        case orc::TypeKind::DATE: {
            int64_t day_offset;
            static const cctz::time_zone utc0 = cctz::utc_time_zone();
            if constexpr (std::is_same_v<CppType, VecDateTimeValue>) {
                const VecDateTimeValue date_v1 = *reinterpret_cast<const VecDateTimeValue*>(value);
                cctz::civil_day civil_date(date_v1.year(), date_v1.month(), date_v1.day());
                day_offset =
                        cctz::convert(civil_date, utc0).time_since_epoch().count() / (24 * 60 * 60);
            } else {
                const DateV2Value<DateV2ValueType> date_v2 =
                        *reinterpret_cast<const DateV2Value<DateV2ValueType>*>(value);
                cctz::civil_day civil_date(date_v2.year(), date_v2.month(), date_v2.day());
                day_offset =
                        cctz::convert(civil_date, utc0).time_since_epoch().count() / (24 * 60 * 60);
            }
            return std::make_tuple(true, orc::Literal(orc::PredicateDataType::DATE, day_offset));
        }
        case orc::TypeKind::TIMESTAMP: {
            int64_t seconds;
            int32_t nanos;
            static const cctz::time_zone utc0 = cctz::utc_time_zone();
            // TODO: ColumnValueRange has lost the precision of microsecond
            if constexpr (std::is_same_v<CppType, VecDateTimeValue>) {
                const VecDateTimeValue datetime_v1 =
                        *reinterpret_cast<const VecDateTimeValue*>(value);
                cctz::civil_second civil_seconds(datetime_v1.year(), datetime_v1.month(),
                                                 datetime_v1.day(), datetime_v1.hour(),
                                                 datetime_v1.minute(), datetime_v1.second());
                seconds = cctz::convert(civil_seconds, utc0).time_since_epoch().count();
                nanos = 0;
            } else {
                const DateV2Value<DateTimeV2ValueType> datetime_v2 =
                        *reinterpret_cast<const DateV2Value<DateTimeV2ValueType>*>(value);
                cctz::civil_second civil_seconds(datetime_v2.year(), datetime_v2.month(),
                                                 datetime_v2.day(), datetime_v2.hour(),
                                                 datetime_v2.minute(), datetime_v2.second());
                seconds = cctz::convert(civil_seconds, utc0).time_since_epoch().count();
                nanos = datetime_v2.microsecond() * 1000;
            }
            return std::make_tuple(true, orc::Literal(seconds, nanos));
        }
        default:
            return std::make_tuple(false, orc::Literal(false));
        }
    } catch (Exception& e) {
        // When table schema changed, and using new schema to read old data.
        LOG(WARNING) << "Failed to convert doris value to orc predicate literal, error = "
                     << e.what();
        return std::make_tuple(false, orc::Literal(false));
    }
}

template <PrimitiveType primitive_type>
std::vector<OrcPredicate> value_range_to_predicate(
        const ColumnValueRange<primitive_type>& col_val_range, const orc::Type* type) {
    using CppType = typename PrimitiveTypeTraits<primitive_type>::CppType;
    std::vector<OrcPredicate> predicates;
    orc::PredicateDataType predicate_data_type;
    auto type_it = TYPEKIND_TO_PREDICATE_TYPE.find(type->getKind());
    if (type_it == TYPEKIND_TO_PREDICATE_TYPE.end()) {
        // Unsupported type
        return predicates;
    } else {
        predicate_data_type = type_it->second;
    }

    if (col_val_range.is_fixed_value_range()) {
        OrcPredicate in_predicate;
        in_predicate.col_name = col_val_range.column_name();
        in_predicate.data_type = predicate_data_type;
        in_predicate.op = SQLFilterOp::FILTER_IN;
        for (const auto& value : col_val_range.get_fixed_value_set()) {
            auto [valid, literal] = convert_to_orc_literal<CppType>(
                    type, &value, col_val_range.precision(), col_val_range.scale());
            if (valid) {
                in_predicate.literals.push_back(literal);
            }
        }
        if (!in_predicate.literals.empty()) {
            predicates.emplace_back(in_predicate);
        }
        return predicates;
    }

    const auto& high_value = col_val_range.get_range_max_value();
    const auto& low_value = col_val_range.get_range_min_value();
    const auto& high_op = col_val_range.get_range_high_op();
    const auto& low_op = col_val_range.get_range_low_op();

    // orc can only push down is_null. When col_value_range._contain_null = true, only indicating that
    // value can be null, not equals null, so ignore _contain_null in col_value_range
    if (col_val_range.is_high_value_maximum() && high_op == SQLFilterOp::FILTER_LESS_OR_EQUAL &&
        col_val_range.is_low_value_mininum() && low_op == SQLFilterOp::FILTER_LARGER_OR_EQUAL) {
        return predicates;
    }

    if (low_value < high_value) {
        if (!col_val_range.is_low_value_mininum() ||
            SQLFilterOp::FILTER_LARGER_OR_EQUAL != low_op) {
            auto [valid, low_literal] = convert_to_orc_literal<CppType>(
                    type, &low_value, col_val_range.precision(), col_val_range.scale());
            if (valid) {
                OrcPredicate low_predicate;
                low_predicate.col_name = col_val_range.column_name();
                low_predicate.data_type = predicate_data_type;
                low_predicate.op = low_op;
                low_predicate.literals.emplace_back(low_literal);
                predicates.emplace_back(low_predicate);
            }
        }
        if (!col_val_range.is_high_value_maximum() ||
            SQLFilterOp::FILTER_LESS_OR_EQUAL != high_op) {
            auto [valid, high_literal] = convert_to_orc_literal<CppType>(
                    type, &high_value, col_val_range.precision(), col_val_range.scale());
            if (valid) {
                OrcPredicate high_predicate;
                high_predicate.col_name = col_val_range.column_name();
                high_predicate.data_type = predicate_data_type;
                high_predicate.op = high_op;
                high_predicate.literals.emplace_back(high_literal);
                predicates.emplace_back(high_predicate);
            }
        }
    }
    return predicates;
}

bool static build_search_argument(std::vector<OrcPredicate>& predicates, int index,
                                  std::unique_ptr<orc::SearchArgumentBuilder>& builder) {
    if (index >= predicates.size()) {
        return false;
    }
    if (index < predicates.size() - 1) {
        builder->startAnd();
    }
    OrcPredicate& predicate = predicates[index];
    switch (predicate.op) {
    case SQLFilterOp::FILTER_IN: {
        if (predicate.literals.size() == 1) {
            builder->equals(predicate.col_name, predicate.data_type, predicate.literals[0]);
        } else {
            builder->in(predicate.col_name, predicate.data_type, predicate.literals);
        }
        break;
    }
    case SQLFilterOp::FILTER_LESS:
        builder->lessThan(predicate.col_name, predicate.data_type, predicate.literals[0]);
        break;
    case SQLFilterOp::FILTER_LESS_OR_EQUAL:
        builder->lessThanEquals(predicate.col_name, predicate.data_type, predicate.literals[0]);
        break;
    case SQLFilterOp::FILTER_LARGER: {
        builder->startNot();
        builder->lessThanEquals(predicate.col_name, predicate.data_type, predicate.literals[0]);
        builder->end();
        break;
    }
    case SQLFilterOp::FILTER_LARGER_OR_EQUAL: {
        builder->startNot();
        builder->lessThan(predicate.col_name, predicate.data_type, predicate.literals[0]);
        builder->end();
        break;
    }
    default:
        return false;
    }
    if (index < predicates.size() - 1) {
        bool can_build = build_search_argument(predicates, index + 1, builder);
        if (!can_build) {
            return false;
        }
        builder->end();
    }
    return true;
}

void OrcReader::_init_search_argument(
        std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    if (colname_to_value_range->empty()) {
        return;
    }
    std::vector<OrcPredicate> predicates;
    auto& root_type = _reader->getType();
    std::unordered_map<std::string, const orc::Type*> type_map;
    for (int i = 0; i < root_type.getSubtypeCount(); ++i) {
        type_map.emplace(_get_field_name_lower_case(&root_type, i), root_type.getSubtype(i));
    }
    for (auto it = colname_to_value_range->begin(); it != colname_to_value_range->end(); ++it) {
        auto type_it = type_map.find(it->first);
        if (type_it != type_map.end()) {
            std::visit(
                    [&](auto& range) {
                        std::vector<OrcPredicate> value_predicates =
                                value_range_to_predicate(range, type_it->second);
                        for (auto& range_predicate : value_predicates) {
                            predicates.emplace_back(range_predicate);
                        }
                    },
                    it->second);
        }
    }
    if (predicates.empty()) {
        return;
    }
    std::unique_ptr<orc::SearchArgumentBuilder> builder = orc::SearchArgumentFactory::newBuilder();
    if (build_search_argument(predicates, 0, builder)) {
        std::unique_ptr<orc::SearchArgument> sargs = builder->build();
        _row_reader_options.searchArgument(std::move(sargs));
    }
}

void OrcReader::_init_bloom_filter(
        std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    // generate bloom filter
    // _reader->getBloomFilters()
}

void OrcReader::_init_system_properties() {
    _system_properties.system_type = _scan_params.file_type;
    _system_properties.properties = _scan_params.properties;
    _system_properties.hdfs_params = _scan_params.hdfs_params;
    if (_scan_params.__isset.broker_addresses) {
        _system_properties.broker_addresses.assign(_scan_params.broker_addresses.begin(),
                                                   _scan_params.broker_addresses.end());
    }
}

void OrcReader::_init_file_description() {
    _file_description.path = _scan_range.path;
    _file_description.start_offset = _scan_range.start_offset;
    _file_description.file_size = _scan_range.__isset.file_size ? _scan_range.file_size : 0;
}

TypeDescriptor OrcReader::_convert_to_doris_type(const orc::Type* orc_type) {
    switch (orc_type->getKind()) {
    case orc::TypeKind::BOOLEAN:
        return TypeDescriptor(PrimitiveType::TYPE_BOOLEAN);
    case orc::TypeKind::BYTE:
        return TypeDescriptor(PrimitiveType::TYPE_TINYINT);
    case orc::TypeKind::SHORT:
        return TypeDescriptor(PrimitiveType::TYPE_SMALLINT);
    case orc::TypeKind::INT:
        return TypeDescriptor(PrimitiveType::TYPE_INT);
    case orc::TypeKind::LONG:
        return TypeDescriptor(PrimitiveType::TYPE_BIGINT);
    case orc::TypeKind::FLOAT:
        return TypeDescriptor(PrimitiveType::TYPE_FLOAT);
    case orc::TypeKind::DOUBLE:
        return TypeDescriptor(PrimitiveType::TYPE_DOUBLE);
    case orc::TypeKind::STRING:
        return TypeDescriptor(PrimitiveType::TYPE_STRING);
    case orc::TypeKind::BINARY:
        return TypeDescriptor(PrimitiveType::TYPE_STRING);
    case orc::TypeKind::TIMESTAMP:
        return TypeDescriptor(PrimitiveType::TYPE_DATETIMEV2);
    case orc::TypeKind::DECIMAL:
        // TODO: using decimal v3 instead
        return TypeDescriptor(PrimitiveType::TYPE_DECIMALV2);
    case orc::TypeKind::DATE:
        return TypeDescriptor(PrimitiveType::TYPE_DATEV2);
    case orc::TypeKind::VARCHAR:
        return TypeDescriptor(PrimitiveType::TYPE_VARCHAR);
    case orc::TypeKind::CHAR:
        return TypeDescriptor(PrimitiveType::TYPE_CHAR);
    case orc::TypeKind::TIMESTAMP_INSTANT:
        return TypeDescriptor(PrimitiveType::TYPE_DATETIMEV2);
    case orc::TypeKind::LIST: {
        TypeDescriptor list_type(PrimitiveType::TYPE_ARRAY);
        list_type.add_sub_type(_convert_to_doris_type(orc_type->getSubtype(0)));
        return list_type;
    }
    case orc::TypeKind::MAP: {
        TypeDescriptor map_type(PrimitiveType::TYPE_MAP);
        map_type.add_sub_type(_convert_to_doris_type(orc_type->getSubtype(0)));
        map_type.add_sub_type(_convert_to_doris_type(orc_type->getSubtype(1)));
        return map_type;
    }
    case orc::TypeKind::STRUCT: {
        TypeDescriptor struct_type(PrimitiveType::TYPE_STRUCT);
        for (int i = 0; i < orc_type->getSubtypeCount(); ++i) {
            struct_type.add_sub_type(_convert_to_doris_type(orc_type->getSubtype(i)),
                                     _get_field_name_lower_case(orc_type, i));
        }
        return struct_type;
    }
    default:
        return TypeDescriptor(PrimitiveType::INVALID_TYPE);
    }
}

std::unordered_map<std::string, TypeDescriptor> OrcReader::get_name_to_type() {
    std::unordered_map<std::string, TypeDescriptor> map;
    auto& root_type = _reader->getType();
    for (int i = 0; i < root_type.getSubtypeCount(); ++i) {
        map.emplace(_get_field_name_lower_case(&root_type, i),
                    _convert_to_doris_type(root_type.getSubtype(i)));
    }
    return map;
}

Status OrcReader::get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                              std::unordered_set<std::string>* missing_cols) {
    auto& root_type = _reader->getType();
    for (int i = 0; i < root_type.getSubtypeCount(); ++i) {
        name_to_type->emplace(_get_field_name_lower_case(&root_type, i),
                              _convert_to_doris_type(root_type.getSubtype(i)));
    }
    for (auto& col : _missing_cols) {
        missing_cols->insert(col);
    }
    return Status::OK();
}

// Hive ORC char type will pad trailing spaces.
// https://docs.cloudera.com/documentation/enterprise/6/6.3/topics/impala_char.html
static inline size_t trim_right(const char* s, size_t size) {
    while (size > 0 && s[size - 1] == ' ') {
        size--;
    }
    return size;
}

Status OrcReader::_decode_string_column(const std::string& col_name,
                                        const MutableColumnPtr& data_column,
                                        const orc::TypeKind& type_kind, orc::ColumnVectorBatch* cvb,
                                        size_t num_values) {
    SCOPED_RAW_TIMER(&_statistics.decode_value_time);
    const static std::string empty_string;
    auto* data = down_cast<orc::StringVectorBatch*>(cvb);
    if (data == nullptr) {
        return Status::InternalError("Wrong data type for colum '{}'", col_name);
    }
    std::vector<StringRef> string_values;
    string_values.reserve(num_values);
    if (type_kind == orc::TypeKind::CHAR) {
        // Possibly there are some zero padding characters in CHAR type, we have to strip them off.
        for (int i = 0; i < num_values; ++i) {
            if (cvb->notNull[i]) {
                string_values.emplace_back(data->data[i],
                                           trim_right(data->data[i], data->length[i]));
            } else {
                // Orc doesn't fill null values in new batch, but the former batch has been release.
                // Other types like int/long/timestamp... are flat types without pointer in them,
                // so other types do not need to be handled separately like string.
                string_values.emplace_back(empty_string.data(), 0);
            }
        }
    } else {
        for (int i = 0; i < num_values; ++i) {
            if (cvb->notNull[i]) {
                string_values.emplace_back(data->data[i], data->length[i]);
            } else {
                string_values.emplace_back(empty_string.data(), 0);
            }
        }
    }
    data_column->insert_many_strings(&string_values[0], num_values);
    return Status::OK();
}

Status OrcReader::_fill_doris_array_offsets(const std::string& col_name,
                                            ColumnArray::Offsets64& doris_offsets,
                                            orc::DataBuffer<int64_t>& orc_offsets,
                                            size_t num_values, size_t* element_size) {
    SCOPED_RAW_TIMER(&_statistics.decode_value_time);
    if (num_values > 0) {
        if (orc_offsets.size() < num_values + 1) {
            return Status::InternalError("Wrong array offsets in orc file for column '{}'",
                                         col_name);
        }
        auto prev_offset = doris_offsets.back();
        auto base_offset = orc_offsets[0];
        for (int i = 1; i < num_values + 1; ++i) {
            doris_offsets.emplace_back(prev_offset + orc_offsets[i] - base_offset);
        }
        *element_size = orc_offsets[num_values] - base_offset;
    } else {
        *element_size = 0;
    }
    return Status::OK();
}

Status OrcReader::_orc_column_to_doris_column(const std::string& col_name,
                                              const ColumnPtr& doris_column,
                                              const DataTypePtr& data_type,
                                              const orc::Type* orc_column_type,
                                              orc::ColumnVectorBatch* cvb, size_t num_values) {
    MutableColumnPtr data_column;
    if (doris_column->is_nullable()) {
        SCOPED_RAW_TIMER(&_statistics.decode_null_map_time);
        auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
                (*std::move(doris_column)).mutate().get());
        data_column = nullable_column->get_nested_column_ptr();
        NullMap& map_data_column = nullable_column->get_null_map_data();
        auto origin_size = map_data_column.size();
        map_data_column.resize(origin_size + num_values);
        if (cvb->hasNulls) {
            auto* cvb_nulls = reinterpret_cast<uint8_t*>(cvb->notNull.data());
            for (int i = 0; i < num_values; ++i) {
                map_data_column[origin_size + i] = !cvb_nulls[i];
            }
        } else {
            for (int i = 0; i < num_values; ++i) {
                map_data_column[origin_size + i] = false;
            }
        }
    } else {
        if (cvb->hasNulls) {
            return Status::InternalError("Not nullable column {} has null values in orc file",
                                         col_name);
        }
        data_column = doris_column->assume_mutable();
    }

    TypeIndex logical_type = remove_nullable(data_type)->get_type_id();
    switch (logical_type) {
#define DISPATCH(FlatType, CppType, OrcColumnType) \
    case FlatType:                                 \
        return _decode_flat_column<CppType, OrcColumnType>(col_name, data_column, cvb, num_values);
        FOR_FLAT_ORC_COLUMNS(DISPATCH)
#undef DISPATCH
    case TypeIndex::Decimal32:
        return _decode_decimal_column<Int32>(col_name, data_column, data_type, cvb, num_values);
    case TypeIndex::Decimal64:
        return _decode_decimal_column<Int64>(col_name, data_column, data_type, cvb, num_values);
    case TypeIndex::Decimal128:
        return _decode_decimal_column<Int128>(col_name, data_column, data_type, cvb, num_values);
    case TypeIndex::Decimal128I:
        return _decode_decimal_column<Int128>(col_name, data_column, data_type, cvb, num_values);
    case TypeIndex::Date:
        return _decode_time_column<VecDateTimeValue, Int64, orc::LongVectorBatch>(
                col_name, data_column, cvb, num_values);
    case TypeIndex::DateV2:
        return _decode_time_column<DateV2Value<DateV2ValueType>, UInt32, orc::LongVectorBatch>(
                col_name, data_column, cvb, num_values);
    case TypeIndex::DateTime:
        return _decode_time_column<VecDateTimeValue, Int64, orc::TimestampVectorBatch>(
                col_name, data_column, cvb, num_values);
    case TypeIndex::DateTimeV2:
        return _decode_time_column<DateV2Value<DateTimeV2ValueType>, UInt64,
                                   orc::TimestampVectorBatch>(col_name, data_column, cvb,
                                                              num_values);
    case TypeIndex::String:
    case TypeIndex::FixedString:
        return _decode_string_column(col_name, data_column, orc_column_type->getKind(), cvb,
                                     num_values);
    case TypeIndex::Array: {
        if (orc_column_type->getKind() != orc::TypeKind::LIST) {
            return Status::InternalError("Wrong data type for colum '{}'", col_name);
        }
        auto* orc_list = down_cast<orc::ListVectorBatch*>(cvb);
        auto& doris_offsets = static_cast<ColumnArray&>(*data_column).get_offsets();
        auto& orc_offsets = orc_list->offsets;
        size_t element_size = 0;
        RETURN_IF_ERROR(_fill_doris_array_offsets(col_name, doris_offsets, orc_offsets, num_values,
                                                  &element_size));
        DataTypePtr& nested_type = const_cast<DataTypePtr&>(
                reinterpret_cast<const DataTypeArray*>(remove_nullable(data_type).get())
                        ->get_nested_type());
        const orc::Type* nested_orc_type = orc_column_type->getSubtype(0);
        return _orc_column_to_doris_column(
                col_name, static_cast<ColumnArray&>(*data_column).get_data_ptr(), nested_type,
                nested_orc_type, orc_list->elements.get(), element_size);
    }
    case TypeIndex::Map: {
        if (orc_column_type->getKind() != orc::TypeKind::MAP) {
            return Status::InternalError("Wrong data type for colum '{}'", col_name);
        }
        auto* orc_map = down_cast<orc::MapVectorBatch*>(cvb);
        auto& doris_map = static_cast<ColumnMap&>(*data_column);
        size_t element_size = 0;
        RETURN_IF_ERROR(_fill_doris_array_offsets(col_name, doris_map.get_offsets(),
                                                  orc_map->offsets, num_values, &element_size));
        DataTypePtr& doris_key_type = const_cast<DataTypePtr&>(
                reinterpret_cast<const DataTypeMap*>(remove_nullable(data_type).get())
                        ->get_key_type());
        DataTypePtr& doris_value_type = const_cast<DataTypePtr&>(
                reinterpret_cast<const DataTypeMap*>(remove_nullable(data_type).get())
                        ->get_value_type());
        const orc::Type* orc_key_type = orc_column_type->getSubtype(0);
        const orc::Type* orc_value_type = orc_column_type->getSubtype(1);
        const ColumnPtr& doris_key_column = doris_map.get_keys_ptr();
        const ColumnPtr& doris_value_column = doris_map.get_values_ptr();
        RETURN_IF_ERROR(_orc_column_to_doris_column(col_name, doris_key_column, doris_key_type,
                                                    orc_key_type, orc_map->keys.get(),
                                                    element_size));
        return _orc_column_to_doris_column(col_name, doris_value_column, doris_value_type,
                                           orc_value_type, orc_map->elements.get(), element_size);
    }
    case TypeIndex::Struct: {
        if (orc_column_type->getKind() != orc::TypeKind::STRUCT) {
            return Status::InternalError("Wrong data type for colum '{}'", col_name);
        }
        auto* orc_struct = down_cast<orc::StructVectorBatch*>(cvb);
        auto& doris_struct = static_cast<ColumnStruct&>(*data_column);
        if (orc_struct->fields.size() != doris_struct.tuple_size()) {
            return Status::InternalError("Wrong number of struct fields for column '{}'", col_name);
        }
        const DataTypeStruct* doris_struct_type =
                reinterpret_cast<const DataTypeStruct*>(remove_nullable(data_type).get());
        for (int i = 0; i < doris_struct.tuple_size(); ++i) {
            orc::ColumnVectorBatch* orc_field = orc_struct->fields[i];
            const orc::Type* orc_type = orc_column_type->getSubtype(i);
            const ColumnPtr& doris_field = doris_struct.get_column_ptr(i);
            const DataTypePtr& doris_type = doris_struct_type->get_element(i);
            RETURN_IF_ERROR(_orc_column_to_doris_column(col_name, doris_field, doris_type, orc_type,
                                                        orc_field, num_values));
        }
        return Status::OK();
    }
    default:
        break;
    }
    return Status::InternalError("Unsupported type for column '{}'", col_name);
}

std::string OrcReader::_get_field_name_lower_case(const orc::Type* orc_type, int pos) {
    std::string name = orc_type->getFieldName(pos);
    transform(name.begin(), name.end(), name.begin(), ::tolower);
    return name;
}

Status OrcReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    SCOPED_RAW_TIMER(&_statistics.column_read_time);
    {
        SCOPED_RAW_TIMER(&_statistics.get_batch_time);
        // reset decimal_scale_params_index
        _decimal_scale_params_index = 0;
        if (!_row_reader->next(*_batch)) {
            *eof = true;
            *read_rows = 0;
            return Status::OK();
        }
    }
    const auto& batch_vec = down_cast<orc::StructVectorBatch*>(_batch.get())->fields;
    for (auto& col : _read_cols_lower_case) {
        auto& column_with_type_and_name = block->get_by_name(col);
        auto& column_ptr = column_with_type_and_name.column;
        auto& column_type = column_with_type_and_name.type;
        auto orc_col_idx = _colname_to_idx.find(col);
        if (orc_col_idx == _colname_to_idx.end()) {
            return Status::InternalError("Wrong read column '{}' in orc file", col);
        }
        RETURN_IF_ERROR(_orc_column_to_doris_column(
                col, column_ptr, column_type, _col_orc_type[orc_col_idx->second],
                batch_vec[orc_col_idx->second], _batch->numElements));
    }
    *read_rows = _batch->numElements;
    return Status::OK();
}

} // namespace doris::vectorized
