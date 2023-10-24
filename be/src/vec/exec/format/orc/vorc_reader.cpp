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

#include <cctz/civil_time_detail.h>
#include <ctype.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>

#include <algorithm>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <exception>
#include <iterator>
#include <map>
#include <ostream>
#include <tuple>
#include <variant>

#include "cctz/civil_time.h"
#include "cctz/time_zone.h"
#include "common/exception.h"
#include "exec/olap_utils.h"
#include "exprs/create_predicate_function.h"
#include "exprs/hybrid_set.h"
#include "gutil/strings/substitute.h"
#include "io/fs/buffered_reader.h"
#include "io/fs/file_reader.h"
#include "orc/Exceptions.hh"
#include "orc/Int128.hh"
#include "orc/MemoryPool.hh"
#include "orc/OrcFile.hh"
#include "orc/sargs/Literal.hh"
#include "orc/sargs/SearchArgument.hh"
#include "runtime/decimalv2_value.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/primitive_type.h"
#include "runtime/thread_context.h"
#include "util/slice.h"
#include "util/timezone_utils.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_struct.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/exec/format/table/transactional_hive_common.h"
#include "vec/exprs/vbloom_predicate.h"
#include "vec/exprs/vdirect_in_predicate.h"
#include "vec/exprs/vectorized_fn_call.h"
#include "vec/exprs/vin_predicate.h"
#include "vec/exprs/vliteral.h"
#include "vec/exprs/vruntimefilter_wrapper.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
class RuntimeState;

namespace io {
struct IOContext;
enum class FileCachePolicy : uint8_t;
} // namespace io
} // namespace doris

namespace doris::vectorized {

// TODO: we need to determine it by test.
static constexpr uint32_t MAX_DICT_CODE_PREDICATE_TO_REWRITE = std::numeric_limits<uint32_t>::max();
static constexpr char EMPTY_STRING_FOR_OVERFLOW[ColumnString::MAX_STRINGS_OVERFLOW_SIZE] = "";

#define FOR_FLAT_ORC_COLUMNS(M)                            \
    M(TypeIndex::Int8, Int8, orc::LongVectorBatch)         \
    M(TypeIndex::UInt8, UInt8, orc::LongVectorBatch)       \
    M(TypeIndex::Int16, Int16, orc::LongVectorBatch)       \
    M(TypeIndex::UInt16, UInt16, orc::LongVectorBatch)     \
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
                     size_t batch_size, const std::string& ctz, io::IOContext* io_ctx,
                     bool enable_lazy_mat)
        : _profile(profile),
          _state(state),
          _scan_params(params),
          _scan_range(range),
          _batch_size(std::max(batch_size, _MIN_BATCH_SIZE)),
          _range_start_offset(range.start_offset),
          _range_size(range.size),
          _ctz(ctz),
          _is_hive(params.__isset.slot_name_to_schema_pos),
          _io_ctx(io_ctx),
          _enable_lazy_mat(enable_lazy_mat) {
    TimezoneUtils::find_cctz_time_zone(ctz, _time_zone);
    VecDateTimeValue t;
    t.from_unixtime(0, ctz);
    _offset_days = t.day() == 31 ? -1 : 0; // If 1969-12-31, then returns -1.
    _init_profile();
    _init_system_properties();
    _init_file_description();
}

OrcReader::OrcReader(const TFileScanRangeParams& params, const TFileRangeDesc& range,
                     const std::string& ctz, io::IOContext* io_ctx, bool enable_lazy_mat)
        : _profile(nullptr),
          _scan_params(params),
          _scan_range(range),
          _ctz(ctz),
          _is_hive(params.__isset.slot_name_to_schema_pos),
          _file_system(nullptr),
          _io_ctx(io_ctx),
          _enable_lazy_mat(enable_lazy_mat) {
    _init_system_properties();
    _init_file_description();
}

OrcReader::~OrcReader() {
    _collect_profile_on_close();
    if (_obj_pool && _obj_pool.get()) {
        _obj_pool->clear();
    }
}

void OrcReader::_collect_profile_on_close() {
    if (_profile != nullptr) {
        COUNTER_UPDATE(_orc_profile.read_time, _statistics.fs_read_time);
        COUNTER_UPDATE(_orc_profile.read_calls, _statistics.fs_read_calls);
        COUNTER_UPDATE(_orc_profile.read_bytes, _statistics.fs_read_bytes);
        COUNTER_UPDATE(_orc_profile.column_read_time, _statistics.column_read_time);
        COUNTER_UPDATE(_orc_profile.get_batch_time, _statistics.get_batch_time);
        COUNTER_UPDATE(_orc_profile.create_reader_time, _statistics.create_reader_time);
        COUNTER_UPDATE(_orc_profile.init_column_time, _statistics.init_column_time);
        COUNTER_UPDATE(_orc_profile.set_fill_column_time, _statistics.set_fill_column_time);
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
        ADD_TIMER_WITH_LEVEL(_profile, orc_profile, 1);
        _orc_profile.read_time = ADD_TIMER_WITH_LEVEL(_profile, "FileReadTime", 1);
        _orc_profile.read_calls = ADD_COUNTER_WITH_LEVEL(_profile, "FileReadCalls", TUnit::UNIT, 1);
        _orc_profile.read_bytes =
                ADD_COUNTER_WITH_LEVEL(_profile, "FileReadBytes", TUnit::BYTES, 1);
        _orc_profile.column_read_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "ColumnReadTime", orc_profile, 1);
        _orc_profile.get_batch_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "GetBatchTime", orc_profile, 1);
        _orc_profile.create_reader_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "CreateReaderTime", orc_profile, 1);
        _orc_profile.init_column_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "InitColumnTime", orc_profile, 1);
        _orc_profile.set_fill_column_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "SetFillColumnTime", orc_profile, 1);
        _orc_profile.decode_value_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "DecodeValueTime", orc_profile, 1);
        _orc_profile.decode_null_map_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "DecodeNullMapTime", orc_profile, 1);
    }
}

Status OrcReader::_create_file_reader() {
    if (_file_input_stream == nullptr) {
        io::FileReaderSPtr inner_reader;
        _file_description.mtime =
                _scan_range.__isset.modification_time ? _scan_range.modification_time : 0;
        io::FileReaderOptions reader_options =
                FileFactory::get_reader_options(_state, _file_description);
        RETURN_IF_ERROR(io::DelegateReader::create_file_reader(
                _profile, _system_properties, _file_description, reader_options, &_file_system,
                &inner_reader, io::DelegateReader::AccessMode::RANDOM, _io_ctx));
        _file_input_stream.reset(new ORCFileInputStream(_scan_range.path, inner_reader,
                                                        &_statistics, _io_ctx, _profile));
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
        // invoker maybe just skip Status.NotFound and continue
        // so we need distinguish between it and other kinds of errors
        std::string _err_msg = e.what();
        if (_err_msg.find("No such file or directory") != std::string::npos) {
            return Status::NotFound(_err_msg);
        }
        return Status::InternalError("Init OrcReader failed. reason = {}", _err_msg);
    }
    return Status::OK();
}

Status OrcReader::init_reader(
        const std::vector<std::string>* column_names,
        std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range,
        const VExprContextSPtrs& conjuncts, bool is_acid, const TupleDescriptor* tuple_descriptor,
        const RowDescriptor* row_descriptor,
        const VExprContextSPtrs* not_single_slot_filter_conjuncts,
        const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts) {
    _column_names = column_names;
    _colname_to_value_range = colname_to_value_range;
    _lazy_read_ctx.conjuncts = conjuncts;
    _is_acid = is_acid;
    _tuple_descriptor = tuple_descriptor;
    _row_descriptor = row_descriptor;
    if (not_single_slot_filter_conjuncts != nullptr && !not_single_slot_filter_conjuncts->empty()) {
        _not_single_slot_filter_conjuncts.insert(_not_single_slot_filter_conjuncts.end(),
                                                 not_single_slot_filter_conjuncts->begin(),
                                                 not_single_slot_filter_conjuncts->end());
    }
    _slot_id_to_filter_conjuncts = slot_id_to_filter_conjuncts;
    _obj_pool = std::make_shared<ObjectPool>();
    {
        SCOPED_RAW_TIMER(&_statistics.create_reader_time);
        RETURN_IF_ERROR(_create_file_reader());
    }
    {
        SCOPED_RAW_TIMER(&_statistics.init_column_time);
        RETURN_IF_ERROR(_init_read_columns());
    }
    return Status::OK();
}

Status OrcReader::get_parsed_schema(std::vector<std::string>* col_names,
                                    std::vector<TypeDescriptor>* col_types) {
    RETURN_IF_ERROR(_create_file_reader());
    auto& root_type = _is_acid ? _remove_acid(_reader->getType()) : _reader->getType();
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
    _init_orc_cols(root_type, orc_cols, orc_cols_lower_case, _type_map);

    for (size_t i = 0; i < _column_names->size(); ++i) {
        auto& col_name = (*_column_names)[i];
        if (_is_hive) {
            auto iter = _scan_params.slot_name_to_schema_pos.find(col_name);
            if (iter != _scan_params.slot_name_to_schema_pos.end()) {
                int pos = iter->second;
                if (_is_acid && i < _column_names->size() - TransactionalHive::READ_PARAMS.size()) {
                    if (TransactionalHive::ROW_OFFSET + 1 + pos < orc_cols_lower_case.size()) {
                        // shift TransactionalHive::ROW_OFFSET + 1 offset, 1 is row struct col
                        orc_cols_lower_case[TransactionalHive::ROW_OFFSET + 1 + pos] = iter->first;
                    }
                } else {
                    if (pos < orc_cols_lower_case.size()) {
                        orc_cols_lower_case[pos] = iter->first;
                    }
                }
            }
        }
        auto iter = std::find(orc_cols_lower_case.begin(), orc_cols_lower_case.end(), col_name);
        if (iter == orc_cols_lower_case.end()) {
            _missing_cols.emplace_back(col_name);
        } else {
            int pos = std::distance(orc_cols_lower_case.begin(), iter);
            std::string read_col;
            if (_is_acid && i < _column_names->size() - TransactionalHive::READ_PARAMS.size()) {
                read_col = fmt::format(
                        "{}.{}",
                        TransactionalHive::ACID_COLUMN_NAMES[TransactionalHive::ROW_OFFSET],
                        orc_cols[pos]);
                _read_cols.emplace_back(read_col);
            } else {
                read_col = orc_cols[pos];
                _read_cols.emplace_back(read_col);
            }
            _read_cols_lower_case.emplace_back(col_name);
            // For hive engine, store the orc column name to schema column name map.
            // This is for Hive 1.x orc file with internal column name _col0, _col1...
            if (_is_hive) {
                _removed_acid_file_col_name_to_schema_col[orc_cols[pos]] = col_name;
            }
            _col_name_to_file_col_name[col_name] = read_col;
        }
    }
    return Status::OK();
}

void OrcReader::_init_orc_cols(const orc::Type& type, std::vector<std::string>& orc_cols,
                               std::vector<std::string>& orc_cols_lower_case,
                               std::unordered_map<std::string, const orc::Type*>& type_map) {
    for (int i = 0; i < type.getSubtypeCount(); ++i) {
        orc_cols.emplace_back(type.getFieldName(i));
        auto filed_name_lower_case = _get_field_name_lower_case(&type, i);
        auto filed_name_lower_case_copy = filed_name_lower_case;
        orc_cols_lower_case.emplace_back(std::move(filed_name_lower_case));
        type_map.emplace(std::move(filed_name_lower_case_copy), type.getSubtype(i));
        if (_is_acid) {
            const orc::Type* sub_type = type.getSubtype(i);
            if (sub_type->getKind() == orc::TypeKind::STRUCT) {
                _init_orc_cols(*sub_type, orc_cols, orc_cols_lower_case, type_map);
            }
        }
    }
}

bool OrcReader::_check_acid_schema(const orc::Type& type) {
    if (orc::TypeKind::STRUCT == type.getKind()) {
        if (type.getSubtypeCount() != TransactionalHive::ACID_COLUMN_NAMES.size()) {
            return false;
        }
        for (uint64_t i = 0; i < type.getSubtypeCount(); ++i) {
            const std::string& field_name = type.getFieldName(i);
            std::string field_name_lower_case = field_name;
            std::transform(field_name.begin(), field_name.end(), field_name_lower_case.begin(),
                           [](unsigned char c) { return std::tolower(c); });
            if (field_name_lower_case != TransactionalHive::ACID_COLUMN_NAMES_LOWER_CASE[i]) {
                return false;
            }
        }
    }
    return true;
}

const orc::Type& OrcReader::_remove_acid(const orc::Type& type) {
    if (_check_acid_schema(type)) {
        return *(type.getSubtype(TransactionalHive::ROW_OFFSET));
    } else {
        return type;
    }
}

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

template <PrimitiveType primitive_type>
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
            if constexpr (primitive_type == TYPE_DECIMALV2) {
                decimal_value = *reinterpret_cast<const int128_t*>(value);
                precision = DecimalV2Value::PRECISION;
                scale = DecimalV2Value::SCALE;
            } else if constexpr (primitive_type == TYPE_DECIMAL32) {
                decimal_value = *((int32_t*)value);
            } else if constexpr (primitive_type == TYPE_DECIMAL64) {
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
            if constexpr (primitive_type == TYPE_DATE) {
                const VecDateTimeValue date_v1 = *reinterpret_cast<const VecDateTimeValue*>(value);
                cctz::civil_day civil_date(date_v1.year(), date_v1.month(), date_v1.day());
                day_offset =
                        cctz::convert(civil_date, utc0).time_since_epoch().count() / (24 * 60 * 60);
            } else { // primitive_type == TYPE_DATEV2
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
            if constexpr (primitive_type == TYPE_DATETIME) {
                const VecDateTimeValue datetime_v1 =
                        *reinterpret_cast<const VecDateTimeValue*>(value);
                cctz::civil_second civil_seconds(datetime_v1.year(), datetime_v1.month(),
                                                 datetime_v1.day(), datetime_v1.hour(),
                                                 datetime_v1.minute(), datetime_v1.second());
                seconds = cctz::convert(civil_seconds, utc0).time_since_epoch().count();
                nanos = 0;
            } else { // primitive_type == TYPE_DATETIMEV2
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
            auto [valid, literal] = convert_to_orc_literal<primitive_type>(
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
            auto [valid, low_literal] = convert_to_orc_literal<primitive_type>(
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
            auto [valid, high_literal] = convert_to_orc_literal<primitive_type>(
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

bool OrcReader::_init_search_argument(
        std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    if (colname_to_value_range->empty()) {
        return false;
    }
    std::vector<OrcPredicate> predicates;
    auto& root_type = _reader->getType();
    std::unordered_map<std::string, const orc::Type*> type_map;
    for (int i = 0; i < root_type.getSubtypeCount(); ++i) {
        type_map.emplace(_get_field_name_lower_case(&root_type, i), root_type.getSubtype(i));
    }
    for (auto& col_name : _lazy_read_ctx.all_read_columns) {
        auto iter = colname_to_value_range->find(col_name);
        if (iter == colname_to_value_range->end()) {
            continue;
        }
        auto type_it = type_map.find(col_name);
        if (type_it == type_map.end()) {
            continue;
        }
        std::visit(
                [&](auto& range) {
                    std::vector<OrcPredicate> value_predicates =
                            value_range_to_predicate(range, type_it->second);
                    for (auto& range_predicate : value_predicates) {
                        predicates.emplace_back(range_predicate);
                    }
                },
                iter->second);
    }
    if (predicates.empty()) {
        return false;
    }
    std::unique_ptr<orc::SearchArgumentBuilder> builder = orc::SearchArgumentFactory::newBuilder();
    if (build_search_argument(predicates, 0, builder)) {
        std::unique_ptr<orc::SearchArgument> sargs = builder->build();
        _row_reader_options.searchArgument(std::move(sargs));
        return true;
    } else {
        return false;
    }
}

Status OrcReader::set_fill_columns(
        const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&
                partition_columns,
        const std::unordered_map<std::string, VExprContextSPtr>& missing_columns) {
    SCOPED_RAW_TIMER(&_statistics.set_fill_column_time);

    // std::unordered_map<column_name, std::pair<col_id, slot_id>>
    std::unordered_map<std::string, std::pair<uint32_t, int>> predicate_columns;
    std::function<void(VExpr * expr)> visit_slot = [&](VExpr* expr) {
        if (VSlotRef* slot_ref = typeid_cast<VSlotRef*>(expr)) {
            auto& expr_name = slot_ref->expr_name();
            predicate_columns.emplace(expr_name,
                                      std::make_pair(slot_ref->column_id(), slot_ref->slot_id()));
            if (slot_ref->column_id() == 0) {
                _lazy_read_ctx.resize_first_column = false;
            }
            return;
        } else if (VRuntimeFilterWrapper* runtime_filter =
                           typeid_cast<VRuntimeFilterWrapper*>(expr)) {
            auto filter_impl = const_cast<VExpr*>(runtime_filter->get_impl().get());
            if (VBloomPredicate* bloom_predicate = typeid_cast<VBloomPredicate*>(filter_impl)) {
                for (auto& child : bloom_predicate->children()) {
                    visit_slot(child.get());
                }
            } else if (VInPredicate* in_predicate = typeid_cast<VInPredicate*>(filter_impl)) {
                if (in_predicate->children().size() > 0) {
                    visit_slot(in_predicate->children()[0].get());
                }
            } else {
                for (auto& child : filter_impl->children()) {
                    visit_slot(child.get());
                }
            }
        } else {
            for (auto& child : expr->children()) {
                visit_slot(child.get());
            }
        }
    };

    for (auto& conjunct : _lazy_read_ctx.conjuncts) {
        visit_slot(conjunct->root().get());
    }

    for (auto& read_col : _read_cols_lower_case) {
        _lazy_read_ctx.all_read_columns.emplace_back(read_col);
        if (predicate_columns.size() > 0) {
            auto iter = predicate_columns.find(read_col);
            if (iter == predicate_columns.end()) {
                _lazy_read_ctx.lazy_read_columns.emplace_back(read_col);
            } else {
                _lazy_read_ctx.predicate_columns.first.emplace_back(iter->first);
                _lazy_read_ctx.predicate_columns.second.emplace_back(iter->second.second);
                _lazy_read_ctx.predicate_orc_columns.emplace_back(
                        _col_name_to_file_col_name[iter->first]);
                _lazy_read_ctx.all_predicate_col_ids.emplace_back(iter->second.first);
            }
        }
    }

    for (auto& kv : partition_columns) {
        auto iter = predicate_columns.find(kv.first);
        if (iter == predicate_columns.end()) {
            _lazy_read_ctx.partition_columns.emplace(kv.first, kv.second);
        } else {
            _lazy_read_ctx.predicate_partition_columns.emplace(kv.first, kv.second);
            _lazy_read_ctx.all_predicate_col_ids.emplace_back(iter->second.first);
        }
    }

    for (auto& kv : missing_columns) {
        auto iter = predicate_columns.find(kv.first);
        if (iter == predicate_columns.end()) {
            _lazy_read_ctx.missing_columns.emplace(kv.first, kv.second);
        } else {
            _lazy_read_ctx.predicate_missing_columns.emplace(kv.first, kv.second);
            _lazy_read_ctx.all_predicate_col_ids.emplace_back(iter->second.first);
        }
    }

    if (_enable_lazy_mat && _lazy_read_ctx.predicate_columns.first.size() > 0 &&
        _lazy_read_ctx.lazy_read_columns.size() > 0) {
        _lazy_read_ctx.can_lazy_read = true;
    }

    if (_colname_to_value_range == nullptr || !_init_search_argument(_colname_to_value_range)) {
        _lazy_read_ctx.can_lazy_read = false;
    }

    if (!_lazy_read_ctx.can_lazy_read) {
        for (auto& kv : _lazy_read_ctx.predicate_partition_columns) {
            _lazy_read_ctx.partition_columns.emplace(kv.first, kv.second);
        }
        for (auto& kv : _lazy_read_ctx.predicate_missing_columns) {
            _lazy_read_ctx.missing_columns.emplace(kv.first, kv.second);
        }
    }

    _fill_all_columns = true;

    // create orc row reader
    try {
        _row_reader_options.range(_range_start_offset, _range_size);
        _row_reader_options.setTimezoneName(_ctz == "CST" ? "Asia/Shanghai" : _ctz);
        _row_reader_options.include(_read_cols);
        if (_lazy_read_ctx.can_lazy_read) {
            _row_reader_options.filter(_lazy_read_ctx.predicate_orc_columns);
            _orc_filter = std::unique_ptr<ORCFilterImpl>(new ORCFilterImpl(this));
        }
        _row_reader_options.setEnableLazyDecoding(true);
        if (!_lazy_read_ctx.conjuncts.empty()) {
            _string_dict_filter = std::make_unique<StringDictFilterImpl>(this);
        }
        _row_reader = _reader->createRowReader(_row_reader_options, _orc_filter.get(),
                                               _string_dict_filter.get());
        _batch = _row_reader->createRowBatch(_batch_size);
        auto& selected_type = _row_reader->getSelectedType();
        int idx = 0;
        static_cast<void>(_init_select_types(selected_type, idx));

        _remaining_rows = _row_reader->getNumberOfRows();

    } catch (std::exception& e) {
        return Status::InternalError("Failed to create orc row reader. reason = {}", e.what());
    }

    if (!_slot_id_to_filter_conjuncts) {
        return Status::OK();
    }

    // Add predicate_partition_columns in _slot_id_to_filter_conjuncts(single slot conjuncts)
    // to _filter_conjuncts, others should be added from not_single_slot_filter_conjuncts.
    for (auto& kv : _lazy_read_ctx.predicate_partition_columns) {
        auto& [value, slot_desc] = kv.second;
        auto iter = _slot_id_to_filter_conjuncts->find(slot_desc->id());
        if (iter != _slot_id_to_filter_conjuncts->end()) {
            for (auto& ctx : iter->second) {
                _filter_conjuncts.push_back(ctx);
            }
        }
    }
    return Status::OK();
}

Status OrcReader::_init_select_types(const orc::Type& type, int idx) {
    for (int i = 0; i < type.getSubtypeCount(); ++i) {
        std::string name;
        // For hive engine, translate the column name in orc file to schema column name.
        // This is for Hive 1.x which use internal column name _col0, _col1...
        if (_is_hive) {
            name = _removed_acid_file_col_name_to_schema_col[type.getFieldName(i)];
        } else {
            name = _get_field_name_lower_case(&type, i);
        }
        _colname_to_idx[name] = idx++;
        const orc::Type* sub_type = type.getSubtype(i);
        _col_orc_type.push_back(sub_type);
        if (_is_acid && sub_type->getKind() == orc::TypeKind::STRUCT) {
            static_cast<void>(_init_select_types(*sub_type, idx));
        }
    }
    return Status::OK();
}

Status OrcReader::_fill_partition_columns(
        Block* block, size_t rows,
        const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&
                partition_columns) {
    DataTypeSerDe::FormatOptions _text_formatOptions;
    for (auto& kv : partition_columns) {
        auto doris_column = block->get_by_name(kv.first).column;
        IColumn* col_ptr = const_cast<IColumn*>(doris_column.get());
        auto& [value, slot_desc] = kv.second;
        auto _text_serde = slot_desc->get_data_type_ptr()->get_serde();
        Slice slice(value.data(), value.size());
        vector<Slice> slices(rows);
        for (int i = 0; i < rows; i++) {
            slices[i] = {value.data(), value.size()};
        }
        int num_deserialized = 0;
        if (_text_serde->deserialize_column_from_json_vector(*col_ptr, slices, &num_deserialized,
                                                             _text_formatOptions) != Status::OK()) {
            return Status::InternalError("Failed to fill partition column: {}={}",
                                         slot_desc->col_name(), value);
        }
        if (num_deserialized != rows) {
            return Status::InternalError(
                    "Failed to fill partition column: {}={} ."
                    "Number of rows expected to be written : {}, number of rows actually written : "
                    "{}",
                    slot_desc->col_name(), value, num_deserialized, rows);
        }
    }
    return Status::OK();
}

Status OrcReader::_fill_missing_columns(
        Block* block, size_t rows,
        const std::unordered_map<std::string, VExprContextSPtr>& missing_columns) {
    for (auto& kv : missing_columns) {
        if (kv.second == nullptr) {
            // no default column, fill with null
            auto nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
                    (*std::move(block->get_by_name(kv.first).column)).mutate().get());
            nullable_column->insert_many_defaults(rows);
        } else {
            // fill with default value
            auto& ctx = kv.second;
            auto origin_column_num = block->columns();
            int result_column_id = -1;
            // PT1 => dest primitive type
            RETURN_IF_ERROR(ctx->execute(block, &result_column_id));
            bool is_origin_column = result_column_id < origin_column_num;
            if (!is_origin_column) {
                // call resize because the first column of _src_block_ptr may not be filled by reader,
                // so _src_block_ptr->rows() may return wrong result, cause the column created by `ctx->execute()`
                // has only one row.
                std::move(*block->get_by_position(result_column_id).column).mutate()->resize(rows);
                auto result_column_ptr = block->get_by_position(result_column_id).column;
                // result_column_ptr maybe a ColumnConst, convert it to a normal column
                result_column_ptr = result_column_ptr->convert_to_full_column_if_const();
                auto origin_column_type = block->get_by_name(kv.first).type;
                bool is_nullable = origin_column_type->is_nullable();
                block->replace_by_position(
                        block->get_position_by_name(kv.first),
                        is_nullable ? make_nullable(result_column_ptr) : result_column_ptr);
                block->erase(result_column_id);
            }
        }
    }
    return Status::OK();
}

void OrcReader::_init_bloom_filter(
        std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    // generate bloom filter
    // _reader->getBloomFilters()
}

void OrcReader::_init_system_properties() {
    if (_scan_range.__isset.file_type) {
        // for compatibility
        _system_properties.system_type = _scan_range.file_type;
    } else {
        _system_properties.system_type = _scan_params.file_type;
    }
    _system_properties.properties = _scan_params.properties;
    _system_properties.hdfs_params = _scan_params.hdfs_params;
    if (_scan_params.__isset.broker_addresses) {
        _system_properties.broker_addresses.assign(_scan_params.broker_addresses.begin(),
                                                   _scan_params.broker_addresses.end());
    }
}

void OrcReader::_init_file_description() {
    _file_description.path = _scan_range.path;
    _file_description.file_size = _scan_range.__isset.file_size ? _scan_range.file_size : -1;
    if (_scan_range.__isset.fs_name) {
        _file_description.fs_name = _scan_range.fs_name;
    }
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
        return TypeDescriptor::create_decimalv3_type(orc_type->getPrecision(),
                                                     orc_type->getScale());
    case orc::TypeKind::DATE:
        return TypeDescriptor(PrimitiveType::TYPE_DATEV2);
    case orc::TypeKind::VARCHAR:
        return TypeDescriptor::create_varchar_type(orc_type->getMaximumLength());
    case orc::TypeKind::CHAR:
        return TypeDescriptor::create_char_type(orc_type->getMaximumLength());
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

template <bool is_filter>
Status OrcReader::_decode_string_column(const std::string& col_name,
                                        const MutableColumnPtr& data_column,
                                        const orc::TypeKind& type_kind, orc::ColumnVectorBatch* cvb,
                                        size_t num_values) {
    SCOPED_RAW_TIMER(&_statistics.decode_value_time);
    auto* data = dynamic_cast<orc::EncodedStringVectorBatch*>(cvb);
    if (data == nullptr) {
        return Status::InternalError("Wrong data type for colum '{}'", col_name);
    }
    if (data->isEncoded) {
        return _decode_string_dict_encoded_column<is_filter>(col_name, data_column, type_kind, data,
                                                             num_values);
    } else {
        return _decode_string_non_dict_encoded_column<is_filter>(col_name, data_column, type_kind,
                                                                 data, num_values);
    }
}

template <bool is_filter>
Status OrcReader::_decode_string_non_dict_encoded_column(const std::string& col_name,
                                                         const MutableColumnPtr& data_column,
                                                         const orc::TypeKind& type_kind,
                                                         orc::EncodedStringVectorBatch* cvb,
                                                         size_t num_values) {
    const static std::string empty_string;
    std::vector<StringRef> string_values;
    string_values.reserve(num_values);
    if (type_kind == orc::TypeKind::CHAR) {
        // Possibly there are some zero padding characters in CHAR type, we have to strip them off.
        if (cvb->hasNulls) {
            for (int i = 0; i < num_values; ++i) {
                if (cvb->notNull[i]) {
                    string_values.emplace_back(cvb->data[i],
                                               trim_right(cvb->data[i], cvb->length[i]));
                } else {
                    // Orc doesn't fill null values in new batch, but the former batch has been release.
                    // Other types like int/long/timestamp... are flat types without pointer in them,
                    // so other types do not need to be handled separately like string.
                    string_values.emplace_back(empty_string.data(), 0);
                }
            }
        } else {
            for (int i = 0; i < num_values; ++i) {
                string_values.emplace_back(cvb->data[i], trim_right(cvb->data[i], cvb->length[i]));
            }
        }
    } else {
        if (cvb->hasNulls) {
            for (int i = 0; i < num_values; ++i) {
                if (cvb->notNull[i]) {
                    string_values.emplace_back(cvb->data[i], cvb->length[i]);
                } else {
                    string_values.emplace_back(empty_string.data(), 0);
                }
            }
        } else {
            for (int i = 0; i < num_values; ++i) {
                string_values.emplace_back(cvb->data[i], cvb->length[i]);
            }
        }
    }
    data_column->insert_many_strings(&string_values[0], num_values);
    return Status::OK();
}

template <bool is_filter>
Status OrcReader::_decode_string_dict_encoded_column(const std::string& col_name,
                                                     const MutableColumnPtr& data_column,
                                                     const orc::TypeKind& type_kind,
                                                     orc::EncodedStringVectorBatch* cvb,
                                                     size_t num_values) {
    std::vector<StringRef> string_values;
    size_t max_value_length = 0;
    string_values.reserve(num_values);
    UInt8* __restrict filter_data;
    if constexpr (is_filter) {
        filter_data = _filter->data();
    }
    if (type_kind == orc::TypeKind::CHAR) {
        // Possibly there are some zero padding characters in CHAR type, we have to strip them off.
        if (cvb->hasNulls) {
            for (int i = 0; i < num_values; ++i) {
                if (cvb->notNull[i]) {
                    if constexpr (is_filter) {
                        if (!filter_data[i]) {
                            string_values.emplace_back(EMPTY_STRING_FOR_OVERFLOW, 0);
                            continue;
                        }
                    }
                    char* val_ptr;
                    int64_t length;
                    cvb->dictionary->getValueByIndex(cvb->index.data()[i], val_ptr, length);
                    length = trim_right(val_ptr, length);
                    if (length > max_value_length) {
                        max_value_length = length;
                    }
                    string_values.emplace_back(val_ptr, length);
                } else {
                    // Orc doesn't fill null values in new batch, but the former batch has been release.
                    // Other types like int/long/timestamp... are flat types without pointer in them,
                    // so other types do not need to be handled separately like string.
                    string_values.emplace_back(EMPTY_STRING_FOR_OVERFLOW, 0);
                }
            }
        } else {
            for (int i = 0; i < num_values; ++i) {
                if constexpr (is_filter) {
                    if (!filter_data[i]) {
                        string_values.emplace_back(EMPTY_STRING_FOR_OVERFLOW, 0);
                        continue;
                    }
                }
                char* val_ptr;
                int64_t length;
                cvb->dictionary->getValueByIndex(cvb->index.data()[i], val_ptr, length);
                length = trim_right(val_ptr, length);
                if (length > max_value_length) {
                    max_value_length = length;
                }
                string_values.emplace_back(val_ptr, length);
            }
        }
    } else {
        if (cvb->hasNulls) {
            for (int i = 0; i < num_values; ++i) {
                if (cvb->notNull[i]) {
                    if constexpr (is_filter) {
                        if (!filter_data[i]) {
                            string_values.emplace_back(EMPTY_STRING_FOR_OVERFLOW, 0);
                            continue;
                        }
                    }
                    char* val_ptr;
                    int64_t length;
                    cvb->dictionary->getValueByIndex(cvb->index.data()[i], val_ptr, length);
                    if (length > max_value_length) {
                        max_value_length = length;
                    }
                    string_values.emplace_back(val_ptr, length);
                } else {
                    string_values.emplace_back(EMPTY_STRING_FOR_OVERFLOW, 0);
                }
            }
        } else {
            for (int i = 0; i < num_values; ++i) {
                if constexpr (is_filter) {
                    if (!filter_data[i]) {
                        string_values.emplace_back(EMPTY_STRING_FOR_OVERFLOW, 0);
                        continue;
                    }
                }
                char* val_ptr;
                int64_t length;
                cvb->dictionary->getValueByIndex(cvb->index.data()[i], val_ptr, length);
                if (length > max_value_length) {
                    max_value_length = length;
                }
                string_values.emplace_back(val_ptr, length);
            }
        }
    }
    data_column->insert_many_strings_overflow(&string_values[0], string_values.size(),
                                              max_value_length);
    return Status::OK();
}

template <bool is_filter>
Status OrcReader::_decode_int32_column(const std::string& col_name,
                                       const MutableColumnPtr& data_column,
                                       orc::ColumnVectorBatch* cvb, size_t num_values) {
    SCOPED_RAW_TIMER(&_statistics.decode_value_time);
    if (dynamic_cast<orc::LongVectorBatch*>(cvb) != nullptr) {
        return _decode_flat_column<Int32, orc::LongVectorBatch>(col_name, data_column, cvb,
                                                                num_values);
    } else if (dynamic_cast<orc::EncodedStringVectorBatch*>(cvb) != nullptr) {
        auto* data = static_cast<orc::EncodedStringVectorBatch*>(cvb);
        if (data == nullptr) {
            return Status::InternalError("Wrong data type for colum '{}'", col_name);
        }
        auto* cvb_data = data->index.data();
        auto& column_data = static_cast<ColumnVector<Int32>&>(*data_column).get_data();
        auto origin_size = column_data.size();
        column_data.resize(origin_size + num_values);
        for (int i = 0; i < num_values; ++i) {
            column_data[origin_size + i] = (Int32)cvb_data[i];
        }
        return Status::OK();
    } else {
        DCHECK(false) << "Bad ColumnVectorBatch type.";
        return Status::InternalError("Bad ColumnVectorBatch type.");
    }
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

template <bool is_filter>
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
    case TypeIndex::Int32:
        return _decode_int32_column<is_filter>(col_name, data_column, cvb, num_values);
    case TypeIndex::Decimal32:
        return _decode_decimal_column<Int32, is_filter>(col_name, data_column, data_type, cvb,
                                                        num_values);
    case TypeIndex::Decimal64:
        return _decode_decimal_column<Int64, is_filter>(col_name, data_column, data_type, cvb,
                                                        num_values);
    case TypeIndex::Decimal128:
        return _decode_decimal_column<Int128, is_filter>(col_name, data_column, data_type, cvb,
                                                         num_values);
    case TypeIndex::Decimal128I:
        return _decode_decimal_column<Int128, is_filter>(col_name, data_column, data_type, cvb,
                                                         num_values);
    case TypeIndex::Date:
        return _decode_time_column<VecDateTimeValue, Int64, orc::LongVectorBatch, is_filter>(
                col_name, data_column, cvb, num_values);
    case TypeIndex::DateV2:
        return _decode_time_column<DateV2Value<DateV2ValueType>, UInt32, orc::LongVectorBatch,
                                   is_filter>(col_name, data_column, cvb, num_values);
    case TypeIndex::DateTime:
        return _decode_time_column<VecDateTimeValue, Int64, orc::TimestampVectorBatch, is_filter>(
                col_name, data_column, cvb, num_values);
    case TypeIndex::DateTimeV2:
        return _decode_time_column<DateV2Value<DateTimeV2ValueType>, UInt64,
                                   orc::TimestampVectorBatch, is_filter>(col_name, data_column, cvb,
                                                                         num_values);
    case TypeIndex::String:
    case TypeIndex::FixedString:
        return _decode_string_column<is_filter>(col_name, data_column, orc_column_type->getKind(),
                                                cvb, num_values);
    case TypeIndex::Array: {
        if (orc_column_type->getKind() != orc::TypeKind::LIST) {
            return Status::InternalError("Wrong data type for colum '{}'", col_name);
        }
        auto* orc_list = dynamic_cast<orc::ListVectorBatch*>(cvb);
        auto& doris_offsets = static_cast<ColumnArray&>(*data_column).get_offsets();
        auto& orc_offsets = orc_list->offsets;
        size_t element_size = 0;
        RETURN_IF_ERROR(_fill_doris_array_offsets(col_name, doris_offsets, orc_offsets, num_values,
                                                  &element_size));
        DataTypePtr& nested_type = const_cast<DataTypePtr&>(
                reinterpret_cast<const DataTypeArray*>(remove_nullable(data_type).get())
                        ->get_nested_type());
        const orc::Type* nested_orc_type = orc_column_type->getSubtype(0);
        return _orc_column_to_doris_column<is_filter>(
                col_name, static_cast<ColumnArray&>(*data_column).get_data_ptr(), nested_type,
                nested_orc_type, orc_list->elements.get(), element_size);
    }
    case TypeIndex::Map: {
        if (orc_column_type->getKind() != orc::TypeKind::MAP) {
            return Status::InternalError("Wrong data type for colum '{}'", col_name);
        }
        auto* orc_map = dynamic_cast<orc::MapVectorBatch*>(cvb);
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
        RETURN_IF_ERROR(_orc_column_to_doris_column<is_filter>(col_name, doris_key_column,
                                                               doris_key_type, orc_key_type,
                                                               orc_map->keys.get(), element_size));
        return _orc_column_to_doris_column<is_filter>(col_name, doris_value_column,
                                                      doris_value_type, orc_value_type,
                                                      orc_map->elements.get(), element_size);
    }
    case TypeIndex::Struct: {
        if (orc_column_type->getKind() != orc::TypeKind::STRUCT) {
            return Status::InternalError("Wrong data type for colum '{}'", col_name);
        }
        auto* orc_struct = dynamic_cast<orc::StructVectorBatch*>(cvb);
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
            RETURN_IF_ERROR(_orc_column_to_doris_column<is_filter>(
                    col_name, doris_field, doris_type, orc_type, orc_field, num_values));
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
    if (_push_down_agg_type == TPushAggOp::type::COUNT) {
        auto rows = std::min(get_remaining_rows(), (int64_t)_batch_size);

        set_remaining_rows(get_remaining_rows() - rows);

        for (auto& col : block->mutate_columns()) {
            col->resize(rows);
        }

        *read_rows = rows;
        if (get_remaining_rows() == 0) {
            *eof = true;
        }
        return Status::OK();
    }

    if (_lazy_read_ctx.can_lazy_read) {
        std::vector<uint32_t> columns_to_filter;
        int column_to_keep = block->columns();
        columns_to_filter.resize(column_to_keep);
        for (uint32_t i = 0; i < column_to_keep; ++i) {
            columns_to_filter[i] = i;
        }
        uint64_t rr;
        SCOPED_RAW_TIMER(&_statistics.column_read_time);
        {
            SCOPED_RAW_TIMER(&_statistics.get_batch_time);
            // reset decimal_scale_params_index;
            _decimal_scale_params_index = 0;
            try {
                rr = _row_reader->nextBatch(*_batch, block);
                if (rr == 0) {
                    *eof = true;
                    *read_rows = 0;
                    return Status::OK();
                }
            } catch (std::exception& e) {
                return Status::InternalError("Orc row reader nextBatch failed. reason = {}",
                                             e.what());
            }
        }

        std::vector<orc::ColumnVectorBatch*> batch_vec;
        _fill_batch_vec(batch_vec, _batch.get(), 0);
        for (auto& col_name : _lazy_read_ctx.lazy_read_columns) {
            auto& column_with_type_and_name = block->get_by_name(col_name);
            auto& column_ptr = column_with_type_and_name.column;
            auto& column_type = column_with_type_and_name.type;
            auto orc_col_idx = _colname_to_idx.find(col_name);
            if (orc_col_idx == _colname_to_idx.end()) {
                return Status::InternalError("Wrong read column '{}' in orc file", col_name);
            }
            RETURN_IF_ERROR(_orc_column_to_doris_column<true>(
                    col_name, column_ptr, column_type, _col_orc_type[orc_col_idx->second],
                    batch_vec[orc_col_idx->second], _batch->numElements));
        }
        *read_rows = rr;

        RETURN_IF_ERROR(_fill_partition_columns(block, _batch->numElements,
                                                _lazy_read_ctx.partition_columns));
        RETURN_IF_ERROR(
                _fill_missing_columns(block, _batch->numElements, _lazy_read_ctx.missing_columns));

        if (block->rows() == 0) {
            *eof = true;
            return Status::OK();
        }

        if (!_not_single_slot_filter_conjuncts.empty()) {
            std::vector<IColumn::Filter*> filters;
            filters.push_back(_filter.get());
            RETURN_IF_CATCH_EXCEPTION(
                    RETURN_IF_ERROR(VExprContext::execute_conjuncts_and_filter_block(
                            _not_single_slot_filter_conjuncts, &filters, block, columns_to_filter,
                            column_to_keep)));
        } else {
            RETURN_IF_CATCH_EXCEPTION(
                    Block::filter_block_internal(block, columns_to_filter, *_filter));
            Block::erase_useless_column(block, column_to_keep);
        }
    } else {
        uint64_t rr;
        SCOPED_RAW_TIMER(&_statistics.column_read_time);
        {
            SCOPED_RAW_TIMER(&_statistics.get_batch_time);
            // reset decimal_scale_params_index;
            _decimal_scale_params_index = 0;
            try {
                rr = _row_reader->nextBatch(*_batch, block);
                if (rr == 0) {
                    *eof = true;
                    *read_rows = 0;
                    return Status::OK();
                }
            } catch (std::exception& e) {
                return Status::InternalError("Orc row reader nextBatch failed. reason = {}",
                                             e.what());
            }
        }

        for (auto& dict_filter_cols : _dict_filter_cols) {
            MutableColumnPtr dict_col_ptr = ColumnVector<Int32>::create();
            size_t pos = block->get_position_by_name(dict_filter_cols.first);
            auto& column_with_type_and_name = block->get_by_position(pos);
            auto& column_type = column_with_type_and_name.type;
            if (column_type->is_nullable()) {
                block->get_by_position(pos).type =
                        std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
                block->replace_by_position(
                        pos, ColumnNullable::create(std::move(dict_col_ptr),
                                                    ColumnUInt8::create(dict_col_ptr->size(), 0)));
            } else {
                block->get_by_position(pos).type = std::make_shared<DataTypeInt32>();
                block->replace_by_position(pos, std::move(dict_col_ptr));
            }
        }

        std::vector<orc::ColumnVectorBatch*> batch_vec;
        _fill_batch_vec(batch_vec, _batch.get(), 0);

        for (auto& col_name : _lazy_read_ctx.all_read_columns) {
            auto& column_with_type_and_name = block->get_by_name(col_name);
            auto& column_ptr = column_with_type_and_name.column;
            auto& column_type = column_with_type_and_name.type;
            auto orc_col_idx = _colname_to_idx.find(col_name);
            if (orc_col_idx == _colname_to_idx.end()) {
                return Status::InternalError("Wrong read column '{}' in orc file", col_name);
            }
            RETURN_IF_ERROR(_orc_column_to_doris_column(
                    col_name, column_ptr, column_type, _col_orc_type[orc_col_idx->second],
                    batch_vec[orc_col_idx->second], _batch->numElements));
        }
        *read_rows = rr;

        RETURN_IF_ERROR(_fill_partition_columns(block, _batch->numElements,
                                                _lazy_read_ctx.partition_columns));
        RETURN_IF_ERROR(
                _fill_missing_columns(block, _batch->numElements, _lazy_read_ctx.missing_columns));

        if (block->rows() == 0) {
            static_cast<void>(_convert_dict_cols_to_string_cols(block, nullptr));
            *eof = true;
            return Status::OK();
        }

        _build_delete_row_filter(block, _batch->numElements);

        std::vector<uint32_t> columns_to_filter;
        int column_to_keep = block->columns();
        columns_to_filter.resize(column_to_keep);
        for (uint32_t i = 0; i < column_to_keep; ++i) {
            columns_to_filter[i] = i;
        }
        if (!_lazy_read_ctx.conjuncts.empty()) {
            VExprContextSPtrs filter_conjuncts;
            filter_conjuncts.insert(filter_conjuncts.end(), _filter_conjuncts.begin(),
                                    _filter_conjuncts.end());
            for (auto& conjunct : _dict_filter_conjuncts) {
                filter_conjuncts.emplace_back(conjunct);
            }
            for (auto& conjunct : _non_dict_filter_conjuncts) {
                filter_conjuncts.emplace_back(conjunct);
            }
            std::vector<IColumn::Filter*> filters;
            if (_delete_rows_filter_ptr) {
                filters.push_back(_delete_rows_filter_ptr.get());
            }
            IColumn::Filter result_filter(block->rows(), 1);
            bool can_filter_all = false;
            RETURN_IF_ERROR_OR_CATCH_EXCEPTION(VExprContext::execute_conjuncts(
                    filter_conjuncts, &filters, block, &result_filter, &can_filter_all));
            if (can_filter_all) {
                for (auto& col : columns_to_filter) {
                    std::move(*block->get_by_position(col).column).assume_mutable()->clear();
                }
                Block::erase_useless_column(block, column_to_keep);
                static_cast<void>(_convert_dict_cols_to_string_cols(block, &batch_vec));
                return Status::OK();
            }
            if (!_not_single_slot_filter_conjuncts.empty()) {
                static_cast<void>(_convert_dict_cols_to_string_cols(block, &batch_vec));
                std::vector<IColumn::Filter*> merged_filters;
                merged_filters.push_back(&result_filter);
                RETURN_IF_CATCH_EXCEPTION(
                        RETURN_IF_ERROR(VExprContext::execute_conjuncts_and_filter_block(
                                _not_single_slot_filter_conjuncts, &merged_filters, block,
                                columns_to_filter, column_to_keep)));
            } else {
                RETURN_IF_CATCH_EXCEPTION(
                        Block::filter_block_internal(block, columns_to_filter, result_filter));
                Block::erase_useless_column(block, column_to_keep);
                static_cast<void>(_convert_dict_cols_to_string_cols(block, &batch_vec));
            }
        } else {
            if (_delete_rows_filter_ptr) {
                RETURN_IF_CATCH_EXCEPTION(Block::filter_block_internal(block, columns_to_filter,
                                                                       (*_delete_rows_filter_ptr)));
            }
            Block::erase_useless_column(block, column_to_keep);
            static_cast<void>(_convert_dict_cols_to_string_cols(block, &batch_vec));
        }
    }
    return Status::OK();
}

void OrcReader::_fill_batch_vec(std::vector<orc::ColumnVectorBatch*>& result,
                                orc::ColumnVectorBatch* batch, int idx) {
    for (auto* field : dynamic_cast<orc::StructVectorBatch*>(batch)->fields) {
        result.push_back(field);
        if (_is_acid && _col_orc_type[idx++]->getKind() == orc::TypeKind::STRUCT) {
            _fill_batch_vec(result, field, idx);
        }
    }
}

void OrcReader::_build_delete_row_filter(const Block* block, size_t rows) {
    // transactional hive orc delete row
    if (_delete_rows != nullptr) {
        _delete_rows_filter_ptr.reset(new IColumn::Filter(rows, 1));
        auto* __restrict _pos_delete_filter_data = _delete_rows_filter_ptr->data();
        const ColumnInt64& original_transaction_column =
                assert_cast<const ColumnInt64&>(*remove_nullable(
                        block->get_by_name(TransactionalHive::ORIGINAL_TRANSACTION_LOWER_CASE)
                                .column));
        const ColumnInt32& bucket_id_column = assert_cast<const ColumnInt32&>(
                *remove_nullable(block->get_by_name(TransactionalHive::BUCKET_LOWER_CASE).column));
        const ColumnInt64& row_id_column = assert_cast<const ColumnInt64&>(
                *remove_nullable(block->get_by_name(TransactionalHive::ROW_ID_LOWER_CASE).column));
        for (int i = 0; i < rows; ++i) {
            Int64 original_transaction = original_transaction_column.get_int(i);
            Int32 bucket_id = bucket_id_column.get_int(i);
            Int64 row_id = row_id_column.get_int(i);

            TransactionalHiveReader::AcidRowID transactional_row_id = {original_transaction,
                                                                       bucket_id, row_id};
            if (_delete_rows->contains(transactional_row_id)) {
                _pos_delete_filter_data[i] = 0;
            }
        }
    }
}

Status OrcReader::filter(orc::ColumnVectorBatch& data, uint16_t* sel, uint16_t size, void* arg) {
    Block* block = (Block*)arg;
    size_t origin_column_num = block->columns();

    for (auto& dict_filter_cols : _dict_filter_cols) {
        MutableColumnPtr dict_col_ptr = ColumnVector<Int32>::create();
        size_t pos = block->get_position_by_name(dict_filter_cols.first);
        auto& column_with_type_and_name = block->get_by_position(pos);
        auto& column_type = column_with_type_and_name.type;
        if (column_type->is_nullable()) {
            block->get_by_position(pos).type =
                    std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
            block->replace_by_position(
                    pos, ColumnNullable::create(std::move(dict_col_ptr),
                                                ColumnUInt8::create(dict_col_ptr->size(), 0)));
        } else {
            block->get_by_position(pos).type = std::make_shared<DataTypeInt32>();
            block->replace_by_position(pos, std::move(dict_col_ptr));
        }
    }
    std::vector<orc::ColumnVectorBatch*> batch_vec;
    _fill_batch_vec(batch_vec, &data, 0);
    std::vector<string> col_names;
    col_names.insert(col_names.end(), _lazy_read_ctx.predicate_columns.first.begin(),
                     _lazy_read_ctx.predicate_columns.first.end());
    if (_is_acid) {
        col_names.insert(col_names.end(),
                         TransactionalHive::READ_ROW_COLUMN_NAMES_LOWER_CASE.begin(),
                         TransactionalHive::READ_ROW_COLUMN_NAMES_LOWER_CASE.end());
    }
    for (auto& col_name : col_names) {
        auto& column_with_type_and_name = block->get_by_name(col_name);
        auto& column_ptr = column_with_type_and_name.column;
        auto& column_type = column_with_type_and_name.type;
        auto orc_col_idx = _colname_to_idx.find(col_name);
        if (orc_col_idx == _colname_to_idx.end()) {
            return Status::InternalError("Wrong read column '{}' in orc file", col_name);
        }
        RETURN_IF_ERROR(_orc_column_to_doris_column(
                col_name, column_ptr, column_type, _col_orc_type[orc_col_idx->second],
                batch_vec[orc_col_idx->second], data.numElements));
    }
    RETURN_IF_ERROR(
            _fill_partition_columns(block, size, _lazy_read_ctx.predicate_partition_columns));
    RETURN_IF_ERROR(_fill_missing_columns(block, size, _lazy_read_ctx.predicate_missing_columns));
    if (_lazy_read_ctx.resize_first_column) {
        // VExprContext.execute has an optimization, the filtering is executed when block->rows() > 0
        // The following process may be tricky and time-consuming, but we have no other way.
        block->get_by_position(0).column->assume_mutable()->resize(size);
    }

    // transactional hive orc delete row
    _build_delete_row_filter(block, size);

    _filter.reset(new IColumn::Filter(size, 1));
    auto* __restrict result_filter_data = _filter->data();
    bool can_filter_all = false;
    VExprContextSPtrs filter_conjuncts;
    filter_conjuncts.insert(filter_conjuncts.end(), _filter_conjuncts.begin(),
                            _filter_conjuncts.end());
    for (auto& conjunct : _dict_filter_conjuncts) {
        filter_conjuncts.emplace_back(conjunct);
    }
    for (auto& conjunct : _non_dict_filter_conjuncts) {
        filter_conjuncts.emplace_back(conjunct);
    }
    std::vector<IColumn::Filter*> filters;
    if (_delete_rows_filter_ptr) {
        filters.push_back(_delete_rows_filter_ptr.get());
    }
    RETURN_IF_ERROR_OR_CATCH_EXCEPTION(VExprContext::execute_conjuncts(
            filter_conjuncts, &filters, block, _filter.get(), &can_filter_all));

    if (_lazy_read_ctx.resize_first_column) {
        // We have to clean the first column to insert right data.
        block->get_by_position(0).column->assume_mutable()->clear();
    }

    if (can_filter_all) {
        for (auto& col : col_names) {
            // clean block to read predicate columns and acid columns
            block->get_by_name(col).column->assume_mutable()->clear();
        }
        for (auto& col : _lazy_read_ctx.predicate_partition_columns) {
            block->get_by_name(col.first).column->assume_mutable()->clear();
        }
        for (auto& col : _lazy_read_ctx.predicate_missing_columns) {
            block->get_by_name(col.first).column->assume_mutable()->clear();
        }
        Block::erase_useless_column(block, origin_column_num);
    }

    uint16_t new_size = 0;
    for (uint16_t i = 0; i < size; i++) {
        sel[new_size] = i;
        new_size += result_filter_data[i] ? 1 : 0;
    }
    data.numElements = new_size;
    if (data.numElements > 0) {
        static_cast<void>(_convert_dict_cols_to_string_cols(block, &batch_vec));
    } else {
        static_cast<void>(_convert_dict_cols_to_string_cols(block, nullptr));
    }
    return Status::OK();
}

Status OrcReader::fill_dict_filter_column_names(
        std::unique_ptr<orc::StripeInformation> current_strip_information,
        std::list<std::string>& column_names) {
    // Check if single slot can be filtered by dict.
    if (!_slot_id_to_filter_conjuncts) {
        return Status::OK();
    }
    _obj_pool->clear();
    _dict_filter_cols.clear();
    _dict_filter_conjuncts.clear();
    _non_dict_filter_conjuncts.clear();

    const std::list<string>& predicate_col_names = _lazy_read_ctx.predicate_columns.first;
    const std::vector<int>& predicate_col_slot_ids = _lazy_read_ctx.predicate_columns.second;
    int i = 0;
    for (auto& predicate_col_name : predicate_col_names) {
        int slot_id = predicate_col_slot_ids[i];
        if (_can_filter_by_dict(slot_id)) {
            _dict_filter_cols.emplace_back(std::make_pair(predicate_col_name, slot_id));
            column_names.emplace_back(_col_name_to_file_col_name[predicate_col_name]);
        } else {
            if (_slot_id_to_filter_conjuncts->find(slot_id) !=
                _slot_id_to_filter_conjuncts->end()) {
                for (auto& ctx : _slot_id_to_filter_conjuncts->at(slot_id)) {
                    _non_dict_filter_conjuncts.push_back(ctx);
                }
            }
        }
        ++i;
    }
    return Status::OK();
}

bool OrcReader::_can_filter_by_dict(int slot_id) {
    SlotDescriptor* slot = nullptr;
    const std::vector<SlotDescriptor*>& slots = _tuple_descriptor->slots();
    for (auto each : slots) {
        if (each->id() == slot_id) {
            slot = each;
            break;
        }
    }
    if (!slot->type().is_string_type()) {
        return false;
    }

    if (_slot_id_to_filter_conjuncts->find(slot_id) == _slot_id_to_filter_conjuncts->end()) {
        return false;
    }

    // TODO：check expr like 'a > 10 is null', 'a > 10' should can be filter by dict.
    std::function<bool(const VExpr* expr)> visit_function_call = [&](const VExpr* expr) {
        if (expr->node_type() == TExprNodeType::FUNCTION_CALL) {
            std::string is_null_str;
            std::string function_name = expr->fn().name.function_name;
            if (function_name.compare("is_null_pred") == 0 ||
                function_name.compare("is_not_null_pred") == 0) {
                return false;
            }
        } else {
            for (auto& child : expr->children()) {
                if (!visit_function_call(child.get())) {
                    return false;
                }
            }
        }
        return true;
    };
    for (auto& ctx : _slot_id_to_filter_conjuncts->at(slot_id)) {
        if (!visit_function_call(ctx->root().get())) {
            return false;
        }
    }
    return true;
}

Status OrcReader::on_string_dicts_loaded(
        std::unordered_map<std::string, orc::StringDictionary*>& file_column_name_to_dict_map,
        bool* is_stripe_filtered) {
    *is_stripe_filtered = false;
    for (auto it = _dict_filter_cols.begin(); it != _dict_filter_cols.end();) {
        std::string& dict_filter_col_name = it->first;
        int slot_id = it->second;

        // Can not dict filter col find because stripe is not dict encoded, then remove it.
        VExprContextSPtrs ctxs;
        auto iter = _slot_id_to_filter_conjuncts->find(slot_id);
        if (iter != _slot_id_to_filter_conjuncts->end()) {
            for (auto& ctx : iter->second) {
                ctxs.push_back(ctx);
            }
        } else {
            std::stringstream msg;
            msg << "_slot_id_to_filter_conjuncts: slot_id [" << slot_id << "] not found";
            return Status::NotFound(msg.str());
        }
        auto file_column_name_to_dict_map_iter =
                file_column_name_to_dict_map.find(_col_name_to_file_col_name[dict_filter_col_name]);
        if (file_column_name_to_dict_map_iter == file_column_name_to_dict_map.end()) {
            it = _dict_filter_cols.erase(it);
            for (auto& ctx : ctxs) {
                _non_dict_filter_conjuncts.emplace_back(ctx);
            }
            continue;
        }

        // 1. Get dictionary values to a string column.
        MutableColumnPtr dict_value_column = ColumnString::create();
        orc::StringDictionary* dict = file_column_name_to_dict_map_iter->second;

        std::vector<StringRef> dict_values;
        std::unordered_map<StringRef, int64_t> dict_value_to_code;
        size_t max_value_length = 0;
        uint64_t dictionaryCount = dict->dictionaryOffset.size() - 1;
        if (dictionaryCount == 0) {
            it = _dict_filter_cols.erase(it);
            for (auto& ctx : ctxs) {
                _non_dict_filter_conjuncts.emplace_back(ctx);
            }
            continue;
        }
        dict_values.reserve(dictionaryCount);
        for (int i = 0; i < dictionaryCount; ++i) {
            char* val_ptr;
            int64_t length;
            dict->getValueByIndex(i, val_ptr, length);
            StringRef dict_value(val_ptr, length);
            if (length > max_value_length) {
                max_value_length = length;
            }
            dict_values.emplace_back(dict_value);
            dict_value_to_code[dict_value] = i;
        }
        dict_value_column->insert_many_strings_overflow(&dict_values[0], dict_values.size(),
                                                        max_value_length);
        size_t dict_value_column_size = dict_value_column->size();
        // 2. Build a temp block from the dict string column, then execute conjuncts and filter block.
        // 2.1 Build a temp block from the dict string column to match the conjuncts executing.
        Block temp_block;
        int dict_pos = -1;
        int index = 0;
        for (const auto slot_desc : _tuple_descriptor->slots()) {
            if (!slot_desc->need_materialize()) {
                // should be ignored from reading
                continue;
            }
            if (slot_desc->id() == slot_id) {
                auto data_type = slot_desc->get_data_type_ptr();
                if (data_type->is_nullable()) {
                    temp_block.insert(
                            {ColumnNullable::create(std::move(dict_value_column),
                                                    ColumnUInt8::create(dict_value_column_size, 0)),
                             std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()),
                             ""});
                } else {
                    temp_block.insert(
                            {std::move(dict_value_column), std::make_shared<DataTypeString>(), ""});
                }
                dict_pos = index;

            } else {
                temp_block.insert(ColumnWithTypeAndName(slot_desc->get_empty_mutable_column(),
                                                        slot_desc->get_data_type_ptr(),
                                                        slot_desc->col_name()));
            }
            ++index;
        }

        // 2.2 Execute conjuncts and filter block.
        std::vector<uint32_t> columns_to_filter(1, dict_pos);
        int column_to_keep = temp_block.columns();
        if (dict_pos != 0) {
            // VExprContext.execute has an optimization, the filtering is executed when block->rows() > 0
            // The following process may be tricky and time-consuming, but we have no other way.
            temp_block.get_by_position(0).column->assume_mutable()->resize(dict_value_column_size);
        }
        RETURN_IF_CATCH_EXCEPTION(RETURN_IF_ERROR(VExprContext::execute_conjuncts_and_filter_block(
                ctxs, nullptr, &temp_block, columns_to_filter, column_to_keep)));
        if (dict_pos != 0) {
            // We have to clean the first column to insert right data.
            temp_block.get_by_position(0).column->assume_mutable()->clear();
        }

        // Check some conditions.
        ColumnPtr& dict_column = temp_block.get_by_position(dict_pos).column;
        // If dict_column->size() == 0, can filter this stripe.
        if (dict_column->size() == 0) {
            *is_stripe_filtered = true;
            return Status::OK();
        }

        // About Performance: if dict_column size is too large, it will generate a large IN filter.
        if (dict_column->size() > MAX_DICT_CODE_PREDICATE_TO_REWRITE) {
            it = _dict_filter_cols.erase(it);
            for (auto& ctx : ctxs) {
                _non_dict_filter_conjuncts.emplace_back(ctx);
            }
            continue;
        }

        // 3. Get dict codes.
        std::vector<int32_t> dict_codes;
        if (dict_column->is_nullable()) {
            const ColumnNullable* nullable_column =
                    static_cast<const ColumnNullable*>(dict_column.get());
            const ColumnString* nested_column = static_cast<const ColumnString*>(
                    nullable_column->get_nested_column_ptr().get());
            for (int i = 0; i < nested_column->size(); ++i) {
                StringRef dict_value = nested_column->get_data_at(i);
                dict_codes.emplace_back(dict_value_to_code[dict_value]);
            }
        } else {
            for (int i = 0; i < dict_column->size(); ++i) {
                StringRef dict_value = dict_column->get_data_at(i);
                dict_codes.emplace_back(dict_value_to_code[dict_value]);
            }
        }

        // 4. Rewrite conjuncts.
        static_cast<void>(_rewrite_dict_conjuncts(dict_codes, slot_id, dict_column->is_nullable()));
        ++it;
    }
    return Status::OK();
}

Status OrcReader::_rewrite_dict_conjuncts(std::vector<int32_t>& dict_codes, int slot_id,
                                          bool is_nullable) {
    VExprSPtr root;
    if (dict_codes.size() == 1) {
        {
            TFunction fn;
            TFunctionName fn_name;
            fn_name.__set_db_name("");
            fn_name.__set_function_name("eq");
            fn.__set_name(fn_name);
            fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
            std::vector<TTypeDesc> arg_types;
            arg_types.push_back(create_type_desc(PrimitiveType::TYPE_INT));
            arg_types.push_back(create_type_desc(PrimitiveType::TYPE_INT));
            fn.__set_arg_types(arg_types);
            fn.__set_ret_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
            fn.__set_has_var_args(false);

            TExprNode texpr_node;
            texpr_node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
            texpr_node.__set_node_type(TExprNodeType::BINARY_PRED);
            texpr_node.__set_opcode(TExprOpcode::EQ);
            texpr_node.__set_fn(fn);
            texpr_node.__set_child_type(TPrimitiveType::INT);
            texpr_node.__set_num_children(2);
            texpr_node.__set_is_nullable(is_nullable);
            root = VectorizedFnCall::create_shared(texpr_node);
        }
        {
            SlotDescriptor* slot = nullptr;
            const std::vector<SlotDescriptor*>& slots = _tuple_descriptor->slots();
            for (auto each : slots) {
                if (each->id() == slot_id) {
                    slot = each;
                    break;
                }
            }
            root->add_child(VSlotRef::create_shared(slot));
        }
        {
            TExprNode texpr_node;
            texpr_node.__set_node_type(TExprNodeType::INT_LITERAL);
            texpr_node.__set_type(create_type_desc(TYPE_INT));
            TIntLiteral int_literal;
            int_literal.__set_value(dict_codes[0]);
            texpr_node.__set_int_literal(int_literal);
            texpr_node.__set_is_nullable(is_nullable);
            root->add_child(VLiteral::create_shared(texpr_node));
        }
    } else {
        {
            TTypeDesc type_desc = create_type_desc(PrimitiveType::TYPE_BOOLEAN);
            TExprNode node;
            node.__set_type(type_desc);
            node.__set_node_type(TExprNodeType::IN_PRED);
            node.in_predicate.__set_is_not_in(false);
            node.__set_opcode(TExprOpcode::FILTER_IN);
            // VdirectInPredicate assume is_nullable = false.
            node.__set_is_nullable(false);

            root = vectorized::VDirectInPredicate::create_shared(node);
            std::shared_ptr<HybridSetBase> hybrid_set(
                    create_set(PrimitiveType::TYPE_INT, dict_codes.size()));
            for (int j = 0; j < dict_codes.size(); ++j) {
                hybrid_set->insert(&dict_codes[j]);
            }
            static_cast<vectorized::VDirectInPredicate*>(root.get())->set_filter(hybrid_set);
        }
        {
            SlotDescriptor* slot = nullptr;
            const std::vector<SlotDescriptor*>& slots = _tuple_descriptor->slots();
            for (auto each : slots) {
                if (each->id() == slot_id) {
                    slot = each;
                    break;
                }
            }
            root->add_child(VSlotRef::create_shared(slot));
        }
    }
    VExprContextSPtr rewritten_conjunct_ctx = VExprContext::create_shared(root);
    RETURN_IF_ERROR(rewritten_conjunct_ctx->prepare(_state, *_row_descriptor));
    RETURN_IF_ERROR(rewritten_conjunct_ctx->open(_state));
    _dict_filter_conjuncts.emplace_back(rewritten_conjunct_ctx);
    return Status::OK();
}

Status OrcReader::_convert_dict_cols_to_string_cols(
        Block* block, const std::vector<orc::ColumnVectorBatch*>* batch_vec) {
    for (auto& dict_filter_cols : _dict_filter_cols) {
        size_t pos = block->get_position_by_name(dict_filter_cols.first);
        ColumnWithTypeAndName& column_with_type_and_name = block->get_by_position(pos);
        const ColumnPtr& column = column_with_type_and_name.column;
        auto orc_col_idx = _colname_to_idx.find(dict_filter_cols.first);
        if (orc_col_idx == _colname_to_idx.end()) {
            return Status::InternalError("Wrong read column '{}' in orc file",
                                         dict_filter_cols.first);
        }
        if (auto* nullable_column = check_and_get_column<ColumnNullable>(*column)) {
            const ColumnPtr& nested_column = nullable_column->get_nested_column_ptr();
            const ColumnInt32* dict_column = assert_cast<const ColumnInt32*>(nested_column.get());
            DCHECK(dict_column);
            const NullMap& null_map = nullable_column->get_null_map_data();

            MutableColumnPtr string_column;
            if (batch_vec != nullptr) {
                string_column = _convert_dict_column_to_string_column(
                        dict_column, &null_map, (*batch_vec)[orc_col_idx->second],
                        _col_orc_type[orc_col_idx->second]);
            } else {
                string_column = ColumnString::create();
            }

            column_with_type_and_name.type =
                    std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
            block->replace_by_position(
                    pos, ColumnNullable::create(std::move(string_column),
                                                nullable_column->get_null_map_column_ptr()));
        } else {
            const ColumnInt32* dict_column = assert_cast<const ColumnInt32*>(column.get());
            MutableColumnPtr string_column;
            if (batch_vec != nullptr) {
                string_column = _convert_dict_column_to_string_column(
                        dict_column, nullptr, (*batch_vec)[orc_col_idx->second],
                        _col_orc_type[orc_col_idx->second]);
            } else {
                string_column = ColumnString::create();
            }

            column_with_type_and_name.type = std::make_shared<DataTypeString>();
            block->replace_by_position(pos, std::move(string_column));
        }
    }
    return Status::OK();
}

// TODO: Possible optimization points.
//  After filtering the dict column, the null_map for the null dict column should always not be null.
//  Then it can avoid checking null_map. However, currently when inert materialization is enabled,
//  the filter column will not be filtered first, but will be filtered together at the end.
MutableColumnPtr OrcReader::_convert_dict_column_to_string_column(
        const ColumnInt32* dict_column, const NullMap* null_map, orc::ColumnVectorBatch* cvb,
        const orc::Type* orc_column_type) {
    SCOPED_RAW_TIMER(&_statistics.decode_value_time);
    auto res = ColumnString::create();
    auto* encoded_string_vector_batch = static_cast<orc::EncodedStringVectorBatch*>(cvb);
    DCHECK(encoded_string_vector_batch);
    std::vector<StringRef> string_values;
    size_t num_values = dict_column->size();
    const int* dict_data = dict_column->get_data().data();
    string_values.reserve(num_values);
    size_t max_value_length = 0;
    auto* null_map_data = null_map->data();
    if (orc_column_type->getKind() == orc::TypeKind::CHAR) {
        // Possibly there are some zero padding characters in CHAR type, we have to strip them off.
        if (null_map) {
            for (int i = 0; i < num_values; ++i) {
                if (!null_map_data[i]) {
                    char* val_ptr;
                    int64_t length;
                    encoded_string_vector_batch->dictionary->getValueByIndex(dict_data[i], val_ptr,
                                                                             length);
                    length = trim_right(val_ptr, length);
                    if (length > max_value_length) {
                        max_value_length = length;
                    }
                    string_values.emplace_back(val_ptr, length);
                } else {
                    // Orc doesn't fill null values in new batch, but the former batch has been release.
                    // Other types like int/long/timestamp... are flat types without pointer in them,
                    // so other types do not need to be handled separately like string.
                    string_values.emplace_back(EMPTY_STRING_FOR_OVERFLOW, 0);
                }
            }
        } else {
            for (int i = 0; i < num_values; ++i) {
                char* val_ptr;
                int64_t length;
                encoded_string_vector_batch->dictionary->getValueByIndex(dict_data[i], val_ptr,
                                                                         length);
                length = trim_right(val_ptr, length);
                if (length > max_value_length) {
                    max_value_length = length;
                }
                string_values.emplace_back(val_ptr, length);
            }
        }
    } else {
        if (null_map) {
            for (int i = 0; i < num_values; ++i) {
                if (!null_map_data[i]) {
                    char* val_ptr;
                    int64_t length;
                    encoded_string_vector_batch->dictionary->getValueByIndex(dict_data[i], val_ptr,
                                                                             length);
                    if (length > max_value_length) {
                        max_value_length = length;
                    }
                    string_values.emplace_back(val_ptr, length);
                } else {
                    string_values.emplace_back(EMPTY_STRING_FOR_OVERFLOW, 0);
                }
            }
        } else {
            for (int i = 0; i < num_values; ++i) {
                char* val_ptr;
                int64_t length;
                encoded_string_vector_batch->dictionary->getValueByIndex(dict_data[i], val_ptr,
                                                                         length);
                if (length > max_value_length) {
                    max_value_length = length;
                }
                string_values.emplace_back(val_ptr, length);
            }
        }
    }
    res->insert_many_strings_overflow(&string_values[0], num_values, max_value_length);
    return res;
}

void ORCFileInputStream::beforeReadStripe(
        std::unique_ptr<orc::StripeInformation> current_strip_information,
        std::vector<bool> selected_columns) {
    // Generate prefetch ranges, build stripe file reader.
    uint64_t offset = current_strip_information->getOffset();
    std::vector<io::PrefetchRange> prefetch_ranges;
    for (uint64_t stream_id = 0; stream_id < current_strip_information->getNumberOfStreams();
         ++stream_id) {
        std::unique_ptr<orc::StreamInformation> stream =
                current_strip_information->getStreamInformation(stream_id);
        uint32_t columnId = stream->getColumnId();
        uint64_t length = stream->getLength();
        if (selected_columns[columnId]) {
            doris::io::PrefetchRange prefetch_range = {offset, offset + length};
            prefetch_ranges.emplace_back(std::move(prefetch_range));
        }
        offset += length;
    }
    // The underlying page reader will prefetch data in column.
    _file_reader.reset(new io::MergeRangeFileReader(_profile, _inner_reader, prefetch_ranges));
}

} // namespace doris::vectorized
