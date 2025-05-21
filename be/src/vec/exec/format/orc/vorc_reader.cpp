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
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/Opcodes_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>

#include <algorithm>
#include <cctype>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <exception>
#include <iterator>
#include <map>
#include <memory>
#include <ostream>
#include <tuple>
#include <utility>

#include "absl/strings/substitute.h"
#include "cctz/civil_time.h"
#include "cctz/time_zone.h"
#include "common/exception.h"
#include "exprs/create_predicate_function.h"
#include "exprs/hybrid_set.h"
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
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/runtime_profile.h"
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
#include "vec/exec/format/orc/orc_file_reader.h"
#include "vec/exec/format/table/transactional_hive_common.h"
#include "vec/exprs/vbloom_predicate.h"
#include "vec/exprs/vdirect_in_predicate.h"
#include "vec/exprs/vectorized_fn_call.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/exprs/vin_predicate.h"
#include "vec/exprs/vruntimefilter_wrapper.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
class RuntimeState;
namespace io {
struct IOContext;
enum class FileCachePolicy : uint8_t;
} // namespace io
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"
// TODO: we need to determine it by test.
static constexpr uint32_t MAX_DICT_CODE_PREDICATE_TO_REWRITE = std::numeric_limits<uint32_t>::max();
static constexpr char EMPTY_STRING_FOR_OVERFLOW[ColumnString::MAX_STRINGS_OVERFLOW_SIZE] = "";
// Because HIVE 0.11 & 0.12 does not support precision and scale for decimal
// The decimal type of orc file produced by HIVE 0.11 & 0.12 are DECIMAL(0,0)
// We should set a default precision and scale for these orc files.
static constexpr int decimal_precision_for_hive11 = BeConsts::MAX_DECIMAL128_PRECISION;
static constexpr int decimal_scale_for_hive11 = 10;

#define FOR_FLAT_ORC_COLUMNS(M)                                   \
    M(PrimitiveType::TYPE_TINYINT, Int8, orc::LongVectorBatch)    \
    M(PrimitiveType::TYPE_BOOLEAN, UInt8, orc::LongVectorBatch)   \
    M(PrimitiveType::TYPE_SMALLINT, Int16, orc::LongVectorBatch)  \
    M(PrimitiveType::TYPE_BIGINT, Int64, orc::LongVectorBatch)    \
    M(PrimitiveType::TYPE_FLOAT, Float32, orc::DoubleVectorBatch) \
    M(PrimitiveType::TYPE_DOUBLE, Float64, orc::DoubleVectorBatch)

void ORCFileInputStream::read(void* buf, uint64_t length, uint64_t offset) {
    _statistics->fs_read_calls++;
    _statistics->fs_read_bytes += length;
    SCOPED_RAW_TIMER(&_statistics->fs_read_time);
    uint64_t has_read = 0;
    char* out = reinterpret_cast<char*>(buf);
    while (has_read < length) {
        if (UNLIKELY(_io_ctx && _io_ctx->should_stop)) {
            throw orc::ParseError("stop");
        }
        size_t loop_read;
        Slice result(out + has_read, length - has_read);
        Status st = _file_reader->read_at(offset + has_read, result, &loop_read, _io_ctx);
        if (!st.ok()) {
            throw orc::ParseError(
                    absl::Substitute("Failed to read $0: $1", _file_name, st.to_string()));
        }
        if (loop_read == 0) {
            break;
        }
        has_read += loop_read;
    }
    if (has_read != length) {
        throw orc::ParseError(absl::Substitute("Try to read $0 bytes from $1, actually read $2",
                                               length, has_read, _file_name));
    }
}

void StripeStreamInputStream::read(void* buf, uint64_t length, uint64_t offset) {
    _statistics->fs_read_calls++;
    _statistics->fs_read_bytes += length;
    SCOPED_RAW_TIMER(&_statistics->fs_read_time);
    uint64_t has_read = 0;
    char* out = reinterpret_cast<char*>(buf);
    while (has_read < length) {
        if (UNLIKELY(_io_ctx && _io_ctx->should_stop)) {
            throw orc::ParseError("stop");
        }
        size_t loop_read;
        Slice result(out + has_read, length - has_read);
        Status st = _inner_reader->read_at(offset + has_read, result, &loop_read, _io_ctx);
        if (!st.ok()) {
            throw orc::ParseError(
                    absl::Substitute("Failed to read $0: $1", _file_name, st.to_string()));
        }
        if (loop_read == 0) {
            break;
        }
        has_read += loop_read;
    }
    if (has_read != length) {
        throw orc::ParseError(absl::Substitute("Try to read $0 bytes from $1, actually read $2",
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
          _io_ctx(io_ctx),
          _enable_lazy_mat(enable_lazy_mat),
          _enable_filter_by_min_max(
                  state == nullptr ? true : state->query_options().enable_orc_filter_by_min_max),
          _dict_cols_has_converted(false) {
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
          _file_system(nullptr),
          _io_ctx(io_ctx),
          _enable_lazy_mat(enable_lazy_mat),
          _enable_filter_by_min_max(true),
          _dict_cols_has_converted(false) {
    _init_system_properties();
    _init_file_description();
}

OrcReader::~OrcReader() {
    if (_obj_pool && _obj_pool.get()) {
        _obj_pool->clear();
    }
}

void OrcReader::_collect_profile_before_close() {
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
        COUNTER_UPDATE(_orc_profile.filter_block_time, _statistics.filter_block_time);

        if (_file_input_stream != nullptr) {
            _file_input_stream->collect_profile_before_close();
        }
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
        _orc_profile.filter_block_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "FilterBlockTime", orc_profile, 1);
        _orc_profile.selected_row_group_count =
                ADD_COUNTER_WITH_LEVEL(_profile, "SelectedRowGroupCount", TUnit::UNIT, 1);
        _orc_profile.evaluated_row_group_count =
                ADD_COUNTER_WITH_LEVEL(_profile, "EvaluatedRowGroupCount", TUnit::UNIT, 1);
    }
}

Status OrcReader::_create_file_reader() {
    if (_file_input_stream == nullptr) {
        _file_description.mtime =
                _scan_range.__isset.modification_time ? _scan_range.modification_time : 0;
        io::FileReaderOptions reader_options =
                FileFactory::get_reader_options(_state, _file_description);
        auto inner_reader = DORIS_TRY(io::DelegateReader::create_file_reader(
                _profile, _system_properties, _file_description, reader_options,
                io::DelegateReader::AccessMode::RANDOM, _io_ctx));
        _file_input_stream = std::make_unique<ORCFileInputStream>(
                _scan_range.path, std::move(inner_reader), &_statistics, _io_ctx, _profile,
                _orc_once_max_read_bytes, _orc_max_merge_distance_bytes);
    }
    if (_file_input_stream->getLength() == 0) {
        return Status::EndOfFile("empty orc file: " + _scan_range.path);
    }
    // create orc reader
    try {
        orc::ReaderOptions options;
        options.setMemoryPool(*ExecEnv::GetInstance()->orc_memory_pool());
        options.setReaderMetrics(&_reader_metrics);
        _reader = orc::createReader(
                std::unique_ptr<ORCFileInputStream>(_file_input_stream.release()), options);
    } catch (std::exception& e) {
        // invoker maybe just skip Status.NotFound and continue
        // so we need distinguish between it and other kinds of errors
        std::string _err_msg = e.what();
        if (_io_ctx && _io_ctx->should_stop && _err_msg == "stop") {
            return Status::EndOfFile("stop");
        }
        // one for fs, the other is for oss.
        if (_err_msg.find("No such file or directory") != std::string::npos ||
            _err_msg.find("NoSuchKey") != std::string::npos) {
            return Status::NotFound(_err_msg);
        }
        return Status::InternalError("Init OrcReader failed. reason = {}", _err_msg);
    }
    return Status::OK();
}

Status OrcReader::init_reader(
        const std::vector<std::string>* column_names,
        const std::vector<std::string>& missing_column_names,
        const std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range,
        const VExprContextSPtrs& conjuncts, bool is_acid, const TupleDescriptor* tuple_descriptor,
        const RowDescriptor* row_descriptor,
        const VExprContextSPtrs* not_single_slot_filter_conjuncts,
        const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts,
        const bool hive_use_column_names) {
    _column_names = column_names;
    _missing_column_names_set.insert(missing_column_names.begin(), missing_column_names.end());
    _colname_to_value_range = colname_to_value_range;
    _lazy_read_ctx.conjuncts = conjuncts;
    _is_acid = is_acid;
    _tuple_descriptor = tuple_descriptor;
    _row_descriptor = row_descriptor;
    _is_hive1_orc_or_use_idx = !hive_use_column_names;
    if (not_single_slot_filter_conjuncts != nullptr && !not_single_slot_filter_conjuncts->empty()) {
        _not_single_slot_filter_conjuncts.insert(_not_single_slot_filter_conjuncts.end(),
                                                 not_single_slot_filter_conjuncts->begin(),
                                                 not_single_slot_filter_conjuncts->end());
    }
    _slot_id_to_filter_conjuncts = slot_id_to_filter_conjuncts;
    _obj_pool = std::make_shared<ObjectPool>();

    if (_state != nullptr) {
        _orc_tiny_stripe_threshold_bytes = _state->query_options().orc_tiny_stripe_threshold_bytes;
        _orc_once_max_read_bytes = _state->query_options().orc_once_max_read_bytes;
        _orc_max_merge_distance_bytes = _state->query_options().orc_max_merge_distance_bytes;
    }

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
                                    std::vector<DataTypePtr>* col_types) {
    RETURN_IF_ERROR(_create_file_reader());
    const auto& root_type = _is_acid ? _remove_acid(_reader->getType()) : _reader->getType();
    for (int i = 0; i < root_type.getSubtypeCount(); ++i) {
        col_names->emplace_back(get_field_name_lower_case(&root_type, i));
        col_types->emplace_back(convert_to_doris_type(root_type.getSubtype(i)));
    }
    return Status::OK();
}

Status OrcReader::get_schema_col_name_attribute(std::vector<std::string>* col_names,
                                                std::vector<int32_t>* col_attributes,
                                                const std::string& attribute,
                                                bool* exist_attribute) {
    RETURN_IF_ERROR(_create_file_reader());
    *exist_attribute = true;
    const auto& root_type = _reader->getType();
    for (int i = 0; i < root_type.getSubtypeCount(); ++i) {
        col_names->emplace_back(get_field_name_lower_case(&root_type, i));

        if (!root_type.getSubtype(i)->hasAttributeKey(attribute)) {
            *exist_attribute = false;
            return Status::OK();
        }
        col_attributes->emplace_back(
                std::stoi(root_type.getSubtype(i)->getAttributeValue(attribute)));
    }
    return Status::OK();
}

Status OrcReader::_init_read_columns() {
    const auto& root_type = _reader->getType();
    std::vector<std::string> orc_cols;
    std::vector<std::string> orc_cols_lower_case;
    bool is_hive1_orc = false;
    _init_orc_cols(root_type, orc_cols, orc_cols_lower_case, _type_map, &is_hive1_orc, false);

    // In old version slot_name_to_schema_pos may not be set in _scan_params
    // TODO, should be removed in 2.2 or later
    _is_hive1_orc_or_use_idx = (is_hive1_orc || _is_hive1_orc_or_use_idx) &&
                               _scan_params.__isset.slot_name_to_schema_pos;
    for (size_t i = 0; i < _column_names->size(); ++i) {
        const auto& col_name = (*_column_names)[i];
        if (_missing_column_names_set.contains(col_name)) {
            _missing_cols.emplace_back(col_name);
            continue;
        }

        if (_is_hive1_orc_or_use_idx) {
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
            auto pos = std::distance(orc_cols_lower_case.begin(), iter);
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
            if (_is_hive1_orc_or_use_idx) {
                _removed_acid_file_col_name_to_schema_col[orc_cols[pos]] = col_name;
            }

            _col_name_to_file_col_name[col_name] = read_col;
        }
    }
    return Status::OK();
}

void OrcReader::_init_orc_cols(const orc::Type& type, std::vector<std::string>& orc_cols,
                               std::vector<std::string>& orc_cols_lower_case,
                               std::unordered_map<std::string, const orc::Type*>& type_map,
                               bool* is_hive1_orc, bool should_add_acid_prefix) const {
    bool hive1_orc = true;
    for (int i = 0; i < type.getSubtypeCount(); ++i) {
        orc_cols.emplace_back(type.getFieldName(i));
        auto filed_name_lower_case = get_field_name_lower_case(&type, i);
        if (hive1_orc) {
            hive1_orc = _is_hive1_col_name(filed_name_lower_case);
        }
        orc_cols_lower_case.emplace_back(std::move(filed_name_lower_case));
        auto file_name = type.getFieldName(i);
        if (should_add_acid_prefix) {
            file_name = fmt::format(
                    "{}.{}", TransactionalHive::ACID_COLUMN_NAMES[TransactionalHive::ROW_OFFSET],
                    file_name);
        }
        type_map.emplace(std::move(file_name), type.getSubtype(i));
        if (_is_acid) {
            const orc::Type* sub_type = type.getSubtype(i);
            if (sub_type->getKind() == orc::TypeKind::STRUCT) {
                _init_orc_cols(*sub_type, orc_cols, orc_cols_lower_case, type_map, is_hive1_orc,
                               true);
            }
        }
    }
    *is_hive1_orc = hive1_orc;
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
    } else {
        return false;
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
        // should not pust down CHAR type, because CHAR type is fixed length and will be padded
        // {orc::TypeKind::CHAR, orc::PredicateDataType::STRING},
        {orc::TypeKind::VARCHAR, orc::PredicateDataType::STRING},
        {orc::TypeKind::DATE, orc::PredicateDataType::DATE},
        {orc::TypeKind::DECIMAL, orc::PredicateDataType::DECIMAL},
        {orc::TypeKind::TIMESTAMP, orc::PredicateDataType::TIMESTAMP},
        {orc::TypeKind::BOOLEAN, orc::PredicateDataType::BOOLEAN}};

template <PrimitiveType primitive_type>
std::tuple<bool, orc::Literal> convert_to_orc_literal(const orc::Type* type,
                                                      StringRef& literal_data, int precision,
                                                      int scale) {
    const auto* value = literal_data.data;
    try {
        switch (type->getKind()) {
        case orc::TypeKind::BOOLEAN: {
            if (primitive_type != TYPE_BOOLEAN) {
                return std::make_tuple(false, orc::Literal(false));
            }
            return std::make_tuple(true, orc::Literal(bool(*((uint8_t*)value))));
        }
        case orc::TypeKind::BYTE:
        case orc::TypeKind::SHORT:
        case orc::TypeKind::INT:
        case orc::TypeKind::LONG: {
            if constexpr (primitive_type == TYPE_TINYINT) {
                return std::make_tuple(true, orc::Literal(int64_t(*((int8_t*)value))));
            } else if constexpr (primitive_type == TYPE_SMALLINT) {
                return std::make_tuple(true, orc::Literal(int64_t(*((int16_t*)value))));
            } else if constexpr (primitive_type == TYPE_INT) {
                return std::make_tuple(true, orc::Literal(int64_t(*((int32_t*)value))));
            } else if constexpr (primitive_type == TYPE_BIGINT) {
                return std::make_tuple(true, orc::Literal(int64_t(*((int64_t*)value))));
            }
            return std::make_tuple(false, orc::Literal(false));
        }
        case orc::TypeKind::FLOAT: {
            if constexpr (primitive_type == TYPE_FLOAT) {
                return std::make_tuple(true, orc::Literal(double(*((float*)value))));
            } else if constexpr (primitive_type == TYPE_DOUBLE) {
                return std::make_tuple(true, orc::Literal(double(*((double*)value))));
            }
            return std::make_tuple(false, orc::Literal(false));
        }
        case orc::TypeKind::DOUBLE: {
            if (primitive_type == TYPE_DOUBLE) {
                return std::make_tuple(true, orc::Literal(*((double*)value)));
            }
            return std::make_tuple(false, orc::Literal(false));
        }
        case orc::TypeKind::STRING:
            [[fallthrough]];
        case orc::TypeKind::BINARY:
            [[fallthrough]];
        // should not pust down CHAR type, because CHAR type is fixed length and will be padded
        // case orc::TypeKind::CHAR:
        //     [[fallthrough]];
        case orc::TypeKind::VARCHAR: {
            if (primitive_type == TYPE_STRING || primitive_type == TYPE_CHAR ||
                primitive_type == TYPE_VARCHAR) {
                return std::make_tuple(true, orc::Literal(literal_data.data, literal_data.size));
            }
            return std::make_tuple(false, orc::Literal(false));
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
            } else if constexpr (primitive_type == TYPE_DECIMAL128I) {
                decimal_value = *((int128_t*)value);
            } else {
                return std::make_tuple(false, orc::Literal(false));
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
            } else if (primitive_type == TYPE_DATEV2) {
                const DateV2Value<DateV2ValueType> date_v2 =
                        *reinterpret_cast<const DateV2Value<DateV2ValueType>*>(value);
                cctz::civil_day civil_date(date_v2.year(), date_v2.month(), date_v2.day());
                day_offset =
                        cctz::convert(civil_date, utc0).time_since_epoch().count() / (24 * 60 * 60);
            } else {
                return std::make_tuple(false, orc::Literal(false));
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
            } else if (primitive_type == TYPE_DATETIMEV2) {
                const DateV2Value<DateTimeV2ValueType> datetime_v2 =
                        *reinterpret_cast<const DateV2Value<DateTimeV2ValueType>*>(value);
                cctz::civil_second civil_seconds(datetime_v2.year(), datetime_v2.month(),
                                                 datetime_v2.day(), datetime_v2.hour(),
                                                 datetime_v2.minute(), datetime_v2.second());
                seconds = cctz::convert(civil_seconds, utc0).time_since_epoch().count();
                nanos = datetime_v2.microsecond() * 1000;
            } else {
                return std::make_tuple(false, orc::Literal(false));
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

std::tuple<bool, orc::Literal, orc::PredicateDataType> OrcReader::_make_orc_literal(
        const VSlotRef* slot_ref, const VLiteral* literal) {
    DCHECK(_col_name_to_file_col_name.contains(slot_ref->expr_name()));
    auto file_col_name = _col_name_to_file_col_name[slot_ref->expr_name()];
    if (!_type_map.contains(file_col_name)) {
        LOG(WARNING) << "Column " << slot_ref->expr_name() << " not found in _type_map";
        return std::make_tuple(false, orc::Literal(false), orc::PredicateDataType::LONG);
    }
    DCHECK(_type_map.contains(file_col_name));
    const auto* orc_type = _type_map[file_col_name];
    if (!TYPEKIND_TO_PREDICATE_TYPE.contains(orc_type->getKind())) {
        LOG(WARNING) << "Unsupported Push Down Orc Type [TypeKind=" << orc_type->getKind() << "]";
        return std::make_tuple(false, orc::Literal(false), orc::PredicateDataType::LONG);
    }
    const auto predicate_type = TYPEKIND_TO_PREDICATE_TYPE[orc_type->getKind()];
    if (literal == nullptr) {
        // only get the predicate_type
        return std::make_tuple(true, orc::Literal(true), predicate_type);
    }
    // this only happens when the literals of in_predicate contains null value, like in (1, null)
    if (literal->get_column_ptr()->is_null_at(0)) {
        return std::make_tuple(false, orc::Literal(false), predicate_type);
    }
    auto literal_data = literal->get_column_ptr()->get_data_at(0);
    auto* slot = _tuple_descriptor->slots()[slot_ref->column_id()];
    auto slot_type = slot->type();
    auto primitive_type = slot_type->get_primitive_type();
    auto src_type = OrcReader::convert_to_doris_type(orc_type)->get_primitive_type();
    // should not down predicate for string type change from other type
    if (src_type != primitive_type && !is_string_type(src_type) && is_string_type(primitive_type)) {
        LOG(WARNING) << "Unsupported Push Down Schema Changed Column " << primitive_type << " to "
                     << src_type;
        return std::make_tuple(false, orc::Literal(false), orc::PredicateDataType::LONG);
    }
    switch (primitive_type) {
#define M(NAME)                                                                              \
    case TYPE_##NAME: {                                                                      \
        auto [valid, orc_literal] = convert_to_orc_literal<TYPE_##NAME>(                     \
                orc_type, literal_data, slot_type->get_precision(), slot_type->get_scale()); \
        return std::make_tuple(valid, orc_literal, predicate_type);                          \
    }
#define APPLY_FOR_PRIMITIVE_TYPE(M) \
    M(TINYINT)                      \
    M(SMALLINT)                     \
    M(INT)                          \
    M(BIGINT)                       \
    M(LARGEINT)                     \
    M(DATE)                         \
    M(DATETIME)                     \
    M(DATEV2)                       \
    M(DATETIMEV2)                   \
    M(VARCHAR)                      \
    M(STRING)                       \
    M(HLL)                          \
    M(DECIMAL32)                    \
    M(DECIMAL64)                    \
    M(DECIMAL128I)                  \
    M(DECIMAL256)                   \
    M(DECIMALV2)                    \
    M(BOOLEAN)                      \
    M(IPV4)                         \
    M(IPV6)
        APPLY_FOR_PRIMITIVE_TYPE(M)
#undef M
    default: {
        VLOG_CRITICAL << "Unsupported Convert Orc Literal [ColName=" << slot->col_name() << "]";
        return std::make_tuple(false, orc::Literal(false), predicate_type);
    }
    }
}

// check if the slot of expr can be pushed down to orc reader and make orc predicate type
bool OrcReader::_check_slot_can_push_down(const VExprSPtr& expr) {
    if (!expr->children()[0]->is_slot_ref()) {
        return false;
    }
    const auto* slot_ref = static_cast<const VSlotRef*>(expr->children()[0].get());
    // check if the slot exists in orc file and not partition column
    if (!_col_name_to_file_col_name.contains(slot_ref->expr_name()) ||
        _lazy_read_ctx.predicate_partition_columns.contains(slot_ref->expr_name())) {
        return false;
    }
    auto [valid, _, predicate_type] = _make_orc_literal(slot_ref, nullptr);
    if (valid) {
        _vslot_ref_to_orc_predicate_data_type[slot_ref] = predicate_type;
    }
    return valid;
}

// check if the literal of expr can be pushed down to orc reader and make orc literal
bool OrcReader::_check_literal_can_push_down(const VExprSPtr& expr, size_t child_id) {
    if (!expr->children()[child_id]->is_literal()) {
        return false;
    }
    // the slot has been checked in _check_slot_can_push_down before calling this function
    const auto* slot_ref = static_cast<const VSlotRef*>(expr->children()[0].get());
    const auto* literal = static_cast<const VLiteral*>(expr->children()[child_id].get());
    auto [valid, orc_literal, _] = _make_orc_literal(slot_ref, literal);
    if (valid) {
        _vliteral_to_orc_literal.insert(std::make_pair(literal, orc_literal));
    }
    return valid;
}

// check if there are rest children of expr can be pushed down to orc reader
bool OrcReader::_check_rest_children_can_push_down(const VExprSPtr& expr) {
    if (expr->children().size() < 2) {
        return false;
    }

    bool at_least_one_child_can_push_down = false;
    for (size_t i = 1; i < expr->children().size(); ++i) {
        if (_check_literal_can_push_down(expr, i)) {
            at_least_one_child_can_push_down = true;
        }
    }
    return at_least_one_child_can_push_down;
}

// check if the expr can be pushed down to orc reader
bool OrcReader::_check_expr_can_push_down(const VExprSPtr& expr) {
    if (expr == nullptr) {
        return false;
    }

    switch (expr->op()) {
    case TExprOpcode::COMPOUND_AND:
        // at least one child can be pushed down
        return std::ranges::any_of(expr->children(), [this](const auto& child) {
            return _check_expr_can_push_down(child);
        });
    case TExprOpcode::COMPOUND_OR:
        // all children must be pushed down
        return std::ranges::all_of(expr->children(), [this](const auto& child) {
            return _check_expr_can_push_down(child);
        });
    case TExprOpcode::COMPOUND_NOT:
        DCHECK_EQ(expr->children().size(), 1);
        return _check_expr_can_push_down(expr->children()[0]);

    case TExprOpcode::GE:
    case TExprOpcode::GT:
    case TExprOpcode::LE:
    case TExprOpcode::LT:
    case TExprOpcode::EQ:
    case TExprOpcode::NE:
    case TExprOpcode::FILTER_IN:
    case TExprOpcode::FILTER_NOT_IN:
        // can't push down if expr is null aware predicate
        return expr->node_type() != TExprNodeType::NULL_AWARE_BINARY_PRED &&
               expr->node_type() != TExprNodeType::NULL_AWARE_IN_PRED &&
               _check_slot_can_push_down(expr) && _check_rest_children_can_push_down(expr);

    case TExprOpcode::INVALID_OPCODE:
        if (expr->node_type() == TExprNodeType::FUNCTION_CALL) {
            auto fn_name = expr->fn().name.function_name;
            // only support is_null_pred and is_not_null_pred
            if (fn_name == "is_null_pred" || fn_name == "is_not_null_pred") {
                return _check_slot_can_push_down(expr);
            }
            VLOG_CRITICAL << "Unsupported function [funciton=" << fn_name << "]";
        }
        return false;
    default:
        VLOG_CRITICAL << "Unsupported Opcode [OpCode=" << expr->op() << "]";
        return false;
    }
}

void OrcReader::_build_less_than(const VExprSPtr& expr,
                                 std::unique_ptr<orc::SearchArgumentBuilder>& builder) {
    DCHECK(expr->children().size() == 2);
    DCHECK(expr->children()[0]->is_slot_ref());
    DCHECK(expr->children()[1]->is_literal());
    const auto* slot_ref = static_cast<const VSlotRef*>(expr->children()[0].get());
    const auto* literal = static_cast<const VLiteral*>(expr->children()[1].get());
    DCHECK(_vslot_ref_to_orc_predicate_data_type.contains(slot_ref));
    auto predicate_type = _vslot_ref_to_orc_predicate_data_type[slot_ref];
    DCHECK(_vliteral_to_orc_literal.contains(literal));
    auto orc_literal = _vliteral_to_orc_literal.find(literal)->second;
    builder->lessThan(slot_ref->expr_name(), predicate_type, orc_literal);
}

void OrcReader::_build_less_than_equals(const VExprSPtr& expr,
                                        std::unique_ptr<orc::SearchArgumentBuilder>& builder) {
    DCHECK(expr->children().size() == 2);
    DCHECK(expr->children()[0]->is_slot_ref());
    DCHECK(expr->children()[1]->is_literal());
    const auto* slot_ref = static_cast<const VSlotRef*>(expr->children()[0].get());
    const auto* literal = static_cast<const VLiteral*>(expr->children()[1].get());
    DCHECK(_vslot_ref_to_orc_predicate_data_type.contains(slot_ref));
    auto predicate_type = _vslot_ref_to_orc_predicate_data_type[slot_ref];
    DCHECK(_vliteral_to_orc_literal.contains(literal));
    auto orc_literal = _vliteral_to_orc_literal.find(literal)->second;
    builder->lessThanEquals(slot_ref->expr_name(), predicate_type, orc_literal);
}

void OrcReader::_build_equals(const VExprSPtr& expr,
                              std::unique_ptr<orc::SearchArgumentBuilder>& builder) {
    DCHECK(expr->children().size() == 2);
    DCHECK(expr->children()[0]->is_slot_ref());
    DCHECK(expr->children()[1]->is_literal());
    const auto* slot_ref = static_cast<const VSlotRef*>(expr->children()[0].get());
    const auto* literal = static_cast<const VLiteral*>(expr->children()[1].get());
    DCHECK(_vslot_ref_to_orc_predicate_data_type.contains(slot_ref));
    auto predicate_type = _vslot_ref_to_orc_predicate_data_type[slot_ref];
    DCHECK(_vliteral_to_orc_literal.contains(literal));
    auto orc_literal = _vliteral_to_orc_literal.find(literal)->second;
    builder->equals(slot_ref->expr_name(), predicate_type, orc_literal);
}

void OrcReader::_build_filter_in(const VExprSPtr& expr,
                                 std::unique_ptr<orc::SearchArgumentBuilder>& builder) {
    DCHECK(expr->children().size() >= 2);
    DCHECK(expr->children()[0]->is_slot_ref());
    const auto* slot_ref = static_cast<const VSlotRef*>(expr->children()[0].get());
    std::vector<orc::Literal> literals;
    DCHECK(_vslot_ref_to_orc_predicate_data_type.contains(slot_ref));
    orc::PredicateDataType predicate_type = _vslot_ref_to_orc_predicate_data_type[slot_ref];
    for (size_t i = 1; i < expr->children().size(); ++i) {
        DCHECK(expr->children()[i]->is_literal());
        const auto* literal = static_cast<const VLiteral*>(expr->children()[i].get());
        if (_vliteral_to_orc_literal.contains(literal)) {
            auto orc_literal = _vliteral_to_orc_literal.find(literal)->second;
            literals.emplace_back(orc_literal);
        }
    }
    DCHECK(!literals.empty());
    if (literals.size() == 1) {
        builder->equals(slot_ref->expr_name(), predicate_type, literals[0]);
    } else {
        builder->in(slot_ref->expr_name(), predicate_type, literals);
    }
}

void OrcReader::_build_is_null(const VExprSPtr& expr,
                               std::unique_ptr<orc::SearchArgumentBuilder>& builder) {
    DCHECK(expr->children().size() == 1);
    DCHECK(expr->children()[0]->is_slot_ref());
    const auto* slot_ref = static_cast<const VSlotRef*>(expr->children()[0].get());
    DCHECK(_vslot_ref_to_orc_predicate_data_type.contains(slot_ref));
    auto predicate_type = _vslot_ref_to_orc_predicate_data_type[slot_ref];
    builder->isNull(slot_ref->expr_name(), predicate_type);
}

bool OrcReader::_build_search_argument(const VExprSPtr& expr,
                                       std::unique_ptr<orc::SearchArgumentBuilder>& builder) {
    // OPTIMIZE: check expr only once
    if (!_check_expr_can_push_down(expr)) {
        return false;
    }
    switch (expr->op()) {
    case TExprOpcode::COMPOUND_AND: {
        builder->startAnd();
        bool at_least_one_can_push_down = false;
        for (const auto& child : expr->children()) {
            if (_build_search_argument(child, builder)) {
                at_least_one_can_push_down = true;
            }
        }
        DCHECK(at_least_one_can_push_down);
        builder->end();
        break;
    }
    case TExprOpcode::COMPOUND_OR: {
        builder->startOr();
        bool all_can_push_down = true;
        for (const auto& child : expr->children()) {
            if (!_build_search_argument(child, builder)) {
                all_can_push_down = false;
            }
        }
        DCHECK(all_can_push_down);
        builder->end();
        break;
    }
    case TExprOpcode::COMPOUND_NOT: {
        DCHECK_EQ(expr->children().size(), 1);
        builder->startNot();
        auto res = _build_search_argument(expr->children()[0], builder);
        DCHECK(res);
        builder->end();
        break;
    }
    case TExprOpcode::GE:
        builder->startNot();
        _build_less_than(expr, builder);
        builder->end();
        break;
    case TExprOpcode::GT:
        builder->startNot();
        _build_less_than_equals(expr, builder);
        builder->end();
        break;
    case TExprOpcode::LE:
        _build_less_than_equals(expr, builder);
        break;
    case TExprOpcode::LT:
        _build_less_than(expr, builder);
        break;
    case TExprOpcode::EQ:
        _build_equals(expr, builder);
        break;
    case TExprOpcode::NE:
        builder->startNot();
        _build_equals(expr, builder);
        builder->end();
        break;
    case TExprOpcode::FILTER_IN:
        _build_filter_in(expr, builder);
        break;
    case TExprOpcode::FILTER_NOT_IN:
        builder->startNot();
        _build_filter_in(expr, builder);
        builder->end();
        break;
    // is null and is not null is represented as function call
    case TExprOpcode::INVALID_OPCODE:
        DCHECK(expr->node_type() == TExprNodeType::FUNCTION_CALL);
        if (expr->fn().name.function_name == "is_null_pred") {
            _build_is_null(expr, builder);
        } else if (expr->fn().name.function_name == "is_not_null_pred") {
            builder->startNot();
            _build_is_null(expr, builder);
            builder->end();
        } else {
            // should not reach here, because _check_expr_can_push_down has already checked
            __builtin_unreachable();
        }
        break;

    default:
        // should not reach here, because _check_expr_can_push_down has already checked
        __builtin_unreachable();
    }
    return true;
}

bool OrcReader::_init_search_argument(const VExprContextSPtrs& conjuncts) {
    // build search argument, if any expr can not be pushed down, return false
    auto builder = orc::SearchArgumentFactory::newBuilder();
    bool at_least_one_can_push_down = false;
    builder->startAnd();
    for (const auto& expr_ctx : conjuncts) {
        _vslot_ref_to_orc_predicate_data_type.clear();
        _vliteral_to_orc_literal.clear();
        if (_build_search_argument(expr_ctx->root(), builder)) {
            at_least_one_can_push_down = true;
        }
    }
    if (!at_least_one_can_push_down) {
        // if all exprs can not be pushed down, builder->end() will throw exception
        return false;
    }
    builder->end();

    auto sargs = builder->build();
    _profile->add_info_string("OrcReader SearchArgument: ", sargs->toString());
    _row_reader_options.searchArgument(std::move(sargs));
    return true;
}

Status OrcReader::set_fill_columns(
        const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&
                partition_columns,
        const std::unordered_map<std::string, VExprContextSPtr>& missing_columns) {
    SCOPED_RAW_TIMER(&_statistics.set_fill_column_time);

    // std::unordered_map<column_name, std::pair<col_id, slot_id>>
    std::unordered_map<std::string, std::pair<uint32_t, int>> predicate_columns;
    std::function<void(VExpr * expr)> visit_slot = [&](VExpr* expr) {
        if (auto* slot_ref = typeid_cast<VSlotRef*>(expr)) {
            auto expr_name = slot_ref->expr_name();
            auto iter = _table_col_to_file_col.find(expr_name);
            if (iter != _table_col_to_file_col.end()) {
                expr_name = iter->second;
            }
            predicate_columns.emplace(expr_name,
                                      std::make_pair(slot_ref->column_id(), slot_ref->slot_id()));
            if (slot_ref->column_id() == 0) {
                _lazy_read_ctx.resize_first_column = false;
            }
            return;
        } else if (auto* runtime_filter = typeid_cast<VRuntimeFilterWrapper*>(expr)) {
            auto* filter_impl = const_cast<VExpr*>(runtime_filter->get_impl().get());
            if (auto* bloom_predicate = typeid_cast<VBloomPredicate*>(filter_impl)) {
                for (const auto& child : bloom_predicate->children()) {
                    visit_slot(child.get());
                }
            } else if (auto* in_predicate = typeid_cast<VInPredicate*>(filter_impl)) {
                if (!in_predicate->children().empty()) {
                    visit_slot(in_predicate->children()[0].get());
                }
            } else {
                for (const auto& child : filter_impl->children()) {
                    visit_slot(child.get());
                }
            }
        } else {
            for (const auto& child : expr->children()) {
                visit_slot(child.get());
            }
        }
    };

    for (auto& conjunct : _lazy_read_ctx.conjuncts) {
        visit_slot(conjunct->root().get());
    }

    if (_is_acid) {
        _lazy_read_ctx.predicate_orc_columns.insert(
                _lazy_read_ctx.predicate_orc_columns.end(),
                TransactionalHive::READ_ROW_COLUMN_NAMES.begin(),
                TransactionalHive::READ_ROW_COLUMN_NAMES.end());
    }

    for (auto& read_col : _read_cols_lower_case) {
        _lazy_read_ctx.all_read_columns.emplace_back(read_col);
        if (!predicate_columns.empty()) {
            auto iter = predicate_columns.find(read_col);
            if (iter == predicate_columns.end()) {
                if (!_is_acid ||
                    std::find(TransactionalHive::READ_ROW_COLUMN_NAMES_LOWER_CASE.begin(),
                              TransactionalHive::READ_ROW_COLUMN_NAMES_LOWER_CASE.end(),
                              read_col) ==
                            TransactionalHive::READ_ROW_COLUMN_NAMES_LOWER_CASE.end()) {
                    _lazy_read_ctx.lazy_read_columns.emplace_back(read_col);
                }
            } else {
                _lazy_read_ctx.predicate_columns.first.emplace_back(iter->first);
                _lazy_read_ctx.predicate_columns.second.emplace_back(iter->second.second);
                _lazy_read_ctx.predicate_orc_columns.emplace_back(
                        _col_name_to_file_col_name[iter->first]);
                _lazy_read_ctx.all_predicate_col_ids.emplace_back(iter->second.first);
            }
        }
    }

    if (_tuple_descriptor != nullptr) {
        for (const auto& each : _tuple_descriptor->slots()) {
            PrimitiveType column_type = each->col_type();
            if (is_complex_type(column_type)) {
                _has_complex_type = true;
                break;
            }
        }
    }

    for (const auto& kv : partition_columns) {
        auto iter = predicate_columns.find(kv.first);
        if (iter == predicate_columns.end()) {
            _lazy_read_ctx.partition_columns.emplace(kv.first, kv.second);
        } else {
            _lazy_read_ctx.predicate_partition_columns.emplace(kv.first, kv.second);
            _lazy_read_ctx.all_predicate_col_ids.emplace_back(iter->second.first);
        }
    }

    for (const auto& kv : missing_columns) {
        auto iter = predicate_columns.find(kv.first);
        if (iter == predicate_columns.end()) {
            _lazy_read_ctx.missing_columns.emplace(kv.first, kv.second);
        } else {
            //For check missing column :   missing column == xx, missing column is null,missing column is not null.
            if (_slot_id_to_filter_conjuncts->find(iter->second.second) !=
                _slot_id_to_filter_conjuncts->end()) {
                for (const auto& ctx :
                     _slot_id_to_filter_conjuncts->find(iter->second.second)->second) {
                    _filter_conjuncts.emplace_back(ctx);
                }
            }

            // predicate_missing_columns is VLiteral.To fill in default values for missing columns.
            _lazy_read_ctx.predicate_missing_columns.emplace(kv.first, kv.second);
            _lazy_read_ctx.all_predicate_col_ids.emplace_back(iter->second.first);
        }
    }

    if (_enable_lazy_mat && !_lazy_read_ctx.predicate_columns.first.empty() &&
        !_lazy_read_ctx.lazy_read_columns.empty()) {
        _lazy_read_ctx.can_lazy_read = true;
    }

    if (_lazy_read_ctx.conjuncts.empty()) {
        _lazy_read_ctx.can_lazy_read = false;
    } else if (_enable_filter_by_min_max) {
        auto res = _init_search_argument(_lazy_read_ctx.conjuncts);
        if (_state->query_options().check_orc_init_sargs_success && !res) {
            std::stringstream ss;
            for (const auto& conjunct : _lazy_read_ctx.conjuncts) {
                ss << conjunct->root()->debug_string() << "\n";
            }
            std::string conjuncts_str = ss.str();
            return Status::InternalError(
                    "Session variable check_orc_init_sargs_success is set, but "
                    "_init_search_argument returns false because all exprs can not be pushed "
                    "down:\n " +
                    conjuncts_str);
        }
    }
    try {
        _row_reader_options.range(_range_start_offset, _range_size);
        _row_reader_options.setTimezoneName(_ctz == "CST" ? "Asia/Shanghai" : _ctz);
        _row_reader_options.include(_read_cols);
        _row_reader_options.setEnableLazyDecoding(true);

        uint64_t number_of_stripes = _reader->getNumberOfStripes();
        auto all_stripes_needed = _reader->getNeedReadStripes(_row_reader_options);

        int64_t range_end_offset = _range_start_offset + _range_size;

        bool all_tiny_stripes = true;
        std::vector<io::PrefetchRange> tiny_stripe_ranges;

        for (uint64_t i = 0; i < number_of_stripes; i++) {
            std::unique_ptr<orc::StripeInformation> strip_info = _reader->getStripe(i);
            uint64_t strip_start_offset = strip_info->getOffset();
            uint64_t strip_end_offset = strip_start_offset + strip_info->getLength();

            if (strip_start_offset >= range_end_offset || strip_end_offset < _range_start_offset ||
                !all_stripes_needed[i]) {
                continue;
            }
            if (strip_info->getLength() > _orc_tiny_stripe_threshold_bytes) {
                all_tiny_stripes = false;
                break;
            }

            tiny_stripe_ranges.emplace_back(strip_start_offset, strip_end_offset);
        }
        if (all_tiny_stripes && number_of_stripes > 0) {
            std::vector<io::PrefetchRange> prefetch_merge_ranges =
                    io::PrefetchRange::merge_adjacent_seq_ranges(tiny_stripe_ranges,
                                                                 _orc_max_merge_distance_bytes,
                                                                 _orc_once_max_read_bytes);
            auto range_finder =
                    std::make_shared<io::LinearProbeRangeFinder>(std::move(prefetch_merge_ranges));

            auto* orc_input_stream_ptr = static_cast<ORCFileInputStream*>(_reader->getStream());
            orc_input_stream_ptr->set_all_tiny_stripes();
            auto& orc_file_reader = orc_input_stream_ptr->get_file_reader();
            auto orc_inner_reader = orc_input_stream_ptr->get_inner_reader();
            orc_file_reader = std::make_shared<io::RangeCacheFileReader>(_profile, orc_inner_reader,
                                                                         range_finder);
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
        if (_lazy_read_ctx.can_lazy_read) {
            _row_reader_options.filter(_lazy_read_ctx.predicate_orc_columns);
            _orc_filter = std::make_unique<ORCFilterImpl>(this);
        }
        if (!_lazy_read_ctx.conjuncts.empty()) {
            _string_dict_filter = std::make_unique<StringDictFilterImpl>(this);
        }
        _row_reader = _reader->createRowReader(_row_reader_options, _orc_filter.get(),
                                               _string_dict_filter.get());
        _batch = _row_reader->createRowBatch(_batch_size);
        const auto& selected_type = _row_reader->getSelectedType();
        int idx = 0;
        RETURN_IF_ERROR(_init_select_types(selected_type, idx));

        _remaining_rows = _row_reader->getNumberOfRows();

    } catch (std::exception& e) {
        std::string _err_msg = e.what();
        // ignore stop exception
        if (!(_io_ctx && _io_ctx->should_stop && _err_msg == "stop")) {
            return Status::InternalError("Failed to create orc row reader. reason = {}", _err_msg);
        }
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
            for (const auto& ctx : iter->second) {
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
        if (_is_hive1_orc_or_use_idx) {
            name = _removed_acid_file_col_name_to_schema_col[type.getFieldName(i)];
        } else {
            name = get_field_name_lower_case(&type, i);
        }
        _colname_to_idx[name] = idx++;
        const orc::Type* sub_type = type.getSubtype(i);
        _col_orc_type.push_back(sub_type);
        if (_is_acid && sub_type->getKind() == orc::TypeKind::STRUCT) {
            RETURN_IF_ERROR(_init_select_types(*sub_type, idx));
        }
    }
    return Status::OK();
}

Status OrcReader::_fill_partition_columns(
        Block* block, uint64_t rows,
        const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&
                partition_columns) {
    DataTypeSerDe::FormatOptions _text_formatOptions;
    for (const auto& kv : partition_columns) {
        auto doris_column = block->get_by_name(kv.first).column;
        auto* col_ptr = const_cast<IColumn*>(doris_column.get());
        const auto& [value, slot_desc] = kv.second;
        auto _text_serde = slot_desc->get_data_type_ptr()->get_serde();
        Slice slice(value.data(), value.size());
        uint64_t num_deserialized = 0;
        if (_text_serde->deserialize_column_from_fixed_json(*col_ptr, slice, rows,
                                                            &num_deserialized,
                                                            _text_formatOptions) != Status::OK()) {
            return Status::InternalError("Failed to fill partition column: {}={}",
                                         slot_desc->col_name(), value);
        }
        if (num_deserialized != rows) {
            return Status::InternalError(
                    "Failed to fill partition column: {}={} ."
                    "Number of rows expected to be written : {}, number of rows actually "
                    "written : "
                    "{}",
                    slot_desc->col_name(), value, num_deserialized, rows);
        }
    }
    return Status::OK();
}

Status OrcReader::_fill_missing_columns(
        Block* block, uint64_t rows,
        const std::unordered_map<std::string, VExprContextSPtr>& missing_columns) {
    for (const auto& kv : missing_columns) {
        if (kv.second == nullptr) {
            // no default column, fill with null
            auto mutable_column = block->get_by_name(kv.first).column->assume_mutable();
            auto* nullable_column = static_cast<vectorized::ColumnNullable*>(mutable_column.get());
            nullable_column->insert_many_defaults(rows);
        } else {
            // fill with default value
            const auto& ctx = kv.second;
            auto origin_column_num = block->columns();
            int result_column_id = -1;
            // PT1 => dest primitive type
            RETURN_IF_ERROR(ctx->execute(block, &result_column_id));
            bool is_origin_column = result_column_id < origin_column_num;
            if (!is_origin_column) {
                // call resize because the first column of _src_block_ptr may not be filled by reader,
                // so _src_block_ptr->rows() may return wrong result, cause the column created by `ctx->execute()`
                // has only one row.
                auto result_column_ptr = block->get_by_position(result_column_id).column;
                auto mutable_column = result_column_ptr->assume_mutable();
                mutable_column->resize(rows);
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

DataTypePtr OrcReader::convert_to_doris_type(const orc::Type* orc_type) {
    switch (orc_type->getKind()) {
    case orc::TypeKind::BOOLEAN:
        return DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_BOOLEAN, true);
    case orc::TypeKind::BYTE:
        return DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_TINYINT, true);
    case orc::TypeKind::SHORT:
        return DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_SMALLINT, true);
    case orc::TypeKind::INT:
        return DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
    case orc::TypeKind::LONG:
        return DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_BIGINT, true);
    case orc::TypeKind::FLOAT:
        return DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_FLOAT, true);
    case orc::TypeKind::DOUBLE:
        return DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_DOUBLE, true);
    case orc::TypeKind::STRING:
        return DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_STRING, true);
    case orc::TypeKind::BINARY:
        return DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_STRING, true);
    case orc::TypeKind::TIMESTAMP:
        return DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_DATETIMEV2, true, 0,
                                                            6);
    case orc::TypeKind::DECIMAL:
        return DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_DECIMAL128I, true,
                orc_type->getPrecision() == 0 ? decimal_precision_for_hive11
                                              : cast_set<int>(orc_type->getPrecision()),
                orc_type->getPrecision() == 0 ? decimal_scale_for_hive11
                                              : cast_set<int>(orc_type->getScale()));
    case orc::TypeKind::DATE:
        return DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_DATEV2, true);
    case orc::TypeKind::VARCHAR:
        return DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_VARCHAR, true, 0, 0,
                cast_set<int>(orc_type->getMaximumLength()));
    case orc::TypeKind::CHAR:
        return DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_CHAR, true, 0, 0, cast_set<int>(orc_type->getMaximumLength()));
    case orc::TypeKind::TIMESTAMP_INSTANT:
        return DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_DATETIMEV2, true, 0,
                                                            6);
    case orc::TypeKind::LIST: {
        return make_nullable(
                std::make_shared<DataTypeArray>(convert_to_doris_type(orc_type->getSubtype(0))));
    }
    case orc::TypeKind::MAP: {
        return make_nullable(
                std::make_shared<DataTypeMap>(convert_to_doris_type(orc_type->getSubtype(0)),
                                              convert_to_doris_type(orc_type->getSubtype(1))));
    }
    case orc::TypeKind::STRUCT: {
        DataTypes res_data_types;
        std::vector<std::string> names;
        for (int i = 0; i < orc_type->getSubtypeCount(); ++i) {
            res_data_types.push_back(convert_to_doris_type(orc_type->getSubtype(i)));
            names.push_back(get_field_name_lower_case(orc_type, i));
        }
        return make_nullable(std::make_shared<DataTypeStruct>(res_data_types, names));
    }
    default:
        throw Exception(Status::InternalError("Orc type is not supported!"));
        return nullptr;
    }
}

Status OrcReader::get_columns(std::unordered_map<std::string, DataTypePtr>* name_to_type,
                              std::unordered_set<std::string>* missing_cols) {
    const auto& root_type = _reader->getType();
    for (int i = 0; i < root_type.getSubtypeCount(); ++i) {
        name_to_type->emplace(get_field_name_lower_case(&root_type, i),
                              convert_to_doris_type(root_type.getSubtype(i)));
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
                                        const orc::TypeKind& type_kind,
                                        const orc::ColumnVectorBatch* cvb, size_t num_values) {
    SCOPED_RAW_TIMER(&_statistics.decode_value_time);
    const auto* data = dynamic_cast<const orc::EncodedStringVectorBatch*>(cvb);
    if (data == nullptr) {
        return Status::InternalError(
                "Wrong data type for column '{}', expected EncodedStringVectorBatch", col_name);
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
                                                         const orc::EncodedStringVectorBatch* cvb,
                                                         size_t num_values) {
    const static std::string empty_string;
    std::vector<StringRef> string_values;
    string_values.reserve(num_values);
    if (type_kind == orc::TypeKind::CHAR) {
        // Possibly there are some zero padding characters in CHAR type, we have to strip them off.
        if (cvb->hasNulls) {
            for (int i = 0; i < num_values; ++i) {
                if (cvb->notNull[i]) {
                    size_t length = trim_right(cvb->data[i], cvb->length[i]);
                    string_values.emplace_back((length > 0) ? cvb->data[i] : empty_string.data(),
                                               length);
                } else {
                    // Orc doesn't fill null values in new batch, but the former batch has been release.
                    // Other types like int/long/timestamp... are flat types without pointer in them,
                    // so other types do not need to be handled separately like string.
                    string_values.emplace_back(empty_string.data(), 0);
                }
            }
        } else {
            for (int i = 0; i < num_values; ++i) {
                size_t length = trim_right(cvb->data[i], cvb->length[i]);
                string_values.emplace_back((length > 0) ? cvb->data[i] : empty_string.data(),
                                           length);
            }
        }
    } else {
        if (cvb->hasNulls) {
            for (int i = 0; i < num_values; ++i) {
                if (cvb->notNull[i]) {
                    string_values.emplace_back(
                            (cvb->length[i] > 0) ? cvb->data[i] : empty_string.data(),
                            cvb->length[i]);
                } else {
                    string_values.emplace_back(empty_string.data(), 0);
                }
            }
        } else {
            for (int i = 0; i < num_values; ++i) {
                string_values.emplace_back(
                        (cvb->length[i] > 0) ? cvb->data[i] : empty_string.data(), cvb->length[i]);
            }
        }
    }
    if (!string_values.empty()) {
        data_column->insert_many_strings(string_values.data(), num_values);
    }
    return Status::OK();
}

template <bool is_filter>
Status OrcReader::_decode_string_dict_encoded_column(const std::string& col_name,
                                                     const MutableColumnPtr& data_column,
                                                     const orc::TypeKind& type_kind,
                                                     const orc::EncodedStringVectorBatch* cvb,
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
                    string_values.emplace_back((length > 0) ? val_ptr : EMPTY_STRING_FOR_OVERFLOW,
                                               length);
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
                string_values.emplace_back((length > 0) ? val_ptr : EMPTY_STRING_FOR_OVERFLOW,
                                           length);
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
                    string_values.emplace_back((length > 0) ? val_ptr : EMPTY_STRING_FOR_OVERFLOW,
                                               length);
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
                string_values.emplace_back((length > 0) ? val_ptr : EMPTY_STRING_FOR_OVERFLOW,
                                           length);
            }
        }
    }
    if (!string_values.empty()) {
        data_column->insert_many_strings_overflow(string_values.data(), string_values.size(),
                                                  max_value_length);
    }
    return Status::OK();
}

template <bool is_filter>
Status OrcReader::_decode_int32_column(const std::string& col_name,
                                       const MutableColumnPtr& data_column,
                                       const orc::ColumnVectorBatch* cvb, size_t num_values) {
    SCOPED_RAW_TIMER(&_statistics.decode_value_time);
    if (dynamic_cast<const orc::LongVectorBatch*>(cvb) != nullptr) {
        return _decode_flat_column<Int32, orc::LongVectorBatch>(col_name, data_column, cvb,
                                                                num_values);
    } else if (dynamic_cast<const orc::EncodedStringVectorBatch*>(cvb) != nullptr) {
        const auto* data = static_cast<const orc::EncodedStringVectorBatch*>(cvb);
        const auto* cvb_data = data->index.data();
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
                                            const orc::DataBuffer<int64_t>& orc_offsets,
                                            size_t num_values, size_t* element_size) {
    SCOPED_RAW_TIMER(&_statistics.decode_value_time);
    if (num_values > 0) {
        if (const_cast<orc::DataBuffer<int64_t>&>(orc_offsets).size() < num_values + 1) {
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
Status OrcReader::_fill_doris_data_column(const std::string& col_name,
                                          MutableColumnPtr& data_column,
                                          const DataTypePtr& data_type,
                                          const orc::Type* orc_column_type,
                                          const orc::ColumnVectorBatch* cvb, size_t num_values) {
    auto logical_type = data_type->get_primitive_type();
    switch (logical_type) {
#define DISPATCH(FlatType, CppType, OrcColumnType) \
    case FlatType:                                 \
        return _decode_flat_column<CppType, OrcColumnType>(col_name, data_column, cvb, num_values);
        FOR_FLAT_ORC_COLUMNS(DISPATCH)
#undef DISPATCH
    case PrimitiveType::TYPE_INT:
        return _decode_int32_column<is_filter>(col_name, data_column, cvb, num_values);
    case PrimitiveType::TYPE_DECIMAL32:
        return _decode_decimal_column<Decimal32, is_filter>(col_name, data_column, data_type, cvb,
                                                            num_values);
    case PrimitiveType::TYPE_DECIMAL64:
        return _decode_decimal_column<Decimal64, is_filter>(col_name, data_column, data_type, cvb,
                                                            num_values);
    case PrimitiveType::TYPE_DECIMALV2:
        return _decode_decimal_column<Decimal128V2, is_filter>(col_name, data_column, data_type,
                                                               cvb, num_values);
    case PrimitiveType::TYPE_DECIMAL128I:
        return _decode_decimal_column<Decimal128V3, is_filter>(col_name, data_column, data_type,
                                                               cvb, num_values);
    case PrimitiveType::TYPE_DATEV2:
        return _decode_time_column<DateV2Value<DateV2ValueType>, UInt32, orc::LongVectorBatch,
                                   is_filter>(col_name, data_column, cvb, num_values);
    case PrimitiveType::TYPE_DATETIMEV2:
        return _decode_time_column<DateV2Value<DateTimeV2ValueType>, UInt64,
                                   orc::TimestampVectorBatch, is_filter>(col_name, data_column, cvb,
                                                                         num_values);
    case PrimitiveType::TYPE_STRING:
    case PrimitiveType::TYPE_VARCHAR:
    case PrimitiveType::TYPE_CHAR:
        return _decode_string_column<is_filter>(col_name, data_column, orc_column_type->getKind(),
                                                cvb, num_values);
    case PrimitiveType::TYPE_ARRAY: {
        if (orc_column_type->getKind() != orc::TypeKind::LIST) {
            return Status::InternalError(
                    "Wrong data type for column '{}', expected list, actual {}", col_name,
                    orc_column_type->getKind());
        }
        const auto* orc_list = dynamic_cast<const orc::ListVectorBatch*>(cvb);
        auto& doris_offsets = static_cast<ColumnArray&>(*data_column).get_offsets();
        const auto& orc_offsets = orc_list->offsets;
        size_t element_size = 0;
        RETURN_IF_ERROR(_fill_doris_array_offsets(col_name, doris_offsets, orc_offsets, num_values,
                                                  &element_size));
        DataTypePtr& nested_type = const_cast<DataTypePtr&>(
                reinterpret_cast<const DataTypeArray*>(remove_nullable(data_type).get())
                        ->get_nested_type());
        const orc::Type* nested_orc_type = orc_column_type->getSubtype(0);
        std::string element_name = col_name + ".element";
        return _orc_column_to_doris_column<false>(
                element_name, static_cast<ColumnArray&>(*data_column).get_data_ptr(), nested_type,
                nested_orc_type, orc_list->elements.get(), element_size);
    }
    case PrimitiveType::TYPE_MAP: {
        if (orc_column_type->getKind() != orc::TypeKind::MAP) {
            return Status::InternalError("Wrong data type for column '{}', expected map, actual {}",
                                         col_name, orc_column_type->getKind());
        }
        const auto* orc_map = dynamic_cast<const orc::MapVectorBatch*>(cvb);
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
        ColumnPtr& doris_key_column = doris_map.get_keys_ptr();
        ColumnPtr& doris_value_column = doris_map.get_values_ptr();
        std::string key_col_name = col_name + ".key";
        std::string value_col_name = col_name + ".value";
        RETURN_IF_ERROR(_orc_column_to_doris_column<false>(key_col_name, doris_key_column,
                                                           doris_key_type, orc_key_type,
                                                           orc_map->keys.get(), element_size));
        RETURN_IF_ERROR(_orc_column_to_doris_column<false>(value_col_name, doris_value_column,
                                                           doris_value_type, orc_value_type,
                                                           orc_map->elements.get(), element_size));
        return doris_map.deduplicate_keys();
    }
    case PrimitiveType::TYPE_STRUCT: {
        if (orc_column_type->getKind() != orc::TypeKind::STRUCT) {
            return Status::InternalError(
                    "Wrong data type for column '{}', expected struct, actual {}", col_name,
                    orc_column_type->getKind());
        }
        const auto* orc_struct = dynamic_cast<const orc::StructVectorBatch*>(cvb);
        auto& doris_struct = static_cast<ColumnStruct&>(*data_column);
        std::map<int, int> read_fields;
        std::set<int> missing_fields;
        const auto* doris_struct_type =
                assert_cast<const DataTypeStruct*>(remove_nullable(data_type).get());
        for (int i = 0; i < doris_struct.tuple_size(); ++i) {
            bool is_missing_col = true;
            for (int j = 0; j < orc_column_type->getSubtypeCount(); ++j) {
                if (boost::iequals(doris_struct_type->get_name_by_position(i),
                                   orc_column_type->getFieldName(j))) {
                    read_fields[i] = j;
                    is_missing_col = false;
                    break;
                }
            }
            if (is_missing_col) {
                missing_fields.insert(i);
            }
        }

        for (int missing_field : missing_fields) {
            ColumnPtr& doris_field = doris_struct.get_column_ptr(missing_field);
            if (!doris_field->is_nullable()) {
                return Status::InternalError(
                        "Child field of '{}' is not nullable, but is missing in orc file",
                        col_name);
            }
            reinterpret_cast<ColumnNullable*>(doris_field->assume_mutable().get())
                    ->insert_many_defaults(num_values);
        }

        for (auto read_field : read_fields) {
            orc::ColumnVectorBatch* orc_field = orc_struct->fields[read_field.second];
            const orc::Type* orc_type = orc_column_type->getSubtype(read_field.second);
            std::string field_name =
                    col_name + "." + orc_column_type->getFieldName(read_field.second);
            ColumnPtr& doris_field = doris_struct.get_column_ptr(read_field.first);
            const DataTypePtr& doris_type = doris_struct_type->get_element(read_field.first);
            RETURN_IF_ERROR(_orc_column_to_doris_column<is_filter>(
                    field_name, doris_field, doris_type, orc_type, orc_field, num_values));
        }
        return Status::OK();
    }
    default:
        break;
    }
    return Status::InternalError("Unsupported type for column '{}'", col_name);
}

template <bool is_filter>
Status OrcReader::_orc_column_to_doris_column(const std::string& col_name, ColumnPtr& doris_column,
                                              const DataTypePtr& data_type,
                                              const orc::Type* orc_column_type,
                                              const orc::ColumnVectorBatch* cvb,
                                              size_t num_values) {
    auto src_type = convert_to_doris_type(orc_column_type);
    bool is_dict_filter_col = false;
    for (const std::pair<std::string, int>& dict_col : _dict_filter_cols) {
        if (col_name == dict_col.first) {
            src_type = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
            is_dict_filter_col = true;
            break;
        }
    }
    // If the column can be dictionary filtered, there will be two types.
    // It may be plain or a dictionary, because the same field in different stripes may have different types.
    // Here we use the $dict_ prefix to represent the dictionary type converter.
    auto converter_key = !is_dict_filter_col ? col_name : fmt::format("$dict_{}", col_name);

    if (!_converters.contains(converter_key)) {
        std::unique_ptr<converter::ColumnTypeConverter> converter =
                converter::ColumnTypeConverter::get_converter(src_type, data_type,
                                                              converter::FileFormat::ORC);
        if (!converter->support()) {
            return Status::InternalError(
                    "The column type of '{}' has changed and is not supported: ", col_name,
                    converter->get_error_msg());
        }
        // reuse the cached converter
        _converters[converter_key] = std::move(converter);
    }
    converter::ColumnTypeConverter* converter = _converters[converter_key].get();
    ColumnPtr resolved_column = converter->get_column(src_type, doris_column, data_type);
    const DataTypePtr& resolved_type = converter->get_type();

    MutableColumnPtr data_column;
    if (resolved_column->is_nullable()) {
        SCOPED_RAW_TIMER(&_statistics.decode_null_map_time);
        auto* nullable_column =
                reinterpret_cast<ColumnNullable*>(resolved_column->assume_mutable().get());
        data_column = nullable_column->get_nested_column_ptr();
        NullMap& map_data_column = nullable_column->get_null_map_data();
        auto origin_size = map_data_column.size();
        map_data_column.resize(origin_size + num_values);
        if (cvb->hasNulls) {
            const auto* cvb_nulls = cvb->notNull.data();
            for (int i = 0; i < num_values; ++i) {
                map_data_column[origin_size + i] = !cvb_nulls[i];
            }
        } else {
            memset(map_data_column.data() + origin_size, 0, num_values);
        }
    } else {
        if (cvb->hasNulls) {
            return Status::InternalError("Not nullable column {} has null values in orc file",
                                         col_name);
        }
        data_column = resolved_column->assume_mutable();
    }

    RETURN_IF_ERROR(_fill_doris_data_column<is_filter>(col_name, data_column,
                                                       remove_nullable(resolved_type),
                                                       orc_column_type, cvb, num_values));
    // resolve schema change
    auto converted_column = doris_column->assume_mutable();
    return converter->convert(resolved_column, converted_column);
}

std::string OrcReader::get_field_name_lower_case(const orc::Type* orc_type, int pos) {
    std::string name = orc_type->getFieldName(pos);
    transform(name.begin(), name.end(), name.begin(), ::tolower);
    return name;
}

Status OrcReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    RETURN_IF_ERROR(get_next_block_impl(block, read_rows, eof));
    if (*eof) {
        COUNTER_UPDATE(_orc_profile.selected_row_group_count,
                       _reader_metrics.SelectedRowGroupCount);
        COUNTER_UPDATE(_orc_profile.evaluated_row_group_count,
                       _reader_metrics.EvaluatedRowGroupCount);
    }
    if (_orc_filter) {
        RETURN_IF_ERROR(_orc_filter->get_status());
    }
    if (_string_dict_filter) {
        RETURN_IF_ERROR(_string_dict_filter->get_status());
    }
    return Status::OK();
}

Status OrcReader::get_next_block_impl(Block* block, size_t* read_rows, bool* eof) {
    if (_io_ctx && _io_ctx->should_stop) {
        *eof = true;
        *read_rows = 0;
        return Status::OK();
    }
    if (_push_down_agg_type == TPushAggOp::type::COUNT) {
        auto rows = std::min(get_remaining_rows(), (int64_t)_batch_size);

        set_remaining_rows(get_remaining_rows() - rows);
        auto mutate_columns = block->mutate_columns();
        for (auto& col : mutate_columns) {
            col->resize(rows);
        }
        block->set_columns(std::move(mutate_columns));
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
                if (rr == 0 || _batch->numElements == 0) {
                    *eof = true;
                    *read_rows = 0;
                    return Status::OK();
                }
            } catch (std::exception& e) {
                std::string _err_msg = e.what();
                if (_io_ctx && _io_ctx->should_stop && _err_msg == "stop") {
                    block->clear_column_data();
                    *eof = true;
                    *read_rows = 0;
                    return Status::OK();
                }
                return Status::InternalError("Orc row reader nextBatch failed. reason = {}",
                                             _err_msg);
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

        RETURN_IF_ERROR(_fill_partition_columns(block, _batch->numElements,
                                                _lazy_read_ctx.partition_columns));
        RETURN_IF_ERROR(
                _fill_missing_columns(block, _batch->numElements, _lazy_read_ctx.missing_columns));

        if (block->rows() == 0) {
            RETURN_IF_ERROR(_convert_dict_cols_to_string_cols(block, nullptr));
            *eof = true;
            *read_rows = 0;
            return Status::OK();
        }
        _execute_filter_position_delete_rowids(*_filter);
        {
            SCOPED_RAW_TIMER(&_statistics.decode_null_map_time);
            RETURN_IF_CATCH_EXCEPTION(
                    Block::filter_block_internal(block, columns_to_filter, *_filter));
        }
        if (!_not_single_slot_filter_conjuncts.empty()) {
            RETURN_IF_ERROR(_convert_dict_cols_to_string_cols(block, &batch_vec));
            RETURN_IF_CATCH_EXCEPTION(
                    RETURN_IF_ERROR(VExprContext::execute_conjuncts_and_filter_block(
                            _not_single_slot_filter_conjuncts, block, columns_to_filter,
                            column_to_keep)));
        } else {
            Block::erase_useless_column(block, column_to_keep);
            RETURN_IF_ERROR(_convert_dict_cols_to_string_cols(block, &batch_vec));
        }
        *read_rows = block->rows();
    } else {
        uint64_t rr;
        SCOPED_RAW_TIMER(&_statistics.column_read_time);
        {
            SCOPED_RAW_TIMER(&_statistics.get_batch_time);
            // reset decimal_scale_params_index;
            _decimal_scale_params_index = 0;
            try {
                rr = _row_reader->nextBatch(*_batch, block);
                if (rr == 0 || _batch->numElements == 0) {
                    *eof = true;
                    *read_rows = 0;
                    return Status::OK();
                }
            } catch (std::exception& e) {
                std::string _err_msg = e.what();
                if (_io_ctx && _io_ctx->should_stop && _err_msg == "stop") {
                    block->clear_column_data();
                    *eof = true;
                    *read_rows = 0;
                    return Status::OK();
                }
                return Status::InternalError("Orc row reader nextBatch failed. reason = {}",
                                             _err_msg);
            }
        }

        if (!_dict_cols_has_converted && !_dict_filter_cols.empty()) {
            for (auto& dict_filter_cols : _dict_filter_cols) {
                MutableColumnPtr dict_col_ptr = ColumnVector<Int32>::create();
                size_t pos = block->get_position_by_name(dict_filter_cols.first);
                auto& column_with_type_and_name = block->get_by_position(pos);
                auto& column_type = column_with_type_and_name.type;
                if (column_type->is_nullable()) {
                    block->get_by_position(pos).type =
                            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
                    block->replace_by_position(
                            pos,
                            ColumnNullable::create(std::move(dict_col_ptr),
                                                   ColumnUInt8::create(dict_col_ptr->size(), 0)));
                } else {
                    block->get_by_position(pos).type = std::make_shared<DataTypeInt32>();
                    block->replace_by_position(pos, std::move(dict_col_ptr));
                }
            }
            _dict_cols_has_converted = true;
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

        RETURN_IF_ERROR(_fill_partition_columns(block, _batch->numElements,
                                                _lazy_read_ctx.partition_columns));
        RETURN_IF_ERROR(
                _fill_missing_columns(block, _batch->numElements, _lazy_read_ctx.missing_columns));

        if (block->rows() == 0) {
            RETURN_IF_ERROR(_convert_dict_cols_to_string_cols(block, nullptr));
            *eof = true;
            *read_rows = 0;
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
                return _convert_dict_cols_to_string_cols(block, &batch_vec);
            }
            _execute_filter_position_delete_rowids(result_filter);
            {
                SCOPED_RAW_TIMER(&_statistics.filter_block_time);
                RETURN_IF_CATCH_EXCEPTION(
                        Block::filter_block_internal(block, columns_to_filter, result_filter));
            }
            //_not_single_slot_filter_conjuncts check : missing column1 == missing column2 , missing column == exists column  ...
            if (!_not_single_slot_filter_conjuncts.empty()) {
                RETURN_IF_ERROR(_convert_dict_cols_to_string_cols(block, &batch_vec));
                RETURN_IF_CATCH_EXCEPTION(
                        RETURN_IF_ERROR(VExprContext::execute_conjuncts_and_filter_block(
                                _not_single_slot_filter_conjuncts, block, columns_to_filter,
                                column_to_keep)));
            } else {
                Block::erase_useless_column(block, column_to_keep);
                RETURN_IF_ERROR(_convert_dict_cols_to_string_cols(block, &batch_vec));
            }
        } else {
            if (_delete_rows_filter_ptr) {
                _execute_filter_position_delete_rowids(*_delete_rows_filter_ptr);
                SCOPED_RAW_TIMER(&_statistics.filter_block_time);
                RETURN_IF_CATCH_EXCEPTION(Block::filter_block_internal(block, columns_to_filter,
                                                                       (*_delete_rows_filter_ptr)));
            } else {
                std::unique_ptr<IColumn::Filter> filter(new IColumn::Filter(block->rows(), 1));
                _execute_filter_position_delete_rowids(*filter);
                SCOPED_RAW_TIMER(&_statistics.filter_block_time);
                RETURN_IF_CATCH_EXCEPTION(
                        Block::filter_block_internal(block, columns_to_filter, (*filter)));
            }
            Block::erase_useless_column(block, column_to_keep);
            RETURN_IF_ERROR(_convert_dict_cols_to_string_cols(block, &batch_vec));
        }
        *read_rows = block->rows();
    }
    return Status::OK();
}

void OrcReader::_fill_batch_vec(std::vector<orc::ColumnVectorBatch*>& result,
                                orc::ColumnVectorBatch* batch, int idx) {
    for (auto* field : dynamic_cast<const orc::StructVectorBatch*>(batch)->fields) {
        result.push_back(field);
        if (_is_acid && _col_orc_type[idx++]->getKind() == orc::TypeKind::STRUCT) {
            _fill_batch_vec(result, field, idx);
        }
    }
}

void OrcReader::_build_delete_row_filter(const Block* block, size_t rows) {
    // transactional hive orc delete row
    if (_delete_rows != nullptr) {
        _delete_rows_filter_ptr = std::make_unique<IColumn::Filter>(rows, 1);
        auto* __restrict _pos_delete_filter_data = _delete_rows_filter_ptr->data();
        const auto& original_transaction_column = assert_cast<const ColumnInt64&>(*remove_nullable(
                block->get_by_name(TransactionalHive::ORIGINAL_TRANSACTION_LOWER_CASE).column));
        const auto& bucket_id_column = assert_cast<const ColumnInt32&>(
                *remove_nullable(block->get_by_name(TransactionalHive::BUCKET_LOWER_CASE).column));
        const auto& row_id_column = assert_cast<const ColumnInt64&>(
                *remove_nullable(block->get_by_name(TransactionalHive::ROW_ID_LOWER_CASE).column));
        for (int i = 0; i < rows; ++i) {
            auto original_transaction = original_transaction_column.get_int(i);
            auto bucket_id = bucket_id_column.get_int(i);
            auto row_id = row_id_column.get_int(i);

            TransactionalHiveReader::AcidRowID transactional_row_id = {original_transaction,
                                                                       bucket_id, row_id};
            if (_delete_rows->contains(transactional_row_id)) {
                _pos_delete_filter_data[i] = 0;
            }
        }
    }
}

Status OrcReader::filter(orc::ColumnVectorBatch& data, uint16_t* sel, uint16_t size, void* arg) {
    auto* block = (Block*)arg;
    size_t origin_column_num = block->columns();

    if (!_dict_cols_has_converted && !_dict_filter_cols.empty()) {
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
        _dict_cols_has_converted = true;
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

    _filter = std::make_unique<IColumn::Filter>(size, 1);
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
        RETURN_IF_ERROR(_convert_dict_cols_to_string_cols(block, nullptr));
    }

    uint16_t new_size = 0;
    for (uint16_t i = 0; i < size; i++) {
        sel[new_size] = i;
        new_size += result_filter_data[i] ? 1 : 0;
    }
    data.numElements = new_size;
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
    for (const auto& predicate_col_name : predicate_col_names) {
        int slot_id = predicate_col_slot_ids[i];
        if (_can_filter_by_dict(slot_id)) {
            _dict_filter_cols.emplace_back(predicate_col_name, slot_id);
            column_names.emplace_back(_col_name_to_file_col_name[predicate_col_name]);
        } else {
            if (_slot_id_to_filter_conjuncts->find(slot_id) !=
                _slot_id_to_filter_conjuncts->end()) {
                for (const auto& ctx : _slot_id_to_filter_conjuncts->at(slot_id)) {
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
    for (auto* each : slots) {
        if (each->id() == slot_id) {
            slot = each;
            break;
        }
    }
    if (slot == nullptr) {
        return false;
    }
    if (!is_string_type(slot->type()->get_primitive_type()) &&
        !is_var_len_object(slot->type()->get_primitive_type())) {
        return false;
    }

    if (_slot_id_to_filter_conjuncts->find(slot_id) == _slot_id_to_filter_conjuncts->end()) {
        return false;
    }

    std::function<bool(const VExpr* expr)> visit_function_call = [&](const VExpr* expr) {
        // TODO: The current implementation of dictionary filtering does not take into account
        //  the implementation of NULL values because the dictionary itself does not contain
        //  NULL value encoding. As a result, many NULL-related functions or expressions
        //  cannot work properly, such as is null, is not null, coalesce, etc.
        //  Here we first disable dictionary filtering when predicate expr is not slot.
        //  Implementation of NULL value dictionary filtering will be carried out later.
        if (expr->node_type() != TExprNodeType::SLOT_REF) {
            return false;
        }
        return std::ranges::all_of(expr->children(), [&](const auto& child) {
            return visit_function_call(child.get());
        });
    };
    return std::ranges::all_of(_slot_id_to_filter_conjuncts->at(slot_id), [&](const auto& ctx) {
        return visit_function_call(ctx->root().get());
    });
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
            for (const auto& ctx : iter->second) {
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
            StringRef dict_value((length > 0) ? val_ptr : "", length);
            if (length > max_value_length) {
                max_value_length = length;
            }
            dict_values.emplace_back(dict_value);
        }
        dict_value_column->insert_many_strings_overflow(dict_values.data(), dict_values.size(),
                                                        max_value_length);
        size_t dict_value_column_size = dict_value_column->size();
        // 2. Build a temp block from the dict string column, then execute conjuncts and filter block.
        // 2.1 Build a temp block from the dict string column to match the conjuncts executing.
        Block temp_block;
        int dict_pos = -1;
        int index = 0;
        for (const auto slot_desc : _tuple_descriptor->slots()) {
            if (!slot_desc->is_materialized()) {
                // should be ignored from reading
                continue;
            }
            if (slot_desc->id() == slot_id) {
                auto data_type = slot_desc->get_data_type_ptr();
                if (data_type->is_nullable()) {
                    temp_block.insert(
                            {ColumnNullable::create(
                                     std::move(
                                             dict_value_column), // NOLINT(bugprone-use-after-move)
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

        // 2.2 Execute conjuncts.
        if (dict_pos != 0) {
            // VExprContext.execute has an optimization, the filtering is executed when block->rows() > 0
            // The following process may be tricky and time-consuming, but we have no other way.
            temp_block.get_by_position(0).column->assume_mutable()->resize(dict_value_column_size);
        }
        IColumn::Filter result_filter(temp_block.rows(), 1);
        bool can_filter_all;
        RETURN_IF_ERROR(VExprContext::execute_conjuncts(ctxs, nullptr, &temp_block, &result_filter,
                                                        &can_filter_all));
        if (dict_pos != 0) {
            // We have to clean the first column to insert right data.
            temp_block.get_by_position(0).column->assume_mutable()->clear();
        }

        // If can_filter_all = true, can filter this stripe.
        if (can_filter_all) {
            *is_stripe_filtered = true;
            return Status::OK();
        }

        // 3. Get dict codes.
        std::vector<int32_t> dict_codes;
        for (size_t i = 0; i < result_filter.size(); ++i) {
            if (result_filter[i]) {
                dict_codes.emplace_back(i);
            }
        }

        // About Performance: if dict_column size is too large, it will generate a large IN filter.
        if (dict_codes.size() > MAX_DICT_CODE_PREDICATE_TO_REWRITE) {
            it = _dict_filter_cols.erase(it);
            for (auto& ctx : ctxs) {
                _non_dict_filter_conjuncts.emplace_back(ctx);
            }
            continue;
        }

        // 4. Rewrite conjuncts.
        RETURN_IF_ERROR(_rewrite_dict_conjuncts(
                dict_codes, slot_id, temp_block.get_by_position(dict_pos).column->is_nullable()));
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
            texpr_node.__set_num_children(2);
            texpr_node.__set_is_nullable(is_nullable);
            root = VectorizedFnCall::create_shared(texpr_node);
        }
        {
            SlotDescriptor* slot = nullptr;
            const std::vector<SlotDescriptor*>& slots = _tuple_descriptor->slots();
            for (auto* each : slots) {
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

            std::shared_ptr<HybridSetBase> hybrid_set(
                    create_set(PrimitiveType::TYPE_INT, dict_codes.size(), false));
            for (int& dict_code : dict_codes) {
                hybrid_set->insert(&dict_code);
            }
            root = vectorized::VDirectInPredicate::create_shared(node, hybrid_set);
        }
        {
            SlotDescriptor* slot = nullptr;
            const std::vector<SlotDescriptor*>& slots = _tuple_descriptor->slots();
            for (auto* each : slots) {
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
    if (!_dict_cols_has_converted) {
        return Status::OK();
    }
    if (!_dict_filter_cols.empty()) {
        for (auto& dict_filter_cols : _dict_filter_cols) {
            size_t pos = block->get_position_by_name(dict_filter_cols.first);
            ColumnWithTypeAndName& column_with_type_and_name = block->get_by_position(pos);
            const ColumnPtr& column = column_with_type_and_name.column;
            auto orc_col_idx = _colname_to_idx.find(dict_filter_cols.first);
            if (orc_col_idx == _colname_to_idx.end()) {
                return Status::InternalError("Wrong read column '{}' in orc file",
                                             dict_filter_cols.first);
            }
            if (const auto* nullable_column = check_and_get_column<ColumnNullable>(*column)) {
                const ColumnPtr& nested_column = nullable_column->get_nested_column_ptr();
                const auto* dict_column = assert_cast<const ColumnInt32*>(nested_column.get());
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
                const auto* dict_column = assert_cast<const ColumnInt32*>(column.get());
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
        _dict_cols_has_converted = false;
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
    if (orc_column_type->getKind() == orc::TypeKind::CHAR) {
        // Possibly there are some zero padding characters in CHAR type, we have to strip them off.
        if (null_map) {
            const auto* null_map_data = null_map->data();
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
                    string_values.emplace_back((length > 0) ? val_ptr : EMPTY_STRING_FOR_OVERFLOW,
                                               length);
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
                string_values.emplace_back((length > 0) ? val_ptr : EMPTY_STRING_FOR_OVERFLOW,
                                           length);
            }
        }
    } else {
        if (null_map) {
            const auto* null_map_data = null_map->data();
            for (int i = 0; i < num_values; ++i) {
                if (!null_map_data[i]) {
                    char* val_ptr;
                    int64_t length;
                    encoded_string_vector_batch->dictionary->getValueByIndex(dict_data[i], val_ptr,
                                                                             length);
                    if (length > max_value_length) {
                        max_value_length = length;
                    }
                    string_values.emplace_back((length > 0) ? val_ptr : EMPTY_STRING_FOR_OVERFLOW,
                                               length);
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
                string_values.emplace_back((length > 0) ? val_ptr : EMPTY_STRING_FOR_OVERFLOW,
                                           length);
            }
        }
    }
    if (!string_values.empty()) {
        res->insert_many_strings_overflow(string_values.data(), num_values, max_value_length);
    }
    return res;
}

void ORCFileInputStream::beforeReadStripe(
        std::unique_ptr<orc::StripeInformation> current_strip_information,
        const std::vector<bool>& selected_columns,
        std::unordered_map<orc::StreamId, std::shared_ptr<InputStream>>& streams) {
    if (_is_all_tiny_stripes) {
        return;
    }
    if (_file_reader != nullptr) {
        _file_reader->collect_profile_before_close();
    }
    for (const auto& stripe_stream : _stripe_streams) {
        if (stripe_stream != nullptr) {
            stripe_stream->collect_profile_before_close();
        }
    }
    _stripe_streams.clear();

    uint64_t offset = current_strip_information->getOffset();
    std::unordered_map<orc::StreamId, io::PrefetchRange> prefetch_ranges;
    for (uint64_t stream_id = 0; stream_id < current_strip_information->getNumberOfStreams();
         ++stream_id) {
        std::unique_ptr<orc::StreamInformation> stream =
                current_strip_information->getStreamInformation(stream_id);
        uint64_t columnId = stream->getColumnId();
        uint64_t length = stream->getLength();
        if (selected_columns[columnId]) {
            doris::io::PrefetchRange prefetch_range = {offset, offset + length};
            orc::StreamId streamId(stream->getColumnId(), stream->getKind());
            prefetch_ranges.emplace(std::move(streamId), std::move(prefetch_range));
        }
        offset += length;
    }
    _build_input_stripe_streams(prefetch_ranges, streams);
}

void ORCFileInputStream::_build_input_stripe_streams(
        const std::unordered_map<orc::StreamId, io::PrefetchRange>& ranges,
        std::unordered_map<orc::StreamId, std::shared_ptr<InputStream>>& streams) {
    if (ranges.empty()) {
        return;
    }

    std::unordered_map<orc::StreamId, io::PrefetchRange> small_ranges;
    std::unordered_map<orc::StreamId, io::PrefetchRange> large_ranges;

    for (const auto& range : ranges) {
        if (range.second.end_offset - range.second.start_offset <= _orc_once_max_read_bytes) {
            small_ranges.emplace(range.first, range.second);
        } else {
            large_ranges.emplace(range.first, range.second);
        }
    }

    _build_small_ranges_input_stripe_streams(small_ranges, streams);
    _build_large_ranges_input_stripe_streams(large_ranges, streams);
}

void ORCFileInputStream::_build_small_ranges_input_stripe_streams(
        const std::unordered_map<orc::StreamId, io::PrefetchRange>& ranges,
        std::unordered_map<orc::StreamId, std::shared_ptr<InputStream>>& streams) {
    std::vector<io::PrefetchRange> all_ranges;
    all_ranges.reserve(ranges.size());
    std::transform(ranges.begin(), ranges.end(), std::back_inserter(all_ranges),
                   [](const auto& pair) { return pair.second; });

    auto merged_ranges = io::PrefetchRange::merge_adjacent_seq_ranges(
            all_ranges, _orc_max_merge_distance_bytes, _orc_once_max_read_bytes);

    // Sort ranges by start_offset for efficient searching
    std::vector<std::pair<orc::StreamId, io::PrefetchRange>> sorted_ranges(ranges.begin(),
                                                                           ranges.end());
    std::sort(sorted_ranges.begin(), sorted_ranges.end(), [](const auto& a, const auto& b) {
        return a.second.start_offset < b.second.start_offset;
    });

    for (const auto& merged_range : merged_ranges) {
        auto merge_range_file_reader =
                std::make_shared<OrcMergeRangeFileReader>(_profile, _file_reader, merged_range);

        // Use binary search to find the starting point in sorted_ranges
        auto it =
                std::lower_bound(sorted_ranges.begin(), sorted_ranges.end(),
                                 merged_range.start_offset, [](const auto& pair, uint64_t offset) {
                                     return pair.second.start_offset < offset;
                                 });

        // Iterate from the found starting point
        for (; it != sorted_ranges.end() && it->second.start_offset < merged_range.end_offset;
             ++it) {
            if (it->second.end_offset <= merged_range.end_offset) {
                auto stripe_stream_input_stream = std::make_shared<StripeStreamInputStream>(
                        getName(), merge_range_file_reader, _statistics, _io_ctx, _profile);
                streams.emplace(it->first, stripe_stream_input_stream);
                _stripe_streams.emplace_back(stripe_stream_input_stream);
            }
        }
    }
}

void ORCFileInputStream::_build_large_ranges_input_stripe_streams(
        const std::unordered_map<orc::StreamId, io::PrefetchRange>& ranges,
        std::unordered_map<orc::StreamId, std::shared_ptr<InputStream>>& streams) {
    for (const auto& range : ranges) {
        auto stripe_stream_input_stream = std::make_shared<StripeStreamInputStream>(
                getName(), _file_reader, _statistics, _io_ctx, _profile);
        streams.emplace(range.first,
                        std::make_shared<StripeStreamInputStream>(getName(), _file_reader,
                                                                  _statistics, _io_ctx, _profile));
        _stripe_streams.emplace_back(stripe_stream_input_stream);
    }
}

void ORCFileInputStream::_collect_profile_before_close() {
    if (_file_reader != nullptr) {
        _file_reader->collect_profile_before_close();
    }
    for (const auto& stripe_stream : _stripe_streams) {
        if (stripe_stream != nullptr) {
            stripe_stream->collect_profile_before_close();
        }
    }
}

void OrcReader::_execute_filter_position_delete_rowids(IColumn::Filter& filter) {
    if (_position_delete_ordered_rowids == nullptr) {
        return;
    }
    auto start = _row_reader->getRowNumber();
    auto nums = _batch->numElements;
    auto l = std::lower_bound(_position_delete_ordered_rowids->begin(),
                              _position_delete_ordered_rowids->end(), start);
    auto r = std::upper_bound(_position_delete_ordered_rowids->begin(),
                              _position_delete_ordered_rowids->end(), start + nums - 1);
    for (; l < r; l++) {
        filter[*l - start] = 0;
    }
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
