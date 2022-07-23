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

#include "olap/reader.h"

#include <parallel_hashmap/phmap.h>

#include <boost/algorithm/string/case_conv.hpp>
#include <charconv>
#include <unordered_set>

#include "common/status.h"
#include "olap/bloom_filter_predicate.h"
#include "olap/collect_iterator.h"
#include "olap/comparison_predicate.h"
#include "olap/in_list_predicate.h"
#include "olap/like_column_predicate.h"
#include "olap/null_predicate.h"
#include "olap/olap_common.h"
#include "olap/row.h"
#include "olap/row_cursor.h"
#include "olap/schema.h"
#include "olap/tablet.h"
#include "runtime/mem_pool.h"
#include "util/date_func.h"
#include "util/mem_util.hpp"
#include "vec/data_types/data_type_decimal.h"

using std::nothrow;
using std::set;
using std::vector;

namespace doris {

void TabletReader::ReaderParams::check_validation() const {
    if (UNLIKELY(version.first == -1)) {
        LOG(FATAL) << "version is not set. tablet=" << tablet->full_name();
    }
}

std::string TabletReader::ReaderParams::to_string() const {
    std::stringstream ss;
    ss << "tablet=" << tablet->full_name() << " reader_type=" << reader_type
       << " aggregation=" << aggregation << " version=" << version
       << " start_key_include=" << start_key_include << " end_key_include=" << end_key_include;

    for (const auto& key : start_key) {
        ss << " keys=" << key;
    }

    for (const auto& key : end_key) {
        ss << " end_keys=" << key;
    }

    for (auto& condition : conditions) {
        ss << " conditions=" << apache::thrift::ThriftDebugString(condition);
    }

    return ss.str();
}

std::string TabletReader::KeysParam::to_string() const {
    std::stringstream ss;
    ss << "start_key_include=" << start_key_include << " end_key_include=" << end_key_include;

    for (auto& start_key : start_keys) {
        ss << " keys=" << start_key.to_string();
    }
    for (auto& end_key : end_keys) {
        ss << " end_keys=" << end_key.to_string();
    }

    return ss.str();
}

TabletReader::~TabletReader() {
    VLOG_NOTICE << "merged rows:" << _merged_rows;
    _conditions.finalize();
    if (!_all_conditions.empty()) {
        _all_conditions.finalize();
    }
    _delete_handler.finalize();

    for (auto pred : _col_predicates) {
        delete pred;
    }
    for (auto pred : _value_col_predicates) {
        delete pred;
    }
}

Status TabletReader::init(const ReaderParams& read_params) {
#ifndef NDEBUG
    _predicate_mem_pool.reset(new MemPool());
#else
    _predicate_mem_pool.reset(new MemPool());
#endif

    Status res = _init_params(read_params);
    if (!res.ok()) {
        LOG(WARNING) << "fail to init reader when init params. res:" << res
                     << ", tablet_id:" << read_params.tablet->tablet_id()
                     << ", schema_hash:" << read_params.tablet->schema_hash()
                     << ", reader type:" << read_params.reader_type
                     << ", version:" << read_params.version;
    }
    return res;
}

// When only one rowset has data, and this rowset is nonoverlapping, we can read directly without aggregation
bool TabletReader::_optimize_for_single_rowset(
        const std::vector<RowsetReaderSharedPtr>& rs_readers) {
    bool has_delete_rowset = false;
    bool has_overlapping = false;
    int nonoverlapping_count = 0;
    for (const auto& rs_reader : rs_readers) {
        if (rs_reader->rowset()->rowset_meta()->delete_flag()) {
            has_delete_rowset = true;
            break;
        }
        if (rs_reader->rowset()->rowset_meta()->num_rows() > 0) {
            if (rs_reader->rowset()->rowset_meta()->is_segments_overlapping()) {
                // when there are overlapping segments, can not do directly read
                has_overlapping = true;
                break;
            } else if (++nonoverlapping_count > 1) {
                break;
            }
        }
    }

    return !has_overlapping && nonoverlapping_count == 1 && !has_delete_rowset;
}

Status TabletReader::_capture_rs_readers(const ReaderParams& read_params,
                                         std::vector<RowsetReaderSharedPtr>* valid_rs_readers) {
    const std::vector<RowsetReaderSharedPtr>* rs_readers = &read_params.rs_readers;
    if (rs_readers->empty()) {
        LOG(WARNING) << "fail to acquire data sources. tablet=" << _tablet->full_name();
        return Status::OLAPInternalError(OLAP_ERR_WRITE_PROTOBUF_ERROR);
    }

    bool eof = false;
    bool is_lower_key_included = _keys_param.start_key_include;
    bool is_upper_key_included = _keys_param.end_key_include;

    for (int i = 0; i < _keys_param.start_keys.size(); ++i) {
        // lower bound
        RowCursor& start_key = _keys_param.start_keys[i];
        RowCursor& end_key = _keys_param.end_keys[i];

        if (!is_lower_key_included) {
            if (compare_row_key(start_key, end_key) >= 0) {
                VLOG_NOTICE << "return EOF when lower key not include"
                            << ", start_key=" << start_key.to_string()
                            << ", end_key=" << end_key.to_string();
                eof = true;
                break;
            }
        } else {
            if (compare_row_key(start_key, end_key) > 0) {
                VLOG_NOTICE << "return EOF when lower key include="
                            << ", start_key=" << start_key.to_string()
                            << ", end_key=" << end_key.to_string();
                eof = true;
                break;
            }
        }

        _is_lower_keys_included.push_back(is_lower_key_included);
        _is_upper_keys_included.push_back(is_upper_key_included);
    }

    if (eof) {
        return Status::OK();
    }

    bool need_ordered_result = true;
    if (read_params.reader_type == READER_QUERY) {
        if (_tablet_schema->keys_type() == DUP_KEYS) {
            // duplicated keys are allowed, no need to merge sort keys in rowset
            need_ordered_result = false;
        }
        if (_aggregation) {
            // compute engine will aggregate rows with the same key,
            // it's ok for rowset to return unordered result
            need_ordered_result = false;
        }
    }

    _reader_context.reader_type = read_params.reader_type;
    _reader_context.tablet_schema = _tablet_schema;
    _reader_context.need_ordered_result = need_ordered_result;
    _reader_context.return_columns = &_return_columns;
    _reader_context.seek_columns = &_seek_columns;
    _reader_context.load_bf_columns = &_load_bf_columns;
    _reader_context.load_bf_all_columns = &_load_bf_all_columns;
    _reader_context.conditions = &_conditions;
    _reader_context.all_conditions = &_all_conditions;
    _reader_context.predicates = &_col_predicates;
    _reader_context.value_predicates = &_value_col_predicates;
    _reader_context.lower_bound_keys = &_keys_param.start_keys;
    _reader_context.is_lower_keys_included = &_is_lower_keys_included;
    _reader_context.upper_bound_keys = &_keys_param.end_keys;
    _reader_context.is_upper_keys_included = &_is_upper_keys_included;
    _reader_context.delete_handler = &_delete_handler;
    _reader_context.stats = &_stats;
    _reader_context.runtime_state = read_params.runtime_state;
    _reader_context.use_page_cache = read_params.use_page_cache;
    _reader_context.sequence_id_idx = _sequence_col_idx;
    _reader_context.batch_size = _batch_size;
    _reader_context.is_unique = tablet()->keys_type() == UNIQUE_KEYS;
    _reader_context.merged_rows = &_merged_rows;

    *valid_rs_readers = *rs_readers;

    return Status::OK();
}

Status TabletReader::_init_params(const ReaderParams& read_params) {
    read_params.check_validation();

    _direct_mode = read_params.direct_mode;
    _aggregation = read_params.aggregation;
    _need_agg_finalize = read_params.need_agg_finalize;
    _reader_type = read_params.reader_type;
    _tablet = read_params.tablet;
    _tablet_schema = read_params.tablet_schema;

    _init_conditions_param(read_params);
    _init_load_bf_columns(read_params);

    Status res = _init_delete_condition(read_params);
    if (!res.ok()) {
        LOG(WARNING) << "fail to init delete param. res = " << res;
        return res;
    }

    res = _init_return_columns(read_params);
    if (!res.ok()) {
        LOG(WARNING) << "fail to init return columns. res = " << res;
        return res;
    }

    res = _init_keys_param(read_params);
    if (!res.ok()) {
        LOG(WARNING) << "fail to init keys param. res=" << res;
        return res;
    }

    _init_seek_columns();

    if (_tablet_schema->has_sequence_col()) {
        auto sequence_col_idx = _tablet_schema->sequence_col_idx();
        DCHECK_NE(sequence_col_idx, -1);
        for (auto col : _return_columns) {
            // query has sequence col
            if (col == sequence_col_idx) {
                _sequence_col_idx = sequence_col_idx;
                break;
            }
        }
    }

    return res;
}

Status TabletReader::_init_return_columns(const ReaderParams& read_params) {
    if (read_params.reader_type == READER_QUERY) {
        _return_columns = read_params.return_columns;
        _tablet_columns_convert_to_null_set = read_params.tablet_columns_convert_to_null_set;

        if (!_delete_handler.empty()) {
            // We need to fetch columns which there are deletion conditions on them.
            set<uint32_t> column_set(_return_columns.begin(), _return_columns.end());
            for (const auto& conds : _delete_handler.get_delete_conditions()) {
                for (const auto& cond_column : conds.del_cond->columns()) {
                    if (column_set.find(cond_column.first) == column_set.end()) {
                        column_set.insert(cond_column.first);
                        _return_columns.push_back(cond_column.first);
                    }
                }
            }
        }
        for (auto id : read_params.return_columns) {
            if (_tablet_schema->column(id).is_key()) {
                _key_cids.push_back(id);
            } else {
                _value_cids.push_back(id);
            }
        }
    } else if (read_params.return_columns.empty()) {
        for (size_t i = 0; i < _tablet_schema->num_columns(); ++i) {
            _return_columns.push_back(i);
            if (_tablet_schema->column(i).is_key()) {
                _key_cids.push_back(i);
            } else {
                _value_cids.push_back(i);
            }
        }
        VLOG_NOTICE << "return column is empty, using full column as default.";
    } else if ((read_params.reader_type == READER_CUMULATIVE_COMPACTION ||
                read_params.reader_type == READER_BASE_COMPACTION ||
                read_params.reader_type == READER_ALTER_TABLE) &&
               !read_params.return_columns.empty()) {
        _return_columns = read_params.return_columns;
        for (auto id : read_params.return_columns) {
            if (_tablet_schema->column(id).is_key()) {
                _key_cids.push_back(id);
            } else {
                _value_cids.push_back(id);
            }
        }
    } else if (read_params.reader_type == READER_CHECKSUM) {
        _return_columns = read_params.return_columns;
        for (auto id : read_params.return_columns) {
            if (_tablet_schema->column(id).is_key()) {
                _key_cids.push_back(id);
            } else {
                _value_cids.push_back(id);
            }
        }
    } else {
        LOG(WARNING) << "fail to init return columns. [reader_type=" << read_params.reader_type
                     << " return_columns_size=" << read_params.return_columns.size() << "]";
        return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
    }

    std::sort(_key_cids.begin(), _key_cids.end(), std::greater<uint32_t>());

    return Status::OK();
}

void TabletReader::_init_seek_columns() {
    std::unordered_set<uint32_t> column_set(_return_columns.begin(), _return_columns.end());
    for (auto& it : _conditions.columns()) {
        column_set.insert(it.first);
    }
    size_t max_key_column_count = 0;
    for (const auto& key : _keys_param.start_keys) {
        max_key_column_count = std::max(max_key_column_count, key.field_count());
    }
    for (const auto& key : _keys_param.end_keys) {
        max_key_column_count = std::max(max_key_column_count, key.field_count());
    }

    for (size_t i = 0; i < _tablet_schema->num_columns(); i++) {
        if (i < max_key_column_count || column_set.find(i) != column_set.end()) {
            _seek_columns.push_back(i);
        }
    }
}

Status TabletReader::_init_keys_param(const ReaderParams& read_params) {
    if (read_params.start_key.empty()) {
        return Status::OK();
    }

    _keys_param.start_key_include = read_params.start_key_include;
    _keys_param.end_key_include = read_params.end_key_include;

    size_t start_key_size = read_params.start_key.size();
    //_keys_param.start_keys.resize(start_key_size);
    std::vector<RowCursor>(start_key_size).swap(_keys_param.start_keys);

    size_t scan_key_size = read_params.start_key.front().size();
    if (scan_key_size > _tablet_schema->num_columns()) {
        LOG(WARNING)
                << "Input param are invalid. Column count is bigger than num_columns of schema. "
                << "column_count=" << scan_key_size
                << ", schema.num_columns=" << _tablet_schema->num_columns();
        return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
    }

    std::vector<uint32_t> columns(scan_key_size);
    std::iota(columns.begin(), columns.end(), 0);

    std::shared_ptr<Schema> schema = std::make_shared<Schema>(_tablet_schema->columns(), columns);

    for (size_t i = 0; i < start_key_size; ++i) {
        if (read_params.start_key[i].size() != scan_key_size) {
            LOG(WARNING) << "The start_key.at(" << i
                         << ").size == " << read_params.start_key[i].size() << ", not equals the "
                         << scan_key_size;
            return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
        }

        Status res = _keys_param.start_keys[i].init_scan_key(
                *_tablet_schema, read_params.start_key[i].values(), schema);
        if (!res.ok()) {
            LOG(WARNING) << "fail to init row cursor. res = " << res;
            return res;
        }
        res = _keys_param.start_keys[i].from_tuple(read_params.start_key[i]);
        if (!res.ok()) {
            LOG(WARNING) << "fail to init row cursor from Keys. res=" << res << "key_index=" << i;
            return res;
        }
    }

    size_t end_key_size = read_params.end_key.size();
    //_keys_param.end_keys.resize(end_key_size);
    std::vector<RowCursor>(end_key_size).swap(_keys_param.end_keys);
    for (size_t i = 0; i < end_key_size; ++i) {
        if (read_params.end_key[i].size() != scan_key_size) {
            LOG(WARNING) << "The end_key.at(" << i << ").size == " << read_params.end_key[i].size()
                         << ", not equals the " << scan_key_size;
            return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
        }

        Status res = _keys_param.end_keys[i].init_scan_key(*_tablet_schema,
                                                           read_params.end_key[i].values(), schema);
        if (!res.ok()) {
            LOG(WARNING) << "fail to init row cursor. res = " << res;
            return res;
        }

        res = _keys_param.end_keys[i].from_tuple(read_params.end_key[i]);
        if (!res.ok()) {
            LOG(WARNING) << "fail to init row cursor from Keys. res=" << res << " key_index=" << i;
            return res;
        }
    }

    //TODO:check the valid of start_key and end_key.(eg. start_key <= end_key)

    return Status::OK();
}

void TabletReader::_init_conditions_param(const ReaderParams& read_params) {
    _conditions.set_tablet_schema(_tablet_schema);
    _all_conditions.set_tablet_schema(_tablet_schema);
    for (const auto& condition : read_params.conditions) {
        ColumnPredicate* predicate = _parse_to_predicate(condition);
        if (predicate != nullptr) {
            if (_tablet_schema->column(_tablet_schema->field_index(condition.column_name))
                        .aggregation() != FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE) {
                _value_col_predicates.push_back(predicate);
            } else {
                _col_predicates.push_back(predicate);
                Status status = _conditions.append_condition(condition);
                DCHECK_EQ(Status::OK(), status);
            }
            Status status = _all_conditions.append_condition(condition);
            DCHECK_EQ(Status::OK(), status);
        }
    }

    // Only key column bloom filter will push down to storage engine
    for (const auto& filter : read_params.bloom_filters) {
        _col_predicates.emplace_back(_parse_to_predicate(filter));
    }

    // Function filter push down to storage engine
    for (const auto& filter : read_params.function_filters) {
        _col_predicates.emplace_back(_parse_to_predicate(filter));
    }
}

#define COMPARISON_PREDICATE_CONDITION_VALUE(NAME, PREDICATE)                                      \
    ColumnPredicate* TabletReader::_new_##NAME##_pred(                                             \
            const TabletColumn& column, int index, const std::string& cond, bool opposite) const { \
        ColumnPredicate* predicate = nullptr;                                                      \
        switch (column.type()) {                                                                   \
        case OLAP_FIELD_TYPE_TINYINT: {                                                            \
            int8_t value = 0;                                                                      \
            std::from_chars(cond.data(), cond.data() + cond.size(), value);                        \
            predicate = new PREDICATE<int8_t>(index, value, opposite);                             \
            break;                                                                                 \
        }                                                                                          \
        case OLAP_FIELD_TYPE_SMALLINT: {                                                           \
            int16_t value = 0;                                                                     \
            std::from_chars(cond.data(), cond.data() + cond.size(), value);                        \
            predicate = new PREDICATE<int16_t>(index, value, opposite);                            \
            break;                                                                                 \
        }                                                                                          \
        case OLAP_FIELD_TYPE_DECIMAL32: {                                                          \
            int32_t value = 0;                                                                     \
            StringParser::ParseResult result = StringParser::ParseResult::PARSE_SUCCESS;           \
            value = (int32_t)StringParser::string_to_decimal<int128_t>(                            \
                    cond.data(), cond.size(), column.precision(), column.frac(), &result);         \
                                                                                                   \
            predicate = new PREDICATE<int32_t>(index, value, opposite);                            \
            break;                                                                                 \
        }                                                                                          \
        case OLAP_FIELD_TYPE_DECIMAL64: {                                                          \
            int64_t value = 0;                                                                     \
            StringParser::ParseResult result = StringParser::ParseResult::PARSE_SUCCESS;           \
            value = (int64_t)StringParser::string_to_decimal<int128_t>(                            \
                    cond.data(), cond.size(), column.precision(), column.frac(), &result);         \
            predicate = new PREDICATE<int64_t>(index, value, opposite);                            \
            break;                                                                                 \
        }                                                                                          \
        case OLAP_FIELD_TYPE_DECIMAL128: {                                                         \
            int128_t value = 0;                                                                    \
            StringParser::ParseResult result;                                                      \
            value = StringParser::string_to_decimal<int128_t>(                                     \
                    cond.data(), cond.size(), column.precision(), column.frac(), &result);         \
            predicate = new PREDICATE<int128_t>(index, value, opposite);                           \
            break;                                                                                 \
        }                                                                                          \
        case OLAP_FIELD_TYPE_INT: {                                                                \
            int32_t value = 0;                                                                     \
            std::from_chars(cond.data(), cond.data() + cond.size(), value);                        \
            predicate = new PREDICATE<int32_t>(index, value, opposite);                            \
            break;                                                                                 \
        }                                                                                          \
        case OLAP_FIELD_TYPE_BIGINT: {                                                             \
            int64_t value = 0;                                                                     \
            std::from_chars(cond.data(), cond.data() + cond.size(), value);                        \
            predicate = new PREDICATE<int64_t>(index, value, opposite);                            \
            break;                                                                                 \
        }                                                                                          \
        case OLAP_FIELD_TYPE_LARGEINT: {                                                           \
            int128_t value = 0;                                                                    \
            StringParser::ParseResult result;                                                      \
            value = StringParser::string_to_int<__int128>(cond.data(), cond.size(), &result);      \
            predicate = new PREDICATE<int128_t>(index, value, opposite);                           \
            break;                                                                                 \
        }                                                                                          \
        case OLAP_FIELD_TYPE_DECIMAL: {                                                            \
            decimal12_t value = {0, 0};                                                            \
            value.from_string(cond);                                                               \
            predicate = new PREDICATE<decimal12_t>(index, value, opposite);                        \
            break;                                                                                 \
        }                                                                                          \
        case OLAP_FIELD_TYPE_CHAR: {                                                               \
            StringValue value;                                                                     \
            size_t length = std::max(static_cast<size_t>(column.length()), cond.length());         \
            char* buffer = reinterpret_cast<char*>(_predicate_mem_pool->allocate(length));         \
            memset(buffer, 0, length);                                                             \
            memory_copy(buffer, cond.c_str(), cond.length());                                      \
            value.len = length;                                                                    \
            value.ptr = buffer;                                                                    \
            predicate = new PREDICATE<StringValue>(index, value, opposite);                        \
            break;                                                                                 \
        }                                                                                          \
        case OLAP_FIELD_TYPE_VARCHAR:                                                              \
        case OLAP_FIELD_TYPE_STRING: {                                                             \
            StringValue value;                                                                     \
            int32_t length = cond.length();                                                        \
            char* buffer = reinterpret_cast<char*>(_predicate_mem_pool->allocate(length));         \
            memory_copy(buffer, cond.c_str(), length);                                             \
            value.len = length;                                                                    \
            value.ptr = buffer;                                                                    \
            predicate = new PREDICATE<StringValue>(index, value, opposite);                        \
            break;                                                                                 \
        }                                                                                          \
        case OLAP_FIELD_TYPE_DATE: {                                                               \
            uint24_t value = timestamp_from_date(cond);                                            \
            predicate = new PREDICATE<uint24_t>(index, value, opposite);                           \
            break;                                                                                 \
        }                                                                                          \
        case OLAP_FIELD_TYPE_DATEV2: {                                                             \
            uint32_t value = timestamp_from_date_v2(cond);                                         \
            predicate = new PREDICATE<uint32_t>(index, value, opposite);                           \
            break;                                                                                 \
        }                                                                                          \
        case OLAP_FIELD_TYPE_DATETIMEV2: {                                                         \
            uint64_t value = timestamp_from_datetime_v2(cond);                                     \
            predicate = new PREDICATE<uint64_t>(index, value, opposite);                           \
            break;                                                                                 \
        }                                                                                          \
        case OLAP_FIELD_TYPE_DATETIME: {                                                           \
            uint64_t value = timestamp_from_datetime(cond);                                        \
            predicate = new PREDICATE<uint64_t>(index, value, opposite);                           \
            break;                                                                                 \
        }                                                                                          \
        case OLAP_FIELD_TYPE_BOOL: {                                                               \
            int32_t ivalue = 0;                                                                    \
            auto result = std::from_chars(cond.data(), cond.data() + cond.size(), ivalue);         \
            bool value = false;                                                                    \
            if (result.ec == std::errc()) {                                                        \
                if (ivalue == 0) {                                                                 \
                    value = false;                                                                 \
                } else {                                                                           \
                    value = true;                                                                  \
                }                                                                                  \
            } else {                                                                               \
                StringParser::ParseResult parse_result;                                            \
                value = StringParser::string_to_bool(cond.data(), cond.size(), &parse_result);     \
            }                                                                                      \
            predicate = new PREDICATE<bool>(index, value, opposite);                               \
            break;                                                                                 \
        }                                                                                          \
        default:                                                                                   \
            break;                                                                                 \
        }                                                                                          \
                                                                                                   \
        return predicate;                                                                          \
    }

COMPARISON_PREDICATE_CONDITION_VALUE(eq, EqualPredicate)
COMPARISON_PREDICATE_CONDITION_VALUE(ne, NotEqualPredicate)
COMPARISON_PREDICATE_CONDITION_VALUE(lt, LessPredicate)
COMPARISON_PREDICATE_CONDITION_VALUE(le, LessEqualPredicate)
COMPARISON_PREDICATE_CONDITION_VALUE(gt, GreaterPredicate)
COMPARISON_PREDICATE_CONDITION_VALUE(ge, GreaterEqualPredicate)

ColumnPredicate* TabletReader::_parse_to_predicate(
        const std::pair<std::string, std::shared_ptr<IBloomFilterFuncBase>>& bloom_filter) {
    int32_t index = _tablet_schema->field_index(bloom_filter.first);
    if (index < 0) {
        return nullptr;
    }
    const TabletColumn& column = _tablet_schema->column(index);
    return BloomFilterColumnPredicateFactory::create_column_predicate(index, bloom_filter.second,
                                                                      column.type());
}

ColumnPredicate* TabletReader::_parse_to_predicate(const FunctionFilter& function_filter) {
    int32_t index = _tablet->field_index(function_filter._col_name);
    if (index < 0) {
        return nullptr;
    }

    // currently only support like predicate
    return new LikeColumnPredicate(function_filter._opposite, index, function_filter._fn_ctx,
                                   function_filter._string_param);
}

ColumnPredicate* TabletReader::_parse_to_predicate(const TCondition& condition,
                                                   bool opposite) const {
    // TODO: not equal and not in predicate is not pushed down
    int32_t index = _tablet_schema->field_index(condition.column_name);
    if (index < 0) {
        return nullptr;
    }

    const TabletColumn& column = _tablet_schema->column(index);
    ColumnPredicate* predicate = nullptr;

    if ((condition.condition_op == "*=" || condition.condition_op == "!*=" ||
         condition.condition_op == "=" || condition.condition_op == "!=") &&
        condition.condition_values.size() == 1) {
        predicate = condition.condition_op == "*=" || condition.condition_op == "="
                            ? _new_eq_pred(column, index, condition.condition_values[0], opposite)
                            : _new_ne_pred(column, index, condition.condition_values[0], opposite);
    } else if (condition.condition_op == "<<") {
        predicate = _new_lt_pred(column, index, condition.condition_values[0], opposite);
    } else if (condition.condition_op == "<=") {
        predicate = _new_le_pred(column, index, condition.condition_values[0], opposite);
    } else if (condition.condition_op == ">>") {
        predicate = _new_gt_pred(column, index, condition.condition_values[0], opposite);
    } else if (condition.condition_op == ">=") {
        predicate = _new_ge_pred(column, index, condition.condition_values[0], opposite);
    } else if ((condition.condition_op == "*=" || condition.condition_op == "!*=") &&
               condition.condition_values.size() > 1) {
        switch (column.type()) {
        case OLAP_FIELD_TYPE_TINYINT: {
            phmap::flat_hash_set<int8_t> values;
            int8_t value = 0;
            for (auto& cond_val : condition.condition_values) {
                std::from_chars(cond_val.data(), cond_val.data() + cond_val.size(), value);
                values.insert(value);
            }
            if (condition.condition_op == "*=") {
                predicate = new InListPredicate<int8_t>(index, std::move(values), opposite);
            } else {
                predicate = new NotInListPredicate<int8_t>(index, std::move(values), opposite);
            }
            break;
        }
        case OLAP_FIELD_TYPE_SMALLINT: {
            phmap::flat_hash_set<int16_t> values;
            int16_t value = 0;
            for (auto& cond_val : condition.condition_values) {
                std::from_chars(cond_val.data(), cond_val.data() + cond_val.size(), value);
                values.insert(value);
            }
            if (condition.condition_op == "*=") {
                predicate = new InListPredicate<int16_t>(index, std::move(values), opposite);
            } else {
                predicate = new NotInListPredicate<int16_t>(index, std::move(values), opposite);
            }
            break;
        }
        case OLAP_FIELD_TYPE_DECIMAL32: {
            phmap::flat_hash_set<int32_t> values;
            for (auto& cond_val : condition.condition_values) {
                StringParser::ParseResult result = StringParser::ParseResult::PARSE_SUCCESS;
                int128_t val = StringParser::string_to_decimal<int128_t>(
                        cond_val.data(), cond_val.size(), column.precision(), column.frac(),
                        &result);
                if (result == StringParser::ParseResult::PARSE_SUCCESS) {
                    values.insert((int32_t)val);
                }
            }
            if (condition.condition_op == "*=") {
                predicate = new InListPredicate<int32_t>(index, std::move(values), opposite);
            } else {
                predicate = new NotInListPredicate<int32_t>(index, std::move(values), opposite);
            }
            break;
        }
        case OLAP_FIELD_TYPE_DECIMAL64: {
            phmap::flat_hash_set<int64_t> values;
            for (auto& cond_val : condition.condition_values) {
                StringParser::ParseResult result;
                int128_t val = StringParser::string_to_decimal<int128_t>(
                        cond_val.data(), cond_val.size(), column.precision(), column.frac(),
                        &result);
                if (result == StringParser::ParseResult::PARSE_SUCCESS) {
                    values.insert((int64_t)val);
                }
            }
            if (condition.condition_op == "*=") {
                predicate = new InListPredicate<int64_t>(index, std::move(values), opposite);
            } else {
                predicate = new NotInListPredicate<int64_t>(index, std::move(values), opposite);
            }
            break;
        }
        case OLAP_FIELD_TYPE_DECIMAL128: {
            phmap::flat_hash_set<int128_t> values;
            int128_t val;
            for (auto& cond_val : condition.condition_values) {
                StringParser::ParseResult result = StringParser::ParseResult::PARSE_SUCCESS;
                val = StringParser::string_to_decimal<int128_t>(cond_val.data(), cond_val.size(),
                                                                column.precision(), column.frac(),
                                                                &result);
                if (result == StringParser::ParseResult::PARSE_SUCCESS) {
                    values.insert(val);
                }
            }
            if (condition.condition_op == "*=") {
                predicate = new InListPredicate<int128_t>(index, std::move(values), opposite);
            } else {
                predicate = new NotInListPredicate<int128_t>(index, std::move(values), opposite);
            }
            break;
        }
        case OLAP_FIELD_TYPE_INT: {
            phmap::flat_hash_set<int32_t> values;
            int32_t value = 0;
            for (auto& cond_val : condition.condition_values) {
                std::from_chars(cond_val.data(), cond_val.data() + cond_val.size(), value);
                values.insert(value);
            }
            if (condition.condition_op == "*=") {
                predicate = new InListPredicate<int32_t>(index, std::move(values), opposite);
            } else {
                predicate = new NotInListPredicate<int32_t>(index, std::move(values), opposite);
            }
            break;
        }
        case OLAP_FIELD_TYPE_BIGINT: {
            phmap::flat_hash_set<int64_t> values;
            int64_t value = 0;
            for (auto& cond_val : condition.condition_values) {
                std::from_chars(cond_val.data(), cond_val.data() + cond_val.size(), value);
                values.insert(value);
            }
            if (condition.condition_op == "*=") {
                predicate = new InListPredicate<int64_t>(index, std::move(values), opposite);
            } else {
                predicate = new NotInListPredicate<int64_t>(index, std::move(values), opposite);
            }
            break;
        }
        case OLAP_FIELD_TYPE_LARGEINT: {
            phmap::flat_hash_set<int128_t> values;
            int128_t value = 0;
            StringParser::ParseResult result;
            for (auto& cond_val : condition.condition_values) {
                value = StringParser::string_to_int<__int128>(cond_val.c_str(), cond_val.size(),
                                                              &result);
                values.insert(value);
            }
            if (condition.condition_op == "*=") {
                predicate = new InListPredicate<int128_t>(index, std::move(values), opposite);
            } else {
                predicate = new NotInListPredicate<int128_t>(index, std::move(values), opposite);
            }
            break;
        }
        case OLAP_FIELD_TYPE_DECIMAL: {
            phmap::flat_hash_set<decimal12_t> values;
            for (auto& cond_val : condition.condition_values) {
                decimal12_t value = {0, 0};
                value.from_string(cond_val);
                values.insert(value);
            }
            if (condition.condition_op == "*=") {
                predicate = new InListPredicate<decimal12_t>(index, std::move(values), opposite);
            } else {
                predicate = new NotInListPredicate<decimal12_t>(index, std::move(values), opposite);
            }
            break;
        }
        case OLAP_FIELD_TYPE_CHAR: {
            phmap::flat_hash_set<StringValue> values;
            for (auto& cond_val : condition.condition_values) {
                StringValue value;
                size_t length = std::max(static_cast<size_t>(column.length()), cond_val.length());
                char* buffer = reinterpret_cast<char*>(_predicate_mem_pool->allocate(length));
                memset(buffer, 0, length);
                memory_copy(buffer, cond_val.c_str(), cond_val.length());
                value.len = length;
                value.ptr = buffer;
                values.insert(value);
            }
            if (condition.condition_op == "*=") {
                predicate = new InListPredicate<StringValue>(index, std::move(values), opposite);
            } else {
                predicate = new NotInListPredicate<StringValue>(index, std::move(values), opposite);
            }
            break;
        }
        case OLAP_FIELD_TYPE_VARCHAR:
        case OLAP_FIELD_TYPE_STRING: {
            phmap::flat_hash_set<StringValue> values;
            for (auto& cond_val : condition.condition_values) {
                StringValue value;
                int32_t length = cond_val.length();
                char* buffer = reinterpret_cast<char*>(_predicate_mem_pool->allocate(length));
                memory_copy(buffer, cond_val.c_str(), length);
                value.len = length;
                value.ptr = buffer;
                values.insert(value);
            }
            if (condition.condition_op == "*=") {
                predicate = new InListPredicate<StringValue>(index, std::move(values), opposite);
            } else {
                predicate = new NotInListPredicate<StringValue>(index, std::move(values), opposite);
            }
            break;
        }
        case OLAP_FIELD_TYPE_DATE: {
            phmap::flat_hash_set<uint24_t> values;
            for (auto& cond_val : condition.condition_values) {
                uint24_t value = timestamp_from_date(cond_val);
                values.insert(value);
            }
            if (condition.condition_op == "*=") {
                predicate = new InListPredicate<uint24_t>(index, std::move(values), opposite);
            } else {
                predicate = new NotInListPredicate<uint24_t>(index, std::move(values), opposite);
            }
            break;
        }
        case OLAP_FIELD_TYPE_DATEV2: {
            phmap::flat_hash_set<uint32_t> values;
            for (auto& cond_val : condition.condition_values) {
                uint32_t value = timestamp_from_date_v2(cond_val);
                values.insert(value);
            }
            if (condition.condition_op == "*=") {
                predicate = new InListPredicate<uint32_t>(index, std::move(values), opposite);
            } else {
                predicate = new NotInListPredicate<uint32_t>(index, std::move(values), opposite);
            }
            break;
        }
        case OLAP_FIELD_TYPE_DATETIMEV2: {
            phmap::flat_hash_set<uint64_t> values;
            for (auto& cond_val : condition.condition_values) {
                uint64_t value = timestamp_from_datetime_v2(cond_val);
                values.insert(value);
            }
            if (condition.condition_op == "*=") {
                predicate = new InListPredicate<uint64_t>(index, std::move(values), opposite);
            } else {
                predicate = new NotInListPredicate<uint64_t>(index, std::move(values), opposite);
            }
            break;
        }
        case OLAP_FIELD_TYPE_DATETIME: {
            phmap::flat_hash_set<uint64_t> values;
            for (auto& cond_val : condition.condition_values) {
                uint64_t value = timestamp_from_datetime(cond_val);
                values.insert(value);
            }
            if (condition.condition_op == "*=") {
                predicate = new InListPredicate<uint64_t>(index, std::move(values), opposite);
            } else {
                predicate = new NotInListPredicate<uint64_t>(index, std::move(values), opposite);
            }
            break;
        }
        // OLAP_FIELD_TYPE_BOOL is not valid in this case.
        default:
            break;
        }
    } else if (boost::to_lower_copy(condition.condition_op) == "is") {
        predicate = new NullPredicate(
                index, boost::to_lower_copy(condition.condition_values[0]) == "null", opposite);
    }
    return predicate;
}
void TabletReader::_init_load_bf_columns(const ReaderParams& read_params) {
    _init_load_bf_columns(read_params, &_conditions, &_load_bf_columns);
    _init_load_bf_columns(read_params, &_all_conditions, &_load_bf_all_columns);
}

void TabletReader::_init_load_bf_columns(const ReaderParams& read_params, Conditions* conditions,
                                         std::set<uint32_t>* load_bf_columns) {
    // add all columns with condition to load_bf_columns
    for (const auto& cond_column : conditions->columns()) {
        if (!_tablet_schema->column(cond_column.first).is_bf_column()) {
            continue;
        }
        for (const auto& cond : cond_column.second->conds()) {
            if (cond->op == OP_EQ ||
                (cond->op == OP_IN && cond->operand_set.size() < MAX_OP_IN_FIELD_NUM)) {
                load_bf_columns->insert(cond_column.first);
            }
        }
    }

    // remove columns which have same value between start_key and end_key
    int min_scan_key_len = _tablet_schema->num_columns();
    for (const auto& start_key : read_params.start_key) {
        min_scan_key_len = std::min(min_scan_key_len, static_cast<int>(start_key.size()));
    }
    for (const auto& end_key : read_params.end_key) {
        min_scan_key_len = std::min(min_scan_key_len, static_cast<int>(end_key.size()));
    }

    int max_equal_index = -1;
    for (int i = 0; i < read_params.start_key.size(); ++i) {
        int j = 0;
        for (; j < min_scan_key_len; ++j) {
            if (read_params.start_key[i].get_value(j) != read_params.end_key[i].get_value(j)) {
                break;
            }
        }

        if (max_equal_index < j - 1) {
            max_equal_index = j - 1;
        }
    }

    for (int i = 0; i < max_equal_index; ++i) {
        load_bf_columns->erase(i);
    }

    // remove the max_equal_index column when it's not varchar
    // or longer than number of short key fields
    if (max_equal_index == -1) {
        return;
    }
    FieldType type = _tablet_schema->column(max_equal_index).type();
    if ((type != OLAP_FIELD_TYPE_VARCHAR && type != OLAP_FIELD_TYPE_STRING) ||
        max_equal_index + 1 > _tablet->num_short_key_columns()) {
        load_bf_columns->erase(max_equal_index);
    }
}

Status TabletReader::_init_delete_condition(const ReaderParams& read_params) {
    if (read_params.reader_type == READER_CUMULATIVE_COMPACTION) {
        return Status::OK();
    }
    // Only BASE_COMPACTION need set filter_delete = true
    // other reader type:
    // QUERY will filter the row in query layer to keep right result use where clause.
    // CUMULATIVE_COMPACTION will lost the filter_delete info of base rowset
    if (read_params.reader_type == READER_BASE_COMPACTION) {
        _filter_delete = true;
    }

    auto delete_init = [&]() -> Status {
        return _delete_handler.init(*_tablet_schema, _tablet->delete_predicates(),
                                    read_params.version.second, this);
    };

    if (read_params.reader_type == READER_ALTER_TABLE) {
        return delete_init();
    }

    std::shared_lock rdlock(_tablet->get_header_lock());
    return delete_init();
}

} // namespace doris
