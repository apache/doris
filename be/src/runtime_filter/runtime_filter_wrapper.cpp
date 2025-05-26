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

#include "runtime_filter/runtime_filter_wrapper.h"

#include "exprs/create_predicate_function.h"
#include "runtime_filter/runtime_filter_definitions.h"

namespace doris {
#include "common/compile_check_begin.h"
RuntimeFilterWrapper::RuntimeFilterWrapper(const RuntimeFilterParams* params)
        : RuntimeFilterWrapper(params->column_return_type, params->filter_type, params->filter_id,
                               State::UNINITED, params->max_in_num) {
    switch (_filter_type) {
    case RuntimeFilterType::IN_FILTER: {
        _hybrid_set.reset(create_set(_column_return_type, params->null_aware));
        return;
    }
    // Only use in nested loop join not need set null aware
    case RuntimeFilterType::MIN_FILTER:
    case RuntimeFilterType::MAX_FILTER:
    case RuntimeFilterType::MINMAX_FILTER: {
        _minmax_func.reset(create_minmax_filter(_column_return_type, params->null_aware));
        return;
    }
    case RuntimeFilterType::BLOOM_FILTER: {
        _bloom_filter_func.reset(create_bloom_filter(_column_return_type, params->null_aware));
        _bloom_filter_func->init_params(params);
        return;
    }
    case RuntimeFilterType::IN_OR_BLOOM_FILTER: {
        _hybrid_set.reset(create_set(_column_return_type, params->null_aware));
        _bloom_filter_func.reset(create_bloom_filter(_column_return_type, params->null_aware));
        _bloom_filter_func->init_params(params);
        return;
    }
    case RuntimeFilterType::BITMAP_FILTER: {
        _bitmap_filter_func.reset(create_bitmap_filter(_column_return_type));
        _bitmap_filter_func->set_not_in(params->bitmap_filter_not_in);
        return;
    }
    default:
        break;
    }
}

Status RuntimeFilterWrapper::_change_to_bloom_filter() {
    if (_filter_type != RuntimeFilterType::IN_OR_BLOOM_FILTER) {
        return Status::InternalError("Can not change to bloom filter, {}", debug_string());
    }
    if (_bloom_filter_func->get_size() != 0) {
        _bloom_filter_func->insert_set(_hybrid_set);
    }

    // release in filter to change real type to bloom
    _hybrid_set.reset();
    return Status::OK();
}

Status RuntimeFilterWrapper::init(const size_t real_size) {
    if (_filter_type == RuntimeFilterType::IN_OR_BLOOM_FILTER && real_size > _max_in_num) {
        RETURN_IF_ERROR(_change_to_bloom_filter());
    }
    if (get_real_type() == RuntimeFilterType::IN_FILTER && real_size > _max_in_num) {
        set_state(RuntimeFilterWrapper::State::DISABLED, "reach max in num");
    }
    if (_bloom_filter_func) {
        RETURN_IF_ERROR(_bloom_filter_func->init_with_fixed_length(real_size));
    }
    return Status::OK();
}

Status RuntimeFilterWrapper::insert(const vectorized::ColumnPtr& column, size_t start) {
    switch (_filter_type) {
    case RuntimeFilterType::IN_FILTER: {
        _hybrid_set->insert_fixed_len(column, start);
        if (_hybrid_set->size() > _max_in_num) [[unlikely]] {
            _hybrid_set->clear();
            set_state(State::DISABLED, fmt::format("reach max in num: {}", _max_in_num));
            return Status::InternalError(
                    "Size of in set with actual size {} should be less than the limitation {} in "
                    "runtime filter {}.",
                    _hybrid_set->size(), _max_in_num, _filter_id);
        }
        break;
    }
    case RuntimeFilterType::MIN_FILTER:
    case RuntimeFilterType::MAX_FILTER:
    case RuntimeFilterType::MINMAX_FILTER: {
        _minmax_func->insert_fixed_len(column, start);
        break;
    }
    case RuntimeFilterType::BLOOM_FILTER: {
        _bloom_filter_func->insert_fixed_len(column, start);
        break;
    }
    case RuntimeFilterType::IN_OR_BLOOM_FILTER: {
        if (get_real_type() == RuntimeFilterType::BLOOM_FILTER) {
            _bloom_filter_func->insert_fixed_len(column, start);
        } else {
            _hybrid_set->insert_fixed_len(column, start);
        }
        break;
    }
    case RuntimeFilterType::BITMAP_FILTER: {
        std::vector<const BitmapValue*> bitmaps;
        if (column->is_nullable()) {
            const auto* nullable = assert_cast<const vectorized::ColumnNullable*>(column.get());
            const auto& col =
                    assert_cast<const vectorized::ColumnBitmap&>(nullable->get_nested_column());
            const auto& nullmap =
                    assert_cast<const vectorized::ColumnUInt8&>(nullable->get_null_map_column())
                            .get_data();
            for (size_t i = start; i < column->size(); i++) {
                if (!nullmap[i]) {
                    bitmaps.push_back(&(col.get_data()[i]));
                }
            }
        } else {
            const auto* col = assert_cast<const vectorized::ColumnBitmap*>(column.get());
            for (size_t i = start; i < column->size(); i++) {
                bitmaps.push_back(&(col->get_data()[i]));
            }
        }
        _bitmap_filter_func->insert_many(bitmaps);
        break;
    }
    default:
        return Status::InternalError("`RuntimeFilterWrapper::insert` is not supported for RF {}",
                                     int(_filter_type));
    }
    return Status::OK();
}

bool RuntimeFilterWrapper::build_bf_by_runtime_size() const {
    return _bloom_filter_func ? _bloom_filter_func->build_bf_by_runtime_size() : false;
}

Status RuntimeFilterWrapper::merge(const RuntimeFilterWrapper* other) {
    if (_state == State::DISABLED) {
        return Status::OK();
    }
    if (other->_state == State::DISABLED) {
        if (_hybrid_set) {
            _hybrid_set->clear();
        }
        set_state(State::DISABLED, std::string(other->_reason.status().msg()));
        return Status::OK();
    }

    DCHECK(other->_state == State::READY);
    DCHECK(_filter_type == other->_filter_type);

    switch (_filter_type) {
    case RuntimeFilterType::IN_FILTER: {
        _hybrid_set->insert(other->_hybrid_set.get());
        if (_max_in_num >= 0 && _hybrid_set->size() > _max_in_num) {
            _hybrid_set->clear();
            set_state(State::DISABLED, fmt::format("reach max in num: {}", _max_in_num));
        }
        break;
    }
    case RuntimeFilterType::MIN_FILTER:
    case RuntimeFilterType::MAX_FILTER:
    case RuntimeFilterType::MINMAX_FILTER: {
        RETURN_IF_ERROR(_minmax_func->merge(other->_minmax_func.get()));
        break;
    }
    case RuntimeFilterType::BLOOM_FILTER: {
        RETURN_IF_ERROR(_bloom_filter_func->merge(other->_bloom_filter_func.get()));
        break;
    }
    case RuntimeFilterType::IN_OR_BLOOM_FILTER: {
        auto real_filter_type = get_real_type();

        auto other_filter_type = other->_filter_type;
        if (other_filter_type == RuntimeFilterType::IN_OR_BLOOM_FILTER) {
            other_filter_type = other->get_real_type();
        }

        if (real_filter_type == RuntimeFilterType::IN_FILTER) {
            // when we meet base rf is in-filter, threre only have two case:
            // case1: all input-filter's build_bf_by_runtime_size is true, inited by synced global size
            // case2: all input-filter's build_bf_by_runtime_size is false, inited by default size
            if (other_filter_type == RuntimeFilterType::IN_FILTER) {
                _hybrid_set->insert(other->_hybrid_set.get());
                if (_max_in_num >= 0 && _hybrid_set->size() > _max_in_num) {
                    // case2: use default size to init bf
                    RETURN_IF_ERROR(_bloom_filter_func->init_with_fixed_length(0));
                    RETURN_IF_ERROR(_change_to_bloom_filter());
                }
            } else {
                // case1&case2: use input bf directly and insert hybrid set data into bf
                _bloom_filter_func = other->_bloom_filter_func;
                RETURN_IF_ERROR(_change_to_bloom_filter());
            }
        } else {
            if (other_filter_type == RuntimeFilterType::IN_FILTER) {
                // case2: insert data to global filter
                _bloom_filter_func->insert_set(other->_hybrid_set);
            } else {
                // case1&case2: all input bf must has same size
                RETURN_IF_ERROR(_bloom_filter_func->merge(other->_bloom_filter_func.get()));
            }
        }
        break;
    }
    case RuntimeFilterType::BITMAP_FILTER: {
        // use input bitmap directly because we assume bitmap filter join always have full data
        _bitmap_filter_func = other->_bitmap_filter_func;
        break;
    }
    default:
        return Status::InternalError("unknown runtime filter");
    }
    set_state(State::READY);
    return Status::OK();
}

Status RuntimeFilterWrapper::_assign(const PInFilter& in_filter, bool contain_null) {
    if (contain_null) {
        _hybrid_set->insert((const void*)nullptr);
    }

    auto batch_assign =
            [this](const PInFilter& filter,
                   void (*assign_func)(std::shared_ptr<HybridSetBase>&, PColumnValue&)) {
                for (int i = 0; i < filter.values_size(); ++i) {
                    PColumnValue column = filter.values(i);
                    assign_func(_hybrid_set, column);
                }
            };

    switch (_column_return_type) {
    case TYPE_BOOLEAN: {
        batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column) {
            bool bool_val = column.boolval();
            set->insert(&bool_val);
        });
        break;
    }
    case TYPE_TINYINT: {
        batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column) {
            auto int_val = static_cast<int8_t>(column.intval());
            set->insert(&int_val);
        });
        break;
    }
    case TYPE_SMALLINT: {
        batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column) {
            auto int_val = static_cast<int16_t>(column.intval());
            set->insert(&int_val);
        });
        break;
    }
    case TYPE_INT: {
        batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column) {
            int32_t int_val = column.intval();
            set->insert(&int_val);
        });
        break;
    }
    case TYPE_BIGINT: {
        batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column) {
            int64_t long_val = column.longval();
            set->insert(&long_val);
        });
        break;
    }
    case TYPE_LARGEINT: {
        batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column) {
            auto string_val = column.stringval();
            StringParser::ParseResult result;
            auto int128_val = StringParser::string_to_int<int128_t>(string_val.c_str(),
                                                                    string_val.length(), &result);
            DCHECK(result == StringParser::PARSE_SUCCESS);
            set->insert(&int128_val);
        });
        break;
    }
    case TYPE_FLOAT: {
        batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column) {
            auto float_val = static_cast<float>(column.doubleval());
            set->insert(&float_val);
        });
        break;
    }
    case TYPE_DOUBLE: {
        batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column) {
            double double_val = column.doubleval();
            set->insert(&double_val);
        });
        break;
    }
    case TYPE_DATEV2: {
        batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column) {
            auto date_v2_val = column.intval();
            set->insert(&date_v2_val);
        });
        break;
    }
    case TYPE_DATETIMEV2: {
        batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column) {
            auto date_v2_val = column.longval();
            set->insert(&date_v2_val);
        });
        break;
    }
    case TYPE_DATETIME:
    case TYPE_DATE: {
        batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column) {
            const auto& string_val_ref = column.stringval();
            VecDateTimeValue datetime_val;
            datetime_val.from_date_str(string_val_ref.c_str(), string_val_ref.length());
            set->insert(&datetime_val);
        });
        break;
    }
    case TYPE_DECIMALV2: {
        batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column) {
            const auto& string_val_ref = column.stringval();
            DecimalV2Value decimal_val(string_val_ref);
            set->insert(&decimal_val);
        });
        break;
    }
    case TYPE_DECIMAL32: {
        batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column) {
            int32_t decimal_32_val = column.intval();
            set->insert(&decimal_32_val);
        });
        break;
    }
    case TYPE_DECIMAL64: {
        batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column) {
            int64_t decimal_64_val = column.longval();
            set->insert(&decimal_64_val);
        });
        break;
    }
    case TYPE_DECIMAL128I: {
        batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column) {
            auto string_val = column.stringval();
            StringParser::ParseResult result;
            auto int128_val = StringParser::string_to_int<int128_t>(string_val.c_str(),
                                                                    string_val.length(), &result);
            DCHECK(result == StringParser::PARSE_SUCCESS);
            set->insert(&int128_val);
        });
        break;
    }
    case TYPE_DECIMAL256: {
        batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column) {
            auto string_val = column.stringval();
            StringParser::ParseResult result;
            auto int_val = StringParser::string_to_int<wide::Int256>(string_val.c_str(),
                                                                     string_val.length(), &result);
            DCHECK(result == StringParser::PARSE_SUCCESS);
            set->insert(&int_val);
        });
        break;
    }
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_STRING: {
        batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column) {
            const std::string& string_value = column.stringval();
            // string_value is std::string, call insert(data, size) function in StringSet will not cast as StringRef
            // so could avoid some cast error at different class object.
            set->insert((void*)string_value.data(), string_value.size());
        });
        break;
    }
    case TYPE_IPV4: {
        batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column) {
            int32_t tmp = column.intval();
            set->insert(&tmp);
        });
        break;
    }
    case TYPE_IPV6: {
        batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column) {
            auto string_val = column.stringval();
            StringParser::ParseResult result;
            auto int128_val = StringParser::string_to_int<uint128_t>(string_val.c_str(),
                                                                     string_val.length(), &result);
            DCHECK(result == StringParser::PARSE_SUCCESS);
            set->insert(&int128_val);
        });
        break;
    }
    default: {
        return Status::InternalError("not support assign to in filter, type: " +
                                     type_to_string(_column_return_type));
    }
    }
    return Status::OK();
}

Status RuntimeFilterWrapper::_assign(const PBloomFilter& bloom_filter,
                                     butil::IOBufAsZeroCopyInputStream* data, bool contain_null) {
    DCHECK(_bloom_filter_func);
    RETURN_IF_ERROR(_bloom_filter_func->assign(data, bloom_filter.filter_length(), contain_null));
    return Status::OK();
}

Status RuntimeFilterWrapper::_assign(const PMinMaxFilter& minmax_filter, bool contain_null) {
    _minmax_func->set_contain_null(contain_null);

    switch (_column_return_type) {
    case TYPE_BOOLEAN: {
        bool min_val = minmax_filter.min_val().boolval();
        bool max_val = minmax_filter.max_val().boolval();
        return _minmax_func->assign(&min_val, &max_val);
    }
    case TYPE_TINYINT: {
        auto min_val = static_cast<int8_t>(minmax_filter.min_val().intval());
        auto max_val = static_cast<int8_t>(minmax_filter.max_val().intval());
        return _minmax_func->assign(&min_val, &max_val);
    }
    case TYPE_SMALLINT: {
        auto min_val = static_cast<int16_t>(minmax_filter.min_val().intval());
        auto max_val = static_cast<int16_t>(minmax_filter.max_val().intval());
        return _minmax_func->assign(&min_val, &max_val);
    }
    case TYPE_INT: {
        int32_t min_val = minmax_filter.min_val().intval();
        int32_t max_val = minmax_filter.max_val().intval();
        return _minmax_func->assign(&min_val, &max_val);
    }
    case TYPE_BIGINT: {
        int64_t min_val = minmax_filter.min_val().longval();
        int64_t max_val = minmax_filter.max_val().longval();
        return _minmax_func->assign(&min_val, &max_val);
    }
    case TYPE_LARGEINT: {
        auto min_string_val = minmax_filter.min_val().stringval();
        auto max_string_val = minmax_filter.max_val().stringval();
        StringParser::ParseResult result;
        auto min_val = StringParser::string_to_int<int128_t>(min_string_val.c_str(),
                                                             min_string_val.length(), &result);
        DCHECK(result == StringParser::PARSE_SUCCESS);
        auto max_val = StringParser::string_to_int<int128_t>(max_string_val.c_str(),
                                                             max_string_val.length(), &result);
        DCHECK(result == StringParser::PARSE_SUCCESS);
        return _minmax_func->assign(&min_val, &max_val);
    }
    case TYPE_FLOAT: {
        auto min_val = static_cast<float>(minmax_filter.min_val().doubleval());
        auto max_val = static_cast<float>(minmax_filter.max_val().doubleval());
        return _minmax_func->assign(&min_val, &max_val);
    }
    case TYPE_DOUBLE: {
        auto min_val = minmax_filter.min_val().doubleval();
        auto max_val = minmax_filter.max_val().doubleval();
        return _minmax_func->assign(&min_val, &max_val);
    }
    case TYPE_DATEV2: {
        int32_t min_val = minmax_filter.min_val().intval();
        int32_t max_val = minmax_filter.max_val().intval();
        return _minmax_func->assign(&min_val, &max_val);
    }
    case TYPE_DATETIMEV2: {
        int64_t min_val = minmax_filter.min_val().longval();
        int64_t max_val = minmax_filter.max_val().longval();
        return _minmax_func->assign(&min_val, &max_val);
    }
    case TYPE_DATETIME:
    case TYPE_DATE: {
        const auto& min_val_ref = minmax_filter.min_val().stringval();
        const auto& max_val_ref = minmax_filter.max_val().stringval();
        VecDateTimeValue min_val;
        VecDateTimeValue max_val;
        min_val.from_date_str(min_val_ref.c_str(), min_val_ref.length());
        max_val.from_date_str(max_val_ref.c_str(), max_val_ref.length());
        return _minmax_func->assign(&min_val, &max_val);
    }
    case TYPE_DECIMALV2: {
        const auto& min_val_ref = minmax_filter.min_val().stringval();
        const auto& max_val_ref = minmax_filter.max_val().stringval();
        DecimalV2Value min_val(min_val_ref);
        DecimalV2Value max_val(max_val_ref);
        return _minmax_func->assign(&min_val, &max_val);
    }
    case TYPE_DECIMAL32: {
        int32_t min_val = minmax_filter.min_val().intval();
        int32_t max_val = minmax_filter.max_val().intval();
        return _minmax_func->assign(&min_val, &max_val);
    }
    case TYPE_DECIMAL64: {
        int64_t min_val = minmax_filter.min_val().longval();
        int64_t max_val = minmax_filter.max_val().longval();
        return _minmax_func->assign(&min_val, &max_val);
    }
    case TYPE_DECIMAL128I: {
        auto min_string_val = minmax_filter.min_val().stringval();
        auto max_string_val = minmax_filter.max_val().stringval();
        StringParser::ParseResult result;
        auto min_val = StringParser::string_to_int<int128_t>(min_string_val.c_str(),
                                                             min_string_val.length(), &result);
        DCHECK(result == StringParser::PARSE_SUCCESS);
        auto max_val = StringParser::string_to_int<int128_t>(max_string_val.c_str(),
                                                             max_string_val.length(), &result);
        DCHECK(result == StringParser::PARSE_SUCCESS);
        return _minmax_func->assign(&min_val, &max_val);
    }
    case TYPE_DECIMAL256: {
        auto min_string_val = minmax_filter.min_val().stringval();
        auto max_string_val = minmax_filter.max_val().stringval();
        StringParser::ParseResult result;
        auto min_val = StringParser::string_to_int<wide::Int256>(min_string_val.c_str(),
                                                                 min_string_val.length(), &result);
        DCHECK(result == StringParser::PARSE_SUCCESS);
        auto max_val = StringParser::string_to_int<wide::Int256>(max_string_val.c_str(),
                                                                 max_string_val.length(), &result);
        DCHECK(result == StringParser::PARSE_SUCCESS);
        return _minmax_func->assign(&min_val, &max_val);
    }
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_STRING: {
        auto min_val_ref = minmax_filter.min_val().stringval();
        auto max_val_ref = minmax_filter.max_val().stringval();
        return _minmax_func->assign(&min_val_ref, &max_val_ref);
    }
    case TYPE_IPV4: {
        int tmp_min = minmax_filter.min_val().intval();
        int tmp_max = minmax_filter.max_val().intval();
        return _minmax_func->assign(&tmp_min, &tmp_max);
    }
    case TYPE_IPV6: {
        auto min_string_val = minmax_filter.min_val().stringval();
        auto max_string_val = minmax_filter.max_val().stringval();
        StringParser::ParseResult result;
        auto min_val = StringParser::string_to_int<uint128_t>(min_string_val.c_str(),
                                                              min_string_val.length(), &result);
        DCHECK(result == StringParser::PARSE_SUCCESS);
        auto max_val = StringParser::string_to_int<uint128_t>(max_string_val.c_str(),
                                                              max_string_val.length(), &result);
        DCHECK(result == StringParser::PARSE_SUCCESS);
        return _minmax_func->assign(&min_val, &max_val);
    }
    default:
        break;
    }
    return Status::InternalError("not support!");
}

bool RuntimeFilterWrapper::contain_null() const {
    if (get_real_type() == RuntimeFilterType::BLOOM_FILTER) {
        return _bloom_filter_func->contain_null();
    }
    if (_hybrid_set) {
        return _hybrid_set->contain_null();
    }
    if (_minmax_func) {
        return _minmax_func->contain_null();
    }
    return false;
}

std::string RuntimeFilterWrapper::debug_string() const {
    auto type_string = _filter_type == RuntimeFilterType::IN_OR_BLOOM_FILTER
                               ? fmt::format("{}({})", filter_type_to_string(_filter_type),
                                             filter_type_to_string(get_real_type()))
                               : filter_type_to_string(_filter_type);
    auto result = fmt::format("[id: {}, state: {}, type: {}, column_type: {}", _filter_id,
                              states_to_string<RuntimeFilterWrapper>({_state}), type_string,
                              type_to_string(_column_return_type));

    if (_state == State::READY) {
        if (get_real_type() == RuntimeFilterType::BLOOM_FILTER) {
            result += fmt::format(
                    ", bf_size: {}, build_bf_by_runtime_size: {}", _bloom_filter_func->get_size(),
                    _bloom_filter_func->build_bf_by_runtime_size() ? "true" : "false");
        }
        if (get_real_type() == RuntimeFilterType::IN_FILTER) {
            result += fmt::format(", size: {}, max_in_num: {}", _hybrid_set->size(), _max_in_num);
        }
        if (get_real_type() == RuntimeFilterType::BITMAP_FILTER) {
            result += fmt::format(", size: {}, not_in: {}", _bitmap_filter_func->size(),
                                  _bitmap_filter_func->is_not_in() ? "true" : "false");
        }
    }

    if (!_reason.ok()) {
        result += fmt::format(", reason: {}", _reason.status().msg());
    }
    return result + "]";
}

Status RuntimeFilterWrapper::to_protobuf(PInFilter* filter) {
    if (get_real_type() != RuntimeFilterType::IN_FILTER) {
        return Status::InternalError("Runtime filter {} cannot serialize to PInFilter",
                                     int(get_real_type()));
    }
    filter->set_column_type(
            PColumnType::
                    COLUMN_TYPE_BOOL); // set deprecated field coz it is required and we can't delete it
    _hybrid_set->to_pb(filter);
    return Status::OK();
}

Status RuntimeFilterWrapper::to_protobuf(PMinMaxFilter* filter) {
    if (get_real_type() != RuntimeFilterType::MINMAX_FILTER &&
        get_real_type() != RuntimeFilterType::MIN_FILTER &&
        get_real_type() != RuntimeFilterType::MAX_FILTER) {
        return Status::InternalError("Runtime filter {} cannot serialize to PMinMaxFilter",
                                     int(get_real_type()));
    }
    filter->set_column_type(
            PColumnType::
                    COLUMN_TYPE_BOOL); // set deprecated field coz it is required and we can't delete it
    _minmax_func->to_pb(filter);
    return Status::OK();
}

Status RuntimeFilterWrapper::to_protobuf(PBloomFilter* filter, char** data, int* filter_length) {
    if (get_real_type() != RuntimeFilterType::BLOOM_FILTER) {
        return Status::InternalError("Runtime filter {} cannot serialize to PBloomFilter",
                                     int(get_real_type()));
    }
    _bloom_filter_func->get_data(data, filter_length);
    filter->set_filter_length(*filter_length);
    filter->set_always_true(false);
    return Status::OK();
}

template <class T>
Status RuntimeFilterWrapper::assign(const T& request, butil::IOBufAsZeroCopyInputStream* data) {
    PFilterType filter_type = request.filter_type();

    if ((request.has_disabled() && request.disabled()) ||
        (request.has_ignored() && request.ignored())) {
        set_state(State::DISABLED, "get disabled from remote");
        return Status::OK();
    }

    set_state(State::READY);

    switch (filter_type) {
    case PFilterType::IN_FILTER: {
        DCHECK(request.has_in_filter());
        return _assign(request.in_filter(), request.contain_null());
    }
    case PFilterType::BLOOM_FILTER: {
        DCHECK(request.has_bloom_filter());
        _hybrid_set.reset(); // change in_or_bloom filter to bloom filter
        return _assign(request.bloom_filter(), data, request.contain_null());
    }
    case PFilterType::MIN_FILTER:
    case PFilterType::MAX_FILTER:
    case PFilterType::MINMAX_FILTER: {
        DCHECK(request.has_minmax_filter());
        return _assign(request.minmax_filter(), request.contain_null());
    }
    default:
        return Status::InternalError(
                "`RuntimeFilterWrapper::assign` is not supported by Filter type {}",
                int(filter_type));
    }
}

template Status RuntimeFilterWrapper::assign<doris::PMergeFilterRequest>(
        doris::PMergeFilterRequest const&, butil::IOBufAsZeroCopyInputStream*);
template Status RuntimeFilterWrapper::assign<doris::PPublishFilterRequestV2>(
        doris::PPublishFilterRequestV2 const&, butil::IOBufAsZeroCopyInputStream*);

} // namespace doris
