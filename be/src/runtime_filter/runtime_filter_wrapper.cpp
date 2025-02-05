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
#include "vec/exprs/vbitmap_predicate.h"
#include "vec/exprs/vbloom_predicate.h"
#include "vec/exprs/vdirect_in_predicate.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {

RuntimeFilterWrapper::RuntimeFilterWrapper(const RuntimeFilterParams* params)
        : RuntimeFilterWrapper(params->column_return_type, params->filter_type, params->filter_id,
                               State::UNINITED) {
    _max_in_num = params->max_in_num;
    switch (_filter_type) {
    case RuntimeFilterType::IN_FILTER: {
        _hybrid_set.reset(create_set(_column_return_type));
        _hybrid_set->set_null_aware(params->null_aware);
        _hybrid_set->set_filter_id(_filter_id);
        return;
    }
    // Only use in nested loop join not need set null aware
    case RuntimeFilterType::MIN_FILTER:
    case RuntimeFilterType::MAX_FILTER: {
        _minmax_func.reset(create_minmax_filter(_column_return_type));
        _minmax_func->set_filter_id(_filter_id);
        return;
    }
    case RuntimeFilterType::MINMAX_FILTER: {
        _minmax_func.reset(create_minmax_filter(_column_return_type));
        _minmax_func->set_null_aware(params->null_aware);
        _minmax_func->set_filter_id(_filter_id);
        return;
    }
    case RuntimeFilterType::BLOOM_FILTER: {
        _bloom_filter_func.reset(create_bloom_filter(_column_return_type));
        _bloom_filter_func->init_params(params);
        _bloom_filter_func->set_filter_id(_filter_id);
        return;
    }
    case RuntimeFilterType::IN_OR_BLOOM_FILTER: {
        _hybrid_set.reset(create_set(_column_return_type));
        _hybrid_set->set_null_aware(params->null_aware);
        _hybrid_set->set_filter_id(_filter_id);
        _bloom_filter_func.reset(create_bloom_filter(_column_return_type));
        _bloom_filter_func->init_params(params);
        _bloom_filter_func->set_filter_id(_filter_id);
        return;
    }
    case RuntimeFilterType::BITMAP_FILTER: {
        _bitmap_filter_func.reset(create_bitmap_filter(_column_return_type));
        _bitmap_filter_func->set_not_in(params->bitmap_filter_not_in);
        _bitmap_filter_func->set_filter_id(_filter_id);
        return;
    }
    default:
        break;
    }
}

Status RuntimeFilterWrapper::get_push_exprs(std::list<vectorized::VExprContextSPtr>& probe_ctxs,
                                            std::vector<vectorized::VRuntimeFilterPtr>& container,
                                            const TExpr& probe_expr) {
    vectorized::VExprContextSPtr probe_ctx;
    RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(probe_expr, probe_ctx));
    probe_ctxs.push_back(probe_ctx);
    DCHECK(probe_ctx->root()->type().type == _column_return_type ||
           (is_string_type(probe_ctx->root()->type().type) &&
            is_string_type(_column_return_type)) ||
           _filter_type == RuntimeFilterType::BITMAP_FILTER)
            << " prob_expr->root()->type().type: " << int(probe_ctx->root()->type().type)
            << " _column_return_type: " << int(_column_return_type)
            << " _filter_type: " << filter_type_to_string(_filter_type);

    auto real_filter_type = get_real_type();
    bool null_aware = contain_null();
    switch (real_filter_type) {
    case RuntimeFilterType::IN_FILTER: {
        TTypeDesc type_desc = create_type_desc(PrimitiveType::TYPE_BOOLEAN);
        type_desc.__set_is_nullable(false);
        TExprNode node;
        node.__set_type(type_desc);
        node.__set_node_type(null_aware ? TExprNodeType::NULL_AWARE_IN_PRED
                                        : TExprNodeType::IN_PRED);
        node.in_predicate.__set_is_not_in(false);
        node.__set_opcode(TExprOpcode::FILTER_IN);
        node.__set_is_nullable(false);
        auto in_pred = vectorized::VDirectInPredicate::create_shared(node, _hybrid_set);
        in_pred->add_child(probe_ctx->root());
        auto wrapper = vectorized::VRuntimeFilterWrapper::create_shared(
                node, in_pred, get_in_list_ignore_thredhold(_hybrid_set->size()), null_aware);
        container.push_back(wrapper);
        break;
    }
    case RuntimeFilterType::MIN_FILTER: {
        // create min filter
        vectorized::VExprSPtr min_pred;
        TExprNode min_pred_node;
        RETURN_IF_ERROR(create_vbin_predicate(probe_ctx->root()->type(), TExprOpcode::GE, min_pred,
                                              &min_pred_node, null_aware));
        vectorized::VExprSPtr min_literal;
        RETURN_IF_ERROR(
                create_literal(probe_ctx->root()->type(), _minmax_func->get_min(), min_literal));
        min_pred->add_child(probe_ctx->root());
        min_pred->add_child(min_literal);
        container.push_back(vectorized::VRuntimeFilterWrapper::create_shared(
                min_pred_node, min_pred, get_comparison_ignore_thredhold()));
        break;
    }
    case RuntimeFilterType::MAX_FILTER: {
        vectorized::VExprSPtr max_pred;
        // create max filter
        TExprNode max_pred_node;
        RETURN_IF_ERROR(create_vbin_predicate(probe_ctx->root()->type(), TExprOpcode::LE, max_pred,
                                              &max_pred_node, null_aware));
        vectorized::VExprSPtr max_literal;
        RETURN_IF_ERROR(
                create_literal(probe_ctx->root()->type(), _minmax_func->get_max(), max_literal));
        max_pred->add_child(probe_ctx->root());
        max_pred->add_child(max_literal);
        container.push_back(vectorized::VRuntimeFilterWrapper::create_shared(
                max_pred_node, max_pred, get_comparison_ignore_thredhold()));
        break;
    }
    case RuntimeFilterType::MINMAX_FILTER: {
        vectorized::VExprSPtr max_pred;
        // create max filter
        TExprNode max_pred_node;
        RETURN_IF_ERROR(create_vbin_predicate(probe_ctx->root()->type(), TExprOpcode::LE, max_pred,
                                              &max_pred_node, null_aware));
        vectorized::VExprSPtr max_literal;
        RETURN_IF_ERROR(
                create_literal(probe_ctx->root()->type(), _minmax_func->get_max(), max_literal));
        max_pred->add_child(probe_ctx->root());
        max_pred->add_child(max_literal);
        container.push_back(vectorized::VRuntimeFilterWrapper::create_shared(
                max_pred_node, max_pred, get_comparison_ignore_thredhold(), null_aware));

        vectorized::VExprContextSPtr new_probe_ctx;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(probe_expr, new_probe_ctx));
        probe_ctxs.push_back(new_probe_ctx);

        // create min filter
        vectorized::VExprSPtr min_pred;
        TExprNode min_pred_node;
        RETURN_IF_ERROR(create_vbin_predicate(new_probe_ctx->root()->type(), TExprOpcode::GE,
                                              min_pred, &min_pred_node, null_aware));
        vectorized::VExprSPtr min_literal;
        RETURN_IF_ERROR(create_literal(new_probe_ctx->root()->type(), _minmax_func->get_min(),
                                       min_literal));
        min_pred->add_child(new_probe_ctx->root());
        min_pred->add_child(min_literal);
        container.push_back(vectorized::VRuntimeFilterWrapper::create_shared(
                min_pred_node, min_pred, get_comparison_ignore_thredhold(), null_aware));
        break;
    }
    case RuntimeFilterType::BLOOM_FILTER: {
        // create a bloom filter
        TTypeDesc type_desc = create_type_desc(PrimitiveType::TYPE_BOOLEAN);
        type_desc.__set_is_nullable(false);
        TExprNode node;
        node.__set_type(type_desc);
        node.__set_node_type(TExprNodeType::BLOOM_PRED);
        node.__set_opcode(TExprOpcode::RT_FILTER);
        node.__set_is_nullable(false);
        auto bloom_pred = vectorized::VBloomPredicate::create_shared(node);
        bloom_pred->set_filter(_bloom_filter_func);
        bloom_pred->add_child(probe_ctx->root());
        auto wrapper = vectorized::VRuntimeFilterWrapper::create_shared(
                node, bloom_pred, get_bloom_filter_ignore_thredhold());
        container.push_back(wrapper);
        break;
    }
    case RuntimeFilterType::BITMAP_FILTER: {
        // create a bitmap filter
        TTypeDesc type_desc = create_type_desc(PrimitiveType::TYPE_BOOLEAN);
        type_desc.__set_is_nullable(false);
        TExprNode node;
        node.__set_type(type_desc);
        node.__set_node_type(TExprNodeType::BITMAP_PRED);
        node.__set_opcode(TExprOpcode::RT_FILTER);
        node.__set_is_nullable(false);
        auto bitmap_pred = vectorized::VBitmapPredicate::create_shared(node);
        bitmap_pred->set_filter(_bitmap_filter_func);
        bitmap_pred->add_child(probe_ctx->root());
        auto wrapper = vectorized::VRuntimeFilterWrapper::create_shared(node, bitmap_pred, 0);
        container.push_back(wrapper);
        break;
    }
    default:
        DCHECK(false);
        break;
    }
    return Status::OK();
}

Status RuntimeFilterWrapper::change_to_bloom_filter() {
    if (_filter_type != RuntimeFilterType::IN_OR_BLOOM_FILTER) {
        return Status::InternalError("Can not change to bloom filter, {}", debug_string());
    }
    BloomFilterFuncBase* bf = _bloom_filter_func.get();

    if (bf != nullptr) {
        insert_to_bloom_filter(bf);
    } else if (_hybrid_set != nullptr && _hybrid_set->size() != 0) {
        return Status::InternalError("change to bloom filter need empty set, {}", debug_string());
    }

    // release in filter
    _hybrid_set.reset();
    return Status::OK();
}

void RuntimeFilterWrapper::batch_assign(
        const PInFilter& filter,
        void (*assign_func)(std::shared_ptr<HybridSetBase>& _hybrid_set, PColumnValue&)) {
    for (int i = 0; i < filter.values_size(); ++i) {
        PColumnValue column = filter.values(i);
        assign_func(_hybrid_set, column);
    }
}

Status RuntimeFilterWrapper::init_bloom_filter(const size_t runtime_size) {
    if (_filter_type != RuntimeFilterType::BLOOM_FILTER &&
        _filter_type != RuntimeFilterType::IN_OR_BLOOM_FILTER) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "init_bloom_filter meet invalid input type {}",
                        int(_filter_type));
    }
    return _bloom_filter_func->init_with_cardinality(runtime_size);
}

void RuntimeFilterWrapper::insert_to_bloom_filter(BloomFilterFuncBase* bloom_filter) const {
    if (_hybrid_set->size() > 0) {
        auto* it = _hybrid_set->begin();
        while (it->has_next()) {
            bloom_filter->insert(it->get_value());
            it->next();
        }
    }
    if (_hybrid_set->contain_null()) {
        bloom_filter->set_contain_null_and_null_aware();
    }
}

void RuntimeFilterWrapper::insert_fixed_len(const vectorized::ColumnPtr& column, size_t start) {
    switch (_filter_type) {
    case RuntimeFilterType::IN_FILTER: {
        _hybrid_set->insert_fixed_len(column, start);
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
    default:
        DCHECK(false);
        break;
    }
}

void RuntimeFilterWrapper::bitmap_filter_insert_batch(const vectorized::ColumnPtr column,
                                                      size_t start) {
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
}

bool RuntimeFilterWrapper::build_bf_by_runtime_size() const {
    return _bloom_filter_func ? _bloom_filter_func->build_bf_by_runtime_size() : false;
}

Status RuntimeFilterWrapper::merge(const RuntimeFilterWrapper* other) {
    if (other->_state == State::DISABLED) {
        disable(other->_disabled_reason);
    }

    if (other->_state == State::IGNORED || _state == State::DISABLED) {
        return Status::OK();
    }

    DCHECK(_state != State::IGNORED);

    set_state(State::READY);

    DCHECK(_filter_type == other->_filter_type) << debug_string();

    switch (_filter_type) {
    case RuntimeFilterType::IN_FILTER: {
        _hybrid_set->insert(other->_hybrid_set.get());
        if (_max_in_num >= 0 && _hybrid_set->size() >= _max_in_num) {
            disable(fmt::format("reach max in num: {}", _max_in_num));
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
            // case1: all input-filter's build_bf_exactly is true, inited by synced global size
            // case2: all input-filter's build_bf_exactly is false, inited by default size
            if (other_filter_type == RuntimeFilterType::IN_FILTER) {
                _hybrid_set->insert(other->_hybrid_set.get());
                if (_max_in_num >= 0 && _hybrid_set->size() >= _max_in_num) {
                    // case2: use default size to init bf
                    RETURN_IF_ERROR(_bloom_filter_func->init_with_fixed_length());
                    RETURN_IF_ERROR(change_to_bloom_filter());
                }
            } else {
                // case1&case2: use input bf directly and insert hybrid set data into bf
                _bloom_filter_func = other->_bloom_filter_func;
                RETURN_IF_ERROR(change_to_bloom_filter());
            }
        } else {
            if (other_filter_type == RuntimeFilterType::IN_FILTER) {
                // case2: insert data to global filter
                other->insert_to_bloom_filter(_bloom_filter_func.get());
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
    return Status::OK();
}

Status RuntimeFilterWrapper::assign(const PInFilter& in_filter, bool contain_null) {
    if (contain_null) {
        _hybrid_set->set_null_aware(true);
        _hybrid_set->insert((const void*)nullptr);
    }

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

Status RuntimeFilterWrapper::assign(const PBloomFilter& bloom_filter,
                                    butil::IOBufAsZeroCopyInputStream* data, bool contain_null) {
    RETURN_IF_ERROR(_bloom_filter_func->assign(data, bloom_filter.filter_length(), contain_null));
    return Status::OK();
}

// used by shuffle runtime filter
// assign this filter by protobuf
Status RuntimeFilterWrapper::assign(const PMinMaxFilter& minmax_filter, bool contain_null) {
    if (contain_null) {
        _minmax_func->set_null_aware(true);
        _minmax_func->set_contain_null();
    }

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
        auto min_val = static_cast<double>(minmax_filter.min_val().doubleval());
        auto max_val = static_cast<double>(minmax_filter.max_val().doubleval());
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

void RuntimeFilterWrapper::get_bloom_filter_desc(char** data, int* filter_length) {
    _bloom_filter_func->get_data(data, filter_length);
}

bool RuntimeFilterWrapper::contain_null() const {
    if (get_real_type() == RuntimeFilterType::BLOOM_FILTER) {
        return _bloom_filter_func->contain_null();
    }
    if (_hybrid_set) {
        if (get_real_type() != RuntimeFilterType::IN_FILTER) {
            throw Exception(ErrorCode::INTERNAL_ERROR, "rf has hybrid_set but real type is {}",
                            int(get_real_type()));
        }
        return _hybrid_set->contain_null();
    }
    if (_minmax_func) {
        return _minmax_func->contain_null();
    }
    return false;
}

std::string RuntimeFilterWrapper::debug_string() const {
    auto result = fmt::format("[id: {}, state: {}, type: {}({}), column_type: {}", _filter_id,
                              to_string(_state), filter_type_to_string(_filter_type),
                              filter_type_to_string(get_real_type()),
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

    if (!_disabled_reason.empty()) {
        result += fmt::format(", disabled_reason: {}", _disabled_reason);
    }
    return result + "]";
}

void RuntimeFilterWrapper::_to_protobuf(PInFilter* filter) {
    filter->set_column_type(to_proto(column_type()));
    _hybrid_set->to_pb(filter);
}

void RuntimeFilterWrapper::_to_protobuf(PMinMaxFilter* filter) {
    filter->set_column_type(to_proto(column_type()));
    _minmax_func->to_pb(filter);
}

} // namespace doris
