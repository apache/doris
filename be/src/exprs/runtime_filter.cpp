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

#include "runtime_filter.h"

#include <memory>

#include "common/object_pool.h"
#include "common/status.h"
#include "exec/hash_join_node.h"
#include "exprs/binary_predicate.h"
#include "exprs/bloomfilter_predicate.h"
#include "exprs/create_predicate_function.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/hybrid_set.h"
#include "exprs/in_predicate.h"
#include "exprs/literal.h"
#include "exprs/minmax_predicate.h"
#include "exprs/predicate.h"
#include "gen_cpp/internal_service.pb.h"
#include "gen_cpp/types.pb.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/runtime_state.h"
#include "runtime/type_limit.h"
#include "util/defer_op.h"
#include "util/runtime_profile.h"
#include "util/string_parser.hpp"

namespace doris {
// PrimitiveType->TExprNodeType
// TODO: use constexpr if we use c++14
TExprNodeType::type get_expr_node_type(PrimitiveType type) {
    switch (type) {
    case TYPE_BOOLEAN:
        return TExprNodeType::BOOL_LITERAL;

    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
        return TExprNodeType::INT_LITERAL;

    case TYPE_LARGEINT:
        return TExprNodeType::LARGE_INT_LITERAL;
        break;

    case TYPE_NULL:
        return TExprNodeType::NULL_LITERAL;

    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_TIME:
        return TExprNodeType::FLOAT_LITERAL;
        break;

    case TYPE_DECIMALV2:
        return TExprNodeType::DECIMAL_LITERAL;

    case TYPE_DATETIME:
        return TExprNodeType::DATE_LITERAL;

    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_OBJECT:
    case TYPE_STRING:
        return TExprNodeType::STRING_LITERAL;

    default:
        DCHECK(false) << "Invalid type.";
        return TExprNodeType::NULL_LITERAL;
    }
}

// PrimitiveType-> PColumnType
// TODO: use constexpr if we use c++14
PColumnType to_proto(PrimitiveType type) {
    switch (type) {
    case TYPE_BOOLEAN:
        return PColumnType::COLUMN_TYPE_BOOL;
    case TYPE_TINYINT:
        return PColumnType::COLUMN_TYPE_TINY_INT;
    case TYPE_SMALLINT:
        return PColumnType::COLUMN_TYPE_SMALL_INT;
    case TYPE_INT:
        return PColumnType::COLUMN_TYPE_INT;
    case TYPE_BIGINT:
        return PColumnType::COLUMN_TYPE_BIGINT;
    case TYPE_LARGEINT:
        return PColumnType::COLUMN_TYPE_LARGEINT;
    case TYPE_FLOAT:
        return PColumnType::COLUMN_TYPE_FLOAT;
    case TYPE_DOUBLE:
        return PColumnType::COLUMN_TYPE_DOUBLE;
    case TYPE_DATE:
        return PColumnType::COLUMN_TYPE_DATE;
    case TYPE_DATETIME:
        return PColumnType::COLUMN_TYPE_DATETIME;
    case TYPE_DECIMALV2:
        return PColumnType::COLUMN_TYPE_DECIMALV2;
    case TYPE_CHAR:
        return PColumnType::COLUMN_TYPE_CHAR;
    case TYPE_VARCHAR:
        return PColumnType::COLUMN_TYPE_VARCHAR;
    case TYPE_STRING:
        return PColumnType::COLUMN_TYPE_STRING;
    default:
        DCHECK(false) << "Invalid type.";
    }
    DCHECK(false);
    return PColumnType::COLUMN_TYPE_INT;
}

// PColumnType->PrimitiveType
// TODO: use constexpr if we use c++14
PrimitiveType to_primitive_type(PColumnType type) {
    switch (type) {
    case PColumnType::COLUMN_TYPE_BOOL:
        return TYPE_BOOLEAN;
    case PColumnType::COLUMN_TYPE_TINY_INT:
        return TYPE_TINYINT;
    case PColumnType::COLUMN_TYPE_SMALL_INT:
        return TYPE_SMALLINT;
    case PColumnType::COLUMN_TYPE_INT:
        return TYPE_INT;
    case PColumnType::COLUMN_TYPE_BIGINT:
        return TYPE_BIGINT;
    case PColumnType::COLUMN_TYPE_LARGEINT:
        return TYPE_LARGEINT;
    case PColumnType::COLUMN_TYPE_FLOAT:
        return TYPE_FLOAT;
    case PColumnType::COLUMN_TYPE_DOUBLE:
        return TYPE_DOUBLE;
    case PColumnType::COLUMN_TYPE_DATE:
        return TYPE_DATE;
    case PColumnType::COLUMN_TYPE_DATETIME:
        return TYPE_DATETIME;
    case PColumnType::COLUMN_TYPE_DECIMALV2:
        return TYPE_DECIMALV2;
    case PColumnType::COLUMN_TYPE_VARCHAR:
        return TYPE_VARCHAR;
    case PColumnType::COLUMN_TYPE_CHAR:
        return TYPE_CHAR;
    case PColumnType::COLUMN_TYPE_STRING:
        return TYPE_STRING;
    default:
        DCHECK(false);
    }
    return TYPE_INT;
}

// PFilterType -> RuntimeFilterType
RuntimeFilterType get_type(int filter_type) {
    switch (filter_type) {
    case PFilterType::BLOOM_FILTER: {
        return RuntimeFilterType::BLOOM_FILTER;
    }
    case PFilterType::MINMAX_FILTER:
        return RuntimeFilterType::MINMAX_FILTER;
    default:
        return RuntimeFilterType::UNKNOWN_FILTER;
    }
}

// RuntimeFilterType -> PFilterType
PFilterType get_type(RuntimeFilterType type) {
    switch (type) {
    case RuntimeFilterType::BLOOM_FILTER:
        return PFilterType::BLOOM_FILTER;
    case RuntimeFilterType::MINMAX_FILTER:
        return PFilterType::MINMAX_FILTER;
    default:
        return PFilterType::UNKNOW_FILTER;
    }
}

TTypeDesc create_type_desc(PrimitiveType type) {
    TTypeDesc type_desc;
    std::vector<TTypeNode> node_type;
    node_type.emplace_back();
    TScalarType scalarType;
    scalarType.__set_type(to_thrift(type));
    scalarType.__set_len(-1);
    scalarType.__set_precision(-1);
    scalarType.__set_scale(-1);
    node_type.back().__set_scalar_type(scalarType);
    type_desc.__set_types(node_type);
    return type_desc;
}

// only used to push down to olap engine
Expr* create_literal(ObjectPool* pool, PrimitiveType type, const void* data) {
    TExprNode node;

    switch (type) {
    case TYPE_BOOLEAN: {
        TBoolLiteral boolLiteral;
        boolLiteral.__set_value(*reinterpret_cast<const bool*>(data));
        node.__set_bool_literal(boolLiteral);
        break;
    }
    case TYPE_TINYINT: {
        TIntLiteral intLiteral;
        intLiteral.__set_value(*reinterpret_cast<const int8_t*>(data));
        node.__set_int_literal(intLiteral);
        break;
    }
    case TYPE_SMALLINT: {
        TIntLiteral intLiteral;
        intLiteral.__set_value(*reinterpret_cast<const int16_t*>(data));
        node.__set_int_literal(intLiteral);
        break;
    }
    case TYPE_INT: {
        TIntLiteral intLiteral;
        intLiteral.__set_value(*reinterpret_cast<const int32_t*>(data));
        node.__set_int_literal(intLiteral);
        break;
    }
    case TYPE_BIGINT: {
        TIntLiteral intLiteral;
        intLiteral.__set_value(*reinterpret_cast<const int64_t*>(data));
        node.__set_int_literal(intLiteral);
        break;
    }
    case TYPE_LARGEINT: {
        TLargeIntLiteral largeIntLiteral;
        largeIntLiteral.__set_value(
                LargeIntValue::to_string(*reinterpret_cast<const int128_t*>(data)));
        node.__set_large_int_literal(largeIntLiteral);
        break;
    }
    case TYPE_FLOAT: {
        TFloatLiteral floatLiteral;
        floatLiteral.__set_value(*reinterpret_cast<const float*>(data));
        node.__set_float_literal(floatLiteral);
        break;
    }
    case TYPE_DOUBLE: {
        TFloatLiteral floatLiteral;
        floatLiteral.__set_value(*reinterpret_cast<const double*>(data));
        node.__set_float_literal(floatLiteral);
        break;
    }
    case TYPE_DATE:
    case TYPE_DATETIME: {
        TDateLiteral dateLiteral;
        char convert_buffer[30];
        reinterpret_cast<const DateTimeValue*>(data)->to_string(convert_buffer);
        dateLiteral.__set_value(convert_buffer);
        node.__set_date_literal(dateLiteral);
        break;
    }
    case TYPE_DECIMALV2: {
        TDecimalLiteral decimalLiteral;
        decimalLiteral.__set_value(reinterpret_cast<const DecimalV2Value*>(data)->to_string());
        node.__set_decimal_literal(decimalLiteral);
        break;
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING: {
        const StringValue* string_value = reinterpret_cast<const StringValue*>(data);
        TStringLiteral tstringLiteral;
        tstringLiteral.__set_value(std::string(string_value->ptr, string_value->len));
        node.__set_string_literal(tstringLiteral);
        break;
    }
    default:
        DCHECK(false);
        return nullptr;
    }
    node.__set_node_type(get_expr_node_type(type));
    node.__set_type(create_type_desc(type));
    return pool->add(new Literal(node));
}

BinaryPredicate* create_bin_predicate(ObjectPool* pool, PrimitiveType prim_type,
                                      TExprOpcode::type opcode) {
    TExprNode node;
    TScalarType tscalar_type;
    tscalar_type.__set_type(TPrimitiveType::BOOLEAN);
    TTypeNode ttype_node;
    ttype_node.__set_type(TTypeNodeType::SCALAR);
    ttype_node.__set_scalar_type(tscalar_type);
    TTypeDesc t_type_desc;
    t_type_desc.types.push_back(ttype_node);
    node.__set_type(t_type_desc);
    node.__set_opcode(opcode);
    node.__set_child_type(to_thrift(prim_type));
    node.__set_num_children(2);
    node.__set_output_scale(-1);
    node.__set_node_type(TExprNodeType::BINARY_PRED);
    return (BinaryPredicate*)pool->add(BinaryPredicate::from_thrift(node));
}
// This class is a wrapper of runtime predicate function
class RuntimePredicateWrapper {
public:
    RuntimePredicateWrapper(RuntimeState* state, MemTracker* tracker, ObjectPool* pool,
                            const RuntimeFilterParams* params)
            : _tracker(tracker),
              _pool(pool),
              _column_return_type(params->column_return_type),
              _filter_type(params->filter_type) {}
    // for a 'tmp' runtime predicate wrapper
    // only could called assign method or as a param for merge
    RuntimePredicateWrapper(MemTracker* tracker, ObjectPool* pool, RuntimeFilterType type)
            : _tracker(tracker), _pool(pool), _filter_type(type) {}
    // init runtimefilter wrapper
    // alloc memory to init runtime filter function
    Status init(const RuntimeFilterParams* params) {
        switch (_filter_type) {
        case RuntimeFilterType::IN_FILTER: {
            _hybrid_set.reset(create_set(_column_return_type));
            break;
        }
        case RuntimeFilterType::MINMAX_FILTER: {
            _minmax_func.reset(create_minmax_filter(_column_return_type));
            break;
        }
        case RuntimeFilterType::BLOOM_FILTER: {
            _bloomfilter_func.reset(create_bloom_filter(_tracker, _column_return_type));
            return _bloomfilter_func->init_with_fixed_length(params->bloom_filter_size);
        }
        default:
            return Status::InvalidArgument("Unknown Filter type");
        }
        return Status::OK();
    }

    void insert(const void* data) {
        switch (_filter_type) {
        case RuntimeFilterType::IN_FILTER: {
            _hybrid_set->insert(data);
            break;
        }
        case RuntimeFilterType::MINMAX_FILTER: {
            _minmax_func->insert(data);
            break;
        }
        case RuntimeFilterType::BLOOM_FILTER: {
            _bloomfilter_func->insert(data);
            break;
        }
        default:
            DCHECK(false);
            break;
        }
    }

    template <class T>
    Status get_push_context(T* container, RuntimeState* state, ExprContext* prob_expr) {
        DCHECK(state != nullptr);
        DCHECK(container != nullptr);
        DCHECK(_pool != nullptr);
        DCHECK(prob_expr->root()->type().type == _column_return_type);

        switch (_filter_type) {
        case RuntimeFilterType::IN_FILTER: {
            TTypeDesc type_desc = create_type_desc(_column_return_type);
            TExprNode node;
            node.__set_type(type_desc);
            node.__set_node_type(TExprNodeType::IN_PRED);
            node.in_predicate.__set_is_not_in(false);
            node.__set_opcode(TExprOpcode::FILTER_IN);
            node.__isset.vector_opcode = true;
            node.__set_vector_opcode(to_in_opcode(_column_return_type));
            auto in_pred = _pool->add(new InPredicate(node));
            RETURN_IF_ERROR(in_pred->prepare(state, _hybrid_set.release()));
            in_pred->add_child(Expr::copy(_pool, prob_expr->root()));
            ExprContext* ctx = _pool->add(new ExprContext(in_pred));
            container->push_back(ctx);
            break;
        }
        case RuntimeFilterType::MINMAX_FILTER: {
            // create max filter
            auto max_pred = create_bin_predicate(_pool, _column_return_type, TExprOpcode::LE);
            auto max_literal = create_literal(_pool, _column_return_type, _minmax_func->get_max());
            max_pred->add_child(Expr::copy(_pool, prob_expr->root()));
            max_pred->add_child(max_literal);
            container->push_back(_pool->add(new ExprContext(max_pred)));
            // create min filter
            auto min_pred = create_bin_predicate(_pool, _column_return_type, TExprOpcode::GE);
            auto min_literal = create_literal(_pool, _column_return_type, _minmax_func->get_min());
            min_pred->add_child(Expr::copy(_pool, prob_expr->root()));
            min_pred->add_child(min_literal);
            container->push_back(_pool->add(new ExprContext(min_pred)));
            break;
        }
        case RuntimeFilterType::BLOOM_FILTER: {
            // create a bloom filter
            TTypeDesc type_desc = create_type_desc(_column_return_type);
            TExprNode node;
            node.__set_type(type_desc);
            node.__set_node_type(TExprNodeType::BLOOM_PRED);
            node.__set_opcode(TExprOpcode::RT_FILTER);
            node.__isset.vector_opcode = true;
            node.__set_vector_opcode(to_in_opcode(_column_return_type));
            auto bloom_pred = _pool->add(new BloomFilterPredicate(node));
            RETURN_IF_ERROR(bloom_pred->prepare(state, _bloomfilter_func.release()));
            bloom_pred->add_child(Expr::copy(_pool, prob_expr->root()));
            ExprContext* ctx = _pool->add(new ExprContext(bloom_pred));
            container->push_back(ctx);
            break;
        }
        default:
            DCHECK(false);
            break;
        }
        return Status::OK();
    }

    Status merge(const RuntimePredicateWrapper* wrapper) {
        DCHECK(_filter_type == wrapper->_filter_type);
        if (_filter_type != wrapper->_filter_type) {
            return Status::InvalidArgument("invalid filter type");
        }
        switch (_filter_type) {
        case RuntimeFilterType::IN_FILTER: {
            DCHECK(false) << "in filter should't apply in shuffle join";
            return Status::InternalError("in filter should't apply in shuffle join");
        }
        case RuntimeFilterType::MINMAX_FILTER: {
            _minmax_func->merge(wrapper->_minmax_func.get(), _pool);
            break;
        }
        case RuntimeFilterType::BLOOM_FILTER: {
            _bloomfilter_func->merge(wrapper->_bloomfilter_func.get());
            break;
        }
        default:
            DCHECK(false);
            return Status::InternalError("unknown runtime filter");
        }
        return Status::OK();
    }

    // used by shuffle runtime filter
    // assign this filter by protobuf
    Status assign(const PBloomFilter* bloom_filter, const char* data) {
        DCHECK(_tracker != nullptr);
        // we won't use this class to insert or find any data
        // so any type is ok
        _bloomfilter_func.reset(create_bloom_filter(_tracker, PrimitiveType::TYPE_INT));
        return _bloomfilter_func->assign(data, bloom_filter->filter_length());
    }

    // used by shuffle runtime filter
    // assign this filter by protobuf
    Status assign(const PMinMaxFilter* minmax_filter) {
        DCHECK(_tracker != nullptr);
        PrimitiveType type = to_primitive_type(minmax_filter->column_type());
        _minmax_func.reset(create_minmax_filter(type));
        switch (type) {
        case TYPE_BOOLEAN: {
            bool min_val;
            bool max_val;
            min_val = minmax_filter->min_val().boolval();
            max_val = minmax_filter->max_val().boolval();
            return _minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_TINYINT: {
            int8_t min_val;
            int8_t max_val;
            min_val = static_cast<int8_t>(minmax_filter->min_val().intval());
            max_val = static_cast<int8_t>(minmax_filter->max_val().intval());
            return _minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_SMALLINT: {
            int16_t min_val;
            int16_t max_val;
            min_val = static_cast<int16_t>(minmax_filter->min_val().intval());
            max_val = static_cast<int16_t>(minmax_filter->max_val().intval());
            return _minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_INT: {
            int32_t min_val;
            int32_t max_val;
            min_val = minmax_filter->min_val().intval();
            max_val = minmax_filter->max_val().intval();
            return _minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_BIGINT: {
            int64_t min_val;
            int64_t max_val;
            min_val = minmax_filter->min_val().longval();
            max_val = minmax_filter->max_val().longval();
            return _minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_LARGEINT: {
            int128_t min_val;
            int128_t max_val;
            auto min_string_val = minmax_filter->min_val().stringval();
            auto max_string_val = minmax_filter->max_val().stringval();
            StringParser::ParseResult result;
            min_val = StringParser::string_to_int<int128_t>(min_string_val.c_str(),
                                                            min_string_val.length(), &result);
            DCHECK(result == StringParser::PARSE_SUCCESS);
            max_val = StringParser::string_to_int<int128_t>(max_string_val.c_str(),
                                                            max_string_val.length(), &result);
            DCHECK(result == StringParser::PARSE_SUCCESS);
            return _minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_FLOAT: {
            float min_val;
            float max_val;
            min_val = static_cast<float>(minmax_filter->min_val().doubleval());
            max_val = static_cast<float>(minmax_filter->max_val().doubleval());
            return _minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_DOUBLE: {
            double min_val;
            double max_val;
            min_val = static_cast<double>(minmax_filter->min_val().doubleval());
            max_val = static_cast<double>(minmax_filter->max_val().doubleval());
            return _minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_DATETIME:
        case TYPE_DATE: {
            auto& min_val_ref = minmax_filter->min_val().stringval();
            auto& max_val_ref = minmax_filter->max_val().stringval();
            DateTimeValue min_val;
            DateTimeValue max_val;
            min_val.from_date_str(min_val_ref.c_str(), min_val_ref.length());
            max_val.from_date_str(max_val_ref.c_str(), max_val_ref.length());
            return _minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_VARCHAR:
        case TYPE_CHAR:
        case TYPE_STRING: {
            auto& min_val_ref = minmax_filter->min_val().stringval();
            auto& max_val_ref = minmax_filter->max_val().stringval();
            auto min_val_ptr = _pool->add(new std::string(min_val_ref));
            auto max_val_ptr = _pool->add(new std::string(max_val_ref));
            StringValue min_val(const_cast<char*>(min_val_ptr->c_str()), min_val_ptr->length());
            StringValue max_val(const_cast<char*>(max_val_ptr->c_str()), max_val_ptr->length());
            return _minmax_func->assign(&min_val, &max_val);
        }
        default:
            DCHECK(false) << "unknown type";
            break;
        }
        return Status::InvalidArgument("not support!");
    }

    Status get_bloom_filter_desc(char** data, int* filter_length) {
        return _bloomfilter_func->get_data(data, filter_length);
    }

    Status get_minmax_filter_desc(void** min_data, void** max_data) {
        *min_data = _minmax_func->get_min();
        *max_data = _minmax_func->get_max();
        return Status::OK();
    }

    PrimitiveType column_type() { return _column_return_type; }

    void ready_for_publish() {
        if (_filter_type == RuntimeFilterType::MINMAX_FILTER) {
            switch (_column_return_type) {
            case TYPE_VARCHAR:
            case TYPE_CHAR:
            case TYPE_STRING: {
                StringValue* min_value = static_cast<StringValue*>(_minmax_func->get_min());
                StringValue* max_value = static_cast<StringValue*>(_minmax_func->get_max());
                auto min_val_ptr = _pool->add(new std::string(min_value->ptr));
                auto max_val_ptr = _pool->add(new std::string(max_value->ptr));
                StringValue min_val(const_cast<char*>(min_val_ptr->c_str()), min_val_ptr->length());
                StringValue max_val(const_cast<char*>(max_val_ptr->c_str()), max_val_ptr->length());
                _minmax_func->assign(&min_val, &max_val);
            }
            default:
                break;
            }
        }
    }

private:
    MemTracker* _tracker;
    ObjectPool* _pool;
    PrimitiveType _column_return_type; // column type
    RuntimeFilterType _filter_type;
    std::unique_ptr<MinMaxFuncBase> _minmax_func;
    std::unique_ptr<HybridSetBase> _hybrid_set;
    std::unique_ptr<IBloomFilterFuncBase> _bloomfilter_func;
};

Status IRuntimeFilter::create(RuntimeState* state, MemTracker* tracker, ObjectPool* pool,
                              const TRuntimeFilterDesc* desc, const RuntimeFilterRole role,
                              int node_id, IRuntimeFilter** res) {
    *res = pool->add(new IRuntimeFilter(state, tracker, pool));
    (*res)->set_role(role);
    return (*res)->init_with_desc(desc, node_id);
}

void IRuntimeFilter::insert(const void* data) {
    DCHECK(is_producer());
    _wrapper->insert(data);
}

Status IRuntimeFilter::publish() {
    DCHECK(is_producer());
    if (_has_local_target) {
        IRuntimeFilter* consumer_filter = nullptr;
        // TODO: log if err
        Status status =
                _state->runtime_filter_mgr()->get_consume_filter(_filter_id, &consumer_filter);
        DCHECK(status.ok());
        // push down
        std::swap(this->_wrapper, consumer_filter->_wrapper);
        consumer_filter->signal();
        return Status::OK();
    } else {
        TNetworkAddress addr;
        RETURN_IF_ERROR(_state->runtime_filter_mgr()->get_merge_addr(&addr));
        return push_to_remote(_state, &addr);
    }
}

void IRuntimeFilter::publish_finally() {
    DCHECK(is_producer());
    join_rpc();
}

Status IRuntimeFilter::get_push_expr_ctxs(std::list<ExprContext*>* push_expr_ctxs) {
    DCHECK(is_consumer());
    if (!_is_ignored) {
        return _wrapper->get_push_context(push_expr_ctxs, _state, _probe_ctx);
    }
    return Status::OK();
}

Status IRuntimeFilter::get_push_expr_ctxs(std::list<ExprContext*>* push_expr_ctxs,
                                          ExprContext* probe_ctx) {
    DCHECK(is_producer());
    return _wrapper->get_push_context(push_expr_ctxs, _state, probe_ctx);
}

Status IRuntimeFilter::get_prepared_context(std::vector<ExprContext*>* push_expr_ctxs,
                                            const RowDescriptor& desc,
                                            const std::shared_ptr<MemTracker>& tracker) {
    DCHECK(_is_ready);
    DCHECK(is_consumer());
    std::lock_guard<std::mutex> guard(_inner_mutex);

    if (!_push_down_ctxs.empty()) {
        push_expr_ctxs->insert(push_expr_ctxs->end(), _push_down_ctxs.begin(),
                               _push_down_ctxs.end());
        return Status::OK();
    }
    // push expr
    RETURN_IF_ERROR(_wrapper->get_push_context(&_push_down_ctxs, _state, _probe_ctx));
    RETURN_IF_ERROR(Expr::prepare(_push_down_ctxs, _state, desc, tracker));
    return Expr::open(_push_down_ctxs, _state);
}

bool IRuntimeFilter::await() {
    DCHECK(is_consumer());
    SCOPED_TIMER(_await_time_cost);
    int64_t wait_times_ms = _state->runtime_filter_wait_time_ms();
    if (!_is_ready) {
        std::unique_lock<std::mutex> lock(_inner_mutex);
        return _inner_cv.wait_for(lock, std::chrono::milliseconds(wait_times_ms),
                                  [this] { return this->_is_ready; });
    }
    return true;
}

void IRuntimeFilter::signal() {
    DCHECK(is_consumer());
    _is_ready = true;
    _inner_cv.notify_all();
    _effect_timer.reset();
}

Status IRuntimeFilter::init_with_desc(const TRuntimeFilterDesc* desc, int node_id) {
    // if node_id == -1 , it shouldn't be a consumer
    DCHECK(node_id >= 0 || (node_id == -1 && !is_consumer()));

    if (desc->type == TRuntimeFilterType::BLOOM) {
        _runtime_filter_type = RuntimeFilterType::BLOOM_FILTER;
    } else if (desc->type == TRuntimeFilterType::MIN_MAX) {
        _runtime_filter_type = RuntimeFilterType::MINMAX_FILTER;
    } else if (desc->type == TRuntimeFilterType::IN) {
        _runtime_filter_type = RuntimeFilterType::IN_FILTER;
    } else {
        return Status::InvalidArgument("unknown filter type");
    }

    _is_broadcast_join = desc->is_broadcast_join;
    _has_local_target = desc->has_local_targets;
    _has_remote_target = desc->has_remote_targets;
    _expr_order = desc->expr_order;
    _filter_id = desc->filter_id;

    ExprContext* build_ctx = nullptr;
    RETURN_IF_ERROR(Expr::create_expr_tree(_pool, desc->src_expr, &build_ctx));

    RuntimeFilterParams params;
    params.filter_type = _runtime_filter_type;
    params.column_return_type = build_ctx->root()->type().type;
    if (desc->__isset.bloom_filter_size_bytes) {
        params.bloom_filter_size = desc->bloom_filter_size_bytes;
    }

    if (node_id >= 0) {
        DCHECK(is_consumer());
        const auto iter = desc->planId_to_target_expr.find(node_id);
        if (iter == desc->planId_to_target_expr.end()) {
            DCHECK(false) << "runtime filter not found node_id:" << node_id;
            return Status::InternalError("not found a node id");
        }
        RETURN_IF_ERROR(Expr::create_expr_tree(_pool, iter->second, &_probe_ctx));
    }

    _wrapper = _pool->add(new RuntimePredicateWrapper(_state, _mem_tracker, _pool, &params));
    return _wrapper->init(&params);
}

Status IRuntimeFilter::serialize(PMergeFilterRequest* request, void** data, int* len) {
    return _serialize(request, data, len);
}

Status IRuntimeFilter::serialize(PPublishFilterRequest* request, void** data, int* len) {
    return _serialize(request, data, len);
}

Status IRuntimeFilter::create_wrapper(const MergeRuntimeFilterParams* param, MemTracker* tracker,
                                      ObjectPool* pool,
                                      std::unique_ptr<RuntimePredicateWrapper>* wrapper) {
    return _create_wrapper(param, tracker, pool, wrapper);
}

Status IRuntimeFilter::create_wrapper(const UpdateRuntimeFilterParams* param, MemTracker* tracker,
                                      ObjectPool* pool,
                                      std::unique_ptr<RuntimePredicateWrapper>* wrapper) {
    return _create_wrapper(param, tracker, pool, wrapper);
}

template <class T>
Status IRuntimeFilter::_create_wrapper(const T* param, MemTracker* tracker, ObjectPool* pool,
                                       std::unique_ptr<RuntimePredicateWrapper>* wrapper) {
    int filter_type = param->request->filter_type();
    wrapper->reset(new RuntimePredicateWrapper(tracker, pool, get_type(filter_type)));

    switch (filter_type) {
    case PFilterType::BLOOM_FILTER: {
        DCHECK(param->request->has_bloom_filter());
        return (*wrapper)->assign(&param->request->bloom_filter(), param->data);
    }
    case PFilterType::MINMAX_FILTER: {
        DCHECK(param->request->has_minmax_filter());
        return (*wrapper)->assign(&param->request->minmax_filter());
    }
    default:
        return Status::InvalidArgument("unknow filter type");
    }
}

void IRuntimeFilter::init_profile(RuntimeProfile* parent_profile) {
    DCHECK(parent_profile != nullptr);
    _profile.reset(new RuntimeProfile("RuntimeFilter:" + ::doris::to_string(_runtime_filter_type)));
    parent_profile->add_child(_profile.get(), true, nullptr);

    _effect_time_cost = ADD_TIMER(_profile, "EffectTimeCost");
    _await_time_cost = ADD_TIMER(_profile, "AWaitTimeCost");
    _effect_timer.reset(new ScopedTimer<MonotonicStopWatch>(_effect_time_cost));
    _effect_timer->start();
}

void IRuntimeFilter::set_push_down_profile() {
    _profile->add_info_string("HasPushDownToEngine", "true");
}

void IRuntimeFilter::ready_for_publish() {
    _wrapper->ready_for_publish();
}

Status IRuntimeFilter::merge_from(const RuntimePredicateWrapper* wrapper) {
    return _wrapper->merge(wrapper);
}

template <class T>
Status IRuntimeFilter::_serialize(T* request, void** data, int* len) {
    request->set_filter_type(get_type(_runtime_filter_type));

    if (_runtime_filter_type == RuntimeFilterType::BLOOM_FILTER) {
        RETURN_IF_ERROR(_wrapper->get_bloom_filter_desc((char**)data, len));
        DCHECK(data != nullptr);
        request->mutable_bloom_filter()->set_filter_length(*len);
        request->mutable_bloom_filter()->set_always_true(false);
    } else if (_runtime_filter_type == RuntimeFilterType::MINMAX_FILTER) {
        auto minmax_filter = request->mutable_minmax_filter();
        to_protobuf(minmax_filter);
    } else {
        return Status::InvalidArgument("not implemented !");
    }
    return Status::OK();
}

void IRuntimeFilter::to_protobuf(PMinMaxFilter* filter) {
    void* min_data = nullptr;
    void* max_data = nullptr;
    _wrapper->get_minmax_filter_desc(&min_data, &max_data);
    DCHECK(min_data != nullptr);
    DCHECK(max_data != nullptr);
    filter->set_column_type(to_proto(_wrapper->column_type()));

    switch (_wrapper->column_type()) {
    case TYPE_BOOLEAN: {
        filter->mutable_min_val()->set_boolval(*reinterpret_cast<const int32_t*>(min_data));
        filter->mutable_max_val()->set_boolval(*reinterpret_cast<const int32_t*>(max_data));
        return;
    }
    case TYPE_TINYINT: {
        filter->mutable_min_val()->set_intval(*reinterpret_cast<const int8_t*>(min_data));
        filter->mutable_max_val()->set_intval(*reinterpret_cast<const int8_t*>(max_data));
        return;
    }
    case TYPE_SMALLINT: {
        filter->mutable_min_val()->set_intval(*reinterpret_cast<const int16_t*>(min_data));
        filter->mutable_max_val()->set_intval(*reinterpret_cast<const int16_t*>(max_data));
        return;
    }
    case TYPE_INT: {
        filter->mutable_min_val()->set_intval(*reinterpret_cast<const int32_t*>(min_data));
        filter->mutable_max_val()->set_intval(*reinterpret_cast<const int32_t*>(max_data));
        return;
    }
    case TYPE_BIGINT: {
        filter->mutable_min_val()->set_longval(*reinterpret_cast<const int64_t*>(min_data));
        filter->mutable_max_val()->set_longval(*reinterpret_cast<const int64_t*>(max_data));
        return;
    }
    case TYPE_LARGEINT: {
        filter->mutable_min_val()->set_stringval(
                LargeIntValue::to_string(*reinterpret_cast<const int128_t*>(min_data)));
        filter->mutable_max_val()->set_stringval(
                LargeIntValue::to_string(*reinterpret_cast<const int128_t*>(max_data)));
        return;
    }
    case TYPE_FLOAT: {
        filter->mutable_min_val()->set_doubleval(*reinterpret_cast<const float*>(min_data));
        filter->mutable_max_val()->set_doubleval(*reinterpret_cast<const float*>(max_data));
        return;
    }
    case TYPE_DOUBLE: {
        filter->mutable_min_val()->set_doubleval(*reinterpret_cast<const double*>(min_data));
        filter->mutable_max_val()->set_doubleval(*reinterpret_cast<const double*>(max_data));
        return;
    }
    case TYPE_DATE:
    case TYPE_DATETIME: {
        char convert_buffer[30];
        reinterpret_cast<const DateTimeValue*>(min_data)->to_string(convert_buffer);
        filter->mutable_min_val()->set_stringval(convert_buffer);
        reinterpret_cast<const DateTimeValue*>(max_data)->to_string(convert_buffer);
        filter->mutable_max_val()->set_stringval(convert_buffer);
        return;
    }
    case TYPE_DECIMALV2: {
        filter->mutable_min_val()->set_stringval(
                reinterpret_cast<const DecimalV2Value*>(min_data)->to_string());
        filter->mutable_max_val()->set_stringval(
                reinterpret_cast<const DecimalV2Value*>(max_data)->to_string());
        return;
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING: {
        const StringValue* min_string_value = reinterpret_cast<const StringValue*>(min_data);
        filter->mutable_min_val()->set_stringval(
                std::string(min_string_value->ptr, min_string_value->len));
        const StringValue* max_string_value = reinterpret_cast<const StringValue*>(max_data);
        filter->mutable_max_val()->set_stringval(
                std::string(max_string_value->ptr, max_string_value->len));
        break;
    }
    default: {
        DCHECK(false) << "unknown type";
        break;
    }
    }
}

Status IRuntimeFilter::update_filter(const UpdateRuntimeFilterParams* param) {
    std::unique_ptr<RuntimePredicateWrapper> wrapper;
    RETURN_IF_ERROR(IRuntimeFilter::create_wrapper(param, _mem_tracker, _pool, &wrapper));
    RETURN_IF_ERROR(_wrapper->merge(wrapper.get()));
    this->signal();
    return Status::OK();
}

Status IRuntimeFilter::consumer_close() {
    DCHECK(is_consumer());
    Expr::close(_push_down_ctxs, _state);
    return Status::OK();
}

RuntimeFilterWrapperHolder::RuntimeFilterWrapperHolder() = default;
RuntimeFilterWrapperHolder::~RuntimeFilterWrapperHolder() = default;

} // namespace doris
