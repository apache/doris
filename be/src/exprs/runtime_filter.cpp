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

#include <gen_cpp/Opcodes_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <stddef.h>

#include <algorithm>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <utility>

#include "common/logging.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exprs/bitmapfilter_predicate.h"
#include "exprs/bloom_filter_func.h"
#include "exprs/create_predicate_function.h"
#include "exprs/hybrid_set.h"
#include "exprs/minmax_predicate.h"
#include "gutil/strings/substitute.h"
#include "pipeline/pipeline_x/dependency.h"
#include "runtime/define_primitive_type.h"
#include "runtime/large_int_value.h"
#include "runtime/primitive_type.h"
#include "runtime/runtime_filter_mgr.h"
#include "util/bitmap_value.h"
#include "util/runtime_profile.h"
#include "util/string_parser.hpp"
#include "vec/columns/column.h"
#include "vec/columns/column_complex.h"
#include "vec/common/assert_cast.h"
#include "vec/core/wide_integer.h"
#include "vec/core/wide_integer_to_string.h"
#include "vec/exprs/vbitmap_predicate.h"
#include "vec/exprs/vbloom_predicate.h"
#include "vec/exprs/vdirect_in_predicate.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vliteral.h"
#include "vec/exprs/vruntimefilter_wrapper.h"
#include "vec/runtime/shared_hash_table_controller.h"
namespace doris {

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
    case TYPE_DATEV2:
        return PColumnType::COLUMN_TYPE_DATEV2;
    case TYPE_DATETIMEV2:
        return PColumnType::COLUMN_TYPE_DATETIMEV2;
    case TYPE_DATETIME:
        return PColumnType::COLUMN_TYPE_DATETIME;
    case TYPE_DECIMALV2:
        return PColumnType::COLUMN_TYPE_DECIMALV2;
    case TYPE_DECIMAL32:
        return PColumnType::COLUMN_TYPE_DECIMAL32;
    case TYPE_DECIMAL64:
        return PColumnType::COLUMN_TYPE_DECIMAL64;
    case TYPE_DECIMAL128I:
        return PColumnType::COLUMN_TYPE_DECIMAL128I;
    case TYPE_DECIMAL256:
        return PColumnType::COLUMN_TYPE_DECIMAL256;
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
    case PColumnType::COLUMN_TYPE_DATEV2:
        return TYPE_DATEV2;
    case PColumnType::COLUMN_TYPE_DATETIMEV2:
        return TYPE_DATETIMEV2;
    case PColumnType::COLUMN_TYPE_DATETIME:
        return TYPE_DATETIME;
    case PColumnType::COLUMN_TYPE_DECIMALV2:
        return TYPE_DECIMALV2;
    case PColumnType::COLUMN_TYPE_DECIMAL32:
        return TYPE_DECIMAL32;
    case PColumnType::COLUMN_TYPE_DECIMAL64:
        return TYPE_DECIMAL64;
    case PColumnType::COLUMN_TYPE_DECIMAL128I:
        return TYPE_DECIMAL128I;
    case PColumnType::COLUMN_TYPE_DECIMAL256:
        return TYPE_DECIMAL256;
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
    case PFilterType::IN_FILTER: {
        return RuntimeFilterType::IN_FILTER;
    }
    case PFilterType::BLOOM_FILTER: {
        return RuntimeFilterType::BLOOM_FILTER;
    }
    case PFilterType::MINMAX_FILTER:
        return RuntimeFilterType::MINMAX_FILTER;
    case PFilterType::MIN_FILTER:
        return RuntimeFilterType::MIN_FILTER;
    case PFilterType::MAX_FILTER:
        return RuntimeFilterType::MAX_FILTER;
    default:
        return RuntimeFilterType::UNKNOWN_FILTER;
    }
}

// RuntimeFilterType -> PFilterType
PFilterType get_type(RuntimeFilterType type) {
    switch (type) {
    case RuntimeFilterType::IN_FILTER:
        return PFilterType::IN_FILTER;
    case RuntimeFilterType::BLOOM_FILTER:
        return PFilterType::BLOOM_FILTER;
    case RuntimeFilterType::MIN_FILTER:
        return PFilterType::MIN_FILTER;
    case RuntimeFilterType::MAX_FILTER:
        return PFilterType::MAX_FILTER;
    case RuntimeFilterType::MINMAX_FILTER:
        return PFilterType::MINMAX_FILTER;
    case RuntimeFilterType::IN_OR_BLOOM_FILTER:
        return PFilterType::IN_OR_BLOOM_FILTER;
    default:
        return PFilterType::UNKNOW_FILTER;
    }
}

Status create_literal(const TypeDescriptor& type, const void* data, vectorized::VExprSPtr& expr) {
    TExprNode node = create_texpr_node_from(data, type.type, type.precision, type.scale);

    try {
        expr = vectorized::VLiteral::create_shared(node);
    } catch (const Exception& e) {
        return e.to_status();
    }

    return Status::OK();
}

Status create_vbin_predicate(const TypeDescriptor& type, TExprOpcode::type opcode,
                             vectorized::VExprSPtr& expr, TExprNode* tnode) {
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
    node.__set_child_type(to_thrift(type.type));
    node.__set_num_children(2);
    node.__set_output_scale(type.scale);
    node.__set_node_type(TExprNodeType::BINARY_PRED);
    TFunction fn;
    TFunctionName fn_name;
    fn_name.__set_db_name("");
    switch (opcode) {
    case TExprOpcode::LE:
        fn_name.__set_function_name("le");
        break;
    case TExprOpcode::GE:
        fn_name.__set_function_name("ge");
        break;
    default:
        return Status::InvalidArgument(
                strings::Substitute("Invalid opcode for max_min_runtimefilter: '$0'", opcode));
    }
    fn.__set_name(fn_name);
    fn.__set_binary_type(TFunctionBinaryType::BUILTIN);

    TTypeNode type_node;
    type_node.__set_type(TTypeNodeType::SCALAR);
    TScalarType scalar_type;
    scalar_type.__set_type(to_thrift(type.type));
    scalar_type.__set_precision(type.precision);
    scalar_type.__set_scale(type.scale);
    type_node.__set_scalar_type(scalar_type);

    std::vector<TTypeNode> type_nodes;
    type_nodes.push_back(type_node);

    TTypeDesc type_desc;
    type_desc.__set_types(type_nodes);

    std::vector<TTypeDesc> arg_types;
    arg_types.push_back(type_desc);
    arg_types.push_back(type_desc);
    fn.__set_arg_types(arg_types);

    fn.__set_ret_type(t_type_desc);
    fn.__set_has_var_args(false);
    node.__set_fn(fn);
    *tnode = node;
    return vectorized::VExpr::create_expr(node, expr);
}
// This class is a wrapper of runtime predicate function
class RuntimePredicateWrapper {
public:
    RuntimePredicateWrapper(RuntimeState* state, ObjectPool* pool,
                            const RuntimeFilterParams* params)
            : _state(state),
              _be_exec_version(_state->be_exec_version()),
              _pool(pool),
              _column_return_type(params->column_return_type),
              _filter_type(params->filter_type),
              _filter_id(params->filter_id),
              _use_batch(
                      IRuntimeFilter::enable_use_batch(_be_exec_version > 0, _column_return_type)),
              _use_new_hash(_be_exec_version >= 2) {}
    // for a 'tmp' runtime predicate wrapper
    // only could called assign method or as a param for merge
    RuntimePredicateWrapper(RuntimeState* state, ObjectPool* pool, PrimitiveType column_type,
                            RuntimeFilterType type, uint32_t filter_id)
            : _state(state),
              _be_exec_version(_state->be_exec_version()),
              _pool(pool),
              _column_return_type(column_type),
              _filter_type(type),
              _filter_id(filter_id),
              _use_batch(
                      IRuntimeFilter::enable_use_batch(_be_exec_version > 0, _column_return_type)),
              _use_new_hash(_be_exec_version >= 2) {}

    RuntimePredicateWrapper(QueryContext* query_ctx, ObjectPool* pool,
                            const RuntimeFilterParams* params)
            : _query_ctx(query_ctx),
              _be_exec_version(_query_ctx->be_exec_version()),
              _pool(pool),
              _column_return_type(params->column_return_type),
              _filter_type(params->filter_type),
              _filter_id(params->filter_id),
              _use_batch(
                      IRuntimeFilter::enable_use_batch(_be_exec_version > 0, _column_return_type)),
              _use_new_hash(_be_exec_version >= 2) {}
    // for a 'tmp' runtime predicate wrapper
    // only could called assign method or as a param for merge
    RuntimePredicateWrapper(QueryContext* query_ctx, ObjectPool* pool, PrimitiveType column_type,
                            RuntimeFilterType type, uint32_t filter_id)
            : _query_ctx(query_ctx),
              _be_exec_version(_query_ctx->be_exec_version()),
              _pool(pool),
              _column_return_type(column_type),
              _filter_type(type),
              _filter_id(filter_id),
              _use_batch(
                      IRuntimeFilter::enable_use_batch(_be_exec_version > 0, _column_return_type)),
              _use_new_hash(_be_exec_version >= 2) {}
    // init runtime filter wrapper
    // alloc memory to init runtime filter function
    Status init(const RuntimeFilterParams* params) {
        _max_in_num = params->max_in_num;
        switch (_filter_type) {
        case RuntimeFilterType::IN_FILTER: {
            _context.hybrid_set.reset(create_set(_column_return_type));
            break;
        }
        case RuntimeFilterType::MIN_FILTER:
        case RuntimeFilterType::MAX_FILTER:
        case RuntimeFilterType::MINMAX_FILTER: {
            _context.minmax_func.reset(create_minmax_filter(_column_return_type));
            break;
        }
        case RuntimeFilterType::BLOOM_FILTER: {
            _is_bloomfilter = true;
            _context.bloom_filter_func.reset(create_bloom_filter(_column_return_type));
            _context.bloom_filter_func->set_length(params->bloom_filter_size);
            _context.bloom_filter_func->set_build_bf_exactly(params->build_bf_exactly);
            return Status::OK();
        }
        case RuntimeFilterType::IN_OR_BLOOM_FILTER: {
            _context.hybrid_set.reset(create_set(_column_return_type));
            _context.bloom_filter_func.reset(create_bloom_filter(_column_return_type));
            _context.bloom_filter_func->set_length(params->bloom_filter_size);
            _context.bloom_filter_func->set_build_bf_exactly(params->build_bf_exactly);
            return Status::OK();
        }
        case RuntimeFilterType::BITMAP_FILTER: {
            _context.bitmap_filter_func.reset(create_bitmap_filter(_column_return_type));
            _context.bitmap_filter_func->set_not_in(params->bitmap_filter_not_in);
            return Status::OK();
        }
        default:
            return Status::InvalidArgument("Unknown Filter type");
        }
        return Status::OK();
    }

    void change_to_bloom_filter() {
        CHECK(_filter_type == RuntimeFilterType::IN_OR_BLOOM_FILTER)
                << "Can not change to bloom filter because of runtime filter type is "
                << IRuntimeFilter::to_string(_filter_type);
        _is_bloomfilter = true;
        BloomFilterFuncBase* bf = _context.bloom_filter_func.get();
        // BloomFilter may be not init
        static_cast<void>(bf->init_with_fixed_length());
        insert_to_bloom_filter(bf);
        // release in filter
        _context.hybrid_set.reset(create_set(_column_return_type));
    }

    Status init_bloom_filter(const size_t build_bf_cardinality) {
        DCHECK(_filter_type == RuntimeFilterType::BLOOM_FILTER ||
               _filter_type == RuntimeFilterType::IN_OR_BLOOM_FILTER);
        return _context.bloom_filter_func->init_with_cardinality(build_bf_cardinality);
    }

    void insert_to_bloom_filter(BloomFilterFuncBase* bloom_filter) const {
        if (_context.hybrid_set->size() > 0) {
            auto it = _context.hybrid_set->begin();

            if (_use_batch) {
                while (it->has_next()) {
                    bloom_filter->insert_fixed_len((char*)it->get_value());
                    it->next();
                }
            } else {
                while (it->has_next()) {
                    if (_use_new_hash) {
                        bloom_filter->insert_crc32_hash(it->get_value());
                    } else {
                        bloom_filter->insert(it->get_value());
                    }

                    it->next();
                }
            }
        }
    }

    BloomFilterFuncBase* get_bloomfilter() const { return _context.bloom_filter_func.get(); }

    void insert(const void* data) {
        switch (_filter_type) {
        case RuntimeFilterType::IN_FILTER: {
            if (_is_ignored_in_filter) {
                break;
            }
            _context.hybrid_set->insert(data);
            break;
        }
        case RuntimeFilterType::MIN_FILTER:
        case RuntimeFilterType::MAX_FILTER:
        case RuntimeFilterType::MINMAX_FILTER: {
            _context.minmax_func->insert(data);
            break;
        }
        case RuntimeFilterType::BLOOM_FILTER: {
            if (_use_new_hash) {
                _context.bloom_filter_func->insert_crc32_hash(data);
            } else {
                _context.bloom_filter_func->insert(data);
            }
            break;
        }
        case RuntimeFilterType::IN_OR_BLOOM_FILTER: {
            if (_is_bloomfilter) {
                if (_use_new_hash) {
                    _context.bloom_filter_func->insert_crc32_hash(data);
                } else {
                    _context.bloom_filter_func->insert(data);
                }
            } else {
                _context.hybrid_set->insert(data);
            }
            break;
        }
        case RuntimeFilterType::BITMAP_FILTER: {
            _context.bitmap_filter_func->insert(data);
            break;
        }
        default:
            DCHECK(false);
            break;
        }
    }

    void insert_fixed_len(const char* data, const int* offsets, int number) {
        switch (_filter_type) {
        case RuntimeFilterType::IN_FILTER: {
            if (_is_ignored_in_filter) {
                break;
            }
            _context.hybrid_set->insert_fixed_len(data, offsets, number);
            break;
        }
        case RuntimeFilterType::MIN_FILTER:
        case RuntimeFilterType::MAX_FILTER:
        case RuntimeFilterType::MINMAX_FILTER: {
            _context.minmax_func->insert_fixed_len(data, offsets, number);
            break;
        }
        case RuntimeFilterType::BLOOM_FILTER: {
            _context.bloom_filter_func->insert_fixed_len(data, offsets, number);
            break;
        }
        case RuntimeFilterType::IN_OR_BLOOM_FILTER: {
            if (_is_bloomfilter) {
                _context.bloom_filter_func->insert_fixed_len(data, offsets, number);
            } else {
                _context.hybrid_set->insert_fixed_len(data, offsets, number);
            }
            break;
        }
        default:
            DCHECK(false);
            break;
        }
    }

    void insert(const StringRef& value) {
        switch (_column_return_type) {
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_HLL:
        case TYPE_STRING: {
            // StringRef->StringRef
            StringRef data = StringRef(value.data, value.size);
            insert(reinterpret_cast<const void*>(&data));
            break;
        }

        default:
            insert(reinterpret_cast<const void*>(value.data));
            break;
        }
    }

    void insert_batch(const vectorized::ColumnPtr column, const std::vector<int>& rows) {
        if (get_real_type() == RuntimeFilterType::BITMAP_FILTER) {
            bitmap_filter_insert_batch(column, rows);
        } else if (IRuntimeFilter::enable_use_batch(_be_exec_version > 0, _column_return_type)) {
            insert_fixed_len(column->get_raw_data().data, rows.data(), rows.size());
        } else {
            for (int index : rows) {
                insert(column->get_data_at(index));
            }
        }
    }

    void bitmap_filter_insert_batch(const vectorized::ColumnPtr column,
                                    const std::vector<int>& rows) {
        std::vector<const BitmapValue*> bitmaps;
        auto* col = assert_cast<const vectorized::ColumnComplexType<BitmapValue>*>(column.get());
        for (int index : rows) {
            bitmaps.push_back(&(col->get_data()[index]));
        }
        _context.bitmap_filter_func->insert_many(bitmaps);
    }

    RuntimeFilterType get_real_type() {
        auto real_filter_type = _filter_type;
        if (real_filter_type == RuntimeFilterType::IN_OR_BLOOM_FILTER) {
            real_filter_type = _is_bloomfilter ? RuntimeFilterType::BLOOM_FILTER
                                               : RuntimeFilterType::IN_FILTER;
        }
        return real_filter_type;
    }

    size_t get_bloom_filter_size() {
        if (_is_bloomfilter) {
            return _context.bloom_filter_func->get_size();
        }
        return 0;
    }

    Status get_push_exprs(std::list<vectorized::VExprContextSPtr>& probe_ctxs,
                          std::vector<vectorized::VExprSPtr>& push_exprs, const TExpr& probe_expr);

    Status merge(const RuntimePredicateWrapper* wrapper) {
        bool can_not_merge_in_or_bloom = _filter_type == RuntimeFilterType::IN_OR_BLOOM_FILTER &&
                                         (wrapper->_filter_type != RuntimeFilterType::IN_FILTER &&
                                          wrapper->_filter_type != RuntimeFilterType::BLOOM_FILTER);

        bool can_not_merge_other = _filter_type != RuntimeFilterType::IN_OR_BLOOM_FILTER &&
                                   _filter_type != wrapper->_filter_type;

        CHECK(!can_not_merge_in_or_bloom && !can_not_merge_other)
                << " can not merge runtime filter(id=" << _filter_id
                << "), current is filter type is " << IRuntimeFilter::to_string(_filter_type)
                << ", other filter type is " << IRuntimeFilter::to_string(wrapper->_filter_type);

        switch (_filter_type) {
        case RuntimeFilterType::IN_FILTER: {
            if (_is_ignored_in_filter) {
                break;
            } else if (wrapper->_is_ignored_in_filter) {
                VLOG_DEBUG << " ignore merge runtime filter(in filter id " << _filter_id
                           << ") because: " << *(wrapper->get_ignored_in_filter_msg());

                _is_ignored_in_filter = true;
                _ignored_in_filter_msg = wrapper->_ignored_in_filter_msg;
                // release in filter
                _context.hybrid_set.reset(create_set(_column_return_type));
                break;
            }
            // try insert set
            _context.hybrid_set->insert(wrapper->_context.hybrid_set.get());
            if (_max_in_num >= 0 && _context.hybrid_set->size() >= _max_in_num) {
#ifdef VLOG_DEBUG_IS_ON
                std::stringstream msg;
                msg << " ignore merge runtime filter(in filter id " << _filter_id
                    << ") because: in_num(" << _context.hybrid_set->size() << ") >= max_in_num("
                    << _max_in_num << ")";
                _ignored_in_filter_msg = _pool->add(new std::string(msg.str()));
#else
                _ignored_in_filter_msg = _pool->add(new std::string("ignored"));
#endif
                _is_ignored_in_filter = true;

                // release in filter
                _context.hybrid_set.reset(create_set(_column_return_type));
            }
            break;
        }
        case RuntimeFilterType::MIN_FILTER:
        case RuntimeFilterType::MAX_FILTER:
        case RuntimeFilterType::MINMAX_FILTER: {
            RETURN_IF_ERROR(
                    _context.minmax_func->merge(wrapper->_context.minmax_func.get(), _pool));
            break;
        }
        case RuntimeFilterType::BLOOM_FILTER: {
            RETURN_IF_ERROR(
                    _context.bloom_filter_func->merge(wrapper->_context.bloom_filter_func.get()));
            break;
        }
        case RuntimeFilterType::IN_OR_BLOOM_FILTER: {
            auto real_filter_type = _is_bloomfilter ? RuntimeFilterType::BLOOM_FILTER
                                                    : RuntimeFilterType::IN_FILTER;
            if (real_filter_type == RuntimeFilterType::IN_FILTER) {
                if (wrapper->_filter_type == RuntimeFilterType::IN_FILTER) { // in merge in
                    CHECK(!wrapper->_is_ignored_in_filter)
                            << " can not ignore merge runtime filter(in filter id "
                            << wrapper->_filter_id << ") when used IN_OR_BLOOM_FILTER, ignore msg: "
                            << *(wrapper->get_ignored_in_filter_msg());
                    _context.hybrid_set->insert(wrapper->_context.hybrid_set.get());
                    if (_max_in_num >= 0 && _context.hybrid_set->size() >= _max_in_num) {
                        VLOG_DEBUG << " change runtime filter to bloom filter(id=" << _filter_id
                                   << ") because: in_num(" << _context.hybrid_set->size()
                                   << ") >= max_in_num(" << _max_in_num << ")";
                        change_to_bloom_filter();
                    }
                    // in merge bloom filter
                } else {
                    VLOG_DEBUG << " change runtime filter to bloom filter(id=" << _filter_id
                               << ") because: already exist a bloom filter";
                    change_to_bloom_filter();
                    RETURN_IF_ERROR(_context.bloom_filter_func->merge(
                            wrapper->_context.bloom_filter_func.get()));
                }
            } else {
                if (wrapper->_filter_type ==
                    RuntimeFilterType::IN_FILTER) { // bloom filter merge in
                    CHECK(!wrapper->_is_ignored_in_filter)
                            << " can not ignore merge runtime filter(in filter id "
                            << wrapper->_filter_id << ") when used IN_OR_BLOOM_FILTER, ignore msg: "
                            << *(wrapper->get_ignored_in_filter_msg());
                    wrapper->insert_to_bloom_filter(_context.bloom_filter_func.get());
                    // bloom filter merge bloom filter
                } else {
                    RETURN_IF_ERROR(_context.bloom_filter_func->merge(
                            wrapper->_context.bloom_filter_func.get()));
                }
            }
            break;
        }
        default:
            return Status::InternalError("unknown runtime filter");
        }
        return Status::OK();
    }

    Status assign(const PInFilter* in_filter) {
        PrimitiveType type = to_primitive_type(in_filter->column_type());
        if (in_filter->has_ignored_msg()) {
            VLOG_DEBUG << "Ignore in filter(id=" << _filter_id
                       << ") because: " << in_filter->ignored_msg();
            _is_ignored_in_filter = true;
            _ignored_in_filter_msg = _pool->add(new std::string(in_filter->ignored_msg()));
            return Status::OK();
        }
        _context.hybrid_set.reset(create_set(type));
        switch (type) {
        case TYPE_BOOLEAN: {
            batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column,
                                       ObjectPool* pool) {
                bool bool_val = column.boolval();
                set->insert(&bool_val);
            });
            break;
        }
        case TYPE_TINYINT: {
            batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column,
                                       ObjectPool* pool) {
                int8_t int_val = static_cast<int8_t>(column.intval());
                set->insert(&int_val);
            });
            break;
        }
        case TYPE_SMALLINT: {
            batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column,
                                       ObjectPool* pool) {
                int16_t int_val = static_cast<int16_t>(column.intval());
                set->insert(&int_val);
            });
            break;
        }
        case TYPE_INT: {
            batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column,
                                       ObjectPool* pool) {
                int32_t int_val = column.intval();
                set->insert(&int_val);
            });
            break;
        }
        case TYPE_BIGINT: {
            batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column,
                                       ObjectPool* pool) {
                int64_t long_val = column.longval();
                set->insert(&long_val);
            });
            break;
        }
        case TYPE_LARGEINT: {
            batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column,
                                       ObjectPool* pool) {
                auto string_val = column.stringval();
                StringParser::ParseResult result;
                int128_t int128_val = StringParser::string_to_int<int128_t>(
                        string_val.c_str(), string_val.length(), &result);
                DCHECK(result == StringParser::PARSE_SUCCESS);
                set->insert(&int128_val);
            });
            break;
        }
        case TYPE_FLOAT: {
            batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column,
                                       ObjectPool* pool) {
                float float_val = static_cast<float>(column.doubleval());
                set->insert(&float_val);
            });
            break;
        }
        case TYPE_DOUBLE: {
            batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column,
                                       ObjectPool* pool) {
                double double_val = column.doubleval();
                set->insert(&double_val);
            });
            break;
        }
        case TYPE_DATEV2: {
            batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column,
                                       ObjectPool* pool) {
                auto date_v2_val = column.intval();
                set->insert(&date_v2_val);
            });
            break;
        }
        case TYPE_DATETIMEV2: {
            batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column,
                                       ObjectPool* pool) {
                auto date_v2_val = column.longval();
                set->insert(&date_v2_val);
            });
            break;
        }
        case TYPE_DATETIME:
        case TYPE_DATE: {
            batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column,
                                       ObjectPool* pool) {
                auto& string_val_ref = column.stringval();
                VecDateTimeValue datetime_val;
                datetime_val.from_date_str(string_val_ref.c_str(), string_val_ref.length());
                set->insert(&datetime_val);
            });
            break;
        }
        case TYPE_DECIMALV2: {
            batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column,
                                       ObjectPool* pool) {
                auto& string_val_ref = column.stringval();
                DecimalV2Value decimal_val(string_val_ref);
                set->insert(&decimal_val);
            });
            break;
        }
        case TYPE_DECIMAL32: {
            batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column,
                                       ObjectPool* pool) {
                int32_t decimal_32_val = column.intval();
                set->insert(&decimal_32_val);
            });
            break;
        }
        case TYPE_DECIMAL64: {
            batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column,
                                       ObjectPool* pool) {
                int64_t decimal_64_val = column.longval();
                set->insert(&decimal_64_val);
            });
            break;
        }
        case TYPE_DECIMAL128I: {
            batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column,
                                       ObjectPool* pool) {
                auto string_val = column.stringval();
                StringParser::ParseResult result;
                int128_t int128_val = StringParser::string_to_int<int128_t>(
                        string_val.c_str(), string_val.length(), &result);
                DCHECK(result == StringParser::PARSE_SUCCESS);
                set->insert(&int128_val);
            });
            break;
        }
        case TYPE_DECIMAL256: {
            batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column,
                                       ObjectPool* pool) {
                auto string_val = column.stringval();
                StringParser::ParseResult result;
                auto int_val = StringParser::string_to_int<wide::Int256>(
                        string_val.c_str(), string_val.length(), &result);
                DCHECK(result == StringParser::PARSE_SUCCESS);
                set->insert(&int_val);
            });
            break;
        }
        case TYPE_VARCHAR:
        case TYPE_CHAR:
        case TYPE_STRING: {
            batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column,
                                       ObjectPool* pool) {
                auto& string_val_ref = column.stringval();
                auto val_ptr = pool->add(new std::string(string_val_ref));
                StringRef string_val(val_ptr->c_str(), val_ptr->length());
                set->insert(&string_val);
            });
            break;
        }
        default: {
            DCHECK(false) << "unknown type: " << type_to_string(type);
            return Status::InvalidArgument("not support assign to in filter, type: " +
                                           type_to_string(type));
        }
        }
        return Status::OK();
    }

    // used by shuffle runtime filter
    // assign this filter by protobuf
    Status assign(const PBloomFilter* bloom_filter, butil::IOBufAsZeroCopyInputStream* data) {
        _is_bloomfilter = true;
        // we won't use this class to insert or find any data
        // so any type is ok
        _context.bloom_filter_func.reset(create_bloom_filter(PrimitiveType::TYPE_INT));
        return _context.bloom_filter_func->assign(data, bloom_filter->filter_length());
    }

    // used by shuffle runtime filter
    // assign this filter by protobuf
    Status assign(const PMinMaxFilter* minmax_filter) {
        PrimitiveType type = to_primitive_type(minmax_filter->column_type());
        _context.minmax_func.reset(create_minmax_filter(type));
        switch (type) {
        case TYPE_BOOLEAN: {
            bool min_val = minmax_filter->min_val().boolval();
            bool max_val = minmax_filter->max_val().boolval();
            return _context.minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_TINYINT: {
            int8_t min_val = static_cast<int8_t>(minmax_filter->min_val().intval());
            int8_t max_val = static_cast<int8_t>(minmax_filter->max_val().intval());
            return _context.minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_SMALLINT: {
            int16_t min_val = static_cast<int16_t>(minmax_filter->min_val().intval());
            int16_t max_val = static_cast<int16_t>(minmax_filter->max_val().intval());
            return _context.minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_INT: {
            int32_t min_val = minmax_filter->min_val().intval();
            int32_t max_val = minmax_filter->max_val().intval();
            return _context.minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_BIGINT: {
            int64_t min_val = minmax_filter->min_val().longval();
            int64_t max_val = minmax_filter->max_val().longval();
            return _context.minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_LARGEINT: {
            auto min_string_val = minmax_filter->min_val().stringval();
            auto max_string_val = minmax_filter->max_val().stringval();
            StringParser::ParseResult result;
            int128_t min_val = StringParser::string_to_int<int128_t>(
                    min_string_val.c_str(), min_string_val.length(), &result);
            DCHECK(result == StringParser::PARSE_SUCCESS);
            int128_t max_val = StringParser::string_to_int<int128_t>(
                    max_string_val.c_str(), max_string_val.length(), &result);
            DCHECK(result == StringParser::PARSE_SUCCESS);
            return _context.minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_FLOAT: {
            float min_val = static_cast<float>(minmax_filter->min_val().doubleval());
            float max_val = static_cast<float>(minmax_filter->max_val().doubleval());
            return _context.minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_DOUBLE: {
            double min_val = static_cast<double>(minmax_filter->min_val().doubleval());
            double max_val = static_cast<double>(minmax_filter->max_val().doubleval());
            return _context.minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_DATEV2: {
            int32_t min_val = minmax_filter->min_val().intval();
            int32_t max_val = minmax_filter->max_val().intval();
            return _context.minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_DATETIMEV2: {
            int64_t min_val = minmax_filter->min_val().longval();
            int64_t max_val = minmax_filter->max_val().longval();
            return _context.minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_DATETIME:
        case TYPE_DATE: {
            auto& min_val_ref = minmax_filter->min_val().stringval();
            auto& max_val_ref = minmax_filter->max_val().stringval();
            VecDateTimeValue min_val;
            VecDateTimeValue max_val;
            min_val.from_date_str(min_val_ref.c_str(), min_val_ref.length());
            max_val.from_date_str(max_val_ref.c_str(), max_val_ref.length());
            return _context.minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_DECIMALV2: {
            auto& min_val_ref = minmax_filter->min_val().stringval();
            auto& max_val_ref = minmax_filter->max_val().stringval();
            DecimalV2Value min_val(min_val_ref);
            DecimalV2Value max_val(max_val_ref);
            return _context.minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_DECIMAL32: {
            int32_t min_val = minmax_filter->min_val().intval();
            int32_t max_val = minmax_filter->max_val().intval();
            return _context.minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_DECIMAL64: {
            int64_t min_val = minmax_filter->min_val().longval();
            int64_t max_val = minmax_filter->max_val().longval();
            return _context.minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_DECIMAL128I: {
            auto min_string_val = minmax_filter->min_val().stringval();
            auto max_string_val = minmax_filter->max_val().stringval();
            StringParser::ParseResult result;
            int128_t min_val = StringParser::string_to_int<int128_t>(
                    min_string_val.c_str(), min_string_val.length(), &result);
            DCHECK(result == StringParser::PARSE_SUCCESS);
            int128_t max_val = StringParser::string_to_int<int128_t>(
                    max_string_val.c_str(), max_string_val.length(), &result);
            DCHECK(result == StringParser::PARSE_SUCCESS);
            return _context.minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_DECIMAL256: {
            auto min_string_val = minmax_filter->min_val().stringval();
            auto max_string_val = minmax_filter->max_val().stringval();
            StringParser::ParseResult result;
            auto min_val = StringParser::string_to_int<wide::Int256>(
                    min_string_val.c_str(), min_string_val.length(), &result);
            DCHECK(result == StringParser::PARSE_SUCCESS);
            auto max_val = StringParser::string_to_int<wide::Int256>(
                    max_string_val.c_str(), max_string_val.length(), &result);
            DCHECK(result == StringParser::PARSE_SUCCESS);
            return _context.minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_VARCHAR:
        case TYPE_CHAR:
        case TYPE_STRING: {
            auto& min_val_ref = minmax_filter->min_val().stringval();
            auto& max_val_ref = minmax_filter->max_val().stringval();
            auto min_val_ptr = _pool->add(new std::string(min_val_ref));
            auto max_val_ptr = _pool->add(new std::string(max_val_ref));
            StringRef min_val(min_val_ptr->c_str(), min_val_ptr->length());
            StringRef max_val(max_val_ptr->c_str(), max_val_ptr->length());
            return _context.minmax_func->assign(&min_val, &max_val);
        }
        default:
            DCHECK(false) << "unknown type";
            break;
        }
        return Status::InvalidArgument("not support!");
    }

    Status get_in_filter_iterator(HybridSetBase::IteratorBase** it) {
        *it = _context.hybrid_set->begin();
        return Status::OK();
    }

    Status get_bloom_filter_desc(char** data, int* filter_length) {
        return _context.bloom_filter_func->get_data(data, filter_length);
    }

    Status get_minmax_filter_desc(void** min_data, void** max_data) {
        *min_data = _context.minmax_func->get_min();
        *max_data = _context.minmax_func->get_max();
        return Status::OK();
    }

    PrimitiveType column_type() { return _column_return_type; }

    bool is_bloomfilter() const { return _is_bloomfilter; }

    bool is_ignored_in_filter() const { return _is_ignored_in_filter; }

    std::string* get_ignored_in_filter_msg() const { return _ignored_in_filter_msg; }

    void batch_assign(const PInFilter* filter,
                      void (*assign_func)(std::shared_ptr<HybridSetBase>& _hybrid_set,
                                          PColumnValue&, ObjectPool*)) {
        for (int i = 0; i < filter->values_size(); ++i) {
            PColumnValue column = filter->values(i);
            assign_func(_context.hybrid_set, column, _pool);
        }
    }

    size_t get_in_filter_size() const { return _context.hybrid_set->size(); }

    std::shared_ptr<BitmapFilterFuncBase> get_bitmap_filter() const {
        return _context.bitmap_filter_func;
    }

    friend class IRuntimeFilter;

    void set_filter_id(int id) {
        if (_context.bloom_filter_func) {
            _context.bloom_filter_func->set_filter_id(id);
        }
        if (_context.bitmap_filter_func) {
            _context.bitmap_filter_func->set_filter_id(id);
        }
        if (_context.hybrid_set) {
            _context.hybrid_set->set_filter_id(id);
        }
    }

private:
    RuntimeState* _state;
    QueryContext* _query_ctx;
    int _be_exec_version;
    ObjectPool* _pool;

    // When a runtime filter received from remote and it is a bloom filter, _column_return_type will be invalid.
    PrimitiveType _column_return_type; // column type
    RuntimeFilterType _filter_type;
    int32_t _max_in_num = -1;

    vectorized::SharedRuntimeFilterContext _context;
    bool _is_bloomfilter = false;
    bool _is_ignored_in_filter = false;
    std::string* _ignored_in_filter_msg = nullptr;
    uint32_t _filter_id;

    // When _column_return_type is invalid, _use_batch will be always false.
    bool _use_batch;

    // When _use_new_hash is set to true, use the new hash method.
    // This is only to be used if the be_exec_version may be less than 2. If updated, please delete it.
    const bool _use_new_hash;
};

Status IRuntimeFilter::create(RuntimeState* state, ObjectPool* pool, const TRuntimeFilterDesc* desc,
                              const TQueryOptions* query_options, const RuntimeFilterRole role,
                              int node_id, IRuntimeFilter** res, bool build_bf_exactly) {
    *res = pool->add(new IRuntimeFilter(state, pool, desc));
    (*res)->set_role(role);
    return (*res)->init_with_desc(desc, query_options, node_id, build_bf_exactly);
}

Status IRuntimeFilter::create(QueryContext* query_ctx, ObjectPool* pool,
                              const TRuntimeFilterDesc* desc, const TQueryOptions* query_options,
                              const RuntimeFilterRole role, int node_id, IRuntimeFilter** res,
                              bool build_bf_exactly) {
    *res = pool->add(new IRuntimeFilter(query_ctx, pool, desc));
    (*res)->set_role(role);
    return (*res)->init_with_desc(desc, query_options, node_id, build_bf_exactly);
}

void IRuntimeFilter::copy_to_shared_context(vectorized::SharedRuntimeFilterContext& context) {
    context = _wrapper->_context;
}

Status IRuntimeFilter::copy_from_shared_context(vectorized::SharedRuntimeFilterContext& context) {
    _wrapper->_context = context;
    return Status::OK();
}

void IRuntimeFilter::copy_from_other(IRuntimeFilter* other) {
    _wrapper->_filter_type = other->_wrapper->_filter_type;
    _wrapper->_is_bloomfilter = other->is_bloomfilter();
    _wrapper->_context = other->_wrapper->_context;
}

void IRuntimeFilter::insert(const void* data) {
    DCHECK(is_producer());
    if (!_is_ignored) {
        _wrapper->insert(data);
    }
}

void IRuntimeFilter::insert(const StringRef& value) {
    DCHECK(is_producer());
    _wrapper->insert(value);
}

void IRuntimeFilter::insert_batch(const vectorized::ColumnPtr column,
                                  const std::vector<int>& rows) {
    DCHECK(is_producer());
    _wrapper->insert_batch(column, rows);
}

Status IRuntimeFilter::publish() {
    DCHECK(is_producer());
    if (_has_local_target) {
        std::vector<IRuntimeFilter*> filters;
        RETURN_IF_ERROR(_state->runtime_filter_mgr()->get_consume_filters(_filter_id, filters));
        // push down
        for (auto filter : filters) {
            filter->_wrapper = _wrapper;
            filter->update_runtime_filter_type_to_profile();
            filter->signal();
        }
        return Status::OK();
    } else {
        TNetworkAddress addr;
        DCHECK(_state != nullptr);
        RETURN_IF_ERROR(_state->runtime_filter_mgr()->get_merge_addr(&addr));
        return push_to_remote(_state, &addr, _opt_remote_rf);
    }
}

Status IRuntimeFilter::get_push_expr_ctxs(std::list<vectorized::VExprContextSPtr>& probe_ctxs,
                                          std::vector<vectorized::VExprSPtr>& push_exprs,
                                          bool is_late_arrival) {
    DCHECK(is_consumer());
    if (_is_ignored) {
        return Status::OK();
    }
    if (!is_late_arrival) {
        _set_push_down();
    }
    _profile->add_info_string("Info", _format_status());
    return _wrapper->get_push_exprs(probe_ctxs, push_exprs, _probe_expr);
}

bool IRuntimeFilter::await() {
    DCHECK(is_consumer());
    auto execution_timeout = _state == nullptr ? _query_ctx->execution_timeout() * 1000
                                               : _state->execution_timeout() * 1000;
    auto runtime_filter_wait_time_ms = _state == nullptr ? _query_ctx->runtime_filter_wait_time_ms()
                                                         : _state->runtime_filter_wait_time_ms();
    // bitmap filter is precise filter and only filter once, so it must be applied.
    int64_t wait_times_ms = _wrapper->get_real_type() == RuntimeFilterType::BITMAP_FILTER
                                    ? execution_timeout
                                    : runtime_filter_wait_time_ms;
    if (_enable_pipeline_exec) {
        auto expected = _rf_state_atomic.load(std::memory_order_acquire);
        if (expected == RuntimeFilterState::NOT_READY) {
            if (!_rf_state_atomic.compare_exchange_strong(
                        expected,
                        MonotonicMillis() - registration_time_ >= wait_times_ms
                                ? RuntimeFilterState::TIME_OUT
                                : RuntimeFilterState::NOT_READY,
                        std::memory_order_acq_rel)) {
                DCHECK(expected == RuntimeFilterState::READY ||
                       expected == RuntimeFilterState::TIME_OUT);
                return (expected == RuntimeFilterState::READY);
            }
            return false;
        } else if (expected == RuntimeFilterState::TIME_OUT) {
            return false;
        }
    } else {
        std::unique_lock lock(_inner_mutex);
        if (_rf_state != RuntimeFilterState::READY) {
            int64_t ms_since_registration = MonotonicMillis() - registration_time_;
            int64_t ms_remaining = wait_times_ms - ms_since_registration;
            _rf_state = RuntimeFilterState::TIME_OUT;
            if (ms_remaining <= 0) {
                return false;
            }
#if !defined(USE_BTHREAD_SCANNER)
            return _inner_cv.wait_for(lock, std::chrono::milliseconds(ms_remaining),
                                      [this] { return _rf_state == RuntimeFilterState::READY; });
#else
            auto timeout_ms = butil::milliseconds_from_now(ms_remaining);
            while (_rf_state != RuntimeFilterState::READY) {
                if (_inner_cv.wait_until(lock, timeout_ms) != 0) {
                    // timeout
                    return _rf_state == RuntimeFilterState::READY;
                }
            }
#endif
        }
    }
    return true;
}

bool IRuntimeFilter::is_ready_or_timeout() {
    DCHECK(is_consumer());
    auto cur_state = _rf_state_atomic.load(std::memory_order_acquire);
    auto execution_timeout = _state == nullptr ? _query_ctx->execution_timeout() * 1000
                                               : _state->execution_timeout() * 1000;
    auto runtime_filter_wait_time_ms = _state == nullptr ? _query_ctx->runtime_filter_wait_time_ms()
                                                         : _state->runtime_filter_wait_time_ms();
    // bitmap filter is precise filter and only filter once, so it must be applied.
    int64_t wait_times_ms = _wrapper->get_real_type() == RuntimeFilterType::BITMAP_FILTER
                                    ? execution_timeout
                                    : runtime_filter_wait_time_ms;
    int64_t ms_since_registration = MonotonicMillis() - registration_time_;
    if (!_enable_pipeline_exec) {
        _rf_state = RuntimeFilterState::TIME_OUT;
        return true;
    } else if (is_ready()) {
        if (cur_state == RuntimeFilterState::NOT_READY) {
            _profile->add_info_string("EffectTime", std::to_string(ms_since_registration) + " ms");
        }
        return true;
    } else {
        if (cur_state == RuntimeFilterState::NOT_READY) {
            _profile->add_info_string("EffectTime", std::to_string(ms_since_registration) + " ms");
        }
        if (is_ready()) {
            return true;
        }
        bool timeout = wait_times_ms <= ms_since_registration;
        auto expected = RuntimeFilterState::NOT_READY;
        if (timeout) {
            if (!_rf_state_atomic.compare_exchange_strong(expected, RuntimeFilterState::TIME_OUT,
                                                          std::memory_order_acq_rel)) {
                DCHECK(expected == RuntimeFilterState::READY ||
                       expected == RuntimeFilterState::TIME_OUT);
                return true;
            }
            return true;
        }
        if (!_rf_state_atomic.compare_exchange_strong(expected, RuntimeFilterState::NOT_READY,
                                                      std::memory_order_acq_rel)) {
            return true;
        }
        return false;
    }
}

void IRuntimeFilter::signal() {
    DCHECK(is_consumer());
    if (_enable_pipeline_exec) {
        _rf_state_atomic.store(RuntimeFilterState::READY);
        if (_dependency) {
            _dependency->set_filter_ready();
        }
    } else {
        std::unique_lock lock(_inner_mutex);
        _rf_state = RuntimeFilterState::READY;
        _inner_cv.notify_all();
    }

    if (_wrapper->get_real_type() == RuntimeFilterType::IN_FILTER) {
        _profile->add_info_string("InFilterSize", std::to_string(_wrapper->get_in_filter_size()));
    }
    if (_wrapper->get_real_type() == RuntimeFilterType::BITMAP_FILTER) {
        auto bitmap_filter = _wrapper->get_bitmap_filter();
        _profile->add_info_string("BitmapSize", std::to_string(bitmap_filter->size()));
        _profile->add_info_string("IsNotIn", bitmap_filter->is_not_in() ? "true" : "false");
    }
    if (_wrapper->get_real_type() == RuntimeFilterType::BLOOM_FILTER) {
        _profile->add_info_string("BloomFilterSize",
                                  std::to_string(_wrapper->get_bloom_filter_size()));
    }
}

void IRuntimeFilter::set_dependency(std::shared_ptr<pipeline::FilterDependency> dependency) {
    _dependency = dependency;
}

BloomFilterFuncBase* IRuntimeFilter::get_bloomfilter() const {
    return _wrapper->get_bloomfilter();
}

Status IRuntimeFilter::init_with_desc(const TRuntimeFilterDesc* desc, const TQueryOptions* options,
                                      int node_id, bool build_bf_exactly) {
    // if node_id == -1 , it shouldn't be a consumer
    DCHECK(node_id >= 0 || (node_id == -1 && !is_consumer()));

    _is_broadcast_join = desc->is_broadcast_join;
    _has_local_target = desc->has_local_targets;
    _has_remote_target = desc->has_remote_targets;
    _expr_order = desc->expr_order;
    _opt_remote_rf = desc->__isset.opt_remote_rf && desc->opt_remote_rf;
    vectorized::VExprContextSPtr build_ctx;
    RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(desc->src_expr, build_ctx));

    RuntimeFilterParams params;
    params.filter_id = _filter_id;
    params.filter_type = _runtime_filter_type;
    params.column_return_type = build_ctx->root()->type().type;
    params.max_in_num = options->runtime_filter_max_in_num;
    // We build runtime filter by exact distinct count iff three conditions are met:
    // 1. Only 1 join key
    // 2. Do not have remote target (e.g. do not need to merge)
    // 3. Bloom filter
    params.build_bf_exactly = build_bf_exactly && !_has_remote_target &&
                              (_runtime_filter_type == RuntimeFilterType::BLOOM_FILTER ||
                               _runtime_filter_type == RuntimeFilterType::IN_OR_BLOOM_FILTER);
    if (desc->__isset.bloom_filter_size_bytes) {
        params.bloom_filter_size = desc->bloom_filter_size_bytes;
    }
    if (_runtime_filter_type == RuntimeFilterType::BITMAP_FILTER) {
        if (!build_ctx->root()->type().is_bitmap_type()) {
            return Status::InvalidArgument("Unexpected src expr type:{} for bitmap filter.",
                                           build_ctx->root()->type().debug_string());
        }
        if (!desc->__isset.bitmap_target_expr) {
            return Status::InvalidArgument("Unknown bitmap filter target expr.");
        }
        vectorized::VExprContextSPtr bitmap_target_ctx;
        RETURN_IF_ERROR(
                vectorized::VExpr::create_expr_tree(desc->bitmap_target_expr, bitmap_target_ctx));
        params.column_return_type = bitmap_target_ctx->root()->type().type;

        if (desc->__isset.bitmap_filter_not_in) {
            params.bitmap_filter_not_in = desc->bitmap_filter_not_in;
        }
    }

    if (node_id >= 0) {
        DCHECK(is_consumer());
        const auto iter = desc->planId_to_target_expr.find(node_id);
        if (iter == desc->planId_to_target_expr.end()) {
            return Status::InternalError("not found a node id:{}", node_id);
        }
        _probe_expr = iter->second;
    }

    if (_state) {
        _wrapper = _pool->add(new RuntimePredicateWrapper(_state, _pool, &params));
    } else {
        _wrapper = _pool->add(new RuntimePredicateWrapper(_query_ctx, _pool, &params));
    }
    return _wrapper->init(&params);
}

Status IRuntimeFilter::serialize(PMergeFilterRequest* request, void** data, int* len) {
    return serialize_impl(request, data, len);
}

Status IRuntimeFilter::serialize(PPublishFilterRequest* request, void** data, int* len) {
    return serialize_impl(request, data, len);
}

Status IRuntimeFilter::serialize(PPublishFilterRequestV2* request, void** data, int* len) {
    return serialize_impl(request, data, len);
}

Status IRuntimeFilter::create_wrapper(RuntimeState* state, const MergeRuntimeFilterParams* param,
                                      ObjectPool* pool,
                                      std::unique_ptr<RuntimePredicateWrapper>* wrapper) {
    return _create_wrapper(state, param, pool, wrapper);
}

Status IRuntimeFilter::create_wrapper(RuntimeState* state, const UpdateRuntimeFilterParams* param,
                                      ObjectPool* pool,
                                      std::unique_ptr<RuntimePredicateWrapper>* wrapper) {
    return _create_wrapper(state, param, pool, wrapper);
}

Status IRuntimeFilter::create_wrapper(QueryContext* query_ctx,
                                      const UpdateRuntimeFilterParamsV2* param, ObjectPool* pool,
                                      std::unique_ptr<RuntimePredicateWrapper>* wrapper) {
    int filter_type = param->request->filter_type();
    PrimitiveType column_type = PrimitiveType::INVALID_TYPE;
    if (param->request->has_in_filter()) {
        column_type = to_primitive_type(param->request->in_filter().column_type());
    }
    wrapper->reset(new RuntimePredicateWrapper(query_ctx, pool, column_type, get_type(filter_type),
                                               param->request->filter_id()));

    switch (filter_type) {
    case PFilterType::IN_FILTER: {
        DCHECK(param->request->has_in_filter());
        return (*wrapper)->assign(&param->request->in_filter());
    }
    case PFilterType::BLOOM_FILTER: {
        DCHECK(param->request->has_bloom_filter());
        return (*wrapper)->assign(&param->request->bloom_filter(), param->data);
    }
    case PFilterType::MIN_FILTER:
    case PFilterType::MAX_FILTER:
    case PFilterType::MINMAX_FILTER: {
        DCHECK(param->request->has_minmax_filter());
        return (*wrapper)->assign(&param->request->minmax_filter());
    }
    default:
        return Status::InvalidArgument("unknown filter type");
    }
}

void IRuntimeFilter::change_to_bloom_filter() {
    auto origin_type = _wrapper->get_real_type();
    _wrapper->change_to_bloom_filter();
    if (origin_type != _wrapper->get_real_type()) {
        update_runtime_filter_type_to_profile();
    }
}

Status IRuntimeFilter::init_bloom_filter(const size_t build_bf_cardinality) {
    return _wrapper->init_bloom_filter(build_bf_cardinality);
}

template <class T>
Status IRuntimeFilter::_create_wrapper(RuntimeState* state, const T* param, ObjectPool* pool,
                                       std::unique_ptr<RuntimePredicateWrapper>* wrapper) {
    int filter_type = param->request->filter_type();
    PrimitiveType column_type = PrimitiveType::INVALID_TYPE;
    if (param->request->has_in_filter()) {
        column_type = to_primitive_type(param->request->in_filter().column_type());
    }
    wrapper->reset(new RuntimePredicateWrapper(state, pool, column_type, get_type(filter_type),
                                               param->request->filter_id()));

    switch (filter_type) {
    case PFilterType::IN_FILTER: {
        DCHECK(param->request->has_in_filter());
        return (*wrapper)->assign(&param->request->in_filter());
    }
    case PFilterType::BLOOM_FILTER: {
        DCHECK(param->request->has_bloom_filter());
        return (*wrapper)->assign(&param->request->bloom_filter(), param->data);
    }
    case PFilterType::MIN_FILTER:
    case PFilterType::MAX_FILTER:
    case PFilterType::MINMAX_FILTER: {
        DCHECK(param->request->has_minmax_filter());
        return (*wrapper)->assign(&param->request->minmax_filter());
    }
    default:
        return Status::InvalidArgument("unknown filter type");
    }
}

void IRuntimeFilter::init_profile(RuntimeProfile* parent_profile) {
    if (_profile_init) {
        parent_profile->add_child(_profile.get(), true, nullptr);
        return;
    }
    _profile_init = true;
    parent_profile->add_child(_profile.get(), true, nullptr);
    _profile->add_info_string("Info", _format_status());
    if (_runtime_filter_type == RuntimeFilterType::IN_OR_BLOOM_FILTER) {
        update_runtime_filter_type_to_profile();
    }
}

void IRuntimeFilter::update_runtime_filter_type_to_profile() {
    if (_profile != nullptr) {
        _profile->add_info_string("RealRuntimeFilterType", to_string(_wrapper->get_real_type()));
    }
}

Status IRuntimeFilter::merge_from(const RuntimePredicateWrapper* wrapper) {
    if (!_is_ignored && wrapper->is_ignored_in_filter()) {
        set_ignored();
        set_ignored_msg(*(wrapper->get_ignored_in_filter_msg()));
    }
    auto origin_type = _wrapper->get_real_type();
    Status status = _wrapper->merge(wrapper);
    if (!_is_ignored && _wrapper->is_ignored_in_filter()) {
        set_ignored();
        set_ignored_msg(*(_wrapper->get_ignored_in_filter_msg()));
    }
    if (origin_type != _wrapper->get_real_type()) {
        update_runtime_filter_type_to_profile();
    }
    return status;
}

template <typename T>
void batch_copy(PInFilter* filter, HybridSetBase::IteratorBase* it,
                void (*set_func)(PColumnValue*, const T*)) {
    while (it->has_next()) {
        const void* void_value = it->get_value();
        auto origin_value = reinterpret_cast<const T*>(void_value);
        set_func(filter->add_values(), origin_value);
        it->next();
    }
}

template <class T>
Status IRuntimeFilter::serialize_impl(T* request, void** data, int* len) {
    auto real_runtime_filter_type = _runtime_filter_type;
    if (real_runtime_filter_type == RuntimeFilterType::IN_OR_BLOOM_FILTER) {
        real_runtime_filter_type = _wrapper->is_bloomfilter() ? RuntimeFilterType::BLOOM_FILTER
                                                              : RuntimeFilterType::IN_FILTER;
    }

    request->set_filter_type(get_type(real_runtime_filter_type));

    if (real_runtime_filter_type == RuntimeFilterType::IN_FILTER) {
        auto in_filter = request->mutable_in_filter();
        to_protobuf(in_filter);
    } else if (real_runtime_filter_type == RuntimeFilterType::BLOOM_FILTER) {
        RETURN_IF_ERROR(_wrapper->get_bloom_filter_desc((char**)data, len));
        DCHECK(data != nullptr);
        request->mutable_bloom_filter()->set_filter_length(*len);
        request->mutable_bloom_filter()->set_always_true(false);
    } else if (real_runtime_filter_type == RuntimeFilterType::MINMAX_FILTER ||
               real_runtime_filter_type == RuntimeFilterType::MIN_FILTER ||
               real_runtime_filter_type == RuntimeFilterType::MAX_FILTER) {
        auto minmax_filter = request->mutable_minmax_filter();
        to_protobuf(minmax_filter);
    } else {
        return Status::InvalidArgument("not implemented !");
    }
    return Status::OK();
}

void IRuntimeFilter::to_protobuf(PInFilter* filter) {
    auto column_type = _wrapper->column_type();
    filter->set_column_type(to_proto(column_type));

    if (_is_ignored) {
        filter->set_ignored_msg(_ignored_msg);
        return;
    }

    HybridSetBase::IteratorBase* it;
    static_cast<void>(_wrapper->get_in_filter_iterator(&it));
    DCHECK(it != nullptr);

    switch (column_type) {
    case TYPE_BOOLEAN: {
        batch_copy<bool>(filter, it, [](PColumnValue* column, const bool* value) {
            column->set_boolval(*value);
        });
        return;
    }
    case TYPE_TINYINT: {
        batch_copy<int8_t>(filter, it, [](PColumnValue* column, const int8_t* value) {
            column->set_intval(*value);
        });
        return;
    }
    case TYPE_SMALLINT: {
        batch_copy<int16_t>(filter, it, [](PColumnValue* column, const int16_t* value) {
            column->set_intval(*value);
        });
        return;
    }
    case TYPE_INT: {
        batch_copy<int32_t>(filter, it, [](PColumnValue* column, const int32_t* value) {
            column->set_intval(*value);
        });
        return;
    }
    case TYPE_BIGINT: {
        batch_copy<int64_t>(filter, it, [](PColumnValue* column, const int64_t* value) {
            column->set_longval(*value);
        });
        return;
    }
    case TYPE_LARGEINT: {
        batch_copy<int128_t>(filter, it, [](PColumnValue* column, const int128_t* value) {
            column->set_stringval(LargeIntValue::to_string(*value));
        });
        return;
    }
    case TYPE_FLOAT: {
        batch_copy<float>(filter, it, [](PColumnValue* column, const float* value) {
            column->set_doubleval(*value);
        });
        return;
    }
    case TYPE_DOUBLE: {
        batch_copy<double>(filter, it, [](PColumnValue* column, const double* value) {
            column->set_doubleval(*value);
        });
        return;
    }
    case TYPE_DATEV2: {
        batch_copy<DateV2Value<DateV2ValueType>>(
                filter, it, [](PColumnValue* column, const DateV2Value<DateV2ValueType>* value) {
                    column->set_intval(*reinterpret_cast<const int32_t*>(value));
                });
        return;
    }
    case TYPE_DATETIMEV2: {
        batch_copy<DateV2Value<DateTimeV2ValueType>>(
                filter, it,
                [](PColumnValue* column, const DateV2Value<DateTimeV2ValueType>* value) {
                    column->set_longval(*reinterpret_cast<const int64_t*>(value));
                });
        return;
    }
    case TYPE_DATE:
    case TYPE_DATETIME: {
        batch_copy<VecDateTimeValue>(filter, it,
                                     [](PColumnValue* column, const VecDateTimeValue* value) {
                                         char convert_buffer[30];
                                         value->to_string(convert_buffer);
                                         column->set_stringval(convert_buffer);
                                     });
        return;
    }
    case TYPE_DECIMALV2: {
        batch_copy<DecimalV2Value>(filter, it,
                                   [](PColumnValue* column, const DecimalV2Value* value) {
                                       column->set_stringval(value->to_string());
                                   });
        return;
    }
    case TYPE_DECIMAL32: {
        batch_copy<int32_t>(filter, it, [](PColumnValue* column, const int32_t* value) {
            column->set_intval(*value);
        });
        return;
    }
    case TYPE_DECIMAL64: {
        batch_copy<int64_t>(filter, it, [](PColumnValue* column, const int64_t* value) {
            column->set_longval(*value);
        });
        return;
    }
    case TYPE_DECIMAL128I: {
        batch_copy<int128_t>(filter, it, [](PColumnValue* column, const int128_t* value) {
            column->set_stringval(LargeIntValue::to_string(*value));
        });
        return;
    }
    case TYPE_DECIMAL256: {
        batch_copy<wide::Int256>(filter, it, [](PColumnValue* column, const wide::Int256* value) {
            column->set_stringval(wide::to_string(*value));
        });
        return;
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING: {
        batch_copy<StringRef>(filter, it, [](PColumnValue* column, const StringRef* value) {
            column->set_stringval(std::string(value->data, value->size));
        });
        return;
    }
    default: {
        DCHECK(false) << "unknown type";
        break;
    }
    }
}

void IRuntimeFilter::to_protobuf(PMinMaxFilter* filter) {
    void* min_data = nullptr;
    void* max_data = nullptr;
    static_cast<void>(_wrapper->get_minmax_filter_desc(&min_data, &max_data));
    DCHECK(min_data != nullptr && max_data != nullptr);
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
    case TYPE_DATEV2: {
        filter->mutable_min_val()->set_intval(*reinterpret_cast<const int32_t*>(min_data));
        filter->mutable_max_val()->set_intval(*reinterpret_cast<const int32_t*>(max_data));
        return;
    }
    case TYPE_DATETIMEV2: {
        filter->mutable_min_val()->set_longval(*reinterpret_cast<const int64_t*>(min_data));
        filter->mutable_max_val()->set_longval(*reinterpret_cast<const int64_t*>(max_data));
        return;
    }
    case TYPE_DATE:
    case TYPE_DATETIME: {
        char convert_buffer[30];
        reinterpret_cast<const VecDateTimeValue*>(min_data)->to_string(convert_buffer);
        filter->mutable_min_val()->set_stringval(convert_buffer);
        reinterpret_cast<const VecDateTimeValue*>(max_data)->to_string(convert_buffer);
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
    case TYPE_DECIMAL32: {
        filter->mutable_min_val()->set_intval(*reinterpret_cast<const int32_t*>(min_data));
        filter->mutable_max_val()->set_intval(*reinterpret_cast<const int32_t*>(max_data));
        return;
    }
    case TYPE_DECIMAL64: {
        filter->mutable_min_val()->set_longval(*reinterpret_cast<const int64_t*>(min_data));
        filter->mutable_max_val()->set_longval(*reinterpret_cast<const int64_t*>(max_data));
        return;
    }
    case TYPE_DECIMAL128I: {
        filter->mutable_min_val()->set_stringval(
                LargeIntValue::to_string(*reinterpret_cast<const int128_t*>(min_data)));
        filter->mutable_max_val()->set_stringval(
                LargeIntValue::to_string(*reinterpret_cast<const int128_t*>(max_data)));
        return;
    }
    case TYPE_DECIMAL256: {
        filter->mutable_min_val()->set_stringval(
                wide::to_string(*reinterpret_cast<const wide::Int256*>(min_data)));
        filter->mutable_max_val()->set_stringval(
                wide::to_string(*reinterpret_cast<const wide::Int256*>(max_data)));
        return;
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING: {
        const StringRef* min_string_value = reinterpret_cast<const StringRef*>(min_data);
        filter->mutable_min_val()->set_stringval(
                std::string(min_string_value->data, min_string_value->size));
        const StringRef* max_string_value = reinterpret_cast<const StringRef*>(max_data);
        filter->mutable_max_val()->set_stringval(
                std::string(max_string_value->data, max_string_value->size));
        break;
    }
    default: {
        DCHECK(false) << "unknown type";
        break;
    }
    }
}

bool IRuntimeFilter::is_bloomfilter() {
    return _wrapper->is_bloomfilter();
}

Status IRuntimeFilter::update_filter(const UpdateRuntimeFilterParams* param) {
    if (param->request->has_in_filter() && param->request->in_filter().has_ignored_msg()) {
        set_ignored();
        const PInFilter in_filter = param->request->in_filter();
        auto msg = param->pool->add(new std::string(in_filter.ignored_msg()));
        set_ignored_msg(*msg);
    }
    std::unique_ptr<RuntimePredicateWrapper> wrapper;
    RETURN_IF_ERROR(IRuntimeFilter::create_wrapper(_state, param, _pool, &wrapper));
    auto origin_type = _wrapper->get_real_type();
    RETURN_IF_ERROR(_wrapper->merge(wrapper.get()));
    if (origin_type != _wrapper->get_real_type()) {
        update_runtime_filter_type_to_profile();
    }
    this->signal();

    _profile->add_info_string("MergeTime", std::to_string(param->request->merge_time()) + " ms");
    return Status::OK();
}

Status IRuntimeFilter::update_filter(const UpdateRuntimeFilterParamsV2* param,
                                     int64_t start_apply) {
    if (param->request->has_in_filter() && param->request->in_filter().has_ignored_msg()) {
        set_ignored();
        const PInFilter in_filter = param->request->in_filter();
        auto msg = param->pool->add(new std::string(in_filter.ignored_msg()));
        set_ignored_msg(*msg);
    }

    std::unique_ptr<RuntimePredicateWrapper> tmp_wrapper;
    RETURN_IF_ERROR(IRuntimeFilter::create_wrapper(_query_ctx, param, _pool, &tmp_wrapper));
    auto origin_type = _wrapper->get_real_type();
    RETURN_IF_ERROR(_wrapper->merge(tmp_wrapper.get()));
    if (origin_type != _wrapper->get_real_type()) {
        update_runtime_filter_type_to_profile();
    }
    this->signal();

    _profile->add_info_string("MergeTime", std::to_string(param->request->merge_time()) + " ms");
    _profile->add_info_string("UpdateTime",
                              std::to_string(MonotonicMillis() - start_apply) + " ms");
    return Status::OK();
}

Status RuntimePredicateWrapper::get_push_exprs(std::list<vectorized::VExprContextSPtr>& probe_ctxs,
                                               std::vector<vectorized::VExprSPtr>& container,
                                               const TExpr& probe_expr) {
    vectorized::VExprContextSPtr probe_ctx;
    RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(probe_expr, probe_ctx));
    probe_ctxs.push_back(probe_ctx);
    set_filter_id(_filter_id);
    DCHECK(probe_ctx->root()->type().type == _column_return_type ||
           (is_string_type(probe_ctx->root()->type().type) &&
            is_string_type(_column_return_type)) ||
           _filter_type == RuntimeFilterType::BITMAP_FILTER)
            << " prob_expr->root()->type().type: " << probe_ctx->root()->type().type
            << " _column_return_type: " << _column_return_type
            << " _filter_type: " << IRuntimeFilter::to_string(_filter_type);

    auto real_filter_type = get_real_type();
    switch (real_filter_type) {
    case RuntimeFilterType::IN_FILTER: {
        if (!_is_ignored_in_filter) {
            TTypeDesc type_desc = create_type_desc(PrimitiveType::TYPE_BOOLEAN);
            type_desc.__set_is_nullable(false);
            TExprNode node;
            node.__set_type(type_desc);
            node.__set_node_type(TExprNodeType::IN_PRED);
            node.in_predicate.__set_is_not_in(false);
            node.__set_opcode(TExprOpcode::FILTER_IN);
            node.__set_is_nullable(false);

            auto in_pred = vectorized::VDirectInPredicate::create_shared(node);
            in_pred->set_filter(_context.hybrid_set);
            in_pred->add_child(probe_ctx->root());
            auto wrapper = vectorized::VRuntimeFilterWrapper::create_shared(node, in_pred);
            container.push_back(wrapper);
        }
        break;
    }
    case RuntimeFilterType::MIN_FILTER: {
        // create min filter
        vectorized::VExprSPtr min_pred;
        TExprNode min_pred_node;
        RETURN_IF_ERROR(create_vbin_predicate(probe_ctx->root()->type(), TExprOpcode::GE, min_pred,
                                              &min_pred_node));
        vectorized::VExprSPtr min_literal;
        RETURN_IF_ERROR(create_literal(probe_ctx->root()->type(), _context.minmax_func->get_min(),
                                       min_literal));
        min_pred->add_child(probe_ctx->root());
        min_pred->add_child(min_literal);
        container.push_back(
                vectorized::VRuntimeFilterWrapper::create_shared(min_pred_node, min_pred));
        vectorized::VExprContextSPtr new_probe_ctx;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(probe_expr, new_probe_ctx));
        probe_ctxs.push_back(new_probe_ctx);
        break;
    }
    case RuntimeFilterType::MAX_FILTER: {
        vectorized::VExprSPtr max_pred;
        // create max filter
        TExprNode max_pred_node;
        RETURN_IF_ERROR(create_vbin_predicate(probe_ctx->root()->type(), TExprOpcode::LE, max_pred,
                                              &max_pred_node));
        vectorized::VExprSPtr max_literal;
        RETURN_IF_ERROR(create_literal(probe_ctx->root()->type(), _context.minmax_func->get_max(),
                                       max_literal));
        max_pred->add_child(probe_ctx->root());
        max_pred->add_child(max_literal);
        container.push_back(
                vectorized::VRuntimeFilterWrapper::create_shared(max_pred_node, max_pred));

        vectorized::VExprContextSPtr new_probe_ctx;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(probe_expr, new_probe_ctx));
        probe_ctxs.push_back(new_probe_ctx);
        break;
    }
    case RuntimeFilterType::MINMAX_FILTER: {
        vectorized::VExprSPtr max_pred;
        // create max filter
        TExprNode max_pred_node;
        RETURN_IF_ERROR(create_vbin_predicate(probe_ctx->root()->type(), TExprOpcode::LE, max_pred,
                                              &max_pred_node));
        vectorized::VExprSPtr max_literal;
        RETURN_IF_ERROR(create_literal(probe_ctx->root()->type(), _context.minmax_func->get_max(),
                                       max_literal));
        max_pred->add_child(probe_ctx->root());
        max_pred->add_child(max_literal);
        container.push_back(
                vectorized::VRuntimeFilterWrapper::create_shared(max_pred_node, max_pred));

        vectorized::VExprContextSPtr new_probe_ctx;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(probe_expr, new_probe_ctx));
        probe_ctxs.push_back(new_probe_ctx);

        // create min filter
        vectorized::VExprSPtr min_pred;
        TExprNode min_pred_node;
        RETURN_IF_ERROR(create_vbin_predicate(new_probe_ctx->root()->type(), TExprOpcode::GE,
                                              min_pred, &min_pred_node));
        vectorized::VExprSPtr min_literal;
        RETURN_IF_ERROR(create_literal(new_probe_ctx->root()->type(),
                                       _context.minmax_func->get_min(), min_literal));
        min_pred->add_child(new_probe_ctx->root());
        min_pred->add_child(min_literal);
        container.push_back(
                vectorized::VRuntimeFilterWrapper::create_shared(min_pred_node, min_pred));
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
        bloom_pred->set_filter(_context.bloom_filter_func);
        bloom_pred->add_child(probe_ctx->root());
        auto wrapper = vectorized::VRuntimeFilterWrapper::create_shared(node, bloom_pred);
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
        bitmap_pred->set_filter(_context.bitmap_filter_func);
        bitmap_pred->add_child(probe_ctx->root());
        auto wrapper = vectorized::VRuntimeFilterWrapper::create_shared(node, bitmap_pred);
        container.push_back(wrapper);
        break;
    }
    default:
        DCHECK(false);
        break;
    }
    return Status::OK();
}

RuntimeFilterWrapperHolder::RuntimeFilterWrapperHolder() = default;
RuntimeFilterWrapperHolder::~RuntimeFilterWrapperHolder() = default;

} // namespace doris
