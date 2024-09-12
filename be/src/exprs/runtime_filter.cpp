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

#include "agent/be_exec_version_manager.h"
#include "common/logging.h"
#include "common/status.h"
#include "exprs/bitmapfilter_predicate.h"
#include "exprs/bloom_filter_func.h"
#include "exprs/create_predicate_function.h"
#include "exprs/hybrid_set.h"
#include "gutil/strings/substitute.h"
#include "pipeline/dependency.h"
#include "runtime/define_primitive_type.h"
#include "runtime/large_int_value.h"
#include "runtime/primitive_type.h"
#include "runtime/runtime_filter_mgr.h"
#include "util/bitmap_value.h"
#include "util/brpc_client_cache.h"
#include "util/ref_count_closure.h"
#include "util/runtime_profile.h"
#include "util/string_parser.hpp"
#include "vec/columns/column.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_nullable.h"
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
    case TYPE_IPV4:
        return PColumnType::COLUMN_TYPE_IPV4;
    case TYPE_IPV6:
        return PColumnType::COLUMN_TYPE_IPV6;
    default:
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "runtime filter meet invalid PrimitiveType type {}", int(type));
    }
}

// PColumnType->PrimitiveType
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
    case PColumnType::COLUMN_TYPE_IPV4:
        return TYPE_IPV4;
    case PColumnType::COLUMN_TYPE_IPV6:
        return TYPE_IPV6;
    default:
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "runtime filter meet invalid PColumnType type {}", int(type));
    }
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
    try {
        TExprNode node = create_texpr_node_from(data, type.type, type.precision, type.scale);
        expr = vectorized::VLiteral::create_shared(node);
    } catch (const Exception& e) {
        return e.to_status();
    }

    return Status::OK();
}

Status create_vbin_predicate(const TypeDescriptor& type, TExprOpcode::type opcode,
                             vectorized::VExprSPtr& expr, TExprNode* tnode,
                             bool contain_null = false) {
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
    node.__set_node_type(contain_null ? TExprNodeType::NULL_AWARE_BINARY_PRED
                                      : TExprNodeType::BINARY_PRED);
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
        return Status::InternalError(
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
    RuntimePredicateWrapper(const RuntimeFilterParams* params)
            : RuntimePredicateWrapper(params->column_return_type, params->filter_type,
                                      params->filter_id) {};
    // for a 'tmp' runtime predicate wrapper
    // only could called assign method or as a param for merge
    RuntimePredicateWrapper(PrimitiveType column_type, RuntimeFilterType type, uint32_t filter_id)
            : _column_return_type(column_type),
              _filter_type(type),
              _context(new RuntimeFilterContext()),
              _filter_id(filter_id) {}

    // init runtime filter wrapper
    // alloc memory to init runtime filter function
    Status init(const RuntimeFilterParams* params) {
        _max_in_num = params->max_in_num;
        switch (_filter_type) {
        case RuntimeFilterType::IN_FILTER: {
            _context->hybrid_set.reset(create_set(_column_return_type));
            _context->hybrid_set->set_null_aware(params->null_aware);
            break;
        }
        // Only use in nested loop join not need set null aware
        case RuntimeFilterType::MIN_FILTER:
        case RuntimeFilterType::MAX_FILTER: {
            _context->minmax_func.reset(create_minmax_filter(_column_return_type));
            break;
        }
        case RuntimeFilterType::MINMAX_FILTER: {
            _context->minmax_func.reset(create_minmax_filter(_column_return_type));
            _context->minmax_func->set_null_aware(params->null_aware);
            break;
        }
        case RuntimeFilterType::BLOOM_FILTER: {
            _context->bloom_filter_func.reset(create_bloom_filter(_column_return_type));
            _context->bloom_filter_func->init_params(params);
            return Status::OK();
        }
        case RuntimeFilterType::IN_OR_BLOOM_FILTER: {
            _context->hybrid_set.reset(create_set(_column_return_type));
            _context->hybrid_set->set_null_aware(params->null_aware);
            _context->bloom_filter_func.reset(create_bloom_filter(_column_return_type));
            _context->bloom_filter_func->init_params(params);
            return Status::OK();
        }
        case RuntimeFilterType::BITMAP_FILTER: {
            _context->bitmap_filter_func.reset(create_bitmap_filter(_column_return_type));
            _context->bitmap_filter_func->set_not_in(params->bitmap_filter_not_in);
            return Status::OK();
        }
        default:
            return Status::InternalError("Unknown Filter type");
        }
        return Status::OK();
    }

    Status change_to_bloom_filter() {
        if (_filter_type != RuntimeFilterType::IN_OR_BLOOM_FILTER) {
            return Status::InternalError(
                    "Can not change to bloom filter because of runtime filter type is {}",
                    IRuntimeFilter::to_string(_filter_type));
        }
        BloomFilterFuncBase* bf = _context->bloom_filter_func.get();

        if (bf != nullptr) {
            insert_to_bloom_filter(bf);
        } else if (_context->hybrid_set != nullptr && _context->hybrid_set->size() != 0) {
            return Status::InternalError("change to bloom filter need empty set ",
                                         IRuntimeFilter::to_string(_filter_type));
        }

        // release in filter
        _context->hybrid_set.reset();
        return Status::OK();
    }

    Status init_bloom_filter(const size_t build_bf_cardinality) {
        DCHECK(_filter_type == RuntimeFilterType::BLOOM_FILTER ||
               _filter_type == RuntimeFilterType::IN_OR_BLOOM_FILTER);
        return _context->bloom_filter_func->init_with_cardinality(build_bf_cardinality);
    }

    bool get_build_bf_cardinality() const {
        if (_filter_type == RuntimeFilterType::BLOOM_FILTER ||
            _filter_type == RuntimeFilterType::IN_OR_BLOOM_FILTER) {
            return _context->bloom_filter_func->get_build_bf_cardinality();
        }
        return false;
    }

    void insert_to_bloom_filter(BloomFilterFuncBase* bloom_filter) const {
        if (_context->hybrid_set->size() > 0) {
            auto* it = _context->hybrid_set->begin();
            while (it->has_next()) {
                bloom_filter->insert(it->get_value());
                it->next();
            }
        }
        if (_context->hybrid_set->contain_null()) {
            bloom_filter->set_contain_null_and_null_aware();
        }
    }

    BloomFilterFuncBase* get_bloomfilter() const { return _context->bloom_filter_func.get(); }

    void insert_fixed_len(const vectorized::ColumnPtr& column, size_t start) {
        DCHECK(!is_ignored());
        switch (_filter_type) {
        case RuntimeFilterType::IN_FILTER: {
            _context->hybrid_set->insert_fixed_len(column, start);
            break;
        }
        case RuntimeFilterType::MIN_FILTER:
        case RuntimeFilterType::MAX_FILTER:
        case RuntimeFilterType::MINMAX_FILTER: {
            _context->minmax_func->insert_fixed_len(column, start);
            break;
        }
        case RuntimeFilterType::BLOOM_FILTER: {
            _context->bloom_filter_func->insert_fixed_len(column, start);
            break;
        }
        case RuntimeFilterType::IN_OR_BLOOM_FILTER: {
            if (is_bloomfilter()) {
                _context->bloom_filter_func->insert_fixed_len(column, start);
            } else {
                _context->hybrid_set->insert_fixed_len(column, start);
            }
            break;
        }
        default:
            DCHECK(false);
            break;
        }
    }

    void insert_batch(const vectorized::ColumnPtr& column, size_t start) {
        if (get_real_type() == RuntimeFilterType::BITMAP_FILTER) {
            bitmap_filter_insert_batch(column, start);
        } else {
            insert_fixed_len(column, start);
        }
    }

    void bitmap_filter_insert_batch(const vectorized::ColumnPtr column, size_t start) {
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
        _context->bitmap_filter_func->insert_many(bitmaps);
    }

    RuntimeFilterType get_real_type() const {
        if (_filter_type == RuntimeFilterType::IN_OR_BLOOM_FILTER) {
            if (_context->hybrid_set) {
                return RuntimeFilterType::IN_FILTER;
            }
            return RuntimeFilterType::BLOOM_FILTER;
        }
        return _filter_type;
    }

    size_t get_bloom_filter_size() const {
        return _context->bloom_filter_func ? _context->bloom_filter_func->get_size() : 0;
    }

    Status get_push_exprs(std::list<vectorized::VExprContextSPtr>& probe_ctxs,
                          std::vector<vectorized::VRuntimeFilterPtr>& push_exprs,
                          const TExpr& probe_expr);

    Status merge(const RuntimePredicateWrapper* wrapper) {
        if (is_ignored() || wrapper->is_ignored()) {
            _context->ignored = true;
            return Status::OK();
        }

        bool can_not_merge_in_or_bloom =
                _filter_type == RuntimeFilterType::IN_OR_BLOOM_FILTER &&
                (wrapper->_filter_type != RuntimeFilterType::IN_FILTER &&
                 wrapper->_filter_type != RuntimeFilterType::BLOOM_FILTER &&
                 wrapper->_filter_type != RuntimeFilterType::IN_OR_BLOOM_FILTER);

        bool can_not_merge_other = _filter_type != RuntimeFilterType::IN_OR_BLOOM_FILTER &&
                                   _filter_type != wrapper->_filter_type;

        CHECK(!can_not_merge_in_or_bloom && !can_not_merge_other)
                << " can not merge runtime filter(id=" << _filter_id
                << "), current is filter type is " << IRuntimeFilter::to_string(_filter_type)
                << ", other filter type is " << IRuntimeFilter::to_string(wrapper->_filter_type);

        switch (_filter_type) {
        case RuntimeFilterType::IN_FILTER: {
            // try insert set
            _context->hybrid_set->insert(wrapper->_context->hybrid_set.get());
            if (_max_in_num >= 0 && _context->hybrid_set->size() >= _max_in_num) {
                _context->ignored = true;
                // release in filter
                _context->hybrid_set.reset();
            }
            break;
        }
        case RuntimeFilterType::MIN_FILTER:
        case RuntimeFilterType::MAX_FILTER:
        case RuntimeFilterType::MINMAX_FILTER: {
            RETURN_IF_ERROR(_context->minmax_func->merge(wrapper->_context->minmax_func.get()));
            break;
        }
        case RuntimeFilterType::BLOOM_FILTER: {
            RETURN_IF_ERROR(
                    _context->bloom_filter_func->merge(wrapper->_context->bloom_filter_func.get()));
            break;
        }
        case RuntimeFilterType::IN_OR_BLOOM_FILTER: {
            auto real_filter_type = get_real_type();

            auto other_filter_type = wrapper->_filter_type;
            if (other_filter_type == RuntimeFilterType::IN_OR_BLOOM_FILTER) {
                other_filter_type = wrapper->get_real_type();
            }

            if (real_filter_type == RuntimeFilterType::IN_FILTER) {
                // when we meet base rf is in-filter, threre only have two case:
                // case1: all input-filter's build_bf_exactly is true, inited by synced global size
                // case2: all input-filter's build_bf_exactly is false, inited by default size
                if (other_filter_type == RuntimeFilterType::IN_FILTER) {
                    _context->hybrid_set->insert(wrapper->_context->hybrid_set.get());
                    if (_max_in_num >= 0 && _context->hybrid_set->size() >= _max_in_num) {
                        // case2: use default size to init bf
                        RETURN_IF_ERROR(_context->bloom_filter_func->init_with_fixed_length());
                        RETURN_IF_ERROR(change_to_bloom_filter());
                    }
                } else {
                    // case1&case2: use input bf directly and insert hybrid set data into bf
                    _context->bloom_filter_func = wrapper->_context->bloom_filter_func;
                    RETURN_IF_ERROR(change_to_bloom_filter());
                }
            } else {
                if (other_filter_type == RuntimeFilterType::IN_FILTER) {
                    // case2: insert data to global filter
                    wrapper->insert_to_bloom_filter(_context->bloom_filter_func.get());
                } else {
                    // case1&case2: all input bf must has same size
                    RETURN_IF_ERROR(_context->bloom_filter_func->merge(
                            wrapper->_context->bloom_filter_func.get()));
                }
            }
            break;
        }
        case RuntimeFilterType::BITMAP_FILTER: {
            // use input bitmap directly because we assume bitmap filter join always have full data
            _context->bitmap_filter_func = wrapper->_context->bitmap_filter_func;
            break;
        }
        default:
            return Status::InternalError("unknown runtime filter");
        }
        return Status::OK();
    }

    Status assign(const PInFilter* in_filter, bool contain_null) {
        _context->hybrid_set.reset(create_set(_column_return_type));
        if (contain_null) {
            _context->hybrid_set->set_null_aware(true);
            _context->hybrid_set->insert((const void*)nullptr);
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
                auto int128_val = StringParser::string_to_int<int128_t>(
                        string_val.c_str(), string_val.length(), &result);
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
                auto int128_val = StringParser::string_to_int<int128_t>(
                        string_val.c_str(), string_val.length(), &result);
                DCHECK(result == StringParser::PARSE_SUCCESS);
                set->insert(&int128_val);
            });
            break;
        }
        case TYPE_DECIMAL256: {
            batch_assign(in_filter, [](std::shared_ptr<HybridSetBase>& set, PColumnValue& column) {
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
                auto int128_val = StringParser::string_to_int<uint128_t>(
                        string_val.c_str(), string_val.length(), &result);
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

    // used by shuffle runtime filter
    // assign this filter by protobuf
    Status assign(const PBloomFilter* bloom_filter, butil::IOBufAsZeroCopyInputStream* data,
                  bool contain_null) {
        // we won't use this class to insert or find any data
        // so any type is ok
        _context->bloom_filter_func.reset(create_bloom_filter(_column_return_type == INVALID_TYPE
                                                                      ? PrimitiveType::TYPE_INT
                                                                      : _column_return_type));
        RETURN_IF_ERROR(_context->bloom_filter_func->assign(data, bloom_filter->filter_length(),
                                                            contain_null));
        return Status::OK();
    }

    // used by shuffle runtime filter
    // assign this filter by protobuf
    Status assign(const PMinMaxFilter* minmax_filter, bool contain_null) {
        _context->minmax_func.reset(create_minmax_filter(_column_return_type));

        if (contain_null) {
            _context->minmax_func->set_null_aware(true);
            _context->minmax_func->set_contain_null();
        }

        switch (_column_return_type) {
        case TYPE_BOOLEAN: {
            bool min_val = minmax_filter->min_val().boolval();
            bool max_val = minmax_filter->max_val().boolval();
            return _context->minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_TINYINT: {
            auto min_val = static_cast<int8_t>(minmax_filter->min_val().intval());
            auto max_val = static_cast<int8_t>(minmax_filter->max_val().intval());
            return _context->minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_SMALLINT: {
            auto min_val = static_cast<int16_t>(minmax_filter->min_val().intval());
            auto max_val = static_cast<int16_t>(minmax_filter->max_val().intval());
            return _context->minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_INT: {
            int32_t min_val = minmax_filter->min_val().intval();
            int32_t max_val = minmax_filter->max_val().intval();
            return _context->minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_BIGINT: {
            int64_t min_val = minmax_filter->min_val().longval();
            int64_t max_val = minmax_filter->max_val().longval();
            return _context->minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_LARGEINT: {
            auto min_string_val = minmax_filter->min_val().stringval();
            auto max_string_val = minmax_filter->max_val().stringval();
            StringParser::ParseResult result;
            auto min_val = StringParser::string_to_int<int128_t>(min_string_val.c_str(),
                                                                 min_string_val.length(), &result);
            DCHECK(result == StringParser::PARSE_SUCCESS);
            auto max_val = StringParser::string_to_int<int128_t>(max_string_val.c_str(),
                                                                 max_string_val.length(), &result);
            DCHECK(result == StringParser::PARSE_SUCCESS);
            return _context->minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_FLOAT: {
            auto min_val = static_cast<float>(minmax_filter->min_val().doubleval());
            auto max_val = static_cast<float>(minmax_filter->max_val().doubleval());
            return _context->minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_DOUBLE: {
            auto min_val = static_cast<double>(minmax_filter->min_val().doubleval());
            auto max_val = static_cast<double>(minmax_filter->max_val().doubleval());
            return _context->minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_DATEV2: {
            int32_t min_val = minmax_filter->min_val().intval();
            int32_t max_val = minmax_filter->max_val().intval();
            return _context->minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_DATETIMEV2: {
            int64_t min_val = minmax_filter->min_val().longval();
            int64_t max_val = minmax_filter->max_val().longval();
            return _context->minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_DATETIME:
        case TYPE_DATE: {
            const auto& min_val_ref = minmax_filter->min_val().stringval();
            const auto& max_val_ref = minmax_filter->max_val().stringval();
            VecDateTimeValue min_val;
            VecDateTimeValue max_val;
            min_val.from_date_str(min_val_ref.c_str(), min_val_ref.length());
            max_val.from_date_str(max_val_ref.c_str(), max_val_ref.length());
            return _context->minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_DECIMALV2: {
            const auto& min_val_ref = minmax_filter->min_val().stringval();
            const auto& max_val_ref = minmax_filter->max_val().stringval();
            DecimalV2Value min_val(min_val_ref);
            DecimalV2Value max_val(max_val_ref);
            return _context->minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_DECIMAL32: {
            int32_t min_val = minmax_filter->min_val().intval();
            int32_t max_val = minmax_filter->max_val().intval();
            return _context->minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_DECIMAL64: {
            int64_t min_val = minmax_filter->min_val().longval();
            int64_t max_val = minmax_filter->max_val().longval();
            return _context->minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_DECIMAL128I: {
            auto min_string_val = minmax_filter->min_val().stringval();
            auto max_string_val = minmax_filter->max_val().stringval();
            StringParser::ParseResult result;
            auto min_val = StringParser::string_to_int<int128_t>(min_string_val.c_str(),
                                                                 min_string_val.length(), &result);
            DCHECK(result == StringParser::PARSE_SUCCESS);
            auto max_val = StringParser::string_to_int<int128_t>(max_string_val.c_str(),
                                                                 max_string_val.length(), &result);
            DCHECK(result == StringParser::PARSE_SUCCESS);
            return _context->minmax_func->assign(&min_val, &max_val);
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
            return _context->minmax_func->assign(&min_val, &max_val);
        }
        case TYPE_VARCHAR:
        case TYPE_CHAR:
        case TYPE_STRING: {
            auto min_val_ref = minmax_filter->min_val().stringval();
            auto max_val_ref = minmax_filter->max_val().stringval();
            return _context->minmax_func->assign(&min_val_ref, &max_val_ref);
        }
        case TYPE_IPV4: {
            int tmp_min = minmax_filter->min_val().intval();
            int tmp_max = minmax_filter->max_val().intval();
            return _context->minmax_func->assign(&tmp_min, &tmp_max);
        }
        case TYPE_IPV6: {
            auto min_string_val = minmax_filter->min_val().stringval();
            auto max_string_val = minmax_filter->max_val().stringval();
            StringParser::ParseResult result;
            auto min_val = StringParser::string_to_int<uint128_t>(min_string_val.c_str(),
                                                                  min_string_val.length(), &result);
            DCHECK(result == StringParser::PARSE_SUCCESS);
            auto max_val = StringParser::string_to_int<uint128_t>(max_string_val.c_str(),
                                                                  max_string_val.length(), &result);
            DCHECK(result == StringParser::PARSE_SUCCESS);
            return _context->minmax_func->assign(&min_val, &max_val);
        }
        default:
            break;
        }
        return Status::InternalError("not support!");
    }

    HybridSetBase::IteratorBase* get_in_filter_iterator() { return _context->hybrid_set->begin(); }

    void get_bloom_filter_desc(char** data, int* filter_length) {
        _context->bloom_filter_func->get_data(data, filter_length);
    }

    void get_minmax_filter_desc(void** min_data, void** max_data) {
        *min_data = _context->minmax_func->get_min();
        *max_data = _context->minmax_func->get_max();
    }

    PrimitiveType column_type() { return _column_return_type; }

    bool is_bloomfilter() const { return get_real_type() == RuntimeFilterType::BLOOM_FILTER; }

    bool contain_null() const {
        if (is_bloomfilter()) {
            return _context->bloom_filter_func->contain_null();
        }
        if (_context->hybrid_set) {
            DCHECK(get_real_type() == RuntimeFilterType::IN_FILTER);
            return _context->hybrid_set->contain_null();
        }
        if (_context->minmax_func) {
            return _context->minmax_func->contain_null();
        }
        return false;
    }

    bool is_ignored() const { return _context->ignored; }

    void set_ignored() { _context->ignored = true; }

    void batch_assign(const PInFilter* filter,
                      void (*assign_func)(std::shared_ptr<HybridSetBase>& _hybrid_set,
                                          PColumnValue&)) {
        for (int i = 0; i < filter->values_size(); ++i) {
            PColumnValue column = filter->values(i);
            assign_func(_context->hybrid_set, column);
        }
    }

    size_t get_in_filter_size() const {
        return _context->hybrid_set ? _context->hybrid_set->size() : 0;
    }

    std::shared_ptr<BitmapFilterFuncBase> get_bitmap_filter() const {
        return _context->bitmap_filter_func;
    }

    friend class IRuntimeFilter;

    void set_filter_id(int id) {
        if (_context->bloom_filter_func) {
            _context->bloom_filter_func->set_filter_id(id);
        }
        if (_context->bitmap_filter_func) {
            _context->bitmap_filter_func->set_filter_id(id);
        }
        if (_context->hybrid_set) {
            _context->hybrid_set->set_filter_id(id);
        }
    }

private:
    // When a runtime filter received from remote and it is a bloom filter, _column_return_type will be invalid.
    PrimitiveType _column_return_type; // column type
    RuntimeFilterType _filter_type;
    int32_t _max_in_num = -1;

    RuntimeFilterContextSPtr _context;
    uint32_t _filter_id;
};

Status IRuntimeFilter::create(RuntimeFilterParamsContext* state, const TRuntimeFilterDesc* desc,
                              const TQueryOptions* query_options, const RuntimeFilterRole role,
                              int node_id, std::shared_ptr<IRuntimeFilter>* res,
                              bool build_bf_exactly, bool need_local_merge) {
    *res = std::make_shared<IRuntimeFilter>(state, desc, need_local_merge);
    (*res)->set_role(role);
    return (*res)->init_with_desc(desc, query_options, node_id, build_bf_exactly);
}

RuntimeFilterContextSPtr& IRuntimeFilter::get_shared_context_ref() {
    return _wrapper->_context;
}

void IRuntimeFilter::insert_batch(const vectorized::ColumnPtr column, size_t start) {
    DCHECK(is_producer());
    _wrapper->insert_batch(column, start);
}

Status IRuntimeFilter::publish(bool publish_local) {
    DCHECK(is_producer());

    auto send_to_remote = [&](IRuntimeFilter* filter) {
        TNetworkAddress addr;
        DCHECK(_state != nullptr);
        RETURN_IF_ERROR(_state->runtime_filter_mgr->get_merge_addr(&addr));
        return filter->push_to_remote(&addr);
    };
    auto send_to_local = [&](std::shared_ptr<RuntimePredicateWrapper> wrapper) {
        std::vector<std::shared_ptr<IRuntimeFilter>> filters;
        RETURN_IF_ERROR(_state->runtime_filter_mgr->get_consume_filters(_filter_id, filters));
        DCHECK(!filters.empty());
        // push down
        for (auto filter : filters) {
            filter->_wrapper = wrapper;
            filter->update_runtime_filter_type_to_profile();
            filter->signal();
        }
        return Status::OK();
    };
    auto do_local_merge = [&]() {
        LocalMergeFilters* local_merge_filters = nullptr;
        RETURN_IF_ERROR(_state->runtime_filter_mgr->get_local_merge_producer_filters(
                _filter_id, &local_merge_filters));
        std::lock_guard l(*local_merge_filters->lock);
        RETURN_IF_ERROR(local_merge_filters->filters[0]->merge_from(_wrapper.get()));
        local_merge_filters->merge_time--;
        if (local_merge_filters->merge_time == 0) {
            if (_has_local_target) {
                RETURN_IF_ERROR(send_to_local(local_merge_filters->filters[0]->_wrapper));
            } else {
                RETURN_IF_ERROR(send_to_remote(local_merge_filters->filters[0].get()));
            }
        }
        return Status::OK();
    };

    if (_need_local_merge && _has_local_target) {
        RETURN_IF_ERROR(do_local_merge());
    } else if (_has_local_target) {
        RETURN_IF_ERROR(send_to_local(_wrapper));
    } else if (!publish_local) {
        if (_is_broadcast_join || _state->be_exec_version < USE_NEW_SERDE) {
            RETURN_IF_ERROR(send_to_remote(this));
        } else {
            RETURN_IF_ERROR(do_local_merge());
        }
    } else {
        // remote broadcast join only push onetime in build shared hash table
        // publish_local only set true on copy shared hash table
        DCHECK(_is_broadcast_join);
    }
    return Status::OK();
}

class SyncSizeClosure : public AutoReleaseClosure<PSendFilterSizeRequest,
                                                  DummyBrpcCallback<PSendFilterSizeResponse>> {
    std::shared_ptr<pipeline::Dependency> _dependency;
    // Should use weak ptr here, because when query context deconstructs, should also delete runtime filter
    // context, it not the memory is not released. And rpc is in another thread, it will hold rf context
    // after query context because the rpc is not returned.
    std::weak_ptr<RuntimeFilterContext> _rf_context;
    std::string _rf_debug_info;
    using Base =
            AutoReleaseClosure<PSendFilterSizeRequest, DummyBrpcCallback<PSendFilterSizeResponse>>;
    ENABLE_FACTORY_CREATOR(SyncSizeClosure);

    void _process_if_rpc_failed() override {
        ((pipeline::CountedFinishDependency*)_dependency.get())->sub();
        LOG(WARNING) << "sync filter size meet rpc error, filter=" << _rf_debug_info;
        Base::_process_if_rpc_failed();
    }

    void _process_if_meet_error_status(const Status& status) override {
        ((pipeline::CountedFinishDependency*)_dependency.get())->sub();
        if (status.is<ErrorCode::END_OF_FILE>()) {
            // rf merger backend may finished before rf's send_filter_size, we just ignore filter in this case.
            auto ctx = _rf_context.lock();
            if (ctx) {
                ctx->ignored = true;
            } else {
                LOG(WARNING) << "sync filter size returned but context is released, filter="
                             << _rf_debug_info;
            }
        } else {
            LOG(WARNING) << "sync filter size meet error status, filter=" << _rf_debug_info;
            Base::_process_if_meet_error_status(status);
        }
    }

public:
    SyncSizeClosure(std::shared_ptr<PSendFilterSizeRequest> req,
                    std::shared_ptr<DummyBrpcCallback<PSendFilterSizeResponse>> callback,
                    std::shared_ptr<pipeline::Dependency> dependency,
                    RuntimeFilterContextSPtr rf_context, std::string_view rf_debug_info)
            : Base(req, callback),
              _dependency(std::move(dependency)),
              _rf_context(rf_context),
              _rf_debug_info(rf_debug_info) {}
};

Status IRuntimeFilter::send_filter_size(RuntimeState* state, uint64_t local_filter_size) {
    DCHECK(is_producer());

    if (_need_local_merge) {
        LocalMergeFilters* local_merge_filters = nullptr;
        RETURN_IF_ERROR(_state->runtime_filter_mgr->get_local_merge_producer_filters(
                _filter_id, &local_merge_filters));
        std::lock_guard l(*local_merge_filters->lock);
        local_merge_filters->merge_size_times--;
        local_merge_filters->local_merged_size += local_filter_size;
        if (local_merge_filters->merge_size_times) {
            return Status::OK();
        } else {
            if (_has_local_target) {
                for (auto filter : local_merge_filters->filters) {
                    filter->set_synced_size(local_merge_filters->local_merged_size);
                }
                return Status::OK();
            } else {
                local_filter_size = local_merge_filters->local_merged_size;
            }
        }
    } else if (_has_local_target) {
        set_synced_size(local_filter_size);
        return Status::OK();
    }

    TNetworkAddress addr;
    DCHECK(_state != nullptr);
    RETURN_IF_ERROR(_state->runtime_filter_mgr->get_merge_addr(&addr));
    std::shared_ptr<PBackendService_Stub> stub(
            _state->exec_env->brpc_internal_client_cache()->get_client(addr));
    if (!stub) {
        return Status::InternalError("Get rpc stub failed, host={}, port={}", addr.hostname,
                                     addr.port);
    }

    auto request = std::make_shared<PSendFilterSizeRequest>();
    auto callback = DummyBrpcCallback<PSendFilterSizeResponse>::create_shared();
    // IRuntimeFilter maybe deconstructed before the rpc finished, so that could not use
    // a raw pointer in closure. Has to use the context's shared ptr.
    auto closure = SyncSizeClosure::create_unique(request, callback, _dependency,
                                                  _wrapper->_context, this->debug_string());
    auto* pquery_id = request->mutable_query_id();
    pquery_id->set_hi(_state->query_id.hi());
    pquery_id->set_lo(_state->query_id.lo());

    auto* source_addr = request->mutable_source_addr();
    source_addr->set_hostname(BackendOptions::get_local_backend().host);
    source_addr->set_port(BackendOptions::get_local_backend().brpc_port);

    request->set_filter_size(local_filter_size);
    request->set_filter_id(_filter_id);
    callback->cntl_->set_timeout_ms(std::min(3600, state->execution_timeout()) * 1000);

    stub->send_filter_size(closure->cntl_.get(), closure->request_.get(), closure->response_.get(),
                           closure.get());
    closure.release();
    return Status::OK();
}

Status IRuntimeFilter::push_to_remote(const TNetworkAddress* addr) {
    DCHECK(is_producer());
    std::shared_ptr<PBackendService_Stub> stub(
            _state->exec_env->brpc_internal_client_cache()->get_client(*addr));
    if (!stub) {
        return Status::InternalError(
                fmt::format("Get rpc stub failed, host={}, port={}", addr->hostname, addr->port));
    }

    auto merge_filter_request = std::make_shared<PMergeFilterRequest>();
    auto merge_filter_callback = DummyBrpcCallback<PMergeFilterResponse>::create_shared();
    auto merge_filter_closure =
            AutoReleaseClosure<PMergeFilterRequest, DummyBrpcCallback<PMergeFilterResponse>>::
                    create_unique(merge_filter_request, merge_filter_callback);
    void* data = nullptr;
    int len = 0;

    auto* pquery_id = merge_filter_request->mutable_query_id();
    pquery_id->set_hi(_state->query_id.hi());
    pquery_id->set_lo(_state->query_id.lo());

    auto* pfragment_instance_id = merge_filter_request->mutable_fragment_instance_id();
    pfragment_instance_id->set_hi(BackendOptions::get_local_backend().id);
    pfragment_instance_id->set_lo((int64_t)this);

    merge_filter_request->set_filter_id(_filter_id);
    merge_filter_request->set_is_pipeline(true);
    auto column_type = _wrapper->column_type();
    RETURN_IF_CATCH_EXCEPTION(merge_filter_request->set_column_type(to_proto(column_type)));
    merge_filter_callback->cntl_->set_timeout_ms(wait_time_ms());

    if (get_ignored()) {
        merge_filter_request->set_filter_type(PFilterType::UNKNOW_FILTER);
        merge_filter_request->set_ignored(true);
    } else {
        RETURN_IF_ERROR(serialize(merge_filter_request.get(), &data, &len));
    }

    if (len > 0) {
        DCHECK(data != nullptr);
        merge_filter_callback->cntl_->request_attachment().append(data, len);
    }

    stub->merge_filter(merge_filter_closure->cntl_.get(), merge_filter_closure->request_.get(),
                       merge_filter_closure->response_.get(), merge_filter_closure.get());
    // the closure will be released by brpc during closure->Run.
    merge_filter_closure.release();
    return Status::OK();
}

Status IRuntimeFilter::get_push_expr_ctxs(std::list<vectorized::VExprContextSPtr>& probe_ctxs,
                                          std::vector<vectorized::VRuntimeFilterPtr>& push_exprs,
                                          bool is_late_arrival) {
    DCHECK(is_consumer());
    auto origin_size = push_exprs.size();
    if (!_wrapper->is_ignored()) {
        _set_push_down(!is_late_arrival);
        RETURN_IF_ERROR(_wrapper->get_push_exprs(probe_ctxs, push_exprs, _probe_expr));
    }
    _profile->add_info_string("Info", formatted_state());
    // The runtime filter is pushed down, adding filtering information.
    auto* expr_filtered_rows_counter = ADD_COUNTER(_profile, "expr_filtered_rows", TUnit::UNIT);
    auto* expr_input_rows_counter = ADD_COUNTER(_profile, "expr_input_rows", TUnit::UNIT);
    auto* always_true_counter = ADD_COUNTER(_profile, "always_true", TUnit::UNIT);
    for (auto i = origin_size; i < push_exprs.size(); i++) {
        push_exprs[i]->attach_profile_counter(expr_filtered_rows_counter, expr_input_rows_counter,
                                              always_true_counter);
    }
    return Status::OK();
}

void IRuntimeFilter::update_state() {
    DCHECK(is_consumer());
    auto execution_timeout = _state->execution_timeout * 1000;
    auto runtime_filter_wait_time_ms = _state->runtime_filter_wait_time_ms;
    // bitmap filter is precise filter and only filter once, so it must be applied.
    int64_t wait_times_ms = _wrapper->get_real_type() == RuntimeFilterType::BITMAP_FILTER
                                    ? execution_timeout
                                    : runtime_filter_wait_time_ms;
    auto expected = _rf_state_atomic.load(std::memory_order_acquire);
    // In pipelineX, runtime filters will be ready or timeout before open phase.
    if (expected == RuntimeFilterState::NOT_READY) {
        DCHECK(MonotonicMillis() - registration_time_ >= wait_times_ms);
        _rf_state_atomic = RuntimeFilterState::TIME_OUT;
    }
}

// NOTE: Wait infinitely will not make scan task wait really forever.
// Because BlockTaskSchedule will make it run when query is timedout.
bool IRuntimeFilter::wait_infinitely() const {
    // bitmap filter is precise filter and only filter once, so it must be applied.
    return _wait_infinitely ||
           (_wrapper != nullptr && _wrapper->get_real_type() == RuntimeFilterType::BITMAP_FILTER);
}

PrimitiveType IRuntimeFilter::column_type() const {
    return _wrapper->column_type();
}

void IRuntimeFilter::signal() {
    DCHECK(is_consumer());
    _rf_state_atomic.store(RuntimeFilterState::READY);
    if (!_filter_timer.empty()) {
        for (auto& timer : _filter_timer) {
            timer->call_ready();
        }
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

void IRuntimeFilter::set_filter_timer(std::shared_ptr<pipeline::RuntimeFilterTimer> timer) {
    _filter_timer.push_back(timer);
}

void IRuntimeFilter::set_dependency(std::shared_ptr<pipeline::Dependency> dependency) {
    _dependency = dependency;
    ((pipeline::CountedFinishDependency*)_dependency.get())->add();
    CHECK(_dependency);
}

void IRuntimeFilter::set_synced_size(uint64_t global_size) {
    _synced_size = global_size;
    if (_dependency) {
        ((pipeline::CountedFinishDependency*)_dependency.get())->sub();
    }
}

void IRuntimeFilter::set_ignored() {
    _wrapper->_context->ignored = true;
}

bool IRuntimeFilter::get_ignored() {
    return _wrapper->is_ignored();
}

std::string IRuntimeFilter::formatted_state() const {
    return fmt::format(
            "[IsPushDown = {}, RuntimeFilterState = {}, HasRemoteTarget = {}, "
            "HasLocalTarget = {}, Ignored = {}]",
            _is_push_down, _get_explain_state_string(), _has_remote_target, _has_local_target,
            _wrapper->_context->ignored);
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
    vectorized::VExprContextSPtr build_ctx;
    RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(desc->src_expr, build_ctx));

    RuntimeFilterParams params;
    params.filter_id = _filter_id;
    params.filter_type = _runtime_filter_type;
    params.column_return_type = build_ctx->root()->type().type;
    params.max_in_num = options->runtime_filter_max_in_num;
    params.runtime_bloom_filter_min_size = options->__isset.runtime_bloom_filter_min_size
                                                   ? options->runtime_bloom_filter_min_size
                                                   : 0;
    params.runtime_bloom_filter_max_size = options->__isset.runtime_bloom_filter_max_size
                                                   ? options->runtime_bloom_filter_max_size
                                                   : 0;
    // We build runtime filter by exact distinct count iff three conditions are met:
    // 1. Only 1 join key
    // 2. Do not have remote target (e.g. do not need to merge), or broadcast join
    // 3. Bloom filter
    params.build_bf_exactly =
            build_bf_exactly && (_runtime_filter_type == RuntimeFilterType::BLOOM_FILTER ||
                                 _runtime_filter_type == RuntimeFilterType::IN_OR_BLOOM_FILTER);

    params.bloom_filter_size_calculated_by_ndv = desc->bloom_filter_size_calculated_by_ndv;

    if (!desc->__isset.sync_filter_size || !desc->sync_filter_size) {
        params.build_bf_exactly &= (!_has_remote_target || _is_broadcast_join);
    }

    if (desc->__isset.bloom_filter_size_bytes) {
        params.bloom_filter_size = desc->bloom_filter_size_bytes;
    }
    if (desc->__isset.null_aware) {
        params.null_aware = desc->null_aware;
    }
    if (_runtime_filter_type == RuntimeFilterType::BITMAP_FILTER) {
        if (!build_ctx->root()->type().is_bitmap_type()) {
            return Status::InternalError("Unexpected src expr type:{} for bitmap filter.",
                                         build_ctx->root()->type().debug_string());
        }
        if (!desc->__isset.bitmap_target_expr) {
            return Status::InternalError("Unknown bitmap filter target expr.");
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

    _wrapper = std::make_shared<RuntimePredicateWrapper>(&params);
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

Status IRuntimeFilter::create_wrapper(const MergeRuntimeFilterParams* param,
                                      std::unique_ptr<RuntimePredicateWrapper>* wrapper) {
    return _create_wrapper(param, wrapper);
}

Status IRuntimeFilter::create_wrapper(const UpdateRuntimeFilterParams* param,
                                      std::unique_ptr<RuntimePredicateWrapper>* wrapper) {
    return _create_wrapper(param, wrapper);
}

Status IRuntimeFilter::create_wrapper(const UpdateRuntimeFilterParamsV2* param,
                                      std::shared_ptr<RuntimePredicateWrapper>* wrapper) {
    auto filter_type = param->request->filter_type();
    PrimitiveType column_type = param->column_type;
    *wrapper = std::make_shared<RuntimePredicateWrapper>(column_type, get_type(filter_type),
                                                         param->request->filter_id());

    if (param->request->has_ignored() && param->request->ignored()) {
        (*wrapper)->set_ignored();
        return Status::OK();
    }

    switch (filter_type) {
    case PFilterType::IN_FILTER: {
        DCHECK(param->request->has_in_filter());
        return (*wrapper)->assign(&param->request->in_filter(), param->request->contain_null());
    }
    case PFilterType::BLOOM_FILTER: {
        DCHECK(param->request->has_bloom_filter());
        return (*wrapper)->assign(&param->request->bloom_filter(), param->data,
                                  param->request->contain_null());
    }
    case PFilterType::MIN_FILTER:
    case PFilterType::MAX_FILTER:
    case PFilterType::MINMAX_FILTER: {
        DCHECK(param->request->has_minmax_filter());
        return (*wrapper)->assign(&param->request->minmax_filter(), param->request->contain_null());
    }
    default:
        return Status::InternalError("unknown filter type");
    }
}

Status IRuntimeFilter::change_to_bloom_filter() {
    RETURN_IF_ERROR(_wrapper->change_to_bloom_filter());
    return Status::OK();
}

Status IRuntimeFilter::init_bloom_filter(const size_t build_bf_cardinality) {
    return _wrapper->init_bloom_filter(build_bf_cardinality);
}

template <class T>
Status IRuntimeFilter::_create_wrapper(const T* param,
                                       std::unique_ptr<RuntimePredicateWrapper>* wrapper) {
    int filter_type = param->request->filter_type();
    if (!param->request->has_column_type()) {
        return Status::InternalError("unknown filter column type");
    }
    PrimitiveType column_type = to_primitive_type(param->request->column_type());
    *wrapper = std::make_unique<RuntimePredicateWrapper>(column_type, get_type(filter_type),
                                                         param->request->filter_id());

    if (param->request->has_ignored() && param->request->ignored()) {
        (*wrapper)->set_ignored();
        return Status::OK();
    }

    switch (filter_type) {
    case PFilterType::IN_FILTER: {
        DCHECK(param->request->has_in_filter());
        return (*wrapper)->assign(&param->request->in_filter(), param->request->contain_null());
    }
    case PFilterType::BLOOM_FILTER: {
        DCHECK(param->request->has_bloom_filter());
        return (*wrapper)->assign(&param->request->bloom_filter(), param->data,
                                  param->request->contain_null());
    }
    case PFilterType::MIN_FILTER:
    case PFilterType::MAX_FILTER:
    case PFilterType::MINMAX_FILTER: {
        DCHECK(param->request->has_minmax_filter());
        return (*wrapper)->assign(&param->request->minmax_filter(), param->request->contain_null());
    }
    default:
        return Status::InternalError("unknown filter type");
    }
}

void IRuntimeFilter::init_profile(RuntimeProfile* parent_profile) {
    if (_profile_init) {
        parent_profile->add_child(_profile.get(), true, nullptr);
    } else {
        _profile_init = true;
        parent_profile->add_child(_profile.get(), true, nullptr);
        _profile->add_info_string("Info", formatted_state());
    }
}

void IRuntimeFilter::update_runtime_filter_type_to_profile() {
    _profile->add_info_string("RealRuntimeFilterType", to_string(_wrapper->get_real_type()));
}

std::string IRuntimeFilter::debug_string() const {
    return fmt::format(
            "RuntimeFilter: (id = {}, type = {}, need_local_merge: {}, is_broadcast: {}, "
            "build_bf_cardinality: {}",
            _filter_id, to_string(_runtime_filter_type), _need_local_merge, _is_broadcast_join,
            _wrapper->get_build_bf_cardinality());
}

Status IRuntimeFilter::merge_from(const RuntimePredicateWrapper* wrapper) {
    auto status = _wrapper->merge(wrapper);
    if (!status) {
        return Status::InternalError("runtime filter merge failed: {}, error_msg: {}",
                                     debug_string(), status.msg());
    }
    return Status::OK();
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
    auto real_runtime_filter_type = _wrapper->get_real_type();

    request->set_filter_type(get_type(real_runtime_filter_type));
    request->set_contain_null(_wrapper->contain_null());

    if (real_runtime_filter_type == RuntimeFilterType::IN_FILTER) {
        auto in_filter = request->mutable_in_filter();
        to_protobuf(in_filter);
    } else if (real_runtime_filter_type == RuntimeFilterType::BLOOM_FILTER) {
        _wrapper->get_bloom_filter_desc((char**)data, len);
        DCHECK(data != nullptr);
        request->mutable_bloom_filter()->set_filter_length(*len);
        request->mutable_bloom_filter()->set_always_true(false);
    } else if (real_runtime_filter_type == RuntimeFilterType::MINMAX_FILTER ||
               real_runtime_filter_type == RuntimeFilterType::MIN_FILTER ||
               real_runtime_filter_type == RuntimeFilterType::MAX_FILTER) {
        auto minmax_filter = request->mutable_minmax_filter();
        to_protobuf(minmax_filter);
    } else {
        return Status::InternalError("not implemented !");
    }
    return Status::OK();
}

void IRuntimeFilter::to_protobuf(PInFilter* filter) {
    auto column_type = _wrapper->column_type();
    filter->set_column_type(to_proto(column_type));

    auto* it = _wrapper->get_in_filter_iterator();
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
        //const void* void_value = it->get_value();
        //Now the get_value return void* is StringRef
        batch_copy<StringRef>(filter, it, [](PColumnValue* column, const StringRef* value) {
            column->set_stringval(value->to_string());
        });
        return;
    }
    case TYPE_IPV4: {
        batch_copy<IPv4>(filter, it, [](PColumnValue* column, const IPv4* value) {
            column->set_intval(*reinterpret_cast<const int32_t*>(value));
        });
        return;
    }
    case TYPE_IPV6: {
        batch_copy<IPv6>(filter, it, [](PColumnValue* column, const IPv6* value) {
            column->set_stringval(LargeIntValue::to_string(*value));
        });
        return;
    }
    default: {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "runtime filter meet invalid PrimitiveType type {}", int(column_type));
    }
    }
}

void IRuntimeFilter::to_protobuf(PMinMaxFilter* filter) {
    void* min_data = nullptr;
    void* max_data = nullptr;
    _wrapper->get_minmax_filter_desc(&min_data, &max_data);
    DCHECK(min_data != nullptr && max_data != nullptr);
    filter->set_column_type(to_proto(_wrapper->column_type()));

    switch (_wrapper->column_type()) {
    case TYPE_BOOLEAN: {
        filter->mutable_min_val()->set_boolval(*reinterpret_cast<const bool*>(min_data));
        filter->mutable_max_val()->set_boolval(*reinterpret_cast<const bool*>(max_data));
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
        const auto* min_string_value = reinterpret_cast<const std::string*>(min_data);
        filter->mutable_min_val()->set_stringval(*min_string_value);
        const auto* max_string_value = reinterpret_cast<const std::string*>(max_data);
        filter->mutable_max_val()->set_stringval(*max_string_value);
        break;
    }
    case TYPE_IPV4: {
        filter->mutable_min_val()->set_intval(*reinterpret_cast<const int32_t*>(min_data));
        filter->mutable_max_val()->set_intval(*reinterpret_cast<const int32_t*>(max_data));
        return;
    }
    case TYPE_IPV6: {
        filter->mutable_min_val()->set_stringval(
                LargeIntValue::to_string(*reinterpret_cast<const uint128_t*>(min_data)));
        filter->mutable_max_val()->set_stringval(
                LargeIntValue::to_string(*reinterpret_cast<const uint128_t*>(max_data)));
        return;
    }
    default: {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "runtime filter meet invalid PrimitiveType type {}",
                        int(_wrapper->column_type()));
    }
    }
}

RuntimeFilterType IRuntimeFilter::get_real_type() {
    return _wrapper->get_real_type();
}

bool IRuntimeFilter::need_sync_filter_size() {
    return (type() == RuntimeFilterType::IN_OR_BLOOM_FILTER ||
            type() == RuntimeFilterType::BLOOM_FILTER) &&
           _wrapper->get_build_bf_cardinality() && !_is_broadcast_join;
}

Status IRuntimeFilter::update_filter(const UpdateRuntimeFilterParams* param) {
    _profile->add_info_string("MergeTime", std::to_string(param->request->merge_time()) + " ms");

    if (param->request->has_ignored() && param->request->ignored()) {
        set_ignored();
    } else {
        std::unique_ptr<RuntimePredicateWrapper> wrapper;
        RETURN_IF_ERROR(IRuntimeFilter::create_wrapper(param, &wrapper));
        RETURN_IF_ERROR(_wrapper->merge(wrapper.get()));
        update_runtime_filter_type_to_profile();
    }
    this->signal();

    return Status::OK();
}

void IRuntimeFilter::update_filter(std::shared_ptr<RuntimePredicateWrapper> wrapper,
                                   int64_t merge_time, int64_t start_apply) {
    _profile->add_info_string("UpdateTime",
                              std::to_string(MonotonicMillis() - start_apply) + " ms");
    _profile->add_info_string("MergeTime", std::to_string(merge_time) + " ms");
    // prevent apply filter to not have right column_return_type remove
    // the code in the future
    if (_wrapper->column_type() != wrapper->column_type()) {
        wrapper->_column_return_type = _wrapper->_column_return_type;
    }
    _wrapper = wrapper;
    update_runtime_filter_type_to_profile();
    signal();
}

Status RuntimePredicateWrapper::get_push_exprs(
        std::list<vectorized::VExprContextSPtr>& probe_ctxs,
        std::vector<vectorized::VRuntimeFilterPtr>& container, const TExpr& probe_expr) {
    vectorized::VExprContextSPtr probe_ctx;
    RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(probe_expr, probe_ctx));
    probe_ctxs.push_back(probe_ctx);
    set_filter_id(_filter_id);
    DCHECK(probe_ctx->root()->type().type == _column_return_type ||
           (is_string_type(probe_ctx->root()->type().type) &&
            is_string_type(_column_return_type)) ||
           _filter_type == RuntimeFilterType::BITMAP_FILTER)
            << " prob_expr->root()->type().type: " << int(probe_ctx->root()->type().type)
            << " _column_return_type: " << int(_column_return_type)
            << " _filter_type: " << IRuntimeFilter::to_string(_filter_type);

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
        auto in_pred = vectorized::VDirectInPredicate::create_shared(node, _context->hybrid_set);
        in_pred->add_child(probe_ctx->root());
        auto wrapper = vectorized::VRuntimeFilterWrapper::create_shared(
                node, in_pred, get_in_list_ignore_thredhold(_context->hybrid_set->size()),
                null_aware);
        container.push_back(wrapper);
        break;
    }
    case RuntimeFilterType::MIN_FILTER: {
        // create min filter
        vectorized::VExprSPtr min_pred;
        TExprNode min_pred_node;
        RETURN_IF_ERROR(create_vbin_predicate(probe_ctx->root()->type(), TExprOpcode::GE, min_pred,
                                              &min_pred_node));
        vectorized::VExprSPtr min_literal;
        RETURN_IF_ERROR(create_literal(probe_ctx->root()->type(), _context->minmax_func->get_min(),
                                       min_literal));
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
                                              &max_pred_node));
        vectorized::VExprSPtr max_literal;
        RETURN_IF_ERROR(create_literal(probe_ctx->root()->type(), _context->minmax_func->get_max(),
                                       max_literal));
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
        RETURN_IF_ERROR(create_literal(probe_ctx->root()->type(), _context->minmax_func->get_max(),
                                       max_literal));
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
        RETURN_IF_ERROR(create_literal(new_probe_ctx->root()->type(),
                                       _context->minmax_func->get_min(), min_literal));
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
        bloom_pred->set_filter(_context->bloom_filter_func);
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
        bitmap_pred->set_filter(_context->bitmap_filter_func);
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

RuntimeFilterWrapperHolder::RuntimeFilterWrapperHolder() = default;
RuntimeFilterWrapperHolder::~RuntimeFilterWrapperHolder() = default;

} // namespace doris
